use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Instant, SystemTime};

use axum::Json;
use axum::body::{Body, BodyDataStream, Bytes};
use axum::extract::{Request, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderName, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_core::Stream;
use serde::Deserialize;
use serde_json::json;

use crate::capture::Capture;
use crate::config::Config;
use crate::fallback::{self, FallbackRequest};
use crate::route_state::RouteState;
use crate::route_updates::{RequestOutcome, RouteUpdates};
use crate::router::{self, RouteDecision};
use crate::state::AppState;

const MESSAGES_PATH: &str = "/v1/messages";
const COUNT_TOKENS_PATH: &str = "/v1/messages/count_tokens";

/// A request body has to be in hand before its route is known — the `model`
/// that decides it is inside. This is what keeps that bounded (Global
/// Constraint 3): a body past the cap is not inspected at all, and the bytes
/// already read are handed back in front of the rest of the stream, so the
/// Anthropic route still forwards exactly what the client sent. Far larger
/// than any real Claude Code request, including one carrying images, so what
/// it protects against is a runaway rather than ordinary traffic — and it is
/// per request, with no bound across concurrent ones, which is the reason to
/// keep it as small as that allows rather than as large as Anthropic's own
/// request-size limit.
///
/// The cost of exceeding it is a lost routing decision, not a failed request:
/// a `claude-*` request that would have failed over goes to Anthropic instead,
/// and a name-routed one reaches Anthropic under a name Anthropic will reject.
pub(crate) const ROUTING_BODY_CAP: usize = 8 * 1024 * 1024;

pub async fn forward(State(state): State<AppState>, request: Request) -> Response {
    let start = Instant::now();
    let (parts, body) = request.into_parts();

    let path = parts.uri.path();
    let count_tokens = path == COUNT_TOKENS_PATH;
    // Every other path under `/v1` carries no `model` to route on. With no
    // profile configured, nothing can route anywhere else either: the router
    // could only ever answer `Anthropic`, so reading the body would buy a
    // decision that is already made. Both cases keep Milestone 1's streamed
    // verbatim forward, body untouched.
    if !(count_tokens || path == MESSAGES_PATH) || state.config.profiles.is_empty() {
        let body = reqwest::Body::wrap_stream(body.into_data_stream());
        return to_anthropic(&state, start, &parts, body, None).await;
    }

    let buffered = match read_for_routing(body, state.routing_body_cap).await {
        Ok(buffered) => buffered,
        Err(err) => {
            tracing::warn!(error = %err, "could not read the request body");
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "request_body_read_failed" })),
            )
                .into_response();
        }
    };

    let body = match buffered {
        RequestBody::TooLarge(rest) => {
            tracing::warn!(
                cap_bytes = state.routing_body_cap,
                "request body too large to route on; forwarding to Anthropic"
            );
            let body = reqwest::Body::wrap_stream(rest);
            return to_anthropic(&state, start, &parts, body, None).await;
        }
        RequestBody::Buffered(body) => body,
    };

    let view = RoutingView::parse(&body);
    let Some(model) = view.model.clone() else {
        // No `model` to route on, so §7d has nothing to say: Anthropic, and
        // its own error if the request was malformed.
        return to_anthropic(&state, start, &parts, body.into(), None).await;
    };

    // Read once, here, at the point the route is decided: `active_profile`
    // covers both the startup default and any `/control/profile` switch, and
    // this value — not a later re-read of either — is what the rest of this
    // request (including a streamed response) is bound to (spec §8b: a
    // switch applies to new requests only).
    let active_profile = state.active_profile();
    let named = router::route(&model, &state.config.profiles, active_profile.as_deref());
    let target = match named {
        // §7d: routed by name, so the name is passed through unremapped. A
        // `count_tokens` request may only go to a profile that can actually
        // count — see `counts_tokens`.
        Ok(RouteDecision::Profile(name)) => {
            (!count_tokens || counts_tokens(&state, &name)).then_some((name, false))
        }
        // Spec §6: `count_tokens` never fails over, whatever the route state
        // and whatever the policy mode. Anything else may, and that one *is*
        // remapped (§7a).
        Ok(RouteDecision::Anthropic) if count_tokens => None,
        Ok(RouteDecision::Anthropic) => failover(&state, &view, active_profile)
            .await
            .map(|name| (name, true)),
        // Global Constraint 7 from the other side: a name nothing claims has
        // no route but Anthropic's, and a count is pinned there regardless.
        // Answering the relay's own 400 would put the relay's opinion of the
        // name where the tokenizer's belongs (spec §6: "on failure, pass the
        // error through").
        Err(_) if count_tokens => None,
        Err(err) => {
            tracing::warn!(model = %model, error = %err, "no route for the requested model");
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "no_route_for_model" })),
            )
                .into_response();
        }
    };

    let Some((name, remap)) = target else {
        return to_anthropic(&state, start, &parts, body.into(), Some(model)).await;
    };

    let Some(profile) = state.config.profiles.get(&name) else {
        // Startup validation rules this out. If it happens anyway, the
        // always-available route is a better answer than a 500.
        tracing::error!(profile = %name, "routed to an unconfigured profile; staying on Anthropic");
        return to_anthropic(&state, start, &parts, body.into(), Some(model)).await;
    };

    fallback::forward(
        &state,
        start,
        parts.method.clone(),
        parts.uri.path().to_owned(),
        body,
        FallbackRequest {
            profile_name: &name,
            profile,
            model: &model,
            remap,
        },
    )
    .await
}

/// Milestone 1's route, unchanged: everything the client sent, forwarded
/// verbatim to Anthropic, and everything Anthropic sent, streamed back
/// verbatim. The only additions are the `route`/`model` log fields.
async fn to_anthropic(
    state: &AppState,
    start: Instant,
    parts: &Parts,
    body: reqwest::Body,
    model: Option<String>,
) -> Response {
    let target = format!(
        "{}{}",
        state.config.anthropic.base_url.trim_end_matches('/'),
        parts
            .uri
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or_else(|| parts.uri.path())
    );

    let method = parts.method.clone();
    let path = parts.uri.path().to_owned();

    let upstream = state
        .http
        .request(method.clone(), target)
        .headers(forwardable(&parts.headers))
        .body(body)
        .send()
        .await;

    let upstream = match upstream {
        Ok(upstream) => upstream,
        Err(err) => {
            tracing::warn!(
                method = %method,
                path = %path,
                latency_ms = elapsed_ms(start),
                // `without_url` keeps any credentials embedded in `base_url` out of the log.
                error = %err.without_url(),
                "upstream request failed"
            );
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": "upstream_unreachable" })),
            )
                .into_response();
        }
    };

    let status = upstream.status();
    let headers = forwardable(upstream.headers());
    // Latency is taken at response headers, not at end of body: a streamed
    // response stays open for as long as the model keeps generating.
    let log = RequestLog {
        route: "anthropic",
        profile: None,
        model_in: model.clone(),
        model_out: model,
        method,
        path,
        status,
        latency_ms: elapsed_ms(start),
    };

    // Spec §4's `PROBING -> ACTIVE`: the response headers are the success, so
    // this does not wait for a stream that may run for minutes.
    if status.is_success() {
        state.route_updates.record(RequestOutcome::Succeeded);
    }

    // 2xx responses are never observed, so they pay no clone/accumulate cost at
    // all. Below 2xx, the body is accumulated once and read by both consumers:
    // limit detection always (it cannot depend on a debug flag being set), and
    // a `--capture-errors` fixture when the flag is on.
    let observation = (!status.is_success())
        .then(|| ErrorObservation::new(state, status, &headers))
        .flatten();

    let body = Body::from_stream(CountingStream {
        inner: Box::pin(upstream.bytes_stream()),
        response_bytes: 0,
        log: Some(log),
        observation,
    });

    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

/// Whether a `count_tokens` request may be routed to this profile at all.
///
/// Global Constraint 7 pins `count_tokens` to Anthropic; spec §7d routes every
/// non-`claude-*` name to the profile that claims it. They only reconcile for
/// an `anthropic`-format profile, whose `/v1/messages/count_tokens` this route
/// mirrors and which answers in the shape the client is expecting. An
/// `openai`-format profile has no counting endpoint at all: the request would
/// go to `/v1/chat/completions`, bill a real inference call, and come back as
/// a `message` where the client wanted `{"input_tokens": N}`. So that one
/// keeps the Anthropic pin — not a route this can safely extend to.
fn counts_tokens(state: &AppState, profile: &str) -> bool {
    state
        .config
        .profiles
        .get(profile)
        .is_some_and(|profile| profile.format == "anthropic")
}

/// Spec §6's failover decision, reached only by a `claude-*` request the
/// router already pointed at Anthropic. `Some(profile)` fails it over;
/// `None` leaves it on Anthropic, where a limit error passes through to the
/// client as the visible failure the mode asked for.
///
/// `active_profile` is a parameter, not a second call to
/// `state.active_profile()`: the caller already read it once, at the single
/// point this request's route is decided, and passing it in makes "read
/// exactly once per request" structural rather than something that happens
/// to hold because the other branch that would re-read it is unreachable
/// from here.
async fn failover(
    state: &AppState,
    view: &RoutingView,
    active_profile: Option<String>,
) -> Option<String> {
    let active = active_profile?;
    let policy = &state.config.policy;
    let eligible = match policy.mode.as_str() {
        "all" => true,
        // The session-start heuristic: a conversation with no assistant turn
        // yet has no thought to switch models in the middle of. Known
        // imperfection, per spec §6 — Claude Code's own title-generation and
        // summarization requests look like session starts too, and land on the
        // fallback harmlessly. Not worth engineering around.
        "new-sessions" => view.is_session_start(),
        // "notify-only"; startup validation admits no other value.
        _ => false,
    };
    // The state query comes last because it is the expensive half: it can
    // write the state file on the lazy `Limited -> Probing` transition, so a
    // request that could not fail over anyway never pays for it.
    if !eligible || !is_limited(state).await {
        return None;
    }
    Some(active)
}

async fn is_limited(state: &AppState) -> bool {
    let route = state.route.clone();
    // Same reason `/status` does this: the query can persist a transition, and
    // a synchronous file write does not belong on an async worker.
    match tokio::task::spawn_blocking(move || route.current_state()).await {
        Ok(state) => matches!(state, RouteState::Limited { .. }),
        Err(err) => {
            tracing::warn!(error = %err, "route state query failed; staying on the Anthropic route");
            false
        }
    }
}

/// The only two things a routing decision reads out of a `/v1/messages` body:
/// which model it asks for (§7d) and whether the conversation has an assistant
/// turn yet (§6). Every other field is skipped rather than materialized, so a
/// megabyte of conversation is walked but not copied.
#[derive(Debug, Default, Deserialize)]
struct RoutingView {
    model: Option<String>,
    #[serde(default)]
    messages: Vec<RoutingMessage>,
}

#[derive(Debug, Deserialize)]
struct RoutingMessage {
    role: Option<String>,
}

impl RoutingView {
    /// A body this cannot read is a body with no route to decide — the request
    /// goes to Anthropic and gets Anthropic's own opinion of it. This proxy
    /// does not validate requests.
    fn parse(body: &[u8]) -> Self {
        serde_json::from_slice(body).unwrap_or_default()
    }

    fn is_session_start(&self) -> bool {
        !self
            .messages
            .iter()
            .any(|message| message.role.as_deref() == Some("assistant"))
    }
}

enum RequestBody {
    Buffered(Bytes),
    /// The prefix already read, in front of the rest of the client's stream.
    TooLarge(Prefixed),
}

async fn read_for_routing(body: Body, cap: usize) -> Result<RequestBody, axum::Error> {
    let mut stream = Box::pin(body.into_data_stream());
    let mut buffered: Vec<u8> = Vec::new();
    loop {
        let Some(chunk) = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await else {
            return Ok(RequestBody::Buffered(Bytes::from(buffered)));
        };
        let chunk = chunk?;
        let over_cap = buffered.len() + chunk.len() > cap;
        buffered.extend_from_slice(&chunk);
        if over_cap {
            return Ok(RequestBody::TooLarge(Prefixed {
                prefix: Some(Bytes::from(buffered)),
                rest: stream,
            }));
        }
    }
}

struct Prefixed {
    prefix: Option<Bytes>,
    rest: Pin<Box<BodyDataStream>>,
}

impl Stream for Prefixed {
    type Item = Result<Bytes, axum::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(prefix) = this.prefix.take() {
            return Poll::Ready(Some(Ok(prefix)));
        }
        this.rest.as_mut().poll_next(cx)
    }
}

pub(crate) fn elapsed_ms(start: Instant) -> u64 {
    start.elapsed().as_millis() as u64
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "host" | "content-length" | "transfer-encoding" | "connection"
    )
}

/// Everything the peer sent, minus the hop-by-hop headers the next connection
/// recomputes for itself and minus the relay's own audit marker, which only
/// the relay may set.
///
/// Both of the Anthropic route's call sites share this, so `x-relay-route` is
/// stripped in both directions. On the response it is the anti-forgery rule: an
/// upstream that emits it — misconfigured, or hostile in the case of a
/// `base_url` pointed somewhere it shouldn't be — must not be able to forge a
/// claim about which route served a response. On the request it keeps a client
/// from putting a marker of its own on the wire to Anthropic, which is the same
/// rule read forwards: the header means what the relay says it means.
///
/// This is the *Anthropic* route's rule, and a denylist on purpose: that route
/// exists to forward what the client sent. The fallback route must never reuse
/// it for a request — see `fallback::outgoing_headers`, which builds its
/// request headers from nothing instead (spec §7b).
pub(crate) fn forwardable(headers: &HeaderMap) -> HeaderMap {
    let mut forwarded = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers {
        if is_hop_by_hop(name) || name == crate::fallback::ROUTE_MARKER {
            continue;
        }
        // `append`, not `insert`: repeated headers (`set-cookie`) must survive.
        forwarded.append(name.clone(), value.clone());
    }
    forwarded
}

pub(crate) struct RequestLog {
    pub(crate) route: &'static str,
    pub(crate) profile: Option<String>,
    pub(crate) model_in: Option<String>,
    pub(crate) model_out: Option<String>,
    pub(crate) method: Method,
    pub(crate) path: String,
    pub(crate) status: StatusCode,
    pub(crate) latency_ms: u64,
}

impl RequestLog {
    /// Spec §9's one line per request. Names only — a model name, a profile
    /// name, a path — never a body and never a header value.
    pub(crate) fn emit(self, response_bytes: u64) {
        tracing::info!(
            route = self.route,
            profile = self.profile.as_deref().unwrap_or("-"),
            model_in = self.model_in.as_deref().unwrap_or("-"),
            model_out = self.model_out.as_deref().unwrap_or("-"),
            method = %self.method,
            path = %self.path,
            status = self.status.as_u16(),
            latency_ms = self.latency_ms,
            response_bytes,
            "proxied request"
        );
    }
}

/// An accumulated body is the one thing this proxy holds in memory, so it is
/// bounded: a broken or hostile upstream must not be able to turn an error
/// response into unbounded allocation (Global Constraint 3).
const ERROR_BODY_CAP: usize = 1024 * 1024;

/// A non-2xx response's status/headers plus the body bytes accumulated so far,
/// so detection can classify — and a fixture can be written — once the stream
/// ends, without holding up any chunk on its way to the client.
struct ErrorObservation {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
    truncated: bool,
    /// The config to classify against, held only when the status is one the
    /// rule could match; any other error response is accumulated solely for
    /// `capture`, if at all.
    detect: Option<Arc<Config>>,
    capture: Option<Capture>,
    route_updates: RouteUpdates,
}

impl ErrorObservation {
    fn new(state: &AppState, status: StatusCode, headers: &HeaderMap) -> Option<Self> {
        let detect = state
            .config
            .detect
            .matches_status(status)
            .then(|| state.config.clone());
        if detect.is_none() && state.capture.is_none() {
            return None;
        }
        Some(Self {
            status,
            headers: headers.clone(),
            body: Vec::new(),
            truncated: false,
            detect,
            capture: state.capture.clone(),
            route_updates: state.route_updates.clone(),
        })
    }

    /// Copies what still fits under the cap and drops the rest; the client's
    /// stream is untouched either way and keeps receiving every byte.
    fn accumulate(&mut self, chunk: &[u8]) {
        let remaining = ERROR_BODY_CAP - self.body.len();
        if chunk.len() > remaining {
            self.body.extend_from_slice(&chunk[..remaining]);
            self.truncated = true;
        } else {
            self.body.extend_from_slice(chunk);
        }
    }

    fn finish(self, incomplete: bool) {
        if let Some(config) = &self.detect
            && let Some(reset_at) = config.detect.classify(
                &self.headers,
                &self.body,
                incomplete,
                SystemTime::now(),
                config.policy.min_reset_horizon_secs,
                config.policy.max_reset_horizon_secs,
            )
        {
            self.route_updates
                .record(RequestOutcome::LimitDetected { reset_at });
        }
        if let Some(capture) = &self.capture {
            capture.write_fixture(self.status, &self.headers, &self.body, incomplete);
        }
    }
}

/// Passes upstream bytes straight through, tallying them so the per-request log
/// line can be emitted once the body ends — including when the client hangs up
/// early and the stream is dropped mid-flight. When `observation` is set, each
/// chunk is also copied into it so limit detection can classify the response,
/// and a `--capture-errors` fixture can be written, on the same terminal events
/// as the log line — without delaying forwarding.
///
/// The fallback route shares this for the counting and the log line, and never
/// for the observation: a fallback provider's 429 says nothing about
/// Anthropic's limit window, and its error bodies are not fixtures Anthropic
/// detection rules can be derived from.
pub(crate) struct CountingStream {
    inner: Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>,
    response_bytes: u64,
    log: Option<RequestLog>,
    observation: Option<ErrorObservation>,
}

impl CountingStream {
    pub(crate) fn new(
        inner: Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>,
        log: RequestLog,
    ) -> Self {
        Self {
            inner,
            response_bytes: 0,
            log: Some(log),
            observation: None,
        }
    }

    fn emit(&mut self) {
        let Some(log) = self.log.take() else {
            return;
        };
        log.emit(self.response_bytes);
    }

    /// `ended_early` covers every way the body stopped short of its own end —
    /// the cap, an upstream failure, a client hangup — all of which produce a
    /// fixture indistinguishable from a complete one unless it says so, and a
    /// partial document detection must not classify from.
    fn finish_observation(&mut self, ended_early: bool) {
        let Some(observation) = self.observation.take() else {
            return;
        };
        let incomplete = ended_early || observation.truncated;
        observation.finish(incomplete);
    }
}

impl Stream for CountingStream {
    type Item = reqwest::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                this.response_bytes += chunk.len() as u64;
                if let Some(observation) = &mut this.observation {
                    observation.accumulate(&chunk);
                }
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(err))) => {
                this.emit();
                this.finish_observation(true);
                // Same reason as the handler's error path: whatever renders this
                // error must not be handed a URL that may carry credentials.
                Poll::Ready(Some(Err(err.without_url())))
            }
            Poll::Ready(None) => {
                this.emit();
                this.finish_observation(false);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for CountingStream {
    fn drop(&mut self) {
        self.emit();
        // Reaching `Drop` with an observation still pending means the stream
        // never reported its end — the client hung up (or the task was
        // cancelled).
        self.finish_observation(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn drops_hop_by_hop_headers_only() {
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("relay.local"));
        headers.insert("content-length", HeaderValue::from_static("12"));
        headers.insert("transfer-encoding", HeaderValue::from_static("chunked"));
        headers.insert("connection", HeaderValue::from_static("keep-alive"));
        headers.insert("authorization", HeaderValue::from_static("Bearer token"));
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));

        let forwarded = forwardable(&headers);

        assert_eq!(forwarded.len(), 2);
        assert_eq!(forwarded["authorization"], "Bearer token");
        assert_eq!(forwarded["anthropic-version"], "2023-06-01");
    }

    #[test]
    fn keeps_repeated_header_values() {
        let mut headers = HeaderMap::new();
        headers.append("set-cookie", HeaderValue::from_static("a=1"));
        headers.append("set-cookie", HeaderValue::from_static("b=2"));

        let forwarded = forwardable(&headers);

        let cookies: Vec<_> = forwarded.get_all("set-cookie").iter().collect();
        assert_eq!(cookies, vec!["a=1", "b=2"]);
    }

    #[test]
    fn a_conversation_with_an_assistant_turn_is_not_a_session_start() {
        let mid = RoutingView::parse(
            br#"{"model":"claude-opus-4-6","messages":[
                {"role":"user","content":"hi"},
                {"role":"assistant","content":"hello"},
                {"role":"user","content":"more"}]}"#,
        );
        assert_eq!(mid.model.as_deref(), Some("claude-opus-4-6"));
        assert!(!mid.is_session_start());
    }

    #[test]
    fn a_first_user_turn_is_a_session_start() {
        let start = RoutingView::parse(
            br#"{"model":"claude-opus-4-6","messages":[{"role":"user","content":"hi"}]}"#,
        );
        assert!(start.is_session_start());
    }

    /// A tool result comes back as a `user` message, so a request that is
    /// really mid-conversation still carries an assistant turn ahead of it —
    /// which is what the heuristic keys on, rather than the last role.
    #[test]
    fn a_tool_result_turn_is_not_mistaken_for_a_session_start() {
        let after_tool = RoutingView::parse(
            br#"{"model":"claude-opus-4-6","messages":[
                {"role":"user","content":"run ls"},
                {"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Bash","input":{}}]},
                {"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"ok"}]}]}"#,
        );
        assert!(!after_tool.is_session_start());
    }

    #[test]
    fn an_unreadable_body_yields_no_model_and_no_route_of_its_own() {
        let broken = RoutingView::parse(b"not json at all");
        assert!(broken.model.is_none());
        // An empty conversation is vacuously a session start; with no model
        // there is no failover decision to reach it.
        assert!(broken.is_session_start());
    }

    const CAP: usize = 64;

    /// Bytes distinct enough that a dropped or duplicated chunk cannot
    /// coincidentally still compare equal.
    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| b'a' + (i % 26) as u8).collect()
    }

    /// A body whose frames land on exactly the boundaries a test names. The
    /// end-to-end reassembly test in `tests/fallback.rs` takes whatever
    /// framing hyper chooses, which cannot pin the cap arithmetic — one frame
    /// either side of the boundary is the whole question here.
    fn body_of(chunks: &[&[u8]]) -> Body {
        let chunks: Vec<Bytes> = chunks.iter().map(|c| Bytes::copy_from_slice(c)).collect();
        Body::from_stream(ChunkStream {
            chunks: chunks.into_iter().collect(),
        })
    }

    struct ChunkStream {
        chunks: std::collections::VecDeque<Bytes>,
    }

    impl Stream for ChunkStream {
        type Item = Result<Bytes, std::io::Error>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.get_mut().chunks.pop_front().map(Ok))
        }
    }

    /// Every frame `Prefixed` yields, in order — so a test can assert both what
    /// the prefix held and that the whole stream is the client's bytes back.
    async fn frames(mut stream: Prefixed) -> Vec<Bytes> {
        let mut frames = Vec::new();
        while let Some(chunk) = std::future::poll_fn(|cx| Pin::new(&mut stream).poll_next(cx)).await
        {
            frames.push(chunk.expect("the test stream never fails"));
        }
        frames
    }

    #[tokio::test]
    async fn a_body_of_exactly_the_cap_is_still_buffered() {
        let body = pattern(CAP);
        let read = read_for_routing(body_of(&[&body]), CAP)
            .await
            .expect("read failed");
        let RequestBody::Buffered(buffered) = read else {
            panic!("`cap` bytes is at the cap, not over it");
        };
        assert_eq!(buffered, body);
    }

    /// The same boundary reached across two frames: the check is cumulative,
    /// so what matters is the running total, not any one chunk's size.
    #[tokio::test]
    async fn two_frames_summing_to_exactly_the_cap_are_still_buffered() {
        let body = pattern(CAP);
        let (head, tail) = body.split_at(CAP - 1);
        let read = read_for_routing(body_of(&[head, tail]), CAP)
            .await
            .expect("read failed");
        let RequestBody::Buffered(buffered) = read else {
            panic!("two frames summing to `cap` are at the cap, not over it");
        };
        assert_eq!(buffered, body);
    }

    /// One byte past the cap, on a frame of its own. The chunk that busts the
    /// cap is kept in the prefix rather than deferred to `rest`, so the prefix
    /// is everything read so far and the client's bytes come back exactly once.
    #[tokio::test]
    async fn one_byte_past_the_cap_is_too_large_and_loses_nothing() {
        let body = pattern(CAP + 1);
        let (head, tail) = body.split_at(CAP);
        let read = read_for_routing(body_of(&[head, tail]), CAP)
            .await
            .expect("read failed");
        let RequestBody::TooLarge(rest) = read else {
            panic!("`cap + 1` bytes is over the cap");
        };
        let frames = frames(rest).await;
        assert_eq!(
            frames[0].len(),
            CAP + 1,
            "the prefix holds every byte read, including the one that busted the cap"
        );
        assert_eq!(frames.concat(), body, "the client's bytes, unaltered");
    }

    /// Chunks straddling the cap, with a frame after the split still to come:
    /// the prefix and the untouched remainder of the stream reassemble to the
    /// input, in order, with nothing lost or repeated.
    #[tokio::test]
    async fn a_straddling_split_reassembles_byte_identically() {
        let body = pattern(CAP * 2);
        let (head, remainder) = body.split_at(CAP - 10);
        let (straddle, tail) = remainder.split_at(30);
        let read = read_for_routing(body_of(&[head, straddle, tail]), CAP)
            .await
            .expect("read failed");
        let RequestBody::TooLarge(rest) = read else {
            panic!("the second frame crosses the cap");
        };
        let frames = frames(rest).await;
        assert_eq!(
            frames[0].len(),
            CAP - 10 + 30,
            "the prefix ends where the cap was crossed, not where the cap is"
        );
        assert_eq!(frames.concat(), body, "the client's bytes, unaltered");
    }
}
