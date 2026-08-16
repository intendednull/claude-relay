//! The fallback route (spec §6, §7a, §7b): what happens to a request the
//! router sent to a profile instead of to Anthropic.
//!
//! Kept apart from `proxy`'s Anthropic route on purpose. That route is a
//! verbatim byte-for-byte forward and must stay one; this one rewrites the
//! request (model remap, `cache_control` strip, and for an `openai` profile a
//! whole wire-format translation) and builds its outgoing headers from
//! nothing. The two share only the counting/logging tail.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::time::Instant;

use axum::Json;
use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_core::Stream;
use indexmap::IndexMap;
use serde_json::{Value, json};

use crate::config::{PolicyConfig, ProfileConfig};
use crate::notify::{FallbackCause, NotifyEvent};
use crate::provider_error::ProviderError;
use crate::proxy::{CountingStream, ERROR_BODY_CAP, RequestLog, elapsed_ms, forwardable};
use crate::state::AppState;
use crate::translate::{self, BUFFER_CAP};

/// Spec §9's audit marker. Only the fallback route sets it: the Anthropic
/// route's response is a verbatim copy of Anthropic's own, and adding a header
/// to it would end that. `proxy::forwardable` strips it from everything an
/// upstream sends, on both routes, so the relay is the only thing that can
/// ever put it on a response.
pub(crate) const ROUTE_MARKER: HeaderName = HeaderName::from_static("x-relay-route");

/// The one thing this route buffers whole: a non-streaming upstream response,
/// which has to be complete before it can be translated. Shared with the SSE
/// translator's own ceiling rather than given a second number to drift from.
const RESPONSE_CAP: usize = BUFFER_CAP;

pub struct FallbackRequest<'a> {
    pub profile_name: &'a str,
    pub profile: &'a ProfileConfig,
    /// The model the client asked for.
    pub model: &'a str,
    /// Whether `model_map` applies. A `claude-*` request that failed over is
    /// remapped (spec §7a); a request routed here by name (§7d) keeps the name
    /// the client chose.
    pub remap: bool,
}

pub async fn forward(
    state: &AppState,
    start: Instant,
    method: Method,
    path: String,
    body: Bytes,
    request: FallbackRequest<'_>,
) -> Response {
    // Copied out before `request` is consumed. A `&str` copy, so the success path
    // allocates nothing: the profile name is only formatted into a notification.
    let profile_name = request.profile_name;

    match deliver(state, start, method, path, body, request).await {
        // Spec §4: **only a delivered 2xx counts toward a re-arm**, and it takes
        // `RE_ARM_SUCCESSES` of them. A request-attributable 4xx counts for
        // nothing, however much reaching the provider looks like proof that the
        // route is fine. `AppState` owns the whole rule — see `fallback_delivered`
        // and `fallback_failed` for why one success is not enough and why the edge
        // is a `compare_exchange`.
        Outcome::Served(response) => {
            state.fallback_delivered();
            response
        }
        Outcome::Rejected(response) => response,
        Outcome::Broken(response, cause) => {
            if state.fallback_failed() {
                state.notifier.notify_event(NotifyEvent::FallbackError {
                    profile: profile_name.to_string(),
                    cause,
                });
            }
            response
        }
    }
}

/// What the fallback route hands the client, and what that means for spec §4's
/// `fallback_error`. **The event is decided from this, not from each upstream
/// attempt:** escalation (§7e) turns one client request into as many as three
/// upstream requests, and a superseded attempt's failure is not an outage. The
/// loop below `continue`s past such an attempt without producing an `Outcome` at
/// all, which is what makes that structural rather than a rule to remember.
enum Outcome {
    /// A 2xx. The only thing that re-arms the edge-trigger.
    Served(Response),
    /// *This request* was wrong; nothing about the route is broken and the next
    /// request may be fine. Neither fires nor re-arms.
    Rejected(Response),
    /// *The route* is not delivering answers — the thing an operator is worth
    /// waking for.
    Broken(Response, FallbackCause),
}

async fn deliver(
    state: &AppState,
    start: Instant,
    method: Method,
    path: String,
    body: Bytes,
    request: FallbackRequest<'_>,
) -> Outcome {
    let profile = request.profile;
    // The slot exists only where the remap happened. A request routed here by
    // name (§7d) keeps the name the client chose, so there is no alias slot to
    // climb from and it gets no ladder: silently swapping a hand-picked
    // `/model moonshotai/…` for a different model would be wrong behavior, and
    // that case keeps 9B's error translation and nothing more (spec §7e).
    let (mut target_model, slot) = if request.remap {
        resolve_model(request.model, &profile.model_map)
    } else {
        (request.model.to_string(), None)
    };
    let mut ladder = Ladder::new(
        &state.config.policy,
        &profile.model_map,
        slot,
        target_model.clone(),
    );

    let translated = profile.format == "openai";
    let mut prepared = match prepare(&body, &target_model, translated) {
        Ok(prepared) => prepared,
        Err(err) => {
            tracing::warn!(
                profile = request.profile_name,
                // The translator's errors name a location, never a value.
                error = %err,
                "could not prepare the request for the fallback profile"
            );
            return request_failure(StatusCode::BAD_GATEWAY, "fallback_request_untranslatable");
        }
    };

    let headers = match outgoing_headers(profile) {
        Ok(headers) => headers,
        Err(err) => {
            tracing::error!(
                profile = request.profile_name,
                // The env var's *name*, never what it holds (Global Constraint 2).
                api_key_env = profile.api_key_env,
                reason = err.reason(),
                "the fallback profile's API key is unusable"
            );
            // Route-attributable, though the brief's fires/does-not list did not
            // name it: an unset or unusable `api_key_env` fails every request
            // identically until an operator changes config or environment, which
            // is the same class as the 401 this event exists for — and a key
            // rotation that did not take is its likeliest shape.
            return route_failure(StatusCode::BAD_GATEWAY, err.code());
        }
    };

    let target = endpoint(profile, &path, translated);

    // Read once here rather than per-chunk: a config reload mid-stream must not
    // change the shape of a message already in flight.
    let surface_reasoning = state.config.policy.surface_fallback_reasoning;

    // The answer the rung below produced, kept only while a hop is in flight. It
    // is what makes one thing true of this whole feature: turning it on cannot
    // leave the client with a worse answer than leaving it off would have.
    let mut carried: Option<ProviderFailure> = None;

    // One iteration per upstream attempt. A second iteration happens only where
    // the escalation branch below asks for one, and only ever up the ladder —
    // `Ladder::next_target` consumes the rung it returns.
    loop {
        let Prepared {
            body: outgoing,
            stream,
        } = prepared;
        let upstream = state
            .http
            .request(method.clone(), target.clone())
            .headers(headers.clone())
            .body(outgoing)
            .send()
            .await;

        let upstream = match upstream {
            Ok(upstream) => upstream,
            Err(err) => {
                tracing::warn!(
                    profile = request.profile_name,
                    method = %method,
                    path = %path,
                    latency_ms = elapsed_ms(start),
                    // `without_url` keeps a credential embedded in `base_url` out
                    // of the log, exactly as on the Anthropic route.
                    error = %err.without_url(),
                    "fallback upstream request failed"
                );
                // A hop that could not even be sent has nothing of its own to
                // report, and the rung below it produced an answer the client can
                // act on — 9B's translated context-limit error. Handing over the
                // relay's own `upstream_unreachable` instead would replace an
                // actionable Anthropic error with a relay-internal code that says
                // nothing the client can use, purely because the relay tried to
                // help. A response with a status *is* reported as itself, however
                // unhelpful: the freshest truth wins where there is one.
                //
                // Which also settles what this means for §4: the carried answer
                // is a status the provider really sent, so it classifies as
                // itself — the client has an answer it can act on, and the hop
                // that could not be sent is a superseded attempt.
                return match carried {
                    Some(previous) => previous.into_outcome(None),
                    None => route_failure(StatusCode::BAD_GATEWAY, "upstream_unreachable"),
                };
            }
        };

        // Counted per upstream attempt, like §9's log line: an escalated request
        // really did cost the operator two calls to the provider, and a counter
        // that reported one would hide the half that surprises them on the bill.
        state
            .fallback_requests_served
            .fetch_add(1, Ordering::Relaxed);

        let status = upstream.status();
        let log = RequestLog {
            route: "fallback",
            profile: Some(request.profile_name.to_string()),
            model_in: Some(request.model.to_string()),
            model_out: Some(target_model.clone()),
            method: method.clone(),
            path: path.clone(),
            status,
            latency_ms: elapsed_ms(start),
        };

        // A fallback response says nothing about Anthropic's route state, so
        // neither `route_updates` nor `--capture-errors` (whose fixtures exist to
        // derive Anthropic detection rules from) hears about it. A 429 from the
        // fallback provider must not put the Anthropic route into `Limited`, and a
        // 200 from it must not recover the route out of it.
        if !status.is_success() {
            let failure =
                read_provider_error(profile, request.profile_name, status, upstream).await;

            // Spec §7e. This is the only point on this route where escalation is
            // decidable, and that is what makes the mid-stream prohibition
            // structural rather than a rule to remember: an HTTP status arrives
            // before its body, so at this line not one byte has been written
            // toward the client. Every path that sends one is below and returns,
            // and none of them can be re-entered from here — so a context-limit
            // error that arrives *inside* a 200 stream is never escalated, the
            // same rule `a_fallback_stream_that_dies_mid_response_is_never_retried`
            // already holds for a mid-stream death.
            //
            // Detection firing is deliberately *not* the condition —
            // `input_over_limit` is, and the difference is a bill. A body that says
            // the *output reservation* did not fit (measured: a 35k transcript with
            // a 160k `max_tokens`) matches the same markers, and the client already
            // fixes that one for free by shrinking `max_tokens` on 9B's translated
            // error. Escalating it pre-empts the free fix and buys a billed
            // inference on a larger model. So: spend money only on positive
            // evidence that the input itself is over the limit.
            if failure.error.input_over_limit()
                && let Some(next) = ladder.next_target()
            {
                match prepare(&body, &next, translated) {
                    Ok(next_prepared) => {
                        // §9's line for the attempt that just failed, carrying the
                        // model that failed — the same shape the Anthropic route's
                        // re-routed attempt emits before handing over (`proxy`).
                        // First, so the three lines an escalating request writes read
                        // in the order they happened.
                        log.emit(failure.upstream_bytes);
                        tracing::info!(
                            profile = request.profile_name,
                            // Both are `model_map` *values*, never client text: a
                            // target with no slot behind it has no rung and cannot
                            // reach here. Plain fields regardless, so neither can
                            // forge a record. No request content, ever.
                            from_model = target_model.as_str(),
                            to_model = next.as_str(),
                            reason = "context_limit",
                            "the fallback model could not fit the prompt; retrying one rung up"
                        );
                        target_model = next;
                        prepared = next_prepared;
                        carried = Some(failure);
                        continue;
                    }
                    Err(err) => {
                        // Not a 502: the client already has a usable answer — 9B's
                        // translated context-limit error, which is the recovery it
                        // would have had without this feature. Losing that to
                        // report a relay-side failure would be a downgrade.
                        tracing::warn!(
                            profile = request.profile_name,
                            error = %err,
                            "could not prepare the escalated request; answering with the provider's error"
                        );
                    }
                }
            }

            // A hop's own answer replaces the rung below's only if it is at least as
            // *final*. Measured: with the rung above answering 429, the client is
            // handed a 429 in place of a terminal 400 it would have acted on once —
            // so it retries with backoff, and every retry re-walks the whole ladder.
            // Nothing here can bound that, because the loop is the client's, not the
            // relay's: one hop turned a one-shot failure into unbounded request
            // amplification and still lost the session.
            //
            // The rung below's answer is a context-limit 400 the client acts on and
            // does not retry blindly, so it is strictly the better one to hand over.
            // A hop that answers with a *terminal* status keeps its own answer even
            // when that is less useful (a 404 for a retired `model_map` target, say):
            // it is the truth about the model the ladder chose, it cannot amplify,
            // and masking it would hide a misconfigured rung.
            if client_retries(status)
                && let Some(previous) = carried
            {
                // The hop happened and was paid for, so it keeps its own §9 line;
                // the carried answer's line went out when its hop was decided.
                log.emit(failure.upstream_bytes);
                return previous.into_outcome(None);
            }
            return failure.into_outcome(Some(log));
        }

        if !translated {
            return Outcome::Served(passthrough_response(status, upstream, log));
        }

        if stream {
            let body = Body::from_stream(CountingStream::new(
                Box::pin(NeverFails(Box::pin(translate::sse_stream(
                    upstream.bytes_stream(),
                    surface_reasoning,
                )))),
                log,
            ));
            let mut response = Response::new(body);
            // The upstream's own 2xx, not a flat 200: a provider that answers 206
            // or 202 is saying something the client should see.
            *response.status_mut() = status;
            *response.headers_mut() = translated_headers("text/event-stream");
            response
                .headers_mut()
                .insert("cache-control", HeaderValue::from_static("no-cache"));
            return Outcome::Served(response);
        }

        let raw = match read_capped(upstream, RESPONSE_CAP).await {
            Ok(raw) => raw,
            Err(reason) => {
                tracing::warn!(
                    profile = request.profile_name,
                    reason,
                    "fallback response unusable"
                );
                return route_failure(StatusCode::BAD_GATEWAY, "fallback_response_unreadable");
            }
        };
        let anthropic = match translate::response_to_anthropic(&raw, surface_reasoning) {
            Ok(anthropic) => anthropic,
            Err(err) => {
                tracing::warn!(
                    profile = request.profile_name,
                    error = %err,
                    "fallback response untranslatable"
                );
                return route_failure(StatusCode::BAD_GATEWAY, "fallback_response_untranslatable");
            }
        };
        log.emit(anthropic.len() as u64);

        let mut response = Response::new(Body::from(anthropic));
        *response.status_mut() = status;
        *response.headers_mut() = translated_headers("application/json");
        return Outcome::Served(response);
    }
}

struct Prepared {
    body: Vec<u8>,
    stream: bool,
}

/// Whether a client will answer this status by retrying rather than by acting on
/// it — Anthropic's own SDKs retry 408, 409, 429 and every 5xx, and Claude Code
/// rides on one.
///
/// It is the line between "the hop reported something the client can use" and "the
/// hop turned a terminal failure into a loop": each of those client retries is a
/// fresh request that walks the ladder again, so a retryable hop answer multiplies
/// upstream requests without bound (spec §7e).
fn client_retries(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 409 | 429) || status.is_server_error()
}

/// The outgoing body for one target model: a whole wire-format translation for an
/// `openai` profile, the client's own body with the model substituted for an
/// `anthropic` one.
///
/// Called once per upstream attempt rather than once per request, because the
/// target model is *in* the body — an escalated retry re-emits it through the
/// same path that produced the first one, instead of patching a model name inside
/// JSON that has already been serialized.
fn prepare(body: &[u8], target_model: &str, translated: bool) -> anyhow::Result<Prepared> {
    if translated {
        let request = translate::request_to_openai(body, target_model)?;
        Ok(Prepared {
            body: request.body,
            stream: request.stream,
        })
    } else {
        Ok(Prepared {
            body: passthrough_body(body, target_model)?,
            stream: false,
        })
    }
}

/// The rungs a request may still climb to when its target model says the prompt
/// did not fit (spec §7e), resolved lazily against the profile's own `model_map`.
///
/// A cursor rather than a list plus an index: `next_target` consumes what it
/// returns, so "walk the ladder at most once" is structural — there is no
/// position a bug could reset and no way to revisit a rung. That bound is the
/// load-bearing part of this feature, not boilerplate: every hop is a whole extra
/// upstream request the operator pays for, and the top rung is the most expensive
/// model configured.
struct Ladder<'a> {
    /// Slots strictly above the rung this request started on. Empty means
    /// nowhere to climb, which is how *every* no-ladder case is expressed: the
    /// config gate off, a name-routed request, a target that came from `"*"` or
    /// from no entry at all, a slot `escalation_order` does not name, and the top
    /// rung itself.
    rungs: &'a [String],
    model_map: &'a IndexMap<String, String>,
    /// Every target this request has already been sent to. The live map points
    /// **both** `claude-fable` and `claude-opus` at `moonshotai/Kimi-K3`, so
    /// without this a walk re-sends the identical request to the identical model
    /// and buys a guaranteed identical failure at the top rung's price.
    tried: Vec<String>,
}

impl<'a> Ladder<'a> {
    /// `slot` is which `model_map` key the request's target came from, and
    /// `first_target` that target — `None` for every request with no ladder
    /// position at all (see the `rungs` field).
    fn new(
        policy: &'a PolicyConfig,
        model_map: &'a IndexMap<String, String>,
        slot: Option<&str>,
        first_target: String,
    ) -> Self {
        Self {
            rungs: Self::above(policy, slot),
            model_map,
            tried: vec![first_target],
        }
    }

    fn above(policy: &'a PolicyConfig, slot: Option<&str>) -> &'a [String] {
        if !policy.escalate_on_context_limit {
            return &[];
        }
        let Some(slot) = slot else {
            return &[];
        };
        match policy.escalation_order.iter().position(|rung| rung == slot) {
            Some(at) => &policy.escalation_order[at + 1..],
            None => &[],
        }
    }

    /// The next target worth sending to, or `None` when the ladder is out of
    /// rungs. Skips a slot the profile does not define, and a slot whose target
    /// this request has already been sent to.
    fn next_target(&mut self) -> Option<String> {
        while let [slot, rest @ ..] = self.rungs {
            self.rungs = rest;
            let Some(target) = self.model_map.get(slot) else {
                continue;
            };
            if self.tried.iter().any(|seen| seen == target) {
                continue;
            }
            self.tried.push(target.clone());
            return Some(target.clone());
        }
        None
    }
}

/// Spec §7a. The longest matching prefix wins; equal-length matches go to the
/// one declared first, which is the whole reason `model_map` is an `IndexMap`.
/// `"*"` is consulted only when nothing else matched, and a name no entry
/// claims is sent on unchanged — the provider's own "unknown model" is a
/// better answer than one this proxy invents.
pub fn remap_model(model: &str, model_map: &IndexMap<String, String>) -> String {
    resolve_model(model, model_map).0
}

/// `remap_model`, plus *which* `model_map` key decided the answer — `None` when
/// it came from `"*"` or from no entry at all.
///
/// The escalation ladder needs the key rather than the target, because the key is
/// the request's rung and two keys are free to point at the same model (spec §7e).
/// `None` is the honest answer for the other two cases, and the ladder treats it
/// as nowhere to climb: `"*"` is consulted only when no prefix matched, so its
/// target is chosen to be a safe answer for *anything* rather than a size tier —
/// on the live map it is the largest model configured, so reading it as the bottom
/// rung would send an overflowing request *down* to a smaller window.
fn resolve_model<'a>(
    model: &str,
    model_map: &'a IndexMap<String, String>,
) -> (String, Option<&'a str>) {
    let mut best: Option<(&String, &String)> = None;
    for (prefix, target) in model_map {
        if prefix == "*" || !model.starts_with(prefix.as_str()) {
            continue;
        }
        if best.is_none_or(|(current, _)| prefix.len() > current.len()) {
            best = Some((prefix, target));
        }
    }
    if let Some((prefix, target)) = best {
        return (target.clone(), Some(prefix.as_str()));
    }
    (
        model_map
            .get("*")
            .cloned()
            .unwrap_or_else(|| model.to_string()),
        None,
    )
}

/// Spec §7b: `cache_control` is Anthropic's prompt-caching directive. A
/// fallback provider either rejects it or ignores it, and either way the cost
/// behavior it asks for is not the one that will happen. Removed everywhere it
/// can appear — content blocks, system blocks, tool definitions — rather than
/// at the three paths the API documents today.
///
/// (The `openai` path never reaches this: translation rebuilds the request from
/// the fields it knows, and `cache_control` is not one of them.)
pub fn strip_cache_control(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.remove("cache_control");
            for (_, child) in map.iter_mut() {
                strip_cache_control(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                strip_cache_control(item);
            }
        }
        _ => {}
    }
}

/// The `format = "anthropic"` body: the client's own request with the model
/// substituted and `cache_control` stripped. Untested against a real
/// Anthropic-compatible provider — none is configured (Global Constraint 10).
fn passthrough_body(body: &[u8], target_model: &str) -> anyhow::Result<Vec<u8>> {
    let mut value: Value = serde_json::from_slice(body)
        .map_err(|err| translate::parse_failure("request body is not valid JSON", &err))?;
    let Some(map) = value.as_object_mut() else {
        anyhow::bail!("request body is not a JSON object");
    };
    map.insert("model".to_string(), json!(target_model));
    strip_cache_control(&mut value);
    Ok(serde_json::to_vec(&value)?)
}

/// `base_url` is the provider's API root, not a full endpoint URL: the path is
/// this route's to choose, because an `openai` profile is served at the
/// OpenAI endpoint no matter which Anthropic path the client called.
fn endpoint(profile: &ProfileConfig, path: &str, translated: bool) -> String {
    let base = profile.base_url.trim_end_matches('/');
    let suffix = if translated {
        "/v1/chat/completions"
    } else {
        path
    };
    format!("{base}{suffix}")
}

/// Spec §7b, security-critical. The outgoing headers are *built*, never
/// filtered: nothing at all is copied from the client, so no client header can
/// reach a third party by being absent from a denylist somebody forgot to
/// extend. That covers the named ones — `Authorization`, `x-api-key`, every
/// `anthropic-*` including `anthropic-beta` — and two that matter for
/// different reasons: `accept-encoding` (Claude Code asks for gzip, and a
/// compressed body is one the translator cannot read) and `content-length`
/// (the body it described is not the body being sent).
fn outgoing_headers(profile: &ProfileConfig) -> Result<HeaderMap, KeyError> {
    let key = std::env::var(&profile.api_key_env).map_err(|_| KeyError::Missing)?;
    let mut bearer =
        HeaderValue::try_from(format!("Bearer {key}")).map_err(|_| KeyError::Unusable)?;
    bearer.set_sensitive(true);

    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    headers.insert("authorization", bearer);

    if profile.format == "anthropic" {
        // The profile's own key again, in the scheme the Anthropic Messages
        // API documents, and the version header that API requires. Both are
        // constants of ours — the client's `anthropic-version` is discarded
        // with everything else it sent.
        let mut api_key = HeaderValue::try_from(key).map_err(|_| KeyError::Unusable)?;
        api_key.set_sensitive(true);
        headers.insert("x-api-key", api_key);
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
    }
    Ok(headers)
}

enum KeyError {
    Missing,
    Unusable,
}

impl KeyError {
    fn reason(&self) -> &'static str {
        match self {
            Self::Missing => "the environment variable is unset or not valid unicode",
            Self::Unusable => "the value cannot be sent as a header",
        }
    }

    fn code(&self) -> &'static str {
        match self {
            Self::Missing => "fallback_key_missing",
            Self::Unusable => "fallback_key_unusable",
        }
    }
}

/// Every response this route produces carries the marker, relay-generated
/// errors included: the question it answers after the fact is "did this come
/// from Anthropic", and a failed fallback attempt did not.
fn fallback_error(status: StatusCode, code: &'static str) -> Response {
    let mut response = (status, Json(json!({ "error": code }))).into_response();
    response
        .headers_mut()
        .insert(ROUTE_MARKER, HeaderValue::from_static("fallback"));
    response
}

/// A relay-generated failure **the route** is answerable for: it fails every
/// request the same way until something outside this request changes, so it is
/// worth an operator's `fallback_error` (spec §4).
///
/// Named for its attribution, and paired with `request_failure`, deliberately:
/// the alternative shapes — one central `match code`, or a marker read off the
/// outgoing response — both need a default for a code they do not recognise, and
/// **either default is silently wrong** (a new request-attributable code fires a
/// false outage, or a new route failure notifies nothing). Two named
/// constructors have no default: a seventh failure site cannot compile without
/// choosing one, and the choice is legible where the failure is known.
fn route_failure(status: StatusCode, code: &'static str) -> Outcome {
    Outcome::Broken(fallback_error(status, code), FallbackCause::Relay(code))
}

/// A relay-generated failure **this request** is answerable for. Its twin above
/// carries the reasoning.
fn request_failure(status: StatusCode, code: &'static str) -> Outcome {
    Outcome::Rejected(fallback_error(status, code))
}

/// Which provider statuses mean *the route* is broken rather than *this request*
/// (spec §4). Classified by attributability, not by status class — 401/402/403
/// are the motivating case and none of them is a 5xx, so no `is_client_error`
/// shortcut may stand in for this list.
///
/// An allowlist, not "everything but a denylist": a status nobody has captured
/// from a provider here should cost a missed notification rather than a false
/// "your fallback is down", which is the more expensive of the two mistakes. 407
/// and 408 were the near misses considered and left out on that rule.
fn route_attributable(status: StatusCode) -> bool {
    matches!(
        status.as_u16(),
        // Credentials, billing, or an intermediary refusing us.
        401 | 402 | 403
        // The fallback provider is throttling the relay. Note this changes
        // nothing about *Anthropic's* route state — see the branch in `deliver`
        // that reads the status, and the invariant documented there.
        | 429
    ) || status.is_server_error()
}

fn translated_headers(content_type: &'static str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static(content_type));
    headers.insert(ROUTE_MARKER, HeaderValue::from_static("fallback"));
    headers
}

/// One provider error, read exactly once (spec §7d): the body capped, the
/// profile's key redacted on the bytes both sinks share, those bytes logged, and
/// the result parsed.
///
/// Split from the response it becomes so that escalation can decide on the
/// *parsed* error while no envelope exists yet (spec §7e). One call per upstream
/// attempt, which is what keeps an escalating request's two failures at one log
/// line each — neither doubled, neither lost — and the redaction running exactly
/// once over each attempt's own bytes.
async fn read_provider_error(
    profile: &ProfileConfig,
    profile_name: &str,
    status: StatusCode,
    upstream: reqwest::Response,
) -> ProviderFailure {
    let retry_after = upstream.headers().get("retry-after").cloned();

    // `ERROR_BODY_CAP`, not `RESPONSE_CAP`: the repo already has a cap argued for
    // exactly this hazard — the Anthropic route's error accumulator, whose 1 MiB is
    // there so "a broken or hostile upstream must not be able to turn an error
    // response into unbounded allocation" (Global Constraint 3). `RESPONSE_CAP` is
    // 4x that and is about a non-streaming 2xx that has to be complete before it can
    // be translated, which is a different question; inheriting it here would have
    // been 4x by accident rather than by choice.
    //
    // Note the change in resource profile this branch made: a provider error used to
    // stream through at O(1) and is now buffered whole, so a burst of concurrent
    // errors costs O(cap) each. Bounded per request, with no aggregate bound across
    // them — the same property spec §12 already records for every other buffer here.
    let raw = match read_capped(upstream, ERROR_BODY_CAP).await {
        Ok(raw) => raw,
        Err(reason) => {
            tracing::warn!(
                profile = profile_name,
                reason,
                "the fallback provider's error body was unreadable"
            );
            // Not a 502: the provider's status is the honest answer, and losing
            // it would tell the client a different thing went wrong.
            Vec::new()
        }
    };
    // What the provider sent, taken before the redaction changes the length:
    // §9's per-attempt line reports it for an attempt escalation replaces, which
    // is what the Anthropic route's re-routed attempt reports too (`proxy`).
    let upstream_bytes = raw.len() as u64;
    // Redacted once, here, before anything reads it. This body has *two* sinks —
    // the log line below and the message that goes into the client's envelope —
    // and redacting at each of them is how one of them ends up forgotten. Doing it
    // to the bytes both share is the only version that cannot diverge. It also has
    // to happen before the log's clip: redacting a clipped body leaves a key that
    // straddles the boundary partially present, so the literal match misses and
    // the truncated key is logged in cleartext.
    let raw = redact_profile_key(raw, &profile.api_key_env);

    // The envelope necessarily reshapes what the provider sent, so the bytes have
    // to stay findable by a human — the log is the place for that.
    tracing::warn!(
        profile = profile_name,
        status = status.as_u16(),
        // No `%` sigil: that renders through `format_args!` unescaped, and this
        // value is provider-controlled, so a newline in it would forge a whole
        // record (`log_safety`). A plain field gets `record_str`'s escaping.
        body = clipped_for_log(&raw),
        "the fallback provider returned an error"
    );

    ProviderFailure {
        status,
        error: ProviderError::read(status, &raw),
        retry_after,
        upstream_bytes,
    }
}

/// A provider error, read and logged, not yet answered.
struct ProviderFailure {
    /// The provider's own, preserved on the way out and the one the error was
    /// parsed with — held here rather than passed on again so the two cannot
    /// diverge.
    status: StatusCode,
    error: ProviderError,
    retry_after: Option<HeaderValue>,
    upstream_bytes: u64,
}

impl ProviderFailure {
    /// Spec §7d: a provider's error reaches the client in Anthropic's envelope,
    /// with the provider's own status and message preserved. It used to pass
    /// through verbatim; the shapes were unknown when that rule was written and
    /// are captured now, and for a context-limit error the passthrough cost the
    /// user the whole session — Claude Code's compact-and-retry keys on
    /// Anthropic's wording, which no provider here uses (`docs/decisions.md`).
    ///
    /// Reached on the last rung of an escalation too, unchanged: escalation
    /// failing is not a reason for the client to lose the recovery it already had.
    ///
    /// `log` is `None` in exactly one case — answering on behalf of an attempt
    /// whose §9 line was already emitted when its hop was decided, so emitting a
    /// second one would log that one attempt twice.
    ///
    /// Classifies itself for spec §4 on the way out, from `self.status` — the
    /// status this error was *parsed* with, which is why it is held here rather
    /// than passed in again. This is the only place a provider status is
    /// classified, and it reads nothing from the provider's body: the message is
    /// user- and attacker-influenced, and `RELAY_DETAIL` goes to an operator's
    /// hook.
    fn into_outcome(self, log: Option<RequestLog>) -> Outcome {
        let status = self.status;
        let mut headers = translated_headers("application/json");
        // An allowlist of one, not `forwardable`'s denylist: this body is the
        // relay's, so the provider's `content-length` and `content-encoding`
        // describe bytes that are no longer being sent. `retry-after` is the only
        // header on an error the client acts on, so it is the only one kept.
        if let Some(retry_after) = self.retry_after {
            headers.insert("retry-after", retry_after);
        }

        let body = self.error.to_anthropic();
        if let Some(log) = log {
            log.emit(body.len() as u64);
        }

        let mut response = Response::new(Body::from(body));
        *response.status_mut() = status;
        *response.headers_mut() = headers;

        if route_attributable(status) {
            Outcome::Broken(response, FallbackCause::Status(status.as_u16()))
        } else {
            Outcome::Rejected(response)
        }
    }
}

/// How much of a provider's error body reaches the log. The real ones are a few
/// hundred bytes; the cap is here because the body is provider-controlled and
/// unbounded, and because a provider is free to echo request content into its
/// error message — Together's context error carries only numbers, but that is an
/// observation about one provider, not a guarantee. Counted in `char`s so the
/// clip cannot split a multi-byte boundary.
const LOGGED_ERROR_BODY_CHARS: usize = 512;

fn clipped_for_log(body: &[u8]) -> String {
    String::from_utf8_lossy(body)
        .chars()
        .take(LOGGED_ERROR_BODY_CHARS)
        .collect()
}

/// The profile's own key is the one credential that ever reaches this provider
/// (spec §7b builds the outgoing headers from nothing else), and a provider is
/// free to quote it back in an error — an authentication failure is exactly the
/// error most likely to. Redacted for the same reason `err.without_url()` is used
/// above: neither the log nor the client's session transcript may be where a
/// credential lands (Global Constraint 2).
///
/// Applied to the whole body rather than a clipped view of it, because the clip
/// happens downstream of here in two different places and a key straddling either
/// boundary must not survive.
fn redact_profile_key(body: Vec<u8>, api_key_env: &str) -> Vec<u8> {
    redact_key(body, std::env::var(api_key_env).ok().as_deref())
}

/// Split from the environment read so the floor can be tested without writing to
/// the process environment.
fn redact_key(body: Vec<u8>, key: Option<&str>) -> Vec<u8> {
    match key {
        Some(key) if key.len() >= MIN_REDACTABLE_KEY_LEN => {
            replace_bytes(&body, key.as_bytes(), b"[REDACTED]")
        }
        _ => body,
    }
}

/// A floor on what is treated as a key to redact. Not a guess at key formats — it
/// is here because this redaction now runs *ahead of* the context-limit parse, so a
/// placeholder value left in the environment (`=changeme`, `=token`, `=context`)
/// would quietly eat words out of the provider's message and take the recovery with
/// them: `context` alone stops detection firing at all, and `token` costs the token
/// pair. Before the redaction moved in front of the parser a bad key could only
/// mangle a log line. Real keys are far longer than this; anything shorter is a
/// mistake, and a mistake should not be able to disable the recovery silently.
const MIN_REDACTABLE_KEY_LEN: usize = 8;

/// `str::replace` over bytes, because an error body need not be valid UTF-8 while
/// the key always is. A redacted JSON body still parses in practice — `[REDACTED]`
/// carries nothing JSON treats specially — but that is an observation about where a
/// provider puts its key, not a property this can guarantee, and a body that stops
/// parsing degrades to the snippet path rather than failing.
fn replace_bytes(haystack: &[u8], needle: &[u8], with: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(haystack.len());
    let mut rest = haystack;
    while let Some(at) = rest
        .windows(needle.len())
        .position(|window| window == needle)
    {
        out.extend_from_slice(&rest[..at]);
        out.extend_from_slice(with);
        rest = &rest[at + needle.len()..];
    }
    out.extend_from_slice(rest);
    out
}

/// Streams the upstream response through untouched. Used for a
/// `format = "anthropic"` profile's 2xx, which needs no translation.
fn passthrough_response(
    status: StatusCode,
    upstream: reqwest::Response,
    log: RequestLog,
) -> Response {
    let mut headers = forwardable(upstream.headers());
    headers.insert(ROUTE_MARKER, HeaderValue::from_static("fallback"));

    let body = Body::from_stream(CountingStream::new(Box::pin(upstream.bytes_stream()), log));
    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

async fn read_capped(upstream: reqwest::Response, cap: usize) -> Result<Vec<u8>, &'static str> {
    let mut stream = Box::pin(upstream.bytes_stream());
    let mut buffered = Vec::new();
    while let Some(chunk) = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        let chunk = chunk.map_err(|_| "the upstream stream failed")?;
        if buffered.len() + chunk.len() > cap {
            return Err("the response exceeded the buffer cap");
        }
        buffered.extend_from_slice(&chunk);
    }
    Ok(buffered)
}

/// `sse_stream`'s output cannot fail — an upstream failure is already a
/// terminal Anthropic `error` event by the time it comes out. This carries it
/// into the error type `CountingStream` speaks, so both routes log through the
/// same tail instead of growing a second one.
struct NeverFails<S>(Pin<Box<S>>);

impl<S> Stream for NeverFails<S>
where
    S: Stream<Item = Result<Bytes, Infallible>>,
{
    type Item = reqwest::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut().0.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(Some(Err(never))) => match never {},
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model_map(entries: &[(&str, &str)]) -> IndexMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn the_longest_matching_prefix_wins_over_a_shorter_one_declared_first() {
        let map = model_map(&[
            ("claude-", "generic/Model"),
            ("claude-opus", "big/Model"),
            ("*", "fallback/Model"),
        ]);
        assert_eq!(remap_model("claude-opus-4-6", &map), "big/Model");
        assert_eq!(remap_model("claude-haiku-4-5", &map), "generic/Model");
    }

    #[test]
    fn equal_length_prefixes_are_settled_by_config_order() {
        let map = model_map(&[
            ("claude-opus", "first/Model"),
            ("claude-opus", "second/Model"),
        ]);
        // Duplicate keys collapse in a map, so the tie has to be built from two
        // distinct keys of the same length.
        assert_eq!(map.len(), 1);
        let map = model_map(&[
            ("claude-opu", "first/Model"),
            ("claude-son", "second/Model"),
        ]);
        assert_eq!(remap_model("claude-opus-4-6", &map), "first/Model");
    }

    #[test]
    fn the_catch_all_applies_only_when_no_prefix_matched() {
        let map = model_map(&[("claude-opus", "big/Model"), ("*", "fallback/Model")]);
        assert_eq!(remap_model("claude-sonnet-4-6", &map), "fallback/Model");
    }

    #[test]
    fn an_unmapped_name_with_no_catch_all_passes_through_unchanged() {
        let map = model_map(&[("claude-opus", "big/Model")]);
        assert_eq!(remap_model("claude-haiku-4-5", &map), "claude-haiku-4-5");
        assert_eq!(
            remap_model("claude-haiku-4-5", &IndexMap::new()),
            "claude-haiku-4-5"
        );
    }

    // --- the escalation ladder (spec §7e) ---

    /// The live map's three windows: 131k, 262k, 1M.
    const SMALL: &str = "openai/gpt-oss-20b";
    const MEDIUM: &str = "moonshotai/Kimi-K2.7-Code";
    const LARGE: &str = "moonshotai/Kimi-K3";

    fn ladder_map() -> IndexMap<String, String> {
        model_map(&[
            ("claude-haiku", SMALL),
            ("claude-sonnet", MEDIUM),
            ("claude-opus", LARGE),
            ("*", LARGE),
        ])
    }

    fn order(slots: &[&str]) -> PolicyConfig {
        PolicyConfig {
            escalation_order: slots.iter().map(|slot| (*slot).to_string()).collect(),
            ..PolicyConfig::default()
        }
    }

    /// The whole walk, from the bottom rung: every slot above the one the request
    /// landed on, in order, and then nothing — twice, because "at most once" is
    /// the requirement that costs money when it is missed.
    #[test]
    fn the_ladder_climbs_the_slots_above_the_one_the_request_landed_on() {
        let map = ladder_map();
        let policy = PolicyConfig::default();
        let (target, slot) = resolve_model("claude-haiku-4-5", &map);
        assert_eq!((target.as_str(), slot), (SMALL, Some("claude-haiku")));

        let mut ladder = Ladder::new(&policy, &map, slot, target);
        assert_eq!(ladder.next_target().as_deref(), Some(MEDIUM));
        assert_eq!(ladder.next_target().as_deref(), Some(LARGE));
        assert_eq!(ladder.next_target(), None, "the top rung is the end");
        assert_eq!(ladder.next_target(), None, "and it stays the end");
    }

    /// The requirement most likely to be missed, and the one that costs real
    /// money: the live map points **both** `claude-fable` and `claude-opus` at
    /// Kimi-K3, so a naive walk re-sends the identical request to the identical
    /// model at the top rung's price.
    #[test]
    fn a_target_this_request_already_failed_on_is_never_sent_to_again() {
        let map = model_map(&[
            ("claude-haiku", SMALL),
            ("claude-sonnet", MEDIUM),
            ("claude-opus", LARGE),
            ("claude-fable", LARGE),
        ]);
        let policy = order(&[
            "claude-haiku",
            "claude-sonnet",
            "claude-opus",
            "claude-fable",
        ]);
        let (target, slot) = resolve_model("claude-sonnet-4-6", &map);
        assert_eq!(target, MEDIUM);

        let mut ladder = Ladder::new(&policy, &map, slot, target);
        assert_eq!(ladder.next_target().as_deref(), Some(LARGE));
        assert_eq!(
            ladder.next_target(),
            None,
            "`claude-fable` resolves to the model that just failed"
        );
    }

    /// A profile that maps two of the three slots is a valid profile, so the gap
    /// is stepped over rather than treated as the end of the ladder.
    #[test]
    fn a_slot_the_profile_does_not_define_is_skipped_not_stopped_at() {
        let map = model_map(&[("claude-haiku", SMALL), ("claude-opus", LARGE)]);
        let policy = PolicyConfig::default();
        let (target, slot) = resolve_model("claude-haiku-4-5", &map);

        let mut ladder = Ladder::new(&policy, &map, slot, target);
        assert_eq!(ladder.next_target().as_deref(), Some(LARGE));
        assert_eq!(ladder.next_target(), None);
    }

    /// Every way a request has nowhere to climb. Each of these would otherwise be
    /// a hop the operator pays for and did not ask for.
    #[test]
    fn nothing_climbs_without_a_rung_to_climb_from() {
        let map = ladder_map();
        let default = PolicyConfig::default();

        // `"*"`: consulted only because no prefix matched, so it is a safe default
        // for anything rather than a size tier — and on this map it is the largest
        // model, so reading it as the bottom rung would hop *down*.
        let (target, slot) = resolve_model("claude-3-5-sonnet-20241022", &map);
        assert_eq!((target.as_str(), slot), (LARGE, None), "the catch-all");
        assert_eq!(
            Ladder::new(&default, &map, slot, target).next_target(),
            None
        );

        // A name no entry claims and no catch-all to catch it: sent on unchanged
        // (§7a), so there is no slot behind the model name at all.
        let sparse = model_map(&[("claude-opus", LARGE)]);
        let (target, slot) = resolve_model("claude-haiku-4-5", &sparse);
        assert_eq!((target.as_str(), slot), ("claude-haiku-4-5", None));
        assert_eq!(
            Ladder::new(&default, &sparse, slot, target).next_target(),
            None
        );

        // The top rung.
        let (target, slot) = resolve_model("claude-opus-4-6", &map);
        assert_eq!(slot, Some("claude-opus"));
        assert_eq!(
            Ladder::new(&default, &map, slot, target).next_target(),
            None
        );

        // A slot the order does not name — `claude-fable` under the default order.
        let with_fable = model_map(&[("claude-fable", SMALL), ("claude-opus", LARGE)]);
        let (target, slot) = resolve_model("claude-fable-5", &with_fable);
        assert_eq!((target.as_str(), slot), (SMALL, Some("claude-fable")));
        assert_eq!(
            Ladder::new(&default, &with_fable, slot, target).next_target(),
            None
        );

        // The config gate.
        let off = PolicyConfig {
            escalate_on_context_limit: false,
            ..PolicyConfig::default()
        };
        let (target, slot) = resolve_model("claude-haiku-4-5", &map);
        assert_eq!(Ladder::new(&off, &map, slot, target).next_target(), None);
    }

    #[test]
    fn cache_control_is_stripped_wherever_it_is_nested() {
        let mut value = serde_json::json!({
            "model": "claude-opus-4-6",
            "system": [{"type": "text", "text": "hi", "cache_control": {"type": "ephemeral"}}],
            "tools": [{"name": "Bash", "cache_control": {"type": "ephemeral"}}],
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": "go", "cache_control": {"type": "ephemeral"}}
                ]}
            ],
            "cache_control": {"type": "ephemeral"}
        });
        strip_cache_control(&mut value);
        assert!(
            !serde_json::to_string(&value)
                .unwrap()
                .contains("cache_control"),
            "{value}"
        );
        assert_eq!(value["messages"][0]["content"][0]["text"], "go");
    }

    #[test]
    fn the_passthrough_body_substitutes_the_model_and_keeps_everything_else() {
        let raw = br#"{"model":"claude-opus-4-6","max_tokens":16,"messages":[{"role":"user","content":"hi","cache_control":{"type":"ephemeral"}}]}"#;
        let out = passthrough_body(raw, "target/Model").expect("valid body");
        let value: Value = serde_json::from_slice(&out).expect("valid json");
        assert_eq!(value["model"], "target/Model");
        assert_eq!(value["max_tokens"], 16);
        assert_eq!(value["messages"][0]["content"], "hi");
        assert!(value["messages"][0].get("cache_control").is_none());
    }

    #[test]
    fn an_openai_profiles_endpoint_is_the_chat_completions_path_whatever_was_called() {
        let profile = ProfileConfig {
            base_url: "https://api.example.com/".to_string(),
            api_key_env: "RELAY_TEST_KEY".to_string(),
            format: "openai".to_string(),
            serves: Vec::new(),
            model_map: IndexMap::new(),
            params: IndexMap::new(),
        };
        assert_eq!(
            endpoint(&profile, "/v1/messages", true),
            "https://api.example.com/v1/chat/completions"
        );
    }

    /// Fix round 2. This redaction runs *ahead of* the context-limit parse, so a
    /// placeholder value left in the environment would quietly eat words out of the
    /// provider's message and take the recovery with it — `context` alone stops
    /// detection firing, `token` costs the token pair. The floor is what keeps a
    /// mistake in a config file from silently disabling the thing this route exists
    /// for.
    #[test]
    fn a_placeholder_key_value_is_not_treated_as_a_key() {
        const SENTENCE: &[u8] =
            b"The input (170071 tokens) is longer than the model's context length (131072 tokens).";

        for placeholder in ["context", "token", "70", "", "x"] {
            assert_eq!(
                redact_key(SENTENCE.to_vec(), Some(placeholder)),
                SENTENCE.to_vec(),
                "{placeholder:?} is a mistake, not a key, and must not touch the message"
            );
        }
        assert_eq!(redact_key(SENTENCE.to_vec(), None), SENTENCE.to_vec());

        // A real key still redacts, or the floor would have bought safety by doing
        // nothing at all.
        let key = "sk-together-0123456789abcdef";
        let body = format!("Invalid API key provided: {key}.").into_bytes();
        let redacted = redact_key(body, Some(key));
        let text = String::from_utf8(redacted).expect("still UTF-8");
        assert_eq!(text, "Invalid API key provided: [REDACTED].");
    }

    /// The reason the floor matters, stated as behaviour rather than as a comment:
    /// with a placeholder key the provider's sentence survives intact, so detection
    /// still fires and still reports the pair.
    #[test]
    fn a_placeholder_key_value_does_not_disable_context_limit_recovery() {
        let body =
            br#"{"error":{"message":"The input (170071 tokens) is longer than the model's context length (131072 tokens).","type":"invalid_request_error"}}"#;
        let redacted = redact_key(body.to_vec(), Some("context"));
        let error = ProviderError::read(StatusCode::BAD_REQUEST, &redacted);
        assert_eq!(
            error.context_limit.and_then(|limit| limit.counts),
            Some((170071, 131072))
        );
    }

    #[test]
    fn an_anthropic_profiles_endpoint_mirrors_the_path_the_client_called() {
        let profile = ProfileConfig {
            base_url: "https://api.example.com".to_string(),
            api_key_env: "RELAY_TEST_KEY".to_string(),
            format: "anthropic".to_string(),
            serves: Vec::new(),
            model_map: IndexMap::new(),
            params: IndexMap::new(),
        };
        assert_eq!(
            endpoint(&profile, "/v1/messages", false),
            "https://api.example.com/v1/messages"
        );
    }
}
