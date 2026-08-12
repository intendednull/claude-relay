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

use crate::config::ProfileConfig;
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
    let profile = request.profile;
    let target_model = if request.remap {
        remap_model(request.model, &profile.model_map)
    } else {
        request.model.to_string()
    };

    let translated = profile.format == "openai";
    let prepared = if translated {
        translate::request_to_openai(&body, &target_model).map(|request| Prepared {
            body: request.body,
            stream: request.stream,
        })
    } else {
        passthrough_body(&body, &target_model).map(|body| Prepared {
            body,
            stream: false,
        })
    };
    let prepared = match prepared {
        Ok(prepared) => prepared,
        Err(err) => {
            tracing::warn!(
                profile = request.profile_name,
                // The translator's errors name a location, never a value.
                error = %err,
                "could not prepare the request for the fallback profile"
            );
            return fallback_error(StatusCode::BAD_GATEWAY, "fallback_request_untranslatable");
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
            return fallback_error(StatusCode::BAD_GATEWAY, err.code());
        }
    };

    let target = endpoint(profile, &path, translated);
    let upstream = state
        .http
        .request(method.clone(), target)
        .headers(headers)
        .body(prepared.body)
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
            return fallback_error(StatusCode::BAD_GATEWAY, "upstream_unreachable");
        }
    };

    state
        .fallback_requests_served
        .fetch_add(1, Ordering::Relaxed);

    let status = upstream.status();
    let log = RequestLog {
        route: "fallback",
        profile: Some(request.profile_name.to_string()),
        model_in: Some(request.model.to_string()),
        model_out: Some(target_model),
        method,
        path,
        status,
        latency_ms: elapsed_ms(start),
    };

    // A fallback response says nothing about Anthropic's route state, so
    // neither `route_updates` nor `--capture-errors` (whose fixtures exist to
    // derive Anthropic detection rules from) hears about it. A 429 from the
    // fallback provider must not put the Anthropic route into `Limited`, and a
    // 200 from it must not recover the route out of it.
    if !status.is_success() {
        return provider_error_response(profile, request.profile_name, status, upstream, log).await;
    }

    if !translated {
        return passthrough_response(status, upstream, log);
    }

    // Read once here rather than per-chunk: a config reload mid-stream must not
    // change the shape of a message already in flight.
    let surface_reasoning = state.config.policy.surface_fallback_reasoning;

    if prepared.stream {
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
        return response;
    }

    let raw = match read_capped(upstream, RESPONSE_CAP).await {
        Ok(raw) => raw,
        Err(reason) => {
            tracing::warn!(
                profile = request.profile_name,
                reason,
                "fallback response unusable"
            );
            return fallback_error(StatusCode::BAD_GATEWAY, "fallback_response_unreadable");
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
            return fallback_error(StatusCode::BAD_GATEWAY, "fallback_response_untranslatable");
        }
    };
    log.emit(anthropic.len() as u64);

    let mut response = Response::new(Body::from(anthropic));
    *response.status_mut() = status;
    *response.headers_mut() = translated_headers("application/json");
    response
}

struct Prepared {
    body: Vec<u8>,
    stream: bool,
}

/// Spec §7a. The longest matching prefix wins; equal-length matches go to the
/// one declared first, which is the whole reason `model_map` is an `IndexMap`.
/// `"*"` is consulted only when nothing else matched, and a name no entry
/// claims is sent on unchanged — the provider's own "unknown model" is a
/// better answer than one this proxy invents.
pub fn remap_model(model: &str, model_map: &IndexMap<String, String>) -> String {
    let mut best: Option<(&String, &String)> = None;
    for (prefix, target) in model_map {
        if prefix == "*" || !model.starts_with(prefix.as_str()) {
            continue;
        }
        if best.is_none_or(|(current, _)| prefix.len() > current.len()) {
            best = Some((prefix, target));
        }
    }
    if let Some((_, target)) = best {
        return target.clone();
    }
    model_map
        .get("*")
        .cloned()
        .unwrap_or_else(|| model.to_string())
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

fn translated_headers(content_type: &'static str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static(content_type));
    headers.insert(ROUTE_MARKER, HeaderValue::from_static("fallback"));
    headers
}

/// Spec §7d: a provider's error reaches the client in Anthropic's envelope,
/// with the provider's own status and message preserved. It used to pass through
/// verbatim; the shapes were unknown when that rule was written and are captured
/// now, and for a context-limit error the passthrough cost the user the whole
/// session — Claude Code's compact-and-retry keys on Anthropic's wording, which
/// no provider here uses (`docs/decisions.md`).
async fn provider_error_response(
    profile: &ProfileConfig,
    profile_name: &str,
    status: StatusCode,
    upstream: reqwest::Response,
    log: RequestLog,
) -> Response {
    let mut headers = translated_headers("application/json");
    // An allowlist of one, not `forwardable`'s denylist: this body is the
    // relay's, so the provider's `content-length` and `content-encoding`
    // describe bytes that are no longer being sent. `retry-after` is the only
    // header on an error the client acts on, so it is the only one kept.
    if let Some(retry_after) = upstream.headers().get("retry-after").cloned() {
        headers.insert("retry-after", retry_after);
    }

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

    let body = ProviderError::read(status, &raw).to_anthropic();
    log.emit(body.len() as u64);

    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
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
        };
        assert_eq!(
            endpoint(&profile, "/v1/messages", false),
            "https://api.example.com/v1/messages"
        );
    }
}
