//! The fallback route end to end (spec §6, §7a, §7b, §7d): failover policy,
//! name-based routing, model remap, and the header-hygiene invariant — driven
//! over real HTTP against three mock upstreams (Anthropic, an OpenAI-format
//! profile, and an Anthropic-format profile).

mod common;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::Request;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::any;
use indexmap::IndexMap;
use serde_json::Value;

use common::{
    relay_config, serve, serve_relay_with, serve_relay_with_routing_cap, truncated_body,
    unique_temp_dir,
};
use relay::config::{Config, ProfileConfig};

/// The client's own credentials. Every one of them must reach Anthropic
/// verbatim and none of them may reach a profile (spec §7b).
const CLIENT_AUTH: &str = "Bearer sk-ant-oat01-CLIENT-TOKEN-MUST-NOT-LEAK";
const CLIENT_API_KEY: &str = "sk-ant-api03-CLIENT-KEY-MUST-NOT-LEAK";
const CLIENT_BETA: &str = "prompt-caching-CLIENT-BETA-MUST-NOT-LEAK";
const CLIENT_VERSION: &str = "2099-01-01-CLIENT-VERSION";

const OPENAI_KEY_ENV: &str = "RELAY_TEST_OPENAI_PROFILE_KEY";
const OPENAI_KEY: &str = "together-key-for-the-openai-profile";
const COMPAT_KEY_ENV: &str = "RELAY_TEST_COMPAT_PROFILE_KEY";
const COMPAT_KEY: &str = "compat-key-for-the-anthropic-profile";
/// Deliberately never set, for the missing-key path.
const ABSENT_KEY_ENV: &str = "RELAY_TEST_KEY_THAT_IS_NEVER_SET";

const OPUS: &str = "claude-opus-4-6";
const OPUS_TARGET: &str = "target/Big-Model";
const CATCH_ALL_TARGET: &str = "target/Small-Model";
const OPEN_MODEL: &str = "deepseek-ai/DeepSeek-V4";

/// Spec §5's shape with the subscription marker, matching the default
/// `[detect]` rules — this is what drives the relay into `LIMITED`.
const LIMIT_BODY: &str = r#"{"type":"error","error":{"type":"rate_limit_error","message":"You have reached your Claude Pro usage limit. Your limit will reset at 6pm."}}"#;
const ANTHROPIC_OK: &str = r#"{"id":"msg_anthropic","type":"message","content":[{"type":"text","text":"from anthropic"}]}"#;
const COMPAT_OK: &str = r#"{"id":"msg_compat","type":"message","content":[{"type":"text","text":"from the compat profile"}]}"#;
const COMPAT_COUNT: &str = r#"{"input_tokens":41}"#;
const OPENAI_COMPLETION: &str = concat!(
    r#"{"id":"chatcmpl-1","object":"chat.completion","model":"target/Big-Model","#,
    r#""choices":[{"index":0,"message":{"role":"assistant","content":"from the openai profile"},"#,
    r#""finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":5}}"#
);

/// One OpenAI chunk plus its terminator: enough to prove the translator ran on
/// the way back without restating what `tests/translate_stream.rs` already
/// covers frame by frame.
const OPENAI_CHUNKS: [&str; 3] = [
    concat!(
        r#"data: {"id":"chatcmpl-1","object":"chat.completion.chunk","model":"target/Big-Model","#,
        r#""choices":[{"index":0,"delta":{"role":"assistant","content":"streamed"},"#,
        r#""finish_reason":null}]}"#,
        "\n\n"
    ),
    concat!(
        r#"data: {"id":"chatcmpl-1","object":"chat.completion.chunk","model":"target/Big-Model","#,
        r#""choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#,
        "\n\n"
    ),
    "data: [DONE]\n\n",
];

static KEYS: Once = Once::new();

/// A profile's key comes from the environment by design (spec §7b), so the
/// tests have to put it there. Every test calls this before it builds
/// anything, and `Once` makes the write happen exactly once with every other
/// test thread parked behind it.
fn set_profile_keys() {
    KEYS.call_once(|| {
        // SAFETY: the only writes to these variables in this process, done
        // before any relay in this file exists to read them.
        unsafe {
            std::env::set_var(OPENAI_KEY_ENV, OPENAI_KEY);
            std::env::set_var(COMPAT_KEY_ENV, COMPAT_KEY);
        }
    });
}

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Recorded>>>);

struct Recorded {
    path: String,
    headers: HeaderMap,
    body: String,
}

impl Recorder {
    fn count(&self) -> usize {
        self.0.lock().expect("recorder poisoned").len()
    }

    fn only(&self) -> Recorded {
        let mut seen = self.0.lock().expect("recorder poisoned");
        assert_eq!(seen.len(), 1, "expected exactly one recorded request");
        seen.pop().expect("checked above")
    }
}

impl Recorded {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }

    fn json(&self) -> Value {
        serde_json::from_str(&self.body).expect("recorded body must be JSON")
    }

    /// Every header name the upstream saw, sorted — so a test can assert the
    /// *whole* set rather than the absence of the few names someone thought to
    /// list. Spec §7b calls the fallback's headers an allowlist; only an
    /// equality assertion actually tests one.
    fn header_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.headers.keys().map(|name| name.as_str()).collect();
        names.sort_unstable();
        names
    }
}

/// What hyper and reqwest put on the wire themselves whatever the relay asked
/// for: the connection's `host`, the framing `content-length`, and reqwest's
/// default `accept`. Not part of the allowlist, but part of what an upstream
/// sees.
const TRANSPORT_HEADERS: [&str; 3] = ["accept", "content-length", "host"];

fn expected_headers(allowlist: &[&'static str]) -> Vec<&'static str> {
    let mut names: Vec<&'static str> = TRANSPORT_HEADERS.to_vec();
    names.extend_from_slice(allowlist);
    names.sort_unstable();
    names
}

async fn record(recorder: &Recorder, request: Request) -> Value {
    let (parts, body) = request.into_parts();
    let bytes = to_bytes(body, 8 * 1024 * 1024)
        .await
        .expect("failed to read the recorded body");
    let body = String::from_utf8_lossy(&bytes).into_owned();
    let parsed = serde_json::from_str(&body).unwrap_or(Value::Null);
    recorder
        .0
        .lock()
        .expect("recorder poisoned")
        .push(Recorded {
            path: parts.uri.path().to_string(),
            headers: parts.headers,
            body,
        });
    parsed
}

fn json(status: StatusCode, body: &'static str) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .expect("failed to build mock response")
}

fn limit_response() -> Response {
    Response::builder()
        .status(StatusCode::TOO_MANY_REQUESTS)
        .header("content-type", "application/json")
        .header("retry-after", "3600")
        .body(Body::from(LIMIT_BODY))
        .expect("failed to build mock response")
}

/// `/v1/limit` always serves the limit 429 (that is how a test drives the
/// relay into `LIMITED` without disturbing `/v1/messages`); `/v1/messages`
/// serves the limit error only when the test asked for it, so a passthrough
/// test can assert the client got Anthropic's own error.
fn anthropic_upstream(recorder: Recorder, messages_limited: bool) -> Router {
    let messages = recorder.clone();
    let count_tokens = recorder;
    Router::new()
        .route(
            "/v1/messages",
            any(move |request: Request| {
                let recorder = messages.clone();
                async move {
                    record(&recorder, request).await;
                    if messages_limited {
                        limit_response()
                    } else {
                        json(StatusCode::OK, ANTHROPIC_OK)
                    }
                }
            }),
        )
        .route(
            "/v1/messages/count_tokens",
            any(move |request: Request| {
                let recorder = count_tokens.clone();
                async move {
                    record(&recorder, request).await;
                    json(StatusCode::OK, r#"{"input_tokens":7}"#)
                }
            }),
        )
        .route("/v1/limit", any(|| async { limit_response() }))
}

fn openai_upstream(recorder: Recorder, stream_dies: bool) -> Router {
    Router::new().route(
        "/v1/chat/completions",
        any(move |request: Request| {
            let recorder = recorder.clone();
            async move {
                let body = record(&recorder, request).await;
                if body["stream"] != Value::Bool(true) {
                    return json(StatusCode::OK, OPENAI_COMPLETION);
                }
                let body = if stream_dies {
                    truncated_body(OPENAI_CHUNKS[0])
                } else {
                    Body::from(OPENAI_CHUNKS.concat())
                };
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(body)
                    .expect("failed to build mock response")
            }
        }),
    )
}

fn compat_upstream(recorder: Recorder) -> Router {
    let count_tokens = recorder.clone();
    Router::new()
        .route(
            "/v1/messages",
            any(move |request: Request| {
                let recorder = recorder.clone();
                async move {
                    record(&recorder, request).await;
                    json(StatusCode::OK, COMPAT_OK)
                }
            }),
        )
        .route(
            "/v1/messages/count_tokens",
            any(move |request: Request| {
                let recorder = count_tokens.clone();
                async move {
                    record(&recorder, request).await;
                    json(StatusCode::OK, COMPAT_COUNT)
                }
            }),
        )
}

fn profile(base: SocketAddr, format: &str, api_key_env: &str) -> ProfileConfig {
    ProfileConfig {
        base_url: format!("http://{base}"),
        api_key_env: api_key_env.to_string(),
        format: format.to_string(),
        serves: vec!["deepseek-ai/".to_string()],
        model_map: [("claude-opus", OPUS_TARGET), ("*", CATCH_ALL_TARGET)]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    }
}

fn config(anthropic: SocketAddr, mode: &str, fallback: ProfileConfig) -> Config {
    let mut config = relay_config(format!("http://{anthropic}"));
    let mut profiles = IndexMap::new();
    profiles.insert("fallback".to_string(), fallback);
    config.profiles = profiles;
    config.policy.mode = mode.to_string();
    config.policy.active_profile = Some("fallback".to_string());
    config
}

/// The relay, its Anthropic mock's recorder, and its fallback mock's recorder.
struct Relay {
    addr: SocketAddr,
    anthropic: Recorder,
    fallback: Recorder,
}

async fn start(
    mode: &str,
    messages_limited: bool,
    fallback_router: impl FnOnce(Recorder) -> Router,
    format: &str,
    api_key_env: &str,
) -> Relay {
    set_profile_keys();
    let anthropic = Recorder::default();
    let fallback = Recorder::default();
    let anthropic_addr = serve(anthropic_upstream(anthropic.clone(), messages_limited)).await;
    let fallback_addr = serve(fallback_router(fallback.clone())).await;
    let addr = serve_relay_with(
        config(
            anthropic_addr,
            mode,
            profile(fallback_addr, format, api_key_env),
        ),
        None,
    )
    .await;
    Relay {
        addr,
        anthropic,
        fallback,
    }
}

/// The common case: an OpenAI-format profile, a healthy fallback mock.
async fn start_openai(mode: &str, messages_limited: bool) -> Relay {
    start(
        mode,
        messages_limited,
        |recorder| openai_upstream(recorder, false),
        "openai",
        OPENAI_KEY_ENV,
    )
    .await
}

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

/// A request carrying every credential the client would really send, so an
/// absence assertion on the fallback side is about a header that was in flight
/// rather than one that was never sent.
fn authenticated(url: String, body: String) -> reqwest::RequestBuilder {
    client()
        .post(url)
        .header("authorization", CLIENT_AUTH)
        .header("x-api-key", CLIENT_API_KEY)
        .header("anthropic-beta", CLIENT_BETA)
        // Deliberately not the version the relay injects for an
        // anthropic-format profile, so "the profile saw 2023-06-01" proves the
        // injected constant rather than a passed-through client header.
        .header("anthropic-version", CLIENT_VERSION)
        .header("accept-encoding", "gzip")
        .body(body)
}

fn session_start(model: &str) -> String {
    format!(
        r#"{{"model":"{model}","max_tokens":64,"messages":[{{"role":"user","content":"hello"}}]}}"#
    )
}

fn mid_conversation(model: &str) -> String {
    format!(
        r#"{{"model":"{model}","max_tokens":64,"messages":[
            {{"role":"user","content":"hello"}},
            {{"role":"assistant","content":"hi there"}},
            {{"role":"user","content":"more"}}]}}"#
    )
}

async fn drive_to_limited(relay: SocketAddr) {
    let response = client()
        .get(format!("http://{relay}/v1/limit"))
        .send()
        .await
        .expect("limit request failed");
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    // The body has to be drained: detection classifies when the stream ends.
    response.bytes().await.expect("failed to read limit body");

    for _ in 0..200 {
        if status(relay).await["state"] == "LIMITED" {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the relay never reached LIMITED");
}

async fn status(relay: SocketAddr) -> Value {
    let bytes = client()
        .get(format!("http://{relay}/status"))
        .send()
        .await
        .expect("status request failed")
        .bytes()
        .await
        .expect("failed to read status body");
    serde_json::from_slice(&bytes).expect("status must be JSON")
}

/// `(event name, data)` for every SSE frame in `bytes`.
fn events(bytes: &[u8]) -> Vec<(String, Value)> {
    std::str::from_utf8(bytes)
        .expect("an SSE body must be UTF-8")
        .split("\n\n")
        .filter(|frame| !frame.trim().is_empty())
        .map(|frame| {
            let mut event = None;
            let mut data = String::new();
            for line in frame.split('\n') {
                if let Some(rest) = line.strip_prefix("event: ") {
                    event = Some(rest.to_string());
                } else if let Some(rest) = line.strip_prefix("data: ") {
                    data.push_str(rest);
                }
            }
            (
                event.expect("every frame carries an event name"),
                serde_json::from_str(&data).expect("every frame carries JSON data"),
            )
        })
        .collect()
}

/// Even in `all` mode — the most permissive policy there is — an `ACTIVE`
/// route means no `claude-*` request has any business leaving for a profile.
#[tokio::test]
async fn a_claude_request_while_active_never_touches_a_fallback_profile() {
    let relay = start_openai("all", false).await;

    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(session_start(OPUS))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers().get("x-relay-route").is_none(),
        "the Anthropic route marks nothing: its response is Anthropic's own"
    );
    assert_eq!(
        response.bytes().await.expect("failed to read body"),
        ANTHROPIC_OK.as_bytes()
    );
    assert_eq!(relay.fallback.count(), 0);
    assert_eq!(relay.anthropic.only().json()["model"], OPUS);
}

#[tokio::test]
async fn a_session_start_routes_to_the_fallback_while_limited_under_new_sessions() {
    let relay = start_openai("new-sessions", true).await;
    drive_to_limited(relay.addr).await;

    let response = authenticated(
        format!("http://{}/v1/messages", relay.addr),
        session_start(OPUS),
    )
    .send()
    .await
    .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    let body: Value = serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
        .expect("the translated response must be JSON");
    assert_eq!(body["type"], "message");
    assert_eq!(body["content"][0]["text"], "from the openai profile");

    assert_eq!(relay.anthropic.count(), 0, "nothing reached Anthropic");
    let seen = relay.fallback.only();
    assert_eq!(seen.path, "/v1/chat/completions");
    let sent = seen.json();
    assert_eq!(
        sent["model"], OPUS_TARGET,
        "spec §7a: the model is remapped"
    );
    assert_eq!(
        sent["messages"][0]["role"], "user",
        "the body reached the profile in OpenAI format"
    );
    assert_eq!(
        seen.header("authorization"),
        Some(&format!("Bearer {OPENAI_KEY}")[..])
    );
}

/// The point of `new-sessions`: a conversation already in flight fails
/// visibly rather than switching models mid-thought.
#[tokio::test]
async fn a_mid_conversation_request_gets_anthropics_limit_error_under_new_sessions() {
    let relay = start_openai("new-sessions", true).await;
    drive_to_limited(relay.addr).await;

    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(mid_conversation(OPUS))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(response.headers().get("x-relay-route").is_none());
    assert_eq!(
        response.bytes().await.expect("failed to read body"),
        LIMIT_BODY.as_bytes(),
        "the client must see Anthropic's own limit error, unaltered"
    );
    assert_eq!(relay.fallback.count(), 0);
    assert_eq!(relay.anthropic.count(), 1);
}

#[tokio::test]
async fn all_mode_routes_a_mid_conversation_request_to_the_fallback() {
    let relay = start_openai("all", true).await;
    drive_to_limited(relay.addr).await;

    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(mid_conversation(OPUS))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    response.bytes().await.expect("failed to read body");
    assert_eq!(relay.anthropic.count(), 0);
    assert_eq!(relay.fallback.only().json()["model"], OPUS_TARGET);
}

#[tokio::test]
async fn notify_only_never_routes_to_the_fallback() {
    let relay = start_openai("notify-only", true).await;
    drive_to_limited(relay.addr).await;

    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(session_start(OPUS))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(response.headers().get("x-relay-route").is_none());
    response.bytes().await.expect("failed to read body");
    assert_eq!(relay.fallback.count(), 0);
    assert_eq!(relay.anthropic.count(), 1);
}

/// Spec §7d: name-based routing is ordinary routing, not failover. `ACTIVE` is
/// the state that proves it — nothing about the limit machinery is involved —
/// and the name is passed through unremapped even though `model_map` has a
/// catch-all that would have rewritten it.
#[tokio::test]
async fn a_non_claude_model_routes_by_name_while_anthropic_is_active() {
    let relay = start_openai("notify-only", false).await;

    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(session_start(OPEN_MODEL))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    response.bytes().await.expect("failed to read body");
    assert_eq!(relay.anthropic.count(), 0);
    assert_eq!(
        relay.fallback.only().json()["model"],
        OPEN_MODEL,
        "a name-routed request keeps the name the client asked for"
    );
}

/// Global Constraint 7. `all` while `LIMITED` is the configuration under which
/// every other `claude-*` request leaves for the profile; this one still must
/// not.
#[tokio::test]
async fn count_tokens_never_routes_to_the_fallback_even_while_limited_in_all_mode() {
    let relay = start_openai("all", false).await;
    drive_to_limited(relay.addr).await;

    let response = client()
        .post(format!("http://{}/v1/messages/count_tokens", relay.addr))
        .body(session_start(OPUS))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("x-relay-route").is_none());
    assert_eq!(
        response.bytes().await.expect("failed to read body"),
        br#"{"input_tokens":7}"#.as_slice()
    );
    assert_eq!(relay.fallback.count(), 0);
    assert_eq!(relay.anthropic.only().path, "/v1/messages/count_tokens");
}

/// Spec §7b's tested invariant. The same request is sent twice: once while
/// `ACTIVE`, where Anthropic must receive every credential verbatim, and once
/// while `LIMITED`, where the profile must receive none of them.
#[tokio::test]
async fn the_clients_credentials_never_reach_a_fallback_profile() {
    let relay = start_openai("all", false).await;

    let response = authenticated(
        format!("http://{}/v1/messages", relay.addr),
        session_start(OPUS),
    )
    .send()
    .await
    .expect("request failed");
    assert_eq!(response.status(), StatusCode::OK);
    response.bytes().await.expect("failed to read body");

    let to_anthropic = relay.anthropic.only();
    assert_eq!(to_anthropic.header("authorization"), Some(CLIENT_AUTH));
    assert_eq!(to_anthropic.header("x-api-key"), Some(CLIENT_API_KEY));
    assert_eq!(to_anthropic.header("anthropic-beta"), Some(CLIENT_BETA));
    assert_eq!(
        to_anthropic.header("anthropic-version"),
        Some(CLIENT_VERSION)
    );
    assert_eq!(
        to_anthropic.header("accept-encoding"),
        Some("gzip"),
        "the absence assertions below are only meaningful if this was in flight"
    );

    drive_to_limited(relay.addr).await;

    let response = authenticated(
        format!("http://{}/v1/messages", relay.addr),
        session_start(OPUS),
    )
    .send()
    .await
    .expect("request failed");
    assert_eq!(response.status(), StatusCode::OK);
    response.bytes().await.expect("failed to read body");

    let to_fallback = relay.fallback.only();
    assert_eq!(
        to_fallback.header("authorization"),
        Some(&format!("Bearer {OPENAI_KEY}")[..]),
        "the profile is authenticated with its own key"
    );
    assert_eq!(to_fallback.header("x-api-key"), None);
    assert_eq!(to_fallback.header("anthropic-beta"), None);
    assert_eq!(to_fallback.header("anthropic-version"), None);
    assert_eq!(
        to_fallback.header("accept-encoding"),
        None,
        "the client asks for gzip; a compressed body is one the translator cannot read"
    );

    // The allowlist itself, not a list of names someone remembered to deny: a
    // regression that started forwarding `cookie`, `user-agent`, or any client
    // `x-*` header would pass a blocklist-shaped assertion and fails this one.
    assert_eq!(
        to_fallback.header_names(),
        expected_headers(&["authorization", "content-type"]),
        "an openai profile's request is built from exactly these headers"
    );

    let raw = format!("{:?}", to_fallback.headers);
    for secret in [CLIENT_AUTH, CLIENT_API_KEY, CLIENT_BETA, "sk-ant"] {
        assert!(
            !raw.contains(secret),
            "the profile received {secret:?} in some header: {raw}"
        );
    }
    assert!(
        !to_fallback.body.contains("sk-ant"),
        "the profile received a credential in the body"
    );
}

/// Spec §7c Phase 1: passthrough plus remap plus hygiene, no translator. No
/// such provider is configured (Global Constraint 10), so a mock is the only
/// thing this code path has ever been run against.
#[tokio::test]
async fn an_anthropic_format_profile_is_a_hygienic_remapped_passthrough() {
    let relay = start("all", true, compat_upstream, "anthropic", COMPAT_KEY_ENV).await;
    drive_to_limited(relay.addr).await;

    let body = r#"{"model":"claude-haiku-4-5","max_tokens":64,"messages":[
            {"role":"user","content":[
                {"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}]}"#;
    let response = authenticated(
        format!("http://{}/v1/messages", relay.addr),
        body.to_string(),
    )
    .send()
    .await
    .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    assert_eq!(
        response.bytes().await.expect("failed to read body"),
        COMPAT_OK.as_bytes(),
        "an anthropic-format profile's response is passed through untouched"
    );

    assert_eq!(relay.anthropic.count(), 0);
    let seen = relay.fallback.only();
    assert_eq!(seen.path, "/v1/messages");
    assert_eq!(
        seen.json()["model"],
        CATCH_ALL_TARGET,
        "no prefix claims claude-haiku, so the catch-all applies"
    );
    assert_eq!(seen.json()["messages"][0]["content"][0]["text"], "hello");
    assert!(
        !seen.body.contains("cache_control"),
        "spec §7b: cache_control is stripped on the way to a fallback"
    );
    assert_eq!(
        seen.header("authorization"),
        Some(&format!("Bearer {COMPAT_KEY}")[..])
    );
    assert_eq!(
        seen.header("x-api-key"),
        Some(COMPAT_KEY),
        "the profile's own key in the scheme the Anthropic API documents"
    );
    assert_eq!(seen.header("anthropic-version"), Some("2023-06-01"));
    assert_eq!(
        seen.header_names(),
        expected_headers(&[
            "anthropic-version",
            "authorization",
            "content-type",
            "x-api-key",
        ]),
        "an anthropic profile's request is built from exactly these headers"
    );
    assert!(
        !format!("{:?}", seen.headers).contains("sk-ant"),
        "no client credential reached the profile"
    );
}

#[tokio::test]
async fn a_streamed_fallback_response_is_translated_back_into_anthropic_events() {
    let relay = start_openai("all", true).await;
    drive_to_limited(relay.addr).await;

    let body = format!(
        r#"{{"model":"{OPUS}","max_tokens":64,"stream":true,"messages":[{{"role":"user","content":"hi"}}]}}"#
    );
    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(body)
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    assert_eq!(response.headers()["content-type"], "text/event-stream");

    let bytes = response.bytes().await.expect("failed to read body");
    let events = events(&bytes);
    let names: Vec<&str> = events.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
        ]
    );
    assert!(relay.fallback.only().json()["stream"] == Value::Bool(true));
}

/// Global Constraint 6. The eligibility decision happened before any byte
/// reached the client, so a stream that dies afterwards is terminal: an error
/// event, and no second attempt anywhere.
#[tokio::test]
async fn a_fallback_stream_that_dies_mid_response_is_never_retried() {
    let relay = start(
        "all",
        true,
        |recorder| openai_upstream(recorder, true),
        "openai",
        OPENAI_KEY_ENV,
    )
    .await;
    drive_to_limited(relay.addr).await;

    let body = format!(
        r#"{{"model":"{OPUS}","max_tokens":64,"stream":true,"messages":[{{"role":"user","content":"hi"}}]}}"#
    );
    let mut response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(body)
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    let mut collected = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .expect("the body must end cleanly so the error event is readable")
    {
        collected.extend_from_slice(&chunk);
    }

    let events = events(&collected);
    let (name, data) = events.last().expect("at least one event");
    assert_eq!(name, "error");
    assert_eq!(
        data["error"]["message"],
        "upstream stream ended unexpectedly"
    );

    assert_eq!(
        relay.fallback.count(),
        1,
        "the failed stream must not be retried on the fallback"
    );
    assert_eq!(
        relay.anthropic.count(),
        0,
        "and must not be retried on Anthropic either"
    );
}

/// A profile whose key env var is unset cannot be reached at all — and the
/// failure must be a clean 502 rather than a request sent without auth, or a
/// panic.
#[tokio::test]
async fn a_profile_with_no_key_in_the_environment_fails_without_sending_anything() {
    let relay = start(
        "all",
        true,
        |recorder| openai_upstream(recorder, false),
        "openai",
        ABSENT_KEY_ENV,
    )
    .await;
    drive_to_limited(relay.addr).await;

    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(session_start(OPUS))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    let body: Value = serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
        .expect("error body must be JSON");
    assert_eq!(body["error"], "fallback_key_missing");
    assert_eq!(relay.fallback.count(), 0);
}

/// A profile configured but no `active_profile` — a valid config, and the only
/// shape in which `router::route` returns `Err`, so both of the tests below
/// need it and nothing else does.
async fn start_without_an_active_profile() -> Relay {
    set_profile_keys();
    let anthropic = Recorder::default();
    let fallback = Recorder::default();
    let anthropic_addr = serve(anthropic_upstream(anthropic.clone(), false)).await;
    let fallback_addr = serve(openai_upstream(fallback.clone(), false)).await;
    let mut config = config(
        anthropic_addr,
        "all",
        profile(fallback_addr, "openai", OPENAI_KEY_ENV),
    );
    config.policy.active_profile = None;
    let addr = serve_relay_with(config, None).await;
    Relay {
        addr,
        anthropic,
        fallback,
    }
}

/// §7d's dead end: a non-`claude-*` name no profile claims, with no active
/// profile to fall through to. The router has nothing to resolve it against, so
/// the relay says so rather than sending an open-model name to Anthropic to be
/// rejected there.
#[tokio::test]
async fn a_name_no_profile_claims_with_no_active_profile_is_a_clean_error() {
    let relay = start_without_an_active_profile().await;

    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(session_start("some-other-provider/model"))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
        .expect("error body must be JSON");
    assert_eq!(body["error"], "no_route_for_model");
    assert_eq!(relay.anthropic.count(), 0);
    assert_eq!(relay.fallback.count(), 0);
}

/// The same dead end reached by a count, which does not share that answer.
/// Global Constraint 7 pins `count_tokens` to Anthropic whatever the state, so
/// an unroutable name there is Anthropic's to reject — the relay's own 400 in
/// its place would contradict spec §6's "on failure, pass the error through",
/// and would change where this request went before `count_tokens` was routed
/// at all.
#[tokio::test]
async fn count_tokens_for_an_unroutable_name_still_reaches_anthropic() {
    let relay = start_without_an_active_profile().await;

    let response = client()
        .post(format!("http://{}/v1/messages/count_tokens", relay.addr))
        .body(session_start("some-other-provider/model"))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("x-relay-route").is_none());
    assert_eq!(
        response.bytes().await.expect("failed to read body"),
        br#"{"input_tokens":7}"#.as_slice()
    );
    assert_eq!(relay.fallback.count(), 0);
    let seen = relay.anthropic.only();
    assert_eq!(seen.path, "/v1/messages/count_tokens");
    assert_eq!(
        seen.json()["model"],
        "some-other-provider/model",
        "the name reaches the tokenizer that owns the verdict on it, unremapped"
    );
}

#[tokio::test]
async fn status_counts_fallback_requests_and_only_those() {
    let relay = start_openai("all", false).await;

    client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(session_start(OPUS))
        .send()
        .await
        .expect("request failed")
        .bytes()
        .await
        .expect("failed to read body");
    assert_eq!(status(relay.addr).await["fallback_requests_served"], 0);

    drive_to_limited(relay.addr).await;
    client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(session_start(OPUS))
        .send()
        .await
        .expect("request failed")
        .bytes()
        .await
        .expect("failed to read body");

    assert_eq!(status(relay.addr).await["fallback_requests_served"], 1);
}

/// Global Constraint 7, narrowed. An `anthropic`-format profile has a
/// `/v1/messages/count_tokens` of its own, which this route mirrors, so a
/// name-routed count reaches the provider that owns the tokenizer being asked
/// about.
#[tokio::test]
async fn count_tokens_routes_by_name_to_an_anthropic_format_profile() {
    let relay = start(
        "notify-only",
        false,
        compat_upstream,
        "anthropic",
        COMPAT_KEY_ENV,
    )
    .await;

    let response = client()
        .post(format!("http://{}/v1/messages/count_tokens", relay.addr))
        .body(session_start(OPEN_MODEL))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    assert_eq!(
        response.bytes().await.expect("failed to read body"),
        COMPAT_COUNT.as_bytes()
    );
    assert_eq!(relay.anthropic.count(), 0);
    let seen = relay.fallback.only();
    assert_eq!(seen.path, "/v1/messages/count_tokens");
    assert_eq!(
        seen.json()["model"],
        OPEN_MODEL,
        "a name-routed count is not remapped"
    );
}

/// The other half of the same rule: an `openai` profile has no counting
/// endpoint at all. Routing there would send the request to
/// `/v1/chat/completions` — a billed inference call answering with a `message`
/// where the client wants `{"input_tokens": N}` — so it keeps the Anthropic
/// pin instead.
#[tokio::test]
async fn count_tokens_for_an_openai_profiles_model_stays_on_anthropic() {
    let relay = start_openai("notify-only", false).await;

    let response = client()
        .post(format!("http://{}/v1/messages/count_tokens", relay.addr))
        .body(session_start(OPEN_MODEL))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("x-relay-route").is_none());
    assert_eq!(
        response.bytes().await.expect("failed to read body"),
        br#"{"input_tokens":7}"#.as_slice()
    );
    assert_eq!(relay.fallback.count(), 0);
    assert_eq!(
        relay.anthropic.only().json()["model"],
        OPEN_MODEL,
        "the open-model name reaches Anthropic, which is what rejects it"
    );
}

/// Spec §7d's fall-through, with no limit state involved at all: a name no
/// profile's `serves` claims goes to `active_profile`. This is the path a
/// model-name typo takes, so it is worth proving it lands where the spec says
/// and nowhere else.
#[tokio::test]
async fn an_unclaimed_name_falls_through_to_the_active_profile_while_active() {
    let relay = start_openai("notify-only", false).await;

    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(session_start("typo-provider/DeepSeek-V4"))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    response.bytes().await.expect("failed to read body");
    assert_eq!(relay.anthropic.count(), 0);
    assert_eq!(
        relay.fallback.only().json()["model"],
        "typo-provider/DeepSeek-V4",
        "fall-through is still name routing: no remap"
    );
}

/// Item 2 of review round 1. With no profile configured the router could only
/// ever answer `Anthropic`, so reading the body would buy a decision already
/// made — and Milestone 1's streamed passthrough would have been given up for
/// nothing. `transfer-encoding: chunked` is the observable difference: a
/// buffered body is sent with a `content-length` instead.
#[tokio::test]
async fn a_zero_profile_relay_never_reads_the_request_body() {
    set_profile_keys();
    let recorder = Recorder::default();
    let anthropic_addr = serve(anthropic_upstream(recorder.clone(), false)).await;
    let relay = serve_relay_with(relay_config(format!("http://{anthropic_addr}")), None).await;

    client()
        .post(format!("http://{relay}/v1/messages"))
        .body(session_start(OPUS))
        .send()
        .await
        .expect("request failed")
        .bytes()
        .await
        .expect("failed to read body");

    let seen = recorder.only();
    assert_eq!(
        seen.header("transfer-encoding"),
        Some("chunked"),
        "a zero-profile relay must stream the request body through, not buffer it"
    );
    assert_eq!(seen.header("content-length"), None);
}

/// The contrast case, so the assertion above is about the guard rather than
/// about how hyper happens to frame things.
#[tokio::test]
async fn a_relay_with_a_profile_does_buffer_the_request_body() {
    let relay = start_openai("notify-only", false).await;

    client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(session_start(OPUS))
        .send()
        .await
        .expect("request failed")
        .bytes()
        .await
        .expect("failed to read body");

    let seen = relay.anthropic.only();
    assert!(seen.header("content-length").is_some());
    assert_eq!(seen.header("transfer-encoding"), None);
}

/// The over-cap path reassembles the body from the prefix already read plus
/// the rest of the client's stream. Nothing may be dropped or duplicated
/// across that split, and the request must still reach Anthropic — the cap
/// costs a routing decision, never a byte.
///
/// End to end over real HTTP, so hyper picks the framing and this cannot say
/// where the split lands; what it proves is that reassembly survives whatever
/// framing production actually produces. The boundary arithmetic itself is
/// pinned by the unit tests in `src/proxy.rs`, which drive `read_for_routing`
/// over frames they choose.
#[tokio::test]
async fn a_body_past_the_routing_cap_reaches_anthropic_byte_for_byte() {
    set_profile_keys();
    let anthropic = Recorder::default();
    let fallback = Recorder::default();
    let anthropic_addr = serve(anthropic_upstream(anthropic.clone(), false)).await;
    let fallback_addr = serve(openai_upstream(fallback.clone(), false)).await;
    // Far below the body, so the very first frame busts the cap and everything
    // after it has to come back through `Prefixed::rest`.
    let relay = serve_relay_with_routing_cap(
        config(
            anthropic_addr,
            "all",
            profile(fallback_addr, "openai", OPENAI_KEY_ENV),
        ),
        512,
    )
    .await;
    drive_to_limited(relay).await;

    // Every byte distinct enough that a duplicated or dropped chunk cannot
    // coincidentally still compare equal.
    let filler: String = (0..40_000)
        .map(|i| char::from(b'a' + (i % 26) as u8))
        .collect();
    let body = format!(
        r#"{{"model":"{OPUS}","max_tokens":64,"messages":[{{"role":"user","content":"{filler}"}}]}}"#
    );
    assert!(
        body.len() > 512 * 8,
        "the body must cross the cap by a margin"
    );

    let response = client()
        .post(format!("http://{relay}/v1/messages"))
        .body(body.clone())
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    response.bytes().await.expect("failed to read body");

    let seen = anthropic.only();
    assert_eq!(
        seen.body.len(),
        body.len(),
        "the reassembled body changed length"
    );
    assert_eq!(
        seen.body, body,
        "the reassembled body is not byte-identical"
    );
    assert_eq!(
        fallback.count(),
        0,
        "an uninspectable body cannot have been routed by its model name"
    );
}

/// The documented invariant, tested rather than left to hold by accident: a
/// fallback provider's error says nothing about Anthropic's limit window. If
/// it did, one 429 from the fallback would pin every later request to the
/// fallback that produced it.
#[tokio::test]
async fn a_fallback_limit_error_never_changes_anthropics_route_state() {
    set_profile_keys();
    let anthropic = Recorder::default();
    let anthropic_addr = serve(anthropic_upstream(anthropic.clone(), false)).await;
    // A fallback that answers with the exact body `[detect]` classifies as the
    // subscription limit — the strongest possible version of this test.
    let fallback_addr =
        serve(Router::new().route("/v1/chat/completions", any(|| async { limit_response() })))
            .await;
    let fixtures = unique_temp_dir("fallback-no-state-change");
    let relay = serve_relay_with(
        config(
            anthropic_addr,
            "notify-only",
            profile(fallback_addr, "openai", OPENAI_KEY_ENV),
        ),
        Some(fixtures.clone()),
    )
    .await;

    // Name-routed (§7d), so Anthropic is ACTIVE throughout and nothing but the
    // fallback's own response could move it.
    let response = client()
        .post(format!("http://{relay}/v1/messages"))
        .body(session_start(OPEN_MODEL))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    assert_eq!(
        response.bytes().await.expect("failed to read body"),
        LIMIT_BODY.as_bytes(),
        "the provider's own error surfaces, untranslated"
    );

    // Give the state applier and any fixture write a chance to happen, so this
    // is an assertion about behavior rather than about being fast.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        status(relay).await["state"],
        "ACTIVE",
        "a fallback's 429 must not put the Anthropic route into LIMITED"
    );
    // `.expect`, not a default of 0: `Capture::new` creates this directory at
    // startup, so a missing one means the assertion below never looked at
    // anything.
    let fixtures_written = std::fs::read_dir(&fixtures)
        .expect("--capture-errors creates its directory at startup")
        .count();
    assert_eq!(
        fixtures_written, 0,
        "capture fixtures exist to derive Anthropic detection rules; a fallback's \
         error is not one"
    );
    assert_eq!(anthropic.count(), 0);
    let _ = std::fs::remove_dir_all(&fixtures);
}

/// The relay's own audit marker must mean the relay said so. An upstream that
/// emits `x-relay-route` — misconfigured, or a `base_url` pointed somewhere it
/// shouldn't be — cannot forge it.
#[tokio::test]
async fn an_upstream_cannot_forge_the_route_marker() {
    set_profile_keys();
    let anthropic_addr = serve(Router::new().route(
        "/v1/messages",
        any(|| async {
            Response::builder()
                .status(StatusCode::OK)
                .header("x-relay-route", "fallback")
                .header("content-type", "application/json")
                .body(Body::from(ANTHROPIC_OK))
                .expect("failed to build mock response")
        }),
    ))
    .await;
    let relay = serve_relay_with(relay_config(format!("http://{anthropic_addr}")), None).await;

    // Straight at the mock first: the absence assertion below is only about
    // stripping if the header was really being sent.
    let direct = client()
        .post(format!("http://{anthropic_addr}/v1/messages"))
        .send()
        .await
        .expect("request failed");
    assert_eq!(direct.headers()["x-relay-route"], "fallback");

    let response = client()
        .post(format!("http://{relay}/v1/messages"))
        .body(session_start(OPUS))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers().get("x-relay-route").is_none(),
        "only the relay may set the marker"
    );
}

/// A translated response is synthesized by the relay, but the status it
/// carries is still the provider's answer — a 202 must not silently become a
/// 200.
#[tokio::test]
async fn a_translated_response_keeps_the_upstreams_status() {
    set_profile_keys();
    let anthropic_addr = serve(anthropic_upstream(Recorder::default(), false)).await;
    let fallback_addr = serve(Router::new().route(
        "/v1/chat/completions",
        any(|| async { json(StatusCode::ACCEPTED, OPENAI_COMPLETION) }),
    ))
    .await;
    let relay = serve_relay_with(
        config(
            anthropic_addr,
            "notify-only",
            profile(fallback_addr, "openai", OPENAI_KEY_ENV),
        ),
        None,
    )
    .await;

    let response = client()
        .post(format!("http://{relay}/v1/messages"))
        .body(session_start(OPEN_MODEL))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    let body: Value = serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
        .expect("the translated response must be JSON");
    assert_eq!(body["content"][0]["text"], "from the openai profile");
}
