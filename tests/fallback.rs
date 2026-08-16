//! The fallback route end to end (spec §6, §7a, §7b, §7d): failover policy,
//! name-based routing, model remap, and the header-hygiene invariant — driven
//! over real HTTP against three mock upstreams (Anthropic, an OpenAI-format
//! profile, and an Anthropic-format profile).

mod common;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::Request;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::any;
use indexmap::IndexMap;
use serde_json::Value;

use common::{
    closed_port, dripped_body, relay_config, serve, serve_relay_with, serve_relay_with_routing_cap,
    truncated_body, unique_temp_dir,
};
use relay::config::{Config, ProfileConfig};
use relay::state::RE_ARM_SUCCESSES;

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
        params: IndexMap::new(),
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
    wait_for_limited(relay).await;
}

/// The state applier runs on a thread of its own, so `LIMITED` is waited for
/// rather than assumed. Returns the `/status` body that satisfied the wait.
async fn wait_for_limited(relay: SocketAddr) -> Value {
    for _ in 0..200 {
        let status = status(relay).await;
        if status["state"] == "LIMITED" {
            return status;
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

/// `start_openai`, plus a `params` table on the profile. Separate rather than a
/// parameter on `start`: every other test in this file wants the default.
async fn start_with_params(params: IndexMap<String, IndexMap<String, Value>>) -> Relay {
    set_profile_keys();
    let anthropic = Recorder::default();
    let fallback = Recorder::default();
    let anthropic_addr = serve(anthropic_upstream(anthropic.clone(), false)).await;
    let fallback_addr = serve(openai_upstream(fallback.clone(), false)).await;
    let mut profile = profile(fallback_addr, "openai", OPENAI_KEY_ENV);
    profile.params = params;
    let addr = serve_relay_with(config(anthropic_addr, "notify-only", profile), None).await;
    Relay {
        addr,
        anthropic,
        fallback,
    }
}

/// One name-routed request, checked as far as the route marker; the assertions
/// this file's params test is about are on what the upstream recorded.
async fn route_by_name(relay: &Relay, model: &str) {
    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(session_start(model))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    response.bytes().await.expect("failed to read body");
    assert_eq!(relay.anthropic.count(), 0, "nothing reached Anthropic");
}

/// Spec §Testing 3, the primary test for per-model `params`. The tuned model in
/// the live config is reachable only by name — `serves`-matched, absent from
/// `model_map` — so the failover path Task 7 covers is not the one production
/// exercises for it. `notify-only` while `ACTIVE` keeps the limit machinery out
/// of it entirely (§7d).
///
/// The untuned neighbour is compared byte for byte against a relay whose profile
/// has no `params` table at all, not merely checked for the absence of the key:
/// a lookup that leaked a set across models inside the profile would still be
/// caught if it injected some *other* value.
#[tokio::test]
async fn params_reach_only_the_tuned_model_on_the_name_routed_path() {
    let tuned_model = format!("{OPEN_MODEL}-Flash-0731");
    let mut set = IndexMap::new();
    set.insert("reasoning_effort".to_string(), Value::from("max"));
    let mut params = IndexMap::new();
    params.insert(tuned_model.clone(), set);

    let relay = start_with_params(params).await;

    route_by_name(&relay, &tuned_model).await;
    let tuned = relay.fallback.only();
    assert_eq!(tuned.path, "/v1/chat/completions");
    assert_eq!(
        tuned.json()["model"],
        tuned_model.as_str(),
        "a name-routed request keeps the name the client asked for"
    );
    assert_eq!(
        tuned.json()["reasoning_effort"],
        "max",
        "the configured param reached the upstream body"
    );

    route_by_name(&relay, OPEN_MODEL).await;
    let untuned = relay.fallback.only();

    let baseline_relay = start_with_params(IndexMap::new()).await;
    route_by_name(&baseline_relay, OPEN_MODEL).await;
    let baseline = baseline_relay.fallback.only();

    assert_eq!(
        untuned.json()["model"],
        OPEN_MODEL,
        "the neighbour is the model the client asked for, not the tuned one"
    );
    assert_eq!(
        untuned.body, baseline.body,
        "a neighbour's tuning must leave this model's body byte-identical to a params-absent baseline"
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

/// A provider that reasons, in both response shapes. Modelled on real Together
/// AI traffic: the reasoning arrives before the answer, and the key it arrives
/// under depends on the model — `reasoning` on most (`Kimi-K2.7-Code`,
/// `DeepSeek-V4-*`, `GLM-5.2`, …), `reasoning_content` on `Kimi-K3`. Both are
/// driven through the route, because a route that only handles one spelling
/// silently drops the reasoning on every other model.
fn reasoning_completion(key: &str) -> String {
    format!(
        concat!(
            r#"{{"id":"chatcmpl-2","object":"chat.completion","model":"target/Big-Model","#,
            r#""choices":[{{"index":0,"message":{{"role":"assistant","#,
            r#""{key}":"All but 9 run away, so 9 remain.","content":"9 sheep."}},"#,
            r#""finish_reason":"stop"}}],"usage":{{"prompt_tokens":3,"completion_tokens":5}}}}"#
        ),
        key = key
    )
}

/// `token_id` rides along on the providers that spell the key `reasoning`;
/// included so the route is exercised against the real delta shape.
fn reasoning_chunks(key: &str) -> String {
    format!(
        concat!(
            r#"data: {{"id":"chatcmpl-2","object":"chat.completion.chunk","model":"target/Big-Model","#,
            r#""choices":[{{"index":0,"delta":{{"role":"assistant","token_id":7,"#,
            r#""{key}":"All but 9"}},"finish_reason":null}}]}}"#,
            "\n\n",
            r#"data: {{"id":"chatcmpl-2","object":"chat.completion.chunk","model":"target/Big-Model","#,
            r#""choices":[{{"index":0,"delta":{{"content":"9 sheep."}},"finish_reason":null}}]}}"#,
            "\n\n",
            r#"data: {{"id":"chatcmpl-2","object":"chat.completion.chunk","model":"target/Big-Model","#,
            r#""choices":[{{"index":0,"delta":{{}},"finish_reason":"stop"}}]}}"#,
            "\n\n",
            "data: [DONE]\n\n",
        ),
        key = key
    )
}

fn reasoning_upstream(recorder: Recorder, key: &'static str) -> Router {
    Router::new().route(
        "/v1/chat/completions",
        any(move |request: Request| {
            let recorder = recorder.clone();
            async move {
                let body = record(&recorder, request).await;
                if body["stream"] != Value::Bool(true) {
                    return Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/json")
                        .body(Body::from(reasoning_completion(key)))
                        .expect("failed to build mock response");
                }
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from(reasoning_chunks(key)))
                    .expect("failed to build mock response")
            }
        }),
    )
}

/// `start`, plus the one config knob this pair of tests is about. Separate
/// rather than a parameter on `start`: every other test in this file wants the
/// default, and threading a flag through all of them to serve two would be
/// noise.
async fn start_reasoning(surface: bool, key: &'static str) -> Relay {
    set_profile_keys();
    let anthropic = Recorder::default();
    let fallback = Recorder::default();
    let anthropic_addr = serve(anthropic_upstream(anthropic.clone(), true)).await;
    let fallback_addr = serve(reasoning_upstream(fallback.clone(), key)).await;
    let mut config = config(
        anthropic_addr,
        "all",
        profile(fallback_addr, "openai", OPENAI_KEY_ENV),
    );
    config.policy.surface_fallback_reasoning = surface;
    let addr = serve_relay_with(config, None).await;
    Relay {
        addr,
        anthropic,
        fallback,
    }
}

async fn fallback_content(relay: &Relay, stream: bool) -> Value {
    let body = format!(
        r#"{{"model":"{OPUS}","max_tokens":64,"stream":{stream},"messages":[{{"role":"user","content":"hi"}}]}}"#
    );
    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(body)
        .send()
        .await
        .expect("request failed");
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.bytes().await.expect("failed to read body");
    if !stream {
        return serde_json::from_slice::<Value>(&bytes).expect("body is not JSON")["content"]
            .clone();
    }
    // The blocks a client would assemble from the event stream, in order.
    Value::Array(
        events(&bytes)
            .into_iter()
            .filter(|(name, _)| name == "content_block_start")
            .map(|(_, data)| data["content_block"].clone())
            .collect(),
    )
}

/// Both reasoning spellings, end to end through the route rather than through
/// the translator alone: nothing else proves `policy.surface_fallback_reasoning`
/// is actually read on the request path, and nothing else proves the route
/// handles the common spelling as well as `Kimi-K3`'s.
#[tokio::test]
async fn the_fallbacks_reasoning_reaches_the_client_as_a_thinking_block() {
    for key in ["reasoning", "reasoning_content"] {
        let relay = start_reasoning(true, key).await;
        drive_to_limited(relay.addr).await;
        assert_eq!(
            fallback_content(&relay, false).await,
            serde_json::json!([
                {"type": "thinking", "thinking": "All but 9 run away, so 9 remain."},
                {"type": "text", "text": "9 sheep."},
            ]),
            "spelled {key:?}"
        );

        let relay = start_reasoning(true, key).await;
        drive_to_limited(relay.addr).await;
        assert_eq!(
            fallback_content(&relay, true).await,
            serde_json::json!([
                {"type": "thinking", "thinking": ""},
                {"type": "text", "text": ""},
            ]),
            "spelled {key:?}"
        );
    }
}

#[tokio::test]
async fn surface_fallback_reasoning_false_restores_the_dropped_reasoning() {
    for key in ["reasoning", "reasoning_content"] {
        let relay = start_reasoning(false, key).await;
        drive_to_limited(relay.addr).await;
        assert_eq!(
            fallback_content(&relay, false).await,
            serde_json::json!([{"type": "text", "text": "9 sheep."}]),
            "spelled {key:?}"
        );

        let relay = start_reasoning(false, key).await;
        drive_to_limited(relay.addr).await;
        assert_eq!(
            fallback_content(&relay, true).await,
            serde_json::json!([{"type": "text", "text": ""}]),
            "spelled {key:?}"
        );
    }
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

/// The rest of the route's relay-generated failure surface, one test per code.
///
/// These are the responses the relay writes itself, so each one has to carry
/// `x-relay-route: fallback` as well as the right status and code: "no marker"
/// is the claim that a response came from Anthropic (`docs/decisions.md`), and
/// a failed fallback attempt did not. `fallback_error` centralizes the marker,
/// so what these tests watch is the funnel into it — every branch that can
/// answer without reaching that helper.
///
/// All of them route by name (§7d) rather than by failover, so the limit
/// machinery is not a variable in a test about a failure path.
///
/// A body past `RESPONSE_CAP` (4 MiB), which is the one thing this route
/// buffers whole. The completion is well-formed and would translate fine — the
/// cap is the only reason it is refused.
fn oversized_upstream(recorder: Recorder) -> Router {
    Router::new().route(
        "/v1/chat/completions",
        any(move |request: Request| {
            let recorder = recorder.clone();
            async move {
                record(&recorder, request).await;
                let filler = "x".repeat(5 * 1024 * 1024);
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        concat!(
                            r#"{{"id":"chatcmpl-big","object":"chat.completion","#,
                            r#""model":"target/Big-Model","choices":[{{"index":0,"#,
                            r#""message":{{"role":"assistant","content":"{}"}},"#,
                            r#""finish_reason":"stop"}}]}}"#
                        ),
                        filler
                    )))
                    .expect("failed to build mock response")
            }
        }),
    )
}

#[tokio::test]
async fn a_fallback_response_past_the_buffer_cap_is_a_marked_502() {
    let relay = start(
        "notify-only",
        false,
        oversized_upstream,
        "openai",
        OPENAI_KEY_ENV,
    )
    .await;

    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(session_start(OPEN_MODEL))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    let body: Value = serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
        .expect("error body must be JSON");
    assert_eq!(body["error"], "fallback_response_unreadable");
    assert_eq!(
        relay.fallback.count(),
        1,
        "the request did reach the profile; it is the answer that was unusable"
    );
}

/// Valid JSON and not a chat completion — what an `openai` profile whose
/// `base_url` actually speaks Anthropic answers with. Being valid JSON is the
/// point: it proves the check is about the response's shape, not about the
/// bytes parsing at all.
const ANTHROPIC_SHAPED_BODY: &str =
    r#"{"id":"msg_x","type":"message","role":"assistant","content":[{"type":"text","text":"hi"}]}"#;

#[tokio::test]
async fn a_two_hundred_that_is_not_a_chat_completion_is_a_marked_502() {
    let relay = start(
        "notify-only",
        false,
        |recorder| {
            Router::new().route(
                "/v1/chat/completions",
                any(move |request: Request| {
                    let recorder = recorder.clone();
                    async move {
                        record(&recorder, request).await;
                        json(StatusCode::OK, ANTHROPIC_SHAPED_BODY)
                    }
                }),
            )
        },
        "openai",
        OPENAI_KEY_ENV,
    )
    .await;

    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(session_start(OPEN_MODEL))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    let body: Value = serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
        .expect("error body must be JSON");
    assert_eq!(body["error"], "fallback_response_untranslatable");
    assert_eq!(
        relay.fallback.count(),
        1,
        "the upstream answered; its answer is what could not be translated"
    );
}

/// A request the translator refuses. `role: "function"` is not a role the
/// Anthropic Messages API has, so nothing can be built from it — and the
/// request must die here rather than reach the profile in some guessed shape.
#[tokio::test]
async fn a_request_the_translator_refuses_is_a_marked_502_that_sends_nothing() {
    let relay = start_openai("notify-only", false).await;

    let body = format!(
        r#"{{"model":"{OPEN_MODEL}","max_tokens":64,"messages":[{{"role":"function","content":"x"}}]}}"#
    );
    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(body)
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    let body: Value = serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
        .expect("error body must be JSON");
    assert_eq!(body["error"], "fallback_request_untranslatable");
    assert_eq!(
        relay.fallback.count(),
        0,
        "a request that could not be translated must not be sent in some other shape"
    );
    assert_eq!(relay.anthropic.count(), 0, "nor sent to Anthropic instead");
}

/// The fallback route's own `upstream_unreachable`, which is a different site
/// from the Anthropic route's namesake (`tests/proxy.rs`) and answers 502 where
/// that one answers 502 too — but this one has to carry the marker.
#[tokio::test]
async fn a_profile_nothing_is_listening_on_is_a_marked_502() {
    set_profile_keys();
    let anthropic = Recorder::default();
    let anthropic_addr = serve(anthropic_upstream(anthropic.clone(), false)).await;
    let unreachable = closed_port().await;
    let relay = serve_relay_with(
        config(
            anthropic_addr,
            "notify-only",
            profile(unreachable, "openai", OPENAI_KEY_ENV),
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

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    let body: Value = serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
        .expect("error body must be JSON");
    assert_eq!(body["error"], "upstream_unreachable");
    assert_eq!(
        status(relay).await["fallback_requests_served"],
        0,
        "a request that never arrived was not served"
    );
    assert_eq!(anthropic.count(), 0);
}

/// The streaming twin of `a_translated_response_keeps_the_upstreams_status`.
/// A synthesized SSE response still carries the provider's own 2xx: a 206 must
/// not silently become a 200. The event names are asserted alongside it so this
/// is a status assertion about the *translated stream* path rather than about
/// some error path that happened to answer 206.
#[tokio::test]
async fn a_translated_stream_keeps_the_upstreams_2xx_status() {
    let relay = start(
        "notify-only",
        false,
        |recorder| {
            Router::new().route(
                "/v1/chat/completions",
                any(move |request: Request| {
                    let recorder = recorder.clone();
                    async move {
                        record(&recorder, request).await;
                        Response::builder()
                            .status(StatusCode::PARTIAL_CONTENT)
                            .header("content-type", "text/event-stream")
                            .body(Body::from(OPENAI_CHUNKS.concat()))
                            .expect("failed to build mock response")
                    }
                }),
            )
        },
        "openai",
        OPENAI_KEY_ENV,
    )
    .await;

    let body = format!(
        r#"{{"model":"{OPEN_MODEL}","max_tokens":64,"stream":true,"messages":[{{"role":"user","content":"hi"}}]}}"#
    );
    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(body)
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    assert_eq!(response.headers()["content-type"], "text/event-stream");

    let bytes = response.bytes().await.expect("failed to read body");
    let names: Vec<String> = events(&bytes).into_iter().map(|(name, _)| name).collect();
    assert_eq!(
        names,
        vec![
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
        ],
        "the 206 above must be the status of a translated stream, not of a failure"
    );
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
    // Spec §7d: the provider's status and message surface, in Anthropic's
    // envelope. This assertion used to pin byte-identity with `LIMIT_BODY`; that
    // encoded the old verbatim-passthrough rule rather than anything this test
    // is about, which is that a fallback's error moves no Anthropic route state.
    let body: Value = serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
        .expect("error body must be JSON");
    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "rate_limit_error");
    assert_eq!(
        body["error"]["message"],
        "You have reached your Claude Pro usage limit. Your limit will reset at 6pm.",
        "the provider's own message is what the client reads"
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

// --- The request that trips the limit (spec §6, `policy.failover_on_detect`) ---

/// A cold relay — `ACTIVE`, no priming request — whose Anthropic mock answers
/// `/v1/messages` with the subscription limit. The request under test is
/// therefore the one that *causes* the transition, which is the case Claude Code
/// treats as terminal: it does not retry, so a 429 here is a hard failure the
/// user has to notice and re-run.
async fn start_cold_limited(mode: &str, failover_on_detect: bool) -> Relay {
    set_profile_keys();
    let anthropic = Recorder::default();
    let fallback = Recorder::default();
    let anthropic_addr = serve(anthropic_upstream(anthropic.clone(), true)).await;
    let fallback_addr = serve(openai_upstream(fallback.clone(), false)).await;
    let mut config = config(
        anthropic_addr,
        mode,
        profile(fallback_addr, "openai", OPENAI_KEY_ENV),
    );
    config.policy.failover_on_detect = failover_on_detect;
    let addr = serve_relay_with(config, None).await;
    Relay {
        addr,
        anthropic,
        fallback,
    }
}

/// The default behavior, and the reason this exists: the limit is classified
/// before anything has been sent to the client, so that request is handed to the
/// fallback instead of being answered with an error the client will not retry.
/// Nothing about mid-stream failover is involved — the decision is made while
/// the response is still entirely in the relay's hands.
#[tokio::test]
async fn the_request_that_trips_the_limit_is_handed_to_the_fallback() {
    let relay = start_cold_limited("new-sessions", true).await;
    assert_eq!(
        status(relay.addr).await["state"],
        "ACTIVE",
        "this test is about the request that trips the limit, not a later one"
    );

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
    assert_eq!(body["content"][0]["text"], "from the openai profile");

    assert_eq!(
        relay.anthropic.count(),
        1,
        "the attempt on Anthropic is what produced the limit response"
    );
    let seen = relay.fallback.only();
    assert_eq!(seen.path, "/v1/chat/completions");
    assert_eq!(
        seen.json()["model"],
        OPUS_TARGET,
        "a failed-over request is remapped (§7a), on this path too"
    );
    assert_eq!(
        seen.header("authorization"),
        Some(&format!("Bearer {OPENAI_KEY}")[..]),
        "the profile's own key, not the client's"
    );

    // The transition still happens: later requests fail over without another
    // Anthropic round trip.
    let status = wait_for_limited(relay.addr).await;
    assert_eq!(status["fallback_requests_served"], 1);
}

/// `failover_on_detect = false` restores the older behavior exactly: the client
/// gets Anthropic's own limit error, and the route still transitions so later
/// requests fail over.
#[tokio::test]
async fn failover_on_detect_off_returns_the_limit_error_and_still_limits_the_route() {
    let relay = start_cold_limited("new-sessions", false).await;

    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(session_start(OPUS))
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

    let status = wait_for_limited(relay.addr).await;
    assert_eq!(status["fallback_requests_served"], 0);
}

/// `notify-only` exists to *not* switch models. Getting this wrong would defeat
/// the whole mode, so it is asserted on the detect-time path as well as on the
/// already-`LIMITED` one.
#[tokio::test]
async fn notify_only_never_fails_over_the_request_that_trips_the_limit() {
    let relay = start_cold_limited("notify-only", true).await;

    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(session_start(OPUS))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(response.headers().get("x-relay-route").is_none());
    assert_eq!(
        response.bytes().await.expect("failed to read body"),
        LIMIT_BODY.as_bytes()
    );
    assert_eq!(relay.fallback.count(), 0);
    // The notification the mode is named for still fires, which is the same
    // thing as the route transitioning.
    wait_for_limited(relay.addr).await;
}

/// `new-sessions`' session-start heuristic applies to the triggering request
/// too: a conversation already in flight fails visibly rather than switching
/// models mid-thought, exactly as it does once the route is `LIMITED`.
#[tokio::test]
async fn a_mid_conversation_request_that_trips_the_limit_is_not_failed_over() {
    let relay = start_cold_limited("new-sessions", true).await;

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
        LIMIT_BODY.as_bytes()
    );
    assert_eq!(relay.fallback.count(), 0);
    wait_for_limited(relay.addr).await;

    // ...and the *next* session start does fail over, so the mode is being
    // applied per request rather than the re-route being off altogether.
    let next = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(session_start(OPUS))
        .send()
        .await
        .expect("request failed");
    assert_eq!(next.headers()["x-relay-route"], "fallback");
    next.bytes().await.expect("failed to read body");
}

/// With no `active_profile` there is nothing to hand the request to, so the
/// limit error passes through — and the route still transitions, which is all
/// the relay can do for this one.
#[tokio::test]
async fn without_an_active_profile_the_triggering_limit_passes_through() {
    set_profile_keys();
    let anthropic = Recorder::default();
    let fallback = Recorder::default();
    let anthropic_addr = serve(anthropic_upstream(anthropic.clone(), true)).await;
    let fallback_addr = serve(openai_upstream(fallback.clone(), false)).await;
    let mut config = config(
        anthropic_addr,
        "all",
        profile(fallback_addr, "openai", OPENAI_KEY_ENV),
    );
    config.policy.active_profile = None;
    let relay = serve_relay_with(config, None).await;

    let response = client()
        .post(format!("http://{relay}/v1/messages"))
        .body(session_start(OPUS))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        response.bytes().await.expect("failed to read body"),
        LIMIT_BODY.as_bytes()
    );
    assert_eq!(fallback.count(), 0);
    wait_for_limited(relay).await;
}

/// Global Constraint 7 on the new path. A count that trips the limit is still
/// Anthropic's to answer: routing it to an `openai` profile would bill an
/// inference call and answer a count with a message.
#[tokio::test]
async fn a_count_tokens_that_trips_the_limit_stays_on_anthropic() {
    set_profile_keys();
    let anthropic = Recorder::default();
    let fallback = Recorder::default();
    let counts = anthropic.clone();
    let anthropic_addr = serve(
        Router::new()
            .route(
                "/v1/messages/count_tokens",
                any(move |request: Request| {
                    let recorder = counts.clone();
                    async move {
                        record(&recorder, request).await;
                        limit_response()
                    }
                }),
            )
            .route("/v1/messages", any(|| async { limit_response() })),
    )
    .await;
    let fallback_addr = serve(openai_upstream(fallback.clone(), false)).await;
    // `all` — the most permissive mode there is, so nothing but the
    // `count_tokens` pin can be what keeps this request on Anthropic.
    let relay = serve_relay_with(
        config(
            anthropic_addr,
            "all",
            profile(fallback_addr, "openai", OPENAI_KEY_ENV),
        ),
        None,
    )
    .await;

    let response = client()
        .post(format!("http://{relay}/v1/messages/count_tokens"))
        .body(session_start(OPUS))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(response.headers().get("x-relay-route").is_none());
    assert_eq!(
        response.bytes().await.expect("failed to read body"),
        LIMIT_BODY.as_bytes(),
        "spec §6: on failure a count passes the error through"
    );
    assert_eq!(fallback.count(), 0);
    assert_eq!(anthropic.count(), 1);
    // Detection itself is unaffected by the pin: the limit is still recorded.
    wait_for_limited(relay).await;
}

/// Spec §5's conservative rule, which the buffering must not disturb: a
/// per-minute burst 429 is not the subscription limit, so it reaches the client
/// with its status, headers and body intact and changes no state — even though
/// it is a `detect.status` response on a request that was eligible to fail over.
#[tokio::test]
async fn a_burst_429_on_an_eligible_request_reaches_the_client_unchanged() {
    set_profile_keys();
    let fallback = Recorder::default();
    const BURST_BODY: &str = r#"{"type":"error","error":{"type":"rate_limit_error","message":"Number of requests has exceeded your per-minute rate limit."}}"#;
    let anthropic_addr = serve(Router::new().route(
        "/v1/messages",
        any(|| async {
            Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .header("content-type", "application/json")
                .header("retry-after", "12")
                .header("anthropic-ratelimit-requests-remaining", "0")
                .body(Body::from(BURST_BODY))
                .expect("failed to build mock response")
        }),
    ))
    .await;
    let fallback_addr = serve(openai_upstream(fallback.clone(), false)).await;
    let relay = serve_relay_with(
        config(
            anthropic_addr,
            "all",
            profile(fallback_addr, "openai", OPENAI_KEY_ENV),
        ),
        None,
    )
    .await;

    let response = client()
        .post(format!("http://{relay}/v1/messages"))
        .body(session_start(OPUS))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["retry-after"], "12");
    assert_eq!(
        response.headers()["anthropic-ratelimit-requests-remaining"],
        "0"
    );
    assert!(response.headers().get("x-relay-route").is_none());
    assert_eq!(
        response.bytes().await.expect("failed to read body"),
        BURST_BODY.as_bytes(),
        "a burst 429 is passed through byte for byte"
    );
    assert_eq!(fallback.count(), 0, "a burst 429 is not a failover trigger");

    // Long enough for the applier to have run had anything been recorded.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(status(relay).await["state"], "ACTIVE");
}

/// A candidate response whose body dies mid-flight classifies nothing — a
/// partial document is not evidence — and must never reach the client looking
/// complete. It fails, as it would have without the buffering; *where* it fails
/// is the one thing buffering moves. The read now finishes before the head is
/// sent, so hyper can abort the connection while the client is still parsing the
/// head instead of after it has a chunk in hand, and both shapes are the same
/// answer: no complete body.
#[tokio::test]
async fn a_candidate_body_that_dies_mid_flight_is_not_classified_and_still_fails() {
    set_profile_keys();
    let fallback = Recorder::default();
    let anthropic_addr = serve(Router::new().route(
        "/v1/messages",
        any(|| async {
            Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .header("content-type", "application/json")
                .body(truncated_body(r#"{"type":"error","error":{"type":"rat"#))
                .expect("failed to build mock response")
        }),
    ))
    .await;
    let fallback_addr = serve(openai_upstream(fallback.clone(), false)).await;
    let relay = serve_relay_with(
        config(
            anthropic_addr,
            "all",
            profile(fallback_addr, "openai", OPENAI_KEY_ENV),
        ),
        None,
    )
    .await;

    let read_whole = async {
        let mut response = client()
            .post(format!("http://{relay}/v1/messages"))
            .body(session_start(OPUS))
            .send()
            .await?;
        let status = response.status();
        let mut collected = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            collected.extend_from_slice(&chunk);
        }
        Ok::<(StatusCode, Vec<u8>), reqwest::Error>((status, collected))
    };
    let outcome = tokio::time::timeout(Duration::from_secs(5), read_whole)
        .await
        .expect("the client hung after the upstream died");
    assert!(
        outcome.is_err(),
        "a body that died mid-flight must never arrive as a complete response: {outcome:?}"
    );

    assert_eq!(fallback.count(), 0);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        status(relay).await["state"],
        "ACTIVE",
        "a partial body is not evidence of a limit"
    );
}

/// The buffering is armed on this request — eligible policy, active profile —
/// and must still never touch a success. A 200 SSE stream keeps its pacing:
/// buffering it would hold every byte until generation finished.
#[tokio::test]
async fn an_eligible_request_still_streams_a_successful_response() {
    set_profile_keys();
    let chunk_delay = Duration::from_millis(300);
    let anthropic_addr = serve(Router::new().route(
        "/v1/messages",
        any(move || async move {
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(dripped_body(
                    vec!["event: a\n", "event: b\n", "event: c\n"],
                    chunk_delay,
                ))
                .expect("failed to build mock response")
        }),
    ))
    .await;
    let fallback_addr = serve(openai_upstream(Recorder::default(), false)).await;
    let relay = serve_relay_with(
        config(
            anthropic_addr,
            "all",
            profile(fallback_addr, "openai", OPENAI_KEY_ENV),
        ),
        None,
    )
    .await;

    let start = Instant::now();
    let mut response = client()
        .post(format!("http://{relay}/v1/messages"))
        .body(session_start(OPUS))
        .send()
        .await
        .expect("request failed");

    let mut time_to_first_chunk = None;
    let mut collected = Vec::new();
    while let Some(chunk) = response.chunk().await.expect("failed to read chunk") {
        time_to_first_chunk.get_or_insert_with(|| start.elapsed());
        collected.extend_from_slice(&chunk);
    }

    assert_eq!(collected, b"event: a\nevent: b\nevent: c\n");
    let first = time_to_first_chunk.expect("stream produced no chunks");
    assert!(
        first < chunk_delay,
        "first chunk took {first:?}, so a successful response was buffered"
    );
}

/// The production shape, not an edge case: Claude Code's client always asks for
/// compression, so the limit response that trips this path arrives gzipped. The
/// re-route decision has to survive that — classification decompresses its own
/// copy, and the buffered body is never inspected any other way.
#[tokio::test]
async fn a_gzipped_limit_response_still_hands_the_request_to_the_fallback() {
    use std::io::Write;

    set_profile_keys();
    let fallback = Recorder::default();
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(LIMIT_BODY.as_bytes())
        .expect("gzip write failed");
    let gzipped = encoder.finish().expect("gzip finish failed");

    let anthropic_addr = serve(Router::new().route(
        "/v1/messages",
        any(move || {
            let gzipped = gzipped.clone();
            async move {
                Response::builder()
                    .status(StatusCode::TOO_MANY_REQUESTS)
                    .header("content-type", "application/json")
                    .header("content-encoding", "gzip")
                    .header("retry-after", "3600")
                    .body(Body::from(gzipped))
                    .expect("failed to build mock response")
            }
        }),
    ))
    .await;
    let fallback_addr = serve(openai_upstream(fallback.clone(), false)).await;
    let relay = serve_relay_with(
        config(
            anthropic_addr,
            "new-sessions",
            profile(fallback_addr, "openai", OPENAI_KEY_ENV),
        ),
        None,
    )
    .await;

    let response = authenticated(format!("http://{relay}/v1/messages"), session_start(OPUS))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    response.bytes().await.expect("failed to read body");
    assert_eq!(fallback.only().json()["model"], OPUS_TARGET);
    wait_for_limited(relay).await;
}

// --- Provider errors in Anthropic's envelope (spec §7d, Task 9B) ---
//
// Claude Code detects a context overflow by lowercased substring match on the
// error message and extracts the two numbers with a regex — so a provider whose
// wording differs gets none of the client's recovery, and the session is
// unrecoverable in place. What the relay emits is therefore Anthropic's wording,
// with the provider's own sentence kept after it (`docs/decisions.md`).

/// Measured through the running Together AI service at 170,071 tokens against a
/// 131k model (the Task 9B brief's capture). Deliberately a literal here rather
/// than a file under `tests/fixtures/together/`: that directory's README is a
/// ledger of two dated capture sessions this test was not part of.
const TOGETHER_CONTEXT_LIMIT: &str = concat!(
    r#"{"id":"ovq5abc-1kFHot-a29afb844e986e7d","error":{"message":"The input (170071 tokens) "#,
    r#"is longer than the model's context length (131072 tokens).","#,
    r#""type":"invalid_request_error","param":null,"code":null}}"#
);

/// Claude Code 2.1.220's two too-long predicates, read out of the installed
/// binary. Both are `.includes()` on the lowercased message, so any one of them
/// firing is enough for the client to start recovering.
const TOO_LONG_PHRASES: [&str; 3] = [
    "prompt is too long",
    "input is too long for requested model",
    "input length and `max_tokens` exceed context limit",
];

/// The number-extraction regex, also from the binary — `M7r` in 2.1.220, whose
/// body is `e.match(/prompt is too long[^0-9]*(\d+)\s*tokens?\s*>\s*(\d+)/i)`
/// followed by `actualTokens: t[1]`, `limitTokens: t[2]`. Those two lines are the
/// evidence that `(tokens, limit)` is the right order and that the relay's
/// input-over-limit guard matches the real client rather than an assumption.
///
/// **The `/i` flag is part of it**, so the client is case-insensitive here; a
/// transcription without it would be stricter than reality and could fail on a
/// casing difference the client would accept. No regex crate is in this tree
/// (Global Constraint 3), so `token_pair` below is a hand transcription of exactly
/// this pattern, flag included; the source stays here so a reader can check the
/// transcription rather than take it on trust.
const TOKEN_PAIR_REGEX: &str = r"prompt is too long[^0-9]*(\d+)\s*tokens?\s*>\s*(\d+)/i";

/// `TOKEN_PAIR_REGEX` applied to `haystack`: the first match's two captures.
///
/// The pattern needs no backtracking to transcribe faithfully: `[^0-9]*` cannot
/// cross a digit, so the digit run it stops at is forced, and shortening the
/// greedy `(\d+)` can only leave a digit where `\s*tokens?` must match. `\s*` is
/// `trim_start`, a superset of the regex's whitespace class.
///
/// `/i` is honoured by matching against a lowercased copy. Only numbers come back
/// out, so nothing indexes into the original — which is what makes that safe, since
/// `to_lowercase` can change a string's length.
fn token_pair(haystack: &str) -> Option<(u64, u64)> {
    const PHRASE: &str = "prompt is too long";
    let lowered = haystack.to_lowercase();
    let mut rest = lowered.as_str();
    loop {
        let at = rest.find(PHRASE)?;
        if let Some(pair) = token_pair_after_phrase(&rest[at + PHRASE.len()..]) {
            return Some(pair);
        }
        rest = &rest[at + 1..];
    }
}

fn token_pair_after_phrase(tail: &str) -> Option<(u64, u64)> {
    // `[^0-9]*(\d+)`
    let (tokens, after) = leading_number(&tail[tail.find(|c: char| c.is_ascii_digit())?..])?;
    // `\s*tokens?\s*>\s*`
    let after = after.trim_start();
    let after = after
        .strip_prefix("tokens")
        .or_else(|| after.strip_prefix("token"))?;
    let after = after.trim_start().strip_prefix('>')?.trim_start();
    // `(\d+)`
    leading_number(after).map(|(limit, _)| (tokens, limit))
}

fn leading_number(text: &str) -> Option<(u64, &str)> {
    let len = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let value: u64 = text[..len].parse().ok()?;
    Some((value, &text[len..]))
}

/// A fallback profile whose only answer is one fixed error.
fn erroring_openai_upstream(
    status: StatusCode,
    body: &'static str,
    retry_after: Option<&'static str>,
) -> Router {
    Router::new().route(
        "/v1/chat/completions",
        any(move || async move {
            let mut response = json(status, body);
            if let Some(retry_after) = retry_after {
                response.headers_mut().insert(
                    "retry-after",
                    axum::http::HeaderValue::from_static(retry_after),
                );
            }
            response
        }),
    )
}

/// Name-routed (§7d) throughout, so Anthropic is never contacted and nothing but
/// the fallback's own answer can be what these tests observe.
async fn start_erroring(
    status: StatusCode,
    body: &'static str,
    retry_after: Option<&'static str>,
) -> SocketAddr {
    set_profile_keys();
    let anthropic_addr = serve(anthropic_upstream(Recorder::default(), false)).await;
    let fallback_addr = serve(erroring_openai_upstream(status, body, retry_after)).await;
    serve_relay_with(
        config(
            anthropic_addr,
            "notify-only",
            profile(fallback_addr, "openai", OPENAI_KEY_ENV),
        ),
        None,
    )
    .await
}

async fn provider_error(relay: SocketAddr) -> (StatusCode, HeaderMap, Value) {
    let response = client()
        .post(format!("http://{relay}/v1/messages"))
        .body(session_start(OPEN_MODEL))
        .send()
        .await
        .expect("request failed");
    let status = response.status();
    let headers = response.headers().clone();
    let body: Value = serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
        .expect("the error body must be JSON");
    (status, headers, body)
}

/// The 170k reproduction: what the client used to receive here matched none of
/// its three too-long phrases, so none of its recovery fired.
#[tokio::test]
async fn a_context_limit_error_reaches_the_client_in_anthropics_own_wording() {
    let relay = start_erroring(StatusCode::BAD_REQUEST, TOGETHER_CONTEXT_LIMIT, None).await;
    let (status, headers, body) = provider_error(relay).await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "the provider's own status");
    assert_eq!(headers["x-relay-route"], "fallback");
    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "invalid_request_error");

    let message = body["error"]["message"]
        .as_str()
        .expect("the envelope carries a message string");

    // The predicate: `.includes()` on the lowercased message.
    let lowered = message.to_lowercase();
    assert!(
        TOO_LONG_PHRASES
            .iter()
            .any(|phrase| lowered.contains(phrase)),
        "no too-long predicate fires on {message:?}"
    );
    // And the number extraction, so the client can shrink `max_tokens` instead of
    // compacting blind. Claude Code sends `max_tokens: 64000`, so on a 131k model
    // ~67k of this failure is the output reservation, not the transcript.
    assert_eq!(
        token_pair(message),
        Some((170071, 131072)),
        "{TOKEN_PAIR_REGEX} must match {message:?}"
    );
    // Debuggability: the provider's sentence is the only thing that reported the
    // real limit, and appending it after the pair is free.
    assert!(
        message.contains("The input (170071 tokens) is longer than the model's context length"),
        "the provider's own sentence must survive: {message:?}"
    );
}

/// A malformed request must not be reshaped into a too-long claim — the client
/// answers one of those by shrinking and retrying, forever. The body is
/// `tests/fixtures/together/H_error_missing_messages.json`'s, a real 400.
#[tokio::test]
async fn a_malformed_request_is_not_reshaped_into_a_too_long_claim() {
    const MISSING_MESSAGES: &str = r#"{"id":"ovq42ih-6Ng1vN-a29afb89ca1ab9f4","error":{"message":"Input required","type":"invalid_request_error","param":null,"code":null}}"#;
    let relay = start_erroring(StatusCode::BAD_REQUEST, MISSING_MESSAGES, None).await;
    let (status, _, body) = provider_error(relay).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert_eq!(body["error"]["message"], "Input required");
    let lowered = body["error"]["message"]
        .as_str()
        .expect("a message string")
        .to_lowercase();
    for phrase in TOO_LONG_PHRASES {
        assert!(
            !lowered.contains(phrase),
            "an ordinary 400 became a {phrase:?} claim: {body}"
        );
    }
    assert_eq!(token_pair(&lowered), None);
}

/// Spec §7d preserves the provider's status: the captures show 400, 401, 404 and
/// 422 all occur, and normalising them would tell the client the wrong thing
/// went wrong. Every one of them still carries the route marker, whose claim —
/// absence means Anthropic answered — is what makes it worth having.
#[tokio::test]
async fn a_provider_error_keeps_its_status_and_carries_the_route_marker() {
    // Bodies from the real captures, paired with Anthropic's type name for the
    // status each was observed on.
    const CASES: [(u16, &str, &str); 4] = [
        (
            400,
            r#"{"error":{"message":"Input required","type":"invalid_request_error"}}"#,
            "invalid_request_error",
        ),
        (
            401,
            r#"{"error":{"message":"Invalid API key provided.","type":"invalid_request_error","code":"invalid_api_key"}}"#,
            "authentication_error",
        ),
        (
            404,
            r#"{"error":{"message":"Unable to access model totally-fake-model.","type":"invalid_request_error","code":"model_not_available"}}"#,
            "not_found_error",
        ),
        (
            422,
            r#"{"error":{"message":"Input validation error","type":"invalid_request_error"}}"#,
            "invalid_request_error",
        ),
    ];

    for (code, provider_body, expected_type) in CASES {
        let status = StatusCode::from_u16(code).expect("a valid status");
        let relay = start_erroring(status, provider_body, None).await;
        let (seen, headers, body) = provider_error(relay).await;

        assert_eq!(seen, status, "{code} must not be normalised");
        assert_eq!(headers["x-relay-route"], "fallback", "{code}");
        assert_eq!(body["type"], "error", "{code}");
        assert_eq!(body["error"]["type"], expected_type, "{code}");
        assert!(
            body["error"]["message"].is_string(),
            "{code} lost its message: {body}"
        );
    }
}

/// The one upstream header the client acts on. Everything else is dropped: the
/// body is the relay's now, so the provider's `content-length` described bytes
/// that are no longer being sent.
#[tokio::test]
async fn a_provider_rate_limit_keeps_its_retry_after() {
    let relay = start_erroring(
        StatusCode::TOO_MANY_REQUESTS,
        r#"{"error":{"message":"Rate limit exceeded","type":"rate_limit_error"}}"#,
        Some("42"),
    )
    .await;
    let (status, headers, body) = provider_error(relay).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(headers["retry-after"], "42");
    assert_eq!(body["error"]["type"], "rate_limit_error");
    assert_eq!(headers["content-type"], "application/json");
}

/// An error body the relay cannot read is still the provider saying no. Emitting
/// the relay's own 502 in its place would report a different failure than the one
/// that happened.
#[tokio::test]
async fn an_unreadable_error_body_keeps_the_providers_status() {
    set_profile_keys();
    let anthropic_addr = serve(anthropic_upstream(Recorder::default(), false)).await;
    let fallback_addr = serve(Router::new().route(
        "/v1/chat/completions",
        any(|| async {
            Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header("content-type", "application/json")
                .body(truncated_body(r#"{"error":{"message":"Inp"#))
                .expect("failed to build mock response")
        }),
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

    let (status, headers, body) = provider_error(relay).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(headers["x-relay-route"], "fallback");
    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert_eq!(
        body["error"]["message"],
        "the fallback provider returned an error with no message"
    );
}

/// Blocker 3 of fix round 1: the provider's error body has two sinks, and the
/// client's is the one that lands in a session transcript. An authentication
/// failure is exactly the error most likely to quote the offending key back, so
/// the redaction has to cover the envelope and not only the log.
#[tokio::test]
async fn a_key_the_provider_quotes_back_never_reaches_the_client() {
    // The profile's own key, echoed the way a provider reporting a bad credential
    // plausibly would. `set_profile_keys` has already put `OPENAI_KEY` in the
    // environment, so this is the real configured value, not a stand-in.
    const ECHOED: &str = concat!(
        r#"{"error":{"message":"Invalid API key provided: "#,
        "together-key-for-the-openai-profile",
        r#". Check your key at https://api.together.ai/settings/api-keys.","#,
        r#""type":"invalid_request_error","code":"invalid_api_key"}}"#
    );
    // The literal above has to *be* the configured key for this test to mean
    // anything; `concat!` cannot interpolate a const, so it is asserted instead.
    assert!(
        ECHOED.contains(OPENAI_KEY),
        "the fixture must carry the configured key verbatim"
    );

    let relay = start_erroring(StatusCode::UNAUTHORIZED, ECHOED, None).await;
    let (status, headers, body) = provider_error(relay).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(headers["x-relay-route"], "fallback");
    assert_eq!(body["error"]["type"], "authentication_error");

    let whole = body.to_string();
    assert!(
        !whole.contains(OPENAI_KEY),
        "the profile's key reached the client: {whole}"
    );
    // Redacted, not merely dropped: the rest of the provider's sentence has to
    // survive or the client is told nothing useful about why it failed.
    let message = body["error"]["message"].as_str().expect("a message string");
    assert!(
        message.contains("[REDACTED]"),
        "the key must be redacted in place: {message}"
    );
    assert!(
        message.starts_with("Invalid API key provided:")
            && message.contains("api.together.ai/settings/api-keys"),
        "the provider's own sentence must survive around the redaction: {message}"
    );
}

/// Blocker 5 of fix round 1: the flat top-level-`message` shape, which several
/// OpenAI-compatible servers use. Reading only `error.message` lost the sentence
/// entirely, and that was a *regression* — under the verbatim pass-through this
/// replaced, the body reached the client's SDK, which prefers a top-level
/// `message`, so the user saw the real reason. Here it also has to reach detection,
/// because the sentence is a context-limit sentence.
#[tokio::test]
async fn a_flat_top_level_message_still_reaches_the_client_and_detection() {
    const FLAT: &str = concat!(
        r#"{"object":"error","message":"This model's maximum context length is 131072 tokens. "#,
        r#"However, you requested 170071 tokens (39071 in the messages, 64000 in the completion)."#,
        r#"","type":"BadRequestError","param":null,"code":400}"#
    );
    let relay = start_erroring(StatusCode::BAD_REQUEST, FLAT, None).await;
    let (status, headers, body) = provider_error(relay).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(headers["x-relay-route"], "fallback");
    let message = body["error"]["message"].as_str().expect("a message string");

    assert!(
        message.contains("This model's maximum context length is 131072 tokens"),
        "the provider's own sentence must survive: {message:?}"
    );
    // Detection fires, so the client can act at all.
    let lowered = message.to_lowercase();
    assert!(
        TOO_LONG_PHRASES
            .iter()
            .any(|phrase| lowered.contains(phrase)),
        "no too-long predicate fires on {message:?}"
    );
    // But no pair: this wording puts the limit before the input, so the anchored
    // parse refuses rather than reporting them backwards. The client compacts
    // blind, which is the safe half of the recovery.
    assert_eq!(
        token_pair(message),
        None,
        "a limit-before-input wording must not yield a reversed pair: {message:?}"
    );
}

/// The error path buffers the provider's body, so the cap on it is a choice worth
/// pinning rather than inheriting. It is `ERROR_BODY_CAP` (1 MiB) — the bound this
/// repo already argued for exactly this hazard on the Anthropic route — not
/// `RESPONSE_CAP` (4 MiB), which is about a non-streaming 2xx that has to be whole
/// before it can be translated.
#[tokio::test]
async fn an_error_body_between_the_two_caps_is_refused_but_keeps_the_status() {
    // Between 1 MiB and 4 MiB: read under the error cap, accepted under the
    // response cap. A `Box::leak` because the mock's body has to be `'static`.
    let oversized: &'static str = Box::leak(
        format!(
            r#"{{"error":{{"message":"{}","type":"invalid_request_error"}}}}"#,
            "z".repeat(2 * 1024 * 1024)
        )
        .into_boxed_str(),
    );
    assert!(
        oversized.len() > 1024 * 1024 && oversized.len() < 4 * 1024 * 1024,
        "the body has to sit between the two caps for this test to distinguish them"
    );

    let relay = start_erroring(StatusCode::BAD_REQUEST, oversized, None).await;
    let (status, headers, body) = provider_error(relay).await;

    // Refused, but the provider's status is still the honest answer.
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(headers["x-relay-route"], "fallback");
    assert_eq!(
        body["error"]["message"], "the fallback provider returned an error with no message",
        "a body past the cap is not read at all"
    );
}

// --- the escalation ladder (spec §7e) -----------------------------------------
//
// Claude Code decides a session's context window client-side, from the
// `claude-*` name it selected, so it overshoots a smaller fallback model
// believing it has room. 9B gives it the wording its own recovery keys on; this
// is the case where that recovery cannot help, because the transcript alone does
// not fit — and a larger model is already configured one slot up.
//
// Every hop is a whole extra upstream request the operator pays for, so most of
// what these tests hold is that the ladder is *not* walked.

/// The live map's three windows: 131k, 262k, 1M.
const SMALL: &str = "openai/gpt-oss-20b";
const MEDIUM: &str = "moonshotai/Kimi-K2.7-Code";
const LARGE: &str = "moonshotai/Kimi-K3";

const HAIKU: &str = "claude-haiku-4-5";
const SONNET: &str = "claude-sonnet-4-6";
const FABLE: &str = "claude-fable-5";

/// The live map's shape, duplicate included: `claude-fable` and `claude-opus`
/// both point at the largest model configured, which is the trap a naive walk
/// falls into — an identical request re-sent to an identical model at K3 prices.
const LIVE_LADDER: [(&str, &str); 5] = [
    ("claude-haiku", SMALL),
    ("claude-sonnet", MEDIUM),
    ("claude-opus", LARGE),
    ("claude-fable", LARGE),
    ("*", LARGE),
];

/// The default order does not name `claude-fable`, so the live map's fourth slot
/// needs this to be a rung at all — which is also what makes the duplicate
/// reachable and therefore testable.
const LIVE_ORDER: [&str; 4] = [
    "claude-haiku",
    "claude-sonnet",
    "claude-opus",
    "claude-fable",
];

/// A context-limit body with no readable pair. Detection fires (the wording is
/// there), the parse refuses (no numbers), and escalation must not care: a prompt
/// that did not fit needs a bigger model whether or not it could be sized.
const CONTEXT_LIMIT_NO_PAIR: &str = r#"{"error":{"message":"The input is longer than the model's context length.","type":"invalid_request_error","param":null,"code":null}}"#;

/// A real 400 that is *not* a context limit
/// (`tests/fixtures/together/H_error_missing_messages.json`).
const MISSING_MESSAGES: &str = r#"{"id":"ovq42ih-6Ng1vN-a29afb89ca1ab9f4","error":{"message":"Input required","type":"invalid_request_error","param":null,"code":null}}"#;

/// A fallback profile whose answer depends on the model asked for: `error` for
/// every model in `overflows`, an ordinary completion for anything else. That is
/// the real failure's shape — the model whose window the transcript does not fit
/// rejects it, and a larger one answers.
fn laddered_upstream(
    recorder: Recorder,
    overflows: &'static [&'static str],
    error: &'static str,
) -> Router {
    Router::new().route(
        "/v1/chat/completions",
        any(move |request: Request| {
            let recorder = recorder.clone();
            async move {
                let body = record(&recorder, request).await;
                let model = body["model"].as_str().unwrap_or_default().to_string();
                if overflows.contains(&model.as_str()) {
                    // A status, and only then a body — which is why escalation is
                    // possible here at all. Together answers a streaming request
                    // that overflows exactly this way.
                    return json(StatusCode::BAD_REQUEST, error);
                }
                if body["stream"] != Value::Bool(true) {
                    return json(StatusCode::OK, OPENAI_COMPLETION);
                }
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from(OPENAI_CHUNKS.concat()))
                    .expect("failed to build mock response")
            }
        }),
    )
}

fn laddered_profile(base: SocketAddr, model_map: &[(&str, &str)]) -> ProfileConfig {
    ProfileConfig {
        model_map: model_map
            .iter()
            .map(|(slot, target)| ((*slot).to_string(), (*target).to_string()))
            .collect(),
        ..profile(base, "openai", OPENAI_KEY_ENV)
    }
}

/// A relay already `LIMITED`, in `all` mode, so a `claude-*` request fails over
/// and is *remapped* (§7a) — which is the only way a request has a ladder
/// position at all.
async fn start_laddered(
    model_map: &[(&str, &str)],
    overflows: &'static [&'static str],
    error: &'static str,
    tune: impl FnOnce(&mut Config),
) -> Relay {
    start_laddered_with(
        model_map,
        |recorder| laddered_upstream(recorder, overflows, error),
        tune,
    )
    .await
}

/// Same, for a fallback mock whose answers are not "overflow or succeed" — the
/// cases where the *rung above* fails in its own way.
async fn start_laddered_with(
    model_map: &[(&str, &str)],
    fallback_router: impl FnOnce(Recorder) -> Router,
    tune: impl FnOnce(&mut Config),
) -> Relay {
    set_profile_keys();
    let anthropic = Recorder::default();
    let fallback = Recorder::default();
    let anthropic_addr = serve(anthropic_upstream(anthropic.clone(), false)).await;
    let fallback_addr = serve(fallback_router(fallback.clone())).await;
    let mut config = config(
        anthropic_addr,
        "all",
        laddered_profile(fallback_addr, model_map),
    );
    tune(&mut config);
    let addr = serve_relay_with(config, None).await;
    drive_to_limited(addr).await;
    Relay {
        addr,
        anthropic,
        fallback,
    }
}

/// The bottom rung overflows; every rung above it answers `above` with
/// `above_body`, so a test can ask what one hop's own failure does to the answer
/// the client ends up with.
fn overflow_then_upstream(
    recorder: Recorder,
    overflows: &'static [&'static str],
    above: StatusCode,
    above_body: &'static str,
) -> Router {
    Router::new().route(
        "/v1/chat/completions",
        any(move |request: Request| {
            let recorder = recorder.clone();
            async move {
                let body = record(&recorder, request).await;
                let model = body["model"].as_str().unwrap_or_default().to_string();
                if overflows.contains(&model.as_str()) {
                    json(StatusCode::BAD_REQUEST, TOGETHER_CONTEXT_LIMIT)
                } else {
                    json(above, above_body)
                }
            }
        }),
    )
}

fn order(slots: &[&str]) -> Vec<String> {
    slots.iter().map(|slot| (*slot).to_string()).collect()
}

/// Every model the fallback profile was asked for, in the order it was asked —
/// so a test can assert the whole walk rather than only its length.
fn models_seen(recorder: &Recorder) -> Vec<String> {
    recorder
        .0
        .lock()
        .expect("recorder poisoned")
        .iter()
        .map(|recorded| {
            recorded.json()["model"]
                .as_str()
                .expect("every outgoing body names a model")
                .to_string()
        })
        .collect()
}

/// Every body the fallback profile received, in order. `Recorder::only` covers
/// the one-request case; an escalated request makes two, and which of them
/// carried what is the whole question.
fn bodies_seen(recorder: &Recorder) -> Vec<Value> {
    recorder
        .0
        .lock()
        .expect("recorder poisoned")
        .iter()
        .map(Recorded::json)
        .collect()
}

async fn ask(relay: &Relay, model: &str) -> (StatusCode, Value) {
    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(session_start(model))
        .send()
        .await
        .expect("request failed");
    let code = response.status();
    let body: Value = serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
        .expect("the body must be JSON");
    (code, body)
}

/// The 170k reproduction, with somewhere to go: the request lands on the haiku
/// slot's 131k model, overflows, and is retried on the sonnet slot's 262k one,
/// which answers. The client never sees an error at all.
#[tokio::test]
async fn a_context_limit_climbs_one_rung_and_succeeds_there() {
    let relay = start_laddered(&LIVE_LADDER, &[SMALL], TOGETHER_CONTEXT_LIMIT, |_| {}).await;

    let (code, body) = ask(&relay, HAIKU).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(models_seen(&relay.fallback), [SMALL, MEDIUM]);
    assert_eq!(body["content"][0]["text"], "from the openai profile");
    // Both attempts are counted, because the operator paid for both.
    assert_eq!(status(relay.addr).await["fallback_requests_served"], 2);
}

/// Spec §Testing 4, and the only test that closes the mutation this feature is
/// most exposed to. A hop is the one case where a single client request reaches
/// two *different* upstream models, so it is the only place where resolving
/// `params` once per client request rather than once per attempt is visible:
/// hoisting the lookup out of `prepare()` into `deliver()` compiles and passes
/// every other test in this suite, including the name-routed params test, which
/// takes no ladder and therefore calls `prepare()` exactly once.
///
/// Both rungs are tuned, with disjoint keys, so a set that leaks is caught
/// whichever way it travels — not only the bottom rung's tuning following the
/// request upward.
#[tokio::test]
async fn params_are_resolved_per_escalation_hop() {
    let relay = start_laddered(&LIVE_LADDER, &[SMALL], TOGETHER_CONTEXT_LIMIT, |config| {
        config
            .profiles
            .get_mut("fallback")
            .expect("the fallback profile")
            .params = IndexMap::from([
            (
                SMALL.to_string(),
                IndexMap::from([("reasoning_effort".to_string(), Value::from("low"))]),
            ),
            (
                MEDIUM.to_string(),
                IndexMap::from([("seed".to_string(), Value::from(7))]),
            ),
        ]);
    })
    .await;

    let (code, body) = ask(&relay, HAIKU).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    // The walk first: without two hops there is nothing for the rest to be about.
    assert_eq!(models_seen(&relay.fallback), [SMALL, MEDIUM]);

    let seen = bodies_seen(&relay.fallback);
    assert_eq!(
        seen[0]["reasoning_effort"], "low",
        "the rung the request landed on carries its own tuning"
    );
    assert!(
        seen[0].get("seed").is_none(),
        "the rung above's tuning must not reach the rung below: {}",
        seen[0]
    );
    assert_eq!(
        seen[1]["seed"], 7,
        "the hop resolved params for the model it actually asked for, not the one it started on"
    );
    assert!(
        seen[1].get("reasoning_effort").is_none(),
        "the rung below's tuning must not follow the request up the ladder: {}",
        seen[1]
    );
}

/// **The knowing cost of fix round 1's blocker 1, as behaviour.** This body is a
/// genuine input overflow with no numbers in it at all, so nothing distinguishes it
/// from a `max_tokens` reservation overflow — and a reservation overflow that
/// escalates buys a billed inference the client would have fixed for free. So it
/// does not climb.
///
/// **This test asserted the opposite one round ago, and the rule changed under it,
/// not the other way around.** The addendum I built from said escalation should care
/// only that detection fired; the reviewer measured what that costs and the lead
/// retracted it. Recording the inversion rather than quietly deleting the test,
/// because the case is still worth pinning: the client keeps 9B's recovery, so what
/// the caution costs is a compaction rather than a hop — not the session.
#[tokio::test]
async fn a_context_limit_with_no_readable_pair_does_not_climb() {
    let relay = start_laddered(&LIVE_LADDER, &[SMALL], CONTEXT_LIMIT_NO_PAIR, |_| {}).await;

    let (code, body) = ask(&relay, HAIKU).await;
    assert_eq!(
        models_seen(&relay.fallback),
        [SMALL],
        "an unpairable overflow is indistinguishable from a reservation overflow"
    );
    assert_eq!(code, StatusCode::BAD_REQUEST);
    let message = body["error"]["message"].as_str().expect("a message string");
    assert!(
        TOO_LONG_PHRASES
            .iter()
            .any(|phrase| message.to_lowercase().contains(phrase)),
        "the client keeps the recovery escalation declined to pay for: {message:?}"
    );
}

/// The walk starts above the rung the request landed on, not at the bottom of the
/// ladder. A ladder walked from the bottom would answer a 262k overflow by trying
/// the 131k model — a hop *down*, guaranteed to fail, paid for.
#[tokio::test]
async fn the_walk_starts_above_the_rung_the_request_landed_on() {
    let relay = start_laddered(&LIVE_LADDER, &[MEDIUM], TOGETHER_CONTEXT_LIMIT, |_| {}).await;

    let (code, body) = ask(&relay, SONNET).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(
        models_seen(&relay.fallback),
        [MEDIUM, LARGE],
        "the smaller model below this request's own rung must never be tried"
    );
}

/// **A `max_tokens` reservation overflow must not walk the ladder** (fix round 1,
/// blocker 1). Measured against a vLLM-shaped body: the transcript is 35k and fits
/// the 131k model comfortably — only the 160k *output reservation* does not. The
/// client fixes that for free by shrinking `max_tokens` on the translated error, so
/// escalating pre-empts the free fix and buys a billed inference on a larger model,
/// which then succeeds and reserves 160k output tokens at that model's price.
///
/// Detection still fires, and the client still gets its own recovery — the caution
/// costs nothing the client was relying on.
#[tokio::test]
async fn a_max_tokens_reservation_overflow_never_walks_the_ladder() {
    const RESERVATION: &str = concat!(
        r#"{"object":"error","message":"This model's maximum context length is 131072 tokens. "#,
        r#"However, you requested 195000 tokens (35000 in the messages, 160000 in the "#,
        r#"completion).","type":"BadRequestError","param":null,"code":400}"#
    );
    let relay = start_laddered(&LIVE_LADDER, &[SMALL], RESERVATION, |_| {}).await;

    let (code, body) = ask(&relay, HAIKU).await;
    assert_eq!(
        models_seen(&relay.fallback),
        [SMALL],
        "a reservation overflow bought a billed inference on a bigger model"
    );
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert_eq!(status(relay.addr).await["fallback_requests_served"], 1);

    // The cheap recovery is intact: the phrase reaches the client, which is what
    // makes it shrink `max_tokens` without any second upstream request.
    let message = body["error"]["message"].as_str().expect("a message string");
    assert!(
        TOO_LONG_PHRASES
            .iter()
            .any(|phrase| message.to_lowercase().contains(phrase)),
        "the client lost the recovery escalation declined to pay for: {message:?}"
    );
    assert!(
        message.contains("160000 in the completion"),
        "the provider's own sentence must survive: {message:?}"
    );
}

/// **A hop the client would only retry keeps the terminal answer** (fix round 1,
/// blocker 2). Measured: with the rung above answering 429, the client got a 429 in
/// place of a terminal 400 it acts on once — so it retried with backoff, and every
/// retry re-walked the whole ladder (4 upstream requests over 2 client attempts,
/// unbounded in principle). The relay cannot bound that loop, because it is the
/// client's.
///
/// So the rung below's answer — a context-limit 400 the client acts on and does not
/// retry blindly — is what goes out. Both attempts still happened and are still
/// counted, because both were paid for.
#[tokio::test]
async fn a_hop_the_client_would_only_retry_keeps_the_terminal_answer() {
    const RETRYABLE: [(u16, &str); 2] = [
        (
            429,
            r#"{"error":{"message":"Rate limit exceeded","type":"rate_limit_error"}}"#,
        ),
        (
            503,
            r#"{"error":{"message":"Service temporarily unavailable","type":"server_error"}}"#,
        ),
    ];

    for (code, above_body) in RETRYABLE {
        let above = StatusCode::from_u16(code).expect("a valid status");
        let relay = start_laddered_with(
            &LIVE_LADDER,
            move |recorder| overflow_then_upstream(recorder, &[SMALL], above, above_body),
            |_| {},
        )
        .await;

        let (seen, body) = ask(&relay, HAIKU).await;
        assert_eq!(
            models_seen(&relay.fallback),
            [SMALL, MEDIUM],
            "{code}: the ladder is still walked once and only once"
        );
        assert_eq!(
            seen,
            StatusCode::BAD_REQUEST,
            "{code}: a status the client retries replaced a terminal one it acts on: {body}"
        );
        assert!(
            body["error"]["message"]
                .as_str()
                .expect("a message string")
                .starts_with("prompt is too long: 170071 tokens > 131072"),
            "{code}: {body}"
        );
        assert_eq!(
            status(relay.addr).await["fallback_requests_served"],
            2,
            "{code}: both attempts were paid for, so both are counted"
        );
    }
}

/// The other half of that rule, and the reason "escalation can never make the
/// answer worse" is **not** the invariant: a hop that fails *terminally* keeps its
/// own answer, even when it is less useful than the rung below's.
///
/// A `model_map` rung pointing at a retired or mistyped model is the real case. The
/// honest answer is that the model is not there — it cannot amplify (no client
/// retries a 404), and masking it behind "prompt is too long" would hide the
/// misconfiguration while the operator paid for the hop.
#[tokio::test]
async fn a_hop_that_fails_terminally_reports_its_own_failure() {
    const RETIRED: &str = r#"{"error":{"message":"Unable to access model moonshotai/Kimi-K2.7-Code.","type":"invalid_request_error","code":"model_not_available"}}"#;
    let relay = start_laddered_with(
        &LIVE_LADDER,
        |recorder| overflow_then_upstream(recorder, &[SMALL], StatusCode::NOT_FOUND, RETIRED),
        |_| {},
    )
    .await;

    let (code, body) = ask(&relay, HAIKU).await;
    assert_eq!(models_seen(&relay.fallback), [SMALL, MEDIUM]);
    assert_eq!(code, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["type"], "not_found_error");
    assert!(
        body["error"]["message"]
            .as_str()
            .expect("a message string")
            .contains("Unable to access model"),
        "a misconfigured rung must be visible as itself: {body}"
    );
}

/// A malformed request must not walk the ladder. Answering it with a second,
/// larger, equally-malformed request costs money and cannot succeed.
#[tokio::test]
async fn an_ordinary_400_never_walks_the_ladder() {
    let relay = start_laddered(&LIVE_LADDER, &[SMALL], MISSING_MESSAGES, |_| {}).await;

    let (code, body) = ask(&relay, HAIKU).await;
    // The walk first, so a regression here names the defect rather than its
    // downstream symptom.
    assert_eq!(
        models_seen(&relay.fallback),
        [SMALL],
        "an ordinary 400 bought a second upstream request"
    );
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["message"], "Input required");
    assert_eq!(status(relay.addr).await["fallback_requests_served"], 1);
}

/// The whole walk, and its end. Every rung overflows, so the request climbs
/// haiku → sonnet → opus and then stops: the fourth rung (`claude-fable`)
/// resolves to the model that just failed, and re-sending an identical request to
/// an identical model buys a guaranteed identical failure at the top rung's
/// price. The client gets 9B's envelope, unchanged — escalation failing is not a
/// reason to lose the recovery it already had.
#[tokio::test]
async fn the_top_of_the_ladder_answers_with_the_translated_error() {
    let relay = start_laddered(
        &LIVE_LADDER,
        &[SMALL, MEDIUM, LARGE],
        TOGETHER_CONTEXT_LIMIT,
        |config| config.policy.escalation_order = order(&LIVE_ORDER),
    )
    .await;

    let (code, body) = ask(&relay, HAIKU).await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert_eq!(
        models_seen(&relay.fallback),
        [SMALL, MEDIUM, LARGE],
        "the ladder is walked once, upward, and never revisits a target"
    );

    // 9B's envelope, intact: the phrase, the pair, and the provider's sentence.
    assert_eq!(body["error"]["type"], "invalid_request_error");
    let message = body["error"]["message"].as_str().expect("a message string");
    assert_eq!(token_pair(message), Some((170071, 131072)), "{message:?}");
    assert!(
        message.contains("The input (170071 tokens) is longer than the model's context length"),
        "{message:?}"
    );
}

/// A request that started at the top has nowhere to go.
#[tokio::test]
async fn a_request_that_starts_at_the_top_has_nowhere_to_climb() {
    let relay = start_laddered(&LIVE_LADDER, &[LARGE], TOGETHER_CONTEXT_LIMIT, |config| {
        config.policy.escalation_order = order(&LIVE_ORDER)
    })
    .await;

    let (code, body) = ask(&relay, OPUS).await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert_eq!(models_seen(&relay.fallback), [LARGE]);
    assert!(
        body["error"]["message"]
            .as_str()
            .expect("a message string")
            .starts_with("prompt is too long: 170071 tokens > 131072"),
        "the last rung still produces the client's recovery: {body}"
    );
}

/// `"*"` is not a rung. It is consulted only because no prefix matched, so its
/// target is a safe default for anything rather than a size tier — and on the live
/// map it is the *largest* model, so reading it as the bottom rung would send an
/// overflowing request down to a smaller window. An older alias
/// (`claude-3-5-sonnet-…`, which no `claude-sonnet` prefix matches) is how a real
/// request lands there.
#[tokio::test]
async fn a_target_the_catch_all_chose_has_no_ladder_position() {
    // The catch-all deliberately points at the *smallest* model here, so a hop
    // would look attractive. It still must not happen.
    let map = [
        ("claude-sonnet", MEDIUM),
        ("claude-opus", LARGE),
        ("*", SMALL),
    ];
    let relay = start_laddered(&map, &[SMALL], TOGETHER_CONTEXT_LIMIT, |_| {}).await;

    let (code, _) = ask(&relay, "claude-3-5-sonnet-20241022").await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert_eq!(models_seen(&relay.fallback), [SMALL]);
}

/// A slot the order does not name has no position either — `claude-fable` under
/// the default order, which is what the live config runs with today.
#[tokio::test]
async fn a_slot_the_order_does_not_name_has_no_ladder_position() {
    let map = [("claude-fable", SMALL), ("claude-opus", LARGE)];
    let relay = start_laddered(&map, &[SMALL], TOGETHER_CONTEXT_LIMIT, |_| {}).await;

    let (code, _) = ask(&relay, FABLE).await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert_eq!(models_seen(&relay.fallback), [SMALL]);
}

/// Direct model selection has no ladder and must not get one: the client named
/// the model itself (`/model deepseek-ai/…`), `model_in == model_out`, and
/// swapping a hand-picked model for a different one would be wrong behavior
/// rather than a recovery. §7d routing, not failover.
#[tokio::test]
async fn a_name_routed_request_is_never_escalated() {
    let relay = start_laddered(&LIVE_LADDER, &[OPEN_MODEL], TOGETHER_CONTEXT_LIMIT, |_| {}).await;

    let (code, body) = ask(&relay, OPEN_MODEL).await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert_eq!(models_seen(&relay.fallback), [OPEN_MODEL]);
    assert!(
        body["error"]["message"]
            .as_str()
            .expect("a message string")
            .starts_with("prompt is too long"),
        "it keeps 9B's translation, and only that: {body}"
    );
}

/// The gate, off: the translated error goes straight to the client, which is the
/// behavior before this feature existed.
#[tokio::test]
async fn the_config_gate_turns_the_ladder_off() {
    let relay = start_laddered(&LIVE_LADDER, &[SMALL], TOGETHER_CONTEXT_LIMIT, |config| {
        config.policy.escalate_on_context_limit = false
    })
    .await;

    let (code, body) = ask(&relay, HAIKU).await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert_eq!(models_seen(&relay.fallback), [SMALL]);
    assert!(
        body["error"]["message"]
            .as_str()
            .expect("a message string")
            .starts_with("prompt is too long: 170071 tokens > 131072"),
        "{body}"
    );
}

/// A raw TCP mock: the first request gets `body` with `status`, and every request
/// after it has its connection dropped with no response at all.
///
/// Not an axum router, because a handler always answers something — and "the hop
/// could not be sent" is precisely the case with no response to report. Written
/// against `TcpStream::try_read`/`try_write` rather than the `AsyncReadExt`
/// extension traits, which this tree's tokio features do not include.
/// Returns its address and a count of the connections it accepted, so a test can
/// prove the hop was really attempted rather than passing because it never was.
async fn one_answer_then_a_dead_connection(
    status: StatusCode,
    body: &'static str,
) -> (SocketAddr, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind ephemeral port");
    let addr = listener.local_addr().expect("failed to read local addr");
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&accepted);
    tokio::spawn(async move {
        let mut served = 0;
        while let Ok((socket, _)) = listener.accept().await {
            // Read what the relay sent before answering (or before hanging up), so
            // the client is never writing into a socket that is already gone.
            socket
                .readable()
                .await
                .expect("socket never became readable");
            let mut scratch = [0u8; 16 * 1024];
            let _ = socket.try_read(&mut scratch);
            served += 1;
            counter.store(served, Ordering::Release);
            if served > 1 {
                drop(socket);
                continue;
            }
            let response = format!(
                "HTTP/1.1 {} {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                status.as_u16(),
                status.canonical_reason().unwrap_or("Error"),
                body.len()
            );
            let mut rest = response.as_bytes();
            while !rest.is_empty() {
                socket
                    .writable()
                    .await
                    .expect("socket never became writable");
                match socket.try_write(rest) {
                    Ok(written) => rest = &rest[written..],
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => continue,
                    Err(_) => break,
                }
            }
        }
    });
    (addr, accepted)
}

/// The invariant that makes this feature safe to leave on: **escalation can never
/// leave the client worse off than not escalating would have.** The hop here cannot
/// even be sent — the provider accepts the connection and hangs up — so it has
/// nothing of its own to report, and the client keeps the rung below's translated
/// context-limit error rather than the relay's own `upstream_unreachable`, which
/// says nothing it can act on.
///
/// Reachable in production: the retry opens a second connection to the same
/// provider, and a provider that has just started failing answers the first and
/// drops the second.
#[tokio::test]
async fn a_hop_that_cannot_be_sent_keeps_the_answer_the_rung_below_produced() {
    set_profile_keys();
    let anthropic_addr = serve(anthropic_upstream(Recorder::default(), false)).await;
    let (fallback_addr, accepted) =
        one_answer_then_a_dead_connection(StatusCode::BAD_REQUEST, TOGETHER_CONTEXT_LIMIT).await;
    let relay = serve_relay_with(
        config(
            anthropic_addr,
            "all",
            laddered_profile(fallback_addr, &LIVE_LADDER),
        ),
        None,
    )
    .await;
    drive_to_limited(relay).await;

    let response = client()
        .post(format!("http://{relay}/v1/messages"))
        .body(session_start(HAIKU))
        .send()
        .await
        .expect("request failed");
    let code = response.status();
    let body: Value = serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
        .expect("the body must be JSON");

    assert_eq!(code, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(
        body["error"]["type"], "invalid_request_error",
        "a relay-internal 502 replaced an answer the client could act on: {body}"
    );
    assert!(
        body["error"]["message"]
            .as_str()
            .expect("a message string")
            .starts_with("prompt is too long: 170071 tokens > 131072"),
        "{body}"
    );
    // And the hop really was attempted, or the assertions above would hold for the
    // uninteresting reason that nothing ever climbed.
    assert_eq!(
        accepted.load(Ordering::Acquire),
        2,
        "the escalated attempt was never sent, so this proved nothing"
    );
}

/// A fallback profile that answers 200, streams a real content delta, and only
/// *then* reports the context limit — inside the stream, which is the only way
/// this error can arrive after bytes have already reached the client.
fn streamed_context_limit_upstream(recorder: Recorder) -> Router {
    const ERROR_FRAME: &str = concat!(
        r#"data: {"error":{"message":"The input (170071 tokens) is longer than the model's "#,
        r#"context length (131072 tokens).","type":"invalid_request_error"}}"#,
        "\n\n"
    );
    Router::new().route(
        "/v1/chat/completions",
        any(move |request: Request| {
            let recorder = recorder.clone();
            async move {
                record(&recorder, request).await;
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from(format!("{}{ERROR_FRAME}", OPENAI_CHUNKS[0])))
                    .expect("failed to build mock response")
            }
        }),
    )
}

/// **If any byte has been sent to the client, do not escalate.** On this route
/// that is structural rather than a rule to remember — the escalation decision is
/// made on the non-2xx branch, and an HTTP status arrives before its body, so
/// nothing has been written at that point. This is the case that would break the
/// rule if it were not: a 200 whose *stream* carries the context-limit error, so
/// the client has already rendered a content block by the time it arrives.
///
/// The client keeps what it was given, terminated by the provider's own message
/// as an Anthropic `error` event (the mid-stream contract spec §6 already sets),
/// and the profile is asked exactly once.
#[tokio::test]
async fn a_context_limit_that_arrives_mid_stream_is_never_escalated() {
    set_profile_keys();
    let fallback = Recorder::default();
    let anthropic_addr = serve(anthropic_upstream(Recorder::default(), false)).await;
    let fallback_addr = serve(streamed_context_limit_upstream(fallback.clone())).await;
    let relay = serve_relay_with(
        config(
            anthropic_addr,
            "all",
            laddered_profile(fallback_addr, &LIVE_LADDER),
        ),
        None,
    )
    .await;
    drive_to_limited(relay).await;

    let response = client()
        .post(format!("http://{relay}/v1/messages"))
        .body(format!(
            r#"{{"model":"{HAIKU}","max_tokens":64,"stream":true,"messages":[{{"role":"user","content":"hi"}}]}}"#
        ))
        .send()
        .await
        .expect("request failed");

    // The subject first: the profile was asked exactly once. Every attempt is
    // recorded before the response it produces reaches the client, so a hop would
    // already be visible here.
    assert_eq!(
        models_seen(&fallback),
        [SMALL],
        "a context limit that arrived mid-stream was escalated"
    );
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    let bytes = response.bytes().await.expect("failed to read body");
    let events = events(&bytes);

    // Bytes really did reach the client before the error: a content block, not
    // just a header.
    assert!(
        events.iter().any(
            |(name, data)| name == "content_block_delta" && data["delta"]["text"] == "streamed"
        ),
        "the stream must have delivered content before the error: {events:?}"
    );
    let (name, data) = events.last().expect("at least one event");
    assert_eq!(name, "error");
    assert!(
        data["error"]["message"]
            .as_str()
            .expect("a message string")
            .contains("longer than the model's context length"),
        "the provider's own message is what the client gets: {data}"
    );
}

/// The other half of the same rule, from the request side. A *streaming* request
/// that overflows gets its 400 before any byte is sent — Together answers one
/// exactly that way — so it escalates like any other, and the retry is re-emitted
/// through the translator rather than patched, which means it is still a stream.
/// A retry that lost the flag would deliver a whole JSON body where the client is
/// parsing SSE.
#[tokio::test]
async fn an_escalated_streaming_request_is_still_streaming_on_the_retry() {
    let relay = start_laddered(&LIVE_LADDER, &[SMALL], TOGETHER_CONTEXT_LIMIT, |_| {}).await;

    let response = client()
        .post(format!("http://{}/v1/messages", relay.addr))
        .body(format!(
            r#"{{"model":"{HAIKU}","max_tokens":64,"stream":true,"messages":[{{"role":"user","content":"hi"}}]}}"#
        ))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    let bytes = response.bytes().await.expect("failed to read body");
    let names: Vec<String> = events(&bytes).into_iter().map(|(name, _)| name).collect();
    assert_eq!(names.first().map(String::as_str), Some("message_start"));
    assert_eq!(names.last().map(String::as_str), Some("message_stop"));

    assert_eq!(models_seen(&relay.fallback), [SMALL, MEDIUM]);
    let sent = relay.fallback.0.lock().expect("recorder poisoned");
    for recorded in sent.iter() {
        assert_eq!(
            recorded.json()["stream"],
            Value::Bool(true),
            "an attempt lost the client's streaming flag"
        );
    }
}

// --- spec §4's `fallback_error` notifier event ---
//
// The event exists for the failure that takes out *every* request: a dead
// provider key, an unreachable endpoint, an exhausted balance. So the two things
// most of these tests hold are that it fires **once per outage** rather than once
// per request, and that a failure belonging to one request does not fire it at
// all — a false "your fallback is down" costs more trust than a missed
// notification.
//
// Every one of them wires a real hook and reads the lines it wrote, because the
// env vars are the contract an operator writes against and nothing below the
// subprocess boundary can prove they arrived.

/// Together's shape for a dead key — the incident this event exists for. Its
/// message is deliberately full of text that must never reach `RELAY_DETAIL`: a
/// provider error body is user- and attacker-influenced, and the notification
/// goes to an operator's hook.
const UNAUTHORIZED: &str = concat!(
    r#"{"error":{"message":"Invalid API key provided: tgp-DEAD-KEY-DO-NOT-NOTIFY. You can find "#,
    r#"your API key at https://api.together.xyz/settings/api-keys","#,
    r#""type":"invalid_request_error","code":"invalid_api_key"}}"#
);

/// A hook that appends spec §4's three env vars, one line per event — the same
/// shape `tests/notify.rs` writes, so a line here is read the same way.
fn logging_hook(log: &Path) -> String {
    format!(
        r#"printf '%s|%s|%s\n' "$RELAY_EVENT" "$RELAY_RESET_AT" "$RELAY_DETAIL" >> {}"#,
        log.display()
    )
}

fn hook_log_path(label: &str) -> PathBuf {
    unique_temp_dir(label).with_extension("log")
}

/// Only this event's lines. A relay driven to `LIMITED` fires `failover_engaged`
/// through the same hook, and every count below is about `fallback_error` alone.
fn fallback_error_lines_now(log: &Path) -> Vec<String> {
    std::fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .filter(|line| line.starts_with("fallback_error|"))
        .map(str::to_string)
        .collect()
}

/// Three times the notifier's 500ms `SWITCH_CHECK_INTERVAL`, matching the margin
/// `tests/control.rs` already uses for the other slot event. A window shorter than
/// one poll — the 300ms this started at — can end before the worker has looked at
/// the slot even once, so a second event that *would* have produced a line has not
/// had the chance to.
const SETTLE: Duration = Duration::from_millis(1500);

/// Waits for at least `count` of them, then reads again past `SETTLE`.
///
/// **What the settle window can and cannot buy, because getting this wrong emptied
/// four tests in this file.** `fallback_error` travels on a *coalescing slot*, not
/// the FIFO: two events sent before the worker drains the slot become **one** hook
/// line, so for a wrongly-fired second event there is often no "extra line arriving
/// just after" to wait for at all. Settling cannot reveal what coalescing has
/// already destroyed, and no amount of it can — re-running the critical mutation at
/// 5× this window stayed green.
///
/// So: **a count assertion over this helper discriminates only if the events are
/// separated in time or distinguished by content.** Separated in time means a drain
/// window between the requests that provoke them (`an_intermittent_failure_notifies_once_not_once_per_failure`)
/// or a wait for the previous line before provoking the next
/// (`k_consecutive_successes_re_arm_the_event_and_a_later_failure_fires_again`, and
/// the preferred shape — it is deterministic rather than timed). Distinguished by
/// content means asserting *which* cause fired, not just how many did
/// (`a_request_attributable_failure_fires_nothing`). Settling is neither of those
/// two instruments and cannot be made into one; it only bounds how long a line that
/// *will* appear has to appear in.
async fn fallback_error_lines(log: &Path, count: usize) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if fallback_error_lines_now(log).len() >= count {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {count} fallback_error line(s), got {:?}",
            std::fs::read_to_string(log).unwrap_or_default()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(SETTLE).await;
    fallback_error_lines_now(log)
}

/// A fallback profile that answers a fixed script, one entry per request, cycling
/// once the script runs out. A failing *streak* is a sequence, not a single
/// answer, so the sequence is what has to be drivable — and an intermittent
/// failure is a sequence that repeats.
fn scripted_upstream(recorder: Recorder, script: &'static [(StatusCode, &'static str)]) -> Router {
    let at = Arc::new(AtomicUsize::new(0));
    Router::new().route(
        "/v1/chat/completions",
        any(move |request: Request| {
            let recorder = recorder.clone();
            let at = at.clone();
            async move {
                record(&recorder, request).await;
                let (status, body) = script[at.fetch_add(1, Ordering::SeqCst) % script.len()];
                json(status, body)
            }
        }),
    )
}

/// `RE_ARM_SUCCESSES` successes followed by one failure — the pattern that
/// notifies as often as the *k*-consecutive-successes rule allows, so it is the
/// adversarial shape for anything that has to hold under repeated firing.
fn intermittent_script() -> &'static [(StatusCode, &'static str)] {
    const OK: (StatusCode, &str) = (StatusCode::OK, OPENAI_COMPLETION);
    const FAIL: (StatusCode, &str) = (StatusCode::UNAUTHORIZED, UNAUTHORIZED);
    // Built once and leaked rather than written out: the length has to track
    // `RE_ARM_SUCCESSES`, and a hand-written literal would silently stop being
    // the worst case if that constant moved.
    static SCRIPT: std::sync::OnceLock<Vec<(StatusCode, &'static str)>> =
        std::sync::OnceLock::new();
    SCRIPT.get_or_init(|| {
        let mut script = vec![OK; RE_ARM_SUCCESSES as usize];
        script.push(FAIL);
        script
    })
}

/// One route-attributable failure, then `RE_ARM_SUCCESSES` **consecutive**
/// request-attributable ones. Cycling, so the request after them is a 401 again.
///
/// The 400s have to be consecutive and there have to be `k` of them, or the weaker
/// of the two defects this shape exists to catch is unreachable: with `k` at five, a
/// single 400 counted as a delivery cannot re-arm anything, and 400s interleaved
/// with 401s never accumulate because each 401 resets the streak.
fn one_failure_then_k_rejections() -> &'static [(StatusCode, &'static str)] {
    const FAIL: (StatusCode, &str) = (StatusCode::UNAUTHORIZED, UNAUTHORIZED);
    const REJECT: (StatusCode, &str) = (StatusCode::BAD_REQUEST, MISSING_MESSAGES);
    static SCRIPT: std::sync::OnceLock<Vec<(StatusCode, &'static str)>> =
        std::sync::OnceLock::new();
    SCRIPT.get_or_init(|| {
        let mut script = vec![FAIL];
        script.extend(std::iter::repeat_n(REJECT, RE_ARM_SUCCESSES as usize));
        script
    })
}

/// A relay with a notifier hook, whose fallback profile answers `script`.
/// Name-routed (§7d) like the other provider-error tests, so Anthropic is never
/// contacted and only the fallback's own answer can be what is observed.
async fn start_scripted(script: &'static [(StatusCode, &'static str)], log: &Path) -> SocketAddr {
    start_notified(|recorder| scripted_upstream(recorder, script), log).await
}

async fn start_notified(
    fallback_router: impl FnOnce(Recorder) -> Router,
    log: &Path,
) -> SocketAddr {
    start_notified_with(fallback_router, logging_hook(log)).await
}

async fn start_notified_with(
    fallback_router: impl FnOnce(Recorder) -> Router,
    command: String,
) -> SocketAddr {
    set_profile_keys();
    let anthropic_addr = serve(anthropic_upstream(Recorder::default(), false)).await;
    let fallback_addr = serve(fallback_router(Recorder::default())).await;
    let mut config = config(
        anthropic_addr,
        "notify-only",
        profile(fallback_addr, "openai", OPENAI_KEY_ENV),
    );
    config.notify = relay::config::NotifyConfig {
        command: Some(command),
        timeout_secs: 5,
    };
    serve_relay_with(config, None).await
}

/// One name-routed request at the fallback profile, whatever it answers.
async fn one_request(relay: SocketAddr) -> StatusCode {
    client()
        .post(format!("http://{relay}/v1/messages"))
        .body(session_start(OPEN_MODEL))
        .send()
        .await
        .expect("request failed")
        .status()
}

fn fields(line: &str) -> Vec<&str> {
    line.split('|').collect()
}

/// The motivating case, end to end: the provider rejects the relay's credentials
/// and the operator is told, in the relay's own vocabulary. `RELAY_RESET_AT` is
/// set-but-empty, so a hook under `set -u` survives an event with no window.
///
/// And not one word of the provider's message, which is where the shape of this
/// event was decided: that body is user- and attacker-influenced, so the detail
/// is built from a status code and the operator's own profile name or from
/// nothing at all.
#[tokio::test]
async fn a_provider_401_fires_one_fallback_error_naming_the_profile_and_the_status() {
    const SCRIPT: &[(StatusCode, &str)] = &[(StatusCode::UNAUTHORIZED, UNAUTHORIZED)];
    let log = hook_log_path("fallback-error-401");
    let relay = start_scripted(SCRIPT, &log).await;

    assert_eq!(one_request(relay).await, StatusCode::UNAUTHORIZED);

    let lines = fallback_error_lines(&log, 1).await;
    assert_eq!(lines.len(), 1, "one outage, one notification: {lines:?}");
    assert_eq!(
        fields(&lines[0]),
        vec!["fallback_error", "", "fallback: http 401"]
    );
    for leaked in ["DEAD-KEY", "api.together.xyz", "invalid_api_key", "Invalid"] {
        assert!(
            !lines[0].contains(leaked),
            "the provider's own text reached the notifier: {:?}",
            lines[0]
        );
    }

    let _ = std::fs::remove_file(&log);
}

/// The reason the event is edge-triggered at all. A dead key fails every request,
/// so a notification per failure is one hook run per request — unusable in itself,
/// and it would delay a real route transition behind a queue of them.
///
/// The wait between the first failure and the rest is what makes this a detector
/// rather than a test of the slot: a harness audit found this test passing with the
/// edge-trigger deleted, because three requests against a local mock finish inside
/// ~5ms and three events in one drain window collapse to one hook line. Draining
/// the first before provoking the others gives the second and third somewhere
/// visible to land.
#[tokio::test]
async fn a_second_and_third_failure_of_the_same_outage_fire_nothing() {
    const SCRIPT: &[(StatusCode, &str)] = &[(StatusCode::UNAUTHORIZED, UNAUTHORIZED)];
    let log = hook_log_path("fallback-error-streak");
    let relay = start_scripted(SCRIPT, &log).await;

    assert_eq!(one_request(relay).await, StatusCode::UNAUTHORIZED);
    assert_eq!(
        fallback_error_lines(&log, 1).await.len(),
        1,
        "the outage has to have been announced before the next failures are provoked"
    );

    assert_eq!(one_request(relay).await, StatusCode::UNAUTHORIZED);
    assert_eq!(one_request(relay).await, StatusCode::UNAUTHORIZED);

    let lines = fallback_error_lines(&log, 1).await;
    assert_eq!(
        lines.len(),
        1,
        "three failures of one outage must notify once: {lines:?}"
    );

    let _ = std::fs::remove_file(&log);
}

/// **`RE_ARM_SUCCESSES` consecutive deliveries re-arm, and one does not.**
///
/// This test asserted the opposite in the first round — one success re-armed —
/// and **the specification changed under it, not the other way around.** Two
/// reviewers independently measured that one-success re-arming makes the
/// edge-trigger bound nothing at all against an *intermittent* failure, which is
/// what a 429 or a 5xx is by nature: the route notified once per failed request
/// and delayed a real `failover_engaged` by 6.79s behind the backlog. Recording
/// the inversion rather than quietly rewriting it, because the case is still worth
/// pinning from both sides.
///
/// Each phase waits for the previous notification to reach the hook before the
/// next one is provoked. That is not politeness: `fallback_error` now has a
/// coalescing slot, so two events sent before the worker drains it collapse into
/// one, and a test that wants to *observe* two has to let the first out.
#[tokio::test]
async fn k_consecutive_successes_re_arm_the_event_and_a_later_failure_fires_again() {
    let log = hook_log_path("fallback-error-rearm");
    let relay = start_scripted(intermittent_script(), &log).await;

    // One cycle of the script: `RE_ARM_SUCCESSES` deliveries, then the failure
    // that opens the outage.
    for _ in 0..RE_ARM_SUCCESSES {
        assert_eq!(one_request(relay).await, StatusCode::OK);
    }
    assert_eq!(one_request(relay).await, StatusCode::UNAUTHORIZED);
    assert_eq!(
        fallback_error_lines(&log, 1).await.len(),
        1,
        "the outage has to have been announced before the next one is provoked"
    );

    // A second cycle: its successes are a full consecutive streak, so they really
    // do end the outage, and the failure after them is a new one. Nothing shorter
    // re-arms — `an_intermittent_failure_notifies_once_not_once_per_failure` is
    // the other side of the same rule.
    for _ in 0..RE_ARM_SUCCESSES {
        assert_eq!(one_request(relay).await, StatusCode::OK);
    }
    assert_eq!(one_request(relay).await, StatusCode::UNAUTHORIZED);

    let lines = fallback_error_lines(&log, 2).await;
    assert_eq!(
        lines.len(),
        2,
        "a failure after {RE_ARM_SUCCESSES} consecutive successes is a new outage: {lines:?}"
    );
    for line in &lines {
        assert_eq!(fields(line)[2], "fallback: http 401");
    }

    let _ = std::fs::remove_file(&log);
}

/// The half the test above cannot show: an **intermittently** failing route
/// notifies once, not once per failure. Alternating success and failure never
/// strings `RE_ARM_SUCCESSES` successes together, so the outage never ends and the
/// operator is told about it exactly once — which is the whole point of the
/// specification change fix round 1 made.
///
/// **Why this test sleeps, when nothing else here does.** The event has a
/// coalescing slot, so several events sent inside one drain window collapse into a
/// single hook run — which means a fast loop over alternating requests produces one
/// hook line whether the re-arm rule is right or wrong, and the test would be blind
/// to exactly the defect it is named for. This was found by mutating
/// `RE_ARM_SUCCESSES` to 1 and watching the fast version pass. So each failure gets
/// its own drain window: `SWITCH_CHECK_INTERVAL` is 500ms (`src/notify.rs`), and
/// this waits longer than that before provoking the next one.
///
/// Flake direction, deliberately: the gap can only be too *short*, never too long.
/// A short gap means a wrongly-fired event coalesces away and the assertion still
/// holds, so this can go falsely green under load but never falsely red.
#[tokio::test]
async fn an_intermittent_failure_notifies_once_not_once_per_failure() {
    const ALTERNATING: &[(StatusCode, &str)] = &[
        (StatusCode::OK, OPENAI_COMPLETION),
        (StatusCode::UNAUTHORIZED, UNAUTHORIZED),
    ];
    const DRAIN_WINDOW: Duration = Duration::from_millis(600);
    let log = hook_log_path("fallback-error-intermittent");
    let relay = start_scripted(ALTERNATING, &log).await;

    let mut failures = 0;
    for _ in 0..3 {
        assert_eq!(one_request(relay).await, StatusCode::OK);
        assert_eq!(one_request(relay).await, StatusCode::UNAUTHORIZED);
        failures += 1;
        tokio::time::sleep(DRAIN_WINDOW).await;
    }

    let lines = fallback_error_lines(&log, 1).await;
    assert_eq!(
        lines.len(),
        1,
        "{failures} failures of one intermittent outage must notify once, and each had \
         its own drain window: {lines:?}"
    );

    let _ = std::fs::remove_file(&log);
}

/// **A request-attributable failure counts for nothing either way** (spec §4) —
/// neither re-arming outright nor advancing the recovery streak. Reaching the
/// provider proves less than it looks like it does: the credentials are still dead
/// and the operator's outage has not ended.
///
/// **This test is the only guard on that rule anywhere in the suite, and for one
/// round it guarded nothing.** A harness audit found the version before this one
/// passing 523/523 with `Outcome::Rejected` re-arming outright — the exact defect
/// round 1 was convened to remove — because the two 401s were 5ms apart and
/// coalesced into a single hook line. No unit test can stand in: the property is the
/// *absence of a call site* in `fallback::forward`, which nothing below the
/// subprocess boundary can observe.
///
/// Two things make it a detector again, one for each of the two defects available
/// here. The wait after the first failure separates the events in time, so a
/// wrongly-fired second one becomes a second line — that catches a 400 re-arming
/// outright. And the 400s are `RE_ARM_SUCCESSES` of them, **consecutive**, which
/// catches the weaker defect of a 400 merely advancing the recovery streak: with `k`
/// at five a single 400 cannot re-arm anything, and 400s interleaved with 401s never
/// accumulate because each 401 resets the streak, so both the count and the adjacency
/// are load-bearing. Round 1 raised `k` and nobody re-checked which tests that made
/// unfalsifiable.
#[tokio::test]
async fn a_request_attributable_failure_between_two_route_failures_does_not_re_arm() {
    let log = hook_log_path("fallback-error-no-rearm");
    let relay = start_scripted(one_failure_then_k_rejections(), &log).await;

    assert_eq!(one_request(relay).await, StatusCode::UNAUTHORIZED);
    assert_eq!(
        fallback_error_lines(&log, 1).await.len(),
        1,
        "the outage has to have been announced before the 400s are provoked"
    );

    for _ in 0..RE_ARM_SUCCESSES {
        assert_eq!(one_request(relay).await, StatusCode::BAD_REQUEST);
    }
    // The script cycles, so this is a 401 again — and the outage it belongs to has
    // already been announced.
    assert_eq!(one_request(relay).await, StatusCode::UNAUTHORIZED);

    let lines = fallback_error_lines(&log, 1).await;
    assert_eq!(
        lines.len(),
        1,
        "a 400 in the middle of an outage re-armed the event: {lines:?}"
    );

    let _ = std::fs::remove_file(&log);
}

/// Neither of the two request-attributable failures notifies: a plain 400 is the
/// provider telling *this* request it was wrong, and
/// `fallback_request_untranslatable` is this request's own body being
/// untranslatable. Nothing about the route is broken in either, and the next
/// request may be fine.
///
/// The 401 at the end is a positive control, not an afterthought: without it a
/// broken hook would make the absence above vacuous.
#[tokio::test]
async fn a_request_attributable_failure_fires_nothing() {
    const SCRIPT: &[(StatusCode, &str)] = &[
        (StatusCode::BAD_REQUEST, MISSING_MESSAGES),
        (StatusCode::UNAUTHORIZED, UNAUTHORIZED),
    ];
    let log = hook_log_path("fallback-error-request-attributable");
    let relay = start_scripted(SCRIPT, &log).await;

    assert_eq!(one_request(relay).await, StatusCode::BAD_REQUEST);

    // `role: "function"` is not a role the Anthropic Messages API has, so the
    // request dies in translation without reaching the profile at all.
    let untranslatable = client()
        .post(format!("http://{relay}/v1/messages"))
        .body(format!(
            r#"{{"model":"{OPEN_MODEL}","max_tokens":64,"messages":[{{"role":"function","content":"x"}}]}}"#
        ))
        .send()
        .await
        .expect("request failed");
    assert_eq!(untranslatable.status(), StatusCode::BAD_GATEWAY);

    // Now something that *is* the route's fault, so the hook is proven live.
    assert_eq!(one_request(relay).await, StatusCode::UNAUTHORIZED);

    let lines = fallback_error_lines(&log, 1).await;
    assert_eq!(
        lines.len(),
        1,
        "only the 401 belongs to the route: {lines:?}"
    );
    // Which one fired, not only how many: a 400 that notified would take the
    // edge with it and suppress the 401, leaving the count above satisfied.
    assert_eq!(
        fields(&lines[0])[2],
        "fallback: http 401",
        "a request-attributable failure fired, and took the edge with it"
    );

    let _ = std::fs::remove_file(&log);
}

/// A 2xx the relay cannot use is the route failing to deliver, so it fires — and
/// it must not re-arm on the way past, however much a 200 looks like proof that
/// the path works. The provider answered; the client got nothing usable.
///
/// The second request is the whole test, and it only means anything with the first
/// event drained: an audit found the two-requests-back-to-back version passing with
/// the edge-trigger deleted, because both events landed in one drain window.
#[tokio::test]
async fn a_two_hundred_the_relay_cannot_translate_fires_and_does_not_re_arm() {
    const SCRIPT: &[(StatusCode, &str)] = &[(StatusCode::OK, ANTHROPIC_SHAPED_BODY)];
    let log = hook_log_path("fallback-error-untranslatable-response");
    let relay = start_scripted(SCRIPT, &log).await;

    assert_eq!(one_request(relay).await, StatusCode::BAD_GATEWAY);
    assert_eq!(
        fallback_error_lines(&log, 1).await.len(),
        1,
        "the first unusable 200 has to have been announced before the second is sent"
    );
    assert_eq!(one_request(relay).await, StatusCode::BAD_GATEWAY);

    let lines = fallback_error_lines(&log, 1).await;
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(
        fields(&lines[0]),
        vec![
            "fallback_error",
            "",
            "fallback: fallback_response_untranslatable"
        ]
    );

    let _ = std::fs::remove_file(&log);
}

/// A failure with no status at all still names its cause, from the relay's own
/// `&'static str` vocabulary. This is the other shape `RELAY_DETAIL` can take,
/// and the one an operator sees when the provider's endpoint has gone away
/// rather than answered.
#[tokio::test]
async fn a_provider_nothing_is_listening_on_fires_upstream_unreachable() {
    set_profile_keys();
    let log = hook_log_path("fallback-error-unreachable");
    let anthropic_addr = serve(anthropic_upstream(Recorder::default(), false)).await;
    let unreachable = closed_port().await;
    let mut config = config(
        anthropic_addr,
        "notify-only",
        profile(unreachable, "openai", OPENAI_KEY_ENV),
    );
    config.notify = relay::config::NotifyConfig {
        command: Some(logging_hook(&log)),
        timeout_secs: 5,
    };
    let relay = serve_relay_with(config, None).await;

    assert_eq!(one_request(relay).await, StatusCode::BAD_GATEWAY);

    let lines = fallback_error_lines(&log, 1).await;
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(
        fields(&lines[0]),
        vec!["fallback_error", "", "fallback: upstream_unreachable"]
    );

    let _ = std::fs::remove_file(&log);
}

/// A fallback profile that holds every request until `concurrent` of them have
/// arrived, then fails all of them at once. A barrier rather than a sleep: the
/// property under test is what several failures *in flight together* do, so they
/// have to be genuinely concurrent rather than probably concurrent.
fn barrier_unauthorized_upstream(concurrent: usize) -> Router {
    let barrier = Arc::new(tokio::sync::Barrier::new(concurrent));
    let arrived = Arc::new(AtomicUsize::new(0));
    Router::new().route(
        "/v1/chat/completions",
        any(move || {
            let barrier = barrier.clone();
            let arrived = arrived.clone();
            async move {
                // Only the first `concurrent` requests are held. A `Barrier`
                // rearms after each generation, so anything later would wait for
                // `concurrent` companions that are never coming — and the test
                // needs one further request *after* the burst has been announced.
                if arrived.fetch_add(1, Ordering::SeqCst) < concurrent {
                    barrier.wait().await;
                }
                json(StatusCode::UNAUTHORIZED, UNAUTHORIZED)
            }
        }),
    )
}

/// Eight failures arriving together notify once.
///
/// **What this does not prove, stated because an earlier version of this comment
/// claimed it.** It said it was "the reason the edge-trigger is a
/// `compare_exchange` and not a load followed by a store". It is not: a reviewer
/// made exactly that mutation and got 30 runs, 30 passes. `#[tokio::test]` with no
/// flavor is a current-thread runtime and `tests/common`'s `serve` spawns onto the
/// *test's* own runtime, so relay, mock and all eight clients share one thread —
/// and with no `.await` between the load and the store there is no interleaving to
/// find. Under `flavor = "multi_thread"` the mutation is caught 2 runs in 20, which
/// is a flaky detector rather than a good one, so the flavor is deliberately left
/// alone: an honest weak test beats a flaky strong one. The argument for the CAS
/// lives next to the CAS, in `AppState::fallback_failed`.
///
/// **And what the concurrent burst alone cannot prove either**, which a harness
/// audit established after the rewrite above: the coalescing slot collapses eight
/// simultaneous events into one hook line *by construction*, so "eight, then one
/// line" held even with the edge-trigger deleted. Events that arrive together can be
/// separated neither in time nor by content — they are identical — so the burst is
/// an exercise, not a detector.
///
/// The trailing failure is the detector. It is sent after the burst's notification
/// has drained, so if the edge-trigger were gone it would produce a second line. The
/// burst still earns its place: it is what sets the flag under real concurrency, over
/// real HTTP, with eight requests genuinely in flight together.
#[tokio::test]
async fn eight_concurrent_failures_notify_once() {
    const CONCURRENT: usize = 8;
    let log = hook_log_path("fallback-error-concurrent");
    let relay = start_notified(|_| barrier_unauthorized_upstream(CONCURRENT), &log).await;

    let mut requests = Vec::new();
    for _ in 0..CONCURRENT {
        requests.push(tokio::spawn(async move { one_request(relay).await }));
    }
    for request in requests {
        assert_eq!(
            request.await.expect("request panicked"),
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        fallback_error_lines(&log, 1).await.len(),
        1,
        "{CONCURRENT} concurrent failures notified more than once: \
         {:?}",
        fallback_error_lines_now(&log)
    );

    // One more, alone, with the burst's line already out.
    assert_eq!(one_request(relay).await, StatusCode::UNAUTHORIZED);

    let lines = fallback_error_lines(&log, 1).await;
    assert_eq!(
        lines.len(),
        1,
        "a ninth failure of the same outage notified again: {lines:?}"
    );

    let _ = std::fs::remove_file(&log);
}

/// `logging_hook`, plus the flood tests' sleep: a queue of runs is then measurable
/// as wall-clock delay on whatever is behind it.
///
/// Deliberately the *same* three-field line as `logging_hook`, not just the event
/// name. It emitted only `$RELAY_EVENT` for one round, which meant
/// `fallback_error_lines_now`'s `"fallback_error|"` filter could never match a line
/// this hook wrote — so the one test using it counted zero `fallback_error`
/// notifications no matter what the relay did. An audit measured that zero and read
/// it as a timing artifact; it was a format mismatch, and the new lower-bound
/// assertion is what surfaced it.
fn slow_logging_hook(log: &Path, delay: Duration) -> String {
    format!(
        r#"sleep {}; printf '%s|%s|%s\n' "$RELAY_EVENT" "$RELAY_RESET_AT" "$RELAY_DETAIL" >> {}"#,
        delay.as_secs_f64(),
        log.display()
    )
}

/// **The regression test for fix round 1's blocker, in the flood tests' shape.**
///
/// `a_live_flood_of_switches_does_not_delay_a_transition_queued_mid_flood` holds
/// this property for `profile_switched`; `fallback_error` was shipped on the FIFO
/// instead of a slot of its own, so it could violate the same property from the
/// other side. Measured before the fix: 40 alternating requests delayed a real
/// `failover_engaged` by **6.79s**, against the 1.8s the flood tests allow for the
/// equivalent bound.
///
/// The traffic is the worst case the *k*-consecutive-successes rule permits —
/// `RE_ARM_SUCCESSES` deliveries then a failure, repeated — so this bounds what is
/// left after 1b rather than what 1b already removed. Every cycle re-arms and
/// re-fires, and on the FIFO every one of those would have been its own hook run.
///
/// **The traffic keeps running while the transition is queued**, which is the
/// live-flood test's shape and not the weaker one. An audit measured the first
/// version of this test: 120 sequential requests finished in ~150ms against the
/// notifier's 500ms poll, so the worker had not looked at the slot even once before
/// the transition was sent — "queued while the backlog is outstanding" was true of
/// the pre-fix FIFO and not of the fixed code. Now a background task feeds requests
/// throughout, and the transition is sent after the worker has been in steady state
/// for longer than one poll interval, exactly as
/// `a_live_flood_of_switches_does_not_delay_a_transition_queued_mid_flood` does.
///
/// `hook_delay * 6`, deliberately the same bound and the same reasoning as the two
/// flood tests: at most one slot event can already be mid-run when the transition
/// is queued, plus the transition's own run, and `* 6` rather than `* 4` for the
/// margin a reviewer measured too tight under contention there.
#[tokio::test]
async fn an_intermittent_failure_backlog_does_not_delay_a_real_transition() {
    let hook_delay = Duration::from_millis(150);
    let log = hook_log_path("fallback-error-backlog");
    let relay = start_notified_with(
        |recorder| scripted_upstream(recorder, intermittent_script()),
        slow_logging_hook(&log, hook_delay),
    )
    .await;

    // Name-routed (§7d) while the route is still ACTIVE, so this traffic is not
    // failover and nothing about it touches Anthropic's state — the transition
    // below is then the only one in the whole test.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let failures = Arc::new(AtomicUsize::new(0));
    let traffic = tokio::spawn({
        let stop = stop.clone();
        let failures = failures.clone();
        async move {
            while !stop.load(Ordering::Relaxed) {
                if one_request(relay).await == StatusCode::UNAUTHORIZED {
                    failures.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    });

    // Long enough that the worker has drained the slot at least once and settled
    // into steady state before the transition is queued — the same reasoning, and
    // the same multiple of the poll interval, as the live-flood test's pre-roll.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let announced_before = fallback_error_lines_now(&log).len();

    let started = Instant::now();
    let limit = client()
        .get(format!("http://{relay}/v1/limit"))
        .send()
        .await
        .expect("limit request failed");
    assert_eq!(limit.status(), StatusCode::TOO_MANY_REQUESTS);
    // Detection classifies when the stream ends, so the body has to be drained.
    limit.bytes().await.expect("failed to read the limit body");

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let logged = std::fs::read_to_string(&log).unwrap_or_default();
        if logged
            .lines()
            .any(|line| line.starts_with("failover_engaged|"))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "failover_engaged never arrived behind the backlog: {logged:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let elapsed = started.elapsed();

    stop.store(true, Ordering::Relaxed);
    let _ = traffic.await;
    let failures = failures.load(Ordering::Relaxed);

    assert!(
        elapsed < hook_delay * 6,
        "{failures} fallback_error events must not delay failover_engaged by more than a \
         couple of hook executions; took {elapsed:?}"
    );

    // The positive control, and the reason it has a *lower* bound. The first
    // version asserted only `announced < failures` and measured 0 < 20 in six of
    // six runs — so it passed with `fallback_error` dropped on the floor entirely,
    // and proved nothing about coalescing either. `failures` counts HTTP 401s, not
    // notifications, so without a lower bound nothing here says the backlog being
    // bounded exists as events at all.
    assert!(
        announced_before >= 1,
        "no fallback_error was ever announced, so this test bounded nothing"
    );
    assert!(
        announced_before < failures,
        "{announced_before} notifications for {failures} failures is not coalescing"
    );

    let _ = std::fs::remove_file(&log);
}

/// `SMALL` overflows, `MEDIUM` answers, and `LARGE` answers 401 — the escalation
/// shape plus a rung that fails the route, so one relay can hold both halves of
/// the test below.
fn escalating_then_unauthorized(recorder: Recorder) -> Router {
    Router::new().route(
        "/v1/chat/completions",
        any(move |request: Request| {
            let recorder = recorder.clone();
            async move {
                let body = record(&recorder, request).await;
                match body["model"].as_str().unwrap_or_default() {
                    SMALL => json(StatusCode::BAD_REQUEST, TOGETHER_CONTEXT_LIMIT),
                    LARGE => json(StatusCode::UNAUTHORIZED, UNAUTHORIZED),
                    _ => json(StatusCode::OK, OPENAI_COMPLETION),
                }
            }
        }),
    )
}

/// **The live drill, as a test.** Escalation (§7e) turns one client request into
/// as many as three upstream requests, and this is the sequence measured on
/// 2026-08-12: `gpt-oss-20b` 400 → `Kimi-K2.7-Code` 200, one client request, one
/// success. The 400 is a superseded attempt, so notifying an outage for it would
/// report one that did not happen — which is why the event is decided from the
/// outcome the route hands the client rather than from each attempt.
///
/// The `claude-opus` request afterwards is the positive control: it lands on the
/// rung that answers 401, so the single line proves the hook was live all along
/// and that the escalated request contributed nothing to it.
#[tokio::test]
async fn a_context_limit_that_escalates_to_a_success_fires_nothing() {
    let log = hook_log_path("fallback-error-escalated");
    let hook = logging_hook(&log);
    let relay = start_laddered_with(&LIVE_LADDER, escalating_then_unauthorized, |config| {
        config.policy.escalation_order = order(&LIVE_ORDER);
        config.notify = relay::config::NotifyConfig {
            command: Some(hook),
            timeout_secs: 5,
        };
    })
    .await;

    let (code, body) = ask(&relay, HAIKU).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(
        models_seen(&relay.fallback),
        [SMALL, MEDIUM],
        "the escalation this test is about did not happen"
    );

    let (code, _) = ask(&relay, OPUS).await;
    assert_eq!(code, StatusCode::UNAUTHORIZED);

    let lines = fallback_error_lines(&log, 1).await;
    assert_eq!(
        lines.len(),
        1,
        "a superseded attempt notified an outage that did not happen: {lines:?}"
    );
    assert_eq!(fields(&lines[0])[2], "fallback: http 401");

    let _ = std::fs::remove_file(&log);
}
