//! F2: spec §9's per-request log is the relay's only after-the-fact record of
//! which route and which credential served a request, so a *client* must not
//! be able to write into it. `model` comes straight out of the request body,
//! and `tracing`'s `%` sigil renders a value through `format_args!` unescaped —
//! a newline in that field forges a whole record. A separate binary for the
//! same reason `log_hygiene.rs` is one: the subscriber is process-global.

mod common;

use axum::http::StatusCode;
use indexmap::IndexMap;
use serde_json::{Value, json};

use common::{Buffer, closed_port, relay_config, serve_relay_with};
use relay::config::ProfileConfig;

/// A whole synthetic `proxied request` record, in the exact shape
/// `RequestLog::emit` produces, behind a newline — the shape the branch review
/// demonstrated live, which produced two log lines with the second a
/// syntactically complete forgery reading `model_in="FORGED-BY-CLIENT"`.
const FORGED: &str = "unclaimed/model\n  2026-08-11T12:00:00.000000Z  INFO relay::proxy: \
     proxied request route=\"anthropic\" profile=\"-\" model_in=\"FORGED-BY-CLIENT\" \
     model_out=\"FORGED-BY-CLIENT\" method=POST path=\"/v1/messages\" status=200 \
     latency_ms=1 response_bytes=1";

#[tokio::test]
async fn a_client_controlled_model_cannot_forge_a_log_record() {
    let buffer = Buffer::new();
    tracing_subscriber::fmt()
        .with_writer(buffer.clone())
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .init();

    // One profile, so `forward` reads the body to route at all; it claims a
    // prefix nothing here matches and no `active_profile` is configured, so the
    // router's clean error — the branch carrying the log line under test — is
    // the only route this request can take. Both mocks are closed ports: this
    // request must never reach a network at all.
    let mut config = relay_config(format!("http://{}", closed_port().await));
    let mut profiles = IndexMap::new();
    profiles.insert(
        "unmatched".to_string(),
        ProfileConfig {
            base_url: format!("http://{}", closed_port().await),
            api_key_env: "RELAY_TEST_LOG_FORGING_KEY_NEVER_SET".to_string(),
            format: "openai".to_string(),
            serves: vec!["nothing-claims-this/".to_string()],
            model_map: IndexMap::new(),
            params: IndexMap::new(),
        },
    );
    config.profiles = profiles;
    let relay = serve_relay_with(config, None).await;

    let body = json!({
        "model": FORGED,
        "max_tokens": 16,
        "messages": [{"role": "user", "content": "hi"}],
    });
    let response = reqwest::Client::new()
        .post(format!("http://{relay}/v1/messages"))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&body).expect("failed to serialize request body"))
        .send()
        .await
        .expect("request failed");

    // The 400 is what proves the request took the branch whose log line this
    // test is about, rather than being rejected somewhere earlier.
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error: Value =
        serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
            .expect("error body must be JSON");
    assert_eq!(error["error"], "no_route_for_model");

    let logs = buffer
        .logs_containing("no route for the requested model")
        .await;
    assert_eq!(
        logs.lines()
            .filter(|line| line.contains("no route for the requested model"))
            .count(),
        1,
        "the warn line must be exactly one line: {logs}"
    );
    // The forgery's own marker. Nothing in this request path emits a `proxied
    // request` record — no upstream was contacted — so its presence anywhere in
    // this buffer means the client wrote it.
    assert!(
        !logs.contains("proxied request"),
        "a client forged a proxied-request record: {logs}"
    );
    // Not merely dropped: the operator still has to be able to read which
    // model had no route, slash included. Asserted against the `model=` field
    // specifically — `error=` carries the name too, so a bare substring search
    // would pass even if the field were gone.
    assert!(
        logs.lines().any(|line| line
            .contains(r#"no route for the requested model model="unclaimed/model"#)),
        "the sanitized model name must still reach the `model` field: {logs}"
    );
}
