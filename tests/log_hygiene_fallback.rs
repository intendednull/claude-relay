//! Global Constraint 2 as it stands after Milestone 3: the rule that no header
//! value which could be a credential reaches the logs now covers the fallback
//! provider's own API key, not only the client's Anthropic credentials.
//!
//! A separate binary from `log_hygiene.rs` because the subscriber is
//! process-global — one test per binary.

mod common;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::any;
use indexmap::IndexMap;
use serde_json::Value;

use common::{Buffer, relay_config, serve, serve_relay_with};
use relay::config::ProfileConfig;

const CLIENT_AUTH: &str = "Bearer sk-ant-oat01-DO-NOT-LOG-THIS-VALUE";
const CLIENT_API_KEY: &str = "sk-ant-api03-DO-NOT-LOG-THIS-EITHER";
const CLIENT_BETA: &str = "prompt-caching-DO-NOT-LOG-THIS-BETA";
const PROFILE_KEY_ENV: &str = "RELAY_TEST_LOG_HYGIENE_PROFILE_KEY";
const PROFILE_KEY: &str = "tgp-DO-NOT-LOG-THIS-PROFILE-KEY";

const LIMIT_BODY: &str = r#"{"type":"error","error":{"type":"rate_limit_error","message":"You have reached your Claude Pro usage limit. Your limit will reset at 6pm."}}"#;
const COMPLETION: &str = concat!(
    r#"{"id":"chatcmpl-1","object":"chat.completion","model":"target/Model","#,
    r#""choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"#,
    r#""finish_reason":"stop"}]}"#
);

/// Records what the profile received. Not reported back through a response
/// header, as `log_hygiene.rs` does on the Anthropic route: the translated
/// path builds its response headers from scratch, so nothing the profile sets
/// survives to the client.
type SeenKey = Arc<Mutex<Option<String>>>;

fn profile_upstream(seen: SeenKey) -> Router {
    Router::new().route(
        "/v1/chat/completions",
        any(move |request: Request| {
            let seen = seen.clone();
            async move {
                *seen.lock().expect("poisoned") = request
                    .headers()
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string);
                Response::builder()
                    .header("content-type", "application/json")
                    .body(Body::from(COMPLETION))
                    .expect("failed to build mock response")
            }
        }),
    )
}

fn anthropic_upstream() -> Router {
    Router::new()
        .route(
            "/v1/limit",
            any(|| async {
                Response::builder()
                    .status(StatusCode::TOO_MANY_REQUESTS)
                    .header("content-type", "application/json")
                    .header("retry-after", "3600")
                    .body(Body::from(LIMIT_BODY))
                    .expect("failed to build mock response")
            }),
        )
        .route("/v1/messages", any(|| async { "unused" }))
}

async fn status(relay: SocketAddr) -> Value {
    let bytes = reqwest::Client::new()
        .get(format!("http://{relay}/status"))
        .send()
        .await
        .expect("status request failed")
        .bytes()
        .await
        .expect("failed to read status body");
    serde_json::from_slice(&bytes).expect("status must be JSON")
}

#[tokio::test]
async fn no_credential_reaches_the_logs_on_the_fallback_route() {
    // SAFETY: the only write to this variable in this process, and it happens
    // before the relay that reads it exists.
    unsafe { std::env::set_var(PROFILE_KEY_ENV, PROFILE_KEY) };

    let buffer = Buffer::new();
    tracing_subscriber::fmt()
        .with_writer(buffer.clone())
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .init();

    let anthropic = serve(anthropic_upstream()).await;
    let seen: SeenKey = Arc::new(Mutex::new(None));
    let profile_addr = serve(profile_upstream(seen.clone())).await;

    let mut config = relay_config(format!("http://{anthropic}"));
    let mut profiles = IndexMap::new();
    profiles.insert(
        "fallback".to_string(),
        ProfileConfig {
            base_url: format!("http://{profile_addr}"),
            api_key_env: PROFILE_KEY_ENV.to_string(),
            format: "openai".to_string(),
            serves: Vec::new(),
            model_map: IndexMap::from([("*".to_string(), "target/Model".to_string())]),
        },
    );
    config.profiles = profiles;
    config.policy.mode = "all".to_string();
    config.policy.active_profile = Some("fallback".to_string());
    let relay = serve_relay_with(config, None).await;

    let client = reqwest::Client::new();
    client
        .get(format!("http://{relay}/v1/limit"))
        .send()
        .await
        .expect("limit request failed")
        .bytes()
        .await
        .expect("failed to read limit body");
    for _ in 0..200 {
        if status(relay).await["state"] == "LIMITED" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let response = client
        .post(format!("http://{relay}/v1/messages"))
        .header("authorization", CLIENT_AUTH)
        .header("x-api-key", CLIENT_API_KEY)
        .header("anthropic-beta", CLIENT_BETA)
        .body(r#"{"model":"claude-opus-4-6","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    response.bytes().await.expect("failed to read body");
    assert_eq!(
        seen.lock().expect("poisoned").as_deref(),
        Some(&format!("Bearer {PROFILE_KEY}")[..]),
        "the profile key must really have been sent for its absence from the logs to mean anything"
    );

    let logs = buffer.logs_containing("route=\"fallback\"").await;

    // Guards against a vacuous pass: the capture really is seeing the fallback
    // request's own log line.
    assert!(
        logs.contains("profile=\"fallback\""),
        "captured logs:\n{logs}"
    );
    assert!(
        logs.contains("model_out=\"target/Model\""),
        "captured logs:\n{logs}"
    );

    for secret in [
        CLIENT_AUTH,
        CLIENT_API_KEY,
        CLIENT_BETA,
        "sk-ant",
        PROFILE_KEY,
        "DO-NOT-LOG",
    ] {
        assert!(
            !logs.contains(secret),
            "captured logs leaked {secret:?}:\n{logs}"
        );
    }
}
