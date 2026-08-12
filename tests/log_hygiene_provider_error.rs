//! The log line that carries a fallback provider's raw error body (spec §7d):
//! the envelope necessarily reshapes what the provider sent, so the original has
//! to stay findable — and an error body is provider-and-attacker-controlled and
//! unbounded, so it is capped, escaped, and stripped of the one credential that
//! ever reaches that provider.
//!
//! A separate binary because the subscriber is process-global — one test per
//! binary, as in `log_hygiene.rs` and `log_hygiene_fallback.rs`.

mod common;

use axum::Router;
use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::any;
use indexmap::IndexMap;
use serde_json::Value;

use common::{Buffer, relay_config, serve, serve_relay_with};
use relay::config::ProfileConfig;

const PROFILE_KEY_ENV: &str = "RELAY_TEST_PROVIDER_ERROR_PROFILE_KEY";
const PROFILE_KEY: &str = "tgp-DO-NOT-LOG-THIS-PROFILE-KEY";
const OPEN_MODEL: &str = "deepseek-ai/DeepSeek-V4";

/// Findable in the log, so the operator can still read what the provider said.
const HEAD: &str = "PROVIDER-SAID-THIS";
/// Past `LOGGED_ERROR_BODY_CHARS`, so it must be clipped away.
const TAIL: &str = "PAST-THE-CAP";

/// A provider error that is not JSON at all — the realistic shape for a gateway
/// sitting in front of the provider — carrying a real newline, a synthetic
/// `proxied request` record behind it, the profile's own key, and enough padding
/// to run past the cap.
fn hostile_body() -> String {
    format!(
        "{HEAD} key={PROFILE_KEY}\n  2026-08-12T12:00:00.000000Z  INFO relay::proxy: \
         proxied request route=\"anthropic\" profile=\"-\" model_in=\"FORGED-BY-PROVIDER\" \
         model_out=\"FORGED-BY-PROVIDER\" method=POST path=\"/v1/messages\" status=200 \
         latency_ms=1 response_bytes=1\n{padding}{TAIL}",
        padding = "x".repeat(600)
    )
}

#[tokio::test]
async fn a_provider_error_body_is_logged_capped_escaped_and_credential_free() {
    // SAFETY: the only write to this variable in this process, and it happens
    // before the relay that reads it exists.
    unsafe { std::env::set_var(PROFILE_KEY_ENV, PROFILE_KEY) };

    let buffer = Buffer::new();
    tracing_subscriber::fmt()
        .with_writer(buffer.clone())
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .init();

    let anthropic = serve(Router::new().route("/v1/messages", any(|| async { "unused" }))).await;
    let body = hostile_body();
    let provider = serve(Router::new().route(
        "/v1/chat/completions",
        any(move || {
            let body = body.clone();
            async move {
                Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header("content-type", "text/html")
                    .body(Body::from(body))
                    .expect("failed to build mock response")
            }
        }),
    ))
    .await;

    let mut config = relay_config(format!("http://{anthropic}"));
    let mut profiles = IndexMap::new();
    profiles.insert(
        "fallback".to_string(),
        ProfileConfig {
            base_url: format!("http://{provider}"),
            api_key_env: PROFILE_KEY_ENV.to_string(),
            format: "openai".to_string(),
            serves: vec!["deepseek-ai/".to_string()],
            model_map: IndexMap::new(),
        },
    );
    config.profiles = profiles;
    config.policy.active_profile = Some("fallback".to_string());
    let relay = serve_relay_with(config, None).await;

    // Name-routed (§7d), so Anthropic is never contacted and this is the only
    // error in play.
    let response = reqwest::Client::new()
        .post(format!("http://{relay}/v1/messages"))
        .header("content-type", "application/json")
        .body(format!(
            r#"{{"model":"{OPEN_MODEL}","max_tokens":8,"messages":[{{"role":"user","content":"hi"}}]}}"#
        ))
        .send()
        .await
        .expect("request failed");

    // The provider's status survives, and the client gets an envelope rather than
    // the hostile bytes — without this the log assertions below could pass on a
    // request that never reached the branch under test.
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.headers()["x-relay-route"], "fallback");
    let envelope: Value =
        serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
            .expect("the client must receive JSON, whatever the provider sent");
    assert_eq!(envelope["type"], "error");

    let logs = buffer
        .logs_containing("the fallback provider returned an error")
        .await;

    // Findable: the wrapper reshaped the body, so the log is where the original
    // has to remain readable.
    assert!(logs.contains(HEAD), "captured logs:\n{logs}");

    // Capped. The clip is what bounds log volume; escaping does not.
    assert!(
        !logs.contains(TAIL),
        "the logged body ran past the cap:\n{logs}"
    );

    // Escaped: the record is one line, and the provider's synthetic record is not
    // a record. `proxied request` itself does appear — this request emits a real
    // one — so the assertion is on the forgery's own field.
    assert_eq!(
        logs.lines()
            .filter(|line| line.contains("the fallback provider returned an error"))
            .count(),
        1,
        "the warn line must be exactly one line:\n{logs}"
    );
    assert!(
        !logs.contains(r#"model_in="FORGED-BY-PROVIDER""#),
        "a provider forged a proxied-request record:\n{logs}"
    );

    // Credential-free (Global Constraint 2): the profile's key is the one secret
    // that ever reaches this provider, and a provider may quote it back.
    for secret in [PROFILE_KEY, "DO-NOT-LOG"] {
        assert!(
            !logs.contains(secret),
            "captured logs leaked {secret:?}:\n{logs}"
        );
    }
    assert!(
        logs.contains("[REDACTED]"),
        "the key must be redacted rather than merely clipped away:\n{logs}"
    );
}
