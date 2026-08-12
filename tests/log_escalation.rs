//! The escalation log line (spec §7e): an operator who sees a surprise bill for
//! the largest model configured has to be able to find out why the relay went
//! there, so every hop says which model could not fit the prompt, which one it
//! moved to, and that a context limit is the reason.
//!
//! Also the negative half: a hop is decided from the *transcript's* size, and the
//! transcript is the one thing that must never appear in a log line.
//!
//! A separate binary because the subscriber is process-global — one test per
//! binary, as in `log_hygiene.rs` and `log_hygiene_provider_error.rs`.

mod common;

use std::time::Duration;

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

const PROFILE_KEY_ENV: &str = "RELAY_TEST_ESCALATION_PROFILE_KEY";
const PROFILE_KEY: &str = "tgp-DO-NOT-LOG-THIS-ESCALATION-KEY";

/// The live map's bottom two rungs: 131k, then 262k.
const SMALL: &str = "openai/gpt-oss-20b";
const MEDIUM: &str = "moonshotai/Kimi-K2.7-Code";

/// What the client sent. A hop is decided from how big this is, which is exactly
/// why none of it may be logged.
const TRANSCRIPT: &str = "SECRET-TRANSCRIPT-CONTENT-MUST-NOT-BE-LOGGED";

/// Anthropic's subscription-limit shape, which drives the route to `LIMITED` so a
/// `claude-*` request fails over and is remapped (§7a) — the only way a request
/// has a ladder position at all.
const LIMIT_BODY: &str = r#"{"type":"error","error":{"type":"rate_limit_error","message":"You have reached your Claude Pro usage limit. Your limit will reset at 6pm."}}"#;

/// Measured through the running Together AI service at 170,071 tokens against a
/// 131k model.
const TOGETHER_CONTEXT_LIMIT: &str = concat!(
    r#"{"id":"ovq5abc-1kFHot-a29afb844e986e7d","error":{"message":"The input (170071 tokens) "#,
    r#"is longer than the model's context length (131072 tokens).","#,
    r#""type":"invalid_request_error","param":null,"code":null}}"#
);

const OPENAI_COMPLETION: &str = concat!(
    r#"{"id":"chatcmpl-1","object":"chat.completion","model":"moonshotai/Kimi-K2.7-Code","#,
    r#""choices":[{"index":0,"message":{"role":"assistant","content":"fits here"},"#,
    r#""finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":5}}"#
);

fn json(status: StatusCode, body: &'static str) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .expect("failed to build mock response")
}

#[tokio::test]
async fn every_escalation_names_both_models_and_never_the_transcript() {
    // SAFETY: the only write to this variable in this process, and it happens
    // before the relay that reads it exists.
    unsafe { std::env::set_var(PROFILE_KEY_ENV, PROFILE_KEY) };

    let buffer = Buffer::new();
    tracing_subscriber::fmt()
        .with_writer(buffer.clone())
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .init();

    let anthropic = serve(Router::new().route(
        "/v1/messages",
        any(|| async { json(StatusCode::TOO_MANY_REQUESTS, LIMIT_BODY) }),
    ))
    .await;
    let provider = serve(Router::new().route(
        "/v1/chat/completions",
        any(|request: Request| async move {
            let bytes = axum::body::to_bytes(request.into_body(), 1024 * 1024)
                .await
                .expect("failed to read the outgoing body");
            let body: Value = serde_json::from_slice(&bytes).expect("the outgoing body is JSON");
            if body["model"] == SMALL {
                json(StatusCode::BAD_REQUEST, TOGETHER_CONTEXT_LIMIT)
            } else {
                json(StatusCode::OK, OPENAI_COMPLETION)
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
            serves: Vec::new(),
            model_map: [("claude-haiku", SMALL), ("claude-sonnet", MEDIUM)]
                .into_iter()
                .map(|(slot, target)| (slot.to_string(), target.to_string()))
                .collect(),
        },
    );
    config.profiles = profiles;
    config.policy.active_profile = Some("fallback".to_string());
    config.policy.mode = "all".to_string();
    let relay = serve_relay_with(config, None).await;

    let client = reqwest::Client::new();
    let request = |model: &str| {
        client
            .post(format!("http://{relay}/v1/messages"))
            .header("content-type", "application/json")
            .body(format!(
                r#"{{"model":"{model}","max_tokens":8,"messages":[{{"role":"user","content":"{TRANSCRIPT}"}}]}}"#
            ))
            .send()
    };

    // The first request trips the limit and is failed over on detection
    // (`failover_on_detect`), which is also how the route becomes `LIMITED`.
    let response = request("claude-haiku-4-5")
        .await
        .expect("the first request failed");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "the escalated attempt is what answers: {:?}",
        response.text().await
    );

    let logs = buffer.logs_containing("retrying one rung up").await;

    // The hop, named on both sides. `from_model` is what could not fit and
    // `to_model` is what the bill will show.
    assert!(
        logs.contains(&format!(r#"from_model="{SMALL}""#)),
        "the model that could not fit must be named:\n{logs}"
    );
    assert!(
        logs.contains(&format!(r#"to_model="{MEDIUM}""#)),
        "the model the operator is about to be billed for must be named:\n{logs}"
    );
    assert!(
        logs.contains(r#"reason="context_limit""#),
        "a hop with no reason is a bill with no explanation:\n{logs}"
    );
    assert!(
        logs.contains(r#"profile="fallback""#),
        "the profile that was climbed:\n{logs}"
    );

    // One hop, once. A second line here is a second upstream request paid for.
    assert_eq!(
        logs.lines()
            .filter(|line| line.contains("retrying one rung up"))
            .count(),
        1,
        "the ladder was walked more than once:\n{logs}"
    );

    // §9 counts one line per *upstream* request, so both attempts are auditable —
    // the one that overflowed and the one that answered.
    let attempts: Vec<&str> = logs
        .lines()
        .filter(|line| line.contains("proxied request") && line.contains(r#"route="fallback""#))
        .collect();
    assert_eq!(attempts.len(), 2, "one line per attempt:\n{logs}");
    assert!(
        attempts[0].contains(&format!(r#"model_out="{SMALL}""#))
            && attempts[0].contains("status=400"),
        "the first line is the attempt that overflowed:\n{logs}"
    );
    assert!(
        attempts[1].contains(&format!(r#"model_out="{MEDIUM}""#))
            && attempts[1].contains("status=200"),
        "the second is the one that answered:\n{logs}"
    );
    // Both carry the model the *client* asked for, which is what ties the two
    // lines to one client request.
    for line in &attempts {
        assert!(
            line.contains(r#"model_in="claude-haiku-4-5""#),
            "an attempt lost the client's own model name:\n{logs}"
        );
    }

    // Not one word of the conversation, and not the profile's key. The escalation
    // decision is *about* the transcript's size, which makes this the log line
    // most tempted to quote it.
    for secret in [TRANSCRIPT, PROFILE_KEY, "DO-NOT-LOG"] {
        assert!(
            !logs.contains(secret),
            "captured logs leaked {secret:?}:\n{logs}"
        );
    }

    // The provider's own error body is still logged once, per §7d — the hop does
    // not replace that line, or an operator debugging the overflow would have the
    // hop and not the reason for it.
    assert_eq!(
        logs.lines()
            .filter(|line| line.contains("the fallback provider returned an error"))
            .count(),
        1,
        "the overflowing attempt's body must be logged exactly once:\n{logs}"
    );

    // Nothing above depends on a sleep; this only proves the assertions were made
    // against a settled route rather than a racing one.
    for _ in 0..200 {
        let state: Value = serde_json::from_slice(
            &client
                .get(format!("http://{relay}/status"))
                .send()
                .await
                .expect("status request failed")
                .bytes()
                .await
                .expect("failed to read status"),
        )
        .expect("status must be JSON");
        if state["state"] == "LIMITED" {
            assert_eq!(
                state["fallback_requests_served"], 2,
                "both attempts are counted: the operator paid for both"
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the relay never reached LIMITED");
}
