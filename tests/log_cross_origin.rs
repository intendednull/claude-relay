//! The origin gate's refusal has to say why. It is the likeliest thing in this
//! codebase to reject a legitimate client nobody has tested, and without this
//! line the user sees a bare 403 from their own proxy with nothing to read.
//!
//! What it may say is bounded: the refusing header's *name* and the status,
//! never a header value — an `Origin` is attacker-controlled, and with
//! `--log-file` that line now persists. Its own test binary for the reason
//! `log_hygiene.rs` is one: the subscriber is process-global.

mod common;

use axum::http::StatusCode;

use common::{Buffer, closed_port, relay_config, serve_relay_with};

const HOSTILE_ORIGIN: &str = "http://evil.example";

#[tokio::test]
async fn a_refused_cross_origin_request_logs_the_header_that_refused_it() {
    let buffer = Buffer::new();
    tracing_subscriber::fmt()
        .with_writer(buffer.clone())
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .init();

    // A closed port: the gate refuses before any forwarding, and nothing here
    // may reach a network.
    let relay = serve_relay_with(
        relay_config(format!("http://{}", closed_port().await)),
        None,
    )
    .await;

    let response = reqwest::Client::new()
        .post(format!("http://{relay}/v1/messages"))
        .header("origin", HOSTILE_ORIGIN)
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-opus-5"}"#)
        .send()
        .await
        .expect("request failed");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    response.bytes().await.expect("failed to read body");

    let logs = buffer.logs_containing("cross-origin request refused").await;
    assert!(
        logs.contains("WARN"),
        "the refusal must be a WARN, not a lower level: {logs}"
    );
    assert!(
        logs.contains(r#"refused_by="origin""#),
        "the refusal must name the header that refused: {logs}"
    );
    assert!(
        logs.contains("status=403"),
        "the refusal must name the status the client saw: {logs}"
    );
    assert!(
        !logs.contains("evil.example"),
        "the refusal echoed the attacker-controlled Origin value: {logs}"
    );
}
