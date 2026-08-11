mod common;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::any;
use serde_json::Value;

use common::{relay_config, serve, serve_relay_with, unique_temp_dir};
use relay::config::NotifyConfig;

/// Spec §5's expected shape, carrying the subscription marker, so it classifies.
const LIMIT_BODY: &str = r#"{"type":"error","error":{"type":"rate_limit_error","message":"You have reached your Claude Pro usage limit."}}"#;
const OK_BODY: &str = r#"{"id":"msg_ok","content":[{"type":"text","text":"hi"}]}"#;
const LIMIT_RETRY_AFTER: u64 = 3600;

fn upstream() -> Router {
    Router::new()
        .route(
            "/v1/limit",
            any(|| async {
                Response::builder()
                    .status(StatusCode::TOO_MANY_REQUESTS)
                    .header("content-type", "application/json")
                    .header("retry-after", LIMIT_RETRY_AFTER.to_string())
                    .body(Body::from(LIMIT_BODY))
                    .expect("failed to build mock response")
            }),
        )
        .route(
            "/v1/messages",
            any(|| async {
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Body::from(OK_BODY))
                    .expect("failed to build mock response")
            }),
        )
}

/// A hook that appends spec §4's three env vars, one line per event.
fn logging_hook(log: &Path) -> String {
    format!(
        r#"printf '%s|%s|%s\n' "$RELAY_EVENT" "$RELAY_RESET_AT" "$RELAY_DETAIL" >> {}"#,
        log.display()
    )
}

fn hook_log_path(label: &str) -> PathBuf {
    unique_temp_dir(label).with_extension("log")
}

async fn relay_with_hook(
    command: String,
    timeout_secs: u64,
    state_file: Option<PathBuf>,
) -> SocketAddr {
    let upstream_addr = serve(upstream()).await;
    let mut config = relay_config(format!("http://{upstream_addr}"));
    config.state_file = state_file;
    config.notify = NotifyConfig {
        command: Some(command),
        timeout_secs,
    };
    serve_relay_with(config, None).await
}

fn seed_probing_state_file(label: &str) -> PathBuf {
    let path = unique_temp_dir(label).with_extension("json");
    std::fs::write(&path, r#"{"state":"PROBING","until":null}"#).expect("failed to seed state");
    path
}

async fn call(relay: SocketAddr, path: &str) -> StatusCode {
    reqwest::Client::new()
        .post(format!("http://{relay}{path}"))
        .body(r#"{"model":"claude-opus-5","messages":[]}"#)
        .send()
        .await
        .expect("request failed")
        .status()
}

async fn status_body(relay: SocketAddr) -> Value {
    let response = reqwest::Client::new()
        .get(format!("http://{relay}/status"))
        .send()
        .await
        .expect("status request failed");
    let bytes = response.bytes().await.expect("failed to read /status body");
    serde_json::from_slice(&bytes).expect("invalid /status json")
}

async fn wait_for_state(relay: SocketAddr, expected: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let body = status_body(relay).await;
        if body["state"] == expected {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {expected}, still {}",
            body["state"]
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The hook runs in a subprocess, so its output is waited for rather than
/// assumed to have landed.
async fn wait_for_hook_lines(log: &Path, count: usize) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let lines: Vec<String> = std::fs::read_to_string(log)
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect();
        if lines.len() >= count {
            return lines;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {count} hook line(s), got {lines:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn fields(line: &str) -> Vec<&str> {
    line.split('|').collect()
}

#[tokio::test]
async fn a_detected_limit_fires_failover_engaged_with_the_reset_time() {
    let log = hook_log_path("notify-failover");
    let relay = relay_with_hook(logging_hook(&log), 5, None).await;

    assert_eq!(
        call(relay, "/v1/limit").await,
        StatusCode::TOO_MANY_REQUESTS
    );

    let lines = wait_for_hook_lines(&log, 1).await;
    assert_eq!(
        lines.len(),
        1,
        "one transition, one notification: {lines:?}"
    );
    let fields = fields(&lines[0]);
    assert_eq!(fields[0], "failover_engaged");
    // The window the operator was told about is the one `/status` reports.
    let limited_until = wait_for_state(relay, "LIMITED").await["limited_until"]
        .as_str()
        .expect("LIMITED should carry a window")
        .to_string();
    assert_eq!(fields[1], limited_until);
    assert_eq!(
        fields[2],
        format!("anthropic route limited until {}", fields[1])
    );

    let _ = std::fs::remove_file(&log);
}

#[tokio::test]
async fn a_success_while_probing_fires_recovered() {
    let log = hook_log_path("notify-recovered");
    let state_file = seed_probing_state_file("notify-recovered-state");
    let relay = relay_with_hook(logging_hook(&log), 5, Some(state_file.clone())).await;
    assert_eq!(status_body(relay).await["state"], "PROBING");

    assert_eq!(call(relay, "/v1/messages").await, StatusCode::OK);

    let lines = wait_for_hook_lines(&log, 1).await;
    assert_eq!(
        fields(&lines[0]),
        vec!["recovered", "", "anthropic route recovered"]
    );

    let _ = std::fs::remove_file(&log);
    let _ = std::fs::remove_file(&state_file);
}

/// A re-limit from `PROBING` is a fresh failover, not a recovery — the one
/// pair of transitions that could plausibly be mapped to the wrong event.
#[tokio::test]
async fn a_limit_while_probing_fires_failover_engaged_again() {
    let log = hook_log_path("notify-relimit");
    let state_file = seed_probing_state_file("notify-relimit-state");
    let relay = relay_with_hook(logging_hook(&log), 5, Some(state_file.clone())).await;

    call(relay, "/v1/limit").await;

    let lines = wait_for_hook_lines(&log, 1).await;
    let fields = fields(&lines[0]);
    assert_eq!(fields[0], "failover_engaged");
    assert!(!fields[1].is_empty(), "a re-limit still carries its window");

    let _ = std::fs::remove_file(&log);
    let _ = std::fs::remove_file(&state_file);
}

/// The property Global Constraint 7 exists for, checked on both sides of the
/// request path: a hook that never returns delays neither the client's response
/// nor any *later* route state change. The second half is the one that matters
/// most — transitions are applied on a single thread, so a notifier that could
/// block it would silently stop route tracking for the whole process.
#[tokio::test]
async fn a_hanging_hook_delays_neither_the_response_nor_later_state_changes() {
    let log = hook_log_path("notify-hanging");
    let state_file = seed_probing_state_file("notify-hanging-state");
    // Records that it started, then hangs well past every assertion below; its
    // matching timeout guarantees it is still running throughout. `exec` with
    // the pipes redirected so the hook, once this test process is gone, holds
    // no fd of the test harness's and keeps nothing waiting on it.
    let relay = relay_with_hook(
        format!(
            "echo started >> {}; exec sleep 30 >/dev/null 2>&1",
            log.display()
        ),
        30,
        Some(state_file.clone()),
    )
    .await;

    let started = Instant::now();
    assert_eq!(call(relay, "/v1/messages").await, StatusCode::OK);
    let response_time = started.elapsed();
    assert!(
        response_time < Duration::from_secs(5),
        "the hook must not be in the response path, took {response_time:?}"
    );

    // The recovery hook is now running and will not return.
    wait_for_hook_lines(&log, 1).await;

    // A second transition, applied on the same thread that just fired that
    // hook: reaching LIMITED proves the applier was never waiting on it.
    let before_second = Instant::now();
    call(relay, "/v1/limit").await;
    let body = wait_for_state(relay, "LIMITED").await;
    assert!(!body["limited_until"].is_null());
    assert!(
        before_second.elapsed() < Duration::from_secs(10),
        "state tracking stalled behind the hanging hook"
    );

    let _ = std::fs::remove_file(&log);
    let _ = std::fs::remove_file(&state_file);
}
