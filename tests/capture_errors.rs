mod common;

use std::path::Path;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::response::Response;
use axum::routing::any;
use serde_json::Value;

use common::{serve, serve_relay_with_capture, unique_temp_dir};

const RATE_LIMITED_BODY: &str = r#"{"error":{"type":"rate_limit_error","message":"limited"}}"#;
const OK_BODY: &str = r#"{"ok":true}"#;

async fn rate_limited(_req: Request) -> Response {
    Response::builder()
        .status(429)
        .header("retry-after", "42")
        .header("anthropic-ratelimit-requests-remaining", "0")
        // A spoofed auth-shaped response header: proves redaction applies
        // defensively even where Anthropic would never actually send one.
        .header("authorization", "Bearer should-never-appear-in-a-fixture")
        .header("content-type", "application/json")
        .body(Body::from(RATE_LIMITED_BODY))
        .expect("failed to build mock response")
}

async fn ok(_req: Request) -> Response {
    Response::builder()
        .status(200)
        .body(Body::from(OK_BODY))
        .expect("failed to build mock response")
}

async fn rate_limited_with_duplicate_headers(_req: Request) -> Response {
    Response::builder()
        .status(429)
        .header("x-request-id", "req-1")
        .header("x-request-id", "req-2")
        .header("set-cookie", "a=1")
        .header("set-cookie", "b=2")
        .body(Body::from(RATE_LIMITED_BODY))
        .expect("failed to build mock response")
}

/// Fixture writing happens after the stream reports its end, which is itself
/// ordered before the client sees end-of-body — but poll under a small retry
/// budget anyway, consistent with this repo's other async-completion tests.
async fn wait_for_fixture(dir: &Path) -> std::path::PathBuf {
    for _ in 0..100 {
        if let Ok(entries) = std::fs::read_dir(dir) {
            let mut files: Vec<_> = entries.filter_map(|e| e.ok()).collect();
            if !files.is_empty() {
                return files.remove(0).path();
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "timed out waiting for a --capture-errors fixture in {}",
        dir.display()
    );
}

#[tokio::test]
async fn non_2xx_response_writes_a_redacted_fixture() {
    let upstream = serve(Router::new().route("/v1/messages", any(rate_limited))).await;
    let capture_dir = unique_temp_dir("capture-fixture-test");
    let relay =
        serve_relay_with_capture(format!("http://{upstream}"), Some(capture_dir.clone())).await;

    let response = reqwest::Client::new()
        .post(format!("http://{relay}/v1/messages"))
        .body("{}")
        .send()
        .await
        .expect("request failed");
    assert_eq!(
        response.status(),
        429,
        "capturing a fixture must not change what the client receives"
    );
    let body = response
        .bytes()
        .await
        .expect("failed to read response body");
    assert_eq!(
        body,
        RATE_LIMITED_BODY.as_bytes(),
        "the response body must still stream to the client unchanged"
    );

    let fixture_path = wait_for_fixture(&capture_dir).await;
    let file_name = fixture_path
        .file_name()
        .and_then(|n| n.to_str())
        .expect("fixture path has no file name");
    assert!(
        file_name.ends_with("-429.json"),
        "fixture file name should be `<n>-429.json`, got {file_name}"
    );
    assert!(
        file_name.chars().next().unwrap().is_ascii_digit(),
        "fixture file name should start with the counter, got {file_name}"
    );

    let contents = std::fs::read_to_string(&fixture_path).expect("failed to read fixture");
    let fixture: Value = serde_json::from_str(&contents).expect("fixture is not valid json");

    assert_eq!(fixture["status"], 429);
    assert_eq!(fixture["body"], RATE_LIMITED_BODY);
    assert_eq!(
        fixture["headers"]["retry-after"], "42",
        "retry-after must survive verbatim"
    );
    assert_eq!(
        fixture["headers"]["anthropic-ratelimit-requests-remaining"], "0",
        "anthropic-ratelimit-* headers must survive verbatim"
    );
    assert_eq!(
        fixture["headers"]["authorization"], "[REDACTED]",
        "authorization value must be redacted even on a response"
    );

    let _ = std::fs::remove_dir_all(&capture_dir);
}

#[tokio::test]
async fn repeated_response_headers_all_survive_in_the_fixture() {
    let upstream =
        serve(Router::new().route("/v1/messages", any(rate_limited_with_duplicate_headers))).await;
    let capture_dir = unique_temp_dir("capture-duplicate-headers-test");
    let relay =
        serve_relay_with_capture(format!("http://{upstream}"), Some(capture_dir.clone())).await;

    let response = reqwest::Client::new()
        .post(format!("http://{relay}/v1/messages"))
        .body("{}")
        .send()
        .await
        .expect("request failed");
    assert_eq!(response.status(), 429);
    response
        .bytes()
        .await
        .expect("failed to read response body");

    let fixture_path = wait_for_fixture(&capture_dir).await;
    let contents = std::fs::read_to_string(&fixture_path).expect("failed to read fixture");
    let fixture: Value = serde_json::from_str(&contents).expect("fixture is not valid json");

    assert_eq!(
        fixture["headers"]["x-request-id"],
        serde_json::json!(["req-1", "req-2"]),
        "a repeated non-sensitive header must not collapse to its last value"
    );
    assert_eq!(
        fixture["headers"]["set-cookie"],
        serde_json::json!(["[REDACTED]", "[REDACTED]"]),
        "a repeated sensitive header must redact every occurrence, not collapse to one"
    );

    let _ = std::fs::remove_dir_all(&capture_dir);
}

#[tokio::test]
async fn success_response_does_not_write_a_fixture() {
    let upstream = serve(Router::new().route("/v1/messages", any(ok))).await;
    let capture_dir = unique_temp_dir("capture-no-fixture-test");
    let relay =
        serve_relay_with_capture(format!("http://{upstream}"), Some(capture_dir.clone())).await;

    let response = reqwest::Client::new()
        .post(format!("http://{relay}/v1/messages"))
        .body("{}")
        .send()
        .await
        .expect("request failed");
    assert_eq!(response.status(), 200);
    response
        .bytes()
        .await
        .expect("failed to read response body");

    // No terminal-event race to win here: give a wrongly-firing write a
    // moment, then assert the directory (created at startup) stayed empty.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let entries: Vec<_> = std::fs::read_dir(&capture_dir)
        .expect("capture dir should have been created at startup")
        .collect();
    assert!(
        entries.is_empty(),
        "a 2xx response must never produce a capture-errors fixture"
    );

    let _ = std::fs::remove_dir_all(&capture_dir);
}
