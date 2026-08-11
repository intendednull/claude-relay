mod common;

use std::path::Path;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::response::Response;
use axum::routing::any;
use serde_json::Value;

use common::{dripped_body, serve, serve_relay_with_capture, truncated_body, unique_temp_dir};

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
/// budget anyway, consistent with this repo's other async-completion tests. The
/// file is created before it is filled, so a fixture that doesn't parse yet is
/// one to keep waiting on rather than a failure.
async fn wait_for_fixture(dir: &Path) -> (std::path::PathBuf, Value) {
    for _ in 0..100 {
        if let Ok(entries) = std::fs::read_dir(dir) {
            let mut paths: Vec<_> = entries.filter_map(|e| e.ok()).map(|e| e.path()).collect();
            paths.sort();
            if let Some(path) = paths.first()
                && let Ok(contents) = std::fs::read_to_string(path)
                && let Ok(fixture) = serde_json::from_str(&contents)
            {
                return (path.clone(), fixture);
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

    let (fixture_path, fixture) = wait_for_fixture(&capture_dir).await;
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

    assert_eq!(fixture["status"], 429);
    assert_eq!(fixture["body"], RATE_LIMITED_BODY);
    assert!(
        fixture.get("truncated").is_none(),
        "a body that ended cleanly must not be marked truncated"
    );
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

    let (_, fixture) = wait_for_fixture(&capture_dir).await;

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

const CAPTURE_BODY_CAP: usize = 1024 * 1024;

/// An upstream that is broken or hostile must not be able to make the proxy
/// hold its whole error body: the fixture stops at the cap, the client does not.
#[tokio::test]
async fn an_oversized_error_body_is_capped_and_marked_truncated() {
    let oversized = "x".repeat(CAPTURE_BODY_CAP + 4096);
    let expected_len = oversized.len();
    let upstream = serve(Router::new().route(
        "/v1/messages",
        any(move || {
            let body = oversized.clone();
            async move {
                Response::builder()
                    .status(429)
                    .body(Body::from(body))
                    .expect("failed to build mock response")
            }
        }),
    ))
    .await;
    let capture_dir = unique_temp_dir("capture-oversized-test");
    let relay =
        serve_relay_with_capture(format!("http://{upstream}"), Some(capture_dir.clone())).await;

    let response = reqwest::Client::new()
        .post(format!("http://{relay}/v1/messages"))
        .body("{}")
        .send()
        .await
        .expect("request failed");
    assert_eq!(response.status(), 429);
    let body = response
        .bytes()
        .await
        .expect("failed to read response body");
    assert_eq!(
        body.len(),
        expected_len,
        "capping the fixture must not cap what the client receives"
    );

    let (_, fixture) = wait_for_fixture(&capture_dir).await;
    assert_eq!(
        fixture["body"].as_str().expect("missing body field").len(),
        CAPTURE_BODY_CAP,
        "the fixture body must stop at the cap"
    );
    assert_eq!(
        fixture["truncated"], true,
        "a capped fixture must say it is partial"
    );

    let _ = std::fs::remove_dir_all(&capture_dir);
}

#[tokio::test]
async fn an_upstream_dying_mid_body_marks_the_fixture_truncated() {
    let upstream = serve(Router::new().route(
        "/v1/messages",
        any(|| async {
            Response::builder()
                .status(500)
                .body(truncated_body(r#"{"error":{"type":"over"#))
                .expect("failed to build mock response")
        }),
    ))
    .await;
    let capture_dir = unique_temp_dir("capture-midstream-error-test");
    let relay =
        serve_relay_with_capture(format!("http://{upstream}"), Some(capture_dir.clone())).await;

    let response = reqwest::Client::new()
        .post(format!("http://{relay}/v1/messages"))
        .body("{}")
        .send()
        .await
        .expect("request failed");
    assert_eq!(response.status(), 500);
    assert!(
        response.bytes().await.is_err(),
        "the client stream must fail rather than end cleanly"
    );

    let (_, fixture) = wait_for_fixture(&capture_dir).await;
    assert_eq!(
        fixture["truncated"], true,
        "a body cut short by the upstream must not read as a complete one"
    );
    assert_eq!(fixture["body"], r#"{"error":{"type":"over"#);

    let _ = std::fs::remove_dir_all(&capture_dir);
}

#[tokio::test]
async fn a_client_hangup_marks_the_fixture_truncated() {
    let upstream = serve(Router::new().route(
        "/v1/messages",
        any(|| async {
            Response::builder()
                .status(503)
                .body(dripped_body(
                    vec!["first-", "second-", "third"],
                    Duration::from_millis(300),
                ))
                .expect("failed to build mock response")
        }),
    ))
    .await;
    let capture_dir = unique_temp_dir("capture-client-hangup-test");
    let relay =
        serve_relay_with_capture(format!("http://{upstream}"), Some(capture_dir.clone())).await;

    let mut response = reqwest::Client::new()
        .post(format!("http://{relay}/v1/messages"))
        .body("{}")
        .send()
        .await
        .expect("request failed");
    assert_eq!(response.status(), 503);
    let first = response
        .chunk()
        .await
        .expect("failed to read the first chunk")
        .expect("stream ended before the first chunk");
    assert_eq!(first, "first-".as_bytes());
    drop(response);

    let (_, fixture) = wait_for_fixture(&capture_dir).await;
    assert_eq!(
        fixture["truncated"], true,
        "a body the client stopped reading must not read as a complete one"
    );

    let _ = std::fs::remove_dir_all(&capture_dir);
}
