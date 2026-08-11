mod common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::any;
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use common::{relay_config, serve, serve_relay_with, unique_temp_dir};

/// Spec §5's expected shape, with a message carrying the subscription marker.
const LIMIT_BODY: &str = r#"{"type":"error","error":{"type":"rate_limit_error","message":"You have reached your Claude Pro usage limit. Your limit will reset at 6pm."}}"#;
/// The 429 the proxy must ignore: same status and error type, per-minute
/// wording, and a short window (Global Constraint 6).
const BURST_BODY: &str = r#"{"type":"error","error":{"type":"rate_limit_error","message":"Number of requests has exceeded your per-minute rate limit."}}"#;
const OK_BODY: &str = r#"{"id":"msg_ok","content":[{"type":"text","text":"hi"}]}"#;
const SERVER_ERROR_BODY: &str =
    r#"{"type":"error","error":{"type":"api_error","message":"Internal server error"}}"#;

const LIMIT_RETRY_AFTER: u64 = 3600;
const BURST_RETRY_AFTER: u64 = 12;
/// `detect.max_reset_horizon_secs`'s default, restated so the test fails if it
/// changes silently.
const MAX_RESET_HORIZON_SECS: u64 = 7 * 24 * 60 * 60;

/// A reset time in epoch *milliseconds*, which the default rule reads as
/// seconds — the units mistake the ceiling exists to survive.
fn epoch_millis_reset() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_secs();
    format!("{secs}000")
}

fn error_response(status: StatusCode, retry_after: u64, body: &'static str) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("retry-after", retry_after.to_string())
        .body(Body::from(body))
        .expect("failed to build mock response")
}

/// One upstream serving every response shape these tests need, on a path each,
/// so a single relay can be driven through a sequence of them.
fn upstream() -> Router {
    Router::new()
        .route(
            "/v1/limit",
            any(|| async {
                error_response(StatusCode::TOO_MANY_REQUESTS, LIMIT_RETRY_AFTER, LIMIT_BODY)
            }),
        )
        .route(
            "/v1/burst",
            any(|| async {
                error_response(StatusCode::TOO_MANY_REQUESTS, BURST_RETRY_AFTER, BURST_BODY)
            }),
        )
        .route(
            "/v1/wrong-unit",
            any(|| async {
                Response::builder()
                    .status(StatusCode::TOO_MANY_REQUESTS)
                    .header("content-type", "application/json")
                    .header("anthropic-ratelimit-unified-reset", epoch_millis_reset())
                    .body(Body::from(LIMIT_BODY))
                    .expect("failed to build mock response")
            }),
        )
        .route(
            "/v1/server-error",
            any(|| async {
                error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    BURST_RETRY_AFTER,
                    SERVER_ERROR_BODY,
                )
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

/// Drives one request through the relay and returns the response the *client*
/// saw, so every test also checks detection left it alone.
async fn call(relay: SocketAddr, path: &str) -> (StatusCode, String, Option<String>) {
    let response = reqwest::Client::new()
        .post(format!("http://{relay}{path}"))
        .body(r#"{"model":"claude-opus-5","messages":[]}"#)
        .send()
        .await
        .expect("request failed");
    let status = response.status();
    let retry_after = response
        .headers()
        .get("retry-after")
        .map(|value| value.to_str().expect("non-utf8 header").to_string());
    let body = response.text().await.expect("failed to read body");
    (status, body, retry_after)
}

async fn status_body(relay: SocketAddr) -> Value {
    let response = reqwest::Client::new()
        .get(format!("http://{relay}/status"))
        .send()
        .await
        .expect("status request failed");
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.bytes().await.expect("failed to read /status body");
    serde_json::from_slice(&bytes).expect("invalid /status json")
}

/// Outcomes are applied off the request path, so tests wait for the state they
/// expect instead of assuming it has landed.
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

fn horizon_secs(limited_until: &Value) -> i64 {
    let text = limited_until
        .as_str()
        .expect("limited_until should be a timestamp string");
    let until = OffsetDateTime::parse(text, &Rfc3339).expect("limited_until should be RFC3339");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_secs() as i64;
    until.unix_timestamp() - now
}

fn seed_state_file(contents: &str) -> PathBuf {
    let path = unique_temp_dir("route-state").with_extension("json");
    std::fs::write(&path, contents).expect("failed to seed state file");
    path
}

async fn relay_over(upstream_addr: SocketAddr) -> SocketAddr {
    serve_relay_with(relay_config(format!("http://{upstream_addr}")), None).await
}

async fn relay_with_state_file(upstream_addr: SocketAddr, state_file: PathBuf) -> SocketAddr {
    let mut config = relay_config(format!("http://{upstream_addr}"));
    config.state_file = Some(state_file);
    serve_relay_with(config, None).await
}

#[tokio::test]
async fn a_limit_shaped_429_flips_the_route_to_limited() {
    let relay = relay_over(serve(upstream()).await).await;
    assert_eq!(status_body(relay).await["state"], "ACTIVE");

    let (status, body, retry_after) = call(relay, "/v1/limit").await;

    // Detection is a side effect: the client still gets the upstream's own 429.
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body, LIMIT_BODY);
    assert_eq!(retry_after.as_deref(), Some("3600"));

    let status_body = wait_for_state(relay, "LIMITED").await;
    assert!(
        !status_body["limited_until"].is_null(),
        "LIMITED without a limited_until leaves an operator no way to see the window"
    );
    let horizon = horizon_secs(&status_body["limited_until"]);
    assert!(
        (LIMIT_RETRY_AFTER as i64 + 10..=LIMIT_RETRY_AFTER as i64 + 60).contains(&horizon),
        "limited_until should be the reported reset plus 15-60s of jitter, got {horizon}s"
    );
    assert_eq!(status_body["fallback_requests_served"], 0);
}

/// The wrong-unit case end to end: epoch *milliseconds* read by a rule
/// expecting seconds is ~55,000 years out. Unbounded it would be persisted,
/// outlive every restart, never elapse, and show up as `LIMITED` with a
/// `limited_until` too far out for `/status` to even render.
#[tokio::test]
async fn a_wrong_unit_reset_still_produces_a_window_an_operator_can_read() {
    let relay = relay_over(serve(upstream()).await).await;

    call(relay, "/v1/wrong-unit").await;

    let body = wait_for_state(relay, "LIMITED").await;
    assert!(
        !body["limited_until"].is_null(),
        "a bounded window must always render"
    );
    let horizon = horizon_secs(&body["limited_until"]);
    assert!(
        horizon <= MAX_RESET_HORIZON_SECS as i64 + 60,
        "the window must be capped at the configured ceiling, got {horizon}s"
    );
}

/// The negative case Global Constraint 6 is about. The proof that the burst was
/// *processed* and ignored, rather than merely not processed yet, is the
/// limit-shaped 429 that follows it: outcomes are applied in order, and a
/// `LimitDetected` on an already-`Limited` route is a no-op — so a state
/// carrying the long window can only mean the burst never took it there.
#[tokio::test]
async fn a_burst_429_does_not_flip_the_route() {
    let relay = relay_over(serve(upstream()).await).await;

    let (status, body, _) = call(relay, "/v1/burst").await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body, BURST_BODY);

    call(relay, "/v1/limit").await;

    let horizon = horizon_secs(&wait_for_state(relay, "LIMITED").await["limited_until"]);
    assert!(
        horizon > BURST_RETRY_AFTER as i64 + 60,
        "the burst's {BURST_RETRY_AFTER}s window must never have been applied, got {horizon}s"
    );
}

/// Same conservative rule, checked where a wrong classification would be
/// visible immediately: from `PROBING`, a mistaken match goes to `LIMITED` and
/// the success that follows can no longer recover the route.
#[tokio::test]
async fn responses_that_are_not_the_limit_signature_leave_probing_intact() {
    for path in ["/v1/burst", "/v1/server-error"] {
        let state_file = seed_state_file(r#"{"state":"PROBING","until":null}"#);
        let relay = relay_with_state_file(serve(upstream()).await, state_file.clone()).await;
        assert_eq!(status_body(relay).await["state"], "PROBING");

        call(relay, path).await;
        call(relay, "/v1/messages").await;

        let body = wait_for_state(relay, "ACTIVE").await;
        assert!(
            body["limited_until"].is_null(),
            "{path} must not have set a limit window"
        );
        let _ = std::fs::remove_file(&state_file);
    }
}

#[tokio::test]
async fn a_success_while_probing_returns_the_route_to_active() {
    let state_file = seed_state_file(r#"{"state":"PROBING","until":null}"#);
    let relay = relay_with_state_file(serve(upstream()).await, state_file.clone()).await;
    assert_eq!(status_body(relay).await["state"], "PROBING");

    let (status, body, _) = call(relay, "/v1/messages").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, OK_BODY);

    wait_for_state(relay, "ACTIVE").await;
    let _ = std::fs::remove_file(&state_file);
}

#[tokio::test]
async fn a_limit_while_probing_limits_the_route_again() {
    let state_file = seed_state_file(r#"{"state":"PROBING","until":null}"#);
    let relay = relay_with_state_file(serve(upstream()).await, state_file.clone()).await;

    call(relay, "/v1/limit").await;

    let horizon = horizon_secs(&wait_for_state(relay, "LIMITED").await["limited_until"]);
    assert!(horizon > LIMIT_RETRY_AFTER as i64);
    let _ = std::fs::remove_file(&state_file);
}

/// `--capture-errors` is a debug flag; detection must not depend on it either
/// way, so it is checked with the flag on as well as off (every other test
/// here runs with it off).
#[tokio::test]
async fn detection_and_capture_coexist() {
    let dir = unique_temp_dir("limit-detection-capture");
    let relay = serve_relay_with(
        relay_config(format!("http://{}", serve(upstream()).await)),
        Some(dir.clone()),
    )
    .await;

    let (_, body, _) = call(relay, "/v1/limit").await;
    assert_eq!(body, LIMIT_BODY);

    wait_for_state(relay, "LIMITED").await;

    let fixtures: Vec<_> = std::fs::read_dir(&dir)
        .expect("capture dir should exist")
        .map(|entry| entry.expect("failed to read dir entry").path())
        .collect();
    assert_eq!(fixtures.len(), 1, "the 429 should still be captured");
    let fixture: Value = serde_json::from_str(
        &std::fs::read_to_string(&fixtures[0]).expect("failed to read fixture"),
    )
    .expect("invalid fixture json");
    assert_eq!(fixture["body"], LIMIT_BODY);

    let _ = std::fs::remove_dir_all(&dir);
}

/// A restart mid-limit must not hammer Anthropic (spec §4), which is the whole
/// point of `state_file` — checked end to end, not just in the state machine.
#[tokio::test]
async fn a_detected_limit_survives_a_restart() {
    let upstream_addr = serve(upstream()).await;
    let state_file = unique_temp_dir("route-state-restart").with_extension("json");

    let relay = relay_with_state_file(upstream_addr, state_file.clone()).await;
    call(relay, "/v1/limit").await;
    let before = wait_for_state(relay, "LIMITED").await;

    let restarted = relay_with_state_file(upstream_addr, state_file.clone()).await;
    let after = status_body(restarted).await;

    assert_eq!(after["state"], "LIMITED");
    assert_eq!(after["limited_until"], before["limited_until"]);
    let _ = std::fs::remove_file(&state_file);
}

/// Detection reads the body as a side effect of forwarding it; a body that
/// arrives in pieces must still reach the client whole, and still classify.
#[tokio::test]
async fn a_chunked_error_body_is_forwarded_intact_and_still_classifies() {
    let upstream_addr = serve(Router::new().route(
        "/v1/messages",
        any(|| async {
            let (head, tail) = LIMIT_BODY.split_at(40);
            Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .header("retry-after", LIMIT_RETRY_AFTER.to_string())
                .body(common::dripped_body(
                    vec![head, tail],
                    Duration::from_millis(20),
                ))
                .expect("failed to build mock response")
        }),
    ))
    .await;
    let relay = relay_over(upstream_addr).await;

    let (status, body, _) = call(relay, "/v1/messages").await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body, LIMIT_BODY);

    wait_for_state(relay, "LIMITED").await;
}
