mod common;

use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::Request;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::any;
use tokio::task::JoinSet;

use common::{closed_port, dripped_body, serve, serve_relay, truncated_body};

const MOCK_BODY: &str = r#"{"id":"msg_mock","content":[{"type":"text","text":"hi"}]}"#;

/// Reports back everything the upstream saw, so assertions stay in the tests
/// rather than in a handler whose panic would surface as a 500.
async fn echo_with_status(status: StatusCode, request: Request) -> Response {
    let method = request.method().to_string();
    let uri = request.uri().to_string();
    let headers = request.headers().clone();
    let seen = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("<none>")
            .to_string()
    };
    let body = to_bytes(request.into_body(), 64 * 1024)
        .await
        .expect("failed to read mock request body");

    Response::builder()
        .status(status)
        .header("x-saw-method", method)
        .header("x-saw-uri", uri)
        .header("x-saw-host", seen("host"))
        .header("x-saw-relay-test", seen("x-relay-test"))
        .header("x-saw-authorization", seen("authorization"))
        .header("x-saw-body", String::from_utf8_lossy(&body).to_string())
        .header("x-mock-response-header", "response-value")
        .header("content-type", "application/json")
        .body(Body::from(MOCK_BODY))
        .expect("failed to build mock response")
}

async fn echo(request: Request) -> Response {
    echo_with_status(StatusCode::OK, request).await
}

async fn echo_teapot(request: Request) -> Response {
    echo_with_status(StatusCode::IM_A_TEAPOT, request).await
}

fn echo_upstream() -> Router {
    Router::new()
        .route("/v1/messages", any(echo))
        .route("/v1/messages/count_tokens", any(echo))
        .route("/v1/{*rest}", any(echo_teapot))
}

#[tokio::test]
async fn non_streaming_response_round_trips_byte_identically() {
    let upstream = serve(echo_upstream()).await;
    let relay = serve_relay(format!("http://{upstream}")).await;

    let response = reqwest::Client::new()
        .post(format!("http://{relay}/v1/messages"))
        .body(r#"{"model":"claude-opus-5","messages":[]}"#)
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-type"],
        "application/json",
        "upstream content-type should survive the proxy"
    );
    assert_eq!(
        response.headers()["x-saw-body"],
        r#"{"model":"claude-opus-5","messages":[]}"#,
        "upstream should receive the request body verbatim"
    );

    let body = response.bytes().await.expect("failed to read body");
    assert_eq!(body, MOCK_BODY.as_bytes());
}

#[tokio::test]
async fn headers_pass_through_in_both_directions() {
    let upstream = serve(echo_upstream()).await;
    let relay = serve_relay(format!("http://{upstream}")).await;

    let response = reqwest::Client::new()
        .post(format!("http://{relay}/v1/messages"))
        .header("x-relay-test", "request-value")
        .header("authorization", "Bearer sk-ant-test-header-passthrough")
        .body("{}")
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-saw-relay-test"], "request-value");
    assert_eq!(
        response.headers()["x-saw-authorization"],
        "Bearer sk-ant-test-header-passthrough",
        "auth headers must reach Anthropic verbatim"
    );
    assert_eq!(
        response.headers()["x-saw-host"],
        upstream.to_string(),
        "Host must be recomputed for the upstream connection"
    );
    assert_eq!(
        response.headers()["x-mock-response-header"],
        "response-value"
    );
}

#[tokio::test]
async fn count_tokens_is_forwarded() {
    let upstream = serve(echo_upstream()).await;
    let relay = serve_relay(format!("http://{upstream}")).await;

    let response = reqwest::Client::new()
        .post(format!("http://{relay}/v1/messages/count_tokens"))
        .body(r#"{"messages":[]}"#)
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-saw-uri"], "/v1/messages/count_tokens");
    assert_eq!(response.headers()["x-saw-method"], "POST");
}

#[tokio::test]
async fn catch_all_forwards_arbitrary_paths_and_statuses() {
    let upstream = serve(echo_upstream()).await;
    let relay = serve_relay(format!("http://{upstream}")).await;

    let response = reqwest::Client::new()
        .get(format!("http://{relay}/v1/some-other-path?foo=bar"))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
    assert_eq!(
        response.headers()["x-saw-uri"],
        "/v1/some-other-path?foo=bar"
    );
    assert_eq!(response.headers()["x-saw-method"], "GET");
    assert_eq!(
        response.bytes().await.expect("failed to read body"),
        MOCK_BODY.as_bytes()
    );
}

#[tokio::test]
async fn paths_outside_v1_are_not_proxied() {
    let upstream = serve(echo_upstream()).await;
    let relay = serve_relay(format!("http://{upstream}")).await;

    let response = reqwest::Client::new()
        .post(format!("http://{relay}/admin/secrets"))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// Buffering would collapse the upstream's pacing: the client would see nothing
/// until the last chunk. Time-to-first-chunk is what tells the two apart.
#[tokio::test]
async fn streamed_response_arrives_incrementally() {
    let chunk_delay = Duration::from_millis(300);
    let upstream = serve(Router::new().route(
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
    let relay = serve_relay(format!("http://{upstream}")).await;

    let start = Instant::now();
    let mut response = reqwest::Client::new()
        .post(format!("http://{relay}/v1/messages"))
        .body(r#"{"stream":true}"#)
        .send()
        .await
        .expect("request failed");

    let mut time_to_first_chunk = None;
    let mut chunks = 0;
    let mut collected = Vec::new();
    while let Some(chunk) = response.chunk().await.expect("failed to read chunk") {
        time_to_first_chunk.get_or_insert_with(|| start.elapsed());
        chunks += 1;
        collected.extend_from_slice(&chunk);
    }
    let total = start.elapsed();

    assert_eq!(collected, b"event: a\nevent: b\nevent: c\n");
    assert!(
        total >= 2 * chunk_delay,
        "mock should have paced its chunks; finished in {total:?}"
    );
    let first = time_to_first_chunk.expect("stream produced no chunks");
    assert!(
        first < chunk_delay,
        "first chunk took {first:?}, so the body was buffered rather than streamed"
    );
    assert!(
        chunks >= 2,
        "expected the upstream's chunk boundaries to survive, got {chunks} chunk(s)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_streams_are_served_concurrently() {
    let chunk_delay = Duration::from_millis(100);
    let upstream = serve(Router::new().route(
        "/v1/messages",
        any(move || async move { Response::new(dripped_body(vec!["a", "b", "c"], chunk_delay)) }),
    ))
    .await;
    let relay = serve_relay(format!("http://{upstream}")).await;

    let client = reqwest::Client::new();
    let start = Instant::now();
    let mut requests = JoinSet::new();
    for _ in 0..10 {
        let client = client.clone();
        let url = format!("http://{relay}/v1/messages");
        requests.spawn(async move {
            let response = client.post(url).send().await.expect("request failed");
            assert_eq!(response.status(), StatusCode::OK);
            response.bytes().await.expect("failed to read body")
        });
    }
    while let Some(result) = requests.join_next().await {
        assert_eq!(result.expect("request task panicked"), "abc".as_bytes());
    }
    let total = start.elapsed();

    // Serialized, ten ~300ms responses would take ~3s.
    assert!(
        total < Duration::from_millis(1200),
        "10 concurrent slow streams took {total:?}, which looks serialized"
    );
}

/// The upstream dying after headers cannot become a 502 — those are already
/// sent. What must hold is that the client's stream ends in an error rather
/// than a silent truncation or a hang.
#[tokio::test]
async fn upstream_dying_mid_stream_fails_the_client_stream() {
    let upstream = serve(Router::new().route(
        "/v1/messages",
        any(|| async { Response::new(truncated_body("event: a\n")) }),
    ))
    .await;
    let relay = serve_relay(format!("http://{upstream}")).await;

    let mut response = reqwest::Client::new()
        .post(format!("http://{relay}/v1/messages"))
        .send()
        .await
        .expect("request failed");
    assert_eq!(response.status(), StatusCode::OK);

    let first = response
        .chunk()
        .await
        .expect("first chunk should arrive before the upstream dies")
        .expect("stream ended before the first chunk");
    assert_eq!(first, "event: a\n".as_bytes());

    let outcome = tokio::time::timeout(Duration::from_secs(5), response.chunk())
        .await
        .expect("client stream hung after the upstream died");
    assert!(
        outcome.is_err(),
        "a truncated upstream body must surface as a client error, not a clean end: {outcome:?}"
    );
}

#[tokio::test]
async fn unreachable_upstream_returns_502_without_leaking_details() {
    let dead = closed_port().await;
    let relay = serve_relay(format!("http://{dead}")).await;

    let response = reqwest::Client::new()
        .post(format!("http://{relay}/v1/messages"))
        .body("{}")
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);

    let body = response.text().await.expect("failed to read body");
    assert_eq!(body, r#"{"error":"upstream_unreachable"}"#);
    assert!(
        !body.contains(&dead.to_string()) && !body.to_lowercase().contains("refused"),
        "502 body leaked upstream connection details: {body}"
    );
}
