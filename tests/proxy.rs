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

/// `gzip -9` of a small JSON error body. Fixed bytes, not a compressor call, so
/// the assertion is against exactly what the mock upstream put on the wire.
const GZIPPED_BODY: &[u8] = &[
    0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x03, 0xab, 0x56, 0x4a, 0x2d, 0x2a, 0xca,
    0x2f, 0x52, 0xb2, 0xaa, 0x56, 0x2a, 0xa9, 0x2c, 0x48, 0x55, 0xb2, 0x52, 0x2a, 0x4a, 0x2c, 0x49,
    0x8d, 0xcf, 0xc9, 0xcc, 0xcd, 0x2c, 0x89, 0x87, 0x48, 0xe9, 0x28, 0xe5, 0xa6, 0x16, 0x17, 0x27,
    0xa6, 0x83, 0x24, 0x93, 0xf3, 0x73, 0x0b, 0x8a, 0x80, 0xbc, 0xd4, 0x14, 0xa5, 0xda, 0x5a, 0x00,
    0x8d, 0x29, 0x13, 0x4c, 0x3c, 0x00, 0x00, 0x00,
];

/// Byte-for-byte fidelity rests on reqwest carrying no compression feature (see
/// Cargo.toml): with one, it would decompress the body while `content-encoding`
/// still says gzip. Adding that feature must fail here, not in the field.
#[tokio::test]
async fn compressed_bodies_pass_through_without_being_decompressed() {
    let upstream = serve(Router::new().route(
        "/v1/messages",
        any(|| async {
            Response::builder()
                .header("content-encoding", "gzip")
                .header("content-type", "application/json")
                .body(Body::from(GZIPPED_BODY))
                .expect("failed to build mock response")
        }),
    ))
    .await;
    let relay = serve_relay(format!("http://{upstream}")).await;

    let response = reqwest::Client::new()
        .post(format!("http://{relay}/v1/messages"))
        .body("{}")
        .send()
        .await
        .expect("request failed");

    assert_eq!(
        response
            .headers()
            .get("content-encoding")
            .map(|value| value.as_bytes()),
        Some(b"gzip".as_slice()),
        "content-encoding must survive the proxy; if it is missing entirely, \
         reqwest decompressed the body — look for a compression feature in Cargo.toml"
    );
    let body = response.bytes().await.expect("failed to read body");
    assert_eq!(
        body.as_ref(),
        GZIPPED_BODY,
        "a compressed body must arrive as the exact bytes the upstream sent"
    );
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
