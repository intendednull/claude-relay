//! The translator's streaming half, driven over real HTTP by a mock
//! OpenAI-format upstream — the shape Task 3 will wire into the proxy, proven
//! here without any of its routing or header handling.

mod common;

use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::response::Response;
use axum::routing::any;
use serde_json::Value;

use common::{dripped_body, serve, truncated_body};
use relay::translate::sse_stream;

/// Three OpenAI chunks that between them open a text block, call a tool with
/// arguments split across frames, and finish.
const CHUNKS: [&str; 5] = [
    concat!(
        r#"data: {"id":"chatcmpl-1","object":"chat.completion.chunk","model":"target/Model","#,
        r#""choices":[{"index":0,"delta":{"role":"assistant","content":"Looking"},"#,
        r#""finish_reason":null}]}"#,
        "\n\n"
    ),
    concat!(
        r#"data: {"id":"chatcmpl-1","object":"chat.completion.chunk","model":"target/Model","#,
        r#""choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","#,
        r#""type":"function","function":{"name":"Bash","arguments":"{\"comm"}}]}}]}"#,
        "\n\n"
    ),
    concat!(
        r#"data: {"id":"chatcmpl-1","object":"chat.completion.chunk","model":"target/Model","#,
        r#""choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"#,
        r#""function":{"arguments":"and\":\"ls\"}"}}]}}]}"#,
        "\n\n"
    ),
    concat!(
        r#"data: {"id":"chatcmpl-1","object":"chat.completion.chunk","model":"target/Model","#,
        r#""choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}],"#,
        r#""usage":{"prompt_tokens":31,"completion_tokens":12}}"#,
        "\n\n"
    ),
    "data: [DONE]\n\n",
];

/// Serves the translated form of a body that releases `CHUNKS` one `delay`
/// apart, mimicking an upstream that generates as it goes.
fn translating_upstream(delay: Duration) -> Router {
    Router::new().route(
        "/v1/chat/completions",
        any(move || async move {
            let upstream = dripped_body(CHUNKS.to_vec(), delay).into_data_stream();
            Response::new(Body::from_stream(sse_stream(upstream)))
        }),
    )
}

fn events(bytes: &[u8]) -> Vec<(String, Value)> {
    std::str::from_utf8(bytes)
        .expect("translated output must be UTF-8")
        .split("\n\n")
        .filter(|frame| !frame.trim().is_empty())
        .map(|frame| {
            let mut event = None;
            let mut data = String::new();
            for line in frame.split('\n') {
                if let Some(rest) = line.strip_prefix("event: ") {
                    event = Some(rest.to_string());
                } else if let Some(rest) = line.strip_prefix("data: ") {
                    data.push_str(rest);
                }
            }
            (
                event.expect("every frame carries an event name"),
                serde_json::from_str(&data).expect("every frame carries JSON data"),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn translated_events_reach_the_client_before_the_upstream_stream_ends() {
    let chunk_delay = Duration::from_millis(100);
    let upstream = serve(translating_upstream(chunk_delay)).await;

    let start = Instant::now();
    let mut response = reqwest::Client::new()
        .post(format!("http://{upstream}/v1/chat/completions"))
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

    assert!(
        total >= 4 * chunk_delay,
        "the mock should have paced its chunks; finished in {total:?}"
    );
    let first = time_to_first_chunk.expect("stream produced no chunks");
    assert!(
        first < chunk_delay,
        "first translated event took {first:?}, so the upstream stream was buffered \
         before anything was synthesized"
    );
    assert!(
        chunks >= 2,
        "synthesized events arrived in {chunks} chunk(s), so they were emitted in one batch"
    );

    let events = events(&collected);
    let names: Vec<&str> = events.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_stop",
            "content_block_start",
            "content_block_delta",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
        ]
    );

    let arguments: String = events
        .iter()
        .filter(|(_, data)| data["delta"]["type"] == "input_json_delta")
        .map(|(_, data)| data["delta"]["partial_json"].as_str().unwrap())
        .collect();
    assert_eq!(arguments, r#"{"command":"ls"}"#);
    assert_eq!(events[4].1["content_block"]["id"], "call_1");
    assert_eq!(events[8].1["delta"]["stop_reason"], "tool_use");
    assert_eq!(
        events[8].1["usage"],
        serde_json::json!({"input_tokens": 31, "output_tokens": 12})
    );
}

/// The proof that the *whole* response is not held: the client must see the
/// first events while the upstream still has frames to send.
#[tokio::test(flavor = "multi_thread")]
async fn the_first_event_arrives_while_later_upstream_frames_are_still_pending() {
    let chunk_delay = Duration::from_millis(150);
    let upstream = serve(translating_upstream(chunk_delay)).await;

    let mut response = reqwest::Client::new()
        .post(format!("http://{upstream}/v1/chat/completions"))
        .send()
        .await
        .expect("request failed");

    let first = response
        .chunk()
        .await
        .expect("failed to read chunk")
        .expect("stream produced no chunks");
    let events = events(&first);

    assert_eq!(
        events
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "message_start",
            "content_block_start",
            "content_block_delta"
        ],
        "the first upstream frame alone must produce the message's opening events"
    );
    assert_eq!(events[2].1["delta"]["text"], "Looking");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_upstream_that_dies_mid_stream_ends_with_an_error_event() {
    let upstream = serve(Router::new().route(
        "/v1/chat/completions",
        any(|| async {
            let upstream = truncated_body(CHUNKS[0]).into_data_stream();
            Response::new(Body::from_stream(sse_stream(upstream)))
        }),
    ))
    .await;

    let mut response = reqwest::Client::new()
        .post(format!("http://{upstream}/v1/chat/completions"))
        .send()
        .await
        .expect("request failed");

    let mut collected = Vec::new();
    // The body ends cleanly even though the upstream died: the failure is
    // reported in-band as an SSE event, which only reaches the client if the
    // body is not aborted mid-frame.
    while let Some(chunk) = response
        .chunk()
        .await
        .expect("the translated body must end cleanly, not abort")
    {
        collected.extend_from_slice(&chunk);
    }

    let events = events(&collected);
    let names: Vec<&str> = events.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_stop",
            "error",
        ]
    );
    let message = events[4].1["error"]["message"].as_str().unwrap();
    assert_eq!(message, "upstream stream ended unexpectedly");
    assert!(
        !message.contains("http://"),
        "the error event must not carry an upstream URL: {message}"
    );
}
