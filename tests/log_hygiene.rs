mod common;

use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::routing::any;
use tracing_subscriber::fmt::MakeWriter;

use common::{closed_port, serve, serve_relay};

const AUTHORIZATION: &str = "Bearer sk-ant-oat01-DO-NOT-LOG-THIS-VALUE";
const API_KEY: &str = "sk-ant-api03-DO-NOT-LOG-THIS-EITHER";
const BETA: &str = "prompt-caching-DO-NOT-LOG-THIS-BETA";

#[derive(Clone)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

impl Buffer {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("log buffer poisoned")).into_owned()
    }
}

impl io::Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .expect("log buffer poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Buffer {
    type Writer = Buffer;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

async fn logs_containing(buffer: &Buffer, needle: &str) -> String {
    for _ in 0..100 {
        let logs = buffer.contents();
        if logs.contains(needle) {
            return logs;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "timed out waiting for {needle:?} in captured logs:\n{}",
        buffer.contents()
    );
}

/// One test per binary: the subscriber is process-global, so a second test in
/// this file would interleave its own output into the buffer.
#[tokio::test]
async fn secret_header_values_never_reach_the_logs() {
    let buffer = Buffer(Arc::new(Mutex::new(Vec::new())));
    tracing_subscriber::fmt()
        .with_writer(buffer.clone())
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .init();

    let upstream = serve(Router::new().route("/v1/messages", any(|| async { "ok" }))).await;
    let relay = serve_relay(format!("http://{upstream}")).await;
    let dead_relay = serve_relay(format!("http://{}", closed_port().await)).await;

    let client = reqwest::Client::new();
    let authenticated = |url: String| {
        client
            .post(url)
            .header("authorization", AUTHORIZATION)
            .header("x-api-key", API_KEY)
            .header("anthropic-beta", BETA)
            .header("anthropic-version", "2023-06-01")
            .body(r#"{"model":"claude-opus-5"}"#)
    };

    let response = authenticated(format!("http://{relay}/v1/messages"))
        .send()
        .await
        .expect("request failed");
    assert_eq!(response.status(), 200);
    response.bytes().await.expect("failed to read body");

    let response = authenticated(format!("http://{dead_relay}/v1/messages"))
        .send()
        .await
        .expect("request failed");
    assert_eq!(response.status(), 502);
    response.bytes().await.expect("failed to read body");

    logs_containing(&buffer, "proxied request").await;
    let logs = logs_containing(&buffer, "upstream request failed").await;

    // Guards against a vacuous pass: the capture really is seeing our requests.
    assert!(logs.contains("/v1/messages"), "captured logs:\n{logs}");
    assert!(logs.contains("status=200"), "captured logs:\n{logs}");

    for secret in [AUTHORIZATION, API_KEY, BETA, "sk-ant"] {
        assert!(
            !logs.contains(secret),
            "captured logs leaked {secret:?}:\n{logs}"
        );
    }
}
