#![allow(dead_code)]

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use futures_core::Stream;
use tokio::sync::mpsc;

use indexmap::IndexMap;

use relay::build_router;
use relay::config::{AnthropicConfig, Config, NotifyConfig, PolicyConfig};
use relay::detect::DetectConfig;
use relay::state::AppState;

/// Serves `app` on an ephemeral loopback port and returns its address.
pub async fn serve(app: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind ephemeral port");
    let addr = listener.local_addr().expect("failed to read local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server error");
    });
    addr
}

pub async fn serve_relay(base_url: String) -> SocketAddr {
    serve_relay_with_capture(base_url, None).await
}

pub async fn serve_relay_with_capture(
    base_url: String,
    capture_errors: Option<PathBuf>,
) -> SocketAddr {
    serve_relay_with(relay_config(base_url), capture_errors).await
}

pub fn relay_config(base_url: String) -> Config {
    Config {
        listen: "127.0.0.1:0".to_string(),
        state_file: None,
        anthropic: AnthropicConfig { base_url },
        detect: DetectConfig::default(),
        notify: NotifyConfig::default(),
        profiles: IndexMap::new(),
        policy: PolicyConfig::default(),
    }
}

pub async fn serve_relay_with(config: Config, capture_errors: Option<PathBuf>) -> SocketAddr {
    let state = AppState::new(Arc::new(config), capture_errors, "test-digest".to_string())
        .expect("failed to build app state");
    serve(build_router(state)).await
}

/// A directory under the OS temp dir that doesn't collide with other tests or
/// runs; the caller creates it (or lets `Capture::new` create it) as needed.
pub fn unique_temp_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "relay-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_nanos()
    ))
}

/// An address nothing is listening on: bind a port, learn it, then release it.
pub async fn closed_port() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind ephemeral port");
    listener.local_addr().expect("failed to read local addr")
}

/// A response body whose chunks are released one `delay` at a time, so a client
/// that receives them incrementally can be told apart from one that waits for
/// the whole body.
pub fn dripped_body(chunks: Vec<&'static str>, delay: Duration) -> Body {
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        for chunk in chunks {
            if tx
                .send(Ok(Bytes::from_static(chunk.as_bytes())))
                .await
                .is_err()
            {
                break;
            }
            tokio::time::sleep(delay).await;
        }
    });
    Body::from_stream(ChannelStream(rx))
}

/// A response body that delivers one chunk and then fails, so the upstream
/// connection dies mid-body rather than ending cleanly.
pub fn truncated_body(chunk: &'static str) -> Body {
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let _ = tx.send(Ok(Bytes::from_static(chunk.as_bytes()))).await;
        let _ = tx
            .send(Err(io::Error::other("mock upstream died mid-body")))
            .await;
    });
    Body::from_stream(ChannelStream(rx))
}

struct ChannelStream(mpsc::Receiver<Result<Bytes, io::Error>>);

impl Stream for ChannelStream {
    type Item = Result<Bytes, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

/// A `tracing` writer that keeps everything in memory, so a test can assert on
/// what did — and did not — reach the logs. The subscriber is process-global,
/// which is why a test binary using this holds exactly one test.
#[derive(Clone)]
pub struct Buffer(Arc<std::sync::Mutex<Vec<u8>>>);

impl Buffer {
    pub fn new() -> Self {
        Self(Arc::new(std::sync::Mutex::new(Vec::new())))
    }

    pub fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("log buffer poisoned")).into_owned()
    }

    /// Waits for `needle` to appear and returns everything captured so far.
    /// Logs are emitted from the stream's terminal event, which can land after
    /// the client has its whole response.
    pub async fn logs_containing(&self, needle: &str) -> String {
        for _ in 0..100 {
            let logs = self.contents();
            if logs.contains(needle) {
                return logs;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!(
            "timed out waiting for {needle:?} in captured logs:\n{}",
            self.contents()
        );
    }
}

impl Default for Buffer {
    fn default() -> Self {
        Self::new()
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

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buffer {
    type Writer = Buffer;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}
