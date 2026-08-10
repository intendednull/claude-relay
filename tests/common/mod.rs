#![allow(dead_code)]

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use futures_core::Stream;
use tokio::sync::mpsc;

use relay::build_router;
use relay::config::{AnthropicConfig, Config};
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
    let config = Config {
        listen: "127.0.0.1:0".to_string(),
        anthropic: AnthropicConfig { base_url },
    };
    let state = AppState::new(Arc::new(config), None).expect("failed to build app state");
    serve(build_router(state)).await
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
