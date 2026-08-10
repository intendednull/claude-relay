use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use axum::Json;
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_core::Stream;
use serde_json::json;

use crate::capture::Capture;
use crate::state::AppState;

pub async fn forward(State(state): State<AppState>, request: Request) -> Response {
    let start = Instant::now();
    let (parts, body) = request.into_parts();

    let target = format!(
        "{}{}",
        state.config.anthropic.base_url.trim_end_matches('/'),
        parts
            .uri
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or_else(|| parts.uri.path())
    );

    let method = parts.method.clone();
    let path = parts.uri.path().to_owned();

    let upstream = state
        .http
        .request(method.clone(), target)
        .headers(forwardable(&parts.headers))
        .body(reqwest::Body::wrap_stream(body.into_data_stream()))
        .send()
        .await;

    let upstream = match upstream {
        Ok(upstream) => upstream,
        Err(err) => {
            tracing::warn!(
                method = %method,
                path = %path,
                latency_ms = elapsed_ms(start),
                // `without_url` keeps any credentials embedded in `base_url` out of the log.
                error = %err.without_url(),
                "upstream request failed"
            );
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": "upstream_unreachable" })),
            )
                .into_response();
        }
    };

    let status = upstream.status();
    let headers = forwardable(upstream.headers());
    // Latency is taken at response headers, not at end of body: a streamed
    // response stays open for as long as the model keeps generating.
    let log = RequestLog {
        method,
        path,
        status,
        latency_ms: elapsed_ms(start),
    };

    // 2xx responses are never captured, so they pay no clone/accumulate cost at all.
    let capture = if status.is_success() {
        None
    } else {
        state.capture.as_ref().map(|capture| PendingCapture {
            capture: capture.clone(),
            status,
            headers: headers.clone(),
            body: Vec::new(),
        })
    };

    let body = Body::from_stream(CountingStream {
        inner: Box::pin(upstream.bytes_stream()),
        response_bytes: 0,
        log: Some(log),
        capture,
    });

    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

fn elapsed_ms(start: Instant) -> u64 {
    start.elapsed().as_millis() as u64
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "host" | "content-length" | "transfer-encoding" | "connection"
    )
}

/// Everything the peer sent, minus the hop-by-hop headers the next connection
/// recomputes for itself.
fn forwardable(headers: &HeaderMap) -> HeaderMap {
    let mut forwarded = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers {
        if is_hop_by_hop(name) {
            continue;
        }
        // `append`, not `insert`: repeated headers (`set-cookie`) must survive.
        forwarded.append(name.clone(), value.clone());
    }
    forwarded
}

struct RequestLog {
    method: Method,
    path: String,
    status: StatusCode,
    latency_ms: u64,
}

/// A non-2xx response's status/headers plus the body bytes accumulated so far,
/// so a fixture can be written once the stream ends without holding up any
/// chunk on its way to the client.
struct PendingCapture {
    capture: Capture,
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}

/// Passes upstream bytes straight through, tallying them so the per-request log
/// line can be emitted once the body ends — including when the client hangs up
/// early and the stream is dropped mid-flight. When `capture` is set, each
/// chunk is also copied into it so a `--capture-errors` fixture can be written
/// on the same terminal events as the log line, without delaying forwarding.
struct CountingStream {
    inner: Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>,
    response_bytes: u64,
    log: Option<RequestLog>,
    capture: Option<PendingCapture>,
}

impl CountingStream {
    fn emit(&mut self) {
        let Some(log) = self.log.take() else {
            return;
        };
        tracing::info!(
            method = %log.method,
            path = %log.path,
            status = log.status.as_u16(),
            latency_ms = log.latency_ms,
            response_bytes = self.response_bytes,
            "proxied request"
        );
    }

    fn finish_capture(&mut self) {
        let Some(pending) = self.capture.take() else {
            return;
        };
        pending
            .capture
            .write_fixture(pending.status, &pending.headers, &pending.body);
    }
}

impl Stream for CountingStream {
    type Item = reqwest::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                this.response_bytes += chunk.len() as u64;
                if let Some(pending) = &mut this.capture {
                    pending.body.extend_from_slice(&chunk);
                }
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(err))) => {
                this.emit();
                this.finish_capture();
                // Same reason as the handler's error path: whatever renders this
                // error must not be handed a URL that may carry credentials.
                Poll::Ready(Some(Err(err.without_url())))
            }
            Poll::Ready(None) => {
                this.emit();
                this.finish_capture();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for CountingStream {
    fn drop(&mut self) {
        self.emit();
        self.finish_capture();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn drops_hop_by_hop_headers_only() {
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("relay.local"));
        headers.insert("content-length", HeaderValue::from_static("12"));
        headers.insert("transfer-encoding", HeaderValue::from_static("chunked"));
        headers.insert("connection", HeaderValue::from_static("keep-alive"));
        headers.insert("authorization", HeaderValue::from_static("Bearer token"));
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));

        let forwarded = forwardable(&headers);

        assert_eq!(forwarded.len(), 2);
        assert_eq!(forwarded["authorization"], "Bearer token");
        assert_eq!(forwarded["anthropic-version"], "2023-06-01");
    }

    #[test]
    fn keeps_repeated_header_values() {
        let mut headers = HeaderMap::new();
        headers.append("set-cookie", HeaderValue::from_static("a=1"));
        headers.append("set-cookie", HeaderValue::from_static("b=2"));

        let forwarded = forwardable(&headers);

        let cookies: Vec<_> = forwarded.get_all("set-cookie").iter().collect();
        assert_eq!(cookies, vec!["a=1", "b=2"]);
    }
}
