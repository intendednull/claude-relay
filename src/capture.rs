use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use axum::http::{HeaderMap, StatusCode};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::{Map, Value};

const REDACTED_HEADERS: [&str; 4] = ["authorization", "x-api-key", "set-cookie", "cookie"];
const REDACTED: &str = "[REDACTED]";

/// Writes `--capture-errors` fixtures for non-2xx upstream responses. The
/// counter is an `AtomicU64` (not a timestamp) so concurrent errors never
/// collide on a filename.
#[derive(Clone)]
pub struct Capture {
    dir: PathBuf,
    counter: Arc<AtomicU64>,
}

impl Capture {
    pub fn new(dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&dir).with_context(|| {
            format!(
                "failed to create --capture-errors directory: {}",
                dir.display()
            )
        })?;
        Ok(Self {
            dir,
            counter: Arc::new(AtomicU64::new(0)),
        })
    }

    /// A write failure here must not take down the request that triggered it —
    /// capturing a fixture is a debugging side effect, not part of the proxy's
    /// contract with the client, so failures are logged and swallowed.
    pub fn write_fixture(&self, status: StatusCode, headers: &HeaderMap, body: &[u8]) {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(format!("{n}-{}.json", status.as_u16()));

        let mut fixture = Map::new();
        fixture.insert("status".to_string(), Value::from(status.as_u16()));
        fixture.insert("headers".to_string(), redacted_headers(headers));
        match std::str::from_utf8(body) {
            Ok(text) => {
                fixture.insert("body".to_string(), Value::String(text.to_string()));
            }
            Err(_) => {
                fixture.insert(
                    "body_base64".to_string(),
                    Value::String(BASE64.encode(body)),
                );
            }
        }

        let json = serde_json::to_vec_pretty(&Value::Object(fixture))
            .expect("fixture is built from known-serializable values");
        if let Err(err) = fs::write(&path, json) {
            tracing::warn!(
                path = %path.display(),
                error = %err,
                "failed to write --capture-errors fixture"
            );
        }
    }
}

/// `HeaderMap` iteration yields one entry per *value*, so a repeated header
/// name (e.g. multiple `Set-Cookie`s) must accumulate rather than overwrite —
/// a single `.insert()` per name would silently drop all but the last value.
/// Single-valued headers stay a plain string; repeated ones become an array.
fn redacted_headers(headers: &HeaderMap) -> Value {
    let mut grouped: Vec<(String, Vec<String>)> = Vec::new();
    for (name, value) in headers {
        let name = name.as_str();
        let value = if REDACTED_HEADERS.contains(&name) {
            REDACTED.to_string()
        } else {
            value
                .to_str()
                .unwrap_or("<non-utf8-header-value>")
                .to_string()
        };
        match grouped.iter_mut().find(|(seen, _)| seen == name) {
            Some((_, values)) => values.push(value),
            None => grouped.push((name.to_string(), vec![value])),
        }
    }

    let map = grouped
        .into_iter()
        .map(|(name, mut values)| {
            let value = if values.len() == 1 {
                Value::String(values.remove(0))
            } else {
                Value::Array(values.into_iter().map(Value::String).collect())
            };
            (name, value)
        })
        .collect();
    Value::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn redacted_headers_only_redacts_sensitive_names() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("Bearer secret"));
        headers.insert("x-api-key", HeaderValue::from_static("sk-ant-secret"));
        headers.insert("set-cookie", HeaderValue::from_static("session=1"));
        headers.insert("cookie", HeaderValue::from_static("a=b"));
        headers.insert("retry-after", HeaderValue::from_static("42"));
        headers.insert(
            "anthropic-ratelimit-requests-remaining",
            HeaderValue::from_static("10"),
        );

        let redacted = redacted_headers(&headers);

        assert_eq!(redacted["authorization"], REDACTED);
        assert_eq!(redacted["x-api-key"], REDACTED);
        assert_eq!(redacted["set-cookie"], REDACTED);
        assert_eq!(redacted["cookie"], REDACTED);
        assert_eq!(redacted["retry-after"], "42");
        assert_eq!(redacted["anthropic-ratelimit-requests-remaining"], "10");
    }

    #[test]
    fn redacted_headers_preserves_all_values_for_repeated_header_names() {
        let mut headers = HeaderMap::new();
        headers.append("set-cookie", HeaderValue::from_static("a=1"));
        headers.append("set-cookie", HeaderValue::from_static("b=2"));
        headers.append("x-request-id", HeaderValue::from_static("req-1"));
        headers.append("x-request-id", HeaderValue::from_static("req-2"));

        let redacted = redacted_headers(&headers);

        assert_eq!(
            redacted["set-cookie"],
            serde_json::json!([REDACTED, REDACTED]),
            "each occurrence of a redacted header must still redact, not just the first"
        );
        assert_eq!(
            redacted["x-request-id"],
            serde_json::json!(["req-1", "req-2"])
        );
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "relay-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock before epoch")
                .as_nanos()
        ))
    }

    #[test]
    fn write_fixture_base64_encodes_non_utf8_bodies() {
        let dir = unique_temp_dir("capture-unit-test");
        let capture = Capture::new(dir.clone()).expect("failed to create capture dir");

        let body: &[u8] = &[0xff, 0xfe, 0x00, 0x80];
        capture.write_fixture(StatusCode::BAD_GATEWAY, &HeaderMap::new(), body);

        let entry = fs::read_dir(&dir)
            .expect("capture dir should exist")
            .next()
            .expect("fixture should have been written")
            .expect("failed to read dir entry");
        let contents = fs::read_to_string(entry.path()).expect("failed to read fixture");
        let fixture: Value = serde_json::from_str(&contents).expect("invalid fixture json");

        assert_eq!(fixture["status"], 502);
        assert!(fixture.get("body").is_none());
        let encoded = fixture["body_base64"]
            .as_str()
            .expect("missing body_base64 field");
        assert_eq!(BASE64.decode(encoded).unwrap(), body);

        let _ = fs::remove_dir_all(&dir);
    }
}
