use std::fs;
use std::io::{self, ErrorKind, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use axum::http::{HeaderMap, StatusCode};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::{Map, Value};

const REDACTED_HEADERS: [&str; 4] = ["authorization", "x-api-key", "set-cookie", "cookie"];
const REDACTED: &str = "[REDACTED]";
/// Fixtures hold (redacted) upstream response bodies, so keep them off every
/// other account on the machine rather than inheriting the umask.
const DIR_MODE: u32 = 0o700;
const FILE_MODE: u32 = 0o600;
/// Bounds the search for a free index, so a directory that refuses every
/// candidate drops one fixture with a warning instead of spinning forever.
const MAX_INDEX_ATTEMPTS: u32 = 1000;

/// Writes `--capture-errors` fixtures for non-2xx upstream responses. The
/// counter is an `AtomicU64` (not a timestamp) so concurrent errors never
/// collide on a filename.
#[derive(Clone)]
pub struct Capture {
    dir: Arc<Path>,
    counter: Arc<AtomicU64>,
}

impl Capture {
    pub fn new(dir: PathBuf) -> Result<Self> {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(DIR_MODE)
            .create(&dir)
            .with_context(|| {
                format!(
                    "failed to create --capture-errors directory: {}",
                    dir.display()
                )
            })?;
        // The flag is meant to be left on across restarts until a rare error is
        // caught, so a counter restarting at 0 would spend every run colliding
        // with the previous one's fixtures — and, before `create_new`, silently
        // overwriting them.
        let counter = AtomicU64::new(next_index(&dir));
        Ok(Self {
            dir: dir.into(),
            counter: Arc::new(counter),
        })
    }

    /// A write failure here must not take down the request that triggered it —
    /// capturing a fixture is a debugging side effect, not part of the proxy's
    /// contract with the client, so failures are logged and swallowed.
    pub fn write_fixture(
        &self,
        status: StatusCode,
        headers: &HeaderMap,
        body: &[u8],
        truncated: bool,
    ) {
        let mut fixture = Map::new();
        fixture.insert("status".to_string(), Value::from(status.as_u16()));
        fixture.insert("headers".to_string(), redacted_headers(headers));
        if truncated {
            // Milestone 2 derives limit-detection matchers from these files: a
            // partial body that reads as a complete one becomes a wrong matcher.
            fixture.insert("truncated".to_string(), Value::Bool(true));
        }
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
        let (path, mut file) = match self.create_fixture(status) {
            Ok(created) => created,
            Err(err) => {
                tracing::warn!(
                    dir = %self.dir.display(),
                    error = %err,
                    "failed to create --capture-errors fixture"
                );
                return;
            }
        };
        if let Err(err) = file.write_all(&json) {
            tracing::warn!(
                path = %path.display(),
                error = %err,
                "failed to write --capture-errors fixture"
            );
        }
    }

    /// `create_new`, so a fixture is never truncated over: an index can still
    /// repeat across a restart or a second relay sharing the directory, and the
    /// point of the flag is that a captured error survives until someone reads it.
    fn create_fixture(&self, status: StatusCode) -> io::Result<(PathBuf, fs::File)> {
        let mut taken = None;
        for _ in 0..MAX_INDEX_ATTEMPTS {
            let n = self.counter.fetch_add(1, Ordering::Relaxed);
            let path = self.dir.join(format!("{n}-{}.json", status.as_u16()));
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(FILE_MODE)
                .open(&path)
            {
                Ok(file) => return Ok((path, file)),
                Err(err) if err.kind() == ErrorKind::AlreadyExists => taken = Some(err),
                Err(err) => return Err(err),
            }
        }
        Err(taken.unwrap_or_else(|| io::Error::other("no free fixture index")))
    }
}

/// One past the highest `<n>-*.json` index already in `dir`, so a new run picks
/// up where the last one left off instead of walking back over it.
fn next_index(dir: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name();
            let (index, rest) = name.to_str()?.split_once('-')?;
            rest.ends_with(".json").then(|| index.parse::<u64>())?.ok()
        })
        .max()
        .map_or(0, |highest| highest.saturating_add(1))
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
        capture.write_fixture(StatusCode::BAD_GATEWAY, &HeaderMap::new(), body, false);

        let entry = fs::read_dir(&dir)
            .expect("capture dir should exist")
            .next()
            .expect("fixture should have been written")
            .expect("failed to read dir entry");
        let contents = fs::read_to_string(entry.path()).expect("failed to read fixture");
        let fixture: Value = serde_json::from_str(&contents).expect("invalid fixture json");

        assert_eq!(fixture["status"], 502);
        assert!(fixture.get("body").is_none());
        assert!(
            fixture.get("truncated").is_none(),
            "a complete body must not be marked truncated"
        );
        let encoded = fixture["body_base64"]
            .as_str()
            .expect("missing body_base64 field");
        assert_eq!(BASE64.decode(encoded).unwrap(), body);

        let _ = fs::remove_dir_all(&dir);
    }

    fn read_fixture(path: &Path) -> Value {
        let contents = fs::read_to_string(path).expect("failed to read fixture");
        serde_json::from_str(&contents).expect("invalid fixture json")
    }

    fn fixture_paths(dir: &Path) -> Vec<PathBuf> {
        let mut paths: Vec<_> = fs::read_dir(dir)
            .expect("capture dir should exist")
            .map(|entry| entry.expect("failed to read dir entry").path())
            .collect();
        paths.sort();
        paths
    }

    /// The flag's whole purpose is catching a rare error over a long run, which
    /// means outliving restarts: a second `Capture` over the same directory must
    /// add to what the first one caught, not overwrite it.
    #[test]
    fn a_restart_does_not_overwrite_existing_fixtures() {
        let dir = unique_temp_dir("capture-restart-test");

        let first_run = Capture::new(dir.clone()).expect("failed to create capture dir");
        first_run.write_fixture(
            StatusCode::TOO_MANY_REQUESTS,
            &HeaderMap::new(),
            b"first run",
            false,
        );
        drop(first_run);

        let second_run = Capture::new(dir.clone()).expect("failed to reopen capture dir");
        second_run.write_fixture(
            StatusCode::TOO_MANY_REQUESTS,
            &HeaderMap::new(),
            b"second run",
            false,
        );

        let paths = fixture_paths(&dir);
        assert_eq!(
            paths.len(),
            2,
            "a restart must add a fixture, not replace one: {paths:?}"
        );
        assert_eq!(read_fixture(&paths[0])["body"], "first run");
        assert_eq!(read_fixture(&paths[1])["body"], "second run");

        let _ = fs::remove_dir_all(&dir);
    }

    /// Even with the counter seeded, a name can still be taken (a second relay
    /// sharing the directory); the write must land somewhere else, not clobber.
    #[test]
    fn a_taken_index_falls_through_to_the_next_one() {
        let dir = unique_temp_dir("capture-collision-test");
        let capture = Capture::new(dir.clone()).expect("failed to create capture dir");
        fs::write(dir.join("0-429.json"), b"written by someone else")
            .expect("failed to seed a colliding fixture");

        capture.write_fixture(
            StatusCode::TOO_MANY_REQUESTS,
            &HeaderMap::new(),
            b"mine",
            false,
        );

        assert_eq!(
            fs::read_to_string(dir.join("0-429.json")).expect("failed to read seeded fixture"),
            "written by someone else"
        );
        assert_eq!(read_fixture(&dir.join("1-429.json"))["body"], "mine");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncated_fixtures_are_marked() {
        let dir = unique_temp_dir("capture-truncated-test");
        let capture = Capture::new(dir.clone()).expect("failed to create capture dir");

        capture.write_fixture(
            StatusCode::TOO_MANY_REQUESTS,
            &HeaderMap::new(),
            b"partial",
            true,
        );

        let fixture = read_fixture(&fixture_paths(&dir)[0]);
        assert_eq!(fixture["truncated"], true);
        assert_eq!(fixture["body"], "partial");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn fixtures_and_their_directory_are_not_readable_by_others() {
        use std::os::unix::fs::PermissionsExt;

        let dir = unique_temp_dir("capture-permissions-test");
        let capture = Capture::new(dir.clone()).expect("failed to create capture dir");
        capture.write_fixture(
            StatusCode::TOO_MANY_REQUESTS,
            &HeaderMap::new(),
            b"secret-ish",
            false,
        );

        let dir_mode = fs::metadata(&dir)
            .expect("capture dir should exist")
            .permissions()
            .mode();
        assert_eq!(
            dir_mode & 0o077,
            0,
            "capture dir is group/world accessible: {dir_mode:o}"
        );

        let file_mode = fs::metadata(&fixture_paths(&dir)[0])
            .expect("fixture should exist")
            .permissions()
            .mode();
        assert_eq!(
            file_mode & 0o077,
            0,
            "fixture is group/world accessible: {file_mode:o}"
        );

        let _ = fs::remove_dir_all(&dir);
    }
}
