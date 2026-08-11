use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::Read;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Result, bail};
use axum::http::{HeaderMap, StatusCode};
use flate2::read::GzDecoder;
use serde::Deserialize;
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// The subscription-limit signature, as config data rather than code (spec §5),
/// so a server-side wording change is a config edit instead of a rebuild.
///
/// Every field has a default, so `[detect]` may be omitted entirely or given
/// one key at a time. The defaults come from spec §5's *expected* shape and
/// have never been checked against a real limit response (docs/decisions.md).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DetectConfig {
    #[serde(default = "default_status")]
    pub status: u16,
    /// Dotted paths into the JSON body that must all equal the given string.
    /// Path segments are split on `.` with no escaping, so a key containing a
    /// dot is not addressable.
    #[serde(default = "default_match_body")]
    pub match_body: BTreeMap<String, String>,
    /// Dotted path to the message the `markers` are searched in. Empty
    /// disables marker matching.
    #[serde(default = "default_marker_field")]
    pub marker_field: String,
    /// Case-insensitive substrings; any hit marks the response as an explicit
    /// subscription limit, which alone is enough to classify (spec §5).
    #[serde(default = "default_markers")]
    pub markers: Vec<String>,
    /// Reset-time sources in preference order: the first that yields a time wins.
    #[serde(default = "default_reset")]
    pub reset: Vec<ResetSource>,
    /// Without a marker, a reset horizon must exceed this to count as the
    /// subscription limit rather than a burst 429 (spec §5). It is also the
    /// floor every classified window gets, so no match ever produces one that
    /// has already expired.
    #[serde(default = "default_min_reset_horizon_secs")]
    pub min_reset_horizon_secs: u64,
    /// The ceiling on a classified window. This is a units/format sanity check,
    /// not a judgement about how long Anthropic's windows are: a reset read in
    /// the wrong unit (epoch *milliseconds* through a rule expecting seconds)
    /// lands ~55,000 years out, and without a ceiling that window is persisted,
    /// survives every restart, and never elapses.
    #[serde(default = "default_max_reset_horizon_secs")]
    pub max_reset_horizon_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResetSource {
    pub from: ResetFrom,
    /// A header name, or a dotted body path, depending on `from`.
    pub name: String,
    pub format: ResetFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResetFrom {
    Header,
    Body,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResetFormat {
    /// Seconds from now, as `retry-after` uses.
    DeltaSeconds,
    /// Absolute unix epoch seconds.
    UnixSeconds,
    /// Absolute RFC3339 timestamp, as Anthropic's reset headers use.
    Rfc3339,
}

fn default_status() -> u16 {
    429
}

fn default_match_body() -> BTreeMap<String, String> {
    BTreeMap::from([("error.type".to_string(), "rate_limit_error".to_string())])
}

fn default_marker_field() -> String {
    "error.message".to_string()
}

fn default_markers() -> Vec<String> {
    vec!["usage limit".to_string(), "subscription".to_string()]
}

fn default_reset() -> Vec<ResetSource> {
    vec![
        ResetSource {
            from: ResetFrom::Header,
            name: "retry-after".to_string(),
            format: ResetFormat::DeltaSeconds,
        },
        ResetSource {
            from: ResetFrom::Header,
            name: "anthropic-ratelimit-unified-reset".to_string(),
            format: ResetFormat::UnixSeconds,
        },
    ]
}

fn default_min_reset_horizon_secs() -> u64 {
    300
}

/// A week. Claude subscription limits include weekly windows, so a tighter
/// ceiling would reject a legitimate reset — and it costs nothing against what
/// the ceiling is actually for, since every wrong-unit or garbage value is
/// orders of magnitude past it.
fn default_max_reset_horizon_secs() -> u64 {
    7 * 24 * 60 * 60
}

impl Default for DetectConfig {
    fn default() -> Self {
        Self {
            status: default_status(),
            match_body: default_match_body(),
            marker_field: default_marker_field(),
            markers: default_markers(),
            reset: default_reset(),
            min_reset_horizon_secs: default_min_reset_horizon_secs(),
            max_reset_horizon_secs: default_max_reset_horizon_secs(),
        }
    }
}

/// The ceiling on the ceiling. `max_reset_horizon_secs` is what stops a
/// wrong-unit reset from producing a window that never elapses — but written in
/// the wrong unit *itself* (milliseconds for seconds) it stops being a bound:
/// large enough and `bounded`'s `checked_add` returns `None`, silently killing
/// every marked classification; merely huge and it yields a `Limited` state
/// whose `until` is past what `rfc3339` can render, so `/status` shows a stuck
/// route with a null window and nothing says why. 10 years is orders of
/// magnitude past any subscription window and orders of magnitude short of
/// either failure.
const MAX_RESET_HORIZON_CEILING_SECS: u64 = 10 * 365 * 24 * 60 * 60;

impl DetectConfig {
    /// A status the proxy never classifies (2xx, or not a status at all) would
    /// leave detection silently dead, which is the one failure this milestone
    /// cannot afford to hide.
    pub fn validate(&self) -> Result<()> {
        if !(400..=599).contains(&self.status) {
            bail!(
                "`detect.status` must be a 4xx or 5xx status code, got {}",
                self.status
            );
        }
        if self.max_reset_horizon_secs > MAX_RESET_HORIZON_CEILING_SECS {
            bail!(
                "`detect.max_reset_horizon_secs` must be at most {MAX_RESET_HORIZON_CEILING_SECS} (10 years), got {}",
                self.max_reset_horizon_secs
            );
        }
        if self.min_reset_horizon_secs > self.max_reset_horizon_secs {
            bail!(
                "`detect.min_reset_horizon_secs` ({}) must not exceed `detect.max_reset_horizon_secs` ({})",
                self.min_reset_horizon_secs,
                self.max_reset_horizon_secs
            );
        }
        Ok(())
    }

    /// Whether a response of this status is worth accumulating a body for.
    pub fn matches_status(&self, status: StatusCode) -> bool {
        status.as_u16() == self.status
    }

    /// The reset time when this response carries the configured
    /// subscription-limit signature, `None` for everything else — including a
    /// partial match, which passes through unchanged (spec §5's conservative
    /// rule: never transition on an ambiguous response).
    pub fn classify(
        &self,
        headers: &HeaderMap,
        body: &[u8],
        truncated: bool,
        now: SystemTime,
    ) -> Option<SystemTime> {
        if truncated {
            return None;
        }
        let body = classification_body(headers, body)?;

        let json: Value = serde_json::from_slice(&body).ok()?;
        for (path, expected) in &self.match_body {
            if lookup(&json, path)?.as_str()? != expected {
                return None;
            }
        }

        let reset_at = self
            .reset
            .iter()
            .find_map(|source| source.extract(headers, &json, now));

        match (self.marker_matches(&json), reset_at) {
            // An explicit marker is the signature on its own, so the response
            // is a limit whatever the reported reset says — but a stale one
            // would expire the window immediately and a wrong-unit one would
            // never expire it, so the window is held inside the configured
            // bounds. Guessing short is self-correcting: the probe after it
            // re-detects and re-limits.
            (true, Some(reset_at)) => self.bounded(reset_at, now),
            (true, None) => self.bounded(now, now),
            // Without a marker the horizon *is* the evidence, so an
            // implausible one is a reason to disbelieve the classification
            // rather than to clamp it: pass the response through untouched.
            (false, Some(reset_at)) => {
                let horizon = reset_at.duration_since(now).ok()?.as_secs();
                (horizon > self.min_reset_horizon_secs && horizon <= self.max_reset_horizon_secs)
                    .then_some(reset_at)
            }
            (false, None) => None,
        }
    }

    /// `max`/`min` rather than `clamp`, which panics when the bounds cross —
    /// `validate` rejects that config, but a panic here would be in the request
    /// path.
    fn bounded(&self, reset_at: SystemTime, now: SystemTime) -> Option<SystemTime> {
        let floor = now.checked_add(Duration::from_secs(self.min_reset_horizon_secs))?;
        let ceiling = now.checked_add(Duration::from_secs(self.max_reset_horizon_secs))?;
        Some(reset_at.max(floor).min(ceiling))
    }

    fn marker_matches(&self, json: &Value) -> bool {
        if self.marker_field.is_empty() || self.markers.is_empty() {
            return false;
        }
        let Some(text) = lookup(json, &self.marker_field).and_then(Value::as_str) else {
            return false;
        };
        let text = text.to_lowercase();
        self.markers
            .iter()
            .any(|marker| text.contains(&marker.to_lowercase()))
    }
}

impl ResetSource {
    fn extract(&self, headers: &HeaderMap, json: &Value, now: SystemTime) -> Option<SystemTime> {
        let raw = match self.from {
            ResetFrom::Header => headers.get(self.name.as_str())?.to_str().ok()?.to_string(),
            ResetFrom::Body => match lookup(json, &self.name)? {
                Value::String(text) => text.clone(),
                Value::Number(number) => number.to_string(),
                _ => return None,
            },
        };
        let raw = raw.trim();
        // Checked, not plain `+`: `SystemTime` addition panics on overflow, and
        // these seconds come off the wire. An absurd value skips this source
        // rather than taking down the request that carried it.
        match self.format {
            ResetFormat::DeltaSeconds => now.checked_add(Duration::from_secs(raw.parse().ok()?)),
            ResetFormat::UnixSeconds => {
                UNIX_EPOCH.checked_add(Duration::from_secs(raw.parse().ok()?))
            }
            ResetFormat::Rfc3339 => OffsetDateTime::parse(raw, &Rfc3339)
                .ok()
                .map(SystemTime::from),
        }
    }
}

/// A ceiling on what one classification is allowed to expand to. The 1 MiB the
/// accumulator already enforces bounds the *compressed* bytes and says nothing
/// about the output: gzip reaches ratios in the thousands, so without this a
/// misbehaving upstream could turn a small error response into gigabytes of
/// allocation. 4x the input cap is far past any error body — the real ones are
/// a few hundred bytes — while keeping the worst case a fixed few megabytes.
const MAX_DECOMPRESSED_BODY: u64 = 4 * 1024 * 1024;

enum BodyEncoding {
    Identity,
    Gzip,
    Unsupported,
}

/// Header values are deliberately not logged (this proxy logs none), so an
/// unsupported encoding is diagnosed by the fact alone, not by naming it.
fn body_encoding(headers: &HeaderMap) -> BodyEncoding {
    let mut encodings = headers
        .get_all("content-encoding")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|encoding| !encoding.is_empty() && !encoding.eq_ignore_ascii_case("identity"));

    // A second encoding means the body was compressed twice; unwinding that is
    // guesswork detection has no reason to take on.
    match (encodings.next(), encodings.next()) {
        (None, _) => BodyEncoding::Identity,
        (Some(encoding), None)
            if encoding.eq_ignore_ascii_case("gzip") || encoding.eq_ignore_ascii_case("x-gzip") =>
        {
            BodyEncoding::Gzip
        }
        _ => BodyEncoding::Unsupported,
    }
}

/// Detection's own view of the body, decompressed where it can be. Nothing here
/// touches what the client receives or what `--capture-errors` writes: both keep
/// the exact bytes the upstream sent (Milestone 1's fidelity guarantee).
///
/// Anthropic gzips error bodies whenever the client asks it to, and Claude
/// Code's client does by default — so this is the ordinary path in production,
/// not an edge case.
fn classification_body<'a>(headers: &HeaderMap, body: &'a [u8]) -> Option<Cow<'a, [u8]>> {
    match body_encoding(headers) {
        BodyEncoding::Identity => Some(Cow::Borrowed(body)),
        BodyEncoding::Gzip => match decompress_gzip(body) {
            Some(decoded) => Some(Cow::Owned(decoded)),
            None => {
                tracing::warn!(
                    "limit detection skipped: the upstream error body did not decompress"
                );
                None
            }
        },
        BodyEncoding::Unsupported => {
            tracing::warn!(
                "limit detection skipped: the upstream error body uses an unsupported content-encoding"
            );
            None
        }
    }
}

/// `None` for a body that is not gzip after all, is truncated mid-stream, or
/// expands past `MAX_DECOMPRESSED_BODY` — all of which mean the same thing to
/// the caller: nothing to classify, so pass the response through.
fn decompress_gzip(body: &[u8]) -> Option<Vec<u8>> {
    let mut decoded = Vec::new();
    // Reading one byte past the cap is what tells a body exactly at the cap from
    // one that ran over it.
    GzDecoder::new(body)
        .take(MAX_DECOMPRESSED_BODY + 1)
        .read_to_end(&mut decoded)
        .ok()?;
    (decoded.len() as u64 <= MAX_DECOMPRESSED_BODY).then_some(decoded)
}

fn lookup<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.')
        .try_fold(value, |current, key| current.get(key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn now() -> SystemTime {
        SystemTime::now()
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.append(
                axum::http::HeaderName::from_bytes(name.as_bytes()).expect("invalid header name"),
                HeaderValue::from_str(value).expect("invalid header value"),
            );
        }
        headers
    }

    const LIMIT_BODY: &str = r#"{"type":"error","error":{"type":"rate_limit_error","message":"You have reached your Claude Pro usage limit. Your limit will reset at 6pm."}}"#;
    const BURST_BODY: &str = r#"{"type":"error","error":{"type":"rate_limit_error","message":"Number of requests has exceeded your per-minute rate limit."}}"#;

    fn classify(body: &str, pairs: &[(&str, &str)]) -> Option<SystemTime> {
        DetectConfig::default().classify(&headers(pairs), body.as_bytes(), false, now())
    }

    fn horizon_secs(reset_at: SystemTime) -> u64 {
        reset_at
            .duration_since(now())
            .expect("reset time should be in the future")
            .as_secs()
    }

    fn unix_secs(time: SystemTime) -> String {
        time.duration_since(UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_secs()
            .to_string()
    }

    /// Horizons are measured against a `now` taken a moment after the one the
    /// classifier used, so they land just under the expected value.
    fn near(actual: u64, expected: u64) -> bool {
        (expected.saturating_sub(5)..=expected).contains(&actual)
    }

    // --- the positive signature ---

    #[test]
    fn marker_and_long_retry_after_classifies_as_a_limit() {
        let reset_at = classify(LIMIT_BODY, &[("retry-after", "3600")])
            .expect("a marked limit response must classify");
        assert!((3500..=3600).contains(&horizon_secs(reset_at)));
    }

    /// The marker is the signature on its own (spec §5), so a short window
    /// alongside it must not disqualify it — but the window it produces is
    /// floored, or the route would recover before it ever stopped anything.
    #[test]
    fn marker_with_a_short_retry_after_classifies_and_is_floored() {
        let reset_at =
            classify(LIMIT_BODY, &[("retry-after", "12")]).expect("a marker alone is enough");
        assert!(near(
            horizon_secs(reset_at),
            default_min_reset_horizon_secs()
        ));
    }

    /// A stale reset — an old `anthropic-ratelimit-*` value, or plain clock
    /// skew — would otherwise produce a window that has already expired, so a
    /// genuine limit would go entirely unprotected.
    #[test]
    fn marker_with_a_reset_in_the_past_is_floored_rather_than_expiring_at_once() {
        let stale = unix_secs(SystemTime::now() - Duration::from_secs(600));
        let reset_at = classify(LIMIT_BODY, &[("anthropic-ratelimit-unified-reset", &stale)])
            .expect("a marker alone is enough");
        assert!(near(
            horizon_secs(reset_at),
            default_min_reset_horizon_secs()
        ));
    }

    #[test]
    fn marker_without_any_reset_source_falls_back_to_the_minimum_horizon() {
        let reset_at = classify(LIMIT_BODY, &[]).expect("a marker alone is enough");
        assert!(near(
            horizon_secs(reset_at),
            default_min_reset_horizon_secs()
        ));
    }

    // --- the ceiling on a classified window ---

    /// The failure this ceiling exists for: a reset read in the wrong unit
    /// (epoch milliseconds through a seconds rule) is ~55,000 years out. Left
    /// alone it is persisted, survives every restart, and never elapses — and
    /// `/status` cannot even render it, so nothing says why the route is stuck.
    #[test]
    fn a_wrong_unit_reset_is_clamped_when_marked_and_rejected_when_not() {
        let epoch_millis = (unix_secs(SystemTime::now()) + "000").to_string();
        let source = [("anthropic-ratelimit-unified-reset", epoch_millis.as_str())];

        let reset_at = classify(LIMIT_BODY, &source).expect("the marker still classifies");
        assert!(
            near(horizon_secs(reset_at), default_max_reset_horizon_secs()),
            "a marked limit keeps a usable window, capped at the ceiling"
        );

        assert!(
            classify(BURST_BODY, &source).is_none(),
            "without a marker the horizon is the evidence, and an implausible one is no evidence"
        );
    }

    #[test]
    fn a_decade_long_horizon_is_clamped_when_marked_and_rejected_when_not() {
        let decade = (10 * 365 * 24 * 60 * 60).to_string();
        let source = [("retry-after", decade.as_str())];

        let reset_at = classify(LIMIT_BODY, &source).expect("the marker still classifies");
        assert!(near(
            horizon_secs(reset_at),
            default_max_reset_horizon_secs()
        ));
        assert!(classify(BURST_BODY, &source).is_none());
    }

    /// Large enough to survive `checked_add` on this platform, so the ceiling —
    /// not the overflow guard — is what has to catch it.
    #[test]
    fn a_reset_near_the_end_of_representable_time_never_escapes_the_ceiling() {
        let absurd = (i64::MAX as u64 / 2).to_string();
        let source = [("anthropic-ratelimit-unified-reset", absurd.as_str())];

        let reset_at = classify(LIMIT_BODY, &source).expect("the marker still classifies");
        assert!(near(
            horizon_secs(reset_at),
            default_max_reset_horizon_secs()
        ));
        assert!(classify(BURST_BODY, &source).is_none());
    }

    #[test]
    fn a_horizon_exactly_at_the_ceiling_still_classifies() {
        let ceiling = default_max_reset_horizon_secs().to_string();
        let reset_at = classify(BURST_BODY, &[("retry-after", &ceiling)])
            .expect("the ceiling is inclusive, unlike the floor");
        assert!(near(
            horizon_secs(reset_at),
            default_max_reset_horizon_secs()
        ));
    }

    /// No marker, but a window far past the burst threshold: the other half of
    /// spec §5's rule.
    #[test]
    fn long_horizon_without_a_marker_classifies_as_a_limit() {
        let reset_at = classify(BURST_BODY, &[("retry-after", "18000")])
            .expect("a long horizon alone is enough");
        assert!(horizon_secs(reset_at) > default_min_reset_horizon_secs());
    }

    // --- the conservative negatives (Global Constraint 6) ---

    #[test]
    fn burst_429_with_a_short_retry_after_does_not_classify() {
        assert!(
            classify(BURST_BODY, &[("retry-after", "12")]).is_none(),
            "a per-minute burst 429 must never be read as the subscription limit"
        );
    }

    #[test]
    fn horizon_exactly_at_the_threshold_does_not_classify() {
        let threshold = default_min_reset_horizon_secs().to_string();
        assert!(
            classify(BURST_BODY, &[("retry-after", &threshold)]).is_none(),
            "the threshold is exclusive: only *above* it counts"
        );
    }

    #[test]
    fn burst_429_with_no_reset_information_does_not_classify() {
        assert!(classify(BURST_BODY, &[]).is_none());
    }

    #[test]
    fn a_body_field_mismatch_does_not_classify() {
        let body = r#"{"error":{"type":"overloaded_error","message":"usage limit"}}"#;
        assert!(
            classify(body, &[("retry-after", "3600")]).is_none(),
            "every configured body matcher must hold"
        );
    }

    #[test]
    fn a_missing_body_field_does_not_classify() {
        let body = r#"{"error":{"message":"You have reached your usage limit"}}"#;
        assert!(classify(body, &[("retry-after", "3600")]).is_none());
    }

    #[test]
    fn a_non_json_body_does_not_classify() {
        assert!(
            classify(
                "<html>429 Too Many Requests</html>",
                &[("retry-after", "3600")]
            )
            .is_none()
        );
    }

    #[test]
    fn an_empty_body_does_not_classify() {
        assert!(classify("", &[("retry-after", "3600")]).is_none());
    }

    /// A partial body is a partial document; matching on it is a guess, and a
    /// wrong guess takes the route out of service.
    #[test]
    fn a_truncated_body_never_classifies() {
        let result = DetectConfig::default().classify(
            &headers(&[("retry-after", "3600")]),
            LIMIT_BODY.as_bytes(),
            true,
            now(),
        );
        assert!(result.is_none());
    }

    // --- compressed bodies (the production path: Anthropic gzips when asked,
    // and Claude Code's client always asks) ---

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(bytes).expect("gzip write failed");
        encoder.finish().expect("gzip finish failed")
    }

    fn classify_gzipped(body: &str, pairs: &[(&str, &str)]) -> Option<SystemTime> {
        DetectConfig::default().classify(&headers(pairs), &gzip(body.as_bytes()), false, now())
    }

    #[test]
    fn a_gzipped_limit_body_classifies() {
        for encoding in ["gzip", "GZIP", "x-gzip", " gzip "] {
            let reset_at = classify_gzipped(
                LIMIT_BODY,
                &[("retry-after", "3600"), ("content-encoding", encoding)],
            )
            .unwrap_or_else(|| panic!("a gzipped limit response must classify ({encoding:?})"));
            assert!((3500..=3600).contains(&horizon_secs(reset_at)));
        }
    }

    /// Decompression must not make detection any less conservative: the burst
    /// 429 is still a burst 429 once it is readable.
    #[test]
    fn a_gzipped_burst_body_still_does_not_classify() {
        assert!(
            classify_gzipped(
                BURST_BODY,
                &[("retry-after", "12"), ("content-encoding", "gzip")]
            )
            .is_none()
        );
    }

    /// The compressed body is capped at 1 MiB by the accumulator, which bounds
    /// nothing about what it expands to: a few hundred bytes of gzip can carry
    /// gigabytes of output.
    #[test]
    fn a_decompression_bomb_is_not_classified() {
        let bomb = gzip(&vec![b'a'; (MAX_DECOMPRESSED_BODY + 1) as usize]);
        assert!(
            bomb.len() < 64 * 1024,
            "the bomb must be small compressed, or it is not testing the cap"
        );
        let result = DetectConfig::default().classify(
            &headers(&[("retry-after", "3600"), ("content-encoding", "gzip")]),
            &bomb,
            false,
            now(),
        );
        assert!(result.is_none());
    }

    /// The other side of the cap: it must be generous enough that a real body
    /// with an unusually long message still gets read in full.
    #[test]
    fn a_large_body_under_the_cap_still_classifies() {
        let filler = "x".repeat(1024 * 1024);
        let body = format!(
            r#"{{"error":{{"type":"rate_limit_error","message":"You have reached your usage limit.","detail":"{filler}"}}}}"#
        );
        assert!(
            classify_gzipped(
                &body,
                &[("retry-after", "3600"), ("content-encoding", "gzip")]
            )
            .is_some()
        );
    }

    #[test]
    fn a_body_labelled_gzip_that_is_not_gzip_does_not_classify() {
        let result = DetectConfig::default().classify(
            &headers(&[("retry-after", "3600"), ("content-encoding", "gzip")]),
            LIMIT_BODY.as_bytes(),
            false,
            now(),
        );
        assert!(
            result.is_none(),
            "a body that fails to decompress is not JSON either; classifying it would be a guess"
        );
    }

    #[test]
    fn a_gzip_stream_that_stops_early_does_not_classify() {
        let complete = gzip(LIMIT_BODY.as_bytes());
        let cut = &complete[..complete.len() - 8];
        let result = DetectConfig::default().classify(
            &headers(&[("retry-after", "3600"), ("content-encoding", "gzip")]),
            cut,
            false,
            now(),
        );
        assert!(result.is_none(), "a partial document must never classify");
    }

    /// Everything detection cannot read is still passed through untouched —
    /// including a doubly-compressed body, which is not worth unwinding.
    #[test]
    fn an_unsupported_content_encoding_does_not_classify() {
        for encoding in ["br", "zstd", "deflate", "gzip, gzip"] {
            let result = DetectConfig::default().classify(
                &headers(&[("retry-after", "3600"), ("content-encoding", encoding)]),
                LIMIT_BODY.as_bytes(),
                false,
                now(),
            );
            assert!(result.is_none(), "{encoding} must not be guessed at");
        }
    }

    /// The cap is on the classifier's own copy; a truncated accumulation is
    /// rejected before any of it is read.
    #[test]
    fn a_truncated_gzipped_body_never_reaches_decompression() {
        let result = DetectConfig::default().classify(
            &headers(&[("retry-after", "3600"), ("content-encoding", "gzip")]),
            &gzip(LIMIT_BODY.as_bytes()),
            true,
            now(),
        );
        assert!(result.is_none());
    }

    #[test]
    fn an_identity_content_encoding_still_classifies() {
        let result = DetectConfig::default().classify(
            &headers(&[("retry-after", "3600"), ("content-encoding", "identity")]),
            LIMIT_BODY.as_bytes(),
            false,
            now(),
        );
        assert!(result.is_some());
    }

    #[test]
    fn a_reset_time_already_in_the_past_does_not_classify() {
        let past = (SystemTime::now() - Duration::from_secs(600))
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string();
        assert!(
            classify(BURST_BODY, &[("anthropic-ratelimit-unified-reset", &past)]).is_none(),
            "a window that already closed is not a reason to stop routing"
        );
    }

    // --- reset extraction ---

    #[test]
    fn reset_sources_are_tried_in_configured_order() {
        let unified = (SystemTime::now() + Duration::from_secs(7200))
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string();
        let reset_at = classify(
            LIMIT_BODY,
            &[
                ("retry-after", "3600"),
                ("anthropic-ratelimit-unified-reset", &unified),
            ],
        )
        .expect("should classify");
        assert!(
            (3500..=3600).contains(&horizon_secs(reset_at)),
            "`retry-after` is first in the default order, so it wins"
        );
    }

    #[test]
    fn a_malformed_first_source_falls_through_to_the_next() {
        let unified = (SystemTime::now() + Duration::from_secs(7200))
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string();
        let reset_at = classify(
            LIMIT_BODY,
            &[
                ("retry-after", "Wed, 21 Oct 2026 07:28:00 GMT"),
                ("anthropic-ratelimit-unified-reset", &unified),
            ],
        )
        .expect("should classify");
        assert!(
            horizon_secs(reset_at) > 7000,
            "an unparseable source is skipped, not fatal"
        );
    }

    #[test]
    fn rfc3339_header_sources_parse() {
        let config = DetectConfig {
            reset: vec![ResetSource {
                from: ResetFrom::Header,
                name: "anthropic-ratelimit-requests-reset".to_string(),
                format: ResetFormat::Rfc3339,
            }],
            ..DetectConfig::default()
        };
        // Inside the ceiling, so this checks the parse rather than the clamp.
        let at = SystemTime::now() + Duration::from_secs(7200);
        let seconds: i64 = unix_secs(at).parse().expect("valid seconds");
        let text = OffsetDateTime::from_unix_timestamp(seconds)
            .expect("representable")
            .format(&Rfc3339)
            .expect("formattable");

        let reset_at = config
            .classify(
                &headers(&[("anthropic-ratelimit-requests-reset", &text)]),
                LIMIT_BODY.as_bytes(),
                false,
                now(),
            )
            .expect("should classify");
        assert_eq!(
            reset_at.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            seconds
        );
    }

    #[test]
    fn body_reset_sources_accept_strings_and_numbers() {
        let config = DetectConfig {
            reset: vec![ResetSource {
                from: ResetFrom::Body,
                name: "error.resets_at".to_string(),
                format: ResetFormat::UnixSeconds,
            }],
            ..DetectConfig::default()
        };
        let at = (SystemTime::now() + Duration::from_secs(7200))
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        for body in [
            format!(
                r#"{{"error":{{"type":"rate_limit_error","message":"burst","resets_at":{at}}}}}"#
            ),
            format!(
                r#"{{"error":{{"type":"rate_limit_error","message":"burst","resets_at":"{at}"}}}}"#
            ),
        ] {
            let reset_at = config
                .classify(&HeaderMap::new(), body.as_bytes(), false, now())
                .expect("should classify from the body");
            assert!(horizon_secs(reset_at) > 7000);
        }
    }

    /// `SystemTime + Duration` panics on overflow, and these seconds arrive
    /// from the network: a hostile or broken upstream must not be able to take
    /// down the request path with a large integer.
    #[test]
    fn an_overflowing_reset_value_is_skipped_rather_than_panicking() {
        let huge = u64::MAX.to_string();
        for header in ["retry-after", "anthropic-ratelimit-unified-reset"] {
            assert!(
                classify(BURST_BODY, &[(header, &huge)]).is_none(),
                "{header} should be skipped, leaving nothing to classify on"
            );
            let reset_at = classify(LIMIT_BODY, &[(header, &huge)])
                .expect("the marker still classifies without a usable reset source");
            assert!(near(
                horizon_secs(reset_at),
                default_min_reset_horizon_secs()
            ));
        }
    }

    #[test]
    fn header_lookup_is_case_insensitive() {
        let reset_at = classify(LIMIT_BODY, &[("Retry-After", "3600")]).expect("should classify");
        assert!((3500..=3600).contains(&horizon_secs(reset_at)));
    }

    // --- config surface ---

    #[test]
    fn the_default_rule_is_the_documented_one() {
        let config = DetectConfig::default();
        assert_eq!(config.status, 429);
        assert_eq!(
            config.match_body.get("error.type").map(String::as_str),
            Some("rate_limit_error")
        );
        assert_eq!(config.min_reset_horizon_secs, 300);
        assert_eq!(config.max_reset_horizon_secs, 7 * 24 * 60 * 60);
        assert_eq!(config.reset.len(), 2);
    }

    #[test]
    fn a_partial_section_keeps_the_defaults_for_everything_else() {
        let config: DetectConfig =
            toml::from_str("min_reset_horizon_secs = 900").expect("should parse");
        assert_eq!(config.min_reset_horizon_secs, 900);
        assert_eq!(config.status, default_status());
        assert_eq!(config.markers, default_markers());
    }

    #[test]
    fn reset_sources_parse_from_toml() {
        let raw = r#"
            [[reset]]
            from = "header"
            name = "retry-after"
            format = "delta-seconds"

            [[reset]]
            from = "body"
            name = "error.resets_at"
            format = "rfc3339"
        "#;
        let config: DetectConfig = toml::from_str(raw).expect("should parse");
        assert_eq!(config.reset[0].from, ResetFrom::Header);
        assert_eq!(config.reset[0].format, ResetFormat::DeltaSeconds);
        assert_eq!(config.reset[1].from, ResetFrom::Body);
        assert_eq!(config.reset[1].format, ResetFormat::Rfc3339);
    }

    #[test]
    fn an_unknown_detect_field_is_a_parse_error() {
        let err = toml::from_str::<DetectConfig>("mystery = 1").expect_err("should fail");
        assert!(err.to_string().contains("mystery"));
    }

    #[test]
    fn validate_rejects_a_status_detection_never_sees() {
        for status in [200u16, 204, 999] {
            let config = DetectConfig {
                status,
                ..DetectConfig::default()
            };
            assert!(
                config.validate().is_err(),
                "{status} would leave detection silently dead"
            );
        }
        assert!(DetectConfig::default().validate().is_ok());
    }

    /// Crossed bounds would make every classified window collapse onto the
    /// ceiling; catching it at startup keeps `bounded` out of that situation.
    #[test]
    fn validate_rejects_crossed_horizon_bounds() {
        let config = DetectConfig {
            min_reset_horizon_secs: 900,
            max_reset_horizon_secs: 300,
            ..DetectConfig::default()
        };
        let err = config.validate().expect_err("should reject");
        assert!(err.to_string().contains("max_reset_horizon_secs"));
    }

    /// The ceiling is what keeps a wrong-unit *reset* survivable, so a
    /// wrong-unit ceiling is the same bug one level up — and it is reachable by
    /// an ordinary TOML integer. Past `bounded`'s `checked_add` every marked
    /// classification silently stops happening; short of it, `Limited` renders
    /// a null window `/status` cannot explain.
    #[test]
    fn validate_rejects_a_max_reset_horizon_that_is_not_a_bound_at_all() {
        for max_reset_horizon_secs in [
            7 * 24 * 60 * 60 * 1000, // 7 days written in milliseconds
            i64::MAX as u64,
            u64::MAX,
        ] {
            let config = DetectConfig {
                max_reset_horizon_secs,
                ..DetectConfig::default()
            };
            let err = config
                .validate()
                .expect_err("an unbounded ceiling bounds nothing")
                .to_string();
            assert!(
                err.contains("max_reset_horizon_secs"),
                "{max_reset_horizon_secs}: {err}"
            );
        }

        assert!(
            DetectConfig {
                max_reset_horizon_secs: MAX_RESET_HORIZON_CEILING_SECS,
                ..DetectConfig::default()
            }
            .validate()
            .is_ok(),
            "the ceiling itself is a valid configuration"
        );
    }

    /// The two failures the ceiling exists to keep unreachable, checked against
    /// the machinery itself rather than assumed from the number.
    #[test]
    fn a_horizon_at_the_ceiling_still_produces_a_renderable_window() {
        let config = DetectConfig {
            max_reset_horizon_secs: MAX_RESET_HORIZON_CEILING_SECS,
            ..DetectConfig::default()
        };
        let at_ceiling = config
            .bounded(
                SystemTime::now() + Duration::from_secs(u32::MAX as u64),
                now(),
            )
            .expect("`checked_add` must not overflow inside the ceiling");
        assert!(
            crate::route_state::rfc3339(at_ceiling).is_some(),
            "a window `/status` cannot render is a stuck route with no diagnosis"
        );
    }

    #[test]
    fn markers_match_case_insensitively() {
        let body = r#"{"error":{"type":"rate_limit_error","message":"USAGE LIMIT REACHED"}}"#;
        assert!(classify(body, &[]).is_some());
    }

    #[test]
    fn an_empty_marker_field_disables_marker_matching() {
        let config = DetectConfig {
            marker_field: String::new(),
            ..DetectConfig::default()
        };
        let result = config.classify(
            &headers(&[("retry-after", "12")]),
            LIMIT_BODY.as_bytes(),
            false,
            now(),
        );
        assert!(
            result.is_none(),
            "with markers off, only the horizon rule remains"
        );
    }
}
