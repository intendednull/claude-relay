use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Result, bail};
use axum::http::{HeaderMap, StatusCode};
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
    /// subscription limit rather than a burst 429 (spec §5).
    #[serde(default = "default_min_reset_horizon_secs")]
    pub min_reset_horizon_secs: u64,
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

impl Default for DetectConfig {
    fn default() -> Self {
        Self {
            status: default_status(),
            match_body: default_match_body(),
            marker_field: default_marker_field(),
            markers: default_markers(),
            reset: default_reset(),
            min_reset_horizon_secs: default_min_reset_horizon_secs(),
        }
    }
}

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
        // The proxy carries no decompression (see Cargo.toml), so a compressed
        // error body is opaque bytes here. Saying so is the difference between
        // a known gap and detection that silently never fires.
        if let Some(encoding) = content_encoding(headers) {
            tracing::warn!(
                encoding = %encoding,
                "limit detection skipped: upstream error body is compressed"
            );
            return None;
        }

        let json: Value = serde_json::from_slice(body).ok()?;
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
            (true, Some(reset_at)) => Some(reset_at),
            // An explicit marker is the signature on its own; only the window
            // length is unknown, so take the shortest one that still counts as
            // a limit. Guessing short is self-correcting — the probe after it
            // re-detects and re-limits.
            (true, None) => now.checked_add(Duration::from_secs(self.min_reset_horizon_secs)),
            (false, Some(reset_at)) => {
                let horizon = reset_at.duration_since(now).ok()?;
                (horizon.as_secs() > self.min_reset_horizon_secs).then_some(reset_at)
            }
            (false, None) => None,
        }
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

fn content_encoding(headers: &HeaderMap) -> Option<&str> {
    let encoding = headers.get("content-encoding")?.to_str().ok()?.trim();
    (!encoding.is_empty() && !encoding.eq_ignore_ascii_case("identity")).then_some(encoding)
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

    // --- the positive signature ---

    #[test]
    fn marker_and_long_retry_after_classifies_as_a_limit() {
        let reset_at = classify(LIMIT_BODY, &[("retry-after", "3600")])
            .expect("a marked limit response must classify");
        assert!((3500..=3600).contains(&horizon_secs(reset_at)));
    }

    /// The marker is the signature on its own (spec §5), so a short window
    /// alongside it must not disqualify it.
    #[test]
    fn marker_with_a_short_retry_after_still_classifies() {
        let reset_at =
            classify(LIMIT_BODY, &[("retry-after", "12")]).expect("a marker alone is enough");
        assert!(horizon_secs(reset_at) <= 12);
    }

    #[test]
    fn marker_without_any_reset_source_falls_back_to_the_minimum_horizon() {
        let reset_at = classify(LIMIT_BODY, &[]).expect("a marker alone is enough");
        let expected = default_min_reset_horizon_secs();
        assert!((expected - 5..=expected).contains(&horizon_secs(reset_at)));
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

    #[test]
    fn a_compressed_body_never_classifies() {
        let result = DetectConfig::default().classify(
            &headers(&[("retry-after", "3600"), ("content-encoding", "gzip")]),
            LIMIT_BODY.as_bytes(),
            false,
            now(),
        );
        assert!(
            result.is_none(),
            "compressed bytes are not JSON; classifying them would be a guess"
        );
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
        let reset_at = config
            .classify(
                &headers(&[("anthropic-ratelimit-requests-reset", "2030-01-01T00:00:00Z")]),
                LIMIT_BODY.as_bytes(),
                false,
                now(),
            )
            .expect("should classify");
        assert_eq!(
            reset_at.duration_since(UNIX_EPOCH).unwrap().as_secs(),
            1_893_456_000
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
            assert!(horizon_secs(reset_at) <= default_min_reset_horizon_secs());
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
