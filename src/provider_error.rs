//! A fallback provider's error, read once and re-emitted in Anthropic's
//! envelope (spec §7d).
//!
//! Its own module rather than part of `translate`, which is the OpenAI wire
//! format and nothing else: an `anthropic` profile's errors come through here
//! too, and the reason they do is that an Anthropic-*compatible* gateway is
//! exactly the thing that rewrites the upstream's wording (docs/decisions.md).
//!
//! The context-limit decision lives here because it has two callers: this
//! module, to emit the phrase Claude Code's recovery keys on, and Task 9A's
//! escalation, which fires on the same condition.

use std::ops::Range;

use axum::http::StatusCode;
use serde_json::{Value, json};

/// The provider said the prompt did not fit the model's context.
pub(crate) struct ContextLimit {
    /// `(prompt tokens, context limit)` when the provider's message named a
    /// usable pair. `None` rather than a guess: a wrong pair would make the
    /// client size its retry wrongly, which is worse than making it retry blind.
    pub(crate) counts: Option<(u64, u64)>,
}

pub(crate) struct ProviderError {
    /// Anthropic's own name for this failure.
    kind: &'static str,
    /// The provider's `error.message`, when the body carried one.
    message: Option<String>,
    /// Set only for the failure Task 9A escalates on.
    pub(crate) context_limit: Option<ContextLimit>,
}

/// Anthropic's documented `error.type` names. Consulted only for a status
/// Anthropic documents no type for — a provider's own type string is not a
/// reliable signal (Together answers a 401 with `invalid_request_error`, see
/// `tests/fixtures/together/I_error_invalid_auth.json`), so the status wins
/// wherever it says anything at all.
const ANTHROPIC_ERROR_TYPES: [&str; 8] = [
    "invalid_request_error",
    "authentication_error",
    "permission_error",
    "not_found_error",
    "request_too_large",
    "rate_limit_error",
    "api_error",
    "overloaded_error",
];

/// Wordings that mean "the prompt did not fit". Only the first is measured —
/// captured from Together AI at 170,071 tokens against a 131k model. The others
/// are this file's guesses at wording no provider here has been observed using,
/// kept narrow on purpose: a false positive sends the client into a pointless
/// shrink-and-retry loop. Lowercase, because the match is.
const CONTEXT_LIMIT_MARKERS: [&str; 3] = [
    "longer than the model's context length",
    "maximum context length",
    "context window",
];

impl ProviderError {
    pub(crate) fn read(status: StatusCode, body: &[u8]) -> Self {
        let parsed: Option<Value> = serde_json::from_slice(body).ok();
        let error = parsed.as_ref().and_then(|value| value.get("error"));
        let field = |name: &str| error.and_then(|error| error.get(name))?.as_str();

        let message = field("message").map(str::to_string);
        Self {
            kind: anthropic_type(status, field("type")),
            context_limit: message
                .as_deref()
                .and_then(|message| context_limit(status, message)),
            message,
        }
    }

    /// The Anthropic error envelope, ready to send. The status is the caller's
    /// to preserve — nothing here normalises it.
    pub(crate) fn to_anthropic(&self) -> Vec<u8> {
        let body = json!({
            "type": "error",
            "error": {"type": self.kind, "message": self.client_message()},
        });
        serde_json::to_vec(&body).expect("a JSON object of owned strings serializes")
    }

    /// The message the client sees. For a context-limit error this is the part
    /// that does the work: Claude Code detects the condition by lowercased
    /// substring match and extracts the two numbers with
    /// `prompt is too long[^0-9]*(\d+)\s*tokens?\s*>\s*(\d+)`, so where the
    /// phrase sits relative to the digits decides whether the client can size
    /// its retry (docs/decisions.md).
    fn client_message(&self) -> String {
        let Some(provider) = self.message.as_deref() else {
            return "the fallback provider returned an error with no message".to_string();
        };
        match &self.context_limit {
            None => provider.to_string(),
            // The pair leads, because the regex permits no digits between the
            // phrase and the token count. The provider's own sentence is free
            // after it — `.includes` does not care — and it is the only thing
            // that told us the real limit.
            Some(ContextLimit {
                counts: Some((tokens, limit)),
            }) => format!(
                "prompt is too long: {tokens} tokens > {limit}. \
                 The fallback provider said: {provider}"
            ),
            // No usable pair, so the phrase goes *last*: any digits the
            // provider's message carries then sit ahead of it, where the regex
            // cannot read them as the pair it is looking for.
            Some(ContextLimit { counts: None }) => format!("{provider} (prompt is too long)"),
        }
    }
}

fn anthropic_type(status: StatusCode, provider_type: Option<&str>) -> &'static str {
    match status.as_u16() {
        400 => "invalid_request_error",
        401 => "authentication_error",
        403 => "permission_error",
        404 => "not_found_error",
        413 => "request_too_large",
        429 => "rate_limit_error",
        500 => "api_error",
        529 => "overloaded_error",
        // Together's 422 lands here. Pass a recognised type through rather than
        // inventing one, and fall back to Anthropic's generic name.
        _ => provider_type
            .and_then(|name| ANTHROPIC_ERROR_TYPES.iter().find(|known| **known == name))
            .copied()
            .unwrap_or("api_error"),
    }
}

/// The single home for "is this the provider saying the prompt did not fit".
///
/// A small matcher rather than `[detect]`-style config data on purpose
/// (`.superpowers/sdd/milestone-3-plan/task-9b-notes.md`): the status gate keeps
/// a malformed request from being reshaped into a too-long claim, which would
/// send the client shrinking and retrying forever.
pub(crate) fn context_limit(status: StatusCode, message: &str) -> Option<ContextLimit> {
    // 400 is what Together answers; 413 is the other status a provider could
    // plausibly use for "your input is too big". Nothing wider: a 500 whose
    // message happens to mention a context window is not this failure.
    if !matches!(status.as_u16(), 400 | 413) {
        return None;
    }
    // Everything downstream reads the lowercased copy, never `message` itself:
    // `to_lowercase` is not length-preserving (`İ` becomes two chars), so a byte
    // offset found here would be the wrong offset — or not a char boundary — in
    // the original. Digits and the markers are ASCII, so the copy answers every
    // question this function asks.
    let lowered = message.to_lowercase();
    // First marker in list order that occurs, so the measured wording wins over
    // the guesses when a message somehow carries both.
    let marker = CONTEXT_LIMIT_MARKERS
        .iter()
        .find_map(|marker| lowered.find(marker).map(|at| at..at + marker.len()))?;
    Some(ContextLimit {
        counts: token_counts(&lowered, marker),
    })
}

/// The two numbers the matched marker's own sentence is about: the last digit run
/// before it, and the first after it.
///
/// Anchored to the marker rather than scanning from the start of the message,
/// because anything an intermediary prepends otherwise *becomes* the pair. A
/// LiteLLM sidecar, corporate proxy or CDN wrapper adding a request id or an ISO
/// timestamp in front of Together's own sentence is enough to turn
/// `(170071, 131072)` into `(2026, 8)` — and telling the client its context limit
/// is 8 tokens drives `max_tokens` toward zero without ever converging, which is
/// the loop this whole task exists to prevent. A wrong pair is worse than no pair
/// (`docs/decisions.md`).
fn token_counts(lowered: &str, marker: Range<usize>) -> Option<(u64, u64)> {
    let before = &lowered[..marker.start];
    let (tokens, tokens_end) = last_digit_run(before)?;
    // The leading number has to be a count of *tokens*. The measured wording says
    // "(170071 tokens) is longer than the model's context length", and requiring
    // the word between the number and the marker is what stops a numeric model
    // name — "Qwen3-480B has a maximum context length of …" — from being read as
    // the input size. Unmeasured wording that omits it gets no pair, which is the
    // safe direction to fail in.
    if !before[tokens_end..].contains("token") {
        return None;
    }
    let limit = first_digit_run(&lowered[marker.end..])?;
    // This failure means input over limit; a pair that does not say that is not
    // the pair, however well-placed it looked.
    (tokens > limit).then_some((tokens, limit))
}

/// The last ASCII digit run in `text`, and the byte offset just past it.
///
/// Byte-indexed on purpose: every byte of a digit run is ASCII, so both ends of
/// the slice are char boundaries whatever else the message carries — where a
/// `char`-based scan would have to trust that it never lands mid-sequence.
fn last_digit_run(text: &str) -> Option<(u64, usize)> {
    let bytes = text.as_bytes();
    let end = bytes.iter().rposition(u8::is_ascii_digit)? + 1;
    let start = bytes[..end]
        .iter()
        .rposition(|byte| !byte.is_ascii_digit())
        .map_or(0, |at| at + 1);
    digits(&bytes[start..end]).map(|value| (value, end))
}

fn first_digit_run(text: &str) -> Option<u64> {
    let bytes = text.as_bytes();
    let start = bytes.iter().position(u8::is_ascii_digit)?;
    let end = bytes[start..]
        .iter()
        .position(|byte| !byte.is_ascii_digit())
        .map_or(bytes.len(), |at| start + at);
    digits(&bytes[start..end])
}

/// `None` for a run too large for `u64`, rather than a wrapped number.
fn digits(run: &[u8]) -> Option<u64> {
    std::str::from_utf8(run).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The captured Together body, byte-for-byte from the running service.
    const TOGETHER_CONTEXT: &str = r#"{"id":"ovq5","error":{"message":"The input (170071 tokens) is longer than the model's context length (131072 tokens).","type":"invalid_request_error","param":null,"code":null}}"#;

    fn read(status: u16, body: &str) -> ProviderError {
        ProviderError::read(
            StatusCode::from_u16(status).expect("valid status"),
            body.as_bytes(),
        )
    }

    fn message(status: u16, body: &str) -> String {
        read(status, body).client_message()
    }

    #[test]
    fn the_context_limit_message_leads_with_the_phrase_and_the_pair() {
        assert_eq!(
            message(400, TOGETHER_CONTEXT),
            "prompt is too long: 170071 tokens > 131072. The fallback provider said: \
             The input (170071 tokens) is longer than the model's context length (131072 tokens)."
        );
    }

    /// The provider's own sentence has to survive, or debuggability regresses:
    /// it is the only thing that reported the real limit.
    #[test]
    fn the_providers_own_sentence_survives_the_wrapper() {
        assert!(message(400, TOGETHER_CONTEXT).contains("the model's context length"));
    }

    #[test]
    fn an_unparseable_pair_emits_the_phrase_without_numbers() {
        let body = r#"{"error":{"message":"The input is longer than the model's context length.","type":"invalid_request_error"}}"#;
        assert_eq!(
            message(400, body),
            "The input is longer than the model's context length. (prompt is too long)"
        );
    }

    /// With no pair to report, no digit the provider's message carries may be
    /// readable as one — so the phrase trails everything.
    #[test]
    fn without_a_pair_the_phrase_trails_every_digit_the_provider_sent() {
        // Digits present, but in limit-then-input order, so no usable pair.
        let body = r#"{"error":{"message":"context window exceeded (limit 131072, sent 170071)","type":"invalid_request_error"}}"#;
        let message = message(400, body);
        let phrase = message
            .find("prompt is too long")
            .expect("the phrase must be present");
        assert!(
            !message[phrase..].chars().any(char::is_numeric),
            "a digit after the phrase can be misread as the token pair: {message}"
        );
    }

    /// The pair for a message, through the real seam rather than the parser
    /// directly — the marker range the parser anchors to is the seam's to find.
    fn counts(message: &str) -> Option<(u64, u64)> {
        context_limit(StatusCode::BAD_REQUEST, message)?.counts
    }

    /// A reversed pair is not this failure's pair: reporting it would tell the
    /// client its prompt was smaller than the limit it just exceeded.
    ///
    /// The first case is the one that reaches the `tokens > limit` guard —
    /// well-placed numbers on both sides of the marker, in the wrong relation.
    /// Anchoring alone rejects the other two earlier, for want of any digit before
    /// the marker at all, so without this case the guard has no test. (It did not,
    /// briefly: anchoring made the old cases stop depending on it, which a re-run
    /// of the round-0 mutation caught.)
    #[test]
    fn a_pair_that_does_not_say_input_over_limit_is_not_used() {
        assert_eq!(
            counts("sent 8192 tokens, past the maximum context length of 131072"),
            None,
            "an input smaller than the limit is not this failure"
        );
        assert_eq!(
            counts("The context window says (131072 tokens) exceeded by (170071 tokens)"),
            None
        );
        assert_eq!(
            counts(
                "The input (170071 tokens) is longer than the model's context length (131072 tokens)."
            ),
            Some((170071, 131072))
        );
    }

    #[test]
    fn an_integer_too_large_for_u64_yields_no_pair() {
        assert_eq!(
            counts(
                "The input (99999999999999999999999 tokens) is longer than the model's context length (10 tokens)."
            ),
            None
        );
    }

    /// The blocker this parser was rewritten for. Scanning the message from the
    /// start took the first two digit runs anywhere in it, so anything an
    /// intermediary prepends became the pair — and the timestamp case reported a
    /// context limit of *8 tokens*, which drives `max_tokens` toward zero and never
    /// converges. Anchoring to the matched marker is what makes a prefix
    /// irrelevant. These are the reviewer's seven cases.
    #[test]
    fn a_prefix_an_intermediary_adds_cannot_become_the_token_pair() {
        for (label, message) in [
            (
                "measured Together",
                "The input (170071 tokens) is longer than the model's context length (131072 tokens).",
            ),
            (
                "numeric request id",
                "request 1234567890: The input (170071 tokens) is longer than the model's context length (131072 tokens).",
            ),
            (
                "ISO timestamp",
                "2026-08-12T12:00:00Z The input (170071 tokens) is longer than the model's context length (131072 tokens).",
            ),
            (
                "trailing doc link",
                "The input (170071 tokens) is longer than the model's context length (131072 tokens). See https://docs.example/errors#400",
            ),
        ] {
            assert_eq!(
                counts(message),
                Some((170071, 131072)),
                "{label} must still yield the provider's own pair"
            );
        }

        // The other three degrade to no pair, which is the blessed outcome: the
        // client retries blind rather than wrongly.
        for (label, message) in [
            (
                "OpenAI wording, limit before input",
                "This model's maximum context length is 131072 tokens. However, your messages resulted in 170071 tokens.",
            ),
            (
                "numeric model name",
                "Model Qwen3-480B has a maximum context length of 131072 tokens; the input was 170071 tokens.",
            ),
            (
                "no digits at all",
                "The input is longer than the model's context length.",
            ),
        ] {
            assert_eq!(counts(message), None, "{label} must yield no pair");
        }
    }

    /// The `token` requirement, which is what stops a numeric model name from
    /// standing in for the input size when the wording puts the limit first.
    #[test]
    fn the_leading_number_has_to_be_a_count_of_tokens() {
        assert_eq!(
            counts("model-99999 has a maximum context length of 8192 tokens"),
            None,
            "a model name's digits are not an input size"
        );
        assert_eq!(
            counts("sent 99999 tokens, past the maximum context length of 8192"),
            Some((99999, 8192))
        );
    }

    /// `to_lowercase` is not length-preserving, so a marker offset found in the
    /// lowercased copy is the wrong offset in the original — and possibly not a
    /// char boundary. Everything downstream reads the copy; this is the case that
    /// would panic if it did not.
    #[test]
    fn a_message_whose_lowercasing_changes_its_length_does_not_panic() {
        let message =
            "İİİ sent 170071 tokens, longer than the model's context length (131072 tokens).";
        assert!(message.to_lowercase().len() > message.len());
        assert_eq!(counts(message), Some((170071, 131072)));
    }

    /// The failure this exists to prevent: a malformed request reshaped into a
    /// too-long claim, which the client answers by shrinking and retrying.
    #[test]
    fn an_ordinary_400_is_not_a_context_limit_error() {
        let body = r#"{"error":{"message":"Input required","type":"invalid_request_error","param":null,"code":null}}"#;
        let error = read(400, body);
        assert!(error.context_limit.is_none());
        assert_eq!(error.client_message(), "Input required");
    }

    /// Together's own `max_new_tokens` validation error, captured
    /// (`J_error_max_tokens_exceeds_context.json`). It carries three integers,
    /// so a matcher that fired on it would report a wrong pair.
    #[test]
    fn the_captured_max_tokens_validation_error_is_not_a_context_limit_error() {
        let body = r#"{"error":{"message":"Input validation error: `inputs` tokens + `max_new_tokens` must be <= 32769. Given: 30 `inputs` tokens and 999999999 `max_new_tokens`","type":"invalid_request_error"}}"#;
        assert!(read(400, body).context_limit.is_none());
    }

    /// The wording alone is not enough: a 5xx that mentions a context window is
    /// the provider being broken, not the prompt being too long.
    #[test]
    fn only_a_status_that_could_mean_too_big_is_considered() {
        for status in [200u16, 429, 500, 502] {
            assert!(
                context_limit(
                    StatusCode::from_u16(status).expect("valid status"),
                    "The input (170071 tokens) is longer than the model's context length (131072 tokens)."
                )
                .is_none(),
                "{status} must not be read as a context limit"
            );
        }
        assert!(context_limit(StatusCode::PAYLOAD_TOO_LARGE, "maximum context length").is_some());
    }

    #[test]
    fn markers_match_case_insensitively() {
        assert!(
            context_limit(StatusCode::BAD_REQUEST, "MAXIMUM CONTEXT LENGTH EXCEEDED").is_some()
        );
    }

    // --- the envelope ---

    #[test]
    fn the_envelope_is_anthropics_and_carries_the_mapped_type() {
        let out = read(400, TOGETHER_CONTEXT).to_anthropic();
        let value: Value = serde_json::from_slice(&out).expect("valid JSON");
        assert_eq!(value["type"], "error");
        assert_eq!(value["error"]["type"], "invalid_request_error");
        assert!(
            value["error"]["message"]
                .as_str()
                .expect("a message string")
                .starts_with("prompt is too long: 170071 tokens > 131072")
        );
    }

    /// The capture that makes status-first mapping necessary: Together answers a
    /// 401 with `type: "invalid_request_error"`.
    #[test]
    fn the_status_decides_the_type_where_anthropic_documents_one() {
        let body = r#"{"error":{"message":"Invalid API key provided.","type":"invalid_request_error","code":"invalid_api_key"}}"#;
        let value: Value =
            serde_json::from_slice(&read(401, body).to_anthropic()).expect("valid JSON");
        assert_eq!(value["error"]["type"], "authentication_error");
        assert_eq!(value["error"]["message"], "Invalid API key provided.");
    }

    #[test]
    fn a_status_anthropic_documents_no_type_for_keeps_a_recognised_provider_type() {
        let body = r#"{"error":{"message":"nope","type":"invalid_request_error"}}"#;
        assert_eq!(read(422, body).kind, "invalid_request_error");
    }

    #[test]
    fn an_unrecognised_provider_type_becomes_the_generic_one() {
        let body = r#"{"error":{"message":"nope","type":"together_specific_error"}}"#;
        assert_eq!(read(422, body).kind, "api_error");
        assert_eq!(read(422, r#"{"error":{}}"#).kind, "api_error");
    }

    /// A body that is not an OpenAI- or Anthropic-shaped error at all still has
    /// to produce a usable envelope, since the status is all the client gets.
    #[test]
    fn an_unreadable_body_still_produces_an_envelope() {
        for body in [
            "<html>502 Bad Gateway</html>",
            "",
            "null",
            r#"{"error":"flat"}"#,
        ] {
            let value: Value =
                serde_json::from_slice(&read(502, body).to_anthropic()).expect("valid JSON");
            assert_eq!(value["type"], "error");
            assert_eq!(value["error"]["type"], "api_error");
            assert_eq!(
                value["error"]["message"],
                "the fallback provider returned an error with no message"
            );
        }
    }

    /// The message is a JSON string, so a provider that puts quotes and newlines
    /// in it cannot break out of the envelope.
    #[test]
    fn a_hostile_message_stays_inside_the_envelope() {
        let body = r#"{"error":{"message":"broken \" }} \n {\"type\":\"forged\"","type":"invalid_request_error"}}"#;
        let out = read(400, body).to_anthropic();
        let value: Value = serde_json::from_slice(&out).expect("valid JSON");
        assert_eq!(value["type"], "error");
        assert_eq!(value["error"]["type"], "invalid_request_error");
        assert!(
            value["error"]["message"]
                .as_str()
                .expect("a message string")
                .contains("forged")
        );
    }
}
