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
    /// What the provider said, from whichever of three shapes carried it — see
    /// `read`. `None` only for a body with nothing in it at all.
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

/// How much of a provider's message reaches the client. Every shape goes through
/// this — an `error.message` is as provider-controlled and as unbounded as a raw
/// body, and clipping only the snippet would leave a provider able to put a
/// megabyte in the user's session transcript by choosing the ordinary field.
/// Counted in `char`s so the clip cannot split a multi-byte boundary. Independent of
/// the log's own cap: this one is about what a client should be asked to render,
/// that one about log volume.
const MESSAGE_SNIPPET_CHARS: usize = 512;

impl ProviderError {
    pub(crate) fn read(status: StatusCode, body: &[u8]) -> Self {
        let parsed: Option<Value> = serde_json::from_slice(body).ok();
        let error = parsed.as_ref().and_then(|value| value.get("error"));
        let field = |name: &str| error.and_then(|error| error.get(name))?.as_str();

        // The two shapes a provider *authored as a message*: the OpenAI/Anthropic
        // `error.message`, and the top level, where vLLM and several
        // OpenAI-compatible servers put it
        // (`{"object":"error","message":…,"type":"BadRequestError"}`).
        let authored = field("message").or_else(|| parsed.as_ref()?.get("message")?.as_str());

        Self {
            kind: anthropic_type(status, field("type")),
            // **Detection reads only an authored message**, never the raw body.
            // Reading the body meant deciding "the prompt did not fit" from bytes
            // the provider never wrote as a message — and a pydantic-shaped 400
            // echoes the rejected request back under `input`, so a *malformed*
            // request whose own chat text mentions a context length was reshaped
            // into a too-long claim built from the user's own numbers. The client
            // then shrinks, retries the same malformed request, and loops. Neither
            // the status gate nor the marker list can defend that: the status
            // really is 400, and the marker arrives inside content the client sent.
            context_limit: authored.and_then(|message| context_limit(status, message)),
            // The client, though, still sees whatever arrived — an unrecognised
            // shape included. A bounded snippet of a `{"detail":…}` body or a
            // `text/plain` 413 beats a sentence that says nothing: the operator has
            // the log, the client's user has only this.
            message: authored.map(clipped_message).or_else(|| snippet(body)),
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
    /// `/prompt is too long[^0-9]*(\d+)\s*tokens?\s*>\s*(\d+)/i`, whose captures it
    /// reads as `actualTokens` then `limitTokens` — so both the order of the pair
    /// and where the phrase sits relative to the digits decide whether the client
    /// can size its retry (docs/decisions.md).
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

/// The single clip every client-visible message passes through.
fn clipped_message(text: &str) -> String {
    text.chars().take(MESSAGE_SNIPPET_CHARS).collect()
}

/// A clipped view of a body no recognised field could be read out of. `None` for
/// one with nothing in it, which is the only case where the relay genuinely has
/// nothing to report — a read that failed its cap, or an empty body.
fn snippet(body: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(body);
    let trimmed = text.trim();
    (!trimmed.is_empty()).then(|| clipped_message(trimmed))
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
    whole_number(bytes, start, end).map(|value| (value, end))
}

fn first_digit_run(text: &str) -> Option<u64> {
    let bytes = text.as_bytes();
    let start = bytes.iter().position(u8::is_ascii_digit)?;
    let end = bytes[start..]
        .iter()
        .position(|byte| !byte.is_ascii_digit())
        .map_or(bytes.len(), |at| start + at);
    whole_number(bytes, start, end)
}

/// The run at `start..end` as a number — but only if it is a whole number rather
/// than one group of a separated one.
///
/// A thousands separator splits `170,071` into `170` and `071`, and the
/// input-over-limit guard cannot catch what that produces: measured, a message
/// reading `(170,071 tokens) … (2,072 tokens)` yields `(71, 2)`, which passes the
/// guard and tells the client to shed 69 tokens from a request tens of thousands
/// over. It then shaves nothing, retries, and loops — carrying the relay's own
/// numbers, which is worse than failing honestly. So a group of a separated number
/// is refused outright: read as one number or not at all.
///
/// This cannot fire on Together's captured wording, which uses no separators. It is
/// here because two of the three `CONTEXT_LIMIT_MARKERS` exist only to match
/// wordings nobody here has captured, so the parser has to survive wordings nobody
/// here has captured.
fn whole_number(bytes: &[u8], start: usize, end: usize) -> Option<u64> {
    let continues_a_number =
        start >= 2 && is_group_separator(bytes[start - 1]) && bytes[start - 2].is_ascii_digit();
    let is_continued_by =
        end + 1 < bytes.len() && is_group_separator(bytes[end]) && bytes[end + 1].is_ascii_digit();
    if continues_a_number || is_continued_by {
        return None;
    }
    digits(&bytes[start..end])
}

/// Separators seen in the wild between groups of one number: `,` (English), `.`
/// (European), and space or `_` (both used by some formatters). A separator only
/// counts as one when a digit sits on both sides of it, so an ordinary sentence's
/// full stop or space is unaffected.
fn is_group_separator(byte: u8) -> bool {
    matches!(byte, b',' | b'.' | b'_' | b' ')
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

    /// Thousands separators split a number into groups, and the input-over-limit
    /// guard cannot catch what that produces — measured, the last case here yielded
    /// `(71, 2)`, which passes the guard and tells the client to shed 69 tokens from
    /// a request tens of thousands over. Anchoring alone does not close this: it
    /// happens to reject the first two, and does not reject the last.
    #[test]
    fn a_separated_number_is_read_whole_or_not_at_all() {
        for (label, message) in [
            (
                "English separators, both numbers",
                "The input (170,071 tokens) is longer than the model's context length (131,072 tokens).",
            ),
            (
                "separators, limit before input",
                "This model's maximum context length is 131,072 tokens. However, you requested 170,071 tokens.",
            ),
            (
                // The one anchoring alone gets wrong: `(71, 2)` survives the guard.
                "separators whose groups pass the guard",
                "The input (170,071 tokens) is longer than the model's context length (2,072 tokens).",
            ),
            (
                "European separators",
                "The input (170.071 tokens) is longer than the model's context length (131.072 tokens).",
            ),
            (
                "space separators",
                "The input (170 071 tokens) is longer than the model's context length (131 072 tokens).",
            ),
        ] {
            assert_eq!(
                counts(message),
                None,
                "{label} must not yield a pair built from one group"
            );
        }

        // A version string is separated the same way, and must not be mistaken for
        // the input size either. Anchoring already looks past it, so this pins that
        // the separator guard did not make the case worse.
        assert_eq!(
            counts(
                "deepseek-ai/DeepSeek-V3.1: The input (170071 tokens) is longer than the model's context length (131072 tokens)."
            ),
            Some((170071, 131072)),
            "a dotted model version must not disturb the real pair"
        );
    }

    /// The separator rule must not fire on ordinary punctuation: a full stop or a
    /// space with no digit on the far side is just prose.
    #[test]
    fn ordinary_punctuation_around_a_number_is_not_a_separator() {
        assert_eq!(
            counts(
                "The input (170071 tokens) is longer than the model's context length (131072 tokens)."
            ),
            Some((170071, 131072)),
            "the measured wording ends in a full stop right after a digit run"
        );
        assert_eq!(
            counts("sent 99999 tokens, past the maximum context length of 8192"),
            Some((99999, 8192)),
            "a comma after a digit run with a space on the far side is prose"
        );
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

    /// Fix round 2's blocker, and the sharpest version of "not every 400 is a
    /// context-limit error". A pydantic/FastAPI-shaped 400 echoes the rejected
    /// request back under `input`, so the body carries the *client's own* chat text.
    /// When detection read the raw body, a **malformed** request whose transcript
    /// happened to mention a context length became a too-long claim built from the
    /// user's own numbers — the client shrinks, retries the same malformed request,
    /// fails identically, and loops. Neither guard can catch it: the status really is
    /// 400, and the marker arrives inside content the client sent.
    ///
    /// So detection reads only a message the *provider* authored.
    #[test]
    fn a_body_that_echoes_the_request_back_is_not_read_as_a_context_limit() {
        let body = concat!(
            r#"{"detail":[{"type":"missing","loc":["body","messages",3,"content"],"#,
            r#""msg":"Field required","input":{"messages":[{"role":"user","content":"#,
            r#""my last run said 170071 tokens which is over the maximum context "#,
            r#"length of 131072 - why?"}]}}]}"#
        );
        let error = read(400, body);
        assert!(
            error.context_limit.is_none(),
            "the client's own words became a too-long claim"
        );

        // The user still sees what the provider said — blocker 5's fix is intact —
        // but with none of Anthropic's recovery wording bolted onto it.
        let message = error.client_message();
        assert!(message.contains("Field required"), "{message}");
        assert!(
            !message.to_lowercase().contains("prompt is too long"),
            "a malformed-request error must not carry the phrase: {message}"
        );
    }

    /// The cost of that fix, taken knowingly: a `text/plain` body carrying a genuine
    /// context-limit sentence is no longer detected, because nothing authored it as a
    /// message. The asymmetry decides it — a false negative costs what already
    /// happened before this task, a false positive is a loop that never terminates.
    #[test]
    fn an_unauthored_body_is_not_detected_even_when_its_wording_is_genuine() {
        let error = read(
            413,
            "Request too large: the input is longer than the model's context length",
        );
        assert!(error.context_limit.is_none());
        // Still surfaced, so the user is told the real reason.
        assert!(error.client_message().contains("Request too large"));
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
    ///
    /// This test used to assert the generic sentence for every one of these, and
    /// that was the suite **defending** a regression rather than missing one: the
    /// verbatim pass-through it replaced let a flat body reach the client's SDK,
    /// which prefers a top-level `message`, so the user saw the real reason. Only
    /// the truly empty body has nothing to report.
    #[test]
    fn a_body_in_an_unrecognised_shape_still_carries_what_it_said() {
        // vLLM and friends: the sentence is at the top level, not under `error`.
        let flat = r#"{"object":"error","message":"This model's maximum context length is 131072 tokens. However, you requested 170071 tokens.","type":"BadRequestError"}"#;
        assert_eq!(
            read(400, flat).client_message(),
            "This model's maximum context length is 131072 tokens. However, you requested 170071 tokens. (prompt is too long)",
            "a top-level message is read, and detection sees it"
        );

        // Neither shape: a snippet of what arrived, which is strictly more than a
        // sentence saying nothing.
        for (body, expected) in [
            (
                r#"{"detail":"The input (170071 tokens) is longer than the model's context length (131072 tokens)."}"#,
                "The input (170071 tokens) is longer than the model's context length",
            ),
            (
                // A `text/plain` 413 from an intermediary. This one is why the rule
                // has to be uniform rather than JSON-only — and, being plain text,
                // it is the same rule that decides the HTML case below.
                "Request too large: the input is longer than the model's context length",
                "Request too large",
            ),
            ("<html>502 Bad Gateway</html>", "502 Bad Gateway"),
            (r#"{"error":"flat"}"#, "flat"),
        ] {
            let message = read(400, body).client_message();
            assert!(
                message.contains(expected),
                "the provider's own bytes must survive into the message: {message:?}"
            );
        }
    }

    /// The one case with genuinely nothing to report: an empty body, which is also
    /// what a read that failed its cap produces.
    #[test]
    fn an_empty_body_still_produces_an_envelope() {
        for body in ["", "   "] {
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

    /// Every shape is bounded, not just the snippet. An `error.message` is as
    /// provider-controlled as a raw body, so clipping only the snippet left a
    /// provider able to put a megabyte in the user's session transcript by choosing
    /// the ordinary field — measured at 900,000 chars before this was fixed.
    #[test]
    fn every_shape_of_message_is_clipped_before_it_reaches_the_client() {
        let huge = "z".repeat(MESSAGE_SNIPPET_CHARS * 4);
        for body in [
            // `error.message`
            format!(r#"{{"error":{{"message":"{huge}","type":"invalid_request_error"}}}}"#),
            // top-level `message`
            format!(r#"{{"object":"error","message":"{huge}"}}"#),
            // neither: the snippet path
            huge.clone(),
        ] {
            let message = read(400, &body).client_message();
            assert_eq!(
                message.chars().count(),
                MESSAGE_SNIPPET_CHARS,
                "an unbounded message reached the client: {} chars",
                message.chars().count()
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
