//! Anthropic ↔ OpenAI wire-format translation (spec §7c Phase 2).
//!
//! Pure transformation: no HTTP client, no routing, no header handling. The
//! caller supplies bytes (or a byte stream) and gets bytes back.

mod anthropic;
mod openai;
mod request;
mod response;
mod sse;

pub use request::{TranslatedRequest, request_to_openai};
pub use response::response_to_anthropic;
pub use sse::{SseTranslator, sse_stream};

/// Ceiling on what SSE synthesis holds in memory: an unterminated frame, and
/// separately the total across all tool-call slots (ids, names, and arguments
/// buffered while another call's block is open). Larger than Milestone 1's
/// 1 MiB error-body cap because a single tool call legitimately carries a whole
/// file's contents as its arguments, and a provider is free to send those in
/// one frame.
pub(crate) const BUFFER_CAP: usize = 4 * 1024 * 1024;

/// A parse failure carrying the location but *not* `serde_json`'s own message,
/// which embeds the offending value on a type mismatch (`invalid type: string
/// "…"`). Everything else in this module is careful never to let a request or
/// response value reach an error string — the caller is free to log what it
/// gets back — and this is the one place that discipline is easiest to lose.
pub(crate) fn parse_failure(context: &str, error: &serde_json::Error) -> anyhow::Error {
    anyhow::anyhow!(
        "{context} (at line {}, column {})",
        error.line(),
        error.column()
    )
}

/// Separator used wherever several Anthropic text blocks collapse into one
/// OpenAI string (`system`, a multi-block message, a `tool_result` body).
/// Anthropic documents no separator of its own, so this picks the one that
/// cannot run two sentences together.
pub(crate) const BLOCK_JOIN: &str = "\n\n";
