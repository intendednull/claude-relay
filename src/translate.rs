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

/// Ceiling on the one thing SSE synthesis accumulates without bound in
/// principle — an unterminated frame, or a tool call's arguments held back
/// while its name is still unknown. Larger than Milestone 1's 1 MiB error-body
/// cap because a single tool call legitimately carries a whole file's contents
/// as its arguments, and a provider is free to send those in one frame.
pub(crate) const BUFFER_CAP: usize = 4 * 1024 * 1024;

/// Separator used wherever several Anthropic text blocks collapse into one
/// OpenAI string (`system`, a multi-block message, a `tool_result` body).
/// Anthropic documents no separator of its own, so this picks the one that
/// cannot run two sentences together.
pub(crate) const BLOCK_JOIN: &str = "\n\n";
