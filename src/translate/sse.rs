//! OpenAI `chat.completion.chunk` SSE → Anthropic SSE events.
//!
//! [`SseTranslator`] is sans-IO on purpose: bytes in, bytes out, no futures, so
//! the synthesis rules are testable one frame at a time. [`sse_stream`] is the
//! thin adapter that drives it from an upstream byte stream, and is the only
//! part that knows what a `Stream` is.

use std::collections::HashMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::Bytes;
use futures_core::Stream;
use serde_json::{Value, json};

use super::BUFFER_CAP;
use super::openai::{ChatCompletionChunk, ToolCallDelta, Usage};
use super::response::stop_reason;

/// What the client is told when the upstream connection itself fails. The
/// underlying error is deliberately *not* interpolated: it can carry the
/// upstream URL, and a profile's `base_url` is one of the few places a
/// credential could hide (Global Constraint 2).
const UPSTREAM_FAILED: &str = "upstream stream ended unexpectedly";

#[derive(Debug, Default)]
pub struct SseTranslator {
    buf: Vec<u8>,
    started: bool,
    done: bool,
    next_index: u32,
    open: Option<Open>,
    tools: HashMap<u32, ToolCallState>,
    scanned: usize,
    last_slot: Option<u32>,
    finish_reason: Option<String>,
    usage: Option<Usage>,
    id: Option<String>,
    model: Option<String>,
    emitted_tool_block: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Open {
    Text(u32),
    Tool { block: u32, slot: u32 },
}

#[derive(Debug, Default)]
struct ToolCallState {
    id: String,
    name: Option<String>,
    /// Argument fragments that arrived before the function name did. Anthropic
    /// cannot open a `tool_use` block without the name, so they wait here and
    /// are replayed in order the moment it turns up.
    pending: String,
    block: Option<u32>,
}

impl SseTranslator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feeds the next upstream bytes in and returns whatever Anthropic events
    /// they completed — empty when the bytes did not finish an SSE frame.
    pub fn push(&mut self, chunk: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        if self.done {
            return out;
        }
        self.buf.extend_from_slice(chunk);
        while !self.done {
            let Some(frame) = self.take_frame() else {
                break;
            };
            self.handle_frame(&frame, &mut out);
        }
        if !self.done && self.buf.len() > BUFFER_CAP {
            self.fail(
                "upstream SSE frame exceeded the relay's buffer cap",
                &mut out,
            );
        }
        out
    }

    /// Closes out the message after the upstream stream ends cleanly. Safe to
    /// call after a `[DONE]` frame already finished it.
    pub fn finish(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        self.finish_into(&mut out);
        out
    }

    /// Terminates the stream with an Anthropic `error` event (spec §6: once
    /// bytes have reached the client, a failure ends that stream — it is never
    /// silently retried).
    pub fn abort(&mut self, message: &str) -> Vec<u8> {
        let mut out = Vec::new();
        self.fail(message, &mut out);
        out
    }

    fn take_frame(&mut self) -> Option<Vec<u8>> {
        let Some((end, skip)) = frame_end(&self.buf, self.scanned) else {
            // Everything but the last two bytes is known to hold no
            // terminator, and re-scanning it on every chunk would make a frame
            // delivered in small pieces cost time quadratic in its length.
            self.scanned = self.buf.len().saturating_sub(2);
            return None;
        };
        let frame = self.buf[..end].to_vec();
        self.buf.drain(..end + skip);
        self.scanned = 0;
        Some(frame)
    }

    fn handle_frame(&mut self, frame: &[u8], out: &mut Vec<u8>) {
        let Some(data) = frame_data(frame) else {
            return;
        };
        if data.trim() == "[DONE]" {
            self.finish_into(out);
            return;
        }
        let Ok(value) = serde_json::from_str::<Value>(&data) else {
            self.fail("upstream sent an SSE frame that is not JSON", out);
            return;
        };
        // An upstream error mid-stream is the provider's own message and the
        // only useful thing to hand the client, so it is forwarded rather than
        // replaced — it is response content, not something that gets logged.
        // `null` does not count: several providers put `"error": null` on every
        // ordinary chunk, and treating that as a failure would kill every
        // stream they serve.
        if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("upstream reported an error");
            self.fail(message, out);
            return;
        }
        match serde_json::from_value::<ChatCompletionChunk>(value) {
            Ok(chunk) => self.apply(chunk, out),
            Err(_) => self.fail("upstream sent a malformed completion chunk", out),
        }
    }

    fn apply(&mut self, chunk: ChatCompletionChunk, out: &mut Vec<u8>) {
        if let Some(usage) = chunk.usage {
            self.usage = Some(usage);
        }
        if !self.started {
            self.id = chunk.id;
            self.model = chunk.model;
            self.start_message(out);
        }
        let Some(choice) = chunk.choices.into_iter().next() else {
            return;
        };
        if let Some(text) = choice.delta.content.filter(|text| !text.is_empty()) {
            let index = self.open_text(out);
            emit(
                out,
                "content_block_delta",
                json!({
                    "type": "content_block_delta",
                    "index": index,
                    "delta": {"type": "text_delta", "text": text},
                }),
            );
        }
        for delta in choice.delta.tool_calls {
            if self.done {
                return;
            }
            self.apply_tool_delta(delta, out);
        }
        if let Some(reason) = choice.finish_reason {
            self.finish_reason = Some(reason);
        }
    }

    fn apply_tool_delta(&mut self, delta: ToolCallDelta, out: &mut Vec<u8>) {
        let slot = self.slot_for(&delta);
        self.last_slot = Some(slot);
        let (name, fragment) = match delta.function {
            Some(function) => (function.name, function.arguments.unwrap_or_default()),
            None => (None, String::new()),
        };

        let state = self.tools.entry(slot).or_default();
        if let Some(id) = delta.id.filter(|id| !id.is_empty())
            && state.id.is_empty()
        {
            state.id = id;
        }
        // Set-once: OpenAI sends the whole function name in the first fragment
        // for a call, but several compatible providers repeat it in every
        // fragment, and appending would corrupt the name.
        if let Some(name) = name.filter(|name| !name.is_empty())
            && state.name.is_none()
        {
            state.name = Some(name);
        }

        if let Some(block) = state.block {
            if self.open != Some(Open::Tool { block, slot }) {
                self.fail(
                    "upstream resumed a tool call whose content block had already closed",
                    out,
                );
                return;
            }
            self.emit_input_json(block, &fragment, out);
            return;
        }

        let Some(name) = state.name.clone() else {
            state.pending.push_str(&fragment);
            if state.pending.len() > BUFFER_CAP {
                self.fail(
                    "upstream tool call arguments exceeded the relay's buffer cap \
                     before a function name arrived",
                    out,
                );
            }
            return;
        };
        if state.id.is_empty() {
            self.fail(
                "upstream tool call has no id to match its result against",
                out,
            );
            return;
        }
        let id = state.id.clone();
        let pending = std::mem::take(&mut state.pending);

        self.close_open(out);
        let block = self.next_index;
        self.next_index += 1;
        self.tools
            .get_mut(&slot)
            .expect("slot was just populated")
            .block = Some(block);
        self.open = Some(Open::Tool { block, slot });
        self.emitted_tool_block = true;
        emit(
            out,
            "content_block_start",
            json!({
                "type": "content_block_start",
                "index": block,
                "content_block": {"type": "tool_use", "id": id, "name": name, "input": {}},
            }),
        );
        self.emit_input_json(block, &pending, out);
        self.emit_input_json(block, &fragment, out);
    }

    /// Argument fragments are re-emitted exactly as they arrived: Anthropic's
    /// `partial_json` and OpenAI's `function.arguments` fragments both simply
    /// concatenate to the complete document, so this is a pass-through, not a
    /// re-encoding. Nothing here can reorder or reshape a tool call's JSON.
    fn emit_input_json(&self, block: u32, fragment: &str, out: &mut Vec<u8>) {
        if fragment.is_empty() {
            return;
        }
        emit(
            out,
            "content_block_delta",
            json!({
                "type": "content_block_delta",
                "index": block,
                "delta": {"type": "input_json_delta", "partial_json": fragment},
            }),
        );
    }

    /// `index` identifies which of several parallel calls a fragment belongs
    /// to. Providers that only ever stream one call sometimes omit it, so an
    /// absent index means "the call already in flight" — unless the fragment
    /// carries a different id, which is a new call.
    fn slot_for(&self, delta: &ToolCallDelta) -> u32 {
        if let Some(index) = delta.index {
            return index;
        }
        let Some(last) = self.last_slot else { return 0 };
        match (self.tools.get(&last), delta.id.as_deref()) {
            (Some(state), Some(id)) if !state.id.is_empty() && state.id != id => last + 1,
            _ => last,
        }
    }

    fn open_text(&mut self, out: &mut Vec<u8>) -> u32 {
        if let Some(Open::Text(index)) = self.open {
            return index;
        }
        self.close_open(out);
        let index = self.next_index;
        self.next_index += 1;
        emit(
            out,
            "content_block_start",
            json!({
                "type": "content_block_start",
                "index": index,
                "content_block": {"type": "text", "text": ""},
            }),
        );
        self.open = Some(Open::Text(index));
        index
    }

    fn close_open(&mut self, out: &mut Vec<u8>) {
        let Some(open) = self.open.take() else { return };
        let index = match open {
            Open::Text(index) => index,
            Open::Tool { block, .. } => block,
        };
        emit(
            out,
            "content_block_stop",
            json!({"type": "content_block_stop", "index": index}),
        );
    }

    fn start_message(&mut self, out: &mut Vec<u8>) {
        self.started = true;
        emit(
            out,
            "message_start",
            json!({
                "type": "message_start",
                "message": {
                    "id": self.id.clone().unwrap_or_else(|| "msg_translated".to_string()),
                    "type": "message",
                    "role": "assistant",
                    "model": self.model.clone().unwrap_or_default(),
                    "content": [],
                    "stop_reason": Value::Null,
                    "stop_sequence": Value::Null,
                    // Totals are reported in `message_delta`, where they are
                    // actually known: OpenAI's usage arrives at the end of the
                    // stream, if at all.
                    "usage": {"input_tokens": 0, "output_tokens": 0},
                },
            }),
        );
    }

    fn finish_into(&mut self, out: &mut Vec<u8>) {
        if self.done {
            return;
        }
        if !self.started {
            self.start_message(out);
        }
        if self
            .tools
            .values()
            .any(|state| state.block.is_none() && !state.pending.is_empty())
        {
            self.fail(
                "upstream ended a tool call that never carried a function name",
                out,
            );
            return;
        }
        self.close_open(out);
        let stop_reason = match self.finish_reason.as_deref() {
            Some(reason) => stop_reason(reason),
            // No `finish_reason` at all: infer from what the turn actually
            // produced rather than reporting a null the client has to guess at.
            None if self.emitted_tool_block => "tool_use",
            None => "end_turn",
        };
        let usage = self.usage.unwrap_or_default();
        emit(
            out,
            "message_delta",
            json!({
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": Value::Null},
                "usage": {
                    "input_tokens": usage.prompt_tokens.unwrap_or(0),
                    "output_tokens": usage.completion_tokens.unwrap_or(0),
                },
            }),
        );
        emit(out, "message_stop", json!({"type": "message_stop"}));
        self.done = true;
    }

    fn fail(&mut self, message: &str, out: &mut Vec<u8>) {
        if self.done {
            return;
        }
        self.close_open(out);
        emit(
            out,
            "error",
            json!({
                "type": "error",
                "error": {"type": "api_error", "message": message},
            }),
        );
        self.done = true;
    }
}

fn emit(out: &mut Vec<u8>, event: &str, data: Value) {
    out.extend_from_slice(b"event: ");
    out.extend_from_slice(event.as_bytes());
    out.extend_from_slice(b"\ndata: ");
    serde_json::to_writer(&mut *out, &data).expect("a serde_json::Value always serializes");
    out.extend_from_slice(b"\n\n");
}

/// Offset of the blank line ending the first complete frame at or after
/// `from`, and how many bytes that terminator occupies (`\n\n` or `\r\n\r\n`).
fn frame_end(buf: &[u8], from: usize) -> Option<(usize, usize)> {
    for index in from..buf.len() {
        if buf[index] != b'\n' {
            continue;
        }
        match (buf.get(index + 1), buf.get(index + 2)) {
            (Some(b'\n'), _) => return Some((index, 2)),
            (Some(b'\r'), Some(b'\n')) => return Some((index, 3)),
            _ => {}
        }
    }
    None
}

/// The frame's `data:` payload, with multiple `data:` lines joined by newline
/// as SSE specifies. `None` for frames carrying no data at all — comments and
/// heartbeats, which several providers send to keep the connection alive.
fn frame_data(frame: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(frame).ok()?;
    let mut data = String::new();
    let mut seen = false;
    for line in text.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        let Some(rest) = line.strip_prefix("data:") else {
            continue;
        };
        if seen {
            data.push('\n');
        }
        seen = true;
        data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
    }
    seen.then_some(data)
}

/// Wraps an upstream OpenAI-format SSE byte stream as an Anthropic-format one.
/// Each poll emits whatever the bytes just received completed, so nothing waits
/// on the end of the upstream response.
///
/// The output stream is infallible by construction: an upstream failure becomes
/// a terminal Anthropic `error` event and the body then ends *cleanly*. That is
/// spec §6's requirement — a failed stream terminates with an error event the
/// client can read — and it only holds if the body ends properly, because a
/// body aborted mid-frame loses whatever was still buffered, error event
/// included (observed, not assumed: an earlier version propagated the error and
/// the client never saw the event).
///
/// The consequence for the caller: the upstream error object does not come back
/// out of here, and the event text never carries it either, since it can hold
/// the upstream URL and a profile's `base_url` is a place a credential could
/// hide (Global Constraint 2). A caller that wants the error's own details in
/// its logs should inspect the upstream stream on the way in, before handing it
/// over.
pub fn sse_stream<S, E>(upstream: S) -> impl Stream<Item = Result<Bytes, Infallible>>
where
    S: Stream<Item = Result<Bytes, E>>,
{
    TranslatingStream {
        inner: Box::pin(upstream),
        translator: SseTranslator::new(),
        drained: false,
    }
}

struct TranslatingStream<S> {
    inner: Pin<Box<S>>,
    translator: SseTranslator,
    drained: bool,
}

impl<S, E> Stream for TranslatingStream<S>
where
    S: Stream<Item = Result<Bytes, E>>,
{
    type Item = Result<Bytes, Infallible>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.drained {
                return Poll::Ready(None);
            }
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    let out = this.translator.push(&chunk);
                    if !out.is_empty() {
                        return Poll::Ready(Some(Ok(Bytes::from(out))));
                    }
                }
                Poll::Ready(Some(Err(_))) => {
                    tracing::warn!("fallback upstream stream failed mid-response");
                    this.drained = true;
                    let out = this.translator.abort(UPSTREAM_FAILED);
                    if !out.is_empty() {
                        return Poll::Ready(Some(Ok(Bytes::from(out))));
                    }
                    return Poll::Ready(None);
                }
                Poll::Ready(None) => {
                    this.drained = true;
                    let out = this.translator.finish();
                    if !out.is_empty() {
                        return Poll::Ready(Some(Ok(Bytes::from(out))));
                    }
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `(event name, data)` for every Anthropic frame in `bytes`.
    fn events(bytes: &[u8]) -> Vec<(String, Value)> {
        let text = std::str::from_utf8(bytes).expect("output must be UTF-8");
        text.split("\n\n")
            .filter(|frame| !frame.trim().is_empty())
            .map(|frame| {
                let mut event = None;
                let mut data = String::new();
                for line in frame.split('\n') {
                    if let Some(rest) = line.strip_prefix("event: ") {
                        event = Some(rest.to_string());
                    } else if let Some(rest) = line.strip_prefix("data: ") {
                        data.push_str(rest);
                    }
                }
                let event = event.expect("every frame carries an event name");
                let data = serde_json::from_str(&data).expect("every frame carries JSON data");
                (event, data)
            })
            .collect()
    }

    /// Feeds each string as one upstream chunk, then ends the stream.
    fn synthesize(chunks: &[&str]) -> Vec<(String, Value)> {
        let mut translator = SseTranslator::new();
        let mut out = Vec::new();
        for chunk in chunks {
            out.extend(translator.push(chunk.as_bytes()));
        }
        out.extend(translator.finish());
        events(&out)
    }

    fn names(events: &[(String, Value)]) -> Vec<&str> {
        events.iter().map(|(name, _)| name.as_str()).collect()
    }

    fn frame(payload: Value) -> String {
        format!("data: {payload}\n\n")
    }

    fn text_chunk(text: &str) -> String {
        frame(json!({
            "id": "chatcmpl-1",
            "object": "chat.completion.chunk",
            "model": "target/Model",
            "choices": [{"index": 0, "delta": {"content": text}, "finish_reason": Value::Null}],
        }))
    }

    fn tool_chunk(delta: Value) -> String {
        frame(json!({
            "id": "chatcmpl-1",
            "object": "chat.completion.chunk",
            "model": "target/Model",
            "choices": [{"index": 0, "delta": {"tool_calls": [delta]}, "finish_reason": Value::Null}],
        }))
    }

    fn finish_chunk(reason: &str) -> String {
        frame(json!({
            "id": "chatcmpl-1",
            "object": "chat.completion.chunk",
            "model": "target/Model",
            "choices": [{"index": 0, "delta": {}, "finish_reason": reason}],
            "usage": {"prompt_tokens": 31, "completion_tokens": 12, "total_tokens": 43},
        }))
    }

    #[test]
    fn a_text_stream_synthesizes_the_full_anthropic_event_sequence() {
        let events = synthesize(&[
            &frame(json!({
                "id": "chatcmpl-1",
                "model": "target/Model",
                "choices": [{"index": 0, "delta": {"role": "assistant", "content": ""}}],
            })),
            &text_chunk("Hello"),
            &text_chunk(" there"),
            &finish_chunk("stop"),
            "data: [DONE]\n\n",
        ]);

        assert_eq!(
            names(&events),
            vec![
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ]
        );
        assert_eq!(
            events[0].1["message"],
            json!({
                "id": "chatcmpl-1",
                "type": "message",
                "role": "assistant",
                "model": "target/Model",
                "content": [],
                "stop_reason": Value::Null,
                "stop_sequence": Value::Null,
                "usage": {"input_tokens": 0, "output_tokens": 0},
            }),
            "the empty first delta must not open a content block of its own"
        );
        assert_eq!(
            events[1].1,
            json!({"type": "content_block_start", "index": 0,
                   "content_block": {"type": "text", "text": ""}})
        );
        assert_eq!(
            events[2].1,
            json!({"type": "content_block_delta", "index": 0,
                   "delta": {"type": "text_delta", "text": "Hello"}})
        );
        assert_eq!(
            events[4].1,
            json!({"type": "content_block_stop", "index": 0})
        );
        assert_eq!(
            events[5].1,
            json!({
                "type": "message_delta",
                "delta": {"stop_reason": "end_turn", "stop_sequence": Value::Null},
                "usage": {"input_tokens": 31, "output_tokens": 12},
            })
        );
    }

    #[test]
    fn tool_arguments_fragmented_across_chunks_reassemble_exactly() {
        let events = synthesize(&[
            &tool_chunk(json!({
                "index": 0, "id": "call_1", "type": "function",
                "function": {"name": "Bash", "arguments": ""},
            })),
            &tool_chunk(json!({"index": 0, "function": {"arguments": "{\"comm"}})),
            &tool_chunk(json!({"index": 0, "function": {"arguments": "and\":\"ls "}})),
            &tool_chunk(json!({"index": 0, "function": {"arguments": "-la\",\"time"}})),
            &tool_chunk(json!({"index": 0, "function": {"arguments": "out\":5000}"}})),
            &finish_chunk("tool_calls"),
            "data: [DONE]\n\n",
        ]);

        assert_eq!(
            names(&events),
            vec![
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_delta",
                "content_block_delta",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ]
        );
        assert_eq!(
            events[1].1,
            json!({"type": "content_block_start", "index": 0, "content_block": {
                "type": "tool_use", "id": "call_1", "name": "Bash", "input": {},
            }}),
            "the empty first arguments fragment must not become a delta of its own"
        );

        let reassembled: String = events
            .iter()
            .filter(|(name, data)| {
                name == "content_block_delta" && data["delta"]["type"] == "input_json_delta"
            })
            .map(|(_, data)| data["delta"]["partial_json"].as_str().unwrap())
            .collect();
        assert_eq!(reassembled, r#"{"command":"ls -la","timeout":5000}"#);
        assert_eq!(
            serde_json::from_str::<Value>(&reassembled).unwrap(),
            json!({"command": "ls -la", "timeout": 5000})
        );
        assert_eq!(events[7].1["delta"]["stop_reason"], "tool_use");
    }

    #[test]
    fn text_followed_by_a_tool_call_closes_the_text_block_first() {
        let events = synthesize(&[
            &text_chunk("Let me look."),
            &tool_chunk(json!({
                "index": 0, "id": "call_1", "function": {"name": "Read", "arguments": "{}"},
            })),
            &finish_chunk("tool_calls"),
        ]);

        assert_eq!(
            names(&events),
            vec![
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ]
        );
        assert_eq!(events[3].1["index"], 0);
        assert_eq!(
            events[4].1["index"], 1,
            "the tool block takes the next index"
        );
        assert_eq!(events[4].1["content_block"]["name"], "Read");
    }

    #[test]
    fn parallel_tool_calls_become_consecutive_blocks() {
        let events = synthesize(&[
            &tool_chunk(json!({
                "index": 0, "id": "call_1", "function": {"name": "Bash", "arguments": "{\"a\":1}"},
            })),
            &tool_chunk(json!({
                "index": 1, "id": "call_2", "function": {"name": "Read", "arguments": "{\"b\":2}"},
            })),
            &finish_chunk("tool_calls"),
        ]);

        let blocks: Vec<_> = events
            .iter()
            .filter(|(name, _)| name == "content_block_start")
            .map(|(_, data)| (data["index"].clone(), data["content_block"]["id"].clone()))
            .collect();
        assert_eq!(
            blocks,
            vec![(json!(0), json!("call_1")), (json!(1), json!("call_2"))]
        );
        assert_eq!(
            names(&events),
            vec![
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ],
            "the first tool block must close before the second opens"
        );
    }

    #[test]
    fn arguments_arriving_before_the_function_name_are_replayed_in_order() {
        // Anthropic cannot open a tool_use block without the name, so these
        // fragments have to wait — and then come out ahead of the fragment
        // that arrived alongside the name.
        let events = synthesize(&[
            &tool_chunk(json!({"index": 0, "id": "call_1", "function": {"arguments": "{\"x\":"}})),
            &tool_chunk(json!({"index": 0, "function": {"arguments": "1,"}})),
            &tool_chunk(json!({
                "index": 0, "function": {"name": "Late", "arguments": "\"y\":2}"},
            })),
            &finish_chunk("tool_calls"),
        ]);

        assert_eq!(events[1].0, "content_block_start");
        assert_eq!(events[1].1["content_block"]["name"], "Late");
        let reassembled: String = events
            .iter()
            .filter(|(name, data)| {
                name == "content_block_delta" && data["delta"]["type"] == "input_json_delta"
            })
            .map(|(_, data)| data["delta"]["partial_json"].as_str().unwrap())
            .collect();
        assert_eq!(reassembled, r#"{"x":1,"y":2}"#);
    }

    #[test]
    fn a_repeated_function_name_does_not_corrupt_it() {
        let events = synthesize(&[
            &tool_chunk(json!({
                "index": 0, "id": "call_1", "function": {"name": "Bash", "arguments": "{"},
            })),
            &tool_chunk(json!({
                "index": 0, "id": "call_1", "function": {"name": "Bash", "arguments": "}"},
            })),
            &finish_chunk("tool_calls"),
        ]);
        assert_eq!(events[1].1["content_block"]["name"], "Bash");
    }

    #[test]
    fn a_tool_call_streamed_without_an_index_stays_one_block() {
        let events = synthesize(&[
            &tool_chunk(
                json!({"id": "call_1", "function": {"name": "Bash", "arguments": "{\"a\""}}),
            ),
            &tool_chunk(json!({"function": {"arguments": ":1}"}})),
            &finish_chunk("tool_calls"),
        ]);

        assert_eq!(
            names(&events),
            vec![
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ]
        );
    }

    #[test]
    fn a_new_id_without_an_index_starts_a_second_block() {
        let events = synthesize(&[
            &tool_chunk(json!({"id": "call_1", "function": {"name": "Bash", "arguments": "{}"}})),
            &tool_chunk(json!({"id": "call_2", "function": {"name": "Read", "arguments": "{}"}})),
            &finish_chunk("tool_calls"),
        ]);

        let blocks: Vec<_> = events
            .iter()
            .filter(|(name, _)| name == "content_block_start")
            .map(|(_, data)| data["content_block"]["id"].clone())
            .collect();
        assert_eq!(blocks, vec![json!("call_1"), json!("call_2")]);
    }

    #[test]
    fn a_reopened_tool_call_fails_loudly_rather_than_splitting_its_json() {
        let events = synthesize(&[
            &tool_chunk(json!({
                "index": 0, "id": "call_1", "function": {"name": "Bash", "arguments": "{\"a\":1}"},
            })),
            &tool_chunk(json!({
                "index": 1, "id": "call_2", "function": {"name": "Read", "arguments": "{}"},
            })),
            &tool_chunk(json!({"index": 0, "function": {"arguments": " oops"}})),
        ]);

        assert_eq!(events.last().unwrap().0, "error");
        assert!(
            events.last().unwrap().1["error"]["message"]
                .as_str()
                .unwrap()
                .contains("already closed")
        );
    }

    #[test]
    fn a_tool_call_that_never_names_its_function_ends_in_an_error() {
        let events = synthesize(&[&tool_chunk(
            json!({"index": 0, "id": "call_1", "function": {"arguments": "{\"a\":1}"}}),
        )]);

        assert_eq!(names(&events), vec!["message_start", "error"]);
        assert!(
            events[1].1["error"]["message"]
                .as_str()
                .unwrap()
                .contains("never carried a function name")
        );
    }

    #[test]
    fn a_tool_call_without_an_id_ends_in_an_error() {
        let events = synthesize(&[&tool_chunk(
            json!({"index": 0, "function": {"name": "Bash", "arguments": "{}"}}),
        )]);
        assert_eq!(events.last().unwrap().0, "error");
        assert!(
            events.last().unwrap().1["error"]["message"]
                .as_str()
                .unwrap()
                .contains("no id")
        );
    }

    #[test]
    fn frame_boundaries_do_not_have_to_line_up_with_chunk_boundaries() {
        let stream: String = [
            text_chunk("Hello"),
            tool_chunk(json!({
                "index": 0, "id": "call_1",
                "function": {"name": "Bash", "arguments": "{\"command\":\"ls\"}"},
            })),
            finish_chunk("tool_calls"),
            "data: [DONE]\n\n".to_string(),
        ]
        .concat();

        // One byte at a time is the worst case a chunked upstream can produce,
        // and must synthesize exactly what whole-frame delivery does.
        let mut translator = SseTranslator::new();
        let mut out = Vec::new();
        for byte in stream.as_bytes() {
            out.extend(translator.push(&[*byte]));
        }
        out.extend(translator.finish());

        let whole = synthesize(&[&stream]);
        assert_eq!(events(&out), whole);
        assert_eq!(
            names(&whole),
            vec![
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ]
        );
    }

    #[test]
    fn crlf_frames_and_comment_heartbeats_are_handled() {
        let mut translator = SseTranslator::new();
        let mut out = Vec::new();
        out.extend(translator.push(b": keep-alive\r\n\r\n"));
        out.extend(
            translator.push(
                format!(
                    "data: {}\r\n\r\n",
                    json!({
                        "id": "chatcmpl-1", "model": "m",
                        "choices": [{"index": 0, "delta": {"content": "hi"}}],
                    })
                )
                .as_bytes(),
            ),
        );
        out.extend(translator.push(b"data: [DONE]\r\n\r\n"));

        let events = events(&out);
        assert_eq!(
            names(&events),
            vec![
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ]
        );
        assert_eq!(events[2].1["delta"]["text"], "hi");
    }

    #[test]
    fn a_null_tool_calls_field_is_tolerated() {
        let events = synthesize(&[&frame(json!({
            "id": "chatcmpl-1", "model": "m",
            "choices": [{"index": 0, "delta": {"content": "hi", "tool_calls": Value::Null},
                         "finish_reason": Value::Null}],
        }))]);
        assert_eq!(events[2].1["delta"]["text"], "hi");
    }

    #[test]
    fn a_null_error_field_is_an_ordinary_chunk_not_a_failure() {
        // Several OpenAI-compatible providers put `"error": null` on every
        // chunk they send; reading that as a failure would kill every stream.
        let events = synthesize(&[
            &frame(json!({
                "id": "chatcmpl-1", "model": "m", "error": Value::Null,
                "choices": [{"index": 0, "delta": {"content": "hi"}, "finish_reason": Value::Null}],
            })),
            "data: [DONE]\n\n",
        ]);
        assert_eq!(
            names(&events),
            vec![
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ]
        );
    }

    #[test]
    fn a_frame_delivered_one_byte_at_a_time_is_not_rescanned_from_the_start() {
        // Guards the incremental scan offset: without it, a large frame
        // arriving in small pieces costs time quadratic in its length. 256 KiB
        // of text delivered byte-wise is unbearable if every push rescans.
        let payload = "x".repeat(256 * 1024);
        let chunk = text_chunk(&payload);
        let mut translator = SseTranslator::new();
        let mut out = Vec::new();
        for byte in chunk.as_bytes() {
            out.extend(translator.push(&[*byte]));
        }
        out.extend(translator.finish());

        let events = events(&out);
        assert_eq!(events[2].1["delta"]["text"], payload);
    }

    #[test]
    fn an_upstream_error_frame_terminates_with_an_error_event() {
        let events = synthesize(&[
            &text_chunk("partial"),
            &frame(
                json!({"error": {"message": "context length exceeded", "type": "invalid_request"}}),
            ),
            &text_chunk("never reaches the client"),
        ]);

        assert_eq!(
            names(&events),
            vec![
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "error",
            ],
            "the open block closes, and nothing after the error is synthesized"
        );
        assert_eq!(
            events[4].1["error"]["message"], "context length exceeded",
            "the provider's own message is what makes the failure legible"
        );
    }

    #[test]
    fn a_non_json_frame_terminates_with_an_error_event() {
        let events = synthesize(&["data: <html>502 Bad Gateway</html>\n\n"]);
        assert_eq!(names(&events), vec!["error"]);
    }

    #[test]
    fn an_oversized_frame_terminates_rather_than_growing_without_bound() {
        let mut translator = SseTranslator::new();
        let mut out = Vec::new();
        let filler = vec![b'x'; 1024 * 1024];
        for _ in 0..5 {
            out.extend(translator.push(&filler));
        }
        let events = events(&out);
        assert_eq!(names(&events), vec!["error"]);
        assert!(
            events[0].1["error"]["message"]
                .as_str()
                .unwrap()
                .contains("buffer cap")
        );
    }

    #[test]
    fn a_stream_that_ends_without_done_is_still_closed_out() {
        let events = synthesize(&[&text_chunk("hi")]);
        assert_eq!(
            names(&events),
            vec![
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ]
        );
        assert_eq!(
            events[4].1["delta"]["stop_reason"], "end_turn",
            "no finish_reason and no tool call means the turn simply ended"
        );
    }

    #[test]
    fn a_tool_turn_without_a_finish_reason_still_reports_tool_use() {
        let events = synthesize(&[&tool_chunk(json!({
            "index": 0, "id": "call_1", "function": {"name": "Bash", "arguments": "{}"},
        }))]);
        assert_eq!(events.last().unwrap().0, "message_stop");
        assert_eq!(
            events[events.len() - 2].1["delta"]["stop_reason"],
            "tool_use"
        );
    }

    #[test]
    fn an_empty_stream_still_produces_a_well_formed_message() {
        let events = synthesize(&[]);
        assert_eq!(
            names(&events),
            vec!["message_start", "message_delta", "message_stop"]
        );
    }

    #[test]
    fn done_ends_the_message_and_later_frames_are_ignored() {
        let events = synthesize(&["data: [DONE]\n\n", &text_chunk("too late")]);
        assert_eq!(
            names(&events),
            vec!["message_start", "message_delta", "message_stop"]
        );
    }

    #[test]
    fn finish_reasons_map_the_same_way_they_do_off_the_stream() {
        for (upstream, expected) in [
            ("stop", "end_turn"),
            ("length", "max_tokens"),
            ("tool_calls", "tool_use"),
            ("content_filter", "refusal"),
        ] {
            let events = synthesize(&[&text_chunk("x"), &finish_chunk(upstream)]);
            let delta = events
                .iter()
                .find(|(name, _)| name == "message_delta")
                .expect("a message_delta is always emitted");
            assert_eq!(delta.1["delta"]["stop_reason"], expected);
        }
    }

    #[test]
    fn an_aborted_stream_emits_an_error_event_and_closes_the_open_block() {
        let mut translator = SseTranslator::new();
        let mut out = Vec::new();
        out.extend(translator.push(text_chunk("partial").as_bytes()));
        out.extend(translator.abort(UPSTREAM_FAILED));
        out.extend(translator.finish());

        let events = events(&out);
        assert_eq!(
            names(&events),
            vec![
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "error",
            ],
            "finish() after an abort must not append a success ending"
        );
        assert_eq!(events[4].1["error"]["message"], UPSTREAM_FAILED);
    }
}
