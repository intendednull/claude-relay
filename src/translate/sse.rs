//! OpenAI `chat.completion.chunk` SSE → Anthropic SSE events.
//!
//! [`SseTranslator`] is sans-IO on purpose: bytes in, bytes out, no futures, so
//! the synthesis rules are testable one frame at a time. [`sse_stream`] is the
//! thin adapter that drives it from an upstream byte stream, and is the only
//! part that knows what a `Stream` is.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::Bytes;
use futures_core::Stream;
use serde_json::{Value, json};

use super::BUFFER_CAP;
use super::openai::{ChatCompletionChunk, TextContent, ToolCallDelta, Usage};
use super::response::stop_reason;

/// What the client is told when the upstream connection itself fails. The
/// underlying error is deliberately *not* interpolated: it can carry the
/// upstream URL, and a profile's `base_url` is one of the few places a
/// credential could hide (Global Constraint 2).
const UPSTREAM_FAILED: &str = "upstream stream ended unexpectedly";

/// A turn may legitimately call several tools at once, but "several" is single
/// digits. Every slot is retained for the life of the stream, so an upstream
/// inventing them without bound — broken or hostile — must hit a ceiling.
const MAX_TOOL_SLOTS: usize = 256;

#[derive(Debug, Default)]
pub struct SseTranslator {
    /// `policy.surface_fallback_reasoning`. False drops every
    /// `reasoning_content` fragment, which is what every version before this one
    /// did unconditionally.
    surface_reasoning: bool,
    buf: Vec<u8>,
    started: bool,
    done: bool,
    next_index: u32,
    open: Option<Open>,
    tools: BTreeMap<u32, ToolCallState>,
    /// Every byte the tool slots are holding onto — ids, names and buffered
    /// arguments, summed across slots. `BUFFER_CAP` bounds a single frame;
    /// this bounds what survives between frames, which is the part an upstream
    /// can grow without ever sending a large frame.
    tool_bytes: usize,
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
    Thinking(u32),
    Tool { block: u32, slot: u32 },
}

#[derive(Debug, Default)]
struct ToolCallState {
    id: String,
    name: Option<String>,
    /// Argument fragments with nowhere to go yet — either the function name
    /// has not arrived (Anthropic cannot open a `tool_use` block without it),
    /// or another call's block is still open and Anthropic allows only one at
    /// a time. Replayed in order the moment this call gets its block.
    pending: String,
    block: Option<u32>,
}

impl ToolCallState {
    fn is_named(&self) -> bool {
        self.name.is_some()
    }
}

impl SseTranslator {
    pub fn new(surface_reasoning: bool) -> Self {
        Self {
            surface_reasoning,
            ..Self::default()
        }
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

    /// Whether a terminal event — `message_stop` or `error` — has been emitted
    /// and nothing further will be synthesized. The driver ends the response
    /// body on this rather than waiting for the upstream connection to close,
    /// which an upstream has no obligation to do promptly.
    pub fn is_done(&self) -> bool {
        self.done
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
        let Ok(frame) = std::str::from_utf8(frame) else {
            // Dropping it would be indistinguishable from a heartbeat, and a
            // frame this module cannot read is a frame whose content never
            // reaches the client — the one thing it must not do quietly.
            self.fail("upstream sent a non-UTF-8 SSE frame", out);
            return;
        };
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
        // Before the `content` branch, so a chunk carrying both — which is not
        // the observed shape, but is a shape a provider is free to send — still
        // gets its reasoning out ahead of the answer.
        let reasoning = choice
            .delta
            .reasoning_content
            .filter(|_| self.surface_reasoning)
            .map(TextContent::into_text)
            .filter(|reasoning| !reasoning.is_empty());
        if let Some(reasoning) = reasoning {
            let Some(index) = self.open_thinking(out) else {
                return;
            };
            emit(
                out,
                "content_block_delta",
                json!({
                    "type": "content_block_delta",
                    "index": index,
                    "delta": {"type": "thinking_delta", "thinking": reasoning},
                }),
            );
        }
        let text = choice
            .delta
            .content
            .map(TextContent::into_text)
            .filter(|text| !text.is_empty());
        if let Some(text) = text {
            let Some(index) = self.open_text(out) else {
                return;
            };
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
        let Some(slot) = self.slot_for(&delta) else {
            self.fail("upstream tool call index is out of range", out);
            return;
        };
        self.last_slot = Some(slot);
        let (name, fragment) = match delta.function {
            Some(function) => (function.name, function.arguments.unwrap_or_default()),
            None => (None, String::new()),
        };

        let mut retained = 0;
        {
            let state = self.tools.entry(slot).or_default();
            if let Some(id) = delta.id.filter(|id| !id.is_empty())
                && state.id.is_empty()
            {
                retained += id.len();
                state.id = id;
            }
            // Set-once: OpenAI sends the whole function name in the first
            // fragment for a call, but several compatible providers repeat it
            // in every fragment, and appending would corrupt the name.
            if let Some(name) = name.filter(|name| !name.is_empty())
                && state.name.is_none()
            {
                retained += name.len();
                state.name = Some(name);
            }
        }
        self.tool_bytes += retained;
        if !self.retained_within_cap(out) {
            return;
        }
        if self.tools.len() > MAX_TOOL_SLOTS {
            self.fail(
                "upstream opened more parallel tool calls than the relay will track",
                out,
            );
            return;
        }

        // The call whose block is open streams straight through.
        if let Some(Open::Tool { block, slot: open }) = self.open
            && open == slot
        {
            self.emit_input_json(block, &fragment, out);
            return;
        }
        // A call whose block was opened and then closed can take no more
        // fragments: its JSON would end up split across two `tool_use` blocks,
        // both of them incomplete.
        if self.tools[&slot].block.is_some() {
            self.fail(
                "upstream resumed a tool call whose content block had already closed",
                out,
            );
            return;
        }
        // Anthropic allows one open content block at a time, so a call that
        // turns up while another is streaming waits its turn rather than
        // displacing it. This is what makes any interleaving order safe:
        // providers are free to batch several calls into one delta, or to
        // alternate between them fragment by fragment.
        let blocked = matches!(self.open, Some(Open::Tool { .. }));
        if blocked || !self.tools[&slot].is_named() {
            self.buffer(slot, &fragment, out);
            return;
        }
        self.close_open(out);
        let Some(block) = self.open_tool_block(slot, out) else {
            return;
        };
        self.emit_input_json(block, &fragment, out);
    }

    /// Holds a fragment until its call can have a block of its own.
    fn buffer(&mut self, slot: u32, fragment: &str, out: &mut Vec<u8>) {
        if fragment.is_empty() {
            return;
        }
        self.tools
            .get_mut(&slot)
            .expect("slot was just populated")
            .pending
            .push_str(fragment);
        self.tool_bytes += fragment.len();
        self.retained_within_cap(out);
    }

    /// Fails the stream when the slots retain more than the cap allows.
    /// Called from *every* path that grows `tool_bytes`, not just the buffered
    /// arguments: ids and names are retained exactly as arguments are, and a
    /// run of frames carrying a large id and no arguments would otherwise
    /// never reach a check at all.
    fn retained_within_cap(&mut self, out: &mut Vec<u8>) -> bool {
        if self.tool_bytes <= BUFFER_CAP {
            return true;
        }
        self.fail("upstream tool calls exceeded the relay's buffer cap", out);
        false
    }

    /// Opens `slot`'s `tool_use` block and replays whatever it buffered. The
    /// caller closes any block that was open first.
    fn open_tool_block(&mut self, slot: u32, out: &mut Vec<u8>) -> Option<u32> {
        let state = self.tools.get_mut(&slot).expect("slot was just populated");
        let name = state.name.clone()?;
        let id = state.id.clone();
        let pending = if id.is_empty() {
            String::new()
        } else {
            std::mem::take(&mut state.pending)
        };
        if id.is_empty() {
            self.fail(
                "upstream tool call has no id to match its result against",
                out,
            );
            return None;
        }
        self.tool_bytes = self.tool_bytes.saturating_sub(pending.len());

        let block = self.next_index;
        self.next_index = self.next_index.saturating_add(1);
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
        Some(block)
    }

    /// Gives every named call still waiting a block of its own, in slot order.
    /// A call whose name has not arrived yet is left alone — a later fragment
    /// may still complete it, and only the end of the stream makes that a
    /// failure.
    fn drain_waiting_tools(&mut self, out: &mut Vec<u8>) {
        let waiting: Vec<u32> = self
            .tools
            .iter()
            .filter(|(_, state)| state.block.is_none() && state.is_named())
            .map(|(slot, _)| *slot)
            .collect();
        for slot in waiting {
            if self.done {
                return;
            }
            self.close_open(out);
            if self.open_tool_block(slot, out).is_some() {
                self.close_open(out);
            }
        }
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
    ///
    /// `None` when no slot can be derived. `index` is upstream-controlled, so
    /// the successor of `u32::MAX` has to be *something*: saturating would
    /// hand the new call the previous call's slot and silently splice two
    /// tool calls' arguments into one block, which is the corruption this
    /// whole layer exists to prevent.
    fn slot_for(&self, delta: &ToolCallDelta) -> Option<u32> {
        if let Some(index) = delta.index {
            return Some(index);
        }
        let Some(last) = self.last_slot else {
            return Some(0);
        };
        match (self.tools.get(&last), delta.id.as_deref()) {
            (Some(state), Some(id)) if !state.id.is_empty() && state.id != id => {
                last.checked_add(1)
            }
            _ => Some(last),
        }
    }

    fn open_text(&mut self, out: &mut Vec<u8>) -> Option<u32> {
        if let Some(Open::Text(index)) = self.open {
            return Some(index);
        }
        self.open_prose(json!({"type": "text", "text": ""}), Open::Text, out)
    }

    /// Reasoning arrives before any `content` on every provider observed, so in
    /// practice this opens block 0 and `open_text` closes it. Nothing depends on
    /// that: a `content` fragment during an open thinking block closes it via
    /// `open_text`, and reasoning arriving *after* content opens a second
    /// thinking block rather than being dropped — a late fragment is still the
    /// user's content, and this whole task exists because it was being thrown
    /// away.
    ///
    /// No `signature` here or in the deltas, and no `signature_delta`: Anthropic
    /// signs its own thinking blocks and this one is not Anthropic's
    /// (`config::PolicyConfig::surface_fallback_reasoning`).
    fn open_thinking(&mut self, out: &mut Vec<u8>) -> Option<u32> {
        if let Some(Open::Thinking(index)) = self.open {
            return Some(index);
        }
        self.open_prose(
            json!({"type": "thinking", "thinking": ""}),
            Open::Thinking,
            out,
        )
    }

    /// Opens a block that streams prose — `text` or `thinking` — closing
    /// whatever was open first, since Anthropic allows one open block at a time.
    fn open_prose(
        &mut self,
        content_block: Value,
        open: fn(u32) -> Open,
        out: &mut Vec<u8>,
    ) -> Option<u32> {
        self.close_open(out);
        // Calls still waiting get their blocks before this one does, so
        // `tool_use` blocks stay in the order the provider streamed them.
        self.drain_waiting_tools(out);
        if self.done {
            return None;
        }
        let index = self.next_index;
        self.next_index = self.next_index.saturating_add(1);
        emit(
            out,
            "content_block_start",
            json!({
                "type": "content_block_start",
                "index": index,
                "content_block": content_block,
            }),
        );
        self.open = Some(open(index));
        Some(index)
    }

    fn close_open(&mut self, out: &mut Vec<u8>) {
        let Some(open) = self.open.take() else { return };
        let index = match open {
            Open::Text(index) | Open::Thinking(index) => index,
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
        self.close_open(out);
        self.drain_waiting_tools(out);
        if self.done {
            return;
        }
        self.close_open(out);
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
fn frame_data(frame: &str) -> Option<String> {
    let mut data = String::new();
    let mut seen = false;
    for line in frame.split('\n') {
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
pub fn sse_stream<S, E>(
    upstream: S,
    surface_reasoning: bool,
) -> impl Stream<Item = Result<Bytes, Infallible>>
where
    S: Stream<Item = Result<Bytes, E>>,
{
    TranslatingStream {
        inner: Box::pin(upstream),
        translator: SseTranslator::new(surface_reasoning),
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
                    // A terminal event ends the *body*, not just the synthesis:
                    // waiting for the upstream to hang up would leave a client
                    // reading to end-of-body blocked on a connection the
                    // upstream is under no obligation to close.
                    this.drained = this.translator.is_done();
                    if !out.is_empty() {
                        return Poll::Ready(Some(Ok(Bytes::from(out))));
                    }
                    if this.drained {
                        return Poll::Ready(None);
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
        synthesize_with(chunks, true)
    }

    fn synthesize_with(chunks: &[&str], surface_reasoning: bool) -> Vec<(String, Value)> {
        let mut translator = SseTranslator::new(surface_reasoning);
        let mut out = Vec::new();
        for chunk in chunks {
            out.extend(translator.push(chunk.as_bytes()));
        }
        out.extend(translator.finish());
        events(&out)
    }

    /// The `input_json_delta` fragments for content block `index`, reassembled.
    fn arguments_for(events: &[(String, Value)], index: u32) -> String {
        events
            .iter()
            .filter(|(name, data)| {
                name == "content_block_delta"
                    && data["delta"]["type"] == "input_json_delta"
                    && data["index"] == index
            })
            .map(|(_, data)| data["delta"]["partial_json"].as_str().unwrap())
            .collect()
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

    fn reasoning_chunk(reasoning: &str) -> String {
        frame(json!({
            "id": "chatcmpl-1",
            "object": "chat.completion.chunk",
            "model": "target/Model",
            "choices": [{"index": 0, "delta": {"reasoning_content": reasoning},
                         "finish_reason": Value::Null}],
        }))
    }

    /// `(event name, index, block type or delta type)` — enough to assert both
    /// the sequence and that the indices are contiguous and in the right order,
    /// which is the part most likely to be wrong.
    fn shape(events: &[(String, Value)]) -> Vec<(String, i64, String)> {
        events
            .iter()
            .filter(|(name, _)| name.starts_with("content_block"))
            .map(|(name, data)| {
                let kind = data["content_block"]["type"]
                    .as_str()
                    .or_else(|| data["delta"]["type"].as_str())
                    .unwrap_or("")
                    .to_string();
                (name.clone(), data["index"].as_i64().unwrap(), kind)
            })
            .collect()
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
    fn reasoning_fragments_become_a_thinking_block_that_closes_before_the_text_block() {
        let events = synthesize(&[
            &reasoning_chunk("This"),
            &reasoning_chunk(" is a riddle."),
            &text_chunk("9 sheep."),
            &finish_chunk("stop"),
            "data: [DONE]\n\n",
        ]);

        assert_eq!(
            shape(&events),
            vec![
                ("content_block_start".into(), 0, "thinking".into()),
                ("content_block_delta".into(), 0, "thinking_delta".into()),
                ("content_block_delta".into(), 0, "thinking_delta".into()),
                ("content_block_stop".into(), 0, String::new()),
                ("content_block_start".into(), 1, "text".into()),
                ("content_block_delta".into(), 1, "text_delta".into()),
                ("content_block_stop".into(), 1, String::new()),
            ]
        );
        assert_eq!(
            events[1].1,
            json!({"type": "content_block_start", "index": 0,
                   "content_block": {"type": "thinking", "thinking": ""}}),
            "no signature on a block this relay synthesized"
        );
        assert_eq!(
            events[2].1,
            json!({"type": "content_block_delta", "index": 0,
                   "delta": {"type": "thinking_delta", "thinking": "This"}})
        );
    }

    /// The case most likely to produce wrong block indices: `content` arriving
    /// while the thinking block is still open, then more reasoning after it.
    #[test]
    fn interleaved_reasoning_and_content_keep_contiguous_ordered_indices() {
        let events = synthesize(&[
            &reasoning_chunk("first thought"),
            &text_chunk("partial answer"),
            &reasoning_chunk("second thought"),
            &text_chunk(" rest"),
            &finish_chunk("stop"),
            "data: [DONE]\n\n",
        ]);

        assert_eq!(
            shape(&events),
            vec![
                ("content_block_start".into(), 0, "thinking".into()),
                ("content_block_delta".into(), 0, "thinking_delta".into()),
                ("content_block_stop".into(), 0, String::new()),
                ("content_block_start".into(), 1, "text".into()),
                ("content_block_delta".into(), 1, "text_delta".into()),
                ("content_block_stop".into(), 1, String::new()),
                ("content_block_start".into(), 2, "thinking".into()),
                ("content_block_delta".into(), 2, "thinking_delta".into()),
                ("content_block_stop".into(), 2, String::new()),
                ("content_block_start".into(), 3, "text".into()),
                ("content_block_delta".into(), 3, "text_delta".into()),
                ("content_block_stop".into(), 3, String::new()),
            ],
            "a late reasoning fragment gets a block of its own rather than being dropped"
        );
    }

    /// Both fields in one chunk: the reasoning still precedes the answer, and
    /// each lands in a block of the right type.
    #[test]
    fn a_chunk_carrying_both_reasoning_and_content_emits_thinking_first() {
        let events = synthesize(&[
            &frame(json!({
                "id": "chatcmpl-1",
                "model": "target/Model",
                "choices": [{"index": 0, "delta": {
                    "reasoning_content": "thinking about it", "content": "answer",
                }}],
            })),
            &finish_chunk("stop"),
            "data: [DONE]\n\n",
        ]);

        assert_eq!(
            shape(&events),
            vec![
                ("content_block_start".into(), 0, "thinking".into()),
                ("content_block_delta".into(), 0, "thinking_delta".into()),
                ("content_block_stop".into(), 0, String::new()),
                ("content_block_start".into(), 1, "text".into()),
                ("content_block_delta".into(), 1, "text_delta".into()),
                ("content_block_stop".into(), 1, String::new()),
            ]
        );
    }

    #[test]
    fn reasoning_fragments_are_dropped_when_the_policy_switch_is_off() {
        let events = synthesize_with(
            &[
                &reasoning_chunk("secret thoughts"),
                &text_chunk("answer"),
                &finish_chunk("stop"),
                "data: [DONE]\n\n",
            ],
            false,
        );

        assert_eq!(
            shape(&events),
            vec![
                ("content_block_start".into(), 0, "text".into()),
                ("content_block_delta".into(), 0, "text_delta".into()),
                ("content_block_stop".into(), 0, String::new()),
            ],
            "the text block takes index 0: no gap where the thinking block would have been"
        );
    }

    #[test]
    fn empty_reasoning_fragments_open_no_thinking_block() {
        let events = synthesize(&[
            &reasoning_chunk(""),
            &frame(json!({
                "id": "chatcmpl-1",
                "model": "target/Model",
                "choices": [{"index": 0, "delta": {"reasoning_content": Value::Null}}],
            })),
            &text_chunk("answer"),
            &finish_chunk("stop"),
            "data: [DONE]\n\n",
        ]);

        assert_eq!(
            shape(&events),
            vec![
                ("content_block_start".into(), 0, "text".into()),
                ("content_block_delta".into(), 0, "text_delta".into()),
                ("content_block_stop".into(), 0, String::new()),
            ]
        );
    }

    /// A turn that reasons and then calls a tool. The `tool_use` block must not
    /// end up ahead of the thinking that led to it, nor share its index.
    #[test]
    fn reasoning_before_a_tool_call_gets_its_own_earlier_block() {
        let events = synthesize(&[
            &reasoning_chunk("I should list the directory"),
            &tool_chunk(json!({
                "index": 0, "id": "call_1", "type": "function",
                "function": {"name": "Bash", "arguments": "{\"command\":\"ls\"}"},
            })),
            &finish_chunk("tool_calls"),
            "data: [DONE]\n\n",
        ]);

        assert_eq!(
            shape(&events),
            vec![
                ("content_block_start".into(), 0, "thinking".into()),
                ("content_block_delta".into(), 0, "thinking_delta".into()),
                ("content_block_stop".into(), 0, String::new()),
                ("content_block_start".into(), 1, "tool_use".into()),
                ("content_block_delta".into(), 1, "input_json_delta".into()),
                ("content_block_stop".into(), 1, String::new()),
            ]
        );
        assert_eq!(arguments_for(&events, 1), "{\"command\":\"ls\"}");
    }

    /// A stream that is nothing but reasoning still closes its block properly,
    /// rather than leaving a `content_block_start` the client never sees closed.
    #[test]
    fn a_reasoning_only_stream_closes_its_thinking_block() {
        let events = synthesize(&[
            &reasoning_chunk("thought"),
            &finish_chunk("stop"),
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
        assert_eq!(events[1].1["content_block"]["type"], "thinking");
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

    /// Anthropic allows one open content block at a time, so a second call
    /// waits rather than displacing the first. That is what makes a provider
    /// free to alternate between parallel calls fragment by fragment.
    #[test]
    fn interleaved_parallel_tool_calls_keep_each_calls_json_together() {
        let events = synthesize(&[
            &tool_chunk(json!({
                "index": 0, "id": "call_1", "function": {"name": "Bash", "arguments": "{\"a\":"},
            })),
            &tool_chunk(json!({
                "index": 1, "id": "call_2", "function": {"name": "Read", "arguments": "{\"b\":"},
            })),
            &tool_chunk(json!({"index": 0, "function": {"arguments": "1}"}})),
            &tool_chunk(json!({"index": 1, "function": {"arguments": "2}"}})),
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
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ]
        );
        assert_eq!(arguments_for(&events, 0), r#"{"a":1}"#);
        assert_eq!(arguments_for(&events, 1), r#"{"b":2}"#);
        assert_eq!(events[1].1["content_block"]["id"], "call_1");
        assert_eq!(events[5].1["content_block"]["id"], "call_2");
    }

    /// A whole turn's worth of parallel calls can arrive in one delta — the
    /// wire format's `tool_calls` is an array precisely so it can.
    #[test]
    fn several_tool_calls_batched_into_one_delta_all_survive() {
        let events = synthesize(&[
            &frame(json!({
                "id": "chatcmpl-1", "model": "target/Model",
                "choices": [{"index": 0, "delta": {"tool_calls": [
                    {"index": 0, "id": "call_1", "type": "function",
                     "function": {"name": "Bash", "arguments": "{\"a\":1}"}},
                    {"index": 1, "id": "call_2", "type": "function",
                     "function": {"name": "Read", "arguments": "{\"b\":2}"}},
                    {"index": 2, "id": "call_3", "type": "function",
                     "function": {"name": "Grep", "arguments": "{\"c\":3}"}},
                ]}, "finish_reason": Value::Null}],
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
            vec![
                (json!(0), json!("call_1")),
                (json!(1), json!("call_2")),
                (json!(2), json!("call_3")),
            ]
        );
        assert_eq!(arguments_for(&events, 0), r#"{"a":1}"#);
        assert_eq!(arguments_for(&events, 1), r#"{"b":2}"#);
        assert_eq!(arguments_for(&events, 2), r#"{"c":3}"#);
    }

    #[test]
    fn a_tool_call_still_waiting_gets_its_block_before_a_later_text_block() {
        let events = synthesize(&[
            &tool_chunk(json!({
                "index": 0, "id": "call_1", "function": {"name": "Bash", "arguments": "{}"},
            })),
            &tool_chunk(json!({
                "index": 1, "id": "call_2", "function": {"name": "Read", "arguments": "{}"},
            })),
            &text_chunk("and here is why"),
            &finish_chunk("stop"),
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
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ]
        );
        assert_eq!(events[7].1["content_block"]["type"], "text");
        assert_eq!(events[8].1["delta"]["text"], "and here is why");
    }

    /// The genuine corruption case the interleaving support must still catch:
    /// a call resuming after its block closed would split one JSON document
    /// across two `tool_use` blocks, leaving both unparseable.
    #[test]
    fn a_call_resumed_after_its_block_closed_fails_loudly() {
        let events = synthesize(&[
            &tool_chunk(json!({
                "index": 0, "id": "call_1", "function": {"name": "Bash", "arguments": "{\"a\":1}"},
            })),
            &text_chunk("done"),
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
    fn more_parallel_calls_than_the_relay_tracks_terminates_the_stream() {
        let chunks: Vec<String> = (0..=MAX_TOOL_SLOTS as u32)
            .map(|index| {
                tool_chunk(json!({
                    "index": index, "id": format!("call_{index}"),
                    "function": {"name": "Bash", "arguments": "{}"},
                }))
            })
            .collect();
        let refs: Vec<&str> = chunks.iter().map(String::as_str).collect();
        let events = synthesize(&refs);

        assert_eq!(events.last().unwrap().0, "error");
        assert!(
            events.last().unwrap().1["error"]["message"]
                .as_str()
                .unwrap()
                .contains("more parallel tool calls")
        );
    }

    /// The per-frame cap bounds one frame; this is the part an upstream can
    /// grow without ever sending a large frame — arguments buffered across
    /// many slots while one call's block is open.
    #[test]
    fn arguments_buffered_across_slots_are_bounded_in_aggregate() {
        let mut translator = SseTranslator::new(true);
        let mut out = Vec::new();
        out.extend(
            translator.push(
                tool_chunk(json!({
                    "index": 0, "id": "call_0", "function": {"name": "Bash", "arguments": "{"},
                }))
                .as_bytes(),
            ),
        );
        let filler = "x".repeat(64 * 1024);
        for index in 1..MAX_TOOL_SLOTS as u32 {
            out.extend(
                translator.push(
                    tool_chunk(json!({
                        "index": index, "id": format!("call_{index}"),
                        "function": {"name": "Bash", "arguments": filler},
                    }))
                    .as_bytes(),
                ),
            );
            if translator.is_done() {
                break;
            }
        }

        let events = events(&out);
        assert_eq!(events.last().unwrap().0, "error");
        assert!(
            events.last().unwrap().1["error"]["message"]
                .as_str()
                .unwrap()
                .contains("buffer cap"),
            "unexpected: {:?}",
            events.last().unwrap().1
        );
    }

    /// The cap has to hold on the paths that grow the total *without* buffering
    /// arguments: ids and names are retained the same way, and a frame carrying
    /// only a large id never reaches the buffered-arguments path at all.
    #[test]
    fn ids_and_names_alone_are_bounded_in_aggregate() {
        let mut translator = SseTranslator::new(true);
        let mut out = Vec::new();
        let long_id = "i".repeat(1024 * 1024);
        for index in 0..8u32 {
            out.extend(
                translator.push(
                    tool_chunk(json!({
                        "index": index, "id": format!("{long_id}{index}"),
                    }))
                    .as_bytes(),
                ),
            );
            if translator.is_done() {
                break;
            }
        }

        let events = events(&out);
        assert_eq!(events.last().unwrap().0, "error");
        assert!(
            events.last().unwrap().1["error"]["message"]
                .as_str()
                .unwrap()
                .contains("buffer cap"),
            "unexpected: {:?}",
            events.last().unwrap().1
        );
    }

    /// `index` is upstream-controlled: a `u32::MAX` slot followed by an
    /// index-less fragment for a *different* call used to overflow — panicking
    /// in debug, wrapping in release. Saturating instead would be worse than
    /// either, since it hands the new call the old one's slot and splices two
    /// tool calls' JSON into one block.
    #[test]
    fn a_tool_call_index_at_the_top_of_the_range_fails_rather_than_colliding() {
        let events = synthesize(&[
            &tool_chunk(json!({
                "index": u32::MAX, "id": "call_1", "function": {"name": "Bash", "arguments": "{}"},
            })),
            &tool_chunk(json!({"id": "call_2", "function": {"name": "Read", "arguments": "{}"}})),
        ]);

        assert_eq!(events.last().unwrap().0, "error");
        assert!(
            events.last().unwrap().1["error"]["message"]
                .as_str()
                .unwrap()
                .contains("out of range")
        );
    }

    #[test]
    fn a_tool_call_index_at_the_top_of_the_range_is_otherwise_ordinary() {
        let events = synthesize(&[
            &tool_chunk(json!({
                "index": u32::MAX, "id": "call_1",
                "function": {"name": "Bash", "arguments": "{\"a\":1}"},
            })),
            &finish_chunk("tool_calls"),
        ]);

        assert!(events.iter().all(|(name, _)| name != "error"));
        assert_eq!(arguments_for(&events, 0), r#"{"a":1}"#);
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
        let mut translator = SseTranslator::new(true);
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
        let mut translator = SseTranslator::new(true);
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
    fn a_null_delta_on_a_final_chunk_is_tolerated() {
        let events = synthesize(&[
            &text_chunk("hi"),
            &frame(json!({
                "id": "chatcmpl-1", "model": "m",
                "choices": [{"index": 0, "delta": Value::Null, "finish_reason": "stop"}],
            })),
        ]);
        assert_eq!(events.last().unwrap().0, "message_stop");
        assert_eq!(
            events[events.len() - 2].1["delta"]["stop_reason"],
            "end_turn"
        );
    }

    /// Some OpenAI-compatible providers stream assistant text as a parts array
    /// — the same shape this module writes in the request direction.
    #[test]
    fn delta_content_as_a_parts_array_is_read_as_text() {
        let events = synthesize(&[&frame(json!({
            "id": "chatcmpl-1", "model": "m",
            "choices": [{"index": 0, "delta": {"content": [{"type": "text", "text": "hi"}]}}],
        }))]);
        assert_eq!(events[2].1["delta"]["text"], "hi");
    }

    #[test]
    fn a_non_utf8_frame_fails_loudly_rather_than_passing_for_a_heartbeat() {
        let mut translator = SseTranslator::new(true);
        let mut out = Vec::new();
        out.extend(translator.push(b"data: \xff\xfe not text\n\n"));
        let events = events(&out);
        assert_eq!(names(&events), vec!["error"]);
        assert!(
            events[0].1["error"]["message"]
                .as_str()
                .unwrap()
                .contains("non-UTF-8")
        );
    }

    #[test]
    fn done_marks_the_translator_finished_so_the_body_can_end() {
        let mut translator = SseTranslator::new(true);
        assert!(!translator.is_done());
        translator.push(text_chunk("hi").as_bytes());
        assert!(
            !translator.is_done(),
            "an ordinary chunk leaves the message open"
        );
        translator.push(b"data: [DONE]\n\n");
        assert!(translator.is_done());
    }

    #[test]
    fn an_error_event_also_marks_the_translator_finished() {
        let mut translator = SseTranslator::new(true);
        translator.push(b"data: not json at all\n\n");
        assert!(translator.is_done());
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
        let mut translator = SseTranslator::new(true);
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
        let mut translator = SseTranslator::new(true);
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

    /// Real Together AI traffic (`tests/fixtures/together/B_stream_two_tool_calls.raw.txt`,
    /// captured 2026-08-11) does something no fixture above modelled:
    /// `finish_reason` lands on call 0's argument chunk, reverts to `null` on
    /// call 1's naming chunk, then reappears on call 1's argument chunk and
    /// the final chunk. That real capture's two non-null observations happen
    /// to be identical (`"tool_calls"` both times), so it cannot by itself
    /// distinguish take-last from first-wins — this reproduces the same
    /// null-in-the-middle shape with two *different* values, so a regression
    /// is actually observable.
    #[test]
    fn finish_reason_flickering_to_null_and_back_settles_on_the_last_real_value() {
        let events = synthesize(&[
            &tool_chunk(json!({
                "index": 0, "id": "call_1", "function": {"name": "get_weather", "arguments": ""},
            })),
            &frame(json!({
                "id": "chatcmpl-1", "model": "target/Model",
                "choices": [{"index": 0, "delta": {"tool_calls": [
                    {"index": 0, "function": {"arguments": "{\"city\":\"Paris\"}"}},
                ]}, "finish_reason": "length"}],
            })),
            &tool_chunk(json!({
                "index": 1, "id": "call_2", "function": {"name": "get_weather", "arguments": ""},
            })),
            &frame(json!({
                "id": "chatcmpl-1", "model": "target/Model",
                "choices": [{"index": 0, "delta": {"tool_calls": [
                    {"index": 1, "function": {"arguments": "{\"city\":\"Tokyo\"}"}},
                ]}, "finish_reason": "tool_calls"}],
            })),
            &finish_chunk("tool_calls"),
            "data: [DONE]\n\n",
        ]);

        let delta = events
            .iter()
            .find(|(name, _)| name == "message_delta")
            .expect("a message_delta is always emitted");
        assert_eq!(
            delta.1["delta"]["stop_reason"], "tool_use",
            "the last finish_reason observed (tool_calls) must win over the \
             earlier length, even with a null arriving in between"
        );
    }

    #[test]
    fn an_aborted_stream_emits_an_error_event_and_closes_the_open_block() {
        let mut translator = SseTranslator::new(true);
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
