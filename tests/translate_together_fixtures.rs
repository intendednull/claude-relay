//! Golden-fixture tests: real Together AI traffic, replayed verbatim through
//! the translator.
//!
//! 12 requests were captured 2026-08-11 against
//! `api.together.xyz/v1/chat/completions`, model `Qwen/Qwen2.5-7B-Instruct-Turbo`
//! (`docs/decisions.md`'s Task 5 entry has the full account). They largely
//! *vindicate* the translator's hand-built fixtures rather than finding bugs
//! in it — nothing here changes translator behavior.
//!
//! Every fixture is `include_bytes!`'d straight from `tests/fixtures/together/`
//! rather than retyped into a Rust literal, so what each test asserts against
//! is provably the bytes Together actually sent (`docs/spec.md` §7c's point
//! of a golden file), not a hand-transcribed copy of them.
//!
//! **What real traffic did not exercise, stated here rather than left
//! implied by the fixtures' presence:**
//! - Every capture's tool-call arguments arrived as a single fragment right
//!   after the naming chunk. The multi-fragment reassembly path
//!   (`src/translate/sse.rs`) is still verified only by that module's own
//!   hand-built fixtures — probably correct, since both formats simply
//!   concatenate fragments, but unbacked by real traffic.
//! - Every streamed chunk's `delta` carries `"role":"assistant"`, not only
//!   the first — contradicting how the hand-built fixtures model it. Zero
//!   consequence: `Delta` (`src/translate/openai.rs`) has no `role` field to
//!   read it into.

use serde_json::Value;

use relay::translate::{SseTranslator, response_to_anthropic};

/// `(event name, data)` for every Anthropic SSE frame in `bytes` — the same
/// shape `tests/translate_stream.rs`'s own helper produces.
fn events(bytes: &[u8]) -> Vec<(String, Value)> {
    std::str::from_utf8(bytes)
        .expect("translated output must be UTF-8")
        .split("\n\n")
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
            (
                event.expect("every frame carries an event name"),
                serde_json::from_str(&data).expect("every frame carries JSON data"),
            )
        })
        .collect()
}

/// Feeds a captured SSE byte stream through the translator in one push. Safe
/// to do in one shot for these fixtures specifically: each is `\n\n`-framed
/// (verified — no CRLF) and ends in `data: [DONE]\n\n`, so `push` alone
/// drains every frame; `finish()` afterward is a documented no-op once
/// `[DONE]` has already closed the message.
fn replay_stream(raw: &[u8]) -> Vec<(String, Value)> {
    let mut translator = SseTranslator::new(true);
    let mut out = translator.push(raw);
    out.extend(translator.finish());
    events(&out)
}

fn translate_response(raw: &[u8]) -> Value {
    let out = response_to_anthropic(raw, true).expect("translation failed");
    serde_json::from_slice(&out).expect("translated output is not valid JSON")
}

#[test]
fn a_single_real_tool_call_stream_translates_exactly() {
    let events = replay_stream(include_bytes!(
        "fixtures/together/A2_stream_single_tool_call_auto.raw.txt"
    ));
    let names: Vec<&str> = events.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        names,
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
        events[0].1["message"]["id"],
        "ovq3gPa-6Ng1vN-a29af9de180b277d"
    );
    assert_eq!(
        events[0].1["message"]["model"],
        "Qwen/Qwen2.5-7B-Instruct-Turbo"
    );
    assert_eq!(
        events[1].1["content_block"],
        serde_json::json!({
            "type": "tool_use",
            "id": "call_lox2uvq7kepi90wvnt8k8jz2",
            "name": "get_weather",
            "input": {},
        }),
        "id and name arrive together in one naming chunk, set once"
    );
    assert_eq!(events[2].1["delta"]["partial_json"], r#"{"city":"Paris"}"#);
    assert_eq!(events[4].1["delta"]["stop_reason"], "tool_use");
    assert_eq!(
        events[4].1["usage"],
        serde_json::json!({"input_tokens": 215, "output_tokens": 20})
    );
}

/// The highest-value capture: two sequential tool calls with real chunk
/// boundaries. Confirms one `tool_calls` entry per chunk, a stable `index`,
/// set-once `id`/`name`, and calls that never interleave.
///
/// It also pins a real-traffic wrinkle no hand-built fixture modelled: call
/// 0's content block is still open when call 1's naming chunk arrives —
/// Together never signals "this call is done" mid-stream — so the
/// translator's buffer-until-the-open-block-closes rule
/// (`docs/decisions.md`) holds call 1's block open, delta, and stop until
/// `[DONE]`, where they land in the same synthesis pass as `message_delta`
/// rather than streaming out earlier. That is the already-tested buffering
/// behavior (`src/translate/sse.rs`'s own `interleaved_parallel_tool_calls_*`
/// tests), now confirmed against real bytes rather than only hand-built ones.
#[test]
fn two_sequential_real_tool_calls_translate_exactly() {
    let events = replay_stream(include_bytes!(
        "fixtures/together/B_stream_two_tool_calls.raw.txt"
    ));
    let names: Vec<&str> = events.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        names,
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
    assert_eq!(
        events[1].1["content_block"],
        serde_json::json!({
            "type": "tool_use", "id": "call_td2toa5xcm6sl1xqs0tuykkp",
            "name": "get_weather", "input": {},
        })
    );
    assert_eq!(events[2].1["delta"]["partial_json"], r#"{"city":"Paris"}"#);
    assert_eq!(
        events[4].1["content_block"],
        serde_json::json!({
            "type": "tool_use", "id": "call_76dfykrwubdiuofgrtiyy57b",
            "name": "get_weather", "input": {},
        }),
        "call 1's id and name are set once, from its own naming chunk — never \
         inherited from call 0's"
    );
    assert_eq!(events[5].1["delta"]["partial_json"], r#"{"city":"Tokyo"}"#);
    assert_eq!(events[7].1["delta"]["stop_reason"], "tool_use");
    assert_eq!(
        events[7].1["usage"],
        serde_json::json!({"input_tokens": 215, "output_tokens": 36})
    );
}

#[test]
fn a_real_plain_text_response_translates_exactly() {
    let out = translate_response(include_bytes!("fixtures/together/C_nonstream_plain.json"));
    assert_eq!(
        out,
        serde_json::json!({
            "id": "ovq3wXL-6Ng1vN-a29afb1c2942ddb8",
            "type": "message",
            "role": "assistant",
            "model": "Qwen/Qwen2.5-7B-Instruct-Turbo",
            "content": [{"type": "text", "text": "The capital of France is Paris."}],
            "stop_reason": "end_turn",
            "stop_sequence": Value::Null,
            "usage": {"input_tokens": 41, "output_tokens": 8},
        })
    );
}

#[test]
fn a_real_tool_call_response_translates_exactly() {
    let out = translate_response(include_bytes!(
        "fixtures/together/D_nonstream_tool_call.json"
    ));
    assert_eq!(
        out,
        serde_json::json!({
            "id": "ovq3wjo-4YNCb4-a29afb1e4bddebd7",
            "type": "message",
            "role": "assistant",
            "model": "Qwen/Qwen2.5-7B-Instruct-Turbo",
            "content": [{
                "type": "tool_use",
                "id": "call_1520q011log4lp8723yh73o7",
                "name": "get_weather",
                "input": {"city": "Berlin"},
            }],
            "stop_reason": "tool_use",
            "stop_sequence": Value::Null,
            "usage": {"input_tokens": 212, "output_tokens": 20},
        }),
        "content: null alongside populated tool_calls is the real shape"
    );
}

#[test]
fn a_real_length_truncated_response_translates_exactly() {
    let out = translate_response(include_bytes!(
        "fixtures/together/E_nonstream_length_finish.json"
    ));
    assert_eq!(
        out,
        serde_json::json!({
            "id": "ovq3wtC-4YNCb4-a29afb2319102b22",
            "type": "message",
            "role": "assistant",
            "model": "Qwen/Qwen2.5-7B-Instruct-Turbo",
            "content": [{"type": "text", "text": "Rome,"}],
            "stop_reason": "max_tokens",
            "stop_sequence": Value::Null,
            "usage": {"input_tokens": 44, "output_tokens": 3},
        })
    );
}

/// Captured while probing a malformed `max_tokens` value; Together tolerated
/// it and answered normally instead of erroring, so what landed is an
/// ordinary `stop`-finished completion, not the error case the filename
/// suggests. Landed and replayed as-is rather than dropped or renamed — the
/// capture is honest about what actually happened on the wire, and it is
/// still real traffic exercising the same `stop` shape as `C`.
#[test]
fn a_real_response_captured_for_a_malformed_request_is_an_ordinary_completion() {
    let out = translate_response(include_bytes!(
        "fixtures/together/G_error_malformed_max_tokens.json"
    ));
    assert_eq!(out["stop_reason"], "end_turn");
    assert_eq!(
        out["content"],
        serde_json::json!([{"type": "text", "text": "Hello! How can I assist you today?"}])
    );
}

/// The response half of a captured multi-turn exchange where the request
/// carried a `system` message, an assistant `tool_calls` turn, and a
/// `role: "tool"` result keyed by `tool_call_id` — confirming Together
/// consumed an injected tool result correctly. Only the response is a
/// translator input; the request shape it answers is evidence for
/// `docs/decisions.md`, not something this file replays.
#[test]
fn a_real_multiturn_roundtrip_response_translates_exactly() {
    let out = translate_response(include_bytes!(
        "fixtures/together/K_multiturn_roundtrip.json"
    ));
    assert_eq!(out["stop_reason"], "end_turn");
    assert_eq!(
        out["content"][0]["text"],
        "Given the sunny and warm weather in Rome, you should wear lightweight \
         clothing and a hat to stay cool and protected from the sun."
    );
}

/// The five pure-error captures never reach the translator in production —
/// `src/fallback.rs`'s non-2xx branch passes an upstream error through
/// verbatim, untranslated — so there is no translator behavior for them to
/// exercise. This pins the shape that comment now cites instead of guesses:
/// OpenAI-style `{"error": {message, type, param, code}}` plus a harmless
/// top-level `id`.
#[test]
fn every_real_error_capture_is_openai_shaped() {
    for raw in [
        &include_bytes!("fixtures/together/A_stream_single_tool_call.raw.txt")[..],
        &include_bytes!("fixtures/together/F_error_unknown_model.json")[..],
        &include_bytes!("fixtures/together/H_error_missing_messages.json")[..],
        &include_bytes!("fixtures/together/I_error_invalid_auth.json")[..],
        &include_bytes!("fixtures/together/J_error_max_tokens_exceeds_context.json")[..],
    ] {
        let value: Value = serde_json::from_slice(raw).expect("capture is not valid JSON");
        assert!(value["id"].is_string(), "missing top-level id: {value}");
        let error = &value["error"];
        assert!(
            error["message"].is_string(),
            "missing error.message: {value}"
        );
        assert!(error["type"].is_string(), "missing error.type: {value}");
        assert!(
            error.get("param").is_some(),
            "missing error.param (even if null): {value}"
        );
        assert!(
            error.get("code").is_some(),
            "missing error.code (even if null): {value}"
        );
    }
}
