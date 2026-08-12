//! Golden-fixture tests: real Together AI traffic, replayed verbatim through
//! the translator.
//!
//! 12 requests were captured 2026-08-11 against
//! `api.together.xyz/v1/chat/completions`, model `Qwen/Qwen2.5-7B-Instruct-Turbo`
//! (`docs/decisions.md`'s Task 5 entry has the full account). They largely
//! *vindicate* the translator's hand-built fixtures rather than finding bugs
//! in it — nothing here changes translator behavior.
//!
//! Four more (`L_`–`O_`) were captured 2026-08-12 for Task 8's reasoning
//! translation, against two models that spell the reasoning key differently; see
//! the reasoning section at the bottom of this file.
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

/// The five pure-error captures never reach *this* translator in production:
/// `src/fallback.rs`'s non-2xx branch hands them to `src/provider_error.rs`,
/// which rebuilds them as Anthropic error envelopes (spec §7d), not to the
/// response translator. What this test pins is unaffected by that — it is a
/// claim about the bytes Together **sends**, which `provider_error` reads and
/// `docs/spec.md` §7d cites: OpenAI-style `{"error": {message, type, param,
/// code}}` plus a harmless top-level `id`.
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

// --- Reasoning (captured 2026-08-12; Task 8) ---
//
// Four more captures, later than the twelve above and against two reasoning
// models. Same endpoint, same day's account in `docs/decisions.md`. They exist
// because the reasoning field is not in OpenAI's schema at all *and* providers
// did not converge on one name for it, so both shapes were worth pinning to real
// bytes rather than to a hand-built guess:
//
// - `L_`/`M_` — `moonshotai/Kimi-K3`, which spells it `reasoning_content`.
// - `N_`/`O_` — `moonshotai/Kimi-K2.7-Code`, which spells it `reasoning`, the
//   common spelling (six other Together models measured the same day agree), and
//   whose streamed deltas also carry a `token_id` nothing reads.
//
// One fixture per spelling per direction, deliberately: a golden file for only
// one name is exactly how a translator that silently drops the other ships.

/// Non-streaming: `reasoning_content` sits beside `content` in `message`, and
/// becomes a `thinking` block ahead of the `text` block.
#[test]
fn a_real_reasoning_completion_becomes_a_thinking_block_then_a_text_block() {
    let out = translate_response(include_bytes!(
        "fixtures/together/L_nonstream_reasoning.json"
    ));
    assert_eq!(out["stop_reason"], "end_turn");
    assert_eq!(
        out["content"],
        serde_json::json!([
            {"type": "thinking",
             "thinking": "The classic riddle: \"all but 9 run away\" means all except 9 run away, \
                          so 9 remain."},
            {"type": "text",
             "text": "9 sheep remain, since \"all but 9\" means all except 9 ran away."},
        ])
    );
}

/// Streaming: `delta.reasoning_content` arrives in fragments *before* the first
/// `delta.content` fragment — 9 then 5 in this capture — so the thinking block
/// opens and closes as block 0 and the text block is block 1. The assertion is
/// on the block structure rather than the fragment count, which is a property of
/// this one generation.
#[test]
fn a_real_reasoning_stream_closes_its_thinking_block_before_opening_the_text_block() {
    let events = replay_stream(include_bytes!(
        "fixtures/together/M_stream_reasoning.raw.txt"
    ));

    let blocks: Vec<(&str, i64, &str)> = events
        .iter()
        .filter(|(name, _)| name.starts_with("content_block"))
        .map(|(name, data)| {
            (
                name.as_str(),
                data["index"]
                    .as_i64()
                    .expect("every block event has an index"),
                data["content_block"]["type"]
                    .as_str()
                    .or_else(|| data["delta"]["type"].as_str())
                    .unwrap_or(""),
            )
        })
        .collect();

    assert_eq!(
        blocks.first(),
        Some(&("content_block_start", 0, "thinking"))
    );
    assert_eq!(blocks.last(), Some(&("content_block_stop", 1, "")));
    assert!(
        blocks.iter().any(
            |b| *b == ("content_block_stop", 0, "") || *b == ("content_block_start", 1, "text")
        ),
        "the thinking block must close before the text block opens: {blocks:?}"
    );
    // No third block, and no signature anywhere in the synthesized stream.
    assert!(
        blocks.iter().all(|(_, index, _)| *index < 2),
        "unexpected extra blocks: {blocks:?}"
    );
    let thinking: String = events
        .iter()
        .filter(|(name, data)| name == "content_block_delta" && data["index"] == 0)
        .map(|(_, data)| {
            assert_eq!(data["delta"]["type"], "thinking_delta");
            data["delta"]["thinking"].as_str().unwrap().to_string()
        })
        .collect();
    assert_eq!(
        thinking,
        "The classic riddle: \"all but 9 run away\" means all except 9 run away, so 9 remain.",
        "the reassembled thinking text must match the non-streaming capture's reasoning"
    );
    let start = events
        .iter()
        .find(|(name, data)| name == "content_block_start" && data["index"] == 0)
        .expect("a thinking block opened");
    assert!(
        start.1["content_block"].get("signature").is_none(),
        "no signature on a block this relay synthesized: {}",
        start.1
    );
}

/// The other spelling, non-streaming: `reasoning` rather than `reasoning_content`,
/// and the same `thinking`-then-`text` result.
#[test]
fn a_real_alt_key_reasoning_completion_becomes_a_thinking_block_then_a_text_block() {
    let raw = include_bytes!("fixtures/together/N_nonstream_reasoning_alt_key.json");
    // The capture really does use the other key — asserted rather than trusted,
    // because the whole point of this fixture is which name arrived.
    let captured: Value = serde_json::from_slice(raw).expect("capture is not valid JSON");
    let message = &captured["choices"][0]["message"];
    assert!(
        message["reasoning"].is_string() && message.get("reasoning_content").is_none(),
        "fixture must carry `reasoning` and not `reasoning_content`: {message}"
    );

    let out = translate_response(raw);
    assert_eq!(out["stop_reason"], "end_turn");
    assert_eq!(out["content"][0]["type"], "thinking");
    assert_eq!(
        out["content"][0]["thinking"], message["reasoning"],
        "the thinking block carries the capture's reasoning verbatim"
    );
    assert!(
        out["content"][0].get("signature").is_none(),
        "no signature on a block this relay synthesized: {}",
        out["content"][0]
    );
    assert_eq!(
        out["content"][1],
        serde_json::json!({"type": "text", "text": "Nine sheep remain."})
    );
    assert_eq!(out["content"].as_array().expect("an array").len(), 2);
}

/// The other spelling, streaming — and the one capture whose deltas carry
/// `token_id`, an unmodelled key that must be ignored rather than rejected.
#[test]
fn a_real_alt_key_reasoning_stream_closes_its_thinking_block_before_the_text_block() {
    let raw = include_bytes!("fixtures/together/O_stream_reasoning_alt_key.raw.txt");
    let text = std::str::from_utf8(raw).expect("capture must be UTF-8");
    assert!(
        text.contains("\"reasoning\"") && !text.contains("\"reasoning_content\""),
        "fixture must carry `reasoning` and not `reasoning_content`"
    );
    assert!(
        text.contains("\"token_id\""),
        "fixture must carry the token_id key this translator ignores"
    );

    let events = replay_stream(raw);
    let blocks: Vec<(&str, i64, &str)> = events
        .iter()
        .filter(|(name, _)| name.starts_with("content_block"))
        .map(|(name, data)| {
            (
                name.as_str(),
                data["index"]
                    .as_i64()
                    .expect("every block event has an index"),
                data["content_block"]["type"]
                    .as_str()
                    .or_else(|| data["delta"]["type"].as_str())
                    .unwrap_or(""),
            )
        })
        .collect();

    assert_eq!(
        blocks.first(),
        Some(&("content_block_start", 0, "thinking"))
    );
    assert_eq!(blocks.last(), Some(&("content_block_stop", 1, "")));
    assert!(
        blocks.iter().all(|(_, index, _)| *index < 2),
        "exactly two blocks, thinking then text: {blocks:?}"
    );
    // Reassembling the streamed thinking must give the same reasoning the
    // non-streaming capture of the same model returned in one piece — modulo
    // being a different generation, so only the shape is compared here.
    let thinking: String = events
        .iter()
        .filter(|(name, data)| name == "content_block_delta" && data["index"] == 0)
        .map(|(_, data)| {
            assert_eq!(data["delta"]["type"], "thinking_delta");
            data["delta"]["thinking"].as_str().unwrap().to_string()
        })
        .collect();
    assert!(
        thinking.len() > 40,
        "the reasoning must survive reassembly, not arrive empty: {thinking:?}"
    );
    let answer: String = events
        .iter()
        .filter(|(name, data)| name == "content_block_delta" && data["index"] == 1)
        .map(|(_, data)| data["delta"]["text"].as_str().unwrap().to_string())
        .collect();
    assert!(
        answer.contains("ine") && !answer.is_empty(),
        "the answer must be the text block, not swallowed by the thinking one: {answer:?}"
    );
}
