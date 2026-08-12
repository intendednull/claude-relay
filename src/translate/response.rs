//! OpenAI `chat.completion` → Anthropic Messages response (non-streaming).

use anyhow::{Result, bail};
use serde_json::{Value, json};

use super::openai::{ChatCompletion, ResponseToolCall, TextContent, reasoning_text};
use super::parse_failure;

/// `surface_reasoning` is `policy.surface_fallback_reasoning`: when false the
/// provider's reasoning is dropped, which is what every version before this one
/// did unconditionally.
pub fn response_to_anthropic(body: &[u8], surface_reasoning: bool) -> Result<Vec<u8>> {
    let completion: ChatCompletion = serde_json::from_slice(body)
        .map_err(|err| parse_failure("upstream response is not a chat completion", &err))?;
    let Some(choice) = completion.choices.into_iter().next() else {
        bail!("upstream response carried no choices");
    };

    let mut content = Vec::new();
    // Ahead of the text block, because that is the order the model produced
    // them. No `signature`: Anthropic signs its own thinking blocks and this one
    // is not Anthropic's, so there is no value to put there that would not be a
    // forgery (`config::PolicyConfig::surface_fallback_reasoning`).
    let reasoning = surface_reasoning
        .then(|| reasoning_text(choice.message.reasoning, choice.message.reasoning_content))
        .flatten();
    if let Some(reasoning) = reasoning {
        content.push(json!({"type": "thinking", "thinking": reasoning}));
    }
    let text = choice
        .message
        .content
        .map(TextContent::into_text)
        .filter(|text| !text.is_empty());
    if let Some(text) = text {
        content.push(json!({"type": "text", "text": text}));
    }
    for call in &choice.message.tool_calls {
        content.push(tool_use_block(call)?);
    }

    let usage = completion.usage.unwrap_or_default();
    let response = json!({
        "id": completion.id.unwrap_or_else(|| "msg_translated".to_string()),
        "type": "message",
        "role": "assistant",
        "model": completion.model.unwrap_or_default(),
        "content": content,
        "stop_reason": choice.finish_reason.as_deref().map(stop_reason),
        "stop_sequence": Value::Null,
        "usage": {
            "input_tokens": usage.prompt_tokens.unwrap_or(0),
            "output_tokens": usage.completion_tokens.unwrap_or(0),
        },
    });
    Ok(serde_json::to_vec(&response)?)
}

fn tool_use_block(call: &ResponseToolCall) -> Result<Value> {
    let name = call.function.name.clone().unwrap_or_default();
    let Some(id) = call.id.clone().filter(|id| !id.is_empty()) else {
        bail!("upstream tool call {name:?} has no id to match its result against");
    };
    Ok(json!({
        "type": "tool_use",
        "id": id,
        "name": name,
        "input": tool_arguments(call.function.arguments.as_deref().unwrap_or(""), &id)?,
    }))
}

/// The one place the translator has to *parse* a tool call's arguments rather
/// than pass them along: Anthropic's `input` is a JSON value where OpenAI's
/// `arguments` is a JSON string. A parse failure is reported, never papered
/// over with `{}` — an empty argument set is a different call than the one the
/// model made. The failure text names the tool call, never its arguments,
/// which are request content.
pub(super) fn tool_arguments(arguments: &str, id: &str) -> Result<Value> {
    if arguments.trim().is_empty() {
        return Ok(json!({}));
    }
    let value: Value = serde_json::from_str(arguments).map_err(|err| {
        parse_failure(
            &format!("tool call {id:?}: arguments are not valid JSON"),
            &err,
        )
    })?;
    if !value.is_object() {
        bail!("tool call {id:?}: arguments are valid JSON but not an object");
    }
    Ok(value)
}

/// OpenAI's `stop` covers both a natural end and a stop sequence being hit,
/// with nothing to tell them apart, so a stop-sequence stop is reported as
/// `end_turn` with a null `stop_sequence`.
pub(super) fn stop_reason(finish_reason: &str) -> &'static str {
    match finish_reason {
        "stop" => "end_turn",
        "length" => "max_tokens",
        "tool_calls" | "function_call" => "tool_use",
        "content_filter" => "refusal",
        other => {
            tracing::warn!(
                finish_reason = other,
                "unrecognised finish_reason, reporting end_turn"
            );
            "end_turn"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn translate(body: Value) -> Value {
        translate_with(body, true)
    }

    fn translate_with(body: Value, surface_reasoning: bool) -> Value {
        let translated = response_to_anthropic(body.to_string().as_bytes(), surface_reasoning)
            .expect("translation failed");
        serde_json::from_slice(&translated).expect("output is not valid JSON")
    }

    fn error(body: Value) -> String {
        format!(
            "{:#}",
            response_to_anthropic(body.to_string().as_bytes(), true)
                .expect_err("expected translation to fail")
        )
    }

    fn completion(message: Value, finish_reason: &str) -> Value {
        json!({
            "id": "chatcmpl-abc123",
            "object": "chat.completion",
            "created": 1_700_000_000_u64,
            "model": "target/Model",
            "choices": [{"index": 0, "message": message, "finish_reason": finish_reason}],
            "usage": {"prompt_tokens": 42, "completion_tokens": 7, "total_tokens": 49},
        })
    }

    #[test]
    fn a_text_completion_becomes_an_anthropic_message() {
        let out = translate(completion(
            json!({"role": "assistant", "content": "Hello there."}),
            "stop",
        ));

        assert_eq!(
            out,
            json!({
                "id": "chatcmpl-abc123",
                "type": "message",
                "role": "assistant",
                "model": "target/Model",
                "content": [{"type": "text", "text": "Hello there."}],
                "stop_reason": "end_turn",
                "stop_sequence": Value::Null,
                "usage": {"input_tokens": 42, "output_tokens": 7},
            })
        );
    }

    #[test]
    fn tool_calls_become_tool_use_blocks_with_parsed_input() {
        let out = translate(completion(
            json!({
                "role": "assistant",
                "content": "Checking.",
                "tool_calls": [
                    {"id": "call_1", "type": "function", "function": {
                        "name": "Bash", "arguments": "{\"command\":\"ls -la\",\"timeout\":5000}",
                    }},
                    {"id": "call_2", "type": "function", "function": {
                        "name": "Read", "arguments": "{\"path\":\"/a\"}",
                    }},
                ],
            }),
            "tool_calls",
        ));

        assert_eq!(
            out["content"],
            json!([
                {"type": "text", "text": "Checking."},
                {"type": "tool_use", "id": "call_1", "name": "Bash",
                 "input": {"command": "ls -la", "timeout": 5000}},
                {"type": "tool_use", "id": "call_2", "name": "Read", "input": {"path": "/a"}},
            ])
        );
        assert_eq!(out["stop_reason"], "tool_use");
    }

    /// The two keys providers use for the reasoning. Every test below runs
    /// against both: a fixture for only one is exactly how a translator that
    /// silently drops the other ships.
    const REASONING_KEYS: [&str; 2] = ["reasoning", "reasoning_content"];

    /// An assistant message carrying `reasoning` under `key`.
    fn reasoned(key: &str, reasoning: Value, content: Value) -> Value {
        json!({"role": "assistant", key: reasoning, "content": content})
    }

    #[test]
    fn either_spelling_of_reasoning_becomes_a_thinking_block_before_the_text_block() {
        for key in REASONING_KEYS {
            let out = translate(completion(
                reasoned(
                    key,
                    json!("All but 9 run away, so 9 remain."),
                    json!("9 sheep."),
                ),
                "stop",
            ));
            assert_eq!(
                out["content"],
                json!([
                    {"type": "thinking", "thinking": "All but 9 run away, so 9 remain."},
                    {"type": "text", "text": "9 sheep."},
                ]),
                "spelled {key:?}"
            );
        }
    }

    /// Both keys on one message: whichever is non-empty wins, and it is never an
    /// error. Serde's derive would reject the pair as a duplicate field if the
    /// two spellings shared one field via `#[serde(alias)]`, which would fail a
    /// whole response over a redundancy.
    #[test]
    fn both_spellings_at_once_pick_the_non_empty_one_rather_than_failing() {
        for (reasoning, reasoning_content, expected) in [
            (json!("from reasoning"), json!(""), "from reasoning"),
            (
                json!(""),
                json!("from reasoning_content"),
                "from reasoning_content",
            ),
            (json!("both set"), json!("also set"), "both set"),
        ] {
            let out = translate(completion(
                json!({"role": "assistant", "reasoning": reasoning,
                       "reasoning_content": reasoning_content, "content": "x"}),
                "stop",
            ));
            assert_eq!(
                out["content"],
                json!([
                    {"type": "thinking", "thinking": expected},
                    {"type": "text", "text": "x"},
                ])
            );
        }
    }

    /// No `signature` key at all, rather than one this relay made up. Pinned as a
    /// requirement because a later "make it look more like Anthropic's shape"
    /// change would be a forged attestation, not a fix.
    #[test]
    fn a_synthesized_thinking_block_carries_no_signature() {
        for key in REASONING_KEYS {
            let out = translate(completion(reasoned(key, json!("hm"), json!("x")), "stop"));
            assert!(
                out["content"][0].get("signature").is_none(),
                "spelled {key:?}: {}",
                out["content"][0]
            );
        }
    }

    #[test]
    fn reasoning_is_dropped_when_the_policy_switch_is_off() {
        for key in REASONING_KEYS {
            let out = translate_with(
                completion(reasoned(key, json!("hm"), json!("x")), "stop"),
                false,
            );
            assert_eq!(
                out["content"],
                json!([{"type": "text", "text": "x"}]),
                "spelled {key:?}"
            );
        }
    }

    #[test]
    fn empty_or_absent_reasoning_produces_no_thinking_block() {
        let mut messages = vec![json!({"role": "assistant", "content": "x"})];
        for key in REASONING_KEYS {
            messages.push(reasoned(key, json!(""), json!("x")));
            messages.push(reasoned(key, Value::Null, json!("x")));
        }
        for message in messages {
            let out = translate(completion(message.clone(), "stop"));
            assert_eq!(
                out["content"],
                json!([{"type": "text", "text": "x"}]),
                "not an empty thinking block, no thinking block: {message}"
            );
        }
    }

    /// Reasoning with no answer after it is still the turn's whole content —
    /// dropping it because `content` was empty would be the original bug again.
    #[test]
    fn reasoning_alone_is_still_surfaced() {
        for key in REASONING_KEYS {
            let out = translate(completion(
                reasoned(key, json!("thought"), Value::Null),
                "stop",
            ));
            assert_eq!(
                out["content"],
                json!([{"type": "thinking", "thinking": "thought"}]),
                "spelled {key:?}"
            );
        }
    }

    #[test]
    fn reasoning_precedes_tool_use_blocks_too() {
        let out = translate(completion(
            json!({"role": "assistant", "reasoning": "need the disk", "content": Value::Null,
                   "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "Bash", "arguments": "{}"}},
            ]}),
            "tool_calls",
        ));
        assert_eq!(out["content"][0]["type"], "thinking");
        assert_eq!(out["content"][1]["type"], "tool_use");
    }

    #[test]
    fn every_finish_reason_maps_onto_a_stop_reason() {
        let with = |reason: &str| {
            translate(completion(
                json!({"role": "assistant", "content": "x"}),
                reason,
            ))["stop_reason"]
                .clone()
        };

        assert_eq!(with("stop"), json!("end_turn"));
        assert_eq!(with("length"), json!("max_tokens"));
        assert_eq!(with("tool_calls"), json!("tool_use"));
        assert_eq!(with("function_call"), json!("tool_use"));
        assert_eq!(with("content_filter"), json!("refusal"));
        assert_eq!(
            with("something_new"),
            json!("end_turn"),
            "an unknown finish_reason degrades to end_turn rather than failing the response"
        );
    }

    #[test]
    fn a_null_finish_reason_stays_null() {
        let out = translate(json!({
            "id": "chatcmpl-1",
            "model": "m",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "x"},
                         "finish_reason": Value::Null}],
        }));
        assert_eq!(out["stop_reason"], Value::Null);
    }

    #[test]
    fn empty_arguments_mean_an_empty_input_object() {
        let out = translate(completion(
            json!({"role": "assistant", "content": Value::Null, "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "Now", "arguments": ""}},
            ]}),
            "tool_calls",
        ));
        assert_eq!(out["content"][0]["input"], json!({}));
    }

    #[test]
    fn null_content_alongside_tool_calls_produces_no_text_block() {
        let out = translate(completion(
            json!({"role": "assistant", "content": Value::Null, "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "Now", "arguments": "{}"}},
            ]}),
            "tool_calls",
        ));
        assert_eq!(out["content"].as_array().unwrap().len(), 1);
        assert_eq!(out["content"][0]["type"], "tool_use");
    }

    #[test]
    fn malformed_tool_arguments_fail_loudly_and_do_not_leak_their_content() {
        let message = error(completion(
            json!({"role": "assistant", "content": Value::Null, "tool_calls": [
                {"id": "call_1", "type": "function", "function": {
                    "name": "Bash", "arguments": "{\"command\": \"rm -rf /secret-token-value",
                }},
            ]}),
            "tool_calls",
        ));

        assert!(message.contains("call_1"), "unexpected error: {message}");
        assert!(
            !message.contains("secret-token-value"),
            "the arguments themselves must not appear in the error: {message}"
        );
    }

    #[test]
    fn non_object_tool_arguments_fail_loudly() {
        let message = error(completion(
            json!({"role": "assistant", "content": Value::Null, "tool_calls": [
                {"id": "call_1", "type": "function", "function": {
                    "name": "Bash", "arguments": "[1,2,3]",
                }},
            ]}),
            "tool_calls",
        ));
        assert!(message.contains("not an object"), "unexpected: {message}");
    }

    #[test]
    fn a_tool_call_without_an_id_fails_loudly() {
        let message = error(completion(
            json!({"role": "assistant", "content": Value::Null, "tool_calls": [
                {"type": "function", "function": {"name": "Bash", "arguments": "{}"}},
            ]}),
            "tool_calls",
        ));
        assert!(message.contains("no id"), "unexpected error: {message}");
    }

    #[test]
    fn a_response_without_choices_is_an_error_not_a_panic() {
        let message = error(json!({"id": "chatcmpl-1", "model": "m", "choices": []}));
        assert!(message.contains("no choices"), "unexpected: {message}");
    }

    #[test]
    fn a_null_message_is_tolerated_rather_than_failing_the_response() {
        let out = translate(json!({
            "id": "chatcmpl-1", "model": "m",
            "choices": [{"index": 0, "message": Value::Null, "finish_reason": "stop"}],
        }));
        assert_eq!(out["content"], json!([]));
        assert_eq!(out["stop_reason"], "end_turn");
    }

    #[test]
    fn message_content_as_a_parts_array_is_read_as_text() {
        let out = translate(completion(
            json!({"role": "assistant", "content": [{"type": "text", "text": "Hello there."}]}),
            "stop",
        ));
        assert_eq!(
            out["content"],
            json!([{"type": "text", "text": "Hello there."}])
        );
    }

    #[test]
    fn missing_usage_reports_zeroes_rather_than_failing() {
        let out = translate(json!({
            "id": "chatcmpl-1",
            "model": "m",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi"},
                         "finish_reason": "stop"}],
        }));
        assert_eq!(out["usage"], json!({"input_tokens": 0, "output_tokens": 0}));
    }

    /// The other half of spec §10 item 4: what the request direction encodes
    /// as a string, the response direction must decode back to the same value.
    #[test]
    fn tool_arguments_survive_the_full_encode_decode_round_trip() {
        let input = json!({
            "path": "/repo/src/main.rs",
            "edits": [{"old": "a\nb", "new": "c\td"}, {"old": "\"q\"", "new": "\\"}],
            "nested": {"deep": {"deeper": [null, true, 1.5, "日本語"]}},
        });

        let request = crate::translate::request_to_openai(
            json!({"messages": [{"role": "assistant", "content": [
                {"type": "tool_use", "id": "toolu_01", "name": "Edit", "input": input},
            ]}]})
            .to_string()
            .as_bytes(),
            "target/Model",
        )
        .expect("request translation failed");
        let request: Value = serde_json::from_slice(&request.body).unwrap();
        let arguments = request["messages"][0]["tool_calls"][0]["function"]["arguments"]
            .as_str()
            .unwrap()
            .to_string();

        let out = translate(completion(
            json!({"role": "assistant", "content": Value::Null, "tool_calls": [
                {"id": "call_1", "type": "function", "function": {
                    "name": "Edit", "arguments": arguments,
                }},
            ]}),
            "tool_calls",
        ));

        assert_eq!(out["content"][0]["input"], input);
    }
}
