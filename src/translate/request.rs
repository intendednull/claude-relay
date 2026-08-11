//! Anthropic Messages request → OpenAI chat-completions request.

use anyhow::{Context as _, Result, bail};

use super::BLOCK_JOIN;
use super::anthropic::{
    Block, ImageSource, KnownBlock, MessageContent, MessagesRequest, SystemPrompt, ToolChoice,
    ToolResultContent,
};
use super::openai::{
    self, ChatRequest, Content, FunctionCall, FunctionDef, ImageUrl, Message, NamedFunction, Part,
    ToolCall,
};

#[derive(Debug)]
pub struct TranslatedRequest {
    pub body: Vec<u8>,
    /// Read off the Anthropic request so the caller picks the streaming or
    /// non-streaming response path without parsing the body a second time.
    pub stream: bool,
}

/// `target_model` is the *already remapped* upstream model name (spec §7a's
/// `model_map`): a translator has no business deciding which model a request
/// runs on, but it must not be possible to forget to substitute it either.
pub fn request_to_openai(body: &[u8], target_model: &str) -> Result<TranslatedRequest> {
    let request: MessagesRequest =
        serde_json::from_slice(body).context("request body is not a valid Anthropic message")?;
    let stream = request.stream.unwrap_or(false);
    let translated = convert(request, target_model, stream)?;
    Ok(TranslatedRequest {
        body: serde_json::to_vec(&translated)?,
        stream,
    })
}

fn convert(request: MessagesRequest, target_model: &str, stream: bool) -> Result<ChatRequest> {
    let mut messages = Vec::new();
    if let Some(system) = &request.system
        && let Some(text) = system_text(system)?
    {
        messages.push(Message::new("system", Content::Text(text)));
    }
    for (position, message) in request.messages.iter().enumerate() {
        let converted = match message.role.as_str() {
            "user" => user_messages(&message.content),
            "assistant" => assistant_message(&message.content).map(|m| vec![m]),
            other => bail!("unsupported message role {other:?}"),
        };
        messages.extend(converted.with_context(|| format!("in messages[{position}]"))?);
    }

    Ok(ChatRequest {
        model: target_model.to_string(),
        messages,
        max_tokens: request.max_tokens,
        temperature: request.temperature,
        top_p: request.top_p,
        stop: request.stop_sequences.clone(),
        tools: tools(&request.tools)?,
        tool_choice: request.tool_choice.as_ref().and_then(tool_choice),
        stream,
    })
}

fn system_text(system: &SystemPrompt) -> Result<Option<String>> {
    let text = match system {
        SystemPrompt::Text(text) => text.clone(),
        SystemPrompt::Blocks(blocks) => {
            let mut texts = Vec::new();
            for block in blocks {
                match block {
                    Block::Known(KnownBlock::Text { text }) => texts.push(text.as_str()),
                    Block::Known(KnownBlock::Thinking {} | KnownBlock::RedactedThinking {}) => {}
                    other => bail!("{} block in the system prompt", other.problem()),
                }
            }
            texts.join(BLOCK_JOIN)
        }
    };
    Ok((!text.is_empty()).then_some(text))
}

/// One Anthropic user turn can become several OpenAI messages: each
/// `tool_result` block is its own `role: "tool"` message, and OpenAI requires
/// those to follow the assistant turn that made the calls with nothing in
/// between — so they are emitted first, ahead of whatever ordinary content
/// shared the turn with them.
fn user_messages(content: &MessageContent) -> Result<Vec<Message>> {
    let blocks = match content {
        MessageContent::Text(text) => {
            return Ok(vec![Message::new("user", Content::Text(text.clone()))]);
        }
        MessageContent::Blocks(blocks) => blocks,
    };

    let mut tool_messages = Vec::new();
    let mut recovered_images = Vec::new();
    let mut parts = Vec::new();
    for block in blocks {
        match block {
            Block::Known(KnownBlock::Text { text }) => {
                parts.push(Part::Text { text: text.clone() })
            }
            Block::Known(KnownBlock::Image { source }) => parts.push(image_part(source)),
            Block::Known(KnownBlock::ToolResult {
                tool_use_id,
                content,
            }) => {
                let (text, images) = tool_result_content(content.as_ref())?;
                tool_messages.push(Message {
                    role: "tool",
                    content: Content::Text(text),
                    tool_call_id: Some(tool_use_id.clone()),
                    tool_calls: Vec::new(),
                });
                recovered_images.extend(images);
            }
            Block::Known(KnownBlock::Thinking {} | KnownBlock::RedactedThinking {}) => {}
            other => bail!("{} block in a user message", other.problem()),
        }
    }

    let mut messages = tool_messages;
    // A `role: "tool"` message's content is a string, so an image returned by a
    // tool cannot ride along inside it; it is carried into the user turn that
    // follows instead, which is the nearest place it survives at all.
    let parts: Vec<Part> = recovered_images.into_iter().chain(parts).collect();
    if !parts.is_empty() {
        messages.push(Message::new("user", collapse(parts)));
    }
    Ok(messages)
}

fn assistant_message(content: &MessageContent) -> Result<Message> {
    let blocks = match content {
        MessageContent::Text(text) => {
            return Ok(Message::new("assistant", Content::Text(text.clone())));
        }
        MessageContent::Blocks(blocks) => blocks,
    };

    let mut texts = Vec::new();
    let mut tool_calls = Vec::new();
    for block in blocks {
        match block {
            Block::Known(KnownBlock::Text { text }) => texts.push(text.as_str()),
            Block::Known(KnownBlock::ToolUse { id, name, input }) => tool_calls.push(ToolCall {
                id: id.clone(),
                kind: "function",
                function: FunctionCall {
                    name: name.clone(),
                    arguments: if input.is_null() {
                        "{}".to_string()
                    } else {
                        serde_json::to_string(input)?
                    },
                },
            }),
            Block::Known(KnownBlock::Thinking {} | KnownBlock::RedactedThinking {}) => {}
            other => bail!("{} block in an assistant message", other.problem()),
        }
    }

    let content = match (texts.is_empty(), tool_calls.is_empty()) {
        (true, false) => Content::Null,
        _ => Content::Text(texts.join(BLOCK_JOIN)),
    };
    Ok(Message {
        role: "assistant",
        content,
        tool_call_id: None,
        tool_calls,
    })
}

fn tool_result_content(content: Option<&ToolResultContent>) -> Result<(String, Vec<Part>)> {
    match content {
        None => Ok((String::new(), Vec::new())),
        Some(ToolResultContent::Text(text)) => Ok((text.clone(), Vec::new())),
        Some(ToolResultContent::Blocks(blocks)) => {
            let mut texts = Vec::new();
            let mut images = Vec::new();
            for block in blocks {
                match block {
                    Block::Known(KnownBlock::Text { text }) => texts.push(text.as_str()),
                    Block::Known(KnownBlock::Image { source }) => images.push(image_part(source)),
                    Block::Known(KnownBlock::Thinking {} | KnownBlock::RedactedThinking {}) => {}
                    other => bail!("{} block in a tool_result", other.problem()),
                }
            }
            Ok((texts.join(BLOCK_JOIN), images))
        }
    }
}

fn image_part(source: &ImageSource) -> Part {
    let url = match source {
        ImageSource::Base64 { media_type, data } => format!("data:{media_type};base64,{data}"),
        ImageSource::Url { url } => url.clone(),
    };
    Part::ImageUrl {
        image_url: ImageUrl { url },
    }
}

/// Text-only content becomes a plain string (spec §7c's row for it); anything
/// with an image stays a parts array, which is the only shape that can carry
/// one.
fn collapse(parts: Vec<Part>) -> Content {
    let texts: Option<Vec<&str>> = parts
        .iter()
        .map(|part| match part {
            Part::Text { text } => Some(text.as_str()),
            Part::ImageUrl { .. } => None,
        })
        .collect();
    match texts {
        Some(texts) => Content::Text(texts.join(BLOCK_JOIN)),
        None => Content::Parts(parts),
    }
}

fn tools(tools: &[super::anthropic::Tool]) -> Result<Vec<openai::Tool>> {
    tools
        .iter()
        .map(|tool| {
            let Some(parameters) = tool.input_schema.clone() else {
                bail!(
                    "tool {:?} has no input_schema, so it is one of Anthropic's server-side \
                     tools and has no OpenAI equivalent",
                    tool.name
                );
            };
            Ok(openai::Tool {
                kind: "function",
                function: FunctionDef {
                    name: tool.name.clone(),
                    description: tool.description.clone(),
                    parameters,
                },
            })
        })
        .collect()
}

fn tool_choice(choice: &ToolChoice) -> Option<openai::ToolChoice> {
    match choice {
        ToolChoice::Auto {} => Some(openai::ToolChoice::Mode("auto")),
        ToolChoice::Any {} => Some(openai::ToolChoice::Mode("required")),
        ToolChoice::None {} => Some(openai::ToolChoice::Mode("none")),
        ToolChoice::Tool { name } => Some(openai::ToolChoice::Function {
            kind: "function",
            function: NamedFunction { name: name.clone() },
        }),
        ToolChoice::Unknown => {
            tracing::warn!("dropping an unrecognised tool_choice type");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn translate(body: Value) -> Value {
        let translated = request_to_openai(body.to_string().as_bytes(), "target/Model")
            .expect("translation failed");
        serde_json::from_slice(&translated.body).expect("output is not valid JSON")
    }

    fn error(body: Value) -> String {
        format!(
            "{:#}",
            request_to_openai(body.to_string().as_bytes(), "target/Model")
                .expect_err("expected translation to fail")
        )
    }

    #[test]
    fn a_minimal_request_maps_its_scalars_and_substitutes_the_target_model() {
        let out = translate(json!({
            "model": "claude-opus-4",
            "max_tokens": 1024,
            "temperature": 0.7,
            "top_p": 0.9,
            "top_k": 40,
            "stop_sequences": ["END"],
            "metadata": {"user_id": "abc"},
            "messages": [{"role": "user", "content": "hi"}],
        }));

        assert_eq!(
            out,
            json!({
                "model": "target/Model",
                "messages": [{"role": "user", "content": "hi"}],
                "max_tokens": 1024,
                "temperature": 0.7,
                "top_p": 0.9,
                "stop": ["END"],
                "stream": false,
            }),
            "top_k and metadata have no OpenAI equivalent and are dropped"
        );
    }

    #[test]
    fn a_system_string_becomes_the_first_message() {
        let out = translate(json!({
            "system": "You are Claude Code.",
            "messages": [{"role": "user", "content": "hi"}],
        }));

        assert_eq!(
            out["messages"],
            json!([
                {"role": "system", "content": "You are Claude Code."},
                {"role": "user", "content": "hi"},
            ])
        );
    }

    #[test]
    fn system_blocks_are_flattened_into_one_system_message() {
        let out = translate(json!({
            "system": [
                {"type": "text", "text": "You are Claude Code.", "cache_control": {"type": "ephemeral"}},
                {"type": "text", "text": "Be concise."},
            ],
            "messages": [{"role": "user", "content": "hi"}],
        }));

        assert_eq!(
            out["messages"][0],
            json!({"role": "system", "content": "You are Claude Code.\n\nBe concise."}),
            "cache_control rides along on the block and must not break the mapping"
        );
    }

    #[test]
    fn an_empty_system_prompt_produces_no_system_message() {
        let out = translate(json!({
            "system": [],
            "messages": [{"role": "user", "content": "hi"}],
        }));
        assert_eq!(out["messages"][0]["role"], "user");
    }

    #[test]
    fn a_single_text_block_collapses_to_string_content() {
        let out = translate(json!({
            "messages": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}],
        }));
        assert_eq!(out["messages"][0]["content"], json!("hi"));
    }

    #[test]
    fn several_text_blocks_collapse_into_one_string() {
        let out = translate(json!({
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "first"},
                {"type": "text", "text": "second"},
            ]}],
        }));
        assert_eq!(out["messages"][0]["content"], json!("first\n\nsecond"));
    }

    #[test]
    fn a_base64_image_becomes_an_image_url_data_uri_part() {
        let out = translate(json!({
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "what is this?"},
                {"type": "image", "source": {
                    "type": "base64", "media_type": "image/png", "data": "iVBORw0KGgo=",
                }},
            ]}],
        }));

        assert_eq!(
            out["messages"][0],
            json!({"role": "user", "content": [
                {"type": "text", "text": "what is this?"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,iVBORw0KGgo="}},
            ]}),
            "an image forces the parts-array shape, and block order is preserved"
        );
    }

    #[test]
    fn a_url_image_source_is_passed_through_as_the_image_url() {
        let out = translate(json!({
            "messages": [{"role": "user", "content": [
                {"type": "image", "source": {"type": "url", "url": "https://example.com/a.png"}},
            ]}],
        }));
        assert_eq!(
            out["messages"][0]["content"],
            json!([{"type": "image_url", "image_url": {"url": "https://example.com/a.png"}}])
        );
    }

    #[test]
    fn an_assistant_tool_use_becomes_a_tool_call_with_string_arguments() {
        let out = translate(json!({
            "messages": [{"role": "assistant", "content": [
                {"type": "tool_use", "id": "toolu_01", "name": "Bash",
                 "input": {"command": "ls -la", "timeout": 5000}},
            ]}],
        }));

        let message = &out["messages"][0];
        assert_eq!(message["role"], "assistant");
        assert_eq!(
            message["content"],
            Value::Null,
            "an assistant turn that is nothing but tool calls carries null content"
        );
        assert_eq!(message["tool_calls"][0]["id"], "toolu_01");
        assert_eq!(message["tool_calls"][0]["type"], "function");
        assert_eq!(message["tool_calls"][0]["function"]["name"], "Bash");
        let arguments = message["tool_calls"][0]["function"]["arguments"]
            .as_str()
            .expect("arguments must be a JSON string");
        assert_eq!(
            serde_json::from_str::<Value>(arguments).unwrap(),
            json!({"command": "ls -la", "timeout": 5000})
        );
    }

    #[test]
    fn assistant_text_and_tool_use_share_one_message() {
        let out = translate(json!({
            "messages": [{"role": "assistant", "content": [
                {"type": "text", "text": "Let me look."},
                {"type": "tool_use", "id": "toolu_01", "name": "Read", "input": {"path": "/a"}},
            ]}],
        }));
        assert_eq!(out["messages"][0]["content"], json!("Let me look."));
        assert_eq!(out["messages"][0]["tool_calls"][0]["id"], "toolu_01");
    }

    #[test]
    fn a_tool_result_becomes_a_tool_role_message_keyed_by_tool_use_id() {
        let out = translate(json!({
            "messages": [{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_01", "content": "total 0"},
            ]}],
        }));

        assert_eq!(
            out["messages"],
            json!([{"role": "tool", "content": "total 0", "tool_call_id": "toolu_01"}])
        );
    }

    #[test]
    fn parallel_tool_results_precede_the_text_that_shared_their_turn() {
        // OpenAI requires every `tool` message to follow the assistant turn
        // that made the calls with no other message in between, so the text
        // block cannot stay in its original position.
        let out = translate(json!({
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "and now continue"},
                {"type": "tool_result", "tool_use_id": "toolu_01", "content": "a"},
                {"type": "tool_result", "tool_use_id": "toolu_02", "content": "b"},
            ]}],
        }));

        assert_eq!(
            out["messages"],
            json!([
                {"role": "tool", "content": "a", "tool_call_id": "toolu_01"},
                {"role": "tool", "content": "b", "tool_call_id": "toolu_02"},
                {"role": "user", "content": "and now continue"},
            ])
        );
    }

    #[test]
    fn tool_result_block_content_is_flattened_to_a_string() {
        let out = translate(json!({
            "messages": [{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_01", "content": [
                    {"type": "text", "text": "line one"},
                    {"type": "text", "text": "line two"},
                ]},
            ]}],
        }));
        assert_eq!(out["messages"][0]["content"], json!("line one\n\nline two"));
    }

    #[test]
    fn an_image_inside_a_tool_result_is_carried_into_the_following_user_message() {
        let out = translate(json!({
            "messages": [{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_01", "content": [
                    {"type": "text", "text": "screenshot taken"},
                    {"type": "image", "source": {
                        "type": "base64", "media_type": "image/png", "data": "AAAA",
                    }},
                ]},
            ]}],
        }));

        assert_eq!(
            out["messages"],
            json!([
                {"role": "tool", "content": "screenshot taken", "tool_call_id": "toolu_01"},
                {"role": "user", "content": [
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}},
                ]},
            ])
        );
    }

    #[test]
    fn an_empty_tool_result_becomes_empty_string_content() {
        let out = translate(json!({
            "messages": [{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_01"},
            ]}],
        }));
        assert_eq!(out["messages"][0]["content"], json!(""));
    }

    #[test]
    fn tools_map_input_schema_onto_function_parameters() {
        let schema = json!({
            "type": "object",
            "properties": {"command": {"type": "string"}},
            "required": ["command"],
        });
        let out = translate(json!({
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [{
                "name": "Bash",
                "description": "Run a command",
                "input_schema": schema,
                "cache_control": {"type": "ephemeral"},
            }],
        }));

        assert_eq!(
            out["tools"],
            json!([{
                "type": "function",
                "function": {
                    "name": "Bash",
                    "description": "Run a command",
                    "parameters": schema,
                },
            }])
        );
    }

    #[test]
    fn a_tool_without_an_input_schema_is_a_loud_error() {
        let message = error(json!({
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [{"type": "web_search_20250305", "name": "web_search"}],
        }));
        assert!(
            message.contains("web_search") && message.contains("input_schema"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn tool_choice_modes_map_onto_openais_vocabulary() {
        let with = |choice: Value| {
            translate(json!({
                "messages": [{"role": "user", "content": "hi"}],
                "tool_choice": choice,
            }))["tool_choice"]
                .clone()
        };

        assert_eq!(with(json!({"type": "auto"})), json!("auto"));
        assert_eq!(with(json!({"type": "any"})), json!("required"));
        assert_eq!(with(json!({"type": "none"})), json!("none"));
        assert_eq!(
            with(json!({"type": "tool", "name": "Bash"})),
            json!({"type": "function", "function": {"name": "Bash"}})
        );
        assert_eq!(
            with(json!({"type": "something_new"})),
            Value::Null,
            "an unrecognised tool_choice is dropped rather than failing the request"
        );
    }

    #[test]
    fn disable_parallel_tool_use_is_dropped_without_failing() {
        let out = translate(json!({
            "messages": [{"role": "user", "content": "hi"}],
            "tool_choice": {"type": "auto", "disable_parallel_tool_use": true},
        }));
        assert_eq!(out["tool_choice"], json!("auto"));
    }

    #[test]
    fn thinking_blocks_are_dropped_cleanly() {
        let out = translate(json!({
            "messages": [
                {"role": "assistant", "content": [
                    {"type": "thinking", "thinking": "long private reasoning", "signature": "sig"},
                    {"type": "redacted_thinking", "data": "encrypted"},
                    {"type": "text", "text": "Here is the answer."},
                ]},
                {"role": "user", "content": [
                    {"type": "thinking", "thinking": "unusual, but must not error"},
                    {"type": "text", "text": "thanks"},
                ]},
            ],
        }));

        assert_eq!(
            out["messages"],
            json!([
                {"role": "assistant", "content": "Here is the answer."},
                {"role": "user", "content": "thanks"},
            ])
        );
    }

    #[test]
    fn an_assistant_turn_of_only_thinking_becomes_empty_content_not_an_error() {
        let out = translate(json!({
            "messages": [{"role": "assistant", "content": [
                {"type": "thinking", "thinking": "…", "signature": "sig"},
            ]}],
        }));
        assert_eq!(out["messages"][0]["content"], json!(""));
    }

    #[test]
    fn an_unmappable_block_type_fails_loudly_rather_than_vanishing() {
        let message = error(json!({
            "messages": [{"role": "user", "content": [
                {"type": "document", "source": {"type": "base64", "data": "JVBER"}},
            ]}],
        }));
        assert!(
            message.contains("document") && message.contains("messages[0]"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn a_malformed_known_block_is_reported_as_that_block_type() {
        let message = error(json!({
            "messages": [{"role": "assistant", "content": [
                {"type": "tool_use", "id": "toolu_01"},
            ]}],
        }));
        assert!(
            message.contains("malformed \"tool_use\""),
            "a known block type that failed to parse is malformed, not unsupported: {message}"
        );
    }

    #[test]
    fn an_unsupported_role_fails_loudly() {
        let message = error(json!({
            "messages": [{"role": "system", "content": "no"}],
        }));
        assert!(message.contains("system"), "unexpected error: {message}");
    }

    #[test]
    fn stream_true_is_reported_and_forwarded() {
        let translated = request_to_openai(
            json!({"stream": true, "messages": [{"role": "user", "content": "hi"}]})
                .to_string()
                .as_bytes(),
            "target/Model",
        )
        .expect("translation failed");

        assert!(translated.stream);
        let body: Value = serde_json::from_slice(&translated.body).unwrap();
        assert_eq!(body["stream"], json!(true));
        assert!(
            body.get("stream_options").is_none(),
            "stream_options is deliberately not sent — see the module's report"
        );
    }

    /// Spec §10 item 4: tool-call argument JSON must survive the trip. The
    /// check is *semantic* (parsed values compare equal), not byte-identical,
    /// because `serde_json`'s `Map` is a `BTreeMap` here — object keys come
    /// back sorted, which JSON says is the same document. Nothing downstream
    /// of this point re-parses the string, so ordering is the only thing that
    /// can differ, and it is the only thing this deliberately does not assert.
    #[test]
    fn nested_tool_arguments_survive_the_round_trip_semantically() {
        let input = json!({
            "zeta": null,
            "alpha": {"nested": {"deep": [1, 2.5, true, null, "☃ \"quoted\" \\ backslash"]}},
            "unicode": "日本語 — em dash",
            "empty_object": {},
            "empty_array": [],
            "big": 9007199254740991_i64,
            "escapes": "line\nbreak\ttab\u{0000}nul",
        });

        let out = translate(json!({
            "messages": [{"role": "assistant", "content": [
                {"type": "tool_use", "id": "toolu_01", "name": "Complex", "input": input},
            ]}],
        }));

        let arguments = out["messages"][0]["tool_calls"][0]["function"]["arguments"]
            .as_str()
            .expect("arguments must be a JSON string");
        let recovered: Value = serde_json::from_str(arguments).expect("arguments must re-parse");
        assert_eq!(recovered, input);
    }

    /// The fixture the rest of the request tests are slices of: a real-shaped
    /// Claude Code exchange — blocked system prompt, tools, a first tool call
    /// and its result, a second parallel pair, an image, and thinking blocks
    /// in the history — translated in one pass.
    #[test]
    fn a_multi_turn_multi_tool_conversation_translates_end_to_end() {
        let out = translate(json!({
            "model": "claude-opus-4",
            "max_tokens": 4096,
            "stream": false,
            "system": [
                {"type": "text", "text": "You are Claude Code."},
                {"type": "text", "text": "Working directory: /repo"},
            ],
            "tools": [
                {"name": "Read", "description": "Read a file",
                 "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}}},
                {"name": "Bash", "description": "Run a command",
                 "input_schema": {"type": "object", "properties": {"command": {"type": "string"}}}},
            ],
            "messages": [
                {"role": "user", "content": "what does src/main.rs do?"},
                {"role": "assistant", "content": [
                    {"type": "thinking", "thinking": "private", "signature": "sig"},
                    {"type": "text", "text": "I'll read it."},
                    {"type": "tool_use", "id": "toolu_01", "name": "Read",
                     "input": {"path": "src/main.rs"}},
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "toolu_01", "content": "fn main() {}"},
                ]},
                {"role": "assistant", "content": [
                    {"type": "text", "text": "Now let me check the tests."},
                    {"type": "tool_use", "id": "toolu_02", "name": "Bash",
                     "input": {"command": "cargo test"}},
                    {"type": "tool_use", "id": "toolu_03", "name": "Read",
                     "input": {"path": "Cargo.toml"}},
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "toolu_02", "content": "ok. 12 passed"},
                    {"type": "tool_result", "tool_use_id": "toolu_03", "content": [
                        {"type": "text", "text": "[package]"},
                    ]},
                    {"type": "text", "text": "here's the failing screenshot"},
                    {"type": "image", "source": {
                        "type": "base64", "media_type": "image/png", "data": "AAAA",
                    }},
                ]},
            ],
        }));

        assert_eq!(
            out,
            json!({
                "model": "target/Model",
                "max_tokens": 4096,
                "stream": false,
                "tools": [
                    {"type": "function", "function": {
                        "name": "Read", "description": "Read a file",
                        "parameters": {"type": "object", "properties": {"path": {"type": "string"}}},
                    }},
                    {"type": "function", "function": {
                        "name": "Bash", "description": "Run a command",
                        "parameters": {"type": "object", "properties": {"command": {"type": "string"}}},
                    }},
                ],
                "messages": [
                    {"role": "system",
                     "content": "You are Claude Code.\n\nWorking directory: /repo"},
                    {"role": "user", "content": "what does src/main.rs do?"},
                    {"role": "assistant", "content": "I'll read it.", "tool_calls": [
                        {"id": "toolu_01", "type": "function", "function": {
                            "name": "Read", "arguments": "{\"path\":\"src/main.rs\"}",
                        }},
                    ]},
                    {"role": "tool", "content": "fn main() {}", "tool_call_id": "toolu_01"},
                    {"role": "assistant", "content": "Now let me check the tests.", "tool_calls": [
                        {"id": "toolu_02", "type": "function", "function": {
                            "name": "Bash", "arguments": "{\"command\":\"cargo test\"}",
                        }},
                        {"id": "toolu_03", "type": "function", "function": {
                            "name": "Read", "arguments": "{\"path\":\"Cargo.toml\"}",
                        }},
                    ]},
                    {"role": "tool", "content": "ok. 12 passed", "tool_call_id": "toolu_02"},
                    {"role": "tool", "content": "[package]", "tool_call_id": "toolu_03"},
                    {"role": "user", "content": [
                        {"type": "text", "text": "here's the failing screenshot"},
                        {"type": "image_url",
                         "image_url": {"url": "data:image/png;base64,AAAA"}},
                    ]},
                ],
            })
        );
    }
}
