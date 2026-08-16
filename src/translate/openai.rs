//! OpenAI chat-completions wire types: what the translator writes on the
//! request side, and what it reads back on the response side.

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::anthropic::null_as_default;

/// Every top-level JSON key `ChatRequest` serializes. A `params` entry using
/// one of these names would double-emit that key via `#[serde(flatten)]` —
/// serde_json accepts this silently (last-write-wins is not guaranteed), so
/// `ProfileConfig::validate()` rejects a colliding key at startup instead.
/// Keep this list in sync with `ChatRequest`'s fields — the pinning test
/// below fails if it drifts.
pub const CHAT_REQUEST_FIELD_NAMES: &[&str] = &[
    "model",
    "messages",
    "max_tokens",
    "temperature",
    "top_p",
    "stop",
    "tools",
    "tool_choice",
    "stream",
];

#[derive(Debug, Serialize, PartialEq)]
pub struct ChatRequest {
    pub model: String,
    pub messages: Vec<Message>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub stop: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<Tool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoice>,
    pub stream: bool,
    /// Operator-configured extra request parameters, merged in as top-level
    /// keys. Last in the struct so the flattened output appends after the
    /// named fields.
    #[serde(flatten, skip_serializing_if = "IndexMap::is_empty")]
    pub params: IndexMap<String, Value>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Message {
    pub role: &'static str,
    /// Always present, `null` included: OpenAI's own documented shape for an
    /// assistant turn that is nothing but tool calls is `"content": null`.
    pub content: Content,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
}

impl Message {
    pub fn new(role: &'static str, content: Content) -> Self {
        Self {
            role,
            content,
            tool_call_id: None,
            tool_calls: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(untagged)]
pub enum Content {
    Text(String),
    Parts(Vec<Part>),
    Null,
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Part {
    Text { text: String },
    ImageUrl { image_url: ImageUrl },
}

#[derive(Debug, Serialize, PartialEq)]
pub struct ImageUrl {
    pub url: String,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub function: FunctionCall,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct FunctionCall {
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Tool {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub function: FunctionDef,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct FunctionDef {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub parameters: Value,
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(untagged)]
pub enum ToolChoice {
    Mode(&'static str),
    Function {
        #[serde(rename = "type")]
        kind: &'static str,
        function: NamedFunction,
    },
}

#[derive(Debug, Serialize, PartialEq)]
pub struct NamedFunction {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct ChatCompletion {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default, deserialize_with = "null_as_default")]
    pub choices: Vec<Choice>,
    #[serde(default)]
    pub usage: Option<Usage>,
}

#[derive(Debug, Deserialize)]
pub struct Choice {
    #[serde(default, deserialize_with = "null_as_default")]
    pub message: ResponseMessage,
    #[serde(default)]
    pub finish_reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ResponseMessage {
    #[serde(default)]
    pub content: Option<TextContent>,
    /// See [`reasoning_text`]. `reasoning` is the common spelling.
    #[serde(default)]
    pub reasoning: Option<TextContent>,
    #[serde(default)]
    pub reasoning_content: Option<TextContent>,
    #[serde(default, deserialize_with = "null_as_default")]
    pub tool_calls: Vec<ResponseToolCall>,
}

/// The model's reasoning, under whichever of the two keys the provider used.
///
/// Not in OpenAI's schema at all, and providers did not converge on one name:
/// measured across Together AI on 2026-08-12, `reasoning` is what
/// `moonshotai/Kimi-K2.7-Code`, `Kimi-K2.6`, `deepseek-ai/DeepSeek-V4-*`,
/// `zai-org/GLM-5.2`, `MiniMaxAI/MiniMax-M3` and `openai/gpt-oss-120b` send,
/// while `moonshotai/Kimi-K3` sends `reasoning_content`. Both appear in the same
/// two places — the non-streaming `message` and a streaming `delta` — so both
/// spellings are read in both directions.
///
/// Two fields rather than one with `#[serde(alias)]`: serde's derive rejects a
/// payload carrying *both* keys as a duplicate field, and failing a whole
/// response over a redundancy is worse than picking one. Whichever is non-empty
/// wins; an empty string and an absent key are the same answer, `None`, because
/// an empty thinking block is not a thing to emit.
///
/// Read as [`TextContent`] for the same reason `content` is: a provider that
/// sends one as a parts array sends the other that way too.
pub fn reasoning_text(
    reasoning: Option<TextContent>,
    reasoning_content: Option<TextContent>,
) -> Option<String> {
    [reasoning, reasoning_content]
        .into_iter()
        .flatten()
        .map(TextContent::into_text)
        .find(|text| !text.is_empty())
}

/// Assistant text as either the plain string OpenAI documents or the parts
/// array several compatible providers emit instead — the same shape this
/// module writes in the request direction, so it is hardly exotic coming back.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum TextContent {
    Text(String),
    Parts(Vec<TextPart>),
}

#[derive(Debug, Deserialize)]
pub struct TextPart {
    #[serde(default)]
    pub text: Option<String>,
}

impl TextContent {
    /// Parts concatenate with no separator: in a stream they are fragments of
    /// one run of text, not distinct blocks.
    pub fn into_text(self) -> String {
        match self {
            TextContent::Text(text) => text,
            TextContent::Parts(parts) => parts.into_iter().filter_map(|part| part.text).collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ResponseToolCall {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub function: ResponseFunction,
}

#[derive(Debug, Default, Deserialize)]
pub struct ResponseFunction {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub arguments: Option<String>,
}

#[derive(Debug, Default, Clone, Copy, Deserialize)]
pub struct Usage {
    #[serde(default)]
    pub prompt_tokens: Option<u64>,
    #[serde(default)]
    pub completion_tokens: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ChatCompletionChunk {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default, deserialize_with = "null_as_default")]
    pub choices: Vec<ChunkChoice>,
    #[serde(default)]
    pub usage: Option<Usage>,
}

#[derive(Debug, Deserialize)]
pub struct ChunkChoice {
    #[serde(default, deserialize_with = "null_as_default")]
    pub delta: Delta,
    #[serde(default)]
    pub finish_reason: Option<String>,
}

/// A streamed `delta` also carries a `token_id` on the providers that spell the
/// reasoning key `reasoning`. Nothing here reads it, and nothing has to: none of
/// this module's types are `deny_unknown_fields`, so an unmodelled key is
/// ignored rather than rejected (deliberately — see `super::anthropic`'s module
/// doc for the same policy on the Anthropic side).
#[derive(Debug, Default, Deserialize)]
pub struct Delta {
    #[serde(default)]
    pub content: Option<TextContent>,
    /// See [`reasoning_text`]. Streamed in fragments exactly as `content` is,
    /// and — on every provider observed — entirely *before* the first `content`
    /// fragment. The translator does not rely on that ordering; see
    /// `SseTranslator::open_thinking`.
    #[serde(default)]
    pub reasoning: Option<TextContent>,
    #[serde(default)]
    pub reasoning_content: Option<TextContent>,
    #[serde(default, deserialize_with = "null_as_default")]
    pub tool_calls: Vec<ToolCallDelta>,
}

#[derive(Debug, Deserialize)]
pub struct ToolCallDelta {
    /// Which tool call this fragment belongs to, when a turn calls several in
    /// parallel. Absent on providers that only ever stream one.
    #[serde(default)]
    pub index: Option<u32>,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub function: Option<FunctionDelta>,
}

#[derive(Debug, Default, Deserialize)]
pub struct FunctionDelta {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub arguments: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every named field populated, so nothing is dropped by
    /// `skip_serializing_if` and the serialized key set is the full one.
    fn fully_populated(params: IndexMap<String, Value>) -> ChatRequest {
        ChatRequest {
            model: "m".to_string(),
            messages: vec![Message::new("user", Content::Text("hi".to_string()))],
            max_tokens: Some(1),
            temperature: Some(0.5),
            top_p: Some(0.9),
            stop: vec!["x".to_string()],
            tools: vec![Tool {
                kind: "function",
                function: FunctionDef {
                    name: "t".to_string(),
                    description: Some("d".to_string()),
                    parameters: serde_json::json!({"type": "object"}),
                },
            }],
            tool_choice: Some(ToolChoice::Mode("auto")),
            stream: true,
            params,
        }
    }

    fn sorted_keys(request: &ChatRequest) -> Vec<String> {
        let json = serde_json::to_value(request).expect("serialization failed");
        let mut keys: Vec<String> = json
            .as_object()
            .expect("ChatRequest does not serialize to an object")
            .keys()
            .cloned()
            .collect();
        keys.sort();
        keys
    }

    #[test]
    fn chat_request_field_names_const_matches_actual_serialized_keys() {
        let mut expected: Vec<String> = CHAT_REQUEST_FIELD_NAMES
            .iter()
            .map(|name| name.to_string())
            .collect();
        expected.sort();
        assert_eq!(sorted_keys(&fully_populated(IndexMap::new())), expected);
    }

    #[test]
    fn params_flatten_into_top_level_keys() {
        let params = IndexMap::from([
            ("reasoning_effort".to_string(), Value::from("max")),
            ("seed".to_string(), Value::from(7)),
        ]);
        let request = fully_populated(params);
        let json = serde_json::to_value(&request).expect("serialization failed");
        assert_eq!(json["reasoning_effort"], Value::from("max"));
        assert_eq!(json["seed"], Value::from(7));

        let mut expected: Vec<String> = CHAT_REQUEST_FIELD_NAMES
            .iter()
            .map(|name| name.to_string())
            .chain(["reasoning_effort".to_string(), "seed".to_string()])
            .collect();
        expected.sort();
        assert_eq!(sorted_keys(&request), expected);
    }
}
