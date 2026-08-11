//! Anthropic Messages-format wire types, as far as translation needs them.
//!
//! Deliberately permissive: unknown *fields* are ignored (Anthropic adds them
//! over time, and `cache_control`/`citations` already ride along on blocks the
//! translator does map), while unknown *block types* are surfaced as
//! [`Block::Unknown`] so the translator can fail loudly rather than silently
//! dropping content it cannot represent.

use serde::{Deserialize, Deserializer};
use serde_json::Value;

/// Accepts an explicit `null` where a container is expected. Several
/// OpenAI-format providers — and Anthropic clients round-tripping their own
/// history — emit `"tool_calls": null` or `"messages": null` rather than
/// omitting the key.
pub(crate) fn null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Debug, Deserialize)]
pub struct MessagesRequest {
    #[serde(default)]
    pub max_tokens: Option<u64>,
    #[serde(default)]
    pub system: Option<SystemPrompt>,
    #[serde(default, deserialize_with = "null_as_default")]
    pub messages: Vec<Message>,
    #[serde(default, deserialize_with = "null_as_default")]
    pub tools: Vec<Tool>,
    #[serde(default)]
    pub tool_choice: Option<ToolChoice>,
    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub top_p: Option<f64>,
    #[serde(default, deserialize_with = "null_as_default")]
    pub stop_sequences: Vec<String>,
    #[serde(default)]
    pub stream: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum SystemPrompt {
    Text(String),
    Blocks(Vec<Block>),
}

#[derive(Debug, Deserialize)]
pub struct Message {
    pub role: String,
    pub content: MessageContent,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum MessageContent {
    Text(String),
    Blocks(Vec<Block>),
}

/// `Unknown` is the untagged fallback: it catches both a block type this
/// translator has no mapping for and a known type whose payload doesn't
/// deserialize. Both are translation errors, distinguished by
/// `Block::type_name`.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Block {
    Known(KnownBlock),
    Unknown(Value),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KnownBlock {
    Text {
        text: String,
    },
    Image {
        source: ImageSource,
    },
    ToolUse {
        id: String,
        name: String,
        #[serde(default)]
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        #[serde(default)]
        content: Option<ToolResultContent>,
    },
    Thinking {},
    RedactedThinking {},
}

const KNOWN_BLOCK_TYPES: [&str; 6] = [
    "text",
    "image",
    "tool_use",
    "tool_result",
    "thinking",
    "redacted_thinking",
];

impl Block {
    /// Whether the mapping table has no row for this block type at all, as
    /// opposed to a type it does map that failed to parse or turned up where
    /// it cannot appear. Only the former degrades to a placeholder; the rest
    /// are malformed requests and fail.
    pub fn is_unmappable_type(&self) -> bool {
        matches!(self, Block::Unknown(_)) && !KNOWN_BLOCK_TYPES.contains(&self.type_name())
    }

    /// Why this block could not be translated, for the error message. A block
    /// that fell through to `Unknown` under a type this translator does map is
    /// malformed rather than misplaced, and saying so is the difference
    /// between looking at the client's payload and looking at the position it
    /// arrived in.
    pub fn problem(&self) -> String {
        match self {
            Block::Unknown(_) if KNOWN_BLOCK_TYPES.contains(&self.type_name()) => {
                format!("malformed {:?}", self.type_name())
            }
            _ => format!("misplaced {:?}", self.type_name()),
        }
    }

    /// The `type` field as the client wrote it. Never the block's payload —
    /// that is request content, not something to put in an error.
    pub fn type_name(&self) -> &str {
        match self {
            Block::Known(KnownBlock::Text { .. }) => "text",
            Block::Known(KnownBlock::Image { .. }) => "image",
            Block::Known(KnownBlock::ToolUse { .. }) => "tool_use",
            Block::Known(KnownBlock::ToolResult { .. }) => "tool_result",
            Block::Known(KnownBlock::Thinking {}) => "thinking",
            Block::Known(KnownBlock::RedactedThinking {}) => "redacted_thinking",
            Block::Unknown(value) => value
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("<no type field>"),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ImageSource {
    Base64 { media_type: String, data: String },
    Url { url: String },
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ToolResultContent {
    Text(String),
    Blocks(Vec<Block>),
}

#[derive(Debug, Deserialize)]
pub struct Tool {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// Absent on Anthropic's server-side tools (`web_search`, `computer_*`),
    /// which have no OpenAI equivalent at all — the translator rejects those
    /// rather than forwarding a tool the provider cannot honour.
    #[serde(default)]
    pub input_schema: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToolChoice {
    Auto {},
    Any {},
    None {},
    Tool {
        name: String,
    },
    #[serde(other)]
    Unknown,
}
