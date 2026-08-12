//! What a client-controlled name may look like by the time it reaches a log
//! line (spec §9's per-request record is the relay's only after-the-fact
//! evidence of which route and which credential served a request, so a request
//! must not be able to write into it).
//!
//! Its own module because two unrelated layers need it: `translate::request`
//! for block types and tool names, and `proxy` for the `model` a request asks
//! for. It lived in the former until the latter turned out to need it too.

/// Clipped and filtered to something an identifier could plausibly be, because
/// these values are client-controlled and unbounded and reach a log line — and,
/// for a block type, text the model reads.
///
/// Escaping alone is not enough on its own terms: `tracing`'s `%` sigil renders
/// a value through `format_args!` *unescaped*, so a newline in one of these
/// forges a whole log record. Passing the result of this function as a plain
/// field (no sigil) gets `record_str`'s `{:?}` escaping as well, which is the
/// belt to this brace — but the clip is what bounds log volume, which escaping
/// does not.
pub(crate) fn safe_identifier(name: &str) -> String {
    const MAX_TYPE_NAME: usize = 64;
    let clipped: String = name
        .chars()
        // `/` and `:` beyond the plain identifier set: real model names carry
        // both (`deepseek-ai/DeepSeek-V4`), and a log line naming the model
        // that had no route is useless to an operator if it mangles the name
        // they wrote. Neither character can break a line or escape a quoted
        // field, which is what this filter is for.
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/' | ':'))
        .take(MAX_TYPE_NAME)
        .collect();
    if clipped.is_empty() {
        "unnamed".to_string()
    } else {
        clipped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The forging shape the branch review demonstrated live: a newline plus a
    /// synthetic record. Every character a log line's structure is made of has
    /// to go.
    #[test]
    fn a_newline_and_a_forged_record_cannot_survive() {
        let forged = safe_identifier(
            "unclaimed/model\n  2026-08-11T00:00:00Z  INFO relay::proxy: \
             proxied request route=\"anthropic\" model_in=\"FORGED-BY-CLIENT\" status=200",
        );
        assert!(!forged.contains('\n'));
        assert!(!forged.contains('"'));
        assert!(!forged.contains('='));
        assert!(!forged.contains(' '));
    }

    /// The reason for the two characters beyond the identifier set: a real
    /// model name has to come out of this recognizable.
    #[test]
    fn a_real_model_name_survives_intact() {
        assert_eq!(
            safe_identifier("deepseek-ai/DeepSeek-V4"),
            "deepseek-ai/DeepSeek-V4"
        );
        assert_eq!(safe_identifier("claude-opus-4-6"), "claude-opus-4-6");
        assert_eq!(
            safe_identifier("llama3:70b-instruct"),
            "llama3:70b-instruct"
        );
    }

    #[test]
    fn unbounded_input_is_clipped() {
        assert_eq!(safe_identifier(&"x".repeat(500)).len(), 64);
    }

    /// A name with nothing legible left is still a field a log line can carry.
    #[test]
    fn a_name_filtered_down_to_nothing_becomes_a_placeholder() {
        assert_eq!(safe_identifier("\n\t \"'"), "unnamed");
        assert_eq!(safe_identifier(""), "unnamed");
    }
}
