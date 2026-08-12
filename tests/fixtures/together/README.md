# Real Together AI traffic, captured

12 responses captured 2026-08-11 against `api.together.xyz/v1/chat/completions`,
model `Qwen/Qwen2.5-7B-Instruct-Turbo`. Replayed verbatim by
`tests/translate_together_fixtures.rs`; the full account of the capture session
is in `docs/decisions.md` (2026-08-11, Task 5).

`L_`–`O_` were captured separately, 2026-08-12, for Task 8's reasoning
translation. Same endpoint. They exist because the reasoning field is not in
OpenAI's schema at all *and* providers did not converge on one name for it, so
both shapes were worth pinning to real bytes rather than to a hand-built guess
(`docs/decisions.md`, 2026-08-12, Task 8):

| fixture | model | reasoning key | notes |
| --- | --- | --- | --- |
| `L_nonstream_reasoning.json` | `moonshotai/Kimi-K3` | `reasoning_content` | the `claude-opus` mapping in real use |
| `M_stream_reasoning.raw.txt` | `moonshotai/Kimi-K3` | `reasoning_content` | 9 reasoning fragments, then 5 answer fragments |
| `N_nonstream_reasoning_alt_key.json` | `moonshotai/Kimi-K2.7-Code` | `reasoning` | the common spelling |
| `O_stream_reasoning_alt_key.raw.txt` | `moonshotai/Kimi-K2.7-Code` | `reasoning` | 37 then 7 fragments; deltas also carry `token_id` |

One fixture per spelling per direction, deliberately: a golden file for only one
name is exactly how a translator that silently drops the other ships. Both
streams are `\n\n`-framed with no CRLF and `data: [DONE]`-terminated, like every
other stream capture here, and in both the reasoning arrives strictly before the
answer.

**What these do not cover.** Golden files are evidence about the traffic they
contain and nothing else, so the one gap worth stating here rather than leaving
implied by their presence:

- **Multi-fragment tool-call arguments.** In every capture, a tool call's
  complete `arguments` JSON arrived in a *single* fragment, immediately after the
  chunk that named the call. The translator's incremental reassembly path
  (`src/translate/sse.rs`) is therefore backed only by that module's own
  hand-built fixtures. It is probably correct — both wire formats simply
  concatenate fragments — but no real provider response in this directory
  exercises it.

`tests/translate_together_fixtures.rs`'s module doc carries the same note plus
the one other difference from the hand-built fixtures (every streamed `delta`
repeats `"role":"assistant"`, which the translator has no field to read and so
ignores).
