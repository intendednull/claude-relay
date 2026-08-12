# Real Together AI traffic, captured

12 responses captured 2026-08-11 against `api.together.xyz/v1/chat/completions`,
model `Qwen/Qwen2.5-7B-Instruct-Turbo`. Replayed verbatim by
`tests/translate_together_fixtures.rs`; the full account of the capture session
is in `docs/decisions.md` (2026-08-11, Task 5).

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
