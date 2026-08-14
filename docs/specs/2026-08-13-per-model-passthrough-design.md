# Per-model parameter passthrough — design brief

**Date:** 2026-08-13
**Status:** draft
**Related:** follow-ups.md item 3 (DeepSeek-V4-Flash rescue)

## Problem

The relay has no way to pass per-model or per-profile request parameters to the
upstream provider (Together). The `ChatRequest` sent to Together is built
entirely from the Anthropic request + a fixed profile mapping — no extension
points exist. DeepSeek-V4-Flash on Together runs at default `reasoning_effort:
"high"` and consumes its whole `max_tokens` budget on reasoning tokens, returning
empty `content` on long/tool-heavy requests (observed ~50% in single-shot
probes; real multi-turn rate unmeasured). Together's API supports `reasoning_effort`
(`high`|`max`) and `thinking: { enabled: false }` — the relay just cannot send
them.

## Solution

Add a **per-profile optional `passthrough` table** in `relay.toml` whose keys
are request parameters to inject into the `ChatRequest` for that profile. Keep
it opaque — the relay does not validate or interpret them; it merges them into
the OpenAI-format request body after the translator builds the base.

```toml
[profiles.together]
# …existing keys…
passthrough = { reasoning_effort = "max", thinking = { type = "enabled" } }
# or per-model (later): passthrough.deepseek-ai = { reasoning_effort = "low" }
```

## Where it hooks

1. **`src/translate/openai.rs`** — extend `ChatRequest` with `#[serde(flatten)]`
   `pub passthrough: Option<serde_json::Value>` (or a typed map). The existing
   `skip_serializing_if = "Option::is_none"` on other fields means absent
   passthrough = no change.
2. **`src/translate/request.rs::to_openai_chat`** — after building the base
   `ChatRequest`, merge `profile.passthrough` into it (JSON merge, profile
   wins on conflict).
3. **`src/state.rs`** — load `passthrough` from TOML into the profile struct
   (optional `Table`/`Value`), pass it through `AppState` → translator call.
4. **Config schema** — `passthrough` is optional, default `None`. No migration
   needed; absent = current behavior.

## Testing

- Unit: `to_openai_chat` produces `thinking`/`reasoning_effort` in output when
  profile has them; absent profile → identical to current output.
- Integration: deploy a test profile with `passthrough = { thinking = { type =
  "enabled" }, reasoning_effort = "low" }`, hit `/v1/messages` with
  `--model deepseek-ai/DeepSeek-V4-Flash-0731`, capture the outbound request
  (log or mock), assert the passthrough fields are present.
- Live probe: after deploy, run a long tool-heavy task on DeepSeek and measure
  empty-content rate vs baseline.

## Scope boundary

- **In scope:** passthrough table on profiles, merged into ChatRequest.
- **Out of scope:** per-model override (later), Anthropic-format `reasoning`
  passthrough (different wire shape), automatic reasoning_budget sizing.

## Verification milestone

Deploy with `passthrough = { reasoning_effort = "low" }` on the `together`
profile; run the same 3-paragraph btrfs probe + a 500-word tool-heavy task on
DeepSeek; empty-content rate should drop vs baseline. If it doesn't, the
failure mode is elsewhere (Together-side, not parameter omission).