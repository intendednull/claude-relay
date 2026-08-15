# Per-model request-parameter overrides — design

**Date:** 2026-08-15
**Status:** draft
**Supersedes:** `2026-08-13-per-model-passthrough-design.md` (per-profile scope
was insufficient — see "Why the prior brief doesn't work" below)
**Related:** `.superpowers/sdd/milestone-3-plan/follow-ups.md` item 3
(DeepSeek-V4-Flash rescue)

## Problem

The relay has no way to inject extra request parameters into the outbound
`ChatRequest` sent to an OpenAI-format upstream (Together). `deepseek-ai/DeepSeek-V4-Flash-0731`
runs at Together's default `reasoning_effort: "high"`, and on this serving,
reasoning ("thinking") tokens draw from the **same** `max_tokens` budget as
visible content — there is no separate reasoning allotment. Reproduced live
against the deployed relay:

```
curl -s -X POST http://127.0.0.1:8600/v1/messages \
  -H "content-type: application/json" -H "x-api-key: test" \
  -H "anthropic-version: 2023-06-01" \
  -d '{"model":"deepseek-ai/DeepSeek-V4-Pro","max_tokens":100,
       "messages":[{"role":"user","content":"say hi in one word"}]}'
```
returns `stop_reason: "max_tokens"`, a `thinking` block cut off mid-sentence,
and **zero** content — at any `max_tokens` low enough to matter for a normal
Claude Code turn. `docs/decisions.md`'s own prior figure (~50% empty on
single-shot probes) is flagged there as unreliable; this reproduction is the
oracle instead (see "Verification milestone").

Together's API accepts `reasoning_effort` (`"none"|"low"|"high"|"max"`) and
`thinking: { enabled: false }` on the chat-completions request. The relay has
no mechanism to send either.

## Why the prior brief doesn't work

The 2026-08-13 brief designed a **per-profile** `passthrough` table — one set
of injected params shared by every model routed through `[profiles.together]`.
That profile is *not* DeepSeek-specific: it also carries `moonshotai/Kimi-K3`,
`moonshotai/Kimi-K2.7-Code`, `openai/gpt-oss-20b`, and every other model in
`serves`. A profile-scoped table cannot express "tune DeepSeek-V4-Flash
without touching Kimi-K3" — the explicit requirement for this round. It also
named the wrong file (`src/state.rs`; config parsing actually lives in
`src/config.rs`) and did not account for the escalation ladder (below).

## Solution

Add a **per-model `params` table**, keyed by the exact resolved upstream model
id, nested under the profile:

```toml
[profiles.together]
# …existing keys (base_url, api_key_env, format, serves, model_map)…

[profiles.together.params."deepseek-ai/DeepSeek-V4-Flash-0731"]
reasoning_effort = "low"
```

Equivalently, inline-table form (matches this file's existing style for
`model_map`):

```toml
[profiles.together]
params = { "deepseek-ai/DeepSeek-V4-Flash-0731" = { reasoning_effort = "low" } }
```

Keys are matched **exactly** against the fully-resolved target model id — not
prefix-matched like `model_map`/`serves`. Rationale: the concrete need is one
specific model id; exact match is unsurprising and needs no `"*"`-catch-all
semantics decision. Prefix matching is an explicit non-goal for this round
(see Scope boundary) and can be added later without breaking this shape if a
real multi-model-family need shows up.

Naming: **not** `passthrough`. That word already means "forward the request
body verbatim" in this codebase (`format = "anthropic"` profiles,
`passthrough_body`, `passthrough_response` in `src/fallback.rs`). This feature
does the near-opposite — inject extra fields into a translated request — so it
is named `params`.

### Where it hooks

1. **`src/config.rs`** (not `state.rs` — corrected from the prior brief) —
   add to `ProfileConfig` (src/config.rs:37-61):
   ```rust
   #[serde(default)]
   pub params: IndexMap<String, IndexMap<String, serde_json::Value>>,
   ```
   `IndexMap` for both levels, consistent with `model_map`'s existing
   ordering convention (even though outer-key lookup here is exact-match, not
   prefix — ordering still matters for deterministic startup validation error
   messages). Add `#[serde(default)]` to avoid re-deriving the five struct-literal
   test helpers found in research (`src/config.rs:986`, `src/state.rs:385`,
   `src/router.rs:76`, `src/fallback.rs:270`, `src/fallback.rs:283`) — confirm
   during planning whether `#[serde(default)]` alone satisfies the Rust struct
   literal requirement, or whether those five call sites need a one-line
   `params: IndexMap::new()` addition regardless.

2. **`ProfileConfig::validate()`** (src/config.rs:82-103) — reject at startup,
   matching the `deny_unknown_fields` / `validate_escalation_order`
   fail-fast convention already in this file:
   - an inner param-set that is empty (mirrors the existing empty-`serves`/
     empty-`model_map` rejection)
   - a param key that collides with a `ChatRequest` field name (`model`,
     `messages`, `max_tokens`, `temperature`, `top_p`, `stop`, `tools`,
     `tool_choice`, `stream`) — colliding keys would double-emit that JSON key
     on flatten with undefined receiver behavior; this must be a startup
     error, not a request-time surprise.

3. **`src/translate/openai.rs`** — extend `ChatRequest`:
   ```rust
   #[serde(flatten, skip_serializing_if = "IndexMap::is_empty")]
   pub params: IndexMap<String, serde_json::Value>,
   ```
   Explicitly **not** `Option<serde_json::Value>` — research found flattening
   a non-object `Value` is a *runtime* serialization error in `serde_json`,
   which would surface as a 500 on a live request rather than being caught at
   startup. A typed map makes that state unrepresentable.

4. **`src/fallback.rs::prepare()`** (src/fallback.rs:417) — resolve params by
   exact lookup on `target_model` against `profile.params`, and pass the
   resolved (possibly empty) map into `request_to_openai` / `convert`
   (`src/translate/request.rs:27`/`:38`) to populate `ChatRequest.params`.

   **This must happen inside `prepare()`, keyed by the model passed to that
   specific call** — not once per client request. Research found `prepare()`
   is called **twice** per request on the escalation path: once at
   `src/fallback.rs:134` (initial target) and again at `src/fallback.rs:275`
   inside the escalation loop, with a *different* `target_model` (`next`).
   Resolving params once (e.g. in `deliver()` before the loop) and reusing
   them across both calls would attach the wrong model's params to an
   escalated hop. Keying the lookup off `prepare()`'s own `target_model`
   argument keeps the two call sites symmetric and correct by construction.

5. **`format = "anthropic"` profiles** — out of scope. Those profiles go
   through `passthrough_body` (src/fallback.rs:577), a raw-`Value` path, not
   the typed translator. No profile in the live config currently uses
   `format = "anthropic"` for a model that needs tuning; adding params there
   would mean a second injection point for no current need. Named explicitly
   as a **non-goal**, not an oversight (see Scope boundary).

6. **`src/control.rs::ProfileView`** (src/control.rs:242-256) — add `params`
   to the view alongside `serves`/`model_map`. It is operator-authored,
   non-secret config (unlike `base_url`, which that struct deliberately
   omits), so the existing transparency rationale for exposing `model_map`
   applies equally here. Low-risk, small addition — include in this round
   rather than deferring, since the plan stage would otherwise need to
   special-case it back in.

### Example config for the concrete goal

```toml
[profiles.together]
base_url = "https://api.together.xyz"
api_key_env = "TOGETHER_API_KEY"
format = "openai"
model_map = { ... }         # unchanged
serves = [ ... ]            # unchanged

[profiles.together.params."deepseek-ai/DeepSeek-V4-Flash-0731"]
reasoning_effort = "low"
```

Every other model in `serves`/`model_map` (Kimi-K3, Kimi-K2.7-Code,
gpt-oss-20b, etc.) has no entry in `params` and therefore gets an
identical outbound request to today — `IndexMap::is_empty` on their lookup
means `ChatRequest.params` stays empty and `skip_serializing_if` drops the
field entirely, so their request bodies are byte-for-byte unchanged. This is
the isolation property the user asked for, and it is the thing the test suite
must prove, not just assert.

## Testing

Following this repo's existing conventions (unit tests co-located per file;
integration tests in `tests/*.rs` using the `Recorder` pattern in
`tests/fallback.rs` that captures outbound request bodies at a mock upstream):

1. **Unit — config parse/validate** (`src/config.rs`, alongside
   `profiles_parse_from_toml_in_declaration_order` and
   `an_unknown_profile_field_is_a_parse_error`):
   - `params` parses from both inline-table and nested-table TOML forms.
   - empty inner param-set at startup → parse/validate error.
   - a param key colliding with a `ChatRequest` field name → validate error.
   - `relay.example.toml` continues to parse and validate
     (`relay_example_toml_parses_and_validates`, src/config.rs:1560) —
     update the example file in the same commit if it gains a `params`
     example.

2. **Unit — translation** (`src/translate/request.rs`): a resolved params map
   is flattened into the serialized `ChatRequest` JSON; an empty map produces
   byte-identical output to today (update
   `a_minimal_request_maps_its_scalars_and_substitutes_the_target_model` and
   `a_multi_turn_multi_tool_conversation_translates_end_to_end`, both of which
   assert whole-object JSON equality and will need the new field accounted
   for).

3. **Integration — isolation proof** (`tests/fallback.rs`, `Recorder`
   pattern): one profile, two models (e.g. DeepSeek + Kimi-style names),
   params configured for model A only. Assert the recorded outbound body for
   model A carries the configured param(s) and the recorded body for model B
   is unchanged from a params-absent baseline. This is the load-bearing test
   for "without affecting other models."

4. **Integration — escalation-hop correctness** (`tests/fallback.rs`): a
   request that escalates from model A to model B (via `escalation_order`)
   must show A's params on the first recorded upstream call and B's params
   (or none) on the second. This is the test the prior per-profile brief's
   design would have failed, and the one the corrected `prepare()`-keyed
   resolution must pass.

## Scope boundary

- **In scope:** per-model `params` table on `openai`-format profiles, exact
  match on resolved target model id, startup validation (empty sets, field
  collisions), `/control/profiles` exposure.
- **Out of scope (explicit non-goals, not deferred TODOs):**
  - Prefix/`"*"`-catch-all matching on `params` keys (only exact match).
  - `format = "anthropic"` profile support (no current model needs it).
  - Per-profile default params layered under per-model overrides (adds a
    merge-precedence question with no current use case; the concrete goal —
    DeepSeek-V4-Flash-0731 — is fully served by per-model alone).
  - Automatic reasoning-budget sizing (e.g. scaling `max_tokens` based on
    `reasoning_effort`) — a different, larger feature.

## Verification milestone

Deploy with:
```toml
[profiles.together.params."deepseek-ai/DeepSeek-V4-Flash-0731"]
reasoning_effort = "low"
```
Re-run the `max_tokens: 100` reproduction from "Problem" above against
`deepseek-ai/DeepSeek-V4-Flash-0731` — it must now return actual `content`
rather than `stop_reason: "max_tokens"` with an empty/truncated `thinking`-only
response. In the same deploy, issue an equivalent request to a model with no
`params` entry (e.g. `moonshotai/Kimi-K3`) and confirm its outbound request
body is unaffected (via the isolation integration test, and/or a manual
capture). Both conditions must hold — fixing DeepSeek while silently
mutating other models' requests is not success.
