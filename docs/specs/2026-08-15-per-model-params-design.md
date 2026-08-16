# Per-model request-parameter overrides — design

**Date:** 2026-08-15
**Status:** draft (revised after adversarial review — see "Revision log")
**Supersedes:** `2026-08-13-per-model-passthrough-design.md` (per-profile scope
was insufficient — see "Why the prior brief doesn't work" below)
**Related:** `.superpowers/sdd/milestone-3-plan/follow-ups.md` item 3
(DeepSeek-V4-Flash rescue)

## Problem

The relay has no way to inject extra request parameters into the outbound
`ChatRequest` sent to an OpenAI-format upstream (Together). `deepseek-ai/DeepSeek-V4-Flash-0731`
runs at Together's default reasoning effort ("high"), and on this serving,
reasoning ("thinking") tokens draw from the **same** `max_tokens` budget as
visible content — there is no separate reasoning allotment. Reproduced live,
direct against Together (bypassing the relay, which cannot send
`reasoning_effort` today), 5 trials at `max_tokens: 500` against
`deepseek-ai/DeepSeek-V4-Flash-0731` with no `reasoning_effort` set:

```
stop |len=830|reasoning_tok=237
length|len=0  |reasoning_tok=500   <- hit budget on reasoning, zero content
length|len=160|reasoning_tok=472
stop |len=729|reasoning_tok=358
length|len=0  |reasoning_tok=500   <- hit budget on reasoning, zero content
```

2 of these 5 runs returned **zero** content with `finish_reason: "length"` —
the reasoning consumed the entire budget before any answer was emitted. This
is the failure the ticket described, reproduced directly against the
provider with no relay code involved, so the fix belongs in what parameters
the relay can send, not in relay routing logic.

(Scoring note, since "N of 5" appears with opposite polarity in this section
and the table below: a run counts as a **success** only when
`finish_reason: "stop"` *and* non-empty content — i.e. it actually answered.
`length|len=160` above is scored a failure even though it emitted some
content, because it was truncated mid-answer, not because it emitted
nothing.)

## Why the prior brief doesn't work

The 2026-08-13 brief designed a **per-profile** `passthrough` table — one set
of injected params shared by every model routed through `[profiles.together]`.
That profile is *not* DeepSeek-specific: it also carries `moonshotai/Kimi-K3`,
`moonshotai/Kimi-K2.7-Code`, `openai/gpt-oss-20b`, and every other model in
`serves`. A profile-scoped table cannot express "tune DeepSeek-V4-Flash
without touching Kimi-K3" — the explicit requirement for this round. It also
named the wrong file (`src/state.rs`; config parsing actually lives in
`src/config.rs`) and did not account for the escalation ladder (below).

## Which parameter value actually fixes it

Adversarial review flagged that the original draft of this spec proposed
`reasoning_effort = "low"` without ever measuring it — a crucial, cheap-to-run
experiment that should happen before building, not after. Together's
`reasoning_effort` accepts `"none"|"low"|"high"(default)|"max"`; `docs/decisions.md`
already records that `"none"` causes the model to echo the system prompt back
instead of answering (a different failure mode, not a fix). Measured directly
against `api.together.xyz`, 5 trials each, same prompt, `max_tokens: 500`,
`deepseek-ai/DeepSeek-V4-Flash-0731`:

| `reasoning_effort` | successes / 5 | reasoning tokens per run |
|---|---|---|
| *(unset, "high")* | 2 / 5 | 237, 500, 472, 358, 500 |
| `"low"` | 1 / 5 | 500, 500, 394, 275, 500 |
| `"max"` | **5 / 5** | 23, 23, 23, 23, 23 |

`"low"` is **worse** than the default, not better — it would have shipped a
regression. `"max"` was fully reliable and, on this model/prompt pair,
produced consistently *fewer* reasoning tokens than either other setting, not
more (Together's effort labels for this model apparently do not map onto a
simple more-tokens-per-level scale — that's an observation about the
provider, not something this spec needs to explain further). **The value to
configure is `reasoning_effort = "max"`.**

This is still one prompt shape and one small batch — not a substitute for the
verification milestone's `max_tokens: 100` check, or for observing real
multi-turn Claude Code sessions after deploy — but it is enough to reject
`"low"` and select `"max"` with actual evidence instead of a guess.

The `thinking: { enabled: false }` toggle from the original ticket was not
pursued further: `reasoning_effort = "max"` alone already reached 5/5, and a
first attempt at `thinking: { enabled: false }` returned a 400
(`missing field 'type'` — the wire schema differs from what the ticket
assumed). Left as a non-goal; revisit only if `reasoning_effort` alone proves
insufficient once deployed.

## Solution

Add a **per-model `params` table**, keyed by the exact resolved upstream model
id, nested under the profile:

```toml
[profiles.together.params."deepseek-ai/DeepSeek-V4-Flash-0731"]
reasoning_effort = "max"
```

Equivalently, inline-table form (matches this file's existing style for
`model_map`):

```toml
[profiles.together]
params = { "deepseek-ai/DeepSeek-V4-Flash-0731" = { reasoning_effort = "max" } }
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
   `#[serde(default)]` covers **deserialization only** — it does not make the
   field optional in a Rust struct literal, and `ProfileConfig` derives no
   `Default`, so `..Default::default()` is not available either. Every
   existing struct-literal construction of `ProfileConfig` and `ChatRequest`
   needs an explicit `params: IndexMap::new()` (or a populated map) added in
   the same commit that adds the field. Confirmed sites (fix all of them —
   this is not an open question for the plan stage):
   ```
   src/config.rs:987        src/state.rs:386         src/router.rs:77
   src/fallback.rs:1196     src/fallback.rs:1255
   src/translate/request.rs:61   (the sole ChatRequest literal — no Default derive there either)
   tests/fallback.rs:271    tests/control.rs:55      tests/log_forging.rs:44
   tests/log_escalation.rs:104   tests/log_hygiene_control.rs:33
   tests/log_hygiene_fallback.rs:113   tests/log_hygiene_provider_error.rs:104
   ```
   (Sites using `..profile(...)` functional-update, e.g. `src/config.rs:1009`,
   `:1169`, `tests/fallback.rs:2780`, need no change.)

2. **`ProfileConfig::validate()`** (src/config.rs:82-103) — reject at startup,
   matching the `deny_unknown_fields` / `validate_escalation_order`
   fail-fast convention already in this file:
   - an inner param-set that is empty (mirrors the existing empty-`serves`/
     empty-`model_map` rejection)
   - an empty outer key (`params."" = {...}` can never match any resolved
     model id) or empty inner key (`{ "" = "x" }` would serialize a literal
     `""` JSON key into the outbound body)
   - a param key that collides with a `ChatRequest` field name. **This is not
     defense-in-depth — verified empirically that `serde_json` silently
     accepts a duplicate JSON key on flatten and returns `Ok` with both
     copies present** (last-one-wins is not guaranteed receiver behavior);
     there is no runtime backstop, so this startup check is the only thing
     preventing a malformed body from reaching the provider. To avoid this
     list drifting from `ChatRequest`'s actual fields, define it as a `const`
     in `src/translate/openai.rs` next to the struct, and add a test that
     serializes a `ChatRequest` with every *named* field populated but
     `params` left empty, and asserts its top-level JSON keys equal that
     const exactly (a non-empty `params` would add its own keys at the top
     level via flatten and break this exact-equality assertion by
     construction) — so a future field addition to `ChatRequest` fails a
     test instead of silently reopening the collision hole.
   - a `params` entry on a `format = "anthropic"` profile (see point 5 below)
   - a `params` key that cannot be reached by this profile's routing at all —
     reject a key that is neither a `model_map` value nor prefix-matched by
     any entry in `serves`. This catches a wrong provider prefix (e.g. a typo
     landing on a prefix not in `serves`); it does **not** catch a wrong
     model *name* under a correct prefix (any `deepseek-ai/…` string passes
     the `serves` check even if the exact model id is wrong) — see point 6
     on the request-time signal that covers the gap this check cannot.

3. **`src/translate/openai.rs`** — extend `ChatRequest`:
   ```rust
   #[serde(flatten, skip_serializing_if = "IndexMap::is_empty")]
   pub params: IndexMap<String, serde_json::Value>,
   ```
   requires importing `indexmap::IndexMap` into this file (currently imports
   only `serde` and `serde_json::Value`). Verified empirically (serde 1 /
   serde_json 1 / indexmap 2, the exact proposed shape): an empty map
   serializes byte-identical to today's output (no `params` key at all); a
   populated map flattens its entries as top-level JSON keys; because the
   map's values are always *leaves*, never re-flattened, the "flattening a
   non-object `Value` is a runtime error" hazard that motivated rejecting
   `Option<serde_json::Value>` in the prior draft does not apply here — it is
   removed by this shape, not merely deferred. `skip_serializing_if` is
   redundant with `IndexMap::is_empty` for its own emission but harmless to
   keep for symmetry with the struct's other fields.

4. **`src/fallback.rs::prepare()`** (src/fallback.rs:417) — resolve params by
   exact lookup on `target_model` against `profile.params`, and pass the
   resolved (possibly empty) map into `request_to_openai` / `convert`
   (`src/translate/request.rs:27`/`:38`) to populate `ChatRequest.params`.
   `prepare()`'s current signature (`body: &[u8], target_model: &str,
   translated: bool`) has no access to the profile — it needs `&ProfileConfig`
   (or the pre-resolved params map) threaded in as a new parameter at both of
   its call sites.

   **This must happen inside `prepare()`, keyed by the model passed to that
   specific call** — not once per client request. `prepare()` is called
   **twice** per request on the escalation path: once at `src/fallback.rs:134`
   (initial target) and again at `src/fallback.rs:275` inside the escalation
   loop, with a *different* `target_model` (`next`, sourced from
   `Ladder::next_target()`, which returns `model_map` values). The existing
   doc comment at `src/fallback.rs:409-416` already states the invariant this
   design relies on: `prepare()` runs once per upstream attempt because the
   target model is *in the body it builds*. Keying the params lookup off
   `prepare()`'s own `target_model` argument keeps both call sites correct by
   construction — verified by tracing both sites; no design change needed
   here, only the signature threading above.

5. **`format = "anthropic"` profiles** — reject `params` at startup
   (validate() addition, point 2 above) rather than silently ignoring it.
   Those profiles go through `passthrough_body` (src/fallback.rs:577), a raw-
   `Value` path, not the typed translator — a `params` entry there would
   parse, pass validation (absent the new check), and then never be applied
   at request time, which is exactly the silent "configured and does
   nothing" class `ProfileConfig::validate()` and
   `validate_escalation_order()` already reject five separate times elsewhere
   in this file. Support for anthropic-format profiles stays a genuine
   non-goal (no current model needs it); silence about it does not.

6. **Observability for a stale key** (new — not in the prior draft). Because
   matching is exact-match on a model id that can itself change (Together
   model ids carry dates, e.g. `-0731`), a future rename silently regresses
   the model to untuned defaults with no signal distinguishing it from a
   fresh provider-side problem — the point 2 reachability check cannot catch
   a same-prefix, wrong-name typo or an upstream rename. `prepare()` knows
   both `target_model` and whether the params lookup matched; add a
   `tracing::debug!` (or a field on the existing `RequestLog`, which already
   carries `model_out`) recording whether params were applied for this
   attempt. This is what lets an operator later tell "matched but the set is
   legitimately small" apart from "stopped matching after a rename."

7. **`src/control.rs::ProfileView`** (src/control.rs:242-256) — expose which
   models have params configured and which **keys** are set, not the
   **values**. `ProfileView`'s own doc comment already omits `base_url` on
   the grounds that an operator-authored, nominally non-secret field "can
   carry a secret of its own in its path or query" — `params` is a strictly
   more open version of the same risk: an uninterpreted bag of arbitrary
   keys and arbitrary JSON values forwarded verbatim to a provider, and nothing
   in this design constrains what an operator writes there (a provider
   callback URL with a signed query param, a gated-model token, etc., are all
   syntactically valid `params` values). The transparency this view exists
   for — "which models are tuned, on what?" — is fully served by key names
   alone:
   ```rust
   pub params: IndexMap<String, Vec<String>>,   // model id -> param key names
   ```

### Example config for the concrete goal

```toml
[profiles.together]
base_url = "https://api.together.xyz"
api_key_env = "TOGETHER_API_KEY"
format = "openai"
model_map = { ... }         # unchanged
serves = [ ... ]            # unchanged

[profiles.together.params."deepseek-ai/DeepSeek-V4-Flash-0731"]
reasoning_effort = "max"
```

Every other model in `serves`/`model_map` (Kimi-K3, Kimi-K2.7-Code,
gpt-oss-20b, etc.) has no entry in `params` and therefore gets an
identical outbound request to today — `IndexMap::is_empty` on their lookup
means `ChatRequest.params` stays empty and `skip_serializing_if` drops the
field entirely, so their request bodies are byte-for-byte unchanged
(measured, not just asserted — see point 3 above). This is the isolation
property the user asked for, and the test suite proves it, not just claims it.

## Testing

Following this repo's existing conventions (unit tests co-located per file;
integration tests in `tests/*.rs` using the `Recorder` pattern in
`tests/fallback.rs` that captures outbound request bodies at a mock upstream):

1. **Unit — config parse/validate** (`src/config.rs`): `params` parses from
   both inline-table and nested-table TOML forms; empty inner param-set,
   empty outer/inner key, field-name collision, `params` on an
   `anthropic`-format profile, and an unreachable `params` key (matches no
   `model_map` value / no `serves` prefix) each fail validation. Update
   `relay.example.toml` and its test (`relay_example_toml_parses_and_validates`,
   src/config.rs:1560) with caution: every `[profiles.*]` block in that file
   is currently commented out, so the test loops over zero profiles today —
   a commented-out `params` example would not actually be parsed or pinned
   by that test. Either add a live (uncommented) example profile with
   `params` so the test genuinely covers the new key, or do not claim
   coverage from this test.

2. **Unit — translation** (`src/translate/request.rs`): a resolved params
   map is flattened into the serialized `ChatRequest` JSON; an empty map
   produces byte-identical output to today. Update
   `a_minimal_request_maps_its_scalars_and_substitutes_the_target_model` and
   `a_multi_turn_multi_tool_conversation_translates_end_to_end` (both assert
   whole-object JSON equality) for the new field.

3. **Integration — isolation proof, name-routed path** (`tests/fallback.rs`,
   `Recorder` pattern). **This is the primary test**, not the escalation one
   below: in the live deployed config, `deepseek-ai/DeepSeek-V4-Flash-0731`
   is reachable *only* via `serves` (direct `/model` selection, `remap:
   false`) — it is absent from `model_map`, so no failover request ever
   resolves to it (`src/proxy.rs:104`, `src/fallback.rs:121-125`). One
   profile, two models reachable by name; params configured for model A
   only. Assert the recorded outbound body for model A carries the
   configured param(s) and the recorded body for model B is unchanged from a
   params-absent baseline.

4. **Integration — escalation-hop correctness** (`tests/fallback.rs`):
   secondary coverage for the `model_map`/`escalation_order` path, in case a
   tuned model is ever also placed there. A request that escalates from
   model A to model B must show A's params on the first recorded upstream
   call and B's (or none) on the second.

## Scope boundary

- **In scope:** per-model `params` table on `openai`-format profiles, exact
  match on resolved target model id, startup validation (empty sets, empty
  keys, field collisions, anthropic-format rejection, reachability),
  request-time observability for match/no-match, `/control/profiles`
  key-only exposure.
- **Out of scope (explicit non-goals, not deferred TODOs):**
  - Prefix/`"*"`-catch-all matching on `params` keys (only exact match) — no
    concrete near-term need identified; the live `serves` list is explicitly
    labelled experiment targets, and a second model needing the same params
    is a two-line config duplication, not a design gap.
  - `format = "anthropic"` profile support (no current model needs it; now
    explicitly rejected at startup rather than silently accepted, see point 5).
  - Per-profile default params layered under per-model overrides — no
    concrete near-term need; the one model that needs tuning is fully served
    by per-model alone.
  - Automatic reasoning-budget sizing (e.g. scaling `max_tokens` based on
    `reasoning_effort`) — a different, larger feature.
  - `thinking: { enabled: false }` — `reasoning_effort = "max"` alone reached
    5/5 in measurement; the `thinking` toggle's actual wire schema also
    differs from the original ticket's assumption (`{"type": ...}` required).
    Revisit only if `reasoning_effort` proves insufficient once deployed.

## Verification milestone

Deploy with:
```toml
[profiles.together.params."deepseek-ai/DeepSeek-V4-Flash-0731"]
reasoning_effort = "max"
```
Re-run the `max_tokens: 100` single-shot reproduction against
`deepseek-ai/DeepSeek-V4-Flash-0731` through the relay (not direct to
Together, to also exercise the new code path) — it should return actual
`content` rather than `stop_reason: "max_tokens"` with an empty/truncated
`thinking`-only response. In the same deploy, issue an equivalent request to
a model with no `params` entry (e.g. `moonshotai/Kimi-K3`) and confirm its
outbound request body is unaffected (via the isolation integration test,
and/or a manual capture). Both conditions must hold — fixing DeepSeek while
silently mutating other models' requests is not success. After deploy,
observe real multi-turn Claude Code sessions on DeepSeek-V4-Flash-0731 over
a few days — the 5-trial batch above is stronger evidence than a single
probe, but it is still one prompt shape at one budget, not a substitute for
real usage.

## Revision log

- **2026-08-15 (this revision):** adversarial review (`spec-review-combined`)
  found the originally-proposed `reasoning_effort = "low"` was never measured
  and, once measured, turned out to be worse than doing nothing (1/5 vs 2/5
  success) — replaced with `reasoning_effort = "max"` (5/5), backed by a
  5-trial batch against the live provider. Also fixed: blast-radius site
  count (5 claimed → 12 actual, `Default`-derivation misunderstanding
  corrected), added the name-routed integration test as primary (the
  escalation-path test alone would never have exercised the path production
  actually uses), added anthropic-format startup rejection, changed
  `/control/profiles` exposure to param keys only (not values, matching the
  existing `base_url` omission's own rationale), added a reachability
  validation check and a request-time match/no-match signal for exact-match
  staleness risk, and linked the field-collision validation list to
  `ChatRequest`'s actual fields via a shared `const` plus a pinning test.
