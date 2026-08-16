# Per-Model Request-Parameter Overrides Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let one Together-routed model (starting with `deepseek-ai/DeepSeek-V4-Flash-0731`) receive extra request parameters (`reasoning_effort`, etc.) without affecting any other model sharing the same `[profiles.together]` block.

**Architecture:** Add a `params: IndexMap<String, IndexMap<String, serde_json::Value>>` field to `ProfileConfig`, keyed by exact resolved upstream model id. Resolve it inside `fallback::prepare()` — keyed by that call's own `target_model` argument, so the escalation path's two `prepare()` calls (different target model each time) each get the right model's params — and flatten the resolved map into the outbound `ChatRequest` via `#[serde(flatten)]`. Startup validation rejects every "configured but does nothing" shape this codebase already guards against elsewhere (empty sets, empty keys, field-name collisions, anthropic-format profiles, unreachable keys).

**Tech Stack:** Rust, serde/serde_json, `indexmap::IndexMap`, existing `tests/fallback.rs` `Recorder` integration-test harness.

**Spec:** `docs/specs/2026-08-15-per-model-params-design.md` (supersedes `2026-08-13-per-model-passthrough-design.md`) — read it in full before starting; this plan implements it and does not repeat its rationale.

## Global Constraints

- Config key is `params`, **not** `passthrough` — `passthrough` already means "forward verbatim" in this codebase (`format = "anthropic"`, `passthrough_body`) and reusing it for the opposite meaning is a naming collision the spec explicitly rejected.
- Matching is **exact** on the fully-resolved target model id. No prefix/`"*"`-catch-all matching — that is an explicit non-goal.
- The concrete deployment value is `reasoning_effort = "max"` for `deepseek-ai/DeepSeek-V4-Flash-0731` — measured 5/5 success vs. 2/5 (no param) and 1/5 (`"low"`) in the spec's "Which parameter value actually fixes it" section. Do not substitute a different value without re-measuring; this plan does not revisit that question.
- `format = "anthropic"` profiles must **reject** a non-empty `params` table at startup (validation, not silent no-op) — those profiles route through `passthrough_body`, not the typed translator, so `params` would never apply there.
- `/control/profiles` exposes `params` **key names only**, never values — matches the existing `base_url`-omission rationale in `ProfileView`'s own doc comment (a nominally non-secret, operator-authored field can still carry secret-shaped content).
- Every existing `ProfileConfig` and `ChatRequest` struct literal in `src/` and `tests/` needs an explicit `params: IndexMap::new()` (or populated) added — `#[serde(default)]` only affects deserialization, not Rust struct-literal construction, and neither struct derives `Default`.
- `IndexMap` (not `HashMap`) for both map levels, consistent with `model_map`'s existing ordering convention in this codebase.
- Run `cargo test` (full suite) before every commit that touches `src/`; run `cargo fmt`/`cargo clippy` if the project's CI does (check `.github/workflows/` or a `justfile`/`Makefile` if present — follow whatever this repo's own check command is).

---

## File Structure

| File | Change |
|---|---|
| `src/translate/openai.rs` | Add `ChatRequest.params` field + `pub const CHAT_REQUEST_FIELD_NAMES` const + pinning test |
| `src/config.rs` | Add `ProfileConfig.params` field, `validate()` checks, TOML parse tests |
| `src/fallback.rs` | Thread `&ProfileConfig` into `prepare()`, resolve params by exact `target_model` lookup, add match/no-match observability, fix struct-literal test helpers |
| `src/translate/request.rs` | Accept resolved params map in `request_to_openai`/`convert`, populate `ChatRequest.params`, fix the one `ChatRequest` literal, update the two whole-object-equality tests |
| `src/router.rs` | Fix struct-literal test helper |
| `src/state.rs` | Fix struct-literal test helper |
| `src/control.rs` | Add `params` (keys only) to `ProfileView` |
| `tests/fallback.rs` | Fix struct-literal test helpers; add name-routed isolation test (primary) and escalation-hop test (secondary) |
| `tests/control.rs`, `tests/log_forging.rs`, `tests/log_escalation.rs`, `tests/log_hygiene_control.rs`, `tests/log_hygiene_fallback.rs`, `tests/log_hygiene_provider_error.rs` | Fix struct-literal test helpers (add `params: IndexMap::new()`) |

---

### Task 1: `ChatRequest.params` field + shared field-name const

**Files:**
- Modify: `src/translate/openai.rs` (the `ChatRequest` struct and its imports)
- Test: `src/translate/openai.rs` (co-located `#[cfg(test)] mod tests`)

**Interfaces:**
- Produces: `ChatRequest.params: IndexMap<String, serde_json::Value>` (public field). `pub const CHAT_REQUEST_FIELD_NAMES: &[&str]` listing every other `ChatRequest` field's serialized JSON name, for Task 3's collision check to consume.

- [ ] **Step 1: Add the `IndexMap` import**

  Add `use indexmap::IndexMap;` to `src/translate/openai.rs`'s import block (the file currently imports only `serde` and `serde_json::Value` — confirm by reading the top of the file first).

- [ ] **Step 2: Add the `params` field to `ChatRequest`**

  Add to the `ChatRequest` struct, after the existing `stream: bool` field:

  ```rust
  #[serde(flatten, skip_serializing_if = "IndexMap::is_empty")]
  pub params: IndexMap<String, serde_json::Value>,
  ```

  This must come **last** in the struct so `#[serde(flatten)]` output appends after the named fields (cosmetic, but keeps generated JSON deterministic and matches this codebase's practice of putting flatten-like/catch-all fields last where used elsewhere).

- [ ] **Step 3: Add the shared field-name const**

  Add near the top of `src/translate/openai.rs`, above the `ChatRequest` struct:

  ```rust
  /// Every top-level JSON key `ChatRequest` serializes. A `params` entry using
  /// one of these names would double-emit that key via `#[serde(flatten)]` —
  /// serde_json accepts this silently (last-write-wins is not guaranteed), so
  /// `ProfileConfig::validate()` rejects a colliding key at startup instead.
  /// Keep this list in sync with `ChatRequest`'s fields — the pinning test
  /// below fails if it drifts.
  pub const CHAT_REQUEST_FIELD_NAMES: &[&str] = &[
      "model", "messages", "max_tokens", "temperature", "top_p", "stop",
      "tools", "tool_choice", "stream",
  ];
  ```

- [ ] **Step 4: Write the pinning test**

  In `src/translate/openai.rs`'s test module, add a test that builds a `ChatRequest` with every field populated (non-default values, non-empty `params`), serializes it, and asserts the JSON object's top-level keys are exactly `CHAT_REQUEST_FIELD_NAMES` plus whatever keys `params` itself contributed. Read the existing test module first (there should already be a `ChatRequest` builder pattern or literal used by nearby tests — follow it) so the new test uses the same construction style. Concretely:

  ```rust
  #[test]
  fn chat_request_field_names_const_matches_actual_serialized_keys() {
      // Build with every named field populated so none are skipped by
      // skip_serializing_if, plus a non-empty params map.
      let request = ChatRequest {
          model: "m".to_string(),
          messages: vec![], // fill with a minimal valid Message per this file's existing test helpers
          max_tokens: Some(1),
          temperature: Some(0.5),
          top_p: Some(0.9),
          stop: vec!["x".to_string()],
          tools: vec![], // fill with a minimal valid Tool per this file's existing test helpers if tools is not skip_serializing_if empty-safe alone
          tool_choice: None, // or Some(...) per existing helper
          stream: true,
          params: IndexMap::new(), // empty here on purpose — asserts the const covers the NAMED fields
      };
      let json = serde_json::to_value(&request).unwrap();
      let obj = json.as_object().unwrap();
      let mut actual_keys: Vec<&str> = obj.keys().map(|k| k.as_str()).collect();
      actual_keys.sort();
      let mut expected: Vec<&str> = CHAT_REQUEST_FIELD_NAMES.to_vec();
      expected.sort();
      // messages/tool_choice/stop have skip_serializing_if too — this test
      // must populate every Option so nothing is skipped. Adjust the
      // construction above using this file's real Message/Tool/ToolChoice
      // types (read them before writing this test) so the request actually
      // serializes with all applicable keys present.
      assert_eq!(actual_keys, expected);
  }
  ```

  Note for the implementer: the exact `Message`/`Tool`/`ToolChoice` construction above is illustrative — read `src/translate/openai.rs` in full first and adapt the literal to those types' real shapes so the test compiles and every optional field that has a value actually serializes (don't leave any field `None`/empty except `params`, or the key-set assertion will be wrong).

- [ ] **Step 5: Run the test**

  `cargo test --lib translate::openai`. Expected: the new test passes, and no existing test in this file breaks (check — `ChatRequest` gained a field, so any other test constructing a `ChatRequest` literal in this file needs `params: IndexMap::new()` added too; fix any compile errors here before moving on).

- [ ] **Step 6: Commit**

  ```bash
  git add src/translate/openai.rs
  git commit -m "feat: add ChatRequest.params field and field-name const"
  ```

---

### Task 2: `ProfileConfig.params` field + fix struct-literal breakage across the codebase

**Files:**
- Modify: `src/config.rs` (`ProfileConfig` struct, ~lines 37-61)
- Modify (struct-literal fixes only — add `params: IndexMap::new()` to each): `src/state.rs:386`, `src/router.rs:77`, `src/fallback.rs:1196`, `src/fallback.rs:1255`, `src/translate/request.rs:61` (this one is a `ChatRequest` literal, not `ProfileConfig` — add `params: IndexMap::new()` there too since Task 1 added the field), `tests/fallback.rs:271`, `tests/control.rs:55`, `tests/log_forging.rs:44`, `tests/log_escalation.rs:104`, `tests/log_hygiene_control.rs:33`, `tests/log_hygiene_fallback.rs:113`, `tests/log_hygiene_provider_error.rs:104`

**Interfaces:**
- Consumes: Task 1's `ChatRequest.params` field (for the `translate/request.rs:61` fix).
- Produces: `ProfileConfig.params: IndexMap<String, IndexMap<String, serde_json::Value>>` (public field), compiling cleanly everywhere.

- [ ] **Step 1: Add the field to `ProfileConfig`**

  In `src/config.rs`, add to `ProfileConfig` (after the existing `model_map` field):

  ```rust
  #[serde(default)]
  pub params: IndexMap<String, IndexMap<String, serde_json::Value>>,
  ```

  `serde_json` must already be a dependency (it's used elsewhere in translation) — if `src/config.rs` doesn't already import it, add `use serde_json;` or qualify as `serde_json::Value` per this file's existing import style.

- [ ] **Step 2: Fix every `ProfileConfig` struct literal**

  For each of these locations, read the surrounding function first, then add `params: IndexMap::new()` as a field in the literal (do **not** use `..Default::default()` — `ProfileConfig` derives no `Default`):
  - `src/state.rs:386` (test helper `fn profile() -> crate::config::ProfileConfig`)
  - `src/router.rs:77` (test helper)
  - `src/fallback.rs:1196` and `src/fallback.rs:1255` (test helpers `fn profile(...)` / `fn config(...)`)
  - `tests/fallback.rs:271` (`Recorder`-pattern test helper)
  - `tests/control.rs:55`
  - `tests/log_forging.rs:44`
  - `tests/log_escalation.rs:104`
  - `tests/log_hygiene_control.rs:33`
  - `tests/log_hygiene_fallback.rs:113`
  - `tests/log_hygiene_provider_error.rs:104`

  Sites using `..profile(...)` functional-update syntax (e.g. `src/config.rs:1009`, `src/config.rs:1169`, `tests/fallback.rs:2780`) need **no** change — confirm each one really is functional-update syntax before skipping it (if any turns out to be a bare literal, fix it too).

- [ ] **Step 3: Fix the one `ChatRequest` literal**

  In `src/translate/request.rs:61`, add `params: IndexMap::new()` to the `ChatRequest { ... }` literal (this will be replaced with a real resolved value in Task 4 — for this task, just make it compile).

- [ ] **Step 4: Compile and run the full test suite**

  `cargo build --all-targets && cargo test`. Fix any remaining struct-literal compile errors this list missed (grep for `ProfileConfig {` and `ChatRequest {` across `src/` and `tests/` to double check nothing was missed). Expected: clean compile, all existing tests still pass (behavior is unchanged — every `params` map added so far is empty).

- [ ] **Step 5: Commit**

  ```bash
  git add -A
  git commit -m "feat: add ProfileConfig.params field, fix struct-literal call sites"
  ```

---

### Task 3: `ProfileConfig::validate()` — reject every silently-broken `params` shape

**Files:**
- Modify: `src/config.rs` (`ProfileConfig::validate()`, ~lines 82-103)
- Test: `src/config.rs` (co-located tests, alongside `an_unknown_profile_field_is_a_parse_error` ~1231-1246 and the TOML parse tests ~1178-1216)

**Interfaces:**
- Consumes: Task 1's `CHAT_REQUEST_FIELD_NAMES` const (`crate::translate::openai::CHAT_REQUEST_FIELD_NAMES` or this file's actual module path — check how `openai.rs` is imported elsewhere in `config.rs`, or whether `config.rs` currently has no dependency on `translate` and this would be a new one; if a new cross-module dependency is awkward, an acceptable alternative is defining the const in `config.rs` itself and having Task 1's pinning test in `openai.rs` import it from there instead — pick whichever direction keeps `config.rs`'s existing dependency graph cleaner, and note the choice in the commit message).
- Produces: `validate()` errors (via this file's existing `anyhow`/`bail!` convention — check `validate_escalation_order`'s error style at ~425-449 and match it) for each of the five new invalid shapes below.

- [ ] **Step 1: Read `validate()` and `validate_escalation_order()` in full**

  Read `src/config.rs:82-103` and `:425-449` to learn this file's exact error-construction style (message wording conventions, `anyhow::bail!` vs `Err(anyhow!(...))`, etc.) before writing new checks — match it exactly rather than inventing a new style.

- [ ] **Step 2: Add the five validation checks**

  Inside `ProfileConfig::validate()` (or a new `params`-specific private method it calls — follow whatever decomposition style the existing checks already use), add, for every profile's `params` table:

  1. **Empty inner param-set**: `params."<model>"` maps to an empty inner table → error naming the profile and model key.
  2. **Empty outer key**: `params.""` → error (can never match a resolved model id).
  3. **Empty inner key**: an inner table containing `"" : <value>` → error (would serialize a literal `""` JSON key).
  4. **Field-name collision**: an inner key equal to any entry in `CHAT_REQUEST_FIELD_NAMES` → error naming the colliding key and model.
  5. **`format = "anthropic"` with non-empty `params`**: → error. (Read how `format` is checked/typed elsewhere in this file — likely a `String` compared against `"anthropic"`/`"openai"`, or an enum; match the existing pattern.)
  6. **Unreachable key**: an outer key (model id) that is not a value in this profile's `model_map` AND not prefix-matched by any entry in this profile's `serves` → error. (This deliberately cannot catch a wrong model name under a correct prefix — that's fine, it's documented as a partial check in the spec; do not try to make it exact-match-aware beyond prefix.)

- [ ] **Step 3: Write TOML parse tests**

  Alongside `profiles_parse_from_toml_in_declaration_order` (~1178-1216), add two tests:
  - `params_parses_from_inline_table_toml` — a profile with `params = { "model-a" = { reasoning_effort = "max" } }` parses and the value round-trips correctly.
  - `params_parses_from_nested_table_toml` — the same via `[profiles.x.params."model-a"]` TOML syntax, confirming both forms produce identical parsed structures.

- [ ] **Step 4: Write validate() failure tests**

  One test per check in Step 2 (6 tests total), each constructing a minimal config with exactly the one invalid shape and asserting `validate()` returns an error. Follow the existing test naming convention in this file (e.g. `an_unknown_profile_field_is_a_parse_error`'s naming style — `snake_case_full_sentence`).

- [ ] **Step 5: Confirm `relay.example.toml` coverage claim honestly**

  Read `relay.example.toml` and `relay_example_toml_parses_and_validates` (~1560-1584). Per the spec's finding, every `[profiles.*]` block in that file is currently commented out, so this test loops over zero profiles today. **Do not** add a commented-out `params` example and claim it's covered — that test would not exercise it. Leave `relay.example.toml` as-is (this plan does not change the file's commented-out convention) and rely on Step 3/4's dedicated unit tests for real coverage. Note this decision in the task's commit message so it isn't mistaken for an oversight later.

- [ ] **Step 6: Run tests**

  `cargo test --lib config`. Expected: all new tests pass, no regressions.

- [ ] **Step 7: Commit**

  ```bash
  git add src/config.rs
  git commit -m "feat: validate params table (empty sets/keys, field collisions, anthropic profiles, reachability)"
  ```

---

### Task 4: Resolve params in `translate::request_to_openai`/`convert`

**Files:**
- Modify: `src/translate/request.rs` (`request_to_openai` ~line 27, `convert` ~line 38, the `ChatRequest` literal fixed in Task 2 Step 3)
- Test: `src/translate/request.rs` (existing whole-object-equality tests)

**Interfaces:**
- Consumes: a resolved `&IndexMap<String, serde_json::Value>` (or owned, whichever avoids an unnecessary clone given how Task 5 calls this) passed in as a new parameter.
- Produces: `ChatRequest.params` populated from that parameter instead of always-empty.

- [ ] **Step 1: Read the current file in full**

  `src/translate/request.rs` is large (1300+ lines per the earlier `a_multi_turn_multi_tool_conversation_translates_end_to_end` test citation around line 1237). Read it fully before editing — this plan does not reproduce its current body.

- [ ] **Step 2: Add a `params` parameter to `request_to_openai` and `convert`**

  Change signatures to accept the resolved params map, e.g.:
  ```rust
  pub fn request_to_openai(body: &[u8], target_model: &str, params: &IndexMap<String, serde_json::Value>) -> Result<TranslatedRequest>
  fn convert(request: MessagesRequest, target_model: &str, stream: bool, params: &IndexMap<String, serde_json::Value>) -> Result<ChatRequest>
  ```
  Adjust exact parameter order/ownership to match this file's existing conventions (check whether other params here are passed by reference or value and follow suit). Populate the `ChatRequest { ..., params: params.clone(), }` literal from Task 2 Step 3 with the real value instead of `IndexMap::new()`.

- [ ] **Step 3: Update the two whole-object-equality tests**

  `a_minimal_request_maps_its_scalars_and_substitutes_the_target_model` and `a_multi_turn_multi_tool_conversation_translates_end_to_end` both assert full JSON equality against a `ChatRequest`. Update both call sites to pass an empty `IndexMap::new()` (confirming behavior is unchanged when no params apply) and update their expected `ChatRequest` literals to include `params: IndexMap::new()`.

- [ ] **Step 4: Add one new translation test with a non-empty params map**

  A minimal request translated with a non-empty params map (e.g. `{"reasoning_effort": "max"}`) produces a `ChatRequest` whose serialized JSON includes that key alongside the normal fields, and an empty params map produces byte-identical output to the pre-existing baseline test.

- [ ] **Step 5: Run tests**

  `cargo test --lib translate::request`. Fix any callers of `request_to_openai`/`convert` outside this file that now fail to compile (there should be exactly one production caller — Task 5 handles it; if this task's compile surfaces it first, that's fine, just make it compile with an empty map for now and let Task 5 wire the real value).

- [ ] **Step 6: Commit**

  ```bash
  git add src/translate/request.rs
  git commit -m "feat: thread resolved params into ChatRequest translation"
  ```

---

### Task 5: Resolve params in `fallback::prepare()`, keyed per upstream attempt

**Files:**
- Modify: `src/fallback.rs` (`prepare()` ~line 417, its two call sites ~134 and ~275)
- Test: `tests/fallback.rs` (new tests — see Task 6/7; this task itself should not need new unit tests beyond what Task 6/7 add, since the resolution logic here is a thin lookup)

**Interfaces:**
- Consumes: Task 4's updated `request_to_openai` signature; `profile.params` (Task 2's field).
- Produces: `prepare()` now takes `&ProfileConfig` (or equivalent) and resolves params by exact lookup on its own `target_model` argument before calling `request_to_openai`.

- [ ] **Step 1: Read `prepare()` and both call sites in full**

  Read `src/fallback.rs:400-440` (the `prepare()` function and its doc comment describing the "once per upstream attempt" invariant) and both call sites at `:134` and `:275` (and the surrounding `deliver()`/escalation-loop context) before editing.

- [ ] **Step 2: Thread `&ProfileConfig` into `prepare()`**

  Add a `profile: &ProfileConfig` parameter to `prepare()`. Inside, before calling `request_to_openai` (the `translated == true` branch), do an **exact-match** lookup: `profile.params.get(target_model).cloned().unwrap_or_default()`, and pass that to `request_to_openai`.

- [ ] **Step 3: Update both call sites**

  At `src/fallback.rs:134` and `:275`, pass `profile` (or `request.profile`/whatever the local binding is — check `deliver()`'s scope) through to `prepare()`. Confirm the second call site (inside the escalation loop, using `next` as `target_model`) naturally gets `next`'s own params via the same lookup — no separate logic needed, this falls out of Step 2 automatically.

- [ ] **Step 4: Add request-time match/no-match observability**

  Read how this file's existing `RequestLog`/tracing calls are structured (there should be an existing per-attempt log point near `prepare()`'s call sites, given the codebase's `tracing`/`RequestLog` conventions referenced in the spec). Add a `tracing::debug!` (or a `RequestLog` field, whichever fits the existing pattern with less structural change) recording whether `profile.params` matched `target_model` for this attempt — this is what lets an operator later distinguish "no params configured for this model" from "params stopped matching after a rename."

- [ ] **Step 5: Compile**

  `cargo build --all-targets`. Fix any remaining callers.

- [ ] **Step 6: Commit**

  ```bash
  git add src/fallback.rs
  git commit -m "feat: resolve per-model params inside prepare(), keyed per upstream attempt"
  ```

---

### Task 6: Integration test — name-routed isolation proof (primary)

**Files:**
- Modify: `tests/fallback.rs`

**Interfaces:**
- Consumes: the `Recorder` pattern already established in `tests/fallback.rs` (an `Arc<Mutex<Vec<...>>>` capturing outbound request bodies at a mock upstream — read existing tests using it before writing this one, and follow the same setup/teardown style).

- [ ] **Step 1: Read the `Recorder` pattern and one existing test using it**

  Read `tests/fallback.rs`'s `Recorder` setup and at least one full existing test that uses it end-to-end, to match its exact conventions (mock server setup, `relay_config`/`serve_relay_with` usage from `tests/common/mod.rs`).

- [ ] **Step 2: Write the isolation test**

  This is the load-bearing test for the whole feature. Set up one profile with two models reachable by name (`remap: false` path — i.e. selected directly via the request's `model` field matching a `serves` prefix, not via `model_map`), configure `params` for model A only (e.g. `{"reasoning_effort": "max"}`), send one request each for model A and model B through the relay, and assert:
  - The recorded outbound body for model A contains the configured param(s).
  - The recorded outbound body for model B is **byte-identical** to a baseline captured with no `params` table configured at all (not just "doesn't contain the param" — genuinely unchanged).

  This exercises the path DeepSeek-V4-Flash-0731 actually uses in the live deployed config (`serves`-matched, not `model_map`-matched) — per the spec, this is the primary test, not the escalation one in Task 7.

- [ ] **Step 3: Run the test**

  `cargo test --test fallback name_routed`. Expected: passes, proving isolation on the production path.

- [ ] **Step 4: Commit**

  ```bash
  git add tests/fallback.rs
  git commit -m "test: prove per-model params isolation on the name-routed path"
  ```

---

### Task 7: Integration test — escalation-hop correctness (secondary)

**Files:**
- Modify: `tests/fallback.rs`

**Interfaces:**
- Consumes: same `Recorder` pattern as Task 6; this repo's existing `escalation_order`/`model_map` test setup (there should be an existing escalation test to model this one after — read it first).

- [ ] **Step 1: Read an existing escalation-path test**

  Find and read an existing test in `tests/fallback.rs` that exercises the escalation ladder (a request that doesn't fit its target and retries one slot up) to match its setup style.

- [ ] **Step 2: Write the escalation-hop test**

  Configure a profile with `escalation_order` covering two models, `params` configured for model A only, and force an escalation from A to B (however the existing escalation test triggers this — e.g. an oversized prompt). Assert the **first** recorded upstream call (to A) carries A's params, and the **second** recorded upstream call (to B) does not carry A's params (either none, or B's own if also configured). This is the test the original per-profile-scoped design would have failed, and the one that proves Task 5's per-attempt resolution is correct.

- [ ] **Step 3: Run the test**

  `cargo test --test fallback escalation`. Expected: passes.

- [ ] **Step 4: Commit**

  ```bash
  git add tests/fallback.rs
  git commit -m "test: prove per-model params resolve correctly per escalation hop"
  ```

---

### Task 8: `/control/profiles` — expose `params` key names only

**Files:**
- Modify: `src/control.rs` (`ProfileView`, ~lines 242-256)
- Test: `tests/control.rs` (existing control-endpoint tests — read the file's conventions first)

**Interfaces:**
- Consumes: `ProfileConfig.params` (Task 2).
- Produces: `ProfileView.params: IndexMap<String, Vec<String>>` — model id → sorted-or-insertion-order list of that model's configured param key names, never values.

- [ ] **Step 1: Read `ProfileView` and its doc comment in full**

  Read `src/control.rs:242-256`, including the existing comment explaining why `base_url` is omitted — this task's `params` field follows the same rationale (operator-authored, nominally non-secret, but could carry secret-shaped content in its values) and should say so in its own doc comment, not just add the field silently.

- [ ] **Step 2: Add the field and its construction**

  Add `pub params: IndexMap<String, Vec<String>>` to `ProfileView`, with a doc comment referencing the `base_url` precedent. Wherever `ProfileView` is constructed from a `ProfileConfig`, build this field as `profile.params.iter().map(|(model, table)| (model.clone(), table.keys().cloned().collect())).collect()`.

- [ ] **Step 3: Update/add a control-endpoint test**

  Read `tests/control.rs`'s existing conventions, then add or extend a test asserting: a profile with `params` configured for one model shows that model's param **key names** in the `/control/profiles` response, and does **not** show the values anywhere in the response body.

- [ ] **Step 4: Run tests**

  `cargo test --test control`. Expected: passes.

- [ ] **Step 5: Commit**

  ```bash
  git add src/control.rs tests/control.rs
  git commit -m "feat: expose per-model params key names (not values) via /control/profiles"
  ```

---

## Final Review and Deployment (orchestrator, not a subagent task)

After Task 8's task-level review passes, dispatch the final whole-branch review (per `subagent-driven-development`, most capable available model) against the full diff on `per-model-params` vs `main`, covering: spec alignment, the isolation property end-to-end, and anything the per-task reviews couldn't see cross-task (e.g. does the observability signal from Task 5 actually show up correctly once Task 8's `/control` exposure and Task 6/7's tests are all in place together).

Once clean, `cargo test` the full suite one more time, then:

1. Merge `per-model-params` into local `main` and push (per this repo's pre-authorized workflow — no need to ask).
2. **Deployment** (separate repo, `~/nixos`, done directly by the orchestrator — small, well-understood, single-file config change, not a fresh SDD task): per `~/nixos/CLAUDE.md`, create a git worktree off `~/nixos` `main` (never edit `~/nixos` in place, even though this session may look "configured to work in place" — that hint does not override this repo's own rule). In the worktree, edit `home/claude-relay.nix`'s `model_map`/profile block to add:
   ```nix
   params."deepseek-ai/DeepSeek-V4-Flash-0731" = { reasoning_effort = "max"; };
   ```
   (adapt to this file's actual Nix attrset syntax for `model_map`/`serves` — read the surrounding block first). Run `home-manager switch --flake .#intendednull` from the worktree, then merge the worktree branch to local `main` and push, per that repo's own workflow.
3. **Verify against the live relay**: re-run the `max_tokens: 100` reproduction from the spec's "Problem" section against `deepseek-ai/DeepSeek-V4-Flash-0731` through `http://127.0.0.1:8600` — expect actual `content`, not an empty `thinking`-only response. Then issue an equivalent request to `moonshotai/Kimi-K3` (no `params` entry) and confirm nothing about its behavior changed.
4. Report the outcome — this is the point the goal ("land on a tuning setup for DeepSeek that doesn't cut off") is actually met, not before.
