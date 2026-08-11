# Milestone 2 implementation plan — limit detection, state machine, notifier, persistence

Source of truth: `docs/spec.md`, specifically §3 (Architecture — Limit
Detector, State Store, Notifier components), §4 (Route state machine), §5
(Limit detection — config-driven rules, conservative classification), §8
(Configuration — `state_file`, `[detect]`, `[notify]` sections), §9
(Observability — `/status` extended fields). Milestone 2's row in §11:

> Limit detection + state machine + notifier + state persistence —
> acceptance: fixture tests pass; real limit event flips state and fires
> notification; burst 429 does not.

**No failover routing in this milestone.** Every request still goes to
Anthropic regardless of state — §3's Router component (picking Anthropic
vs. fallback) and §6's failover policy are Milestone 3. This milestone is
purely: observe every Anthropic response, classify it, transition state,
notify, persist. The proxy's client-facing behavior (what response the
caller gets) is unchanged from Milestone 1.

**No real captured fixture exists yet** (see `docs/decisions.md`'s
2026-08-10 entry) — the default `[detect]` rule is built from spec §5's
documented "expected shape," explicitly provisional. Tests use synthesized
fixtures matching that shape, not a captured real one.

## Global Constraints

Copy these verbatim into every task reviewer's context:

1. **No request/response body logging or persistence beyond what Milestone
   1 already does** (`--capture-errors` fixtures). The Limit Detector reads
   response status/headers/body to classify, but must not introduce a new
   body-logging path — classification happens in-memory, on data already
   available at the point Milestone 1's proxy handler observes the
   response (same place `--capture-errors` hooks in).
2. **Never log Authorization/x-api-key/anthropic-* header VALUES** — same
   invariant as Milestone 1, still binding. The Limit Detector reads
   `retry-after`/`anthropic-ratelimit-*` headers (not sensitive) to extract
   reset timing; it must not need or touch auth headers at all.
3. **No new buffering of streaming response bodies.** Classification needs
   the *status code* (always available before the body streams) and,
   per spec §5, matching against the JSON *body* for `error.type`. Since
   Milestone 1 already established that error bodies are typically small
   JSON (the `--capture-errors` cap is 1 MiB) and detection only applies to
   non-2xx responses (never 2xx, which stream large legitimate bodies),
   reuse Milestone 1's existing non-2xx body-accumulation path
   (`PendingCapture` in `src/proxy.rs`) as the data source for
   classification — do not add a second accumulator. If `--capture-errors`
   is off, the Limit Detector still needs *some* non-2xx body access for
   classification — decide in Task 2 whether that means running a small
   always-on accumulation (bounded, same 1 MiB cap, same truncation
   handling) independent of whether `--capture-errors` is set, or another
   approach; this is a real design decision Task 2's implementer should
   raise if unclear rather than guess.
4. **Stack:** no new heavyweight dependency without a stated reason. State
   persistence is a small JSON file (`serde_json`, already a dependency) —
   do not add a database or embedded KV store for this. A file-execution
   notifier hook uses `tokio::process::Command` (already available via
   tokio) — no new process-spawning crate.
5. **Config additions** (extending Milestone 1's `Config`/`AnthropicConfig`
   subset): `state_file` (optional top-level string), `[detect]` (rule
   data — status/body-matcher/reset-extraction, format TBD by Task 2's
   implementer, but must be config data, not hardcoded match logic, per
   spec §5), `[notify]` (`command`, `timeout_secs`). Do **not** add
   `[profiles.*]` or `[policy]` sections yet — those are Milestone 3/4.
   `deny_unknown_fields` (Milestone 1's established convention) still
   applies to every new struct.
6. **Classify conservatively** (spec §5, non-negotiable): anything not
   matching the subscription-limit signature passes through unchanged —
   state never transitions on an ambiguous or unmatched response. A
   per-minute burst 429 with a short `retry-after` must NOT flip state to
   `LIMITED` — require either an explicit subscription marker in the body
   OR a reset horizon above a configurable threshold (`min_reset_horizon_secs`
   per spec §8, default per spec's suggestion of 300s/5min).
7. **Notifier failures never affect request handling.** Fire-and-forget,
   short timeout (spec §4: "notifier failure never affects request
   handling"). A slow or failing notifier command must not delay or break
   the proxied response.
8. **Minimize doc comments** — only where a WHY is genuinely non-obvious.

## Task 1: State Store + route state machine

Pure logic, no HTTP/axum dependency — this is the most independently
testable piece and has no dependency on Task 2/3, so it can be built and
fully unit-tested standalone.

Implement the state machine from spec §4:

```
ACTIVE ──(limit response detected)──▶ LIMITED{until: reset_at + jitter}
LIMITED ──(now >= until)──▶ PROBING
PROBING ──(next real request succeeds)──▶ ACTIVE
PROBING ──(limit response again)──▶ LIMITED{new until}
```

- A `RouteState` enum (`Active`, `Limited { until: SystemTime }`,
  `Probing`) or equivalent, with an explicit transition function that
  takes the current state + an observed outcome (limit-detected /
  succeeded / time-elapsed check) and returns the next state. Keep the
  transition logic pure/testable (a function from `(State, Event) ->
  State`, not entangled with I/O) — the HTTP-layer wiring happens in Task 2.
- **Jitter:** on transitioning to `Limited`, add 15–60s random slack past
  the reported `reset_at` (spec §4). Use `rand` if not already a
  dependency — state the addition and why; this is a reasonable new
  dependency for this specific need (no existing crate in the tree
  provides random jitter).
- **Persistence:** serialize `{state, until}` to `state_file` (if
  configured) on every transition; load on startup; a stale `until` (in
  the past) on load is treated as `Active`, not `Limited` (spec §4). If
  `state_file` is not configured, state is in-memory only (lost on
  restart, starts `Active`) — this is spec's stated default behavior
  ("optional small JSON file for persistence").
- **PROBING is passive** (spec §4): the state machine doesn't schedule any
  background health-check traffic. Transitioning `Limited -> Probing`
  happens lazily — e.g., checked on next state query, not via a timer
  task. Keep this simple: a `current_state(&self) -> RouteState` method
  that checks `now >= until` and returns `Probing` (updating stored state)
  rather than a background poller.

**Tests:** exhaustive state-transition table tests (every combination:
`Active` + limit detected → `Limited`; `Limited` + time not yet elapsed +
query → still `Limited`; `Limited` + time elapsed + query → `Probing`;
`Probing` + success → `Active`; `Probing` + limit detected again →
`Limited` with new `until`). Jitter range test (transition N times, assert
all `until` values fall within `reset_at + [15,60]s`). Persistence
round-trip test (transition, save, reload into a fresh instance, confirm
state matches). Stale-`until`-on-load test (write a state file with `until`
in the past, load, confirm resulting state is `Active`).

## Task 2: Limit Detector — config-driven classification, wired into the proxy

Depends on Task 1 (consumes its state-machine API) and Milestone 1's
`src/proxy.rs`/`src/capture.rs` (extends the existing non-2xx
response-observation point).

- **Detection rule format** (spec §5): config data, not hardcoded logic —
  status code + body-field matcher(s) (e.g. `error.type ==
  "rate_limit_error"`) + reset-time extraction (from body field and/or
  response headers `retry-after`, `anthropic-ratelimit-*`). Design a
  `[detect]` TOML schema for this. Keep it only as expressive as spec §5's
  example needs — do not build a general JSONPath engine; a small,
  explicit set of matcher fields (status, a body field path + expected
  value, a reset-source preference order) is enough. If the shape is
  ambiguous, ask before building — this is the one genuinely open design
  question in the milestone.
- **Default `[detect]` rule**, shipped in `relay.example.toml` and as the
  code default when `[detect]` is absent from config: matches HTTP 429 +
  body `error.type == "rate_limit_error"` + (per spec §5's own
  conservative-classification requirement) either an explicit subscription
  marker in the message OR reset horizon `> min_reset_horizon_secs`
  (default 300s). Reset time preference: response header
  (`retry-after` or `anthropic-ratelimit-*`) if present, else a body
  field if the rule defines one. **This default is unverified against
  real traffic** (see `docs/decisions.md`) — implement it faithfully to
  spec §5's text, and say so in your report; don't over-invest in
  precision beyond what spec §5 documents.
- **Wiring:** hook into the same point in `src/proxy.rs` where Milestone 1
  observes `status`/`headers` for the `--capture-errors` decision (already
  knows non-2xx before streaming). Resolve Global Constraint 3's open
  question here: whichever approach you choose (always-on bounded
  accumulation vs. capture-errors-gated), the client-facing stream must be
  completely unaffected — same non-negotiable as Milestone 1's tee design.
  On every non-2xx response, run it through the detector; on a match,
  drive Task 1's state machine (`limit detected` event, extracted
  `reset_at`); on 2xx or non-match, drive the `succeeded`/no-op event as
  appropriate (a 2xx response while in `Probing` should trigger the
  `Probing -> Active` transition per spec §4).
- **`/status` extension:** Milestone 1's `/status` already returns
  `{"state": "ACTIVE", "limited_until": null, "fallback_requests_served":
  0, "config_digest": "..."}` with `state` hardcoded. Wire it to the real
  state machine: `state` reflects `RouteState` (`"ACTIVE"` / `"LIMITED"` /
  `"PROBING"`), `limited_until` is the real `until` timestamp when
  `Limited`/`Probing`, else `null`. `fallback_requests_served` stays `0`
  (still no fallback routing — Milestone 3).

**Tests:** synthesized-fixture tests (a mock upstream returning the spec
§5-shaped 429 body/headers) asserting the state machine transitions;
negative case — a burst 429 (short `retry-after`, no subscription marker)
does NOT transition state, per Global Constraint 6, with an assertion this
is a real behavioral test (drive two different burst-shaped and
limit-shaped responses through the real proxy handler, not just unit-test
the matcher function in isolation — at least one test should be end-to-end
through the actual HTTP path). `/status` reflects real state across a
transition. A 2xx response while `Probing` flips to `Active`.

## Task 3: Notifier

Depends on Task 1 (state transition events to notify on).

- Exec hook: `notify.command` from config, invoked via
  `tokio::process::Command` on state transitions. Events per spec §4:
  `failover_engaged` (fires on `Active -> Limited`, with `reset_at`) and
  `recovered` (fires on `-> Active` from a non-`Active` state). (spec §4
  also lists `fallback_error` — that event requires an actual fallback
  attempt to fail, which doesn't exist until Milestone 3; do not build a
  dead code path for it now, just don't preclude adding it later.)
- Event delivered via env vars on the spawned process: `RELAY_EVENT`,
  `RELAY_RESET_AT`, `RELAY_DETAIL` (spec §4's exact names).
- **Fire-and-forget, short timeout** (Global Constraint 7): spawn, apply a
  timeout (a few seconds — pick and justify a value), do not await
  completion before returning control to the request path; a failing or
  slow notifier command must never delay or fail the proxied response.
  This almost certainly means: don't run the notifier synchronously inside
  the request-handling task at all — spawn it as a detached
  `tokio::spawn`, log a warning on failure/timeout, move on.
- `[notify]` config: `command` (optional — if absent, no-op, no error),
  `timeout_secs` (default per spec's example, `5`).

**Tests:** a notifier test using a test script/command (e.g. a shell
command that writes its env vars to a file) confirming the right
event/env-vars fire on a real `Active -> Limited` transition and on
recovery. A timeout test (command that sleeps longer than `timeout_secs`)
confirming it doesn't block the caller and doesn't panic. A
no-command-configured test confirming no-op, no error. Confirm via a
timing assertion that a slow/hanging notifier command doesn't delay the
HTTP response the client receives (spin up a real request through the
proxy with a hanging notifier configured, assert the response returns
quickly regardless).
