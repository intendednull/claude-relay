# Milestone 3 implementation plan — name-based routing + fallback to Together AI

Source of truth: `docs/spec.md` §3 (Router, Translator components), §6
(Failover policy), §7a (model mapping), §7b (auth/header hygiene —
security-critical), §7c (wire format, Phase 2 applies — see below), §7d
(name-based routing), §8 (config — `[profiles.*]`, `[policy]`), §8b
(profiles + control API, minus `/control/mode` which is Milestone 4), §9
(observability additions). Milestone 3's row in `docs/spec.md` §11 (updated
2026-08-10 to reflect the Phase-2 finding below):

> Name-based routing (§7d) + failover to the fallback provider (profile
> schema §8b, model remap, header hygiene, `new-sessions` policy,
> `POST /control/profile`) + the §7c wire-format layer the chosen provider
> actually needs — acceptance: E2E drill passes; auth-stripping invariant
> tested; explicit open-model requests route by name with Anthropic ACTIVE;
> profile switch applies to new requests only.

**Why this milestone is bigger than the table implies:** `docs/decisions.md`
records that Together AI (the confirmed, only fallback provider — see
decisions.md) has no Anthropic-compatible endpoint, only OpenAI-format. Spec
§7c's Phase 2 (the full request/response/SSE translator) is therefore
required for this milestone to have any real fallback provider it can serve
at all — it is not the separately-deferrable milestone-table row 5 the
original design treated it as. Building translated by the user's explicit
decision (`docs/decisions.md`): in `relay` itself, not a LiteLLM sidecar.

**The credential gate, precisely.** Every task below is scoped to be fully
buildable and testable against mock upstreams (both Anthropic-format and
OpenAI-format fakes) with **no Together API key required**. The key becomes
necessary at exactly one point: final live verification — capturing real
recorded traffic to confirm the translator's fixtures match Together's
actual response shapes (spec §7c: "golden-file tests against recorded real
traffic... are mandatory before trusting it"), and spec §10 item 5's E2E
drill (a real Claude Code task completed on the fallback path). That step is
**out of scope for the 4 tasks below** — flagged explicitly as the point
where this plan stops and the credential is needed, not guessed around.

**Resolving an open question from Milestone 2:** `docs/decisions.md` left
open whether `min_reset_horizon_secs`/`reset_jitter_secs` should move to
`[policy]` (matching spec §8 literally) or stay in `[detect]` (where
Milestone 2 put them, since `[policy]` was banned that milestone). Decision
for this milestone: **move them to `[policy]`**, matching spec exactly. The
project has no deployed configs yet beyond this repo's own `relay.example.toml`
(no external users to break), so a clean match to the spec now is better
than carrying a permanent, undocumented-reason deviation forward. Task 1
below implements this move.

## Global Constraints

Copy these verbatim into every task reviewer's context:

1. **Header hygiene is security-critical, tested-invariant, not
   convention** (spec §7b). The fallback route strips ALL client auth
   headers (`Authorization`, `x-api-key`, `anthropic-*` including
   `anthropic-beta`) and injects the active profile's own key, read from
   the env var its `api_key_env` names — **never from the config file
   directly, never logged**. The Anthropic OAuth token must never reach a
   fallback provider under any code path, including error paths. The
   Anthropic route's existing verbatim-forward behavior (Milestone 1/2,
   unchanged) is the contrast case — do not let fallback-route logic bleed
   into it.
2. **Never log any header VALUE that could be a credential** — this now
   includes the fallback provider's own API key (read via `api_key_env`),
   in addition to Milestone 1/2's `Authorization`/`x-api-key`/`anthropic-*`
   rule. Header *names* may appear in logs; values must not.
3. **Streaming discipline, route-specific.** The Anthropic route (existing
   code) stays exactly as it is — verbatim byte-for-byte streaming
   passthrough, untouched by this milestone. The fallback route is
   necessarily different: translation means parsing and re-emitting the
   body, which cannot be a byte-for-byte passthrough. Within that
   constraint: stream what can be streamed (synthesize SSE deltas as they
   arrive from the upstream rather than buffering the whole response
   before responding), and bound memory sensibly for whatever must be
   buffered (a non-streaming JSON response, or a single SSE frame) rather
   than accumulating without limit — match the spirit of Milestone 1/2's
   existing caps (e.g. `CAPTURE_BODY_CAP`) even though this is new code, not
   an extension of theirs.
4. **No new heavyweight dependency without a stated reason.** An HTTP
   client for the fallback route already exists (`reqwest`, via
   `AppState.http` — reuse it, don't add a second one, unless a concrete
   reason requires otherwise). A JSON-transformation library beyond
   `serde_json` (already a dependency) should not be needed for the
   translation table in spec §7c — it's field remapping, not general
   schema transformation.
5. **Config additions, scoped:** `[profiles.<name>]` (`base_url`,
   `api_key_env`, `format` — `"anthropic"` or `"openai"`, `serves`,
   `model_map`, per spec §8) and `[policy]` (`mode`, `active_profile`,
   `min_reset_horizon_secs`, `reset_jitter_secs` — moved here from
   Milestone 2's `[detect]` per the resolution above). Do **not** add
   `/control/mode`, hot-reload, or anything else named in Milestone 4's
   row. `deny_unknown_fields` on every new struct, matching the established
   convention.
6. **Mid-stream failover is prohibited** (spec §6). Once any SSE bytes have
   reached the client, an upstream failure terminates that stream with an
   error event — never a silent retry on the fallback, never a silent
   retry on Anthropic either. Only requests that haven't yet produced
   client-visible bytes are eligible for the fallback route at all.
7. **`count_tokens` always goes to Anthropic**, regardless of route state
   or policy mode (spec §6). Never translate or route it to the fallback —
   token counts against the wrong tokenizer are worse than an error.
8. **Minimize doc comments** — only where a WHY is genuinely non-obvious.
9. **Tool-use fidelity is the make-or-break of the translator** (spec's own
   words). Every translation test involving `tools`/`tool_use`/`tool_result`
   must round-trip realistic, multi-turn, multi-tool-call fixtures — not
   just a single simple exchange. If a design choice trades off tool-use
   fidelity for simplicity anywhere, flag it loudly rather than deciding
   quietly.
10. **No Together credentials are available in this environment.** Every
    task's tests use mock upstreams (a second local HTTP server, matching
    the pattern Milestone 1/2 already established) speaking the relevant
    format — Anthropic-format for the Anthropic-compatible-profile case
    (untested against a real such provider, but the code path spec
    describes), and OpenAI-format for the translator. Do not attempt to
    reach `api.together.ai` or any real provider from tests or from your
    own verification — if you find yourself wanting a real key to verify
    something, that's a signal to stop and report it as a concern, not to
    find a workaround.

## Task 1: Config schema (`[profiles.*]`, `[policy]`) + name-based router

No dependency on Tasks 2/3 — this is the routing *decision* only, not the
forwarding mechanics.

- **`[profiles.<name>]`** (spec §8): `base_url: String`, `api_key_env:
  String`, `format: String` (`"anthropic"` | `"openai"`, reject anything
  else at validation), `serves: Vec<String>` (prefix list), `model_map:
  HashMap<String, String>` (keys matched by prefix against the incoming
  `model` field, `"*"` as catch-all — spec §7a). `deny_unknown_fields`.
  Zero or more profiles may be configured (zero is valid — no fallback
  configured means §7d's non-`claude-*` routing always falls through with
  nothing to fall through to; handle that as a clean "no such profile"
  condition, not a crash).
- **`[policy]`** (spec §8, moved here per this plan's resolution above):
  `mode: String` (`"new-sessions"` default | `"all"` | `"notify-only"` —
  only the field and its validation belong to this task; *behavior* per
  mode is Task 3's job), `active_profile: Option<String>` (must name a
  configured profile if present; validate at startup), `min_reset_horizon_secs`
  and `reset_jitter_secs: [u64; 2]` (moved from Milestone 2's `[detect]` —
  update `DetectConfig`/`route_state.rs`'s jitter accordingly; this is a
  real code move, not just a new struct, since Milestone 2 hardcoded the
  jitter window and put the horizon field elsewhere). `deny_unknown_fields`.
  Update `relay.example.toml` and `docs/decisions.md` to mark the forward-compat
  question as resolved (move, not deviate).
- **Name-based router** (spec §7d): given a request's `model` field, resolve
  which route it takes, evaluated *before* consulting Milestone 2's route
  state machine:
  1. `model` starts with `claude-` → Anthropic route (existing behavior,
     unchanged; subject to Milestone 2's state machine and — once Task 3
     lands — the failover policy).
  2. Otherwise → the first configured profile whose `serves` entry
     prefix-matches `model`, in config order. No match → falls through to
     `policy.active_profile` if one is configured; if `active_profile`
     also doesn't claim it (or none is configured at all), spec says "a
     name the active profile's endpoint rejects surfaces as that
     provider's error — the proxy does not validate model names," so at
     this routing-decision layer, falling through to `active_profile` (or
     failing cleanly with a clear error if no profile exists at all) is
     correct; do not try to be smarter than the spec here.
  - This routing decision is *pure* — a function from `(model: &str,
    config: &Config, current_route_state)` to "which profile, if any, or
    Anthropic" — testable with plain unit tests, no HTTP involved. Keep it
    that way; Task 3 wires it into the actual request-handling path.

**Tests:** exhaustive routing-decision table (claude-* → Anthropic
regardless of profiles configured; a name matching profile A's `serves` →
profile A; a name matching no profile's `serves` → falls through to
`active_profile`; no `active_profile` and no match → clean error, not a
panic). Config validation: `format` rejects anything other than
`anthropic`/`openai`; `active_profile` naming a nonexistent profile is
rejected at startup; `deny_unknown_fields` on both new structs. The jitter
move: Milestone 2's existing jitter tests still pass reading from the new
config location.

## Task 2: The OpenAI-format translator (spec §7c Phase 2)

No dependency on Task 1 — this is a pure transformation module, testable in
complete isolation with hand-built fixtures. **This is the highest-risk
task in the milestone** (spec's own words: "the hairiest code in the
project"); take real care, and ask before guessing on any ambiguous
mapping.

Implement request translation (Anthropic → OpenAI) and response translation
(OpenAI → Anthropic, both non-streaming and streaming SSE) per spec §7c's
table:

| Anthropic | OpenAI |
|---|---|
| top-level `system` (string or blocks) | `messages[0]` with `role: "system"` (blocks flattened to text) |
| `content: [{type: text}]` | string content |
| `tool_use` block | `assistant.tool_calls[]` with `function.arguments` as JSON string |
| `tool_result` block | `role: "tool"` message with `tool_call_id` |
| `tools[].input_schema` | `tools[].function.parameters` |
| image block (base64) | `image_url` with data URI |
| `stop_reason: end_turn / tool_use / max_tokens` | `finish_reason: stop / tool_calls / length` |
| `thinking` blocks in history | drop |
| SSE `message_start / content_block_start / content_block_delta (text_delta, input_json_delta) / content_block_stop / message_delta / message_stop` | synthesized from `chat.completion.chunk` deltas; `input_json_delta` accumulates from streamed `tool_calls[].function.arguments` fragments |

- **Request direction** (Anthropic request in, OpenAI request out): flatten
  `system`, map content blocks, map `tools`, map images. This direction has
  no streaming concerns — it's the request body, a single JSON transform.
- **Response direction, non-streaming**: OpenAI's non-streaming
  `chat.completion` response back to an Anthropic-shaped response —
  `finish_reason`→`stop_reason`, `tool_calls`→`tool_use` blocks, etc.
- **Response direction, streaming (the hard part)**: synthesize Anthropic's
  SSE event sequence from OpenAI's `chat.completion.chunk` deltas as they
  arrive — this must genuinely stream (emit each synthesized Anthropic SSE
  event as soon as enough of the OpenAI chunk stream has arrived to produce
  it), not buffer the whole OpenAI stream and re-emit at the end. Pay
  particular attention to `input_json_delta` accumulation: OpenAI streams
  a tool call's arguments as fragments across multiple chunks, and these
  need to be correctly reassembled into Anthropic's incremental
  `input_json_delta` events (which themselves are also incremental, so
  this is fragment-reassembly on one side feeding incremental-re-emission
  on the other — get the exact semantics right, and if the mapping isn't
  1:1, ask rather than guess).
- **`thinking` blocks**: per spec, dropped when translating history to
  OpenAI (OpenAI has no equivalent concept). Document this as a known
  fidelity gap — `docs/decisions.md` should note it (spec's own risk table
  already anticipates this: "Fallback model inherits Claude-shaped context
  ... `--continue` across the boundary is best-effort").
- **Do not build routing, header hygiene, or HTTP wiring here.** This
  module's public interface should be pure functions/types: given an
  Anthropic-format request, produce an OpenAI-format request; given an
  OpenAI-format response (or a stream of OpenAI-format SSE chunks), produce
  an Anthropic-format response (or a stream of Anthropic-format SSE
  events). Task 3 wires this into the actual proxy request path.

**Tests (heavy — this is where the milestone's real risk lives, per Global
Constraint 9):**
- Golden-file-style round-trip tests: hand-built realistic fixtures (a
  multi-turn conversation with tool use, a system prompt as content
  blocks, an image) translated Anthropic→OpenAI, and separately
  OpenAI→Anthropic, asserting the exact expected shape at each step (not
  just "it doesn't panic").
- A property/round-trip test that tool-call argument JSON survives intact
  end to end (spec §10 item 4 names this explicitly) — a tool call with
  nested JSON arguments translated to OpenAI's string-encoded
  `function.arguments` and back must be byte-identical or semantically
  identical (your call on the exact equality check, document which).
- Streaming synthesis tests: feed a sequence of synthetic
  `chat.completion.chunk` SSE events (including a tool call whose
  arguments arrive fragmented across 3+ chunks) and assert the resulting
  Anthropic SSE event sequence matches spec's documented event types in
  the right order, with `input_json_delta` fragments reassembling to the
  correct final JSON.
- Explicitly test that streaming synthesis doesn't buffer the whole
  response — assert on incremental arrival (mirror Milestone 1's
  time-to-first-chunk test pattern) using a mock OpenAI-format upstream
  that drips chunks with delays.
- `thinking`-block dropping: assert it's dropped cleanly (no panic, no
  malformed output) rather than causing a translation error.

## Task 3: Failover policy + header hygiene + wiring it all together

Depends on Task 1 (router decision, config schema) and Task 2 (the
translator, for `format = "openai"` profiles). This is the integration
task — it pulls Milestone 1's proxy, Milestone 2's state machine, Task 1's
router, and Task 2's translator into one coherent request-handling flow.

- **Header hygiene** (spec §7b, Global Constraint 1 — security-critical):
  for any request routed to a fallback profile, strip ALL client auth
  headers (`Authorization`, `x-api-key`, every `anthropic-*` header
  including `anthropic-beta`) and inject `Authorization: Bearer
  <profile's key>` (or whatever scheme the profile's `format` implies —
  OpenAI-format typically uses Bearer auth, confirm and document) read
  from the env var `api_key_env` names. This is an **allowlist** rebuild
  of the outgoing request (contrast with the Anthropic route's existing
  denylist-based `forwardable()`), matching spec §7b's explicit framing:
  "fallback client constructed with an allowlist of headers, not a
  denylist" (also named in spec §12's risk table). Strip `cache_control`
  blocks from request bodies sent to the fallback (spec §7b).
- **Model remap** (spec §7a): for a `claude-*` request being routed to the
  active profile (i.e. it's in `LIMITED` state and policy says fail over —
  see below), apply the profile's `model_map` (prefix match, `"*"`
  catch-all) before sending. For a non-`claude-*` request routed by name
  (§7d), the name passes through unchanged — no remap.
- **Failover policy** (spec §6), applied only while the Anthropic route is
  `LIMITED` (Milestone 2's state machine) and only to `claude-*` requests
  (non-`claude-*` requests always route by name per §7d, regardless of
  Anthropic's state):
  - `new-sessions` (default): a **session-start heuristic** — a
    `/v1/messages` request whose `messages` array contains no
    `assistant`-role entries — routes to the fallback. Everything else
    (mid-conversation requests) gets the original Anthropic limit error
    passed through, so an in-flight session fails visibly rather than
    silently switching models mid-thought. Document the heuristic's known
    imperfection (Claude Code's internal title-generation/summarization
    requests also look like session-starts and will harmlessly land on
    the fallback) rather than over-engineering around it, per spec §6.
  - `all`: everything routes to the fallback while `LIMITED`.
  - `notify-only`: pure passthrough to Anthropic even while `LIMITED`;
    only the (Milestone 2) notification fires.
  - **Mid-stream failover prohibited** (Global Constraint 6): the
    eligibility decision above happens *before* any bytes are sent to the
    client. Once the response has started streaming, an upstream failure
    on that stream is a terminal error event, never a retry.
  - **`count_tokens` always goes to Anthropic** (Global Constraint 7),
    unconditionally.
- **Wiring**: in the existing `forward()` request path (`src/proxy.rs`),
  before the current "always go to Anthropic" logic, insert: (1) Task 1's
  name-based routing decision; (2) if the decision is "Anthropic" and the
  route state is `Limited`, apply the failover-policy check above to
  decide Anthropic-vs-fallback; (3) if the decision is a fallback profile
  (either from §7d name-based routing or from failover), branch on that
  profile's `format` — `"anthropic"` means passthrough + model remap +
  header hygiene only (no translator involved, spec §7c Phase 1 code path
  — untested against a real such provider since none is configured, but
  the code path must exist and be tested against a mock), `"openai"` means
  the same plus Task 2's translator in both directions.
- **Observability** (spec §9, the fallback-relevant parts): a response
  header `x-relay-route: fallback` on responses served by a fallback
  profile (the Anthropic route's responses get no such header — or
  `x-relay-route: anthropic` if that reads cleaner; your call, document
  it). Structured log line additions: which route/profile was chosen.
  (`/control/*` and `fallback_requests_served` in `/status` are Task 4.)

**Tests:** end-to-end through the real HTTP path (mirroring Milestone
1/2's established mock-upstream pattern, now with a second mock upstream
speaking OpenAI format for the translator path, and a third speaking
Anthropic format for the `format = "anthropic"` code path): a `claude-*`
request while `ACTIVE` never touches a fallback profile; while `LIMITED`
with `new-sessions` policy, a session-start request routes to the fallback
profile (assert the mock fallback received a properly remapped model name,
properly hygiene'd headers — no client auth reached it — and, for an
`openai`-format profile, a properly translated request) while a
mid-conversation request gets the passed-through Anthropic error instead;
`all` mode routes everything; `notify-only` never routes to fallback; a
non-`claude-*` model name routes by `serves` regardless of Anthropic's
state (test this specifically while Anthropic is `ACTIVE`, proving it's
unconditional); `count_tokens` never routes to fallback even while
`LIMITED` in `all` mode; a mid-stream upstream failure on the fallback
route terminates the client stream with an error rather than retrying
anywhere. A dedicated header-hygiene test asserting the client's real
`Authorization` header value never reaches the mock fallback server
(mirror Milestone 1's `secret_header_values_never_reach_the_logs` test
pattern, applied to the outgoing fallback request instead of a log).

## Task 4: Control API (`GET /control/profiles`, `POST /control/profile`)

Depends on Task 1 (profile config) and Task 3 (the active-profile concept
being consulted in real request routing).

- `GET /control/profiles`: list configured profile names + their
  `model_map`s, marking which is active.
- `POST /control/profile {"name": "..."}`: switch the active profile.
  Applies to **new requests only** — an in-flight streamed request
  finishes on whatever profile it started with (this needs the active
  profile to be read once per request at routing time, not re-consulted
  mid-stream; confirm Task 3's wiring already does this naturally, given
  streams are already handled as one continuous response). 404 on an
  unknown name. Fires Milestone 2's notifier with a `profile_switched`
  event (spec §8b) — extend the notifier's event enum (`src/notify.rs`)
  with this new variant; per Milestone 2's own design note, the event enum
  was built to make adding a variant additive, so this should not require
  restructuring existing code.
- Ephemeral by design (spec §8b): a runtime switch does not write back to
  `relay.toml`; a restart returns to `policy.active_profile`. No new
  config field for this — it's in-memory state only, analogous to how
  Milestone 2's route state machine holds `RouteState` in memory (though
  unlike route state, this is **not** persisted to `state_file` — a
  restart deliberately forgets a runtime profile switch, per spec).
- Extend `GET /status` (existing endpoint) with `active_profile` (spec
  §8b's stated extension) and, if not already covered, confirm
  `fallback_requests_served` (already stubbed at `0` since Milestone 1) is
  now genuinely incremented by fallback-routed requests.
- Loopback-only enforcement (spec §8b): since `relay`'s only supported bind
  today is loopback (no code path binds elsewhere), this is largely
  already true — but per spec, this must be **code-enforced**, not just
  true by convention: if `listen` is ever configured non-loopback, the
  `/control/*` routes must refuse to serve (or require a token — simplest
  correct behavior for this milestone is likely "disable `/control/*`
  entirely on a non-loopback bind," since token-based auth is out of this
  milestone's scope; confirm this reading is reasonable or ask).

**Tests:** `GET /control/profiles` lists configured profiles correctly;
`POST /control/profile` switches the active profile and a subsequent
request routes accordingly; 404 on an unknown profile name; an in-flight
stream started on profile A completes on profile A even if profile B is
switched to mid-stream (this is the one genuinely tricky concurrency test
— construct it deliberately, mirroring Milestone 1/2's pattern of proving
timing properties with real concurrent requests, not just asserting the
final state); a runtime switch does not persist across a simulated
restart (rebuild `AppState` fresh, confirm it reads `policy.active_profile`
again, not the switched value); `/status` reports `active_profile`; the
notifier receives a `profile_switched` event on switch (mirror Milestone
2's notifier test pattern); loopback-enforcement behavior for `/control/*`
under a hypothetical non-loopback `listen` (test the enforcement logic
directly even though nothing in this milestone actually binds
non-loopback).

## After Task 4: the credential gate

Once all 4 tasks are implemented, reviewed, and merged, this plan's
SDD-executable scope is complete. What's left needs the Together AI API
key and is **not** part of this plan's task list:

1. Get a real Together AI API key configured (`RELAY_TOGETHER_KEY` env var
   per spec's example, or whatever `api_key_env` names in the real
   config).
2. Run `--capture-errors` or ad-hoc real requests against Together's real
   `/v1/chat/completions` endpoint to capture real request/response/SSE
   traffic, and compare against this milestone's synthetic fixtures —
   correct the translator wherever real traffic disagrees with the
   documented-format assumptions the fixtures were built from.
3. Run spec §10 item 5's E2E drill: with Anthropic genuinely limited (or
   simulated via a mock that returns Milestone 2's limit signature), run
   an actual Claude Code task to completion on the real Together fallback
   path — specifically a tool-heavy session, since tool-use fidelity is
   the named highest risk.
