# Design: Claude Code Limit-Failover Proxy

Working name: `relay` (placeholder — rename freely).

## 1. Purpose

A local HTTP proxy that sits between Claude Code and its upstream API. Under normal
operation it is a transparent passthrough to Anthropic, preserving subscription (OAuth)
billing. When it detects that the subscription usage limit has been hit, it routes
eligible requests to a configured fallback provider (e.g. Together AI serving an open
model), translating model names — and, if required, wire format — in flight. Failover is
**loud** (user notification + status endpoint), **policy-driven** (new sessions only, by
default), and **reversible** (automatic recovery when the limit window resets).

Claude Code is configured once with `ANTHROPIC_BASE_URL=http://127.0.0.1:<port>` (user
`settings.json` env block, so all surfaces — terminal, agent view background sessions,
desktop-launched sessions — go through it). No other client-side change.

## 2. Non-goals

- Not a general multi-provider gateway (LiteLLM already exists; this is deliberately a
  single-purpose, single-user tool).
- No billing/spend tracking beyond counting failover requests.
- No TLS termination or multi-user auth. Binds loopback by default; LAN exposure is the
  operator's problem (front with a reverse proxy + token if desired).
- No request/response persistence. Bodies are never written to disk.
- v1 does not fail over on generic 5xx/overload errors — Claude Code's own
  `fallbackModel` chains already cover those. This tool handles exactly one condition:
  subscription/rate limit exhaustion, which the built-in chains do NOT cover.

## 3. Architecture

```
Claude Code (all surfaces)
        │  Anthropic Messages API, streaming SSE
        ▼
┌──────────────────────────────────────────────┐
│ relay (single binary)                        │
│                                              │
│  Ingress ── Router ──┬── Anthropic route     │──▶ api.anthropic.com
│     │        │       │   (verbatim passthru) │
│     │        │       └── Fallback route      │──▶ provider endpoint
│     │        │           (model remap +      │
│     │        │            optional translate)│
│     │        ▼                               │
│     │   Limit Detector ── State Store        │
│     │                        │               │
│     │                    Notifier (exec hook)│
│     └── /status, /healthz                    │
└──────────────────────────────────────────────┘
```

### Components

1. **Ingress** — HTTP server exposing the Anthropic API surface Claude Code uses:
   `POST /v1/messages` (streaming and non-streaming), `POST /v1/messages/count_tokens`,
   plus a catch-all that forwards any other `/v1/*` path to Anthropic verbatim.
2. **Router** — consults route state and failover policy per request; picks Anthropic or
   fallback.
3. **Limit Detector** — inspects Anthropic responses for the subscription-limit
   condition; extracts the reset timestamp; updates state.
4. **State Store** — in-memory route state with optional small JSON file for persistence
   across restarts (so a restart mid-limit doesn't hammer Anthropic).
5. **Translator** — request/response/SSE translation for the fallback route. Only
   compiled into the request path when the fallback endpoint is not
   Anthropic-compatible (see §7).
6. **Notifier** — executes a user-configured command on state transitions. Platform
   agnostic: the command is the integration point (`notify-send`, `osascript`, `ntfy`,
   whatever the implementor's environment uses).
7. **Status** — `GET /status` returns JSON: current state, reset time, counts of
   failover requests, active config digest.

### Language / stack recommendation

Rust: `tokio` + `axum` (or bare `hyper`) for ingress, `reqwest` (streaming enabled) for
upstreams, `serde_json` for translation, `toml` for config, `tracing` for logs. Ship as
one static binary; config path via `--config` flag or `RELAY_CONFIG` env. Packaging
(Nix flake, service manager unit) is the implementor's concern and must not be assumed
by the binary — no hardcoded paths, no daemonization logic.

## 4. Route state machine

Per upstream route (only Anthropic carries state in v1):

```
ACTIVE ──(limit response detected)──▶ LIMITED{until: reset_at + jitter}
LIMITED ──(now >= until)──▶ PROBING
PROBING ──(next real request succeeds)──▶ ACTIVE
PROBING ──(limit response again)──▶ LIMITED{new until}
```

- `LIMITED`: requests are subject to the failover policy (§6). The proxy does not
  forward eligible-for-failover requests to Anthropic at all during this window — no
  probing on the user's dime beyond the single transition request.
- `PROBING` is passive: the first request after `until` goes to Anthropic; there is no
  background health-check traffic.
- Jitter: add 15–60s random slack past the reported reset to avoid racing the window
  boundary.
- Persist `{state, until}` to the state file on transition; load on start; treat a
  stale `until` in the past as `ACTIVE`.

### State transitions fire the Notifier

Events: `failover_engaged` (with `reset_at`), `recovered`, `fallback_error`. The
notifier command receives the event via env vars (`RELAY_EVENT`, `RELAY_RESET_AT`,
`RELAY_DETAIL`). Fire-and-forget with a short timeout; notifier failure never affects
request handling.

## 5. Limit detection

**Do not hardcode the detection signature from documentation or this design.** The
exact error shape for subscription exhaustion (vs. ordinary per-minute 429s, vs.
overload 529s) must be captured empirically as the first implementation task:

1. Milestone 1's passthrough mode includes a `--capture-errors <dir>` debug flag that
   dumps status + headers + body of any non-2xx Anthropic response (with auth headers
   redacted) to disk.
2. Run it, hit the limit for real once, and turn the captured response into a fixture.
3. Detection then matches on that fixture's stable fields.

Expected shape (to be verified against the fixture, not trusted from here): HTTP 429,
JSON body with `error.type == "rate_limit_error"` and a message distinguishing the
subscription usage limit; reset time available from the body and/or response headers
(`retry-after`, `anthropic-ratelimit-*`). Detection rules live in the config file as
data (status + JSONPath-ish field matchers + reset extraction), not in code, so a
server-side wording change is a config edit, not a rebuild.

Classify conservatively: anything that doesn't match the subscription-limit signature
passes through to the client unchanged. A per-minute burst 429 with `retry-after: 12`
must NOT flip the route to LIMITED — require either an explicit subscription marker in
the body or a reset horizon above a configurable threshold (e.g. > 5 minutes).

## 6. Failover policy

Config `mode`, applied while the Anthropic route is `LIMITED` **and to the request whose
own response puts it there** (see "The triggering request" below):

| mode | Behavior |
|---|---|
| `new-sessions` (default) | Requests classified as session-starts route to fallback. Mid-conversation requests get the original limit error passed through, so in-flight Claude sessions fail visibly rather than silently switching models mid-thought. |
| `all` | Everything routes to fallback. |
| `notify-only` | Pure passthrough; only the notification fires. |

**Session-start heuristic** (for `new-sessions`): a `/v1/messages` request whose
`messages` array contains no `assistant`-role entries. This is imperfect — Claude Code
issues small internal requests (title generation, summarization) that also look like
session starts, and those harmlessly hit the fallback. Document the imperfection;
don't over-engineer. If false positives matter later, refine with request metadata,
but ship the simple heuristic first.

**The triggering request** (`policy.failover_on_detect`, default `true`). A request whose
response is what classifies as the subscription limit is handed to the fallback too,
instead of being answered with that limit error. It is subject to every rule above
without exception — the same `mode`, the same session-start heuristic, the same
requirement that an `active_profile` exist, the same `count_tokens` pin — because it is
the same decision, evaluated once per request.

The default changed from the original design, which applied `mode` only while the route
was already `LIMITED`, leaving the request that *caused* the transition out by
construction. Claude Code treats a subscription-limit 429 as terminal: it does not retry.
So passing that response through cost a hard, user-visible failure and a manual re-run
once per limit window, before the fallback this relay exists for engaged at all. Found by
running a real `claude -p` session, not by a test. `failover_on_detect = false` restores
the older behavior exactly: the limit error reaches the client, the route still
transitions, and only later requests fail over.

This does not weaken §5. Classification is unchanged and still conservative — anything
that does not match the signature (a burst 429 with `retry-after: 12`) reaches the client
with its status, headers and body intact, and changes no state. What changed is only
*when* the classification happens: for a response carrying `detect.status`, the body is
read whole before the response head is handed to the client, so the verdict is available
while the response can still be replaced. Nothing else is buffered: a 2xx, streamed or
not, is untouched (`detect.status` is validated to be a 4xx or 5xx), the read is bounded,
and a read that is interrupted — past the cap, or a failed stream — classifies nothing and
delivers the response exactly as a streamed pass-through would have.

Failing over mid-stream is prohibited: once any SSE bytes have been sent to the
client, an upstream failure terminates that stream with an error event. Only requests
that have not yet produced client-visible bytes are retried on the fallback (and only
when policy allows). The triggering-request re-route above is not in tension with this,
though it looks adjacent to it: the decision is made while the whole response — head
included — is still in the relay's hands, so nothing client-visible has been sent.

One request that fails over this way emits **two** §9 log lines: the Anthropic attempt
that produced the limit response, and the fallback request that answered the client.

`count_tokens` requests always go to Anthropic regardless of state; on failure, pass
the error through. (Token counts against the wrong tokenizer are worse than an error.)

## 7. Fallback route

### 7a. Model mapping

Each profile (§8b) carries its own map, matched by prefix against the incoming
`model` field:

```toml
model_map = { "claude-opus" = "deepseek-ai/DeepSeek-V4", "claude-sonnet" = "deepseek-ai/DeepSeek-V4-Flash", "claude-haiku" = "Qwen/Qwen3.6-27B", "*" = "deepseek-ai/DeepSeek-V4-Flash" }
# model IDs illustrative — verify against the provider catalog
```

### 7b. Auth and header hygiene (security-critical)

- Anthropic route: forward the client's `Authorization` / `x-api-key` and
  `anthropic-*` headers **verbatim**. The OAuth token is what preserves subscription
  billing; Claude Code manages its own refresh. Never log these headers.
- Fallback route: **strip all client auth and `anthropic-beta` headers** and inject
  the fallback provider's key from config/env. The Anthropic OAuth token must never
  leave the machine toward a third party. Treat this as a tested invariant, not a
  convention.
- Strip `cache_control` blocks from request bodies sent to the fallback (provider
  either rejects or ignores them; stripping makes cost behavior predictable).

### 7c. Wire format — phased

**Phase 1 (target): Anthropic-compatible fallback endpoint.** Several providers expose
an Anthropic Messages-format route. If the chosen provider does, the fallback path is
passthrough + model remap + header hygiene only. No translation layer. Confirm the
provider's compat route handles: streaming SSE, tool use, system prompts as content
arrays, images. Tool-use fidelity is the make-or-break — test with a real Claude Code
session, not curl.

**Phase 2 (only if needed): OpenAI-format translation.** If the provider is
OpenAI-format only, implement a translator module:

| Anthropic | OpenAI |
|---|---|
| top-level `system` (string or blocks) | `messages[0]` with `role: "system"` (blocks flattened to text) |
| `content: [{type: text}]` | string content |
| `tool_use` block | `assistant.tool_calls[]` with `function.arguments` as JSON string |
| `tool_result` block | `role: "tool"` message with `tool_call_id` |
| `tools[].input_schema` | `tools[].function.parameters` |
| image block (base64) | `image_url` with data URI |
| `stop_reason: end_turn / tool_use / max_tokens` | `finish_reason: stop / tool_calls / length` |
| `thinking` blocks in history | drop (see risks) |
| `thinking` block in a *response* | `message.reasoning` / `message.reasoning_content` (see below) |
| SSE `message_start / content_block_start / content_block_delta (text_delta, input_json_delta, thinking_delta) / content_block_stop / message_delta / message_stop` | synthesized from `chat.completion.chunk` deltas; `input_json_delta` accumulates from streamed `tool_calls[].function.arguments` fragments, `thinking_delta` from `delta.reasoning` / `delta.reasoning_content` |

**Reasoning, in the response direction** (`policy.surface_fallback_reasoning`, default
`true`). A reasoning model returns its reasoning alongside its answer, under a field name
OpenAI never specified and providers did not converge on: `reasoning` on most of Together
AI's reasoning models, `reasoning_content` on `moonshotai/Kimi-K3`. Both are read, and
whichever is non-empty becomes a `thinking` block ahead of the turn's `text` block — the
order the model produced them. An absent or empty field produces no block at all.

The block carries **no `signature`**. Anthropic signs its own thinking blocks and the
relay cannot, so there is no value to put there that would not be a forgery. That makes
the round trip the open question rather than the response: Claude Code normalizes the
block into its transcript with `"signature": ""`, translating history back to OpenAI
drops `thinking` blocks (so the fallback path is unaffected), but §7's Anthropic route is
a verbatim forward — a session that failed over and later recovers hands Anthropic a
block with an empty signature. Whether Anthropic rejects that is **unverified**; the
config key is the mitigation, and `false` restores the older behavior, where the
reasoning was discarded and the operator paid for tokens they never saw.

The SSE synthesis is the hairiest code in the project. Golden-file tests against
recorded real traffic in both directions are mandatory before trusting it. If Phase 2
looks likely from the start, consider whether running LiteLLM as the translation
sidecar (relay handles only detection/policy/routing and forwards fallback traffic to
LiteLLM) beats reimplementing it — legitimate design fork, implementor's call.

### 7d. Name-based routing (always-on, independent of limit state)

The router's first rule is the request's `model` field, evaluated before limit state:

1. `claude-*` → Anthropic route, subject to the failover state machine and policy
   exactly as designed.
2. Any other model name → the profile that claims it, regardless of Anthropic's
   state. This is ordinary routing, not failover: no notification, no state change,
   no model remap (the name is passed through as-is).

Profiles claim names via a `serves` prefix list (see §8). Resolution: first profile
whose `serves` entry prefix-matches wins, in config order; a non-`claude-*` name no
profile claims falls through to the active profile. A name the active profile's
endpoint rejects surfaces as that provider's error — the proxy does not validate
model names.

**A provider's error surfaces in Anthropic's envelope, not the provider's own shape.**
This rule replaces the original "passed through verbatim, untranslated". That rule was
written when no provider's error shapes had been captured, and translating from a guess
would have been worse; the shapes are captured now
(`tests/fixtures/together/{F,H,I,J}*`), and verbatim pass-through turned out to cost the
user a whole session in the one case that matters most. **Claude Code detects a
context-overflow by lowercased substring match on the error message** — `prompt is too
long`, `input is too long for requested model`, or ``input length and `max_tokens`
exceed context limit`` — and extracts the two token counts with
`prompt is too long[^0-9]*(\d+)\s*tokens?\s*>\s*(\d+)`. A provider that words the same
failure differently (Together AI says "The input (170071 tokens) is longer than the
model's context length (131072 tokens).") matches none of them, so none of the client's
compact-and-retry fires and the session is unrecoverable in place.

So the relay emits `{"type":"error","error":{"type":…,"message":…}}`, and:

- **The provider's status is preserved.** The captures show 400, 401, 404 and 422 all
  occur; normalising them would report a different failure than the one that happened.
  `x-relay-route: fallback` is on every one, §9's claim unchanged.
- **The provider's own message is preserved**, from whichever shape carried it:
  `error.message` first, then a top-level `message` (vLLM and several
  OpenAI-compatible servers put it there), then a bounded snippet of the body itself —
  which is what keeps a `{"detail": …}` body or a `text/plain` 413 from arriving as a
  sentence that says nothing. Only a body with nothing in it produces the relay's own
  "no message" wording. Reading `error.message` alone would be a regression against the
  verbatim pass-through this replaced, where a flat body still reached the client's SDK
  and the user saw the real reason.
- **The `error.type` is mapped onto Anthropic's name for the status**
  (`invalid_request_error`, `authentication_error`, `rate_limit_error`, …). The status
  decides wherever Anthropic documents a type for it, because a provider's type string is
  not reliable — Together answers a 401 with `invalid_request_error`. Anything
  unrecognised becomes Anthropic's generic `api_error` rather than an invented name.
- **For a context-limit error, the message leads with Anthropic's wording and the pair**
  — `prompt is too long: 170071 tokens > 131072` — with the provider's own sentence after
  it, which is the only thing that reported the real limit. This is the recovery that
  matters, because Claude Code sends `max_tokens: 64000` — on a 131k model most of the
  overflow is the output reservation, not the transcript, so shrinking `max_tokens` alone
  fixes it with no compaction.
- **The pair is read from the provider's message, and the parser refuses rather than
  guesses.** "Never invented" is not a strong enough claim to make about numbers taken
  out of arbitrary text: digits that came from the provider can still be the *wrong*
  digits. So the parse is anchored to the wording that matched — the last token count
  before it and the first number after it — and it yields nothing unless the leading
  number is a count of tokens, is a whole number rather than one group of a
  separated one, and exceeds the trailing number. Those checks are each there for a
  measured failure: unanchored, a `2026-08-12T…` prefix an intermediary added in front of
  Together's own sentence reports a context limit of **8 tokens**; and a thousands
  separator splits `170,071` into `170` and `071`, which on one real-shaped wording
  yields `(71, 2)` — both of which drive the client's `max_tokens` toward zero without
  ever converging. When no pair survives the checks the phrase goes *last* in the message
  instead, so no digit the provider sent sits where the extraction regex could read it as
  the pair. A wrong pair is worse than no pair.
- **Not every 4xx is a context-limit error.** Detection needs a plausible status (400 or
  413), matching wording, **and a message the provider itself authored** — the
  `error.message` or top-level `message` field, never the raw body. That last condition is
  not fussiness: a pydantic-shaped 400 echoes the rejected request back inside the body,
  so reading the body let a *malformed* request whose own transcript mentioned a context
  length become a too-long claim built from the user's own numbers, and the client would
  shrink, retry the identical malformed request, and loop. Neither the status nor the
  wording can catch that, because both are genuine. The cost is taken knowingly: a
  `text/plain` body carrying a real context-limit sentence is surfaced to the user but not
  detected. A false negative costs what happened before any of this existed; a false
  positive is a loop that never ends. Only Together's wording is measured — the other
  patterns matched are unverified guesses at other providers.
- **The provider's raw body is logged**, capped and with the profile's own key redacted,
  since the envelope necessarily reshapes what was sent.

This makes deliberate mixed-backend use a client-side choice: `/model` (or
`--model`, or `CLAUDE_CODE_SUBAGENT_MODEL`, or agent-view dispatch pickers) selects
an open model by name and the proxy routes it, while `claude-*` selections continue
on subscription. The failover machinery only ever concerns `claude-*` traffic.

Client-side picker exposure is configuration outside this tool's scope (the
`ANTHROPIC_CUSTOM_MODEL_OPTION` env vars for a single additive picker entry, or an
`availableModels` list, or typing names directly — with a custom base URL, Claude
Code skips model-name validation). Document the recommended combo in the README, but
the proxy itself needs no knowledge of it.

## 8. Configuration

Single TOML file. Everything hot-reloadable except the listen address (SIGHUP or file
watch — implementor's choice; restart-to-reload is acceptable for v1).

```toml
listen = "127.0.0.1:8484"
state_file = "~/.local/state/relay/state.json"   # optional

[anthropic]
base_url = "https://api.anthropic.com"

# Fallback providers are defined as named profiles (§8b). Exactly one is active.
[profiles.deepseek]
base_url = "https://<provider-anthropic-compat-endpoint>"
api_key_env = "RELAY_TOGETHER_KEY"     # read from env, never stored in the file
format = "anthropic"                   # or "openai" (enables translator)
serves = ["deepseek-ai/", "Qwen/"]     # §7d: model-name prefixes this profile claims
model_map = { "claude-opus" = "deepseek-ai/DeepSeek-V4", "*" = "deepseek-ai/DeepSeek-V4-Flash" }

[profiles.kimi]
base_url = "https://<other-endpoint>"
api_key_env = "RELAY_MOONSHOT_KEY"
format = "anthropic"
serves = ["moonshotai/"]
model_map = { "*" = "moonshotai/Kimi-K3" }

[policy]
mode = "new-sessions"                  # new-sessions | all | notify-only
active_profile = "deepseek"            # startup default; runtime switches via /control
failover_on_detect = true              # §6: the request that trips the limit fails over too
surface_fallback_reasoning = true      # §7c: a provider's reasoning becomes a thinking block
min_reset_horizon_secs = 300           # below this, a 429 is a burst, not the limit
reset_jitter_secs = [15, 60]

[detect]
# populated from captured fixtures — status, body matchers, reset extraction

[notify]
command = "/path/to/notify-hook"       # receives RELAY_EVENT etc. in env
timeout_secs = 5
```

## 8b. Profiles and runtime control

Fallback providers/models will change regularly (open-model leaderboard churn), so
switching must not require editing config or restarting.

**Profiles** are named, fully-specified fallback targets (endpoint, key env, format,
model map) as shown in §8. `policy.active_profile` selects the startup default. The
config file is the registry of vetted options; the control API selects among them.

**Control API** — loopback-only, same listener, no auth in v1 (it is bound to
127.0.0.1; if the listen address is ever non-loopback, the control routes must require
a token — enforce this in code, not documentation):

| Endpoint | Behavior |
|---|---|
| `GET /control/profiles` | List profile names + their model maps; mark active |
| `POST /control/profile` `{"name": "kimi"}` | Switch active profile. Applies to **new requests only**; in-flight streams finish on the profile they started with. 404 on unknown name. Fires notifier event `profile_switched` |
| `POST /control/mode` `{"mode": "all"}` | Override failover policy at runtime (same new-requests-only semantics) |
| `GET /status` | Extended to include `active_profile` |

Runtime switches are ephemeral by design: they do not write back to the config file,
and a restart returns to `policy.active_profile`. This keeps the file as the single
source of truth for what's *vetted* while allowing cheap experimentation. If a switch
should persist, the user edits the file — one line.

**CLI wrapper** (thin, optional, ~50 lines or a shell script): `relay use <profile>`,
`relay mode <mode>`, `relay status` — curl veneers over the control API. Ship as a
subcommand of the same binary (`relay ctl ...`) to avoid a second artifact.

Deliberate exclusion: no auto-selection of models from leaderboards or provider
catalogs. A model change alters tool-call behavior under the harness in ways
benchmarks don't capture; every switch is an explicit, instantly-revertible command.

## 9. Observability

- `GET /status` → `{state, limited_until, fallback_requests_served, config_digest}`.
- Structured logs (`tracing`): one line per request — route chosen, model in/out,
  status, latency, stream bytes. **Never bodies, never auth headers.** (One line per
  *upstream* request, strictly: a client request that fails over on detection, §6, logs
  the Anthropic attempt and the fallback request that replaced it.)
- A visible marker for fallback responses: inject a response header
  (`x-relay-route: fallback`) so a session can be audited after the fact. (Claude Code
  ignores unknown headers.)

## 10. Testing strategy

1. **Passthrough fidelity (Milestone 1 gate):** a full interactive Claude Code session
   through the proxy — tool calls, subagents, streaming, images — must be
   indistinguishable from direct connection. Diff-test non-streaming JSON responses
   byte-for-byte where possible.
2. **Fixture-driven detection tests:** captured real limit responses (redacted) as test
   inputs; assert state transitions, including the burst-429 negative case.
3. **Mock upstream chaos tests:** a fake Anthropic that serves canned 429s, mid-stream
   disconnects, and malformed SSE; assert policy behavior (no mid-stream failover,
   correct passthrough of non-matching errors).
4. **Translator golden files (Phase 2 only):** recorded request/response/SSE pairs in
   both formats; property test that tool_call argument JSON survives round-trip intact.
5. **End-to-end failover drill:** mock-limited Anthropic + real fallback provider; run
   an actual Claude Code task to completion on the fallback path.

## 11. Milestones

| # | Deliverable | Acceptance |
|---|---|---|
| 1 | Transparent passthrough proxy, streaming intact, `/status`, `--capture-errors` | Real session indistinguishable from direct; subscription billing confirmed intact |
| 2 | Limit detection + state machine + notifier + state persistence | Fixture tests pass; real limit event flips state and fires notification; burst 429 does not |
| 3 | Name-based routing (§7d) + failover to the fallback provider (profile schema §8b, model remap, header hygiene, `new-sessions` policy, `POST /control/profile`) + the §7c wire-format layer the chosen provider actually needs | E2E drill passes; auth-stripping invariant tested; explicit open-model requests route by name with Anthropic ACTIVE; profile switch applies to new requests only |
| 4 | Policy modes + `/control/mode`, `relay ctl` CLI wrapper, hot reload, jittered recovery, `x-relay-route` marker | Recovery observed across a real reset window; switch-and-revert of profiles verified under concurrent streams |
| 5 | (Only if a later profile needs a wire format Milestone 3 didn't already build) another §7c translator | Golden-file suite green; tool-heavy session completes on fallback |

## 12. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Limit-response shape changes server-side | Detection rules are config data; conservative default = passthrough (fail open to visible errors, never to silent misrouting) |
| Session heuristic misclassifies internal utility requests | Accepted; they land on the cheap fallback harmlessly. Revisit only if it causes real problems |
| Fallback model inherits Claude-shaped context (thinking blocks, Claude idioms) and degrades | `new-sessions` default avoids mid-conversation handoff; thinking blocks dropped in translation; document that `--continue` across the boundary is best-effort |
| OAuth token leaked to third party via bug | Header hygiene as tested invariant (§7b); fallback client constructed with an allowlist of headers, not a denylist |
| Provider Anthropic-compat endpoint has partial tool-use support | Phase-1 acceptance requires a real tool-heavy session, not a smoke test |
| Proxy becomes bottleneck under parallel subagents | Async streaming passthrough wherever the route allows it, and every buffer that remains is capped — but **per request, with no aggregate bound across concurrent ones**. With any profile configured, `/v1/messages` and `/v1/messages/count_tokens` buffer the whole request body (≤8 MiB) before routing, because the `model` that decides the route is inside it; a body past that cap is forwarded to Anthropic uninspected rather than held. The fallback route additionally buffers a non-streaming upstream response (≤4 MiB) to translate it, and SSE translation holds ≤4 MiB per unterminated frame and across tool-call slots. A zero-profile relay still streams frame-for-frame, request and response. Load-test with 10+ concurrent streams in Milestone 1 (`docs/decisions.md`, 2026-08-11 "Failover wiring", for why the routing cap is small and deliberately not a config key) |
| Control API exposed if listener ever bound beyond loopback | Code-enforced: non-loopback listen address disables `/control/*` unless a control token is configured (§8b) |
