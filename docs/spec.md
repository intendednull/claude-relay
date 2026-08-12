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

Config `mode`, applied only while the Anthropic route is `LIMITED`:

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

Failing over mid-stream is prohibited: once any SSE bytes have been sent to the
client, an upstream failure terminates that stream with an error event. Only requests
that have not yet produced client-visible bytes are retried on the fallback (and only
when policy allows).

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
| SSE `message_start / content_block_start / content_block_delta (text_delta, input_json_delta) / content_block_stop / message_delta / message_stop` | synthesized from `chat.completion.chunk` deltas; `input_json_delta` accumulates from streamed `tool_calls[].function.arguments` fragments |

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
  status, latency, stream bytes. **Never bodies, never auth headers.**
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
