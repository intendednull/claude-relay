# claude-relay

A local HTTP proxy that sits between Claude Code and the Anthropic API.
Transparent passthrough under normal operation (preserving OAuth
subscription billing); on detecting subscription usage-limit exhaustion, it
fails over eligible requests to a configured fallback provider.

Full design: [`docs/spec.md`](docs/spec.md). Choices made since, which refine
or extend it: [`docs/decisions.md`](docs/decisions.md).

## Status

Pre-alpha. Milestones 1-3 are complete — see [`docs/plans/`](docs/plans/):

- **1** — transparent passthrough, `/status`, `--capture-errors`.
- **2** — limit detection, the route state machine, state persistence, the
  notifier.
- **3** — name-based routing, failover to a fallback profile (model remap,
  header hygiene), the Anthropic↔OpenAI translator including streaming, all
  three `[policy] mode` values, the `x-relay-route` response marker, and the
  control API.

Not built yet: hot config reload, `/control/mode`, a `relay ctl` CLI wrapper.

**Milestone 3 is not accepted.** Spec §10 item 5 — the end-to-end drill, a real
Claude Code task run to completion on the fallback path against a mock-limited
Anthropic — has not been run. What is below is verified by the test suite, and
the translator additionally against real Together AI traffic captured into
`tests/fixtures/together/`; none of it has yet been driven by a live Claude Code
session.

## Running it

```
cargo run -- --config relay.example.toml
```

Point Claude Code at it by setting `ANTHROPIC_BASE_URL` in the `env` block of
your Claude Code `settings.json`:

```json
{
  "env": {
    "ANTHROPIC_BASE_URL": "http://127.0.0.1:8484"
  }
}
```

**Every path, not just `/control`, refuses a cross-origin browser request**
(`Sec-Fetch-Site`/`Origin`, when the browser attaches them — a page cannot
forge either from script). Claude Code is not a browser and sends neither, so
this costs a real client nothing; `curl` and any other non-browser client are
unaffected too. It is here because `/v1/messages` picks its route from the
request *body* whatever the content type, and `text/plain` needs no CORS
preflight — so without it, a web page you happened to have open could make the
relay spend a fallback profile's API key. A refused request gets `403
{"error": "cross_origin_request_refused"}`, or a bare 404 under `/control`.

Byte-for-byte fidelity — a real Claude Code session (tool calls, subagents,
streaming, images) being indistinguishable from a direct connection to
Anthropic (spec §10 item 1) — is verified by hand with a live session, not by
the automated test suite: that check needs live Anthropic credentials this
repo doesn't have. Two things to confirm during that session, beyond the
session simply working:

- Subscription billing is still attributed to the OAuth subscription, not
  billed as API usage.
- Claude Code tolerates request bodies framed with `Transfer-Encoding:
  chunked` instead of `Content-Length`. Dropping `Content-Length` is a
  deliberate consequence of the hop-by-hop header denylist, and the Anthropic
  API answered both framings identically when checked by hand, but it is a
  real wire-level difference from a direct connection.

## Fallback providers

A **profile** is one third-party provider. Requests reach it two independent
ways:

- **By name** (spec §7d) — the client asked for a model that is not a
  `claude-*` one. This happens whatever Anthropic's state, notifies nothing and
  changes no state. It is ordinary routing, not failover.
- **By failover** (spec §6) — a `claude-*` request arrives while the route is
  `LIMITED` and `[policy] mode` allows it. Only this path is failover, and only
  this path remaps the model.

A minimal working config — against Together AI's OpenAI-compatible endpoint, the
provider this project has real captured traffic for — saved as `relay.toml`:

```toml
listen = "127.0.0.1:8484"

[anthropic]
base_url = "https://api.anthropic.com"

[profiles.together]
base_url = "https://api.together.xyz"   # the API *root*, not an endpoint path
api_key_env = "RELAY_TOGETHER_KEY"      # the variable's name; the key itself never goes in this file
format = "openai"                       # enables the wire-format translator
serves = ["meta-llama/", "deepseek-ai/", "Qwen/"]                # §7d: prefixes this profile claims
model_map = { "*" = "meta-llama/Llama-3.3-70B-Instruct-Turbo" }   # §7a: failover only

[policy]
mode = "new-sessions"
active_profile = "together"
```

```
export RELAY_TOGETHER_KEY=...
cargo run -- --config relay.toml
```

`base_url` must be `https` unless the host is loopback, and must not carry
userinfo (`https://user:secret@host`) — both are refused at startup rather than
leaked or silently dropped per request. `relay.example.toml` documents every
key, including the ones with defaults the config above leaves out. Read
**Choosing a fallback model** below before committing to a `model_map` target:
not every model in a provider's catalogue is reachable, and not every one can
serve a forced `tool_choice`.

### Name-based routing

Any `model` that does not start with `claude-` is resolved against the profiles:
the first profile with a `serves` prefix that matches wins, **in config order**;
a name no profile claims falls through to `policy.active_profile`; with neither
a match nor an active profile the relay answers `400 {"error":
"no_route_for_model"}` rather than sending an open-model name to Anthropic to be
rejected there. A name-routed request is passed through **unremapped** —
`model_map` is failover's, not this path's — so the name you type is the name the
provider sees, and a name it rejects surfaces as that provider's own error.

`/v1/messages/count_tokens` is the exception: it stays on Anthropic unless the
profile that claims the name is a `format = "anthropic"` one, since an `openai`
profile has no counting endpoint and routing there would turn a token count into
a billed inference call.

### Failover, and `[policy] mode`

`mode` decides which `claude-*` requests leave for `active_profile` while the
route is `LIMITED`:

| `mode` | Effect while `LIMITED` |
|---|---|
| `new-sessions` (default) | Only requests whose message list has no `assistant` turn yet — a conversation with no thought to switch models in the middle of. Claude Code's own title-generation and summarization requests look like session starts too and land on the fallback harmlessly. |
| `all` | Every eligible request, mid-conversation ones included. |
| `notify-only` | None. Anthropic's own limit error passes through to the client, and the notifier still fires. |

A failed-over request **is** remapped through `model_map` (spec §7a): longest
matching prefix wins, ties go to whichever was declared first, `"*"` is
consulted only if nothing else matched, and a name no entry claims is sent on
unchanged. `count_tokens` never fails over in any mode, and a stream already
being delivered is never failed over mid-response — the decision is made before
the first byte reaches the client.

Nothing the client sent reaches a profile: the outgoing request's headers are
*built*, not filtered, so no client credential can leak by being missing from a
denylist (spec §7b, and the invariant is a test). Anthropic's prompt-caching
`cache_control` directives are stripped on the way out, since a fallback
provider either rejects them or ignores them.

### Confirming it works, without waiting for a limit

Name-based routing needs no limit state, so it is the cheap end-to-end check
that a profile is configured correctly:

```
curl -sD- http://127.0.0.1:8484/v1/messages -H 'content-type: application/json' \
  -d '{"model":"meta-llama/Llama-3.3-70B-Instruct-Turbo","max_tokens":64,
       "messages":[{"role":"user","content":"say hi"}]}'
```

- The response carries **`x-relay-route: fallback`**. Only the relay ever sets
  that header (it is stripped from anything an upstream sends), and only the
  fallback route sets it — so its absence means the answer came from Anthropic.
- `curl http://127.0.0.1:8484/status` shows `fallback_requests_served`
  incremented, and `active_profile` naming the profile that served it.
- A relay-generated failure on this path is a `502` that names its cause and
  carries the marker too: `fallback_key_missing` or `fallback_key_unusable` (the
  variable `api_key_env` names is unset, or holds something unsendable),
  `upstream_unreachable`, `fallback_request_untranslatable`,
  `fallback_response_unreadable` (past the 4 MiB buffer cap, or the response
  stream failed), `fallback_response_untranslatable`. Anything else — the
  provider's own 4xx/5xx — is passed through untranslated, with its own status.

To exercise the *failover* path instead, a mock-limited Anthropic is the
supported route (see `tests/fallback.rs`); with `[policy] mode = "all"` and a
real limit in effect, `/status` reports `LIMITED` and every subsequent
`claude-*` request answers with the marker.

### Selecting an open model from Claude Code

Exposing the name in the client is Claude Code's configuration, not the relay's
(spec §7d) — the relay routes whatever `model` string arrives. Because
`ANTHROPIC_BASE_URL` points at a custom endpoint, Claude Code passes any model
string through without validating it, so all of these work:

- **Type it.** `/model meta-llama/Llama-3.3-70B-Instruct-Turbo`, `--model
  <name>`, or `ANTHROPIC_MODEL` in the same `env` block as
  `ANTHROPIC_BASE_URL`.
- **Add a `/model` picker entry** — the recommended combo, since it survives
  restarts and needs no typing: `ANTHROPIC_CUSTOM_MODEL_OPTION` (the model ID
  the relay will route on) plus `ANTHROPIC_CUSTOM_MODEL_OPTION_NAME` (what the
  picker shows). `..._DESCRIPTION` and `..._SUPPORTED_CAPABILITIES` are
  optional.
- **`availableModels`** in Claude Code's `settings.json` lists the selectable
  models, and `enforceAvailableModels` makes that list a restriction.
- **`CLAUDE_CODE_SUBAGENT_MODEL`** puts subagents on a model of their own —
  a cheap open model for subagents while the main thread stays on subscription
  is exactly the mixed-backend use §7d exists for.

```json
{
  "env": {
    "ANTHROPIC_BASE_URL": "http://127.0.0.1:8484",
    "ANTHROPIC_CUSTOM_MODEL_OPTION": "meta-llama/Llama-3.3-70B-Instruct-Turbo",
    "ANTHROPIC_CUSTOM_MODEL_OPTION_NAME": "Llama 3.3 70B (via relay)"
  }
}
```

Whatever you pick, the profile's `serves` list has to claim its prefix (or the
profile has to be `active_profile`), or the relay answers
`no_route_for_model`. These variable names are Claude Code's own, taken from its
env-var and model-configuration docs; this project has not driven them through a
live session (see Status).

## Capturing error responses

```
cargo run -- --config relay.example.toml --capture-errors ./fixtures
```

Off unless the flag is passed. While on, every **non-2xx** Anthropic response
is written to `<dir>/<n>-<status>.json`; successful responses are never
captured, and neither are request bodies. It exists to collect real
rate-limit responses for Milestone 2's limit-detection rules, so it is meant
to be left on across restarts — fixtures accumulate rather than overwrite.

- `authorization`, `x-api-key`, `cookie` and `set-cookie` values are replaced
  with `[REDACTED]`. Everything else is kept verbatim, `retry-after` and
  `anthropic-ratelimit-*` included, since those are the point.
- `"truncated": true` means the body is partial — it hit the 1 MiB cap, the
  upstream died mid-body, or the client hung up. Absent means complete.
- A non-UTF-8 body lands in `body_base64` instead of `body`. Fixtures hold the
  exact bytes the upstream sent and are never decompressed — limit detection
  decompresses a copy of its own, not this one — so a gzip-encoded error
  response shows up that way rather than as readable JSON; decode it by hand
  (`base64 -d | zcat`).
- Fixtures are written 0600, into a directory the relay creates 0700 (a
  directory that already exists keeps its own permissions). They are still
  unredacted response bodies on disk — treat them as sensitive.

## Limit detection

Anthropic responses carrying the status named by the `[detect]` rule in the
config file — 429 by default; see `relay.example.toml`, which spells out every
built-in default — are classified against the rest of that rule, and they are
the only responses the relay buffers for classification, so a limit returned
under a different status code goes unnoticed until `detect.status` names it. A
match moves the route to `LIMITED` until the reported reset plus
`policy.reset_jitter_secs` of jitter (default 15–60s); the window elapsing
moves it to `PROBING`; the next successful response moves it back to
`ACTIVE`. `GET /status` reports the current state, `limited_until`, and
`active_profile` (spec §8b, see Control API below).

`min_reset_horizon_secs`, `max_reset_horizon_secs` and `reset_jitter_secs`
live under `[policy]`, not `[detect]` — see `relay.example.toml`.

- **What a match changes.** With no profile configured, or `mode =
  "notify-only"`, the client still receives the upstream's own response byte for
  byte whatever it classified as. Otherwise an eligible `claude-*` request goes
  to the fallback for as long as the window lasts (see **Fallback providers**).
- **Non-matches never move state.** A per-minute burst 429 needs either an
  explicit subscription marker in the message or a reset further out than
  `policy.min_reset_horizon_secs` (default 5 minutes) before it counts.
- **The window is bounded at both ends.** It is never shorter than
  `policy.min_reset_horizon_secs` nor longer than `policy.max_reset_horizon_secs`
  (default 7 days), so neither a stale reset time nor one reported in the
  wrong unit can produce a window that expires instantly or never elapses at
  all.
- **The default rule is a guess** from spec §5's expected shape, not from a
  real limit response (`docs/decisions.md`). Catch one with
  `--capture-errors` and re-derive the rule from the fixture; it is config,
  not code.
- Set `state_file` to keep the state across a restart, so a restart mid-limit
  doesn't go straight back to Anthropic.
- **A gzipped error body is classified normally.** Anthropic compresses error
  bodies whenever the client asks it to, and Claude Code's client always asks,
  so this is the ordinary case rather than an edge one. Only detection's own
  copy is decompressed (capped at 4 MiB of output, so a malicious upstream
  cannot expand a small body into unbounded memory); the client still receives
  the upstream's exact bytes, `content-encoding` included. Any other encoding —
  `br`, `zstd`, or a doubly-compressed body — logs a warning and passes through
  unclassified rather than being guessed at.

## Control API

`GET /control/profiles` and `POST /control/profile` (spec §8b) switch which
profile new requests fail over to, at runtime, without touching the config
file or restarting:

```
curl http://127.0.0.1:8484/control/profiles
curl -X POST http://127.0.0.1:8484/control/profile \
  -H 'content-type: application/json' -d '{"name":"deepseek"}'
```

`content-type: application/json` is required on the `POST` — see below for
why; a plain `curl -d` without `-H` defaults to a different content type and
gets rejected.

`GET /control/profiles` lists every configured profile's `name`, `format`,
`serves`, `model_map` and `api_key_env` (the env var *name*, never its
value), marking which one is active. `POST /control/profile` returns 404 on
a name nothing configured claims, and leaves the active profile untouched
when it does.

- **Ephemeral by design.** A switch lives only in the running process; a
  restart goes back to `policy.active_profile`. Edit the config file if you
  want a change to stick.
- **Applies to new requests only.** A request already in flight finishes on
  the profile it started with, even if a switch lands mid-response.
- **Loopback-only, code-enforced, on three separate axes.** Disabled
  outright if `listen` is ever non-loopback. Independently of that, every
  request must also carry a loopback (or `localhost`) `Host` header —
  otherwise DNS rebinding (an attacker's own domain resolving to
  `127.0.0.1`) could reach this from a browser tab despite the bind being
  loopback. And independently of *that*, a browser-originated request must
  look same-origin (`Sec-Fetch-Site`/`Origin`, when either is present) and
  `POST /control/profile` requires `content-type: application/json` — a page
  loaded directly from `http://127.0.0.1:<port>` has an honestly loopback
  `Host` with no rebinding involved, so a state-changing request from a
  cross-origin tab needs its own check. All three failing look like the
  route was never registered (404) — a plain wrong content type is the one
  exception, which is a `415` naming the problem, since by that point the
  request already passed the other checks.
- Fires the notifier's `profile_switched` event (below) — but only when the
  switch is a real change; switching to the profile that is already active,
  or a rejected switch, notifies nothing. **A rapid run of switches
  coalesces to the most recent one**, so a hook is not guaranteed to see
  every intermediate switch — `A → B → A` in quick succession may announce
  only the final `A`, not `B` in between. This is deliberate: it bounds how
  much a burst of switches (a script bug, or someone hammering the endpoint)
  can delay the next `failover_engaged`/`recovered`, which are never
  coalesced or dropped.

## Choosing a fallback model

Two things Milestone 3's real-traffic testing against Together AI surfaced
(`docs/decisions.md`), worth knowing before picking `model_map` targets:

- **Forced tool choice needs a model whose grammar backend can compile a
  real schema.** `Qwen/Qwen2.5-7B-Instruct-Turbo` 422s on any forced
  `tool_choice` with a non-empty parameter schema (Anthropic's `any`/`tool`
  modes); `meta-llama/Llama-3.3-70B-Instruct-Turbo` handles the identical
  request correctly. `auto`/`none` are unaffected either way.
- **A model priced in `/v1/models` is not necessarily reachable.**
  `meta-llama/Meta-Llama-3.1-8B-Instruct-Turbo` and
  `mistralai/Mistral-7B-Instruct-v0.3` both list a price but return `400
  Unable to access non-serverless model …` — verify with a real request
  before committing a `model_map` entry to one.

## Notifications

Set `notify.command` to be told when the route state changes, or a profile is
switched, instead of polling `/status`. The command runs through `sh -c` and
gets the event in its environment:

| Variable | Value |
|---|---|
| `RELAY_EVENT` | `failover_engaged` when the route becomes `LIMITED`, `recovered` when it returns to `ACTIVE`, `profile_switched` on a real `POST /control/profile` switch |
| `RELAY_RESET_AT` | RFC3339 end of the window on `failover_engaged`, the same value `/status` reports as `limited_until`; empty otherwise |
| `RELAY_DETAIL` | A one-line human-readable summary — for `profile_switched`, this is the only place the switched-to profile's name appears; there is no separate `RELAY_PROFILE` variable |

```toml
[notify]
command = "notify-send 'claude-relay' \"$RELAY_DETAIL\""
timeout_secs = 5
```

Every variable is always set, empty rather than absent where an event has
nothing to say, so a hook can run under `set -u`.

- **Nothing waits on it.** The command is spawned on a thread of its own; a
  slow or hanging hook delays neither the proxied response nor the tracking of
  any later state change. One that has not exited within `timeout_secs` is
  killed, and every failure — a command that will not start, a non-zero exit,
  a timeout — is a log warning and nothing more.
- **It is not a re-limit alarm.** A limit detected while the route is already
  `LIMITED` does not extend the window, so it is not a state change and does
  not notify. Nor does the window merely elapsing (`LIMITED` → `PROBING`),
  which means nothing until a request actually succeeds.
- It inherits the relay's environment, which a desktop notifier needs
  (`DISPLAY`, `DBUS_SESSION_BUS_ADDRESS`), and writes to the relay's own
  stdout/stderr.
- **That environment includes every configured profile's API key value** —
  the relay does not filter its own environment before spawning the hook. A
  hook that itself logs or dumps its environment (`env > debug.log` and
  similar) will write those keys to wherever it sends them. This was already
  true before the control API existed; `POST /control/profile` now runs the
  hook on demand from any loopback caller rather than only on a rare state
  transition, so it is worth knowing if you write a debugging hook.

## Logging

`RUST_LOG` scopes the log level; `relay=info` is the useful default. Avoid a
bare `RUST_LOG=debug` or `RUST_LOG=trace`: the relay itself never logs header
values, but that level turns on logging inside `hyper`/`reqwest` too, which
is not written to that rule. It was verified harmless at the currently
pinned versions — not a property to keep betting on across upgrades.

## Development

```
nix develop
cargo build
```

There is no CI. The check command is these three, and **all three must be clean
before a change lands** — a warning is a failure here:

```
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Run them before opening a pull request; nothing else will. A great deal of this
project's assurance rests on that suite by design — the header-hygiene
invariant of spec §7b is a *test*, not a convention, and so are the control
surface's three loopback axes.
