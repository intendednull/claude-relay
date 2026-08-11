# claude-relay

A local HTTP proxy that sits between Claude Code and the Anthropic API.
Transparent passthrough under normal operation (preserving OAuth
subscription billing); on detecting subscription usage-limit exhaustion, it
fails over eligible requests to a configured fallback provider.

Full design: [`docs/spec.md`](docs/spec.md). Choices made since, which refine
or extend it: [`docs/decisions.md`](docs/decisions.md).

## Status

Pre-alpha. Milestone 1 (transparent passthrough proxy, `/status`,
`--capture-errors`) is complete — see [`docs/plans/`](docs/plans/). Milestone
2 is in progress: limit detection, the route state machine and the notifier
are in. There is still no fallback routing (Milestone 3), so every request
goes to Anthropic, and a detected limit only changes what `/status` reports
and fires a notification.

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
`ACTIVE`. `GET /status` reports the current state and `limited_until`.

`min_reset_horizon_secs`, `max_reset_horizon_secs` and `reset_jitter_secs`
live under `[policy]`, not `[detect]` — see `relay.example.toml`.

- **Nothing else changes yet.** The client always receives the upstream's own
  response, byte for byte, whether or not it classified as a limit.
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

## Notifications

Set `notify.command` to be told when the route state changes instead of
polling `/status`. The command runs through `sh -c` and gets the event in its
environment:

| Variable | Value |
|---|---|
| `RELAY_EVENT` | `failover_engaged` when the route becomes `LIMITED`, `recovered` when it returns to `ACTIVE` |
| `RELAY_RESET_AT` | RFC3339 end of the window on `failover_engaged`, the same value `/status` reports as `limited_until`; empty otherwise |
| `RELAY_DETAIL` | A one-line human-readable summary |

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
cargo test
```
