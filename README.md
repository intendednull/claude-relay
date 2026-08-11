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
2 (limit detection) is next; there is no fallback routing yet, so every
request goes to Anthropic.

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
- A non-UTF-8 body lands in `body_base64` instead of `body`. A gzip-encoded
  error response is opaque bytes to the proxy, so it shows up that way rather
  than as readable JSON; decode and decompress it by hand (`base64 -d | zcat`).
- Fixtures are written 0600, into a directory the relay creates 0700 (a
  directory that already exists keeps its own permissions). They are still
  unredacted response bodies on disk — treat them as sensitive.

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
