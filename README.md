# claude-relay

A local HTTP proxy that sits between Claude Code and the Anthropic API.
Transparent passthrough under normal operation (preserving OAuth
subscription billing); on detecting subscription usage-limit exhaustion, it
fails over eligible requests to a configured fallback provider.

Full design: [`docs/spec.md`](docs/spec.md).

## Status

Pre-alpha. Milestone 1 (transparent passthrough proxy) in progress — see
[`docs/plans/`](docs/plans/).

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
repo doesn't have.

## Development

```
nix develop
cargo build
cargo test
```
