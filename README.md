# claude-relay

A local HTTP proxy that sits between Claude Code and the Anthropic API.
Transparent passthrough under normal operation (preserving OAuth
subscription billing); on detecting subscription usage-limit exhaustion, it
fails over eligible requests to a configured fallback provider.

Full design: [`docs/spec.md`](docs/spec.md).

## Status

Pre-alpha. Milestone 1 (transparent passthrough proxy) in progress — see
[`docs/plans/`](docs/plans/).

## Development

```
nix develop
cargo build
cargo test
```
