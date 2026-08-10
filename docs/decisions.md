# Decisions log

Confirmed choices made during implementation that refine or extend
`docs/spec.md` (which stays as-received from the original design). New
entries go at the bottom.

## 2026-08-10 — Fallback provider: Together AI

Confirmed by the user: the first (and for now, only) fallback profile to
target is Together AI. `docs/spec.md` §8's `deepseek`/`kimi` profiles are
illustrative placeholders from the original design, not a commitment to
those providers.

**Open question for Milestone 3 (fallback route):** whether Together AI
exposes an Anthropic Messages-compatible endpoint (spec §7c Phase 1) or
only an OpenAI-format endpoint (Phase 2, translator required). Per spec §5's
own philosophy, verify this empirically against Together's current API
docs/catalog when Milestone 3 starts — do not assume either shape here.

**Confirmed requirement:** a manual way to switch to the fallback (not just
automatic failover on limit-detection) is required, not optional. This is
already designed for in spec §8b — `POST /control/profile`,
`POST /control/mode`, and the `relay use <profile>` / `relay mode <mode>`
CLI wrapper. No design change needed; noting here so Milestone 3/4 planning
treats the control API as load-bearing rather than a nice-to-have.

Does not affect Milestone 1 (pure passthrough, no fallback routing at all).
