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

## 2026-08-10 — Milestone 2 detection rule is provisional, no real fixture yet

Spec §5 is explicit that the subscription-limit detection signature "must be
captured empirically" via `--capture-errors` against a real rate-limit
event, not trusted from documentation. This environment has no live
Anthropic credentials and no real usage history, so no such fixture exists.

Milestone 2's default `[detect]` rule is built directly from spec §5's
"Expected shape" paragraph (HTTP 429, `error.type == "rate_limit_error"`,
reset via `retry-after`/`anthropic-ratelimit-*`) — explicitly a best guess,
not a verified fixture. This is safe to ship because the design already
anticipates it: detection rules are config data, not code, specifically so
"a server-side wording change is a config edit, not a rebuild" (spec §5).

**Follow-up, not blocking:** once the user runs `relay --capture-errors
<dir>` against real traffic and actually hits the subscription limit, the
captured fixture should replace the guessed `[detect]` defaults — a config
edit, per the design's own intent, not a code change.
