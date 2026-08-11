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

**Check `content-encoding` on that fixture first.** The proxy forwards the
client's `accept-encoding` upstream (it is not a hop-by-hop header) and
reqwest is built with no decompression feature, deliberately — so *asking*
Anthropic to compress is the default behavior, and a compressed error body is
opaque bytes to the detector. Milestone 2 handles this honestly rather than
silently: it logs `limit detection skipped: the upstream error body is
compressed` and passes the response through. But if real error bodies do come
back compressed, detection is inert in production with only that log line as
the symptom, and it becomes a design question for whoever does this
follow-up — add a decompression dependency for the classification path, or
stop forwarding the client's `accept-encoding` on the Anthropic route. A
fixture with `"body_base64"` instead of `"body"` (see the README) is the
tell.

## 2026-08-10 — The notifier runs on its own thread, not on the tokio runtime

Milestone 2's plan (Global Constraint 4, Task 3) says the notifier hook uses
`tokio::process::Command`, on the assumption that transitions are applied
inside the async request path. They are not: Task 2 landed the applier as a
plain `std::thread` reading a channel (`src/route_updates.rs`), because the
state machine persists synchronously and the point where a non-2xx body is
fully known is a stream callback that cannot await. There is no runtime under
that thread, and blocking it would be worse than any notifier failure — its
loop applies *every* future transition, so a hook that hung would silently
stop route tracking for the whole process.

So the notifier is the same shape as the applier: a thread of its own, fed by
a channel, spawning the hook with `std::process::Command` and enforcing the
timeout by polling `try_wait` and killing at the deadline. Firing a
notification is a non-blocking channel send. No new dependency, and the
constraint's actual concern — "no new process-spawning crate" — is met more
directly than by tokio.

**Rejected:** capturing a `tokio::runtime::Handle` at startup and
`handle.spawn`-ing the hook onto the runtime from the applier thread. It
works, but it puts a runtime dependency into a component that otherwise has
none (the state machine and its applier are deliberately runtime-free and
unit-testable without one), needs two more tokio features, and buys nothing
the channel hand-off doesn't already provide.

**The hook runs through `sh -c`.** Spec §8's example is a path to a script,
but §3 names `notify-send`, `osascript` and `ntfy` as the integration points
and every one of those needs arguments — a bare-exec field would force a
wrapper script for all of them. Nothing outside the config file is ever
interpolated into that string: the event reaches the hook through the
environment, never as shell text. The cost is that killing a timed-out hook
reaches the `sh`, so a hook that forks its own children can outlive its
timeout; the relay is unaffected either way, and killing the process group
would mean a libc dependency for one signal.
