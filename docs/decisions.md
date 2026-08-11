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

**The fixture will very likely be gzipped, and that is handled.** This was an
open question when the paragraph above was written; see *Anthropic gzips its
error bodies, so detection decompresses its own copy* below for how it was
settled.

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

**Follow-up for Milestone 3, not a bug now:** the hook inherits the relay's
whole environment, which today holds no credential — but spec §8's
`[profiles.*]` puts a fallback provider's API key in one (`api_key_env =
"RELAY_TOGETHER_KEY"`), and that environment would then be handed to an
`sh -c` command on every state change. Milestone 3 should decide whether the
notifier filters `RELAY_*_KEY`-shaped variables out of the child's
environment, weighed against the inheritance a desktop notifier needs
(`DISPLAY`, `DBUS_SESSION_BUS_ADDRESS`).

## 2026-08-10 — Anthropic gzips its error bodies, so detection decompresses its own copy

**Confirmed, not hypothesised:** an unauthenticated request to
`https://api.anthropic.com/v1/messages` sent with `accept-encoding: gzip,
deflate` comes back `content-encoding: gzip` with a body starting `1f 8b 08
00`. Claude Code's HTTP client (Node/undici) sends `accept-encoding` with
compression by default, and the proxy forwards it verbatim (it is not
hop-by-hop). So the compressed case is the *ordinary* case in production, not
an edge one — and the version of Milestone 2 that skipped classification on
any `content-encoding` would have hit that skip on essentially every real
error response and never classified anything. Spec §11's acceptance criterion
("real limit event flips state and fires notification") was unmeetable as
first shipped.

**The fix decompresses only the classifier's own buffer** (`flate2`, default
features — the pure-Rust `miniz_oxide` backend, no C toolchain, no libz), inside
`DetectConfig::classify`. Nothing else in the pipeline changes: the client's
response and every `--capture-errors` fixture still carry the exact bytes
Anthropic sent, `content-encoding` included, which is Milestone 1's whole
guarantee. `tests/limit_detection.rs` asserts both halves of that in one test,
and `tests/proxy.rs`'s gzip fidelity test is untouched.

**Bounded on both sides.** The accumulator already caps the compressed body at
1 MiB, which bounds nothing about the output — gzip reaches ratios in the
thousands — so decompression stops at 4 MiB of output and treats an overrun
exactly like a body it cannot read: warn, pass through, never classify. Only a
single `gzip`/`x-gzip` encoding is decompressed; `br`, `zstd` and
doubly-compressed bodies keep the old skip-and-warn behavior rather than being
guessed at.

**Rejected: enabling reqwest's own `gzip` feature.** It would decompress every
response before the proxy saw it, so the client would receive bytes the upstream
never sent — precisely the fidelity Milestone 1 exists for. (Note for anyone
reading the old `Cargo.toml` comment: its stated reason, that responses would be
"mislabelled", was wrong — reqwest strips `content-encoding`/`content-length`
when it decodes, so nothing is mislabelled. The comment has been corrected; the
conclusion it reached was right for a different reason.)

**Rejected: stripping the client's `accept-encoding` on the Anthropic route.**
It would make bodies readable without any new dependency, but it silently
degrades every response the proxy carries — including the large streaming ones —
by turning compression off for a client that asked for it, to serve a detector
that only ever looks at error responses.

## 2026-08-10 — Where Milestone 2's config diverges from spec §8, and why

Recorded so none of it reads as an oversight, and so Milestone 3 makes the
compatibility call deliberately.

**`min_reset_horizon_secs` and `max_reset_horizon_secs` live in `[detect]`, not
`[policy]`.** Spec §8 puts `min_reset_horizon_secs` (and `reset_jitter_secs`)
under `[policy]`; Milestone 2's plan banned a `[policy]` section outright
(Global Constraint 5), so they went where the code that reads them lives.
**Forward-compat question for Milestone 3:** every config struct is
`deny_unknown_fields`, so if M3 introduces `[policy]` and moves these keys to
match spec §8 literally, every Milestone-2-era config carrying them under
`[detect]` fails to parse. M3 has to pick one — move them (matching the spec,
breaking existing configs) or leave them in `[detect]` (deviating from the
spec's section names, staying compatible). Not decided here.

**`reset_jitter_secs` is not a config field at all.** Spec §8 lists it; Task 1
implemented the jitter as a hardcoded 15–60s window in `src/route_state.rs`,
again a plan-scope decision rather than an oversight. Same forward-compat
question as above if it ever becomes configurable.

**`RELAY_RESET_AT` carries the jittered `until`, not the raw upstream
`reset_at`.** Spec §4's text names the upstream reset time, but the value an
operator needs is when the relay will actually retry — and it is the same value
`/status` reports as `limited_until`, which is what makes the two agree. This is
a deliberate, tested interpretation; do not "correct" it back from a literal
reading of §4 without also changing `/status`.

**Why `max_reset_horizon_secs` defaults to 7 days.** It is a midpoint between
two failure directions: tighter, and a legitimate weekly subscription window
gets rejected; looser, and a units mistake produces a lockout measured in years.
It does not need to be generous, because once a limit is genuinely hit the
*remaining* time to any real reset can never exceed that limit's own period — 7
days covers the known windows with room to spare. If the captured fixture (still
pending, above) shows different periods, this default moves with it. The
ceiling now has a ceiling of its own — 10 years, enforced in
`DetectConfig::validate` — because a `max_reset_horizon_secs` written in
milliseconds is not a bound at all: large enough and `bounded`'s `checked_add`
returns `None`, silently disabling every marked classification; merely huge and
`/status` reports `LIMITED` with a `limited_until` too far out for RFC3339 to
render, which is a stuck route with nothing to read.
