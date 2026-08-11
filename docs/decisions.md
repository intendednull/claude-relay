# Decisions log

Confirmed choices made during implementation that refine or extend
`docs/spec.md` (which stays as-received from the original design). New
entries go at the bottom.

## 2026-08-10 — Fallback provider: Together AI

Confirmed by the user: the first (and for now, only) fallback profile to
target is Together AI. `docs/spec.md` §8's `deepseek`/`kimi` profiles are
illustrative placeholders from the original design, not a commitment to
those providers.

**Resolved 2026-08-10:** Together AI has no Anthropic Messages-compatible
endpoint — verified against Together's own current documentation
(docs.together.ai/docs/openai-api-compatibility, checked live, no mention
of Anthropic/Claude/`/v1/messages` anywhere in their docs or sitemap). Only
an OpenAI-format endpoint exists: `POST https://api.together.ai/v1/chat/completions`,
Bearer auth, confirmed supporting streaming SSE, tool/function calling,
system messages, and image content parts. So Milestone 3 needs spec §7c's
**Phase 2** (the OpenAI-format translator) — Phase 1 (passthrough + remap
only) is not available for this provider. See the spec.md §11 milestone-table
update and the Milestone 3 plan for how this reshapes scope: the translator
is no longer conditional/deferred (former milestone-table row 5) — it is
required for Milestone 3 to meet its own acceptance criterion at all, since
there is no fallback provider in play yet that Phase 1 would work against.

**Build vs. LiteLLM sidecar (user decision, 2026-08-10):** build the
translator directly in `relay`, not as a LiteLLM sidecar. Matches spec §2's
own non-goal ("not a general multi-provider gateway... deliberately a
single-purpose, single-user tool") and keeps `relay` a single static binary
with nothing else to install or keep alive — the whole point of the tool
for a single operator. The translation surface itself is bounded (spec
§7c's table is the full mapping); the genuinely hard part (tool-use
fidelity, SSE synthesis) needs the same golden-file rigor either way, so a
sidecar doesn't buy safety, only avoids writing the mapping code.

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

**Resolved 2026-08-10, Milestone 3 Task 1: moved to `[policy]`, matching spec
§8 literally.** `min_reset_horizon_secs`, `max_reset_horizon_secs`, and the
now-configurable `reset_jitter_secs` all live in the new `PolicyConfig`
(`src/config.rs`) as of this milestone. `DetectConfig::classify` and
`route_state::add_jitter` take the horizon bounds and jitter range as
parameters instead of reading their own fields. This is the deliberate
breaking move flagged below, not a deviation: no external configs exist yet
beyond this repo's own `relay.example.toml` (updated alongside), so matching
the spec now costs nothing. A Milestone-2-era config that still puts
`min_reset_horizon_secs` under `[detect]` now fails to parse — `[detect]` is
`deny_unknown_fields` and no longer has that field — which is the intended
effect of choosing "move" over "leave in `[detect]`, deviating from the
spec's section names."

**`max_reset_horizon_secs` moved too, though spec §8's `[policy]` example
doesn't list it.** It was never part of the original spec text — Milestone 2
added it as an implementation-level ceiling on top of `min_reset_horizon_secs`
(see the ceiling entry below) — but it is the same horizon pair as
`min_reset_horizon_secs` and reads identically in `classify`/`bounded`, so
splitting them across two config sections would be the more surprising
outcome. Moved together, on the same reasoning as the resolution above.

**`reset_jitter_secs` is now a real config field.** Previously a hardcoded
15–60s window in `src/route_state.rs` (Milestone 2, `[policy]` being banned
that milestone); `RouteStateMachine` now takes `jitter_secs: [u64; 2]` at
construction (`policy.reset_jitter_secs`, default `[15, 60]`, matching the old
hardcoded values so an omitted key changes nothing).

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
`PolicyConfig::validate` (`src/config.rs`, moved from `DetectConfig::validate`
alongside the field, Milestone 3) — because a `max_reset_horizon_secs` written
in milliseconds is not a bound at all: large enough and `detect::bounded`'s
`checked_add` returns `None`, silently disabling every marked classification;
merely huge and `/status` reports `LIMITED` with a `limited_until` too far out
for RFC3339 to render, which is a stuck route with nothing to read.

## 2026-08-11 — What the OpenAI translator cannot carry, and what it refuses to guess

Milestone 3 Task 2 (`src/translate/`). Spec §7c's table is the contract; this
records where the table is silent and a choice had to be made. **None of it is
verified against Together's actual API** — no credential exists in this
environment (Milestone 3 plan, Global Constraint 10), so every "the provider
does X" below is generic OpenAI chat-completions behavior, assumed compatible
because `docs/decisions.md`'s Together entry says the endpoint is
OpenAI-format. The spec's own requirement — golden-file tests against recorded
real traffic — is still outstanding and is the thing that would falsify any of
this.

**Fidelity gaps, in the order they will bite.**

- **`thinking` / `redacted_thinking` blocks are dropped** (spec says to; OpenAI
  has no equivalent). A `--continue`d session that crossed the boundary loses
  the assistant's prior reasoning, exactly as spec §11's risk table anticipates.
  Dropped silently and without error, which is the point — a history full of
  them must still translate.
- **`is_error` on a `tool_result` is dropped.** OpenAI's `role: "tool"` message
  has no failure flag; the error text itself still reaches the model as the
  message content.
- **An image returned inside a `tool_result` moves.** A tool message's content
  is a string, so the image cannot ride inside it; it is carried into the user
  message that follows the tool results instead. The alternative was to drop it,
  which loses a screenshot the model was meant to look at.
- **Tool results are reordered ahead of text that shared their turn.** OpenAI
  requires `role: "tool"` messages to follow the assistant turn that made the
  calls with nothing in between, so a turn holding both cannot keep its original
  order.
- **`top_k`, `metadata`, and `tool_choice.disable_parallel_tool_use` are
  dropped.** No OpenAI equivalent for the first two;
  `parallel_tool_calls: false` exists for the third but is not sent, on the same
  reasoning as `stream_options` below.
- **Object key order inside tool-call arguments is not preserved.**
  `serde_json`'s `Map` is a `BTreeMap` here, so re-encoding an Anthropic
  `tool_use.input` as an OpenAI `arguments` string sorts its keys. JSON says
  that is the same document and the round-trip test asserts semantic equality
  for that reason. It only affects history sent *upstream*: a tool call coming
  *back* streams through as raw fragments and is never re-encoded.

**Refusals, i.e. where a wrong guess would be silent corruption.** A tool with
no `input_schema` (Anthropic's server-side tools), a tool call whose arguments
are not a JSON object, a streamed tool call that never carries a function name
or an id, a call resuming after its content block closed, an upstream tool-call
`index` with no successor left in `u32`, and a block of a type the table *does*
cover arriving malformed or somewhere it cannot appear — all fail loudly rather
than translating into something plausible. Each is a tool contract this
translator cannot honour halfway.

**Amended after review: a content block type the table has no row for does
*not* fail.** It was in the list above, on the reasoning that a failed request
says why while a dropped block is invisible. That missed where such a block
lives: a `document` (a read PDF) or a `server_tool_use`/`web_search_tool_result`
(Claude Code's WebSearch) sits in the conversation *history*, so failing on it
breaks not just the request carrying it but every later request in that session
— permanently, and starting the moment Anthropic rate-limits the user, which is
the session the fallback exists to rescue. It also was not the two-way choice it
looked like. Such a block now becomes a placeholder text block (`[relay: a
"document" content block was dropped here; …]`) plus a `tracing::warn!` naming
the type: visible to the model and to the operator, without making a session
un-fallback-able. Dropped `thinking` blocks log at `debug` rather than `warn` —
they are spec §7c's own instruction, and a thinking-enabled session carries them
in every turn.

**One tool call streams at a time; the rest wait, buffered.** Anthropic allows a
single open content block, and OpenAI's `delta.tool_calls` is an array precisely
so a provider may batch or interleave parallel calls. Requiring the incoming
fragment to match the open block — the first implementation — aborted the stream
on any provider that did either. A call arriving while another's block is open
now buffers into its own slot (bounded, below) and gets its block when the open
one closes. The cost is a bounded delay on the second and later calls'
`input_json_delta` events; the alternative was a translator that only works
against providers that behave exactly like OpenAI's own service, which is
unverifiable here (Global Constraint 10) and is the make-or-break of the whole
milestone (Global Constraint 9).

**Memory is bounded per frame *and* in aggregate.** `BUFFER_CAP` (4 MiB) caps an
unterminated SSE frame, but the tool slots are what survive *between* frames, so
an upstream could have grown them without ever sending a large frame: slot count
is capped at `MAX_TOOL_SLOTS` (256) and the total retained across slots — ids,
names, buffered arguments — is capped at `BUFFER_CAP` too. A `base_url` is
operator config pointing at a third party, not a trusted peer.

**The response body ends at the terminal event, not when the upstream hangs
up.** Emitting `message_stop` or `error` and then continuing to drain the
upstream leaves a client that reads to end-of-body waiting on a connection the
provider is under no obligation to close. `sse_stream` ends the body as soon as
the translator reports itself done.

**`stream_options: {"include_usage": true}` is deliberately not sent.** It is
how OpenAI asks for token counts on a streamed response, and without it the
synthesized `message_delta` reports whatever usage the provider volunteers, or
zeroes. Sending an unknown parameter to a provider that rejects unknown
parameters would break *every* streamed request; the cost of omitting it is a
cosmetic token count. If Together turns out to accept it, this is a one-line
change worth making.

**A failed fallback stream ends cleanly, with an `error` event.** Spec §6 says a
mid-stream failure terminates the stream with an error event. That only works if
the body then ends *properly*: an earlier version propagated the upstream error
to axum after emitting the event, and the client never received the event at all
— the aborted body discarded it (observed in `tests/translate_stream.rs`, not
assumed). So `translate::sse_stream` yields an infallible stream: every failure
becomes an in-band Anthropic `error` event. The upstream error object therefore
does not come back out of the translator, and its text is never interpolated
into the event either, since a `reqwest` error can carry the upstream URL and a
profile's `base_url` is a place a credential could hide. A caller wanting the
error's details in its logs should inspect the upstream stream on the way in.
