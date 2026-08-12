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

**Resolved 2026-08-11, Milestone 3 Task 4 fix round 1:** not filtered.
Documented instead — README's Notifications section now says outright that
the hook's environment carries every profile's key value, since Task 4's
`POST /control/profile` changed the exposure from "an occasional, upstream-
driven state transition" to "on demand, from any loopback caller, including
a debugging hook someone reaches for and forgets is on this path." Filtering
was considered and set aside: the set of variable *names* worth stripping
isn't fixed by this project (`api_key_env` is operator-chosen per profile,
not a `RELAY_*`-prefixed convention), so a filter would need an allowlist of
what to keep (`DISPLAY`, `DBUS_SESSION_BUS_ADDRESS`, `PATH`, …) rather than a
denylist of what to drop — a bigger change than this fix round's scope, and
one that risks quietly breaking an operator's existing hook. Restricting
`profile_switched` to real changes only (the 2026-08-11 fix-round entry
further below) already closes most of the on-demand angle by making a flood
of no-op switches free rather than N hook invocations.

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
  Dropped without failing the request, which is the point — a history full of
  them must still translate — and logged at `debug` rather than `warn`, since a
  thinking-enabled session carries them in every turn and a `warn` per block
  would teach the operator to ignore the level the real gaps are reported at.
- **`is_error` on a `tool_result` is dropped.** OpenAI's `role: "tool"` message
  has no failure flag; the error text itself still reaches the model as the
  message content.
- **An image returned inside a `tool_result` moves.** A tool message's content
  is a string, so the image cannot ride inside it; it is carried into the user
  message that follows the tool results instead. The alternative was to drop it,
  which loses a screenshot the model was meant to look at. It lands *after* that
  turn's own text rather than before, because the text is the part liable to be
  referring to it ("the screenshot above shows…").
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

**Refusals, i.e. where a wrong guess would be silent corruption.** A tool call
whose arguments are not a JSON object, a streamed tool call that never carries a
function name or an id, a call resuming after its content block closed, an
upstream tool-call `index` with no successor left in `u32`, and a block of a
type the table *does* cover arriving malformed or somewhere it cannot appear —
all fail loudly rather than translating into something plausible. Each is a tool
contract this translator cannot honour halfway. (A tool *definition* with no
`input_schema` was on this list and is not any more — see the resolution
below.)

**Amended after review: a content block this translator cannot map does *not*
fail.** That covers both a `type` with no row in the table at all and a `type`
that has one but arrived in a shape this translator does not model — an `image`
sourced from Anthropic's Files API rather than base64 or a URL. The second door
reaches the same defect as the first and was missed on the first attempt at
this.

Failing was in the list above, on the reasoning that a failed request says why
while a dropped block is invisible. That missed where such a block lives: a
`document` (a read PDF) or a `server_tool_use`/`web_search_tool_result` (Claude
Code's WebSearch) sits in the conversation *history*, so failing on it breaks
not just the request carrying it but every later request in that session —
permanently, and starting the moment Anthropic rate-limits the user, which is
the session the fallback exists to rescue. It also was not the two-way choice it
looked like. Such a block now becomes a placeholder text block (`[relay: a
"document" content block was dropped here; …]`) plus a `tracing::warn!` naming
the type: visible to the model and to the operator, without making a session
un-fallback-able. The type name is client-controlled, so it is clipped and
stripped to a plausible identifier before it reaches either the note or the log.

**`tool_use` and `tool_result` are the two exceptions.** A malformed one of
those still fails, because a note cannot stand in for it: the message it pairs
with is left referring to a call that no longer exists, and the provider rejects
*that* with an error far less legible than this one. Every other block type is
ordinary content, and content degrades.

**Resolved: an untranslatable tool *definition* is dropped, not failed.** An
Anthropic server tool in the request's `tools` array —
`{"type": "web_search_20250305", "name": "web_search"}`, which Claude Code sends
on every request when WebSearch is enabled — has no `input_schema` and is
dropped from the outgoing list with a `tracing::warn!` naming it, leaving the
rest of the request to translate normally.

This was raised as the one place where "fail loudly on a tool contract" and
"never strand a session" genuinely conflicted, and was decided rather than
settled by the implementer. Failing had exactly the property the content-block
amendment above exists to remove: `tools` is re-sent on *every* request, so a
WebSearch-enabled session could never reach the fallback at all — the session
the fallback exists for.

**Why this is the opposite call from a malformed `tool_use`/`tool_result`, and
not an inconsistency:** what matters is whether anything in the conversation
*refers* to the thing being dropped. A `tool_use` is referred to by the
`tool_result` that answers it, so replacing one leaves a dangling reference the
provider rejects opaquely. Nothing refers to a tool *definition*: an
OpenAI-format `tools` list constrains only the calls the model may make on this
turn, so a `web_search` call already sitting in the history translates the same
either way, and the only effect is that the model is not offered a capability
the fallback provider could not have served regardless. The history side of that
same session degrades to placeholders (above), so the two halves agree: nothing
is left pointing at anything.

**Assumption, unverifiable here, belonging with the others in this entry:** that
an OpenAI-format provider does not validate historical `tool_calls` in the
message history against the current `tools` list. That is how OpenAI's own API
behaves; Together is assumed to match, like everything else in this module.

A `tool_choice` the remaining tools can no longer satisfy is dropped with them:
either because the list is now empty, or because the choice names a specific
function that did not survive. Both are requests OpenAI-format providers reject
by contract, and dropping a tool is what newly makes either reachable. Only what
*this* broke is compensated for — a `tool_choice` naming a tool the client never
sent, or arriving with no tools at all, was already the client's own doing and is
forwarded unchanged, since the proxy does not validate requests on the provider's
behalf.

**Where the "nothing refers to a tool definition" reasoning does not fully
reach.** It holds for `web_search`, whose calls appear as `server_tool_use`
blocks that degrade to placeholders. Anthropic's *client-executed* tools —
`computer_*`, `bash_*`, `text_editor_*` — are different: they also carry no
`input_schema`, so their definitions are dropped too, but they are invoked
through ordinary `tool_use`/`tool_result` blocks, which translate into a real
`tool_calls` entry naming a tool the outgoing `tools` list no longer offers. That
is fine under the assumption recorded above (a provider does not validate
historical calls against the current list) and is exactly what
`a_prior_call_to_a_dropped_tool_still_translates_intact` pins down — but if that
assumption turns out false for Together, this tool family is the case that breaks
and the test that would need to change. Not live today: Claude Code's own Bash /
Read / Edit tools are ordinary schema-carrying tools, not this family.

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

## 2026-08-11 — Failover wiring: what Task 3 had to decide

Milestone 3 Task 3 (`src/proxy.rs` routing, `src/fallback.rs`). Spec §6, §7a,
§7b, §7d say what must happen; these are the places they did not say how, or
where two sections pulled in different directions.

**A routable request has its body read, and the Anthropic route stops being a
streaming passthrough for it.** The routing decision needs the `model` inside
the body, so that body cannot stay a stream: `/v1/messages` and
`/v1/messages/count_tokens` are read into memory before their route is known,
and the upstream request is then framed with `content-length` rather than
`transfer-encoding: chunked`. The bytes Anthropic receives are identical, but
"verbatim streaming passthrough" is no longer an accurate description of the
request half of that route — the response half is untouched. The `/v1/*`
catch-all still streams: it carries no `model` to route on.

**...but only when a profile is configured at all.** With none, the router
could only ever answer `Anthropic`, so reading the body would buy a decision
already made — and would have given up Milestone 1's streaming for nothing. A
zero-profile relay is therefore byte-for-byte *and* frame-for-frame what
Milestone 1 was, which is what anyone not using the fallback feature is
running. It also narrows who pays for the buffer below.

**The routing buffer is 8 MiB, per request, unbounded across concurrent ones.**
Far past any real Claude Code request including one carrying images, so what it
bounds is a runaway rather than ordinary traffic; kept as small as that allows
rather than as large as Anthropic's own request-size limit, precisely because
nothing bounds the number of requests in flight. A body past the cap is not
inspected at all — the bytes already read go back in front of the rest of the
stream and the request takes the Anthropic route — so the cap costs a routing
decision, never a byte and never a request. It is a field on `AppState` rather
than a constant read in place, so a test can drive that reassembly path
without an 8 MiB fixture; it is deliberately not a config key.

**`count_tokens` is pinned to Anthropic for every `claude-*` model and for
every `openai`-format profile's models — but a non-`claude-*` name routes by
name when the profile that claims it is `anthropic`-format.** §6 says "always
go to Anthropic regardless of state"; §7d says any non-`claude-*` name routes
to the profile that claims it, "regardless of Anthropic's state". They collide
on `POST /v1/messages/count_tokens {"model": "deepseek-ai/…"}`, and the honest
answer differs by format:

- An `anthropic`-format profile has a `/v1/messages/count_tokens` of its own,
  which this route already mirrors, and it owns the tokenizer being asked
  about. §7d applies; §6's stated reason (a count against the wrong tokenizer
  is worse than an error) is *satisfied* by routing there, not violated.
- An `openai`-format profile has no counting endpoint at all. The fallback
  route sends every translated request to `/v1/chat/completions`, so routing a
  count there would bill a real inference call and answer with a `message`
  object where the client wanted `{"input_tokens": N}` — strictly worse than
  the error §6 prefers. So the Anthropic pin stays.
- A name *no* profile claims, with no `active_profile` to fall through to,
  resolves nowhere: the router errors. On `/v1/messages` that becomes the
  relay's own `400 no_route_for_model`; on a count it must not, because the pin
  has no exception for names the relay cannot place. The count goes to
  Anthropic, whose tokenizer owns the verdict on the name — §6's "on failure,
  pass the error through" means Anthropic's error, not the relay's.

Failover never applies to `count_tokens` in any of these cases: route state and
policy mode are not consulted for it at all.

**`x-relay-route` marks fallback responses only, and only the relay may set
it.** Spec §9 offers either that or an explicit `x-relay-route: anthropic`; the
Anthropic route's response is a verbatim copy of Anthropic's own (Milestone 1's
whole purpose) and adding a header would end that. Absence therefore means
Anthropic. Every response the fallback route produces carries the marker,
relay-generated errors included: the question it answers afterwards is "did
this come from Anthropic", and a failed fallback attempt did not. Because
absence is the *claim* on the Anthropic route, `forwardable` strips the marker
from every upstream response on both routes — otherwise an upstream, or a
`base_url` pointed somewhere it should not be, could forge it.

**The fallback's header allowlist is empty.** §7b says allowlist, not denylist;
taken to its end that means *nothing* from the client is copied and the outgoing
headers are built from constants: `content-type`, the profile's own
`Authorization: Bearer`, and (for `format = "anthropic"`) `x-api-key` with the
same key plus a constant `anthropic-version: 2023-06-01`, which that API
requires and which is ours rather than the client's. Two headers matter for
reasons beyond auth: `accept-encoding` is dropped because Claude Code asks for
gzip and a compressed body is one the translator cannot read, and
`content-length` because the body it described is not the body being sent.
Sending both `Authorization` and `x-api-key` to an Anthropic-format profile is
belt-and-braces for a code path no real provider has ever exercised (Global
Constraint 10) — if a provider rejects a request carrying both, that is the
first thing to cut.

**`base_url` is an API root, not an endpoint, and must be https off-host.** An
`openai` profile is served at `{base_url}/v1/chat/completions` whatever
Anthropic path the client called; an `anthropic` profile mirrors the incoming
path. The relay owns the path because the format determines it. A profile's
`base_url` is also a second place a real credential now travels — its own API
key — so plaintext `http` to a non-loopback host is refused at startup rather
than leaked once per request. Loopback stays allowed (local mocks, a sidecar).
The rule is deliberately not applied to `anthropic.base_url`, whose rules
Milestone 1 settled and which carries the client's credentials to a fixed,
always-HTTPS endpoint. Host classification is textual: no DNS lookup happens at
startup, so a name that merely resolves to loopback is still refused.

**A fallback's non-2xx passes through verbatim, untranslated.** Spec §7d already
says a provider's error surfaces as that provider's error. Translating an error
envelope would mean inventing an OpenAI→Anthropic error mapping from shapes this
project has never captured. Claude Code will render an OpenAI-shaped error less
gracefully than an Anthropic one; that is a known, cheap-to-fix gap, and fixing
it on a guess is not. **(Superseded 2026-08-12, Task 9B — the gap turned out to
cost a whole session, not just grace; see that entry.)**

**Nothing a fallback returns touches Anthropic's route state or the capture
fixtures.** A 429 from the fallback provider must not put the Anthropic route
into `LIMITED`, a 200 from it must not recover the route out of it, and its
error bodies are not fixtures Anthropic detection rules can be derived from.

**A model nothing can route is a 400, not a silent forward.** `router::route`'s
clean error (profiles are configured, none claims the name, no active profile)
becomes `400 {"error": "no_route_for_model"}` rather than being forwarded to
Anthropic to be rejected there. A zero-profile relay never reaches this at all
— it does not read the body — so a Milestone 1/2 config keeps forwarding every
name to Anthropic exactly as it did.

**A fallback's 2xx keeps its own status.** A translated response is
synthesized by the relay, but the status it carries is still the provider's
answer: a 202 or 206 does not silently become a 200.

**`fallback_requests_served` counts requests a profile answered**, whatever it
answered with, and not requests that never reached one (untranslatable body,
missing key, unreachable endpoint). `/status` reads it now rather than keeping
its hardcoded `0`, since the field was already in the documented response shape.

## 2026-08-11 — The control API (Task 4): what the plan left open

`src/control.rs`. Spec §8b names the two endpoints and the loopback rule;
these are the places it left the shape to the implementer.

**Loopback enforcement is a route-registration decision, not a per-request
one.** `control::enabled(&Config)` re-parses `listen` via the same
`Config::listen_addr` `main` already calls, and `build_router` only adds the
`/control/*` routes when it says loopback. A `listen` that fails to parse is
treated as not loopback (fail closed: unparsed is not "proven safe," and the
real binary never gets this far on one anyway). The alternative — register the
routes unconditionally and gate inside each handler — was rejected: gating at
registration means a non-loopback bind gets axum's own 404 for an unmatched
path, so there is no handler-shaped place to accidentally return a 403, a 500,
or a body that admits the surface exists.

**Consequence for Milestone 4, flagged rather than solved here:** this check
runs once, when `build_router` is called at startup. Milestone 3 has no config
hot-reload, so that is every time it matters today. If Milestone 4's hot
reload lets `listen` change without a restart, `control::enabled`'s result
would go stale until the next rebuild — hot-reloading `listen` at all is a
bigger question than this task's scope, so this only records the coupling for
whoever picks that up.

**The runtime switch is `AppState`-local, read once per request.**
`AppState::active_profile()` folds `policy.active_profile` and a
`POST /control/profile` override into one value; every routing call site
reads it exactly once and holds the result for the rest of that request. This
is what makes "a switch applies to new requests only" (spec §8b) true by
construction rather than by a lucky timing window — nothing downstream of the
initial read ever re-consults either source.

**`GET /control/profiles` returns `name`, `format`, `serves`, `model_map`,
`api_key_env` (the *name*, never read or returned as a value), and `active`.**
Spec §8b says "list profile names + their model maps; mark active" and leaves
the rest of the shape open; the extra fields were judged worth exposing since
an operator switching profiles blind on `name` alone would have to cross-
reference the config file anyway to know what a name even routes to.

**`profile_switched` reuses the existing three env vars; no `RELAY_PROFILE`
was added.** Spec §4 names exactly `RELAY_EVENT`, `RELAY_RESET_AT`,
`RELAY_DETAIL` and spec §8b adds no fourth for a profile name. The switched-to
name rides in `RELAY_DETAIL` ("active profile switched to <name>"), with
`RELAY_RESET_AT` empty — not applicable to this event, same "empty rather than
absent" rule `Recovered` already uses. Considered and rejected: a dedicated
env var, which would be more convenient for a hook to parse structurally but
is not anything spec asks for, and this project's other additions to the
config/event surface have consistently matched spec's stated shape rather than
extended it speculatively.

**`NotifyEvent` dropped `Copy` for `Clone`, and `Notifier` gained a second
entry point.** `ProfileSwitched { name: String }` cannot be `Copy`.
`Notifier::notify(RouteTransition)` stays as the route-state path;
`Notifier::notify_event(NotifyEvent)` is new and is what `notify` now calls
internally, and what `/control/profile` calls directly — a profile switch is
not a route transition, so routing it through `notify` would have meant
manufacturing a fake `RouteTransition` to smuggle it through. `Notifier`
itself is now `Clone` (`mpsc::Sender<T>` is `Clone` regardless of `T`), so
`AppState` can hold one end for `/control/profile` while `RouteUpdates` holds
the other, both backed by the same channel.

## 2026-08-11 — Task 4 fix round 1: the deviation made explicit, and why `Host` is checked

Recorded per the fix round's own instruction, so none of this reads as a later
surprise.

**Spec §8b's literal text is not what this ships, and that is a deliberate,
ruled-on deviation, not an oversight.** §8b says a non-loopback `listen` "must
require a token"; no token mechanism exists this milestone (out of scope by
the plan), so `docs/spec.md`'s Task 4 brief authorized "disable `/control/*`
entirely" as the milestone-3-correct substitute, and the orchestrator
confirmed that reading. `control::enabled` and `control::routes` implement
the authorized substitute, not the spec's literal words — anyone reading §8b
against this code should read this entry first, not conclude the two
disagree by accident.

**`Host` is now checked, on top of the bind check, because the bind check
alone doesn't deliver what its own premise promises.** The whole point of
"loopback bind implies local operator only" is that nothing off-host can
reach the control surface. That premise is false as shipped without a `Host`
check: DNS rebinding lets an attacker's own domain resolve to `127.0.0.1`,
so a browser tab on that domain reaches the relay as a same-origin request
whose `Host` header the browser still renders as the attacker's domain — the
TCP connection is loopback, but the caller is not "local" in any sense worth
trusting. This was outside the brief's literal requirement (bind-address
enforcement) and outside the peer-address check the implementer explicitly
flagged and deferred — a second reviewer surfaced it, and the orchestrator
ruled to fix it anyway, since the cost (one `Authority`-parse comparison) is
far below the payoff (silent redirection of the user's LLM traffic to an
attacker-chosen profile, from a page the user never knowingly gave this port
to). `control::routes` applied it as middleware over both endpoints, in the
same function that owns the bind gate.

**Correction, fix round 2 (see that entry below): the previous paragraph's
last clause was false, and a reviewer proved it two ways.** "A route added to
this module later inherits both automatically" only held for a route added
*inside* `control::routes()`'s own chain, and only *before* its `.layer()`
call — a route appended after that `.layer()` escaped the `Host` check
entirely (`Router::layer` wraps only what was registered before it), and a
control route registered anywhere else in the crate (`lib.rs`, a future
module) never went through this module at all, so it inherited nothing.
Fixed by moving both gates to `install_gate`, applied once by *path* over the
fully composed router — see the fix round 2 entry for what that actually
guarantees, which is narrower than "automatically, unconditionally."

**`profile_switched`'s `RELAY_DETAIL` packing (recorded above) stands, with
one addendum:** a Milestone 4 `relay ctl` CLI wrapper parsing hook output
programmatically may want the switched-to name in a structured field rather
than free text. Noted for whoever builds that wrapper — nothing here commits
to `RELAY_PROFILE` or against it, only that the tradeoff should be revisited
once there's a concrete parser wanting the value, rather than guessed at now.

**A switch that changes nothing does not notify** (already-active target, or
a 404'd unknown name) — `AppState::set_active_profile` returns whether the
*effective* active profile actually changed, and `/control/profile` fires
`profile_switched` only then. Matches `notify.rs`'s pre-existing "only real
changes are reported" rule for route transitions; before this fix, every
switch notified unconditionally, which meant a rapid run of no-op switches
(the same name, or a client retrying a typo'd one) could queue ahead of a
real `failover_engaged` on the notifier's single FIFO queue and delay it by
the timeout of every wedged hook in front of it.

## 2026-08-11 — Task 4 fix round 2: closing what round 1 left open

A second adversarial pass on `4f0a3fb` found three things round 1's own fixes
had opened or half-closed. None of these are new attack surface Task 4
invented from nothing — R1 is a regression from round 1's own M2 item, R2 is
round 1's I4 not actually landing, R3 is round 1's I2 only half-closing its
own amplification — recorded here rather than folded silently into the round
1 entries above, since a reader tracing *why* the code looks the way it does
needs both what round 1 tried and why it wasn't enough.

**R1 — the M2 fix (switch `switch_profile` off axum's `Json` extractor for a
consistent error envelope) removed a CSRF defense nobody had named.** `Json`
was doing double duty: besides parsing, it required `content-type:
application/json`, which is not one of the three CORS-simple content types —
so requiring it was *also* forcing a browser to preflight, and the
preflight's own failure (no CORS headers on this API, ever) was blocking a
cross-origin write before Task 4's own `Host` check ever had to. Replacing
`Json` with `Bytes` + `serde_json::from_slice` kept the parsing behavior and
silently dropped the content-type requirement, which reopened exactly the
same class of hole the `Host` check exists to close — except this one needs
no DNS rebinding at all: a page loaded directly at `http://127.0.0.1:<port>`
has an honestly loopback `Host`, so the fix has to be a second, independent
check, not a stronger version of the first.

Fixed with two checks that defend different things, not one hardened check:
`switch_profile` requires `content-type: application/json` explicitly (415,
same JSON envelope, restoring the preflight); and `install_gate` (below)
independently rejects a `Sec-Fetch-Site` that isn't `same-origin`/`none` or
an `Origin` that isn't loopback, when either header is present.
`Sec-Fetch-Site`/`Origin` are attached by the browser and cannot be
overridden by page script, so — unlike `Host` — no rebinding-shaped trick
gets around them; a request carrying neither is simply not a browser request
and passes through unaffected (`curl`, `relay ctl`, this project's own
tests). The lesson generalized rather than just patched: replacing one
extractor for a cosmetic reason can silently remove whatever behavior rode
along with it, and that is worth checking for deliberately when a fix touches
a request-parsing path on a security-relevant route, not just verifying the
requested change in isolation.

**R2 — the `Host` gate was a property of `control::routes()`, not of the
path `/control/*`, and a reviewer proved those are different things.**
Two demonstrated bypasses, not a hypothetical: (a) `Router::layer` wraps only
routes registered *before* it in the same chain, so a route appended after
`routes()`'s `.layer()` call skipped the check entirely; (b) a control route
registered anywhere *else* — `lib.rs`, a different module, which is
precisely what Milestone 4's own `POST /control/mode` will be — never went
through `control::routes()` at all, so "the gate" was never in its path to
begin with. The reviewer reproduced both against a live probe route with
`cargo test` green throughout, which is the sharpest possible demonstration
that route-registration-scoped gating cannot be the right shape: passing
tests say nothing about a route the tests never registered.

Fixed by moving both checks (bind-loopback and `Host`-loopback, plus R1's
origin checks) into `install_gate`, a middleware applied once, last, to the
*fully composed* application router in `build_router`, matched on
`request.uri().path().starts_with("/control")`. This is keyed on the
request, not on which function built the route, so it does not matter where
or in what order a `/control/*` route was registered — `routes()` itself no
longer even calls `enabled()` or attaches a layer; it just registers paths.

**What this guarantees is narrower than "any control route, ever, forever",
and that narrower claim is the honest one.** `install_gate` still has to be
the *last* operation `build_router` performs on the router before
`.with_state` — a route chained on after that call would, once again, never
pass through it. The improvement is that this is now **one call site** to
get right (`build_router`, a single function, reviewed once) instead of
**every future control-route addition anywhere in the crate** each needing
to remember to opt in. `control.rs`'s own module doc and this round's tests
(`a_control_route_registered_outside_routes_still_inherits_the_gate`,
`install_gate_does_not_touch_paths_outside_control`) say exactly this, not
more.

**Verified rather than assumed: no path spelling reaches a control handler
while evading the `/control` string-prefix check.** Percent-encoding
(`/%63ontrol/profiles`), doubled slashes (leading, internal), a trailing
slash, case variation, and a trailing NUL were all tried against a live,
control-enabled relay. All 404 — axum's router (`matchit`) matches route
segments literally, with no percent-decoding, case-folding, or slash
collapsing before matching, so anything that would evade the prefix check
also fails to reach *any* handler through axum's own routing, gate or no
gate. This was checked empirically (`tests/control.rs`), not concluded from
reading `matchit`'s source, since the fix's correctness rests on it.

**Two hardening-only `Host` parser laxities, closed alongside, neither
independently exploitable:** `Authority::from_str` accepts (and discards)
userinfo, so `evil.tld@localhost` previously read as loopback via
`.host()`; no compliant client sends `@` in a `Host` value, so its mere
presence is now rejected outright. And more than one `Host` header, previously
resolved by taking the first, is now rejected per RFC 9112's "exactly one" —
picking a "winner" between two `Host` values is the kind of ambiguity
request-smuggling attacks are built from, even though nothing here is
reachable that way today.

**R3 — comparing values (round 1's I2 fix) closed the no-op case, not the
flood.** A switch to a *different* profile every time is a real change by
that comparison's own logic, so alternating `POST /control/profile` between
two valid names was never a no-op and queued every time, same as before I2.
Measured against the fix from round 1: 60 alternating switches (hook
sleeping 3s, `timeout_secs = 60`) queued 60 hook invocations in 21ms and
reproduced the original serial drain — 100 of them reproduce the full
~100-minute delay to the next real `failover_engaged`. R1's fix removes the
purely browser-driven version of this flood, but a local process (or a
`relay ctl` script bug) should not be able to delay the operator's
rate-limit notification by an hour either way.

**Fixed by giving `profile_switched` a coalescing slot instead of a place in
the same FIFO queue `failover_engaged`/`recovered` use** (`Notifier`,
`src/notify.rs`): `notify_event` routes by variant, a route-state event
always goes on the `mpsc` channel (unbounded, FIFO, never dropped — there is
no plausible flood of these, since they only fire on Anthropic's own
responses), and `ProfileSwitched` always overwrites a single `Mutex<Option<
NotifyEvent>>` slot instead. The worker thread blocks on the channel with a
timeout (originally 50ms; raised to 500ms in fix round 3's Y10, see that
entry below — a transition arriving wakes it immediately regardless of that
value either way, since `recv_timeout` does not wait out the timeout once
something is sent), and only on an *idle* tick does it check the slot. Any
number of switches in a row collapse to "the most recent one," so the worst
case a flood can add ahead of a queued transition is one already-in-flight
switch hook's run time (bounded by `timeout_secs`, ≤60s) — not N hooks run
in series. This is the "priority (transitions ahead of switches)" shape the
fix round's own text offered as an acceptable alternative to bounding the
queue, chosen over a bounded/dropping channel because `std::sync::mpsc` has
no built-in way to reorder or peek a FIFO queue, so two channels (or a
channel plus a slot) was the option available without a new dependency —
explicitly out of scope for this fix.

**Correction, fix round 3: the two tests below demonstrate coalescing, not
the priority bound, and that distinction is the whole reason fix round 3's
Y3 item exists.** The paragraph originally here claimed the isolated
200-switch unit test (`notify.rs`) and the 60-alternating-`POST` HTTP test
(`tests/control.rs`) "demonstrate the bound, with real elapsed time asserted
in each." They don't: both stop the flood *before* queuing the transition,
so all they can show is that a stopped flood's last switch gets coalesced
away — true, but not R3's actual property ("a flood *still running* cannot
delay or drop a transition"). A reviewer inverted the worker's drain order
and both tests stayed green. The test that actually pins the bound is
`notify.rs`'s `a_live_flood_of_switches_does_not_delay_a_transition_queued_
mid_flood`, added in fix round 3 specifically because these two didn't —
see that entry for what it took to get *that* test to fail under inversion
too, which was not the first attempt.

## 2026-08-11 — Task 4 fix round 3: the false gate comment, a poll instead of
a token, and a shutdown drain

**`src/lib.rs`'s claim that a route "added here later" inherits the gate
automatically was false, and a reviewer demonstrated it.** A route appended
to `build_router`'s *result* — after `install_gate` had already run — never
passed through it: reached with any `Host`, on any bind. Fixed two ways:
the comment now states the real constraint (`install_gate` must be the last
operation before `.with_state`), and the route chain moved into a separate
`app_routes() -> Router<AppState>` so `build_router` is two lines with
nothing to append to by accident:

```rust
pub fn build_router(state: AppState) -> Router {
    control::install_gate(app_routes(), &state.config).with_state(state)
}
```

Adding a route now means editing `app_routes`, which is inside the gated
region by construction — one call site to get right (`build_router` itself)
instead of every future control-route addition anywhere in the crate.

**The sharp edge this doesn't close, left as a documented gap rather than a
fix:** `control` is `pub(crate)` and `app_routes` is private, so an
out-of-crate consumer of this crate as a library who appends a `/control/*`
route to `build_router`'s returned `Router` can neither re-apply the gate
nor follow the "edit `app_routes`" advice — both are invisible from outside
the crate. Closing it needs `build_router` to return a type that refuses
further `.route()` calls, which means changing `main.rs` and every
integration test's `serve()` helper that takes a plain `axum::Router`. Left
alone: the only realistic way this matters is a genuinely external library
consumer, which spec's own non-goal (a single-purpose, single-user tool, not
a general gateway) argues doesn't exist today. `build_router`'s doc comment
says plainly that external consumers must not append `/control/*` routes at
all, so the constraint is at least stated where it's needed even though it
can't be enforced there.

**Y10: the notifier's idle-poll interval moved from 50ms to 500ms, not to a
wake-token scheme.** A relay with `notify.command` set was waking its
notifier thread 20 times a second for the life of the process even with
nothing to announce, to check a slot that was empty every time. A wake-token
design (a second value on the existing `mpsc` channel, pushed only on the
slot's empty-to-occupied transition, so `recv` never needs a timeout at all)
was fully worked out and would have preserved the priority bound — a
transition still gets ordinary FIFO order ahead of any already-queued token,
and a flood of switches still collapses to at most one token in flight the
same way it collapses to one slot occupant today. Not built anyway: it is a
materially bigger surface (a second channel item type, a write-then-maybe-
send that has to stay correct under concurrent writers) for a problem the
simple fix already satisfies — 500ms is still well inside "no real latency
requirement" for a profile-switch notification, and cuts the wakeup rate
10x. Risk-avoidance, not a correctness objection to the token idea.

**Y6: a `profile_switched` still sitting in the coalescing slot when the
last `Notifier` clone drops is now drained and delivered, not lost.** The
FIFO side already got this for free — `mpsc` drains buffered messages before
reporting `Disconnected` — so a transition sent just before shutdown was
never lost; a pending switch, living outside the channel in its own
`Mutex<Option<NotifyEvent>>` slot, didn't have the same guarantee until this
round added one more check on the `Disconnected` exit path. Chosen over
documenting the gap because it cost one match arm and closes a genuine,
if narrow (both live for the process's lifetime in the shipped binary),
regression from the pre-coalescing behavior.

**Y3's test needed two tries, and the failure of the first try is worth
recording.** The first version of the live-flood test used the "obvious"
inverted-drain-order mutation to prove itself against — `if let Some(event)
= lock().take() { run(event); continue; }` — and passed under it, which is
the opposite of what the test was supposed to show. Root cause was two
independent bugs in the *test*, not the shipped code: a startup race (the
worker's first slot-check can beat the flood's first write, then commits to
one `recv_timeout` wait the test's original delay didn't clear), and the
mutation itself accidentally holding the lock across the hook run via
`if let`'s temporary lifetime extension (Y5's bug, not Y3's), which blocked
every flood thread and let the worker win an unrelated re-lock race once
released. Both fixed; the corrected mutation (binding the lock's result to a
plain `let` first, exactly like the real code) now reliably fails the test
as required. Recorded because "the mutation passed, ship it" would have been
the wrong conclusion, and the right one only came from checking *why* it
passed rather than treating a red-then-green cycle as sufficient on its own.

## 2026-08-11 — Task 5: real Together AI traffic lands as golden fixtures

Milestone 3's plan deliberately left this outside the four SDD tasks — no
Together credential existed. One now does, for verification only; Global
Constraint 10 still binds the test suite exactly as before: no test reaches
a real provider, ever. Every fixture below is a static file replayed against
a mock upstream or fed straight into the translator's own pure functions.

**12 requests were captured against `api.together.xyz/v1/chat/completions`,
model `Qwen/Qwen2.5-7B-Instruct-Turbo`, and landed byte-for-byte at
`tests/fixtures/together/`.** Verified free of credentials before
committing — grepped for the real key, any `tgp_` prefix, any
`authorization`/`bearer` string, on the committed copies, not just the
source captures. They largely *vindicate* the translator's hand-built
fixtures rather than finding bugs in it: `tests/translate_together_fixtures.rs`
replays them and confirms one `tool_calls` entry per chunk, a stable
`index`, set-once `id`/`name`, strictly sequential (never interleaved)
parallel calls, exact non-streaming shapes for all three observed
`finish_reason` values, and OpenAI-style error bodies. **Nothing in the
translator changed** — everything real traffic touched was already correct.

**One shape no hand-built fixture modelled: `finish_reason` flickers.** The
two-tool-call capture (`B_stream_two_tool_calls.raw.txt`) shows it land on
call 0's argument chunk, revert to `null` on call 1's naming chunk, then
reappear on call 1's argument chunk and the final chunk.
`src/translate/sse.rs`'s take-last handling (`self.finish_reason =
Some(reason)` on every `Some`, never touched by a `None`) already gets this
right. The real capture's two non-null observations happen to be identical
(`"tool_calls"` both times), so it cannot by itself distinguish take-last
from first-wins — `sse.rs`'s own test module now has a synthetic
reproduction of the same null-in-the-middle shape with two *different*
values, which does. Mutation-tested: swapping the line to
`self.finish_reason.get_or_insert(reason)` (first-wins) turns the test's
expected `tool_use` into `max_tokens` and fails it; reverting restores green.

**Two gaps, stated plainly rather than left implied by the fixtures'
presence.** Every capture's tool-call arguments arrived as a single fragment
right after the naming chunk, so the multi-fragment reassembly path is still
backed only by hand-built fixtures — probably correct, since both formats
simply concatenate fragments, but unverified against real traffic. And every
streamed chunk's `delta` carries `"role":"assistant"`, not only the first,
contradicting how the hand-built fixtures model it — zero consequence, since
`Delta` (`src/translate/openai.rs`) has no `role` field to read it into.

**A genuine provider limitation, not a translator bug:
`Qwen/Qwen2.5-7B-Instruct-Turbo`'s constrained-grammar backend fails on any
forced tool choice with a non-empty parameter schema.** Together compiles a
constrained grammar for any `tool_choice` that forces a call; this model's
grammar backend cannot compile one against a real schema. Seven controlled
probes:

| probe | result |
|---|---|
| forced specific tool + minimal one-string-property schema | **422** `failed to compile grammar` |
| forced specific tool + required/enum schema | **422** same |
| forced specific tool + **empty** `properties` | **200 OK** |
| `tool_choice: "required"` + minimal schema | **422** same |
| `tool_choice: "auto"` + required/enum schema | **200 OK** |
| `meta-llama/Llama-3.3-70B-Instruct-Turbo`, forced tool + minimal schema | **200 OK**, correct call |

(The first row is `tests/fixtures/together/A_stream_single_tool_call.raw.txt`.)

1. **Not a translator bug.** `src/translate/request.rs`'s `tool_choice`
   mapping emits the canonical OpenAI forced-tool shape
   (`{"type":"function","function":{"name":...}}`) — verified by reading it.
2. **Not a Together-wide limitation.** Llama-3.3-70B handles the identical
   request correctly.
3. **Two of the four mapped `tool_choice` modes are unusable on this specific
   model**: Anthropic `any` (→ `"required"`) and `tool` (→ a named function).
   `auto` and `none` are unaffected.
4. **The mitigation is operator model selection, not code.** Silently
   downgrading a forced tool choice to `auto` on a 422 would convert "you
   must call tool X" into "call whatever you like" — exactly the quiet
   tool-use fidelity loss the plan's Global Constraint 9 says to flag loudly
   rather than decide quietly. A clean 422 the operator can see and act on
   (by choosing a different fallback model) is better than a proxy silently
   deciding the forced choice didn't matter. No workaround added.
   `README.md` and `relay.example.toml` now name which of the two probed
   models is safe.

**A second, unrelated catalogue trap, documented alongside it for the same
reason (an operator picking a `model_map` target needs both):**
`meta-llama/Meta-Llama-3.1-8B-Instruct-Turbo` and
`mistralai/Mistral-7B-Instruct-v0.3` both appear in Together's `/v1/models`
with a price but return `400 Unable to access non-serverless model … create
and start a new dedicated endpoint`. Listed with a price does not mean
reachable.

## 2026-08-12 — Fix wave A: the whole-branch review findings that block a public `main`

Two whole-branch reviews (security and correctness lenses) produced five
items to land before this branch merges. Every one was **demonstrated live**
against a local mock — nothing here is inferred, and no probe or test in this
wave contacted a real provider.

**F1 — a cross-origin web page could make the relay spend a profile's API
key, and it needed nothing but a model name.** `install_gate` returned early
for every path outside `/control`, so `/v1/messages` had no fetch-metadata
check, no content-type check, and no other CSRF defense. `RoutingView::parse`
reads the route out of the JSON body regardless of the request's
`content-type`, and `text/plain` is CORS-safelisted — so a page could send a
body-carrying POST with **no preflight at all**, name a model matching a
profile's `serves` prefix (or just let it fall through to `active_profile`,
which is the first-request path — no route state, no `Limited`, no
`policy.mode` needed), and the mock upstream received
`authorization: Bearer <the value of api_key_env>`. Blind CSRF: the page
cannot read the response and cannot move the client's OAuth token anywhere.
The cost is the operator's provider budget, their rate limit, and log
integrity via F2.

Why it belongs to *this* branch even though `/v1/messages` predates it:
before Milestone 3 the worst a web page could do was cause unauthenticated
401s. This branch added a second credential that the relay itself attaches,
which converts the same reachability into "an attacker page spends the
operator's provider budget".

**Fixed by hoisting the `Sec-Fetch-Site`/`Origin` half of the gate above the
`/control` path check**, so it applies to every path. It costs the real
client nothing — Claude Code is not a browser and sends neither header, which
`header_is_trustworthy` already treats as acceptable — and the test that
pins *that* half matters as much as the one that pins the refusal, since
breaking the real client is the realistic way to get this fix wrong.
Deliberately **not** extended to `/v1/*`: the `content-type:
application/json` requirement, which would change behavior for real clients
on the proxy path. The origin check is sufficient on its own.

A `/control` refusal stays a bare 404 (indistinguishable from a nonexistent
route). Off `/control` it is a `403 {"error": "cross_origin_request_refused"}`
instead: `/v1/messages` is the relay's whole public purpose, its existence is
not a secret, and a 404 there would lie to a legitimate client debugging its
own setup. Rejected alternative: one uniform 404 everywhere, marginally
smaller, and dishonest on the path that matters most.

**F1's amplifier — `proxy::forwardable` forwarded upstream CORS headers
verbatim.** A mock answering `access-control-allow-origin: *` had it copied
straight to the client, on every path that uses `forwardable` for the
response (the whole Anthropic route, and the fallback route's non-2xx and
`format="anthropic"` responses). A CORS-permissive upstream would have turned
the blind CSRF above into a readable one. Not exploitable through Anthropic
today, and the translated 2xx fallback path is immune by construction, but
now stripped anyway — the whole `access-control-*` family, in both
directions, the same way `x-relay-route` already is. Whether the relay's
client may read a response is the relay's decision, not an upstream's.
Rejected alternative: naming only the two headers that matter today, which
leaves a list to keep in sync with the CORS spec.

**F2 — a request could forge log lines, because `%` does not escape.**
`tracing`'s `%` sigil records a `DisplayValue`, rendered through
`format_args!` **unescaped**; a `&str` field goes through `record_str` →
`{:?}` and *is* escaped, which is why `RequestLog::emit` was already safe.
`proxy.rs`'s `model = %model` was the one site that missed it: a body whose
`model` embedded a newline plus a synthetic record produced **two** log lines,
the second a syntactically complete forged `proxied request` entry reading
`model_in="FORGED-BY-CLIENT"`. Spec §9's per-request log is the relay's only
after-the-fact record of which route and which credential served a request,
so this is an audit-integrity failure, plus an unbounded-log-volume lever.

Fixed with the mechanism this codebase already had for exactly this rule —
`safe_identifier`, written because "block types and tool names are
client-controlled and unbounded, and these reach a log line" — **promoted out
of `translate::request` into its own `log_safety` module**, since two
unrelated layers need it now and "the proxy imports the translator to log a
model name" is the wrong dependency to teach the next reader. Rejected
alternative: dropping the sigil (`model = model.as_str()`), which restores
escaping but keeps the volume lever.

`/` and `:` were added to the permitted set. Real model names carry both
(`deepseek-ai/DeepSeek-V4`), neither can break a line or escape a quoted
field, and that log line's entire purpose is telling an operator which model
name had no route — a mangled name sends them hunting a typo that isn't
there.

**And a second half of F2 the review had ruled safe, found by the new test
failing for an unexpected reason.** `error = %err` on that same line is
indeed unforgeable (the message interpolates `{model:?}`, which escapes) —
but `router::route`'s message embedded the *entire raw* name, so the line
whose `model` field had just been bounded still carried up to
`ROUTING_BODY_CAP` of client text beside it. Clipped there too. Left open and
recorded rather than fixed: `RequestLog::emit`'s `model_in`/`model_out` are
escaped but unclipped on the success path.

**Userinfo in a profile's `base_url` is now refused — and the framing that
had it deferred three times was wrong.** The ledger called it "a second place
a real credential travels". Probed on the wire, it is not: reqwest strips
userinfo at request-build time and only synthesizes a `Basic` header when
`Authorization` is unset, which `fallback::outgoing_headers` always sets, and
it does not surface through a `reqwest::Error` either (the three
`without_url()` calls are correct hygiene but are *not* what protects this).
So this closes no disclosure. What it removes is a config field that accepts a
secret, silently discards it, and leaves it in the config file, where the only
thing between it and a log line is every future log line remembering not to
print `base_url`. **Refusing it cannot break a working deployment precisely
because the feature provably never worked** — which is the piece three
earlier deferrals were missing.

Scoped to `profiles.*.base_url` on purpose. On `anthropic.base_url` the same
userinfo is **not** inert: that route forwards the client's own headers and
does not set `Authorization` itself, so reqwest *would* synthesize `Basic`
for a request that arrived without one. That makes it both potentially
load-bearing for an operator proxying through an authenticated gateway and a
genuine second credential channel — the opposite of the argument above, and a
decision of its own rather than a copy of this one.

**D-2 — "is this host loopback" had two answers in two modules, and now has
one.** `control.rs` and `config.rs` disagreed in *both* directions:
`::ffff:127.0.0.1` was loopback to the first and not the second,
`foo.localhost` the reverse. The cause is recorded in `control.rs`'s own test
name — a review fixed the v4-mapped case there ("M5") and the fix was never
propagated. Now one `config::host_str_is_loopback` (plus the `is_loopback_ip`
primitive `enabled` needs for a parsed bind), called by both. It lives in
`config.rs` rather than a new `net` module because `config.rs` already owns
the other question that asks it and `control.rs` already depends on `config`;
the reverse would invert the layering.

"Keep the more conservative answer" does not resolve this, because
conservative points opposite ways per caller (loopback *allows* plaintext
`http` in one and *allows* the control surface in the other). Decided per row
instead:

- `::ffff:127.0.0.1` **is** loopback. `config.rs`'s old answer was factually
  wrong rather than conservative — the key does not leave the host — and a
  local mock reachable only under that spelling cannot serve https.
- `foo.localhost` is **not** loopback. The `.localhost` suffix arm was the
  only one of the four rows that erred *open*: RFC 6761 says a resolver
  *should* map it to loopback, and under one that does not, a profile's API
  key would travel cleartext off-host. It also contradicted the same
  function's own documented rule that "a name that merely *resolves* to
  loopback is still refused".

**This changes no config's control surface.** `enabled` classifies `listen`
as a parsed `SocketAddr`, so only IP literals ever reach it (`listen =
"localhost:8484"` already failed to parse and disabled `/control`), and the
`Host`/`Origin` gate never had a `.localhost` arm to lose. The only behavior
changes are on `profiles.*.base_url`: `http://[::ffff:127.0.0.1]:<port>` now
validates, and `http://foo.localhost` no longer does. Dropping the v4-mapped
arm fails four tests across *both* modules, which is the evidence the
unification is real rather than two copies that happen to agree.

## 2026-08-12 — Fix wave B: two documents that had stopped being true

**Spec §12's "no buffering of bodies" mitigation is corrected in place, not
superseded.** The risk row for "proxy becomes bottleneck under parallel
subagents" still claimed pure streaming passthrough with no body buffering. Task
3 ended that for a routable request (the `model` deciding the route is inside
the body — see the 2026-08-11 "Failover wiring" entry above), and the fallback
route buffers a non-streaming response to translate it. §12 now states what
actually happens, including the part that entry named and did not resolve:
**every buffer is capped per request and none is bounded in aggregate across
concurrent requests.** That is the branch's one memory bound with no ceiling of
any kind, in contrast to `BUFFER_CAP`, `MAX_TOOL_SLOTS`, `ERROR_BODY_CAP` and
`RESPONSE_CAP`, which all have one. Left unresolved deliberately: the realistic
trigger is a runaway local client rather than an attacker, since the listener is
loopback and single-user. Recorded here so the correction is journalled and the
two documents no longer disagree — the mitigation text was the thing that was
wrong, not the tradeoff.

**Golden files are evidence about the traffic they contain.** Every Together
capture delivered a tool call's complete `arguments` in one fragment, so the
translator's multi-fragment reassembly has no real-traffic backing — only
`src/translate/sse.rs`'s hand-built fixtures. That was already stated in
`tests/translate_together_fixtures.rs`'s module doc, which the reader of the
*test* sees; it is now also in `tests/fixtures/together/README.md`, which the
reader of the *directory* sees. Two audiences, and the directory's was the one
being left to infer coverage from the presence of files.

## 2026-08-12 — Task 6: the request that trips the limit is handed to the fallback

**Found by running a real `claude -p` session through the relay, not by any
test.** A tool-heavy session against a mock Anthropic that always reports the
subscription limit died on its very first request: the relay detected the 429,
moved `ACTIVE → LIMITED`, and returned that 429 to the client. **Claude Code
treats a subscription-limit 429 as terminal** — no retry — so it printed
`API Error: ... usage limit` and aborted, and the failover the next request
would have gotten never happened. One visible hard failure and one manual re-run
per limit window, before the relay's entire reason for existing engaged.

This was not a bug against the spec. §6's modes applied "only while the Anthropic
route is `LIMITED`", so the request that *causes* the transition was excluded by
construction; the spec simply did not anticipate the client's behavior. Nor did
the suite miss a defect it was positioned to catch — no test asserted this case
at all, because every failover test drives the route to `LIMITED` first. §6 and
§9 are amended rather than left contradicted.

**Rejected runner-up: leave the behavior alone and document the retry in the
README.** It costs no code and it works — the user re-runs the command and the
second request fails over. Rejected because it makes the product's central
promise conditional on the user knowing to retry: a relay whose stated job is to
survive the limit window would hand the user a hard error at the exact moment it
is supposed to earn its keep. A default that needs a footnote to be correct is
the wrong default.

**Why it was not a five-line change.** Detection ran inside the response-body
wrapper (`CountingStream`'s `ErrorObservation`), which fires as the body streams
past — by which time the response head is already on its way to the client and
cannot be retracted. So a response whose status `[detect]` could match is now
read whole *before* anything is handed to axum, classified there, and either
answered by the fallback or returned exactly as it stands. Constraints kept:
only a candidate status is buffered (a 2xx, SSE or not, keeps streaming;
`detect.status` is validated 4xx/5xx, and `ErrorObservation` independently
refuses to exist for a 2xx, so there are two guards); the read is bounded by the
existing `ERROR_BODY_CAP`; and an interrupted read — past the cap or a failed
stream — classifies nothing and hands the bytes it read back in front of the
rest of the response, *including the error that ended it*, so a truncated body
never arrives looking complete.

**Eligibility is one decision, not two.** `failover` now returns
`Failover::Now | OnDetect | Never` instead of `Option<String>`, so the mode, the
session-start heuristic and the existence of an `active_profile` are evaluated
exactly once and both failover forms read the same answer. A second copy of that
logic was the obvious way to write this and the one thing that could quietly
defeat `notify-only`, whose whole purpose is not switching models.
`ErrorObservation::finish` returns the classified window instead of only
recording it, which keeps one `classify` call site and one
`record(LimitDetected)` call site for both paths. `count_tokens` keeps its
Anthropic pin structurally: the arm that can arm the re-route is the one arm it
never reaches.

**Not in tension with the mid-stream prohibition**, though it sits next to it:
the decision is made while the whole response, head included, is still in the
relay's hands. Said so in §6, where a reader would otherwise think the two
conflict.

**Gated by `policy.failover_on_detect`, default `true`.** `false` restores the
old behavior exactly. The default is the new behavior because the old one is
wrong for the only client this relay serves — and the flag exists because
"never switch models on a request I did not see fail" is a legitimate
preference, distinct from `notify-only`'s "never switch at all".

One cost, accepted and documented: a request that fails over this way emits two
§9 log lines (the Anthropic attempt, then the fallback). The alternative —
suppressing the attempt's line — would hide an upstream request that really
happened from the audit trail.

## 2026-08-12 — Task 8: the fallback's reasoning stops being thrown away

Together AI returns the model's reasoning alongside its answer and the relay
discarded it: neither `ResponseMessage` nor `Delta` had a field for it, so the
operator paid for the reasoning tokens and saw none of them. Measured, not
inferred — `moonshotai/Kimi-K3` reports `completion_tokens_details.reasoning_tokens`
per turn, and its `message` keys are `['content', 'reasoning_content', 'role']`.
Both directions of the response path now translate it into an Anthropic
`thinking` block: non-streaming ahead of the `text` block, streaming as a
`content_block_start` / `thinking_delta` / `content_block_stop` run with
contiguous indices ahead of the text block's.

**Two field names, not one.** The task brief named `reasoning_content`; that is
the *outlier*. Measured across Together AI the same day, `reasoning` is what
`moonshotai/Kimi-K2.7-Code`, `Kimi-K2.6`, `deepseek-ai/DeepSeek-V4-Flash-0731`,
`DeepSeek-V4-Pro`, `zai-org/GLM-5.2`, `MiniMaxAI/MiniMax-M3` and
`openai/gpt-oss-120b` send, while only `Kimi-K3` sends `reasoning_content`. Both
are read, in both directions, through one `translate::openai::reasoning_text`.
Two struct fields rather than one with `#[serde(alias)]`: serde's derive rejects
a payload carrying both keys as a duplicate field, and failing a whole response
over a redundancy is worse than picking one — whichever is non-empty wins. Every
reasoning test runs against both spellings and there is a real capture per
spelling per direction (`tests/fixtures/together/L_`–`O_`), because a golden file
for only one name is exactly how "works on one model, silently drops the rest"
ships with a green suite. `token_id`, which rides along on the
`reasoning`-spelling providers' streamed deltas, needs no handling: none of the
wire types in `translate::openai` are `deny_unknown_fields`.

**The signature hazard, and the verdict.** Anthropic's own `thinking` blocks
carry a cryptographic signature the relay cannot produce. The relay emits **no
`signature` field at all** rather than inventing one: the Anthropic SDK reference
is explicit that a *tampered* signature is rejected, and omission is
weakly-to-strictly better than fabrication in every branch — if unsigned blocks
are tolerated it survives where garbage fails, and either way nothing forges a
cryptographic attestation.

What omission does **not** buy is a safe round trip. Observed in the live drill:
Claude Code normalizes the surfaced block into its own transcript as
`{"type": "thinking", "thinking": "…", "signature": ""}` — it supplies the empty
signature itself. So the block that would go back up carries an empty signature
no matter what the relay omits. On the fallback path that is still harmless
(translating history to OpenAI drops `thinking` blocks), but the Anthropic route
forwards the client's body verbatim by Milestone 1's design, so a session that
failed over and later recovers hands Anthropic that block. **Whether Anthropic
rejects it is unverified** — establishing it would mean spending the user's OAuth
token, which the task forbade — so it is a known unknown, and
`policy.surface_fallback_reasoning` (bool, default `true`) is the mitigation:
`false` restores the drop exactly.

**Runner-up rejected: a `text` block.** It never risks a recovered session, and
was turned down anyway because it is not a free safety win but a different
regression. `Kimi-K3`'s and `Kimi-K2.7-Code`'s reasoning is long, exploratory and
first-person (504 characters for "how many sheep remain" in the drill); folding
it into `text` makes every fallback answer read as paragraphs of rambling ahead
of the answer, with no way for a client to collapse it. That is visible damage on
**every** fallback turn, traded against an unverified 400 on the narrow subset of
sessions that fail over, recover, and still carry the turn — and that failure is
recoverable by clearing the session, not data loss. The default is the new
behavior; the flag exists because "nothing unsigned leaves this relay" is a
legitimate preference.

**Follow-up, logged not done.** The representation is not what closes the hazard;
the round trip is. Marking relay-synthesized thinking blocks and stripping them
on the way back out on the Anthropic route would end it outright — and that route
is a verbatim forward by design, so touching it is a Milestone 1 design change,
out of scope here.

**Interleaving is handled without trusting the observed order.** Reasoning
arrived strictly before content in every capture (9 fragments then 5 on
`Kimi-K3`, 37 then 7 on `Kimi-K2.7-Code`), and nothing depends on that: a
`content` fragment during an open thinking block closes it, and reasoning
arriving *after* content opens a second thinking block rather than being dropped
— a late fragment is still the user's content, which is the whole point of the
task. A chunk carrying both fields emits the thinking first.

## 2026-08-12 — Task 9B: a provider's error stops being the provider's error

Spec §7d's original rule was verbatim pass-through, and the 2026-08-11 entry above
defended it: translating an error envelope from shapes nobody had captured would
have been inventing a mapping. That reasoning was right about the *shape* and
wrong about the *consequence*, because of something not known then.

**Claude Code detects a context overflow by lowercased substring match on the
error message.** Read out of the installed 2.1.220 binary, the too-long predicate
is `.includes("prompt is too long") || .includes("input is too long for requested
model")`, a second one matches ``input length and `max_tokens` exceed context
limit``, and the two numbers come out of
`prompt is too long[^0-9]*(\d+)\s*tokens?\s*>\s*(\d+)`. Together AI, measured
through the running service at 170,071 tokens against a 131k model, answers
`400 {"error":{"message":"The input (170071 tokens) is longer than the model's
context length (131072 tokens).","type":"invalid_request_error",…}}` — which
matches **none** of the three phrases. So none of the client's compact-and-retry
fires, and the session is unrecoverable in place rather than merely rendered
badly. Anthropic's own gateway documentation names this exact failure: a gateway
that enforces a smaller context than the model's native window and rewrites the
upstream error stops the automatic compact-and-retry from firing.

It matters more than it sounds because **Claude Code sends `max_tokens: 64000`**
(verified in the captured request). On a 131k model roughly half the overflow is
the output reservation and not the transcript, so shrinking `max_tokens` alone
fixes those attempts with no compaction at all — recovery the client will do for
itself, given wording it can read.

**What lands.** `src/provider_error.rs` reads the provider's error once and
re-emits `{"type":"error","error":{type,message}}` with the status preserved and
the marker intact; §7d carries the full rule. Two shape decisions worth recording:

- **The status decides `error.type` wherever Anthropic documents one for it.** The
  provider's own type string is not a reliable signal —
  `I_error_invalid_auth.json` is a 401 whose `error.type` is
  `invalid_request_error`. Where Anthropic documents no type (Together's 422), a
  recognised provider type passes through and anything else becomes `api_error`
  rather than an invented name.
- **The token pair leads the message, and is never invented.** The extraction
  regex permits no digits between the phrase and the count, so
  `prompt is too long: 170071 tokens > 131072` comes first and the provider's
  sentence trails it — free, since the predicate is `.includes`, and the provider's
  sentence is the only thing that reported the real limit. When no usable pair can
  be parsed the phrase moves to the *end*, so digits the provider happened to send
  cannot be read as the pair; a wrong pair would make the client size its retry
  wrongly, which is worse than making it retry blind. "Usable" also means the first
  count exceeds the second: a reversed pair is not this failure's pair.

**Detection is a small Rust matcher, not `[detect]`-style config data.** The
earlier draft of the task called for following `[detect]`'s config-as-data
precedent. Rejected: `[detect]`'s rules are config because Anthropic's 429 body is
the load-bearing input to the entire product — if that wording changes the relay
stops working at all, and an operator must be able to fix it without a rebuild.
This is one provider's 400 wording on a recovery path, and the failure mode of a
wrong rule is asymmetric in the other direction. A false negative costs what
already happens today (the client cannot self-rescue); a false positive sends the
client into a pointless shrink-and-retry loop, which config-as-data makes *easier*
for an operator to cause by hand. The matcher is a `const` array of markers plus a
status gate, shaped so moving it into `[detect]` later is a mechanical change if a
second provider's wording ever makes it worth a knob. No new config key: MVP first.

**Runner-up rejected: leave errors verbatim and have the operator set
`CLAUDE_CODE_AUTO_COMPACT_WINDOW`** to the mapped model's window, so the client
compacts before it ever overflows. Rejected as *the* answer on two counts: it is
per-operator manual setup a relay user has to know to perform, and it clamps to
`min(assumed_window, configured)`, so it does nothing at all unless the operator
*also* selects a `[1m]` model ID. It is complementary, not wrong — an operator who
sets both gets pre-emptive compaction and a readable error when it is not enough.
Both knobs, and the fact that `CLAUDE_CODE_MAX_CONTEXT_TOKENS` does nothing on the
failover path, are now in the README rather than left as folklore.

**This is not the whole answer, and should not be recorded as one.** Handing the
client wording it can act on only helps where the client's own recovery can
succeed. Shrinking `max_tokens` fixes the case where the output reservation is what
overflowed; compaction fixes the case where the transcript can be summarised. When
the transcript *alone* exceeds the target's window, neither can — Anthropic
documents `Error during compaction: Conversation too long` as a real terminal state
whose only recovery is `/clear`. Escalating one rung up the model ladder (Task 9A)
is what covers that, and it keys on the same `context_limit` seam this task put in
`src/provider_error.rs`, which is why the seam is public to the crate rather than
private to the error path.

**Coverage is one provider, stated rather than implied.** Only Together's wording
is measured; the other markers are guesses at wording no provider here has been
observed using, kept narrow because a false positive is the expensive direction.
Two follow-ups logged, not done:

- **`J_error_max_tokens_exceeds_context.json` is not detected.** It is Together's
  own `inputs` + `max_new_tokens` validation error — genuinely the third phrase's
  condition (``input length and `max_tokens` exceed context limit``) — but its
  three integers do not map onto (prompt tokens, limit), so emitting a pair from it
  would be emitting a wrong one. Handling it wants that third phrase and its own
  parse, not this one stretched.
- **`error.code` is not consulted.** OpenAI-compatible providers commonly return
  `code: "context_length_exceeded"`, which would be a cheap second signal. No
  capture in this tree carries it, so it stays a guess unmade.

## 2026-08-12 — Task 9A: the ladder is the `model_map`'s own slots

Task 9B gave the client wording its own recovery keys on. This is the case that
recovery cannot reach: the transcript *by itself* does not fit the model the alias
mapped to, so shrinking `max_tokens` changes nothing and compaction needs a request
that also would not fit. Anthropic documents `Error during compaction: Conversation
too long` as a terminal state whose only recovery is `/clear` — a lost session. So a
fallback request that overflows is retried one rung up the model ladder before the
error is emitted at all (spec §7e).

**The ladder is the profile's own `model_map` slots, in an order `[policy]` names.**
The mapping decisions already encode which target is bigger — an operator who points
`claude-haiku` at a 131k model and `claude-opus` at a 1M one has *stated* the size
ordering — so escalation reuses that instead of introducing a second, separately
maintained notion of "bigger" (a per-target window table, which would then have to be
kept true against provider catalogs nobody here controls).

**Rejected: `model_map`'s own declaration order.** It is already load-bearing for
something else — §7a settles equal-length prefix ties by file order — so reusing it
would mean that swapping two lines which tie on nothing silently reorders the ladder.
`policy.escalation_order` is explicit, defaults to Anthropic's alias hierarchy
(`claude-haiku` → `claude-sonnet` → `claude-opus`), and skips slots a profile does not
define.

**Rejected as *the* answer: 9B's error translation alone, letting the client
compact.** It handles the common shape more cheaply than a second upstream request —
Claude Code reserves `max_tokens: 64000`, so on a 131k model most of a typical
overflow is the output reservation rather than the transcript, and shrinking it needs
no extra call at all. It stays the first line of defence, and the two are
complementary. What it cannot do is the case above, where nothing the client shrinks
or summarises will fit.

**`"*"` is not a rung, and the reason is stronger than "don't guess at operator
intent".** `"*"` is consulted only because no prefix matched, so its target is chosen
to be a safe answer for *anything* rather than a position in a size ordering — and on
the live map it is Kimi-K3, the largest model configured. Reading it as the bottom rung
would therefore send an overflowing request *down* to a 131k window: a guaranteed
second failure, paid for. Same rule for a target that came from no entry at all, and
for a slot `escalation_order` does not name.

**`claude-fable` is deliberately absent from the default order.** Where it sits in
Anthropic's size hierarchy is not documented, and a wrong guess is a hop to a model
that also cannot fit the prompt. With the live map (`claude-fable` = Kimi-K3, the top)
the omission costs nothing; an operator who maps it lower has to name it in
`escalation_order` themselves, which `relay.example.toml` says.

**The bound is the feature, not boilerplate, because this is the one task that spends
money.** Every hop is a whole extra upstream request at the larger model's price. Two
things make it structural rather than remembered:

- The ladder is a **cursor consumed as it walks** (`fallback::Ladder`), so "at most
  once, upward" is a property of the type — there is no position a bug could reset and
  no way to walk down.
- A rung whose target this request has already been sent to is **skipped**. The live
  map points *both* `claude-fable` and `claude-opus` at Kimi-K3, so without that a walk
  re-sends an identical request to an identical model and buys a guaranteed identical
  failure at the top rung's price.

**Never mid-response, and that is structural too.** The decision is made on the
non-2xx branch, where a provider's status has arrived and no byte of a response exists;
every path that writes to the client is downstream of it and returns. A context limit
that arrives *inside* a 200 stream is therefore not escalatable — it terminates the
stream as §6 already requires. Proving that with a mutation took three separate edits
(route a 2xx stream into the error path, widen 9B's 400/413 gate to 200, and remove
9B's authored-message-only rule), which is a fair account of how many independent
guards stand in the way.

**Where the retry goes was the load-bearing implementation choice.**
`provider_error_response` did four things in one function — read capped, redact the
profile key once on the bytes both sinks share, log them, build the envelope — and the
decision has to happen after the parse and before the envelope. It split into
`read_provider_error` (once per upstream attempt) and
`ProviderFailure::into_response`, so an escalating request logs each failure exactly
once, on that attempt's own bytes, and the redaction cannot be entered twice for the
same body or skipped for the second one.

**Two operational choices worth recording.** Each attempt gets its own §9 log line,
which spec §9 already required ("one line per *upstream* request, strictly") and the
detect-time re-route already did; and `fallback_requests_served` counts attempts, not
client requests, since an escalated request really did cost two calls and that counter
is where an operator investigating a bill looks.

**What a hop is allowed to do to the answer the client ends up with.** A retry
introduces failure modes the request did not have, and two of them are worse for the
client than not escalating at all:

- **The hop's connection fails.** Answering with the relay's own `upstream_unreachable`
  would hand back a relay-internal shape in place of an actionable Anthropic error the
  client had already earned, purely because the relay tried to help.
- **The hop answers with a status the client retries** (408, 409, 429, any 5xx).
  Measured: with the rung above answering 429, the client received a 429 in place of a
  terminal 400, retried with backoff, and **every retry re-walked the whole ladder** — a
  one-shot failure became unbounded request amplification, and nothing in the relay can
  bound it, because the loop belongs to the client.

So the rung below's failure is carried while a hop is in flight, and it is what goes out
in both cases. A hop that fails **terminally** reports itself instead, even when that is
less useful: a rung pointing at a retired `model_map` target surfaces its 404 rather than
"prompt is too long", because a terminal answer cannot amplify and masking it would hide
a misconfigured rung while the operator paid for the hop.

**The invariant is therefore narrower than the one first written here, and the first one
was false.** It is not "escalation can never leave the client with a worse answer" — one
mistyped model name in `model_map` is a counterexample. It is that escalation never
leaves the client with something it can only retry, and never with a relay-internal code
in place of the provider's own error.

**Only positive evidence of an *input* overflow may spend money — detection firing is
not that evidence.** Measured, a vLLM-shaped body reads "maximum context length is
131072 tokens. However, you requested 195000 tokens (35000 in the messages, 160000 in
the completion)": the markers match, but the transcript is 35k and fits the small model
comfortably. Only the output *reservation* does not — and that case is already fixed for
free by the client shrinking `max_tokens` on the translated error, as this entry's own
runner-up argues. Escalating it pre-empts the free fix and buys a billed inference on a
larger model, which then *succeeds* and reserves 160k output tokens at that model's
price. So the condition is a parsed token pair, which the anchored parser yields only
for a count that precedes the matched wording and exceeds the number after it.

The cost is taken knowingly, in the safe direction of the same asymmetry that decided
detection: a genuine overflow worded limit-first ("your messages resulted in 170071
tokens"), or written with thousands separators, gets the translated error and no hop —
which is what happened before this feature existed. A false negative costs that; a false
positive costs money. One residual is recorded rather than closed: a reservation wording
that happens to put a total count *before* the marker still looks like an input overflow.
Narrowing that needs a matcher for reservation wording, which would be a guess at wording
no capture here contains.
