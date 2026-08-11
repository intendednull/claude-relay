# Milestone 1 implementation plan — transparent passthrough proxy

**Status: complete**, merged to `main` at `a4cda8f`. All 3 tasks landed,
each reviewed twice with fix rounds; two full whole-branch review rounds
(2 reviewers each) before merge. Review history lived in
`.superpowers/sdd/milestone-1-plan/` during execution — removed after
merge per the subagent-driven-development skill's convention; git history
is the record now.

Source of truth: `docs/spec.md` (the design doc), specifically §1
(Purpose), §3 (Architecture — Ingress/Router/Status only; no fallback in
this milestone), §5 (limit-detection's `--capture-errors` debug flag), §7b
(header hygiene), §8 (Configuration — subset), §9 (Observability), §10
(Testing item 1), §12 (concurrency risk row). Milestone 1's row in §11:

> Transparent passthrough proxy, streaming intact, `/status`,
> `--capture-errors` — acceptance: real session indistinguishable from
> direct; subscription billing confirmed intact.

This milestone has **no fallback routing** — every request goes to
Anthropic. Router/state-machine/failover/translation/name-based-routing
(spec §4, §6, §7a, §7c, §7d) are out of scope; do not build toward them
ahead of need.

## Global Constraints

Copy these verbatim into every task reviewer's context — they bind all
three tasks:

1. **Never log or persist request/response bodies**, except the
   `--capture-errors` fixtures (opt-in, response-only, redacted — see Task
   3). No other code path writes a body to disk or to a log line.
2. **Never log the VALUES of `Authorization`, `x-api-key`, or any
   `anthropic-*` header.** Header *names* may appear in logs; values must
   not. This applies to both the request path and the `--capture-errors`
   fixture writer.
3. **Streaming responses must not be buffered in memory.** Forward bytes
   from the upstream to the client as they arrive (`reqwest::Response::bytes_stream()`
   → the axum response body stream). No `.bytes().await` / `.text().await`
   on a response body anywhere in the request-handling path.
4. **Stack:** Rust, `tokio`, `axum`, `reqwest` (with streaming), `serde` /
   `serde_json`, `toml`, `tracing` + `tracing-subscriber`, `clap` (derive)
   for CLI args. Use `anyhow` for binary-level error handling. No other
   heavyweight dependency without a stated reason in the report.
5. **Config file** (TOML) carries ONLY what Milestone 1 uses:
   ```toml
   listen = "127.0.0.1:8484"

   [anthropic]
   base_url = "https://api.anthropic.com"
   ```
   Do **not** add `state_file`, `[profiles.*]`, `[policy]`, `[detect]`, or
   `[notify]` sections yet — those belong to later milestones (spec §8) and
   adding them now is scope creep the plan explicitly rejects.
6. **Deliberate deviation from spec §9 for this milestone:** spec §9 lists
   "model in/out" in the per-request log line. Milestone 1 has only one
   route (Anthropic) and no model-remap decision to log, so extracting the
   `model` field would mean buffering/parsing the request body for no
   present use — skip it. Log method, path, upstream status, latency_ms,
   and response byte count instead. Re-add model logging when Milestone 3
   introduces routing decisions that need it.
7. Every axum handler for `/v1/*` forwards the **full request header set to
   Anthropic verbatim**, except standard hop-by-hop headers (`Host`,
   `Content-Length`, `Transfer-Encoding`, `Connection`) which must be
   recomputed/dropped as normal for a reverse proxy. This is a denylist,
   not an allowlist — Milestone 1 has only the Anthropic route, so there is
   no third party to protect against yet (contrast with the fallback route
   in Milestone 3, which spec §7b requires to strip auth headers — not
   built here).
8. **Minimize doc comments.** Rustdoc/`///` comments and explanatory blocks
   only where a WHY is genuinely non-obvious (a subtle invariant, a
   workaround, something that would surprise a reader) — not on every
   struct/field/function. This code goes to human review; keep diffs fast
   to scan, not narrated.

## Task 1: Project setup, config, CLI, logging skeleton

Add dependencies to `Cargo.toml`: `tokio` (features: `rt-multi-thread`,
`macros`, `net`, `signal`), `axum`, `reqwest` (features: `stream`,
`rustls-tls`, default-features = false — no native OpenSSL dependency),
`serde` (`derive`), `serde_json`, `toml`, `tracing`, `tracing-subscriber`
(features: `env-filter`), `clap` (`derive`), `anyhow`.

CLI (`clap`, derive style):
- `--config <PATH>` — path to the TOML config file. If not passed, fall
  back to the `RELAY_CONFIG` env var. If neither is set, exit with a clear
  error (non-zero exit code, message to stderr) — do not silently default
  to a hardcoded path.
- `--capture-errors <DIR>` — optional. Not wired to any behavior in this
  task; just parse and store it on a shared config/state struct for Task 3
  to consume. Do not create the directory yet in this task.

Config struct: deserialize the TOML above into a `Config` struct
(`listen: String`, `anthropic: AnthropicConfig { base_url: String }`) via
`serde`. Parse `listen` as a `SocketAddr` (fail fast with a clear error on
an invalid address, at startup — not at first request).

Logging: initialize `tracing_subscriber` with an `EnvFilter` (default level
`info` if `RUST_LOG` unset), formatted output to stderr.

`main.rs` wiring: parse CLI args, load + parse config, init tracing, build
an axum `Router` with one route for now — `GET /healthz` returning `200 OK`
with body `"ok"` — bind to `config.listen`, serve with
`axum::serve` on the tokio runtime. Log a single startup line (address,
config path) — no secrets in it.

**Tests:** a config-parsing unit test (valid TOML → expected struct; missing
`[anthropic]` → parse error). An integration test that starts the server on
an ephemeral port (`127.0.0.1:0`) and asserts `GET /healthz` returns `200`
with body `"ok"`. A test that omits both `--config` and `RELAY_CONFIG` and
asserts the process reports the documented error (test the argument-parsing
logic directly rather than spawning the real process, if that's simpler).

## Task 2: Anthropic passthrough (Ingress + streaming forwarding)

Depends on Task 1's config/CLI/logging skeleton being in place.

Implement the ingress routes, all forwarding to
`{config.anthropic.base_url}{path}`:

- `POST /v1/messages` — streaming and non-streaming. Do not branch on
  whether the request is streaming; forward the request body as-is and
  stream whatever the upstream returns back to the client unchanged (a
  non-streaming JSON response is just a body that happens to arrive as one
  chunk — the same code path handles both).
- `POST /v1/messages/count_tokens` — same forwarding mechanism.
- A catch-all for any other method/path under `/v1/*` — forward verbatim.
  Anything outside `/v1/*` is not part of this proxy's surface; let axum's
  default 404 handle it.

**Forwarding mechanics:**
- Build the upstream request with `reqwest`: same method, same path+query,
  the full header set per Global Constraint 7, and the request body
  streamed through (`reqwest::Body::wrap_stream` over the incoming axum
  request body) rather than buffered — request bodies can carry large
  contexts/images.
- On the response: preserve the upstream's status code and full header set
  (same hop-by-hop exclusions as the request side), and stream the body
  through via `response.bytes_stream()` into an axum
  `Body::from_stream(...)`. Do not call `.bytes()`/`.text()` on it.
- Structured log line per request (via `tracing`, `info` level), one line,
  after the response completes: method, path, upstream status, latency_ms
  (measured from request start to response headers received — not full
  body drain, since streams can be long-lived), and total response byte
  count (accumulate as bytes pass through the stream, log when the stream
  ends). No body content, no header values for the headers named in Global
  Constraint 2.
- If the upstream connection itself fails (network error, not an HTTP
  error status), return a `502 Bad Gateway` to the client with a small JSON
  error body (`{"error": "upstream_unreachable"}` or similar) — do not
  crash the handler or leak the underlying error's details (which could
  contain connection strings) into the response.

**Tests:** stand up a lightweight mock "Anthropic" using a second axum
server on an ephemeral port (`127.0.0.1:0`) inside the test — no external
network dependency, no new test-only HTTP-mocking crate. Cover:
- Non-streaming JSON response round-trips byte-identical (mock returns a
  fixed JSON body; assert the client sees exactly that body and status).
- A simulated SSE stream (mock sends chunks with a small delay between
  each, e.g. via a `tokio::time::sleep` between writes) arrives at the
  client incrementally, not all at once — assert on a time-to-first-chunk
  measurement, not just final content, to prove it isn't buffered.
- Request and response headers pass through (set a distinctive header on
  both sides of the mock and assert it arrives).
- `Authorization`/`x-api-key` header values never appear in the captured
  `tracing` log output for a request that sends one (capture the
  subscriber's output in the test and assert the secret value string is
  absent).
- Catch-all path forwarding: an arbitrary `/v1/some-other-path` request
  reaches the mock and its response comes back unchanged.
- Concurrency: fire 10 concurrent requests at slow-streaming mock endpoints
  (each drip-fed over e.g. 200ms) and assert total wall-clock time is close
  to one request's duration, not ~10×, proving they're served concurrently
  rather than serialized.
- Upstream unreachable (point `anthropic.base_url` at a closed port) → `502`
  with no leaked internal error text in the body.

## Task 3: `/status`, `--capture-errors`, sample config, docs

Depends on Task 2.

**`GET /status`:** returns JSON:
```json
{"state": "ACTIVE", "limited_until": null, "fallback_requests_served": 0, "config_digest": "<hex>"}
```
`state` is always `"ACTIVE"` in this milestone (no state machine yet —
these fields exist so Milestone 2 can extend the same shape without a
breaking change). `config_digest` is a SHA-256 hex digest of the raw config
file bytes as loaded at startup (compute once at startup, not per-request).

**`--capture-errors <DIR>`:** when the flag is set, for every response from
the Anthropic upstream (Task 2's forwarding path) with a non-2xx status
code, write a fixture file to `<DIR>/<n>-<status>.json` where `<n>` is a
monotonically increasing counter (an `AtomicU64` shared across requests —
not a timestamp, to avoid collisions under concurrent errors). Create
`<DIR>` (and parents) if it doesn't exist, at startup if the flag is
present. Fixture contents:
```json
{"status": 429, "headers": {"retry-after": "...", "anthropic-ratelimit-...": "..."}, "body": "..."}
```
Redact only `authorization`, `x-api-key`, `set-cookie`, and `cookie`
header values (replace with `"[REDACTED]"`) — every other response header,
including `retry-after` and any `anthropic-ratelimit-*` headers, must be
written verbatim: those are exactly the fields spec §5 needs to build
limit-detection fixtures from later, so over-redacting defeats the flag's
purpose. `body` is the response body as a UTF-8 string if valid, or a
`{"body_base64": "..."}` alternate field if not. Capturing a fixture must
not prevent the response from still streaming to the client normally (tee
the stream: forward to the client AND accumulate for the fixture file,
only for non-2xx responses — 2xx responses are never captured and pass
through with zero extra overhead).

**Sample config:** add `relay.example.toml` at the repo root with the
Milestone-1 config shape (Global Constraint 5), with a comment noting later
milestones add `[profiles.*]` / `[policy]` / `[detect]` / `[notify]`
sections.

**README:** add a short "Running it" section: `cargo run -- --config
relay.example.toml`, and how to point Claude Code at it (`ANTHROPIC_BASE_URL`
in `settings.json`'s env block, per spec §1) for manual verification —
note explicitly that byte-for-byte fidelity and "indistinguishable from
direct" (spec §10 item 1) is verified by hand with a real Claude Code
session, not by the automated test suite, since it requires live
Anthropic credentials this repo doesn't have.

**Tests:** `/status` returns the documented shape and a stable
`config_digest` across two requests without a restart. `--capture-errors`:
point `anthropic.base_url` at the Task-2-style mock configured to return a
`429` with a `retry-after` header and an `authorization`-shaped header
echoed back (simulate a header that shouldn't be there to prove redaction
works even defensively) — assert the fixture file is written, the
`retry-after` value is intact, and any auth-shaped header value is
redacted. A `200` response from the mock must NOT produce a fixture file.
`/healthz` and `/status` still work correctly when `--capture-errors` is
not passed at all (feature fully optional).
