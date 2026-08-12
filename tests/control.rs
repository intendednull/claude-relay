//! `/control/*` end to end (spec §8b): listing profiles, switching the active
//! one, the 404 on an unknown name, the notifier event it fires, and the two
//! properties that get their own review regardless of this task's assurance
//! level — the mid-stream isolation guarantee and the loopback-only bind.
//! (API-key hygiene on this surface has its own dedicated file,
//! `tests/log_hygiene_control.rs`, matching `log_hygiene.rs`/
//! `log_hygiene_fallback.rs`'s one-subscriber-per-binary pattern.)

mod common;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::any;
use indexmap::IndexMap;
use serde_json::{Value, json};

use common::{gated_body, relay_config, serve, serve_relay_with, unique_temp_dir};
use relay::build_router;
use relay::config::{Config, NotifyConfig, ProfileConfig};
use relay::state::AppState;

const API_KEY_ENV: &str = "RELAY_TEST_CONTROL_PROFILE_KEY";
const BODY_A: &str = r#"{"id":"from-profile-a","type":"message","content":[]}"#;
const BODY_B: &str = r#"{"id":"from-profile-b","type":"message","content":[]}"#;
/// Claims no `serves` prefix, so routing it depends entirely on
/// `active_profile` — the property this file is testing.
const OPEN_MODEL: &str = "some-open-model/ignored";

static KEY: Once = Once::new();

/// Both profiles' `format = "anthropic"`, so `fallback::outgoing_headers`
/// needs a real value behind `API_KEY_ENV` to build outgoing headers at all —
/// this file is testing routing, not key hygiene (see
/// `tests/log_hygiene_control.rs` for that), so any value will do.
fn set_profile_key() {
    KEY.call_once(|| {
        // SAFETY: the only write to this variable in this process, done
        // before any relay in this file exists to read it.
        unsafe { std::env::set_var(API_KEY_ENV, "control-test-profile-key") };
    });
}

fn profile(base: SocketAddr) -> ProfileConfig {
    ProfileConfig {
        base_url: format!("http://{base}"),
        api_key_env: API_KEY_ENV.to_string(),
        format: "anthropic".to_string(),
        serves: Vec::new(),
        model_map: IndexMap::new(),
    }
}

fn config(anthropic: SocketAddr, profile_a: SocketAddr, profile_b: SocketAddr) -> Config {
    set_profile_key();
    let mut config = relay_config(format!("http://{anthropic}"));
    let mut profiles = IndexMap::new();
    profiles.insert("profile-a".to_string(), profile(profile_a));
    profiles.insert("profile-b".to_string(), profile(profile_b));
    config.profiles = profiles;
    config.policy.active_profile = Some("profile-a".to_string());
    config
}

fn upstream_ok(body: &'static str) -> Router {
    Router::new().route(
        "/v1/messages",
        any(move || async move {
            Response::builder()
                .header("content-type", "application/json")
                .body(Body::from(body))
                .expect("failed to build mock response")
        }),
    )
}

/// A relay with two profile mocks — "profile-a" active at startup,
/// "profile-b" not — plus an unused Anthropic mock every `Config` needs (an
/// unclaimed name never reaches it, see `OPEN_MODEL`; a closed port makes any
/// accidental hit loud rather than silently answering something).
async fn two_profile_relay() -> SocketAddr {
    two_profile_relay_with(upstream_ok(BODY_A)).await
}

async fn two_profile_relay_with(profile_a_router: Router) -> SocketAddr {
    let anthropic = common::closed_port().await;
    let a = serve(profile_a_router).await;
    let b = serve(upstream_ok(BODY_B)).await;
    serve_relay_with(config(anthropic, a, b), None).await
}

fn session(model: &str) -> String {
    format!(
        r#"{{"model":"{model}","max_tokens":16,"messages":[{{"role":"user","content":"hi"}}]}}"#
    )
}

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

async fn get_json(relay: SocketAddr, path: &str) -> Value {
    let response = client()
        .get(format!("http://{relay}{path}"))
        .send()
        .await
        .expect("request failed");
    assert_eq!(response.status(), StatusCode::OK, "GET {path}");
    serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
        .expect("response must be JSON")
}

async fn post_json(relay: SocketAddr, path: &str, body: Value) -> (StatusCode, Value) {
    // No `reqwest::RequestBuilder::json` here: the `json` feature isn't
    // enabled (`Cargo.toml`'s reqwest dependency is deliberately minimal), so
    // the body is serialized and the header set by hand — which axum's
    // `Json` extractor requires to accept the request at all.
    let response = client()
        .post(format!("http://{relay}{path}"))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&body).expect("failed to serialize request body"))
        .send()
        .await
        .expect("request failed");
    let status = response.status();
    let json = serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
        .expect("response must be JSON");
    (status, json)
}

/// Sends an `OPEN_MODEL` request and returns the parsed body — a request
/// this shape only ever reaches Anthropic if nothing claims `OPEN_MODEL` *and*
/// no profile is active, which never holds in this file's fixtures.
async fn send_open_model_request(relay: SocketAddr) -> Value {
    let response = client()
        .post(format!("http://{relay}/v1/messages"))
        .body(session(OPEN_MODEL))
        .send()
        .await
        .expect("request failed");
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
        .expect("response must be JSON")
}

fn by_name<'a>(profiles: &'a [Value], name: &str) -> &'a Value {
    profiles
        .iter()
        .find(|profile| profile["name"] == name)
        .unwrap_or_else(|| panic!("{name} missing from {profiles:?}"))
}

#[tokio::test]
async fn get_control_profiles_lists_configured_profiles_and_marks_the_active_one() {
    let relay = two_profile_relay().await;

    let body = get_json(relay, "/control/profiles").await;
    let profiles = body["profiles"]
        .as_array()
        .expect("profiles must be an array");
    assert_eq!(profiles.len(), 2);

    let a = by_name(profiles, "profile-a");
    assert_eq!(a["active"], true);
    assert_eq!(a["format"], "anthropic");
    assert_eq!(a["serves"], json!([]));
    assert_eq!(a["model_map"], json!({}));
    assert_eq!(a["api_key_env"], API_KEY_ENV);

    let b = by_name(profiles, "profile-b");
    assert_eq!(b["active"], false);
}

#[tokio::test]
async fn post_control_profile_switches_the_active_profile_and_new_requests_route_accordingly() {
    let relay = two_profile_relay().await;

    assert_eq!(
        send_open_model_request(relay).await["id"],
        "from-profile-a",
        "before any switch, an unclaimed name falls through to the startup default"
    );

    let (status, body) = post_json(relay, "/control/profile", json!({"name": "profile-b"})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["active_profile"], "profile-b");

    assert_eq!(
        send_open_model_request(relay).await["id"],
        "from-profile-b",
        "a new request after the switch must route to the newly active profile"
    );

    let listed = get_json(relay, "/control/profiles").await;
    let profiles = listed["profiles"].as_array().expect("array");
    assert_eq!(by_name(profiles, "profile-a")["active"], false);
    assert_eq!(by_name(profiles, "profile-b")["active"], true);
}

#[tokio::test]
async fn post_control_profile_404s_on_an_unknown_name_and_does_not_change_anything() {
    let relay = two_profile_relay().await;

    let (status, body) = post_json(relay, "/control/profile", json!({"name": "ghost"})).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "unknown_profile");

    let listed = get_json(relay, "/control/profiles").await;
    let profiles = listed["profiles"].as_array().expect("array");
    assert_eq!(
        by_name(profiles, "profile-a")["active"],
        true,
        "a rejected switch must leave the active profile untouched"
    );
}

/// The one genuinely tricky property (spec §8b): a switch applies to *new*
/// requests only. The second half of the mock's body is gated on a signal
/// this test only raises after the switch POST has returned — so "the tail
/// of this response was produced after the switch landed" is guaranteed by
/// construction, not by outrunning a `Duration` under CI load. The assertion
/// at the end pins the *exact* reassembled body, not merely that it contains
/// a marker string that was already true before the switch: a mutation that
/// truncates every in-flight body after the first chunk must fail this,
/// which "contains" alone would not have caught (see the fix-round notes).
#[tokio::test]
async fn an_in_flight_request_finishes_on_the_profile_it_started_on_even_if_switched_mid_stream() {
    let switch_landed = Arc::new(AtomicBool::new(false));
    let dripped = {
        let switch_landed = switch_landed.clone();
        Router::new().route(
            "/v1/messages",
            any(move || {
                let switch_landed = switch_landed.clone();
                async move {
                    Response::new(gated_body(
                        r#"{"id":"from-profile-a","#,
                        r#""type":"message","content":[]}"#,
                        switch_landed,
                    ))
                }
            }),
        )
    };
    let relay = two_profile_relay_with(dripped).await;

    let mut in_flight = client()
        .post(format!("http://{relay}/v1/messages"))
        .body(session(OPEN_MODEL))
        .send()
        .await
        .expect("request failed");
    assert_eq!(in_flight.status(), StatusCode::OK);

    // Proves the request is bound to profile-a and genuinely still streaming
    // — not merely about to be sent — before the switch below.
    let first = in_flight
        .chunk()
        .await
        .expect("read failed")
        .expect("stream ended before any chunk arrived");
    assert_eq!(first.as_ref(), br#"{"id":"from-profile-a","#);

    let (switch_status, _) =
        post_json(relay, "/control/profile", json!({"name": "profile-b"})).await;
    assert_eq!(switch_status, StatusCode::OK);

    // Only now may the mock's second chunk be produced — after the switch
    // has already returned 200, not merely likely to have by this point.
    switch_landed.store(true, Ordering::Release);

    let mut rest = Vec::new();
    while let Some(chunk) = in_flight.chunk().await.expect("read failed") {
        rest.extend_from_slice(&chunk);
    }
    let full = [first.as_ref(), rest.as_slice()].concat();
    assert_eq!(
        String::from_utf8_lossy(&full),
        r#"{"id":"from-profile-a","type":"message","content":[]}"#,
        "an in-flight request must complete on the profile it started with, whole and unaltered"
    );

    assert_eq!(
        send_open_model_request(relay).await["id"],
        "from-profile-b",
        "a request started after the switch must land on the new profile"
    );
}

/// Spec §8b: "ephemeral by design... a restart returns to
/// `policy.active_profile`." Simulated by building a second `AppState` from
/// the same `Config` rather than actually restarting the process — a runtime
/// switch lives only in the `AppState` the first one owned, never in
/// `Config` or on disk, so a fresh `AppState` from the same `Config` is
/// exactly what a real restart produces.
///
/// `state_file` is set to a real path here on purpose: the requirement under
/// test is "never written to `state_file`", which is only observable at all
/// if a state file exists for a violation to write into. With
/// `state_file: None` (this fixture's default), a version of
/// `set_active_profile` that persisted the switch straight into a fabricated
/// path off of `state_file` would leave this test unable to tell — proven by
/// mutation, see the fix-round notes.
#[tokio::test]
async fn a_runtime_switch_does_not_persist_across_a_simulated_restart() {
    let anthropic = common::closed_port().await;
    let a = serve(upstream_ok(BODY_A)).await;
    let b = serve(upstream_ok(BODY_B)).await;
    let mut cfg = config(anthropic, a, b);
    let state_file = unique_temp_dir("control-restart").with_extension("json");
    cfg.state_file = Some(state_file.clone());

    let relay = serve_relay_with(cfg.clone(), None).await;
    post_json(relay, "/control/profile", json!({"name": "profile-b"})).await;
    assert_eq!(
        get_json(relay, "/status").await["active_profile"],
        "profile-b"
    );

    let restarted = serve_relay_with(cfg, None).await;
    assert_eq!(
        get_json(restarted, "/status").await["active_profile"],
        "profile-a",
        "a fresh AppState must read policy.active_profile again, not the switched value"
    );

    let _ = std::fs::remove_file(&state_file);
}

#[tokio::test]
async fn status_reports_active_profile_and_tracks_a_switch() {
    let relay = two_profile_relay().await;

    assert_eq!(
        get_json(relay, "/status").await["active_profile"],
        "profile-a"
    );

    post_json(relay, "/control/profile", json!({"name": "profile-b"})).await;

    assert_eq!(
        get_json(relay, "/status").await["active_profile"],
        "profile-b"
    );
}

async fn wait_for_file(path: &Path, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(contents) = std::fs::read_to_string(path)
            && !contents.is_empty()
        {
            return contents;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {path:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_lines(path: &Path, count: usize) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let lines: Vec<String> = std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect();
        if lines.len() >= count {
            return lines;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {count} line(s), got {lines:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// `wait_for_lines` returns the instant it sees enough lines, so a count
/// asserted on its result cannot catch a spurious extra one landing just
/// after — this settles past that window before the final count is read, the
/// same reasoning `tests/notify.rs::hook_lines_after_settling` uses.
async fn lines_after_settling(path: &Path) -> Vec<String> {
    tokio::time::sleep(Duration::from_millis(300)).await;
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

/// Mirrors `tests/notify.rs`'s pattern for the route-transition events: a
/// hook that dumps spec §4's three env vars, run for real.
#[tokio::test]
async fn post_control_profile_fires_the_profile_switched_notifier_event() {
    let log = unique_temp_dir("control-notify").with_extension("log");
    let anthropic = common::closed_port().await;
    let a = serve(upstream_ok(BODY_A)).await;
    let b = serve(upstream_ok(BODY_B)).await;
    let mut cfg = config(anthropic, a, b);
    cfg.notify = NotifyConfig {
        command: Some(format!(
            r#"printf '%s|%s|%s' "$RELAY_EVENT" "$RELAY_RESET_AT" "$RELAY_DETAIL" > {}"#,
            log.display()
        )),
        timeout_secs: 5,
    };
    let relay = serve_relay_with(cfg, None).await;

    post_json(relay, "/control/profile", json!({"name": "profile-b"})).await;

    let line = wait_for_file(&log, Duration::from_secs(5)).await;
    assert_eq!(
        line,
        "profile_switched||active profile switched to profile-b"
    );
    let _ = std::fs::remove_file(&log);
}

/// A switch that changes nothing — the target is already active, or the name
/// is rejected before anything changes — must not queue a notifier event
/// (matches `notify.rs`'s own "only real changes are reported" rule; a hook
/// firing for a no-op switch would be a lie to whatever it tells the
/// operator). Proven by a later, real switch: the notifier drains one FIFO
/// queue serially, so anything wrongly queued by the two no-op attempts below
/// would already be in the log by the time the real switch's own line
/// appears, and `lines_after_settling` gives it a further window to show up.
#[tokio::test]
async fn a_switch_that_changes_nothing_fires_no_notifier_event() {
    let log = unique_temp_dir("control-notify-noop").with_extension("log");
    let anthropic = common::closed_port().await;
    let a = serve(upstream_ok(BODY_A)).await;
    let b = serve(upstream_ok(BODY_B)).await;
    let mut cfg = config(anthropic, a, b);
    cfg.notify = NotifyConfig {
        command: Some(format!(
            r#"printf '%s\n' "$RELAY_DETAIL" >> {}"#,
            log.display()
        )),
        timeout_secs: 5,
    };
    let relay = serve_relay_with(cfg, None).await;

    // Already-active: no real change (I2).
    let (status, _) = post_json(relay, "/control/profile", json!({"name": "profile-a"})).await;
    assert_eq!(status, StatusCode::OK);

    // Unknown name: rejected before anything changes (M8).
    let (status, _) = post_json(relay, "/control/profile", json!({"name": "ghost"})).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // The one real switch, whose notification is the only one that should exist.
    let (status, _) = post_json(relay, "/control/profile", json!({"name": "profile-b"})).await;
    assert_eq!(status, StatusCode::OK);

    wait_for_lines(&log, 1).await;
    let lines = lines_after_settling(&log).await;
    assert_eq!(
        lines,
        vec!["active profile switched to profile-b"],
        "the two no-op switches above must not have notified"
    );
    let _ = std::fs::remove_file(&log);
}

/// DNS rebinding defeats "loopback bind implies local operator only" unless
/// the control surface also checks `Host`: an attacker's own domain can
/// resolve to 127.0.0.1, making a same-origin browser request carry a `Host`
/// the relay must not trust just because the TCP connection itself arrived
/// over loopback.
#[tokio::test]
async fn control_routes_reject_a_forged_host_header() {
    let relay = two_profile_relay().await;

    let get = client()
        .get(format!("http://{relay}/control/profiles"))
        .header("host", "evil.example")
        .send()
        .await
        .expect("request failed");
    assert_eq!(get.status(), StatusCode::NOT_FOUND);

    let post = client()
        .post(format!("http://{relay}/control/profile"))
        .header("host", "evil.example")
        .header("content-type", "application/json")
        .body(r#"{"name":"profile-b"}"#)
        .send()
        .await
        .expect("request failed");
    assert_eq!(post.status(), StatusCode::NOT_FOUND);

    // The gate isn't blanket-denying: a genuine loopback Host still works.
    let profiles = get_json(relay, "/control/profiles").await;
    assert_eq!(
        profiles["profiles"].as_array().expect("array").len(),
        2,
        "an honest Host header must still reach the handler"
    );
}

/// M2: a malformed body gets the same JSON error envelope every other
/// rejection on this surface uses, not axum's default plain-text rejection.
#[tokio::test]
async fn post_control_profile_with_a_malformed_body_returns_the_json_envelope() {
    let relay = two_profile_relay().await;

    let response = client()
        .post(format!("http://{relay}/control/profile"))
        .header("content-type", "application/json")
        .body("not json at all")
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
        .expect("error body must be JSON, not axum's default plain-text rejection");
    assert_eq!(body["error"], "invalid_request_body");
}

/// M1: an unrecognized field is a client mistake worth surfacing rather than
/// silently discarding — relevant once Milestone 4 adds `POST /control/mode`,
/// so a request meant for the wrong endpoint doesn't half-succeed.
#[tokio::test]
async fn post_control_profile_rejects_an_unknown_field() {
    let relay = two_profile_relay().await;

    let response = client()
        .post(format!("http://{relay}/control/profile"))
        .header("content-type", "application/json")
        .body(r#"{"name":"profile-b","mode":"all"}"#)
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // And the switch must not have applied "the part it understood".
    let profiles = get_json(relay, "/control/profiles").await;
    assert_eq!(
        by_name(profiles["profiles"].as_array().expect("array"), "profile-a")["active"],
        true,
        "a rejected request must not partially apply"
    );
}

/// Spec §8b, code-enforced: a non-loopback `listen` must disable `/control/*`
/// outright. The relay under test is still served on loopback (`serve()`
/// below never binds anywhere else) — only `Config::listen`, the value
/// `control::enabled` reads, is set non-loopback, which is what lets this be
/// tested without the process ever actually binding a non-loopback address.
#[tokio::test]
async fn control_routes_are_absent_on_a_non_loopback_configured_listen() {
    let anthropic = common::closed_port().await;
    let a = serve(upstream_ok(BODY_A)).await;
    let b = serve(upstream_ok(BODY_B)).await;
    let mut cfg = config(anthropic, a, b);
    cfg.listen = "0.0.0.0:8484".to_string();
    let state = AppState::new(Arc::new(cfg), None, "digest".to_string()).expect("should build");
    let relay = serve(build_router(state)).await;

    let get = client()
        .get(format!("http://{relay}/control/profiles"))
        .send()
        .await
        .expect("request failed");
    assert_eq!(get.status(), StatusCode::NOT_FOUND);

    let post = client()
        .post(format!("http://{relay}/control/profile"))
        .header("content-type", "application/json")
        .body(r#"{"name":"profile-b"}"#)
        .send()
        .await
        .expect("request failed");
    assert_eq!(post.status(), StatusCode::NOT_FOUND);

    // Disabling control must not disable the rest of the relay.
    let healthz = client()
        .get(format!("http://{relay}/healthz"))
        .send()
        .await
        .expect("request failed");
    assert_eq!(healthz.status(), StatusCode::OK);
}

/// R1: last round's fix for the malformed-body error envelope (M2) dropped
/// axum's `Json` extractor, which was also enforcing `content-type:
/// application/json` — a content type that is not CORS-simple, so its
/// absence removed the CORS preflight a browser would otherwise be forced
/// into. Restoring the requirement explicitly (415 in this endpoint's own
/// envelope) is half the fix; `control_rejects_cross_origin_fetch_metadata`
/// below is the other half.
#[tokio::test]
async fn post_control_profile_requires_json_content_type() {
    let relay = two_profile_relay().await;

    // The three content types a plain HTML <form> can send without a
    // preflight, plus no content-type at all.
    for content_type in [
        Some("text/plain"),
        Some("text/plain;charset=UTF-8"),
        Some("application/x-www-form-urlencoded"),
        Some("multipart/form-data; boundary=x"),
        None,
    ] {
        let mut request = client()
            .post(format!("http://{relay}/control/profile"))
            .body(r#"{"name":"profile-b"}"#);
        if let Some(content_type) = content_type {
            request = request.header("content-type", content_type);
        }
        let response = request.send().await.expect("request failed");
        assert_eq!(
            response.status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "{content_type:?}"
        );
        let body: Value =
            serde_json::from_slice(&response.bytes().await.expect("failed to read body"))
                .expect("error body must be JSON");
        assert_eq!(body["error"], "unsupported_content_type");
    }

    // None of the above may have applied.
    let listed = get_json(relay, "/control/profiles").await;
    assert_eq!(
        by_name(listed["profiles"].as_array().expect("array"), "profile-a")["active"],
        true
    );
}

/// R1's second half: `Sec-Fetch-Site`/`Origin` are attached by the browser
/// itself and cannot be forged from page script, unlike `Host` — so unlike
/// the DNS-rebinding case, no rebinding trick is available here, and a plain
/// content-type check alone would still miss a request a browser bug or a
/// non-preflight-respecting client could produce.
#[tokio::test]
async fn control_rejects_cross_origin_fetch_metadata_and_origin() {
    let relay = two_profile_relay().await;

    let cross_site = client()
        .post(format!("http://{relay}/control/profile"))
        .header("content-type", "application/json")
        .header("sec-fetch-site", "cross-site")
        .body(r#"{"name":"profile-b"}"#)
        .send()
        .await
        .expect("request failed");
    assert_eq!(cross_site.status(), StatusCode::NOT_FOUND);

    let foreign_origin = client()
        .post(format!("http://{relay}/control/profile"))
        .header("content-type", "application/json")
        .header("origin", "http://evil.example")
        .body(r#"{"name":"profile-b"}"#)
        .send()
        .await
        .expect("request failed");
    assert_eq!(foreign_origin.status(), StatusCode::NOT_FOUND);

    // Neither header present at all: not a browser request (curl, `relay
    // ctl`, this test's own earlier requests) and must not be rejected on
    // that basis alone.
    let (status, _) = post_json(relay, "/control/profile", json!({"name": "profile-b"})).await;
    assert_eq!(status, StatusCode::OK);

    // A genuinely same-origin browser request is unaffected.
    let same_origin = client()
        .post(format!("http://{relay}/control/profile"))
        .header("content-type", "application/json")
        .header("sec-fetch-site", "same-origin")
        .header("origin", "http://127.0.0.1")
        .body(r#"{"name":"profile-a"}"#)
        .send()
        .await
        .expect("request failed");
    assert_eq!(same_origin.status(), StatusCode::OK);
}

/// The exact browser `no-cors` shape the reviewer demonstrated live against
/// the pre-fix code (`text/plain` content type, cross-site fetch metadata,
/// hitting the loopback bind directly — no DNS rebinding involved at all).
/// Rejected at the gate (404), before `switch_profile`'s own content-type
/// check ever runs, which is also why this is 404 and not 415.
#[tokio::test]
async fn the_reported_csrf_shape_is_rejected() {
    let relay = two_profile_relay().await;

    let response = client()
        .post(format!("http://{relay}/control/profile"))
        .header("origin", "http://evil.example")
        .header("sec-fetch-site", "cross-site")
        .header("sec-fetch-mode", "no-cors")
        .header("content-type", "text/plain;charset=UTF-8")
        .body(r#"{"name":"profile-b"}"#)
        .send()
        .await
        .expect("request failed");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let listed = get_json(relay, "/control/profiles").await;
    assert_eq!(
        by_name(listed["profiles"].as_array().expect("array"), "profile-a")["active"],
        true,
        "the switch must not have applied"
    );
}

/// R2's first "verify rather than assume": no path spelling should both
/// reach a control handler *and* evade the `/control` prefix the gate
/// matches on. Run against an enabled (loopback) relay, so a bypass would
/// show up as a 200 rather than being masked by the bind already being
/// disabled.
#[tokio::test]
async fn no_path_spelling_reaches_a_control_handler_while_evading_the_gate() {
    let relay = two_profile_relay().await;
    for path in [
        "/%63ontrol/profiles",  // percent-encoded 'c'
        "//control/profiles",   // doubled leading slash
        "/control/profiles/",   // trailing slash
        "/CONTROL/profiles",    // case variation
        "/Control/Profiles",    // mixed case
        "/control%2Fprofiles",  // encoded slash instead of a real one
        "/control/profiles%00", // trailing NUL
        "/control//profiles",   // doubled internal slash
    ] {
        let response = client()
            .get(format!("http://{relay}{path}"))
            .header("host", "localhost")
            .send()
            .await
            .unwrap_or_else(|err| panic!("{path}: request failed: {err}"));
        assert_ne!(
            response.status(),
            StatusCode::OK,
            "{path} must not reach a control handler (got {})",
            response.status()
        );
    }
}

/// R2's second "verify rather than assume": the path-based gate must not
/// sweep up `/status`, on a bind where `/control/*` is disabled *and* with a
/// forged `Host` — the strongest version of "this route is unaffected".
#[tokio::test]
async fn status_remains_ungated_by_the_control_gate() {
    let anthropic = common::closed_port().await;
    let a = serve(upstream_ok(BODY_A)).await;
    let b = serve(upstream_ok(BODY_B)).await;
    let mut cfg = config(anthropic, a, b);
    cfg.listen = "0.0.0.0:8484".to_string();
    let state = AppState::new(Arc::new(cfg), None, "digest".to_string()).expect("should build");
    let relay = serve(build_router(state)).await;

    let response = client()
        .get(format!("http://{relay}/status"))
        .header("host", "evil.example")
        .send()
        .await
        .expect("request failed");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "/status must not be gated by the control-only Host/bind check"
    );
}

const LIMIT_BODY: &str = r#"{"type":"error","error":{"type":"rate_limit_error","message":"You have reached your Claude Pro usage limit. Your limit will reset at 6pm."}}"#;

fn anthropic_limit_upstream() -> Router {
    Router::new().route(
        "/v1/limit",
        any(|| async {
            Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .header("content-type", "application/json")
                .header("retry-after", "3600")
                .body(Body::from(LIMIT_BODY))
                .expect("failed to build mock response")
        }),
    )
}

async fn drive_to_limited(relay: SocketAddr) {
    let response = client()
        .get(format!("http://{relay}/v1/limit"))
        .send()
        .await
        .expect("limit request failed");
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    response.bytes().await.expect("failed to read limit body");
    for _ in 0..200 {
        if get_json(relay, "/status").await["state"] == "LIMITED" {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the relay never reached LIMITED");
}

/// R3, demonstrated at the HTTP surface with the exact shape the reviewer
/// measured: alternating `POST /control/profile` switches, not the no-op
/// case I2 already closed (switching A/B/A/B... is a real change every
/// time). Each hook run sleeps, so the pre-fix single-FIFO-queue design
/// would have run every one of the 60 switches, in order, before ever
/// reaching `failover_engaged` — the reviewer measured the 100-switch
/// version of this at roughly 100 minutes. This asserts it does not
/// reproduce, with the actual elapsed time in the failure message.
#[tokio::test]
async fn a_flood_of_alternating_profile_switches_does_not_delay_failover_engaged() {
    let log = unique_temp_dir("control-flood").with_extension("log");
    let anthropic = serve(anthropic_limit_upstream()).await;
    let a = serve(upstream_ok(BODY_A)).await;
    let b = serve(upstream_ok(BODY_B)).await;
    let mut cfg = config(anthropic, a, b);
    let hook_delay_secs = 0.15;
    cfg.notify = NotifyConfig {
        command: Some(format!(
            r#"sleep {hook_delay_secs}; printf '%s\n' "$RELAY_EVENT" >> {}"#,
            log.display()
        )),
        timeout_secs: 5,
    };
    let relay = serve_relay_with(cfg, None).await;

    for i in 0..60 {
        let name = if i % 2 == 0 { "profile-a" } else { "profile-b" };
        let (status, _) = post_json(relay, "/control/profile", json!({"name": name})).await;
        assert_eq!(status, StatusCode::OK);
    }

    let started = Instant::now();
    drive_to_limited(relay).await;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let contents = std::fs::read_to_string(&log).unwrap_or_default();
        if contents.lines().any(|line| line == "failover_engaged") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "failover_engaged never arrived: {contents:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(2),
        "60 alternating profile switches (each hook sleeping {hook_delay_secs}s) \
         must not delay failover_engaged by anywhere near 60 hook executions; \
         took {elapsed:?}"
    );

    let _ = std::fs::remove_file(&log);
}
