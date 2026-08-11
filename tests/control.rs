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
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::any;
use indexmap::IndexMap;
use serde_json::{Value, json};

use common::{dripped_body, relay_config, serve, serve_relay_with, unique_temp_dir};
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
/// requests only. Built as a real race rather than an assertion on final
/// state — the mock upstream paces its body over real wall-clock time so the
/// switch genuinely lands while the first request is still receiving bytes,
/// not merely queued behind it.
#[tokio::test]
async fn an_in_flight_request_finishes_on_the_profile_it_started_on_even_if_switched_mid_stream() {
    let chunk_delay = Duration::from_millis(200);
    let dripped = Router::new().route(
        "/v1/messages",
        any(move || async move {
            Response::new(dripped_body(
                vec![
                    r#"{"id":"from-profile-a","#,
                    r#""type":"message","content":[]}"#,
                ],
                chunk_delay,
            ))
        }),
    );
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
    assert!(String::from_utf8_lossy(&first).contains("from-profile-a"));

    let (switch_status, _) =
        post_json(relay, "/control/profile", json!({"name": "profile-b"})).await;
    assert_eq!(switch_status, StatusCode::OK);

    let mut rest = Vec::new();
    while let Some(chunk) = in_flight.chunk().await.expect("read failed") {
        rest.extend_from_slice(&chunk);
    }
    let full = [first.as_ref(), rest.as_slice()].concat();
    assert!(
        String::from_utf8_lossy(&full).contains("from-profile-a"),
        "an in-flight request must complete on the profile it started with: {}",
        String::from_utf8_lossy(&full)
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
#[tokio::test]
async fn a_runtime_switch_does_not_persist_across_a_simulated_restart() {
    let anthropic = common::closed_port().await;
    let a = serve(upstream_ok(BODY_A)).await;
    let b = serve(upstream_ok(BODY_B)).await;
    let cfg = config(anthropic, a, b);

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
