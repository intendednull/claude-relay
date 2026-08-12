//! The `/control/*` half of Global Constraint 2: this surface must never
//! read, let alone return or log, a profile's API key *value* — only its
//! `api_key_env` name, which is not a secret — and it must not leak a second
//! credential-bearing field, a secret embedded in `base_url`, either. A separate binary
//! from `log_hygiene.rs`/`log_hygiene_fallback.rs` for the same reason those
//! two are already split: the tracing subscriber is process-global.

mod common;

use axum::http::StatusCode;
use indexmap::IndexMap;
use serde_json::{Value, json};

use common::{Buffer, relay_config, serve_relay_with};
use relay::config::ProfileConfig;

const KEYED_ENV: &str = "RELAY_TEST_LOG_HYGIENE_CONTROL_KEY";
const KEYED_SECRET: &str = "tgp-DO-NOT-LEAK-THIS-CONTROL-SECRET";
/// Deliberately never set: proves `/control/*` does not need the value to
/// exist at all, which it would if any handler read it.
const UNSET_ENV: &str = "RELAY_TEST_LOG_HYGIENE_CONTROL_KEY_NEVER_SET";
/// Embedded in a profile's `base_url` — the other credential-bearing field on
/// a profile, distinct from `api_key_env`'s value above. It sits in the *path*
/// rather than the userinfo: userinfo is refused at startup as of fix wave A
/// (`ProfileConfig::validate`), which leaves a path or query as the way a
/// configured, validation-passing `base_url` can still carry a secret — a
/// provider whose endpoint embeds a token is an ordinary shape. `ProfileView`
/// (`src/control.rs`) must therefore keep `base_url` out of its response
/// entirely, not merely redact a userinfo component.
const URL_SECRET: &str = "sk-DO-NOT-LEAK-THIS-URL-SECRET";

fn profile(base_url: String, api_key_env: &str) -> ProfileConfig {
    ProfileConfig {
        base_url,
        api_key_env: api_key_env.to_string(),
        format: "anthropic".to_string(),
        serves: Vec::new(),
        model_map: IndexMap::new(),
    }
}

async fn get(relay: std::net::SocketAddr, path: &str) -> (StatusCode, String) {
    let response = reqwest::Client::new()
        .get(format!("http://{relay}{path}"))
        .send()
        .await
        .expect("request failed");
    let status = response.status();
    let body = response.text().await.expect("failed to read body");
    (status, body)
}

async fn post(relay: std::net::SocketAddr, path: &str, body: Value) -> (StatusCode, String) {
    // `reqwest`'s `json` feature isn't enabled here (see `Cargo.toml`), and
    // `switch_profile` itself now requires this header explicitly (R1: a
    // wrong or missing content type is a CORS-preflight-defeating CSRF
    // vector, not just an inconvenience).
    let response = reqwest::Client::new()
        .post(format!("http://{relay}{path}"))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&body).expect("failed to serialize request body"))
        .send()
        .await
        .expect("request failed");
    let status = response.status();
    let body = response.text().await.expect("failed to read body");
    (status, body)
}

#[tokio::test]
async fn control_never_requires_or_leaks_a_profiles_api_key_value() {
    // SAFETY: the only write to this variable in this process, before the
    // relay that could read it exists. `UNSET_ENV` is deliberately never set.
    unsafe { std::env::set_var(KEYED_ENV, KEYED_SECRET) };

    let buffer = Buffer::new();
    tracing_subscriber::fmt()
        .with_writer(buffer.clone())
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .init();

    let anthropic = common::closed_port().await;
    let a = common::closed_port().await;
    let b = common::closed_port().await;
    let mut config = relay_config(format!("http://{anthropic}"));
    let mut profiles = IndexMap::new();
    profiles.insert(
        "keyed".to_string(),
        profile(format!("http://{a}/{URL_SECRET}"), KEYED_ENV),
    );
    profiles.insert(
        "unkeyed".to_string(),
        profile(format!("http://{b}"), UNSET_ENV),
    );
    config.profiles = profiles;
    config.policy.active_profile = Some("keyed".to_string());
    let relay = serve_relay_with(config, None).await;

    let (status, listing) = get(relay, "/control/profiles").await;
    assert_eq!(status, StatusCode::OK);
    assert!(listing.contains("keyed"));
    assert!(
        listing.contains(KEYED_ENV),
        "the env var *name* is fine: {listing}"
    );
    assert!(
        !listing.contains(KEYED_SECRET),
        "GET /control/profiles leaked the API key value: {listing}"
    );
    assert!(
        !listing.contains(URL_SECRET),
        "GET /control/profiles leaked a secret from base_url: {listing}"
    );

    // Switching to (and listing while active) the profile whose key does not
    // even exist must succeed — proof the handler never reads the value at
    // all, since reading a var that was never set would be the one way this
    // could fail.
    let (status, switch_body) = post(relay, "/control/profile", json!({"name": "unkeyed"})).await;
    assert_eq!(status, StatusCode::OK, "{switch_body}");
    assert!(!switch_body.contains(KEYED_SECRET));
    assert!(!switch_body.contains(URL_SECRET));

    let (status, listing) = get(relay, "/control/profiles").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!listing.contains(KEYED_SECRET));
    assert!(!listing.contains(URL_SECRET));

    let (status, switch_body) = post(relay, "/control/profile", json!({"name": "keyed"})).await;
    assert_eq!(status, StatusCode::OK, "{switch_body}");
    assert!(!switch_body.contains(KEYED_SECRET));
    assert!(!switch_body.contains(URL_SECRET));

    // Honesty check on the assertion below: `src/control.rs` emits no
    // `tracing` calls at all today, so nothing in this test's own request
    // path could make this buffer non-empty in the first place. This is a
    // forward guard against a future regression (someone adding a debug log
    // of a profile's full config, `base_url` included) rather than coverage
    // of anything currently logged — the response-body assertions above are
    // this test's real evidence.
    let logs = buffer.contents();
    assert!(!logs.contains(KEYED_SECRET));
    assert!(!logs.contains(URL_SECRET));
}
