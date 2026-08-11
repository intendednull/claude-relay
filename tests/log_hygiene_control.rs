//! The `/control/*` half of Global Constraint 2: this surface must never
//! read, let alone return or log, a profile's API key *value* — only its
//! `api_key_env` name, which is not a secret. A separate binary from
//! `log_hygiene.rs`/`log_hygiene_fallback.rs` for the same reason those two
//! are already split: the tracing subscriber is process-global.

mod common;

use std::net::SocketAddr;

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

fn profile(base: SocketAddr, api_key_env: &str) -> ProfileConfig {
    ProfileConfig {
        base_url: format!("http://{base}"),
        api_key_env: api_key_env.to_string(),
        format: "anthropic".to_string(),
        serves: Vec::new(),
        model_map: IndexMap::new(),
    }
}

async fn get(relay: SocketAddr, path: &str) -> (StatusCode, String) {
    let response = reqwest::Client::new()
        .get(format!("http://{relay}{path}"))
        .send()
        .await
        .expect("request failed");
    let status = response.status();
    let body = response.text().await.expect("failed to read body");
    (status, body)
}

async fn post(relay: SocketAddr, path: &str, body: Value) -> (StatusCode, String) {
    // `reqwest`'s `json` feature isn't enabled here (see `Cargo.toml`), and
    // axum's `Json` extractor requires the content-type header regardless.
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
    profiles.insert("keyed".to_string(), profile(a, KEYED_ENV));
    profiles.insert("unkeyed".to_string(), profile(b, UNSET_ENV));
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

    // Switching to (and listing while active) the profile whose key does not
    // even exist must succeed — proof the handler never reads the value at
    // all, since reading a var that was never set would be the one way this
    // could fail.
    let (status, switch_body) = post(relay, "/control/profile", json!({"name": "unkeyed"})).await;
    assert_eq!(status, StatusCode::OK, "{switch_body}");
    assert!(!switch_body.contains(KEYED_SECRET));

    let (status, listing) = get(relay, "/control/profiles").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!listing.contains(KEYED_SECRET));

    let (status, switch_body) = post(relay, "/control/profile", json!({"name": "keyed"})).await;
    assert_eq!(status, StatusCode::OK, "{switch_body}");
    assert!(!switch_body.contains(KEYED_SECRET));

    let logs = buffer.contents();
    assert!(
        !logs.contains(KEYED_SECRET),
        "captured logs leaked the API key value:\n{logs}"
    );
}
