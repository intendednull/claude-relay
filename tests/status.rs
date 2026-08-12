mod common;

use std::io::Write;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use common::{serve, serve_relay, unique_temp_dir};
use relay::build_router;
use relay::config::Config;
use relay::state::AppState;

fn write_temp_config(raw: &str) -> std::path::PathBuf {
    let path = unique_temp_dir("status-test").with_extension("toml");
    let mut file = std::fs::File::create(&path).expect("failed to create temp config file");
    file.write_all(raw.as_bytes())
        .expect("failed to write temp config file");
    path
}

#[tokio::test]
async fn status_returns_documented_shape_with_stable_digest() {
    let raw = r#"
        listen = "127.0.0.1:0"

        [anthropic]
        base_url = "https://api.anthropic.com"
    "#;
    let path = write_temp_config(raw);

    let loaded = Config::load(&path).expect("failed to load temp config");
    let raw_bytes = std::fs::read(&path).expect("failed to read temp config");
    let expected_digest = Sha256::digest(raw_bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(
        loaded.digest, expected_digest,
        "config_digest must be the SHA-256 of the raw config file bytes"
    );

    let state = AppState::new(Arc::new(loaded.config), None, loaded.digest.clone())
        .expect("failed to build app state");
    let addr = serve(build_router(state)).await;
    let client = reqwest::Client::new();

    for _ in 0..2 {
        let response = client
            .get(format!("http://{addr}/status"))
            .send()
            .await
            .expect("request failed");
        assert_eq!(response.status(), 200);

        let bytes = response
            .bytes()
            .await
            .expect("failed to read response body");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("invalid json body");
        assert_eq!(body["state"], "ACTIVE");
        assert!(body["limited_until"].is_null());
        assert_eq!(body["fallback_requests_served"], 0);
        assert_eq!(
            body.get("active_profile"),
            Some(&serde_json::Value::Null),
            "active_profile must be present and null, not merely absent, \
             with no profiles configured and no policy.active_profile set"
        );
        assert_eq!(body["config_digest"], expected_digest);
    }

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn healthz_and_status_work_without_capture_errors() {
    let relay = serve_relay("https://api.anthropic.com".to_string()).await;
    let client = reqwest::Client::new();

    let healthz = client
        .get(format!("http://{relay}/healthz"))
        .send()
        .await
        .expect("healthz request failed");
    assert_eq!(healthz.status(), 200);

    let status = client
        .get(format!("http://{relay}/status"))
        .send()
        .await
        .expect("status request failed");
    assert_eq!(status.status(), 200);
    let bytes = status.bytes().await.expect("failed to read response body");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("invalid json body");
    assert_eq!(body["state"], "ACTIVE");
}
