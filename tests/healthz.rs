use std::sync::Arc;

use relay::build_router;
use relay::config::{AnthropicConfig, Config};
use relay::state::AppState;

#[tokio::test]
async fn healthz_returns_200_ok() {
    let config = Config {
        listen: "127.0.0.1:0".to_string(),
        anthropic: AnthropicConfig {
            base_url: "https://api.anthropic.com".to_string(),
        },
    };
    let state = AppState::new(Arc::new(config), None, "test-digest".to_string())
        .expect("failed to build app state");
    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind ephemeral port");
    let addr = listener.local_addr().expect("failed to read local addr");

    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server error");
    });

    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{addr}/healthz"))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status(), 200);
    let body = response.text().await.expect("failed to read body");
    assert_eq!(body, "ok");
}
