pub mod cli;
pub mod config;
pub mod proxy;
pub mod state;

use axum::Router;
use axum::routing::{any, get};

use crate::state::AppState;

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/messages", any(proxy::forward))
        .route("/v1/messages/count_tokens", any(proxy::forward))
        .route("/v1/{*rest}", any(proxy::forward))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}
