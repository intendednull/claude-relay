pub mod cli;
pub mod config;
pub mod state;

use axum::Router;
use axum::routing::get;

use crate::state::AppState;

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}
