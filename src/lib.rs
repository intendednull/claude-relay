pub mod capture;
pub mod cli;
pub mod config;
pub mod control;
pub mod detect;
pub mod fallback;
pub mod notify;
pub mod proxy;
pub mod route_state;
pub mod route_updates;
pub mod router;
pub mod state;
pub mod status;
pub mod translate;

use axum::Router;
use axum::routing::{any, get};

use crate::state::AppState;

pub fn build_router(state: AppState) -> Router {
    // `control::routes` owns its own gating (spec §8b: loopback bind and
    // loopback `Host`) and returns an empty router when disabled, so merging
    // it in unconditionally is what keeps that gate reachable-by-construction
    // rather than a condition this function has to remember to check.
    Router::new()
        .route("/healthz", get(healthz))
        .route("/status", get(status::status))
        .route("/v1/messages", any(proxy::forward))
        .route("/v1/messages/count_tokens", any(proxy::forward))
        .route("/v1/{*rest}", any(proxy::forward))
        .merge(control::routes(&state.config))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}
