pub mod capture;
pub mod cli;
pub mod config;
pub(crate) mod control;
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
    let router = Router::new()
        .route("/healthz", get(healthz))
        .route("/status", get(status::status))
        .route("/v1/messages", any(proxy::forward))
        .route("/v1/messages/count_tokens", any(proxy::forward))
        .route("/v1/{*rest}", any(proxy::forward))
        .merge(control::routes());
    // Applied last, over the whole router by request path — not scoped to
    // `control::routes()`'s own sub-router — so a `/control/*` route
    // registered anywhere, including one added here later, inherits the
    // gate automatically instead of depending on being registered through
    // `control::routes()` specifically (`control.rs`'s module doc).
    control::install_gate(router, &state.config).with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}
