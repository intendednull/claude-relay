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
use axum::routing::{any, get, post};

use crate::state::AppState;

pub fn build_router(state: AppState) -> Router {
    let mut router = Router::new()
        .route("/healthz", get(healthz))
        .route("/status", get(status::status))
        .route("/v1/messages", any(proxy::forward))
        .route("/v1/messages/count_tokens", any(proxy::forward))
        .route("/v1/{*rest}", any(proxy::forward));

    // Code-enforced per spec §8b: registering these routes at all, rather
    // than registering them and checking inside each handler, means a
    // non-loopback bind gets axum's ordinary 404 for an unmatched path —
    // nothing here can accidentally say more than that a route doesn't exist.
    if control::enabled(&state.config) {
        router = router
            .route("/control/profiles", get(control::list_profiles))
            .route("/control/profile", post(control::switch_profile));
    }

    router.with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}
