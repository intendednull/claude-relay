pub mod capture;
pub mod cli;
pub mod config;
pub(crate) mod control;
pub mod detect;
pub mod fallback;
pub mod log_file;
pub(crate) mod log_safety;
pub mod notify;
pub(crate) mod provider_error;
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

/// Every route the relay serves, `/control/*` included — deliberately
/// separate from `build_router` so that adding a route means editing *this*
/// function, which is inside the gated region by construction (`build_router`
/// applies `install_gate` to whatever this returns). There is no gate to get
/// wrong here: the wrong place to add a route is chaining onto
/// `build_router`'s *result*, after the gate has already been applied — see
/// its doc comment.
fn app_routes() -> Router<AppState> {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/status", get(status::status))
        .route("/v1/messages", any(proxy::forward))
        .route("/v1/messages/count_tokens", any(proxy::forward))
        .route("/v1/{*rest}", any(proxy::forward))
        .merge(control::routes())
}

/// `install_gate` must be the *last* operation performed on the router
/// before `.with_state` — it is a `tower` layer, and a layer only wraps
/// routes that existed in the chain before it was applied. A route added by
/// calling `.route(...)` on *this function's return value* would not pass
/// through it and would reach its handler ungated, on any bind, with any
/// `Host` — that is not a hypothetical, it was demonstrated live against an
/// earlier version of this comment that claimed otherwise (`docs/decisions.md`).
/// Add new routes to `app_routes` instead, which is inside the gated region
/// by construction; there is nothing to remember there.
///
/// That advice is for code inside this crate only: `control` is `pub(crate)`
/// and `app_routes` is private, so an external consumer of this crate as a
/// library cannot see either one. Such a consumer must not append a
/// `/control/*` route to this function's return value at all — there is no
/// way from outside the crate to re-apply the gate to it.
pub fn build_router(state: AppState) -> Router {
    control::install_gate(app_routes(), &state.config).with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}
