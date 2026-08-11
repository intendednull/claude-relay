use std::sync::atomic::Ordering;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::Serialize;

use crate::route_state::{RouteState, rfc3339};
use crate::state::AppState;

#[derive(Serialize)]
pub struct StatusResponse {
    state: &'static str,
    limited_until: Option<String>,
    fallback_requests_served: u64,
    config_digest: String,
}

/// `limited_until` is null outside `LIMITED`, `PROBING` included: the window it
/// named has already elapsed by then.
pub async fn status(State(state): State<AppState>) -> Result<Json<StatusResponse>, StatusCode> {
    let route = state.route.clone();
    // A state query performs the lazy `Limited -> Probing` transition, which
    // writes the state file synchronously — off the request worker it goes.
    let current = tokio::task::spawn_blocking(move || route.current_state())
        .await
        .map_err(|err| {
            tracing::warn!(error = %err, "route state query failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let (state_label, limited_until) = match current {
        RouteState::Active => ("ACTIVE", None),
        RouteState::Limited { until } => ("LIMITED", rfc3339(until)),
        RouteState::Probing => ("PROBING", None),
    };

    Ok(Json(StatusResponse {
        state: state_label,
        limited_until,
        fallback_requests_served: state.fallback_requests_served.load(Ordering::Relaxed),
        config_digest: state.config_digest.to_string(),
    }))
}
