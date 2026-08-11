use std::time::{SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::route_state::RouteState;
use crate::state::AppState;

#[derive(Serialize)]
pub struct StatusResponse {
    state: &'static str,
    limited_until: Option<String>,
    fallback_requests_served: u64,
    config_digest: String,
}

/// `fallback_requests_served` stays 0 until there is fallback routing to count
/// (Milestone 3). `limited_until` is null outside `LIMITED`, `PROBING`
/// included: the window it named has already elapsed by then.
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
        fallback_requests_served: 0,
        config_digest: state.config_digest.to_string(),
    }))
}

/// Whole seconds, because that is the resolution `state_file` persists: any
/// finer and the same window reads differently before and after a restart.
fn rfc3339(time: SystemTime) -> Option<String> {
    let secs = time.duration_since(UNIX_EPOCH).ok()?.as_secs();
    OffsetDateTime::from_unix_timestamp(secs as i64)
        .ok()?
        .format(&Rfc3339)
        .ok()
}
