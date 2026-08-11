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
///
/// `LIMITED` always carries a `limited_until` in practice — detection bounds
/// every window it produces — so the only way to `None` is a hand-edited state
/// file naming a year RFC3339 cannot express. Failing that silently is what
/// would leave an operator with a stuck route and nothing to read.
fn rfc3339(time: SystemTime) -> Option<String> {
    let rendered = time
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|since_epoch| {
            OffsetDateTime::from_unix_timestamp(since_epoch.as_secs() as i64).ok()
        })
        .and_then(|time| time.format(&Rfc3339).ok());
    if rendered.is_none() {
        tracing::warn!("route state `until` is outside the representable range");
    }
    rendered
}
