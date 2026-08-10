use axum::Json;
use axum::extract::State;
use serde::Serialize;

use crate::state::AppState;

#[derive(Serialize)]
pub struct StatusResponse {
    state: &'static str,
    limited_until: Option<String>,
    fallback_requests_served: u64,
    config_digest: String,
}

/// `limited_until` and `fallback_requests_served` have no state machine to
/// populate them yet (Milestone 2) — the fields exist now so that milestone
/// can extend this shape without a breaking change.
pub async fn status(State(state): State<AppState>) -> Json<StatusResponse> {
    Json(StatusResponse {
        state: "ACTIVE",
        limited_until: None,
        fallback_requests_served: 0,
        config_digest: state.config_digest.clone(),
    })
}
