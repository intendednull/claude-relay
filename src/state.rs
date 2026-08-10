use std::path::PathBuf;
use std::sync::Arc;

use crate::config::Config;

/// Shared application state handed to axum handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    /// Set from `--capture-errors`; not yet acted on (Task 3 wires this up).
    pub capture_errors: Option<PathBuf>,
}
