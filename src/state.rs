use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::config::Config;

/// Bounds connection establishment only. There is deliberately no overall
/// timeout: a streamed response stays open for as long as the model generates,
/// but an upstream that blackholes SYNs must still fail into a 502.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Shared application state handed to axum handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    /// Set from `--capture-errors`; not yet acted on (Task 3 wires this up).
    pub capture_errors: Option<PathBuf>,
    pub http: reqwest::Client,
}

impl AppState {
    pub fn new(config: Arc<Config>, capture_errors: Option<PathBuf>) -> Result<Self> {
        let http = reqwest::Client::builder()
            // A proxy hands 3xx back to its client rather than chasing it: the
            // request body is streamed, so it cannot be replayed on a redirect.
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .context("failed to build the upstream HTTP client")?;

        Ok(Self {
            config,
            capture_errors,
            http,
        })
    }
}
