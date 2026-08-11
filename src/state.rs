use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::capture::Capture;
use crate::config::Config;

/// Bounds connection establishment only. There is deliberately no overall
/// timeout: a streamed response stays open for as long as the model generates,
/// but an upstream that blackholes SYNs must still fail into a 502.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Bounds silence *after* the connection is up, which `CONNECT_TIMEOUT` cannot:
/// an upstream that accepts and then says nothing would otherwise hang the
/// client forever. Unlike an overall timeout this one resets on every byte, so
/// an SSE stream keeps it at bay with its own keepalives.
const READ_TIMEOUT: Duration = Duration::from_secs(90);

/// Shared application state handed to axum handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    /// Set when `--capture-errors <DIR>` was passed; `None` if the flag is absent.
    pub capture: Option<Capture>,
    /// `Arc<str>`, not `String`: axum clones this state on every proxied
    /// request, and only `/status` ever reads the digest.
    pub config_digest: Arc<str>,
    pub http: reqwest::Client,
}

impl AppState {
    pub fn new(
        config: Arc<Config>,
        capture_errors: Option<PathBuf>,
        config_digest: String,
    ) -> Result<Self> {
        let http = reqwest::Client::builder()
            // A proxy hands 3xx back to its client rather than chasing it: the
            // request body is streamed, so it cannot be replayed on a redirect.
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .build()
            .context("failed to build the upstream HTTP client")?;

        let capture = capture_errors.map(Capture::new).transpose()?;

        Ok(Self {
            config,
            capture,
            config_digest: config_digest.into(),
            http,
        })
    }
}
