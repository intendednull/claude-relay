use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::capture::Capture;
use crate::config::Config;
use crate::route_state::RouteStateMachine;
use crate::route_updates::RouteUpdates;

/// Bounds connection establishment only. There is deliberately no overall
/// timeout: a streamed response stays open for as long as the model generates,
/// but an upstream that blackholes SYNs must still fail into a 502.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Bounds silence *after* the connection is up, which `CONNECT_TIMEOUT` cannot:
/// an upstream that accepts and then says nothing would otherwise hang the
/// client forever. It applies in two different shapes, and the second is why
/// this value is generous rather than snappy:
///
/// - Until response headers arrive it is a single deadline that does not reset,
///   so it also caps time-to-first-byte. A non-streaming request holds its
///   headers until generation finishes, so a short value here would fail
///   requests a direct connection would have served — the one thing this proxy
///   must not do. 10 minutes is Anthropic's own ceiling for a non-streaming
///   request, so nothing the API itself would allow can trip this.
/// - While the body streams it resets on every frame received, so an SSE stream
///   keeps it at bay with its own keepalives.
///
/// A request dying at almost exactly this mark is this constant, not the network.
const READ_TIMEOUT: Duration = Duration::from_secs(600);

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
    /// Read by `/status`; written only through `route_updates`, so the request
    /// path never blocks on the state file.
    pub route: Arc<RouteStateMachine>,
    pub route_updates: RouteUpdates,
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

        let route = Arc::new(RouteStateMachine::new(config.state_file()?)?);
        let route_updates = RouteUpdates::spawn(route.clone());

        Ok(Self {
            config,
            capture,
            config_digest: config_digest.into(),
            http,
            route,
            route_updates,
        })
    }
}
