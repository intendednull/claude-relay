use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::capture::Capture;
use crate::config::Config;
use crate::notify::Notifier;
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
        // Here rather than in `main`, so validation is a property of building a
        // relay: every embedding — the test harness included — gets the same
        // rejections the binary does.
        config.detect.validate()?;
        config.notify.validate()?;
        for (name, profile) in &config.profiles {
            profile
                .validate()
                .with_context(|| format!("profile {name:?}"))?;
        }
        config.policy.validate(&config.profiles)?;

        let http = reqwest::Client::builder()
            // A proxy hands 3xx back to its client rather than chasing it: the
            // request body is streamed, so it cannot be replayed on a redirect.
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .build()
            .context("failed to build the upstream HTTP client")?;

        let capture = capture_errors.map(Capture::new).transpose()?;

        let route = Arc::new(RouteStateMachine::new(
            config.state_file()?,
            config.policy.reset_jitter_secs,
        )?);
        let route_updates = RouteUpdates::spawn(route.clone(), Notifier::spawn(&config.notify));

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AnthropicConfig, NotifyConfig, PolicyConfig};
    use crate::detect::DetectConfig;
    use indexmap::IndexMap;

    fn config(detect: DetectConfig, notify: NotifyConfig) -> Config {
        Config {
            listen: "127.0.0.1:0".to_string(),
            state_file: None,
            anthropic: AnthropicConfig {
                base_url: "https://api.anthropic.com".to_string(),
            },
            detect,
            notify,
            profiles: IndexMap::new(),
            policy: PolicyConfig::default(),
        }
    }

    /// Why validation belongs here and not only in `main`: both of these rules
    /// fail silently when they are wrong, and anything that builds an `AppState`
    /// directly — the integration harness included — used to skip them entirely.
    #[test]
    fn a_config_that_could_never_work_is_rejected_at_construction() {
        let dead_detection = config(
            DetectConfig {
                status: 200,
                ..DetectConfig::default()
            },
            NotifyConfig::default(),
        );
        let err = AppState::new(Arc::new(dead_detection), None, "digest".to_string())
            .err()
            .expect("a 2xx detect status leaves detection silently dead");
        assert!(err.to_string().contains("detect.status"), "{err}");

        let unrunnable_hook = config(
            DetectConfig::default(),
            NotifyConfig {
                command: Some("notify-send hi".to_string()),
                timeout_secs: 0,
            },
        );
        let err = AppState::new(Arc::new(unrunnable_hook), None, "digest".to_string())
            .err()
            .expect("a zero timeout kills the hook before it can do anything");
        assert!(err.to_string().contains("timeout_secs"), "{err}");
    }

    /// `policy.active_profile` naming a profile that doesn't exist is another
    /// rule that fails silently if left to the request path: the router would
    /// have nothing to fall through to and every non-`claude-*` request would
    /// 500 with no indication why.
    #[test]
    fn an_active_profile_naming_no_configured_profile_is_rejected_at_construction() {
        let mut bad_config = config(DetectConfig::default(), NotifyConfig::default());
        bad_config.policy.active_profile = Some("ghost".to_string());
        let err = AppState::new(Arc::new(bad_config), None, "digest".to_string())
            .err()
            .expect("an unconfigured active_profile must not silently build a relay");
        assert!(err.to_string().contains("active_profile"), "{err}");
    }

    #[test]
    fn the_default_rules_build_a_relay() {
        assert!(
            AppState::new(
                Arc::new(config(DetectConfig::default(), NotifyConfig::default())),
                None,
                "digest".to_string(),
            )
            .is_ok()
        );
    }
}
