use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex};
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
    /// The other end of the same channel `route_updates` feeds into on a
    /// transition; kept here too so `/control/profile` can fire
    /// `profile_switched` directly, without a fake `RouteTransition` to
    /// smuggle it through `route_updates`.
    pub notifier: Notifier,
    /// Spec §9's `/status` counter: requests a fallback profile answered,
    /// whatever it answered with. A request that never reached one — an
    /// untranslatable body, a missing key, an unreachable endpoint — is not
    /// one the fallback served.
    pub fallback_requests_served: Arc<AtomicU64>,
    /// Whether the fallback route is currently failing in a way the operator has
    /// already been told about — spec §4's `fallback_error` is edge-triggered on
    /// this flag rather than fired per failed request, because the failures it
    /// exists for (a dead key, an unreachable provider, an exhausted balance)
    /// fail *every* request and one hook run per request is unusable. Set by the
    /// first route-attributable failure, cleared only by a 2xx
    /// (`fallback::forward` owns both, and is the only place either happens).
    ///
    /// Global rather than per profile: only one profile is active at a time
    /// (`policy.active_profile` plus `/control/profile`'s override), so a
    /// per-profile flag would model a distinction the request path does not
    /// have. Per-process rather than persisted to the state file: a restart
    /// re-arms it, which is also when an operator is most likely to be looking.
    pub fallback_failing: Arc<AtomicBool>,
    /// How much of a request body may be read to decide its route. A field
    /// rather than a constant read in place, so a test can drive the over-cap
    /// path — and the hand-rolled stream reassembly behind it — without an
    /// 8 MiB fixture. Deliberately not a config key: no deployment should need
    /// it changed.
    pub routing_body_cap: usize,
    /// Runtime override for `policy.active_profile`, set by
    /// `POST /control/profile` (spec §8b). `None` until a switch happens, so a
    /// request falls through to the startup default until then; never
    /// persisted, so a restart drops it and reads `policy.active_profile`
    /// fresh — "ephemeral by design", per spec. Private, unlike every other
    /// field here: the only way to read or write it is `active_profile`/
    /// `set_active_profile`, which is what makes "read once per request, at
    /// routing time" a property of the API rather than a convention callers
    /// have to remember.
    active_profile_override: Arc<Mutex<Option<String>>>,
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
        let notifier = Notifier::spawn(&config.notify);
        let route_updates = RouteUpdates::spawn(route.clone(), notifier.clone());

        Ok(Self {
            config,
            capture,
            config_digest: config_digest.into(),
            http,
            route,
            route_updates,
            notifier,
            fallback_requests_served: Arc::new(AtomicU64::new(0)),
            fallback_failing: Arc::new(AtomicBool::new(false)),
            routing_body_cap: crate::proxy::ROUTING_BODY_CAP,
            active_profile_override: Arc::new(Mutex::new(None)),
        })
    }

    /// The profile a request routes against right now (spec §8b): the
    /// runtime switch if `/control/profile` has made one, else
    /// `policy.active_profile`, the startup default. Callers must read this
    /// once per request at routing time and hold onto the result rather than
    /// re-consulting it — that is what keeps an in-flight stream on the
    /// profile it started with even if a switch lands mid-stream.
    pub fn active_profile(&self) -> Option<String> {
        self.active_profile_override
            .lock()
            .expect("active profile lock poisoned")
            .clone()
            .or_else(|| self.config.policy.active_profile.clone())
    }

    /// Sets the runtime override read by `active_profile`, returning whether
    /// the *effective* active profile actually changed. Trusts `name` the
    /// same way `router::route` trusts its `active_profile` parameter: the
    /// caller — `/control/profile`'s 404 check — has already validated it
    /// names a configured profile, so this never re-checks.
    ///
    /// The return value matters: `/control/profile` fires a `profile_switched`
    /// notification only when this is `true`, matching the notifier's own
    /// "only real changes are reported" rule (`notify.rs`) — switching to the
    /// name that is already active must not queue an event.
    #[must_use]
    pub fn set_active_profile(&self, name: String) -> bool {
        let mut guard = self
            .active_profile_override
            .lock()
            .expect("active profile lock poisoned");
        let previous = guard
            .clone()
            .or_else(|| self.config.policy.active_profile.clone());
        let changed = previous.as_deref() != Some(name.as_str());
        *guard = Some(name);
        changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AnthropicConfig, NotifyConfig, PolicyConfig};
    use crate::detect::DetectConfig;
    use crate::route_state::RouteState;
    use indexmap::IndexMap;
    use std::time::SystemTime;

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

    /// Both `RouteStateMachine::new` and `add_jitter` are unit-tested with
    /// explicit bounds passed directly, which proves the arithmetic but not
    /// that `AppState::new` actually threads `config.policy.reset_jitter_secs`
    /// through to the machine it constructs — a hardcoded `[15, 60]` left in
    /// place of that wiring would leave every one of those tests green.
    #[test]
    fn appstate_new_wires_reset_jitter_secs_into_the_route_state_machine() {
        let mut wired = config(DetectConfig::default(), NotifyConfig::default());
        wired.policy.reset_jitter_secs = [1000, 1000];
        let state =
            AppState::new(Arc::new(wired), None, "digest".to_string()).expect("should build");

        let reset_at = SystemTime::now();
        let transitioned = state
            .route
            .on_limit_detected(reset_at)
            .expect("Active -> Limited must report a transition");
        let RouteState::Limited { until } = transitioned.to else {
            panic!("expected Limited, got {:?}", transitioned.to);
        };
        let delta = until.duration_since(reset_at).unwrap().as_secs();
        assert_eq!(
            delta, 1000,
            "policy.reset_jitter_secs must reach the constructed RouteStateMachine, not just \
             the [15, 60] default"
        );
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

    fn profile() -> crate::config::ProfileConfig {
        crate::config::ProfileConfig {
            base_url: "https://example.com".to_string(),
            api_key_env: "RELAY_KEY".to_string(),
            format: "openai".to_string(),
            serves: Vec::new(),
            model_map: IndexMap::new(),
        }
    }

    /// Before any switch, `active_profile()` reads the startup default —
    /// proving the fallthrough half of spec §8b, not just the override half.
    #[test]
    fn active_profile_falls_through_to_the_policy_default_before_any_switch() {
        let mut wired = config(DetectConfig::default(), NotifyConfig::default());
        wired.profiles.insert("deepseek".to_string(), profile());
        wired.policy.active_profile = Some("deepseek".to_string());
        let state =
            AppState::new(Arc::new(wired), None, "digest".to_string()).expect("should build");

        assert_eq!(state.active_profile(), Some("deepseek".to_string()));
    }

    /// The override half: once set, it wins over the startup default, and
    /// stays in effect across repeated reads — a runtime switch is not a
    /// one-shot signal.
    #[test]
    fn set_active_profile_overrides_the_policy_default() {
        let mut wired = config(DetectConfig::default(), NotifyConfig::default());
        wired.profiles.insert("deepseek".to_string(), profile());
        wired.profiles.insert("kimi".to_string(), profile());
        wired.policy.active_profile = Some("deepseek".to_string());
        let state =
            AppState::new(Arc::new(wired), None, "digest".to_string()).expect("should build");

        let _ = state.set_active_profile("kimi".to_string());

        assert_eq!(state.active_profile(), Some("kimi".to_string()));
        assert_eq!(
            state.active_profile(),
            Some("kimi".to_string()),
            "a second read must see the same switch, not consume it"
        );
    }

    /// `/control/profile` fires a notification only when this returns `true`
    /// (`src/control.rs`) — so it has to be right for both the policy-default
    /// baseline and the override baseline, not just distinguish "changed" from
    /// "unchanged" in one of them.
    #[test]
    fn set_active_profile_reports_whether_it_actually_changed() {
        let mut wired = config(DetectConfig::default(), NotifyConfig::default());
        wired.profiles.insert("deepseek".to_string(), profile());
        wired.profiles.insert("kimi".to_string(), profile());
        wired.policy.active_profile = Some("deepseek".to_string());
        let state =
            AppState::new(Arc::new(wired), None, "digest".to_string()).expect("should build");

        assert!(
            !state.set_active_profile("deepseek".to_string()),
            "switching to the already-active startup default is not a change"
        );
        assert!(
            state.set_active_profile("kimi".to_string()),
            "switching to a different profile is a change"
        );
        assert!(
            !state.set_active_profile("kimi".to_string()),
            "switching to the already-active override is not a change"
        );
    }

    /// `AppState::clone()` is how axum hands state to every handler; a switch
    /// made through one clone (a request on one worker) must be visible from
    /// another (a request on a different worker) — which only holds if the
    /// override is shared state, not a plain field copied by `derive(Clone)`.
    #[test]
    fn a_switch_is_visible_through_a_cloned_appstate() {
        let mut wired = config(DetectConfig::default(), NotifyConfig::default());
        wired.profiles.insert("deepseek".to_string(), profile());
        let state =
            AppState::new(Arc::new(wired), None, "digest".to_string()).expect("should build");
        let cloned = state.clone();

        let _ = cloned.set_active_profile("deepseek".to_string());

        assert_eq!(state.active_profile(), Some("deepseek".to_string()));
    }

    /// No profile configured at all and no startup default is a valid state
    /// (a zero-profile relay), and must read as `None`, not panic.
    #[test]
    fn active_profile_is_none_with_nothing_configured_or_switched() {
        let state = AppState::new(
            Arc::new(config(DetectConfig::default(), NotifyConfig::default())),
            None,
            "digest".to_string(),
        )
        .expect("should build");

        assert_eq!(state.active_profile(), None);
    }
}
