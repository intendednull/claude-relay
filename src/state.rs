use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

/// How many consecutive delivered fallback responses re-arm spec §4's
/// `fallback_error`.
///
/// **One is not enough, and that was the specification's defect rather than an
/// implementation slip.** A 429 or a 5xx is *intermittent* by nature — a provider
/// throttling the relay lets some requests through by definition — so a
/// single-success re-arm fires again on the very next failure. Measured on this
/// branch before the fix: a route alternating success and failure produced one
/// hook run per failed request, and a real `failover_engaged` reached the hook
/// 6.79s after detection, behind the backlog.
///
/// **Five**, because that is where the two costs cross. Expected requests to reach
/// *k* consecutive successes is `(1 - q^k) / ((1 - q) q^k)`; at `q = 0.5` that is
/// ~3 requests for `k = 1` and ~63 for `k = 5`, so five cuts the repeat rate about
/// twentyfold on the measured shape. Against that, Claude Code sends several
/// requests per turn, so a route that has genuinely recovered re-arms within a turn
/// or two and a later, separate outage still fires.
///
/// `pub` so the integration tests can express "exactly this many successes"
/// against the constant rather than against a copy of its value.
///
/// The trade taken at this value, stated without the euphemism an earlier draft
/// used. Against an *intermittently failing* route the cost is a missed **repeat**,
/// and that is cheap — the first notification already went out. But while the flag
/// is un-re-armed a **genuinely new** outage's first failure notifies nothing
/// either, and that is not a repeat of anything. A low-traffic profile, or one whose
/// fallback traffic stops entirely because Anthropic recovered, can sit un-re-armed
/// indefinitely. That residual is why this is 5 and not 10 — the higher the number,
/// the longer the flag survives a quiet period — and it is filed rather than closed
/// (`follow-ups.md` item 5, with the `recovered`-transition re-arm that would close
/// it).
pub const RE_ARM_SUCCESSES: u64 = 5;

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
    /// fail *every* request and one hook run per request is unusable.
    ///
    /// Private, unlike almost every other field here, and for the same reason
    /// `active_profile_override` is: the only way to read or write it is
    /// `fallback_delivered`/`fallback_failed`/`rearm_fallback_error` below, which
    /// is what makes "one owner, one decision" a property of this API rather than
    /// a convention `fallback::forward` has to remember.
    ///
    /// Global rather than per profile: the request path only ever has one active
    /// profile (`policy.active_profile` plus `/control/profile`'s override), so a
    /// per-profile flag would model a distinction it does not have — but a switch
    /// *does* change which profile the flag is about, which is why
    /// `rearm_fallback_error` exists. Per-process rather than persisted to the
    /// state file: a restart re-arms it, which is also when an operator is most
    /// likely to be looking.
    fallback_failing: Arc<AtomicBool>,
    /// Delivered fallback responses since the last route-attributable failure,
    /// counted for `RE_ARM_SUCCESSES`. Left to grow past the threshold rather than
    /// clamped: a `u64` cannot wrap at any request rate a process will live to see,
    /// and every failure resets it.
    fallback_successes: Arc<AtomicU64>,
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
            fallback_successes: Arc::new(AtomicU64::new(0)),
            routing_body_cap: crate::proxy::ROUTING_BODY_CAP,
            active_profile_override: Arc::new(Mutex::new(None)),
        })
    }

    /// A fallback request delivered a 2xx. Re-arms spec §4's `fallback_error`
    /// only on the `RE_ARM_SUCCESSES`th consecutive one — one success proves a
    /// request worked and does not prove the outage ended.
    pub fn fallback_delivered(&self) {
        // `>= k - 1` on the value *before* the add, rather than adding again here:
        // the same test with no arithmetic that could overflow.
        if self.fallback_successes.fetch_add(1, Ordering::Relaxed) >= RE_ARM_SUCCESSES - 1 {
            self.fallback_failing.store(false, Ordering::Relaxed);
        }
    }

    /// A route-attributable fallback failure, returning **whether this is the one
    /// that notifies** — true exactly once per outage.
    ///
    /// `compare_exchange` rather than a load followed by a store: a dead key fails
    /// every in-flight request at once, and a load-then-store would let several of
    /// them all read `false` and all fire. An atomic read-modify-write on one
    /// location always reads the latest value in that location's modification order
    /// whatever the ordering is, so exactly one of N concurrent callers can observe
    /// `false`. `Relaxed` suffices because this flag publishes no other memory: the
    /// event's payload is owned per request and handed over by the notifier's own
    /// channel send.
    ///
    /// **The two atomics are deliberately not updated together.** Nothing reads
    /// them as a pair — the fire is decided by this one `compare_exchange`, and the
    /// counter only ever gates a `store(false)` in `fallback_delivered`. So every
    /// interleaving of a concurrent success and failure costs at most one *extra*
    /// notification and can never lose one: a success in flight across a failure
    /// can be counted toward the streak that follows it, which re-arms slightly
    /// early, and an early re-arm only means the next failure notifies again.
    /// Packing both into one atomic was considered and would remove that skew; it
    /// buys exactness far below the statistical noise of `RE_ARM_SUCCESSES` itself,
    /// at the price of a bit-packed word in place of two named fields.
    #[must_use]
    pub fn fallback_failed(&self) -> bool {
        self.fallback_successes.store(0, Ordering::Relaxed);
        self.fallback_failing
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }

    /// Re-arms `fallback_error` outright, for when the thing the flag describes has
    /// *changed* rather than recovered: `POST /control/profile` (spec §8b) points
    /// the route at a different provider with different credentials, so the outage
    /// the operator was told about is no longer the one they are now looking at —
    /// and the `RELAY_DETAIL` on their screen names a profile that is no longer
    /// active. Without this, a switch to a second broken profile is silent.
    pub fn rearm_fallback_error(&self) {
        self.fallback_successes.store(0, Ordering::Relaxed);
        self.fallback_failing.store(false, Ordering::Relaxed);
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

    // --- spec §4's `fallback_error` edge-trigger ---
    //
    // Driven through the three methods rather than end to end, because the
    // sequences that matter here are long and exact: a hook log cannot tell "the
    // fourth success did not re-arm" from "the fifth event coalesced".

    fn relay() -> AppState {
        AppState::new(
            Arc::new(config(DetectConfig::default(), NotifyConfig::default())),
            None,
            "digest".to_string(),
        )
        .expect("should build")
    }

    /// The edge itself: the first failure of an outage notifies and the rest do
    /// not, however many there are.
    #[test]
    fn only_the_first_failure_of_an_outage_notifies() {
        let state = relay();
        assert!(state.fallback_failed(), "the first failure is the edge");
        for _ in 0..20 {
            assert!(
                !state.fallback_failed(),
                "a dead key fails every request; the operator is told once"
            );
        }
    }

    /// One 2xx proves a request worked, not that the outage ended — which is the
    /// whole of fix round 1's specification change. A route alternating success and
    /// failure used to notify on every failure.
    #[test]
    fn one_delivered_response_does_not_re_arm_but_the_kth_does() {
        let state = relay();

        for short_streak in 1..RE_ARM_SUCCESSES {
            // A fresh outage per iteration, so the streak below starts from zero
            // rather than from whatever the previous iteration left behind.
            state.rearm_fallback_error();
            assert!(state.fallback_failed(), "a fresh outage to recover from");

            for _ in 0..short_streak {
                state.fallback_delivered();
            }
            assert!(
                !state.fallback_failed(),
                "{short_streak} of {RE_ARM_SUCCESSES} successes must not re-arm"
            );
        }

        state.rearm_fallback_error();
        assert!(state.fallback_failed());
        for _ in 0..RE_ARM_SUCCESSES {
            state.fallback_delivered();
        }
        assert!(
            state.fallback_failed(),
            "{RE_ARM_SUCCESSES} consecutive successes are a recovery, and the failure \
             after one is a new outage"
        );
    }

    /// **A lock on the documented value, not on behaviour.** Every other test here is
    /// written against `RE_ARM_SUCCESSES` rather than against a copy of its number,
    /// which is deliberate — it keeps them honest across an intended change — and the
    /// cost is that they are blind to an unintended one. An audit confirmed it: at
    /// three, all 523 tests still passed, while four documents state five as the
    /// contract.
    #[test]
    fn the_documented_re_arm_threshold_is_the_one_in_the_code() {
        assert_eq!(
            RE_ARM_SUCCESSES, 5,
            "`docs/spec.md` §4, `README.md`, `relay.example.toml` and \
             `docs/decisions.md` all quote five consecutive delivered responses to the \
             operator; move them with this constant or not at all"
        );
    }

    /// Consecutive, not cumulative. A near-miss streak broken by a failure starts
    /// over — otherwise a route failing half its requests would re-arm on the
    /// aggregate and notify forever, which is the defect being fixed.
    #[test]
    fn a_failure_resets_a_partial_success_streak() {
        let state = relay();
        assert!(state.fallback_failed());

        for _ in 0..40 {
            for _ in 0..RE_ARM_SUCCESSES - 1 {
                state.fallback_delivered();
            }
            assert!(
                !state.fallback_failed(),
                "an interrupted streak must never accumulate into a re-arm"
            );
        }
    }

    // `a_delivered_response_alone_never_notifies` was retired here, deliberately: it
    // asserted the opposite of its name — that a failure after fifteen deliveries
    // *does* notify — which `only_the_first_failure_of_an_outage_notifies` already
    // covers from a healthy start. The property the name promised is not testable at
    // this seam anyway: "a request-attributable failure does not notify" is the
    // absence of a call site in `fallback::forward`, which only
    // `a_request_attributable_failure_between_two_route_failures_does_not_re_arm`
    // can see.

    /// F6: a switch points the route at different credentials, so the outage the
    /// operator was told about is not the one in front of them now. Without the
    /// re-arm, a switch to a second broken profile is silent and the only detail on
    /// screen names the old profile.
    #[test]
    fn a_profile_switch_re_arms_so_the_next_failure_notifies() {
        let state = relay();
        assert!(state.fallback_failed());
        assert!(!state.fallback_failed());

        state.rearm_fallback_error();

        assert!(
            state.fallback_failed(),
            "the new profile's own failure has to reach the operator"
        );
    }

    /// The flag is shared, not copied by `derive(Clone)` — axum hands a clone to
    /// every handler, so a per-clone flag would notify once per worker.
    #[test]
    fn the_edge_trigger_is_shared_across_cloned_appstates() {
        let state = relay();
        let cloned = state.clone();
        assert!(state.fallback_failed());
        assert!(
            !cloned.fallback_failed(),
            "a clone must see the same outage, not its own"
        );
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
