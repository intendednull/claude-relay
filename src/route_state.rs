use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// The Anthropic route's state per spec §4. Distinct from `AppState`
/// (`src/state.rs`), which is the per-request axum handler state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteState {
    Active,
    Limited { until: SystemTime },
    Probing,
}

/// A state change driven by an observed outcome, for callers that need to
/// react to transitions (e.g. a notifier) rather than only poll state.
/// `None` from the driving methods means the outcome didn't change state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteTransition {
    pub from: RouteState,
    pub to: RouteState,
}

/// An observed outcome that can drive a transition. `TimeElapsed` is
/// synthesized internally by `current_state`, never by a caller.
#[derive(Debug, Clone, Copy)]
enum RouteEvent {
    LimitDetected { until: SystemTime },
    Succeeded,
    TimeElapsed,
}

/// Pure `(State, Event) -> State` core of the machine (spec §4's diagram).
/// Any combination not drawn in the diagram is a no-op: notably, a repeat
/// `LimitDetected` while already `Limited` does not push `until` out further
/// — the state only leaves `Limited` via `TimeElapsed`.
fn transition(state: RouteState, event: RouteEvent) -> RouteState {
    match (state, event) {
        (RouteState::Active, RouteEvent::LimitDetected { until })
        | (RouteState::Probing, RouteEvent::LimitDetected { until }) => {
            RouteState::Limited { until }
        }
        (RouteState::Limited { .. }, RouteEvent::TimeElapsed) => RouteState::Probing,
        (RouteState::Probing, RouteEvent::Succeeded) => RouteState::Active,
        (state, _) => state,
    }
}

/// Renders a state time for the two places one leaves the process: `/status`'s
/// `limited_until` and the notifier's `RELAY_RESET_AT`, which must agree.
///
/// Whole seconds, because that is the resolution `state_file` persists: any
/// finer and the same window reads differently before and after a restart.
///
/// A `Limited` window always renders in practice — detection bounds every one
/// it produces — so the only way to `None` is a hand-edited state file naming a
/// year RFC3339 cannot express. Failing that silently is what would leave an
/// operator with a stuck route and nothing to read.
pub fn rfc3339(time: SystemTime) -> Option<String> {
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

/// `jitter_range` is `policy.reset_jitter_secs`, `[min, max]` inclusive.
fn add_jitter(reset_at: SystemTime, jitter_range: [u64; 2]) -> SystemTime {
    let [min, max] = jitter_range;
    let jitter_secs: u64 = rand::random_range(min..=max);
    // `reset_at` is derived from an upstream-supplied reset time, and plain
    // `SystemTime` addition panics on overflow — here that would kill the
    // thread applying transitions and stop state tracking for good.
    reset_at
        .checked_add(Duration::from_secs(jitter_secs))
        .unwrap_or(reset_at)
}

/// Thread-safe wrapper around `RouteState`, optionally backed by a JSON
/// state file. Shared across concurrently handled requests: every accessor
/// takes `&self`.
pub struct RouteStateMachine {
    state: Mutex<RouteState>,
    state_file: Option<PathBuf>,
    /// `policy.reset_jitter_secs` (spec §4): random slack added past a
    /// reported reset time before transitioning to `Limited`, so the first
    /// probe after `until` doesn't race the upstream window boundary.
    jitter_secs: [u64; 2],
}

impl RouteStateMachine {
    /// Loads persisted state from `state_file` if configured and present;
    /// starts `Active` otherwise, including when `state_file` is `None`
    /// (in-memory only, per spec §4's stated default) or the file doesn't
    /// exist yet (first run).
    pub fn new(state_file: Option<PathBuf>, jitter_secs: [u64; 2]) -> Result<Self> {
        let state = match &state_file {
            Some(path) => load(path)?,
            None => RouteState::Active,
        };
        Ok(Self {
            state: Mutex::new(state),
            state_file,
            jitter_secs,
        })
    }

    /// Current route state. Performs the passive `Limited -> Probing`
    /// transition (spec §4: no background timer, checked lazily on query)
    /// when `until` has passed, persisting the change before returning it.
    pub fn current_state(&self) -> RouteState {
        let mut guard = self.lock();
        if let RouteState::Limited { until } = *guard
            && SystemTime::now() >= until
        {
            *guard = transition(*guard, RouteEvent::TimeElapsed);
            self.persist(*guard);
        }
        *guard
    }

    /// Drives the `-> Limited` transition on a detected limit response.
    /// `reset_at` is the raw reported reset time; jitter is applied here, so
    /// callers should not pre-jitter it.
    pub fn on_limit_detected(&self, reset_at: SystemTime) -> Option<RouteTransition> {
        self.apply(RouteEvent::LimitDetected {
            until: add_jitter(reset_at, self.jitter_secs),
        })
    }

    /// Drives the `Probing -> Active` transition on a successful response.
    /// A no-op in every other state.
    pub fn on_success(&self) -> Option<RouteTransition> {
        self.apply(RouteEvent::Succeeded)
    }

    /// A panic while this guard is held would poison the mutex, and every later
    /// `lock().unwrap()` would panic on it in turn: the applier thread's
    /// `catch_unwind` would keep it alive only to re-panic on every request, and
    /// `/status` would 500 for good. `RouteState` is a `Copy` enum with no
    /// invariant a half-finished mutation could tear, so taking the value back
    /// out of a poisoned lock loses nothing.
    fn lock(&self) -> std::sync::MutexGuard<'_, RouteState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn apply(&self, event: RouteEvent) -> Option<RouteTransition> {
        let mut guard = self.lock();
        let from = *guard;
        let to = transition(from, event);
        if to == from {
            return None;
        }
        *guard = to;
        self.persist(to);
        Some(RouteTransition { from, to })
    }

    fn persist(&self, state: RouteState) {
        let Some(path) = &self.state_file else {
            return;
        };
        if let Err(err) = save(path, state) {
            tracing::warn!(
                path = %path.display(),
                error = ?err,
                "failed to persist route state"
            );
        }
    }
}

/// On-disk shape for `state_file`. `until` is `Some` only for `Limited`;
/// unix seconds rather than `SystemTime` directly, which has no stable
/// serde representation.
#[derive(Debug, Serialize, Deserialize)]
struct PersistedState {
    state: PersistedKind,
    until: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum PersistedKind {
    Active,
    Limited,
    Probing,
}

impl From<RouteState> for PersistedState {
    fn from(state: RouteState) -> Self {
        match state {
            RouteState::Active => PersistedState {
                state: PersistedKind::Active,
                until: None,
            },
            RouteState::Probing => PersistedState {
                state: PersistedKind::Probing,
                until: None,
            },
            RouteState::Limited { until } => PersistedState {
                state: PersistedKind::Limited,
                until: until.duration_since(UNIX_EPOCH).ok().map(|d| d.as_secs()),
            },
        }
    }
}

impl PersistedState {
    /// A stale (past) `until`, or a `Limited` record missing `until`
    /// entirely, resets to `Active` rather than `Limited` (spec §4).
    fn into_route_state(self) -> RouteState {
        match (self.state, self.until) {
            (PersistedKind::Active, _) => RouteState::Active,
            (PersistedKind::Probing, _) => RouteState::Probing,
            (PersistedKind::Limited, Some(secs)) => {
                // `secs` comes off disk, where it can have been corrupted or
                // hand-edited into a value `SystemTime` cannot represent —
                // plain addition would panic there, turning a corrupt file
                // into a failed startup, which is what `load` exists to avoid.
                match UNIX_EPOCH.checked_add(Duration::from_secs(secs)) {
                    Some(until) if SystemTime::now() < until => RouteState::Limited { until },
                    _ => RouteState::Active,
                }
            }
            (PersistedKind::Limited, None) => RouteState::Active,
        }
    }
}

/// A missing file, and a `Limited` record with no `until`, are both handled
/// by `PersistedState`/its caller elsewhere; this also fails open to
/// `Active` on a file that fails to *parse* at all. `state_file` isn't
/// written atomically, so an ordinary unclean shutdown mid-write is a
/// realistic way to produce one, and spec §4 calls persistence "optional" —
/// a corrupt file must not turn it into a hard startup dependency.
fn load(path: &Path) -> Result<RouteState> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(RouteState::Active),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to read state file: {}", path.display()));
        }
    };
    match serde_json::from_slice::<PersistedState>(&bytes) {
        Ok(persisted) => Ok(persisted.into_route_state()),
        Err(err) => {
            tracing::warn!(
                path = %path.display(),
                error = %err,
                "state file is corrupt; starting Active"
            );
            Ok(RouteState::Active)
        }
    }
}

fn save(path: &Path, state: RouteState) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory: {}", parent.display()))?;
    }
    let json = serde_json::to_vec_pretty(&PersistedState::from(state))
        .context("failed to serialize route state")?;
    fs::write(path, json).with_context(|| format!("failed to write state file: {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_temp_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "relay-route-state-{label}-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock before epoch")
                .as_nanos()
        ))
    }

    fn future(secs: u64) -> SystemTime {
        SystemTime::now() + Duration::from_secs(secs)
    }

    fn past(secs: u64) -> SystemTime {
        SystemTime::now() - Duration::from_secs(secs)
    }

    // --- pure transition table (spec §4's diagram, exhaustive) ---

    #[test]
    fn active_plus_limit_detected_becomes_limited() {
        let until = future(100);
        assert_eq!(
            transition(RouteState::Active, RouteEvent::LimitDetected { until }),
            RouteState::Limited { until }
        );
    }

    #[test]
    fn limited_plus_time_elapsed_becomes_probing() {
        let until = past(1);
        assert_eq!(
            transition(RouteState::Limited { until }, RouteEvent::TimeElapsed),
            RouteState::Probing
        );
    }

    #[test]
    fn probing_plus_succeeded_becomes_active() {
        assert_eq!(
            transition(RouteState::Probing, RouteEvent::Succeeded),
            RouteState::Active
        );
    }

    #[test]
    fn probing_plus_limit_detected_becomes_limited_with_new_until() {
        let until = future(200);
        assert_eq!(
            transition(RouteState::Probing, RouteEvent::LimitDetected { until }),
            RouteState::Limited { until }
        );
    }

    #[test]
    fn active_plus_succeeded_is_a_no_op() {
        assert_eq!(
            transition(RouteState::Active, RouteEvent::Succeeded),
            RouteState::Active
        );
    }

    #[test]
    fn active_plus_time_elapsed_is_a_no_op() {
        assert_eq!(
            transition(RouteState::Active, RouteEvent::TimeElapsed),
            RouteState::Active
        );
    }

    #[test]
    fn limited_plus_limit_detected_again_is_a_no_op_until_unchanged() {
        let original_until = future(100);
        let state = RouteState::Limited {
            until: original_until,
        };
        let result = transition(state, RouteEvent::LimitDetected { until: future(500) });
        assert_eq!(
            result,
            RouteState::Limited {
                until: original_until
            },
            "a repeat limit signal while already Limited must not push `until` out"
        );
    }

    #[test]
    fn limited_plus_succeeded_is_a_no_op() {
        let until = future(100);
        assert_eq!(
            transition(RouteState::Limited { until }, RouteEvent::Succeeded),
            RouteState::Limited { until }
        );
    }

    #[test]
    fn probing_plus_time_elapsed_is_a_no_op() {
        assert_eq!(
            transition(RouteState::Probing, RouteEvent::TimeElapsed),
            RouteState::Probing
        );
    }

    // --- RouteStateMachine: current_state's lazy Limited -> Probing check ---

    #[test]
    fn current_state_stays_limited_before_until() {
        let machine = RouteStateMachine::new(None, [JITTER_MIN_SECS, JITTER_MAX_SECS]).unwrap();
        machine.on_limit_detected(future(3600));
        assert!(matches!(
            machine.current_state(),
            RouteState::Limited { .. }
        ));
    }

    #[test]
    fn current_state_flips_to_probing_after_until() {
        let machine = RouteStateMachine {
            state: Mutex::new(RouteState::Limited { until: past(1) }),
            state_file: None,
            jitter_secs: [JITTER_MIN_SECS, JITTER_MAX_SECS],
        };
        assert_eq!(machine.current_state(), RouteState::Probing);
    }

    #[test]
    fn current_state_defaults_to_active() {
        let machine = RouteStateMachine::new(None, [JITTER_MIN_SECS, JITTER_MAX_SECS]).unwrap();
        assert_eq!(machine.current_state(), RouteState::Active);
    }

    // --- RouteStateMachine: driving methods + observability ---

    #[test]
    fn on_limit_detected_from_active_reports_the_transition() {
        let machine = RouteStateMachine::new(None, [JITTER_MIN_SECS, JITTER_MAX_SECS]).unwrap();
        let transitioned = machine.on_limit_detected(future(1000)).unwrap();
        assert_eq!(transitioned.from, RouteState::Active);
        assert!(matches!(transitioned.to, RouteState::Limited { .. }));
    }

    #[test]
    fn on_success_from_probing_reports_the_transition() {
        let machine = RouteStateMachine {
            state: Mutex::new(RouteState::Probing),
            state_file: None,
            jitter_secs: [JITTER_MIN_SECS, JITTER_MAX_SECS],
        };
        let transitioned = machine.on_success().unwrap();
        assert_eq!(
            transitioned,
            RouteTransition {
                from: RouteState::Probing,
                to: RouteState::Active,
            }
        );
    }

    #[test]
    fn on_success_from_active_reports_no_transition() {
        let machine = RouteStateMachine::new(None, [JITTER_MIN_SECS, JITTER_MAX_SECS]).unwrap();
        assert!(machine.on_success().is_none());
        assert_eq!(machine.current_state(), RouteState::Active);
    }

    #[test]
    fn full_lifecycle_active_limited_probing_active() {
        let machine = RouteStateMachine {
            state: Mutex::new(RouteState::Active),
            state_file: None,
            jitter_secs: [JITTER_MIN_SECS, JITTER_MAX_SECS],
        };

        let to_limited = machine.on_limit_detected(future(0)).unwrap();
        assert_eq!(to_limited.from, RouteState::Active);
        assert!(matches!(to_limited.to, RouteState::Limited { .. }));

        // Force past `until` without waiting out the jitter window.
        {
            let mut guard = machine.state.lock().unwrap();
            *guard = RouteState::Limited { until: past(1) };
        }
        assert_eq!(machine.current_state(), RouteState::Probing);

        let to_active = machine.on_success().unwrap();
        assert_eq!(
            to_active,
            RouteTransition {
                from: RouteState::Probing,
                to: RouteState::Active,
            }
        );
    }

    // --- jitter range ---

    /// The documented default (`config::default_reset_jitter_secs`, spec §8's
    /// `reset_jitter_secs = [15, 60]`), restated here so `RouteStateMachine`
    /// stays testable without depending on `crate::config`.
    const JITTER_MIN_SECS: u64 = 15;
    const JITTER_MAX_SECS: u64 = 60;

    #[test]
    fn jitter_always_falls_within_the_configured_range() {
        let reset_at = SystemTime::now();
        for _ in 0..500 {
            let jittered = add_jitter(reset_at, [JITTER_MIN_SECS, JITTER_MAX_SECS]);
            let delta = jittered
                .duration_since(reset_at)
                .expect("jittered time must be after reset_at")
                .as_secs();
            assert!(
                (JITTER_MIN_SECS..=JITTER_MAX_SECS).contains(&delta),
                "jitter {delta}s outside [{JITTER_MIN_SECS}, {JITTER_MAX_SECS}]"
            );
        }
    }

    #[test]
    fn jitter_respects_a_configured_range() {
        let reset_at = SystemTime::now();
        for _ in 0..50 {
            let jittered = add_jitter(reset_at, [100, 120]);
            let delta = jittered.duration_since(reset_at).unwrap().as_secs();
            assert!(
                (100..=120).contains(&delta),
                "jitter {delta}s outside [100, 120]"
            );
        }
    }

    /// `SystemTime + Duration` panics on overflow, and `reset_at` is derived
    /// from an upstream-supplied reset time. A panic here would kill the thread
    /// that applies transitions, silently ending state tracking for the process.
    #[test]
    fn add_jitter_saturates_instead_of_panicking_at_the_edge_of_representable_time() {
        let edge = UNIX_EPOCH + Duration::from_secs(i64::MAX as u64 - 10);
        assert_eq!(
            add_jitter(edge, [JITTER_MIN_SECS, JITTER_MAX_SECS]),
            edge,
            "no jitter fits, so the un-jittered time stands"
        );
    }

    #[test]
    fn on_limit_detected_applies_jitter_via_the_machine() {
        let machine = RouteStateMachine::new(None, [JITTER_MIN_SECS, JITTER_MAX_SECS]).unwrap();
        let reset_at = SystemTime::now();
        for _ in 0..50 {
            let transitioned = machine.on_limit_detected(reset_at).unwrap();
            let RouteState::Limited { until } = transitioned.to else {
                panic!("expected Limited");
            };
            let delta = until.duration_since(reset_at).unwrap().as_secs();
            assert!((JITTER_MIN_SECS..=JITTER_MAX_SECS).contains(&delta));
            // Reset back to Active so the next iteration exercises Active -> Limited again.
            let mut guard = machine.state.lock().unwrap();
            *guard = RouteState::Active;
        }
    }

    #[test]
    fn on_limit_detected_from_probing_also_applies_jitter() {
        let machine = RouteStateMachine {
            state: Mutex::new(RouteState::Probing),
            state_file: None,
            jitter_secs: [JITTER_MIN_SECS, JITTER_MAX_SECS],
        };
        let reset_at = SystemTime::now();
        let transitioned = machine.on_limit_detected(reset_at).unwrap();
        assert_eq!(transitioned.from, RouteState::Probing);
        let RouteState::Limited { until } = transitioned.to else {
            panic!("expected Limited");
        };
        let delta = until.duration_since(reset_at).unwrap().as_secs();
        assert!(
            (JITTER_MIN_SECS..=JITTER_MAX_SECS).contains(&delta),
            "Probing -> Limited must apply the same jitter window as Active -> Limited"
        );
    }

    // --- persistence ---

    #[test]
    fn persistence_round_trips_active() {
        let path = unique_temp_path("round-trip-active");
        let machine = RouteStateMachine {
            state: Mutex::new(RouteState::Probing),
            state_file: Some(path.clone()),
            jitter_secs: [JITTER_MIN_SECS, JITTER_MAX_SECS],
        };
        // Drive the real Probing -> Active transition so this exercises
        // `apply`'s persist call, not just the free `save` function.
        let transitioned = machine.on_success().unwrap();
        assert_eq!(transitioned.to, RouteState::Active);

        let reloaded =
            RouteStateMachine::new(Some(path.clone()), [JITTER_MIN_SECS, JITTER_MAX_SECS]).unwrap();
        assert_eq!(reloaded.current_state(), RouteState::Active);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn persistence_round_trips_limited() {
        let path = unique_temp_path("round-trip-limited");
        let machine =
            RouteStateMachine::new(Some(path.clone()), [JITTER_MIN_SECS, JITTER_MAX_SECS]).unwrap();
        let until = future(3600);
        machine.on_limit_detected(until - Duration::from_secs(JITTER_MAX_SECS));
        // Read back the exact `until` the machine persisted (jitter makes it
        // non-deterministic up front) so the reload assertion is exact.
        let persisted_until = match machine.current_state() {
            RouteState::Limited { until } => until,
            other => panic!("expected Limited, got {other:?}"),
        };

        let reloaded =
            RouteStateMachine::new(Some(path.clone()), [JITTER_MIN_SECS, JITTER_MAX_SECS]).unwrap();
        assert_eq!(
            reloaded.current_state(),
            RouteState::Limited {
                until: UNIX_EPOCH
                    + Duration::from_secs(
                        persisted_until
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs()
                    )
            }
        );
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn persistence_round_trips_probing() {
        let path = unique_temp_path("round-trip-probing");
        let machine = RouteStateMachine {
            state: Mutex::new(RouteState::Active),
            state_file: Some(path.clone()),
            jitter_secs: [JITTER_MIN_SECS, JITTER_MAX_SECS],
        };
        machine.on_limit_detected(past(JITTER_MAX_SECS + 1));
        // `until` is already behind `now` even after jitter, so the next
        // query lazily flips to Probing and persists it.
        assert_eq!(machine.current_state(), RouteState::Probing);

        let reloaded =
            RouteStateMachine::new(Some(path.clone()), [JITTER_MIN_SECS, JITTER_MAX_SECS]).unwrap();
        assert_eq!(reloaded.current_state(), RouteState::Probing);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn stale_until_on_load_resets_to_active() {
        let path = unique_temp_path("stale-until");
        let stale = SystemTime::now() - Duration::from_secs(3600);
        save(&path, RouteState::Limited { until: stale }).unwrap();

        let machine =
            RouteStateMachine::new(Some(path.clone()), [JITTER_MIN_SECS, JITTER_MAX_SECS]).unwrap();
        assert_eq!(machine.current_state(), RouteState::Active);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn fresh_until_on_load_stays_limited() {
        let path = unique_temp_path("fresh-until");
        let until = future(3600);
        save(&path, RouteState::Limited { until }).unwrap();

        let machine =
            RouteStateMachine::new(Some(path.clone()), [JITTER_MIN_SECS, JITTER_MAX_SECS]).unwrap();
        assert!(matches!(
            machine.current_state(),
            RouteState::Limited { .. }
        ));
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn corrupt_state_file_fails_open_to_active() {
        let path = unique_temp_path("corrupt-file");
        fs::write(&path, b"{not valid json at all").unwrap();

        let machine =
            RouteStateMachine::new(Some(path.clone()), [JITTER_MIN_SECS, JITTER_MAX_SECS])
                .expect("a corrupt file must not error");
        assert_eq!(machine.current_state(), RouteState::Active);
        let _ = fs::remove_file(&path);
    }

    /// Same fail-open rule as a corrupt file: an `until` too large for
    /// `SystemTime` must not panic the process at startup.
    #[test]
    fn state_file_with_an_unrepresentable_until_fails_open_to_active() {
        let path = unique_temp_path("unrepresentable-until");
        fs::write(
            &path,
            br#"{"state": "LIMITED", "until": 18446744073709551615}"#,
        )
        .unwrap();

        let machine =
            RouteStateMachine::new(Some(path.clone()), [JITTER_MIN_SECS, JITTER_MAX_SECS])
                .expect("an absurd `until` must not error");
        assert_eq!(machine.current_state(), RouteState::Active);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn state_file_with_wrong_types_fails_open_to_active() {
        let path = unique_temp_path("wrong-types");
        fs::write(&path, br#"{"state": "LIMITED", "until": "not-a-number"}"#).unwrap();

        let machine =
            RouteStateMachine::new(Some(path.clone()), [JITTER_MIN_SECS, JITTER_MAX_SECS])
                .expect("wrong field types must not error");
        assert_eq!(machine.current_state(), RouteState::Active);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn missing_state_file_starts_active() {
        let path = unique_temp_path("missing-file");
        let _ = fs::remove_file(&path);
        let machine =
            RouteStateMachine::new(Some(path), [JITTER_MIN_SECS, JITTER_MAX_SECS]).unwrap();
        assert_eq!(machine.current_state(), RouteState::Active);
    }

    #[test]
    fn no_state_file_configured_is_in_memory_only() {
        let machine = RouteStateMachine::new(None, [JITTER_MIN_SECS, JITTER_MAX_SECS]).unwrap();
        machine.on_limit_detected(future(100));
        assert!(matches!(
            machine.current_state(),
            RouteState::Limited { .. }
        ));
        // Nothing to reload from — this just documents the in-memory-only
        // default and that it doesn't panic or attempt any I/O.
    }

    #[test]
    fn save_creates_missing_parent_directories() {
        let dir = std::env::temp_dir().join(format!(
            "relay-route-state-nested-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = dir.join("nested").join("state.json");
        assert!(!dir.exists());

        save(&path, RouteState::Active).unwrap();
        assert!(path.exists());

        let _ = fs::remove_dir_all(&dir);
    }
}
