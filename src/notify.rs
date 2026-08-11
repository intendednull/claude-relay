use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use crate::config::NotifyConfig;
use crate::route_state::{RouteState, RouteTransition, rfc3339};

/// How often a running hook is checked against its deadline. The notifier is
/// out of band, so latency here costs nothing.
const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// A transition worth announcing (spec §4), plus `ProfileSwitched` for
/// `POST /control/profile` (spec §8b) — not a route transition at all, which
/// is why it arrives through `notify_event` rather than `from_transition`.
/// `Limited -> Probing` is not one of these: the window merely elapsed, and
/// nothing has changed for the user until a request actually succeeds.
///
/// `Copy` dropped to `Clone` for this variant's `name: String` — every other
/// variant would still be `Copy` alone, but the enum is one type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotifyEvent {
    FailoverEngaged { reset_at: SystemTime },
    Recovered,
    ProfileSwitched { name: String },
}

impl NotifyEvent {
    fn from_transition(transition: RouteTransition) -> Option<Self> {
        match transition.to {
            RouteState::Limited { until } => Some(Self::FailoverEngaged { reset_at: until }),
            // Only real changes are reported, so arriving at `Active` means
            // arriving from somewhere else — which is what `recovered` is.
            RouteState::Active => Some(Self::Recovered),
            RouteState::Probing => None,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::FailoverEngaged { .. } => "failover_engaged",
            Self::Recovered => "recovered",
            Self::ProfileSwitched { .. } => "profile_switched",
        }
    }
}

/// Spec §4's env var contract, as a pure function of the event so it can be
/// checked without spawning anything.
///
/// A variable the event has nothing to say in is empty rather than absent: a
/// hook run under `set -u` would otherwise die on `$RELAY_RESET_AT`.
///
/// `profile_switched` carries its name in `RELAY_DETAIL`: spec §4 and §8b
/// name exactly these three vars and no fourth for a profile, so this reuses
/// the contract that exists rather than inventing one.
fn env_vars(event: &NotifyEvent) -> [(&'static str, String); 3] {
    let reset_at = match event {
        NotifyEvent::FailoverEngaged { reset_at } => rfc3339(*reset_at).unwrap_or_default(),
        NotifyEvent::Recovered | NotifyEvent::ProfileSwitched { .. } => String::new(),
    };
    let detail = match event {
        NotifyEvent::FailoverEngaged { .. } if reset_at.is_empty() => {
            "anthropic route limited".to_string()
        }
        NotifyEvent::FailoverEngaged { .. } => {
            format!("anthropic route limited until {reset_at}")
        }
        NotifyEvent::Recovered => "anthropic route recovered".to_string(),
        NotifyEvent::ProfileSwitched { name } => format!("active profile switched to {name}"),
    };
    [
        ("RELAY_EVENT", event.name().to_string()),
        ("RELAY_RESET_AT", reset_at),
        ("RELAY_DETAIL", detail),
    ]
}

/// Runs the configured hook on state transitions, on a thread of its own.
///
/// The thread exists for one reason: transitions are applied on a plain thread
/// with no async runtime under it (`route_updates`), and that thread must never
/// wait on anything. Blocking it on a hook would stall its channel loop, so
/// *every later* transition would go unapplied until the hook finished — a far
/// worse failure than the missed notification the timeout exists to bound.
/// Handing the event over a channel keeps spawning, waiting and killing over
/// here, where the only thing a slow hook can delay is the next notification.
///
/// `Clone`: `AppState` hands one end to `RouteUpdates` and keeps another for
/// `/control/profile` to fire `profile_switched` directly — both are the same
/// underlying channel, so either can send without the other's cooperation.
#[derive(Clone)]
pub struct Notifier {
    /// `None` when no command is configured: no thread, no work, no error.
    events: Option<Sender<NotifyEvent>>,
}

impl Notifier {
    pub fn spawn(config: &NotifyConfig) -> Self {
        let Some(command) = config.command.clone() else {
            return Self { events: None };
        };
        let timeout = Duration::from_secs(config.timeout_secs);
        let (events, inbox) = mpsc::channel::<NotifyEvent>();
        thread::spawn(move || {
            // Ends when every clone of this `Notifier` is dropped — `AppState`
            // holds one directly and hands another to `RouteUpdates`, both the
            // same underlying `Sender`, so this outlives either alone.
            while let Ok(event) = inbox.recv() {
                // A panic here would otherwise end the thread, and every later
                // event would vanish into a channel nobody reads.
                if catch_unwind(AssertUnwindSafe(|| run(&command, timeout, event))).is_err() {
                    tracing::error!("notifier panicked; state changes may go unannounced");
                }
            }
        });
        Self {
            events: Some(events),
        }
    }

    /// Never blocks and never fails: a hook that hangs, fails to start, or
    /// exits non-zero is the notifier's problem alone.
    pub fn notify(&self, transition: RouteTransition) {
        let Some(event) = NotifyEvent::from_transition(transition) else {
            return;
        };
        self.notify_event(event);
    }

    /// Same guarantee as `notify`, for an event that is not itself a route
    /// transition — `profile_switched` (spec §8b), fired directly by the
    /// `/control/profile` handler rather than derived from `RouteTransition`.
    pub fn notify_event(&self, event: NotifyEvent) {
        let Some(events) = &self.events else {
            return;
        };
        if events.send(event).is_err() {
            tracing::warn!("notifier thread is gone; state changes are no longer announced");
        }
    }
}

fn run(command: &str, timeout: Duration, event: NotifyEvent) {
    // The hook inherits the environment deliberately: a desktop notifier needs
    // DISPLAY/DBUS_SESSION_BUS_ADDRESS to reach a session bus at all, and any
    // hook needs PATH.
    let spawned = Command::new("sh")
        .arg("-c")
        .arg(command)
        .envs(env_vars(&event))
        // A hook that reads stdin would otherwise read the relay's.
        .stdin(Stdio::null())
        .spawn();

    let mut child = match spawned {
        Ok(child) => child,
        Err(err) => {
            tracing::warn!(
                event = event.name(),
                error = %err,
                "notifier command failed to start"
            );
            return;
        }
    };

    match wait_or_kill(&mut child, timeout) {
        Ok(Some(status)) if status.success() => {
            tracing::debug!(event = event.name(), "notifier command ran")
        }
        Ok(Some(status)) => tracing::warn!(
            event = event.name(),
            status = %status,
            "notifier command failed"
        ),
        Ok(None) => tracing::warn!(
            event = event.name(),
            timeout_secs = timeout.as_secs(),
            "notifier command timed out and was killed"
        ),
        Err(err) => tracing::warn!(
            event = event.name(),
            error = %err,
            "notifier command could not be waited on"
        ),
    }
}

/// `Ok(None)` means the deadline passed and the hook was killed.
///
/// The kill reaches the `sh` the hook runs as, which for a single command is
/// the command itself; a hook that forks its own children can outlive its
/// timeout. Killing the whole process group would need a libc dependency for
/// one signal, and either way the relay is unaffected — so this is best effort,
/// deliberately.
fn wait_or_kill(child: &mut Child, timeout: Duration) -> io::Result<Option<ExitStatus>> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
        }
        let now = Instant::now();
        if now >= deadline {
            child.kill()?;
            // Reap it, or a killed hook stays a zombie for the life of the relay.
            let _ = child.wait();
            return Ok(None);
        }
        thread::sleep(POLL_INTERVAL.min(deadline - now));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::UNIX_EPOCH;

    fn temp_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "relay-notify-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock before epoch")
                .as_nanos()
        ))
    }

    fn transition(from: RouteState, to: RouteState) -> RouteTransition {
        RouteTransition { from, to }
    }

    fn limited(secs_ahead: u64) -> RouteState {
        RouteState::Limited {
            until: SystemTime::now() + Duration::from_secs(secs_ahead),
        }
    }

    fn value_of(event: &NotifyEvent, key: &str) -> String {
        env_vars(event)
            .into_iter()
            .find(|(name, _)| *name == key)
            .unwrap_or_else(|| panic!("{key} should be set"))
            .1
    }

    /// Polls rather than sleeps a fixed time: the hook runs in a subprocess.
    fn wait_for_file(path: &PathBuf, timeout: Duration) -> String {
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok(contents) = fs::read_to_string(path)
                && !contents.is_empty()
            {
                return contents;
            }
            assert!(Instant::now() < deadline, "timed out waiting for {path:?}");
            thread::sleep(Duration::from_millis(10));
        }
    }

    // --- which transition means which event ---

    #[test]
    fn active_to_limited_is_failover_engaged() {
        let to = limited(3600);
        let RouteState::Limited { until } = to else {
            unreachable!()
        };
        assert_eq!(
            NotifyEvent::from_transition(transition(RouteState::Active, to)),
            Some(NotifyEvent::FailoverEngaged { reset_at: until })
        );
    }

    /// A re-limit while probing is a fresh limit, not a recovery.
    #[test]
    fn probing_to_limited_is_failover_engaged_again() {
        let to = limited(1800);
        assert!(matches!(
            NotifyEvent::from_transition(transition(RouteState::Probing, to)),
            Some(NotifyEvent::FailoverEngaged { .. })
        ));
    }

    #[test]
    fn probing_to_active_is_recovered() {
        assert_eq!(
            NotifyEvent::from_transition(transition(RouteState::Probing, RouteState::Active)),
            Some(NotifyEvent::Recovered)
        );
    }

    #[test]
    fn limited_to_probing_announces_nothing() {
        assert_eq!(
            NotifyEvent::from_transition(transition(limited(60), RouteState::Probing)),
            None
        );
    }

    // --- the env var contract ---

    #[test]
    fn failover_engaged_carries_the_reset_time() {
        let reset_at = SystemTime::now() + Duration::from_secs(3600);
        let event = NotifyEvent::FailoverEngaged { reset_at };
        assert_eq!(value_of(&event, "RELAY_EVENT"), "failover_engaged");
        assert_eq!(
            value_of(&event, "RELAY_RESET_AT"),
            rfc3339(reset_at).expect("should render")
        );
        assert!(value_of(&event, "RELAY_DETAIL").contains("limited until"));
    }

    /// Set-but-empty, not absent, so a hook under `set -u` survives it.
    #[test]
    fn recovered_carries_an_empty_reset_time() {
        let event = NotifyEvent::Recovered;
        assert_eq!(value_of(&event, "RELAY_EVENT"), "recovered");
        assert_eq!(value_of(&event, "RELAY_RESET_AT"), "");
        assert_eq!(
            value_of(&event, "RELAY_DETAIL"),
            "anthropic route recovered"
        );
    }

    #[test]
    fn a_reset_time_that_cannot_be_rendered_still_produces_a_detail() {
        // A year RFC3339 cannot express, which `SystemTime` holds happily.
        let event = NotifyEvent::FailoverEngaged {
            reset_at: UNIX_EPOCH + Duration::from_secs(1_000_000_000_000),
        };
        assert_eq!(value_of(&event, "RELAY_RESET_AT"), "");
        assert_eq!(value_of(&event, "RELAY_DETAIL"), "anthropic route limited");
    }

    /// `profile_switched` has no reset time to carry — the name is what
    /// matters, and `RELAY_DETAIL` is where it lands (spec §4/§8b name no
    /// dedicated env var for it).
    #[test]
    fn profile_switched_carries_the_name_in_detail_and_an_empty_reset_at() {
        let event = NotifyEvent::ProfileSwitched {
            name: "kimi".to_string(),
        };
        assert_eq!(value_of(&event, "RELAY_EVENT"), "profile_switched");
        assert_eq!(value_of(&event, "RELAY_RESET_AT"), "");
        assert_eq!(
            value_of(&event, "RELAY_DETAIL"),
            "active profile switched to kimi"
        );
    }

    // --- running the hook ---

    #[test]
    fn no_command_configured_is_a_no_op() {
        let notifier = Notifier::spawn(&NotifyConfig::default());
        // Nothing to observe but the absence of a panic and of a thread: this
        // is the default every existing config runs under.
        notifier.notify(transition(RouteState::Active, limited(3600)));
        notifier.notify(transition(RouteState::Probing, RouteState::Active));
    }

    #[test]
    fn the_hook_receives_the_event_in_its_environment() {
        let log = temp_path("env");
        let notifier = Notifier::spawn(&NotifyConfig {
            command: Some(format!(
                r#"printf '%s|%s|%s' "$RELAY_EVENT" "$RELAY_RESET_AT" "$RELAY_DETAIL" > {}"#,
                log.display()
            )),
            timeout_secs: 5,
        });

        notifier.notify(transition(RouteState::Probing, RouteState::Active));

        let line = wait_for_file(&log, Duration::from_secs(5));
        assert_eq!(line, "recovered||anthropic route recovered");
        let _ = fs::remove_file(&log);
    }

    /// `notify_event` is the entry point a caller with an event already in
    /// hand uses — `/control/profile` — rather than one deriving from a
    /// `RouteTransition`. Proven separately from `notify` because nothing
    /// else in this file calls it.
    #[test]
    fn notify_event_runs_the_hook_for_an_event_with_no_route_transition() {
        let log = temp_path("profile-switched");
        let notifier = Notifier::spawn(&NotifyConfig {
            command: Some(format!(
                r#"printf '%s|%s|%s' "$RELAY_EVENT" "$RELAY_RESET_AT" "$RELAY_DETAIL" > {}"#,
                log.display()
            )),
            timeout_secs: 5,
        });

        notifier.notify_event(NotifyEvent::ProfileSwitched {
            name: "deepseek".to_string(),
        });

        let line = wait_for_file(&log, Duration::from_secs(5));
        assert_eq!(
            line,
            "profile_switched||active profile switched to deepseek"
        );
        let _ = fs::remove_file(&log);
    }

    #[test]
    fn a_hanging_hook_is_killed_at_the_timeout() {
        let started = Instant::now();
        // `exec` with the pipes redirected: killed or not, the hook must not be
        // left holding an fd of the test harness's.
        run(
            "exec sleep 30 >/dev/null 2>&1",
            Duration::from_millis(300),
            NotifyEvent::Recovered,
        );
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "a hanging hook must not outlast its timeout, took {elapsed:?}"
        );
        assert!(
            elapsed >= Duration::from_millis(300),
            "the timeout must actually be waited out, took {elapsed:?}"
        );
    }

    #[test]
    fn a_command_that_does_not_exist_is_survivable() {
        run(
            "definitely-not-a-real-command-9f3a",
            Duration::from_secs(5),
            NotifyEvent::Recovered,
        );
    }

    /// The queue outlives a hook that had to be killed, so one bad
    /// notification does not end notifications.
    #[test]
    fn a_later_event_still_runs_after_a_timed_out_one() {
        let log = temp_path("after-timeout");
        let notifier = Notifier::spawn(&NotifyConfig {
            // The first event hangs and is killed a second in; the second one
            // writes, which can only happen once the first has been given up on.
            command: Some(format!(
                r#"if [ "$RELAY_EVENT" = recovered ]; then echo "$RELAY_EVENT" > {}; else exec sleep 30 >/dev/null 2>&1; fi"#,
                log.display()
            )),
            timeout_secs: 1,
        });

        let sent = Instant::now();
        notifier.notify(transition(RouteState::Active, limited(3600)));
        notifier.notify(transition(RouteState::Probing, RouteState::Active));
        assert!(
            sent.elapsed() < Duration::from_millis(200),
            "recording a notification must not wait on the hook"
        );

        assert_eq!(
            wait_for_file(&log, Duration::from_secs(10)).trim(),
            "recovered"
        );
        let _ = fs::remove_file(&log);
    }
}
