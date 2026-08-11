use std::sync::Arc;
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::SystemTime;

use crate::route_state::{RouteStateMachine, RouteTransition};

/// What one proxied response said about the upstream route.
#[derive(Debug, Clone, Copy)]
pub enum RequestOutcome {
    Succeeded,
    LimitDetected { reset_at: SystemTime },
}

/// Applies request outcomes to the route state machine on a thread of its own.
///
/// The machine persists synchronously while holding its lock, and the only
/// point where a non-2xx body is fully known is a stream callback that cannot
/// await. Handing outcomes over a channel keeps both problems off the request
/// path: recording is a non-blocking send from any context, and the state
/// file is written on a thread that owns no request.
#[derive(Clone)]
pub struct RouteUpdates {
    outcomes: Sender<RequestOutcome>,
}

impl RouteUpdates {
    pub fn spawn(machine: Arc<RouteStateMachine>) -> Self {
        let (outcomes, inbox) = mpsc::channel::<RequestOutcome>();
        thread::spawn(move || {
            // Ends when the last `RouteUpdates` clone — and so the last
            // `AppState` — is dropped.
            while let Ok(outcome) = inbox.recv() {
                apply(&machine, outcome);
            }
        });
        Self { outcomes }
    }

    /// Never blocks and never fails a request: if the applier is gone, route
    /// state stops tracking, which is not a reason to break a response.
    pub fn record(&self, outcome: RequestOutcome) {
        let _ = self.outcomes.send(outcome);
    }
}

fn apply(machine: &RouteStateMachine, outcome: RequestOutcome) {
    // `Limited -> Probing` is checked lazily on a state query (spec §4), so a
    // window that has already elapsed has to be settled here first — otherwise
    // a success lands on a stale `Limited`, where it is a no-op, and the route
    // never recovers until something else happens to query the state.
    machine.current_state();

    let transition = match outcome {
        RequestOutcome::Succeeded => machine.on_success(),
        RequestOutcome::LimitDetected { reset_at } => machine.on_limit_detected(reset_at),
    };

    if let Some(RouteTransition { from, to }) = transition {
        tracing::info!(from = ?from, to = ?to, "route state changed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::route_state::RouteState;
    use std::time::{Duration, Instant};

    /// Outcomes are applied asynchronously, so tests wait for the state they
    /// expect rather than assuming the applier has run.
    fn wait_for(machine: &RouteStateMachine, want: fn(RouteState) -> bool) -> RouteState {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let state = machine.current_state();
            if want(state) {
                return state;
            }
            assert!(Instant::now() < deadline, "timed out in state {state:?}");
            thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn a_detected_limit_moves_the_machine_to_limited() {
        let machine = Arc::new(RouteStateMachine::new(None).unwrap());
        let updates = RouteUpdates::spawn(machine.clone());

        updates.record(RequestOutcome::LimitDetected {
            reset_at: SystemTime::now() + Duration::from_secs(3600),
        });

        wait_for(&machine, |state| {
            matches!(state, RouteState::Limited { .. })
        });
    }

    #[test]
    fn a_success_recovers_from_probing() {
        let machine = Arc::new(RouteStateMachine::new(None).unwrap());
        let updates = RouteUpdates::spawn(machine.clone());

        updates.record(RequestOutcome::LimitDetected {
            reset_at: SystemTime::now() - Duration::from_secs(120),
        });
        // The jittered `until` is already in the past, so the next query flips
        // to Probing; a success then completes the round trip to Active.
        wait_for(&machine, |state| state == RouteState::Probing);

        updates.record(RequestOutcome::Succeeded);
        wait_for(&machine, |state| state == RouteState::Active);
    }

    /// The applier settles an elapsed window itself, so recovery does not
    /// depend on someone having polled `/status` first.
    #[test]
    fn a_success_recovers_without_an_intervening_state_query() {
        let machine = Arc::new(RouteStateMachine::new(None).unwrap());
        let updates = RouteUpdates::spawn(machine.clone());

        updates.record(RequestOutcome::LimitDetected {
            reset_at: SystemTime::now() - Duration::from_secs(120),
        });
        updates.record(RequestOutcome::Succeeded);

        wait_for(&machine, |state| state == RouteState::Active);
    }

    /// Outcomes are applied in the order they were recorded, which is what
    /// lets a later outcome stand as proof that an earlier one was handled.
    #[test]
    fn outcomes_are_applied_in_order() {
        let machine = Arc::new(RouteStateMachine::new(None).unwrap());
        let updates = RouteUpdates::spawn(machine.clone());

        updates.record(RequestOutcome::Succeeded);
        updates.record(RequestOutcome::LimitDetected {
            reset_at: SystemTime::now() + Duration::from_secs(3600),
        });
        updates.record(RequestOutcome::Succeeded);

        // The trailing success lands on `Limited`, where it is a no-op.
        wait_for(&machine, |state| {
            matches!(state, RouteState::Limited { .. })
        });
    }
}
