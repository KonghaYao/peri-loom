use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};

#[cfg(test)]
#[path = "shutdown_budget_tests.rs"]
mod tests;

/// A native/backend call cannot safely be interrupted by dropping its owner.
/// On expiry terminate unclean rather than return while live work can publish.
type SharedDeadline = Arc<(Mutex<Option<(Instant, bool)>>, Condvar)>;

#[derive(Clone)]
pub(crate) struct ShutdownBudget {
    duration: Duration,
    state: SharedDeadline,
}

pub(crate) struct ShutdownReturn<'a>(&'a ShutdownBudget);

/// Terminates without any clean evidence. The watchdog and an over-budget
/// `finish` both report through this one line, so an operator greps a single
/// token no matter which path observed the deadline first.
fn terminate_unclean(reason: &str) -> ! {
    eprintln!("shutdown outcome=unclean reason={reason} exit_code=70");
    std::process::exit(70);
}

impl Drop for ShutdownReturn<'_> {
    fn drop(&mut self) {
        let mut state = self
            .0
            .state
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((deadline, complete)) = state.as_mut() {
            // Only an in-budget return may disarm the watchdog. Once the deadline
            // has passed the unclean outcome is already decided, and setting
            // `complete` here is what let an over-budget shutdown leave through a
            // generic error exit instead of the explicit one the watchdog reports.
            if Instant::now() < *deadline {
                *complete = true;
            }
        }
        self.0.state.1.notify_all();
    }
}

impl ShutdownBudget {
    pub fn configured() -> Result<Self> {
        let millis = match std::env::var("PERI_SHUTDOWN_BUDGET_MS") {
            Ok(value) => value.parse::<u64>().context("invalid shutdown budget")?,
            Err(std::env::VarError::NotPresent) => 10_000,
            Err(error) => return Err(error.into()),
        };
        ensure!(
            (1..=20_000).contains(&millis),
            "shutdown budget must be 1..=20000 ms"
        );
        Ok(Self {
            duration: Duration::from_millis(millis),
            state: Arc::new((Mutex::new(None), Condvar::new())),
        })
    }

    pub fn cancel_on_return(&self) -> ShutdownReturn<'_> {
        ShutdownReturn(self)
    }

    pub fn start(&self) {
        self.start_with(self.duration);
    }

    pub fn start_with(&self, remaining: Duration) {
        let mut state = self
            .state
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let deadline = Instant::now() + remaining.min(self.duration);
        if let Some((previous, _)) = state.as_mut() {
            *previous = (*previous).min(deadline);
            self.state.1.notify_all();
            return;
        }
        *state = Some((deadline, false));
        let shared = self.state.clone();
        // No destructor, seal mutation or implicit cancellation may manufacture
        // clean evidence after the budget expires.
        std::thread::spawn(move || {
            let mut state = shared
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            loop {
                if state.as_ref().is_some_and(|(_, complete)| *complete) {
                    return;
                }
                let remaining = state
                    .as_ref()
                    .unwrap()
                    .0
                    .saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    terminate_unclean("deadline_exceeded");
                }
                state = shared
                    .1
                    .wait_timeout(state, remaining)
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .0;
            }
        });
    }

    /// Time left in the process-wide shutdown budget, or `None` before it is started.
    /// Inner waits cap themselves with this so they cannot outlive the watchdog.
    pub fn remaining(&self) -> Option<Duration> {
        let state = self
            .state
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .as_ref()
            .map(|(deadline, _)| deadline.saturating_duration_since(Instant::now()))
    }

    pub fn check(&self) -> Result<()> {
        let state = self
            .state
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ensure!(
            state
                .as_ref()
                .is_none_or(|(deadline, _)| Instant::now() < *deadline),
            "shutdown deadline exceeded; not clean"
        );
        Ok(())
    }

    pub fn finish(&self) {
        let mut state = self
            .state
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((deadline, complete)) = state.as_mut() {
            if Instant::now() >= *deadline {
                // Past the budget the outcome is already decided. Returning an
                // error here would race the watchdog and let the caller exit
                // generically, so hand over to the explicit unclean termination.
                self.state.1.notify_all();
                terminate_unclean("deadline_exceeded");
            }
            *complete = true;
            self.state.1.notify_all();
        }
    }
}
