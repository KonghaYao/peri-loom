use std::sync::atomic::Ordering;
use std::sync::{Mutex, TryLockError};
use std::time::{Duration, Instant};

use anyhow::{ensure, Result};
use turso_core::{NoncommittingAbort, NoncommittingRetirement, RetirementErrors};

use super::retirement::DatabaseRetirementGuard;
use super::RemoteDatabase;
use crate::session::AdmissionError;
use crate::session::RequestDeadline;

type ReclamationHook = Box<dyn FnOnce() + Send>;

struct CleanupLeader<'instance>(&'instance std::sync::atomic::AtomicBool);

impl Drop for CleanupLeader<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CleanupFailure {
    Deadline,
    Contended,
    InvalidProof,
    RetirementError,
}

impl std::fmt::Display for CleanupFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Deadline => "native cleanup deadline exceeded",
            Self::Contended => "native cleanup remains contended",
            Self::InvalidProof => "native cleanup proof not satisfied",
            Self::RetirementError => "native retirement reported an unsettled failure",
        })
    }
}

impl std::error::Error for CleanupFailure {}

#[derive(Default)]
struct State {
    collected: bool,
    hooks: Vec<ReclamationHook>,
    last_failure: Option<CleanupFailure>,
    retirement_failure: Option<CleanupFailure>,
}

#[derive(Default)]
pub(super) struct Reclamation {
    state: Mutex<State>,
    #[cfg(test)]
    hooks: test_hooks::Hooks,
}

impl RemoteDatabase {
    pub(crate) fn on_native_reclaimed(&self, hook: impl FnOnce() + Send + 'static) -> Result<()> {
        let mut state = self
            .reclamation
            .state
            .lock()
            .map_err(|_| AdmissionError::Quarantine)?;
        if state.collected {
            drop(state);
            hook();
        } else {
            state.hooks.push(Box::new(hook));
        }
        Ok(())
    }

    pub(crate) fn collect_inactive_native(
        &self,
        guard: &DatabaseRetirementGuard<'_>,
    ) -> Result<()> {
        ensure!(
            self.admission().healthy().is_err(),
            "native collection requires quarantine"
        );
        let lanes = self.collect_lanes(guard)?;
        let mut connections = Vec::with_capacity(lanes.len());
        for lane in &lanes {
            lane.mark_retired_unsettled();
            let connection = match lane.connection.try_lock() {
                Ok(connection) => connection,
                Err(TryLockError::WouldBlock) => {
                    #[cfg(test)]
                    self.reclamation.hooks.conflict.run();
                    return Err(AdmissionError::Conflict.into());
                }
                Err(TryLockError::Poisoned(_)) => return Err(AdmissionError::Quarantine.into()),
            };
            connections.push(connection);
        }
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| AdmissionError::Quarantine)?;
        while let Some(statement) = pending.first_mut() {
            if matches!(
                statement.abort_noncommitting(),
                Ok(NoncommittingAbort::StatementStopped)
            ) {
                drop(pending.remove(0));
                continue;
            }
            match statement.retire_noncommitting(guard)? {
                NoncommittingRetirement::PendingIo => return Err(AdmissionError::Conflict.into()),
                NoncommittingRetirement::RetiredUnsettled(errors) => {
                    self.record_retirement_errors(&errors);
                    drop(pending.remove(0));
                }
            }
        }
        drop(pending);
        for connection in &connections {
            if let Some(connection) = connection.as_ref() {
                match connection.retire_inactive_noncommitting(guard)? {
                    NoncommittingRetirement::PendingIo => {
                        return Err(AdmissionError::Conflict.into())
                    }
                    NoncommittingRetirement::RetiredUnsettled(errors) => {
                        self.record_retirement_errors(&errors);
                    }
                }
            }
        }
        for connection in &connections {
            if let Some(connection) = connection.as_ref() {
                connection.verify_noncommitting_retired(guard)?;
            }
        }
        let mut databases = Vec::with_capacity(lanes.len());
        for lane in &lanes {
            databases.push(lane.detach_database()?);
        }
        for connection in &mut connections {
            drop(connection.take());
        }
        drop(connections);
        drop(databases);
        drop(
            self.engine
                .lock()
                .map_err(|_| AdmissionError::Quarantine)?
                .take(),
        );
        let hooks = {
            let mut state = self
                .reclamation
                .state
                .lock()
                .map_err(|_| AdmissionError::Quarantine)?;
            state.collected = true;
            std::mem::take(&mut state.hooks)
        };
        for hook in hooks {
            hook();
        }
        Ok(())
    }

    pub(crate) fn defer_native_retirement(&self, statement: turso_core::Statement) {
        self.admission().quarantine();
        self.io.disable_native_dispatch();
        #[cfg(test)]
        self.reclamation.hooks.before_enqueue.run();
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(statement);
        #[cfg(test)]
        self.reclamation.hooks.after_enqueue.run();
    }

    #[cfg(test)]
    pub(crate) fn before_native_abort(&self) {
        self.reclamation.hooks.before_abort.run();
    }

    pub(crate) fn cleanup_deferred_until(&self, deadline: Instant) -> Result<()> {
        let result = self.cleanup_attempts(deadline);
        if let Err(error) = &result {
            let failure = match error.downcast_ref::<AdmissionError>() {
                Some(AdmissionError::Deadline) => CleanupFailure::Deadline,
                Some(AdmissionError::Conflict) => CleanupFailure::Contended,
                _ => CleanupFailure::InvalidProof,
            };
            self.reclamation
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .last_failure = Some(failure);
        }
        result
    }

    pub(crate) fn cleanup_failure(&self) -> Option<CleanupFailure> {
        let state = self
            .reclamation
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.last_failure.or(state.retirement_failure)
    }

    fn record_retirement_errors(&self, errors: &RetirementErrors) {
        if errors.abort_error.is_some()
            || errors.io_error.is_some()
            || errors.cleanup_error.is_some()
        {
            self.reclamation
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .retirement_failure = Some(CleanupFailure::RetirementError);
        }
    }

    fn cleanup_attempts(&self, deadline: Instant) -> Result<()> {
        let deadline = RequestDeadline::until(deadline);
        loop {
            let attempt = match self.cleanup.compare_exchange(
                false,
                true,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let _coordinator = CleanupLeader(&self.cleanup);
                    self.retirement_guard(&deadline)
                        .and_then(|guard| self.collect_inactive_native(&guard))
                }
                Err(_) => Err(AdmissionError::Conflict.into()),
            };
            match attempt {
                Ok(()) => return Ok(()),
                Err(error)
                    if error.downcast_ref::<AdmissionError>()
                        == Some(&AdmissionError::Conflict) =>
                {
                    deadline.check()?;
                    let remaining = deadline.end().saturating_duration_since(Instant::now());
                    std::thread::sleep(remaining.min(Duration::from_millis(10)));
                }
                Err(error) => return Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod peer_tests;

#[cfg(test)]
mod simultaneous_tests;

#[cfg(test)]
mod temp_tests;

#[cfg(test)]
mod late_settled_tests;

#[cfg(test)]
mod test_hooks {
    use std::sync::Mutex;

    #[derive(Default)]
    pub(super) struct Hook(Mutex<Option<Box<dyn FnOnce() + Send>>>);

    impl Hook {
        pub fn install(&self, hook: impl FnOnce() + Send + 'static) {
            *self.0.lock().unwrap() = Some(Box::new(hook));
        }

        pub fn run(&self) {
            let hook = self.0.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }
    }

    #[derive(Default)]
    pub(super) struct Hooks {
        pub before_abort: Hook,
        pub before_enqueue: Hook,
        pub after_enqueue: Hook,
        pub conflict: Hook,
    }
}
