use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};

use crate::ownership::OwnedHandle;
use crate::runtime::DatabaseId;
use crate::session::AdmissionError;

use super::protocol::SessionState;

#[cfg(test)]
#[path = "session_entry_tests.rs"]
mod tests;

struct State {
    handle: Option<Arc<OwnedHandle>>,
    active: bool,
    closing: bool,
    close_result: Option<std::result::Result<SessionState, String>>,
    closing_since: Option<Instant>,
    created_at: Instant,
    last_activity: Instant,
    sequence: u64,
}

pub(super) struct Entry {
    pub database: DatabaseId,
    pub owner_incarnation: Option<String>,
    pub open_request_id: String,
    pub binding: crate::ownership::types::LeaseBinding,
    state: Mutex<State>,
    changed: Condvar,
}

/// Handle-less entries in a fixed cleanup state, so shutdown aggregation, retry
/// and owner-scoped reconciliation can be exercised without a native instance.
#[cfg(test)]
pub(super) enum StubPhase {
    /// Still open without a handle: the cleanup thread fails for real instead of
    /// reading an injected result.
    OpenWithoutHandle,
    /// A cleanup that has started and has not produced a verdict yet.
    ClosingWithoutResult,
    Closed,
    Failed(String),
}

#[cfg(test)]
impl Entry {
    pub(super) fn stub(
        database: DatabaseId,
        owner_incarnation: Option<&str>,
        open_request_id: &str,
        phase: StubPhase,
    ) -> Arc<Self> {
        let binding = crate::ownership::types::LeaseBinding {
            database: database.clone(),
            root_incarnation: "root".into(),
            authority_term: 1,
            branch_incarnation: 1,
            epoch: 1,
            worker_id: "worker".into(),
            lease_id: "lease".into(),
            grant_id: "grant".into(),
            renewal_sequence: 0,
            role: crate::ownership::types::AccessRole::Writer,
        };
        let close_result = match &phase {
            StubPhase::Closed => Some(Ok(SessionState {
                session: "s".repeat(64),
                binding: binding.clone(),
                owner: false,
                sequence: 1,
            })),
            StubPhase::Failed(reason) => Some(Err(reason.clone())),
            StubPhase::OpenWithoutHandle | StubPhase::ClosingWithoutResult => None,
        };
        Arc::new(Self {
            database,
            owner_incarnation: owner_incarnation.map(str::to_owned),
            open_request_id: open_request_id.to_owned(),
            binding,
            state: Mutex::new(State {
                handle: None,
                active: false,
                closing: !matches!(phase, StubPhase::OpenWithoutHandle),
                close_result,
                closing_since: Some(Instant::now()),
                created_at: Instant::now(),
                last_activity: Instant::now(),
                sequence: 0,
            }),
            changed: Condvar::new(),
        })
    }
}

pub(super) struct SessionCall {
    entry: Arc<Entry>,
    handle: Option<Arc<OwnedHandle>>,
    identifier: String,
    pub owner: bool,
}

impl Entry {
    pub fn new(
        database: DatabaseId,
        handle: Arc<OwnedHandle>,
        owner_incarnation: Option<String>,
        open_request_id: String,
    ) -> Result<Arc<Self>> {
        let sequence = handle.writer_state()?.sequence;
        let binding = handle.lease_binding_snapshot()?;
        Ok(Arc::new(Self {
            database,
            owner_incarnation,
            open_request_id,
            binding,
            state: Mutex::new(State {
                handle: Some(handle),
                active: false,
                closing: false,
                close_result: None,
                closing_since: None,
                created_at: Instant::now(),
                last_activity: Instant::now(),
                sequence,
            }),
            changed: Condvar::new(),
        }))
    }

    pub fn rebindable_sequence(&self) -> Result<u64> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?;
        ensure!(
            !state.active && !state.closing,
            "session busy during image publication"
        );
        let handle = state
            .handle
            .as_ref()
            .context("worker session already closed")?;
        ensure!(
            !handle.in_transaction()?,
            "cannot rebind an active transaction"
        );
        Ok(state.sequence)
    }

    pub fn preserve_lifetime(&self, previous: &Entry) -> Result<()> {
        let previous = previous
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?;
        state.created_at = previous.created_at;
        state.last_activity = previous.last_activity;
        Ok(())
    }

    pub fn preserve_sequence(&self, sequence: u64) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?;
        state.sequence = state.sequence.max(sequence);
        Ok(())
    }

    pub fn opened_state(&self, identifier: &str) -> Result<SessionState> {
        let handle = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?
            .handle
            .clone()
            .context("worker session already closed")?;
        snapshot(identifier, &handle)
    }

    pub fn begin(self: &Arc<Self>, identifier: &str) -> Result<SessionCall> {
        let handle = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?;
            ensure!(!state.closing, "worker session closing");
            ensure!(!state.active, "worker session RPC already active");
            let handle = state
                .handle
                .clone()
                .context("worker session already closed")?;
            state.active = true;
            state.last_activity = Instant::now();
            handle
        };
        let mut call = SessionCall {
            entry: self.clone(),
            handle: Some(handle),
            identifier: identifier.to_owned(),
            owner: false,
        };
        let native = call.handle().writer_state()?;
        call.owner = native.owner;
        Ok(call)
    }

    pub fn close(
        self: &Arc<Self>,
        identifier: &str,
        deadline: Instant,
        version: u32,
    ) -> Result<Option<SessionState>> {
        self.start_close(identifier)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?;
        loop {
            if let Some(result) = &state.close_result {
                return match result {
                    Ok(terminal) => Ok((version == 2).then(|| terminal.clone())),
                    Err(reason) => anyhow::bail!("session cleanup quarantined: {reason}"),
                };
            }
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or(AdmissionError::Deadline)?;
            state = self
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?
                .0;
        }
    }

    pub fn start_close(self: &Arc<Self>, identifier: &str) -> Result<()> {
        self.start_close_when(identifier, None)
    }

    pub fn expire(
        self: &Arc<Self>,
        identifier: &str,
        idle: Duration,
        max_age: Duration,
    ) -> Result<()> {
        self.start_close_when(identifier, Some((idle, max_age)))
    }

    fn start_close_when(
        self: &Arc<Self>,
        identifier: &str,
        expiry: Option<(Duration, Duration)>,
    ) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?;
        if let Some((idle, max_age)) = expiry {
            if (state.active || state.last_activity.elapsed() < idle)
                && state.created_at.elapsed() < max_age
            {
                return Ok(());
            }
        }
        if state.closing {
            return Ok(());
        }
        state.closing = true;
        state.closing_since = Some(Instant::now());
        let entry = self.clone();
        let identifier = identifier.to_owned();
        // A request timeout ends only its wait, never the cleanup's ownership.
        if let Err(error) = std::thread::Builder::new().spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                entry.finish_close(&identifier)
            }))
            .unwrap_or_else(|_| Err(anyhow::anyhow!("session cleanup panicked")))
            .map_err(|error| format!("{error:#}"));
            if result.is_err() {
                eprintln!("level=error event=session_cleanup outcome=quarantined resources_released=false");
            }
            let mut state = entry
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.close_result = Some(result);
            entry.changed.notify_all();
        }) {
            state.close_result = Some(Err(error.to_string()));
            self.changed.notify_all();
        }
        Ok(())
    }

    pub fn lifecycle(
        &self,
        idle: Duration,
        max_age: Duration,
    ) -> (bool, bool, bool, bool, u64, u64) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let expired = (!state.active && state.last_activity.elapsed() >= idle)
            || state.created_at.elapsed() >= max_age;
        (
            expired,
            state.closing,
            matches!(state.close_result, Some(Ok(_))),
            matches!(state.close_result, Some(Err(_))),
            state.created_at.elapsed().as_millis() as u64,
            state
                .closing_since
                .map_or(0, |at| at.elapsed().as_millis() as u64),
        )
    }

    fn finish_close(&self, identifier: &str) -> Result<SessionState> {
        let handle = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?
            .handle
            .clone()
            .context("worker session already closed")?;
        handle.begin_close()?;
        // Active execution already owns its deadline; do not replay native close
        // merely because an HTTP waiter abandoned this cleanup.
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?;
        while state.active {
            state = self
                .changed
                .wait_timeout(state, Duration::from_millis(10))
                .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?
                .0;
        }
        drop(state);
        let deadline = Instant::now() + crate::session::Budget::trusted_default().wall_duration();
        handle.request_close_until(deadline)?;
        let native = handle.writer_state()?;
        ensure!(!native.owner, "worker close did not release native writer");
        let terminal = {
            let mut terminal = snapshot(identifier, &handle)?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?;
            terminal.sequence = terminal.sequence.max(
                state
                    .sequence
                    .checked_add(1)
                    .context("worker session sequence exhausted")?,
            );
            state.sequence = terminal.sequence;
            terminal
        };
        let removed = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?
            .handle
            .take();
        drop(removed);
        drop(handle);
        Ok(terminal)
    }
}

impl SessionCall {
    pub fn handle(&self) -> &OwnedHandle {
        self.handle.as_deref().unwrap()
    }

    pub fn terminal_state(&self) -> Result<Option<SessionState>> {
        let mut terminal = snapshot(&self.identifier, self.handle())?;
        let mut state = self
            .entry
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?;
        terminal.sequence = terminal.sequence.max(
            state
                .sequence
                .checked_add(1)
                .context("worker session sequence exhausted")?,
        );
        state.sequence = terminal.sequence;
        Ok(Some(terminal))
    }
}

impl Drop for SessionCall {
    fn drop(&mut self) {
        drop(self.handle.take());
        let mut state = self
            .entry
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active = false;
        state.last_activity = Instant::now();
        self.entry.changed.notify_all();
    }
}

fn snapshot(identifier: &str, handle: &OwnedHandle) -> Result<SessionState> {
    let native = handle.writer_state()?;
    Ok(SessionState {
        session: identifier.to_owned(),
        binding: handle.lease_binding_snapshot()?,
        owner: native.owner,
        sequence: native.sequence,
    })
}
