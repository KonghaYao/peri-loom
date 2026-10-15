use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};

use crate::ownership::OwnedHandle;
use crate::runtime::DatabaseId;
use crate::session::AdmissionError;

use super::protocol::SessionState;

struct State {
    handle: Option<Arc<OwnedHandle>>,
    active: bool,
    closing: bool,
    sequence: u64,
}

pub(super) struct Entry {
    pub database: DatabaseId,
    state: Mutex<State>,
    changed: Condvar,
}

pub(super) struct SessionCall {
    entry: Arc<Entry>,
    handle: Option<Arc<OwnedHandle>>,
    identifier: String,
    pub owner: bool,
}

impl Entry {
    pub fn new(database: DatabaseId, handle: Arc<OwnedHandle>) -> Result<Arc<Self>> {
        let sequence = handle.writer_state()?.sequence;
        Ok(Arc::new(Self {
            database,
            state: Mutex::new(State {
                handle: Some(handle),
                active: false,
                closing: false,
                sequence,
            }),
            changed: Condvar::new(),
        }))
    }

    pub fn available(&self) -> Result<bool> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?;
        Ok(!state.closing && state.handle.is_some())
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
        &self,
        identifier: &str,
        deadline: Instant,
        version: u32,
    ) -> Result<Option<SessionState>> {
        let handle = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?;
            ensure!(!state.closing, "worker session closing");
            state.closing = true;
            state
                .handle
                .clone()
                .context("worker session already closed")?
        };
        handle.begin_close()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?;
        while state.active {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or(AdmissionError::Deadline)?;
            state = self
                .changed
                .wait_timeout(state, remaining.min(Duration::from_millis(10)))
                .map_err(|_| anyhow::anyhow!("worker session state poisoned"))?
                .0;
        }
        drop(state);
        ensure!(Instant::now() < deadline, "worker close deadline elapsed");
        handle.request_close_until(deadline)?;
        let native = handle.writer_state()?;
        ensure!(!native.owner, "worker close did not release native writer");
        let terminal = if version == 2 {
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
            Some(terminal)
        } else {
            None
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
