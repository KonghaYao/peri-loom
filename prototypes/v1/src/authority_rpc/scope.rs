use std::sync::{Condvar, Mutex};
use std::time::Instant;

use anyhow::{ensure, Context, Result};

#[derive(Default)]
pub(super) struct Scope {
    state: Mutex<State>,
    changed: Condvar,
}

#[derive(Default)]
struct State {
    closing: bool,
    inflight: usize,
}

pub(super) struct Dispatch<'scope>(&'scope Scope);

impl Scope {
    pub fn dispatch(&self) -> Result<Dispatch<'_>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("scope poisoned"))?;
        ensure!(!state.closing, "authority scope retired");
        state.inflight += 1;
        Ok(Dispatch(self))
    }

    pub fn retire(&self, deadline: Instant) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("scope poisoned"))?;
        state.closing = true;
        while state.inflight != 0 {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .context("scope retirement deadline; namespace quarantined")?;
            let (next, _) = self
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_| anyhow::anyhow!("scope poisoned"))?;
            state = next;
        }
        Ok(())
    }
}

impl Drop for Dispatch<'_> {
    fn drop(&mut self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.inflight -= 1;
        self.0.changed.notify_all();
    }
}
