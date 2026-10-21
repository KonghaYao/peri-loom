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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{mpsc, Arc};
    use std::thread;
    use std::time::Duration;

    /// A reaper may only release the pin once work that already entered the scope has
    /// left it. Retirement therefore blocks new dispatch first, then waits for the
    /// in-flight count to reach zero.
    #[test]
    fn retire_waits_for_inflight_dispatch_and_refuses_new_work() {
        let scope = Arc::new(Scope::default());
        let dispatch = scope.dispatch().expect("initial dispatch");
        let (started, entered) = mpsc::channel();
        let (finished, completed) = mpsc::channel();
        let retiring = scope.clone();
        let handle = thread::spawn(move || {
            started.send(()).expect("signal retirement start");
            let result = retiring.retire(Instant::now() + Duration::from_secs(5));
            finished
                .send(result.is_ok())
                .expect("signal retirement end");
        });
        entered.recv().expect("retirement did not start");
        assert!(
            completed.recv_timeout(Duration::from_millis(100)).is_err(),
            "retirement returned while a dispatch was still in flight"
        );
        assert!(scope.dispatch().is_err(), "new work admitted after retire");
        drop(dispatch);
        assert!(completed
            .recv_timeout(Duration::from_secs(5))
            .expect("retirement did not finish after drain"));
        handle.join().expect("retirement thread panicked");
    }
}
