use std::thread;
use std::time::{Duration, Instant};

use super::budget::ExecutionBudget;
use crate::object_store::ObjectStore;
use anyhow::Result;

fn check(guard: &ExecutionBudget<'_>, store: &dyn ObjectStore) -> Result<()> {
    guard.check()?;
    store.validate_local()
}

pub(super) fn sleep(
    duration: Duration,
    store: &dyn ObjectStore,
    guard: &ExecutionBudget<'_>,
) -> Result<()> {
    let started = Instant::now();
    check(guard, store)?;
    loop {
        let remaining = duration.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            break;
        }
        check(guard, store)?;
        thread::sleep(remaining.min(Duration::from_millis(10)));
        check(guard, store)?;
    }
    check(guard, store)?;
    Ok(())
}

#[cfg(test)]
mod tests;
