use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use turso_core::{LimboError, Row, Statement, StepResult};

use super::budget::ExecutionBudget;
use crate::object_store::ObjectStore;

fn check(guard: &ExecutionBudget<'_>, store: &dyn ObjectStore) -> turso_core::Result<()> {
    guard
        .check()
        .map_err(|error| LimboError::InternalError(error.to_string()))?;
    store
        .validate_local()
        .map_err(|error| LimboError::InternalError(error.to_string()))
}

fn sleep(duration: Duration, store: &dyn ObjectStore, guard: &ExecutionBudget<'_>) -> Result<()> {
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

pub(super) fn next_row<'statement>(
    statement: &'statement mut Statement,
    store: &dyn ObjectStore,
    guard: &ExecutionBudget<'_>,
) -> Result<Option<&'statement Row>> {
    loop {
        check(guard, store)?;
        match statement.step()? {
            StepResult::Row => {
                return statement
                    .row()
                    .context("native statement row missing")
                    .map(Some);
            }
            StepResult::Done => return Ok(None),
            StepResult::Busy => return Err(LimboError::Busy.into()),
            StepResult::Interrupt => return Err(LimboError::Interrupt.into()),
            StepResult::IO | StepResult::Yield => {
                check(guard, store)?;
                statement._io().step()?;
                check(guard, store)?;
            }
            StepResult::Sleep { duration } => sleep(duration, store, guard)?,
        }
    }
}
