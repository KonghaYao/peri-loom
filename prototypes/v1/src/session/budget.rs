use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant};

use crate::object_store::ObjectStore;
use anyhow::{ensure, Result};
use turso_core::{Connection, Value};

use super::admission::{AdmissionError, SessionControl};

#[derive(Clone, Debug)]
pub struct Budget {
    pub max_sql_bytes: usize,
    pub max_rows: usize,
    pub max_result_bytes: usize,
    pub max_value_bytes: usize,
    pub max_vm_steps: u64,
    pub max_query_duration: Duration,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            max_sql_bytes: 64 * 1024,
            max_rows: 10_000,
            max_result_bytes: 4 * 1024 * 1024,
            max_value_bytes: 8 * 1024 * 1024,
            max_vm_steps: 10_000_000,
            max_query_duration: Duration::from_secs(5),
        }
    }
}

impl Budget {
    pub(crate) fn wall_duration(&self) -> Duration {
        // 预留一份执行预算给排队及远程 I/O；墙钟上限仍约束没有 CPU 进展的请求。
        self.max_query_duration.saturating_mul(2)
    }
    pub fn compatibility() -> Self {
        Self {
            max_sql_bytes: 8 * 1024 * 1024,
            ..Self::default()
        }
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(self.max_sql_bytes > 0, "SQL budget must be positive");
        ensure!(self.max_value_bytes > 0, "value budget must be positive");
        ensure!(self.max_vm_steps > 0, "VM budget must be positive");
        ensure!(
            !self.max_query_duration.is_zero(),
            "duration budget must be positive"
        );
        Ok(())
    }

    pub fn set_default(budget: Self) -> Result<()> {
        budget.validate()?;
        *defaults()
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = budget;
        Ok(())
    }

    pub fn trusted_default() -> Self {
        defaults()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

fn defaults() -> &'static RwLock<Budget> {
    static DEFAULT: OnceLock<RwLock<Budget>> = OnceLock::new();
    DEFAULT.get_or_init(|| RwLock::new(Budget::compatibility()))
}

thread_local! {
    static RPC_DEADLINE: std::cell::Cell<Option<Instant>> = const { std::cell::Cell::new(None) };
}

pub(crate) struct DeadlineScope {
    previous: Option<Instant>,
    thread: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl Drop for DeadlineScope {
    fn drop(&mut self) {
        RPC_DEADLINE.set(self.previous);
    }
}

#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_execution_budget_keeps_a_double_wall_fallback() {
        let budget = Budget::default();
        assert_eq!(budget.max_query_duration, Duration::from_secs(5));
        assert_eq!(budget.wall_duration(), Duration::from_secs(10));
        let before = Instant::now();
        let end = RequestDeadline::new(&budget).unwrap().end();
        assert!(end >= before + budget.max_query_duration);
        assert!(end <= Instant::now() + budget.wall_duration());
    }
}

pub(crate) struct RequestDeadline {
    end: Instant,
    pub(super) evidence: super::evidence::ExecutionEvidence,
    clock: Arc<super::execution_clock::ExecutionClock>,
}

impl RequestDeadline {
    pub fn scope(end: Instant) -> DeadlineScope {
        DeadlineScope {
            previous: RPC_DEADLINE.replace(Some(end)),
            thread: std::marker::PhantomData,
        }
    }
    pub fn until(deadline: Instant) -> Self {
        Self {
            end: deadline,
            evidence: Default::default(),
            clock: Default::default(),
        }
    }
    pub fn new(budget: &Budget) -> Result<Self> {
        budget.validate()?;
        let started = Instant::now();
        let end = started
            .checked_add(budget.wall_duration())
            .ok_or_else(|| anyhow::anyhow!("query deadline overflow"))?;
        let end = RPC_DEADLINE.get().map_or(end, |outer| outer.min(end));
        Ok(Self {
            end,
            evidence: Default::default(),
            clock: Default::default(),
        })
    }

    pub fn end(&self) -> Instant {
        self.end
    }

    pub fn check(&self) -> Result<()> {
        if Instant::now() >= self.end {
            return Err(AdmissionError::Deadline.into());
        }
        Ok(())
    }
}

pub(super) struct ExecutionBudget<'a> {
    connection: &'a Connection,
    budget: &'a Budget,
    wall_end: Instant,
    clock: Arc<super::execution_clock::ExecutionClock>,
    steps: Arc<AtomicU64>,
    rows: usize,
    bytes: usize,
    control: Arc<SessionControl>,
    previous_max_blob_bytes: usize,
}

impl<'a> ExecutionBudget<'a> {
    pub fn install(
        connection: &'a Connection,
        sql: &str,
        budget: &'a Budget,
        store: Arc<dyn ObjectStore>,
        deadline: &RequestDeadline,
        control: Arc<SessionControl>,
    ) -> Result<Self> {
        budget.validate()?;
        ensure!(
            sql.len() <= budget.max_sql_bytes,
            "SQL byte budget exceeded"
        );
        let wall_end = deadline.end();
        let clock = deadline.clock.clone();
        let progress_clock = clock.clone();
        let steps = Arc::new(AtomicU64::new(0));
        let progress_steps = steps.clone();
        let max_steps = budget.max_vm_steps;
        let duration = budget.max_query_duration;
        let progress_control = control.clone();
        connection.set_progress_handler(
            1000,
            Some(Box::new(move || {
                let executed = progress_steps
                    .fetch_add(1000, Ordering::Relaxed)
                    .saturating_add(1000);
                executed >= max_steps
                    || Instant::now() >= wall_end
                    || progress_clock
                        .elapsed()
                        .map_or(true, |elapsed| elapsed >= duration)
                    || store.validate_local().is_err()
                    || progress_control.closing.load(Ordering::Acquire)
            })),
        );
        let previous_max_blob_bytes = connection.max_blob_bytes();
        connection.set_max_blob_bytes(budget.max_value_bytes);
        Ok(Self {
            connection,
            budget,
            wall_end,
            clock,
            steps,
            rows: 0,
            bytes: 0,
            control,
            previous_max_blob_bytes,
        })
    }

    pub fn check(&self) -> Result<()> {
        if self.control.closing.load(Ordering::Acquire) {
            return Err(AdmissionError::Closing.into());
        }
        if Instant::now() >= self.wall_end
            || self.clock.elapsed()? >= self.budget.max_query_duration
        {
            return Err(
                anyhow::anyhow!("query duration budget exceeded").context(AdmissionError::Deadline)
            );
        }
        ensure!(
            self.steps.load(Ordering::Relaxed) < self.budget.max_vm_steps,
            "VM step budget exceeded"
        );
        Ok(())
    }

    pub fn enter_step(&self) -> Result<super::execution_clock::StepClock> {
        self.clock.enter()
    }

    pub fn account_steps(&self, steps: u64) -> Result<()> {
        ensure!(steps <= self.budget.max_vm_steps, "VM step budget exceeded");
        self.check()
    }

    pub fn resume_bytes(&mut self, bytes: usize) -> Result<()> {
        ensure!(
            bytes <= self.budget.max_result_bytes,
            "result byte budget exceeded"
        );
        self.bytes = bytes;
        Ok(())
    }

    pub fn accounted_bytes(&self) -> usize {
        self.bytes
    }

    pub fn account_row<'value>(
        &mut self,
        values: impl Iterator<Item = &'value Value>,
    ) -> Result<()> {
        ensure!(
            self.rows < self.budget.max_rows,
            "result row budget exceeded"
        );
        let mut bytes = std::mem::size_of::<Vec<Value>>();
        for value in values {
            let payload = match value {
                Value::Text(text) => text.as_str().len(),
                Value::Blob(blob) => blob.len(),
                _ => 0,
            };
            bytes = bytes
                .saturating_add(std::mem::size_of::<Value>())
                .saturating_add(payload);
        }
        ensure!(
            bytes <= self.budget.max_result_bytes.saturating_sub(self.bytes),
            "result byte budget exceeded"
        );
        self.rows += 1;
        self.bytes += bytes;
        Ok(())
    }
}

impl Drop for ExecutionBudget<'_> {
    fn drop(&mut self) {
        self.connection.set_progress_handler(0, None);
        self.connection
            .set_max_blob_bytes(self.previous_max_blob_bytes);
    }
}
