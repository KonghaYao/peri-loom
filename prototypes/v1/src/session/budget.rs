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
    pub max_vm_steps: u64,
    pub max_query_duration: Duration,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            max_sql_bytes: 64 * 1024,
            max_rows: 10_000,
            max_result_bytes: 4 * 1024 * 1024,
            max_vm_steps: 10_000_000,
            max_query_duration: Duration::from_secs(5),
        }
    }
}

impl Budget {
    pub fn compatibility() -> Self {
        Self {
            max_sql_bytes: 8 * 1024 * 1024,
            ..Self::default()
        }
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(self.max_sql_bytes > 0, "SQL budget must be positive");
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

pub(crate) struct RequestDeadline {
    pub started: Instant,
    end: Instant,
}

impl RequestDeadline {
    pub fn until(deadline: Instant) -> Self {
        let started = Instant::now();
        Self {
            started,
            end: deadline,
        }
    }
    pub fn new(budget: &Budget) -> Result<Self> {
        budget.validate()?;
        let started = Instant::now();
        let end = started
            .checked_add(budget.max_query_duration)
            .ok_or_else(|| anyhow::anyhow!("query deadline overflow"))?;
        Ok(Self { started, end })
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
    started: Instant,
    steps: Arc<AtomicU64>,
    rows: usize,
    bytes: usize,
    control: Arc<SessionControl>,
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
        let started = deadline.started;
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
                    || started.elapsed() >= duration
                    || store.validate_local().is_err()
                    || progress_control.closing.load(Ordering::Acquire)
            })),
        );
        Ok(Self {
            connection,
            budget,
            started,
            steps,
            rows: 0,
            bytes: 0,
            control,
        })
    }

    pub fn check(&self) -> Result<()> {
        if self.control.closing.load(Ordering::Acquire) {
            return Err(AdmissionError::Closing.into());
        }
        if self.started.elapsed() >= self.budget.max_query_duration {
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

    pub fn account_steps(&self, steps: u64) -> Result<()> {
        ensure!(steps <= self.budget.max_vm_steps, "VM step budget exceeded");
        self.check()
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
    }
}
