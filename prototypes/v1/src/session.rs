use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use turso_core::{Connection, Statement, SyncMode, Value};

use crate::database::RemoteDatabase;
use crate::metrics::{Metrics, Operation};
use crate::object_store::ObjectStore;

mod budget;
mod drive;
pub use budget::Budget;
use budget::ExecutionBudget;

pub struct RemoteSession {
    connection: Mutex<Arc<Connection>>,
    database: Arc<RemoteDatabase>,
}

impl RemoteSession {
    pub(crate) fn new(database: Arc<RemoteDatabase>, connection: Arc<Connection>) -> Self {
        connection.set_busy_timeout(Duration::from_secs(4));
        Self {
            connection: Mutex::new(connection),
            database,
        }
    }

    pub fn execute(&self, sql: &str) -> Result<()> {
        let _operation = self.database.operation()?;
        let connection = self.connection()?;
        execute_sql(&connection, &self.database.storage().store(), sql)
    }

    pub fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        let _operation = self.database.operation()?;
        let connection = self.connection()?;
        query_sql(&connection, &self.database.storage().store(), sql)
    }

    pub fn execute_with_budget(&self, sql: &str, budget: &Budget) -> Result<()> {
        let _operation = self.database.operation()?;
        let connection = self.connection()?;
        execute_bounded(&connection, &self.database.storage().store(), sql, budget)
    }

    pub fn query_with_budget(&self, sql: &str, budget: &Budget) -> Result<Vec<Vec<Value>>> {
        let _operation = self.database.operation()?;
        let connection = self.connection()?;
        query_bounded(&connection, &self.database.storage().store(), sql, budget)
    }

    pub fn in_transaction(&self) -> Result<bool> {
        self.database.storage().store().validate_session()?;
        Ok(!self.connection()?.get_auto_commit())
    }

    pub fn is_readonly(&self) -> Result<bool> {
        self.database.storage().store().validate_session()?;
        Ok(self.connection()?.is_readonly(0))
    }

    fn connection(&self) -> Result<MutexGuard<'_, Arc<Connection>>> {
        self.connection
            .lock()
            .map_err(|_| anyhow!("remote session connection mutex poisoned"))
    }
}

impl Drop for RemoteSession {
    fn drop(&mut self) {
        let _operation = self.database.cleanup_operation();
        let connection = self
            .connection
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        configure(connection);
        if !connection.get_auto_commit() {
            let _ = connection.execute("ROLLBACK");
        }
        let _ = connection.close();
    }
}

pub(crate) fn configure(connection: &Connection) {
    connection.set_sync_mode(SyncMode::Full);
    connection.wal_auto_actions_disable();
    connection.set_load_extension_enabled(false);
}

pub(crate) fn execute_sql(
    connection: &Arc<Connection>,
    store: &Arc<dyn ObjectStore>,
    sql: &str,
) -> Result<()> {
    execute_bounded(connection, store, sql, &Budget::trusted_default())
}

pub(crate) fn query_sql(
    connection: &Arc<Connection>,
    store: &Arc<dyn ObjectStore>,
    sql: &str,
) -> Result<Vec<Vec<Value>>> {
    query_bounded(connection, store, sql, &Budget::trusted_default())
}

fn execute_bounded(
    connection: &Arc<Connection>,
    store: &Arc<dyn ObjectStore>,
    sql: &str,
    budget: &Budget,
) -> Result<()> {
    Metrics::global().observe(
        Operation::Execute,
        || {
            let mut guard = ExecutionBudget::install(connection, sql, budget, store.clone())?;
            let mut runner = connection.query_runner(sql.as_bytes());
            let mut steps = 0;
            loop {
                guard.check()?;
                store.validate_local()?;
                configure(connection);
                let Some(statement) = runner.next() else {
                    break;
                };
                if let Some(mut statement) = statement.context("prepare remote SQL statement")? {
                    store.validate_transaction(!connection.get_auto_commit())?;
                    step_rows(&mut statement, store.as_ref(), &mut guard, false, steps)?;
                    steps += statement.metrics().vm_steps;
                }
            }
            Ok(())
        },
        |_| 0,
    )
}

fn query_bounded(
    connection: &Arc<Connection>,
    store: &Arc<dyn ObjectStore>,
    sql: &str,
    budget: &Budget,
) -> Result<Vec<Vec<Value>>> {
    Metrics::global().observe(
        Operation::Query,
        || {
            let mut guard = ExecutionBudget::install(connection, sql, budget, store.clone())?;
            store.validate_transaction(!connection.get_auto_commit())?;
            configure(connection);
            let Some(mut statement) = connection.query(sql).context("prepare remote query")? else {
                return Ok(Vec::new());
            };
            step_rows(&mut statement, store.as_ref(), &mut guard, true, 0)
        },
        |_| 0,
    )
}

fn step_rows(
    statement: &mut Statement,
    store: &dyn ObjectStore,
    guard: &mut ExecutionBudget<'_>,
    collect: bool,
    prior_steps: u64,
) -> Result<Vec<Vec<Value>>> {
    let mut rows = Vec::new();
    loop {
        guard.check()?;
        store.validate_local()?;
        let row = drive::next_row(statement, store, guard).context("step bounded remote SQL")?;
        if let Some(row) = row {
            guard.account_row(row.get_values())?;
            if collect {
                rows.push(row.get_values().cloned().collect());
            }
        } else {
            guard.account_steps(prior_steps.saturating_add(statement.metrics().vm_steps))?;
            store.validate_local()?;
            break;
        }
        guard.account_steps(prior_steps.saturating_add(statement.metrics().vm_steps))?;
    }
    Ok(rows)
}
