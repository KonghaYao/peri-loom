use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{anyhow, Context, Result};
use turso_core::{Connection, SyncMode, Value};

use crate::database::RemoteDatabase;
use crate::object_store::ObjectStore;

pub struct RemoteSession {
    connection: Mutex<Arc<Connection>>,
    database: Arc<RemoteDatabase>,
}

impl RemoteSession {
    pub(crate) fn new(database: Arc<RemoteDatabase>, connection: Arc<Connection>) -> Self {
        Self {
            connection: Mutex::new(connection),
            database,
        }
    }

    pub fn execute(&self, sql: &str) -> Result<()> {
        let _operation = self.database.operation()?;
        let connection = self.connection()?;
        execute_sql(&connection, self.database.storage().store().as_ref(), sql)
    }

    pub fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        let _operation = self.database.operation()?;
        let connection = self.connection()?;
        query_sql(&connection, self.database.storage().store().as_ref(), sql)
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
    store: &dyn ObjectStore,
    sql: &str,
) -> Result<()> {
    let mut runner = connection.query_runner(sql.as_bytes());
    loop {
        configure(connection);
        let Some(statement) = runner.next() else {
            break;
        };
        if let Some(mut statement) = statement.context("prepare remote SQL statement")? {
            store.validate_transaction(!connection.get_auto_commit())?;
            statement
                .run_ignore_rows()
                .context("execute remote SQL statement")?;
        }
    }
    Ok(())
}

pub(crate) fn query_sql(
    connection: &Arc<Connection>,
    store: &dyn ObjectStore,
    sql: &str,
) -> Result<Vec<Vec<Value>>> {
    store.validate_transaction(!connection.get_auto_commit())?;
    configure(connection);
    let Some(mut statement) = connection.query(sql).context("prepare remote query")? else {
        return Ok(Vec::new());
    };
    statement
        .run_collect_rows()
        .context("collect remote query rows")
}
