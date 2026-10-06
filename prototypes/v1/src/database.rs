use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{anyhow, Context, Result};
use turso_core::{
    CheckpointMode, Connection, Database, OpenOptions, SqliteDialect, SyncMode, Value,
};

use crate::io::RemoteIO;
use crate::namespace::Namespace;
use crate::object_store::ObjectStore;
use crate::storage::RemoteStorage;

pub struct RemoteDatabase {
    connection: Mutex<Arc<Connection>>,
    io: Arc<RemoteIO>,
}

impl RemoteDatabase {
    pub fn open(
        store: Arc<dyn ObjectStore>,
        namespace: Namespace,
        cache_root: impl AsRef<Path>,
        epoch: u64,
    ) -> Result<Self> {
        Self::open_with_limits(
            store,
            namespace,
            cache_root,
            epoch,
            8 * 1024 * 1024,
            64 * 1024 * 1024,
        )
    }

    pub fn open_with_limits(
        store: Arc<dyn ObjectStore>,
        namespace: Namespace,
        cache_root: impl AsRef<Path>,
        epoch: u64,
        ram_limit: usize,
        disk_limit: u64,
    ) -> Result<Self> {
        let io = RemoteIO::open(store, namespace, cache_root, epoch, ram_limit, disk_limit)?;
        let database = Database::open(
            io.clone(),
            &io.database_path(),
            OpenOptions::new(Arc::new(SqliteDialect)),
        )
        .context("open remote SQL database")?;
        let connection = database.connect().context("connect remote SQL database")?;
        connection.wal_auto_actions_disable();
        connection.set_sync_mode(SyncMode::Full);
        Ok(Self {
            connection: Mutex::new(connection),
            io,
        })
    }

    pub fn execute(&self, sql: &str) -> Result<()> {
        self.io.storage().store().validate_session()?;
        let connection = self.connection()?;
        let mut runner = connection.query_runner(sql.as_bytes());
        loop {
            connection.set_sync_mode(SyncMode::Full);
            connection.wal_auto_actions_disable();
            let Some(statement) = runner.next() else {
                break;
            };
            if let Some(mut statement) = statement.context("prepare remote SQL statement")? {
                statement
                    .run_ignore_rows()
                    .context("execute remote SQL statement")?;
            }
        }
        Ok(())
    }

    pub fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        self.io.storage().store().validate_session()?;
        let connection = self.connection()?;
        connection.set_sync_mode(SyncMode::Full);
        connection.wal_auto_actions_disable();
        let Some(mut statement) = connection.query(sql).context("prepare remote query")? else {
            return Ok(Vec::new());
        };
        statement
            .run_collect_rows()
            .context("collect remote query rows")
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.io.storage().store().validate_session()?;
        let connection = self.connection()?;
        connection.set_sync_mode(SyncMode::Full);
        connection.wal_auto_actions_disable();
        connection
            .checkpoint(CheckpointMode::Full)
            .context("checkpoint remote database")?;
        Ok(())
    }

    pub fn storage(&self) -> Arc<RemoteStorage> {
        self.io.storage()
    }
    pub fn io(&self) -> Arc<RemoteIO> {
        self.io.clone()
    }

    fn connection(&self) -> Result<MutexGuard<'_, Arc<Connection>>> {
        self.connection
            .lock()
            .map_err(|_| anyhow!("remote SQL connection mutex poisoned"))
    }
}
