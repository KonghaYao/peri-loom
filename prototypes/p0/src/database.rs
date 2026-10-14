use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{anyhow, Context, Result};
use turso_core::{
    CheckpointMode, Connection, Database, OpenOptions, SqliteDialect, SyncMode, Value,
};

use crate::io::SegmentedIO;
use crate::storage::SegmentedStorage;

pub struct SegmentedDatabase {
    connection: Mutex<Arc<Connection>>,
    io: Arc<SegmentedIO>,
}

impl SegmentedDatabase {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let io = SegmentedIO::open(root)?;
        let database = Database::open(
            io.clone(),
            &io.database_path(),
            OpenOptions::new(Arc::new(SqliteDialect)),
        )
        .context("open segmented SQL database")?;
        let connection = database
            .connect()
            .context("connect segmented SQL database")?;
        connection.wal_auto_actions_disable();
        connection.set_sync_mode(SyncMode::Full);
        Ok(Self {
            connection: Mutex::new(connection),
            io,
        })
    }

    pub fn execute(&self, sql: &str) -> Result<()> {
        self.connection()?.execute(sql).context("execute SQL")
    }

    pub fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        let connection = self.connection()?;
        let Some(mut statement) = connection.query(sql).context("prepare SQL query")? else {
            return Ok(Vec::new());
        };
        statement
            .run_collect_rows()
            .context("collect SQL query rows")
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.connection()?
            .checkpoint(CheckpointMode::Full)
            .context("checkpoint segmented SQL database")?;
        Ok(())
    }

    pub fn storage(&self) -> Arc<SegmentedStorage> {
        self.io.storage()
    }

    fn connection(&self) -> Result<MutexGuard<'_, Arc<Connection>>> {
        self.connection
            .lock()
            .map_err(|_| anyhow!("SQL connection mutex is poisoned"))
    }
}
