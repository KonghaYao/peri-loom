use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard};

use anyhow::{anyhow, ensure, Context, Result};
use turso_core::io::OpenFlags;
use turso_core::{
    CheckpointMode, Connection, Database, DatabaseOpts, EncryptionOpts, OpenOptions, SqliteDialect,
    Value,
};

use crate::io::RemoteIO;
use crate::namespace::Namespace;
use crate::object_store::ObjectStore;
use crate::ownership::types::AccessRole;
use crate::resources::ResourceUsage;
pub use crate::session::RemoteSession;
use crate::session::{configure, execute_sql, query_sql};
use crate::storage::RemoteStorage;

pub struct RemoteDatabase {
    key: Option<crate::encryption::ResolvedDatabaseKey>,
    connection: Mutex<Arc<Connection>>,
    engine: Arc<Database>,
    operations: RwLock<()>,
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
        Self::open_configured(
            store, namespace, cache_root, epoch, ram_limit, disk_limit, None,
        )
    }

    pub fn open_with_encryption_and_limits(
        store: Arc<dyn ObjectStore>,
        namespace: Namespace,
        cache_root: impl AsRef<Path>,
        epoch: u64,
        ram_limit: usize,
        disk_limit: u64,
        key: crate::encryption::ResolvedDatabaseKey,
    ) -> Result<Self> {
        key.validate_open(&namespace, epoch, store.access_role())?;
        Self::open_configured(
            store,
            namespace,
            cache_root,
            epoch,
            ram_limit,
            disk_limit,
            Some(key),
        )
    }

    fn open_configured(
        store: Arc<dyn ObjectStore>,
        namespace: Namespace,
        cache_root: impl AsRef<Path>,
        epoch: u64,
        ram_limit: usize,
        disk_limit: u64,
        key: Option<crate::encryption::ResolvedDatabaseKey>,
    ) -> Result<Self> {
        let readonly = store.access_role() == AccessRole::ReadOnly;
        store.validate_transaction(false)?;
        let io = RemoteIO::open(store, namespace, cache_root, epoch, ram_limit, disk_limit)?;
        let mut options = OpenOptions::new(Arc::new(SqliteDialect)).db_opts(
            DatabaseOpts::new()
                .with_attach(false)
                .with_encryption(key.is_some()),
        );
        if let Some(key) = &key {
            options = options.encryption(EncryptionOpts {
                cipher: "aegis256".to_owned(),
                hexkey: key.hex_key().to_string(),
            });
        }
        if readonly {
            options = options.flags(OpenFlags::ReadOnly);
        }
        let database = Database::open(io.clone(), &io.database_path(), options)
            .context("open remote SQL database")?;
        io.storage().store().validate_transaction(false)?;
        let connection =
            Self::connect(&database, key.as_ref()).context("connect remote SQL database")?;
        io.storage().store().validate_transaction(false)?;
        ensure!(
            connection.is_readonly(0) == readonly,
            "native database role mismatch"
        );
        configure(&connection);
        Ok(Self {
            key,
            connection: Mutex::new(connection),
            engine: database,
            operations: RwLock::new(()),
            io,
        })
    }

    fn connect(
        engine: &Arc<Database>,
        key: Option<&crate::encryption::ResolvedDatabaseKey>,
    ) -> Result<Arc<Connection>> {
        Ok(match key {
            Some(key) => engine.connect_with_encryption(Some(key.native_key()))?,
            None => engine.connect()?,
        })
    }

    pub fn execute(&self, sql: &str) -> Result<()> {
        let _operation = self.operation()?;
        let connection = self.connection()?;
        execute_sql(&connection, &self.storage().store(), sql)
    }

    pub fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        let _operation = self.operation()?;
        let connection = self.connection()?;
        query_sql(&connection, &self.storage().store(), sql)
    }

    pub fn checkpoint(&self) -> Result<()> {
        let _operation = self
            .operations
            .write()
            .map_err(|_| anyhow!("database operation gate poisoned"))?;
        self.io.check_writer()?;
        self.io.storage().store().validate_transaction(false)?;
        let connection = self.connection()?;
        configure(&connection);
        connection
            .checkpoint(CheckpointMode::Full)
            .context("checkpoint remote database")?;
        Ok(())
    }

    pub fn session(self: &Arc<Self>) -> Result<RemoteSession> {
        let _operation = self.operation()?;
        self.storage().store().validate_transaction(false)?;
        let connection = Self::connect(&self.engine, self.key.as_ref())
            .context("connect independent remote session")?;
        self.storage().store().validate_transaction(false)?;
        configure(&connection);
        Ok(RemoteSession::new(self.clone(), connection))
    }

    pub fn resources(&self) -> Result<ResourceUsage> {
        self.io.resources()
    }

    pub fn is_readonly(&self) -> Result<bool> {
        self.storage().store().validate_session()?;
        Ok(self.connection()?.is_readonly(0))
    }

    pub fn compact_wal(&self) -> Result<()> {
        let _operation = self
            .operations
            .write()
            .map_err(|_| anyhow!("database operation gate poisoned"))?;
        self.io.check_writer()?;
        self.storage().store().validate_transaction(false)?;
        let connection = self.connection()?;
        ensure!(
            connection.get_auto_commit(),
            "cannot compact inside a transaction"
        );
        configure(&connection);
        self.io.check_compaction()?;
        let checkpoint = connection
            .checkpoint(CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            })
            .context("truncate-checkpoint remote WAL")?;
        ensure!(
            checkpoint.wal_max_frame == checkpoint.wal_total_backfilled,
            "checkpoint did not backfill the complete WAL"
        );
        self.io.compact_wal()?;
        Ok(())
    }

    pub(crate) fn operation(&self) -> Result<RwLockReadGuard<'_, ()>> {
        self.operations
            .read()
            .map_err(|_| anyhow!("database operation gate poisoned"))
    }

    pub(crate) fn cleanup_operation(&self) -> RwLockReadGuard<'_, ()> {
        self.operations
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
