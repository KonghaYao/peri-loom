use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, TryLockError, Weak};
use std::thread;
use std::time::Duration;

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
use crate::session::{
    configure, AdmissionError, AdmissionStatus, Budget, RequestDeadline, SessionLane,
    WriterAdmission,
};
use crate::storage::RemoteStorage;

mod connections;
mod reclamation;
mod retirement;

pub struct RemoteDatabase {
    key: Option<crate::encryption::ResolvedDatabaseKey>,
    lane: Arc<SessionLane>,
    lanes: Mutex<Vec<Weak<SessionLane>>>,
    reclamation: reclamation::Reclamation,
    pending: Mutex<Vec<turso_core::Statement>>,
    cleanup: AtomicBool,
    engine: Mutex<Option<Arc<Database>>>,
    operations: RwLock<()>,
    io: Arc<RemoteIO>,
    admission: Arc<WriterAdmission>,
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
        let admission = Arc::new(WriterAdmission::default());
        let lane = Arc::new(SessionLane::new(connection, &admission)?);
        let lanes = Mutex::new(vec![Arc::downgrade(&lane)]);
        let database = Self {
            key,
            lane,
            lanes,
            reclamation: reclamation::Reclamation::default(),
            pending: Mutex::new(Vec::new()),
            cleanup: AtomicBool::new(false),
            engine: Mutex::new(Some(database)),
            operations: RwLock::new(()),
            io,
            admission,
        };
        if !readonly {
            database.query("SELECT name FROM sqlite_master LIMIT 1")?;
            database.io.publish_open_header()?;
        }
        Ok(database)
    }

    fn connect(
        engine: &Arc<Database>,
        key: Option<&crate::encryption::ResolvedDatabaseKey>,
    ) -> Result<Arc<Connection>> {
        let connection = match key {
            Some(key) => engine.connect_with_encryption(Some(key.native_key()))?,
            None => engine.connect()?,
        };
        connection.set_temp_store(turso_core::TempStore::Memory)?;
        connection.set_page_cache_budget_bytes(16 * 1024 * 1024)?;
        connection.set_max_blob_bytes(8 * 1024 * 1024);
        Ok(connection)
    }

    pub fn execute(&self, sql: &str) -> Result<()> {
        self.lane.execute(self, sql, &Budget::trusted_default())
    }

    pub fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        self.lane.query(self, sql, &Budget::trusted_default())
    }

    pub fn checkpoint(&self) -> Result<()> {
        let _reads = crate::authority::request_reads::Scope::enter();
        let result = self.checkpoint_once();
        self.finish_maintenance(result)
    }

    fn checkpoint_once(&self) -> Result<()> {
        let deadline = RequestDeadline::new(&Budget::trusted_default())?;
        let mut permit = self.admission.maintenance()?;
        let connection = self.lane.maintenance_connection(&deadline, self)?;
        let connection = connection.as_ref().ok_or(AdmissionError::Quarantine)?;
        let _operation = self.maintenance_operation(&deadline)?;
        self.io.check_writer()?;
        self.io.storage().store().validate_transaction(false)?;
        configure(connection);
        permit.dispatch();
        connection
            .checkpoint(CheckpointMode::Full)
            .context("checkpoint remote database")
            .map_err(|error| error.context(crate::session::UnsettledSqlOutcome))?;
        permit.complete()
    }

    pub fn session(self: &Arc<Self>) -> Result<RemoteSession> {
        let deadline = RequestDeadline::new(&Budget::trusted_default())?;
        let _operation = self.operation_until(&deadline, false)?;
        self.admission.healthy()?;
        self.storage().store().validate_transaction(false)?;
        let engine = self
            .engine
            .lock()
            .map_err(|_| AdmissionError::Quarantine)?
            .as_ref()
            .cloned()
            .ok_or(AdmissionError::Quarantine)?;
        let connection = Self::connect(&engine, self.key.as_ref())
            .context("connect independent remote session")?;
        self.storage().store().validate_transaction(false)?;
        configure(&connection);
        deadline.check()?;
        self.admission.healthy()?;
        RemoteSession::new(self.clone(), connection)
    }

    pub fn resources(&self) -> Result<ResourceUsage> {
        self.io.resources()
    }

    pub fn is_readonly(&self) -> Result<bool> {
        self.storage().store().validate_session()?;
        self.lane.is_readonly(self)
    }

    pub fn compact_wal(&self) -> Result<()> {
        let _reads = crate::authority::request_reads::Scope::enter();
        let result = self.compact_wal_once();
        self.finish_maintenance(result)
    }

    fn finish_maintenance(&self, result: Result<()>) -> Result<()> {
        if result.as_ref().err().is_some_and(|error| {
            error
                .downcast_ref::<crate::session::UnsettledSqlOutcome>()
                .is_some()
        }) && self
            .cleanup_deferred_until(std::time::Instant::now() + Duration::from_secs(20))
            .is_err()
        {
            if let Some(failure) = self.cleanup_failure() {
                return result.map_err(|error| error.context(failure));
            }
        }
        result
    }

    fn compact_wal_once(&self) -> Result<()> {
        let deadline = RequestDeadline::new(&Budget::trusted_default())?;
        let mut permit = self.admission.maintenance()?;
        let connection = self.lane.maintenance_connection(&deadline, self)?;
        let connection = connection.as_ref().ok_or(AdmissionError::Quarantine)?;
        let _operation = self.maintenance_operation(&deadline)?;
        self.io.check_writer()?;
        self.storage().store().validate_transaction(false)?;
        ensure!(
            connection.get_auto_commit(),
            "cannot compact inside a transaction"
        );
        configure(connection);
        self.io.check_compaction()?;
        self.preflight_compaction()?;
        permit.dispatch();
        let checkpoint = connection
            .checkpoint(CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            })
            .context("truncate-checkpoint remote WAL")
            .map_err(|error| error.context(crate::session::UnsettledSqlOutcome))?;
        Self::require_complete_backfill(
            checkpoint.wal_max_frame == checkpoint.wal_total_backfilled,
        )?;
        self.io
            .compact_wal()
            .map_err(|error| error.context(crate::session::UnsettledSqlOutcome))?;
        permit.complete()
    }

    fn require_complete_backfill(complete: bool) -> Result<()> {
        if complete {
            Ok(())
        } else {
            Err(anyhow!("checkpoint did not backfill the complete WAL")
                .context(crate::session::UnsettledSqlOutcome))
        }
    }

    pub(crate) fn operation_until(
        &self,
        deadline: &RequestDeadline,
        cleanup: bool,
    ) -> Result<RwLockReadGuard<'_, ()>> {
        loop {
            if !cleanup {
                deadline.check()?;
                self.admission.healthy()?;
                self.storage().store().validate_local()?;
            }
            match self.operations.try_read() {
                Ok(operation) => {
                    if !cleanup {
                        self.admission.healthy()?;
                    }
                    return Ok(operation);
                }
                Err(TryLockError::WouldBlock) => {
                    deadline.check()?;
                    let remaining = deadline
                        .end()
                        .saturating_duration_since(std::time::Instant::now());
                    thread::sleep(remaining.min(Duration::from_millis(10)));
                }
                Err(TryLockError::Poisoned(_)) => {
                    self.admission.quarantine();
                    return Err(anyhow!("database operation gate poisoned"));
                }
            }
        }
    }

    fn maintenance_operation(
        &self,
        deadline: &RequestDeadline,
    ) -> Result<std::sync::RwLockWriteGuard<'_, ()>> {
        loop {
            deadline.check()?;
            self.storage().store().validate_local()?;
            match self.operations.try_write() {
                Ok(operation) => return Ok(operation),
                Err(TryLockError::WouldBlock) => thread::sleep(Duration::from_millis(10)),
                Err(TryLockError::Poisoned(_)) => return Err(AdmissionError::Quarantine.into()),
            }
        }
    }

    pub(crate) fn admission(&self) -> &Arc<WriterAdmission> {
        &self.admission
    }

    #[doc(hidden)]
    pub fn writer_admission_status(&self) -> Result<AdmissionStatus> {
        self.admission.status()
    }

    pub fn storage(&self) -> Arc<RemoteStorage> {
        self.io.storage()
    }
    pub fn io(&self) -> Arc<RemoteIO> {
        self.io.clone()
    }
}

impl Drop for RemoteDatabase {
    fn drop(&mut self) {
        let _ = self.lane.close(self);
    }
}

#[cfg(test)]
mod remote_wal_sync_failure_tests {
    use super::*;
    use crate::object_store::{MemoryObjectStore, PutCondition, StoredObject};
    use crate::wal::WalAppendRequest;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Default)]
    struct FailingAppendStore {
        inner: MemoryObjectStore,
        armed: AtomicBool,
        failures: AtomicUsize,
    }

    impl ObjectStore for FailingAppendStore {
        fn supports_strict_fencing(&self) -> bool {
            true
        }

        fn append_wal(&self, _: &WalAppendRequest) -> Result<Option<u64>> {
            if self.armed.load(Ordering::SeqCst) {
                self.failures.fetch_add(1, Ordering::SeqCst);
                anyhow::bail!("injected remote WAL append failure");
            }
            Ok(None)
        }

        fn get(&self, key: &str) -> Result<Option<StoredObject>> {
            self.inner.get(key)
        }

        fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
            self.inner.put(key, bytes, condition)
        }

        fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
            self.inner.delete(key, expected)
        }

        fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.inner.list(prefix)
        }
    }

    fn assert_remote_append_failure_is_unsettled(retry: bool) -> Result<()> {
        let cache = tempfile::tempdir()?;
        let store = Arc::new(FailingAppendStore::default());
        let database = RemoteDatabase::open(
            store.clone(),
            Namespace::new("fault", if retry { "retry" } else { "default" })?,
            cache.path(),
            1,
        )?;
        database.execute("CREATE TABLE entries(value INTEGER)")?;
        {
            let lane = database.lane.connection.lock().unwrap();
            let connection = lane.as_ref().unwrap();
            assert!(!connection.get_data_sync_retry());
            if retry {
                connection.set_data_sync_retry(true);
            }
        }
        store.armed.store(true, Ordering::SeqCst);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            database.execute("INSERT INTO entries VALUES(1)")
        }));
        let error = result
            .expect("remote append failure must return an error")
            .unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains("injected remote WAL append failure"),
            "{message}"
        );
        assert!(message.contains("outcome is unsettled"), "{message}");
        assert_eq!(store.failures.load(Ordering::SeqCst), 1);
        assert!(database.admission.status()?.quarantined);
        Ok(())
    }

    #[test]
    fn remote_wal_append_failure_returns_unsettled_with_default_retry() -> Result<()> {
        assert_remote_append_failure_is_unsettled(false)
    }

    #[test]
    fn remote_wal_append_failure_returns_unsettled_with_retry_enabled() -> Result<()> {
        assert_remote_append_failure_is_unsettled(true)
    }
}
