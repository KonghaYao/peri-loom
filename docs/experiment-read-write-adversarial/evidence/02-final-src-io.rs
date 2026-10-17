mod files;
mod lock;
mod native;
mod native_file;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{ensure, Context, Result};
use turso_core::io::clock::{Clock, MonotonicInstant, WallClockInstant};
use turso_core::io::{Completion, File, FileId, OpenFlags, PlatformIO, IO};
use turso_core::{CompletionError, LimboError};

use crate::cache::SegmentCache;
use crate::manifest::WalPosition;
use crate::metrics::{Metrics, Operation};
use crate::namespace::Namespace;
use crate::object_store::ObjectStore;
use crate::ownership::types::AccessRole;
use crate::recovery::{batch_covers_position, WalImage};
use crate::resources::ResourceUsage;
use crate::storage::RemoteStorage;
use crate::wal::{DurableWal, RemoteWal, WalBatch};
use crate::work_metrics::{AppendPosition, LockDomain, LockWait, ObservedLock};
use files::RemoteFile;
use lock::DirectoryLock;
use native::NativeIoGate;
pub(crate) use native::NativeIoToken;
use native_file::NativeFile;

pub struct RemoteIO {
    platform: Arc<PlatformIO>,
    shared: Arc<Shared>,
    path: String,
    native: Arc<NativeIoGate>,
}

struct Shared {
    storage: Arc<RemoteStorage>,
    wal: RemoteWal,
    namespace: Namespace,
    epoch: u64,
    gate: Mutex<State>,
    _directory_lock: DirectoryLock,
}

#[derive(Default)]
struct State {
    image: WalImage,
    batch: WalBatch,
    lsn: u64,
    dirty: bool,
    publication_failed: bool,
    wal_failed: bool,
    position: Option<WalPosition>,
    last_commit: Option<WalPosition>,
}

impl RemoteIO {
    pub(crate) fn native_health(&self) -> serde_json::Value {
        self.native.health()
    }

    pub fn open(
        store: Arc<dyn ObjectStore>,
        namespace: Namespace,
        cache_root: impl AsRef<Path>,
        epoch: u64,
        ram_limit: usize,
        disk_limit: u64,
    ) -> Result<Arc<Self>> {
        store.validate_session()?;
        std::fs::create_dir_all(cache_root.as_ref())?;
        let root = cache_root.as_ref().canonicalize()?;
        let directory_lock = DirectoryLock::acquire(&root)?;
        let cache = Arc::new(SegmentCache::new(
            root.join("segments"),
            ram_limit,
            disk_limit,
        )?);
        let storage = RemoteStorage::open(store.clone(), namespace.clone(), cache)?;
        let wal = RemoteWal::new(store);
        let metadata = wal.metadata(&namespace)?;
        ensure!(epoch >= metadata.epoch, "stale WAL epoch");
        let mut state = State {
            lsn: metadata.last_lsn,
            ..State::default()
        };
        wal.for_each_record(&namespace, 0, |record| {
            state.image.apply(&record.batch)?;
            Ok(())
        })?;
        state.last_commit = state.image.committed_position().ok().flatten();
        let path = root
            .join("database")
            .to_str()
            .context("cache path is not UTF-8")?
            .to_owned();
        ensure!(
            !Path::new(&path).exists() && !PathBuf::from(format!("{path}-wal")).exists(),
            "authoritative local database/WAL files are prohibited"
        );
        Ok(Arc::new(Self {
            platform: Arc::new(PlatformIO::new()?),
            native: Arc::new(NativeIoGate::default()),
            shared: Arc::new(Shared {
                storage,
                wal,
                namespace,
                epoch,
                gate: Mutex::new(state),
                _directory_lock: directory_lock,
            }),
            path,
        }))
    }

    pub fn storage(&self) -> Arc<RemoteStorage> {
        self.shared.storage.clone()
    }

    pub(crate) fn disable_native_dispatch(&self) {
        self.native.disable();
    }

    /// Test-only: moves this IO's WAL image to the control arm that re-derives the checksum
    /// chain from the header on every scan. The image it switches belongs to this IO, so two
    /// arms — or two tests running in parallel — cannot switch each other's scan.
    #[cfg(test)]
    pub(crate) fn force_whole_image_scan(&self) {
        let wait = LockWait::begin(LockDomain::State);
        let mut state = wait.acquired(self.shared.gate.lock().expect("remote IO mutex poisoned"));
        state.image.force_whole_image();
    }

    pub(crate) fn retirement_token(
        self: &Arc<Self>,
        deadline: std::time::Instant,
    ) -> Result<NativeIoToken<'_>> {
        self.native.retirement_token(self.clone(), deadline)
    }
    pub fn database_path(&self) -> String {
        self.path.clone()
    }
    pub fn wal_lsn(&self) -> u64 {
        let wait = LockWait::begin(LockDomain::State);
        let state = wait.acquired(self.shared.gate.lock().expect("remote IO mutex poisoned"));
        state.lsn
    }

    pub fn resources(&self) -> Result<ResourceUsage> {
        let state = self.shared.lock()?;
        let mut usage = self.shared.storage.resource_usage()?;
        usage.wal_bytes = state.image.len();
        usage.pending_wal_bytes =
            state
                .batch
                .operations
                .iter()
                .try_fold(0usize, |total, operation| {
                    let bytes = match operation {
                        crate::wal::WalOperation::Write { bytes, .. } => bytes.len(),
                        crate::wal::WalOperation::Truncate { .. } => 0,
                    };
                    total
                        .checked_add(bytes)
                        .context("pending WAL payload overflow")
                })?;
        Ok(usage)
    }

    /// The maintenance inputs this layer owns, taken under the state lock the request path
    /// already uses and against no object-store request.
    ///
    /// `unconfirmed_wal` is a conservative reading of the WAL reset precondition: it is set
    /// whenever the durable LSN and the published baseline disagree, or the state is dirty or
    /// failed. A false reading here only means the compaction lane stays ineligible; a true
    /// reading can never make a compaction pass a precondition it would otherwise fail,
    /// because `compact_wal_once` still applies them itself.
    pub(crate) fn maintenance_signals(&self) -> Result<crate::maintenance::MaintenanceSignals> {
        let state = self.shared.lock()?;
        let usage = self.shared.storage.resource_usage()?;
        let pending_wal_bytes =
            state
                .batch
                .operations
                .iter()
                .try_fold(0u64, |total, operation| {
                    let bytes = match operation {
                        crate::wal::WalOperation::Write { bytes, .. } => bytes.len() as u64,
                        crate::wal::WalOperation::Truncate { .. } => 0,
                    };
                    total
                        .checked_add(bytes)
                        .context("pending WAL payload overflow")
                })?;
        let applied = self.shared.storage.applied_lsn();
        let wal_bytes = state.image.len() as u64;
        Ok(crate::maintenance::MaintenanceSignals {
            wal_bytes,
            pending_wal_bytes,
            delta_count: state.batch.operations.len() as u64,
            // Only the part of the mirror the publication does not cover is replayed by a
            // cold start; a current baseline means there is nothing left to apply.
            replay_bytes: if state.lsn > applied { wal_bytes } else { 0 },
            dirty_segment_bytes: usage.dirty_segment_bytes,
            oldest_reader_ms: 0,
            unconfirmed_wal: state.lsn != applied
                || state.dirty
                || state.wal_failed
                || state.publication_failed
                || !state.batch.operations.is_empty(),
        })
    }

    pub(crate) fn check_writer(&self) -> Result<()> {
        self.shared.check_writer()
    }
    pub(crate) fn publish_open_header(&self) -> Result<()> {
        let mut state = self.shared.lock()?;
        if !state.dirty {
            return Ok(());
        }
        ensure!(
            !state.publication_failed
                && !state.wal_failed
                && state.position.is_none()
                && state.batch.operations.is_empty()
                && state.image.len() <= 32
                && state.lsn == self.shared.storage.applied_lsn(),
            "database open has uncommitted WAL or an unsettled publication"
        );
        self.shared
            .publish(&mut state)
            .map_err(|error| error.context(crate::session::UnsettledSqlOutcome))
    }

    pub(crate) fn check_compaction(&self) -> Result<()> {
        let state = self.shared.lock()?;
        ensure!(
            !state.wal_failed && !state.publication_failed && state.batch.operations.is_empty(),
            "WAL compaction requires confirmed durability and no pending operations"
        );
        Ok(())
    }

    pub(crate) fn compact_wal(&self) -> Result<()> {
        let mut state = self.shared.lock()?;
        self.shared.allow_reset(&state)?;
        ensure!(
            state.image.is_empty(),
            "WAL compaction requires a successful native truncate-checkpoint"
        );
        let before = state
            .lsn
            .checked_add(1)
            .context("WAL compaction LSN overflow")?;
        let compacted = (|| {
            if self.shared.wal.metadata(&self.shared.namespace)?.epoch < self.shared.epoch {
                self.shared
                    .wal
                    .fence(&self.shared.namespace, self.shared.epoch)?;
            }
            self.shared.wal.truncate(&self.shared.namespace, before)
        })();
        if let Err(error) = compacted {
            state.wal_failed = true;
            return Err(error);
        }
        Ok(())
    }

    fn kind(&self, path: &str) -> Option<bool> {
        if matches_path(path, Path::new(&self.path)) {
            return Some(false);
        }
        if matches_path(path, Path::new(&format!("{}-wal", self.path))) {
            return Some(true);
        }
        None
    }
}

fn matches_path(path: &str, expected: &Path) -> bool {
    let path = Path::new(path);
    path == expected
        || match (path.parent(), path.file_name()) {
            (Some(parent), Some(name)) => parent
                .canonicalize()
                .is_ok_and(|parent| parent.join(name) == expected),
            _ => false,
        }
}

impl Shared {
    fn check_local_writer(&self) -> Result<()> {
        ensure!(
            self.storage.store().access_role() == AccessRole::Writer,
            "read-only remote store"
        );
        self.storage.store().validate_local()
    }

    fn check_writer(&self) -> Result<()> {
        ensure!(
            self.storage.store().access_role() == AccessRole::Writer,
            "read-only remote store"
        );
        self.storage.store().validate_session()
    }

    fn lock(&self) -> turso_core::Result<ObservedLock<'_, State>> {
        let wait = LockWait::begin(LockDomain::State);
        self.gate
            .lock()
            .map(|guard| wait.acquired(guard))
            .map_err(|_| adapter_error("remote IO mutex poisoned"))
    }

    fn allow_reset(&self, state: &State) -> Result<()> {
        ensure!(
            !state.dirty
                && !state.publication_failed
                && !state.wal_failed
                && state.position.is_none(),
            "WAL reset requires successful database publication"
        );
        ensure!(
            state.batch.operations.is_empty(),
            "WAL reset cannot discard pending writes"
        );
        ensure!(
            state.image.len() <= 32 || self.storage.applied_lsn() >= state.lsn,
            "WAL reset cannot discard an uncheckpointed durable tail"
        );
        Ok(())
    }

    fn sync_wal(&self, state: &mut State) -> Result<()> {
        self.check_writer()?;
        ensure!(
            !state.wal_failed,
            "WAL durability failure requires cold recovery"
        );
        if state.batch.operations.is_empty() {
            return Ok(());
        }
        // The scan resumes at the frames the pending batch added: bytes an earlier commit
        // verified are never re-read, and a write inside them already reset the state.
        let scanned = state.image.committed_position().ok().flatten();
        // The filter chain this replaced dropped the position silently. A dropped position
        // sends the append without a commit position, which makes the authority re-derive
        // the whole WAL index, so the reason is named instead of inferred.
        let resolution = match scanned {
            None => AppendPosition::Unscanned,
            Some(position) if Some(position) == state.last_commit => AppendPosition::Unchanged,
            Some(position) => {
                match batch_covers_position(&state.batch, state.image.bytes(), position) {
                    Ok(true) => AppendPosition::Reused,
                    // The cover check either said no or failed; both leave the position unusable.
                    Ok(false) | Err(_) => AppendPosition::NotCovered,
                }
            }
        };
        let position = scanned.filter(|_| resolution == AppendPosition::Reused);
        let bytes = state
            .batch
            .operations
            .iter()
            .fold(0u64, |total, operation| match operation {
                crate::wal::WalOperation::Write { bytes, .. } => {
                    total.saturating_add(bytes.len() as u64)
                }
                crate::wal::WalOperation::Truncate { .. } => total,
            });
        crate::work_metrics::observe_append(
            resolution,
            state.image.len(),
            bytes,
            position.map_or(0, |position| position.frame_count),
        );
        let started = std::time::Instant::now();
        let appended =
            self.wal
                .append_commit(&self.namespace, self.epoch, state.batch.clone(), position);
        let elapsed = started.elapsed();
        let metrics = Metrics::global();
        let transferred = if appended.is_ok() { bytes } else { 0 };
        metrics.record(
            Operation::WalDurability,
            elapsed,
            appended.is_ok(),
            transferred,
        );
        if position.is_some() {
            metrics.record(
                Operation::NativeCommitDurability,
                elapsed,
                appended.is_ok(),
                transferred,
            );
        }
        match appended {
            Ok(lsn) => {
                state.lsn = lsn;
                if position.is_some() {
                    state.last_commit = position;
                }
                state.batch.operations.clear();
                Ok(())
            }
            Err(error) => {
                state.wal_failed = true;
                Err(error)
            }
        }
    }

    fn publish(&self, state: &mut State) -> Result<()> {
        self.check_writer()?;
        state.publication_failed = true;
        ensure!(
            !state.wal_failed && state.batch.operations.is_empty(),
            "database publication requires durable WAL"
        );
        ensure!(
            !state.dirty || state.position.is_some() || state.image.len() <= 32,
            "live WAL publication requires native checkpoint position"
        );
        let position = state
            .position
            .unwrap_or(self.storage.applied_wal_position());
        self.storage.publish(position, state.lsn)?;
        state.position = None;
        state.dirty = false;
        state.publication_failed = false;
        Ok(())
    }
}

impl Clock for RemoteIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.platform.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.platform.current_time_wall_clock()
    }
}

impl IO for RemoteIO {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        let _ticket = self.native.enter()?;
        if let Some(wal) = self.kind(path) {
            self.shared
                .storage
                .store()
                .validate_session()
                .map_err(adapter_error)?;
            return Ok(NativeFile::wrap_synchronous(
                Arc::new(RemoteFile {
                    shared: self.shared.clone(),
                    wal,
                    readonly: flags.contains(OpenFlags::ReadOnly)
                        || self.shared.storage.store().access_role() == AccessRole::ReadOnly,
                }),
                self.native.clone(),
            ));
        }
        if !is_temporary(path) {
            return Err(adapter_error("only native temporary files may use host IO"));
        }
        Ok(NativeFile::wrap_platform(
            self.platform.open_file(path, flags, direct)?,
            self.native.clone(),
        ))
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        let _ticket = self.native.enter()?;
        match self.kind(path) {
            Some(false) => Err(adapter_error("cannot remove virtual database")),
            Some(true) => {
                self.shared.check_local_writer().map_err(adapter_error)?;
                let mut state = self.shared.lock()?;
                self.shared.allow_reset(&state).map_err(adapter_error)?;
                state.image.clear();
                state
                    .batch
                    .operations
                    .push(crate::wal::WalOperation::Truncate { size: 0 });
                self.shared.sync_wal(&mut state).map_err(adapter_error)
            }
            None if is_temporary(path) => self.platform.remove_file(path),
            None => Err(adapter_error("cannot remove non-temporary host file")),
        }
    }

    fn step(&self) -> turso_core::Result<()> {
        let _ticket = self.native.enter()?;
        self.platform.step()
    }

    fn cancel(&self, completions: &[Completion]) -> turso_core::Result<()> {
        let _ticket = self.native.enter()?;
        for completion in completions {
            completion.abort();
        }
        Ok(())
    }

    fn file_id(&self, path: &str) -> turso_core::Result<FileId> {
        let _ticket = self.native.enter()?;
        if let Some(wal) = self.kind(path) {
            Ok(FileId::from_path_hash(&format!(
                "peri-loom:{}:{}:{wal}",
                self.path,
                self.shared.namespace.prefix()
            )))
        } else if is_temporary(path) {
            self.platform.file_id(path)
        } else {
            Err(adapter_error("host database file identity is prohibited"))
        }
    }
}

fn is_temporary(path: &str) -> bool {
    let path = Path::new(path);
    if path
        .file_name()
        .is_none_or(|name| name != "tursodb_temp_file")
    {
        return false;
    }
    let Some(parent) = path.parent() else {
        return false;
    };
    if !parent
        .file_name()
        .is_some_and(|name| name.to_string_lossy().starts_with(".tmp"))
    {
        return false;
    }
    match (
        parent.parent().and_then(|path| path.canonicalize().ok()),
        std::env::temp_dir().canonicalize().ok(),
    ) {
        (Some(parent), Some(root)) => parent == root,
        _ => false,
    }
}

fn adapter_error(error: impl std::fmt::Display) -> LimboError {
    LimboError::InternalError(format!("remote IO: {error}"))
}

fn finish(completion: Completion, result: Result<usize>) -> turso_core::Result<Completion> {
    match result.and_then(|bytes| Ok(i32::try_from(bytes)?)) {
        Ok(bytes) => completion.complete(bytes),
        Err(error) => {
            completion.error(CompletionError::IOError(
                std::io::ErrorKind::Other,
                "remote IO operation failed",
            ));
            return Err(adapter_error(error));
        }
    }
    Ok(completion)
}

#[cfg(test)]
mod experiment02;
