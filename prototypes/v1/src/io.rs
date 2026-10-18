mod files;
mod lock;
mod native;
mod native_file;
#[cfg(test)]
mod tests;

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
    /// Serializes WAL appends against publication, reset and main-database writes.
    /// Readers do not take it: the WAL image stays readable while an append is in
    /// flight, because the frozen batch was removed before the remote call.
    append_gate: Mutex<()>,
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
    /// Set while a frozen batch is being appended to durable WAL. Publication and
    /// reset must not treat the empty `batch` as "durable" in that window.
    append_in_flight: bool,
    frozen_wal_bytes: u64,
}

/// A batch taken out of [`State`] under the lock so the remote append can run without
/// blocking readers of the WAL image.
struct FrozenWalAppend {
    batch: WalBatch,
    position: Option<WalPosition>,
    started: std::time::Instant,
    bytes: u64,
}

/// The inputs of one database publication, taken under the state lock so the remote
/// writes can run after it is released. The bookkeeping fields are the snapshot the
/// publication must settle against: the append gate keeps mutators out, so a difference
/// at settlement means a caller bypassed the gate rather than a legitimate update.
struct FrozenPublication {
    position: WalPosition,
    applied_lsn: u64,
    state_position: Option<WalPosition>,
    dirty: bool,
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
                append_gate: Mutex::new(()),
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
        // Sample the storage-owned counters before taking the state lock. A publication
        // holds the storage lock across its remote writes, so waiting for it under the
        // state lock would stall every WAL reader behind an upload it does not use.
        let mut usage = self.shared.storage.resource_usage()?;
        let state = self.shared.lock()?;
        usage.wal_bytes = state.image.len();
        usage.pending_wal_bytes = state.batch.operations.iter().try_fold(
            state.frozen_wal_bytes as usize,
            |total, operation| {
                let bytes = match operation {
                    crate::wal::WalOperation::Write { bytes, .. } => bytes.len(),
                    crate::wal::WalOperation::Truncate { .. } => 0,
                };
                total
                    .checked_add(bytes)
                    .context("pending WAL payload overflow")
            },
        )?;
        Ok(usage)
    }

    /// The maintenance inputs this layer owns, sampled without holding the state lock across
    /// a storage lock. A publication holds that lock across its remote writes, and the
    /// scheduler calls this on a periodic driver: taking the two in the other order would
    /// let an upload stall WAL readers for its whole duration.
    ///
    /// The storage figures are read first, so `applied` may lag a publication that finished
    /// in between. That direction is safe: an older applied LSN can only raise
    /// `replay_bytes`, which makes cold-start replay more conservative, never less.
    ///
    /// `unconfirmed_wal` is the append state, not the image state: an append still in
    /// flight, a pending batch, a dirty image or a failed publication. The image's applied
    /// LSN is deliberately absent. A truncate checkpoint applies the WAL tail itself, and
    /// `allow_reset` re-checks coverage against `state.lsn` after that backfill; using the
    /// image LSN here would keep the compaction lane closed for as long as writes continue,
    /// because every checkpoint freezes its applied LSN before a multi-second upload and
    /// the writes that land during it always leave the image behind.
    pub(crate) fn maintenance_signals(&self) -> Result<crate::maintenance::MaintenanceSignals> {
        let usage = self.shared.storage.resource_usage()?;
        let applied = self.shared.storage.applied_lsn();
        let state = self.shared.lock()?;
        let pending_wal_bytes = state.batch.operations.iter().try_fold(
            state.frozen_wal_bytes,
            |total, operation| {
                let bytes = match operation {
                    crate::wal::WalOperation::Write { bytes, .. } => bytes.len() as u64,
                    crate::wal::WalOperation::Truncate { .. } => 0,
                };
                total
                    .checked_add(bytes)
                    .context("pending WAL payload overflow")
            },
        )?;
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
            // Foreground activity is tracked by the database handle, which owns the
            // operations gate; the IO layer never sees a request begin.
            quiet_for_ms: 0,
            unconfirmed_wal: state.dirty
                || state.wal_failed
                || state.publication_failed
                || state.append_in_flight
                || !state.batch.operations.is_empty(),
        })
    }

    pub(crate) fn check_writer(&self) -> Result<()> {
        self.shared.check_writer()
    }
    pub(crate) fn publish_open_header(&self) -> Result<()> {
        let _append = self.shared.append_guard()?;
        let publication = {
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
                .freeze_publication(&mut state)
                .map_err(|error| error.context(crate::session::UnsettledSqlOutcome))
        }?;
        let published = self.shared.publish_frozen(&publication);
        let mut state = self.shared.lock()?;
        self.shared
            .finish_publication(&mut state, &publication, published)
            .map_err(|error| error.context(crate::session::UnsettledSqlOutcome))
    }

    pub(crate) fn check_compaction(&self) -> Result<()> {
        let state = self.shared.lock()?;
        ensure!(
            !state.wal_failed
                && !state.publication_failed
                && !state.append_in_flight
                && state.batch.operations.is_empty(),
            "WAL compaction requires confirmed durability and no pending operations"
        );
        Ok(())
    }

    pub(crate) fn compact_wal(&self) -> Result<()> {
        let _append = self.shared.append_guard()?;
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

    fn append_guard(&self) -> turso_core::Result<std::sync::MutexGuard<'_, ()>> {
        self.append_gate
            .lock()
            .map_err(|_| adapter_error("remote IO append mutex poisoned"))
    }

    fn allow_reset(&self, state: &State) -> Result<()> {
        ensure!(
            !state.append_in_flight,
            "WAL reset requires the in-flight append to settle"
        );
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

    /// Appends the pending batch without holding the state lock across the remote call.
    /// The caller holds [`Self::append_guard`], which keeps appends, publication, resets
    /// and main-database writes in the same order as before; readers only need the state
    /// lock and never observe the frozen batch. Publication and reset additionally check
    /// `append_in_flight`, so a caller that forgets the gate fails closed instead of
    /// mistaking the emptied batch for durable state.
    fn sync_wal(&self, _append: &std::sync::MutexGuard<'_, ()>) -> Result<()> {
        let mut state = self.lock()?;
        let Some(mut frozen) = self.freeze_wal(&mut state)? else {
            return Ok(());
        };
        drop(state);
        let appended = self.append_frozen(&mut frozen);
        let mut state = self.lock()?;
        self.finish_wal(&mut state, frozen, appended)
    }

    /// Appends while the caller holds the state lock. Reset paths that mutate the WAL
    /// image and append in the same critical section use this so readers never observe
    /// the mutation before its durable record is settled.
    fn sync_wal_locked(&self, state: &mut State) -> Result<()> {
        let Some(mut frozen) = self.freeze_wal(state)? else {
            return Ok(());
        };
        let appended = self.append_frozen(&mut frozen);
        self.finish_wal(state, frozen, appended)
    }

    fn freeze_wal(&self, state: &mut State) -> Result<Option<FrozenWalAppend>> {
        self.check_writer()?;
        ensure!(
            !state.wal_failed,
            "WAL durability failure requires cold recovery"
        );
        if state.batch.operations.is_empty() {
            return Ok(None);
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
        let batch = std::mem::take(&mut state.batch);
        state.append_in_flight = true;
        state.frozen_wal_bytes = bytes;
        Ok(Some(FrozenWalAppend {
            batch,
            position,
            started: std::time::Instant::now(),
            bytes,
        }))
    }

    fn append_frozen(&self, frozen: &mut FrozenWalAppend) -> Result<u64> {
        let appended = self.wal.append_commit(
            &self.namespace,
            self.epoch,
            frozen.batch.clone(),
            frozen.position,
        );
        let elapsed = frozen.started.elapsed();
        let metrics = Metrics::global();
        let transferred = if appended.is_ok() { frozen.bytes } else { 0 };
        metrics.record(
            Operation::WalDurability,
            elapsed,
            appended.is_ok(),
            transferred,
        );
        if frozen.position.is_some() {
            metrics.record(
                Operation::NativeCommitDurability,
                elapsed,
                appended.is_ok(),
                transferred,
            );
        }
        appended
    }

    fn finish_wal(
        &self,
        state: &mut State,
        mut frozen: FrozenWalAppend,
        appended: Result<u64>,
    ) -> Result<()> {
        state.append_in_flight = false;
        state.frozen_wal_bytes = 0;
        match appended {
            Ok(lsn) => {
                state.lsn = lsn;
                if frozen.position.is_some() {
                    state.last_commit = frozen.position;
                }
                Ok(())
            }
            // The unconfirmed batch stays visible so resource accounting and maintenance
            // keep seeing the bytes the failed append did not settle. No path retries it:
            // `wal_failed` already closes every write and forces cold recovery.
            Err(error) => {
                state.wal_failed = true;
                let mut restored = std::mem::take(&mut frozen.batch.operations);
                restored.append(&mut state.batch.operations);
                state.batch.operations = restored;
                Err(error)
            }
        }
    }

    /// Captures everything a publication needs while the state lock is held. The remote
    /// writes run in [`Self::publish_frozen`] after the lock is dropped: segment uploads,
    /// the manifest PUT and the HEAD comparison can take long enough that keeping the
    /// state lock across them stalls WAL readers for work they never touch.
    ///
    /// Only one main-database publication may be in flight. The append gate no longer
    /// enforces that, because WAL appends must proceed during the upload: every checkpoint
    /// runs under the database's maintenance unit, and the open-header publication happens
    /// before the database handle is shared, so no second caller can reach the uploads.
    /// A new publisher needs its own exclusion before calling this.
    fn freeze_publication(&self, state: &mut State) -> Result<FrozenPublication> {
        self.check_writer()?;
        ensure!(
            !state.wal_failed && !state.append_in_flight && state.batch.operations.is_empty(),
            "database publication requires durable WAL"
        );
        ensure!(
            !state.dirty || state.position.is_some() || state.image.len() <= 32,
            "live WAL publication requires native checkpoint position"
        );
        // Set only after the preconditions hold: a rejected publication must not leave the
        // flag behind and make the maintenance snapshot report unconfirmed state.
        state.publication_failed = true;
        Ok(FrozenPublication {
            position: state
                .position
                .unwrap_or(self.storage.applied_wal_position()),
            applied_lsn: state.lsn,
            state_position: state.position,
            dirty: state.dirty,
        })
    }

    fn publish_frozen(&self, publication: &FrozenPublication) -> Result<()> {
        self.storage
            .publish(publication.position, publication.applied_lsn)
    }

    /// Settles the bookkeeping of a finished publication. Only a successful remote
    /// publication clears `publication_failed`; an error leaves it set. WAL appends stay
    /// available because they never publish the main database, but reset and compaction
    /// remain blocked until a later publication succeeds.
    fn finish_publication(
        &self,
        state: &mut State,
        publication: &FrozenPublication,
        published: Result<()>,
    ) -> Result<()> {
        published?;
        // A commit that lands while the publication uploads advances `lsn`; that frame is
        // newer than the checkpoint position this image was frozen at and recovery replays
        // it from the WAL. The image itself is still the one the checkpoint produced, so
        // only the checkpoint position and the dirty snapshot must stand still.
        ensure!(
            state.position == publication.state_position && state.dirty == publication.dirty,
            "database state changed while the publication was in flight"
        );
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
                let _append = self.shared.append_guard()?;
                let mut state = self.shared.lock()?;
                self.shared.allow_reset(&state).map_err(adapter_error)?;
                state.image.clear();
                state
                    .batch
                    .operations
                    .push(crate::wal::WalOperation::Truncate { size: 0 });
                self.shared
                    .sync_wal_locked(&mut state)
                    .map_err(adapter_error)
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
