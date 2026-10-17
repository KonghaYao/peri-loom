use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};

use crate::manifest::WalPosition;
use crate::namespace::Namespace;
use crate::object_store::{ObjectStore, PutCondition};

pub trait DurableWal: Send + Sync {
    fn append(&self, namespace: &Namespace, epoch: u64, batch: WalBatch) -> Result<u64>;
    fn read(&self, namespace: &Namespace, from_lsn: u64) -> Result<Vec<WalRecord>>;
    fn truncate(&self, namespace: &Namespace, before_lsn: u64) -> Result<()>;
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalBatch {
    pub operations: Vec<WalOperation>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum WalOperation {
    Write {
        offset: u64,
        #[serde(with = "crate::object_store::payload")]
        bytes: Vec<u8>,
    },
    Truncate {
        size: u64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalRecord {
    pub append_id: String,
    pub namespace: Namespace,
    pub epoch: u64,
    pub lsn: u64,
    pub timestamp_millis: u64,
    pub batch: WalBatch,
    #[serde(default)]
    pub commit: Option<WalCommit>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalCommit {
    pub lsn: u64,
    pub position: WalPosition,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WalMetadata {
    pub epoch: u64,
    pub last_lsn: u64,
    pub replay_baseline_lsn: u64,
    pub(crate) entries: Vec<Entry>,
    pub(crate) receipts: BTreeMap<String, u64>,
    #[serde(default)]
    pub(crate) receipt_digests: BTreeMap<String, String>,
    #[serde(default)]
    pub commit: Option<WalCommit>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Entry {
    pub(crate) lsn: u64,
    pub(crate) hash: String,
}

pub struct RemoteWal {
    store: Arc<dyn ObjectStore>,
}

#[derive(Debug)]
struct ChunkUploadFailure(anyhow::Error);

impl std::fmt::Display for ChunkUploadFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for ChunkUploadFailure {}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalAppendRequest {
    pub namespace: Namespace,
    pub epoch: u64,
    pub batch: WalBatch,
    pub position: Option<WalPosition>,
    pub append_id: String,
    pub timestamp_millis: u64,
}

impl WalAppendRequest {
    fn record(&self, lsn: u64) -> WalRecord {
        WalRecord {
            append_id: self.append_id.clone(),
            namespace: self.namespace.clone(),
            epoch: self.epoch,
            lsn,
            timestamp_millis: self.timestamp_millis,
            batch: self.batch.clone(),
            commit: self.position.map(|position| WalCommit { lsn, position }),
        }
    }
    pub(crate) fn validate(&self) -> Result<()> {
        Namespace::new(
            self.namespace.tenant.clone(),
            self.namespace.database.clone(),
        )?;
        ensure!(
            self.epoch > 0
                && self.append_id.len() <= 128
                && !self.append_id.is_empty()
                && self
                    .append_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
            "invalid WAL append identity"
        );
        ensure!(
            self.batch.operations.len() <= 16_384,
            "WAL operation limit exceeded"
        );
        let mut bytes = 0usize;
        for operation in &self.batch.operations {
            if let WalOperation::Write { bytes: payload, .. } = operation {
                bytes = bytes
                    .checked_add(payload.len())
                    .context("WAL payload overflow")?;
            }
        }
        ensure!(
            bytes <= crate::authority_rpc::protocol::MAX_RPC_BYTES / 2,
            "WAL payload limit exceeded"
        );
        Ok(())
    }
}

/// Bytes a replay may hold in flight, and the window that budget buys.
///
/// Without a window a cold open pays one serial round trip per chunk, so its cost is the whole
/// history. The window is bounded by *bytes* rather than by chunks because it is memory, not
/// request count, that must not grow with the history: every in-flight fetch holds one chunk,
/// and one chunk is at most half the authority RPC ceiling (`WalAppendRequest::validate`).
/// Charging that ceiling per fetch makes `window * WAL_CHUNK_CHARGE` a provable bound on the
/// bytes held rather than an estimate from the sizes seen so far — a later chunk may be larger
/// than any earlier one, so an estimate could be exceeded by the very fetch it admitted. The
/// window is capped separately as well: every fetch occupies one of the authority client's call
/// slots (`authority_rpc::client::LIVE_CONNECTIONS`) that the other sessions on this worker need.
const WAL_REPLAY_BYTES_ENV: &str = "PERI_WORKER_WAL_REPLAY_BYTES";
/// What the ledger charges one in-flight fetch: the raw ceiling of one chunk, not its sample.
pub(crate) const WAL_CHUNK_CHARGE: usize = crate::authority_rpc::protocol::MAX_RPC_BYTES / 2;
/// Four chunks at the ceiling: the conservative default this replaces, now stated in bytes.
const WAL_REPLAY_BYTES_DEFAULT: usize = 4 * WAL_CHUNK_CHARGE;
const WAL_REPLAY_WINDOW_MAX: usize = 8;

// The cap is only worth the call-slot tradeoff if it keeps one cold open out of the slots the
// worker's other sessions are using, so the relation is checked where the constant is set rather
// than in a case that would have to restate both numbers.
const _: () = assert!(
    WAL_REPLAY_WINDOW_MAX * 3 <= crate::authority_rpc::client::LIVE_CONNECTIONS,
    "a WAL replay may hold at most a third of the authority client call gate"
);

/// Illegal values fail closed: replay is on the open path, and a budget silently coerced to
/// something else would hide the misconfiguration behind a slower or hungrier open.
fn wal_replay_window() -> Result<usize> {
    let budget = match std::env::var(WAL_REPLAY_BYTES_ENV) {
        Ok(value) => value
            .parse::<usize>()
            .with_context(|| format!("invalid {WAL_REPLAY_BYTES_ENV}"))?,
        Err(std::env::VarError::NotPresent) => WAL_REPLAY_BYTES_DEFAULT,
        Err(error) => return Err(error.into()),
    };
    ensure!(
        budget > 0,
        "invalid {WAL_REPLAY_BYTES_ENV}: expected a positive byte budget"
    );
    Ok(wal_replay_window_for(budget))
}

/// The budget expressed as a window. A replay cannot fetch nothing, so every accepted budget
/// buys at least one chunk even when it is smaller than the charge; the upper cap keeps the
/// call-slot gate described above intact.
fn wal_replay_window_for(budget: usize) -> usize {
    (budget / WAL_CHUNK_CHARGE).clamp(1, WAL_REPLAY_WINDOW_MAX)
}

/// Reads and verifies one chunk. Runs on a prefetch thread, so it borrows the store and
/// owns nothing but the key it derives.
fn load_chunk_record(
    store: &dyn ObjectStore,
    namespace: &Namespace,
    entry: &Entry,
) -> Result<WalRecord> {
    let object = store
        .get(&namespace.key(&format!("wal/chunks/{}", entry.hash))?)?
        .context("committed WAL chunk missing")?;
    ensure!(
        blake3::hash(&object.bytes).to_hex().as_str() == entry.hash,
        "WAL chunk checksum mismatch"
    );
    let record: WalRecord = serde_json::from_slice(&object.bytes)?;
    ensure!(
        record.namespace == *namespace && record.lsn == entry.lsn,
        "WAL record identity mismatch"
    );
    Ok(record)
}

impl RemoteWal {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    fn check_writer(&self) -> Result<()> {
        ensure!(
            self.store.access_role() == crate::ownership::types::AccessRole::Writer,
            "read-only WAL mutation"
        );
        self.store.validate_session()
    }

    fn index(&self, namespace: &Namespace) -> Result<(WalMetadata, Option<String>)> {
        self.store.validate_session()?;
        match self.store.get(&namespace.key("wal/index")?)? {
            Some(object) => {
                let index: WalMetadata = serde_json::from_slice(&object.bytes)?;
                ensure!(
                    index
                        .entries
                        .windows(2)
                        .all(|pair| pair[0].lsn < pair[1].lsn),
                    "unordered WAL index"
                );
                ensure!(
                    index.entries.last().map_or(0, |entry| entry.lsn) == index.last_lsn,
                    "incomplete WAL index"
                );
                ensure!(
                    index.replay_baseline_lsn == 0
                        || index
                            .entries
                            .first()
                            .is_some_and(|entry| entry.lsn == index.replay_baseline_lsn),
                    "missing WAL replay baseline"
                );
                ensure!(
                    index
                        .receipts
                        .values()
                        .all(|lsn| *lsn > 0 && *lsn <= index.last_lsn),
                    "invalid WAL append receipts"
                );
                ensure!(
                    index
                        .commit
                        .is_none_or(|commit| commit.lsn > 0 && commit.lsn == index.last_lsn),
                    "invalid WAL index commit LSN"
                );
                crate::work_metrics::observe_wal_index_read(
                    object.bytes.len(),
                    index.entries.len(),
                );
                Ok((index, Some(object.etag)))
            }
            None => Ok((WalMetadata::default(), None)),
        }
    }

    pub fn metadata(&self, namespace: &Namespace) -> Result<WalMetadata> {
        Ok(self.index(namespace)?.0)
    }

    fn chunk(&self, record: &WalRecord) -> Result<String> {
        self.check_writer()?;
        let bytes = serde_json::to_vec(record)?;
        let hash = blake3::hash(&bytes).to_hex().to_string();
        let key = record.namespace.key(&format!("wal/chunks/{hash}"))?;
        if let Err(error) = self.store.put(&key, &bytes, PutCondition::Absent) {
            let existing = self.store.get(&key)?.context("WAL chunk upload failed")?;
            ensure!(
                existing.bytes == bytes,
                "immutable WAL chunk conflict: {error}"
            );
        }
        Ok(hash)
    }

    fn commit(
        &self,
        namespace: &Namespace,
        metadata: &WalMetadata,
        etag: Option<String>,
    ) -> Result<()> {
        self.check_writer()?;
        let encoded = serde_json::to_vec(metadata)?;
        crate::work_metrics::observe_wal_index_commit(encoded.len(), metadata.receipts.len());
        self.store.put(
            &namespace.key("wal/index")?,
            &encoded,
            etag.map_or(PutCondition::Absent, PutCondition::Match),
        )?;
        Ok(())
    }

    pub fn fence(&self, namespace: &Namespace, epoch: u64) -> Result<()> {
        self.check_writer()?;
        for _ in 0..64 {
            let (mut index, etag) = self.index(namespace)?;
            ensure!(epoch >= index.epoch, "stale WAL epoch");
            index.epoch = epoch;
            match self.commit(namespace, &index, etag.clone()) {
                Ok(()) => return Ok(()),
                Err(error) if self.index(namespace)?.1 == etag => return Err(error),
                Err(_) => crate::work_metrics::observe_wal_index_retry(),
            }
        }
        bail!("WAL fencing CAS contention")
    }
    pub fn append_commit(
        &self,
        namespace: &Namespace,
        epoch: u64,
        batch: WalBatch,
        position: Option<WalPosition>,
    ) -> Result<u64> {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let append_id = format!(
            "{}-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        let timestamp_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis()
            .try_into()?;
        let request = WalAppendRequest {
            namespace: namespace.clone(),
            epoch,
            batch,
            position,
            append_id,
            timestamp_millis,
        };
        if let Some(lsn) = self.store.append_wal(&request)? {
            return Ok(lsn);
        }
        self.append_local(&request)
    }

    pub(crate) fn append_local(&self, request: &WalAppendRequest) -> Result<u64> {
        self.append_using(request, |record, index, etag| {
            self.chunk(record)
                .map_err(|error| anyhow::Error::new(ChunkUploadFailure(error)))?;
            self.commit(&record.namespace, index, etag)
        })
    }

    pub(crate) fn append_using<Publish>(
        &self,
        request: &WalAppendRequest,
        mut publish: Publish,
    ) -> Result<u64>
    where
        Publish: FnMut(&WalRecord, &WalMetadata, Option<String>) -> Result<()>,
    {
        let WalAppendRequest {
            namespace,
            epoch,
            append_id,
            ..
        } = request;
        self.check_writer()?;
        for _ in 0..64 {
            let (mut index, etag) = self.index(namespace)?;
            ensure!(*epoch >= index.epoch, "stale WAL epoch");
            if let Some(lsn) = index.receipts.get(append_id) {
                return self.verified_receipt(request, &index, *lsn);
            }
            let lsn = index.last_lsn.checked_add(1).context("WAL LSN exhausted")?;
            let record = request.record(lsn);
            let entry = Entry {
                lsn,
                hash: blake3::hash(&serde_json::to_vec(&record)?)
                    .to_hex()
                    .to_string(),
            };
            index.entries.push(entry);
            index.receipts.insert(append_id.clone(), lsn);
            index.receipt_digests.insert(
                append_id.clone(),
                blake3::hash(&serde_json::to_vec(&record)?)
                    .to_hex()
                    .to_string(),
            );
            index.last_lsn = lsn;
            index.epoch = *epoch;
            index.commit = record.commit;
            match publish(&record, &index, etag.clone()) {
                Ok(()) => return Ok(lsn),
                Err(error) => {
                    if error.is::<ChunkUploadFailure>() {
                        return Err(error.downcast::<ChunkUploadFailure>().unwrap().0);
                    }
                    let (current, current_etag) = self.index(namespace)?;
                    ensure!(*epoch >= current.epoch, "stale WAL epoch");
                    if let Some(lsn) = current.receipts.get(append_id) {
                        return self.verified_receipt(request, &current, *lsn);
                    }
                    if current_etag == etag {
                        return Err(error);
                    }
                    // The conditional write lost to another appender, so this attempt is
                    // rebuilt from the index that won.
                    crate::work_metrics::observe_wal_index_retry();
                }
            }
        }
        bail!("WAL append CAS contention")
    }

    fn verified_receipt(
        &self,
        request: &WalAppendRequest,
        index: &WalMetadata,
        lsn: u64,
    ) -> Result<u64> {
        self.check_writer()?;
        let expected = blake3::hash(&serde_json::to_vec(&request.record(lsn))?);
        if let Some(digest) = index.receipt_digests.get(&request.append_id) {
            ensure!(
                digest == expected.to_hex().as_str(),
                "WAL receipt request identity or content conflict"
            );
        }
        let entry = index
            .entries
            .iter()
            .find(|entry| entry.lsn == lsn)
            .context("WAL receipt content unavailable; refusing replay")?;
        let object = self
            .store
            .get(
                &request
                    .namespace
                    .key(&format!("wal/chunks/{}", entry.hash))?,
            )?
            .context("WAL receipt chunk unavailable; refusing replay")?;
        ensure!(
            blake3::hash(&object.bytes).to_hex().as_str() == entry.hash,
            "WAL receipt checksum mismatch"
        );
        let record: WalRecord = serde_json::from_slice(&object.bytes)?;
        let persisted = blake3::hash(&serde_json::to_vec(&record)?);
        ensure!(
            persisted == expected,
            "WAL receipt request identity or content conflict"
        );
        self.check_writer()?;
        Ok(lsn)
    }

    pub fn for_each_record(
        &self,
        namespace: &Namespace,
        from_lsn: u64,
        visit: impl FnMut(WalRecord) -> Result<()>,
    ) -> Result<()> {
        self.for_each_record_with_window(namespace, from_lsn, wal_replay_window()?, visit)
    }

    /// Same contract as [`Self::for_each_record`], with the fetch window given rather than
    /// derived from the environment.
    ///
    /// Chunks are fetched ahead of the delivery cursor and delivered in index order, so
    /// `visit` sees exactly the sequence a serial replay would produce and the first error
    /// returned is the one belonging to the lowest LSN — a later chunk that answered
    /// earlier can neither be visited nor reported first. The index is walked lazily and the
    /// entry list is never materialised, so what this holds does not grow with the history:
    /// at most `window` chunks at [`WAL_CHUNK_CHARGE`] bytes each, and a chunk the visitor has
    /// seen is not retained. A refill starts only after a chunk was accepted, so a rejected
    /// chunk never starts work whose result nothing reads.
    fn for_each_record_with_window(
        &self,
        namespace: &Namespace,
        from_lsn: u64,
        window: usize,
        mut visit: impl FnMut(WalRecord) -> Result<()>,
    ) -> Result<()> {
        ensure!(window > 0, "invalid WAL replay window");
        let index = self.index(namespace)?.0;
        ensure!(
            from_lsn == 0 || from_lsn >= index.replay_baseline_lsn,
            "requested WAL history was truncated; replay from baseline"
        );
        let mut pending = index.entries.iter().filter(|entry| entry.lsn >= from_lsn);
        let store: &dyn ObjectStore = &*self.store;
        std::thread::scope(|scope| -> Result<()> {
            let mut in_flight: VecDeque<std::thread::ScopedJoinHandle<'_, Result<WalRecord>>> =
                VecDeque::with_capacity(window);
            while in_flight.len() < window {
                let Some(entry) = pending.next() else {
                    break;
                };
                in_flight
                    .push_back(scope.spawn(move || load_chunk_record(store, namespace, entry)));
            }
            while let Some(handle) = in_flight.pop_front() {
                let record = match handle.join() {
                    Ok(record) => record?,
                    Err(_) => bail!("WAL chunk prefetch worker panicked"),
                };
                // Refill once the chunk was accepted, before the visitor sees it, so the
                // window stays full while the visitor applies it. Threads still in flight are
                // joined by the scope, including on the error and `visit` failure paths.
                if let Some(entry) = pending.next() {
                    in_flight
                        .push_back(scope.spawn(move || load_chunk_record(store, namespace, entry)));
                }
                visit(record)?;
            }
            Ok(())
        })
    }
}

impl DurableWal for RemoteWal {
    fn append(&self, namespace: &Namespace, epoch: u64, batch: WalBatch) -> Result<u64> {
        self.append_commit(namespace, epoch, batch, None)
    }

    fn read(&self, namespace: &Namespace, from_lsn: u64) -> Result<Vec<WalRecord>> {
        let mut records = Vec::new();
        self.for_each_record(namespace, from_lsn, |record| {
            records.push(record);
            Ok(())
        })?;
        Ok(records)
    }

    fn truncate(&self, namespace: &Namespace, before_lsn: u64) -> Result<()> {
        self.check_writer()?;
        for _ in 0..64 {
            let (mut index, etag) = self.index(namespace)?;
            let removed: Vec<_> = index
                .entries
                .iter()
                .filter(|entry| entry.lsn < before_lsn)
                .collect();
            let Some(last) = removed.last() else {
                return Ok(());
            };
            let baseline_lsn = last.lsn;
            let records = self.read(namespace, 0)?;
            if self.index(namespace)?.1 != etag {
                continue;
            }
            let mut bytes = Vec::new();
            for record in records.iter().filter(|record| record.lsn <= baseline_lsn) {
                apply_batch(&mut bytes, &record.batch)?;
            }
            let baseline = WalRecord {
                append_id: format!("baseline-{baseline_lsn}"),
                namespace: namespace.clone(),
                epoch: index.epoch,
                lsn: baseline_lsn,
                timestamp_millis: records
                    .iter()
                    .find(|record| record.lsn == baseline_lsn)
                    .context("missing baseline record")?
                    .timestamp_millis,
                batch: WalBatch {
                    operations: vec![
                        WalOperation::Truncate { size: 0 },
                        WalOperation::Write { offset: 0, bytes },
                    ],
                },
                commit: records
                    .iter()
                    .find(|record| record.lsn == baseline_lsn)
                    .and_then(|record| record.commit),
            };
            let hash = self.chunk(&baseline)?;
            index.entries.retain(|entry| entry.lsn >= before_lsn);
            index.entries.insert(
                0,
                Entry {
                    lsn: baseline_lsn,
                    hash,
                },
            );
            index.replay_baseline_lsn = baseline_lsn;
            match self.commit(namespace, &index, etag.clone()) {
                Ok(()) => return Ok(()),
                Err(error) if self.index(namespace)?.1 == etag => return Err(error),
                Err(_) => {}
            }
        }
        bail!("WAL truncate CAS contention")
    }
}

pub(crate) fn apply_batch(target: &mut Vec<u8>, batch: &WalBatch) -> Result<()> {
    for operation in &batch.operations {
        match operation {
            WalOperation::Write { offset, bytes } => {
                let offset = usize::try_from(*offset)?;
                let end = offset
                    .checked_add(bytes.len())
                    .context("WAL write overflow")?;
                if target.len() < end {
                    target.try_reserve(end - target.len())?;
                    target.resize(end, 0);
                }
                target[offset..end].copy_from_slice(bytes);
            }
            WalOperation::Truncate { size } => {
                let size = usize::try_from(*size)?;
                if size > target.len() {
                    target.try_reserve(size - target.len())?;
                }
                target.resize(size, 0);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod receipt_tests {
    use super::*;
    use crate::object_store::LocalObjectStore;

    fn request() -> Result<WalAppendRequest> {
        Ok(WalAppendRequest {
            namespace: Namespace::new("receipt", "binding")?,
            epoch: 10,
            batch: WalBatch::default(),
            position: None,
            append_id: "controlled-id".into(),
            timestamp_millis: 1,
        })
    }

    fn change(request: &mut WalAppendRequest, field: &str) {
        match field {
            "payload" => request.batch.operations.push(WalOperation::Write {
                offset: 7,
                bytes: vec![42],
            }),
            "position" => request.position = Some(WalPosition::default()),
            "epoch" => request.epoch = 11,
            "timestamp" => request.timestamp_millis = 2,
            "same" => {}
            _ => unreachable!(),
        }
    }

    #[test]
    fn receipt_initial_and_publication_error_paths_bind_full_content() -> Result<()> {
        for legacy in [false, true] {
            for field in ["same", "payload", "position", "epoch", "timestamp"] {
                let directory = tempfile::tempdir()?;
                let store = Arc::new(LocalObjectStore::open(directory.path())?);
                let wal = RemoteWal::new(store.clone());
                let original = request()?;
                assert_eq!(wal.append_local(&original)?, 1);
                if legacy {
                    let (mut index, etag) = wal.index(&original.namespace)?;
                    index.receipt_digests.clear();
                    wal.commit(&original.namespace, &index, etag)?;
                }
                let mut changed = original.clone();
                change(&mut changed, field);
                let before = store.get(&original.namespace.key("wal/index")?)?.unwrap();
                let result = wal.append_local(&changed);
                assert_eq!(
                    result.is_ok(),
                    field == "same",
                    "initial legacy={legacy} field={field}"
                );
                assert_eq!(
                    store
                        .get(&original.namespace.key("wal/index")?)?
                        .unwrap()
                        .bytes,
                    before.bytes
                );
                let mut stale = original.clone();
                stale.epoch = 9;
                assert!(wal
                    .append_local(&stale)
                    .unwrap_err()
                    .to_string()
                    .contains("stale WAL epoch"));
            }
            for field in ["same", "payload", "position", "epoch", "timestamp"] {
                let directory = tempfile::tempdir()?;
                let store = Arc::new(LocalObjectStore::open(directory.path())?);
                let wal = RemoteWal::new(store);
                let original = request()?;
                let mut competing = original.clone();
                change(&mut competing, field);
                let result = wal.append_using(&original, |_, _, _| {
                    wal.append_local(&competing)?;
                    if legacy {
                        let (mut index, etag) = wal.index(&original.namespace)?;
                        index.receipt_digests.clear();
                        wal.commit(&original.namespace, &index, etag)?;
                    }
                    bail!("publication response lost after physical terminal")
                });
                assert_eq!(
                    result.is_ok(),
                    field == "same",
                    "readback legacy={legacy} field={field}"
                );
                assert_eq!(wal.index(&original.namespace)?.0.last_lsn, 1);
            }
        }
        Ok(())
    }

    #[test]
    fn legacy_receipt_without_original_chunk_refuses_replay() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let wal = RemoteWal::new(store.clone());
        let request = request()?;
        wal.append_local(&request)?;
        let (mut index, etag) = wal.index(&request.namespace)?;
        index.receipt_digests.clear();
        wal.commit(&request.namespace, &index, etag)?;
        store.delete(
            &request
                .namespace
                .key(&format!("wal/chunks/{}", index.entries[0].hash))?,
            None,
        )?;
        assert!(wal
            .append_local(&request)
            .unwrap_err()
            .to_string()
            .contains("unavailable"));
        Ok(())
    }
}

#[cfg(test)]
mod replay_window_tests {
    use super::*;
    use crate::object_store::{PutCondition, StoredObject};
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    const CHILD_ENV: &str = "PERI_WAL_REPLAY_TEST_CHILD";
    const CHILD_TEST: &str = "wal::replay_window_tests::wal_replay_window_follows_the_byte_budget";

    #[derive(Clone, Copy)]
    enum Chunk {
        Present,
        Missing,
        Corrupt,
    }

    /// Fake branch store: serves a hand-built index and chunk bodies, and counts the chunk
    /// reads that overlapped, so the fetch window is observable from outside the replay.
    struct PrefetchStore {
        index: Vec<u8>,
        chunks: BTreeMap<String, Vec<u8>>,
        delays: BTreeMap<String, Duration>,
        missing: BTreeSet<String>,
        completed: Mutex<Vec<String>>,
        live: AtomicUsize,
        peak: AtomicUsize,
    }

    impl ObjectStore for PrefetchStore {
        fn get(&self, key: &str) -> Result<Option<StoredObject>> {
            if key.ends_with("/wal/index") {
                return Ok(Some(StoredObject {
                    bytes: self.index.clone(),
                    etag: "index".into(),
                }));
            }
            let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(live, Ordering::SeqCst);
            // Held past every return path, so a failed read still counts as in flight.
            let _live = Live(&self.live);
            if let Some(delay) = self.delays.get(key) {
                std::thread::sleep(*delay);
            }
            self.completed.lock().unwrap().push(key.to_owned());
            if self.missing.contains(key) {
                return Ok(None);
            }
            Ok(self.chunks.get(key).map(|bytes| StoredObject {
                bytes: bytes.clone(),
                etag: key.to_owned(),
            }))
        }

        fn put(&self, _: &str, _: &[u8], _: PutCondition) -> Result<String> {
            bail!("prefetch fixture store is read-only")
        }

        fn delete(&self, _: &str, _: Option<&str>) -> Result<()> {
            bail!("prefetch fixture store is read-only")
        }

        fn list(&self, _: &str) -> Result<Vec<String>> {
            bail!("prefetch fixture store is read-only")
        }
    }

    struct Live<'a>(&'a AtomicUsize);

    impl Drop for Live<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    struct Fixture {
        store: Arc<PrefetchStore>,
        namespace: Namespace,
        keys: BTreeMap<u64, String>,
    }

    impl Fixture {
        /// Writes the index and the chunk bodies a replay of `plan` would find, in the shape
        /// the append path publishes: `(lsn, read latency, chunk state)`.
        fn new(plan: &[(u64, u64, Chunk)]) -> Result<Self> {
            let namespace = Namespace::new("prefetch", "binding")?;
            let mut entries = Vec::new();
            let mut chunks = BTreeMap::new();
            let mut delays = BTreeMap::new();
            let mut missing = BTreeSet::new();
            let mut keys = BTreeMap::new();
            for &(lsn, delay_ms, chunk) in plan {
                let record = WalRecord {
                    append_id: format!("append-{lsn}"),
                    namespace: namespace.clone(),
                    epoch: 1,
                    lsn,
                    timestamp_millis: 1,
                    batch: WalBatch::default(),
                    commit: None,
                };
                let bytes = serde_json::to_vec(&record)?;
                let hash = blake3::hash(&bytes).to_hex().to_string();
                let key = namespace.key(&format!("wal/chunks/{hash}"))?;
                entries.push(Entry {
                    lsn,
                    hash: hash.clone(),
                });
                delays.insert(key.clone(), Duration::from_millis(delay_ms));
                match chunk {
                    Chunk::Present => {
                        chunks.insert(key.clone(), bytes);
                    }
                    // The index still names the hash of the real body, so only a reader that
                    // verifies the chunk can notice.
                    Chunk::Corrupt => {
                        chunks.insert(key.clone(), b"corrupted body".to_vec());
                    }
                    Chunk::Missing => {
                        missing.insert(key.clone());
                    }
                }
                keys.insert(lsn, key);
            }
            let index = WalMetadata {
                epoch: 1,
                last_lsn: entries.last().map_or(0, |entry: &Entry| entry.lsn),
                replay_baseline_lsn: 0,
                entries,
                receipts: BTreeMap::new(),
                receipt_digests: BTreeMap::new(),
                commit: None,
            };
            Ok(Self {
                store: Arc::new(PrefetchStore {
                    index: serde_json::to_vec(&index)?,
                    chunks,
                    delays,
                    missing,
                    completed: Mutex::new(Vec::new()),
                    live: AtomicUsize::new(0),
                    peak: AtomicUsize::new(0),
                }),
                namespace,
                keys,
            })
        }

        /// Replays with an explicit window, so the cases below pin one window each instead of
        /// inheriting whatever the environment of the suite happens to hold.
        fn replay(&self, from_lsn: u64, window: usize) -> Result<Vec<u64>> {
            let wal = RemoteWal::new(self.store.clone());
            let mut seen = Vec::new();
            wal.for_each_record_with_window(&self.namespace, from_lsn, window, |record| {
                seen.push(record.lsn);
                Ok(())
            })?;
            Ok(seen)
        }

        fn peak(&self) -> usize {
            self.store.peak.load(Ordering::SeqCst)
        }

        fn completion_order(&self) -> Vec<String> {
            self.store.completed.lock().unwrap().clone()
        }
    }

    #[test]
    fn replay_delivers_records_in_lsn_order_when_chunks_finish_in_reverse() -> Result<()> {
        // Every chunk waits longer than the one after it, so the reads finish in the reverse
        // of LSN order and only the delivery order can make `visit` ascending.
        let plan: Vec<(u64, u64, Chunk)> = (1..=6)
            .map(|lsn| (lsn, (7 - lsn) * 20, Chunk::Present))
            .collect();
        let fixture = Fixture::new(&plan)?;
        let ascending: Vec<String> = (1..=6).map(|lsn| fixture.keys[&lsn].clone()).collect();
        assert_eq!(fixture.replay(0, 4)?, (1..=6u64).collect::<Vec<_>>());
        assert_ne!(
            fixture.completion_order(),
            ascending,
            "the reads finished in LSN order, so this case proves nothing about delivery"
        );
        // The window covers the requested range, not the whole index.
        assert_eq!(fixture.replay(4, 2)?, vec![4u64, 5, 6]);
        Ok(())
    }

    #[test]
    fn replay_reports_the_lowest_lsn_failure_and_stops_delivering() -> Result<()> {
        // LSN 2 fails slowly, LSN 3 fails fast with a different reason: a window that reported
        // whichever chunk answered first would return LSN 3's error instead.
        let plan = [
            (1, 10, Chunk::Present),
            (2, 120, Chunk::Corrupt),
            (3, 10, Chunk::Missing),
        ];
        let fixture = Fixture::new(&plan)?;
        let wal = RemoteWal::new(fixture.store.clone());
        let mut seen = Vec::new();
        let error = wal
            .for_each_record_with_window(&fixture.namespace, 0, 4, |record| {
                seen.push(record.lsn);
                Ok(())
            })
            .unwrap_err();
        assert!(
            error.to_string().contains("WAL chunk checksum mismatch"),
            "{error}"
        );
        assert_eq!(seen, vec![1]);
        assert!(
            fixture.completion_order().contains(&fixture.keys[&3]),
            "the later failure was never fetched, so this case proves nothing about ordering"
        );
        Ok(())
    }

    #[test]
    fn replay_stops_delivering_when_the_visitor_rejects_a_record() -> Result<()> {
        let plan: Vec<(u64, u64, Chunk)> = (1..=6).map(|lsn| (lsn, 10, Chunk::Present)).collect();
        let fixture = Fixture::new(&plan)?;
        let wal = RemoteWal::new(fixture.store.clone());
        let mut seen = Vec::new();
        let error = wal
            .for_each_record_with_window(&fixture.namespace, 0, 3, |record| {
                seen.push(record.lsn);
                if record.lsn == 2 {
                    bail!("replay stopped by visitor");
                }
                Ok(())
            })
            .unwrap_err();
        assert!(
            error.to_string().contains("replay stopped by visitor"),
            "{error}"
        );
        assert_eq!(seen, vec![1, 2]);
        Ok(())
    }

    #[test]
    fn replay_bounds_in_flight_bytes_to_the_budget() -> Result<()> {
        let plan: Vec<(u64, u64, Chunk)> = (1..=12).map(|lsn| (lsn, 30, Chunk::Present)).collect();
        let fixture = Fixture::new(&plan)?;
        let window = wal_replay_window_for(WAL_REPLAY_BYTES_DEFAULT);
        assert_eq!(fixture.replay(0, window)?, (1..=12u64).collect::<Vec<_>>());
        let peak = fixture.peak();
        assert!(
            peak <= window,
            "read {peak} chunks at once, window {window}"
        );
        assert!(
            peak >= 2,
            "a {window}-wide window never overlapped a read: peak={peak}"
        );
        assert!(
            peak * WAL_CHUNK_CHARGE <= WAL_REPLAY_BYTES_DEFAULT,
            "held {peak} chunks, which the ledger charges as {} bytes, over a budget of {}",
            peak * WAL_CHUNK_CHARGE,
            WAL_REPLAY_BYTES_DEFAULT
        );
        Ok(())
    }

    /// The window is a *derived* quantity: the budget is the setting, the window is what it
    /// buys. Pinning the arithmetic here keeps the byte bound from drifting away from the
    /// window the replay actually uses.
    #[test]
    fn the_replay_window_is_the_budget_divided_by_the_per_chunk_charge() {
        assert_eq!(wal_replay_window_for(1), 1);
        assert_eq!(wal_replay_window_for(WAL_CHUNK_CHARGE - 1), 1);
        assert_eq!(wal_replay_window_for(WAL_CHUNK_CHARGE), 1);
        assert_eq!(wal_replay_window_for(WAL_CHUNK_CHARGE + 1), 1);
        assert_eq!(wal_replay_window_for(2 * WAL_CHUNK_CHARGE), 2);
        assert_eq!(wal_replay_window_for(WAL_REPLAY_BYTES_DEFAULT), 4);
        assert_eq!(wal_replay_window_for(usize::MAX), 8);
        assert!(
            wal_replay_window_for(WAL_REPLAY_BYTES_DEFAULT) >= 2,
            "the default budget has to buy a real window, not a serial replay"
        );
    }

    #[test]
    fn wal_replay_window_follows_the_byte_budget() -> Result<()> {
        if std::env::var_os(CHILD_ENV).is_some() {
            return child_case();
        }
        // The child re-exec is how the environment reaches `for_each_record`: this suite must
        // not mutate the process environment that its own parallel cases read from.
        let triple = (3 * WAL_CHUNK_CHARGE).to_string();
        for (value, expected) in [
            (Some("1".to_string()), "outcome=single"),
            (None, "outcome=default"),
            (Some(triple.clone()), "outcome=triple"),
            (Some("0".to_string()), "outcome=rejected"),
            (Some("abc".to_string()), "outcome=rejected"),
            (Some("-1".to_string()), "outcome=rejected"),
        ] {
            let mut command = std::process::Command::new(std::env::current_exe()?);
            command
                .args(["--exact", CHILD_TEST, "--nocapture"])
                .env(CHILD_ENV, "1");
            match &value {
                Some(value) => command.env(WAL_REPLAY_BYTES_ENV, value),
                None => command.env_remove(WAL_REPLAY_BYTES_ENV),
            };
            let output = command.output()?;
            let report = String::from_utf8_lossy(&output.stderr).into_owned();
            assert!(
                output.status.success(),
                "child for {WAL_REPLAY_BYTES_ENV}={value:?} failed: {report}"
            );
            assert!(
                report.contains("WAL_REPLAY_CHILD") && report.contains(expected),
                "child for {WAL_REPLAY_BYTES_ENV}={value:?} did not report {expected}: {report}"
            );
        }
        Ok(())
    }

    /// Child arm of [`wal_replay_window_follows_the_byte_budget`]: replays 12 chunks and asserts
    /// the window its budget asked for is the one the replay used.
    fn child_case() -> Result<()> {
        let plan: Vec<(u64, u64, Chunk)> = (1..=12).map(|lsn| (lsn, 20, Chunk::Present)).collect();
        let fixture = Fixture::new(&plan)?;
        let wal = RemoteWal::new(fixture.store.clone());
        let mut seen = 0;
        let result = wal.for_each_record(&fixture.namespace, 0, |_| {
            seen += 1;
            Ok(())
        });
        let peak = fixture.peak();
        let configured = std::env::var(WAL_REPLAY_BYTES_ENV);
        match (&configured, &result) {
            (Ok(value), Ok(())) if value == "1" => {
                assert_eq!(seen, 12);
                assert_eq!(
                    peak, 1,
                    "{WAL_REPLAY_BYTES_ENV}=1 replayed more than one chunk at a time"
                );
                eprintln!("WAL_REPLAY_CHILD value=1 outcome=single peak={peak} visited={seen}");
            }
            (Err(std::env::VarError::NotPresent), Ok(())) => {
                assert_eq!(seen, 12);
                let window = wal_replay_window_for(WAL_REPLAY_BYTES_DEFAULT);
                assert!(
                    (2..=window).contains(&peak),
                    "the default budget replayed with peak={peak}, expected 2..={window}"
                );
                eprintln!(
                    "WAL_REPLAY_CHILD value=unset outcome=default peak={peak} visited={seen}"
                );
            }
            (Ok(value), Ok(())) => {
                assert_eq!(seen, 12);
                let window = wal_replay_window_for(
                    value
                        .parse::<usize>()
                        .with_context(|| format!("invalid {WAL_REPLAY_BYTES_ENV}"))?,
                );
                assert_eq!(
                    window, 3,
                    "the triple-charge budget has to buy a window of 3"
                );
                assert!(
                    (2..=window).contains(&peak),
                    "a window of {window} replayed with peak={peak}"
                );
                assert!(
                    peak * WAL_CHUNK_CHARGE <= value.parse::<usize>()?,
                    "{peak} chunks in flight exceed the {value}-byte budget"
                );
                eprintln!(
                    "WAL_REPLAY_CHILD value={value} outcome=triple peak={peak} window={window}"
                );
            }
            (Ok(value), Err(error)) => {
                assert!(
                    error
                        .to_string()
                        .contains(&format!("invalid {WAL_REPLAY_BYTES_ENV}")),
                    "{error}"
                );
                assert_eq!(seen, 0, "a rejected budget must not have replayed a record");
                assert_eq!(peak, 0, "a rejected budget must not have read a chunk");
                eprintln!("WAL_REPLAY_CHILD value={value} outcome=rejected error={error}");
            }
            (_, outcome) => bail!("child case for {configured:?} ended as {outcome:?}"),
        }
        Ok(())
    }

    /// Manual timing harness: `cargo test --lib -- --ignored --nocapture`.
    ///
    /// Fake-store microbenchmark, not platform evidence: the sleeps stand in for authority
    /// round trips and no authority, object store, TLS or contention is involved, so the
    /// numbers cannot be extrapolated to a real open. Both arms run the same code path and
    /// differ only in the window, and neither reads `PERI_WORKER_WAL_REPLAY_BYTES`.
    #[test]
    #[ignore = "manual fake-store timing comparison; not platform performance evidence"]
    fn wal_replay_wall_clock_against_serial_replay() -> Result<()> {
        const CHUNKS: u64 = 20;
        const READ_MS: u64 = 30;
        let plan: Vec<(u64, u64, Chunk)> = (1..=CHUNKS)
            .map(|lsn| (lsn, READ_MS, Chunk::Present))
            .collect();
        let mut walls = BTreeMap::new();
        for window in [1, 4] {
            let fixture = Fixture::new(&plan)?;
            let started = Instant::now();
            let seen = fixture.replay(0, window)?;
            let wall_ms = started.elapsed().as_millis();
            assert_eq!(seen, (1..=CHUNKS).collect::<Vec<_>>());
            eprintln!(
                "WAL_REPLAY_BENCH chunks={CHUNKS} read_ms={READ_MS} window={window} \
                 wall_ms={wall_ms} peak={}",
                fixture.peak(),
            );
            walls.insert(window, wall_ms);
        }
        eprintln!(
            "WAL_REPLAY_BENCH_SUMMARY serial_ms={} window4_ms={} ratio={:.2}",
            walls[&1],
            walls[&4],
            walls[&1] as f64 / walls[&4] as f64
        );
        Ok(())
    }
}
