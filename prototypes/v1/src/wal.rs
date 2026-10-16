use std::collections::BTreeMap;
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
        mut visit: impl FnMut(WalRecord) -> Result<()>,
    ) -> Result<()> {
        let index = self.index(namespace)?.0;
        ensure!(
            from_lsn == 0 || from_lsn >= index.replay_baseline_lsn,
            "requested WAL history was truncated; replay from baseline"
        );
        for entry in index.entries.iter().filter(|entry| entry.lsn >= from_lsn) {
            let object = self
                .store
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
            visit(record)?;
        }
        Ok(())
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
