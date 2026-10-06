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
    Write { offset: u64, bytes: Vec<u8> },
    Truncate { size: u64 },
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

impl RemoteWal {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    fn index(&self, namespace: &Namespace) -> Result<(WalMetadata, Option<String>)> {
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
        self.store.put(
            &namespace.key("wal/index")?,
            &serde_json::to_vec(metadata)?,
            etag.map_or(PutCondition::Absent, PutCondition::Match),
        )?;
        Ok(())
    }

    pub fn fence(&self, namespace: &Namespace, epoch: u64) -> Result<()> {
        for _ in 0..64 {
            let (mut index, etag) = self.index(namespace)?;
            ensure!(epoch >= index.epoch, "stale WAL epoch");
            index.epoch = epoch;
            match self.commit(namespace, &index, etag.clone()) {
                Ok(()) => return Ok(()),
                Err(error) if self.index(namespace)?.1 == etag => return Err(error),
                Err(_) => {}
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
        for _ in 0..64 {
            let (mut index, etag) = self.index(namespace)?;
            if let Some(lsn) = index.receipts.get(&append_id) {
                return Ok(*lsn);
            }
            ensure!(epoch >= index.epoch, "stale WAL epoch");
            let lsn = index.last_lsn.checked_add(1).context("WAL LSN exhausted")?;
            let record = WalRecord {
                append_id: append_id.clone(),
                namespace: namespace.clone(),
                epoch,
                lsn,
                timestamp_millis,
                batch: batch.clone(),
                commit: position.map(|position| WalCommit { lsn, position }),
            };
            let entry = Entry {
                lsn,
                hash: self.chunk(&record)?,
            };
            index.entries.push(entry);
            index.receipts.insert(append_id.clone(), lsn);
            index.last_lsn = lsn;
            index.epoch = epoch;
            index.commit = record.commit;
            match self.commit(namespace, &index, etag.clone()) {
                Ok(()) => return Ok(lsn),
                Err(error) => {
                    let (current, current_etag) = self.index(namespace)?;
                    if let Some(lsn) = current.receipts.get(&append_id) {
                        return Ok(*lsn);
                    }
                    if current_etag == etag {
                        return Err(error);
                    }
                }
            }
        }
        bail!("WAL append CAS contention")
    }
}

impl DurableWal for RemoteWal {
    fn append(&self, namespace: &Namespace, epoch: u64, batch: WalBatch) -> Result<u64> {
        self.append_commit(namespace, epoch, batch, None)
    }

    fn read(&self, namespace: &Namespace, from_lsn: u64) -> Result<Vec<WalRecord>> {
        let index = self.index(namespace)?.0;
        ensure!(
            from_lsn == 0 || from_lsn >= index.replay_baseline_lsn,
            "requested WAL history was truncated; replay from baseline"
        );
        let mut records = Vec::new();
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
            records.push(record);
        }
        Ok(records)
    }

    fn truncate(&self, namespace: &Namespace, before_lsn: u64) -> Result<()> {
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
