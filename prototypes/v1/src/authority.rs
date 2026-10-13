mod branch;
mod catalog;
mod codec;
mod migration;
pub mod ownership;
mod ownership_view;
pub(crate) mod request_reads;
mod semantic;
pub mod types;

pub use branch::BranchStore;
pub use types::{BranchRef, CatalogRoot, ObjectEntry, RecoveryPoint, RootSnapshot, Tombstone};

use crate::ownership::types::{MutationGuard, MutationStage};
use crate::{
    namespace::Namespace,
    object_store::{ObjectStore, PreconditionFailed, PutCondition},
};
use anyhow::{ensure, Context, Result};
use codec::{decode, encode};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

pub struct Authority {
    store: Arc<dyn ObjectStore>,
    namespace: Namespace,
    incarnation: String,
    guard: Option<Arc<dyn MutationGuard>>,
}

#[cfg(test)]
mod upload_batch_tests {
    use super::*;
    use crate::object_store::LocalObjectStore;

    #[test]
    fn segment_batch_reserves_intents_once_and_verifies_physical_bytes() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let authority = Authority::open(store, Namespace::new("bulk", "segments")?)?;
        let segments = vec![
            vec![0x11; crate::manifest::SEGMENT_SIZE],
            vec![0x22; crate::manifest::SEGMENT_SIZE],
        ];
        let before = authority.snapshot()?.root.revision;
        let entries = authority.upload_segments(&segments)?;
        assert_eq!(authority.snapshot()?.root.revision, before + 1);
        for (entry, expected) in entries.iter().zip(&segments) {
            assert_eq!(authority.read_blob(entry)?, *expected);
            assert_eq!(
                authority.snapshot()?.root.uploads.get(&entry.blob),
                Some(entry)
            );
        }
        authority.register_segments(&entries)?;
        for entry in &entries {
            let key = format!("segments/{}", entry.hash);
            assert_eq!(authority.snapshot()?.root.objects.get(&key), Some(entry));
        }
        let before = authority.snapshot()?.root.revision;
        assert!(authority.upload_segments(&[]).is_err());
        assert!(authority.upload_segments(&[vec![0; 1]]).is_err());
        assert_eq!(authority.snapshot()?.root.revision, before);
        Ok(())
    }

    #[test]
    fn full_segment_batch_uses_two_catalog_publications() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let authority = Authority::open(
            Arc::new(LocalObjectStore::open(directory.path())?),
            Namespace::new("bulk", "full")?,
        )?;
        let segments = (0..128)
            .map(|index| {
                let mut bytes = vec![0; crate::manifest::SEGMENT_SIZE];
                bytes[..8].copy_from_slice(&(index as u64).to_be_bytes());
                bytes
            })
            .collect::<Vec<_>>();
        let before = authority.snapshot()?.root.revision;
        let entries = authority.upload_segments(&segments)?;
        authority.register_segments(&entries)?;
        let after = authority.snapshot()?.root;
        assert_eq!(after.revision, before + 2);
        assert_eq!(after.objects.len(), segments.len());
        Ok(())
    }
}

pub(super) fn now_ms() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

pub(super) fn unique_id() -> Result<String> {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let mut hash = blake3::Hasher::new();
    hash.update(&time.to_le_bytes());
    hash.update(&std::process::id().to_le_bytes());
    hash.update(&SEQUENCE.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    Ok(hash.finalize().to_hex().to_string())
}

impl Authority {
    pub fn open(store: Arc<dyn ObjectStore>, namespace: Namespace) -> Result<Arc<Self>> {
        let namespace = Namespace::new(namespace.tenant, namespace.database)?;
        let key = namespace.key("authority/root")?;
        let object = match store.get(&key)? {
            Some(object) => object,
            None => {
                let root = CatalogRoot {
                    version: 1,
                    revision: 1,
                    incarnation: unique_id()?,
                    namespace: namespace.clone(),
                    ownership: None,
                    blobs: BTreeMap::new(),
                    receipts: BTreeMap::new(),
                    objects: BTreeMap::new(),
                    branches: BTreeMap::from([("main".into(), BranchRef::default())]),
                    snapshots: BTreeMap::new(),
                    pins: BTreeMap::new(),
                    uploads: BTreeMap::new(),
                    tombstones: BTreeMap::new(),
                };
                let result = store.put(&key, &encode(&root)?, PutCondition::Absent);
                match store.get(&key)? {
                    Some(object) => object,
                    None => {
                        return Err(result.err().unwrap_or_else(|| {
                            anyhow::anyhow!("authority root missing after initialization")
                        }))
                    }
                }
            }
        };
        let root = decode(&object.bytes)?;
        ensure!(
            root.namespace == namespace,
            "authority root namespace mismatch"
        );
        let authority = Arc::new(Self {
            store,
            namespace,
            incarnation: root.incarnation.clone(),
            guard: None,
        });
        authority.validate_root(&root)?;
        Ok(authority)
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    pub(crate) fn scoped(&self, guard: Arc<dyn MutationGuard>) -> Arc<Self> {
        Arc::new(Self {
            store: self.store.clone(),
            namespace: self.namespace.clone(),
            incarnation: self.incarnation.clone(),
            guard: Some(guard),
        })
    }

    fn check(&self, stage: MutationStage) -> Result<()> {
        if let Some(guard) = &self.guard {
            guard.validate(stage)?;
        }
        Ok(())
    }

    fn uncertain(&self, stage: MutationStage, error: &anyhow::Error) {
        if !error.is::<PreconditionFailed>() {
            if let Some(guard) = &self.guard {
                guard.uncertain(stage);
            }
        }
    }

    pub fn snapshot(&self) -> Result<RootSnapshot> {
        self.check(MutationStage::Read)?;
        let object = self
            .store
            .get(&self.namespace.key("authority/root")?)?
            .context("authority root missing; refusing reset")?;
        let root = decode(&object.bytes)?;
        self.validate_root(&root)?;
        Ok(RootSnapshot {
            root,
            etag: object.etag,
        })
    }

    pub fn transact<F>(&self, expected_revision: Option<u64>, mutation: F) -> Result<RootSnapshot>
    where
        F: FnMut(&mut CatalogRoot) -> Result<()>,
    {
        self.transact_inner(expected_revision, mutation, None)
    }

    fn transact_inner<F>(
        &self,
        expected_revision: Option<u64>,
        mut mutation: F,
        deletion: Option<&str>,
    ) -> Result<RootSnapshot>
    where
        F: FnMut(&mut CatalogRoot) -> Result<()>,
    {
        let _reads = request_reads::Scope::enter();
        let operation = unique_id()?;
        for _ in 0..128 {
            let current = self.snapshot()?;
            if let Some(revision) = expected_revision {
                ensure!(current.root.revision == revision, PreconditionFailed);
            }
            let mut candidate = current.root.clone();
            mutation(&mut candidate)?;
            self.prepare(&current.root, &mut candidate, deletion)?;
            candidate.revision = current
                .root
                .revision
                .checked_add(1)
                .context("authority revision overflow")?;
            candidate
                .receipts
                .insert(operation.clone(), candidate.revision);
            self.validate_root(&candidate)?;
            let encoded = encode(&candidate)?;
            if let Some(guard) = &self.guard {
                guard.validate_root(&current.root, &candidate)?;
            }
            self.check(MutationStage::RootPublish)?;
            match self.store.put(
                &self.namespace.key("authority/root")?,
                &encoded,
                PutCondition::Match(current.etag),
            ) {
                Ok(etag) => {
                    return Ok(RootSnapshot {
                        root: candidate,
                        etag,
                    })
                }
                Err(error) => {
                    self.uncertain(MutationStage::RootPublish, &error);
                    let observed = self.snapshot()?;
                    if observed.root.receipts.get(&operation) == Some(&candidate.revision) {
                        return Ok(observed);
                    }
                    if error.is::<PreconditionFailed>() && expected_revision.is_none() {
                        continue;
                    }
                    return Err(error);
                }
            }
        }
        Err(PreconditionFailed.into())
    }

    pub fn upload(&self, bytes: &[u8]) -> Result<ObjectEntry> {
        self.check(MutationStage::Upload)?;
        let entry = Self::upload_entry(bytes)?;
        self.transact(None, |root| {
            ensure!(
                !root.blobs.contains_key(&entry.blob) && !root.tombstones.contains_key(&entry.blob),
                "physical instance collision"
            );
            root.blobs.insert(entry.blob.clone(), entry.clone());
            root.uploads.insert(entry.blob.clone(), entry.clone());
            Ok(())
        })?;
        self.finish_upload(entry, bytes)
    }

    pub(crate) fn upload_segments(&self, segments: &[Vec<u8>]) -> Result<Vec<ObjectEntry>> {
        ensure!(
            !segments.is_empty() && segments.len() <= 128,
            "invalid segment upload batch"
        );
        self.check(MutationStage::Upload)?;
        let entries = segments
            .iter()
            .map(|segment| {
                ensure!(
                    segment.len() == crate::manifest::SEGMENT_SIZE,
                    "invalid segment upload size"
                );
                Self::upload_entry(segment)
            })
            .collect::<Result<Vec<_>>>()?;
        self.transact(None, |root| {
            for entry in &entries {
                ensure!(
                    !root.blobs.contains_key(&entry.blob)
                        && !root.tombstones.contains_key(&entry.blob),
                    "physical instance collision"
                );
                root.blobs.insert(entry.blob.clone(), entry.clone());
                root.uploads.insert(entry.blob.clone(), entry.clone());
            }
            Ok(())
        })?;
        entries
            .into_iter()
            .zip(segments)
            .map(|(entry, bytes)| self.finish_upload(entry, bytes))
            .collect()
    }

    pub(crate) fn register_segments(&self, entries: &[ObjectEntry]) -> Result<()> {
        ensure!(
            !entries.is_empty() && entries.len() <= 128,
            "invalid segment registration batch"
        );
        for entry in entries {
            ensure!(
                self.read_blob(entry)?.len() == crate::manifest::SEGMENT_SIZE,
                "invalid staged segment size"
            );
        }
        self.transact(None, |root| {
            for entry in entries {
                semantic::registered(root, entry)?;
                let key = format!("segments/{}", entry.hash);
                ensure!(
                    root.objects
                        .get(&key)
                        .is_none_or(|current| current == entry),
                    "segment logical identity conflict"
                );
                root.objects.insert(key, entry.clone());
            }
            Ok(())
        })?;
        Ok(())
    }

    fn upload_entry(bytes: &[u8]) -> Result<ObjectEntry> {
        Ok(ObjectEntry {
            blob: unique_id()?,
            hash: blake3::hash(bytes).to_hex().to_string(),
            etag: unique_id()?,
            created_ms: now_ms()?,
        })
    }

    pub(super) fn upload_wal_pair<Verify>(
        &self,
        chunk_bytes: &[u8],
        index_bytes: &[u8],
        mut verify: Verify,
    ) -> Result<(ObjectEntry, ObjectEntry)>
    where
        Verify: FnMut(&CatalogRoot) -> Result<()>,
    {
        self.check(MutationStage::Upload)?;
        let chunk = Self::upload_entry(chunk_bytes)?;
        let index = Self::upload_entry(index_bytes)?;
        self.transact(None, |root| {
            verify(root)?;
            for entry in [&chunk, &index] {
                ensure!(
                    !root.blobs.contains_key(&entry.blob)
                        && !root.tombstones.contains_key(&entry.blob),
                    "physical instance collision"
                );
                root.blobs.insert(entry.blob.clone(), entry.clone());
                root.uploads.insert(entry.blob.clone(), entry.clone());
            }
            Ok(())
        })?;
        Ok((
            self.finish_upload(chunk, chunk_bytes)?,
            self.finish_upload(index, index_bytes)?,
        ))
    }

    fn finish_upload(&self, entry: ObjectEntry, bytes: &[u8]) -> Result<ObjectEntry> {
        let key = self.physical_key(&entry.blob)?;
        self.check(MutationStage::Upload)?;
        if let Err(error) = self.store.put(&key, bytes, PutCondition::Absent) {
            self.uncertain(MutationStage::Upload, &error);
            let object = self
                .store
                .get(&key)?
                .context("unresolved physical upload intent")?;
            ensure!(
                object.bytes == bytes,
                "physical upload content conflict: {error}"
            );
        }
        request_reads::remember(
            Arc::as_ptr(&self.store) as *const () as usize,
            key,
            &entry,
            bytes,
        );
        Ok(entry)
    }

    pub fn read_blob(&self, entry: &ObjectEntry) -> Result<Vec<u8>> {
        self.check(MutationStage::Read)?;
        semantic::validate_entry(entry)?;
        let object = self
            .store
            .get(&self.physical_key(&entry.blob)?)?
            .context("physical blob missing")?;
        ensure!(
            blake3::hash(&object.bytes).to_hex().as_str() == entry.hash,
            "physical blob checksum mismatch"
        );
        Ok(object.bytes)
    }

    fn read_metadata_blob(&self, entry: &ObjectEntry) -> Result<Vec<u8>> {
        self.check(MutationStage::Read)?;
        semantic::validate_entry(entry)?;
        let backend = Arc::as_ptr(&self.store) as *const () as usize;
        let key = self.physical_key(&entry.blob)?;
        if let Some(bytes) = request_reads::get(backend, &key, entry) {
            return Ok(bytes);
        }
        let bytes = self.read_blob(entry)?;
        request_reads::remember(backend, key, entry, &bytes);
        Ok(bytes)
    }

    fn physical_key(&self, blob: &str) -> Result<String> {
        ensure!(
            crate::manifest::valid_hash(blob),
            "invalid physical blob identifier"
        );
        self.namespace.key(&format!("authority/blobs/{blob}"))
    }

    pub fn reachable(&self, root: &CatalogRoot) -> Result<BTreeSet<String>> {
        self.validate_root(root)?;
        self.mark(root)
    }

    pub fn delete_tombstone(&self, blob: &str) -> Result<()> {
        self.check(MutationStage::Delete)?;
        let snapshot = self.snapshot()?;
        let tombstone = snapshot
            .root
            .tombstones
            .get(blob)
            .context("physical deletion requires persisted tombstone")?;
        ensure!(
            !snapshot.root.uploads.contains_key(blob),
            "unresolved upload intent prevents deletion"
        );
        ensure!(
            !self.reachable(&snapshot.root)?.contains(blob),
            "reachable blob cannot be deleted"
        );
        let entry = tombstone.entry.clone();
        let key = self.physical_key(blob)?;
        if tombstone.deleted && self.store.get(&key)?.is_none() {
            return Ok(());
        }
        self.check(MutationStage::Delete)?;
        if let Err(error) = self.store.delete(&key, None) {
            self.uncertain(MutationStage::Delete, &error);
            if self.store.get(&key)?.is_some() {
                return Err(error);
            }
        }
        if tombstone.deleted {
            return Ok(());
        }
        self.transact_inner(
            None,
            |root| {
                let tombstone = root
                    .tombstones
                    .get_mut(blob)
                    .context("tombstone disappeared")?;
                ensure!(tombstone.entry == entry, "tombstone instance changed");
                ensure!(
                    !root.uploads.contains_key(blob),
                    "unresolved upload intent prevents deletion"
                );
                tombstone.deleted = true;
                root.blobs.remove(blob);
                Ok(())
            },
            Some(blob),
        )?;
        Ok(())
    }
}
