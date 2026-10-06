mod branch;
mod catalog;
mod codec;
mod migration;
pub mod ownership;
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
        let entry = ObjectEntry {
            blob: unique_id()?,
            hash: blake3::hash(bytes).to_hex().to_string(),
            etag: unique_id()?,
            created_ms: now_ms()?,
        };
        self.transact(None, |root| {
            ensure!(
                !root.blobs.contains_key(&entry.blob) && !root.tombstones.contains_key(&entry.blob),
                "physical instance collision"
            );
            root.blobs.insert(entry.blob.clone(), entry.clone());
            root.uploads.insert(entry.blob.clone(), entry.clone());
            Ok(())
        })?;
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
