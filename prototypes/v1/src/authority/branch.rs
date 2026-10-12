#[path = "branch_publication.rs"]
mod publication;

use super::{
    now_ms, semantic, unique_id, Authority, BranchRef, CatalogRoot, ObjectEntry, RecoveryPoint,
};
use crate::{
    object_store::{ObjectStore, PreconditionFailed, PutCondition, StoredObject},
    recovery::validate_index_commit,
};
use anyhow::{ensure, Context, Result};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

#[derive(Clone)]
struct View {
    reference: BranchRef,
    objects: BTreeMap<String, ObjectEntry>,
}

pub struct BranchStore {
    authority: Arc<Authority>,
    branch: String,
    incarnation: u64,
    pin: String,
    view: Mutex<View>,
    drop_cleanup: AtomicBool,
}

impl Authority {
    pub fn branch_store(self: &Arc<Self>, branch: &str) -> Result<Arc<BranchStore>> {
        semantic::validate_name(branch)?;
        let pin = unique_id()?;
        let mut selected = None;
        self.transact(None, |root| {
            let reference = root.branches.get(branch).context("branch missing")?.clone();
            root.pins.insert(pin.clone(), entries(root));
            selected = Some(View {
                reference,
                objects: root.objects.clone(),
            });
            Ok(())
        })?;
        let view = selected.context("reader pin publication missing")?;
        Ok(Arc::new(BranchStore {
            authority: self.clone(),
            branch: branch.to_owned(),
            incarnation: view.reference.incarnation,
            pin,
            view: Mutex::new(view),
            drop_cleanup: AtomicBool::new(true),
        }))
    }
}

fn entries(root: &CatalogRoot) -> Vec<ObjectEntry> {
    let mut entries: BTreeMap<String, ObjectEntry> = root
        .objects
        .values()
        .map(|entry| (entry.blob.clone(), entry.clone()))
        .collect();
    for branch in root.branches.values() {
        for entry in branch.head.iter().chain(branch.wal.iter()) {
            entries.insert(entry.blob.clone(), entry.clone());
        }
        for point in &branch.history {
            for entry in point.head.iter().chain(point.wal.iter()) {
                entries.insert(entry.blob.clone(), entry.clone());
            }
        }
    }
    for point in root.snapshots.values() {
        for entry in point.head.iter().chain(point.wal.iter()) {
            entries.insert(entry.blob.clone(), entry.clone());
        }
    }
    entries.into_values().collect()
}

impl BranchStore {
    pub(crate) fn suppress_drop_cleanup(&self) {
        self.drop_cleanup.store(false, Ordering::Release);
    }

    pub(crate) fn release_pin(&self, authority: &Authority) -> Result<()> {
        authority.transact(None, |root| {
            root.pins.remove(&self.pin);
            Ok(())
        })?;
        Ok(())
    }
    fn relative<'a>(&self, key: &'a str) -> Result<&'a str> {
        let prefix = self.authority.namespace.prefix();
        let relative = key
            .strip_prefix(&prefix)
            .context("object key outside branch namespace")?;
        self.authority.namespace.key(relative)?;
        ensure!(
            !relative.starts_with("authority/"),
            "private authority path is not an engine key"
        );
        Ok(relative)
    }

    fn bound<'a>(&self, root: &'a CatalogRoot) -> Result<&'a BranchRef> {
        let branch = root
            .branches
            .get(&self.branch)
            .context("stale branch incarnation: branch missing")?;
        ensure!(
            branch.incarnation == self.incarnation,
            "stale branch incarnation"
        );
        ensure!(root.pins.contains_key(&self.pin), "reader pin disappeared");
        Ok(branch)
    }

    fn extend_pin(&self, root: &mut CatalogRoot) -> Result<()> {
        let current = entries(root);
        let pin = root
            .pins
            .get_mut(&self.pin)
            .context("reader pin disappeared")?;
        let mut merged: BTreeMap<_, _> = pin
            .iter()
            .cloned()
            .map(|entry| (entry.blob.clone(), entry))
            .collect();
        for entry in current {
            merged.insert(entry.blob.clone(), entry);
        }
        *pin = merged.into_values().collect();
        Ok(())
    }

    fn selected(root: &CatalogRoot, branch: &str) -> Result<View> {
        Ok(View {
            reference: root.branches.get(branch).context("branch missing")?.clone(),
            objects: root.objects.clone(),
        })
    }

    fn refresh(view: &mut View, selected: View) {
        view.reference = selected.reference;
        view.objects.extend(selected.objects);
    }

    fn get_entry(&self, key: &str, view: &View) -> Option<ObjectEntry> {
        match key {
            "HEAD" => view.reference.head.clone(),
            "wal/index" => view.reference.wal.clone(),
            _ => view.objects.get(key).cloned(),
        }
    }

    fn condition(current: Option<&ObjectEntry>, condition: &PutCondition) -> Result<()> {
        let satisfied = match condition {
            PutCondition::Any => true,
            PutCondition::Absent => current.is_none(),
            PutCondition::Match(etag) => current.is_some_and(|entry| entry.etag == *etag),
        };
        ensure!(satisfied, PreconditionFailed);
        Ok(())
    }

    fn commit_point(
        &self,
        root: &mut CatalogRoot,
        bytes: &[u8],
        entry: &ObjectEntry,
    ) -> Result<()> {
        let commit = validate_index_commit(bytes, self.authority.namespace(), |key| {
            let relative = self.relative(key)?;
            let entry = root
                .objects
                .get(relative)
                .context("commit chunk missing from candidate catalog")?;
            semantic::registered(root, entry)?;
            self.authority.read_metadata_blob(entry)
        })?;
        let previous = root.branches[&self.branch]
            .wal
            .as_ref()
            .map(|entry| {
                Ok::<_, anyhow::Error>(
                    semantic::index(&self.authority.read_metadata_blob(entry)?)?
                        .commit
                        .map_or(0, |commit| commit.lsn),
                )
            })
            .transpose()?
            .unwrap_or(0);
        let branch = root
            .branches
            .get_mut(&self.branch)
            .context("branch missing")?;
        branch.wal = Some(entry.clone());
        if commit.is_some_and(|commit| commit.lsn > previous) {
            branch.sequence = branch
                .sequence
                .checked_add(1)
                .context("commit sequence overflow")?;
            branch.last_commit_ms = now_ms()?.max(
                branch
                    .last_commit_ms
                    .checked_add(1)
                    .context("commit timestamp overflow")?,
            );
            branch.current_sequence = branch.sequence;
            branch.current_commit_ms = branch.last_commit_ms;
            branch.history.push(RecoveryPoint {
                id: entry.etag.clone(),
                branch: self.branch.clone(),
                timeline: branch.timeline,
                sequence: branch.sequence,
                timestamp_ms: branch.last_commit_ms,
                head: branch.head.clone(),
                wal: branch.wal.clone(),
            });
        }
        Ok(())
    }

    pub(super) fn publish_wal_append(
        &self,
        record: &crate::wal::WalRecord,
        metadata: &crate::wal::WalMetadata,
        expected: Option<String>,
    ) -> Result<()> {
        ensure!(
            record.namespace == *self.authority.namespace()
                && metadata.epoch == record.epoch
                && metadata.last_lsn == record.lsn,
            "WAL publication identity mismatch"
        );
        let chunk_bytes = serde_json::to_vec(record)?;
        let hash = blake3::hash(&chunk_bytes).to_hex().to_string();
        let chunk_key = format!("wal/chunks/{hash}");
        let index_bytes = serde_json::to_vec(metadata)?;
        let condition = expected.map_or(PutCondition::Absent, PutCondition::Match);
        let mut view = self
            .view
            .lock()
            .map_err(|_| anyhow::anyhow!("branch session lock poisoned"))?;
        let (chunk, index) =
            self.authority
                .upload_wal_pair(&chunk_bytes, &index_bytes, |root| {
                    Self::condition(self.bound(root)?.wal.as_ref(), &condition)?;
                    self.validate_publication(root, "wal/index", &index_bytes)?;
                    ensure!(!root.objects.contains_key(&chunk_key), PreconditionFailed);
                    Ok(())
                })?;
        let mut selected = None;
        self.authority.transact(None, |root| {
            Self::condition(self.bound(root)?.wal.as_ref(), &condition)?;
            self.validate_publication(root, "wal/index", &index_bytes)?;
            ensure!(!root.objects.contains_key(&chunk_key), PreconditionFailed);
            root.objects.insert(chunk_key.clone(), chunk.clone());
            self.commit_point(root, &index_bytes, &index)?;
            self.extend_pin(root)?;
            selected = Some(Self::selected(root, &self.branch)?);
            Ok(())
        })?;
        Self::refresh(&mut view, selected.context("WAL publication view missing")?);
        Ok(())
    }
}

impl ObjectStore for BranchStore {
    fn validate_local(&self) -> Result<()> {
        Ok(())
    }

    fn validate_session(&self) -> Result<()> {
        self.bound(&self.authority.snapshot()?.root)?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        let relative = self.relative(key)?;
        let mut view = self
            .view
            .lock()
            .map_err(|_| anyhow::anyhow!("branch session lock poisoned"))?;
        let entry = if let Some(entry) = self.get_entry(relative, &view) {
            Some(entry)
        } else if matches!(relative, "HEAD" | "wal/index") {
            None
        } else {
            let snapshot = self.authority.snapshot()?;
            self.bound(&snapshot.root)?;
            if !snapshot.root.objects.contains_key(relative) {
                return Ok(None);
            }
            let mut selected = None;
            self.authority.transact(None, |root| {
                self.bound(root)?;
                selected = root.objects.get(relative).cloned();
                if let Some(entry) = &selected {
                    root.pins
                        .get_mut(&self.pin)
                        .context("reader pin missing")?
                        .push(entry.clone());
                }
                Ok(())
            })?;
            if let Some(entry) = &selected {
                view.objects.insert(relative.to_owned(), entry.clone());
            }
            selected
        };
        entry
            .map(|entry| {
                Ok(StoredObject {
                    bytes: if matches!(relative, "HEAD" | "wal/index") {
                        self.authority.read_metadata_blob(&entry)?
                    } else {
                        self.authority.read_blob(&entry)?
                    },
                    etag: entry.etag,
                })
            })
            .transpose()
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        let relative = self.relative(key)?;
        let mut view = self
            .view
            .lock()
            .map_err(|_| anyhow::anyhow!("branch session lock poisoned"))?;
        let snapshot = self.authority.snapshot()?;
        if let Some(guard) = &self.authority.guard {
            guard.validate_root(&snapshot.root, &snapshot.root)?;
        }
        let branch = self.bound(&snapshot.root)?;
        let current = match relative {
            "HEAD" => branch.head.as_ref(),
            "wal/index" => branch.wal.as_ref(),
            _ => snapshot.root.objects.get(relative),
        };
        Self::condition(current, &condition)?;
        self.validate_publication(&snapshot.root, relative, bytes)?;
        let entry = self.authority.upload(bytes)?;
        let mut selected = None;
        self.authority.transact(None, |root| {
            let branch = self.bound(root)?;
            let current = match relative {
                "HEAD" => branch.head.as_ref(),
                "wal/index" => branch.wal.as_ref(),
                _ => root.objects.get(relative),
            };
            Self::condition(current, &condition)?;
            self.validate_publication(root, relative, bytes)?;
            match relative {
                "HEAD" => {
                    root.branches
                        .get_mut(&self.branch)
                        .context("branch missing")?
                        .head = Some(entry.clone())
                }
                "wal/index" => self.commit_point(root, bytes, &entry)?,
                _ => {
                    root.objects.insert(relative.to_owned(), entry.clone());
                }
            }
            self.extend_pin(root)?;
            selected = Some(Self::selected(root, &self.branch)?);
            Ok(())
        })?;
        Self::refresh(
            &mut view,
            selected.context("branch publication view missing")?,
        );
        Ok(entry.etag)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        let relative = self.relative(key)?;
        let mut view = self
            .view
            .lock()
            .map_err(|_| anyhow::anyhow!("branch session lock poisoned"))?;
        let mut selected = None;
        self.authority.transact(None, |root| {
            let branch = self.bound(root)?;
            let current = match relative {
                "HEAD" => branch.head.as_ref(),
                "wal/index" => branch.wal.as_ref(),
                _ => root.objects.get(relative),
            };
            if let (Some(expected), Some(current)) = (expected, current) {
                ensure!(current.etag == expected, PreconditionFailed);
            }
            match relative {
                "HEAD" => {
                    root.branches
                        .get_mut(&self.branch)
                        .context("branch missing")?
                        .head = None
                }
                "wal/index" => {
                    root.branches
                        .get_mut(&self.branch)
                        .context("branch missing")?
                        .wal = None
                }
                _ => {
                    root.objects.remove(relative);
                }
            }
            self.extend_pin(root)?;
            selected = Some(Self::selected(root, &self.branch)?);
            Ok(())
        })?;
        Self::refresh(&mut view, selected.context("branch deletion view missing")?);
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let namespace = self.authority.namespace.prefix();
        let relative = prefix
            .strip_prefix(&namespace)
            .context("prefix outside branch namespace")?;
        if !relative.is_empty() {
            self.authority
                .namespace
                .key(relative.strip_suffix('/').unwrap_or(relative))?;
        }
        let view = self
            .view
            .lock()
            .map_err(|_| anyhow::anyhow!("branch session lock poisoned"))?;
        let mut keys: Vec<String> = view
            .objects
            .keys()
            .filter(|key| key.starts_with(relative))
            .map(|key| format!("{namespace}{key}"))
            .collect();
        if view.reference.head.is_some() && "HEAD".starts_with(relative) {
            keys.push(format!("{namespace}HEAD"));
        }
        if view.reference.wal.is_some() && "wal/index".starts_with(relative) {
            keys.push(format!("{namespace}wal/index"));
        }
        keys.sort();
        Ok(keys)
    }
}

impl Drop for BranchStore {
    fn drop(&mut self) {
        if !self.drop_cleanup.load(Ordering::Acquire) {
            return;
        }
        let _ = self.authority.transact(None, |root| {
            root.pins.remove(&self.pin);
            Ok(())
        });
    }
}
