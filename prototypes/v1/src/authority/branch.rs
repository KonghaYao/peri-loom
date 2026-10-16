#[path = "branch_publication.rs"]
mod publication;

use super::{
    now_ms, semantic, unique_id, Authority, BranchRef, CatalogRoot, ObjectEntry, RecoveryPoint,
};
use crate::{
    manifest::{Head, Manifest},
    object_store::{ObjectStore, PreconditionFailed, PutCondition, StoredObject},
    recovery::{validate_index_append, validate_index_state, ValidatedWal},
};
use anyhow::{ensure, Context, Result};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

struct View {
    incarnation: u64,
    head: Option<ObjectEntry>,
    wal: Option<ObjectEntry>,
    objects: BTreeMap<String, ObjectEntry>,
    pinned: BTreeSet<String>,
    validated_wal: Option<(ObjectEntry, ValidatedWal)>,
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
            let reference = root.branches.get(branch).context("branch missing")?;
            let incarnation = reference.incarnation;
            let head = reference.head.clone();
            let wal = reference.wal.clone();
            let entries = referenced(self, root, head.as_ref(), wal.as_ref())?;
            let pinned = entries.values().map(|entry| entry.blob.clone()).collect();
            root.pins
                .insert(pin.clone(), entries.values().cloned().collect());
            selected = Some(View {
                incarnation,
                head,
                wal,
                objects: entries
                    .into_iter()
                    .filter(|(key, _)| !matches!(key.as_str(), "HEAD" | "wal/index"))
                    .collect(),
                pinned,
                validated_wal: None,
            });
            Ok(())
        })?;
        let view = selected.context("reader pin publication missing")?;
        Ok(Arc::new(BranchStore {
            authority: self.clone(),
            branch: branch.to_owned(),
            incarnation: view.incarnation,
            pin,
            view: Mutex::new(view),
            drop_cleanup: AtomicBool::new(true),
        }))
    }
}

fn referenced(
    authority: &Authority,
    root: &CatalogRoot,
    head: Option<&ObjectEntry>,
    wal: Option<&ObjectEntry>,
) -> Result<BTreeMap<String, ObjectEntry>> {
    let mut entries = BTreeMap::new();
    if let Some(head) = head {
        semantic::registered(root, head)?;
        let metadata: Head = serde_json::from_slice(&authority.read_metadata_blob(head)?)?;
        let key = format!("manifests/{}", metadata.manifest);
        manifest_dependencies(authority, root, &key, &mut entries)?;
        entries.insert("HEAD".into(), head.clone());
    }
    if let Some(wal) = wal {
        semantic::registered(root, wal)?;
        let index = semantic::index(&authority.read_metadata_blob(wal)?)?;
        for chunk in index.entries {
            let key = format!("wal/chunks/{}", chunk.hash);
            let entry = root.objects.get(&key)?.context("WAL chunk missing")?;
            semantic::registered(root, &entry)?;
            entries.insert(key, entry.clone());
        }
        entries.insert("wal/index".into(), wal.clone());
    }
    Ok(entries)
}

fn manifest_dependencies(
    authority: &Authority,
    root: &CatalogRoot,
    key: &str,
    entries: &mut BTreeMap<String, ObjectEntry>,
) -> Result<()> {
    let entry = root.objects.get(key)?.context("manifest missing")?;
    semantic::registered(root, &entry)?;
    let manifest: Manifest = serde_json::from_slice(&authority.read_metadata_blob(&entry)?)?;
    manifest.validate()?;
    entries.insert(key.to_owned(), entry.clone());
    for hash in manifest.segments.values() {
        let key = format!("segments/{hash}");
        let entry = root
            .objects
            .get(&key)?
            .context("manifest segment missing")?;
        semantic::registered(root, &entry)?;
        entries.insert(key, entry.clone());
    }
    Ok(())
}

fn accessible(
    authority: &Authority,
    root: &CatalogRoot,
    key: &str,
    entry: &ObjectEntry,
) -> Result<BTreeMap<String, ObjectEntry>> {
    let mut entries = match key {
        "HEAD" => referenced(authority, root, Some(entry), None)?,
        "wal/index" => referenced(authority, root, None, Some(entry))?,
        _ => BTreeMap::new(),
    };
    if key.starts_with("manifests/") {
        manifest_dependencies(authority, root, key, &mut entries)?;
    }
    semantic::registered(root, entry)?;
    entries.insert(key.to_owned(), entry.clone());
    Ok(entries)
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

    fn extend_pin(
        &self,
        root: &mut CatalogRoot,
        view: &View,
        entries: BTreeMap<String, ObjectEntry>,
    ) -> Result<BTreeMap<String, ObjectEntry>> {
        let pin = root
            .pins
            .get_mut(&self.pin)
            .context("reader pin disappeared")?;
        let mut added = BTreeMap::new();
        let mut seen = BTreeSet::new();
        for (key, entry) in entries {
            if !view.pinned.contains(&entry.blob) && seen.insert(entry.blob.clone()) {
                pin.push(entry.clone());
            }
            added.insert(key, entry);
        }
        Ok(added)
    }

    fn selected(root: &CatalogRoot, branch: &str) -> Result<View> {
        let reference = root.branches.get(branch).context("branch missing")?;
        Ok(View {
            incarnation: reference.incarnation,
            head: reference.head.clone(),
            wal: reference.wal.clone(),
            objects: BTreeMap::new(),
            pinned: BTreeSet::new(),
            validated_wal: None,
        })
    }

    fn refresh(view: &mut View, selected: View, added: BTreeMap<String, ObjectEntry>) {
        if view.validated_wal.as_ref().map(|(entry, _)| entry) != selected.wal.as_ref() {
            view.validated_wal = None;
        }
        view.incarnation = selected.incarnation;
        view.head = selected.head;
        view.wal = selected.wal;
        view.pinned
            .extend(added.values().map(|entry| entry.blob.clone()));
        view.objects.extend(
            added
                .into_iter()
                .filter(|(key, _)| !matches!(key.as_str(), "HEAD" | "wal/index")),
        );
    }

    fn get_entry(&self, key: &str, view: &View) -> Option<ObjectEntry> {
        match key {
            "HEAD" => view.head.clone(),
            "wal/index" => view.wal.clone(),
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
        previous: Option<&ValidatedWal>,
    ) -> Result<ValidatedWal> {
        let load_chunk = |key: &str| {
            let relative = self.relative(key)?;
            let entry = root
                .objects
                .get(relative)?
                .context("commit chunk missing from candidate catalog")?;
            semantic::registered(root, &entry)?;
            self.authority.read_metadata_blob(&entry)
        };
        let validated = match previous {
            Some(previous) => {
                validate_index_append(previous, bytes, self.authority.namespace(), load_chunk)?
            }
            None => validate_index_state(bytes, self.authority.namespace(), load_chunk)?,
        };
        let commit = validated.commit();
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
        Ok(validated)
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
                    ensure!(!root.objects.contains(&chunk_key)?, PreconditionFailed);
                    Ok(())
                })?;
        let mut selected = None;
        let mut added = BTreeMap::new();
        let mut validated = None;
        let mut attempt = 0;
        let published = self.authority.transact(None, |root| {
            attempt += 1;
            Self::condition(self.bound(root)?.wal.as_ref(), &condition)?;
            self.validate_publication(root, "wal/index", &index_bytes)?;
            ensure!(!root.objects.contains(&chunk_key)?, PreconditionFailed);
            root.objects.insert(chunk_key.clone(), chunk.clone())?;
            let previous = if attempt == 1 {
                view.validated_wal.as_ref().and_then(|(entry, state)| {
                    (self.bound(root).ok()?.wal.as_ref() == Some(entry)).then_some(state)
                })
            } else {
                None
            };
            validated = Some(self.commit_point(root, &index_bytes, &index, previous)?);
            added = self.extend_pin(
                root,
                &view,
                BTreeMap::from([
                    (chunk_key.clone(), chunk.clone()),
                    ("wal/index".into(), index.clone()),
                ]),
            )?;
            selected = Some(Self::selected(root, &self.branch)?);
            Ok(())
        })?;
        Self::refresh(
            &mut view,
            selected.context("WAL publication view missing")?,
            added,
        );
        if published
            .root
            .branches
            .get(&self.branch)
            .and_then(|branch| branch.wal.as_ref())
            == Some(&index)
        {
            view.validated_wal = Some((index, validated.context("WAL validation missing")?));
        }
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
            if !snapshot.root.objects.contains(relative)? {
                return Ok(None);
            }
            let mut selected = None;
            let mut added = BTreeMap::new();
            self.authority.transact(None, |root| {
                self.bound(root)?;
                selected = root.objects.get(relative)?;
                if let Some(entry) = &selected {
                    let entries = accessible(&self.authority, root, relative, entry)?;
                    added = self.extend_pin(root, &view, entries)?;
                }
                Ok(())
            })?;
            view.pinned
                .extend(added.values().map(|entry| entry.blob.clone()));
            view.objects.extend(added);
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
        let snapshot_owned;
        let current = match relative {
            "HEAD" => branch.head.as_ref(),
            "wal/index" => branch.wal.as_ref(),
            _ => {
                snapshot_owned = snapshot.root.objects.get(relative)?;
                snapshot_owned.as_ref()
            }
        };
        Self::condition(current, &condition)?;
        self.validate_publication(&snapshot.root, relative, bytes)?;
        let entry = self.authority.upload(bytes)?;
        let mut selected = None;
        let mut added = BTreeMap::new();
        self.authority.transact(None, |root| {
            let branch = self.bound(root)?;
            let owned;
            let current = match relative {
                "HEAD" => branch.head.as_ref(),
                "wal/index" => branch.wal.as_ref(),
                _ => {
                    owned = root.objects.get(relative)?;
                    owned.as_ref()
                }
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
                "wal/index" => {
                    self.commit_point(root, bytes, &entry, None)?;
                }
                _ => {
                    root.objects.insert(relative.to_owned(), entry.clone())?;
                }
            }
            let entries = accessible(&self.authority, root, relative, &entry)?;
            added = self.extend_pin(root, &view, entries)?;
            selected = Some(Self::selected(root, &self.branch)?);
            Ok(())
        })?;
        Self::refresh(
            &mut view,
            selected.context("branch publication view missing")?,
            added,
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
            let owned;
            let current = match relative {
                "HEAD" => branch.head.as_ref(),
                "wal/index" => branch.wal.as_ref(),
                _ => {
                    owned = root.objects.get(relative)?;
                    owned.as_ref()
                }
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
                    root.objects.remove(relative)?;
                }
            }
            selected = Some(Self::selected(root, &self.branch)?);
            Ok(())
        })?;
        Self::refresh(
            &mut view,
            selected.context("branch deletion view missing")?,
            BTreeMap::new(),
        );
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
        let mut view = self
            .view
            .lock()
            .map_err(|_| anyhow::anyhow!("branch session lock poisoned"))?;
        let snapshot = self.authority.snapshot()?;
        self.bound(&snapshot.root)?;
        let listed = snapshot.root.objects.entries()?;
        if listed
            .keys()
            .any(|key| key.starts_with(relative) && !view.objects.contains_key(key))
        {
            let mut added = BTreeMap::new();
            self.authority.transact(None, |root| {
                self.bound(root)?;
                let mut candidates = BTreeMap::new();
                for (key, entry) in root
                    .objects
                    .entries()?
                    .iter()
                    .filter(|(key, _)| key.starts_with(relative))
                {
                    if !view.objects.contains_key(key) {
                        candidates.extend(accessible(&self.authority, root, key, entry)?);
                    }
                }
                added = self.extend_pin(root, &view, candidates)?;
                Ok(())
            })?;
            view.pinned
                .extend(added.values().map(|entry| entry.blob.clone()));
            view.objects.extend(added);
        }
        let mut keys: Vec<String> = view
            .objects
            .keys()
            .filter(|key| key.starts_with(relative))
            .map(|key| format!("{namespace}{key}"))
            .collect();
        if view.head.is_some() && "HEAD".starts_with(relative) {
            keys.push(format!("{namespace}HEAD"));
        }
        if view.wal.is_some() && "wal/index".starts_with(relative) {
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
