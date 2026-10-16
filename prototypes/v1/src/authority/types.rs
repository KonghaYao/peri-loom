use crate::namespace::Namespace;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::registry::Registry;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ObjectEntry {
    pub blob: String,
    pub hash: String,
    pub etag: String,
    pub created_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecoveryPoint {
    pub id: String,
    pub branch: String,
    pub timeline: u64,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub head: Option<ObjectEntry>,
    pub wal: Option<ObjectEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BranchRef {
    pub incarnation: u64,
    pub timeline: u64,
    pub sequence: u64,
    pub last_commit_ms: u64,
    #[serde(default)]
    pub current_sequence: u64,
    #[serde(default)]
    pub current_commit_ms: u64,
    pub head: Option<ObjectEntry>,
    pub wal: Option<ObjectEntry>,
    pub history: Vec<RecoveryPoint>,
}

impl Default for BranchRef {
    fn default() -> Self {
        Self {
            incarnation: 1,
            timeline: 1,
            sequence: 0,
            last_commit_ms: 0,
            current_sequence: 0,
            current_commit_ms: 0,
            head: None,
            wal: None,
            history: Vec::new(),
        }
    }
}

impl BranchRef {
    /// Cheap "did this branch change?" predicate for change-scoped validation: it
    /// compares the fields a publication can move and the history length, never the
    /// history entries themselves, so per-commit cost does not grow with commit count.
    pub(super) fn unchanged_refs(&self, other: &BranchRef) -> bool {
        self.incarnation == other.incarnation
            && self.sequence == other.sequence
            && self.current_sequence == other.current_sequence
            && self.head == other.head
            && self.wal == other.wal
            && self.history.len() == other.history.len()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Tombstone {
    pub entry: ObjectEntry,
    pub marked_ms: u64,
    pub deleted: bool,
}

/// Durable representation of the authority head. Volume registries appear twice: the
/// inline window carries the state while it is small, the `*_index` pointer addresses an
/// immutable page tree once it is not. A document never carries both, so a reader that
/// finds a pointer ignores the inline window entirely.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CatalogDocument {
    pub(super) version: u32,
    pub(super) revision: u64,
    pub(super) incarnation: String,
    pub(super) namespace: Namespace,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) ownership: Option<crate::ownership::types::OwnershipCatalog>,
    #[serde(default)]
    pub(super) blobs: BTreeMap<String, ObjectEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) blob_index: Option<String>,
    #[serde(default)]
    pub(super) receipts: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) receipt_index: Option<String>,
    #[serde(default)]
    pub(super) objects: BTreeMap<String, ObjectEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) object_index: Option<String>,
    pub(super) branches: BTreeMap<String, BranchRef>,
    #[serde(default)]
    pub(super) snapshots: BTreeMap<String, RecoveryPoint>,
    #[serde(default)]
    pub(super) pins: BTreeMap<String, Vec<ObjectEntry>>,
    pub(super) uploads: BTreeMap<String, ObjectEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) upload_index: Option<String>,
    pub(super) tombstones: BTreeMap<String, Tombstone>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) tombstone_index: Option<String>,
}

/// In-memory authority head. Identity state (branches, snapshots, reader pins, receipt
/// window) stays inline because it is bounded by distinct identities, while volume state
/// is held by `Registry` handles that page themselves once they outgrow the inline budget.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "CatalogDocument", into = "CatalogDocument")]
pub struct CatalogRoot {
    pub version: u32,
    pub revision: u64,
    pub incarnation: String,
    pub namespace: Namespace,
    pub ownership: Option<crate::ownership::types::OwnershipCatalog>,
    pub blobs: Registry<ObjectEntry>,
    pub receipts: BTreeMap<String, u64>,
    pub receipt_index: Option<String>,
    pub objects: Registry<ObjectEntry>,
    pub branches: BTreeMap<String, BranchRef>,
    pub snapshots: BTreeMap<String, RecoveryPoint>,
    pub pins: BTreeMap<String, Vec<ObjectEntry>>,
    pub uploads: Registry<ObjectEntry>,
    pub tombstones: Registry<Tombstone>,
}

impl CatalogRoot {
    pub(super) fn empty(namespace: Namespace, incarnation: String) -> Self {
        Self {
            version: 1,
            revision: 1,
            incarnation,
            namespace,
            ownership: None,
            blobs: Registry::inline(BTreeMap::new()),
            receipts: BTreeMap::new(),
            receipt_index: None,
            objects: Registry::inline(BTreeMap::new()),
            branches: BTreeMap::from([("main".into(), BranchRef::default())]),
            snapshots: BTreeMap::new(),
            pins: BTreeMap::new(),
            uploads: Registry::inline(BTreeMap::new()),
            tombstones: Registry::inline(BTreeMap::new()),
        }
    }

    pub(super) fn attach(
        &mut self,
        store: std::sync::Arc<dyn crate::object_store::ObjectStore>,
        namespace: Namespace,
    ) {
        for registry in [&mut self.blobs, &mut self.objects, &mut self.uploads] {
            registry.attach(store.clone(), namespace.clone());
        }
        self.tombstones.attach(store, namespace);
    }

    /// A registry either carries its state inline or points at a page tree, never both.
    pub(super) fn validate_shape(&self) -> anyhow::Result<()> {
        for (name, pointer, inline) in [
            ("blobs", self.blobs.pointer(), self.blobs.window_len()),
            ("objects", self.objects.pointer(), self.objects.window_len()),
            ("uploads", self.uploads.pointer(), self.uploads.window_len()),
            (
                "tombstones",
                self.tombstones.pointer(),
                self.tombstones.window_len(),
            ),
        ] {
            anyhow::ensure!(
                pointer.is_none() || inline == 0,
                "registry {name} mixes inline state with a page pointer"
            );
            anyhow::ensure!(
                pointer.is_none_or(|pointer| pointer.len() == 64
                    && pointer
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))),
                "registry {name} pointer is not a page identity"
            );
        }
        Ok(())
    }

    /// Guard for the publication path: no registry may still hold unreleased changes when
    /// the head is encoded, and the encoded window must respect the inline budget.
    pub(super) fn seal(&self) -> anyhow::Result<()> {
        self.validate_shape()?;
        anyhow::ensure!(
            !self.blobs.has_changes()
                && !self.objects.has_changes()
                && !self.uploads.has_changes()
                && !self.tombstones.has_changes(),
            "registry changes were not published"
        );
        Ok(())
    }
}

impl From<CatalogDocument> for CatalogRoot {
    fn from(document: CatalogDocument) -> Self {
        Self {
            version: document.version,
            revision: document.revision,
            incarnation: document.incarnation,
            namespace: document.namespace,
            ownership: document.ownership,
            blobs: Registry::paged(document.blob_index, document.blobs),
            receipts: document.receipts,
            receipt_index: document.receipt_index,
            objects: Registry::paged(document.object_index, document.objects),
            branches: document.branches,
            snapshots: document.snapshots,
            pins: document.pins,
            uploads: Registry::paged(document.upload_index, document.uploads),
            tombstones: Registry::paged(document.tombstone_index, document.tombstones),
        }
    }
}

impl From<CatalogRoot> for CatalogDocument {
    fn from(root: CatalogRoot) -> Self {
        let (blob_index, blobs) = root.blobs.into_parts();
        let (object_index, objects) = root.objects.into_parts();
        let (upload_index, uploads) = root.uploads.into_parts();
        let (tombstone_index, tombstones) = root.tombstones.into_parts();
        Self {
            version: root.version,
            revision: root.revision,
            incarnation: root.incarnation,
            namespace: root.namespace,
            ownership: root.ownership,
            blobs,
            blob_index,
            receipts: root.receipts,
            receipt_index: root.receipt_index,
            objects,
            object_index,
            branches: root.branches,
            snapshots: root.snapshots,
            pins: root.pins,
            uploads,
            upload_index,
            tombstones,
            tombstone_index,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RootSnapshot {
    pub root: CatalogRoot,
    pub etag: String,
}
