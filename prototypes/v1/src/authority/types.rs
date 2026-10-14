use crate::namespace::Namespace;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Tombstone {
    pub entry: ObjectEntry,
    pub marked_ms: u64,
    pub deleted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CatalogRoot {
    pub version: u32,
    pub revision: u64,
    pub incarnation: String,
    pub namespace: Namespace,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ownership: Option<crate::ownership::types::OwnershipCatalog>,
    #[serde(default)]
    pub blobs: BTreeMap<String, ObjectEntry>,
    #[serde(default)]
    pub receipts: BTreeMap<String, u64>,
    pub objects: BTreeMap<String, ObjectEntry>,
    pub branches: BTreeMap<String, BranchRef>,
    pub snapshots: BTreeMap<String, RecoveryPoint>,
    pub pins: BTreeMap<String, Vec<ObjectEntry>>,
    pub uploads: BTreeMap<String, ObjectEntry>,
    pub tombstones: BTreeMap<String, Tombstone>,
}

#[derive(Clone, Debug)]
pub struct RootSnapshot {
    pub root: CatalogRoot,
    pub etag: String,
}
