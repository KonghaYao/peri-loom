//! Input classification only: these counts are not physical writes or pack jobs.
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;

use serde::Serialize;

use crate::object_store::{GroupWrite, PutCondition};

#[derive(Default, Serialize)]
struct Family {
    entries: u64,
    bytes: u64,
    any_entries: u64,
    at_most_32k_any_entries: u64,
    at_most_32k_any_bytes: u64,
}

#[derive(Default, Serialize)]
pub(crate) struct GroupShape {
    distinct_input_keys: u64,
    duplicate_input_entries: u64,
    pack_would_fallback_for_duplicate_keys: bool,
    families: BTreeMap<&'static str, Family>,
}

impl GroupShape {
    /// Called only for explicit storage diagnostics. Never inspect payloads or emit keys.
    pub(crate) fn from_writes(writes: &[GroupWrite]) -> Self {
        let mut shape = Self::default();
        let mut keys = BTreeSet::new();
        for write in writes {
            if !keys.insert(write.key.as_str()) {
                shape.duplicate_input_entries += 1;
            }
            // Namespace has exactly tenant/database components. Opaque Authority
            // bodies stay opaque even if their bytes happen to contain a receipt page.
            let relative = write.key.splitn(3, '/').nth(2).unwrap_or("");
            let family = if relative.starts_with("authority/blobs/") {
                "authority_blob"
            } else if relative.starts_with("authority/index-pages/") {
                "authority_index_page"
            } else if relative.starts_with("authority/recovery-index/") {
                "authority_recovery_page"
            } else if relative.starts_with("wal/receipt-pages/") {
                "direct_receipt_page"
            } else {
                "other"
            };
            let counters = shape.families.entry(family).or_default();
            counters.entries += 1;
            counters.bytes += write.bytes.len() as u64;
            if matches!(write.condition, PutCondition::Any) {
                counters.any_entries += 1;
                if write.bytes.len() <= 32 * 1024 {
                    counters.at_most_32k_any_entries += 1;
                    counters.at_most_32k_any_bytes += write.bytes.len() as u64;
                }
            }
        }
        shape.distinct_input_keys = keys.len() as u64;
        shape.pack_would_fallback_for_duplicate_keys = shape.duplicate_input_entries > 0;
        shape
    }

    pub(crate) fn emit(&self, stamp: &super::Stamp, group_id: u64) {
        let record = serde_json::json!({
            "event": "group_input_shape", "evidence_class": "diagnostic",
            "pid": std::process::id(), "request_id": stamp.request_id,
            "publication_id": stamp.publication_id, "group_id": group_id,
            "start_unix_ns": stamp.start_unix_ns,
            "scope": "successful_group_input_before_adoption_or_deduplication",
            "not_physical_sync_or_pack_counts": true,
            "distinct_input_keys": self.distinct_input_keys,
            "duplicate_input_entries": self.duplicate_input_entries,
            "pack_would_fallback_for_duplicate_keys": self.pack_would_fallback_for_duplicate_keys,
            "families": self.families,
        });
        let _ = writeln!(std::io::stderr().lock(), "STORAGE_PHASE {record}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_shape_keeps_opaque_receipts_duplicates_and_size_boundaries_explicit() {
        let writes = [
            GroupWrite::new(
                "tenant/db/authority/blobs/id",
                b"wal/receipt-pages/x".to_vec(),
                PutCondition::Any,
            ),
            GroupWrite::new(
                "tenant/db/authority/index-pages/id",
                vec![0; 32 * 1024],
                PutCondition::Any,
            ),
            GroupWrite::new(
                "tenant/db/authority/index-pages/id",
                vec![0; 32 * 1024],
                PutCondition::Any,
            ),
            GroupWrite::new(
                "tenant/db/authority/index-pages/large",
                vec![0; 32 * 1024 + 1],
                PutCondition::Any,
            ),
            GroupWrite::absent("tenant/db/wal/receipt-pages/id", vec![0; 3]),
            GroupWrite::new(
                "tenant/db/authority/recovery-index/id",
                vec![0; 4],
                PutCondition::Any,
            ),
            GroupWrite::new(
                "tenant/db/authority/root",
                vec![0; 5],
                PutCondition::Match("secret-etag".into()),
            ),
        ];
        let shape = GroupShape::from_writes(&writes);
        assert_eq!(shape.distinct_input_keys, 6);
        assert_eq!(shape.duplicate_input_entries, 1);
        assert!(shape.pack_would_fallback_for_duplicate_keys);
        assert_eq!(shape.families["authority_blob"].entries, 1);
        let index = &shape.families["authority_index_page"];
        assert_eq!(
            (
                index.entries,
                index.any_entries,
                index.at_most_32k_any_entries
            ),
            (3, 3, 2)
        );
        assert_eq!(index.at_most_32k_any_bytes, 64 * 1024);
        assert_eq!(shape.families["direct_receipt_page"].any_entries, 0);
        assert_eq!(shape.families["other"].any_entries, 0);
        assert_eq!(
            shape.families.values().map(|f| f.entries).sum::<u64>(),
            writes.len() as u64
        );
        assert_eq!(
            shape.families.values().map(|f| f.bytes).sum::<u64>(),
            writes.iter().map(|w| w.bytes.len() as u64).sum::<u64>()
        );
        let encoded = serde_json::to_string(&shape).unwrap();
        assert!(!encoded.contains("tenant"));
        assert!(!encoded.contains("secret-etag"));
        assert!(!encoded.contains("wal/receipt-pages"));
    }

    #[test]
    fn unknown_paths_are_not_inferred_from_payloads() {
        let writes = [GroupWrite::absent(
            "opaque-key",
            b"authority/index-pages/id".to_vec(),
        )];
        let shape = GroupShape::from_writes(&writes);
        assert_eq!(shape.families.len(), 1);
        assert_eq!(shape.families["other"].entries, 1);
        assert!(GroupShape::from_writes(&[]).families.is_empty());
    }
}
