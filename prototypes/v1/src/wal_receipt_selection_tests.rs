use super::select;
use crate::authority::paged_index::PagedIndex;
use crate::namespace::Namespace;
use crate::object_store::{MemoryObjectStore, ObjectStore, PutCondition};
use crate::recovery::{validate_index_append, validate_index_state, ValidatedWal};
use crate::wal::{
    ArchivedReceipt, RemoteWal, WalAppendRequest, WalBatch, WalMetadata, WAL_RECEIPT_ARCHIVE_BATCH,
    WAL_RECEIPT_PAGE_PREFIX,
};
use anyhow::{Context, Result};
use std::{collections::BTreeMap, sync::Arc};

#[test]
fn selection_wraps_in_full_hash_order_and_always_includes_the_oldest() {
    let mut candidates = (0..32)
        .map(|i| (i + 10, format!("receipt-{i}")))
        .collect::<Vec<_>>();
    candidates.sort_by_cached_key(|(_, key)| *blake3::hash(key.as_bytes()).as_bytes());
    candidates.last_mut().unwrap().0 = 1;
    let anchor = candidates.last().unwrap().clone();
    let mut expected = candidates[..6].to_vec();
    expected.push(anchor.clone());
    expected.sort();
    assert_eq!(select(candidates.clone(), 7), expected);
    candidates.reverse();
    assert_eq!(select(candidates, 7), expected);
    assert_eq!(expected.first(), Some(&anchor));
}

#[test]
fn selection_handles_empty_full_zero_and_same_lsn_without_duplicates() {
    assert!(select(Vec::new(), usize::MAX).is_empty());
    let candidates = (0..31)
        .map(|i| (1, format!("same-lsn-{i:02}")))
        .collect::<Vec<_>>();
    assert!(select(candidates.clone(), 0).is_empty());
    assert_eq!(select(candidates.clone(), usize::MAX), candidates);
    let selected = select(candidates.clone(), 9);
    assert_eq!(selected.len(), 9);
    assert_eq!(selected.first(), candidates.first());
    assert!(selected.windows(2).all(|pair| pair[0] < pair[1]));
    let mut shuffled = candidates;
    shuffled.rotate_left(13);
    shuffled.reverse();
    assert_eq!(select(shuffled, 9), selected);
}

#[test]
fn staged_roll_preserves_legacy_and_new_receipts_and_uses_canonical_replay() -> Result<()> {
    let namespace = Namespace::new("receipt", "locality-filter")?;
    let store = Arc::new(MemoryObjectStore::default());
    let wal = RemoteWal::new(store.clone());
    let mut index = WalMetadata::default();
    for ordinal in 1..=260 {
        let id = format!("receipt-{ordinal:04}");
        index.receipts.insert(id.clone(), ordinal);
        if ordinal > 3 {
            index.receipt_digests.insert(
                id,
                blake3::hash(ordinal.to_string().as_bytes())
                    .to_hex()
                    .to_string(),
            );
        }
    }
    let before = index.clone();
    let writes = wal.stage_receipts(&namespace, &mut index, Some("receipt-0260"))?;
    for ordinal in [1, 2, 3, 260] {
        assert_eq!(
            index.receipts.get(&format!("receipt-{ordinal:04}")),
            Some(&ordinal)
        );
    }
    assert_eq!(index.receipts.len(), 192);
    let removed = before
        .receipts
        .iter()
        .filter(|(id, _)| !index.receipts.contains_key(*id))
        .map(|(id, lsn)| (*lsn, id.clone()))
        .collect::<Vec<_>>();
    assert_eq!(removed.len(), 68);
    assert!(removed.iter().any(|(lsn, _)| *lsn == 4));
    assert!(removed
        .iter()
        .all(|(_, id)| before.receipt_digests.contains_key(id)));
    let replay_store = MemoryObjectStore::default();
    let replay = PagedIndex::with_prefix(&replay_store, &namespace, WAL_RECEIPT_PAGE_PREFIX);
    let mut ordered = removed;
    ordered.sort();
    let mut root = None;
    for chunk in ordered.chunks(WAL_RECEIPT_ARCHIVE_BATCH) {
        let updates = chunk
            .iter()
            .map(|(lsn, id)| {
                Ok((
                    id.clone(),
                    serde_json::to_value(ArchivedReceipt {
                        lsn: *lsn,
                        digest: before.receipt_digests.get(id).cloned(),
                    })?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        root = Some(replay.insert_many(root.as_deref(), updates)?);
    }
    assert_eq!(root, index.receipt_index);
    for write in writes {
        assert_eq!(
            replay_store
                .get(&write.key)?
                .context("canonical page missing")?
                .bytes,
            write.bytes
        );
    }
    for (id, lsn) in &index.receipts {
        assert_eq!(before.receipts.get(id), Some(lsn));
        assert_eq!(
            before.receipt_digests.get(id),
            index.receipt_digests.get(id)
        );
    }
    Ok(())
}

fn request(namespace: &Namespace, ordinal: u64) -> WalAppendRequest {
    WalAppendRequest {
        namespace: namespace.clone(),
        epoch: 1,
        batch: WalBatch::default(),
        position: None,
        append_id: format!("receipt-{ordinal:04}"),
        timestamp_millis: 1,
    }
}

fn page(
    store: &MemoryObjectStore,
    namespace: &Namespace,
    folder: &str,
    hash: &str,
) -> Result<Vec<u8>> {
    Ok(store
        .get(&namespace.key(&format!("wal/{folder}/{hash}"))?)?
        .context("fixture page missing")?
        .bytes)
}

fn cold(store: &MemoryObjectStore, namespace: &Namespace, bytes: &[u8]) -> Result<ValidatedWal> {
    validate_index_state(
        bytes,
        namespace,
        |hash| page(store, namespace, "index-segments", hash),
        |key| Ok(store.get(key)?.context("fixture chunk missing")?.bytes),
        |hash| page(store, namespace, "receipt-pages", hash),
    )
}

#[test]
fn locality_roll_extends_an_oldest_policy_archive_with_unchanged_hot_and_cold_validation(
) -> Result<()> {
    let namespace = Namespace::new("receipt", "policy-transition")?;
    let store = Arc::new(MemoryObjectStore::default());
    let wal = RemoteWal::new(store.clone());
    for ordinal in 1..=256 {
        wal.append_local(&request(&namespace, ordinal))?;
    }
    let before = wal.metadata(&namespace)?;
    wal.append_local(&request(&namespace, 257))?;
    let (mut old_policy, etag) = wal.index(&namespace)?;
    // Build the original oldest policy's actual publication over the same records.
    // Its selected set is intentionally independent of the new selector.
    old_policy.receipts.extend(before.receipts);
    old_policy.receipt_digests.extend(before.receipt_digests);
    old_policy.receipt_index = None;
    let mut selected = old_policy
        .receipts
        .iter()
        .map(|(id, lsn)| (*lsn, id.clone()))
        .collect::<Vec<_>>();
    selected.sort();
    selected.truncate(65);
    let archive = PagedIndex::with_prefix(store.as_ref(), &namespace, WAL_RECEIPT_PAGE_PREFIX);
    for chunk in selected.chunks(WAL_RECEIPT_ARCHIVE_BATCH) {
        let updates = chunk
            .iter()
            .map(|(lsn, id)| {
                Ok((
                    id.clone(),
                    serde_json::to_value(ArchivedReceipt {
                        lsn: *lsn,
                        digest: old_policy.receipt_digests.get(id).cloned(),
                    })?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        old_policy.receipt_index =
            Some(archive.insert_many(old_policy.receipt_index.as_deref(), updates)?);
    }
    for (_, id) in selected {
        old_policy.receipts.remove(&id);
        old_policy.receipt_digests.remove(&id);
    }
    wal.commit(&namespace, &old_policy, etag)?;
    let mut validated = cold(
        store.as_ref(),
        &namespace,
        &serde_json::to_vec(&old_policy)?,
    )?;
    let mut previous_root = old_policy.receipt_index;
    let mut rolls = 0;
    for ordinal in 258..=700 {
        wal.append_local(&request(&namespace, ordinal))?;
        let metadata = wal.metadata(&namespace)?;
        if metadata.receipt_index != previous_root {
            rolls += 1;
            previous_root = metadata.receipt_index.clone();
        }
        validated = validate_index_append(
            validated,
            &serde_json::to_vec(&metadata)?,
            &namespace,
            |hash| page(store.as_ref(), &namespace, "index-segments", hash),
            |key| Ok(store.get(key)?.context("fixture chunk missing")?.bytes),
            |hash| page(store.as_ref(), &namespace, "receipt-pages", hash),
        )?;
    }
    assert_eq!(rolls, 6);
    let final_metadata = wal.metadata(&namespace)?;
    assert_eq!(
        cold(
            store.as_ref(),
            &namespace,
            &serde_json::to_vec(&final_metadata)?
        )?
        .archive_entries(),
        validated.archive_entries()
    );
    let reopened = RemoteWal::new(store.clone());
    for ordinal in [1, 66, 257, 322, 700] {
        assert_eq!(
            reopened.append_local(&request(&namespace, ordinal))?,
            ordinal
        );
    }
    let mut changed = request(&namespace, 1);
    changed.timestamp_millis = 2;
    assert!(reopened.append_local(&changed).is_err());
    assert_eq!(reopened.metadata(&namespace)?.last_lsn, 700);
    let root_key = namespace.key(&format!(
        "{WAL_RECEIPT_PAGE_PREFIX}/{}",
        final_metadata.receipt_index.unwrap()
    ))?;
    let valid = store.get(&root_key)?.unwrap();
    store.put(&root_key, b"corrupt", PutCondition::Any)?;
    assert!(cold(
        store.as_ref(),
        &namespace,
        &serde_json::to_vec(&reopened.metadata(&namespace)?)?
    )
    .is_err());
    store.put(&root_key, &valid.bytes, PutCondition::Any)?;
    assert_eq!(reopened.append_local(&request(&namespace, 1))?, 1);
    Ok(())
}
