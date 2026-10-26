use super::*;
use crate::object_store::{MemoryObjectStore, StoredObject};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct PageStore {
    inner: MemoryObjectStore,
    groups: AtomicUsize,
    fail_group: AtomicUsize,
}

impl ObjectStore for PageStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.inner.put(key, bytes, condition)
    }

    fn publish_pages(&self, writes: &[GroupWrite]) -> Result<bool> {
        assert!(append_pages_fit(writes));
        assert!(writes
            .iter()
            .all(|write| write.key.contains("/wal/receipt-pages/")));
        let group = self.groups.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_group.load(Ordering::SeqCst) == group {
            bail!("injected prewrite failure");
        }
        for write in writes {
            // Publication of an already-durable immutable page is idempotent.
            if let Some(existing) = self.inner.get(&write.key)? {
                ensure!(existing.bytes == write.bytes, "immutable page conflict");
            } else {
                self.inner
                    .put(&write.key, &write.bytes, PutCondition::Absent)?;
            }
        }
        Ok(true)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

#[test]
fn oversized_page_prewrite_is_bounded_and_failure_cannot_advance_the_head() -> Result<()> {
    let namespace = Namespace::new("wal", "oversized-pages")?;
    let store = Arc::new(PageStore::default());
    let wal = RemoteWal::new(store.clone());
    let request = WalAppendRequest {
        namespace: namespace.clone(),
        epoch: 1,
        batch: WalBatch::default(),
        position: None,
        append_id: "before-prewrite".into(),
        timestamp_millis: 1,
    };
    wal.append_local(&request)?;
    let before = store.get(&namespace.key("wal/index")?)?.unwrap();
    let mut pages = (0..=MAX_APPEND_STAGED_PAGES)
        .map(|index| {
            let bytes = format!("page-{index}").into_bytes();
            Ok(GroupWrite::absent(
                namespace.key(&format!(
                    "{WAL_RECEIPT_PAGE_PREFIX}/{}",
                    blake3::hash(&bytes)
                ))?,
                bytes,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    assert!(!append_pages_fit(&pages));
    let expected = pages.clone();
    store.fail_group.store(2, Ordering::SeqCst);
    assert!(wal
        .prewrite_oversized_pages(&namespace, &mut pages)
        .is_err());
    assert_eq!(pages.len(), expected.len());
    assert_eq!(store.groups.load(Ordering::SeqCst), 2);
    assert!(store.get(&expected[0].key)?.is_some());
    assert!(store.get(&expected[MAX_APPEND_STAGED_PAGES].key)?.is_none());
    assert_eq!(
        store.get(&namespace.key("wal/index")?)?.unwrap().etag,
        before.etag
    );

    // A caller can retry immutable prewrites; only after every page is durable may the
    // append publisher receive an empty staged list and publish its reference root.
    store.fail_group.store(0, Ordering::SeqCst);
    wal.prewrite_oversized_pages(&namespace, &mut pages)?;
    assert!(pages.is_empty());
    assert_eq!(store.groups.load(Ordering::SeqCst), 4);
    for page in expected {
        assert_eq!(store.get(&page.key)?.unwrap().bytes, page.bytes);
    }
    let after = store.get(&namespace.key("wal/index")?)?.unwrap();
    assert_eq!(after.etag, before.etag);
    assert_eq!(after.bytes, before.bytes);
    Ok(())
}
