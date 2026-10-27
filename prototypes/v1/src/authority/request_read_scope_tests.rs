use super::*;
use crate::authority::paged_index::PagedIndex;
use crate::namespace::Namespace;
use crate::object_store::{MemoryObjectStore, ObjectStore, PutCondition};
use anyhow::Result;
use serde_json::json;

fn identity(store: &dyn ObjectStore) -> usize {
    store as *const dyn ObjectStore as *const () as usize
}

fn entry() -> ObjectEntry {
    ObjectEntry {
        blob: "blob".into(),
        hash: "hash".into(),
        etag: "etag".into(),
        created_ms: 1,
    }
}

fn assert_accounting() {
    CURRENT.with(|current| {
        let current = current.borrow();
        let reads = current.as_ref().unwrap();
        assert_eq!(
            reads.bytes,
            reads
                .entries
                .values()
                .map(|(_, bytes)| bytes.len())
                .sum::<usize>()
                + reads.pages.values().map(Vec::len).sum::<usize>()
        );
        assert_eq!(
            reads.parsed_bytes,
            reads
                .parsed
                .values()
                .map(|cached| cached.charge)
                .sum::<usize>()
        );
        assert!(reads.parsed_bytes <= MAX_PARSED_BYTES);
        assert_eq!(reads.parsed_clock.len(), reads.parsed.len());
        let clock = reads
            .parsed_clock
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(clock.len(), reads.parsed.len());
        assert!(reads.parsed.keys().all(|key| clock.contains(key)));
    });
}

#[test]
fn backend_scope_preserves_enclosing_cache_and_revalidates_a_reused_address() -> Result<()> {
    let store = MemoryObjectStore::default();
    let other = MemoryObjectStore::default();
    let namespace = Namespace::new("scope", "reuse")?;
    let pages = PagedIndex::new(&store, &namespace);
    let other_pages = PagedIndex::new(&other, &namespace);
    let root = pages.insert_many(None, BTreeMap::from([("key".into(), json!(1))]))?;
    let other_root = other_pages.insert_many(None, BTreeMap::from([("other".into(), json!(2))]))?;
    let backend = identity(&store);
    let other_backend = identity(&other);
    let _request = Scope::new();
    remember(backend, "metadata".into(), &entry(), b"adapter");
    remember(other_backend, "metadata".into(), &entry(), b"durable");
    assert_eq!(pages.get(Some(&root), "key")?, Some(json!(1)));
    assert_eq!(other_pages.get(Some(&other_root), "other")?, Some(json!(2)));
    let key = namespace.key(&format!("authority/index-pages/{root}"))?;
    let valid = store.get(&key)?.unwrap().bytes;
    store.put(&key, b"bad before scope", PutCondition::Any)?;
    {
        let _adapter = BackendScope::new(&store);
        assert!(get(backend, "metadata", &entry()).is_none());
        // Entry invalidation covers both raw and decoded pages. Either stale
        // cache alone would hide this digest error.
        assert!(pages.get(Some(&root), "key").is_err());
        assert_eq!(
            get(other_backend, "metadata", &entry()),
            Some(b"durable".to_vec())
        );
        assert_eq!(other_pages.get(Some(&other_root), "other")?, Some(json!(2)));
    }
    store.put(&key, &valid, PutCondition::Any)?;
    {
        let _adapter = BackendScope::new(&store);
        assert_eq!(pages.get(Some(&root), "key")?, Some(json!(1)));
        remember(backend, "metadata".into(), &entry(), b"adapter again");
    }
    assert!(get(backend, "metadata", &entry()).is_none());
    assert!(get_page(backend, &key).is_none());
    assert!(parsed_page(backend, &key, KeyOrder::Hashed, 0, &[]).is_none());
    assert_eq!(
        get(other_backend, "metadata", &entry()),
        Some(b"durable".to_vec())
    );
    assert_accounting();
    // Same object/address, new adapter contents: no entries survive the prior
    // lifetime to answer on its behalf, including malformed bytes cached above.
    store.put(&key, b"bad after scope", PutCondition::Any)?;
    let _adapter = BackendScope::new(&store);
    assert!(pages.get(Some(&root), "key").is_err());
    assert_accounting();
    Ok(())
}

#[test]
fn backend_scope_joins_or_owns_the_request_and_cleans_up_on_unwind() {
    let store = MemoryObjectStore::default();
    let backend = identity(&store);
    {
        let _adapter = BackendScope::new(&store);
        remember_page(backend, "local".into(), b"bytes");
        assert!(get_page(backend, "local").is_some());
        assert_accounting();
    }
    CURRENT.with(|current| assert!(current.borrow().is_none()));
    let _request = Scope::new();
    remember_page(backend + 1, "outer".into(), b"retained");
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _adapter = BackendScope::new(&store);
        remember_page(backend, "local".into(), b"bytes");
        remember(backend, "metadata".into(), &entry(), b"metadata");
        remember_parsed_page(
            backend,
            "parsed".into(),
            KeyOrder::Hashed,
            0,
            &[],
            Arc::new(Page::Leaf {
                version: 1,
                depth: 0,
                entries: BTreeMap::from([("key".into(), json!(1))]),
            }),
        );
        panic!("simulated adapter error");
    }));
    assert!(failure.is_err());
    assert!(get_page(backend, "local").is_none());
    assert!(get(backend, "metadata", &entry()).is_none());
    assert!(parsed_page(backend, "parsed", KeyOrder::Hashed, 0, &[]).is_none());
    assert_eq!(get_page(backend + 1, "outer"), Some(b"retained".to_vec()));
    assert_accounting();
}

#[test]
fn backend_retirement_and_clock_eviction_keep_exact_independent_budgets() {
    let store = MemoryObjectStore::default();
    let backend = identity(&store);
    let _request = Scope::new();
    let page = Arc::new(Page::Leaf {
        version: 1,
        depth: 0,
        entries: BTreeMap::from([("key".into(), json!(1))]),
    });
    for round in 0..4 {
        {
            let _adapter = BackendScope::new(&store);
            for number in 0..1_100 {
                let selected = if number % 2 == 0 {
                    backend
                } else {
                    backend + 1
                };
                let key = format!("{round}/{number}");
                remember_parsed_page(
                    selected,
                    key.clone(),
                    KeyOrder::Hashed,
                    0,
                    &[],
                    Arc::clone(&page),
                );
                remember_page(selected, key.clone(), b"page");
                remember(selected, key, &entry(), b"metadata");
            }
            assert_accounting();
        }
        assert_accounting();
        CURRENT.with(|current| {
            let current = current.borrow();
            let reads = current.as_ref().unwrap();
            assert!(reads
                .parsed
                .keys()
                .all(|(identity, _)| *identity != backend));
            assert!(reads.pages.keys().all(|(identity, _)| *identity != backend));
            assert!(reads
                .entries
                .keys()
                .all(|(identity, _)| *identity != backend));
        });
    }
    assert!(test_parsed_evictions() > 0);
}
