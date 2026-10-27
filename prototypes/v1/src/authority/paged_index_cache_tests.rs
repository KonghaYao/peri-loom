use super::*;
use crate::object_store::{GroupWrite, MemoryObjectStore};
use serde_json::json;

#[test]
fn parsed_pages_share_within_one_scope_and_mutations_leave_cached_pages_immutable() -> Result<()> {
    let store = MemoryObjectStore::default();
    let namespace = Namespace::new("parsed", "scope")?;
    let pages = PagedIndex::new(&store, &namespace);
    let root = pages.insert_many(None, BTreeMap::from([("first".into(), json!(1))]))?;
    let retired;
    {
        let _scope = request_reads::Scope::new();
        let first = pages.read(&root, 0, &[])?;
        retired = Arc::downgrade(&first);
        assert!(Arc::ptr_eq(&first, &pages.read(&root, 0, &[])?));
        {
            let _nested = request_reads::Scope::new();
            let nested = pages.read(&root, 0, &[])?;
            assert!(!Arc::ptr_eq(&first, &nested));
            assert!(Arc::ptr_eq(&nested, &pages.read(&root, 0, &[])?));
        }
        assert!(Arc::ptr_eq(&first, &pages.read(&root, 0, &[])?));
        let changed = pages
            .apply(
                Some(&root),
                BTreeMap::from([("first".into(), json!(2))]),
                &BTreeSet::new(),
            )?
            .unwrap();
        assert_eq!(pages.get(Some(&root), "first")?, Some(json!(1)));
        assert_eq!(pages.get(Some(&changed), "first")?, Some(json!(2)));
        assert!(Arc::ptr_eq(&first, &pages.read(&root, 0, &[])?));
        let key = pages.page_key(&root)?;
        store.put(&key, b"corrupt after the request read", PutCondition::Any)?;
    }
    assert!(retired.upgrade().is_none());
    let _next_scope = request_reads::Scope::new();
    assert!(pages.read(&root, 0, &[]).is_err());
    Ok(())
}

#[test]
fn cached_pages_do_not_cross_backend_namespace_depth_prefix_or_order() -> Result<()> {
    let store = MemoryObjectStore::default();
    let other = MemoryObjectStore::default();
    let namespace = Namespace::new("parsed", "identity")?;
    let another = Namespace::new("parsed", "other")?;
    let pages = PagedIndex::new(&store, &namespace);
    let identity = (0..100)
        .map(|number| format!("pin-{number}/entry"))
        .find(|key| {
            let pin = key.split_once('/').unwrap().0;
            pages.nibble(key, 0).unwrap() != u16::from(PagedIndex::bucket_nibble(pin, 0).unwrap())
        })
        .unwrap();
    let slot = pages.nibble(&identity, 0)? as u8;
    let root = pages.write(&Page::Leaf {
        version: 1,
        depth: 1,
        entries: BTreeMap::from([(identity, json!(1))]),
    })?;
    let _scope = request_reads::Scope::new();
    let first = pages.read(&root, 1, &[slot])?;
    assert!(Arc::ptr_eq(&first, &pages.read(&root, 1, &[slot])?));
    assert!(PagedIndex::new(&other, &namespace)
        .read(&root, 1, &[slot])
        .is_err());
    assert!(PagedIndex::new(&store, &another)
        .read(&root, 1, &[slot])
        .is_err());
    assert!(pages.read(&root, 0, &[]).is_err());
    assert!(pages.read(&root, 1, &[(slot + 1) % 16]).is_err());
    assert!(pages
        .ordered(KeyOrder::Bucketed)
        .read(&root, 1, &[slot])
        .is_err());
    assert!(Arc::ptr_eq(&first, &pages.read(&root, 1, &[slot])?));
    Ok(())
}

#[test]
fn pending_bytes_are_validated_before_any_parsed_cache_hit() -> Result<()> {
    let store = MemoryObjectStore::default();
    let namespace = Namespace::new("parsed", "pending")?;
    let pages = PagedIndex::new(&store, &namespace);
    let root = pages.insert_many(None, BTreeMap::from([("first".into(), json!(1))]))?;
    let key = pages.page_key(&root)?;
    let _scope = request_reads::Scope::new();
    let durable = pages.read(&root, 0, &[])?;
    let pending = std::cell::RefCell::new(PendingPages {
        writes: vec![GroupWrite::new(
            key.clone(),
            b"tampered".to_vec(),
            PutCondition::Any,
        )],
    });
    assert!(pages.deferring(&pending).read(&root, 0, &[]).is_err());
    pending.borrow_mut().writes[0].bytes = store.get(&key)?.unwrap().bytes;
    let staged = pages.deferring(&pending).read(&root, 0, &[])?;
    assert!(!Arc::ptr_eq(&durable, &staged));
    assert!(Arc::ptr_eq(&durable, &pages.read(&root, 0, &[])?));
    Ok(())
}

#[test]
fn invalid_pages_are_not_cached_as_successful_parses() -> Result<()> {
    let store = MemoryObjectStore::default();
    let namespace = Namespace::new("parsed", "invalid")?;
    let pages = PagedIndex::new(&store, &namespace);
    let malformed = pages.write(&Page::Leaf {
        version: 2,
        depth: 0,
        entries: BTreeMap::from([("first".into(), json!(1))]),
    })?;
    let _scope = request_reads::Scope::new();
    assert!(pages.read(&malformed, 0, &[]).is_err());
    assert_eq!(request_reads::test_parsed_usage().0, 0);
    assert!(pages.read(&malformed, 0, &[]).is_err());
    assert_eq!(request_reads::test_parsed_usage().0, 0);
    Ok(())
}

#[test]
fn decoded_cache_budget_counts_nested_allocations_and_follows_new_working_sets() -> Result<()> {
    let store = MemoryObjectStore::default();
    let namespace = Namespace::new("parsed", "budget")?;
    let pages = PagedIndex::new(&store, &namespace);
    let nested = json!((0..512).map(|_| json!({"x": 0})).collect::<Vec<_>>());
    let mut roots = Vec::new();
    for number in 0..24 {
        roots.push(pages.insert_many(
            None,
            BTreeMap::from([(format!("key-{number}"), nested.clone())]),
        )?);
    }
    let _scope = request_reads::Scope::new();
    let first = pages.read(&roots[0], 0, &[])?;
    assert!(first.memory_charge() > serde_json::to_vec(first.as_ref())?.len() * 20);
    for root in &roots {
        pages.read(root, 0, &[])?;
    }
    let (entries, bytes, rejected) = request_reads::test_parsed_usage();
    assert!(entries < roots.len());
    assert!(bytes <= 4 * 1024 * 1024);
    assert_eq!(rejected, 0);
    assert!(request_reads::test_parsed_evictions() > 0);
    let last = pages.read(roots.last().unwrap(), 0, &[])?;
    assert!(Arc::ptr_eq(
        &last,
        &pages.read(roots.last().unwrap(), 0, &[])?
    ));
    let reloaded = pages.read(&roots[0], 0, &[])?;
    assert!(!Arc::ptr_eq(&first, &reloaded));
    assert_eq!(
        serde_json::to_value(first.as_ref())?,
        serde_json::to_value(reloaded.as_ref())?
    );
    assert!(Arc::ptr_eq(&reloaded, &pages.read(&roots[0], 0, &[])?));
    Ok(())
}

#[test]
fn parsed_cache_can_accept_validated_pages_when_raw_byte_cache_is_full() -> Result<()> {
    let store = MemoryObjectStore::default();
    let namespace = Namespace::new("parsed", "raw-full")?;
    let pages = PagedIndex::new(&store, &namespace);
    let root = pages.insert_many(None, BTreeMap::from([("first".into(), json!(1))]))?;
    let _scope = request_reads::Scope::new();
    request_reads::remember_page(1, "fill".into(), &vec![0; 4 * 1024 * 1024]);
    let first = pages.read(&root, 0, &[])?;
    assert!(Arc::ptr_eq(&first, &pages.read(&root, 0, &[])?));
    assert_eq!(request_reads::test_parsed_usage().0, 1);
    Ok(())
}

#[test]
fn decoded_charge_includes_nested_arrays_and_short_string_allocations() {
    let values = (0..512)
        .map(|_| Value::Array(vec![Value::String("x".to_owned())]))
        .collect::<Vec<_>>();
    let charge = value_heap_charge(&Value::Array(values));
    assert!(charge >= 512 * (128 + std::mem::size_of::<Value>() + 1));
}

#[test]
fn parsed_entry_limit_is_independent_of_the_byte_budget() -> Result<()> {
    let store = MemoryObjectStore::default();
    let namespace = Namespace::new("parsed", "entry-limit")?;
    let pages = PagedIndex::new(&store, &namespace);
    let _scope = request_reads::Scope::new();
    let mut last = String::new();
    for number in 0..1_100 {
        last = pages.insert_many(None, BTreeMap::from([("key".into(), json!(number))]))?;
        pages.read(&last, 0, &[])?;
    }
    let (entries, bytes, rejected) = request_reads::test_parsed_usage();
    assert_eq!(entries, 1024);
    assert!(bytes < 4 * 1024 * 1024);
    assert_eq!(rejected, 0);
    assert_eq!(request_reads::test_parsed_evictions(), 76);
    let page = pages.read(&last, 0, &[])?;
    assert!(Arc::ptr_eq(&page, &pages.read(&last, 0, &[])?));
    Ok(())
}

#[test]
fn parsed_clock_retains_referenced_pages_and_has_no_stale_slots() -> Result<()> {
    let store = MemoryObjectStore::default();
    let namespace = Namespace::new("parsed", "clock")?;
    let pages = PagedIndex::new(&store, &namespace);
    let roots = (0..1_100)
        .map(|number| pages.insert_many(None, BTreeMap::from([("key".into(), json!(number))])))
        .collect::<Result<Vec<_>>>()?;
    let _scope = request_reads::Scope::new();
    for root in &roots[..1_024] {
        pages.read(root, 0, &[])?;
    }
    let hot = pages.read(&roots[0], 0, &[])?;
    let cold = pages.read(&roots[1], 0, &[])?;
    // Both entries have a second chance; walking the complete ring clears that
    // chance and evicts cold entries, while the repeated root remains referenced.
    for root in &roots[1_024..] {
        pages.read(root, 0, &[])?;
        assert!(Arc::ptr_eq(&hot, &pages.read(&roots[0], 0, &[])?));
    }
    assert!(Arc::ptr_eq(&cold, &pages.read(&roots[1], 0, &[])?));
    for _ in 0..4 {
        for root in &roots {
            let page = pages.read(root, 0, &[])?;
            assert!(Arc::ptr_eq(&page, &pages.read(root, 0, &[])?));
        }
        let (entries, charge, rejected) = request_reads::test_parsed_usage();
        assert_eq!(entries, 1_024);
        assert!(charge <= 4 * 1_024 * 1_024);
        assert_eq!(rejected, 0);
    }
    assert!(request_reads::test_parsed_evictions() > 1_024);
    Ok(())
}

#[test]
fn oversized_parsed_page_does_not_flush_useful_pages_or_hide_context_errors() -> Result<()> {
    let store = MemoryObjectStore::default();
    let namespace = Namespace::new("parsed", "oversized")?;
    let pages = PagedIndex::new(&store, &namespace);
    let root = pages.insert_many(None, BTreeMap::from([("key".into(), json!(1))]))?;
    let _scope = request_reads::Scope::new();
    let cached = pages.read(&root, 0, &[])?;
    let oversized = Arc::new(Page::Leaf {
        version: 1,
        depth: 0,
        entries: BTreeMap::from([("key".into(), Value::String("x".repeat(4 * 1_024 * 1_024)))]),
    });
    request_reads::remember_parsed_page(1, "oversized".into(), KeyOrder::Hashed, 0, &[], oversized);
    assert_eq!(request_reads::test_parsed_usage().0, 1);
    assert_eq!(request_reads::test_parsed_usage().2, 1);
    assert_eq!(request_reads::test_parsed_evictions(), 0);
    assert!(Arc::ptr_eq(&cached, &pages.read(&root, 0, &[])?));
    assert!(pages.read(&root, 1, &[0]).is_err());
    assert!(Arc::ptr_eq(&cached, &pages.read(&root, 0, &[])?));
    Ok(())
}

#[test]
fn evicted_pages_still_validate_context_and_do_not_alias_later_cached_pages() -> Result<()> {
    let store = MemoryObjectStore::default();
    let namespace = Namespace::new("parsed", "eviction-validation")?;
    let pages = PagedIndex::new(&store, &namespace);
    let identity = "original".to_owned();
    let slot = pages.nibble(&identity, 0)? as u8;
    let root = pages.write(&Page::Leaf {
        version: 1,
        depth: 1,
        entries: BTreeMap::from([(identity.clone(), json!(1))]),
    })?;
    let key = pages.page_key(&root)?;
    let _scope = request_reads::Scope::new();
    // Saturating the independent raw cache ensures the reload below uses the
    // actual durable bytes, rather than proving only the raw cache's behavior.
    request_reads::remember_page(1, "fill".into(), &vec![0; 4 * 1_024 * 1_024]);
    let mut held = pages.read(&root, 1, &[slot])?;
    for number in 0..1_024 {
        let pressure = pages.insert_many(None, BTreeMap::from([("key".into(), json!(number))]))?;
        pages.read(&pressure, 0, &[])?;
    }
    assert_eq!(request_reads::test_parsed_evictions(), 1);
    assert!(pages.read(&root, 1, &[(slot + 1) % 16]).is_err());
    assert!(pages.read(&root, 0, &[]).is_err());
    let reloaded = pages.read(&root, 1, &[slot])?;
    assert!(!Arc::ptr_eq(&held, &reloaded));
    let Page::Leaf { entries, .. } = Arc::make_mut(&mut held) else {
        panic!("leaf expected")
    };
    entries.insert(identity.clone(), json!(2));
    let Page::Leaf { entries, .. } = reloaded.as_ref() else {
        panic!("leaf expected")
    };
    assert_eq!(entries.get(&identity), Some(&json!(1)));
    assert!(Arc::ptr_eq(&reloaded, &pages.read(&root, 1, &[slot])?));
    let pending = std::cell::RefCell::new(PendingPages {
        writes: vec![GroupWrite::new(
            key.clone(),
            b"corrupt".to_vec(),
            PutCondition::Any,
        )],
    });
    assert!(pages.deferring(&pending).read(&root, 1, &[slot]).is_err());
    // A new scope does not inherit either parsed or raw bytes, including after
    // an eviction/reload cycle in the enclosing scope.
    store.put(&key, b"corrupt", PutCondition::Any)?;
    let _nested = request_reads::Scope::new();
    assert!(pages.read(&root, 1, &[slot]).is_err());
    Ok(())
}
