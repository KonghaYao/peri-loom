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
fn decoded_cache_budget_counts_nested_allocations_and_rejects_without_hiding_reads() -> Result<()> {
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
    assert!(rejected > 0);
    let last = pages.read(roots.last().unwrap(), 0, &[])?;
    assert!(!Arc::ptr_eq(
        &last,
        &pages.read(roots.last().unwrap(), 0, &[])?
    ));
    assert!(Arc::ptr_eq(&first, &pages.read(&roots[0], 0, &[])?));
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
    assert!(rejected > 0);
    let page = pages.read(&last, 0, &[])?;
    assert!(!Arc::ptr_eq(&page, &pages.read(&last, 0, &[])?));
    Ok(())
}
