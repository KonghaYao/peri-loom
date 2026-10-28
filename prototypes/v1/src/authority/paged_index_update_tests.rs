use super::*;
use crate::object_store::{GroupWrite, MemoryObjectStore, StoredObject};
use serde_json::json;
use std::cell::RefCell;

fn entries(pin: &str, generation: usize, count: usize) -> BTreeMap<String, Value> {
    (0..count).map(|ordinal| {
        let blob = blake3::hash(format!("blob-{generation}-{ordinal}").as_bytes()).to_hex().to_string();
        let key = format!("{pin}/{generation}/{blob}");
        let value = json!({"blob":blob,"hash":"a".repeat(64),"etag":"b".repeat(64),"created_ms":1_700_000_000_000u64});
        (key,value)
    }).collect()
}

fn namespace() -> Namespace {
    Namespace::new("draft", "pins").unwrap()
}

fn publish(store: &dyn ObjectStore, pending: &RefCell<PendingPages>) -> Result<()> {
    store.put_group(pending.borrow().writes())?;
    Ok(())
}

#[test]
fn small_replacement_collapses_without_extra_reads_or_intermediate_writes() -> Result<()> {
    let store = MemoryObjectStore::default();
    let ns = namespace();
    let index = PagedIndex::with_order(&store, &ns, "authority/index-pages", KeyOrder::Bucketed);
    let before = entries("same-pin", 0, 67);
    let root = index.build(before.clone())?.unwrap();
    assert!(matches!(
        index.read(&root, 0, &[])?.as_ref(),
        Page::Branch { .. }
    ));
    let after = entries("same-pin", 1, 2);
    let removals = before.keys().cloned().collect();
    let original = RefCell::new(PendingPages::default());
    let previous = index.pages_read();
    let baseline = index.deferring(&original);
    let baseline_root = baseline
        .apply_at(Some(&root), after.clone(), &removals, 0, &[])?
        .unwrap();
    let baseline_reads = baseline.pages_read();
    assert_eq!(index.pages_read(), previous);
    let pending = RefCell::new(PendingPages::default());
    let changed = index.deferring(&pending);
    let next = changed
        .apply(Some(&root), after.clone(), &removals)?
        .unwrap();
    assert_eq!(changed.pages_read(), baseline_reads);
    assert_eq!(
        pending.borrow().writes().len(),
        1,
        "discarded drafts must not be staged"
    );
    assert!(original.borrow().writes().len() > pending.borrow().writes().len());
    publish(&store, &pending)?;
    publish(&store, &original)?;
    assert_eq!(index.entries(Some(&next))?, after);
    assert_eq!(index.entries(Some(&baseline_root))?, after);
    assert_eq!(
        index.entries(Some(&root))?,
        before,
        "old reader must retain its tree"
    );
    assert!(matches!(
        index.read(&next, 0, &[])?.as_ref(),
        Page::Leaf { .. }
    ));
    assert_eq!(
        index.entries_with_prefix(Some(&next), "same-pin/1/")?.0,
        after
    );
    Ok(())
}

#[test]
fn unchanged_siblings_remain_opaque_and_noops_preserve_root() -> Result<()> {
    let store = MemoryObjectStore::default();
    let ns = namespace();
    let index = PagedIndex::with_order(&store, &ns, "authority/index-pages", KeyOrder::Bucketed);
    let (left, right) = distinct_bucket_keys(&index);
    let root = two_leaf_branch(&index, &left, &right, json!(1), json!(2))?;
    let pending = RefCell::new(PendingPages::default());
    let changed = index.deferring(&pending);
    let next = changed
        .apply(
            Some(&root),
            BTreeMap::from([(left.clone(), json!(3))]),
            &BTreeSet::new(),
        )?
        .unwrap();
    assert_eq!(
        changed.pages_read(),
        2,
        "must not read an untouched sibling"
    );
    assert_eq!(pending.borrow().writes().len(), 2);
    assert!(matches!(
        changed.read(&next, 0, &[])?.as_ref(),
        Page::Branch { .. }
    ));
    let empty = RefCell::new(PendingPages::default());
    let unchanged = index.deferring(&empty);
    assert_eq!(
        unchanged.apply(
            Some(&root),
            BTreeMap::from([(left.clone(), json!(1))]),
            &BTreeSet::from([left.clone(), "absent/key".into()])
        )?,
        Some(root.clone())
    );
    assert!(empty.borrow().writes().is_empty());
    assert_eq!(index.get(Some(&root), &right)?, Some(json!(2)));
    Ok(())
}

fn distinct_bucket_keys(index: &PagedIndex<'_>) -> (String, String) {
    let first = "pin-0/entry".to_owned();
    let second = (1..100)
        .map(|n| format!("pin-{n}/entry"))
        .find(|key| index.nibble(key, 0).unwrap() != index.nibble(&first, 0).unwrap())
        .unwrap();
    (first, second)
}

fn two_leaf_branch(
    index: &PagedIndex<'_>,
    left: &str,
    right: &str,
    a: Value,
    b: Value,
) -> Result<String> {
    let mut children = BTreeMap::new();
    for (key, value) in [(left, a), (right, b)] {
        let hash = index.write(&Page::Leaf {
            version: 1,
            depth: 1,
            entries: BTreeMap::from([(key.to_owned(), value)]),
        })?;
        children.insert(index.nibble(key, 0)?.to_string(), hash);
    }
    index.write(&Page::Branch {
        version: 1,
        depth: 0,
        children,
    })
}

#[test]
fn exact_encoded_limit_accepts_16k_and_rejects_one_more_byte() -> Result<()> {
    for extra in [0, 1] {
        let store = MemoryObjectStore::default();
        let ns = namespace();
        let index =
            PagedIndex::with_order(&store, &ns, "authority/index-pages", KeyOrder::Bucketed);
        let (left, right) = distinct_bucket_keys(&index);
        let root = two_leaf_branch(&index, &left, &right, json!("old"), json!("old"))?;
        let mut inserts =
            BTreeMap::from([(left.clone(), json!("")), (right.clone(), json!("new"))]);
        let envelope = serde_json::to_vec(&Page::Leaf {
            version: 1,
            depth: 0,
            entries: inserts.clone(),
        })?
        .len();
        inserts.insert(left, json!("x".repeat(PAGE_BYTES - envelope + extra)));
        assert_eq!(
            serde_json::to_vec(&Page::Leaf {
                version: 1,
                depth: 0,
                entries: inserts.clone()
            })?
            .len(),
            PAGE_BYTES + extra
        );
        let pending = RefCell::new(PendingPages::default());
        let changed = index.deferring(&pending);
        let next = changed
            .apply(Some(&root), inserts.clone(), &BTreeSet::new())?
            .unwrap();
        assert_eq!(changed.pages_read(), 3);
        assert_eq!(
            pending.borrow().writes().len(),
            if extra == 0 { 1 } else { 3 }
        );
        publish(&store, &pending)?;
        assert_eq!(index.entries(Some(&next))?, inserts);
        assert_eq!(
            matches!(index.read(&next, 0, &[])?.as_ref(), Page::Leaf { .. }),
            extra == 0
        );
    }
    Ok(())
}

/// Test the sizing arithmetic independently of routing. A depth digit changes
/// between parent 9 and child 10, and JSON escaping/UTF-8 affect byte lengths.
#[test]
fn merge_size_matches_json_across_depth_digits_and_escaped_values() -> Result<()> {
    let store = MemoryObjectStore::default();
    let ns = namespace();
    let index = PagedIndex::with_order(&store, &ns, "authority/index-pages", KeyOrder::Bucketed);
    for depth in [0, 9, 10, 63, 67] {
        let mut children = BTreeMap::new();
        for slot in 0..3 {
            let entries = BTreeMap::from([(
                format!("pin/{slot}"),
                json!({"text":"line\n汉字\"\\", "values":[null, true, 1.5, {"x":"\t"}]}),
            )]);
            let bytes = serde_json::to_vec(&Page::Leaf {
                version: 1,
                depth: depth + 1,
                entries: entries.clone(),
            })?
            .len();
            children.insert(
                slot.to_string(),
                Draft::Leaf {
                    depth: depth + 1,
                    entries,
                    bytes,
                },
            );
        }
        let Draft::Leaf {
            entries,
            bytes,
            depth: actual_depth,
        } = index.collapse_draft(children, depth)?
        else {
            panic!("small known leaves must merge");
        };
        assert_eq!(actual_depth, depth);
        assert_eq!(
            bytes,
            serde_json::to_vec(&Page::Leaf {
                version: 1,
                depth,
                entries
            })?
            .len()
        );
    }
    Ok(())
}

#[test]
fn empty_result_is_none_and_insertion_wins_without_changing_old_pages() -> Result<()> {
    let store = MemoryObjectStore::default();
    let ns = namespace();
    let index = PagedIndex::with_order(&store, &ns, "authority/index-pages", KeyOrder::Bucketed);
    let original = entries("p", 0, 67);
    let root = index.build(original.clone())?.unwrap();
    let removed = original.keys().cloned().collect();
    let pending = RefCell::new(PendingPages::default());
    assert!(index
        .deferring(&pending)
        .apply(Some(&root), BTreeMap::new(), &removed)?
        .is_none());
    assert!(pending.borrow().writes().is_empty());
    let key = original.keys().next().unwrap().clone();
    let next = index
        .apply(
            Some(&root),
            BTreeMap::from([(key.clone(), json!("replaced"))]),
            &removed,
        )?
        .unwrap();
    assert_eq!(
        index.entries(Some(&next))?,
        BTreeMap::from([(key, json!("replaced"))])
    );
    assert_eq!(index.entries(Some(&root))?, original);
    Ok(())
}

#[test]
fn hashed_archive_apply_is_byte_for_byte_unchanged() -> Result<()> {
    let store = MemoryObjectStore::default();
    let ns = namespace();
    let index = PagedIndex::new(&store, &ns);
    let original = entries("p", 0, 67);
    let root = index.build(original.clone())?.unwrap();
    let adds = entries("p", 1, 2);
    let removes = original.keys().cloned().collect();
    let old = RefCell::new(PendingPages::default());
    let new = RefCell::new(PendingPages::default());
    let before = collapse_test_stats().calls;
    assert_eq!(
        index
            .deferring(&old)
            .apply_at(Some(&root), adds.clone(), &removes, 0, &[])?,
        index.deferring(&new).apply(Some(&root), adds, &removes)?
    );
    assert_eq!(collapse_test_stats().calls, before);
    let old = old.borrow();
    let new = new.borrow();
    assert_eq!(old.writes().len(), new.writes().len());
    for (a, b) in old.writes().iter().zip(new.writes()) {
        assert_eq!(a.key, b.key);
        assert_eq!(a.bytes, b.bytes);
    }
    Ok(())
}

#[test]
fn corrupted_read_and_pending_cache_tamper_fail_before_any_draft_is_written() -> Result<()> {
    let store = MemoryObjectStore::default();
    let ns = namespace();
    let index = PagedIndex::with_order(&store, &ns, "authority/index-pages", KeyOrder::Bucketed);
    let original = entries("p", 0, 67);
    let root = index.build(original.clone())?.unwrap();
    let _scope = request_reads::Scope::new();
    let cached = index.read(&root, 0, &[])?;
    let pending = RefCell::new(PendingPages {
        writes: vec![GroupWrite::new(
            index.page_key(&root)?,
            b"tampered".to_vec(),
            PutCondition::Any,
        )],
    });
    assert!(index
        .deferring(&pending)
        .apply(
            Some(&root),
            entries("p", 1, 2),
            &original.keys().cloned().collect()
        )
        .is_err());
    assert_eq!(pending.borrow().writes().len(), 1);
    assert!(Arc::ptr_eq(&cached, &index.read(&root, 0, &[])?));
    drop(_scope);
    store.put(&index.page_key(&root)?, b"corrupt", PutCondition::Any)?;
    let pending = RefCell::new(PendingPages::default());
    assert!(index
        .deferring(&pending)
        .apply(Some(&root), entries("p", 1, 2), &BTreeSet::new())
        .is_err());
    assert!(pending.borrow().writes().is_empty());
    Ok(())
}

#[test]
fn depth_and_prefix_errors_are_not_hidden_by_collapse() -> Result<()> {
    for bad_depth in [false, true] {
        let store = MemoryObjectStore::default();
        let ns = namespace();
        let index =
            PagedIndex::with_order(&store, &ns, "authority/index-pages", KeyOrder::Bucketed);
        let (left, right) = distinct_bucket_keys(&index);
        let bad = index.write(&Page::Leaf {
            version: 1,
            depth: if bad_depth { 2 } else { 1 },
            entries: BTreeMap::from([(right, json!(1))]),
        })?;
        let root = index.write(&Page::Branch {
            version: 1,
            depth: 0,
            children: BTreeMap::from([(index.nibble(&left, 0)?.to_string(), bad)]),
        })?;
        let pending = RefCell::new(PendingPages::default());
        assert!(index
            .deferring(&pending)
            .apply(
                Some(&root),
                BTreeMap::from([(left, json!(2))]),
                &BTreeSet::new()
            )
            .is_err());
        assert!(pending.borrow().writes().is_empty());
    }
    Ok(())
}

#[test]
fn repeated_threshold_crossings_keep_multiple_pins_and_old_roots_exact() -> Result<()> {
    let store = MemoryObjectStore::default();
    let ns = namespace();
    let index = PagedIndex::with_order(&store, &ns, "authority/index-pages", KeyOrder::Bucketed);
    let active = "a".repeat(64);
    let other = "b".repeat(64);
    let prefix = format!("{active}/");
    let mut current = entries(&active, 0, 67);
    current.extend(entries(&other, 0, 80));
    let mut root = index.build(current.clone())?.unwrap();
    let first_root = root.clone();
    let first = current.clone();
    for generation in 1..=80 {
        let removes = current
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .cloned()
            .collect();
        let above = generation % 2 == 0;
        let adds = entries(&active, generation, if above { 43 } else { 42 });
        let leaf_bytes = serde_json::to_vec(&Page::Leaf {
            version: 1,
            depth: 4,
            entries: adds.clone(),
        })?
        .len();
        assert_eq!(
            leaf_bytes > PAGE_BYTES,
            above,
            "fixture must actually cross the encoded page limit"
        );
        for key in &removes {
            current.remove(key);
        }
        current.extend(adds.clone());
        root = index.apply(Some(&root), adds, &removes)?.unwrap();
        assert_eq!(index.entries(Some(&root))?, current);
        assert_eq!(
            index
                .entries_with_prefix(Some(&root), &format!("{other}/"))?
                .0,
            entries(&other, 0, 80)
        );
    }
    assert_eq!(index.entries(Some(&first_root))?, first);
    Ok(())
}

#[test]
fn policy_guard_restores_nested_policy_and_is_thread_local() {
    assert!(collapse_enabled());
    {
        let _old = CollapsePolicyGuard::new(false);
        assert!(!collapse_enabled());
        {
            let _new = CollapsePolicyGuard::new(true);
            assert!(collapse_enabled());
        }
        assert!(!collapse_enabled());
        assert!(std::thread::spawn(collapse_enabled).join().unwrap());
    }
    assert!(collapse_enabled());
}

#[derive(Default)]
struct FailWrites {
    inner: MemoryObjectStore,
    fail: std::sync::atomic::AtomicBool,
}
impl ObjectStore for FailWrites {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }
    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        if self.fail.load(std::sync::atomic::Ordering::Relaxed) {
            anyhow::bail!("injected write failure");
        }
        self.inner.put(key, bytes, condition)
    }
    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

#[test]
fn failed_materialization_does_not_change_the_old_tree() -> Result<()> {
    let store = FailWrites::default();
    let ns = namespace();
    let index = PagedIndex::with_order(&store, &ns, "authority/index-pages", KeyOrder::Bucketed);
    let before = entries("p", 0, 67);
    let root = index.build(before.clone())?.unwrap();
    store.fail.store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(index
        .apply(
            Some(&root),
            entries("p", 1, 2),
            &before.keys().cloned().collect()
        )
        .is_err());
    assert_eq!(index.entries(Some(&root))?, before);
    Ok(())
}
