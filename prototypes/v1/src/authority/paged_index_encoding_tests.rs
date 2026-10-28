//! A test-only switch retains the former second encoding, without duplicating
//! tree construction. The comparison covers actual serialized bytes and work;
//! it is not a wall-time benchmark or an alternate production policy.

use super::*;
use crate::object_store::{LocalObjectStore, MemoryObjectStore};
use serde_json::json;
use std::cell::{Cell, RefCell};

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub(crate) struct LeafEncodingTestStats {
    /// Calls through this module's typed page encoder, including oversized
    /// sizing attempts. Other modules' sizing/serialization is not counted.
    pub calls: usize,
    pub encoded_bytes: usize,
    pub reusable_leaves: usize,
    pub reusable_leaf_bytes: usize,
}

thread_local! {
    static REUSE: Cell<bool> = const { Cell::new(true) };
    static STATS: Cell<LeafEncodingTestStats> = const { Cell::new(LeafEncodingTestStats {
        calls: 0, encoded_bytes: 0, reusable_leaves: 0, reusable_leaf_bytes: 0,
    }) };
}

fn observe(update: impl FnOnce(&mut LeafEncodingTestStats)) {
    STATS.with(|stats| {
        let mut next = stats.get();
        update(&mut next);
        stats.set(next);
    });
}

pub(super) fn observe_encode(bytes: usize) {
    observe(|stats| {
        stats.calls += 1;
        stats.encoded_bytes += bytes;
    });
}

pub(super) fn observe_reusable_leaf(bytes: usize) {
    observe(|stats| {
        stats.reusable_leaves += 1;
        stats.reusable_leaf_bytes += bytes;
    });
}

pub(super) fn reuse_enabled() -> bool {
    REUSE.with(Cell::get)
}

pub(crate) fn leaf_encoding_test_stats() -> LeafEncodingTestStats {
    STATS.with(Cell::get)
}

/// Thread-local mechanism policy. Production contains neither state nor switch.
/// Like the existing collapse counter, stats are cumulative within this thread;
/// callers may snapshot them at the exact measured interval boundaries.
pub(crate) struct LeafEncodingPolicyGuard {
    previous: bool,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl LeafEncodingPolicyGuard {
    pub(crate) fn new(reuse: bool) -> Self {
        Self {
            previous: REUSE.with(|policy| policy.replace(reuse)),
            _local: std::marker::PhantomData,
        }
    }
}

impl Drop for LeafEncodingPolicyGuard {
    fn drop(&mut self) {
        REUSE.with(|policy| policy.set(self.previous));
    }
}

fn namespace() -> Namespace {
    Namespace::new("encoding", "equivalence").unwrap()
}

fn entries(start: usize, end: usize) -> BTreeMap<String, Value> {
    (start..end)
        .map(|ordinal| {
            (
                format!(
                    "pin-{}/generation-{}/entry-{ordinal:04}",
                    ordinal % 3,
                    ordinal / 64
                ),
                json!({"ordinal": ordinal, "escaped": "quote\" slash\\ line\n λ".repeat(8),
                       "array": [null, true, {"nested": ordinal}]}),
            )
        })
        .collect()
}

#[derive(Debug, PartialEq)]
struct History {
    roots: Vec<String>,
    writes: Vec<Vec<(String, Vec<u8>)>>,
}

fn history(order: KeyOrder, reuse: bool) -> Result<(History, LeafEncodingTestStats)> {
    let _policy = super::LeafEncodingPolicyGuard::new(reuse);
    let before = super::leaf_encoding_test_stats();
    let directory = tempfile::tempdir()?;
    let ns = namespace();
    let mut result = History {
        roots: Vec::new(),
        writes: Vec::new(),
    };
    let mut snapshots = Vec::new();
    {
        let store = LocalObjectStore::open(directory.path())?;
        let index = PagedIndex::with_order(&store, &ns, "authority/index-pages", order);
        let mut expected = BTreeMap::new();
        let mut root: Option<String> = None;
        for step in 0..5 {
            let pending = RefCell::new(PendingPages::default());
            let deferred = index.deferring(&pending);
            root = match step {
                0 => {
                    expected = entries(0, 120);
                    deferred.build(expected.clone())?
                }
                1 => {
                    // Exercise the same 64+1 pending-read boundary as a 65-receipt
                    // archive extension. Neither mode discards intermediate pages.
                    for (start, end) in [(120, 184), (184, 185)] {
                        let incoming = entries(start, end);
                        root = Some(deferred.insert_many(root.as_deref(), incoming.clone())?);
                        expected.extend(incoming);
                    }
                    root
                }
                2 => {
                    let unchanged = deferred.insert_many(root.as_deref(), entries(0, 1))?;
                    assert_eq!(root.as_deref(), Some(unchanged.as_str()));
                    assert!(pending.borrow().writes().is_empty());
                    Some(unchanged)
                }
                3 => {
                    let removals: BTreeSet<_> = entries(0, 50).into_keys().collect();
                    let replacements: BTreeMap<_, _> = entries(60, 65)
                        .into_keys()
                        .map(|key| (key, json!({"replacement":"\"\\\n λ"})))
                        .collect();
                    expected.retain(|key, _| !removals.contains(key));
                    expected.extend(replacements.clone());
                    deferred.apply(root.as_deref(), replacements, &removals)?
                }
                4 => {
                    let removals: BTreeSet<_> = expected.keys().skip(3).cloned().collect();
                    expected.retain(|key, _| !removals.contains(key));
                    deferred.apply(root.as_deref(), BTreeMap::new(), &removals)?
                }
                _ => unreachable!(),
            };
            let root = root.as_ref().unwrap();
            assert_eq!(deferred.entries(Some(root))?, expected);
            result.writes.push(
                pending
                    .borrow()
                    .writes()
                    .iter()
                    .map(|write| {
                        assert!(matches!(write.condition, PutCondition::Any));
                        assert!(write.bytes.len() <= PAGE_BYTES);
                        (write.key.clone(), write.bytes.clone())
                    })
                    .collect(),
            );
            store.put_group(pending.borrow().writes())?;
            result.roots.push(root.clone());
            snapshots.push(expected.clone());
            // Every prior root remains a valid independent snapshot after each
            // later publication, including a collapse/removal of its children.
            for (old_root, old_entries) in result.roots.iter().zip(&snapshots) {
                assert_eq!(index.entries(Some(old_root))?, *old_entries);
            }
        }
    }
    let reopened = LocalObjectStore::open(directory.path())?;
    let index = PagedIndex::with_order(&reopened, &ns, "authority/index-pages", order);
    for (root, expected) in result.roots.iter().zip(snapshots) {
        assert_eq!(index.entries(Some(root))?, expected);
    }
    let after = super::leaf_encoding_test_stats();
    Ok((
        result,
        LeafEncodingTestStats {
            calls: after.calls - before.calls,
            encoded_bytes: after.encoded_bytes - before.encoded_bytes,
            reusable_leaves: after.reusable_leaves - before.reusable_leaves,
            reusable_leaf_bytes: after.reusable_leaf_bytes - before.reusable_leaf_bytes,
        },
    ))
}

#[test]
fn leaf_encoding_reuse_preserves_complete_histories_and_reopen() -> Result<()> {
    for order in [KeyOrder::Hashed, KeyOrder::Bucketed] {
        let (old, baseline) = history(order, false)?;
        let (new, candidate) = history(order, true)?;
        assert_eq!(
            old, new,
            "every staged byte, key, order and root must match"
        );
        assert!(candidate.reusable_leaves > 0);
        assert_eq!(baseline.reusable_leaves, candidate.reusable_leaves);
        assert_eq!(baseline.reusable_leaf_bytes, candidate.reusable_leaf_bytes);
        assert_eq!(baseline.calls - candidate.calls, candidate.reusable_leaves);
        assert_eq!(
            baseline.encoded_bytes - candidate.encoded_bytes,
            candidate.reusable_leaf_bytes
        );
        eprintln!(
            "LEAF_ENCODING_WORK order={order:?} baseline={} candidate={}",
            serde_json::to_string(&baseline)?,
            serde_json::to_string(&candidate)?
        );
    }
    Ok(())
}

#[test]
fn leaf_encoding_reuse_keeps_the_exact_limit_at_depth_digit_boundaries() -> Result<()> {
    for order in [KeyOrder::Hashed, KeyOrder::Bucketed] {
        for depth in [0, 9, 10] {
            for extra in [0, 1] {
                let escaped = "quote\" slash\\ line\n λ";
                let initial = BTreeMap::from([("pin/key".to_owned(), json!(escaped))]);
                let overhead = serde_json::to_vec(&Page::Leaf {
                    version: 1,
                    depth,
                    entries: initial,
                })?
                .len();
                let entries = BTreeMap::from([(
                    "pin/key".to_owned(),
                    json!(format!(
                        "{escaped}{}",
                        "x".repeat(PAGE_BYTES - overhead + extra)
                    )),
                )]);
                let expected = serde_json::to_vec(&Page::Leaf {
                    version: 1,
                    depth,
                    entries: entries.clone(),
                })?;
                assert_eq!(expected.len(), PAGE_BYTES + extra);
                let mut results = Vec::new();
                for reuse in [false, true] {
                    let _policy = super::LeafEncodingPolicyGuard::new(reuse);
                    let store = MemoryObjectStore::default();
                    let ns = namespace();
                    let index = PagedIndex::with_order(&store, &ns, "authority/index-pages", order);
                    let pending = RefCell::new(PendingPages::default());
                    let deferred = index.deferring(&pending);
                    let result = deferred.leaf_or_branch(entries.clone(), depth);
                    if extra == 0 {
                        let hash = result?;
                        assert_eq!(hash, blake3::hash(&expected).to_hex().as_str());
                        assert_eq!(pending.borrow().writes().len(), 1);
                        assert_eq!(pending.borrow().writes()[0].bytes, expected);
                        let prefix = (0..depth)
                            .map(|position| deferred.nibble("pin/key", position).map(|n| n as u8))
                            .collect::<Result<Vec<_>>>()?;
                        assert!(matches!(
                            deferred.read(&hash, depth, &prefix)?.as_ref(),
                            Page::Leaf { .. }
                        ));
                        results.push(Some(hash));
                    } else {
                        assert!(result.is_err(), "a single oversized entry cannot split");
                        assert!(pending.borrow().writes().is_empty());
                        results.push(None);
                    }
                }
                assert_eq!(results[0], results[1]);
            }
        }
    }
    Ok(())
}

#[test]
fn leaf_encoding_reuse_preserves_golden_escaped_bytes() -> Result<()> {
    let store = MemoryObjectStore::default();
    let ns = namespace();
    let index = PagedIndex::new(&store, &ns);
    let entries = BTreeMap::from([(
        "pin/key".to_owned(),
        json!({"a":"quote\"slash\\line\nλ","n":7}),
    )]);
    let expected = r#"{"kind":"Leaf","version":1,"depth":0,"entries":{"pin/key":{"a":"quote\"slash\\line\nλ","n":7}}}"#.as_bytes();
    let hash = index.build(entries.clone())?.unwrap();
    let bytes = store.get(&index.page_key(&hash)?)?.unwrap().bytes;
    assert_eq!(bytes, expected);
    assert_eq!(hash, blake3::hash(expected).to_hex().as_str());
    assert_eq!(index.entries(Some(&hash))?, entries);
    Ok(())
}
