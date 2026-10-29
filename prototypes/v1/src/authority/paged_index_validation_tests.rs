//! Test-only controls compare the historical validation work with identical
//! accepted/rejected paths. Production has neither counters nor policy state.

use super::*;
use crate::object_store::MemoryObjectStore;
use serde_json::json;
use std::cell::Cell;

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub(crate) struct ValidationTestStats {
    pub routing_digests: usize,
    pub routing_digest_bytes: usize,
    pub prefix_keys: usize,
    pub prefix_nibbles: usize,
    pub pending_reads: usize,
    /// Successful original size/digest checks, before any pending parse hit.
    pub pending_hash_checks: usize,
    pub pending_parsed_hits: usize,
    pub page_decodes: usize,
    pub decoded_bytes: usize,
    pub pending_decodes: usize,
    pub pending_decoded_bytes: usize,
}

thread_local! {
    static REUSE_PREFIX: Cell<bool> = const { Cell::new(true) };
    static STATS: Cell<ValidationTestStats> = const { Cell::new(ValidationTestStats {
        routing_digests: 0, routing_digest_bytes: 0, prefix_keys: 0, prefix_nibbles: 0,
        pending_reads: 0, pending_hash_checks: 0, pending_parsed_hits: 0,
        page_decodes: 0, decoded_bytes: 0, pending_decodes: 0, pending_decoded_bytes: 0,
    }) };
}

fn observe(change: impl FnOnce(&mut ValidationTestStats)) {
    STATS.with(|stats| {
        let mut next = stats.get();
        change(&mut next);
        stats.set(next);
    });
}

pub(super) fn observe_digest(bytes: usize) {
    observe(|stats| {
        stats.routing_digests += 1;
        stats.routing_digest_bytes += bytes;
    });
}

pub(super) fn observe_prefix(nibbles: usize) {
    observe(|stats| {
        stats.prefix_keys += 1;
        stats.prefix_nibbles += nibbles;
    });
}

pub(super) fn prefix_reuse_enabled() -> bool {
    REUSE_PREFIX.with(Cell::get)
}

pub(super) fn observe_pending_read() {
    observe(|stats| stats.pending_reads += 1);
}

pub(super) fn observe_pending_hash_check() {
    observe(|stats| stats.pending_hash_checks += 1);
}

pub(super) fn observe_decode(bytes: usize, pending: bool) {
    observe(|stats| {
        stats.page_decodes += 1;
        stats.decoded_bytes += bytes;
        if pending {
            stats.pending_decodes += 1;
            stats.pending_decoded_bytes += bytes;
        }
    });
}

pub(crate) fn validation_test_stats() -> ValidationTestStats {
    STATS.with(Cell::get)
}

pub(crate) struct PrefixValidationPolicyGuard {
    previous: bool,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl PrefixValidationPolicyGuard {
    pub(crate) fn new(enabled: bool) -> Self {
        Self {
            previous: REUSE_PREFIX.with(|policy| policy.replace(enabled)),
            _local: std::marker::PhantomData,
        }
    }
}

impl Drop for PrefixValidationPolicyGuard {
    fn drop(&mut self) {
        REUSE_PREFIX.with(|policy| policy.set(self.previous));
    }
}

#[test]
fn shared_routing_digest_matches_every_historical_prefix_and_rejects_each_bad_ancestor(
) -> Result<()> {
    let store = MemoryObjectStore::default();
    let namespace = Namespace::new("routing", "equivalence")?;
    let keys = ["plain", "pin/generation/blob", "p-_.~/entry-123", "a/b/c/d"];
    for order in [KeyOrder::Hashed, KeyOrder::Bucketed] {
        let index = PagedIndex::with_order(&store, &namespace, "authority/index-pages", order);
        for key in keys {
            for depth in 0..=index.max_depth() {
                let Ok(prefix) = (0..depth)
                    .map(|position| index.nibble(key, position).map(|value| value as u8))
                    .collect::<Result<Vec<_>>>()
                else {
                    // The bucketed layout historically refuses keys without a pin
                    // separator whenever at least one bucket nibble is inspected.
                    assert_eq!(order, KeyOrder::Bucketed);
                    assert!(depth > 0 && !key.contains('/'));
                    assert!(!index.matches_prefix(key, &vec![0; usize::from(depth)]));
                    continue;
                };
                assert!(index.matches_prefix(key, &prefix));
                let before = validation_test_stats();
                assert!(index.matches_prefix(key, &prefix));
                let after = validation_test_stats();
                let expected = if depth == 0 {
                    0
                } else if order == KeyOrder::Bucketed && depth > BUCKET_NIBBLES {
                    2
                } else {
                    1
                };
                assert_eq!(after.routing_digests - before.routing_digests, expected);
                for position in 0..prefix.len() {
                    let mut wrong = prefix.clone();
                    wrong[position] = (wrong[position] + 1) % 16;
                    let expected = {
                        let _old = PrefixValidationPolicyGuard::new(false);
                        index.matches_prefix(key, &wrong)
                    };
                    assert!(!expected);
                    assert_eq!(index.matches_prefix(key, &wrong), expected);
                }
            }
            assert!(!index.matches_prefix(key, &vec![0; usize::from(index.max_depth()) + 1]));
            assert!(!index.matches_prefix(key, &[16]));
        }
    }
    Ok(())
}

#[test]
fn shared_routing_digest_keeps_leaf_shape_and_full_path_validation() -> Result<()> {
    let store = MemoryObjectStore::default();
    let namespace = Namespace::new("routing", "validation")?;
    for order in [KeyOrder::Hashed, KeyOrder::Bucketed] {
        let index = PagedIndex::with_order(&store, &namespace, "authority/index-pages", order);
        let key = "pin/generation/entry";
        let depth = 6;
        let prefix = (0..depth)
            .map(|position| index.nibble(key, position).map(|value| value as u8))
            .collect::<Result<Vec<_>>>()?;
        let hash = index.write(&Page::Leaf {
            version: 1,
            depth,
            entries: BTreeMap::from([(key.into(), json!(1))]),
        })?;
        for reuse in [false, true] {
            let _policy = PrefixValidationPolicyGuard::new(reuse);
            let _scope = request_reads::Scope::new();
            assert!(index.read(&hash, depth, &prefix).is_ok());
            let mut wrong = prefix.clone();
            wrong[0] = (wrong[0] + 1) % 16;
            assert!(index.read(&hash, depth, &wrong).is_err());
            assert!(index.read(&hash, depth - 1, &prefix).is_err());
            let invalid = index.write(&Page::Leaf {
                version: 1,
                depth,
                entries: BTreeMap::from([("pin/../entry".into(), json!(1))]),
            })?;
            assert!(index.read(&invalid, depth, &prefix).is_err());
        }
    }
    Ok(())
}

#[test]
fn whole_tree_validation_keeps_all_ancestor_checks_with_reused_digest() -> Result<()> {
    let store = MemoryObjectStore::default();
    let namespace = Namespace::new("routing", "whole-tree")?;
    let index = PagedIndex::new(&store, &namespace);
    let key = "entry/one";
    let prefix = (0..6)
        .map(|depth| index.nibble(key, depth).map(|n| n as u8))
        .collect::<Result<Vec<_>>>()?;
    let mut root = index.write(&Page::Leaf {
        version: 1,
        depth: 6,
        entries: BTreeMap::from([(key.into(), json!(1))]),
    })?;
    for depth in (0..6).rev() {
        root = index.write(&Page::Branch {
            version: 1,
            depth: depth as u16,
            children: BTreeMap::from([(prefix[depth].to_string(), root)]),
        })?;
    }
    let bytes = store.get(&index.page_key(&root)?)?.unwrap().bytes;
    let Page::Branch { mut children, .. } = serde_json::from_slice(&bytes)? else {
        unreachable!()
    };
    let child = children.pop_first().unwrap().1;
    let wrong = index.write(&Page::Branch {
        version: 1,
        depth: 0,
        children: BTreeMap::from([(((prefix[0] + 1) % 16).to_string(), child)]),
    })?;
    for reuse in [false, true] {
        let _policy = PrefixValidationPolicyGuard::new(reuse);
        let mut entries = BTreeMap::new();
        walk_tree_with(
            Some(&root),
            |hash| Ok(store.get(&index.page_key(hash)?)?.unwrap().bytes),
            |leaf| {
                entries.extend(leaf.clone());
                Ok(())
            },
        )?;
        assert_eq!(entries, BTreeMap::from([(key.into(), json!(1))]));
        assert!(walk_tree(Some(&wrong), |hash| Ok(store
            .get(&index.page_key(hash)?)?
            .unwrap()
            .bytes))
        .is_err());
    }
    Ok(())
}
