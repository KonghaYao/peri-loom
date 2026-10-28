//! Keep changed pin-index nodes in memory until their final shape is known. A
//! shrinking pin must not pay for the old split path forever. Unchanged subtrees
//! stay opaque: collapse never reads a sibling merely to try to merge it.

use super::*;

enum Draft {
    Existing(String),
    Leaf {
        depth: u16,
        entries: BTreeMap<String, Value>,
        bytes: usize,
    },
    Branch {
        depth: u16,
        children: BTreeMap<String, Draft>,
    },
}

pub(super) fn collapse_enabled() -> bool {
    #[cfg(test)]
    {
        POLICY.with(std::cell::Cell::get)
    }
    #[cfg(not(test))]
    {
        true
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub(crate) struct CollapseTestStats {
    pub calls: usize,
    pub merge_attempts: usize,
    pub merges: usize,
    pub sized_bytes: usize,
    pub drafts: usize,
    /// Conservative charge for the final draft tree before materialization. This
    /// is not process RSS or a peak-allocation measurement.
    pub max_final_draft_charge: usize,
}

#[cfg(test)]
thread_local! {
    static POLICY: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
    static STATS: std::cell::Cell<CollapseTestStats> = const { std::cell::Cell::new(CollapseTestStats { calls: 0, merge_attempts: 0, merges: 0, sized_bytes: 0, drafts: 0, max_final_draft_charge: 0 }) };
}

#[cfg(test)]
fn observe(f: impl FnOnce(&mut CollapseTestStats)) {
    STATS.with(|stats| {
        let mut next = stats.get();
        f(&mut next);
        stats.set(next);
    });
}

#[cfg(test)]
pub(crate) fn collapse_test_stats() -> CollapseTestStats {
    STATS.with(std::cell::Cell::get)
}

/// Test-process-local mechanism comparison. Production has no policy switch.
#[cfg(test)]
pub(crate) struct CollapsePolicyGuard {
    previous: bool,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}

#[cfg(test)]
impl CollapsePolicyGuard {
    pub(crate) fn new(enabled: bool) -> Self {
        let previous = POLICY.with(|policy| policy.replace(enabled));
        Self {
            previous,
            _local: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
impl Drop for CollapsePolicyGuard {
    fn drop(&mut self) {
        POLICY.with(|policy| policy.set(self.previous));
    }
}

// The serialized leaf envelope is identical for every leaf at this depth. Its
// length lets us reject an oversized merge before cloning/moving any entries.
fn empty_leaf_bytes(depth: u16) -> Result<usize> {
    Ok(serde_json::to_vec(&Page::Leaf {
        version: 1,
        depth,
        entries: BTreeMap::new(),
    })?
    .len())
}

impl PagedIndex<'_> {
    pub(super) fn apply_bucketed(
        &self,
        root: Option<&str>,
        inserts: BTreeMap<String, Value>,
        removals: &BTreeSet<String>,
    ) -> Result<Option<String>> {
        debug_assert_eq!(self.order, KeyOrder::Bucketed);
        #[cfg(test)]
        observe(|stats| stats.calls += 1);
        let draft = self.update_draft(root, inserts, removals, 0, &[])?;
        #[cfg(test)]
        observe(|stats| {
            stats.max_final_draft_charge = stats
                .max_final_draft_charge
                .max(draft.as_ref().map_or(0, Draft::memory_charge))
        });
        draft.map(|draft| self.materialize_draft(draft)).transpose()
    }

    fn update_draft(
        &self,
        root: Option<&str>,
        inserts: BTreeMap<String, Value>,
        removals: &BTreeSet<String>,
        depth: u16,
        prefix: &[u8],
    ) -> Result<Option<Draft>> {
        let Some(hash) = root else {
            return if inserts.is_empty() {
                Ok(None)
            } else {
                self.leaf_draft(inserts, depth).map(Some)
            };
        };
        match Arc::unwrap_or_clone(self.read(hash, depth, prefix)?) {
            Page::Leaf { mut entries, .. } => {
                let mut changed = false;
                for key in removals {
                    // Insertions win. Skipping their removal also preserves an exact
                    // no-op when the insertion restores the original value.
                    if !inserts.contains_key(key) && entries.remove(key).is_some() {
                        changed = true;
                    }
                }
                for (key, value) in inserts {
                    if entries.get(&key) != Some(&value) {
                        entries.insert(key, value);
                        changed = true;
                    }
                }
                if entries.is_empty() {
                    return Ok(None);
                }
                if !changed {
                    return Ok(Some(Draft::Existing(hash.to_owned())));
                }
                self.leaf_draft(entries, depth).map(Some)
            }
            Page::Branch { children, .. } => {
                let mut children: BTreeMap<_, _> = children
                    .into_iter()
                    .map(|(slot, hash)| (slot, Draft::Existing(hash)))
                    .collect();
                let mut groups: BTreeMap<u16, (BTreeMap<String, Value>, BTreeSet<String>)> =
                    BTreeMap::new();
                for (key, value) in inserts {
                    groups
                        .entry(self.nibble(&key, depth)?)
                        .or_default()
                        .0
                        .insert(key, value);
                }
                for key in removals {
                    groups
                        .entry(self.nibble(key, depth)?)
                        .or_default()
                        .1
                        .insert(key.clone());
                }
                let mut changed = false;
                for (selected, (adds, removes)) in groups {
                    let slot = selected.to_string();
                    let previous = match children.get(&slot) {
                        Some(Draft::Existing(hash)) => Some(hash.as_str()),
                        None => None,
                        _ => unreachable!(),
                    };
                    let mut next_prefix = prefix.to_vec();
                    next_prefix.push(selected as u8);
                    let next =
                        self.update_draft(previous, adds, &removes, depth + 1, &next_prefix)?;
                    match next {
                        Some(Draft::Existing(ref hash)) if previous == Some(hash.as_str()) => (),
                        Some(next) => {
                            children.insert(slot, next);
                            changed = true;
                        }
                        None => {
                            changed |= children.remove(&slot).is_some();
                        }
                    }
                }
                if children.is_empty() {
                    return Ok(None);
                }
                if !changed {
                    return Ok(Some(Draft::Existing(hash.to_owned())));
                }
                self.collapse_draft(children, depth).map(Some)
            }
        }
    }

    fn leaf_draft(&self, entries: BTreeMap<String, Value>, depth: u16) -> Result<Draft> {
        let page = Page::Leaf {
            version: 1,
            depth,
            entries,
        };
        let bytes = serde_json::to_vec(&page)?.len();
        #[cfg(test)]
        observe(|stats| {
            stats.sized_bytes += bytes;
            stats.drafts += 1;
        });
        let Page::Leaf { entries, .. } = page else {
            unreachable!()
        };
        if bytes <= PAGE_BYTES {
            return Ok(Draft::Leaf {
                depth,
                entries,
                bytes,
            });
        }
        ensure!(
            depth < self.max_depth(),
            "index path exhausted before the page budget"
        );
        let mut groups: BTreeMap<u16, BTreeMap<String, Value>> = BTreeMap::new();
        for (key, value) in entries {
            groups
                .entry(self.nibble(&key, depth)?)
                .or_default()
                .insert(key, value);
        }
        let children = groups
            .into_iter()
            .map(|(slot, entries)| Ok((slot.to_string(), self.leaf_draft(entries, depth + 1)?)))
            .collect::<Result<_>>()?;
        Ok(Draft::Branch { depth, children })
    }

    fn collapse_draft(&self, children: BTreeMap<String, Draft>, depth: u16) -> Result<Draft> {
        #[cfg(test)]
        observe(|stats| {
            stats.merge_attempts += 1;
            stats.drafts += 1;
        });
        if children
            .values()
            .all(|node| matches!(node, Draft::Leaf { .. }))
        {
            let child_envelope = empty_leaf_bytes(depth + 1)?;
            let mut bytes = empty_leaf_bytes(depth)?;
            for node in children.values() {
                let Draft::Leaf {
                    bytes: child_bytes, ..
                } = node
                else {
                    unreachable!()
                };
                // Each nonempty child's entry map contributes its contents, and
                // joining N nonempty maps needs N-1 additional commas.
                bytes = bytes.saturating_add(child_bytes.saturating_sub(child_envelope));
            }
            bytes = bytes.saturating_add(children.len() - 1);
            if bytes <= PAGE_BYTES {
                let mut merged = BTreeMap::new();
                for node in children.into_values() {
                    let Draft::Leaf { entries, .. } = node else {
                        unreachable!()
                    };
                    for (key, value) in entries {
                        ensure!(
                            merged.insert(key, value).is_none(),
                            "duplicate index key across pages"
                        );
                    }
                }
                #[cfg(test)]
                observe(|stats| stats.merges += 1);
                return Ok(Draft::Leaf {
                    depth,
                    entries: merged,
                    bytes,
                });
            }
        }
        Ok(Draft::Branch { depth, children })
    }

    fn materialize_draft(&self, draft: Draft) -> Result<String> {
        match draft {
            Draft::Existing(hash) => Ok(hash),
            Draft::Leaf { depth, entries, .. } => self.write(&Page::Leaf {
                version: 1,
                depth,
                entries,
            }),
            Draft::Branch { depth, children } => {
                let children = children
                    .into_iter()
                    .map(|(slot, child)| Ok((slot, self.materialize_draft(child)?)))
                    .collect::<Result<_>>()?;
                self.write(&Page::Branch {
                    version: 1,
                    depth,
                    children,
                })
            }
        }
    }
}

#[cfg(test)]
impl Draft {
    fn memory_charge(&self) -> usize {
        let heap = match self {
            Draft::Existing(hash) => allocation_charge(hash.capacity()),
            Draft::Leaf { entries, .. } => entries.iter().fold(
                map_charge::<String, Value>(entries.len()),
                |total, (key, value)| {
                    total
                        .saturating_add(allocation_charge(key.capacity()))
                        .saturating_add(value_heap_charge(value))
                },
            ),
            Draft::Branch { children, .. } => children.iter().fold(
                map_charge::<String, Draft>(children.len()),
                |total, (slot, child)| {
                    total
                        .saturating_add(allocation_charge(slot.capacity()))
                        .saturating_add(child.memory_charge())
                },
            ),
        };
        std::mem::size_of::<Self>().saturating_add(heap)
    }
}

#[cfg(test)]
#[path = "paged_index_update_tests.rs"]
mod tests;
