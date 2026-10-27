use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::request_reads;
use crate::namespace::Namespace;
use crate::object_store::{ObjectStore, PutCondition};

const PAGE_BYTES: usize = 16 * 1024;
const HASH_NIBBLES: u16 = 64;
/// Reader-pin keys are bucketed by a two-byte digest of the pin identity. A prefix scan
/// visits one bucket and filters its leaves, so its cost follows the entries of that pin
/// (plus negligible hash collisions), not every pin in the namespace. The bucket prefix is
/// deliberately short: using the 64-character pin itself as the trie path would create one
/// branch page per shared character and make both release and the full walk enormous.
const BUCKET_NIBBLES: u16 = 4;

/// How a page tree derives the nibble path from a registry key.
///
/// `Hashed` is the historical layout: keys are scattered by `blake3(key)`, which gives
/// uniform leaves but makes a key prefix useless for pruning. `Bucketed` is the reader-pin
/// layout: the first two digest bytes of the pin form a bucket path, and every key in a
/// bucket is scattered by its own digest. A pin's entries share only the bucket, so a
/// release reads the bucket pages plus the leaf pages that pin occupies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyOrder {
    Hashed,
    Bucketed,
}

#[cfg(test)]
thread_local! {
    /// Pages read through any `PagedIndex` on this thread. Tests use it to bound a whole
    /// release (prefix scan plus publication apply) without measuring wall time.
    static TEST_PAGE_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_test_page_reads() {
    TEST_PAGE_READS.with(|reads| reads.set(0));
}

#[cfg(test)]
pub(crate) fn test_page_reads() -> usize {
    TEST_PAGE_READS.with(std::cell::Cell::get)
}

/// Pages a publication rewrites are buffered here instead of being written one by one,
/// so the whole attempt reaches the store as one durability group: immutable
/// content-addressed page bodies first, the authority head CAS last.
#[derive(Default)]
pub(crate) struct PendingPages {
    writes: Vec<crate::object_store::GroupWrite>,
}

impl PendingPages {
    pub(crate) fn writes(&self) -> &[crate::object_store::GroupWrite] {
        &self.writes
    }

    /// A page this attempt has already staged. Validation of a candidate runs before the
    /// group is written, so a page it just created must be readable from the buffer;
    /// otherwise the authority would reject its own in-flight publication.
    fn page(&self, hash: &str) -> Option<Vec<u8>> {
        self.writes
            .iter()
            .find(|write| write.key.ends_with(hash))
            .map(|write| write.bytes.clone())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub(super) enum Page {
    Leaf {
        version: u8,
        depth: u16,
        entries: BTreeMap<String, Value>,
    },
    Branch {
        version: u8,
        depth: u16,
        children: BTreeMap<String, String>,
    },
}

// Charge a full internal BTree node per entry, even though ordinary nodes hold
// several entries. This deliberately overestimates sparse maps and includes
// allocator/node overhead rather than treating encoded JSON bytes as heap size.
fn map_charge<K, V>(entries: usize) -> usize {
    entries.saturating_mul(
        64usize
            .saturating_add(12 * std::mem::size_of::<usize>())
            .saturating_add(11 * (std::mem::size_of::<K>() + std::mem::size_of::<V>())),
    )
}

fn allocation_charge(capacity: usize) -> usize {
    capacity.saturating_add(if capacity == 0 { 0 } else { 64 })
}

fn value_heap_charge(value: &Value) -> usize {
    match value {
        Value::String(value) => allocation_charge(value.capacity()),
        Value::Array(values) => values.iter().fold(
            allocation_charge(
                values
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Value>()),
            ),
            |charge, value| charge.saturating_add(value_heap_charge(value)),
        ),
        Value::Object(values) => values.iter().fold(
            map_charge::<String, Value>(values.len()),
            |charge, (key, value)| {
                charge
                    .saturating_add(allocation_charge(key.capacity()))
                    .saturating_add(value_heap_charge(value))
            },
        ),
        // The locked serde_json configuration does not enable arbitrary_precision,
        // so Number has no separately allocated decimal representation.
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
    }
}

impl Page {
    pub(super) fn memory_charge(&self) -> usize {
        let heap = match self {
            Page::Leaf { entries, .. } => entries.iter().fold(
                map_charge::<String, Value>(entries.len()),
                |charge, (key, value)| {
                    charge
                        .saturating_add(allocation_charge(key.capacity()))
                        .saturating_add(value_heap_charge(value))
                },
            ),
            Page::Branch { children, .. } => children.iter().fold(
                map_charge::<String, String>(children.len()),
                |charge, (key, child)| {
                    charge
                        .saturating_add(allocation_charge(key.capacity()))
                        .saturating_add(allocation_charge(child.capacity()))
                },
            ),
        };
        std::mem::size_of::<Self>().saturating_add(heap)
    }
}

pub(crate) struct PagedIndex<'a> {
    store: &'a dyn ObjectStore,
    namespace: &'a Namespace,
    prefix: &'static str,
    order: KeyOrder,
    pending: Option<&'a std::cell::RefCell<PendingPages>>,
    /// Pages this handle read. Maintenance reports how much of the tree a bounded tick
    /// actually touched; the counter lives on the handle because completion paths create
    /// their own index and would otherwise pollute a process-wide total.
    reads: std::cell::Cell<usize>,
    /// A read-only handle over a durable archive the caller declared unreclaimable. Page
    /// reads must not extend a reader pin: GC never removes these pages.
    retained: bool,
}

impl<'a> PagedIndex<'a> {
    pub(crate) fn new(store: &'a dyn ObjectStore, namespace: &'a Namespace) -> Self {
        Self::with_prefix(store, namespace, "authority/index-pages")
    }

    /// Open a reader over an archive whose pages survive GC. Only a store that can serve a
    /// retained read may answer it; the handle is for lookups, not for writes.
    pub(crate) fn retained_with_prefix(
        store: &'a dyn ObjectStore,
        namespace: &'a Namespace,
        prefix: &'static str,
    ) -> Self {
        Self {
            retained: true,
            ..Self::with_prefix(store, namespace, prefix)
        }
    }

    /// Some trees live outside the authority's private namespace: the WAL receipt archive is
    /// written by the worker through the branch session, so its pages follow the branch key
    /// shape instead of the authority one.
    pub(crate) fn with_prefix(
        store: &'a dyn ObjectStore,
        namespace: &'a Namespace,
        prefix: &'static str,
    ) -> Self {
        Self::with_order(store, namespace, prefix, KeyOrder::Hashed)
    }

    pub(crate) fn with_order(
        store: &'a dyn ObjectStore,
        namespace: &'a Namespace,
        prefix: &'static str,
        order: KeyOrder,
    ) -> Self {
        Self {
            store,
            namespace,
            prefix,
            order,
            pending: None,
            reads: std::cell::Cell::new(0),
            retained: false,
        }
    }

    /// Route every page this index writes into `pack` instead of its own object.
    /// Buffer page bodies for one durability group instead of writing them immediately.
    pub(crate) fn deferring(
        &self,
        pending: &'a std::cell::RefCell<PendingPages>,
    ) -> PagedIndex<'a> {
        PagedIndex {
            store: self.store,
            namespace: self.namespace,
            prefix: self.prefix,
            order: self.order,
            pending: Some(pending),
            reads: std::cell::Cell::new(0),
            retained: self.retained,
        }
    }

    pub(crate) fn pages_read(&self) -> usize {
        self.reads.get()
    }

    /// Re-use this handle's store and pending group with another key order. A registry must
    /// publish its pages in the order it reads them; the transaction-wide handle is hashed
    /// because that is the layout every other registry uses.
    pub(crate) fn ordered(&self, order: KeyOrder) -> PagedIndex<'_> {
        PagedIndex {
            store: self.store,
            namespace: self.namespace,
            prefix: self.prefix,
            order,
            pending: self.pending,
            reads: std::cell::Cell::new(0),
            retained: self.retained,
        }
    }

    pub(crate) fn verify_root(&self, root: Option<&str>) -> Result<()> {
        if let Some(hash) = root {
            self.read(hash, 0, &[])?;
        }
        Ok(())
    }

    fn page_key(&self, hash: &str) -> Result<String> {
        validate_hash(hash)?;
        self.namespace.key(&format!("{}/{hash}", self.prefix))
    }

    fn max_depth(&self) -> u16 {
        match self.order {
            KeyOrder::Hashed => HASH_NIBBLES,
            KeyOrder::Bucketed => BUCKET_NIBBLES + HASH_NIBBLES,
        }
    }

    fn nibble(&self, key: &str, depth: u16) -> Result<u16> {
        ensure!(depth < self.max_depth(), "index path exhausted");
        let selected = match self.order {
            KeyOrder::Hashed => {
                let digest = blake3::hash(key.as_bytes());
                let byte = digest.as_bytes()[(depth / 2) as usize];
                if depth.is_multiple_of(2) {
                    byte >> 4
                } else {
                    byte & 15
                }
            }
            KeyOrder::Bucketed => {
                if depth < BUCKET_NIBBLES {
                    let (pin, _) = key.split_once('/').context("bucketed key without pin")?;
                    Self::bucket_nibble(pin, depth)?
                } else {
                    let tail = depth - BUCKET_NIBBLES;
                    let digest = blake3::hash(key.as_bytes());
                    let byte = digest.as_bytes()[(tail / 2) as usize];
                    if tail.is_multiple_of(2) {
                        byte >> 4
                    } else {
                        byte & 15
                    }
                }
            }
        };
        Ok(u16::from(selected))
    }

    fn bucket_nibble(pin: &str, depth: u16) -> Result<u8> {
        ensure!(depth < BUCKET_NIBBLES, "pin bucket path exhausted");
        let digest = blake3::hash(pin.as_bytes());
        let byte = digest.as_bytes()[(depth / 2) as usize];
        Ok(if depth.is_multiple_of(2) {
            byte >> 4
        } else {
            byte & 15
        })
    }

    pub(crate) fn get(&self, root: Option<&str>, key: &str) -> Result<Option<Value>> {
        validate_key(key)?;
        let mut current = root.map(str::to_owned);
        let mut prefix = Vec::new();
        while let Some(hash) = current {
            match self.read(&hash, prefix.len() as u16, &prefix)?.as_ref() {
                Page::Leaf { entries, .. } => return Ok(entries.get(key).cloned()),
                Page::Branch { children, .. } => {
                    let nibble = self.nibble(key, prefix.len() as u16)?;
                    prefix.push(nibble as u8);
                    current = children.get(&nibble.to_string()).cloned();
                }
            }
        }
        Ok(None)
    }

    pub(crate) fn insert_many(
        &self,
        root: Option<&str>,
        entries: BTreeMap<String, Value>,
    ) -> Result<String> {
        ensure!(!entries.is_empty(), "empty index update");
        for key in entries.keys() {
            validate_key(key)?;
        }
        self.insert_many_at(root, entries, 0, &[])
    }

    pub(crate) fn build(&self, entries: BTreeMap<String, Value>) -> Result<Option<String>> {
        for key in entries.keys() {
            validate_key(key)?;
        }
        if entries.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.leaf_or_branch(entries, 0)?))
        }
    }

    fn insert_many_at(
        &self,
        root: Option<&str>,
        updates: BTreeMap<String, Value>,
        depth: u16,
        prefix: &[u8],
    ) -> Result<String> {
        match root {
            None => self.leaf_or_branch(updates, depth),
            Some(hash) => match Arc::unwrap_or_clone(self.read(hash, depth, prefix)?) {
                Page::Leaf { mut entries, .. } => {
                    let mut changed = false;
                    for (key, value) in updates {
                        if let Some(previous) = entries.get(&key) {
                            ensure!(previous == &value, "index key identity conflict");
                        } else {
                            entries.insert(key, value);
                            changed = true;
                        }
                    }
                    if changed {
                        self.leaf_or_branch(entries, depth)
                    } else {
                        Ok(hash.to_owned())
                    }
                }
                Page::Branch { mut children, .. } => {
                    let mut groups: BTreeMap<u16, BTreeMap<String, Value>> = BTreeMap::new();
                    for (key, value) in updates {
                        groups
                            .entry(self.nibble(&key, depth)?)
                            .or_default()
                            .insert(key, value);
                    }
                    let mut changed = false;
                    for (selected, group) in groups {
                        let slot = selected.to_string();
                        let mut next_prefix = prefix.to_vec();
                        next_prefix.push(selected as u8);
                        let next = self.insert_many_at(
                            children.get(&slot).map(String::as_str),
                            group,
                            depth + 1,
                            &next_prefix,
                        )?;
                        if children.get(&slot) != Some(&next) {
                            children.insert(slot, next);
                            changed = true;
                        }
                    }
                    if changed {
                        self.write(&Page::Branch {
                            version: 1,
                            depth,
                            children,
                        })
                    } else {
                        Ok(hash.to_owned())
                    }
                }
            },
        }
    }

    /// Registry publication shares one immutable page tree, so a batch must apply
    /// additions and removals together: removing the last key of a page has to prune
    /// the child slot, otherwise an empty page (rejected by `read`) would be published.
    pub(crate) fn apply(
        &self,
        root: Option<&str>,
        inserts: BTreeMap<String, Value>,
        removals: &BTreeSet<String>,
    ) -> Result<Option<String>> {
        self.apply_at(root, inserts, removals, 0, &[])
    }

    fn apply_at(
        &self,
        root: Option<&str>,
        inserts: BTreeMap<String, Value>,
        removals: &BTreeSet<String>,
        depth: u16,
        prefix: &[u8],
    ) -> Result<Option<String>> {
        let Some(hash) = root else {
            if inserts.is_empty() {
                return Ok(None);
            }
            return Ok(Some(self.leaf_or_branch(inserts, depth)?));
        };
        match Arc::unwrap_or_clone(self.read(hash, depth, prefix)?) {
            Page::Leaf { mut entries, .. } => {
                let mut changed = false;
                for key in removals {
                    if entries.remove(key).is_some() {
                        changed = true;
                    }
                }
                for (key, value) in inserts {
                    match entries.get(&key) {
                        Some(previous) if previous == &value => (),
                        _ => {
                            entries.insert(key, value);
                            changed = true;
                        }
                    }
                }
                if entries.is_empty() {
                    return Ok(None);
                }
                if !changed {
                    return Ok(Some(hash.to_owned()));
                }
                Ok(Some(self.leaf_or_branch(entries, depth)?))
            }
            Page::Branch { mut children, .. } => {
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
                for (selected, (group_inserts, group_removals)) in groups {
                    let slot = selected.to_string();
                    let mut next_prefix = prefix.to_vec();
                    next_prefix.push(selected as u8);
                    let next = self.apply_at(
                        children.get(&slot).map(String::as_str),
                        group_inserts,
                        &group_removals,
                        depth + 1,
                        &next_prefix,
                    )?;
                    match next {
                        Some(next) if children.get(&slot) == Some(&next) => (),
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
                    return Ok(Some(hash.to_owned()));
                }
                Ok(Some(self.write(&Page::Branch {
                    version: 1,
                    depth,
                    children,
                })?))
            }
        }
    }

    /// Full page walk used by maintenance paths (garbage collection, deep verification,
    /// one-time inline spilling). Each access validates its page or reuses the same
    /// immutable page already verified for this exact path in the current request.
    pub(crate) fn entries(&self, root: Option<&str>) -> Result<BTreeMap<String, Value>> {
        let mut entries = BTreeMap::new();
        self.walk(root, 0, &[], &mut entries)?;
        Ok(entries)
    }

    /// Entries whose key starts with `prefix`, using the pin bucket to skip every branch
    /// that cannot contain the prefix. Returns the matching entries and the number of leaf
    /// entries the walk actually looked at. Only `KeyOrder::Bucketed` can prune; a hashed
    /// tree must fall back to a full walk, which the caller decides explicitly.
    pub(crate) fn entries_with_prefix(
        &self,
        root: Option<&str>,
        prefix: &str,
    ) -> Result<(BTreeMap<String, Value>, usize)> {
        ensure!(
            self.order == KeyOrder::Bucketed,
            "prefix scan requires a bucketed index"
        );
        ensure!(
            !prefix.is_empty()
                && prefix.len() <= 1024
                && prefix
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.~/".contains(&byte)),
            "invalid index prefix"
        );
        let mut entries = BTreeMap::new();
        let mut scanned = 0usize;
        self.walk_prefix(root, 0, &mut Vec::new(), prefix, &mut entries, &mut scanned)?;
        Ok((entries, scanned))
    }

    fn walk_prefix(
        &self,
        root: Option<&str>,
        depth: u16,
        path: &mut Vec<u8>,
        target: &str,
        entries: &mut BTreeMap<String, Value>,
        scanned: &mut usize,
    ) -> Result<()> {
        let Some(location) = root else {
            return Ok(());
        };
        let pin = target
            .split_once('/')
            .map(|(pin, _)| pin)
            .context("index prefix is not a reader pin")?;
        match self.read(location, depth, path)?.as_ref() {
            Page::Leaf { entries: page, .. } => {
                for (key, value) in page {
                    *scanned += 1;
                    if key.starts_with(target) {
                        ensure!(
                            entries.insert(key.clone(), value.clone()).is_none(),
                            "duplicate index key across pages"
                        );
                    }
                }
            }
            Page::Branch { children, .. } => {
                if depth < BUCKET_NIBBLES {
                    let slot = Self::bucket_nibble(pin, depth)?;
                    if let Some(child) = children.get(&slot.to_string()) {
                        path.push(slot);
                        self.walk_prefix(Some(child), depth + 1, path, target, entries, scanned)?;
                        path.pop();
                    }
                } else {
                    for (slot, child) in children {
                        let selected = slot.parse::<u8>()?;
                        path.push(selected);
                        self.walk_prefix(
                            Some(child.as_str()),
                            depth + 1,
                            path,
                            target,
                            entries,
                            scanned,
                        )?;
                        path.pop();
                    }
                }
            }
        }
        Ok(())
    }

    fn walk(
        &self,
        root: Option<&str>,
        depth: u16,
        prefix: &[u8],
        entries: &mut BTreeMap<String, Value>,
    ) -> Result<()> {
        let Some(location) = root else {
            return Ok(());
        };
        match self.read(location, depth, prefix)?.as_ref() {
            Page::Leaf { entries: page, .. } => {
                for (key, value) in page {
                    ensure!(
                        entries.insert(key.clone(), value.clone()).is_none(),
                        "duplicate index key across pages"
                    );
                }
            }
            Page::Branch { children, .. } => {
                for (slot, child) in children {
                    let selected = slot.parse::<u8>()?;
                    let mut next_prefix = prefix.to_vec();
                    next_prefix.push(selected);
                    self.walk(Some(child), depth + 1, &next_prefix, entries)?;
                }
            }
        }
        Ok(())
    }

    fn leaf_or_branch(&self, entries: BTreeMap<String, Value>, depth: u16) -> Result<String> {
        let leaf = Page::Leaf {
            version: 1,
            depth,
            entries,
        };
        if serde_json::to_vec(&leaf)?.len() <= PAGE_BYTES {
            return self.write(&leaf);
        }
        ensure!(
            depth < self.max_depth(),
            "index path exhausted before the page budget"
        );
        let mut groups: BTreeMap<u8, BTreeMap<String, Value>> = BTreeMap::new();
        let Page::Leaf { entries, .. } = leaf else {
            unreachable!()
        };
        for (key, value) in entries {
            groups
                .entry(self.nibble(&key, depth)? as u8)
                .or_default()
                .insert(key, value);
        }
        let mut children = BTreeMap::new();
        for (selected, group) in groups {
            children.insert(selected.to_string(), self.leaf_or_branch(group, depth + 1)?);
        }
        self.write(&Page::Branch {
            version: 1,
            depth,
            children,
        })
    }

    fn read(&self, hash: &str, depth: u16, prefix: &[u8]) -> Result<Arc<Page>> {
        self.reads.set(self.reads.get() + 1);
        #[cfg(test)]
        TEST_PAGE_READS.with(|reads| reads.set(reads.get() + 1));
        let key = self.page_key(hash)?;
        let backend = self.store as *const dyn ObjectStore as *const () as usize;
        let buffered = self.pending.and_then(|pending| pending.borrow().page(hash));
        crate::work_metrics::observe_index_page_read();
        // Pending bytes take precedence even when a durable page with the same key was
        // cached earlier. They must pass the original digest and path checks themselves.
        let cacheable = buffered.is_none();
        if cacheable {
            if let Some(page) = request_reads::parsed_page(backend, &key, self.order, depth, prefix)
            {
                return Ok(page);
            }
        }
        let bytes = match buffered {
            Some(bytes) => bytes,
            None => match request_reads::get_page(backend, &key) {
                Some(bytes) => bytes,
                None => {
                    let object = if self.retained {
                        self.store.get_retained(&key)?
                    } else {
                        self.store.get(&key)?
                    };
                    let bytes = object.context("published index page missing")?.bytes;
                    request_reads::remember_page(backend, key.clone(), &bytes);
                    bytes
                }
            },
        };
        ensure!(
            bytes.len() <= PAGE_BYTES && blake3::hash(&bytes).to_hex().as_str() == hash,
            "index page size or digest mismatch"
        );
        request_reads::observe_page_decode(bytes.len());
        let page: Page = serde_json::from_slice(&bytes)?;
        match &page {
            Page::Leaf {
                version,
                depth: stored,
                entries,
            } => {
                ensure!(
                    *version == 1 && *stored == depth && !entries.is_empty(),
                    "invalid index leaf"
                );
                for key in entries.keys() {
                    validate_key(key)?;
                    ensure!(
                        prefix.iter().enumerate().all(|(position, expected)| {
                            self.nibble(key, position as u16)
                                .is_ok_and(|actual| actual == u16::from(*expected))
                        }),
                        "index leaf outside selected path"
                    );
                }
                request_reads::observe_leaf_validation(entries.len(), prefix.len());
            }
            Page::Branch {
                version,
                depth: stored,
                children,
            } => {
                ensure!(
                    *version == 1
                        && *stored == depth
                        && depth < self.max_depth()
                        && !children.is_empty()
                        && children.len() <= 16,
                    "invalid index branch"
                );
                for (selected, child) in children {
                    let slot = selected.parse::<u8>()?;
                    ensure!(
                        slot < 16 && selected == &slot.to_string(),
                        "invalid index branch slot"
                    );
                    validate_hash(child)?;
                }
            }
        }
        let page = Arc::new(page);
        if cacheable {
            request_reads::remember_parsed_page(
                backend,
                key,
                self.order,
                depth,
                prefix,
                Arc::clone(&page),
            );
        }
        Ok(page)
    }

    fn write(&self, page: &Page) -> Result<String> {
        let bytes = serde_json::to_vec(page)?;
        ensure!(bytes.len() <= PAGE_BYTES, "index page byte budget exceeded");
        let hash = blake3::hash(&bytes).to_hex().to_string();
        let key = self.page_key(&hash)?;
        crate::work_metrics::observe_index_page_write(bytes.len());
        if let Some(pending) = self.pending {
            // Content-addressed bodies are idempotent, so they carry no condition: the
            // group stays condition-checked exactly once, by the head CAS that ends it.
            let mut pending = pending.borrow_mut();
            pending.writes.push(crate::object_store::GroupWrite::new(
                key,
                bytes,
                PutCondition::Any,
            ));
            return Ok(hash);
        }
        let backend = self.store as *const dyn ObjectStore as *const () as usize;
        if self.store.put(&key, &bytes, PutCondition::Absent).is_err() {
            let existing = self
                .store
                .get(&key)?
                .context("index page upload unresolved")?;
            ensure!(existing.bytes == bytes, "immutable index page conflict");
        }
        request_reads::remember_page(backend, key, &bytes);
        Ok(hash)
    }
}

#[cfg(test)]
#[path = "paged_index_cache_tests.rs"]
mod cache_tests;

/// Page keys are registry keys, so they follow the object-key shape (URL-safe path
/// components) rather than a flat hash alphabet: logical catalog keys such as
/// `segments/<hash>` and `wal/chunks/<hash>` are paged as-is.
fn validate_key(key: &str) -> Result<()> {
    ensure!(
        !key.is_empty() && key.len() <= 1024,
        "invalid index key length"
    );
    for component in key.split('/') {
        ensure!(
            !component.is_empty() && component != "." && component != "..",
            "invalid index key component"
        );
        ensure!(
            component
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte)),
            "index keys must be ASCII URL-safe paths"
        );
    }
    Ok(())
}

fn validate_hash(hash: &str) -> Result<()> {
    ensure!(
        hash.len() == 64
            && hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "invalid index page identity"
    );
    Ok(())
}

fn nibble(key: &str, depth: u16) -> Result<u16> {
    ensure!(depth < HASH_NIBBLES, "index path exhausted");
    let digest = blake3::hash(key.as_bytes());
    let byte = digest.as_bytes()[(depth / 2) as usize];
    Ok(u16::from(if depth.is_multiple_of(2) {
        byte >> 4
    } else {
        byte & 15
    }))
}

/// Walks every page reachable from `root`, validating content addressing and page shape.
///
/// Reachability is a whole-tree property: GC and publication validation must see pages a
/// key lookup would never touch. `leaf` receives every leaf's entries so a caller can
/// validate the values it stores; callers that only pin pages ignore them.
pub(crate) fn walk_tree_with(
    root: Option<&str>,
    mut read: impl FnMut(&str) -> Result<Vec<u8>>,
    mut leaf: impl FnMut(&BTreeMap<String, Value>) -> Result<()>,
) -> Result<()> {
    fn walk(
        hash: &str,
        depth: u16,
        prefix: &[u8],
        read: &mut impl FnMut(&str) -> Result<Vec<u8>>,
        leaf: &mut impl FnMut(&BTreeMap<String, Value>) -> Result<()>,
    ) -> Result<()> {
        validate_hash(hash)?;
        let bytes = read(hash)?;
        ensure!(
            bytes.len() <= PAGE_BYTES && blake3::hash(&bytes).to_hex().as_str() == hash,
            "index page size or digest mismatch"
        );
        match serde_json::from_slice(&bytes)? {
            Page::Leaf {
                version,
                depth: stored,
                entries,
            } => {
                ensure!(
                    version == 1 && stored == depth && !entries.is_empty(),
                    "invalid index leaf"
                );
                for key in entries.keys() {
                    validate_key(key)?;
                    ensure!(
                        prefix.iter().enumerate().all(|(position, expected)| {
                            nibble(key, position as u16)
                                .is_ok_and(|actual| actual == u16::from(*expected))
                        }),
                        "index leaf outside selected path"
                    );
                }
                leaf(&entries)?;
            }
            Page::Branch {
                version,
                depth: stored,
                children,
            } => {
                ensure!(
                    version == 1
                        && stored == depth
                        && depth < HASH_NIBBLES
                        && !children.is_empty()
                        && children.len() <= 16,
                    "invalid index branch"
                );
                for (selected, child) in &children {
                    let slot = selected.parse::<u8>()?;
                    ensure!(
                        slot < 16 && selected == &slot.to_string(),
                        "invalid index branch slot"
                    );
                    let mut next = prefix.to_vec();
                    next.push(slot);
                    walk(child, depth + 1, &next, read, leaf)?;
                }
            }
        }
        Ok(())
    }
    if let Some(root) = root {
        walk(root, 0, &[], &mut read, &mut leaf)?;
    }
    Ok(())
}

pub(crate) fn walk_tree(
    root: Option<&str>,
    read: impl FnMut(&str) -> Result<Vec<u8>>,
) -> Result<()> {
    walk_tree_with(root, read, |_| Ok(()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::{LocalObjectStore, MemoryObjectStore};
    use serde_json::json;

    fn namespace() -> Namespace {
        Namespace::new("index", "test").unwrap()
    }

    #[test]
    fn many_receipts_keep_every_page_bounded_and_lookup_independent() -> Result<()> {
        let store = MemoryObjectStore::default();
        let namespace = namespace();
        let index = PagedIndex::new(&store, &namespace);
        let mut root = None;
        for sequence in 0..1_000 {
            root = Some(index.insert_many(
                root.as_deref(),
                BTreeMap::from([(format!("receipt-{sequence:05}"), json!(sequence))]),
            )?);
        }
        for sequence in 0..1_000 {
            assert_eq!(
                index.get(root.as_deref(), &format!("receipt-{sequence:05}"))?,
                Some(json!(sequence))
            );
        }
        assert_eq!(index.get(root.as_deref(), "absent")?, None);
        let prefix = namespace.key("authority/index-pages")?;
        let pages = store.list(&prefix)?;
        assert!(pages.len() > 1);
        for page in pages {
            assert!(store.get(&page)?.unwrap().bytes.len() <= PAGE_BYTES);
        }
        Ok(())
    }

    #[test]
    fn legacy_bulk_build_writes_pages_once_and_preserves_all_receipts() -> Result<()> {
        let store = MemoryObjectStore::default();
        let namespace = namespace();
        let index = PagedIndex::new(&store, &namespace);
        let entries: BTreeMap<_, _> = (0..1_000)
            .map(|sequence| (format!("legacy-{sequence:05}"), json!(sequence)))
            .collect();
        let root = index.build(entries)?.unwrap();
        for sequence in 0..1_000 {
            assert_eq!(
                index.get(Some(&root), &format!("legacy-{sequence:05}"))?,
                Some(json!(sequence))
            );
        }
        let pages = store.list(&namespace.key("authority/index-pages")?)?;
        assert!(
            pages.len() <= 40,
            "bulk migration rewrote pages per receipt"
        );
        Ok(())
    }

    #[test]
    fn archive_batch_rewrites_each_affected_page_once() -> Result<()> {
        let store = MemoryObjectStore::default();
        let namespace = namespace();
        let index = PagedIndex::new(&store, &namespace);
        let root = index
            .build(
                (0..1_000)
                    .map(|sequence| (format!("receipt-{sequence:05}"), json!(sequence)))
                    .collect(),
            )?
            .unwrap();
        let prefix = namespace.key("authority/index-pages")?;
        let before = store.list(&prefix)?.len();
        let updated = index.insert_many(
            Some(&root),
            (1_000..1_064)
                .map(|sequence| (format!("receipt-{sequence:05}"), json!(sequence)))
                .collect(),
        )?;
        assert!(store.list(&prefix)?.len() <= before + 32);
        assert_eq!(index.get(Some(&root), "receipt-01000")?, None);
        for sequence in 0..1_064 {
            assert_eq!(
                index.get(Some(&updated), &format!("receipt-{sequence:05}"))?,
                Some(json!(sequence))
            );
        }
        assert_eq!(
            index.insert_many(
                Some(&updated),
                BTreeMap::from([("receipt-01000".into(), json!(1000))])
            )?,
            updated
        );
        assert!(index
            .insert_many(
                Some(&updated),
                BTreeMap::from([("receipt-01000".into(), json!(0))])
            )
            .is_err());
        Ok(())
    }

    #[test]
    fn unpublished_and_corrupt_pages_never_become_valid_receipts() -> Result<()> {
        let store = MemoryObjectStore::default();
        let namespace = namespace();
        let index = PagedIndex::new(&store, &namespace);
        let published = index.insert_many(None, BTreeMap::from([("first".into(), json!(1))]))?;
        let pending = index.insert_many(
            Some(&published),
            BTreeMap::from([("second".into(), json!(2))]),
        )?;
        assert_eq!(index.get(Some(&published), "second")?, None);
        assert_eq!(index.get(Some(&pending), "second")?, Some(json!(2)));
        assert_eq!(
            index.insert_many(
                Some(&published),
                BTreeMap::from([("first".into(), json!(1))])
            )?,
            published
        );
        assert!(index
            .insert_many(
                Some(&published),
                BTreeMap::from([("first".into(), json!(3))])
            )
            .is_err());
        let key = namespace.key(&format!("authority/index-pages/{pending}"))?;
        store.put(&key, b"corrupt", PutCondition::Any)?;
        assert!(index.get(Some(&pending), "second").is_err());
        assert!(index
            .insert_many(Some(&pending), BTreeMap::from([("third".into(), json!(3))]))
            .is_err());
        Ok(())
    }

    #[test]
    fn single_oversized_receipt_is_rejected_before_publication() -> Result<()> {
        let store = MemoryObjectStore::default();
        let namespace = namespace();
        let index = PagedIndex::new(&store, &namespace);
        let value = json!("x".repeat(PAGE_BYTES));
        assert!(index
            .insert_many(None, BTreeMap::from([("oversized".into(), value)]))
            .is_err());
        assert!(store
            .list(&namespace.key("authority/index-pages")?)?
            .is_empty());
        Ok(())
    }

    #[test]
    fn batch_apply_removes_prunes_and_rebuilds_from_published_pages() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let namespace = namespace();
        let store = LocalObjectStore::open(directory.path())?;
        let index = PagedIndex::new(&store, &namespace);
        let root = index
            .build(
                (0..200)
                    .map(|sequence| (format!("key-{sequence:05}"), json!(sequence)))
                    .collect(),
            )?
            .unwrap();
        let removals: BTreeSet<_> = (0..200)
            .filter(|sequence| sequence % 2 == 0)
            .map(|sequence| format!("key-{sequence:05}"))
            .collect();
        let applied = index
            .apply(
                Some(&root),
                BTreeMap::from([("key-00201".into(), json!(201))]),
                &removals,
            )?
            .unwrap();
        assert_eq!(index.get(Some(&applied), "key-00201")?, Some(json!(201)));
        assert_eq!(index.get(Some(&applied), "key-00001")?, Some(json!(1)));
        for sequence in removals.iter() {
            assert_eq!(index.get(Some(&applied), sequence)?, None);
        }
        let all = index.entries(Some(&applied))?;
        assert_eq!(all.len(), 101);
        assert_eq!(index.entries(Some(&root))?.len(), 200);
        // Every page stays inside the budget and the removed leaf keeps no empty slot.
        for page in store.list(&namespace.key("authority/index-pages")?)? {
            assert!(store.get(&page)?.unwrap().bytes.len() <= PAGE_BYTES);
        }
        let emptied = index.apply(
            Some(&applied),
            BTreeMap::new(),
            &all.keys().cloned().collect(),
        )?;
        assert_eq!(emptied, None);
        assert_eq!(index.entries(None)?.len(), 0);
        // A crashed publication writes pages but never the pointer: the old root survives.
        drop(store);
        let reopened = LocalObjectStore::open(directory.path())?;
        let index = PagedIndex::new(&reopened, &namespace);
        assert_eq!(index.entries(Some(&applied))?.len(), 101);
        let key = namespace.key(&format!("authority/index-pages/{applied}"))?;
        assert!(reopened.get(&key)?.is_some());
        Ok(())
    }

    #[test]
    fn apply_upserts_mutable_registry_values_and_keeps_absent_removals_free() -> Result<()> {
        let store = MemoryObjectStore::default();
        let namespace = namespace();
        let index = PagedIndex::new(&store, &namespace);
        let root = index.build(BTreeMap::from([("key".into(), json!(1))]))?;
        // Registry values may change (tombstones flip `deleted`), so a batch apply is an
        // upsert; receipt archival keeps the stricter `insert_many` identity check.
        let updated = index
            .apply(
                root.as_deref(),
                BTreeMap::from([("key".into(), json!(2))]),
                &BTreeSet::new(),
            )?
            .context("update dropped the index")?;
        assert_eq!(index.get(Some(&updated), "key")?, Some(json!(2)));
        assert_eq!(index.entries(root.as_deref())?.len(), 1);
        assert_eq!(index.get(root.as_deref(), "key")?, Some(json!(1)));
        assert!(index
            .insert_many(root.as_deref(), BTreeMap::from([("key".into(), json!(2))]))
            .is_err());
        let removed = index.apply(
            root.as_deref(),
            BTreeMap::new(),
            &BTreeSet::from(["key".into()]),
        )?;
        assert_eq!(removed, None);
        let absent = index.apply(
            None,
            BTreeMap::new(),
            &BTreeSet::from(["key".into(), "other".into()]),
        )?;
        assert_eq!(absent, None);
        Ok(())
    }
}
