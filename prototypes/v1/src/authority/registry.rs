use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Serialize};

use crate::namespace::Namespace;
use crate::object_store::ObjectStore;

use super::paged_index::PagedIndex;

/// A registry stays inline in the authority head while it fits this budget and is
/// spilled into an immutable page tree beyond it. The budget exists because the head
/// document is decoded, validated and re-encoded on every statement: volume state
/// (physical objects, logical objects, uploads, tombstones) must not drive that cost,
/// while small catalogs keep a self-contained head that older readers can inspect.
///
/// The mode is one-way and the two states are mutually exclusive: a registry with a
/// pointer never carries inline entries, so a lookup has exactly one authoritative
/// source and no merge semantics. Pages are content addressed and immutable, so a
/// crashed or CAS-lost publication only leaves unreferenced pages behind.
pub(crate) const MAX_INLINE_BYTES: usize = 16 * 1024;

/// Controlled experiment hook: raising the budget to `usize::MAX` keeps every registry
/// inline, which reproduces the monolithic head the paged design replaced. Only tests
/// touch it, so production behaviour is the constant above.
#[cfg(test)]
pub(crate) static INLINE_BUDGET: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(MAX_INLINE_BYTES);

fn inline_budget() -> usize {
    #[cfg(test)]
    {
        INLINE_BUDGET.load(std::sync::atomic::Ordering::Relaxed)
    }
    #[cfg(not(test))]
    {
        MAX_INLINE_BYTES
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Change<V> {
    Added(V),
    Removed,
}

impl<V> Change<V> {
    fn added(&self) -> Option<&V> {
        match self {
            Change::Added(value) => Some(value),
            Change::Removed => None,
        }
    }
}

/// One durable registry. `store`/`namespace` are attached by the authority after
/// decoding; a hand-decoded root (tests, migration fixtures) keeps them unset and can
/// only serve inline content.
#[derive(Clone)]
pub struct Registry<V> {
    store: Option<Arc<dyn ObjectStore>>,
    namespace: Option<Namespace>,
    pointer: Option<String>,
    inline: BTreeMap<String, V>,
    changes: BTreeMap<String, Change<V>>,
}

impl<V: std::fmt::Debug> std::fmt::Debug for Registry<V> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Registry")
            .field("pointer", &self.pointer)
            .field("inline", &self.inline.len())
            .field("changed", &self.changes.len())
            .finish()
    }
}

impl<V> Registry<V> {
    pub(crate) fn inline(inline: BTreeMap<String, V>) -> Self {
        Self {
            store: None,
            namespace: None,
            pointer: None,
            inline,
            changes: BTreeMap::new(),
        }
    }

    pub(crate) fn paged(pointer: Option<String>, inline: BTreeMap<String, V>) -> Self {
        Self {
            pointer,
            ..Self::inline(inline)
        }
    }

    pub(crate) fn pointer(&self) -> Option<&str> {
        self.pointer.as_deref()
    }

    /// Size of the inline window, not of the registry: head-size accounting and probes
    /// reason about the encoded document, which is exactly what this returns.
    pub(crate) fn window_len(&self) -> usize {
        self.inline.len()
    }

    /// Compare the durable base a transaction started from. A mutation may add
    /// `changes`, but replacing the base would hide removals from change-scoped
    /// validation, so the publish path requires this identity to be unchanged.
    pub(crate) fn same_base(&self, other: &Self) -> bool
    where
        V: PartialEq,
    {
        self.pointer == other.pointer && self.inline == other.inline
    }

    pub(crate) fn changes(&self) -> impl Iterator<Item = (&String, &Change<V>)> {
        self.changes.iter()
    }

    pub(crate) fn has_changes(&self) -> bool {
        !self.changes.is_empty()
    }

    /// Exact while inline. A paged registry cannot answer this without walking its
    /// pages, so it must fail loudly instead of reporting a plausible zero.
    pub fn len(&self) -> usize {
        self.expect_inline("length");
        self.inline.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pointer.is_none() && self.inline.is_empty()
    }

    /// Inline-window reads. A paged registry cannot answer these without store I/O, so
    /// they fail loudly instead of reporting an empty registry. Callers that may face a
    /// paged registry must use `entries()`, which walks the pages.
    fn expect_inline(&self, operation: &str) {
        assert!(
            self.pointer.is_none(),
            "paged registry {operation} requires entries()"
        );
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.expect_inline("membership");
        self.inline.contains_key(key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.expect_inline("key listing");
        self.inline.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.expect_inline("value listing");
        self.inline.values()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &V)> {
        self.expect_inline("iteration");
        self.inline.iter()
    }

    pub(crate) fn taken(&self, key: &str) -> bool {
        self.changes.contains_key(key)
    }

    pub(crate) fn added(&self, key: &str) -> Option<&V> {
        self.changes.get(key).and_then(Change::added)
    }

    pub(crate) fn attach(&mut self, store: Arc<dyn ObjectStore>, namespace: Namespace) {
        self.store = Some(store);
        self.namespace = Some(namespace);
    }

    fn index(&self) -> Result<PagedIndex<'_>> {
        Ok(PagedIndex::new(
            self.store
                .as_ref()
                .context("registry store handle missing")?
                .as_ref(),
            self.namespace
                .as_ref()
                .context("registry namespace missing")?,
        ))
    }

    /// Read the published state, ignoring this transaction's pending changes.
    fn published(&self, key: &str) -> Result<Option<V>>
    where
        V: Clone + DeserializeOwned,
    {
        match &self.pointer {
            Some(pointer) => {
                let stored = self.index()?.get(Some(pointer), key)?;
                stored
                    .map(|value| serde_json::from_value(value).map_err(Into::into))
                    .transpose()
            }
            None => Ok(self.inline.get(key).cloned()),
        }
    }

    fn published_entries(&self) -> Result<BTreeMap<String, V>>
    where
        V: Clone + DeserializeOwned,
    {
        match &self.pointer {
            Some(pointer) => self
                .index()?
                .entries(Some(pointer))?
                .into_iter()
                .map(|(key, value)| Ok((key, serde_json::from_value(value)?)))
                .collect(),
            None => Ok(self.inline.clone()),
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<V>>
    where
        V: Clone + DeserializeOwned,
    {
        if self.taken(key) {
            return Ok(self.added(key).cloned());
        }
        self.published(key)
    }

    pub fn contains(&self, key: &str) -> Result<bool>
    where
        V: Clone + DeserializeOwned,
    {
        Ok(self.get(key)?.is_some())
    }

    /// Full content including this transaction's pending changes. Walks every page
    /// for a paged registry, so maintenance paths only.
    pub fn entries(&self) -> Result<BTreeMap<String, V>>
    where
        V: Clone + DeserializeOwned,
    {
        let mut entries = self.published_entries()?;
        for (key, change) in &self.changes {
            match change {
                Change::Added(value) => {
                    entries.insert(key.clone(), value.clone());
                }
                Change::Removed => {
                    entries.remove(key);
                }
            }
        }
        Ok(entries)
    }

    /// Upsert. Which value changes are legal is a per-registry rule and lives in
    /// `catalog::prepare`: tombstones flip their `deleted` flag and pin entries grow, while
    /// physical and logical registries must keep their identity.
    pub fn insert(&mut self, key: String, value: V) -> Result<()>
    where
        V: PartialEq + Clone + DeserializeOwned,
    {
        if let Some(previous) = self.get(&key)? {
            if previous == value {
                return Ok(());
            }
        }
        self.changes.insert(key, Change::Added(value));
        Ok(())
    }

    pub fn remove(&mut self, key: &str) -> Result<()>
    where
        V: Clone + DeserializeOwned,
    {
        if self.get(key)?.is_some() {
            self.changes.insert(key.to_owned(), Change::Removed);
        }
        Ok(())
    }

    pub fn update(&mut self, key: &str, apply: impl FnOnce(&mut V) -> Result<()>) -> Result<()>
    where
        V: PartialEq + Clone + DeserializeOwned,
    {
        let mut value = self
            .get(key)?
            .with_context(|| format!("registry entry missing: {key}"))?;
        apply(&mut value)?;
        self.insert(key.to_owned(), value)
    }

    /// Apply this transaction's changes durably. Pages may be written even when the
    /// caller later loses the head CAS: they stay unreferenced, never half-published.
    pub(crate) fn publish(&mut self, index: &PagedIndex<'_>) -> Result<()>
    where
        V: PartialEq + Clone + Serialize + DeserializeOwned,
    {
        let changes = std::mem::take(&mut self.changes);
        if let Some(pointer) = self.pointer.take() {
            if changes.is_empty() {
                self.pointer = Some(pointer);
                return Ok(());
            }
            let mut inserts = BTreeMap::new();
            let mut removals = std::collections::BTreeSet::new();
            for (key, change) in changes {
                match change {
                    Change::Added(value) => {
                        inserts.insert(key, serde_json::to_value(value)?);
                    }
                    Change::Removed => {
                        removals.insert(key);
                    }
                }
            }
            self.pointer = index.apply(Some(&pointer), inserts, &removals)?;
            self.inline.clear();
            return Ok(());
        }
        for (key, change) in changes {
            match change {
                Change::Added(value) => {
                    self.inline.insert(key, value);
                }
                Change::Removed => {
                    self.inline.remove(&key);
                }
            }
        }
        // Legacy heads may already exceed the budget without any change: spill them so a
        // single migrated statement cannot keep carrying the payload.
        if self.inline_bytes()? <= inline_budget() {
            return Ok(());
        }
        let entries = self
            .inline
            .iter()
            .map(|(key, value)| Ok((key.clone(), serde_json::to_value(value)?)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        self.pointer = index.build(entries)?;
        self.inline.clear();
        Ok(())
    }

    fn inline_bytes(&self) -> Result<usize>
    where
        V: Serialize,
    {
        if self.inline.is_empty() {
            return Ok(0);
        }
        Ok(serde_json::to_vec(&self.inline)?.len())
    }

    pub(crate) fn into_parts(self) -> (Option<String>, BTreeMap<String, V>) {
        (self.pointer, self.inline)
    }
}

/// Indexing reads the inline window only; a paged registry fails loudly rather than
/// pretending the value is absent. Callers that may face pages use `get`.
impl<V> std::ops::Index<&str> for Registry<V> {
    type Output = V;

    fn index(&self, key: &str) -> &V {
        self.expect_inline("indexing");
        &self.inline[key]
    }
}

/// Identity is the durable state: pointer, inline window and unreleased changes. Guards
/// compare registries to refuse unauthorized mutations (`current.tombstones ==
/// candidate.tombstones`), so a transaction that changes a paged registry must compare
/// unequal even though its pages only change on publication.
impl<V: PartialEq> PartialEq for Registry<V> {
    fn eq(&self, other: &Self) -> bool {
        self.pointer == other.pointer
            && self.inline == other.inline
            && self.changes == other.changes
    }
}

impl<V: Eq> Eq for Registry<V> {}

/// Diagnostics must not present a paged registry as an empty one.
impl<V: Serialize> Serialize for Registry<V> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Registry", 2)?;
        state.serialize_field("pointer", &self.pointer)?;
        state.serialize_field("entries", &self.inline)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::MemoryObjectStore;
    use serde_json::{json, Value};

    fn attached(store: &Arc<dyn ObjectStore>, namespace: &Namespace) -> Registry<Value> {
        let mut registry = Registry::inline(BTreeMap::new());
        registry.attach(store.clone(), namespace.clone());
        registry
    }

    #[test]
    fn inline_registry_spills_once_and_stays_readable_from_pages() -> Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryObjectStore::default());
        let namespace = Namespace::new("registry", "test")?;
        let index = PagedIndex::new(store.as_ref(), &namespace);
        let mut registry = attached(&store, &namespace);
        for number in 0..2_000 {
            registry.insert(format!("key-{number:04}"), json!(number))?;
        }
        assert_eq!(registry.entries()?.len(), 2_000);
        registry.publish(&index)?;
        let pointer = registry
            .pointer()
            .context("registry did not spill")?
            .to_owned();
        assert!(
            registry.window_len() == 0,
            "paged registry kept inline state"
        );
        assert_eq!(registry.get("key-0042")?, Some(json!(42)));
        // Rebuild a handle that shares nothing with the writer: content comes from pages.
        drop(registry);
        let mut reopened = attached(&store, &namespace);
        reopened.pointer = Some(pointer);
        assert_eq!(reopened.get("key-0042")?, Some(json!(42)));
        assert_eq!(reopened.get("absent")?, None);
        assert_eq!(reopened.entries()?.len(), 2_000);
        Ok(())
    }

    #[test]
    fn removal_drops_pages_and_reports_missing_lookups() -> Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryObjectStore::default());
        let namespace = Namespace::new("registry", "removal")?;
        let index = PagedIndex::new(store.as_ref(), &namespace);
        let mut registry = attached(&store, &namespace);
        for number in 0..120 {
            registry.insert(format!("key-{number:04}"), json!(number))?;
        }
        registry.publish(&index)?;
        for number in 0..120 {
            registry.remove(&format!("key-{number:04}"))?;
        }
        registry.publish(&index)?;
        assert_eq!(registry.get("key-0000")?, None);
        assert!(registry.is_empty(), "emptied registry kept a page pointer");
        assert!(registry.entries()?.is_empty());
        // Removing an absent key stays a no-op instead of inventing page churn.
        registry.remove("key-0000")?;
        assert!(!registry.has_changes());
        Ok(())
    }

    #[test]
    fn equal_insert_is_free_and_changed_insert_is_recorded() -> Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryObjectStore::default());
        let namespace = Namespace::new("registry", "identity")?;
        let index = PagedIndex::new(store.as_ref(), &namespace);
        let mut registry = attached(&store, &namespace);
        for number in 0..80 {
            registry.insert(format!("key-{number:04}"), json!(number))?;
        }
        registry.publish(&index)?;
        assert!(registry.insert("key-0000".into(), json!(0)).is_ok());
        assert!(!registry.has_changes(), "equal insert recorded a change");
        // Mutable registries legitimately replace a value; `catalog::prepare` decides
        // which replacements are legal for which registry.
        assert!(registry.insert("key-0000".into(), json!(7)).is_ok());
        assert!(registry.has_changes());
        registry.publish(&index)?;
        assert_eq!(registry.get("key-0000")?, Some(json!(7)));
        assert!(registry.insert("absent".into(), json!(1)).is_ok());
        Ok(())
    }

    #[test]
    fn paged_registry_without_store_handle_fails_closed() {
        let registry: Registry<Value> = Registry::paged(Some("0".repeat(64)), BTreeMap::new());
        assert!(registry.get("key").is_err());
        assert!(registry.entries().is_err());
    }
}
