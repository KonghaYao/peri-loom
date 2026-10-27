use std::cell::RefCell;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::{Arc, OnceLock};

use super::paged_index::{KeyOrder, Page};
use super::ObjectEntry;

// One publication re-reads the trie paths it is about to rewrite: at registry sizes the
// contract exercises that is a few hundred page reads. A cap below that working set turned
// every repeat into a store read (the request trace showed >1000 rejections per
// publication), so keep enough room for the whole request while still bounding a lane.
const MAX_BYTES: usize = 4 * 1024 * 1024;
const MAX_ENTRIES: usize = 1024;
const MAX_PARSED_BYTES: usize = 4 * 1024 * 1024;
const MAX_PARSED_ENTRIES: usize = 1024;

struct ParsedPage {
    page: Arc<Page>,
    order: KeyOrder,
    depth: u16,
    prefix: Vec<u8>,
}

fn trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("PERI_REQUEST_READ_TRACE").as_deref() == Ok("1"))
}

#[derive(Default)]
struct Reads {
    entries: BTreeMap<(usize, String), (ObjectEntry, Vec<u8>)>,
    pages: BTreeMap<(usize, String), Vec<u8>>,
    parsed: BTreeMap<(usize, String), ParsedPage>,
    parsed_bytes: usize,
    parsed_hits: u64,
    parsed_misses: u64,
    parsed_rejected: u64,
    bytes: usize,
    hits: u64,
    misses: u64,
    rejected: u64,
    decoded_pages: u64,
    decoded_bytes: u64,
    validated_leaf_keys: u64,
    validated_path_checks: u64,
}

thread_local! {
    static CURRENT: RefCell<Option<Reads>> = const { RefCell::new(None) };
}

pub(crate) struct Scope {
    previous: Option<Reads>,
    thread: PhantomData<Rc<()>>,
    request_id: Option<String>,
    started: std::time::Instant,
}

impl Scope {
    pub(crate) fn enter() -> Option<Self> {
        CURRENT
            .with(|current| current.borrow().is_none())
            .then(Self::new)
    }

    pub(crate) fn new() -> Self {
        Self {
            previous: CURRENT.with(|current| current.replace(Some(Reads::default()))),
            thread: PhantomData,
            request_id: None,
            started: std::time::Instant::now(),
        }
    }

    pub(crate) fn for_rpc(request_id: &str) -> Self {
        let mut scope = Self::new();
        scope.request_id = Some(request_id.to_owned());
        scope
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        let retired = CURRENT.with(|current| current.replace(self.previous.take()));
        if trace_enabled() {
            if let Some(reads) = retired {
                eprintln!(
                    concat!(
                        "NATIVE_REQUEST_READS request_id={} elapsed_us={} hits={} misses={} ",
                        "rejected={} entries={} bytes={} decoded_pages={} decoded_bytes={} ",
                        "validated_leaf_keys={} validated_path_checks={} parsed_hits={} ",
                        "parsed_misses={} parsed_rejected={} parsed_entries={} parsed_bytes={}"
                    ),
                    self.request_id.as_deref().unwrap_or("local"),
                    self.started.elapsed().as_micros(),
                    reads.hits,
                    reads.misses,
                    reads.rejected,
                    reads.entries.len() + reads.pages.len(),
                    reads.bytes,
                    reads.decoded_pages,
                    reads.decoded_bytes,
                    reads.validated_leaf_keys,
                    reads.validated_path_checks,
                    reads.parsed_hits,
                    reads.parsed_misses,
                    reads.parsed_rejected,
                    reads.parsed.len(),
                    reads.parsed_bytes
                );
            }
        }
    }
}

/// A content-addressed page is reusable only in the exact access context that was
/// validated. A different order, depth or path must go through the ordinary reader.
pub(super) fn parsed_page(
    backend: usize,
    key: &str,
    order: KeyOrder,
    depth: u16,
    prefix: &[u8],
) -> Option<Arc<Page>> {
    CURRENT.with(|current| {
        let mut current = current.borrow_mut();
        let reads = current.as_mut()?;
        let page = reads
            .parsed
            .get(&(backend, key.to_owned()))
            .filter(|cached| {
                cached.order == order && cached.depth == depth && cached.prefix == prefix
            })
            .map(|cached| Arc::clone(&cached.page));
        if page.is_some() {
            reads.parsed_hits += 1;
        } else {
            reads.parsed_misses += 1;
        }
        page
    })
}

/// This budget is independent of the raw-byte cache: decoded JSON can be much larger
/// than its encoding. `memory_charge` recursively charges allocated containers; the
/// additional allowance covers a full map node, Arc and the access-context fields.
pub(super) fn remember_parsed_page(
    backend: usize,
    key: String,
    order: KeyOrder,
    depth: u16,
    prefix: &[u8],
    page: Arc<Page>,
) {
    CURRENT.with(|current| {
        let mut current = current.borrow_mut();
        let Some(reads) = current.as_mut() else {
            return;
        };
        let charge = page
            .memory_charge()
            .saturating_add(key.capacity())
            .saturating_add(prefix.len())
            .saturating_add(2048);
        if reads.parsed.len() >= MAX_PARSED_ENTRIES
            || charge > MAX_PARSED_BYTES.saturating_sub(reads.parsed_bytes)
            || reads.parsed.contains_key(&(backend, key.clone()))
        {
            reads.parsed_rejected += 1;
            return;
        }
        reads.parsed_bytes += charge;
        reads.parsed.insert(
            (backend, key),
            ParsedPage {
                page,
                order,
                depth,
                prefix: prefix.to_vec(),
            },
        );
    });
}

#[cfg(test)]
pub(super) fn test_parsed_usage() -> (usize, usize, u64) {
    CURRENT.with(|current| {
        let current = current.borrow();
        let reads = current.as_ref().unwrap();
        (
            reads.parsed.len(),
            reads.parsed_bytes,
            reads.parsed_rejected,
        )
    })
}

/// Raw-byte cache hits still decode and validate a page. These diagnostic counts
/// distinguish repeated CPU work from physical reads without adding per-page clocks.
pub(super) fn observe_page_decode(bytes: usize) {
    if !trace_enabled() {
        return;
    }
    CURRENT.with(|current| {
        if let Some(reads) = current.borrow_mut().as_mut() {
            reads.decoded_pages = reads.decoded_pages.saturating_add(1);
            reads.decoded_bytes = reads.decoded_bytes.saturating_add(bytes as u64);
        }
    });
}

pub(super) fn observe_leaf_validation(keys: usize, path_length: usize) {
    if !trace_enabled() {
        return;
    }
    CURRENT.with(|current| {
        if let Some(reads) = current.borrow_mut().as_mut() {
            reads.validated_leaf_keys = reads.validated_leaf_keys.saturating_add(keys as u64);
            reads.validated_path_checks = reads
                .validated_path_checks
                .saturating_add((keys as u64).saturating_mul(path_length as u64));
        }
    });
}

pub(super) fn get(backend: usize, key: &str, entry: &ObjectEntry) -> Option<Vec<u8>> {
    CURRENT.with(|current| {
        let mut current = current.borrow_mut();
        let reads = current.as_mut()?;
        let result = reads
            .entries
            .get(&(backend, key.to_owned()))
            .and_then(|(stored, bytes)| (stored == entry).then(|| bytes.clone()));
        if result.is_some() {
            reads.hits += 1;
        } else {
            reads.misses += 1;
        }
        result
    })
}

pub(super) fn remember(backend: usize, key: String, entry: &ObjectEntry, bytes: &[u8]) {
    CURRENT.with(|current| {
        let mut current = current.borrow_mut();
        let Some(reads) = current.as_mut() else {
            return;
        };
        if reads.entries.len() + reads.pages.len() >= MAX_ENTRIES
            || bytes.len() > MAX_BYTES.saturating_sub(reads.bytes)
            || reads.entries.contains_key(&(backend, key.clone()))
        {
            reads.rejected += 1;
            return;
        }
        reads.bytes += bytes.len();
        reads
            .entries
            .insert((backend, key), (entry.clone(), bytes.to_vec()));
    });
}

pub(super) fn get_page(backend: usize, key: &str) -> Option<Vec<u8>> {
    CURRENT.with(|current| {
        let mut current = current.borrow_mut();
        let reads = current.as_mut()?;
        let result = reads.pages.get(&(backend, key.to_owned())).cloned();
        if result.is_some() {
            reads.hits += 1;
        } else {
            reads.misses += 1;
        }
        result
    })
}

pub(super) fn remember_page(backend: usize, key: String, bytes: &[u8]) {
    CURRENT.with(|current| {
        let mut current = current.borrow_mut();
        let Some(reads) = current.as_mut() else {
            return;
        };
        if reads.entries.len() + reads.pages.len() >= MAX_ENTRIES
            || bytes.len() > MAX_BYTES.saturating_sub(reads.bytes)
            || reads.pages.contains_key(&(backend, key.clone()))
        {
            reads.rejected += 1;
            return;
        }
        reads.bytes += bytes.len();
        reads.pages.insert((backend, key), bytes.to_vec());
    });
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    use anyhow::{ensure, Result};

    use super::*;
    use crate::authority::{Authority, CatalogRoot};
    use crate::namespace::Namespace;
    use crate::object_store::{LocalObjectStore, ObjectStore, PutCondition, StoredObject};
    use crate::ownership::types::{MutationGuard, MutationStage};

    struct CountingStore {
        local: LocalObjectStore,
        reads: AtomicUsize,
    }

    impl ObjectStore for CountingStore {
        fn get(&self, key: &str) -> Result<Option<StoredObject>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.local.get(key)
        }

        fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
            self.local.put(key, bytes, condition)
        }

        fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
            self.local.delete(key, expected)
        }

        fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.local.list(prefix)
        }
    }

    struct Guard(AtomicBool);

    impl MutationGuard for Guard {
        fn validate(&self, _: MutationStage) -> Result<()> {
            ensure!(!self.0.load(Ordering::Acquire), "old binding fenced");
            Ok(())
        }

        fn validate_root(&self, _: &CatalogRoot, _: &CatalogRoot) -> Result<()> {
            self.validate(MutationStage::RootPublish)
        }

        fn uncertain(&self, _: MutationStage) {}
    }

    #[test]
    fn metadata_reuse_is_request_local_but_root_direct_reads_and_fences_stay_live() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(CountingStore {
            local: LocalObjectStore::open(directory.path())?,
            reads: AtomicUsize::new(0),
        });
        let authority = Authority::open(store.clone(), Namespace::new("tenant", "database")?)?;
        let entry = authority.upload(b"immutable metadata")?;
        let guard = Arc::new(Guard(AtomicBool::new(false)));
        let scoped = authority.scoped(guard.clone());
        {
            let _request = Scope::new();
            let before = store.reads.load(Ordering::Relaxed);
            assert_eq!(scoped.read_metadata_blob(&entry)?, b"immutable metadata");
            assert_eq!(scoped.read_metadata_blob(&entry)?, b"immutable metadata");
            assert_eq!(store.reads.load(Ordering::Relaxed), before + 1);
            scoped.snapshot()?;
            scoped.snapshot()?;
            assert_eq!(store.reads.load(Ordering::Relaxed), before + 3);
            guard.0.store(true, Ordering::Release);
            assert!(scoped.read_metadata_blob(&entry).is_err());
            guard.0.store(false, Ordering::Release);
            let mut changed = entry.clone();
            changed.hash = blake3::hash(b"different").to_hex().to_string();
            assert!(scoped.read_metadata_blob(&changed).is_err());
            store.put(
                &authority.physical_key(&entry.blob)?,
                b"corrupt",
                PutCondition::Any,
            )?;
            assert!(scoped.read_blob(&entry).is_err());
        }
        let _next_request = Scope::new();
        assert!(scoped.read_metadata_blob(&entry).is_err());
        assert!(scoped.read_metadata_blob(&entry).is_err());
        Ok(())
    }

    #[test]
    fn cache_is_byte_and_entry_bounded_and_does_not_cross_backend_or_namespace() {
        let _request = Scope::new();
        let entry = ObjectEntry {
            blob: "blob".into(),
            hash: "hash".into(),
            etag: "etag".into(),
            created_ms: 1,
        };
        remember(1, "namespace/blob".into(), &entry, b"verified");
        assert!(get(2, "namespace/blob", &entry).is_none());
        assert!(get(1, "other/blob", &entry).is_none());
        let oversized = vec![0; MAX_BYTES + 1];
        remember(1, "oversized".into(), &entry, &oversized);
        assert!(get(1, "oversized", &entry).is_none());
        for index in 0..MAX_ENTRIES {
            remember(1, format!("key-{index}"), &entry, b"");
        }
        assert!(get(1, &format!("key-{}", MAX_ENTRIES - 1), &entry).is_none());
        CURRENT.with(|current| {
            assert_eq!(
                current.borrow().as_ref().unwrap().entries.len(),
                MAX_ENTRIES
            )
        });
    }

    #[test]
    fn nested_operations_share_only_the_enclosing_request_cache() {
        let entry = ObjectEntry {
            blob: "blob".into(),
            hash: "hash".into(),
            etag: "etag".into(),
            created_ms: 1,
        };
        {
            let _request = Scope::enter();
            remember(1, "key".into(), &entry, b"verified");
            {
                let nested = Scope::enter();
                assert!(nested.is_none());
                assert_eq!(get(1, "key", &entry).unwrap(), b"verified");
            }
            assert!(get(1, "key", &entry).is_some());
        }
        let _next_request = Scope::enter();
        assert!(get(1, "key", &entry).is_none());
    }
}
