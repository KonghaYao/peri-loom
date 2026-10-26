//! Branch-session tests for the accepted WAL image the publication path caches.
//!
//! The image only exists to save the next append a cold replay, so every test here asks the
//! same two questions: was it reused when the publication succeeded, and was it dropped when
//! the publication did not land? A failed publication leaves no trace in the store, so the
//! only way to observe the second answer is the scan the next append pays.

use super::*;
use crate::{
    manifest::WalPosition,
    namespace::Namespace,
    object_store::MemoryObjectStore,
    recovery::{
        fixture::{write_at, WalBuilder, FRAME_SIZE, PAGE_SIZE},
        recovery_observations, reset_recovery_observations, validate_index_state, WAL_HEADER,
    },
    wal::{DurableWal, RemoteWal, WalAppendRequest, WalBatch, WalCommit, WalOperation},
};
use anyhow::{bail, Context as _, Result};
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Mutex,
};

const DATABASE: &str = "wal-cache";
const SALTS: [u32; 2] = [41, 42];

/// A store that can fail the authority head write before or after persistence, and run one
/// hook immediately before or after it. The heads are the only writes with a condition, so
/// injecting there is what separates "the publication never started" from "it landed and the
/// caller lost the acknowledgement".
#[derive(Default)]
struct FaultStore {
    inner: MemoryObjectStore,
    /// 0 = normal, 1 = lose the acknowledgement after persistence, 2 = fail before it.
    root_fault: AtomicU8,
    before_root: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    after_root: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl ObjectStore for FaultStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        let is_root = key.ends_with("/authority/root");
        if is_root {
            // The hook runs work that itself writes the head, so the slot guard must be
            // released before it is called; holding it across `hook()` deadlocks on the
            // same store.
            let hook = self.before_root.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        let fault = if is_root {
            self.root_fault.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if fault == 2 {
            bail!("injected head failure before persistence");
        }
        let etag = self.inner.put(key, bytes, condition)?;
        if is_root {
            let hook = self.after_root.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        if fault == 1 {
            bail!("injected lost head acknowledgement after persistence");
        }
        Ok(etag)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

type Fixture = (Arc<FaultStore>, Arc<Authority>, Arc<BranchStore>, Namespace);

fn fixture() -> Result<Fixture> {
    let store = Arc::new(FaultStore::default());
    let namespace = Namespace::new("tenant", DATABASE)?;
    let authority = Authority::open(store.clone(), namespace.clone())?;
    let branch = authority.branch_store("main")?;
    Ok((store, authority, branch, namespace))
}

fn committed(frames: u64) -> Option<WalPosition> {
    Some(WalPosition {
        salts: SALTS,
        frame_count: frames,
    })
}

fn pin_entries(root: &CatalogRoot, pin: &str) -> Result<BTreeMap<String, ObjectEntry>> {
    let prefix = format!("{pin}/");
    Ok(root
        .pin_entries
        .entries()?
        .into_iter()
        .filter(|(key, _)| key.starts_with(&prefix))
        .collect())
}

/// Register `count` immutable segments and return their branch keys. These objects are
/// reachable through the logical registry, so a session can widen its pin by reading them
/// without moving the branch snapshot (which would retire the previous generation).
fn seed_segments(
    authority: &Authority,
    namespace: &Namespace,
    count: usize,
) -> Result<Vec<String>> {
    let mut keys = Vec::with_capacity(count);
    for batch_start in (0..count).step_by(96) {
        let batch_end = (batch_start + 96).min(count);
        let segments = (batch_start..batch_end)
            .map(|index| {
                let mut bytes = vec![0x5a; crate::manifest::SEGMENT_SIZE];
                bytes[0] = index as u8;
                bytes[1] = (index >> 8) as u8;
                bytes
            })
            .collect::<Vec<_>>();
        let uploaded = authority.upload_segments(&segments)?;
        authority.register_segments(&uploaded)?;
        for entry in uploaded {
            keys.push(namespace.key(&format!("segments/{}", entry.hash))?);
        }
    }
    Ok(keys)
}

fn pin_keys(branch: &Arc<BranchStore>, keys: &[String]) -> Result<()> {
    for key in keys {
        branch.get(key)?;
    }
    Ok(())
}

fn frame_batch(byte: u8) -> WalBatch {
    WalBatch {
        operations: vec![WalOperation::Write {
            offset: 0,
            bytes: vec![byte; 8],
        }],
    }
}

/// Publish one manifest that references every seeded segment and point HEAD at it. A cold
/// session then pins the whole closure in one transaction, which keeps fixture setup cheap
/// while making each pin large enough to spill the page tree.
fn publish_many_segment_head(
    branch: &Arc<BranchStore>,
    namespace: &Namespace,
    keys: &[String],
) -> Result<()> {
    let mut segments = BTreeMap::new();
    let mut size = 0u64;
    for (index, key) in keys.iter().enumerate() {
        let hash = key
            .rsplit('/')
            .next()
            .context("segment key without hash")?
            .to_owned();
        segments.insert(index as u64, hash);
        size = size.saturating_add(crate::manifest::SEGMENT_SIZE as u64);
    }
    let manifest = crate::manifest::Manifest {
        logical_db_size: size,
        segments,
        ..Default::default()
    };
    manifest.validate()?;
    let bytes = serde_json::to_vec(&manifest)?;
    let hash = crate::manifest::digest(&bytes);
    branch.put(
        &namespace.key(&format!("manifests/{hash}"))?,
        &bytes,
        crate::object_store::PutCondition::Absent,
    )?;
    branch.put(
        &namespace.key("HEAD")?,
        &serde_json::to_vec(&serde_json::json!({ "manifest": hash }))?,
        crate::object_store::PutCondition::Any,
    )?;
    Ok(())
}

fn legacy_inline_root(
    authority: &Authority,
    pin: &str,
) -> Result<(CatalogRoot, BTreeMap<String, ObjectEntry>)> {
    let mut root = authority.snapshot()?.root;
    let entries = pin_entries(&root, pin)?;
    let expected = entries
        .values()
        .map(|entry| (pin_entry_key(pin, 0, &entry.blob), entry.clone()))
        .collect();
    root.pin_entries = crate::authority::registry::Registry::inline(BTreeMap::new());
    root.pin_schema = None;
    root.pin_generations.clear();
    root.pins
        .insert(pin.to_owned(), entries.values().cloned().collect());
    Ok((root, expected))
}

/// Publishes through the same entry point the fenced worker path uses.
fn append(
    branch: &Arc<BranchStore>,
    namespace: &Namespace,
    batch: WalBatch,
    position: Option<WalPosition>,
    append_id: &str,
) -> Result<u64> {
    let request = WalAppendRequest {
        namespace: namespace.clone(),
        epoch: 1,
        batch,
        position,
        append_id: append_id.to_owned(),
        timestamp_millis: 1,
    };
    RemoteWal::new(branch.clone()).append_using(&request, |encoded, metadata, expected, pages| {
        branch.publish_wal_append(encoded, metadata, expected, pages)
    })
}

/// Reads the committed WAL through a fresh session: the index entry and the chunk bodies are
/// resolved from the catalog the current head names, not from the session under test.
fn cold_commit(authority: &Arc<Authority>, namespace: &Namespace) -> Result<Option<WalCommit>> {
    let session = authority.branch_store("main")?;
    let index = session
        .get(&namespace.key("wal/index")?)?
        .context("missing WAL index")?
        .bytes;
    Ok(validate_index_state(
        &index,
        namespace,
        |hash| {
            Ok(session
                .get(&namespace.key(&format!("wal/index-segments/{hash}"))?)?
                .context("missing WAL segment")?
                .bytes)
        },
        |key| Ok(session.get(key)?.context("missing WAL chunk")?.bytes),
        |hash| {
            Ok(session
                .get(&namespace.key(&format!("wal/receipt-pages/{hash}"))?)?
                .context("missing WAL receipt page")?
                .bytes)
        },
    )?
    .commit())
}

/// A successful append extends the cached image and pays only for its frame. A publication
/// that fails before the head write drops that image, so the retry validates the chain.
#[test]
fn a_cached_image_is_reused_and_a_failed_publication_forces_revalidation() -> Result<()> {
    let (store, authority, branch, namespace) = fixture()?;
    let mut builder = WalBuilder::new(SALTS);
    let first = builder.image(4);
    assert_eq!(
        append(
            &branch,
            &namespace,
            write_at(0, &first),
            committed(4),
            "first"
        )?,
        1
    );

    let frame5 = builder.frame(5, PAGE_SIZE as u32);
    reset_recovery_observations();
    assert_eq!(
        append(
            &branch,
            &namespace,
            write_at(first.len(), &frame5),
            committed(5),
            "second"
        )?,
        2
    );
    assert_eq!(
        recovery_observations().scanned_bytes,
        (WAL_HEADER + FRAME_SIZE) as u64,
        "the second append did not reuse the cached image"
    );
    assert_eq!(
        cold_commit(&authority, &namespace)?,
        Some(WalCommit {
            lsn: 2,
            position: WalPosition {
                salts: SALTS,
                frame_count: 5
            }
        })
    );

    // The head write fails before persistence. The chunk body may already be in the store,
    // but no index reaches it, so the accepted image is the state from before this append.
    store.root_fault.store(2, Ordering::SeqCst);
    let frame6 = builder.frame(6, PAGE_SIZE as u32);
    let offset = first.len() + FRAME_SIZE;
    assert!(append(
        &branch,
        &namespace,
        write_at(offset, &frame6),
        committed(6),
        "failed"
    )
    .is_err());

    // A fresh request for the same frame: the image must be re-derived, which costs one
    // header per record plus the frames those records contain.
    reset_recovery_observations();
    assert_eq!(
        append(
            &branch,
            &namespace,
            write_at(offset, &frame6),
            committed(6),
            "recovered"
        )?,
        3
    );
    assert_eq!(
        recovery_observations().scanned_bytes,
        (3 * WAL_HEADER + 6 * FRAME_SIZE) as u64,
        "the append after a failed publication trusted the dropped image"
    );
    assert_eq!(
        cold_commit(&authority, &namespace)?,
        Some(WalCommit {
            lsn: 3,
            position: WalPosition {
                salts: SALTS,
                frame_count: 6
            }
        })
    );
    Ok(())
}

/// A record that rewrites bytes inside the verified prefix keeps the image (it is still the
/// accepted state) but resets its scan, so the append that repairs and extends it re-derives
/// the chain from the header.
#[test]
fn an_append_that_rewrites_the_prefix_keeps_the_bytes_and_resets_the_scan() -> Result<()> {
    let (_store, authority, branch, namespace) = fixture()?;
    let mut builder = WalBuilder::new(SALTS);
    let first = builder.image(4);
    assert_eq!(
        append(
            &branch,
            &namespace,
            write_at(0, &first),
            committed(4),
            "first"
        )?,
        1
    );

    let damage = WAL_HEADER + FRAME_SIZE + 24;
    let mut corrupted = first[damage..damage + 8].to_vec();
    corrupted[0] ^= 0xff;
    assert_eq!(
        append(
            &branch,
            &namespace,
            write_at(damage, &corrupted),
            None,
            "overwrite"
        )?,
        2
    );

    let repaired = first[damage..damage + 8].to_vec();
    let mut batch = write_at(damage, &repaired);
    for (page, db_size) in [(5, 0), (6, PAGE_SIZE as u32)] {
        batch.operations.push(WalOperation::Write {
            offset: (WAL_HEADER + (page - 1) * FRAME_SIZE) as u64,
            bytes: builder.frame(page as u32, db_size),
        });
    }
    reset_recovery_observations();
    assert_eq!(
        append(&branch, &namespace, batch, committed(6), "repair")?,
        3
    );
    assert_eq!(
        recovery_observations().scanned_bytes,
        (WAL_HEADER + 6 * FRAME_SIZE) as u64,
        "the overwrite did not invalidate the cached prefix"
    );
    assert_eq!(
        cold_commit(&authority, &namespace)?,
        Some(WalCommit {
            lsn: 3,
            position: WalPosition {
                salts: SALTS,
                frame_count: 6
            }
        })
    );
    Ok(())
}

/// The head moving under an attempt makes it retry. The first attempt extended the cached
/// image; the retry must not extend that same image again, so the cache has already been
/// dropped by the ownership move and the retry replays the chain.
#[test]
fn a_lost_head_race_drops_the_cache_and_the_retry_revalidates() -> Result<()> {
    let (store, authority, branch, namespace) = fixture()?;
    let mut builder = WalBuilder::new(SALTS);
    let first = builder.image(4);
    assert_eq!(
        append(
            &branch,
            &namespace,
            write_at(0, &first),
            committed(4),
            "first"
        )?,
        1
    );
    let frame5 = builder.frame(5, PAGE_SIZE as u32);
    assert_eq!(
        append(
            &branch,
            &namespace,
            write_at(first.len(), &frame5),
            committed(5),
            "second"
        )?,
        2
    );

    // Move the head without touching this branch's WAL: the condition and the cached entry
    // still match, so the attempt gets all the way to its append and only then loses the CAS.
    let competitor = authority.clone();
    *store.before_root.lock().unwrap() = Some(Box::new(move || {
        competitor
            .transact(None, |root| {
                root.branches
                    .insert("competitor".into(), BranchRef::default());
                Ok(())
            })
            .unwrap();
    }));

    let frame6 = builder.frame(6, PAGE_SIZE as u32);
    reset_recovery_observations();
    assert_eq!(
        append(
            &branch,
            &namespace,
            write_at(first.len() + FRAME_SIZE, &frame6),
            committed(6),
            "racing"
        )?,
        3
    );
    // Attempt one extended the cache (header plus its frame); the retry re-derived the three
    // records it now describes. Both scans are paid by this append.
    assert_eq!(
        recovery_observations().scanned_bytes,
        (4 * WAL_HEADER + 7 * FRAME_SIZE) as u64,
        "the retried append did not re-derive the image"
    );

    // The retry's cold state is cached, so the next append is incremental again.
    let frame7 = builder.frame(7, PAGE_SIZE as u32);
    reset_recovery_observations();
    assert_eq!(
        append(
            &branch,
            &namespace,
            write_at(first.len() + 2 * FRAME_SIZE, &frame7),
            committed(7),
            "after-race"
        )?,
        4
    );
    assert_eq!(
        recovery_observations().scanned_bytes,
        (WAL_HEADER + FRAME_SIZE) as u64,
        "the retry did not leave a reusable image behind"
    );
    assert_eq!(
        cold_commit(&authority, &namespace)?,
        Some(WalCommit {
            lsn: 4,
            position: WalPosition {
                salts: SALTS,
                frame_count: 7
            }
        })
    );
    Ok(())
}

/// A record whose commit position the appended frame does not produce must fail before the
/// head is written, and the failure must discard the image it already extended in place.
#[test]
fn a_forged_commit_position_discards_the_extended_image() -> Result<()> {
    let (_store, authority, branch, namespace) = fixture()?;
    let mut builder = WalBuilder::new(SALTS);
    let first = builder.image(4);
    assert_eq!(
        append(
            &branch,
            &namespace,
            write_at(0, &first),
            committed(4),
            "first"
        )?,
        1
    );

    let frame5 = builder.frame(5, PAGE_SIZE as u32);
    assert!(append(
        &branch,
        &namespace,
        write_at(first.len(), &frame5),
        committed(3),
        "forged"
    )
    .is_err());

    reset_recovery_observations();
    assert_eq!(
        append(
            &branch,
            &namespace,
            write_at(first.len(), &frame5),
            committed(5),
            "recovered"
        )?,
        2
    );
    assert_eq!(
        recovery_observations().scanned_bytes,
        (2 * WAL_HEADER + 5 * FRAME_SIZE) as u64,
        "the append after a forged commit reused the extended image"
    );
    assert_eq!(
        cold_commit(&authority, &namespace)?,
        Some(WalCommit {
            lsn: 2,
            position: WalPosition {
                salts: SALTS,
                frame_count: 5
            }
        })
    );
    Ok(())
}

/// A lost acknowledgement can resolve to a later head whose branch WAL is not the index this
/// attempt published. The session must adopt that head and drop the image: caching it would
/// make the next append extend a chain the branch no longer names.
#[test]
fn a_lost_acknowledgement_that_resolves_to_a_later_wal_does_not_keep_the_image() -> Result<()> {
    let (store, authority, branch, namespace) = fixture()?;
    let mut builder = WalBuilder::new(SALTS);
    let first = builder.image(4);
    assert_eq!(
        append(
            &branch,
            &namespace,
            write_at(0, &first),
            committed(4),
            "first"
        )?,
        1
    );

    // The competing session is opened inside the hook, after this attempt's head has been
    // persisted: its view is seeded from that head, so it builds on this attempt's index
    // instead of racing it. The hook runs between persistence and the lost acknowledgement.
    let frame5 = builder.frame(5, PAGE_SIZE as u32);
    let frame6 = builder.frame(6, PAGE_SIZE as u32);
    let competitor = authority.clone();
    let later_namespace = namespace.clone();
    let later_batch = write_at(first.len() + FRAME_SIZE, &frame6);
    *store.after_root.lock().unwrap() = Some(Box::new(move || {
        let later = competitor.branch_store("main").unwrap();
        append(&later, &later_namespace, later_batch, committed(6), "later").unwrap();
    }));
    store.root_fault.store(1, Ordering::SeqCst);
    assert_eq!(
        append(
            &branch,
            &namespace,
            write_at(first.len(), &frame5),
            committed(5),
            "lost-ack"
        )?,
        2
    );

    // The next append must read the later index and replay the whole chain.
    let frame7 = builder.frame(7, PAGE_SIZE as u32);
    reset_recovery_observations();
    assert_eq!(
        append(
            &branch,
            &namespace,
            write_at(first.len() + 2 * FRAME_SIZE, &frame7),
            committed(7),
            "after-lost-ack"
        )?,
        4
    );
    assert_eq!(
        recovery_observations().scanned_bytes,
        (4 * WAL_HEADER + 7 * FRAME_SIZE) as u64,
        "the session cached an image the acknowledged head does not name"
    );
    assert_eq!(
        cold_commit(&authority, &namespace)?,
        Some(WalCommit {
            lsn: 4,
            position: WalPosition {
                salts: SALTS,
                frame_count: 7
            }
        })
    );
    Ok(())
}

/// The later markerless index has no recovery-history owner. Once it leaves the branch,
/// only the original session's durable pin can keep that adopted view readable through GC.
#[test]
fn a_later_wal_adopted_after_a_lost_ack_is_pinned_through_gc() -> Result<()> {
    let (store, authority, branch, namespace) = fixture()?;
    let mut builder = WalBuilder::new(SALTS);
    let first = builder.image(4);
    assert_eq!(
        append(
            &branch,
            &namespace,
            write_at(0, &first),
            committed(4),
            "first"
        )?,
        1
    );

    let competitor = authority.clone();
    let later_namespace = namespace.clone();
    *store.after_root.lock().unwrap() = Some(Box::new(move || {
        let later = competitor.branch_store("main").unwrap();
        assert_eq!(
            append(
                &later,
                &later_namespace,
                WalBatch::default(),
                None,
                "later-markerless"
            )
            .unwrap(),
            3
        );
        // Its own session pin must disappear before the original session adopts the view.
        drop(later);
    }));
    store.root_fault.store(1, Ordering::SeqCst);
    let frame5 = builder.frame(5, PAGE_SIZE as u32);
    assert_eq!(
        append(
            &branch,
            &namespace,
            write_at(first.len(), &frame5),
            committed(5),
            "lost-ack"
        )?,
        2
    );

    let adopted = branch.view.lock().unwrap().wal.clone().unwrap();
    let snapshot = authority.snapshot()?;
    let pin_prefix = format!("{}/", branch.pin);
    assert!(snapshot
        .root
        .pin_entries
        .entries()?
        .iter()
        .any(|(key, entry)| { key.starts_with(&pin_prefix) && entry.blob == adopted.blob }));
    assert!(!snapshot.root.branches["main"]
        .history
        .iter()
        .any(|point| point.wal.as_ref() == Some(&adopted)));
    // Retire the current reference to isolate the reader pin from branch reachability.
    authority.transact(None, |root| {
        root.branches.get_mut("main").unwrap().wal = None;
        Ok(())
    })?;
    crate::gc::GarbageCollector::new(authority.clone(), std::time::Duration::ZERO).collect()?;

    let wal = RemoteWal::new(branch.clone());
    assert_eq!(wal.metadata(&namespace)?.last_lsn, 3);
    assert_eq!(wal.read(&namespace, 0)?.len(), 3);
    // Reattach the preserved point, then extend it through the same original session.
    authority.transact(None, |root| {
        root.branches.get_mut("main").unwrap().wal = Some(adopted.clone());
        Ok(())
    })?;
    let frame6 = builder.frame(6, PAGE_SIZE as u32);
    assert_eq!(
        append(
            &branch,
            &namespace,
            write_at(first.len() + FRAME_SIZE, &frame6),
            committed(6),
            "after-gc"
        )?,
        4
    );
    assert_eq!(
        cold_commit(&authority, &namespace)?,
        Some(WalCommit {
            lsn: 4,
            position: WalPosition {
                salts: SALTS,
                frame_count: 6
            }
        })
    );
    Ok(())
}

#[test]
fn truncation_archives_receipts_into_pages_the_gc_walk_reaches() -> Result<()> {
    let (_store, authority, branch, namespace) = fixture()?;
    let wal = RemoteWal::new(branch.clone());
    let request = |identity: &str, bytes: &[u8]| WalAppendRequest {
        namespace: namespace.clone(),
        epoch: 1,
        batch: WalBatch {
            operations: vec![WalOperation::Write {
                offset: 0,
                bytes: bytes.to_vec(),
            }],
        },
        position: None,
        append_id: identity.into(),
        timestamp_millis: 3,
    };
    let archived = request("archived-receipt", &[1, 2, 3]);
    assert_eq!(
        wal.append_using(&archived, |encoded, metadata, expected, pages| {
            branch.publish_wal_append(encoded, metadata, expected, pages)
        })?,
        1
    );
    assert_eq!(
        wal.append_using(
            &request("kept-receipt", &[4]),
            |encoded, metadata, expected, pages| {
                branch.publish_wal_append(encoded, metadata, expected, pages)
            }
        )?,
        2
    );

    wal.truncate(&namespace, 2)?;

    let index_bytes = branch
        .get(&namespace.key("wal/index")?)?
        .context("missing WAL index")?
        .bytes;
    let index: crate::wal::WalMetadata = serde_json::from_slice(&index_bytes)?;
    assert_eq!(index.replay_baseline_lsn, 1);
    assert!(!index.receipts.contains_key("archived-receipt"));
    assert!(index.receipts.contains_key("kept-receipt"));
    let root = index
        .receipt_index
        .clone()
        .context("missing receipt archive")?;

    // A duplicate append is answered from the archive without a second record.
    assert_eq!(
        wal.append_using(&archived, |encoded, metadata, expected, pages| {
            branch.publish_wal_append(encoded, metadata, expected, pages)
        })?,
        1
    );
    assert_eq!(wal.metadata(&namespace)?.last_lsn, 2);

    let page_key = format!("wal/receipt-pages/{root}");
    let snapshot = authority.snapshot()?;
    let page = snapshot
        .root
        .objects
        .get(&page_key)?
        .context("receipt page not registered")?;
    let reachable = authority.reachable(&snapshot.root)?;
    assert!(
        reachable.contains(&page.blob),
        "GC walk did not reach the receipt archive"
    );
    Ok(())
}

#[test]
fn segmented_wal_pins_and_gc_reach_segments_and_chunks() -> Result<()> {
    let (_, authority, branch, namespace) = fixture()?;
    for ordinal in 1..=513 {
        append(
            &branch,
            &namespace,
            WalBatch::default(),
            None,
            &format!("segment-{ordinal}"),
        )?;
    }
    let wal = RemoteWal::new(branch.clone());
    let index = wal.metadata(&namespace)?;
    assert_eq!(index.entry_count, Some(513));
    assert_eq!(index.entries.len(), 1);
    let snapshot = authority.snapshot()?;
    let reference = &snapshot.root.branches["main"];
    let objects = referenced(&authority, &snapshot.root, None, reference.wal.as_ref())?;
    assert_eq!(
        objects
            .keys()
            .filter(|key| key.starts_with("wal/index-segments/"))
            .count(),
        2
    );
    assert_eq!(
        objects
            .keys()
            .filter(|key| key.starts_with("wal/chunks/"))
            .count(),
        513
    );
    let mut unpinned = snapshot.root.clone();
    unpinned.pins.clear();
    unpinned.pin_entries = crate::authority::registry::Registry::inline(BTreeMap::new());
    let reachable = authority.reachable(&unpinned)?;
    for entry in objects.values() {
        assert!(reachable.contains(&entry.blob));
    }
    let cold = authority.branch_store("main")?;
    let pinned = authority.snapshot()?;
    let pin_prefix = format!("{}/", cold.pin);
    let pin_entries = pinned.root.pin_entries.entries()?;
    for entry in objects.values() {
        assert!(pin_entries
            .iter()
            .any(|(key, pinned)| key.starts_with(&pin_prefix) && pinned.blob == entry.blob));
    }
    assert_eq!(RemoteWal::new(cold).read(&namespace, 0)?.len(), 513);
    Ok(())
}

#[test]
fn pin_entries_spill_without_growing_the_head_and_survive_cold_recovery() -> Result<()> {
    let (store, authority, branch, namespace) = fixture()?;
    let keys = seed_segments(&authority, &namespace, 160)?;
    pin_keys(&branch, &keys)?;
    let pin = branch.pin.clone();
    let snapshot = authority.snapshot()?;
    assert!(snapshot.root.pins[&pin].is_empty());
    assert!(
        snapshot.root.pin_entries.pointer().is_some(),
        "pin entries stayed inline"
    );
    assert_eq!(snapshot.root.pin_entries.window_len(), 0);
    let entries = pin_entries(&snapshot.root, &pin)?;
    assert!(entries.len() >= 160);
    let pin_bytes = serde_json::to_vec(&entries)?.len();
    let head_bytes = crate::authority::codec::encode(&snapshot.root)?.len();
    assert!(
        pin_bytes > 16 * 1024,
        "pin payload did not exceed the inline budget: {pin_bytes}"
    );
    assert!(
        head_bytes < pin_bytes / 2,
        "head grew with the pin payload: head={head_bytes} pin={pin_bytes}"
    );

    let reopened = Authority::open(store, namespace)?;
    let reopened_root = reopened.snapshot()?.root;
    assert!(reopened_root.pins.contains_key(&pin));
    assert_eq!(pin_entries(&reopened_root, &pin)?, entries);
    Ok(())
}

#[test]
fn releasing_a_reader_pin_drops_entries_and_releases_its_blobs() -> Result<()> {
    let (_, authority, branch, namespace) = fixture()?;
    for ordinal in 1..=8 {
        append(
            &branch,
            &namespace,
            WalBatch::default(),
            None,
            &format!("pin-release-{ordinal}"),
        )?;
    }
    let pin = branch.pin.clone();
    let pinned = pin_entries(&authority.snapshot()?.root, &pin)?;
    assert!(!pinned.is_empty());
    let blobs: BTreeSet<String> = pinned.values().map(|entry| entry.blob.clone()).collect();
    let reachable = authority.reachable(&authority.snapshot()?.root)?;
    assert!(blobs.iter().all(|blob| reachable.contains(blob)));

    // The descriptor cannot disappear while its entries still name the pin.
    assert!(authority
        .transact(None, |root| {
            root.pins.remove(&pin);
            Ok(())
        })
        .is_err());

    drop(branch);
    let root = authority.snapshot()?.root;
    assert!(!root.pins.contains_key(&pin));
    assert!(pin_entries(&root, &pin)?.is_empty());
    authority.transact(None, |root| {
        let branch = root.branches.get_mut("main").context("branch missing")?;
        branch.head = None;
        branch.wal = None;
        branch.history.clear();
        Ok(())
    })?;
    let reachable = authority.reachable(&authority.snapshot()?.root)?;
    for blob in blobs {
        assert!(
            !reachable.contains(&blob),
            "released pin still marks blob {blob}"
        );
    }
    Ok(())
}

#[test]
fn pin_entries_fail_closed_when_pages_are_missing_corrupt_or_legacy_inline() -> Result<()> {
    let (store, authority, branch, namespace) = fixture()?;
    let keys = seed_segments(&authority, &namespace, 160)?;
    pin_keys(&branch, &keys)?;
    let snapshot = authority.snapshot()?;
    let pointer = snapshot
        .root
        .pin_entries
        .pointer()
        .context("pin entries did not spill")?
        .to_owned();
    let key = namespace.key(&format!("authority/index-pages/{pointer}"))?;
    let original = store.inner.get(&key)?.context("pin page missing")?;

    store
        .inner
        .put(&key, b"{}", crate::object_store::PutCondition::Any)?;
    assert!(authority.reachable(&snapshot.root).is_err());
    store.inner.put(
        &key,
        &original.bytes,
        crate::object_store::PutCondition::Any,
    )?;
    assert!(authority.reachable(&snapshot.root).is_ok());

    let mut legacy = authority.snapshot()?.root;
    let pin = branch.pin.clone();
    let entry = legacy
        .pin_entries
        .entries()?
        .into_values()
        .next()
        .context("pin entry missing")?;
    legacy.pins.get_mut(&pin).unwrap().push(entry);
    assert!(authority.reachable(&legacy).is_err());

    store.inner.delete(&key, None)?;
    assert!(authority.reachable(&authority.snapshot()?.root).is_err());
    Ok(())
}

#[test]
fn legacy_inline_pins_are_migrated_on_load_and_bad_entries_fail_closed() -> Result<()> {
    let (store, authority, branch, namespace) = fixture()?;
    for ordinal in 1..=8 {
        append(
            &branch,
            &namespace,
            WalBatch::default(),
            None,
            &format!("pin-migrate-{ordinal}"),
        )?;
    }
    let pin = branch.pin.clone();
    let (legacy, expected) = legacy_inline_root(&authority, &pin)?;
    let root_key = namespace.key("authority/root")?;
    store.inner.put(
        &root_key,
        &crate::authority::codec::encode(&legacy)?,
        PutCondition::Any,
    )?;

    let reopened = Authority::open(store.clone(), namespace.clone())?;
    let migrated = reopened.snapshot()?.root;
    assert!(migrated.pins.contains_key(&pin));
    assert!(migrated.pins[&pin].is_empty());
    assert_eq!(
        migrated.pin_schema,
        Some(crate::authority::types::PIN_SCHEMA_VERSION)
    );
    assert_eq!(pin_entries(&migrated, &pin)?, expected);

    let (mut bad, _) = legacy_inline_root(&authority, &pin)?;
    bad.pins.get_mut(&pin).unwrap()[0].blob = "e".repeat(64);
    store.inner.put(
        &root_key,
        &crate::authority::codec::encode(&bad)?,
        PutCondition::Any,
    )?;
    assert!(Authority::open(store, namespace).is_err());
    Ok(())
}

#[test]
fn removing_a_pin_entry_without_removing_its_pin_is_rejected() -> Result<()> {
    let (_, authority, branch, namespace) = fixture()?;
    let segment = vec![7; crate::manifest::SEGMENT_SIZE];
    let relative = format!("segments/{}", blake3::hash(&segment).to_hex());
    branch.put(
        &namespace.key(&relative)?,
        &segment,
        crate::object_store::PutCondition::Absent,
    )?;

    let pin = branch.pin.clone();
    let generation = branch.generation()?;
    let snapshot = authority.snapshot()?;
    let entries = pin_entries(&snapshot.root, &pin)?;
    let (key, entry) = entries
        .iter()
        .find(|(_, entry)| entry.hash == blake3::hash(&segment).to_hex().as_str())
        .context("pinned segment missing")?;
    assert_eq!(
        pin_entry_parts(key).map(|parts| parts.generation),
        Some(generation)
    );
    let blob = entry.blob.clone();
    assert!(authority.reachable(&snapshot.root)?.contains(&blob));

    assert!(authority
        .transact(None, |root| {
            root.pin_entries.remove(key)?;
            Ok(())
        })
        .is_err());
    assert!(authority.snapshot()?.root.pins.contains_key(&pin));
    assert!(authority
        .reachable(&authority.snapshot()?.root)?
        .contains(&blob));
    Ok(())
}

#[test]
fn whole_pin_registry_replacements_are_rejected_and_reader_protection_survives() -> Result<()> {
    let (_, authority, branch, namespace) = fixture()?;
    let seeded = seed_segments(&authority, &namespace, 160)?;
    pin_keys(&branch, &seeded)?;
    let segment = vec![7; crate::manifest::SEGMENT_SIZE];
    let relative = format!("segments/{}", blake3::hash(&segment).to_hex());
    branch.put(
        &namespace.key(&relative)?,
        &segment,
        crate::object_store::PutCondition::Absent,
    )?;
    // The delete starts a new generation. Hold the one that wrote the segment so its
    // entries remain the protection under test instead of being retired as idle.
    let _guard = branch.hold_generation()?;
    branch.delete(&namespace.key(&relative)?, None)?;
    let pin = branch.pin.clone();
    let segment_hash = blake3::hash(&segment).to_hex().to_string();
    let snapshot = authority.snapshot()?;
    let entries = pin_entries(&snapshot.root, &pin)?;
    let segment_key = entries
        .iter()
        .find(|(_, entry)| entry.hash == segment_hash)
        .map(|(key, _)| key.clone())
        .context("pinned segment missing")?;
    let segment_blob = entries[&segment_key].blob.clone();
    let old_inline = snapshot.root.pin_entries.clone();

    assert!(authority
        .transact(None, |root| {
            root.pin_entries = crate::authority::registry::Registry::inline(BTreeMap::new());
            Ok(())
        })
        .is_err());
    let mut subset = entries.clone();
    subset.remove(&segment_key);
    assert!(authority
        .transact(None, |root| {
            root.pin_entries = crate::authority::registry::Registry::inline(subset.clone());
            Ok(())
        })
        .is_err());

    for ordinal in 1..=4 {
        append(
            &branch,
            &namespace,
            WalBatch::default(),
            None,
            &format!("pin-replace-{ordinal}"),
        )?;
    }
    assert!(authority
        .transact(None, |root| {
            root.pin_entries = old_inline.clone();
            Ok(())
        })
        .is_err());

    for ordinal in 5..=52 {
        append(
            &branch,
            &namespace,
            WalBatch::default(),
            None,
            &format!("pin-replace-{ordinal}"),
        )?;
    }
    let paged = authority.snapshot()?.root;
    assert!(paged.pin_entries.pointer().is_some());
    let old_paged = paged.pin_entries.clone();
    for ordinal in 53..=56 {
        append(
            &branch,
            &namespace,
            WalBatch::default(),
            None,
            &format!("pin-replace-{ordinal}"),
        )?;
    }
    assert!(authority
        .transact(None, |root| {
            root.pin_entries = old_paged.clone();
            Ok(())
        })
        .is_err());

    let root = authority.snapshot()?.root;
    assert!(root.pins.contains_key(&pin));
    assert!(pin_entries(&root, &pin)?
        .values()
        .any(|entry| entry.blob == segment_blob));
    assert!(authority.reachable(&root)?.contains(&segment_blob));
    let collector = crate::gc::GarbageCollector::new(authority.clone(), std::time::Duration::ZERO);
    collector.collect()?;
    let root = authority.snapshot()?.root;
    assert!(root.blobs.get(&segment_blob)?.is_some());
    assert!(authority.reachable(&root)?.contains(&segment_blob));
    Ok(())
}

#[test]
fn split_transitional_inline_root_loads_and_is_upgraded() -> Result<()> {
    let (store, authority, branch, namespace) = fixture()?;
    for ordinal in 1..=4 {
        append(
            &branch,
            &namespace,
            WalBatch::default(),
            None,
            &format!("pin-transition-inline-{ordinal}"),
        )?;
    }
    let pin = branch.pin.clone();
    let root = authority.snapshot()?.root;
    let entries = pin_entries(&root, &pin)?;
    assert!(!entries.is_empty());
    assert!(root.pin_entries.pointer().is_none());
    let mut transitional = root;
    transitional.pin_schema = None;
    assert!(transitional.pins.values().all(Vec::is_empty));
    drop(branch);

    let root_key = namespace.key("authority/root")?;
    store.inner.put(
        &root_key,
        &crate::authority::codec::encode(&transitional)?,
        PutCondition::Any,
    )?;
    let reopened = Authority::open(store.clone(), namespace.clone())?;
    let loaded = reopened.snapshot()?.root;
    assert_eq!(pin_entries(&loaded, &pin)?, entries);

    reopened.transact(None, |_| Ok(()))?;
    let stored = store.inner.get(&root_key)?.context("root missing")?;
    let envelope: serde_json::Value = serde_json::from_slice(&stored.bytes)?;
    assert_eq!(envelope["root"]["pin_schema"], 2);
    Ok(())
}

#[test]
fn split_transitional_paged_root_loads_and_is_upgraded() -> Result<()> {
    let (store, authority, branch, namespace) = fixture()?;
    let keys = seed_segments(&authority, &namespace, 160)?;
    pin_keys(&branch, &keys)?;
    let pin = branch.pin.clone();
    let root = authority.snapshot()?.root;
    let entries = pin_entries(&root, &pin)?;
    assert!(root.pin_entries.pointer().is_some());
    let mut transitional = root;
    transitional.pin_schema = None;
    // A genuine schema-1 document stored its entries in the hash-ordered tree.
    let serialized = entries
        .iter()
        .map(|(key, entry)| Ok((key.clone(), serde_json::to_value(entry)?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    let pointer = crate::authority::paged_index::PagedIndex::new(&store.inner, &namespace)
        .build(serialized)?
        .context("hashed pin page tree missing")?;
    transitional.pin_entries = crate::authority::registry::Registry::paged_with_order(
        Some(pointer),
        BTreeMap::new(),
        crate::authority::paged_index::KeyOrder::Hashed,
    );
    assert!(transitional.pins.values().all(Vec::is_empty));
    drop(branch);

    let root_key = namespace.key("authority/root")?;
    store.inner.put(
        &root_key,
        &crate::authority::codec::encode(&transitional)?,
        PutCondition::Any,
    )?;
    let reopened = Authority::open(store.clone(), namespace.clone())?;
    let loaded = reopened.snapshot()?.root;
    assert_eq!(pin_entries(&loaded, &pin)?, entries);

    reopened.transact(None, |_| Ok(()))?;
    let stored = store.inner.get(&root_key)?.context("root missing")?;
    let envelope: serde_json::Value = serde_json::from_slice(&stored.bytes)?;
    assert_eq!(envelope["root"]["pin_schema"], 2);
    Ok(())
}

#[test]
fn empty_split_transitional_root_loads_and_is_upgraded() -> Result<()> {
    let store = Arc::new(FaultStore::default());
    let namespace = Namespace::new("tenant", DATABASE)?;
    let authority = Authority::open(store.clone(), namespace.clone())?;
    let root = authority.snapshot()?.root;
    assert!(root.pins.is_empty());
    assert!(root.pin_entries.is_empty());
    let mut transitional = root;
    transitional.pin_schema = None;

    let root_key = namespace.key("authority/root")?;
    store.inner.put(
        &root_key,
        &crate::authority::codec::encode(&transitional)?,
        PutCondition::Any,
    )?;
    let reopened = Authority::open(store.clone(), namespace.clone())?;
    let loaded = reopened.snapshot()?.root;
    assert_eq!(
        loaded.pin_schema,
        Some(crate::authority::types::PIN_SCHEMA_VERSION)
    );
    assert!(loaded.pins.is_empty());
    assert!(loaded.pin_entries.is_empty());

    reopened.transact(None, |_| Ok(()))?;
    let stored = store.inner.get(&root_key)?.context("root missing")?;
    let envelope: serde_json::Value = serde_json::from_slice(&stored.bytes)?;
    assert_eq!(envelope["root"]["pin_schema"], 2);
    assert_eq!(envelope["root"]["pins"], serde_json::json!({}));
    assert_eq!(envelope["root"]["pin_entries"], serde_json::json!({}));
    Ok(())
}

#[test]
fn mixed_pin_representations_are_rejected_on_load() -> Result<()> {
    let (store, authority, branch, namespace) = fixture()?;
    let keys = seed_segments(&authority, &namespace, 160)?;
    pin_keys(&branch, &keys)?;
    let root = authority.snapshot()?.root;
    let pin = branch.pin.clone();
    let entries = pin_entries(&root, &pin)?;
    let vectors: Vec<ObjectEntry> = entries.values().cloned().collect();
    drop(branch);
    let root_key = namespace.key("authority/root")?;

    let mut legacy_with_inline = root.clone();
    legacy_with_inline.pin_schema = None;
    legacy_with_inline.pin_entries = crate::authority::registry::Registry::inline(entries.clone());
    legacy_with_inline.pins.insert(pin.clone(), vectors.clone());
    store.inner.put(
        &root_key,
        &crate::authority::codec::encode(&legacy_with_inline)?,
        PutCondition::Any,
    )?;
    assert!(Authority::open(store.clone(), namespace.clone()).is_err());

    let mut split_with_vectors = root.clone();
    split_with_vectors.pin_schema = Some(crate::authority::types::PIN_SCHEMA_VERSION);
    split_with_vectors.pins.insert(pin.clone(), vectors.clone());
    store.inner.put(
        &root_key,
        &crate::authority::codec::encode(&split_with_vectors)?,
        PutCondition::Any,
    )?;
    assert!(Authority::open(store.clone(), namespace.clone()).is_err());

    let mut legacy_with_paged = root;
    legacy_with_paged.pin_schema = None;
    legacy_with_paged.pins.insert(pin, vectors);
    assert!(legacy_with_paged.pin_entries.pointer().is_some());
    store.inner.put(
        &root_key,
        &crate::authority::codec::encode(&legacy_with_paged)?,
        PutCondition::Any,
    )?;
    assert!(Authority::open(store, namespace).is_err());
    Ok(())
}

#[test]
fn unsupported_pin_schema_is_rejected_at_decode_time() -> Result<()> {
    let (_, authority, _, _) = fixture()?;
    let mut root = authority.snapshot()?.root;
    root.pin_schema = Some(3);
    let encoded = crate::authority::codec::encode(&root)?;
    assert!(crate::authority::codec::decode(&encoded).is_err());
    Ok(())
}

#[test]
fn a_failed_drop_release_is_counted_and_leaves_the_pin_visible() -> Result<()> {
    let (store, authority, branch, _) = fixture()?;
    let pin = branch.pin.clone();
    crate::work_metrics::set_enabled(true);
    let before = crate::work_metrics::snapshot()
        .context("work metrics unavailable")?
        .metadata
        .pin_release_failures;
    store.root_fault.store(2, Ordering::SeqCst);
    drop(branch);
    let after = crate::work_metrics::snapshot()
        .context("work metrics unavailable")?
        .metadata
        .pin_release_failures;
    assert!(after.saturating_sub(before) >= 1);
    assert!(authority.snapshot()?.root.pins.contains_key(&pin));
    assert!(matches!(
        authority.pin_state(&pin)?,
        super::super::PinState::Orphan
    ));
    Ok(())
}

#[test]
fn a_segment_group_publishes_and_pins_every_body_in_one_publication() -> Result<()> {
    let (_store, authority, branch, namespace) = fixture()?;
    let bodies = [
        vec![0x11u8; crate::manifest::SEGMENT_SIZE],
        vec![0x22u8; crate::manifest::SEGMENT_SIZE],
    ];
    let writes = bodies
        .iter()
        .map(|bytes| {
            Ok(crate::object_store::GroupWrite::absent(
                namespace.key(&format!("segments/{}", crate::manifest::digest(bytes)))?,
                bytes.clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let before = authority.snapshot()?.root.revision;
    let etags = branch.put_group(&writes)?;
    assert_eq!(etags.len(), bodies.len());
    let root = authority.snapshot()?.root;
    // One publication: the retry, the reservation and the registration share it.
    assert_eq!(root.revision, before + 1);
    for bytes in &bodies {
        let key = format!("segments/{}", crate::manifest::digest(bytes));
        let entry = root
            .objects
            .get(&key)?
            .with_context(|| format!("segment {key} not registered"))?;
        assert_eq!(authority.read_blob(&entry)?, *bytes);
        assert!(!root.uploads.contains(&entry.blob)?);
        assert!(root
            .pin_entries
            .get(&pin_entry_key(
                branch.pin_identity(),
                branch.generation()?,
                &entry.blob
            ))?
            .is_some());
    }
    // A group is all-or-nothing: retrying landed bodies leaves the catalog unchanged and
    // fails the pre-group condition, which is what sends the caller to its identity check.
    let rejected = branch
        .put_group(&writes)
        .expect_err("an existing segment must reject the group");
    assert!(rejected.is::<crate::object_store::PreconditionFailed>());
    assert_eq!(authority.snapshot()?.root.revision, root.revision);
    Ok(())
}

#[test]
fn releasing_one_of_many_pins_reads_only_its_own_page_range() -> Result<()> {
    let (_, authority, branch, namespace) = fixture()?;
    let keys = seed_segments(&authority, &namespace, 128)?;
    publish_many_segment_head(&branch, &namespace, &keys)?;
    let mut sessions = Vec::new();
    for _ in 0..64 {
        sessions.push(authority.branch_store("main")?);
    }
    let snapshot = authority.snapshot()?;
    assert!(
        snapshot.root.pin_entries.pointer().is_some(),
        "the shared pin registry did not spill"
    );
    let (_, global_pages) = snapshot.root.pin_entries.entries_counted()?;
    assert!(
        global_pages > 20,
        "global pin registry is too small for a page bound: {global_pages}"
    );
    let target = &sessions[0];
    let other = &sessions[1];
    let target_entries = pin_entries(&snapshot.root, target.pin_identity())?.len();
    let other_entries = pin_entries(&snapshot.root, other.pin_identity())?.len();
    assert!(target_entries > 0 && other_entries > 0);

    crate::authority::paged_index::reset_test_page_reads();
    let stats = target.release_pin(&authority)?;
    let pages = crate::authority::paged_index::test_page_reads();

    assert_eq!(stats.entries, target_entries);
    assert!(stats.batches >= 2, "release was not split into batches");
    assert!(
        stats.scanned <= target_entries + 256,
        "release scanned too many entries: {} for {} owned",
        stats.scanned,
        target_entries
    );
    assert!(
        pages <= 128,
        "release read {pages} pages for one pin of {target_entries} entries"
    );
    assert!(
        pages * 4 < global_pages,
        "release cost {pages} pages is not clearly below the global {global_pages}"
    );
    let root = authority.snapshot()?.root;
    assert!(
        pin_entries(&root, target.pin_identity())?.is_empty(),
        "released pin left entries"
    );
    assert_eq!(
        pin_entries(&root, other.pin_identity())?.len(),
        other_entries,
        "releasing one pin changed another"
    );
    assert!(matches!(
        authority.pin_state(target.pin_identity())?,
        super::super::PinState::Absent
    ));
    Ok(())
}

#[test]
fn long_lived_store_retires_superseded_generations() -> Result<()> {
    let (_, authority, branch, namespace) = fixture()?;
    append(
        &branch,
        &namespace,
        WalBatch::default(),
        None,
        "generation-0",
    )?;
    let initial = branch.pinned_entries()?.len();
    for ordinal in 1..=32 {
        append(
            &branch,
            &namespace,
            WalBatch::default(),
            None,
            &format!("generation-{ordinal}"),
        )?;
    }
    let observation = branch.observe_pin()?;
    assert!(
        observation.live_generations <= 2,
        "store kept {} generations",
        observation.live_generations
    );
    assert!(
        observation.pinned_entries <= initial + 128,
        "pin grew with history: {} entries, initial {initial}",
        observation.pinned_entries
    );
    let root = authority.snapshot()?.root;
    let current = branch.generation()?;
    for key in pin_entries(&root, branch.pin_identity())?.keys() {
        let parts = pin_entry_parts(key).context("invalid pin key")?;
        assert_eq!(
            parts.generation, current,
            "an idle old generation is still registered"
        );
    }
    Ok(())
}

#[test]
fn a_held_generation_is_not_retired_until_its_reader_exits() -> Result<()> {
    let (_, authority, branch, namespace) = fixture()?;
    append(&branch, &namespace, WalBatch::default(), None, "held-0")?;
    let held_generation = branch.generation()?;
    let guard = branch.hold_generation()?;
    for ordinal in 1..=2 {
        append(
            &branch,
            &namespace,
            WalBatch::default(),
            None,
            &format!("held-{ordinal}"),
        )?;
    }
    assert_ne!(branch.generation()?, held_generation);
    let root = authority.snapshot()?.root;
    assert!(
        !pin_entries(&root, branch.pin_identity())?
            .keys()
            .filter_map(|key| pin_entry_parts(key))
            .filter(|parts| parts.generation == held_generation)
            .collect::<Vec<_>>()
            .is_empty(),
        "a held generation was retired by refresh"
    );
    let observation = branch.observe_pin()?;
    assert!(observation.live_generations >= 2);
    assert!(observation.held_generations >= 1);

    drop(guard);
    let root = authority.snapshot()?.root;
    assert!(
        pin_entries(&root, branch.pin_identity())?
            .keys()
            .filter_map(|key| pin_entry_parts(key))
            .all(|parts| parts.generation != held_generation),
        "the generation survived its last reader"
    );
    let observation = branch.observe_pin()?;
    assert_eq!(observation.held_generations, 0);
    assert_eq!(observation.live_generations, 1);
    Ok(())
}

#[test]
fn concurrent_refresh_release_and_gc_do_not_lose_objects() -> Result<()> {
    let (_, authority, branch, namespace) = fixture()?;
    append(
        &branch,
        &namespace,
        WalBatch::default(),
        None,
        "concurrent-0",
    )?;
    let writer = branch.clone();
    let writer_namespace = namespace.clone();
    let writer_handle = std::thread::spawn(move || -> Result<()> {
        for ordinal in 1..=8 {
            append(
                &writer,
                &writer_namespace,
                frame_batch(ordinal as u8),
                None,
                &format!("concurrent-writer-{ordinal}"),
            )?;
        }
        Ok(())
    });
    let collector_authority = authority.clone();
    let collector_handle = std::thread::spawn(move || -> Result<()> {
        for _ in 0..8 {
            let root = collector_authority.snapshot()?.root;
            collector_authority.reachable(&root)?;
        }
        crate::gc::GarbageCollector::new(collector_authority.clone(), std::time::Duration::ZERO)
            .collect()?;
        Ok(())
    });
    let releaser_authority = authority.clone();
    let releaser_handle = std::thread::spawn(move || -> Result<()> {
        for _ in 0..8 {
            let session = releaser_authority.branch_store("main")?;
            drop(session);
        }
        Ok(())
    });
    for ordinal in 1..=8 {
        append(
            &branch,
            &namespace,
            frame_batch((ordinal + 100) as u8),
            None,
            &format!("concurrent-main-{ordinal}"),
        )?;
    }
    writer_handle.join().expect("writer panicked")?;
    collector_handle.join().expect("collector panicked")?;
    releaser_handle.join().expect("releaser panicked")?;

    let session = authority.branch_store("main")?;
    let index_bytes = session
        .get(&namespace.key("wal/index")?)?
        .context("WAL index lost")?
        .bytes;
    let index = crate::authority::semantic::index(&index_bytes)?;
    assert!(
        index.last_lsn >= 16,
        "concurrent appends lost WAL positions: {}",
        index.last_lsn
    );
    assert!(!RemoteWal::new(session).read(&namespace, 0)?.is_empty());
    let root = authority.snapshot()?.root;
    authority.reachable(&root)?;
    Ok(())
}

/// A1 attribution: every append publication charges one `append` origin, and the receipt
/// archive lookup after a roll no longer extends the reader pin. Origin totals still
/// reconcile exactly with `root_publish_attempts`.
#[test]
fn append_origin_reconciles_and_receipt_roll_does_not_extend_a_pin() -> Result<()> {
    crate::work_metrics::set_enabled(true);
    let (_store, authority, branch, namespace) = fixture()?;
    let before = crate::work_metrics::snapshot().expect("enabled snapshot");

    let appends = crate::wal::WAL_RECEIPT_INLINE_LIMIT + 2;
    for ordinal in 1..=appends {
        append(
            &branch,
            &namespace,
            frame_batch(ordinal as u8),
            None,
            &format!("attribution-{ordinal}"),
        )?;
    }
    assert!(
        crate::wal::RemoteWal::new(branch.clone())
            .metadata(&namespace)?
            .receipt_index
            .is_some(),
        "the fixture must have rolled receipts into the archive"
    );

    // An archived duplicate is answered without an append publication and, crucially,
    // without a pin extension: the page is served from the retained archive instead of
    // being admitted into the branch view.
    let revision = authority.snapshot()?.root.revision;
    let pinned = branch.pinned_entries()?;
    assert_eq!(
        append(&branch, &namespace, frame_batch(1), None, "attribution-1")?,
        1
    );
    assert_eq!(
        authority.snapshot()?.root.revision,
        revision,
        "an archived receipt lookup must not publish"
    );
    assert_eq!(
        branch.pinned_entries()?,
        pinned,
        "an archived receipt lookup must not extend the reader pin"
    );

    let after = crate::work_metrics::snapshot().expect("enabled snapshot");
    let origin = |name: &str| after.metadata.publish_origin[name].calls;
    assert!(
        origin("append") - before.metadata.publish_origin["append"].calls >= appends as u64,
        "each append must charge the append origin"
    );
    let attributed: u64 = after
        .metadata
        .publish_origin
        .values()
        .map(|count| count.calls)
        .sum();
    assert_eq!(
        attributed, after.metadata.root_publish_attempts,
        "sum(origin) must equal root_publish_attempts"
    );
    Ok(())
}

/// A1: archived WAL receipt pages are unreclaimable metadata. An append may advance the
/// index and retire the pin that protected the old page path, then GC runs; a reader that
/// still holds the old index must keep resolving archived receipts.
#[test]
fn archived_receipt_pages_survive_gc_and_old_index_readers() -> Result<()> {
    crate::work_metrics::set_enabled(true);
    let (_store, authority, branch, namespace) = fixture()?;
    let wal = RemoteWal::new(branch.clone());
    let appends = crate::wal::WAL_RECEIPT_INLINE_LIMIT + 2;
    for ordinal in 1..=appends {
        append(
            &branch,
            &namespace,
            frame_batch(ordinal as u8),
            None,
            &format!("retained-{ordinal}"),
        )?;
    }
    let old_root = wal
        .metadata(&namespace)?
        .receipt_index
        .context("missing first archive")?;
    let old_pages = |root: &CatalogRoot| -> Result<BTreeMap<String, ObjectEntry>> {
        Ok(root
            .objects
            .entries()?
            .into_iter()
            .filter(|(key, _)| key.starts_with("wal/receipt-pages/"))
            .collect())
    };
    let archived_pages = old_pages(&authority.snapshot()?.root)?;
    assert!(!archived_pages.is_empty());

    // Cross the next roll so the first archive stops being reachable from the branch head,
    // and let the generation that pinned the old page path retire.
    for ordinal in (appends + 1)..=(appends + crate::wal::WAL_RECEIPT_ROLL_BATCH + 2) {
        append(
            &branch,
            &namespace,
            frame_batch(ordinal as u8),
            None,
            &format!("retained-{ordinal}"),
        )?;
    }
    let new_root = wal
        .metadata(&namespace)?
        .receipt_index
        .context("missing second archive")?;
    assert_ne!(new_root, old_root);

    // The same GC still prunes an ordinary logical alias: retention is scoped to the
    // archived receipt prefix, not a global freeze.
    let session = authority.branch_store("main")?;
    let orphan = vec![0x6b; crate::manifest::SEGMENT_SIZE];
    let orphan_key = namespace.key(&format!("segments/{}", crate::manifest::digest(&orphan)))?;
    session.put(
        &orphan_key,
        &orphan,
        crate::object_store::PutCondition::Absent,
    )?;
    drop(session);

    let before = crate::work_metrics::snapshot().expect("enabled snapshot");
    let collected =
        crate::gc::GarbageCollector::new(authority.clone(), std::time::Duration::ZERO).collect()?;
    let after = crate::work_metrics::snapshot().expect("enabled snapshot");
    assert!(
        after.gc.scanned_entries > before.gc.scanned_entries,
        "work.gc must observe the GC scan"
    );
    assert!(
        collected.deleted >= 1,
        "GC did not collect the ordinary orphan"
    );

    let root = authority.snapshot()?.root;
    assert!(
        root.objects.get(&orphan_key)?.is_none(),
        "an ordinary alias survived GC"
    );
    for (key, entry) in &archived_pages {
        assert!(
            root.objects.get(key)?.is_some(),
            "retained archive alias lost: {key}"
        );
        assert!(
            root.blobs.get(&entry.blob)?.is_some(),
            "retained archive blob lost: {key}"
        );
        authority
            .read_blob(entry)
            .with_context(|| format!("retained archive page unreadable: {key}"))?;
    }

    // An old index page path, advanced past and pruned-over, still answers.
    let pages = crate::authority::paged_index::PagedIndex::retained_with_prefix(
        branch.as_ref(),
        &namespace,
        crate::wal::WAL_RECEIPT_PAGE_PREFIX,
    );
    let archived: crate::wal::ArchivedReceipt = serde_json::from_value(
        pages
            .get(Some(&old_root), "retained-1")?
            .context("old archive page missing")?,
    )?;
    assert_eq!(archived.lsn, 1);
    let current = pages.entries(Some(&new_root))?;
    assert!(!current.is_empty());
    let (identity, value) = current.iter().next().context("second archive is empty")?;
    let archived: crate::wal::ArchivedReceipt = serde_json::from_value(value.clone())?;
    assert!(
        archived.lsn > 0 && archived.lsn <= wal.metadata(&namespace)?.last_lsn,
        "second archive value for {identity} is outside the WAL"
    );

    // The append gate itself keeps resolving old identities and accepts a new one.
    assert_eq!(
        append(&branch, &namespace, frame_batch(1), None, "retained-1")?,
        1
    );
    let last = wal.metadata(&namespace)?.last_lsn;
    assert_eq!(
        append(&branch, &namespace, frame_batch(0xfe), None, "after-gc")?,
        last + 1
    );
    Ok(())
}

/// A1 concurrency: GC may prune and publish while appends advance the index and resolve
/// archived identities. Neither side may observe a missing receipt page.
#[test]
fn archived_receipt_reads_stay_available_while_gc_runs() -> Result<()> {
    let (_store, authority, branch, namespace) = fixture()?;
    let wal = RemoteWal::new(branch.clone());
    let appends = crate::wal::WAL_RECEIPT_INLINE_LIMIT + 2;
    for ordinal in 1..=appends {
        append(
            &branch,
            &namespace,
            frame_batch(ordinal as u8),
            None,
            &format!("concurrent-retained-{ordinal}"),
        )?;
    }
    let old_root = wal
        .metadata(&namespace)?
        .receipt_index
        .context("missing archive")?;

    let collector_authority = authority.clone();
    let collector = std::thread::spawn(move || -> Result<()> {
        for _ in 0..6 {
            crate::gc::GarbageCollector::new(
                collector_authority.clone(),
                std::time::Duration::ZERO,
            )
            .collect()?;
        }
        Ok(())
    });
    for ordinal in 0..12 {
        append(
            &branch,
            &namespace,
            frame_batch(ordinal as u8),
            None,
            &format!("concurrent-writer-{ordinal}"),
        )?;
        assert_eq!(
            append(
                &branch,
                &namespace,
                frame_batch(1),
                None,
                "concurrent-retained-1"
            )?,
            1,
            "an archived receipt stopped answering during GC"
        );
    }
    collector.join().expect("collector panicked")?;

    let root = authority.snapshot()?.root;
    let pages = crate::authority::paged_index::PagedIndex::retained_with_prefix(
        branch.as_ref(),
        &namespace,
        crate::wal::WAL_RECEIPT_PAGE_PREFIX,
    );
    let archived: crate::wal::ArchivedReceipt = serde_json::from_value(
        pages
            .get(Some(&old_root), "concurrent-retained-1")?
            .context("archived page lost under GC")?,
    )?;
    assert_eq!(archived.lsn, 1);
    assert!(
        root.objects
            .entries()?
            .keys()
            .any(|key| key.starts_with("wal/receipt-pages/")),
        "GC removed every archived receipt page"
    );
    Ok(())
}

/// A1 lost acknowledgement and restart: a head write whose acknowledgement is lost still
/// resolves through the inline receipt, archived identities keep answering, and a reopened
/// authority resolves them through the retained archive without a pin.
#[test]
fn archived_receipt_reads_survive_lost_ack_and_reopen() -> Result<()> {
    let (store, authority, branch, namespace) = fixture()?;
    let appends = crate::wal::WAL_RECEIPT_INLINE_LIMIT + 2;
    for ordinal in 1..=appends {
        append(
            &branch,
            &namespace,
            frame_batch(ordinal as u8),
            None,
            &format!("reopen-retained-{ordinal}"),
        )?;
    }
    let archived_pages = authority
        .snapshot()?
        .root
        .objects
        .entries()?
        .into_iter()
        .filter(|(key, _)| key.starts_with("wal/receipt-pages/"))
        .count();
    assert!(archived_pages > 0);

    store.root_fault.store(1, Ordering::SeqCst);
    let lost = append(
        &branch,
        &namespace,
        frame_batch(0xee),
        None,
        "lost-after-roll",
    )?;
    assert_eq!(lost, appends as u64 + 1);
    assert_eq!(
        append(
            &branch,
            &namespace,
            frame_batch(0xee),
            None,
            "lost-after-roll"
        )?,
        lost,
        "the inline receipt must resolve the retry"
    );
    assert_eq!(
        append(
            &branch,
            &namespace,
            frame_batch(1),
            None,
            "reopen-retained-1"
        )?,
        1
    );

    crate::gc::GarbageCollector::new(authority.clone(), std::time::Duration::ZERO).collect()?;
    drop(branch);
    drop(authority);

    let reopened = Authority::open(store, namespace.clone())?;
    let branch = reopened.branch_store("main")?;
    let wal = RemoteWal::new(branch.clone());
    assert_eq!(wal.metadata(&namespace)?.last_lsn, lost);
    assert_eq!(
        append(
            &branch,
            &namespace,
            frame_batch(1),
            None,
            "reopen-retained-1"
        )?,
        1,
        "a reopened authority must resolve the archived receipt"
    );
    assert!(
        reopened
            .snapshot()?
            .root
            .objects
            .entries()?
            .keys()
            .any(|key| key.starts_with("wal/receipt-pages/")),
        "archived receipt pages did not survive restart"
    );
    Ok(())
}
