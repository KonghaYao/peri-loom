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
    RemoteWal::new(branch.clone()).append_using(&request, |encoded, metadata, expected| {
        branch.publish_wal_append(encoded, metadata, expected)
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
    assert!(snapshot.root.pins[&branch.pin]
        .iter()
        .any(|entry| entry.blob == adopted.blob));
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
        wal.append_using(&archived, |encoded, metadata, expected| {
            branch.publish_wal_append(encoded, metadata, expected)
        })?,
        1
    );
    assert_eq!(
        wal.append_using(
            &request("kept-receipt", &[4]),
            |encoded, metadata, expected| {
                branch.publish_wal_append(encoded, metadata, expected)
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
        wal.append_using(&archived, |encoded, metadata, expected| {
            branch.publish_wal_append(encoded, metadata, expected)
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
    let reachable = authority.reachable(&unpinned)?;
    for entry in objects.values() {
        assert!(reachable.contains(&entry.blob));
    }
    let cold = authority.branch_store("main")?;
    let pinned = authority.snapshot()?;
    for entry in objects.values() {
        assert!(pinned.root.pins[&cold.pin]
            .iter()
            .any(|pin| pin.blob == entry.blob));
    }
    assert_eq!(RemoteWal::new(cold).read(&namespace, 0)?.len(), 513);
    Ok(())
}
