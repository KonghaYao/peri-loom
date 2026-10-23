use super::*;
use crate::{
    authority::{Authority, BranchRef},
    manifest::WalPosition,
    object_store::MemoryObjectStore,
    provider::LocalAuthorityProvider,
    recovery::{
        fixture::{write_at, WalBuilder, FRAME_SIZE, PAGE_SIZE},
        recovery_observations, reset_recovery_observations, WAL_HEADER,
    },
    runtime::{DatabaseId, DatabaseProvider},
    wal::{WalBatch, WalOperation, MAX_APPEND_OPERATIONS, MAX_APPEND_PAYLOAD_BYTES},
};
use anyhow::{bail, Context};
use std::sync::{
    atomic::{AtomicU8, AtomicUsize, Ordering},
    Mutex,
};

#[derive(Default)]
struct Probe {
    inner: MemoryObjectStore,
    roots: AtomicUsize,
    groups: AtomicUsize,
    fault: AtomicU8,
    before_root: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    after_root: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl Probe {
    fn reset(&self) {
        self.roots.store(0, Ordering::SeqCst);
        self.groups.store(0, Ordering::SeqCst);
    }

    fn roots(&self) -> usize {
        self.roots.load(Ordering::SeqCst)
    }
}

impl ObjectStore for Probe {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        let root = key.ends_with("/authority/root");
        if root {
            let hook = self.before_root.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        let fault = if root {
            self.fault.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if fault == 2 {
            bail!("injected failure before root");
        }
        let etag = self.inner.put(key, bytes, condition)?;
        if root {
            self.roots.fetch_add(1, Ordering::SeqCst);
            let hook = self.after_root.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        if fault == 1 {
            bail!("injected lost root ACK");
        }
        Ok(etag)
    }

    fn put_group(&self, writes: &[GroupWrite]) -> Result<Vec<String>> {
        self.groups.fetch_add(1, Ordering::SeqCst);
        assert!(writes.last().unwrap().key.ends_with("/authority/root"));
        assert!(writes[..writes.len() - 1]
            .iter()
            .all(|write| !write.key.ends_with("/authority/root")));
        writes
            .iter()
            .map(|write| self.put(&write.key, &write.bytes, write.condition.clone()))
            .collect()
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

fn fixture(epoch: u64) -> Result<(Arc<Probe>, Arc<dyn ObjectStore>, Namespace)> {
    let probe = Arc::new(Probe::default());
    let namespace = Namespace::new("local", "fast-path")?;
    let identifier = DatabaseId::new(namespace.clone(), "main")?;
    let store = LocalAuthorityProvider::new(probe.clone()).open_store(&identifier, epoch)?;
    probe.reset();
    Ok((probe, store, namespace))
}

fn request(namespace: &Namespace, epoch: u64, id: &str) -> WalAppendRequest {
    WalAppendRequest {
        namespace: namespace.clone(),
        epoch,
        batch: WalBatch {
            operations: vec![WalOperation::Write {
                offset: 0,
                bytes: vec![7; 8],
            }],
        },
        position: None,
        append_id: id.into(),
        timestamp_millis: 1,
    }
}

#[test]
fn local_provider_non_roll_append_publishes_root_exactly_once() -> Result<()> {
    for epoch in [0, 3] {
        let (probe, store, namespace) = fixture(epoch)?;
        let wal = RemoteWal::new(store);
        assert_eq!(
            wal.append_commit(
                &namespace,
                epoch,
                request(&namespace, epoch, "x").batch,
                None
            )?,
            1
        );
        assert_eq!(probe.roots(), 1);
        assert_eq!(probe.groups.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[test]
fn local_epoch_zero_is_not_accepted_by_production_validation() -> Result<()> {
    let (probe, store, namespace) = fixture(0)?;
    let request = request(&namespace, 0, "zero");
    assert!(request.validate().is_err());
    request.validate_local()?;
    assert_eq!(store.append_wal(&request)?, Some(1));
    assert_eq!(probe.roots(), 1);
    assert!(!store.supports_strict_fencing());
    Ok(())
}

#[test]
fn local_binding_rejects_wrong_epoch_namespace_and_stale_wal() -> Result<()> {
    let (probe, store, namespace) = fixture(2)?;
    let valid = request(&namespace, 2, "valid");
    for epoch in [0, 1, 3] {
        let wrong = WalAppendRequest {
            epoch,
            ..valid.clone()
        };
        assert!(store
            .append_wal(&wrong)
            .unwrap_err()
            .to_string()
            .contains("identity"));
    }
    let other = Namespace::new("other", "database")?;
    let wrong = WalAppendRequest {
        namespace: other.clone(),
        ..valid.clone()
    };
    assert!(store.append_wal(&wrong).is_err());
    assert!(store.get(&other.key("wal/index")?).is_err());
    assert_eq!(probe.roots(), 0);
    assert_eq!(store.append_wal(&valid)?, Some(1));
    let stale = LocalAuthorityProvider::new(probe.clone())
        .open_store(&DatabaseId::new(namespace.clone(), "main")?, 1)?;
    probe.reset();
    assert!(stale
        .append_wal(&request(&namespace, 1, "stale"))
        .unwrap_err()
        .to_string()
        .contains("stale WAL epoch"));
    assert_eq!(probe.roots(), 0);
    Ok(())
}

#[test]
fn local_duplicate_receipt_rejects_changed_content_without_publication() -> Result<()> {
    let (probe, store, namespace) = fixture(0)?;
    let original = request(&namespace, 0, "same-id");
    assert_eq!(store.append_wal(&original)?, Some(1));
    probe.reset();
    assert_eq!(store.append_wal(&original)?, Some(1));
    let changed = WalAppendRequest {
        timestamp_millis: 2,
        ..original
    };
    assert!(store
        .append_wal(&changed)
        .unwrap_err()
        .to_string()
        .contains("conflict"));
    assert_eq!(probe.roots(), 0);
    Ok(())
}

#[test]
fn local_bad_identity_and_oversized_payload_fail_before_publication() -> Result<()> {
    let (probe, store, namespace) = fixture(0)?;
    for id in ["", "bad_id", &"a".repeat(129)] {
        assert!(store.append_wal(&request(&namespace, 0, id)).is_err());
    }
    let mut oversized = request(&namespace, 0, "large");
    oversized.batch.operations = vec![WalOperation::Write {
        offset: 0,
        bytes: vec![0; MAX_APPEND_PAYLOAD_BYTES + 1],
    }];
    assert!(store
        .append_wal(&oversized)
        .unwrap_err()
        .to_string()
        .contains("payload limit"));
    oversized.batch.operations =
        vec![WalOperation::Truncate { size: 0 }; MAX_APPEND_OPERATIONS + 1];
    assert!(store
        .append_wal(&oversized)
        .unwrap_err()
        .to_string()
        .contains("operation limit"));
    assert_eq!(probe.roots(), 0);
    Ok(())
}

#[test]
fn local_lost_ack_resolves_receipt_and_uncommitted_failure_is_not_retried() -> Result<()> {
    let (probe, store, namespace) = fixture(0)?;
    let original = request(&namespace, 0, "lost-ack");
    probe.fault.store(1, Ordering::SeqCst);
    assert_eq!(store.append_wal(&original)?, Some(1));
    assert_eq!(probe.roots(), 1, "lost ACK must not replay an append");
    assert_eq!(probe.groups.load(Ordering::SeqCst), 1);
    probe.reset();
    assert_eq!(store.append_wal(&original)?, Some(1));
    assert_eq!(probe.roots(), 0);

    let uncommitted = request(&namespace, 0, "not-committed");
    probe.fault.store(2, Ordering::SeqCst);
    assert!(store.append_wal(&uncommitted).is_err());
    assert_eq!(
        probe.groups.load(Ordering::SeqCst),
        1,
        "no automatic retry on unchanged head"
    );
    assert_eq!(probe.roots(), 0);
    assert!(!RemoteWal::new(store.clone())
        .metadata(&namespace)?
        .receipts
        .contains_key("not-committed"));
    probe.reset();
    assert_eq!(store.append_wal(&uncommitted)?, Some(2));
    assert_eq!(probe.roots(), 1, "only the explicit caller retry publishes");
    Ok(())
}

#[test]
fn local_receipt_roll_keeps_archived_identity_and_conflict_checks() -> Result<()> {
    let (probe, store, namespace) = fixture(0)?;
    let original = request(&namespace, 0, "append-0");
    for index in 0..=crate::wal::WAL_RECEIPT_INLINE_LIMIT {
        let request = request(&namespace, 0, &format!("append-{index}"));
        assert_eq!(store.append_wal(&request)?, Some(index as u64 + 1));
    }
    let index = RemoteWal::new(store.clone()).metadata(&namespace)?;
    assert!(index.receipt_index.is_some());
    assert!(!index.receipts.contains_key("append-0"));
    let cold = LocalAuthorityProvider::new(probe.clone())
        .open_store(&DatabaseId::new(namespace, "main")?, 0)?;
    probe.reset();
    assert_eq!(cold.append_wal(&original)?, Some(1));
    let changed = WalAppendRequest {
        timestamp_millis: 2,
        ..original
    };
    assert!(cold
        .append_wal(&changed)
        .unwrap_err()
        .to_string()
        .contains("conflict"));
    assert_eq!(probe.roots(), 0);
    Ok(())
}

#[test]
fn local_publish_pages_delegates_one_group_with_checksum_and_namespace_validation() -> Result<()> {
    let (probe, store, namespace) = fixture(0)?;
    let writes = [b"page-one".as_slice(), b"page-two".as_slice()]
        .into_iter()
        .map(|bytes| {
            Ok(GroupWrite::absent(
                namespace.key(&format!("wal/receipt-pages/{}", blake3::hash(bytes)))?,
                bytes.to_vec(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    assert!(store.publish_pages(&writes)?);
    assert_eq!(probe.roots(), 1);
    assert_eq!(probe.groups.load(Ordering::SeqCst), 1);
    for write in &writes {
        assert_eq!(
            store.get(&write.key)?.context("page missing")?.bytes,
            write.bytes
        );
    }
    probe.reset();
    assert!(store.publish_pages(&writes)?);
    assert_eq!(probe.roots(), 0, "identical pages are already published");
    let mut invalid = writes[0].clone();
    invalid.bytes.push(0);
    assert!(store
        .publish_pages(&[invalid])
        .unwrap_err()
        .to_string()
        .contains("checksum"));
    let mut outside = writes[0].clone();
    outside.key = Namespace::new("other", "db")?.key("wal/receipt-pages/invalid")?;
    assert!(store.publish_pages(&[outside]).is_err());
    assert_eq!(probe.roots(), 0);
    Ok(())
}

fn frame_request(
    namespace: &Namespace,
    frames: u64,
    offset: usize,
    bytes: &[u8],
) -> WalAppendRequest {
    WalAppendRequest {
        batch: write_at(offset, bytes),
        position: Some(WalPosition {
            salts: [41, 42],
            frame_count: frames,
        }),
        ..request(namespace, 0, &format!("frame-{frames}"))
    }
}

#[test]
fn local_cache_is_incremental_and_invalidated_after_failure_and_cas_retry() -> Result<()> {
    let (probe, store, namespace) = fixture(0)?;
    let mut builder = WalBuilder::new([41, 42]);
    let image = builder.image(4);
    assert_eq!(
        store.append_wal(&frame_request(&namespace, 4, 0, &image))?,
        Some(1)
    );
    let frame = builder.frame(5, PAGE_SIZE as u32);
    reset_recovery_observations();
    assert_eq!(
        store.append_wal(&frame_request(&namespace, 5, image.len(), &frame))?,
        Some(2)
    );
    assert_eq!(
        recovery_observations().scanned_bytes,
        (WAL_HEADER + FRAME_SIZE) as u64
    );

    let frame = builder.frame(6, PAGE_SIZE as u32);
    let next = frame_request(&namespace, 6, image.len() + FRAME_SIZE, &frame);
    probe.fault.store(2, Ordering::SeqCst);
    assert!(store.append_wal(&next).is_err());
    reset_recovery_observations();
    assert_eq!(store.append_wal(&next)?, Some(3));
    assert_eq!(
        recovery_observations().scanned_bytes,
        (3 * WAL_HEADER + 6 * FRAME_SIZE) as u64
    );

    let authority = Authority::open(probe.clone(), namespace.clone())?;
    *probe.before_root.lock().unwrap() = Some(Box::new(move || {
        authority
            .transact(None, |root| {
                root.branches
                    .insert("competitor".into(), BranchRef::default());
                Ok(())
            })
            .unwrap();
    }));
    let frame = builder.frame(7, PAGE_SIZE as u32);
    reset_recovery_observations();
    assert_eq!(
        store.append_wal(&frame_request(
            &namespace,
            7,
            image.len() + 2 * FRAME_SIZE,
            &frame
        ))?,
        Some(4)
    );
    assert_eq!(
        recovery_observations().scanned_bytes,
        (5 * WAL_HEADER + 8 * FRAME_SIZE) as u64
    );
    Ok(())
}

#[test]
fn local_lost_ack_later_index_does_not_install_mismatched_cache() -> Result<()> {
    let (probe, store, namespace) = fixture(0)?;
    let mut builder = WalBuilder::new([41, 42]);
    let image = builder.image(4);
    store.append_wal(&frame_request(&namespace, 4, 0, &image))?;
    let frame5 = builder.frame(5, PAGE_SIZE as u32);
    let frame6 = builder.frame(6, PAGE_SIZE as u32);
    let later = frame_request(&namespace, 6, image.len() + FRAME_SIZE, &frame6);
    let provider = LocalAuthorityProvider::new(probe.clone());
    let identifier = DatabaseId::new(namespace.clone(), "main")?;
    *probe.after_root.lock().unwrap() = Some(Box::new(move || {
        assert_eq!(
            provider
                .open_store(&identifier, 0)
                .unwrap()
                .append_wal(&later)
                .unwrap(),
            Some(3)
        );
    }));
    probe.fault.store(1, Ordering::SeqCst);
    assert_eq!(
        store.append_wal(&frame_request(&namespace, 5, image.len(), &frame5))?,
        Some(2)
    );
    let frame7 = builder.frame(7, PAGE_SIZE as u32);
    reset_recovery_observations();
    assert_eq!(
        store.append_wal(&frame_request(
            &namespace,
            7,
            image.len() + 2 * FRAME_SIZE,
            &frame7
        ))?,
        Some(4)
    );
    assert_eq!(
        recovery_observations().scanned_bytes,
        (4 * WAL_HEADER + 7 * FRAME_SIZE) as u64
    );
    Ok(())
}

#[test]
fn local_branch_incarnation_change_invalidates_adapter() -> Result<()> {
    let (probe, store, namespace) = fixture(0)?;
    let authority = Authority::open(probe.clone(), namespace.clone())?;
    authority.transact(None, |root| {
        root.branches.get_mut("main").unwrap().incarnation += 1;
        Ok(())
    })?;
    probe.reset();
    assert!(store
        .append_wal(&request(&namespace, 0, "stale-branch"))
        .is_err());
    assert_eq!(probe.roots(), 0);
    Ok(())
}
