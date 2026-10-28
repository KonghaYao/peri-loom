use super::*;
use crate::object_store::{GroupWrite, ObjectStore, PutCondition};
use std::cell::RefCell;
use std::sync::atomic::Ordering;

use super::super::LocalObjectStore;

thread_local! {
    static CAPTURE: RefCell<Option<Vec<Value>>> = const { RefCell::new(None) };
}

pub(super) fn capture(record: &Value) -> bool {
    CAPTURE.with(|capture| {
        let mut capture = capture.borrow_mut();
        if let Some(records) = capture.as_mut() {
            records.push(record.clone());
            true
        } else {
            false
        }
    })
}

fn captured(action: impl FnOnce()) -> Vec<Value> {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            CAPTURE.with(|capture| *capture.borrow_mut() = None);
        }
    }
    CAPTURE.with(|capture| assert!(capture.borrow_mut().replace(Vec::new()).is_none()));
    let _reset = Reset;
    action();
    CAPTURE.with(|capture| capture.borrow_mut().take().unwrap())
}

fn diagnostic_store(path: &std::path::Path, packed: bool) -> LocalObjectStore {
    let mut store = LocalObjectStore::open_layout(path, packed).unwrap();
    // Tests do not change the cached global environment profile.
    store.physical_diagnostics = CountersDiagnostic::new(true);
    store
}

#[test]
fn disabled_diagnostic_never_evaluates_snapshot() {
    assert!(CountersDiagnostic::new(false).is_none());
    emit_if_enabled(None, SnapshotReason::StoreDrop, None, || {
        panic!("disabled observation evaluated its snapshot")
    });
}

#[test]
fn schema_keeps_cumulative_scope_and_boundary_separate() {
    let diagnostic = CountersDiagnostic::new(true).unwrap();
    let records = captured(|| {
        emit_if_enabled(
            Some(&diagnostic),
            SnapshotReason::SuccessfulGroupBoundary,
            Some(17),
            || serde_json::json!({"body_sync_calls": 12, "directory_sync_calls": 4}),
        );
        emit_if_enabled(
            Some(&diagnostic),
            SnapshotReason::StoreDrop,
            None,
            || serde_json::json!({"body_sync_calls": 13, "directory_sync_calls": 5}),
        );
    });
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["boundary_group_id"], 17);
    assert!(records[1]["boundary_group_id"].is_null());
    assert_eq!(records[0]["snapshot_sequence"], 1);
    assert_eq!(records[1]["snapshot_sequence"], 2);
    for record in &records {
        assert_eq!(record["scope"], "store_instance_since_construction");
        assert_eq!(record["atomic_snapshot"], false);
        assert_eq!(record["group_delta"], false);
        assert_eq!(record["coverage_complete"], false);
        assert_eq!(record["durable_diagnostic_log"], false);
        assert_eq!(record["store_instance_id"], diagnostic.instance);
        assert_eq!(record["pid"], std::process::id());
    }
}

#[test]
fn diagnostics_contain_no_store_path_key_payload_or_etag() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("private-root-do-not-emit");
    let mut etag = String::new();
    let records = captured(|| {
        let store = diagnostic_store(&path, false);
        etag = store
            .put_group(&[GroupWrite::new(
                "secret-tenant/private-db/authority/blobs/private-key",
                b"plk1_secret_payload_do_not_emit".to_vec(),
                PutCondition::Any,
            )])
            .unwrap()[0]
            .clone();
    });
    assert_eq!(records.len(), 2);
    let encoded = serde_json::to_string(&records).unwrap();
    for secret in [
        "private-root-do-not-emit",
        "secret-tenant",
        "private-db",
        "private-key",
        "plk1_secret_payload_do_not_emit",
        &etag,
    ] {
        assert!(!encoded.contains(secret));
    }
    assert_eq!(records[1]["snapshot_reason"], "store_drop");
    assert_eq!(records[1]["metrics"]["body_sync_calls"], 1);
}

#[test]
fn real_legacy_and_pack_groups_expose_physical_not_logical_counts() {
    let temporary = tempfile::tempdir().unwrap();
    for packed in [false, true] {
        let path = temporary
            .path()
            .join(if packed { "packed" } else { "legacy" });
        let records = captured(|| {
            let store = diagnostic_store(&path, packed);
            let writes: Vec<_> = (0..4)
                .map(|i| {
                    GroupWrite::new(
                        format!("tenant/db/authority/index-pages/{i}"),
                        vec![i as u8; 64],
                        PutCondition::Any,
                    )
                })
                .collect();
            store.put_group(&writes).unwrap();
            for write in &writes {
                assert_eq!(store.get(&write.key).unwrap().unwrap().bytes, write.bytes);
            }
        });
        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0]["metrics"]["body_sync_calls"],
            if packed { 1 } else { 4 }
        );
        assert_eq!(
            records[1]["metrics"]["pack_entries_created"],
            if packed { 4 } else { 0 }
        );
        assert_eq!(
            records[1]["metrics"]["pack_payload_bytes_read"],
            if packed { 256 } else { 0 }
        );
        assert!(records[1]["metrics"]["directory_cache"].is_object());
    }
}

#[test]
fn independent_handles_do_not_share_counter_identity() {
    let temporary = tempfile::tempdir().unwrap();
    let records = captured(|| {
        let first = diagnostic_store(temporary.path(), false);
        let second = diagnostic_store(temporary.path(), false);
        assert_ne!(
            first.physical_diagnostics.as_ref().unwrap().instance,
            second.physical_diagnostics.as_ref().unwrap().instance
        );
        first
            .put("tenant/db/key", b"body", PutCondition::Absent)
            .unwrap();
        drop(first);
        drop(second);
    });
    assert_eq!(records.len(), 2);
    assert_ne!(
        records[0]["store_instance_id"],
        records[1]["store_instance_id"]
    );
    assert_eq!(records[0]["metrics"]["body_sync_calls"], 1);
    assert_eq!(records[1]["metrics"]["body_sync_calls"], 0);
}

#[test]
fn failed_group_has_no_success_boundary_but_drop_retains_attempt_counts() {
    let temporary = tempfile::tempdir().unwrap();
    let records = captured(|| {
        let store = diagnostic_store(temporary.path(), false);
        let key = "tenant/db/key";
        // A nonregular target permits staging but makes the locked publication
        // fail. Its already attempted body Sync remains part of lifetime work.
        std::fs::create_dir(store.path(key)).unwrap();
        assert!(store
            .put_group(&[GroupWrite::new(key, b"body".to_vec(), PutCondition::Any)])
            .is_err());
    });
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["snapshot_reason"], "store_drop");
    assert_eq!(records[0]["metrics"]["body_sync_calls"], 1);
}

#[test]
fn drop_during_unwind_does_not_claim_a_terminal_snapshot() {
    let temporary = tempfile::tempdir().unwrap();
    let records = captured(|| {
        let result = std::panic::catch_unwind(|| {
            let _store = diagnostic_store(temporary.path(), false);
            panic!("simulated owner unwind");
        });
        assert!(result.is_err());
    });
    assert!(records.is_empty());
}

#[test]
fn snapshot_and_sink_panics_do_not_escape_the_diagnostic() {
    let diagnostic = CountersDiagnostic::new(true).unwrap();
    diagnostic.observe(
        SnapshotReason::StoreDrop,
        None,
        || panic!("snapshot"),
        |_| {},
    );
    diagnostic.observe(
        SnapshotReason::StoreDrop,
        None,
        || serde_json::json!({}),
        |_| panic!("sink"),
    );
    struct FailedWriter;
    impl std::io::Write for FailedWriter {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("diagnostic sink unavailable"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    diagnostic.observe(
        SnapshotReason::StoreDrop,
        None,
        || serde_json::json!({}),
        |record| {
            let _ = writeln!(FailedWriter, "STORAGE_PHASE {record}");
        },
    );
    assert_eq!(diagnostic.sequence.load(Ordering::Relaxed), 3);
}
