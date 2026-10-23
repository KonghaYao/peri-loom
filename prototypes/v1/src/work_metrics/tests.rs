//! Verification that the counters move. The switch is process-wide and other tests in the
//! same binary may record at the same time once it is on, so every assertion over the
//! process-wide totals reads a delta and lower-bounds it. Exact per-thread accounting is
//! asserted where it belongs: on `RecoveryObservations` in `recovery/tests.rs`.

use super::*;

#[test]
fn armed_lock_guard_records_wait_and_hold() {
    let before = snapshot().unwrap_or_default();
    let mutex = std::sync::Mutex::new(0u64);
    // Armed directly: this test pins the accounting of one acquisition, not the switch, so
    // it must not depend on which other test set the environment-dependent state first.
    let wait = LockWait {
        domain: LockDomain::Storage,
        started: Some(Instant::now()),
    };
    {
        let mut guard = wait.acquired(mutex.lock().expect("test mutex"));
        *guard += 1;
    }
    let after = snapshot().unwrap_or_default();
    let storage = &after.locks["storage"];
    let before = &before.locks["storage"];
    assert!(storage.waits - before.waits >= 1);
    assert!(storage.holds - before.holds >= 1);
}

#[test]
fn an_unarmed_guard_holds_no_timer() {
    let mutex = std::sync::Mutex::new(0u64);
    let wait = LockWait {
        domain: LockDomain::Cache,
        started: None,
    };
    let mut guard = wait.acquired(mutex.lock().expect("test mutex"));
    // No clock was read and none will be read on drop: the guard this hands out carries no
    // hold timer, so the disabled path cannot reach a counter.
    assert!(guard.hold.is_none());
    *guard += 1;
}

#[test]
fn armed_execute_timer_records_the_span_it_bracketed() {
    set_enabled(true);
    let before = snapshot().expect("enabled snapshot");
    let timer = ExecuteTimer::begin();
    std::thread::sleep(Duration::from_millis(2));
    timer.finish(Some(9));
    let after = snapshot().expect("enabled snapshot");
    assert!(after.execution.native_execute_calls - before.execution.native_execute_calls >= 1);
    assert!(
        after.execution.native_execute_ns - before.execution.native_execute_ns >= 1_000_000,
        "a 2 ms span was not reported"
    );
    assert!(after.execution.native_execute_max_ns >= 1_000_000);
    assert!(after.execution.native_execute_steps - before.execution.native_execute_steps >= 9);
}

#[test]
fn armed_publish_timer_records_each_stage_and_the_attempt_total() {
    set_enabled(true);
    let before = snapshot().expect("enabled snapshot");
    {
        let mut timer = PublishTimer::begin();
        timer.stage(PublishStage::Prepare);
        std::thread::sleep(Duration::from_millis(2));
        timer.stage(PublishStage::Group);
        std::thread::sleep(Duration::from_millis(1));
    }
    let after = snapshot().expect("enabled snapshot");
    for stage in ["prepare", "group"] {
        assert!(
            after.metadata.publish_stage_calls[stage] - before.metadata.publish_stage_calls[stage]
                >= 1,
            "the timer did not charge {stage}"
        );
    }
    assert!(
        after.metadata.publish_stage_max_ns["prepare"] >= 1_000_000,
        "a 2 ms stage was not reported"
    );
    assert!(
        after.metadata.publish_total_ns - before.metadata.publish_total_ns >= 3_000_000,
        "the attempt total did not cover both stages"
    );
    assert!(after.metadata.publish_total_max_ns >= 3_000_000);
    assert!(
        after.metadata.publish_stage_ns["prepare"] - before.metadata.publish_stage_ns["prepare"]
            >= 1_000_000,
        "the per-stage duration was not reported"
    );
}

#[test]
fn armed_snapshot_timer_records_each_stage_and_the_read_total() {
    set_enabled(true);
    let before = snapshot().expect("enabled snapshot");
    {
        let mut timer = SnapshotTimer::begin();
        timer.stage(SnapshotStage::Store);
        std::thread::sleep(Duration::from_millis(2));
        timer.root_bytes(4096);
        timer.stage(SnapshotStage::Refs);
        std::thread::sleep(Duration::from_millis(1));
    }
    let after = snapshot().expect("enabled snapshot");
    for stage in ["store", "refs"] {
        assert!(
            after.metadata.snapshot_stage_calls[stage]
                - before.metadata.snapshot_stage_calls[stage]
                >= 1,
            "the timer did not charge {stage}"
        );
    }
    assert!(
        after.metadata.snapshot_stage_max_ns["store"] >= 1_000_000,
        "a 2 ms stage was not reported"
    );
    assert!(
        after.metadata.snapshot_total_ns - before.metadata.snapshot_total_ns >= 3_000_000,
        "the read total did not cover both stages"
    );
    assert!(after.metadata.snapshot_total_max_ns >= 3_000_000);
    assert!(
        after.metadata.snapshot_stage_ns["store"] - before.metadata.snapshot_stage_ns["store"]
            >= 1_000_000,
        "the per-stage duration was not reported"
    );
    assert!(
        after.metadata.snapshot_root_bytes - before.metadata.snapshot_root_bytes >= 4096,
        "the root size was not reported"
    );
    assert!(after.metadata.snapshot_root_max_bytes >= 4096);
}

#[test]
fn enabled_sinks_move_the_snapshot() {
    set_enabled(true);
    let before = snapshot().expect("enabled snapshot");
    observe_root_publish(128, 3, 7);
    observe_root_publish_retry();
    observe_root_encoding(
        4096,
        crate::authority::types::IdentityCounts {
            branches: 5,
            snapshots: 2,
            pin_identities: 3,
            branch_history_points: 11,
        },
    );
    observe_pin_release_failure();
    observe_wal_index_commit(64, 5);
    observe_wal_index_retry();
    observe_index_page_read();
    observe_index_page_write(32);
    let after = snapshot().expect("enabled snapshot");
    assert!(after.metadata.root_encoded_bytes - before.metadata.root_encoded_bytes >= 128);
    assert!(after.metadata.root_publish_attempts - before.metadata.root_publish_attempts >= 1);
    assert!(after.metadata.root_publish_retries - before.metadata.root_publish_retries >= 1);
    assert!(after.metadata.inline_pin_refs - before.metadata.inline_pin_refs >= 3);
    assert!(after.metadata.pin_release_failures - before.metadata.pin_release_failures >= 1);
    assert!(after.metadata.root_receipts - before.metadata.root_receipts >= 7);
    assert!(after.metadata.wal_index_commits - before.metadata.wal_index_commits >= 1);
    assert!(after.metadata.wal_index_bytes - before.metadata.wal_index_bytes >= 64);
    assert!(after.metadata.wal_index_receipts - before.metadata.wal_index_receipts >= 5);
    assert!(after.metadata.wal_index_retries - before.metadata.wal_index_retries >= 1);
    assert!(after.metadata.index_page_reads - before.metadata.index_page_reads >= 1);
    assert!(after.metadata.index_page_writes - before.metadata.index_page_writes >= 1);
    assert!(after.metadata.index_page_write_bytes - before.metadata.index_page_write_bytes >= 32);
    assert!(after.metadata.root_encoded_max_bytes >= 4096);
    assert!(after.metadata.inline_branches - before.metadata.inline_branches >= 5);
    assert!(after.metadata.inline_snapshots - before.metadata.inline_snapshots >= 2);
    assert!(after.metadata.inline_pin_identities - before.metadata.inline_pin_identities >= 3);
    assert!(after.metadata.branch_history_points - before.metadata.branch_history_points >= 11);
    assert!(after.metadata.max_inline_branches >= 5);
    assert!(after.metadata.max_inline_snapshots >= 2);
    assert!(after.metadata.max_inline_pin_identities >= 3);
    assert!(after.metadata.max_branch_history_points >= 11);
}

#[test]
fn enabled_result_sink_records_rows_and_json_bytes() {
    set_enabled(true);
    let before = snapshot().expect("enabled snapshot");
    observe_result(3, 128);
    let after = snapshot().expect("enabled snapshot");
    assert!(after.results.batches - before.results.batches >= 1);
    assert!(after.results.rows - before.results.rows >= 3);
    assert!(after.results.json_bytes - before.results.json_bytes >= 128);
}

#[test]
fn snapshot_serializes_every_group_and_reason() {
    set_enabled(true);
    let value = serde_json::to_value(snapshot().expect("enabled snapshot")).expect("serialize");
    for group in ["wal_workload", "metadata", "locks", "execution", "results"] {
        assert!(value.get(group).is_some(), "missing {group}");
    }
    for field in ["batches", "rows", "json_bytes"] {
        assert!(
            value["results"].get(field).is_some(),
            "missing results.{field}"
        );
    }
    for reason in RESET_NAMES {
        assert!(value["wal_workload"]["scan_resets"].get(reason).is_some());
    }
    for reason in STOP_NAMES {
        assert!(value["wal_workload"]["scan_stops"].get(reason).is_some());
    }
    for resolution in POSITION_NAMES {
        assert!(value["wal_workload"]["append_position"]
            .get(resolution)
            .is_some());
    }
    for domain in LOCK_NAMES {
        assert!(value["locks"].get(domain).is_some());
    }
}

/// Holds the process-wide path for one test and restores whatever was there before. The lock
/// stays held until the restore, so two tests cannot interleave their environment changes.
struct SnapshotPathEnv {
    previous: Option<std::ffi::OsString>,
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl SnapshotPathEnv {
    fn set(value: Option<&str>) -> Self {
        let lock = SNAPSHOT_ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = std::env::var_os(SNAPSHOT_PATH_ENV);
        match value {
            Some(value) => std::env::set_var(SNAPSHOT_PATH_ENV, value),
            None => std::env::remove_var(SNAPSHOT_PATH_ENV),
        }
        Self {
            previous,
            _lock: lock,
        }
    }
}

impl Drop for SnapshotPathEnv {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => std::env::set_var(SNAPSHOT_PATH_ENV, value),
            None => std::env::remove_var(SNAPSHOT_PATH_ENV),
        }
    }
}

fn snapshot_directory_entries(directory: &std::path::Path) -> Vec<String> {
    std::fs::read_dir(directory)
        .expect("read snapshot directory")
        .map(|entry| {
            entry
                .expect("directory entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect()
}

#[test]
fn exit_snapshot_is_written_atomically_with_the_expected_schema() {
    set_enabled(true);
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("metrics.json");
    observe_append(AppendPosition::Reused, 4096, 512, 2);

    let _env = SnapshotPathEnv::set(Some(path.to_str().expect("utf-8 path")));
    write_exit_snapshot_if_configured().expect("write exit snapshot");

    let encoded = std::fs::read(&path).expect("snapshot exists");
    let value: serde_json::Value = serde_json::from_slice(&encoded).expect("snapshot parses");
    assert_eq!(value["schema_version"], serde_json::json!(1));
    assert!(value["generated_at_unix_ms"]
        .as_u64()
        .is_some_and(|millis| millis > 0));
    assert_eq!(
        value["process"]["pid"],
        serde_json::json!(std::process::id())
    );
    assert!(value["process"]["version"].is_string());
    assert_eq!(value["work_metrics_enabled"], serde_json::json!(true));
    assert_eq!(value["group_phases"]["available"], serde_json::json!(false));

    let phases = value["phases"].as_array().expect("phases array");
    assert_eq!(phases.len(), PublishStage::ALL.len());
    for (phase, stage) in phases.iter().zip(PublishStage::ALL) {
        assert_eq!(phase["name"], serde_json::json!(stage.name()));
        for field in ["count", "total_ns", "max_ns"] {
            assert!(
                phase[field].is_u64(),
                "phase {} is missing {field}",
                stage.name()
            );
        }
    }

    let counters = value["counters"].as_array().expect("counters array");
    let counter = |name: &str| {
        counters
            .iter()
            .find(|counter| counter["name"] == serde_json::json!(name))
            .map(|counter| counter["value"].as_u64().expect("u64 counter"))
    };
    assert!(counter("work.wal_workload.append_calls").expect("append counter") >= 1);
    assert!(counter("work.wal_workload.append_position.reused").expect("position counter") >= 1);
    assert!(
        counter("object_store.get.total").is_some(),
        "object store counters are part of the document"
    );

    let entries = snapshot_directory_entries(directory.path());
    assert!(
        entries.iter().all(|name| !name.contains(".tmp")),
        "temporary file left behind: {entries:?}"
    );
    assert_eq!(entries, vec!["metrics.json".to_owned()]);
}

#[test]
fn exit_snapshot_stays_off_when_the_path_is_unset_or_empty() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("metrics.json");

    {
        let _env = SnapshotPathEnv::set(Some(path.to_str().expect("utf-8 path")));
        write_exit_snapshot_if_configured().expect("write exit snapshot");
        assert!(path.exists());
    }
    std::fs::remove_file(&path).expect("remove snapshot");

    {
        let _env = SnapshotPathEnv::set(None);
        write_exit_snapshot_if_configured().expect("unset path is a no-op");
    }
    assert!(!path.exists(), "unset path must not write the old file");

    {
        let _env = SnapshotPathEnv::set(Some(""));
        write_exit_snapshot_if_configured().expect("empty path is a no-op");
    }
    assert!(!path.exists(), "empty path must be treated as unset");
    assert!(snapshot_directory_entries(directory.path()).is_empty());
}

#[test]
fn exit_snapshot_failure_is_reported_and_leaves_no_file() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("missing").join("metrics.json");

    let _env = SnapshotPathEnv::set(Some(path.to_str().expect("utf-8 path")));
    let error = write_exit_snapshot_if_configured().expect_err("missing directory must fail");
    assert!(
        format!("{error:#}").contains("metrics.json"),
        "unexpected error: {error:#}"
    );
    assert!(!path.exists());
    assert!(snapshot_directory_entries(directory.path()).is_empty());
}

#[test]
fn exit_snapshot_counter_names_are_stable() {
    set_enabled(true);
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("metrics.json");
    let _env = SnapshotPathEnv::set(Some(path.to_str().expect("utf-8 path")));
    write_exit_snapshot_if_configured().expect("write exit snapshot");

    let value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).expect("read snapshot")).expect("parse");
    let counters = value["counters"].as_array().expect("counters array");
    let names = counters
        .iter()
        .map(|counter| counter["name"].as_str().expect("counter name"))
        .collect::<Vec<_>>();
    for name in [
        "work.wal_workload.append_calls",
        "work.wal_workload.scan_resets.header_changed",
        "work.wal_workload.append_position.not_covered",
        "work.metadata.root_publish_attempts",
        "work.metadata.root_publish_retries",
        "work.metadata.publish_stage_calls.snapshot",
        "work.metadata.publish_stage_calls.group",
        "work.metadata.publish_stage_max_ns.recover",
        "work.locks.storage.waits",
        "work.gates.operation.timeouts",
        "work.execution.native_execute_calls",
        "work.results.batches",
        "work.maintenance.wal_budget_bytes",
        "work.maintenance.reader_block_events",
        "work.gc.requests",
        "object_store.get.total",
        "object_store.put.total",
        "object_store.put.bytes",
        "object_store.wal_durability.total",
        "object_store.cold_open.total",
    ] {
        assert!(names.contains(&name), "counter name {name} disappeared");
    }
}

#[test]
fn exit_snapshot_reports_object_store_counters_after_operations() {
    set_enabled(true);
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("metrics.json");
    crate::metrics::Metrics::global().record(
        crate::metrics::Operation::Put,
        Duration::from_millis(1),
        true,
        64,
    );

    let _env = SnapshotPathEnv::set(Some(path.to_str().expect("utf-8 path")));
    write_exit_snapshot_if_configured().expect("write exit snapshot");

    let value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).expect("read snapshot")).expect("parse");
    let counters = value["counters"].as_array().expect("counters array");
    let counter = |name: &str| {
        counters
            .iter()
            .find(|counter| counter["name"] == serde_json::json!(name))
            .map(|counter| counter["value"].as_u64().expect("u64 counter"))
    };
    assert!(counter("object_store.put.total").expect("put counter") >= 1);
    assert!(counter("object_store.put.bytes").expect("put bytes") >= 64);
}
