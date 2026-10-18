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
}

#[test]
fn enabled_sinks_move_the_snapshot() {
    set_enabled(true);
    let before = snapshot().expect("enabled snapshot");
    observe_root_publish(128, 3, 7);
    observe_root_publish_retry();
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
