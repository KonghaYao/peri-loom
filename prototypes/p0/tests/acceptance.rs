use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use peri_loom_p0::storage::SegmentedStorage;
use serde_json::Value;

// Child spawns and in-process lock-taking opens go through `common`; see
// tests/common/mod.rs for why they share a fork gate.
mod common;

fn cli(root: &Path, arguments: &[&str]) -> Value {
    let output = common::spawn(|command| {
        command.arg(root).args(arguments);
    })
    .unwrap()
    .wait_with_output()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

struct CrashProcess(Child);

impl Drop for CrashProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn ready_process(root: &Path, arguments: &[&str]) -> CrashProcess {
    let mut process = CrashProcess(
        common::spawn(|command| {
            command
                .arg(root)
                .args(arguments)
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit());
        })
        .unwrap(),
    );
    let stdout = process.0.stdout.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(stdout).read_line(&mut line);
        let _ = sender.send((result, line));
    });
    let (result, line) = receiver.recv_timeout(Duration::from_secs(60)).unwrap();
    assert!(result.unwrap() > 0, "child exited before readiness");
    let message: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(message["ready"], true, "{message}");
    process
}

#[test]
fn checkpoint_kill_restart_and_lazy_query_without_database_file() {
    let directory = tempfile::tempdir().unwrap();
    cli(directory.path(), &["demo", "400"]);
    let process = ready_process(directory.path(), &["checkpoint-wait"]);
    drop(process);

    let storage = SegmentedStorage::open(directory.path()).unwrap();
    assert_eq!(storage.stats().segment_loads, 0);
    let manifest = storage.manifest();
    assert!(manifest.segments.len() > 1);
    assert!(manifest.logical_db_size > 128 * 1024);
    assert!(!directory.path().join("database").exists());
    assert!(!directory.path().join("database.db").exists());
    drop(storage);

    let rows = cli(
        directory.path(),
        &["query", "SELECT count(*) FROM p0_records"],
    );
    assert_eq!(rows[0][0], 400);
    let database = common::open_database(directory.path()).unwrap();
    database
        .query("SELECT payload FROM p0_records WHERE id = 1")
        .unwrap();
    let stats = database.storage().stats();
    assert!(stats.segment_loads > 0);
    assert!(stats.segment_loads < manifest.segments.len() as u64);
}

#[test]
fn committed_wal_tail_survives_sigkill_and_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    cli(directory.path(), &["demo", "400"]);
    let before = SegmentedStorage::open(directory.path()).unwrap().manifest();
    let process = ready_process(directory.path(), &["crash-write", "40"]);
    drop(process);
    let persisted = SegmentedStorage::open(directory.path()).unwrap().manifest();
    assert_eq!(persisted.generation, before.generation);
    assert_eq!(
        cli(
            directory.path(),
            &["query", "SELECT count(*) FROM p0_records"]
        )[0][0],
        440
    );
    cli(directory.path(), &["checkpoint"]);
    assert_eq!(
        cli(
            directory.path(),
            &["query", "SELECT count(*) FROM p0_records"]
        )[0][0],
        440
    );
}

#[test]
fn checkpoints_reuse_untouched_immutable_segments() {
    let directory = tempfile::tempdir().unwrap();
    let database = common::open_database(directory.path()).unwrap();
    database
        .execute("CREATE TABLE records(id INTEGER PRIMARY KEY, payload TEXT)")
        .unwrap();
    database.execute("BEGIN").unwrap();
    for record in 1..=400 {
        database
            .execute(&format!(
                "INSERT INTO records VALUES({record}, '{}')",
                "x".repeat(2048)
            ))
            .unwrap();
    }
    database.execute("COMMIT").unwrap();
    database.checkpoint().unwrap();
    let before = database.storage().manifest();
    database
        .execute("UPDATE records SET payload = 'changed' WHERE id = 1")
        .unwrap();
    database.checkpoint().unwrap();
    let after = database.storage().manifest();
    assert!(after.generation > before.generation);
    let reused = before
        .segments
        .iter()
        .filter(|(index, identifier)| after.segments.get(index) == Some(identifier))
        .count();
    assert!(reused > 0);
    assert!(reused < before.segments.len());
    drop(database);
    let database = common::open_database(directory.path()).unwrap();
    let rows = database
        .query("SELECT payload FROM records WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0].to_string(), "changed");
}

#[test]
fn a_second_process_cannot_open_an_active_database() {
    let directory = tempfile::tempdir().unwrap();
    cli(directory.path(), &["demo", "40"]);
    let process = ready_process(directory.path(), &["crash-write", "1"]);
    let output = common::spawn(|command| {
        command
            .arg(directory.path())
            .args(["query", "SELECT count(*) FROM p0_records"]);
    })
    .unwrap()
    .wait_with_output()
    .unwrap();
    assert!(!output.status.success());
    drop(process);
    assert_eq!(
        cli(
            directory.path(),
            &["query", "SELECT count(*) FROM p0_records"]
        )[0][0],
        41
    );
}

#[test]
fn rollback_and_wal_epoch_reuse_preserve_committed_state() {
    let directory = tempfile::tempdir().unwrap();
    let database = common::open_database(directory.path()).unwrap();
    database
        .execute("CREATE TABLE records(id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();
    database
        .execute("INSERT INTO records VALUES(1, 'first')")
        .unwrap();
    database.checkpoint().unwrap();
    let first = database.storage().manifest();
    assert!(first.applied_wal_position.frame_count > 0);
    database.execute("BEGIN").unwrap();
    database
        .execute("INSERT INTO records VALUES(2, 'rolled back')")
        .unwrap();
    database.execute("ROLLBACK").unwrap();
    database
        .execute("INSERT INTO records VALUES(3, 'second')")
        .unwrap();
    database.checkpoint().unwrap();
    let second = database.storage().manifest();
    assert!(second.generation > first.generation);
    drop(database);
    let rows = cli(
        directory.path(),
        &["query", "SELECT id FROM records ORDER BY id"],
    );
    assert_eq!(rows, serde_json::json!([[1], [3]]));
}

#[test]
fn truncated_wal_checkpoint_then_new_tail_recovers() {
    let directory = tempfile::tempdir().unwrap();
    cli(directory.path(), &["demo", "400"]);
    cli(
        directory.path(),
        &["exec", "PRAGMA wal_checkpoint(TRUNCATE)"],
    );
    assert_eq!(
        std::fs::metadata(directory.path().join("database-wal"))
            .unwrap()
            .len(),
        0
    );
    let process = ready_process(directory.path(), &["crash-write", "10"]);
    drop(process);
    assert_eq!(
        cli(
            directory.path(),
            &["query", "SELECT count(*) FROM p0_records"]
        )[0][0],
        410
    );
    cli(directory.path(), &["checkpoint"]);
    assert_eq!(
        cli(directory.path(), &["query", "PRAGMA integrity_check"]),
        serde_json::json!([["ok"]])
    );
}

#[test]
fn truncate_checkpoint_records_the_pre_restart_wal_generation() {
    let directory = tempfile::tempdir().unwrap();
    cli(directory.path(), &["demo", "400"]);
    let process = ready_process(directory.path(), &["crash-write", "10"]);
    drop(process);
    let wal = std::fs::read(directory.path().join("database-wal")).unwrap();
    let salts = [
        u32::from_be_bytes(wal[16..20].try_into().unwrap()),
        u32::from_be_bytes(wal[20..24].try_into().unwrap()),
    ];
    let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame_count = ((wal.len() - 32) / (page_size + 24)) as u64;
    cli(
        directory.path(),
        &["exec", "PRAGMA wal_checkpoint(TRUNCATE)"],
    );
    let after = SegmentedStorage::open(directory.path()).unwrap().manifest();
    assert_eq!(after.applied_wal_position.salts, salts);
    assert_eq!(after.applied_wal_position.frame_count, frame_count);
    assert_eq!(
        std::fs::metadata(directory.path().join("database-wal"))
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        cli(
            directory.path(),
            &["query", "SELECT count(*) FROM p0_records"]
        )[0][0],
        410
    );
    assert_eq!(
        cli(directory.path(), &["query", "PRAGMA integrity_check"]),
        serde_json::json!([["ok"]])
    );
}

#[test]
fn incomplete_wal_tail_does_not_hide_committed_records() {
    use std::io::Write;

    let directory = tempfile::tempdir().unwrap();
    cli(directory.path(), &["demo", "400"]);
    let process = ready_process(directory.path(), &["crash-write", "10"]);
    drop(process);
    let mut wal = std::fs::OpenOptions::new()
        .append(true)
        .open(directory.path().join("database-wal"))
        .unwrap();
    wal.write_all(b"incomplete frame").unwrap();
    wal.sync_all().unwrap();
    drop(wal);
    assert_eq!(
        cli(
            directory.path(),
            &["query", "SELECT count(*) FROM p0_records"]
        )[0][0],
        410
    );
    cli(directory.path(), &["checkpoint"]);
    assert_eq!(
        cli(directory.path(), &["query", "PRAGMA integrity_check"]),
        serde_json::json!([["ok"]])
    );
}
