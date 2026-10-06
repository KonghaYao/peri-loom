use super::identifier;
use anyhow::{ensure, Context, Result};
use peri_loom_v1::{
    authority::Authority, capabilities::Capabilities, object_store::LocalObjectStore,
};
use serde_json::{json, Value};
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    os::unix::process::ExitStatusExt,
    path::Path,
    process::{Child, ChildStdin, Command, ExitStatus, Stdio},
    sync::{mpsc, Arc},
    thread,
    time::{Duration, Instant},
};

struct Worker {
    child: Child,
    input: Option<ChildStdin>,
    replies: mpsc::Receiver<std::io::Result<String>>,
    sequence: u64,
}

impl Worker {
    fn start(store: &Path, cache: &Path) -> Result<Self> {
        let mut child = Command::new(env!("CARGO_BIN_EXE_peri-loom-v1"))
            .arg("worker")
            .arg(store)
            .arg(cache)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let input = child.stdin.take();
        let output = child.stdout.take().context("worker stdout missing")?;
        let (sender, replies) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        Ok(Self {
            child,
            input,
            replies,
            sequence: 0,
        })
    }

    fn request(&mut self, mut request: Value) -> Result<Value> {
        self.sequence += 1;
        request["id"] = json!(self.sequence);
        let input = self.input.as_mut().context("worker input closed")?;
        serde_json::to_writer(&mut *input, &request)?;
        writeln!(input)?;
        input.flush()?;
        let line = self
            .replies
            .recv_timeout(Duration::from_secs(8))
            .context("worker response timeout/disconnect")??;
        let response: Value = serde_json::from_str(&line)?;
        ensure!(
            response["id"] == request["id"],
            "response ID mismatch: {response}"
        );
        Ok(response)
    }

    fn success(&mut self, request: Value) -> Result<Value> {
        let response = self.request(request)?;
        ensure!(
            response["ok"] == true,
            "worker rejected request: {response}"
        );
        Ok(response)
    }

    fn open(&mut self, database: Value) -> Result<Value> {
        Ok(self.success(json!({"op":"open","database":database,"epoch":0}))?["session"].clone())
    }

    fn sql(&mut self, session: &Value, op: &str, sql: &str) -> Result<Value> {
        Ok(self.success(json!({"op":op,"session":session,"sql":sql}))?["result"].clone())
    }

    fn wait(&mut self) -> Result<ExitStatus> {
        let start = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            ensure!(
                start.elapsed() < Duration::from_secs(8),
                "worker exit timeout"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn eof(&mut self) -> Result<()> {
        drop(self.input.take());
        ensure!(self.wait()?.success(), "EOF worker failed");
        Ok(())
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn eof_aborts_writer_with_live_reader_and_protocol_errors_do_not_leak_sessions() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = root.path().join("store");
    let cache = root.path().join("cache");
    let database = serde_json::to_value(identifier("eof", "db"))?;
    let mut worker = Worker::start(&store, &cache)?;
    let first = worker.open(database.clone())?;
    worker.sql(
        &first,
        "exec",
        "CREATE TABLE records(id INTEGER PRIMARY KEY); INSERT INTO records VALUES(1)",
    )?;
    let reader = worker.open(database.clone())?;
    worker.sql(&reader, "exec", "BEGIN")?;
    assert_eq!(
        worker.sql(&reader, "query", "SELECT count(*) FROM records")?,
        json!([[1]])
    );
    worker.sql(
        &first,
        "exec",
        "BEGIN IMMEDIATE; INSERT INTO records VALUES(2)",
    )?;
    for request in [
        json!({"op":"open","database":database,"epoch":0,"session":"invalid"}),
        json!({"op":"open","database":{"namespace":{"tenant":"../eof","database":"db"},"branch":"main"},"epoch":0}),
        json!({"op":"query","session":first,"sql":"SELECT 1","unexpected":true}),
    ] {
        assert_eq!(worker.request(request)?["ok"], false);
    }
    assert_eq!(
        worker.success(json!({"op":"status"}))?["result"]["active_sessions"],
        2
    );
    worker.eof()?;
    fs::remove_dir_all(&cache)?;
    let mut fresh = Worker::start(&store, &cache)?;
    let session = fresh.open(database)?;
    assert_eq!(
        fresh.sql(&session, "query", "SELECT id FROM records")?,
        json!([[1]])
    );
    fresh.sql(&session, "exec", "INSERT INTO records VALUES(3)")?;
    assert_eq!(
        fresh.sql(&session, "query", "PRAGMA integrity_check")?,
        json!([["ok"]])
    );
    fresh.eof()?;
    Ok(())
}

#[test]
fn sigkill_after_compacted_ack_excludes_dirty_tail_and_preserves_old_pitr() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = root.path().join("store");
    let cache = root.path().join("old-cache");
    let new_cache = root.path().join("fresh-cache");
    let id = identifier("kill", "large");
    let database = serde_json::to_value(&id)?;
    let mut worker = Worker::start(&store, &cache)?;
    let session = worker.open(database.clone())?;
    let mut sql =
        String::from("CREATE TABLE records(id INTEGER PRIMARY KEY, payload BLOB); BEGIN;");
    for number in 0..320 {
        sql.push_str(&format!(
            "INSERT INTO records VALUES({number}, randomblob(8192));"
        ));
    }
    sql.push_str("COMMIT");
    worker.sql(&session, "exec", &sql)?;
    let authority = Authority::open(
        Arc::new(LocalObjectStore::open(&store)?),
        id.namespace.clone(),
    )?;
    let capabilities = Capabilities::new(authority.clone());
    let old = capabilities.snapshot("main", "before-compaction")?;
    worker.sql(&session, "exec", "INSERT INTO records VALUES(400, X'AA')")?;
    worker.success(json!({"op":"compact","session":session}))?;
    let compacted_sequence = authority.snapshot()?.root.branches["main"].current_sequence;
    worker.sql(&session, "exec", "BEGIN; INSERT INTO records VALUES(401, X'BB'); INSERT INTO records VALUES(402, X'CC'); COMMIT")?;
    let ack_sequence = authority.snapshot()?.root.branches["main"].current_sequence;
    ensure!(
        ack_sequence > compacted_sequence,
        "ACK had no durable commit point"
    );
    worker.sql(
        &session,
        "exec",
        "BEGIN IMMEDIATE; INSERT INTO records VALUES(999, randomblob(8192))",
    )?;
    worker.child.kill()?;
    let status = worker.wait()?;
    assert_eq!(status.signal(), Some(9));
    eprintln!("P3 verifier actual SIGKILL signal=9 compact_sequence={compacted_sequence} ack_sequence={ack_sequence}");
    fs::remove_dir_all(&cache)?;
    assert!(!new_cache.exists());
    let mut fresh = Worker::start(&store, &new_cache)?;
    assert_eq!(
        fresh.success(json!({"op":"status"}))?["result"]["loaded_databases"],
        0
    );
    let recovered = fresh.open(database)?;
    let resources =
        fresh.success(json!({"op":"status"}))?["result"]["databases"][0]["resources"].clone();
    let opening_loads = resources["segment_loads"]
        .as_u64()
        .context("segment loads missing")?;
    assert!(
        opening_loads <= 4,
        "cold startup loaded bulk segments: {resources}"
    );
    assert!(
        resources["wal_bytes"].as_u64().unwrap() < 256 * 1024,
        "compacted cold recovery replayed bulk history: {resources}"
    );
    assert_eq!(
        fresh.sql(
            &recovered,
            "query",
            "SELECT id,length(payload) FROM records WHERE id >= 400 ORDER BY id"
        )?,
        json!([[400, 1], [401, 1], [402, 1]])
    );
    assert_eq!(
        fresh.sql(
            &recovered,
            "query",
            "SELECT length(payload) FROM records WHERE id=319"
        )?,
        json!([[8192]])
    );
    let point = capabilities.pitr("main", old.sequence)?;
    capabilities.branch("main", "past")?;
    capabilities.restore_point("past", &point)?;
    let mut past_id = id.clone();
    past_id.branch = "past".to_owned();
    let past = fresh.open(serde_json::to_value(past_id)?)?;
    assert_eq!(
        fresh.sql(&past, "query", "SELECT count(*),max(id) FROM records")?,
        json!([[320, 319]])
    );
    assert_eq!(
        fresh.sql(&recovered, "query", "SELECT count(*) FROM records")?,
        json!([[323]])
    );
    assert_eq!(
        fresh.sql(&recovered, "query", "PRAGMA integrity_check")?,
        json!([["ok"]])
    );
    fresh.sql(&recovered, "exec", "INSERT INTO records VALUES(403, X'DD')")?;
    fresh.eof()?;
    eprintln!("P3 verifier recovery opening_loads={opening_loads} resources={resources} old_pitr_sequence={}", old.sequence);
    Ok(())
}
