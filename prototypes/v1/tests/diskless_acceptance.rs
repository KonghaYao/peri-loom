use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use serde_json::Value;

fn cli(store: &Path, cache: &Path, arguments: &[&str]) -> Result<Value> {
    let result = Command::new(env!("CARGO_BIN_EXE_peri-loom-v1"))
        .arg(store)
        .arg(cache)
        .args(["tenant", "database"])
        .args(arguments)
        .output()?;
    ensure!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    Ok(serde_json::from_slice(&result.stdout)?)
}

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn ready(store: &Path, cache: &Path, arguments: &[&str]) -> Result<Process> {
    let mut process = Process(
        Command::new(env!("CARGO_BIN_EXE_peri-loom-v1"))
            .arg(store)
            .arg(cache)
            .args(["tenant", "database"])
            .args(arguments)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?,
    );
    let stdout = process.0.stdout.take().context("missing stdout")?;
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(stdout).read_line(&mut line);
        let _ = sender.send((result, line));
    });
    let (result, line) = receiver.recv_timeout(Duration::from_secs(60))?;
    ensure!(result? > 0, "child exited before readiness");
    let value: Value = serde_json::from_str(&line)?;
    ensure!(value["ready"] == true, "missing readiness: {value}");
    Ok(process)
}

#[test]
fn fresh_worker_recovers_committed_remote_wal_after_entire_cache_disposal() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = root.path().join("authoritative-objects");
    let first_cache = root.path().join("worker-a");
    cli(&store, &first_cache, &["demo", "400"])?;
    let inspection = cli(&store, &first_cache, &["inspect"])?;
    ensure!(
        inspection["manifest"]["segments"]
            .as_object()
            .context("mapping missing")?
            .len()
            > 1,
        "database is not segmented"
    );
    ensure!(
        inspection["stats"]["segment_loads"] == 0,
        "storage open prefetched data"
    );
    let mut process = ready(&store, &first_cache, &["crash-write", "40"])?;
    process.0.kill()?;
    let killed = process.0.wait()?;
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        ensure!(killed.signal() == Some(9), "not a SIGKILL");
    }
    std::fs::remove_dir_all(&first_cache)?;
    let fresh_cache = root.path().join("worker-b-empty");
    ensure!(!fresh_cache.exists(), "cold cache unexpectedly exists");
    ensure!(
        cli(
            &store,
            &fresh_cache,
            &["query", "SELECT count(*) FROM records"]
        )? == serde_json::json!([[440]]),
        "remote committed tail lost"
    );
    ensure!(
        !fresh_cache.join("database").exists() && !fresh_cache.join("database-wal").exists(),
        "local authoritative DB/WAL materialized"
    );
    cli(&store, &fresh_cache, &["checkpoint"])?;
    ensure!(
        cli(&store, &fresh_cache, &["query", "PRAGMA integrity_check"])?
            == serde_json::json!([["ok"]]),
        "integrity error"
    );
    Ok(())
}
