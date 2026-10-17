//! End-to-end checks for the suspend-time pre-tidy on the real ownership drain path.
//!
//! The regression this pins down cannot be seen from `WorkerRuntime::drain` alone: the owner
//! moves the lease to `Draining` first, and the ownership store rejects new transactions in
//! that mode. A unit test that calls the worker directly never crosses that gate. These tests
//! use the real authority, a real Writer lease, a real session and `DrainDatabase`, and they
//! make the maintenance publication slow so the drain's classification and wait are
//! observable.
//!
//! The child process is needed because the classification is written to stderr by the
//! production log line; the parent asserts that the real drain reported
//! `completed_over_budget` rather than `failed_unconfirmed`, and that the drain did not return
//! before the paused authoritative write finished.

#[allow(dead_code)]
#[path = "p4_runtime_helpers/clock_gate.rs"]
mod clock_gate;
#[allow(dead_code)]
#[path = "p4_runtime_helpers/remote.rs"]
mod remote;

use std::time::{Duration, Instant};

use anyhow::{ensure, Result};
use peri_loom_v1::ownership::types::{AccessRole, OwnershipCommand};

const CHILD: &str = "PERI_OWNERSHIP_DRAIN_MAINTENANCE_CHILD";
const BEFORE: &str = "ownership_drain_pre_tidy_runs_before_the_session_closes";
const AFTER: &str = "ownership_drain_pre_tidy_runs_when_the_last_session_leaves";
const PAUSE: Duration = Duration::from_millis(300);
const BUDGET_MS: &str = "50";

#[test]
fn ownership_drain_pre_tidy_runs_before_the_session_closes() -> Result<()> {
    if std::env::var(CHILD).as_deref() != Ok("before") {
        return parent("before", BEFORE);
    }
    let fixture = fixture()?;
    let handle = fixture.runtime.open_session(&fixture.identifier)?;
    handle.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    drop(handle);

    // The first authoritative put after this point is the maintenance publication. A
    // pre-dispatch refusal would finish quickly and be logged as `failed_unconfirmed`.
    fixture.pause.arm(PAUSE);
    let started = Instant::now();
    fixture.runtime.command(OwnershipCommand::DrainDatabase {
        database: fixture.identifier.clone(),
        expected_epoch: 1,
    })?;
    let elapsed = started.elapsed();
    fixture.pause.disarm();
    ensure!(
        elapsed >= PAUSE,
        "the drain returned before the paused maintenance write completed: {elapsed:?}"
    );
    verify_after_detach(fixture)
}

#[test]
fn ownership_drain_pre_tidy_runs_when_the_last_session_leaves() -> Result<()> {
    if std::env::var(CHILD).as_deref() != Ok("after") {
        return parent("after", AFTER);
    }
    let fixture = fixture()?;
    let handle = fixture.runtime.open_session(&fixture.identifier)?;
    handle.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;

    fixture.pause.arm(PAUSE);
    let drain_started = Instant::now();
    fixture.runtime.command(OwnershipCommand::DrainDatabase {
        database: fixture.identifier.clone(),
        expected_epoch: 1,
    })?;
    let drain_elapsed = drain_started.elapsed();
    ensure!(
        drain_elapsed < PAUSE,
        "a drain with an open session must only mark the database draining: {drain_elapsed:?}"
    );

    // The last session leaving is the point where the retirement unit actually runs.
    let drop_started = Instant::now();
    drop(handle);
    let drop_elapsed = drop_started.elapsed();
    fixture.pause.disarm();
    ensure!(
        drop_elapsed >= PAUSE,
        "the last-session release returned before the paused maintenance write completed: \
         {drop_elapsed:?}"
    );
    verify_after_detach(fixture)
}

fn parent(scenario: &str, test: &str) -> Result<()> {
    let output = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", test, "--nocapture"])
        .env(CHILD, scenario)
        .output()?;
    ensure!(
        output.status.success(),
        "child drain test failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    ensure!(
        stderr.contains("outcome=completed_over_budget"),
        "the pre-tidy did not complete under the real drain path: {stderr}"
    );
    ensure!(
        stderr.contains("started=true"),
        "the completion was not tied to a started unit: {stderr}"
    );
    Ok(())
}

fn fixture() -> Result<remote::Fixture> {
    std::env::set_var("PERI_MAINTENANCE_SCHEDULE", "1");
    std::env::set_var("PERI_MAINTENANCE_SUSPEND_BUDGET_MS", BUDGET_MS);
    let fixture = remote::Fixture::new(AccessRole::Writer, 10_000)?;
    fixture.acquire(AccessRole::Writer)?;
    Ok(fixture)
}

fn verify_after_detach(fixture: remote::Fixture) -> Result<()> {
    // The suspension still completed and the row is available under the next epoch.
    let lease = fixture.grant_next(2)?;
    fixture.runtime.command(OwnershipCommand::AcquireDatabase {
        lease,
        role: AccessRole::Writer,
    })?;
    let handle = fixture.runtime.open_session(&fixture.identifier)?;
    ensure!(
        handle.query("SELECT id FROM items")?[0][0].to_string() == "1",
        "the pre-tidy lost the row"
    );
    drop(handle);
    Ok(())
}
