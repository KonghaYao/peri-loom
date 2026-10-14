use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, ensure, Context, Result};
use peri_loom_v1::session::AdmissionError;

use super::support::{
    batch, expect_admission, expect_owner, short_budget, verify, wait_queue, Fixture,
};

#[test]
fn confirmed_owner_close_or_drop_aborts_uncommitted_effects_then_waiter_progresses() -> Result<()> {
    for close in [false, true] {
        let fixture = Fixture::open()?;
        let holder = fixture.database.session()?;
        let contender = Arc::new(fixture.database.session()?);
        holder.execute(
            "BEGIN IMMEDIATE; INSERT INTO ledger VALUES(1501,10); \
             UPDATE account SET balance=balance+10 WHERE id=1;",
        )?;
        thread::scope(|scope| -> Result<()> {
            let writer = contender.clone();
            let waiting = scope.spawn(move || writer.execute(&batch(1502, 11)));
            wait_queue(&fixture.database, &[contender.admission_id()])?;
            let started = Instant::now();
            if close {
                holder.close()?;
            }
            drop(holder);
            waiting
                .join()
                .map_err(|_| anyhow!("post-cleanup writer panicked"))??;
            ensure!(
                started.elapsed() < Duration::from_secs(2),
                "cleanup failed to unblock writer"
            );
            Ok(())
        })?;
        expect_owner(&fixture.database, None)?;
        wait_queue(&fixture.database, &[])?;
        verify(&fixture.database.session()?, &[(1502, 11)])?;
    }
    Ok(())
}

#[test]
fn maintenance_rejects_live_writer_and_queue_without_blocking_owner_rollback() -> Result<()> {
    let fixture = Fixture::open()?;
    let holder = fixture.database.session()?;
    let contender = Arc::new(fixture.database.session()?);
    holder.execute("BEGIN IMMEDIATE;")?;
    thread::scope(|scope| -> Result<()> {
        let writer = contender.clone();
        let waiting = scope.spawn(move || writer.execute(&batch(1601, 12)));
        wait_queue(&fixture.database, &[contender.admission_id()])?;
        let physical = fixture.store.physical();
        let started = Instant::now();
        let error = fixture.database.checkpoint().unwrap_err();
        expect_admission(&error, AdmissionError::Conflict)?;
        let error = fixture.database.compact_wal().unwrap_err();
        expect_admission(&error, AdmissionError::Conflict)?;
        ensure!(
            started.elapsed() < Duration::from_millis(300),
            "maintenance waited behind owner"
        );
        ensure!(
            fixture.store.physical() == physical,
            "failed maintenance dispatched object I/O"
        );
        expect_owner(&fixture.database, Some(holder.admission_id()))?;
        holder.execute("ROLLBACK;")?;
        waiting
            .join()
            .map_err(|_| anyhow!("maintenance-blocked writer panicked"))??;
        Ok(())
    })?;
    fixture.database.checkpoint()?;
    fixture.database.compact_wal()?;
    fixture.database.execute(&batch(1602, 13))?;
    verify(&fixture.database.session()?, &[(1601, 12), (1602, 13)])
}

#[test]
fn same_session_mutex_wait_uses_budget_from_call_entry() -> Result<()> {
    let fixture = Fixture::open()?;
    let session = Arc::new(fixture.database.session()?);
    let (reached, resume) = fixture.store.block_statement_entry("writer-mutex-owner");
    thread::scope(|scope| -> Result<()> {
        let reader = session.clone();
        let active = thread::Builder::new()
            .name("writer-mutex-owner".into())
            .spawn_scoped(scope, move || reader.query("SELECT 1;"))?;
        reached
            .recv_timeout(Duration::from_secs(1))
            .context("native statement entry not observed")?;
        let physical = fixture.store.physical();
        let started = Instant::now();
        let result = session.execute_with_budget(&batch(1701, 14), &short_budget());
        let released = resume.send(());
        let active_result = active
            .join()
            .map_err(|_| anyhow!("active session caller panicked"))?;
        released.context("statement gate release failed")?;
        active_result?;
        let error = result.unwrap_err();
        expect_admission(&error, AdmissionError::Deadline)?;
        ensure!(
            started.elapsed() < Duration::from_millis(400),
            "session mutex installed a fresh budget"
        );
        ensure!(
            fixture.store.physical() == physical,
            "expired session lane dispatched object I/O"
        );
        Ok(())
    })?;
    verify(&fixture.database.session()?, &[])
}

#[test]
fn explicit_close_of_deferred_reader_does_not_release_different_writer() -> Result<()> {
    let fixture = Fixture::open()?;
    let reader = fixture.database.session()?;
    let holder = fixture.database.session()?;
    reader.execute("BEGIN;")?;
    verify(&reader, &[])?;
    holder.execute("BEGIN IMMEDIATE;")?;
    reader.close()?;
    expect_owner(&fixture.database, Some(holder.admission_id()))?;
    holder.execute("ROLLBACK;")?;
    expect_owner(&fixture.database, None)?;
    fixture.database.execute(&batch(1801, 15))?;
    verify(&fixture.database.session()?, &[(1801, 15)])
}
