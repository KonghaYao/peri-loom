use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, ensure, Result};
use peri_loom_v1::session::{AdmissionError, Budget};

use super::support::{
    batch, expect_admission, expect_owner, short_budget, verify, wait_queue, Fixture,
};

#[test]
fn short_queue_budget_cancels_head_without_io_or_replay_and_tail_progresses() -> Result<()> {
    let fixture = Fixture::open()?;
    let holder = fixture.database.session()?;
    let head = Arc::new(fixture.database.session()?);
    let tail = Arc::new(fixture.database.session()?);
    holder.execute("BEGIN IMMEDIATE;")?;
    let physical = fixture.store.physical();
    thread::scope(|scope| -> Result<()> {
        let writer = head.clone();
        let started = Instant::now();
        let first =
            scope.spawn(move || writer.execute_with_budget(&batch(901, 1), &short_budget()));
        wait_queue(&fixture.database, &[head.admission_id()])?;
        let writer = tail.clone();
        let second = scope.spawn(move || writer.execute(&batch(902, 2)));
        wait_queue(
            &fixture.database,
            &[head.admission_id(), tail.admission_id()],
        )?;
        let error = first
            .join()
            .map_err(|_| anyhow!("queue head panicked"))?
            .unwrap_err();
        expect_admission(&error, AdmissionError::Deadline)?;
        ensure!(
            started.elapsed() < Duration::from_millis(400),
            "short admission deadline ignored"
        );
        wait_queue(&fixture.database, &[tail.admission_id()])?;
        expect_owner(&fixture.database, Some(holder.admission_id()))?;
        ensure!(
            fixture.store.physical() == physical,
            "queued failure dispatched object I/O"
        );
        holder.execute("ROLLBACK;")?;
        second
            .join()
            .map_err(|_| anyhow!("queue tail panicked"))??;
        Ok(())
    })?;
    verify(&fixture.database.session()?, &[(902, 2)])
}

#[test]
fn close_cancels_queued_head_without_releasing_other_owner() -> Result<()> {
    let fixture = Fixture::open()?;
    let holder = fixture.database.session()?;
    let head = Arc::new(fixture.database.session()?);
    let tail = Arc::new(fixture.database.session()?);
    holder.execute("BEGIN IMMEDIATE;")?;
    thread::scope(|scope| -> Result<()> {
        let writer = head.clone();
        let first = scope.spawn(move || writer.execute(&batch(1001, 1)));
        wait_queue(&fixture.database, &[head.admission_id()])?;
        let writer = tail.clone();
        let second = scope.spawn(move || writer.execute(&batch(1002, 2)));
        wait_queue(
            &fixture.database,
            &[head.admission_id(), tail.admission_id()],
        )?;
        let physical = fixture.store.physical();
        head.close()?;
        let error = first
            .join()
            .map_err(|_| anyhow!("closing queue head panicked"))?
            .unwrap_err();
        expect_admission(&error, AdmissionError::Closing)?;
        ensure!(
            fixture.store.physical() == physical,
            "queue cancellation dispatched object I/O"
        );
        wait_queue(&fixture.database, &[tail.admission_id()])?;
        expect_owner(&fixture.database, Some(holder.admission_id()))?;
        holder.execute("COMMIT;")?;
        second
            .join()
            .map_err(|_| anyhow!("queue tail panicked"))??;
        Ok(())
    })?;
    let physical = fixture.store.physical();
    let error = head.execute("SELECT 1;").unwrap_err();
    expect_admission(&error, AdmissionError::Closing)?;
    ensure!(
        fixture.store.physical() == physical,
        "closed session dispatched another call"
    );
    verify(&fixture.database.session()?, &[(1002, 2)])
}

#[test]
fn bounded_eight_ticket_queue_rejects_ninth_without_sql_dispatch() -> Result<()> {
    let fixture = Fixture::open()?;
    let holder = fixture.database.session()?;
    let sessions = (0..9)
        .map(|_| fixture.database.session().map(Arc::new))
        .collect::<Result<Vec<_>>>()?;
    holder.execute("BEGIN IMMEDIATE;")?;
    thread::scope(|scope| -> Result<()> {
        let mut workers = Vec::new();
        let mut queued = Vec::new();
        for (index, session) in sessions.iter().take(8).enumerate() {
            let identifier = 1101 + i64::try_from(index)?;
            let writer = session.clone();
            workers.push(scope.spawn(move || writer.execute(&batch(identifier, 1))));
            queued.push(session.admission_id());
            wait_queue(&fixture.database, &queued)?;
        }
        let physical = fixture.store.physical();
        let error = sessions[8].execute(&batch(1109, 9)).unwrap_err();
        expect_admission(&error, AdmissionError::QueueFull)?;
        ensure!(
            fixture.store.physical() == physical,
            "queue-full request dispatched object I/O"
        );
        wait_queue(&fixture.database, &queued)?;
        holder.execute("ROLLBACK;")?;
        for worker in workers {
            worker
                .join()
                .map_err(|_| anyhow!("queued writer panicked"))??;
        }
        Ok(())
    })?;
    let expected = (1101..=1108)
        .map(|identifier| (identifier, 1))
        .collect::<Vec<_>>();
    verify(&fixture.database.session()?, &expected)?;
    wait_queue(&fixture.database, &[])?;
    expect_owner(&fixture.database, None)
}

#[test]
fn queued_local_revoke_and_expiry_cancel_ticket_without_io_or_failed_business() -> Result<()> {
    for revoke in [false, true] {
        let fixture = Fixture::open()?;
        let holder = fixture.database.session()?;
        let contender = Arc::new(fixture.database.session()?);
        holder.execute("BEGIN IMMEDIATE;")?;
        thread::scope(|scope| -> Result<()> {
            let writer = contender.clone();
            let waiting = scope.spawn(move || writer.execute(&batch(1201, 4)));
            wait_queue(&fixture.database, &[contender.admission_id()])?;
            let physical = fixture.store.physical();
            let started = Instant::now();
            if revoke {
                fixture.store.revoke(true);
            } else {
                fixture.store.expire();
            }
            let error = waiting
                .join()
                .map_err(|_| anyhow!("fenced queue writer panicked"))?
                .unwrap_err();
            let expected = if revoke {
                "writer admission mock local ownership revoked"
            } else {
                "writer admission mock local lease expired"
            };
            ensure!(
                format!("{error:#}").contains(expected),
                "mock local failure was replaced"
            );
            ensure!(
                started.elapsed() < Duration::from_millis(300),
                "local check ignored in queue"
            );
            ensure!(
                fixture.store.physical() == physical,
                "invalidated ticket dispatched object I/O"
            );
            wait_queue(&fixture.database, &[])?;
            expect_owner(&fixture.database, Some(holder.admission_id()))?;
            Ok(())
        })?;
        fixture.store.revoke(false);
        fixture.store.renew_mock();
        holder.execute("ROLLBACK;")?;
        contender.execute(&batch(1202, 5))?;
        verify(&fixture.database.session()?, &[(1202, 5)])?;
    }
    Ok(())
}

#[test]
fn direct_database_write_joins_session_queue_instead_of_bypassing_holder() -> Result<()> {
    let fixture = Fixture::open()?;
    let holder = fixture.database.session()?;
    holder.execute("BEGIN IMMEDIATE;")?;
    thread::scope(|scope| -> Result<()> {
        let database = fixture.database.clone();
        let waiting = scope.spawn(move || database.execute(&batch(1301, 6)));
        let started = Instant::now();
        loop {
            let status = fixture.database.writer_admission_status()?;
            if status.queued.len() == 1 {
                ensure!(
                    status.owner == Some(holder.admission_id()),
                    "default connection stole owner"
                );
                break;
            }
            ensure!(
                started.elapsed() < Duration::from_secs(1),
                "default connection never joined queue"
            );
            thread::sleep(Duration::from_millis(1));
        }
        holder.execute("COMMIT;")?;
        waiting
            .join()
            .map_err(|_| anyhow!("default writer panicked"))??;
        Ok(())
    })?;
    expect_owner(&fixture.database, None)?;
    verify(&fixture.database.session()?, &[(1301, 6)])
}

#[test]
fn default_five_second_budget_is_a_typed_admission_deadline_not_native_busy() -> Result<()> {
    let fixture = Fixture::open()?;
    let holder = fixture.database.session()?;
    let contender = fixture.database.session()?;
    holder.execute("BEGIN IMMEDIATE;")?;
    let physical = fixture.store.physical();
    let started = Instant::now();
    let error = contender.execute(&batch(1401, 7)).unwrap_err();
    expect_admission(&error, AdmissionError::Deadline)?;
    ensure!(
        started.elapsed() >= Budget::default().max_query_duration
            && started.elapsed() < Duration::from_secs(6),
        "default queue request did not retain its five-second budget"
    );
    ensure!(
        fixture.store.physical() == physical,
        "deadline writer dispatched object I/O"
    );
    wait_queue(&fixture.database, &[])?;
    holder.execute("ROLLBACK;")?;
    verify(&fixture.database.session()?, &[])
}
