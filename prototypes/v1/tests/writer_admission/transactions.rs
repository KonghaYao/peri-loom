use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, ensure, Result};
use peri_loom_v1::session::{AdmissionError, Budget};
use turso_core::LimboError;

use super::support::{batch, expect_owner, verify, wait_queue, Fixture};

#[test]
fn fifo_uses_admitted_ticket_order_and_each_business_batch_executes_once() -> Result<()> {
    let fixture = Fixture::open()?;
    let holder = fixture.database.session()?;
    holder.execute(
        "CREATE TABLE admitted_order(position INTEGER PRIMARY KEY AUTOINCREMENT, business_id INTEGER UNIQUE);",
    )?;
    holder.execute("BEGIN IMMEDIATE;")?;
    expect_owner(&fixture.database, Some(holder.admission_id()))?;
    let sessions = (0..3)
        .map(|_| fixture.database.session().map(Arc::new))
        .collect::<Result<Vec<_>>>()?;
    thread::scope(|scope| -> Result<()> {
        let mut workers = Vec::new();
        let mut admitted = Vec::new();
        for (index, session) in sessions.iter().enumerate() {
            let identifier = 401 + i64::try_from(index)?;
            let writer = session.clone();
            workers.push(scope.spawn(move || {
                writer.execute(&format!(
                    "BEGIN IMMEDIATE; INSERT INTO admitted_order(business_id) VALUES({identifier}); \
                     INSERT INTO ledger VALUES({identifier},1); \
                     UPDATE account SET balance=balance+1 WHERE id=1; COMMIT;"
                ))
            }));
            admitted.push(session.admission_id());
            wait_queue(&fixture.database, &admitted)?;
        }
        verify(&fixture.database.session()?, &[])?;
        holder.execute("COMMIT;")?;
        for worker in workers {
            worker.join().map_err(|_| anyhow!("writer panicked"))??;
        }
        Ok(())
    })?;
    expect_owner(&fixture.database, None)?;
    wait_queue(&fixture.database, &[])?;
    let rows = fixture
        .database
        .query("SELECT business_id FROM admitted_order ORDER BY position;")?;
    let order = rows
        .iter()
        .map(|row| row[0].to_string())
        .collect::<Vec<_>>();
    ensure!(
        order == ["401", "402", "403"],
        "actual execution violated ticket FIFO"
    );
    verify(
        &fixture.database.session()?,
        &[(401, 1), (402, 1), (403, 1)],
    )
}

#[test]
fn constraint_and_savepoint_rollback_keep_owner_until_explicit_transaction_end() -> Result<()> {
    for commit in [false, true] {
        let fixture = Fixture::open()?;
        let holder = fixture.database.session()?;
        let contender = Arc::new(fixture.database.session()?);
        holder.execute(
            "BEGIN IMMEDIATE; INSERT INTO ledger VALUES(501,5); \
             UPDATE account SET balance=balance+5 WHERE id=1; \
             SAVEPOINT suffix; INSERT INTO ledger VALUES(502,6); \
             UPDATE account SET balance=balance+6 WHERE id=1;",
        )?;
        let error = holder
            .execute("INSERT INTO ledger VALUES(501,99);")
            .unwrap_err();
        ensure!(
            matches!(
                error.downcast_ref::<LimboError>(),
                Some(LimboError::Constraint(..))
            ),
            "constraint did not preserve native cause"
        );
        ensure!(
            holder.in_transaction()?,
            "constraint aborted interactive transaction"
        );
        expect_owner(&fixture.database, Some(holder.admission_id()))?;
        thread::scope(|scope| -> Result<()> {
            let writer = contender.clone();
            let waiting = scope.spawn(move || writer.execute(&batch(503, 7)));
            wait_queue(&fixture.database, &[contender.admission_id()])?;
            holder.execute("ROLLBACK TO suffix; RELEASE suffix;")?;
            expect_owner(&fixture.database, Some(holder.admission_id()))?;
            wait_queue(&fixture.database, &[contender.admission_id()])?;
            ensure!(
                !waiting.is_finished(),
                "savepoint cleanup prematurely released owner"
            );
            verify(&fixture.database.session()?, &[])?;
            holder.execute(if commit { "COMMIT;" } else { "ROLLBACK;" })?;
            waiting.join().map_err(|_| anyhow!("writer panicked"))??;
            Ok(())
        })?;
        expect_owner(&fixture.database, None)?;
        let expected = if commit {
            vec![(501, 5), (503, 7)]
        } else {
            vec![(503, 7)]
        };
        verify(&fixture.database.session()?, &expected)?;
    }
    Ok(())
}

#[test]
fn deferred_transaction_without_snapshot_first_write_enters_fifo() -> Result<()> {
    let fixture = Fixture::open()?;
    let holder = fixture.database.session()?;
    let contender = Arc::new(fixture.database.session()?);
    contender.execute("BEGIN DEFERRED;")?;
    expect_owner(&fixture.database, None)?;
    holder.execute("BEGIN IMMEDIATE;")?;
    thread::scope(|scope| -> Result<()> {
        let writer = contender.clone();
        let waiting = scope.spawn(move || {
            writer.execute(
                "INSERT INTO ledger VALUES(601,8); UPDATE account SET balance=balance+8 WHERE id=1; COMMIT;",
            )
        });
        wait_queue(&fixture.database, &[contender.admission_id()])?;
        holder.execute("COMMIT;")?;
        waiting.join().map_err(|_| anyhow!("writer panicked"))??;
        Ok(())
    })?;
    ensure!(
        !contender.in_transaction()?,
        "deferred writer did not commit"
    );
    verify(&fixture.database.session()?, &[(601, 8)])
}

#[test]
fn read_snapshot_promotion_conflicts_immediately_then_true_busy_snapshot_is_retained() -> Result<()>
{
    let fixture = Fixture::open()?;
    let reader = fixture.database.session()?;
    let holder = fixture.database.session()?;
    reader.execute("BEGIN;")?;
    verify(&reader, &[])?;
    holder.execute(
        "BEGIN IMMEDIATE; INSERT INTO ledger VALUES(701,2); UPDATE account SET balance=balance+2 WHERE id=1;",
    )?;
    let physical = fixture.store.physical();
    let started = Instant::now();
    let error = reader
        .execute_with_budget(
            "INSERT INTO ledger VALUES(702,3);",
            &Budget {
                max_query_duration: Duration::from_millis(200),
                ..Budget::default()
            },
        )
        .unwrap_err();
    ensure!(
        error.downcast_ref::<AdmissionError>() == Some(&AdmissionError::Conflict)
            || matches!(
                error.downcast_ref::<LimboError>(),
                Some(LimboError::BusySnapshot)
            ),
        "snapshot promotion was queued or returned an unrelated cause"
    );
    ensure!(
        started.elapsed() < Duration::from_millis(300),
        "promotion waited for owner"
    );
    ensure!(
        fixture.store.physical() == physical,
        "conflicted promotion dispatched object I/O"
    );
    wait_queue(&fixture.database, &[])?;
    expect_owner(&fixture.database, Some(holder.admission_id()))?;
    ensure!(
        reader.in_transaction()?,
        "promotion reopened or aborted read transaction"
    );
    verify(&reader, &[])?;
    holder.execute("COMMIT;")?;
    let error = reader
        .execute("INSERT INTO ledger VALUES(703,4);")
        .unwrap_err();
    ensure!(
        matches!(
            error.downcast_ref::<LimboError>(),
            Some(LimboError::BusySnapshot)
        ),
        "stale native read snapshot was silently reopened"
    );
    ensure!(
        reader.in_transaction()?,
        "BusySnapshot discarded interactive transaction"
    );
    verify(&reader, &[])?;
    reader.execute("ROLLBACK;")?;
    reader.execute(&batch(704, 5))?;
    verify(&fixture.database.session()?, &[(701, 2), (704, 5)])
}

#[test]
fn query_returning_trigger_and_ddl_wait_behind_the_same_native_owner() -> Result<()> {
    let fixture = Fixture::open()?;
    let holder = fixture.database.session()?;
    let contender = Arc::new(fixture.database.session()?);
    holder.execute(
        "CREATE TRIGGER hidden_balance AFTER INSERT ON ledger \
         BEGIN UPDATE account SET balance=balance+NEW.delta WHERE id=1; END;",
    )?;
    holder.execute("BEGIN IMMEDIATE;")?;
    thread::scope(|scope| -> Result<()> {
        let writer = contender.clone();
        let waiting =
            scope.spawn(move || writer.query("INSERT INTO ledger VALUES(801,6) RETURNING id;"));
        wait_queue(&fixture.database, &[contender.admission_id()])?;
        holder.execute("COMMIT;")?;
        let rows = waiting
            .join()
            .map_err(|_| anyhow!("query writer panicked"))??;
        ensure!(
            rows.len() == 1 && rows[0][0].to_string() == "801",
            "RETURNING row mismatch"
        );
        Ok(())
    })?;
    holder.execute("BEGIN IMMEDIATE;")?;
    thread::scope(|scope| -> Result<()> {
        let writer = contender.clone();
        let waiting =
            scope.spawn(move || writer.query("CREATE TABLE queued_ddl(id INTEGER PRIMARY KEY);"));
        wait_queue(&fixture.database, &[contender.admission_id()])?;
        holder.execute("ROLLBACK;")?;
        ensure!(
            waiting
                .join()
                .map_err(|_| anyhow!("DDL writer panicked"))??
                .is_empty(),
            "DDL returned rows"
        );
        Ok(())
    })?;
    expect_owner(&fixture.database, None)?;
    let schema = fixture
        .database
        .query("SELECT name FROM sqlite_schema WHERE name='queued_ddl';")?;
    ensure!(schema.len() == 1, "queued query DDL did not commit");
    verify(&fixture.database.session()?, &[(801, 6)])
}
