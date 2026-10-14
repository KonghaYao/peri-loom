#[path = "writer_admission/lifecycle.rs"]
mod lifecycle;
#[path = "writer_admission/queue.rs"]
mod queue;
#[path = "writer_admission/support.rs"]
mod support;
#[path = "writer_admission/transactions.rs"]
mod transactions;

use anyhow::{ensure, Result};
use support::{batch, verify, Fixture};
use turso_core::LimboError;

#[test]
fn independent_reader_observes_only_committed_interactive_writer_effects() -> Result<()> {
    let fixture = Fixture::open()?;
    let holder = fixture.database.session()?;
    let reader = fixture.database.session()?;
    holder.execute(
        "BEGIN IMMEDIATE; INSERT INTO ledger VALUES(101,7); \
         UPDATE account SET balance=balance+7 WHERE id=1;",
    )?;
    ensure!(holder.in_transaction()?, "holder transaction disappeared");
    verify(&reader, &[])?;
    holder.execute("COMMIT;")?;
    ensure!(!holder.in_transaction()?, "holder COMMIT did not finish");
    verify(&reader, &[(101, 7)])?;
    verify(&fixture.database.session()?, &[(101, 7)])
}

#[test]
fn readonly_native_connection_rejects_query_returning_without_business_effect() -> Result<()> {
    let fixture = Fixture::open()?;
    fixture.database.execute(&batch(201, 3))?;
    let readonly = fixture.readonly()?;
    let session = readonly.session()?;
    ensure!(session.is_readonly()?, "native ReadOnly flag is absent");
    verify(&session, &[(201, 3)])?;
    let physical = fixture.store.physical();
    let error = session
        .query("INSERT INTO ledger VALUES(202,4) RETURNING id;")
        .unwrap_err();
    ensure!(
        matches!(
            error.downcast_ref::<LimboError>(),
            Some(LimboError::ReadOnly)
        ),
        "query write did not preserve native ReadOnly"
    );
    ensure!(
        fixture.store.physical() == physical,
        "readonly query write dispatched additional object I/O"
    );
    verify(&session, &[(201, 3)])?;
    verify(&readonly.session()?, &[(201, 3)])
}

#[test]
fn query_returning_runs_trigger_once_and_query_ddl_has_real_schema_effect() -> Result<()> {
    let fixture = Fixture::open()?;
    let session = fixture.database.session()?;
    session.execute(
        "CREATE TRIGGER ledger_balance AFTER INSERT ON ledger \
         BEGIN UPDATE account SET balance=balance+NEW.delta WHERE id=1; END;",
    )?;
    let rows = session.query("INSERT INTO ledger VALUES(301,9) RETURNING id,delta;")?;
    ensure!(
        rows.len() == 1
            && rows[0].len() == 2
            && rows[0][0].to_string() == "301"
            && rows[0][1].to_string() == "9",
        "query RETURNING did not return the single acknowledged business row"
    );
    ensure!(
        !session.in_transaction()?,
        "implicit query writer retained a transaction"
    );
    verify(&session, &[(301, 9)])?;
    let ddl = session.query("CREATE TABLE query_ddl(id INTEGER PRIMARY KEY);")?;
    ensure!(ddl.is_empty(), "DDL unexpectedly returned business rows");
    let schema = fixture
        .database
        .query("SELECT name FROM sqlite_schema WHERE name='query_ddl';")?;
    ensure!(
        schema.len() == 1 && schema[0][0].to_string() == "query_ddl",
        "query DDL did not persist the expected schema"
    );
    verify(&fixture.database.session()?, &[(301, 9)])
}
