use super::*;

fn first(worker: &Worker, session: &str, sql: &str, writing: bool, limit: usize) -> Result<Value> {
    let command = if writing {
        WorkerCommand::OpenWriteCursorFirst {
            session: session.into(),
            sql: sql.into(),
            args: vec![],
            named_args: vec![],
            max_rows: limit,
        }
    } else {
        WorkerCommand::OpenReadCursorFirst {
            session: session.into(),
            sql: sql.into(),
            args: vec![],
            named_args: vec![],
            max_rows: limit,
        }
    };
    worker.call(command, false)
}

#[test]
fn cursor_first_zero_column_write_and_completed_read_release_native_slots() -> Result<()> {
    for profile in [false, true] {
        let worker = Worker::with_profile(profile)?;
        let session = worker.session()?;
        assert_eq!(
            worker.statement(
                &session,
                "CREATE TABLE fused_plain(id INTEGER PRIMARY KEY)",
                vec![]
            )?["ok"],
            true
        );
        let inserted = first(
            &worker,
            &session,
            "INSERT INTO fused_plain VALUES(1),(2)",
            true,
            256,
        )?;
        assert_eq!(inserted["ok"], true, "{inserted}");
        assert_eq!(inserted["result"]["cols"], json!([]));
        assert_eq!(inserted["result"]["rows"], json!([]));
        assert_eq!(inserted["result"]["done"], true);
        assert_eq!(inserted["result"]["affected_row_count"], 2);
        assert_eq!(inserted["result"]["last_insert_rowid"], 2);
        let read = first(
            &worker,
            &session,
            "SELECT id FROM fused_plain ORDER BY id",
            false,
            256,
        )?;
        assert_eq!(read["ok"], true, "{read}");
        assert_eq!(read["result"]["rows"], json!([[1], [2]]));
        assert_eq!(read["result"]["done"], true);
        assert_eq!(
            worker.statement(&session, "SELECT count(*) FROM fused_plain", vec![])?["result"]
                ["rows"],
            json!([[2]])
        );
    }
    Ok(())
}

#[test]
fn cursor_first_read_and_returning_pages_preserve_order_and_abort_proof() -> Result<()> {
    let worker = Worker::with_profile(true)?;
    let session = worker.session()?;
    assert_eq!(
        worker.statement(
            &session,
            "CREATE TABLE fused_pages(id INTEGER PRIMARY KEY)",
            vec![]
        )?["ok"],
        true
    );
    let sql = "WITH RECURSIVE numbers(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM numbers WHERE id<601) INSERT INTO fused_pages SELECT id FROM numbers RETURNING id";
    let aborted = first(&worker, &session, sql, true, 256)?;
    assert_eq!(aborted["ok"], true, "{aborted}");
    assert_eq!(aborted["result"]["done"], false);
    assert_eq!(aborted["result"]["rows"].as_array().unwrap().len(), 256);
    assert_eq!(aborted["result"]["affected_row_count"], Value::Null);
    assert_eq!(aborted["result"]["last_insert_rowid"], Value::Null);
    let closed = worker.call(
        WorkerCommand::CloseWriteCursor {
            session: session.clone(),
        },
        false,
    )?;
    assert_eq!(closed["result"]["abort_proven"], true, "{closed}");
    assert_eq!(
        worker.statement(&session, "SELECT count(*) FROM fused_pages", vec![])?["result"]["rows"],
        json!([[0]])
    );
    for writing in [true, false] {
        let mut page = first(
            &worker,
            &session,
            if writing {
                sql
            } else {
                "SELECT id FROM fused_pages ORDER BY id"
            },
            writing,
            256,
        )?;
        let mut count = 0;
        let mut pages = 0;
        loop {
            assert_eq!(page["ok"], true, "{page}");
            pages += 1;
            for row in page["result"]["rows"].as_array().unwrap() {
                count += 1;
                assert_eq!(row[0], count, "{page}");
            }
            if page["result"]["done"] == true {
                if writing {
                    assert_eq!(page["result"]["affected_row_count"], 601);
                    assert_eq!(page["result"]["last_insert_rowid"], 601);
                }
                break;
            }
            page = worker.call(
                if writing {
                    WorkerCommand::FetchWriteCursor {
                        session: session.clone(),
                        max_rows: 256,
                    }
                } else {
                    WorkerCommand::FetchReadCursor {
                        session: session.clone(),
                        max_rows: 256,
                    }
                },
                false,
            )?;
        }
        assert_eq!((count, pages), (601, 3));
    }
    Ok(())
}

#[test]
fn cursor_first_done_inside_transaction_stays_rollbackable() -> Result<()> {
    for profile in [false, true] {
        let worker = Worker::with_profile(profile)?;
        let session = worker.session()?;
        assert_eq!(
            worker.statement(
                &session,
                "CREATE TABLE fused_transaction(id INTEGER PRIMARY KEY)",
                vec![]
            )?["ok"],
            true
        );
        assert_eq!(worker.statement(&session, "BEGIN", vec![])?["ok"], true);
        let write = first(
            &worker,
            &session,
            "INSERT INTO fused_transaction VALUES(1) RETURNING id",
            true,
            256,
        )?;
        assert_eq!(write["ok"], true, "{write}");
        assert_eq!(write["result"]["done"], true);
        assert_eq!(write["result"]["rows"], json!([[1]]));
        if profile {
            assert_eq!(write["session_state"]["owner"], true);
        }
        let read = first(
            &worker,
            &session,
            "SELECT id FROM fused_transaction",
            false,
            256,
        )?;
        assert_eq!(read["result"]["rows"], json!([[1]]), "{read}");
        assert_eq!(read["result"]["done"], true);
        assert_eq!(worker.statement(&session, "ROLLBACK", vec![])?["ok"], true);
        assert_eq!(
            worker.statement(&session, "SELECT count(*) FROM fused_transaction", vec![])?["result"]
                ["rows"],
            json!([[0]])
        );
    }
    Ok(())
}

#[test]
fn cursor_first_invalid_bounds_prepare_and_step_errors_leave_no_cursor() -> Result<()> {
    let worker = Worker::with_profile(true)?;
    let session = worker.session()?;
    assert_eq!(
        worker.statement(
            &session,
            "CREATE TABLE fused_errors(id INTEGER PRIMARY KEY)",
            vec![]
        )?["ok"],
        true
    );
    for (sql, writing, limit) in [
        ("SELECT 1", false, 0),
        ("INSERT INTO fused_errors VALUES(1)", true, 257),
        ("SELECT ?", false, 256),
        ("INSERT INTO fused_errors VALUES(1)", false, 256),
    ] {
        let result = first(&worker, &session, sql, writing, limit)?;
        assert_eq!(result["ok"], false, "{result}");
        assert_eq!(result["execution_completed"], false, "{result}");
        assert_eq!(
            worker.statement(&session, "SELECT count(*) FROM fused_errors", vec![])?["result"]
                ["rows"],
            json!([[0]])
        );
    }
    let failure = first(
        &worker,
        &session,
        "INSERT INTO fused_errors VALUES(1),(1) RETURNING id",
        true,
        256,
    )?;
    assert_eq!(failure["ok"], false, "{failure}");
    assert_eq!(failure["execution_completed"], true, "{failure}");
    assert_eq!(
        worker.statement(&session, "SELECT count(*) FROM fused_errors", vec![])?["result"]["rows"],
        json!([[0]])
    );
    Ok(())
}
