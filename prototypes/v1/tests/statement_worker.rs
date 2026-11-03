#[path = "statement_worker/cursor_first.rs"]
mod cursor_first;

#[allow(dead_code)]
#[path = "p4_runtime_helpers/clock_gate.rs"]
mod clock_gate;
#[allow(dead_code)]
#[path = "p4_runtime_helpers/remote.rs"]
mod remote;

use std::net::SocketAddr;
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{ensure, Result};
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::ownership::types::AccessRole;
use peri_loom_v1::runtime::DatabaseId;
use peri_loom_v1::worker_rpc::protocol::{
    BatchStatement, NamedStatementArg, StatementArg, WorkerCommand, WorkerRequest,
};
use peri_loom_v1::worker_rpc::server::{WorkerLimits, WorkerServer};
use serde_json::{json, Value};

const CONTROL: &str = "statement_control_credential______________";
const SQL: &str = "statement_sql_credential__________________";

struct Worker {
    fixture: remote::Fixture,
    address: SocketAddr,
    server: Option<JoinHandle<Result<()>>>,
    version: u32,
}

impl Worker {
    fn new() -> Result<Self> {
        Self::with_profile(false)
    }

    fn with_profile(enabled: bool) -> Result<Self> {
        Self::with_access(AccessRole::Writer, enabled)
    }

    fn with_access(role: AccessRole, enabled: bool) -> Result<Self> {
        let fixture = remote::Fixture::new(role, 60_000)?;
        fixture.acquire(role)?;
        let server = WorkerServer::bind_with_limits_and_writer_profile(
            "127.0.0.1:0".parse()?,
            fixture.runtime.clone(),
            CONTROL.into(),
            SQL.into(),
            WorkerLimits::default(),
            enabled,
        )?;
        let address = server.local_addr()?;
        let server = Some(std::thread::spawn(move || server.run()));
        Ok(Self {
            fixture,
            address,
            server,
            version: if enabled { 2 } else { 1 },
        })
    }

    fn call(&self, command: WorkerCommand, control: bool) -> Result<Value> {
        let response = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()?
            .post(format!(
                "http://{}/{}",
                self.address,
                if control { "control" } else { "sql" }
            ))
            .bearer_auth(if control { CONTROL } else { SQL })
            .json(&WorkerRequest {
                version: if control { 1 } else { self.version },
                request_id: "statement-test".into(),
                command,
            })
            .send()?;
        ensure!(
            response.status().is_success(),
            "worker transport failed: {}",
            response.status()
        );
        Ok(response.json()?)
    }

    fn statement(&self, session: &str, sql: &str, args: Vec<StatementArg>) -> Result<Value> {
        self.statement_named(session, sql, args, vec![])
    }

    fn statement_named(
        &self,
        session: &str,
        sql: &str,
        args: Vec<StatementArg>,
        named_args: Vec<NamedStatementArg>,
    ) -> Result<Value> {
        self.statement_named_projection(session, sql, args, named_args, true)
    }

    fn statement_named_projection(
        &self,
        session: &str,
        sql: &str,
        args: Vec<StatementArg>,
        named_args: Vec<NamedStatementArg>,
        want_rows: bool,
    ) -> Result<Value> {
        self.call(
            WorkerCommand::Statement {
                session: session.into(),
                sql: sql.into(),
                args,
                named_args,
                want_rows,
            },
            false,
        )
    }

    fn guarded_statement(
        &self,
        session: &str,
        sql: &str,
        args: Vec<StatementArg>,
    ) -> Result<Value> {
        self.call(
            WorkerCommand::GuardedReadonlyStatement {
                session: session.into(),
                sql: sql.into(),
                args,
                named_args: vec![],
                want_rows: true,
            },
            false,
        )
    }

    fn guarded_batch(
        &self,
        session: &str,
        statements: Vec<BatchStatement>,
        max_result_bytes: usize,
    ) -> Result<Value> {
        self.call(
            WorkerCommand::GuardedReadonlyBatch {
                session: session.into(),
                statements,
                max_result_bytes,
            },
            false,
        )
    }

    fn session(&self) -> Result<String> {
        let response = self.call(
            WorkerCommand::OpenSession {
                database: self.fixture.identifier.clone(),
            },
            false,
        )?;
        ensure!(response["ok"] == true, "open failed: {response}");
        Ok(response["result"]["session"].as_str().unwrap().into())
    }

    fn raw(&self, body: &str) -> Result<(u16, String)> {
        let response = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()?
            .post(format!("http://{}/sql", self.address))
            .bearer_auth(SQL)
            .header("Content-Type", "application/json")
            .body(body.to_owned())
            .send()?;
        Ok((response.status().as_u16(), response.text()?))
    }
}

#[test]
fn describe_prepares_without_executing_writes() -> Result<()> {
    let worker = Worker::new()?;
    let session = worker.session()?;
    let create = worker.statement(
        &session,
        "CREATE TABLE describe_probe(id INTEGER PRIMARY KEY)",
        vec![],
    )?;
    assert_eq!(create["ok"], true, "{create}");
    let describe = |sql: &str| {
        worker.call(
            WorkerCommand::Describe {
                session: session.clone(),
                sql: sql.into(),
            },
            false,
        )
    };
    let select = describe("SELECT id FROM describe_probe WHERE id = :lookup")?;
    assert_eq!(select["ok"], true, "{select}");
    assert_eq!(select["result"]["params"], json!([{"name": ":lookup"}]));
    assert_eq!(select["result"]["cols"][0]["name"], "id");
    assert_eq!(select["result"]["is_readonly"], true);
    assert_eq!(select["result"]["is_explain"], false);
    let explain_plan = describe("EXPLAIN QUERY PLAN SELECT id FROM describe_probe")?;
    assert_eq!(explain_plan["ok"], true, "{explain_plan}");
    assert_eq!(explain_plan["result"]["is_explain"], true);
    let explain = describe("EXPLAIN SELECT id FROM describe_probe")?;
    assert_eq!(explain["ok"], true, "{explain}");
    assert_eq!(explain["result"]["is_explain"], true);
    let insert = describe("INSERT INTO describe_probe(id) VALUES(91)")?;
    assert_eq!(insert["ok"], true, "{insert}");
    assert_eq!(insert["result"]["is_readonly"], false);
    let absent = worker.statement(&session, "SELECT id FROM describe_probe", vec![])?;
    assert_eq!(absent["result"]["rows"], json!([]));
    Ok(())
}

#[test]
fn named_arguments_bind_natively_and_reject_ambiguous_or_missing_slots() -> Result<()> {
    let worker = Worker::new()?;
    let session = worker.session()?;
    let named = |name: &str, value: &str| NamedStatementArg {
        name: name.into(),
        value: StatementArg::Text {
            value: value.into(),
        },
    };
    let mixed = worker.statement_named(
        &session,
        "SELECT :name, ? AS positional, :name AS repeated",
        vec![StatementArg::Integer { value: "42".into() }],
        vec![named("name", "a' --")],
    )?;
    assert_eq!(mixed["ok"], true, "{mixed}");
    assert_eq!(mixed["result"]["rows"], json!([["a' --", 42, "a' --"]]));
    let sigiled = worker.statement_named(
        &session,
        "SELECT @name AS named",
        vec![],
        vec![named("@name", "safe")],
    )?;
    assert_eq!(sigiled["result"]["rows"], json!([["safe"]]));
    for (sql, named_args) in [
        (
            "SELECT :name, @name",
            vec![named("name", "ambiguous"), named("@name", "a")],
        ),
        (
            "SELECT :name",
            vec![named("name", "one"), named("name", "two")],
        ),
        ("SELECT :name", vec![named("missing", "one")]),
    ] {
        let result = worker.statement_named(&session, sql, vec![], named_args)?;
        assert_eq!(result["ok"], false, "{result}");
        assert_eq!(result["execution_completed"], false);
    }
    let after = worker.statement(&session, "SELECT 7", vec![])?;
    assert_eq!(after["result"]["rows"], json!([[7]]));
    assert_eq!(
        worker.call(WorkerCommand::CloseSession { session }, false)?["ok"],
        true
    );
    Ok(())
}

#[test]
fn read_cursor_continues_one_native_statement_beyond_aggregate_row_budget() -> Result<()> {
    let worker = Worker::new()?;
    let session = worker.session()?;
    let created = worker.statement(
        &session,
        "CREATE TABLE cursor_numbers(id INTEGER PRIMARY KEY)",
        vec![],
    )?;
    assert_eq!(created["ok"], true, "{created}");
    let inserted = worker.statement(&session, "WITH RECURSIVE numbers(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM numbers WHERE id<12000) INSERT INTO cursor_numbers SELECT id FROM numbers", vec![])?;
    assert_eq!(inserted["ok"], true, "{inserted}");
    let sql = "SELECT id FROM cursor_numbers ORDER BY id";
    let opened = worker.call(
        WorkerCommand::OpenReadCursor {
            session: session.clone(),
            sql: sql.into(),
            args: vec![],
            named_args: vec![],
        },
        false,
    )?;
    assert_eq!(opened["ok"], true, "{opened}");
    assert_eq!(opened["result"]["cols"][0]["name"], "id");
    let blocked = worker.statement(&session, "SELECT 7", vec![])?;
    assert_eq!(blocked["ok"], false, "{blocked}");
    let mut count = 0;
    loop {
        let page = worker.call(
            WorkerCommand::FetchReadCursor {
                session: session.clone(),
                max_rows: 256,
            },
            false,
        )?;
        assert_eq!(page["ok"], true, "{page}");
        let rows = page["result"]["rows"].as_array().unwrap();
        assert!(rows.len() <= 256);
        for row in rows {
            count += 1;
            assert_eq!(row[0], count, "unexpected cursor row at {count}");
        }
        if page["result"]["done"] == true {
            break;
        }
    }
    assert_eq!(count, 12000);
    let restored = worker.statement(&session, "SELECT 7", vec![])?;
    assert_eq!(restored["result"]["rows"], json!([[7]]));
    assert_eq!(
        worker.call(WorkerCommand::CloseSession { session }, false)?["ok"],
        true
    );
    Ok(())
}

#[test]
fn read_cursor_rejects_writes_and_early_close_releases_native_state() -> Result<()> {
    let worker = Worker::with_profile(true)?;
    let session = worker.session()?;
    let opened = worker.call(
        WorkerCommand::OpenReadCursor {
            session: session.clone(),
            sql: "SELECT 1 AS value UNION ALL SELECT 2".into(),
            args: vec![],
            named_args: vec![],
        },
        false,
    )?;
    assert_eq!(opened["ok"], true, "{opened}");
    assert!(opened["session_state"].is_object(), "{opened}");
    let first = worker.call(
        WorkerCommand::FetchReadCursor {
            session: session.clone(),
            max_rows: 1,
        },
        false,
    )?;
    assert_eq!(first["result"]["rows"], json!([[1]]), "{first}");
    assert_eq!(first["result"]["done"], false, "{first}");
    assert!(first["session_state"].is_object(), "{first}");
    let closed = worker.call(
        WorkerCommand::CloseReadCursor {
            session: session.clone(),
        },
        false,
    )?;
    assert_eq!(closed["result"]["closed"], true, "{closed}");
    assert!(closed["session_state"].is_object(), "{closed}");
    let write = worker.call(
        WorkerCommand::OpenReadCursor {
            session: session.clone(),
            sql: "CREATE TABLE cursor_must_not_write(value INTEGER)".into(),
            args: vec![],
            named_args: vec![],
        },
        false,
    )?;
    assert_eq!(write["ok"], false, "{write}");
    let after = worker.statement(
        &session,
        "SELECT name FROM sqlite_master WHERE name='cursor_must_not_write'",
        vec![],
    )?;
    assert_eq!(after["result"]["rows"], json!([]), "{after}");
    let reopened = worker.call(
        WorkerCommand::OpenReadCursor {
            session: session.clone(),
            sql: "SELECT 3 UNION ALL SELECT 4".into(),
            args: vec![],
            named_args: vec![],
        },
        false,
    )?;
    assert_eq!(reopened["ok"], true, "{reopened}");
    assert_eq!(
        worker.call(WorkerCommand::CloseSession { session }, false)?["ok"],
        true
    );
    Ok(())
}

#[test]
fn write_cursor_returns_twelve_thousand_rows_with_final_native_counts() -> Result<()> {
    let worker = Worker::with_profile(true)?;
    let session = worker.session()?;
    let created = worker.statement(
        &session,
        "CREATE TABLE write_cursor_numbers(id INTEGER PRIMARY KEY)",
        vec![],
    )?;
    assert_eq!(created["ok"], true, "{created}");
    let opened = worker.call(WorkerCommand::OpenWriteCursor {
        session: session.clone(),
        sql: "WITH RECURSIVE numbers(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM numbers WHERE id<12000) INSERT INTO write_cursor_numbers SELECT id FROM numbers RETURNING id".into(),
        args: vec![],
        named_args: vec![],
    }, false)?;
    assert_eq!(opened["ok"], true, "{opened}");
    assert_eq!(opened["result"]["cols"][0]["name"], "id");
    assert_eq!(opened["result"].as_object().unwrap().len(), 1);
    let mut count = 0;
    loop {
        let page = worker.call(
            WorkerCommand::FetchWriteCursor {
                session: session.clone(),
                max_rows: 256,
            },
            false,
        )?;
        assert_eq!(page["ok"], true, "{page}");
        for row in page["result"]["rows"].as_array().unwrap() {
            count += 1;
            assert_eq!(row[0], count, "row {count}");
        }
        if page["result"]["done"] == true {
            assert_eq!(page["result"]["affected_row_count"], 12000, "{page}");
            assert_eq!(page["result"]["last_insert_rowid"], 12000, "{page}");
            assert_eq!(page["session_state"]["owner"], false, "{page}");
            break;
        }
        assert!(
            page["result"]
                .get("affected_row_count")
                .is_some_and(Value::is_null),
            "{page}"
        );
        assert!(
            page["result"]
                .get("last_insert_rowid")
                .is_some_and(Value::is_null),
            "{page}"
        );
        assert_eq!(page["session_state"]["owner"], true, "{page}");
    }
    assert_eq!(count, 12000);
    let saved = worker.statement(
        &session,
        "SELECT count(*), max(id) FROM write_cursor_numbers",
        vec![],
    )?;
    assert_eq!(saved["result"]["rows"], json!([[12000, 12000]]), "{saved}");
    Ok(())
}

#[test]
fn write_cursor_without_returning_reports_counts_and_no_rows() -> Result<()> {
    let worker = Worker::with_profile(true)?;
    let session = worker.session()?;
    assert_eq!(
        worker.statement(
            &session,
            "CREATE TABLE plain_write_cursor(id INTEGER PRIMARY KEY, name TEXT)",
            vec![]
        )?["ok"],
        true
    );
    let opened = worker.call(
        WorkerCommand::OpenWriteCursor {
            session: session.clone(),
            sql: "INSERT INTO plain_write_cursor(name) VALUES ('one'), ('two')".into(),
            args: vec![],
            named_args: vec![],
        },
        false,
    )?;
    assert_eq!(opened["ok"], true, "{opened}");
    assert_eq!(opened["result"]["cols"], json!([]), "{opened}");
    let page = worker.call(
        WorkerCommand::FetchWriteCursor {
            session: session.clone(),
            max_rows: 256,
        },
        false,
    )?;
    assert_eq!(page["ok"], true, "{page}");
    assert_eq!(page["result"]["rows"], json!([]), "{page}");
    assert_eq!(page["result"]["done"], true, "{page}");
    assert_eq!(page["result"]["affected_row_count"], 2, "{page}");
    assert_eq!(page["result"]["last_insert_rowid"], 2, "{page}");
    let saved = worker.statement(&session, "SELECT count(*) FROM plain_write_cursor", vec![])?;
    assert_eq!(saved["result"]["rows"], json!([[2]]), "{saved}");
    Ok(())
}

#[test]
fn write_cursor_early_close_aborts_without_committing_and_releases_owner() -> Result<()> {
    let worker = Worker::with_profile(true)?;
    let session = worker.session()?;
    assert_eq!(
        worker.statement(
            &session,
            "CREATE TABLE abort_write_cursor(id INTEGER PRIMARY KEY)",
            vec![]
        )?["ok"],
        true
    );
    let opened = worker.call(WorkerCommand::OpenWriteCursor {
        session: session.clone(),
        sql: "WITH RECURSIVE numbers(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM numbers WHERE id<12000) INSERT INTO abort_write_cursor SELECT id FROM numbers RETURNING id".into(),
        args: vec![], named_args: vec![],
    }, false)?;
    assert_eq!(opened["ok"], true, "{opened}");
    let first = worker.call(
        WorkerCommand::FetchWriteCursor {
            session: session.clone(),
            max_rows: 64,
        },
        false,
    )?;
    assert_eq!(first["ok"], true, "{first}");
    assert_eq!(first["result"]["done"], false, "{first}");
    assert_eq!(first["session_state"]["owner"], true, "{first}");
    let blocked = worker.statement(&session, "SELECT 1", vec![])?;
    assert_eq!(blocked["ok"], false, "{blocked}");
    let second_open = worker.call(
        WorkerCommand::OpenReadCursor {
            session: session.clone(),
            sql: "SELECT 1".into(),
            args: vec![],
            named_args: vec![],
        },
        false,
    )?;
    assert_eq!(second_open["ok"], false, "{second_open}");
    let contender = worker.session()?;
    let competing_write = worker.statement(
        &contender,
        "INSERT INTO abort_write_cursor(id) VALUES(999)",
        vec![],
    )?;
    assert_eq!(competing_write["ok"], false, "{competing_write}");
    assert_eq!(
        competing_write["execution_completed"], false,
        "{competing_write}"
    );
    let closed = worker.call(
        WorkerCommand::CloseWriteCursor {
            session: session.clone(),
        },
        false,
    )?;
    assert_eq!(closed["result"]["closed"], true, "{closed}");
    assert_eq!(closed["session_state"]["owner"], false, "{closed}");
    let saved = worker.statement(&session, "SELECT count(*) FROM abort_write_cursor", vec![])?;
    assert_eq!(saved["result"]["rows"], json!([[0]]), "{saved}");
    let again = worker.call(
        WorkerCommand::OpenWriteCursor {
            session: session.clone(),
            sql: "INSERT INTO abort_write_cursor(id) VALUES(42) RETURNING id".into(),
            args: vec![],
            named_args: vec![],
        },
        false,
    )?;
    assert_eq!(again["ok"], true, "{again}");
    let first = worker.call(
        WorkerCommand::FetchWriteCursor {
            session: session.clone(),
            max_rows: 1,
        },
        false,
    )?;
    assert_eq!(first["result"]["done"], false, "{first}");
    let close_session = worker.call(WorkerCommand::CloseSession { session }, false)?;
    assert_eq!(close_session["ok"], true, "{close_session}");
    let check_session = worker.session()?;
    let saved = worker.statement(
        &check_session,
        "SELECT count(*) FROM abort_write_cursor",
        vec![],
    )?;
    assert_eq!(saved["result"]["rows"], json!([[0]]), "{saved}");
    Ok(())
}

#[test]
fn write_cursor_invalid_page_size_aborts_without_confirming_rows() -> Result<()> {
    let worker = Worker::with_profile(true)?;
    let session = worker.session()?;
    assert_eq!(
        worker.statement(
            &session,
            "CREATE TABLE invalid_write_page(id INTEGER PRIMARY KEY)",
            vec![]
        )?["ok"],
        true
    );
    assert_eq!(
        worker.call(
            WorkerCommand::OpenWriteCursor {
                session: session.clone(),
                sql: "INSERT INTO invalid_write_page(id) VALUES(1),(2) RETURNING id".into(),
                args: vec![],
                named_args: vec![],
            },
            false
        )?["ok"],
        true
    );
    let first = worker.call(
        WorkerCommand::FetchWriteCursor {
            session: session.clone(),
            max_rows: 1,
        },
        false,
    )?;
    assert_eq!(first["result"]["done"], false, "{first}");
    let invalid = worker.call(
        WorkerCommand::FetchWriteCursor {
            session: session.clone(),
            max_rows: 257,
        },
        false,
    )?;
    assert_eq!(invalid["ok"], false, "{invalid}");
    assert_eq!(invalid["execution_completed"], true, "{invalid}");
    assert_eq!(invalid["session_state"]["owner"], false, "{invalid}");
    assert!(
        invalid.get("result").is_some_and(Value::is_null),
        "{invalid}"
    );
    let saved = worker.statement(&session, "SELECT count(*) FROM invalid_write_page", vec![])?;
    assert_eq!(saved["result"]["rows"], json!([[0]]), "{saved}");
    Ok(())
}

#[test]
fn write_cursor_update_returns_changes_without_stale_insert_rowid() -> Result<()> {
    let worker = Worker::with_profile(true)?;
    let session = worker.session()?;
    assert_eq!(
        worker.statement(
            &session,
            "CREATE TABLE update_cursor(id INTEGER PRIMARY KEY)",
            vec![]
        )?["ok"],
        true
    );
    assert_eq!(
        worker.statement(&session, "INSERT INTO update_cursor(id) VALUES(1)", vec![])?["ok"],
        true
    );
    let opened = worker.call(
        WorkerCommand::OpenWriteCursor {
            session: session.clone(),
            sql: "UPDATE update_cursor SET id = 2 RETURNING id".into(),
            args: vec![],
            named_args: vec![],
        },
        false,
    )?;
    assert_eq!(opened["ok"], true, "{opened}");
    let first = worker.call(
        WorkerCommand::FetchWriteCursor {
            session: session.clone(),
            max_rows: 1,
        },
        false,
    )?;
    assert_eq!(first["result"]["rows"], json!([[2]]), "{first}");
    assert_eq!(first["result"]["done"], false, "{first}");
    let terminal = worker.call(
        WorkerCommand::FetchWriteCursor {
            session,
            max_rows: 1,
        },
        false,
    )?;
    assert_eq!(terminal["ok"], true, "{terminal}");
    assert_eq!(terminal["result"]["rows"], json!([]), "{terminal}");
    assert_eq!(terminal["result"]["done"], true, "{terminal}");
    assert_eq!(terminal["result"]["affected_row_count"], 1, "{terminal}");
    assert!(
        terminal["result"]["last_insert_rowid"].is_null(),
        "{terminal}"
    );
    Ok(())
}

#[test]
fn write_cursor_enforces_byte_budget_across_pages_and_aborts() -> Result<()> {
    let worker = Worker::with_profile(true)?;
    let session = worker.session()?;
    assert_eq!(
        worker.statement(
            &session,
            "CREATE TABLE byte_budget_cursor(payload TEXT)",
            vec![]
        )?["ok"],
        true
    );
    let opened = worker.call(
        WorkerCommand::OpenWriteCursor {
            session: session.clone(),
            sql: "WITH RECURSIVE numbers(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM numbers WHERE id<800) INSERT INTO byte_budget_cursor SELECT printf('%06000d', id) FROM numbers RETURNING payload".into(),
            args: vec![],
            named_args: vec![],
        },
        false,
    )?;
    assert_eq!(opened["ok"], true, "{opened}");
    let mut delivered = 0;
    loop {
        let page = worker.call(
            WorkerCommand::FetchWriteCursor {
                session: session.clone(),
                max_rows: 128,
            },
            false,
        )?;
        if page["ok"] == false {
            assert!(delivered > 128, "byte budget did not span pages: {page}");
            assert!(
                page["error"].as_str().unwrap().contains("byte budget"),
                "{page}"
            );
            assert_eq!(page["execution_completed"], true, "{page}");
            break;
        }
        assert_eq!(page["result"]["done"], false, "byte limit was bypassed");
        delivered += page["result"]["rows"].as_array().unwrap().len();
    }
    let saved = worker.statement(&session, "SELECT count(*) FROM byte_budget_cursor", vec![])?;
    assert_eq!(saved["result"]["rows"], json!([[0]]), "{saved}");
    Ok(())
}

#[test]
fn write_cursor_rejects_read_only_role_and_read_cursor_sql() -> Result<()> {
    let readonly = Worker::with_access(AccessRole::ReadOnly, false)?;
    let session = readonly.session()?;
    let denied = readonly.call(
        WorkerCommand::OpenWriteCursor {
            session: session.clone(),
            sql: "CREATE TABLE forbidden_cursor(id INTEGER) RETURNING id".into(),
            args: vec![],
            named_args: vec![],
        },
        false,
    )?;
    assert_eq!(denied["ok"], false, "{denied}");
    let denied = readonly.call(
        WorkerCommand::OpenWriteCursor {
            session: session.clone(),
            sql: "SELECT 1 AS id".into(),
            args: vec![],
            named_args: vec![],
        },
        false,
    )?;
    assert_eq!(denied["ok"], false, "{denied}");
    let after = readonly.statement(
        &session,
        "SELECT name FROM sqlite_master WHERE name='forbidden_cursor'",
        vec![],
    )?;
    assert_eq!(after["result"]["rows"], json!([]), "{after}");
    let writer = Worker::new()?;
    let session = writer.session()?;
    let rejected = writer.call(
        WorkerCommand::OpenWriteCursor {
            session: session.clone(),
            sql: "SELECT 42".into(),
            args: vec![],
            named_args: vec![],
        },
        false,
    )?;
    assert_eq!(rejected["ok"], false, "{rejected}");
    let read = writer.call(
        WorkerCommand::OpenReadCursor {
            session: session.clone(),
            sql: "SELECT 42".into(),
            args: vec![],
            named_args: vec![],
        },
        false,
    )?;
    assert_eq!(read["ok"], true, "{read}");
    let active = writer.call(
        WorkerCommand::OpenWriteCursor {
            session: session.clone(),
            sql: "INSERT INTO nowhere VALUES(1) RETURNING 1".into(),
            args: vec![],
            named_args: vec![],
        },
        false,
    )?;
    assert_eq!(active["ok"], false, "{active}");
    assert_eq!(
        writer.call(WorkerCommand::CloseReadCursor { session }, false)?["ok"],
        true
    );
    Ok(())
}

#[test]
fn statement_readonly_role_cannot_write_or_bypass_native_admission() -> Result<()> {
    let worker = Worker::with_access(AccessRole::ReadOnly, false)?;
    let session = worker.session()?;
    let select = worker.statement(&session, "SELECT 42 AS answer", vec![])?;
    assert_eq!(select["ok"], true, "{select}");
    assert_eq!(select["result"]["rows"], json!([[42]]));
    let rejected = worker.statement(&session, "CREATE TABLE forbidden(value INTEGER)", vec![])?;
    assert_eq!(rejected["ok"], false, "{rejected}");
    assert_eq!(rejected["execution_completed"], true);
    let after = worker.statement(
        &session,
        "SELECT name FROM sqlite_master WHERE name = 'forbidden'",
        vec![],
    )?;
    assert_eq!(after["result"]["rows"], json!([]));
    assert_eq!(
        worker.call(WorkerCommand::CloseSession { session }, false)?["ok"],
        true
    );
    Ok(())
}

#[test]
fn statement_v2_owner_state_and_cleanup_ack() -> Result<()> {
    let mut worker = Worker::with_profile(true)?;
    let session = worker.session()?;
    let begun = worker.statement(&session, "BEGIN IMMEDIATE", vec![])?;
    assert_eq!(begun["ok"], true, "{begun}");
    assert_eq!(begun["result"]["cols"], json!([]));
    assert_eq!(begun["result"]["rows"], json!([]));
    assert_eq!(begun["result"]["affected_row_count"], 0);
    assert_eq!(begun["session_state"]["owner"], true);
    let created = worker.statement(
        &session,
        "CREATE TABLE inside_transaction(id INTEGER)",
        vec![],
    )?;
    assert_eq!(created["ok"], true, "{created}");
    assert_eq!(created["session_state"]["owner"], true);
    let rejected = worker.statement(&session, "INSERT INTO missing_table VALUES (1)", vec![])?;
    assert_eq!(rejected["ok"], false, "{rejected}");
    assert_eq!(rejected["execution_completed"], false);
    assert!(rejected.get("session_state").is_none(), "{rejected}");
    worker.version = 1;
    let legacy = worker.statement(&session, "INSERT INTO missing_table VALUES (1)", vec![])?;
    assert_eq!(legacy["ok"], false, "{legacy}");
    assert_eq!(legacy["execution_completed"], false, "{legacy}");
    assert!(legacy.get("session_state").is_none(), "{legacy}");
    worker.version = 2;
    let committed = worker.statement(&session, "COMMIT", vec![])?;
    assert_eq!(committed["ok"], true, "{committed}");
    assert_eq!(committed["session_state"]["owner"], false);
    assert!(
        committed["session_state"]["sequence"].as_u64().unwrap()
            > created["session_state"]["sequence"].as_u64().unwrap(),
        "{committed}"
    );
    let data = worker.statement(
        &session,
        "SELECT id FROM inside_transaction WHERE 0",
        vec![],
    )?;
    assert_eq!(data["result"]["cols"][0]["name"], "id");
    assert_eq!(data["result"]["rows"], json!([]));
    assert_eq!(
        worker.call(WorkerCommand::CloseSession { session }, false)?["ok"],
        true
    );
    Ok(())
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.fixture.gate.release();
        let _ = self.call(WorkerCommand::Shutdown, true);
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
    }
}

#[test]
fn native_statement_metadata_parameters_and_transactions_via_worker_rpc() -> Result<()> {
    let worker = Worker::new()?;
    let session = worker.session()?;
    let create = worker.statement(
        &session,
        "CREATE TABLE records (id INTEGER PRIMARY KEY, label TEXT, payload BLOB)",
        vec![],
    )?;
    assert_eq!(create["ok"], true, "{create}");
    assert_eq!(create["result"]["cols"], json!([]));
    assert_eq!(create["result"]["rows"], json!([]));
    assert_eq!(create["result"]["affected_row_count"], 0);
    assert_eq!(create["result"]["autocommit"], true);

    let quoted = "a' ); DROP TABLE records; --";
    let insert = worker.statement(
        &session,
        "INSERT INTO records (id, label, payload) VALUES (?1, ?2, ?3)",
        vec![
            StatementArg::Integer {
                value: i64::MAX.to_string(),
            },
            StatementArg::Text {
                value: quoted.into(),
            },
            StatementArg::Blob {
                base64: "AAEC/w==".into(),
            },
        ],
    )?;
    assert_eq!(insert["ok"], true, "{insert}");
    assert_eq!(insert["result"]["affected_row_count"], 1);
    assert_eq!(insert["result"]["last_insert_rowid"], json!(i64::MAX));

    let ddl_after_insert =
        worker.statement(&session, "CREATE TABLE another (value INTEGER)", vec![])?;
    assert_eq!(ddl_after_insert["ok"], true, "{ddl_after_insert}");
    assert_eq!(ddl_after_insert["result"]["affected_row_count"], 0);
    assert!(ddl_after_insert["result"]["last_insert_rowid"].is_null());
    let begun = worker.statement(&session, "BEGIN IMMEDIATE", vec![])?;
    assert_eq!(begun["ok"], true, "{begun}");
    assert_eq!(begun["result"]["autocommit"], false);
    let next_insert =
        worker.statement(&session, "INSERT INTO another(value) VALUES (6)", vec![])?;
    assert_eq!(next_insert["result"]["affected_row_count"], 1);
    assert_eq!(next_insert["result"]["autocommit"], false);
    let committed_insert = worker.statement(&session, "COMMIT", vec![])?;
    assert_eq!(committed_insert["ok"], true, "{committed_insert}");
    assert_eq!(committed_insert["result"]["cols"], json!([]));
    assert_eq!(committed_insert["result"]["rows"], json!([]));
    assert_eq!(committed_insert["result"]["affected_row_count"], 0);
    assert_eq!(committed_insert["result"]["autocommit"], true);
    assert!(committed_insert["result"]["last_insert_rowid"].is_null());
    let delete = worker.statement(&session, "DELETE FROM another WHERE value = 6", vec![])?;
    assert_eq!(delete["result"]["affected_row_count"], 1);
    let same_rowid = worker.statement(&session, "INSERT INTO another(value) VALUES (6)", vec![])?;
    assert_eq!(
        same_rowid["result"]["last_insert_rowid"],
        json!(1),
        "{same_rowid}"
    );
    let ddl_after_commit = worker.statement(
        &session,
        "CREATE INDEX another_value ON another(value)",
        vec![],
    )?;
    assert_eq!(ddl_after_commit["result"]["affected_row_count"], 0);
    assert!(ddl_after_commit["result"]["last_insert_rowid"].is_null());
    let zero_update = worker.statement(
        &session,
        "UPDATE another SET value = 7 WHERE value = 404",
        vec![],
    )?;
    assert_eq!(zero_update["result"]["affected_row_count"], 0);
    assert!(zero_update["result"]["last_insert_rowid"].is_null());
    let audit = worker.statement(&session, "CREATE TABLE audit(value INTEGER)", vec![])?;
    assert_eq!(audit["ok"], true, "{audit}");
    let trigger = worker.statement(&session, "CREATE TRIGGER audit_update AFTER UPDATE ON another BEGIN INSERT INTO audit VALUES(NEW.value); END", vec![])?;
    assert_eq!(trigger["ok"], true, "{trigger}");
    let triggered = worker.statement(
        &session,
        "UPDATE another SET value = 8 WHERE value = 6",
        vec![],
    )?;
    assert_eq!(triggered["ok"], true, "{triggered}");
    assert_eq!(triggered["result"]["affected_row_count"], 1);
    assert_eq!(
        worker.statement(&session, "SELECT value FROM audit", vec![])?["result"]["rows"],
        json!([[8]])
    );

    let empty = worker.statement(
        &session,
        "SELECT id AS record_id, label FROM records WHERE 0",
        vec![],
    )?;
    assert_eq!(empty["ok"], true, "{empty}");
    assert_eq!(empty["result"]["cols"][0]["name"], "record_id");
    assert_eq!(empty["result"]["cols"][1]["name"], "label");
    assert_eq!(empty["result"]["rows"], json!([]));
    assert_eq!(empty["result"]["affected_row_count"], 0);
    let select = worker.statement(
        &session,
        "SELECT * FROM records WHERE id = ?",
        vec![StatementArg::Integer {
            value: i64::MAX.to_string(),
        }],
    )?;
    assert_eq!(select["ok"], true, "{select}");
    assert_eq!(select["result"]["cols"][0]["name"], "id");
    assert_eq!(select["result"]["cols"][0]["decltype"], "INTEGER");
    assert_eq!(select["result"]["cols"][1]["decltype"], "TEXT");
    assert_eq!(select["result"]["rows"][0][0], json!(i64::MAX));
    assert_eq!(select["result"]["rows"][0][1], quoted);
    assert_eq!(select["result"]["rows"][0][2], json!([0, 1, 2, 255]));

    for sql in [
        "BEGIN IMMEDIATE",
        "UPDATE records SET label = 'updated' WHERE id = 9223372036854775807",
        "ROLLBACK",
    ] {
        let result = worker.statement(&session, sql, vec![])?;
        assert_eq!(result["ok"], true, "{sql}: {result}");
        if sql.starts_with("UPDATE") {
            assert_eq!(result["result"]["affected_row_count"], 1);
            assert!(result["result"]["last_insert_rowid"].is_null());
        }
    }
    let original = worker.statement(&session, "SELECT label FROM records", vec![])?;
    assert_eq!(original["result"]["rows"], json!([[quoted]]));
    assert_eq!(
        worker.statement(&session, "BEGIN IMMEDIATE", vec![])?["ok"],
        true
    );
    assert_eq!(
        worker.statement(&session, "UPDATE records SET label = 'committed'", vec![])?["result"]
            ["affected_row_count"],
        1
    );
    assert_eq!(worker.statement(&session, "COMMIT", vec![])?["ok"], true);
    let committed = worker.statement(&session, "SELECT label FROM records", vec![])?;
    assert_eq!(committed["result"]["rows"], json!([["committed"]]));
    let returning = worker.statement(
        &session,
        "UPDATE records SET label = 'returned' RETURNING id",
        vec![],
    )?;
    assert_eq!(returning["ok"], true, "{returning}");
    assert_eq!(returning["result"]["rows"], json!([[i64::MAX]]));
    assert_eq!(returning["result"]["affected_row_count"], 1);

    let bad_args = worker.statement(&session, "DELETE FROM records WHERE id = ?", vec![])?;
    assert_eq!(bad_args["ok"], false);
    assert_eq!(bad_args["execution_completed"], false);
    let multiple = worker.statement(&session, "DELETE FROM records; DROP TABLE records", vec![])?;
    assert_eq!(multiple["ok"], false);
    assert_eq!(
        worker.statement(&session, "SELECT count(*) FROM records", vec![])?["result"]["rows"],
        json!([[1]])
    );

    let old = worker.call(
        WorkerCommand::Query {
            session: session.clone(),
            sql: "SELECT label FROM records".into(),
        },
        false,
    )?;
    assert_eq!(old["result"], json!([["returned"]]));
    assert_eq!(
        worker.call(WorkerCommand::CloseSession { session }, false)?["ok"],
        true
    );
    Ok(())
}

#[test]
fn statement_without_rows_still_settles_its_write() -> Result<()> {
    let worker = Worker::new()?;
    let session = worker.session()?;
    let created = worker.statement(
        &session,
        "CREATE TABLE returning_records(id INTEGER PRIMARY KEY, label TEXT)",
        vec![],
    )?;
    assert_eq!(created["ok"], true, "{created}");

    let projected = worker.statement_named_projection(
        &session,
        "INSERT INTO returning_records(id, label) VALUES (1, 'hidden') RETURNING label",
        vec![],
        vec![],
        false,
    )?;
    assert_eq!(projected["ok"], true, "{projected}");
    assert_eq!(projected["result"]["rows"], json!([]));
    assert_eq!(projected["result"]["affected_row_count"], 1);

    let read_back = worker.statement(
        &session,
        "SELECT label FROM returning_records WHERE id = 1",
        vec![],
    )?;
    assert_eq!(read_back["result"]["rows"], json!([["hidden"]]));
    assert_eq!(
        worker.call(WorkerCommand::CloseSession { session }, false)?["ok"],
        true
    );
    Ok(())
}

#[test]
fn statement_result_carries_engine_counters_and_duration() -> Result<()> {
    let worker = Worker::new()?;
    let session = worker.session()?;
    let created = worker.statement(&session, "CREATE TABLE counters(value INTEGER)", vec![])?;
    assert_eq!(created["ok"], true, "{created}");

    let inserted = worker.statement(&session, "INSERT INTO counters VALUES (1), (2)", vec![])?;
    assert_eq!(inserted["ok"], true, "{inserted}");
    assert_eq!(inserted["result"]["affected_row_count"], 2);
    assert!(
        inserted["result"]["rows_written"]
            .as_u64()
            .expect("rows_written is a JSON number")
            >= 2,
        "{inserted}"
    );

    let selected = worker.statement(&session, "SELECT value FROM counters", vec![])?;
    assert_eq!(selected["ok"], true, "{selected}");
    let result = selected["result"]
        .as_object()
        .expect("statement result object");
    let mut keys = result.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    assert_eq!(
        keys,
        [
            "affected_row_count",
            "autocommit",
            "cols",
            "last_insert_rowid",
            "query_duration_ms",
            "rows",
            "rows_read",
            "rows_written",
        ],
        "{selected}"
    );
    assert_eq!(result["rows"], json!([[1], [2]]));
    assert_eq!(result["cols"][0]["name"], "value");
    assert_eq!(result["affected_row_count"].as_i64(), Some(0));
    assert_eq!(result["last_insert_rowid"], Value::Null);
    assert_eq!(result["autocommit"], true);
    assert!(
        result["rows_read"]
            .as_u64()
            .expect("rows_read is a JSON number")
            >= 2,
        "{selected}"
    );
    assert_eq!(result["rows_written"].as_u64(), Some(0));
    let duration = result["query_duration_ms"]
        .as_f64()
        .expect("query duration is a JSON number");
    assert!(duration.is_finite() && duration >= 0.0, "{selected}");
    assert_eq!(
        worker.call(WorkerCommand::CloseSession { session }, false)?["ok"],
        true
    );
    Ok(())
}

#[test]
fn guarded_statement_result_carries_engine_counters_and_duration() -> Result<()> {
    let worker = Worker::new()?;
    let session = worker.session()?;
    let created = worker.statement(
        &session,
        "CREATE TABLE guarded_counters(value INTEGER)",
        vec![],
    )?;
    assert_eq!(created["ok"], true, "{created}");
    let inserted = worker.statement(
        &session,
        "INSERT INTO guarded_counters VALUES (1), (2)",
        vec![],
    )?;
    assert_eq!(inserted["ok"], true, "{inserted}");

    let guarded =
        worker.guarded_statement(&session, "SELECT value FROM guarded_counters", vec![])?;
    assert_eq!(guarded["ok"], true, "{guarded}");
    let result = guarded["result"]
        .as_object()
        .expect("guarded statement result object");
    let mut keys = result.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    assert_eq!(
        keys,
        [
            "affected_row_count",
            "autocommit",
            "cols",
            "last_insert_rowid",
            "query_duration_ms",
            "rows",
            "rows_read",
            "rows_written",
        ],
        "{guarded}"
    );
    assert_eq!(result["rows"], json!([[1], [2]]));
    assert_eq!(result["cols"][0]["name"], "value");
    assert_eq!(result["affected_row_count"].as_i64(), Some(0));
    assert_eq!(result["last_insert_rowid"], Value::Null);
    assert_eq!(result["autocommit"], true);
    assert!(
        result["rows_read"]
            .as_u64()
            .expect("rows_read is a JSON number")
            >= 2,
        "{guarded}"
    );
    assert_eq!(result["rows_written"].as_u64(), Some(0));
    let duration = result["query_duration_ms"]
        .as_f64()
        .expect("query duration is a JSON number");
    assert!(duration.is_finite() && duration >= 0.0, "{guarded}");
    assert_eq!(
        worker.call(WorkerCommand::CloseSession { session }, false)?["ok"],
        true
    );
    Ok(())
}

#[test]
fn guarded_write_refusal_wire_shape_carries_no_statement_counters() -> Result<()> {
    let worker = Worker::new()?;
    let session = worker.session()?;
    let created = worker.statement(
        &session,
        "CREATE TABLE guarded_writes(value INTEGER)",
        vec![],
    )?;
    assert_eq!(created["ok"], true, "{created}");

    let refused =
        worker.guarded_statement(&session, "INSERT INTO guarded_writes VALUES (1)", vec![])?;
    assert_eq!(refused["ok"], true, "{refused}");
    let result = refused["result"]
        .as_object()
        .expect("guarded refusal object");
    let mut keys = result.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    assert_eq!(keys, ["reason", "requires_intent"], "{refused}");
    assert_eq!(
        refused["result"],
        json!({"requires_intent": true, "reason": "engine_writable"}),
        "{refused}"
    );
    for metric in ["rows_read", "rows_written", "query_duration_ms"] {
        assert!(
            result.get(metric).is_none(),
            "{metric} leaked into the refusal: {refused}"
        );
    }
    let count = worker.statement(&session, "SELECT count(*) FROM guarded_writes", vec![])?;
    assert_eq!(count["result"]["rows"], json!([[0]]), "{count}");
    assert_eq!(
        worker.call(WorkerCommand::CloseSession { session }, false)?["ok"],
        true
    );
    Ok(())
}

fn batch_statement(sql: &str) -> BatchStatement {
    BatchStatement {
        sql: sql.into(),
        args: vec![],
        named_args: vec![],
        want_rows: true,
    }
}

fn batch_bound_statement(sql: &str, args: Vec<StatementArg>) -> BatchStatement {
    BatchStatement {
        sql: sql.into(),
        args,
        named_args: vec![],
        want_rows: true,
    }
}

fn statement_result_without_duration(result: &Value) -> Value {
    let mut result = result.clone();
    if let Some(fields) = result.as_object_mut() {
        fields.remove("query_duration_ms");
    }
    result
}

#[test]
fn capabilities_answer_the_readonly_batch_protocol_on_v1_only() -> Result<()> {
    let worker = Worker::new()?;
    let capabilities = worker.call(WorkerCommand::Capabilities, false)?;
    assert_eq!(capabilities["ok"], true, "{capabilities}");
    assert_eq!(
        capabilities["result"],
        json!({"readonly_batch_protocol": 1, "open_guarded_statement": 2})
    );
    let session = worker.session()?;
    let empty = worker.guarded_batch(&session, vec![], 1024)?;
    assert_eq!(empty["ok"], false, "{empty}");
    assert_eq!(empty["execution_completed"], false, "{empty}");
    // A capability request creates no session and changes no state: the same
    // session keeps working after the empty batch was refused.
    assert_eq!(
        worker.statement(&session, "SELECT 5", vec![])?["result"]["rows"],
        json!([[5]])
    );

    let profile = Worker::with_profile(true)?;
    assert!(
        profile.call(WorkerCommand::Capabilities, false).is_err(),
        "v2 worker answered a v1-only capability command"
    );
    Ok(())
}

#[test]
fn fused_open_runs_the_first_read_in_the_open_request() -> Result<()> {
    let worker = Worker::new()?;
    let response = worker.call(
        WorkerCommand::OpenGuardedStatement {
            database: worker.fixture.identifier.clone(),
            sql: "SELECT 7 AS value".into(),
            args: vec![],
            named_args: vec![],
            want_rows: true,
            close_after_statement: false,
        },
        false,
    )?;
    assert_eq!(response["ok"], true, "{response}");
    assert_eq!(response["result"]["readonly"], false);
    assert!(
        response["result"].get("close").is_none(),
        "an open that did not ask for the close keeps its session: {response}"
    );
    let session = response["result"]["session"]
        .as_str()
        .expect("fused open returns its session");
    assert_eq!(session.len(), 64);
    assert_eq!(
        response["result"]["statement"]["status"], "ok",
        "{response}"
    );
    assert_eq!(
        response["result"]["statement"]["result"]["rows"],
        json!([[7]]),
        "{response}"
    );
    // The session is registered, so the caller can keep using it after the
    // statement that rode along with the open.
    assert_eq!(
        worker.statement(session, "SELECT 8", vec![])?["result"]["rows"],
        json!([[8]])
    );
    assert_eq!(
        worker.call(
            WorkerCommand::CloseSession {
                session: session.into()
            },
            false
        )?["ok"],
        true
    );
    Ok(())
}

#[test]
fn fused_open_refuses_a_write_before_it_runs_and_keeps_the_session() -> Result<()> {
    let worker = Worker::new()?;
    let response = worker.call(
        WorkerCommand::OpenGuardedStatement {
            database: worker.fixture.identifier.clone(),
            sql: "CREATE TABLE fused_guard_must_not_exist(id INTEGER)".into(),
            args: vec![],
            named_args: vec![],
            want_rows: true,
            close_after_statement: false,
        },
        false,
    )?;
    assert_eq!(response["ok"], true, "{response}");
    let session = response["result"]["session"]
        .as_str()
        .expect("fused open returns its session");
    assert_eq!(
        response["result"]["statement"]["status"], "requires_intent",
        "{response}"
    );
    let reason = response["result"]["statement"]["reason"]
        .as_str()
        .expect("refusal names its reason");
    assert!(
        [
            "engine_writable",
            "transaction_context",
            "connection_control"
        ]
        .contains(&reason),
        "{response}"
    );
    // The refusal is a proof the statement did not run, and the session the
    // open created is still usable for what the platform does next.
    assert_eq!(
        worker.statement(
            session,
            "SELECT count(*) FROM sqlite_master WHERE name = 'fused_guard_must_not_exist'",
            vec![],
        )?["result"]["rows"],
        json!([[0]])
    );
    assert_eq!(
        worker.call(
            WorkerCommand::CloseSession {
                session: session.into()
            },
            false
        )?["ok"],
        true
    );
    Ok(())
}

#[test]
fn fused_open_reports_a_statement_error_and_returns_the_session() -> Result<()> {
    let worker = Worker::new()?;
    let response = worker.call(
        WorkerCommand::OpenGuardedStatement {
            database: worker.fixture.identifier.clone(),
            sql: "SELECT * FROM fused_missing_table".into(),
            args: vec![],
            named_args: vec![],
            want_rows: true,
            close_after_statement: false,
        },
        false,
    )?;
    assert_eq!(response["ok"], true, "{response}");
    let statement = &response["result"]["statement"];
    assert_eq!(statement["status"], "error", "{response}");
    assert!(
        !statement["error"].as_str().unwrap_or("").is_empty(),
        "{response}"
    );
    let session = response["result"]["session"]
        .as_str()
        .expect("fused open returns its session");
    assert_eq!(
        worker.statement(session, "SELECT 9", vec![])?["result"]["rows"],
        json!([[9]])
    );
    assert_eq!(
        worker.call(
            WorkerCommand::CloseSession {
                session: session.into()
            },
            false
        )?["ok"],
        true
    );
    Ok(())
}

#[test]
fn fused_open_reports_an_open_failure_without_a_session() -> Result<()> {
    let worker = Worker::new()?;
    let invalid = DatabaseId {
        namespace: Namespace {
            tenant: String::new(),
            database: "runtime".into(),
        },
        branch: "main".into(),
    };
    let response = worker.call(
        WorkerCommand::OpenGuardedStatement {
            database: invalid,
            sql: "SELECT 1".into(),
            args: vec![],
            named_args: vec![],
            want_rows: true,
            close_after_statement: false,
        },
        false,
    )?;
    assert_eq!(response["ok"], false, "{response}");
    assert!(response["result"].is_null(), "{response}");
    // A refused open creates nothing: the worker still serves a normal session.
    let session = worker.session()?;
    assert_eq!(
        worker.statement(&session, "SELECT 6", vec![])?["result"]["rows"],
        json!([[6]])
    );
    Ok(())
}

#[test]
fn fused_close_retires_the_session_in_the_open_request() -> Result<()> {
    let worker = Worker::new()?;
    let response = worker.call(
        WorkerCommand::OpenGuardedStatement {
            database: worker.fixture.identifier.clone(),
            sql: "SELECT 7 AS value".into(),
            args: vec![],
            named_args: vec![],
            want_rows: true,
            close_after_statement: true,
        },
        false,
    )?;
    assert_eq!(response["ok"], true, "{response}");
    assert_eq!(
        response["result"]["statement"]["status"], "ok",
        "{response}"
    );
    assert_eq!(
        response["result"]["close"],
        json!({"status": "ok"}),
        "{response}"
    );
    let session = response["result"]["session"]
        .as_str()
        .expect("fused open returns its session");
    // The session is really gone: a later command naming it is refused instead
    // of being served by a session the platform believes it retired.
    let used = worker.statement(session, "SELECT 8", vec![])?;
    assert_eq!(used["ok"], false, "{used}");
    assert!(
        used["error"]
            .as_str()
            .unwrap_or("")
            .contains("unknown worker session"),
        "{used}"
    );
    // The worker keeps the closed tombstone for the retry window, so the
    // platform's fallback close stays idempotent after a lost reply.
    let closed = worker.call(
        WorkerCommand::CloseSession {
            session: session.into(),
        },
        false,
    )?;
    assert_eq!(closed["ok"], true, "{closed}");
    Ok(())
}

#[test]
fn fused_close_leaves_the_session_for_a_refused_write() -> Result<()> {
    let worker = Worker::new()?;
    let response = worker.call(
        WorkerCommand::OpenGuardedStatement {
            database: worker.fixture.identifier.clone(),
            sql: "CREATE TABLE fused_close_must_not_exist(id INTEGER)".into(),
            args: vec![],
            named_args: vec![],
            want_rows: true,
            close_after_statement: true,
        },
        false,
    )?;
    assert_eq!(response["ok"], true, "{response}");
    assert_eq!(
        response["result"]["statement"]["status"], "requires_intent",
        "{response}"
    );
    assert!(
        response["result"].get("close").is_none(),
        "a refused write must keep the session: {response}"
    );
    let session = response["result"]["session"]
        .as_str()
        .expect("fused open returns its session");
    // The committing path and the ordinary close both still have their session.
    assert_eq!(
        worker.statement(
            session,
            "SELECT count(*) FROM sqlite_master WHERE name = 'fused_close_must_not_exist'",
            vec![],
        )?["result"]["rows"],
        json!([[0]])
    );
    assert_eq!(
        worker.call(
            WorkerCommand::CloseSession {
                session: session.into()
            },
            false
        )?["ok"],
        true
    );
    Ok(())
}

#[test]
fn fused_close_retires_the_session_after_a_statement_error() -> Result<()> {
    let worker = Worker::new()?;
    let response = worker.call(
        WorkerCommand::OpenGuardedStatement {
            database: worker.fixture.identifier.clone(),
            sql: "SELECT * FROM fused_close_missing_table".into(),
            args: vec![],
            named_args: vec![],
            want_rows: true,
            close_after_statement: true,
        },
        false,
    )?;
    assert_eq!(response["ok"], true, "{response}");
    assert_eq!(
        response["result"]["statement"]["status"], "error",
        "{response}"
    );
    // A settled statement failure is answer data; the close still belongs to
    // it, so the client sees one error and a retired session.
    assert_eq!(
        response["result"]["statement"]["execution_completed"], false,
        "{response}"
    );
    assert_eq!(
        response["result"]["close"],
        json!({"status": "ok"}),
        "{response}"
    );
    let session = response["result"]["session"]
        .as_str()
        .expect("fused open returns its session");
    let used = worker.statement(session, "SELECT 9", vec![])?;
    assert_eq!(used["ok"], false, "{used}");
    Ok(())
}

#[test]
fn a_worker_that_predates_the_close_field_refuses_the_whole_body() -> Result<()> {
    let worker = Worker::new()?;
    // The command struct denies unknown fields, which is exactly what a binary
    // that predates `close_after_statement` is: it answers the parse refusal
    // before reading any session, so a field it cannot honor is never ignored
    // into a session the platform believes it retired.
    let (status, body) = worker.raw(
        r#"{"version":1,"request_id":"statement-test","command":{"command":"open_guarded_statement","database":{"namespace":{"tenant":"p4","database":"runtime"},"branch":"main"},"sql":"SELECT 1","args":[],"named_args":[],"want_rows":true,"close_after_session":true}}"#,
    )?;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body, r#"{"error":"invalid worker request"}"#);
    Ok(())
}

#[test]
fn guarded_batch_matches_the_single_statement_results_field_by_field() -> Result<()> {
    let worker = Worker::new()?;
    let session = worker.session()?;
    assert_eq!(
        worker.statement(
            &session,
            "CREATE TABLE batch_values(value INTEGER, label TEXT)",
            vec![]
        )?["ok"],
        true
    );
    assert_eq!(
        worker.statement(
            &session,
            "INSERT INTO batch_values VALUES (1, 'one'), (2, 'two')",
            vec![]
        )?["ok"],
        true
    );
    let select = "SELECT value, label FROM batch_values WHERE value >= ? ORDER BY value";
    let single = worker.guarded_statement(
        &session,
        select,
        vec![StatementArg::Integer { value: "1".into() }],
    )?;
    assert_eq!(single["ok"], true, "{single}");
    let repeated = (0..3)
        .map(|_| batch_bound_statement(select, vec![StatementArg::Integer { value: "1".into() }]))
        .collect::<Vec<_>>();
    let batch = worker.guarded_batch(&session, repeated, usize::MAX)?;
    assert_eq!(batch["ok"], true, "{batch}");
    let items = batch["result"]["items"].as_array().expect("batch items");
    assert_eq!(batch["result"]["next"], 3);
    assert_eq!(items.len(), 3);
    for (index, item) in items.iter().enumerate() {
        assert_eq!(item["index"], index, "{batch}");
        assert_eq!(item["status"], "ok", "{batch}");
        assert_eq!(
            statement_result_without_duration(&item["result"]),
            statement_result_without_duration(&single["result"]),
            "item {index} differs from the single statement path"
        );
    }
    assert_eq!(items[2]["result"]["rows"], json!([[1, "one"], [2, "two"]]));
    Ok(())
}

#[test]
fn guarded_batch_stops_before_a_write_and_proves_it_did_not_run() -> Result<()> {
    let worker = Worker::new()?;
    let session = worker.session()?;
    assert_eq!(
        worker.statement(&session, "CREATE TABLE batch_writes(value INTEGER)", vec![])?["ok"],
        true
    );
    let batch = worker.guarded_batch(
        &session,
        vec![
            batch_statement("SELECT 1"),
            batch_statement("INSERT INTO batch_writes VALUES (1)"),
            batch_statement("SELECT 3"),
        ],
        usize::MAX,
    )?;
    assert_eq!(batch["ok"], true, "{batch}");
    let items = batch["result"]["items"].as_array().expect("batch items");
    assert_eq!(batch["result"]["next"], 2);
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["status"], "ok");
    assert_eq!(
        items[1],
        json!({"index": 1, "status": "requires_intent", "reason": "engine_writable"})
    );
    // The refused write never ran, and the session that answered the block is
    // still usable for the statement the block did not reach.
    assert_eq!(
        worker.statement(&session, "SELECT count(*) FROM batch_writes", vec![])?["result"]["rows"],
        json!([[0]])
    );
    assert_eq!(
        worker.guarded_statement(&session, "SELECT 3", vec![])?["result"]["rows"],
        json!([[3]])
    );
    Ok(())
}

#[test]
fn guarded_batch_reports_a_sql_error_and_keeps_the_session_usable() -> Result<()> {
    let worker = Worker::new()?;
    let session = worker.session()?;
    // Integer overflow settles while stepping, so the failure proves the
    // statement ran. A prepare failure proves the opposite and is tested below.
    let batch = worker.guarded_batch(
        &session,
        vec![
            batch_statement("SELECT 1"),
            batch_statement("SELECT 1 UNION ALL SELECT abs(-9223372036854775808)"),
            batch_statement("SELECT 3"),
        ],
        usize::MAX,
    )?;
    assert_eq!(batch["ok"], true, "{batch}");
    let items = batch["result"]["items"].as_array().expect("batch items");
    assert_eq!(batch["result"]["next"], 2);
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["status"], "ok");
    assert_eq!(items[1]["index"], 1);
    assert_eq!(items[1]["status"], "error");
    assert_eq!(items[1]["execution_completed"], true, "{batch}");
    // A statement that cannot be prepared never entered execution.
    let rejected = worker.guarded_batch(
        &session,
        vec![
            batch_statement("SELECT 1"),
            batch_statement("SELECT * FROM batch_missing_table"),
        ],
        usize::MAX,
    )?;
    assert_eq!(rejected["ok"], true, "{rejected}");
    assert_eq!(
        rejected["result"]["items"][1]["execution_completed"], false,
        "{rejected}"
    );
    // The failed statements did not poison the session for the statement the
    // block stopped at.
    assert_eq!(
        worker.guarded_statement(&session, "SELECT 3", vec![])?["result"]["rows"],
        json!([[3]])
    );
    Ok(())
}

#[test]
fn guarded_batch_validation_refuses_the_whole_block_before_any_statement_runs() -> Result<()> {
    let worker = Worker::new()?;
    let session = worker.session()?;
    assert_eq!(
        worker.statement(
            &session,
            "CREATE TABLE batch_refused(value INTEGER)",
            vec![]
        )?["ok"],
        true
    );
    let invalid_argument = BatchStatement {
        sql: "INSERT INTO batch_refused VALUES (?)".into(),
        args: vec![StatementArg::Blob {
            base64: "not base64".into(),
        }],
        named_args: vec![],
        want_rows: true,
    };
    for (name, statements) in [
        ("empty", vec![]),
        (
            "too many",
            (0..9)
                .map(|index| batch_statement(&format!("SELECT {index}")))
                .collect(),
        ),
        (
            "invalid argument",
            vec![batch_statement("SELECT 1"), invalid_argument],
        ),
    ] {
        let batch = worker.guarded_batch(&session, statements, usize::MAX)?;
        assert_eq!(batch["ok"], false, "{name}: {batch}");
        assert_eq!(batch["execution_completed"], false, "{name}: {batch}");
    }
    assert_eq!(
        worker.statement(&session, "SELECT count(*) FROM batch_refused", vec![])?["result"]["rows"],
        json!([[0]])
    );
    Ok(())
}

#[test]
fn guarded_batch_returns_the_statement_that_crosses_the_result_budget_and_stops() -> Result<()> {
    let worker = Worker::new()?;
    let session = worker.session()?;
    let batch = worker.guarded_batch(
        &session,
        vec![
            batch_statement("SELECT 1"),
            batch_statement("SELECT 2"),
            batch_statement("SELECT 3"),
        ],
        1,
    )?;
    assert_eq!(batch["ok"], true, "{batch}");
    let items = batch["result"]["items"].as_array().expect("batch items");
    assert_eq!(batch["result"]["next"], 1);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["status"], "ok");
    assert_eq!(items[0]["result"]["rows"], json!([[1]]));
    // The entries the block did not reach were not executed by the block; a
    // caller can resume at `next` without replaying.
    let resumed = worker.guarded_batch(
        &session,
        vec![batch_statement("SELECT 2"), batch_statement("SELECT 3")],
        usize::MAX,
    )?;
    assert_eq!(resumed["result"]["next"], 2);
    Ok(())
}

#[test]
fn guarded_batch_projects_rows_but_still_reports_counters() -> Result<()> {
    let worker = Worker::new()?;
    let session = worker.session()?;
    assert_eq!(
        worker.statement(
            &session,
            "CREATE TABLE batch_projection(value INTEGER)",
            vec![]
        )?["ok"],
        true
    );
    assert_eq!(
        worker.statement(
            &session,
            "INSERT INTO batch_projection VALUES (1), (2)",
            vec![]
        )?["ok"],
        true
    );
    let batch = worker.guarded_batch(
        &session,
        vec![BatchStatement {
            sql: "SELECT value FROM batch_projection ORDER BY value".into(),
            args: vec![],
            named_args: vec![],
            want_rows: false,
        }],
        usize::MAX,
    )?;
    assert_eq!(batch["ok"], true, "{batch}");
    let result = &batch["result"]["items"][0]["result"];
    assert_eq!(result["rows"], json!([]), "{batch}");
    assert!(result["rows_read"].as_u64().unwrap() >= 2, "{batch}");
    Ok(())
}
