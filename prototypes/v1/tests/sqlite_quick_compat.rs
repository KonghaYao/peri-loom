use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};
use peri_loom_v1::database::RemoteDatabase;
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::LocalObjectStore;
use turso_core::{Numeric, Value};

#[derive(Debug, PartialEq)]
enum Cell {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
}

struct Case {
    name: &'static str,
    setup: &'static str,
    rejected: Option<&'static str>,
    query: &'static str,
}

fn oracle(database: &Path, sql: &str) -> Result<Output> {
    Command::new("/usr/bin/sqlite3")
        .args(["-init", "/dev/null", "-batch", "-bail", "-json"])
        .arg(database)
        .arg(sql)
        .output()
        .context("execute installed SQLite oracle")
}

fn oracle_rows(output: Output) -> Result<Vec<Vec<Cell>>> {
    ensure!(output.status.success(), "SQLite oracle query failed");
    if output.stdout.is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<serde_json::Map<String, serde_json::Value>> =
        serde_json::from_slice(&output.stdout)?;
    rows.into_iter()
        .map(|row| {
            (0..row.len())
                .map(|column| match row.get(&format!("c{column}")) {
                    Some(serde_json::Value::Null) => Ok(Cell::Null),
                    Some(serde_json::Value::String(text)) => Ok(Cell::Text(text.clone())),
                    Some(serde_json::Value::Number(number)) => {
                        if let Some(integer) = number.as_i64() {
                            Ok(Cell::Integer(integer))
                        } else {
                            Ok(Cell::Real(number.as_f64().context("oracle real")?))
                        }
                    }
                    _ => bail!("unexpected oracle column or type"),
                })
                .collect()
        })
        .collect()
}

fn native_rows(rows: Vec<Vec<Value>>) -> Result<Vec<Vec<Cell>>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| match value {
                    Value::Null => Ok(Cell::Null),
                    Value::Numeric(Numeric::Integer(integer)) => Ok(Cell::Integer(integer)),
                    Value::Numeric(Numeric::Float(real)) => Ok(Cell::Real(real.into())),
                    Value::Text(text) => Ok(Cell::Text(text.as_str().to_owned())),
                    Value::Blob(_) => bail!("unexpected native blob"),
                })
                .collect()
        })
        .collect()
}

#[test]
#[ignore = "historical SQLite differential test; reference workflow retired from v1 gate"]
fn sqlite_quick_semantics_match_installed_oracle() -> Result<()> {
    let oracle_version = oracle_rows(oracle(
        Path::new(":memory:"),
        "SELECT sqlite_version() AS c0",
    )?)?;
    println!("sqlite_quick_compat oracle_version={oracle_version:?} native_package_version={} scope=15_case_subset_not_official_sqlite_suite", env!("CARGO_PKG_VERSION"));
    let cases = [
        Case {
            name: "null_logic",
            setup: "",
            rejected: None,
            query: "SELECT NULL AS c0, NULL = NULL AS c1, NULL IS NULL AS c2, coalesce(NULL, 7) AS c3, 0 AND NULL AS c4, 1 OR NULL AS c5",
        },
        Case {
            name: "unicode_text",
            setup: "",
            rejected: None,
            query: "SELECT '雪🙂é' AS c0, length('雪🙂é') AS c1, substr('雪🙂é', 2, 1) AS c2, '雪' || '界' AS c3, hex('雪') AS c4",
        },
        Case {
            name: "exact_i64_boundaries",
            setup: "CREATE TABLE numbers(value INTEGER); INSERT INTO numbers VALUES(-9223372036854775808),(9007199254740993),(9223372036854775807)",
            rejected: None,
            query: "SELECT value AS c0, typeof(value) AS c1 FROM numbers ORDER BY value",
        },
        Case {
            name: "integer_and_real_arithmetic",
            setup: "",
            rejected: None,
            query: "SELECT 7/2 AS c0, 7/2.0 AS c1, -7%3 AS c2, 2+3*4 AS c3, 1/0 AS c4, 0.5+0.25 AS c5",
        },
        Case {
            name: "grouped_aggregates",
            setup: "CREATE TABLE measures(category TEXT, value INTEGER); INSERT INTO measures VALUES('a',2),('a',NULL),('a',4),('b',8)",
            rejected: None,
            query: "SELECT category AS c0, count(*) AS c1, count(value) AS c2, sum(value) AS c3, avg(value) AS c4, min(value) AS c5, max(value) AS c6 FROM measures GROUP BY category ORDER BY category",
        },
        Case {
            name: "empty_aggregates",
            setup: "CREATE TABLE empty_values(value INTEGER)",
            rejected: None,
            query: "SELECT count(*) AS c0, sum(value) AS c1, avg(value) AS c2, min(value) AS c3, total(value) AS c4 FROM empty_values",
        },
        Case {
            name: "recursive_cte",
            setup: "",
            rejected: None,
            query: "WITH RECURSIVE sequence(value) AS (VALUES(1) UNION ALL SELECT value+1 FROM sequence WHERE value<5) SELECT value AS c0, value*value AS c1 FROM sequence ORDER BY value",
        },
        Case {
            name: "left_join_null_extension",
            setup: "CREATE TABLE parents(id INTEGER); CREATE TABLE children(parent_id INTEGER, label TEXT); INSERT INTO parents VALUES(1),(2); INSERT INTO children VALUES(1,'child')",
            rejected: None,
            query: "SELECT parents.id AS c0, children.label AS c1 FROM parents LEFT JOIN children ON parents.id=children.parent_id ORDER BY parents.id",
        },
        Case {
            name: "distinct_null_ordering",
            setup: "CREATE TABLE ordered_values(value INTEGER); INSERT INTO ordered_values VALUES(2),(NULL),(1),(2),(NULL)",
            rejected: None,
            query: "SELECT DISTINCT value AS c0 FROM ordered_values ORDER BY value",
        },
        Case {
            name: "column_affinity",
            setup: "CREATE TABLE affinity(integer_value INTEGER, text_value TEXT, real_value REAL); INSERT INTO affinity VALUES('42',42,42)",
            rejected: None,
            query: "SELECT integer_value AS c0, typeof(integer_value) AS c1, text_value AS c2, typeof(text_value) AS c3, real_value AS c4, typeof(real_value) AS c5 FROM affinity",
        },
        Case {
            name: "transaction_commit",
            setup: "CREATE TABLE committed(id INTEGER); BEGIN; INSERT INTO committed VALUES(1),(2); COMMIT",
            rejected: None,
            query: "SELECT id AS c0 FROM committed ORDER BY id",
        },
        Case {
            name: "transaction_rollback",
            setup: "CREATE TABLE rolled_back(id INTEGER); INSERT INTO rolled_back VALUES(1); BEGIN; INSERT INTO rolled_back VALUES(2); ROLLBACK",
            rejected: None,
            query: "SELECT id AS c0 FROM rolled_back ORDER BY id",
        },
        Case {
            name: "unique_constraint_preserves_rows",
            setup: "CREATE TABLE unique_values(id INTEGER PRIMARY KEY, label TEXT UNIQUE); INSERT INTO unique_values VALUES(1,'kept')",
            rejected: Some("INSERT INTO unique_values VALUES(2,'kept')"),
            query: "SELECT id AS c0, label AS c1 FROM unique_values ORDER BY id",
        },
        Case {
            name: "not_null_constraint_preserves_rows",
            setup: "CREATE TABLE required_values(id INTEGER, label TEXT NOT NULL); INSERT INTO required_values VALUES(1,'kept')",
            rejected: Some("INSERT INTO required_values VALUES(2,NULL)"),
            query: "SELECT id AS c0, label AS c1 FROM required_values ORDER BY id",
        },
        Case {
            name: "check_constraint_preserves_rows",
            setup: "CREATE TABLE positive_values(value INTEGER CHECK(value>0)); INSERT INTO positive_values VALUES(1)",
            rejected: Some("INSERT INTO positive_values VALUES(-1)"),
            query: "SELECT value AS c0 FROM positive_values ORDER BY value",
        },
    ];
    for case in cases {
        println!("sqlite_quick_compat case={} begin", case.name);
        let objects = tempfile::tempdir()?;
        let cache = tempfile::tempdir()?;
        let oracle_directory = tempfile::tempdir()?;
        let oracle_path = oracle_directory.path().join("oracle.sqlite");
        let database = Arc::new(RemoteDatabase::open(
            Arc::new(LocalObjectStore::open(objects.path())?),
            Namespace::new("sqlite_quick_compat", case.name)?,
            cache.path(),
            0,
        )?);
        let session = database.session()?;
        let engine_version = native_rows(session.query("SELECT sqlite_version() AS c0")?)?;
        println!(
            "sqlite_quick_compat case={} engine_sqlite_version={engine_version:?}",
            case.name
        );
        if !case.setup.is_empty() {
            ensure!(
                oracle(&oracle_path, case.setup)?.status.success(),
                "case={} oracle setup failed",
                case.name
            );
            session
                .execute(case.setup)
                .with_context(|| format!("case={} native setup", case.name))?;
        }
        if let Some(rejected) = case.rejected {
            ensure!(
                !oracle(&oracle_path, rejected)?.status.success(),
                "case={} oracle must reject constraint violation",
                case.name
            );
            ensure!(
                session.execute(rejected).is_err(),
                "case={} native must reject constraint violation",
                case.name
            );
        }
        ensure!(
            !session.in_transaction()?,
            "case={} transaction left open",
            case.name
        );
        let expected = oracle_rows(oracle(&oracle_path, case.query)?)?;
        let actual = native_rows(
            session
                .query(case.query)
                .with_context(|| format!("case={} native query", case.name))?,
        )?;
        assert_eq!(actual, expected, "case={} SQLite differential", case.name);
        println!("sqlite_quick_compat case={} pass", case.name);
    }
    Ok(())
}
