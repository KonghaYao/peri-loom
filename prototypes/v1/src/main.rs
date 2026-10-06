use std::io::{self, Write};
use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};
use peri_loom_v1::cache::SegmentCache;
use peri_loom_v1::config::backend;
use peri_loom_v1::database::RemoteDatabase;
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::storage::RemoteStorage;
use serde_json::{json, Value as JsonValue};
use turso_core::{Numeric, Value};

fn main() {
    if let Err(error) = run() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args
        .first()
        .is_some_and(|command| peri_loom_v1::service_cli::handles(command))
    {
        return peri_loom_v1::service_cli::run(&args);
    }
    if args.first().is_some_and(|command| command == "worker") {
        ensure!(args.len() == 3, "usage: peri-loom-v1 worker STORE CACHE");
        return peri_loom_v1::runtime_cli::run(backend(&args[1])?, &args[2]);
    }
    ensure!(
        args.len() >= 5,
        "usage: peri-loom-v1 STORE CACHE TENANT DB COMMAND [ARG]"
    );
    let store = backend(&args[0])?;
    let namespace = Namespace::new(&args[2], &args[3])?;
    let command = args[4].as_str();
    if peri_loom_v1::capability_cli::handles(command) {
        return output(&peri_loom_v1::capability_cli::run(
            store,
            namespace,
            std::path::Path::new(&args[1]),
            &args[4..],
        )?);
    }
    if command == "inspect" {
        ensure!(args.len() == 5, "inspect takes no arguments");
        let cache = Arc::new(SegmentCache::new(
            &args[1],
            8 * 1024 * 1024,
            64 * 1024 * 1024,
        )?);
        let storage = RemoteStorage::open(store, namespace, cache)?;
        return output(&json!({"manifest": storage.manifest(), "stats": storage.stats()}));
    }
    let database = RemoteDatabase::open(store, namespace, &args[1], 0)?;
    match command {
        "exec" | "query" => {
            ensure!(args.len() == 6, "SQL must be a single argument");
            if command == "exec" {
                database.execute(&args[5])?;
                output(&json!({"ok": true}))
            } else {
                let rows = database.query(&args[5])?;
                output(&JsonValue::Array(
                    rows.iter()
                        .map(|row| JsonValue::Array(row.iter().map(sql_value).collect()))
                        .collect(),
                ))
            }
        }
        "checkpoint" | "checkpoint-wait" => {
            ensure!(args.len() == 5, "checkpoint takes no arguments");
            database.checkpoint()?;
            output(
                &json!({"ready": command == "checkpoint-wait", "manifest": database.storage().manifest()}),
            )?;
            if command == "checkpoint-wait" {
                park();
            }
            Ok(())
        }
        "demo" | "crash-write" => {
            let count = args
                .get(5)
                .map(String::as_str)
                .unwrap_or("400")
                .parse::<i64>()?;
            ensure!(
                args.len() <= 6 && count > 0 && count <= 100000,
                "invalid row count"
            );
            append(&database, count)?;
            if command == "demo" {
                database.checkpoint()?;
            }
            output(
                &json!({"ready": command == "crash-write", "inserted": count, "manifest": database.storage().manifest()}),
            )?;
            if command == "crash-write" {
                park();
            }
            Ok(())
        }
        _ => bail!("unknown command"),
    }
}

fn append(database: &RemoteDatabase, count: i64) -> Result<()> {
    database.execute("CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, payload TEXT)")?;
    database.execute("BEGIN IMMEDIATE")?;
    let result = (|| {
        let rows = database.query("SELECT coalesce(max(id), 0) FROM records")?;
        let Some(Value::Numeric(Numeric::Integer(previous))) =
            rows.first().and_then(|row| row.first())
        else {
            bail!("invalid maximum ID");
        };
        let last = previous.checked_add(count).context("record ID overflow")?;
        let first = previous.checked_add(1).context("record ID overflow")?;
        let payload = "x".repeat(2048);
        for record in first..=last {
            database.execute(&format!(
                "INSERT INTO records VALUES ({record}, '{payload}')"
            ))?;
        }
        database.execute("COMMIT")
    })();
    if result.is_err() {
        let _ = database.execute("ROLLBACK");
    }
    result
}

fn sql_value(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Numeric(Numeric::Integer(integer)) => json!(integer),
        Value::Numeric(Numeric::Float(float)) => json!(f64::from(*float)),
        Value::Text(text) => json!(text.as_str()),
        Value::Blob(bytes) => json!(bytes.as_slice()),
    }
}

fn output(value: &JsonValue) -> Result<()> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, value)?;
    writeln!(stdout)?;
    stdout.flush()?;
    Ok(())
}

fn park() -> ! {
    loop {
        std::thread::park();
    }
}
