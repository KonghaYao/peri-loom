use std::env;
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use peri_loom_p0::database::SegmentedDatabase;
use peri_loom_p0::storage::SegmentedStorage;
use serde::Serialize;
use serde_json::{json, Value as JsonValue};
use turso_core::{Numeric, Value};

const USAGE: &str = "usage: peri-loom-p0 <directory> <command> [args]\ncommands: exec SQL | query SQL | checkpoint | inspect | demo [rows] | crash-write COUNT | checkpoint-wait";

fn main() {
    if let Err(error) = run() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let mut arguments = env::args_os().skip(1);
    let root = PathBuf::from(arguments.next().context(USAGE)?);
    let command = arguments.next().context(USAGE)?;
    let command = command.to_str().context("command must be valid UTF-8")?;
    let arguments = arguments
        .map(|argument| {
            argument
                .into_string()
                .map_err(|_| anyhow::anyhow!("arguments must be valid UTF-8"))
        })
        .collect::<Result<Vec<_>>>()?;
    match command {
        "exec" | "query" if arguments.len() == 1 => {}
        "checkpoint" | "checkpoint-wait" | "inspect" if arguments.is_empty() => {}
        "demo" if arguments.len() <= 1 => {}
        "crash-write" if arguments.len() == 1 => {}
        _ => bail!(USAGE),
    }
    let count = match command {
        "demo" => Some(parse_count(
            arguments.first().map(String::as_str).unwrap_or("400"),
        )?),
        "crash-write" => Some(parse_count(&arguments[0])?),
        _ => None,
    };
    if command == "inspect" {
        let storage = SegmentedStorage::open(&root)?;
        return print_json(&json!({"manifest": storage.manifest(), "stats": storage.stats()}));
    }
    let database = SegmentedDatabase::open(&root)?;
    match command {
        "exec" => {
            database.execute(&arguments[0])?;
            print_json(&json!({"stats": database.storage().stats()}))
        }
        "query" => {
            let rows = database.query(&arguments[0])?;
            let rows = rows
                .iter()
                .map(|row| row.iter().map(sql_value).collect::<Vec<_>>())
                .collect::<Vec<_>>();
            print_json(&rows)
        }
        "checkpoint" | "checkpoint-wait" => {
            database.checkpoint()?;
            let message = json!({
                "ready": command == "checkpoint-wait",
                "manifest": database.storage().manifest(),
                "stats": database.storage().stats()
            });
            print_json(&message)?;
            if command == "checkpoint-wait" {
                park();
            }
            Ok(())
        }
        "demo" | "crash-write" => {
            let count = count.context("missing record count")?;
            let (first_id, last_id) = append_records(&database, count)?;
            if command == "demo" {
                database.checkpoint()?;
            }
            print_json(&json!({
                "ready": command == "crash-write",
                "inserted": count,
                "first_id": first_id,
                "last_id": last_id,
                "manifest": database.storage().manifest(),
                "stats": database.storage().stats()
            }))?;
            if command == "crash-write" {
                park();
            }
            Ok(())
        }
        _ => unreachable!(),
    }
}

fn parse_count(argument: &str) -> Result<i64> {
    let count = argument
        .parse::<i64>()
        .context("record count must be a positive integer")?;
    if count <= 0 {
        bail!("record count must be a positive integer");
    }
    Ok(count)
}

fn append_records(database: &SegmentedDatabase, count: i64) -> Result<(i64, i64)> {
    database
        .execute("CREATE TABLE IF NOT EXISTS p0_records(id INTEGER PRIMARY KEY, payload TEXT)")?;
    database.execute("BEGIN IMMEDIATE")?;
    let result = (|| {
        let rows = database.query("SELECT coalesce(max(id), 0) FROM p0_records")?;
        let Some(Value::Numeric(Numeric::Integer(previous_id))) =
            rows.first().and_then(|row| row.first())
        else {
            bail!("p0_records maximum ID is not an integer");
        };
        let first_id = previous_id.checked_add(1).context("record ID overflow")?;
        let last_id = previous_id
            .checked_add(count)
            .context("record ID overflow")?;
        let payload = "x".repeat(2048);
        for record_id in first_id..=last_id {
            database.execute(&format!(
                "INSERT INTO p0_records(id, payload) VALUES ({record_id}, '{payload}')"
            ))?;
        }
        database.execute("COMMIT")?;
        Ok((first_id, last_id))
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
        Value::Blob(blob) => json!(blob.as_slice()),
    }
}

fn print_json(value: &impl Serialize) -> Result<()> {
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
