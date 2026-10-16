//! E01 native TursoDB harness: executes a frozen SQL plan against a local
//! persistent database and reports exactly what the engine returned.
//!
//! `import` and `query` are separate processes, so the fixture is reopened from
//! disk before any query runs. The harness never rewrites caller SQL, never
//! retries, and never turns a failure into a success: a prepare or bind failure
//! is reported as `not_dispatched`, and an execution error as `known_failure`.

use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::num::NonZero;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde_json::{json, Value as Json};
use turso_core::io::IO;
use turso_core::{
    Connection, Database, LimboError, Numeric, OpenOptions, SqliteDialect, SyncMode, UnixIO, Value,
};

const MAX_PLAN_BYTES: u64 = 64 * 1024 * 1024;
const MAX_MESSAGE_CHARS: usize = 200;

type Fallible<T> = Result<T, String>;

struct Plan {
    database_path: String,
    import_path: Option<String>,
    schema: Vec<String>,
    dataset_rows: i64,
    sync_mode: SyncMode,
    sync_mode_name: String,
    ops: Vec<Op>,
}

#[derive(Clone)]
struct Op {
    op_id: String,
    sql: String,
    params: Vec<Value>,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("e01 native harness: {error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Fallible<()> {
    let mut arguments = std::env::args().skip(1);
    let mode = arguments.next().ok_or("missing mode: import|query|explain")?;
    let mut plan_path = None;
    let mut out_path = None;
    let mut connections = 1_usize;
    while let Some(flag) = arguments.next() {
        let value = arguments
            .next()
            .ok_or_else(|| format!("{flag} requires a value"))?;
        match flag.as_str() {
            "--plan" => plan_path = Some(value),
            "--out" => out_path = Some(value),
            "--connections" => {
                connections = value
                    .parse::<usize>()
                    .map_err(|_| "invalid --connections".to_string())?;
                if connections < 1 || connections > 256 {
                    return Err("--connections must be 1..=256".to_string());
                }
            }
            other => return Err(format!("unknown argument {other}")),
        }
    }
    let plan_path = plan_path.ok_or("--plan is required")?;
    let out_path = out_path.ok_or("--out is required")?;
    let plan = load_plan(&plan_path)?;
    let start = Instant::now();
    let file = create_output(&out_path)?;
    let mut writer = BufWriter::new(file);
    match mode.as_str() {
        "import" => import(&plan, &mut writer, start),
        "query" => query(&plan, &mut writer, start, connections),
        "explain" => explain(&plan, &mut writer, start),
        other => Err(format!("unknown mode {other}")),
    }
}

fn load_plan(path: &str) -> Fallible<Plan> {
    let file = File::open(path).map_err(|error| format!("plan open failed: {error}"))?;
    let length = file
        .metadata()
        .map_err(|error| format!("plan stat failed: {error}"))?
        .len();
    if length > MAX_PLAN_BYTES {
        return Err("plan exceeds the 64 MiB cap".to_string());
    }
    let document: Json = serde_json::from_reader(BufReader::new(file))
        .map_err(|error| format!("plan is not valid JSON: {error}"))?;
    let allowed = [
        "version",
        "database_path",
        "import_path",
        "import_transaction",
        "schema",
        "dataset_rows",
        "sync_mode",
        "ops",
    ];
    let object = document.as_object().ok_or("plan must be a JSON object")?;
    for key in object.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(format!("unexpected plan field {key}"));
        }
    }
    let sync_mode_name = string_field(&document, "sync_mode")?;
    let sync_mode = match sync_mode_name.as_str() {
        "full" => SyncMode::Full,
        "normal" => SyncMode::Normal,
        "off" => SyncMode::Off,
        other => return Err(format!("unsupported sync_mode {other}")),
    };
    let transaction = string_field(&document, "import_transaction")?;
    if transaction != "single" {
        return Err(format!("unsupported import_transaction {transaction}"));
    }
    let schema = document
        .get("schema")
        .and_then(Json::as_array)
        .ok_or("schema must be an array")?
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| "schema entries must be strings".to_string())
        })
        .collect::<Fallible<Vec<String>>>()?;
    let ops = load_ops(&document)?;
    Ok(Plan {
        database_path: string_field(&document, "database_path")?,
        import_path: document
            .get("import_path")
            .map(|value| value.as_str().map(str::to_owned).ok_or_else(|| "import_path must be a string".to_string()))
            .transpose()?,
        schema,
        dataset_rows: document
            .get("dataset_rows")
            .and_then(Json::as_i64)
            .ok_or("dataset_rows must be an integer")?,
        sync_mode,
        sync_mode_name,
        ops,
    })
}

fn load_ops(document: &Json) -> Fallible<Vec<Op>> {
    let mut ops = Vec::new();
    for entry in document
        .get("ops")
        .and_then(Json::as_array)
        .ok_or("ops must be an array")?
    {
        let allowed = ["op_id", "sql", "params"];
        for key in entry.as_object().ok_or("each op must be an object")?.keys() {
            if !allowed.contains(&key.as_str()) {
                return Err(format!("unexpected op field {key}"));
            }
        }
        let mut params = Vec::new();
        match entry.get("params") {
            None => {}
            Some(Json::Array(list)) => {
                for param in list {
                    params.push(bind_value(param)?);
                }
            }
            Some(_) => return Err("op params must be an array".to_string()),
        }
        ops.push(Op {
            op_id: string_field(entry, "op_id")?,
            sql: string_field(entry, "sql")?,
            params,
        });
    }
    Ok(ops)
}

fn bind_value(value: &Json) -> Fallible<Value> {
    match value {
        Json::Number(number) => number
            .as_i64()
            .map(Value::from_i64)
            .ok_or_else(|| "plan parameters must be 64-bit integers or strings".to_string()),
        Json::String(text) => Ok(Value::build_text(text.clone())),
        other => Err(format!("unsupported plan parameter {other}")),
    }
}

fn string_field(document: &Json, key: &str) -> Fallible<String> {
    document
        .get(key)
        .and_then(Json::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("plan field {key} must be a string"))
}

struct Fixture {
    _database: Arc<Database>,
    connection: Arc<Connection>,
}

fn open(plan: &Plan) -> Fallible<Fixture> {
    let path = std::path::Path::new(&plan.database_path);
    let io: Arc<dyn IO> = Arc::new(UnixIO::new().map_err(|error| format!("io init failed: {error}"))?);
    let options = OpenOptions::new(Arc::new(SqliteDialect));
    let database = Database::open(io, &plan.database_path, options)
        .map_err(|error| format!("database open failed ({}): {}", path.display(), describe(&error)))?;
    let connection = database
        .connect()
        .map_err(|error| format!("connect failed: {}", describe(&error)))?;
    connection.set_sync_mode(plan.sync_mode);
    connection.set_load_extension_enabled(false);
    Ok(Fixture {
        _database: database,
        connection,
    })
}

fn import(plan: &Plan, writer: &mut impl Write, start: Instant) -> Fallible<()> {
    let source = plan
        .import_path
        .as_deref()
        .ok_or("import mode requires import_path")?;
    let fixture = open(plan)?;
    let schema_started = nanos(start);
    for statement in &plan.schema {
        fixture
            .connection
            .execute(statement)
            .map_err(|error| format!("schema statement failed: {}", describe(&error)))?;
    }
    let schema_completed = nanos(start);
    write_event(
        writer,
        &json!({
            "kind": "schema",
            "statements": plan.schema.len(),
            "started_ns": schema_started,
            "completed_ns": schema_completed,
        }),
    )?;
    let reader = BufReader::new(
        File::open(source).map_err(|error| format!("import statements open failed: {error}"))?,
    );
    let import_started = nanos(start);
    let mut statements = 0_i64;
    let mut failure = None;
    fixture
        .connection
        .execute("BEGIN")
        .map_err(|error| format!("begin failed: {}", describe(&error)))?;
    for line in reader.lines() {
        let line = line.map_err(|error| format!("import read failed: {error}"))?;
        if line.trim().is_empty() {
            continue;
        }
        let statement: String = serde_json::from_str(&line)
            .map_err(|error| format!("import statement is not a JSON string: {error}"))?;
        if let Err(error) = fixture.connection.execute(&statement) {
            failure = Some(describe(&error));
            break;
        }
        statements += 1;
    }
    if let Some(error) = failure {
        // 事务内的部分导入必须回滚并暴露，不能被当作成功数据集。
        let _ = fixture.connection.execute("ROLLBACK");
        return Err(format!("import statement failed after {statements} statements: {error}"));
    }
    fixture
        .connection
        .execute("COMMIT")
        .map_err(|error| format!("commit failed: {}", describe(&error)))?;
    let rows = count_rows(&fixture.connection)?;
    let import_completed = nanos(start);
    if rows != plan.dataset_rows {
        return Err(format!(
            "imported row count {rows} does not match declared {}",
            plan.dataset_rows
        ));
    }
    write_event(
        writer,
        &json!({
            "kind": "import",
            "statements": statements,
            "rows": rows,
            "started_ns": import_started,
            "completed_ns": import_completed,
            "sync_mode": plan.sync_mode_name,
        }),
    )
}

fn query(plan: &Plan, writer: &mut impl Write, start: Instant, connections: usize) -> Fallible<()> {
    let io: Arc<dyn IO> = Arc::new(UnixIO::new().map_err(|error| format!("io init failed: {error}"))?);
    let database = Database::open(io, &plan.database_path, OpenOptions::new(Arc::new(SqliteDialect)))
        .map_err(|error| format!("database open failed: {}", describe(&error)))?;
    let counting = database.connect().map_err(|error| format!("connect failed: {}", describe(&error)))?;
    counting.set_sync_mode(plan.sync_mode);
    counting.set_load_extension_enabled(false);
    let rows = count_rows(&counting)?;
    if rows != plan.dataset_rows {
        return Err(format!("fixture holds {rows} rows but the plan declares {}", plan.dataset_rows));
    }
    let queue = Arc::new(Mutex::new(plan.ops.iter().cloned().enumerate().collect::<VecDeque<_>>()));
    let collected: Arc<Mutex<Vec<(usize, Json)>>> = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for _ in 0..connections {
        // 每个线程持有自己的连接：串行独占、结束即释放。
        let connection = database.connect().map_err(|error| format!("connect failed: {}", describe(&error)))?;
        connection.set_sync_mode(plan.sync_mode);
        connection.set_load_extension_enabled(false);
        let (queue, collected) = (queue.clone(), collected.clone());
        handles.push(std::thread::spawn(move || {
            loop {
                let item = { queue.lock().expect("queue lock").pop_front() };
                let Some((index, op)) = item else { break };
                let planned = nanos(start);
                let mut statement = match connection.prepare(&op.sql) {
                    Ok(statement) => statement,
                    Err(error) => {
                        collected.lock().expect("collect lock").push((index, not_dispatched(&op, planned, &error)));
                        continue;
                    }
                };
                let mut bind_error = None;
                for (position, value) in op.params.iter().enumerate() {
                    let slot = NonZero::new(position + 1).expect("parameter positions start at one");
                    if let Err(error) = statement.bind_at(slot, value.clone()) {
                        bind_error = Some(error);
                        break;
                    }
                }
                if let Some(error) = bind_error {
                    collected.lock().expect("collect lock").push((index, not_dispatched(&op, planned, &error)));
                    continue;
                }
                let dispatched = nanos(start);
                let event = match statement.run_collect_rows() {
                    Ok(values) => json!({
                        "kind": "op", "op_id": op.op_id, "outcome": "success",
                        "planned_ns": planned, "dispatched_ns": dispatched, "completed_ns": nanos(start),
                        "rows": values.iter().map(|row| row_json(row)).collect::<Vec<Json>>(),
                        "error_class": Json::Null, "error_message": Json::Null}),
                    Err(error) => json!({
                        "kind": "op", "op_id": op.op_id, "outcome": "known_failure",
                        "planned_ns": planned, "dispatched_ns": dispatched, "completed_ns": nanos(start),
                        "rows": Json::Null, "error_class": class(&error), "error_message": message(&error)}),
                };
                collected.lock().expect("collect lock").push((index, event));
            }
        }));
    }
    for handle in handles {
        handle.join().map_err(|_| "worker thread panicked".to_string())?;
    }
    let mut events = collected.lock().expect("collect lock").drain(..).collect::<Vec<_>>();
    events.sort_by_key(|(index, _)| *index);
    for (_, event) in events {
        write_event(writer, &event)?;
    }
    Ok(())
}

fn explain(plan: &Plan, writer: &mut impl Write, start: Instant) -> Fallible<()> {
    let fixture = open(plan)?;
    for op in &plan.ops {
        let sql = format!("EXPLAIN QUERY PLAN {}", op.sql);
        let planned = nanos(start);
        let event = match fixture.connection.prepare(&sql) {
            Ok(mut statement) => {
                let mut bind_error = None;
                for (index, value) in op.params.iter().enumerate() {
                    let position = NonZero::new(index + 1).expect("parameter positions start at one");
                    if let Err(error) = statement.bind_at(position, value.clone()) {
                        bind_error = Some(error);
                        break;
                    }
                }
                match bind_error {
                    Some(error) => not_dispatched(op, planned, &error),
                    None => match statement.run_collect_rows() {
                        Ok(values) => json!({
                            "kind": "explain",
                            "op_id": op.op_id,
                            "outcome": "success",
                            "rows": values.iter().map(|row| row_json(row)).collect::<Vec<Json>>(),
                            "error_class": Json::Null,
                            "error_message": Json::Null,
                        }),
                        Err(error) => explain_failure(op, &error),
                    },
                }
            }
            Err(error) => not_dispatched(op, planned, &error),
        };
        write_event(writer, &event)?;
    }
    Ok(())
}

fn explain_failure(op: &Op, error: &LimboError) -> Json {
    json!({
        "kind": "explain",
        "op_id": op.op_id,
        "outcome": "known_failure",
        "rows": Json::Null,
        "error_class": class(error),
        "error_message": message(error),
    })
}

fn not_dispatched(op: &Op, planned: i64, error: &LimboError) -> Json {
    json!({
        "kind": "op",
        "op_id": op.op_id,
        "outcome": "not_dispatched",
        "planned_ns": planned,
        "dispatched_ns": Json::Null,
        "completed_ns": Json::Null,
        "rows": Json::Null,
        "error_class": class(error),
        "error_message": message(error),
    })
}

fn count_rows(connection: &Arc<Connection>) -> Fallible<i64> {
    let mut statement = connection
        .prepare("SELECT count(*) FROM e01_records")
        .map_err(|error| format!("count prepare failed: {}", describe(&error)))?;
    let rows = statement
        .run_collect_rows()
        .map_err(|error| format!("count failed: {}", describe(&error)))?;
    match rows.first().and_then(|row| row.first()) {
        Some(Value::Numeric(Numeric::Integer(count))) => Ok(*count),
        _ => Err("count(*) did not return an integer".to_string()),
    }
}

fn row_json(row: &[Value]) -> Json {
    Json::Array(row.iter().map(value_json).collect())
}

fn value_json(value: &Value) -> Json {
    match value {
        Value::Null => Json::Null,
        Value::Numeric(Numeric::Integer(integer)) => json!(integer),
        Value::Numeric(Numeric::Float(float)) => {
            let number = f64::from(*float);
            if number.is_finite() {
                json!(number)
            } else {
                Json::String("nonfinite".to_string())
            }
        }
        Value::Text(text) => json!(text.as_str()),
        Value::Blob(bytes) => json!(hex(bytes)),
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

fn create_output(path: &str) -> Fallible<File> {
    // 独占创建并收紧权限：证据文件既不覆盖既有文件，也不被同机其他用户读取。
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .map_err(|error| format!("output create failed: {error}"))
}

fn write_event(writer: &mut impl Write, event: &Json) -> Fallible<()> {
    serde_json::to_writer(&mut *writer, event).map_err(|error| format!("output write failed: {error}"))?;
    writer
        .write_all(b"\n")
        .map_err(|error| format!("output write failed: {error}"))?;
    writer.flush().map_err(|error| format!("output flush failed: {error}"))
}

fn nanos(start: Instant) -> i64 {
    start.elapsed().as_nanos().min(i64::MAX as u128) as i64
}

fn class(error: &LimboError) -> String {
    format!("{error:?}")
        .chars()
        .take_while(|character| character.is_ascii_alphanumeric() || *character == '_')
        .take(64)
        .collect()
}

fn message(error: &LimboError) -> String {
    format!("{error}")
        .chars()
        .map(|character| if character.is_control() { ' ' } else { character })
        .take(MAX_MESSAGE_CHARS)
        .collect()
}

fn describe(error: &LimboError) -> String {
    format!("{}: {}", class(error), message(error))
}
