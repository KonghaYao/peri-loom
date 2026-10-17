//! Frozen, real local-store SQL workload; run only in an exclusive release timing slot.
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{ensure, Context, Result};
use peri_loom_v1::authority::ownership::OwnershipAuthority;
use peri_loom_v1::database::RemoteDatabase;
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::LocalObjectStore;
use peri_loom_v1::ownership::types::*;
use peri_loom_v1::runtime::DatabaseId;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Json};
use turso_core::Value;

const ROWS: usize = 512;
const PAYLOAD_BYTES: usize = 512;
const READS: usize = 256;
const SHORT_COMMITS: usize = 32;
const HISTORY_COMMITS: usize = 512;
const CHECKPOINTS: usize = 8;

struct Clock;
impl LeaseClock for Clock {
    fn now_ms(&self) -> Result<u64> {
        Ok(1_000)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Row {
    id: usize,
    version: usize,
    payload: String,
}

impl Row {
    fn new(id: usize, version: usize) -> Self {
        let prefix = format!("row-{id:04}-v-{version:06}-");
        let fill = char::from(b'a' + ((id + version) % 26) as u8);
        Self {
            id,
            version,
            payload: format!(
                "{prefix}{}",
                fill.to_string().repeat(PAYLOAD_BYTES - prefix.len())
            ),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Oracle {
    binding: LeaseBinding,
    rows: Vec<Row>,
}

fn open(
    root: &Path,
    cache: &Path,
    previous: Option<&LeaseBinding>,
    role: AccessRole,
) -> Result<(RemoteDatabase, LeaseBinding)> {
    let store = Arc::new(LocalObjectStore::open(root)?);
    let control = OwnershipAuthority::new(store, Arc::new(Clock));
    let database = DatabaseId::new(Namespace::new("localperf", "sql")?, "main")?;
    // A new Authority boots a new term and revokes the previous process's lease.
    // Reacquire through the real grant protocol instead of reusing its stale binding.
    let state = control.state(&database)?;
    if let Some(previous) = previous {
        ensure!(
            state.epoch == previous.epoch,
            "persisted epoch floor changed"
        );
        ensure!(
            control.validate_lease(previous, previous.role).is_err(),
            "restart accepted stale lease"
        );
    } else {
        ensure!(state.epoch == 0, "initial store was not empty");
    }
    let epoch = state.epoch.checked_add(1).context("epoch exhausted")?;
    let intent = GrantIntent {
        database,
        worker_id: format!("local-worker-{epoch}"),
        epoch,
        expected_epoch: state.epoch,
        lease_id: format!("local-lease-{epoch}"),
        ttl_ms: 10_000,
        role,
    };
    let nonce = control.challenge(LeaseIntent::Grant(intent.clone()))?.nonce;
    let binding = control
        .grant(GrantRequest {
            request_id: format!("grant-{epoch}"),
            nonce,
            intent,
        })?
        .binding;
    let scoped = control.open_session(&binding, role)?;
    let db = RemoteDatabase::open(scoped, binding.database.namespace.clone(), cache, epoch)?;
    Ok((db, binding))
}

fn verify<'a>(
    actual: &[Vec<Value>],
    expected: impl IntoIterator<Item = &'a Row>,
) -> Result<String> {
    let expected = expected.into_iter().collect::<Vec<_>>();
    ensure!(
        actual.len() == expected.len(),
        "row count {} != {}",
        actual.len(),
        expected.len()
    );
    let mut digest = blake3::Hasher::new();
    for (actual, expected) in actual.iter().zip(expected) {
        ensure!(actual.len() == 3, "column count mismatch");
        ensure!(
            actual[0] == Value::from_i64(expected.id as i64),
            "id mismatch"
        );
        ensure!(
            actual[1] == Value::from_i64(expected.version as i64),
            "version mismatch at {}",
            expected.id
        );
        let Value::Text(payload) = &actual[2] else {
            anyhow::bail!("payload is not TEXT")
        };
        ensure!(
            payload.as_str() == expected.payload,
            "payload mismatch at {}",
            expected.id
        );
        for value in actual {
            let text = value.to_string();
            digest.update(&(text.len() as u64).to_le_bytes());
            digest.update(text.as_bytes());
        }
    }
    Ok(digest.finalize().to_hex().to_string())
}

fn verify_all(db: &RemoteDatabase, rows: &[Row]) -> Result<String> {
    verify(
        &db.query("SELECT id,version,payload FROM items ORDER BY id")?,
        rows,
    )
}

fn updates(rows: &mut [Row], start: usize, count: usize, batch: usize) -> Vec<String> {
    (0..count)
        .map(|operation| {
            let mut sql = if batch > 1 {
                "BEGIN;".to_owned()
            } else {
                String::new()
            };
            for index in 0..batch {
                let sequence = start + operation * batch + index;
                let id = (sequence * 73) % ROWS;
                rows[id] = Row::new(id + 1, sequence);
                let row = &rows[id];
                sql.push_str(&format!(
                    "UPDATE items SET version={},payload='{}' WHERE id={};",
                    row.version, row.payload, row.id
                ));
            }
            if batch > 1 {
                sql.push_str("COMMIT;");
            }
            sql
        })
        .collect()
}

fn writes(
    db: &RemoteDatabase,
    name: &str,
    statements: &[String],
    phases: &mut Vec<Json>,
) -> Result<()> {
    let started = Instant::now();
    for sql in statements {
        db.execute(sql)?;
    }
    phases.push(json!({"name":name,"requests":statements.len(),"wall_ns":started.elapsed().as_nanos() as u64}));
    Ok(())
}

fn cold_child() -> Result<()> {
    let root = std::env::var("PERI_LOCAL_PERF_COLD_ROOT")?;
    let mut oracle: Oracle =
        serde_json::from_slice(&std::fs::read(Path::new(&root).join("oracle.json"))?)?;
    let started = Instant::now();
    let (db, binding) = open(
        &Path::new(&root).join("objects"),
        &Path::new(&root).join("cold-cache"),
        Some(&oracle.binding),
        AccessRole::ReadOnly,
    )?;
    let rows = db.query("SELECT id,version,payload FROM items ORDER BY id")?;
    let wall_ns = started.elapsed().as_nanos() as u64;
    let digest = verify(&rows, &oracle.rows)?;
    oracle.binding = binding;
    std::fs::write(
        Path::new(&root).join("oracle.json"),
        serde_json::to_vec(&oracle)?,
    )?;
    eprintln!(
        "LOCAL_PERF_COLD {}",
        json!({"wall_ns":wall_ns,"rows":rows.len(),"oracle_digest":digest,"epoch":oracle.binding.epoch,"prior_binding_rejected":true})
    );
    Ok(())
}

fn cold(root: &Path, oracle: &mut Oracle, name: &str, phases: &mut Vec<Json>) -> Result<()> {
    std::fs::write(root.join("oracle.json"), serde_json::to_vec(oracle)?)?;
    let cache = root.join("cold-cache");
    if cache.exists() {
        std::fs::remove_dir_all(cache)?;
    }
    let output = Command::new(std::env::current_exe()?)
        .args([
            "--ignored",
            "--exact",
            "local_read_write_workload",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("PERI_LOCAL_PERF_COLD_ROOT", root)
        .output()?;
    ensure!(
        output.status.success(),
        "cold reopen failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8(output.stderr)?;
    let line = stderr
        .lines()
        .find_map(|line| line.strip_prefix("LOCAL_PERF_COLD "))
        .context("missing cold oracle")?;
    let mut value: Json = serde_json::from_str(line)?;
    let refreshed: Oracle = serde_json::from_slice(&std::fs::read(root.join("oracle.json"))?)?;
    ensure!(
        refreshed.binding.epoch == oracle.binding.epoch + 1,
        "cold grant did not advance epoch"
    );
    ensure!(
        refreshed.binding.root_incarnation == oracle.binding.root_incarnation,
        "cold reopen changed root identity"
    );
    oracle.binding = refreshed.binding;
    value["name"] = json!(name);
    value["requests"] = json!(1);
    phases.push(value);
    Ok(())
}

#[test]
#[ignore = "real durable SQL benchmark: exclusive release timing slot only"]
fn local_read_write_workload() -> Result<()> {
    ensure!(
        !cfg!(debug_assertions),
        "use --release; debug timings are not acceptance evidence"
    );
    if std::env::var_os("PERI_LOCAL_PERF_COLD_ROOT").is_some() {
        return cold_child();
    }
    let run =
        std::env::var("PERI_LOCAL_PERF_RUN_ID").context("set unique PERI_LOCAL_PERF_RUN_ID")?;
    let source =
        std::env::var("PERI_LOCAL_PERF_SOURCE_ID").context("set frozen source identity")?;
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let fixture_started = Instant::now();
    let (mut db, binding) = open(
        &root.join("objects"),
        &root.join("warm-cache"),
        None,
        AccessRole::Writer,
    )?;
    let mut oracle = Oracle {
        binding,
        rows: (1..=ROWS).map(|id| Row::new(id, 0)).collect(),
    };
    let mut seed = "CREATE TABLE items(id INTEGER PRIMARY KEY,version INTEGER NOT NULL,payload TEXT NOT NULL);BEGIN;".to_owned();
    for row in &oracle.rows {
        seed.push_str(&format!(
            "INSERT INTO items VALUES({},{},'{}');",
            row.id, row.version, row.payload
        ));
    }
    seed.push_str("COMMIT;");
    db.execute(&seed)?;
    verify_all(&db, &oracle.rows)?;
    let fixture_ns = fixture_started.elapsed().as_nanos() as u64;
    let mut phases = Vec::new();
    let queries = (0..READS)
        .map(|index| {
            let begin = (index * 37) % (ROWS - 16);
            let width = if index % 2 == 0 { 1 } else { 16 };
            (
                begin,
                width,
                format!(
                    "SELECT id,version,payload FROM items WHERE id>={0} AND id<{1} ORDER BY id",
                    begin + 1,
                    begin + width + 1
                ),
            )
        })
        .collect::<Vec<_>>();
    for (_, _, sql) in queries.iter().take(32) {
        db.query(sql)?;
    }
    let started = Instant::now();
    let answers = queries
        .iter()
        .map(|(_, _, sql)| db.query(sql))
        .collect::<Result<Vec<_>>>()?;
    let wall_ns = started.elapsed().as_nanos() as u64;
    for ((begin, width, _), answer) in queries.iter().zip(&answers) {
        verify(answer, &oracle.rows[*begin..begin + width])?;
    }
    phases.push(json!({"name":"hot_reads","requests":READS,"returned_rows":answers.iter().map(Vec::len).sum::<usize>(),"wall_ns":wall_ns}));
    let sql = updates(&mut oracle.rows, 1, SHORT_COMMITS, 1);
    writes(&db, "small_commits", &sql, &mut phases)?;
    verify_all(&db, &oracle.rows)?;
    let sql = updates(&mut oracle.rows, 1_000, HISTORY_COMMITS, 1);
    writes(&db, "history_commits", &sql, &mut phases)?;
    verify_all(&db, &oracle.rows)?;
    let sql = updates(&mut oracle.rows, 2_000, SHORT_COMMITS, 1);
    writes(&db, "long_history_commits", &sql, &mut phases)?;
    let wal_digest = verify_all(&db, &oracle.rows)?;
    drop(db);
    cold(root, &mut oracle, "cold_wal_reopen", &mut phases)?;
    let started = Instant::now();
    let (reopened, binding) = open(
        &root.join("objects"),
        &root.join("checkpoint-cache"),
        Some(&oracle.binding),
        AccessRole::Writer,
    )?;
    phases.push(json!({"name":"writer_reopen","requests":1,"epoch":binding.epoch,"wall_ns":started.elapsed().as_nanos() as u64}));
    db = reopened;
    oracle.binding = binding;
    let sql = updates(&mut oracle.rows, 3_000, CHECKPOINTS, 32);
    let mut update_ns = 0u64;
    let mut checkpoint_ns = 0u64;
    for sql in sql {
        let started = Instant::now();
        db.execute(&sql)?;
        update_ns += started.elapsed().as_nanos() as u64;
        let started = Instant::now();
        db.checkpoint()?;
        checkpoint_ns += started.elapsed().as_nanos() as u64;
    }
    phases.push(json!({"name":"checkpoint_updates","requests":CHECKPOINTS,"updated_rows":CHECKPOINTS*32,"wall_ns":update_ns}));
    phases.push(json!({"name":"checkpoints","requests":CHECKPOINTS,"wall_ns":checkpoint_ns}));
    let final_digest = verify_all(&db, &oracle.rows)?;
    drop(db);
    cold(root, &mut oracle, "cold_checkpoint_reopen", &mut phases)?;
    for (name, digest) in [
        ("cold_wal_reopen", wal_digest.as_str()),
        ("cold_checkpoint_reopen", final_digest.as_str()),
    ] {
        let phase = phases
            .iter()
            .find(|phase| phase["name"] == name)
            .context("cold phase missing")?;
        ensure!(
            phase["oracle_digest"] == digest,
            "cold digest differs: {name}"
        );
    }
    let total_ns = phases
        .iter()
        .map(|phase| phase["wall_ns"].as_u64().unwrap())
        .sum::<u64>();
    eprintln!(
        "LOCAL_PERF {}",
        json!({
            "schema":1,"workload":"local-sql-v1","run":run,"source":source,
            "rows":ROWS,"payload_bytes":PAYLOAD_BYTES,"initial_payload_bytes":ROWS*PAYLOAD_BYTES,
            "seed_transactions":2,"warmup_reads":32,"hot_reads":READS,"small_commits":SHORT_COMMITS,
            "history_commits":HISTORY_COMMITS,"long_history_commits":SHORT_COMMITS,
            "checkpoint_transactions":CHECKPOINTS,"checkpoint_updated_rows":CHECKPOINTS*32,
        "checkpoints":CHECKPOINTS,"cold_reopens":2,"authority_boots":4,"lease_grants":4,"final_epoch":oracle.binding.epoch,"fixture_ns":fixture_ns,
            "total_request_ns":total_ns,"wal_oracle_digest":wal_digest,"final_oracle_digest":final_digest,"phases":phases
        })
    );
    Ok(())
}
