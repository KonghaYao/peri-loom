#[allow(dead_code)]
#[path = "p4_runtime_helpers/clock_gate.rs"]
mod clock_gate;
#[allow(dead_code)]
#[path = "p4_runtime_helpers/remote.rs"]
mod remote;

use anyhow::{ensure, Result};
use peri_loom_v1::ownership::types::AccessRole;
use peri_loom_v1::worker_rpc::server::WorkerServer;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

#[test]
fn worker_deadline_reclaims_lost_open_cursor_and_mapping_without_commit() -> Result<()> {
    if std::env::var_os("PERI_LIFECYCLE_CHILD").is_none() {
        let output = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "worker_deadline_reclaims_lost_open_cursor_and_mapping_without_commit",
                "--nocapture",
            ])
            .env("PERI_LIFECYCLE_CHILD", "1")
            .env("PERI_WORKER_SESSION_IDLE_MS", "500")
            .env("PERI_WORKER_SESSION_MAX_AGE_MS", "10000")
            .env("PERI_WORKER_DB_IDLE_UNLOAD_MS", "20")
            .output()?;
        ensure!(
            output.status.success(),
            "child failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        return Ok(());
    }
    let fixture = remote::Fixture::new(AccessRole::Writer, 60_000)?;
    fixture.acquire(AccessRole::Writer)?;
    let token = || uuid_token();
    let control = token();
    let sql = token();
    let server = WorkerServer::bind(
        "127.0.0.1:0".parse()?,
        fixture.runtime.clone(),
        control.clone(),
        sql.clone(),
    )?;
    let address = server.local_addr()?;
    let server = std::thread::spawn(move || server.run());
    let client = reqwest::blocking::Client::builder()
        .no_proxy()
        .retry(reqwest::retry::never())
        .timeout(Duration::from_secs(5))
        .build()?;
    let call = |command: Value, id: &str, admin: bool| -> Result<Value> {
        let response: Value = client
            .post(format!(
                "http://{address}/{}",
                if admin { "control" } else { "sql" }
            ))
            .bearer_auth(if admin { &control } else { &sql })
            .json(&json!({"version":1,"request_id":id,"command":command}))
            .send()?
            .json()?;
        ensure!(
            response["ok"] == true,
            "worker operation failed: {response}"
        );
        Ok(response["result"].clone())
    };
    let open = |id: &str| {
        call(
            json!({"command":"open_owned_session","database":fixture.identifier,"owner_incarnation":"gateway-generation-1"}),
            id,
            false,
        )
    };
    for scenario in ["lost-open", "lost-map", "cursor"] {
        let session = open(scenario)?["session"].as_str().unwrap().to_owned();
        if scenario == "lost-map" {
            call(
                json!({"command":"execute","session":session,"sql":"CREATE TABLE items(value); INSERT INTO items VALUES(7); BEGIN; INSERT INTO items VALUES(99)"}),
                "write",
                false,
            )?;
        }
        if scenario == "cursor" {
            call(
                json!({"command":"open_write_cursor","session":session,"sql":"INSERT INTO items VALUES(101) RETURNING value"}),
                "cursor-open",
                false,
            )?;
        }
        let deadline = Instant::now() + Duration::from_secs(4);
        loop {
            let metrics = call(json!({"command":"get_metrics"}), "metrics", true)?;
            if metrics["sessions"] == 0 && metrics["resources"]["resident_count"] == 0 {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "reclamation did not finish: {metrics}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let proof = call(
            json!({"command":"close_owned_sessions","binding":fixture.lease.binding,"owner_incarnation":"gateway-generation-1","open_request_id":scenario}),
            "reconcile",
            true,
        )?;
        assert_eq!(proof["completed"], true);
        assert_eq!(proof["matched"], 1);
    }
    let session = open("verify")?["session"].as_str().unwrap().to_owned();
    let rows = call(
        json!({"command":"query","session":session,"sql":"SELECT value FROM items ORDER BY value"}),
        "read",
        false,
    )?;
    assert_eq!(rows, json!([[7]]));
    let close = json!({"command":"close_session","session":session});
    call(close.clone(), "close-1", false)?;
    call(close, "close-2", false)?;
    assert_eq!(
        call(json!({"command":"get_metrics"}), "final", true)?["sessions"],
        0
    );
    call(json!({"command":"shutdown"}), "shutdown", true)?;
    server.join().unwrap()?;
    Ok(())
}

/// A database-bearing worker with the maintenance driver armed must still prove a safe
/// retirement boundary when no write is in flight. The unit-level tests cover the
/// in-flight lock attribution; this exercises the full `sessions.clear` → quiesce →
/// bounded sample → clean exit chain.
#[test]
fn database_worker_with_maintenance_enabled_shuts_down_clean() -> Result<()> {
    if std::env::var_os("PERI_MAINTENANCE_SHUTDOWN_CHILD").is_none() {
        let output = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "database_worker_with_maintenance_enabled_shuts_down_clean",
                "--nocapture",
            ])
            .env("PERI_MAINTENANCE_SHUTDOWN_CHILD", "1")
            .env("PERI_MAINTENANCE_SCHEDULE", "1")
            .env("PERI_WORKER_DB_IDLE_UNLOAD_MS", "600000")
            .output()?;
        ensure!(
            output.status.success(),
            "child failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        return Ok(());
    }
    let fixture = remote::Fixture::new(AccessRole::Writer, 60_000)?;
    fixture.acquire(AccessRole::Writer)?;
    let control = uuid_token();
    let sql = uuid_token();
    let server = WorkerServer::bind(
        "127.0.0.1:0".parse()?,
        fixture.runtime.clone(),
        control.clone(),
        sql.clone(),
    )?;
    let address = server.local_addr()?;
    let server = std::thread::spawn(move || server.run());
    let client = reqwest::blocking::Client::builder()
        .no_proxy()
        .retry(reqwest::retry::never())
        .timeout(Duration::from_secs(5))
        .build()?;
    let call = |command: Value, id: &str, admin: bool| -> Result<Value> {
        let response: Value = client
            .post(format!(
                "http://{address}/{}",
                if admin { "control" } else { "sql" }
            ))
            .bearer_auth(if admin { &control } else { &sql })
            .json(&json!({"version":1,"request_id":id,"command":command}))
            .send()?
            .json()?;
        ensure!(
            response["ok"] == true,
            "worker operation failed: {response}"
        );
        Ok(response["result"].clone())
    };
    let session = call(
        json!({"command":"open_session","database":fixture.identifier}),
        "open",
        false,
    )?["session"]
        .as_str()
        .unwrap()
        .to_owned();
    call(
        json!({"command":"execute","session":session,"sql":"CREATE TABLE items(value); INSERT INTO items VALUES(7)"}),
        "write",
        false,
    )?;
    call(
        json!({"command":"close_session","session":session}),
        "close",
        false,
    )?;
    // Let the watchdog start the maintenance driver, so shutdown quiesces a live scheduler
    // rather than a runtime on which maintenance never started.
    std::thread::sleep(Duration::from_millis(150));
    let shutdown = call(json!({"command":"shutdown"}), "shutdown", true)?;
    assert_eq!(shutdown["stopping"], true);
    let joined = server.join().expect("worker server thread panicked");
    ensure!(joined.is_ok(), "worker shutdown was unclean: {joined:?}");
    Ok(())
}

fn uuid_token() -> String {
    use std::io::Read;
    let mut bytes = [0u8; 32];
    std::fs::File::open("/dev/urandom")
        .unwrap()
        .read_exact(&mut bytes)
        .unwrap();
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
