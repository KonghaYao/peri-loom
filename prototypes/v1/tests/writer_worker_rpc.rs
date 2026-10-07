#[allow(dead_code)]
#[path = "p4_runtime_helpers/clock_gate.rs"]
mod clock_gate;
#[allow(dead_code)]
#[path = "p4_runtime_helpers/remote.rs"]
mod remote;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{ensure, Result};
use peri_loom_v1::ownership::types::AccessRole;
use peri_loom_v1::worker_rpc::protocol::{WorkerCommand, WorkerRequest};
use peri_loom_v1::worker_rpc::server::{WorkerLimits, WorkerServer};
use serde_json::{json, Value};

const CONTROL: &str = "writer_control_credential__________________";
const SQL: &str = "writer_sql_credential______________________";
static REQUESTS: AtomicU64 = AtomicU64::new(0);

struct Worker {
    fixture: remote::Fixture,
    address: SocketAddr,
    server: Option<JoinHandle<Result<()>>>,
}

impl Worker {
    fn new(enabled: bool) -> Result<Self> {
        let fixture = remote::Fixture::new(AccessRole::Writer, 60_000)?;
        fixture.acquire(AccessRole::Writer)?;
        let server = WorkerServer::bind_with_limits_and_writer_profile(
            "127.0.0.1:0".parse()?,
            fixture.runtime.clone(),
            CONTROL.into(),
            SQL.into(),
            WorkerLimits {
                max_inflight: 6,
                control_reserve: 1,
                ..WorkerLimits::default()
            },
            enabled,
        )?;
        let address = server.local_addr()?;
        Ok(Self {
            fixture,
            address,
            server: Some(std::thread::spawn(move || server.run())),
        })
    }

    fn call(&self, command: WorkerCommand, version: u32, control: bool) -> Result<(u16, Value)> {
        call(self.address, command, version, control)
    }

    fn open(&self, version: u32) -> Result<(String, Value)> {
        let (status, response) = self.call(
            WorkerCommand::OpenSession {
                database: self.fixture.identifier.clone(),
            },
            version,
            false,
        )?;
        ensure!(
            status == 200 && response["ok"] == true,
            "worker open failed"
        );
        let session = response["result"]["session"].as_str().unwrap().to_owned();
        assert_eq!(session.len(), 64);
        assert!(session
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
        Ok((session, response))
    }

    fn execute(&self, session: &str, sql: &str) -> Result<Value> {
        let (status, response) = self.call(
            WorkerCommand::Execute {
                session: session.into(),
                sql: sql.into(),
            },
            2,
            false,
        )?;
        assert_eq!(status, 200);
        Ok(response)
    }

    fn await_general(&self, count: u64) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let (_, response) = self.call(WorkerCommand::GetMetrics, 1, true)?;
            ensure!(response["ok"] == true, "worker metrics failed");
            if response["result"]["rpc_lanes"]["general_sql"] == count {
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "general lane count did not settle"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.fixture.gate.release();
        let _ = self.call(WorkerCommand::Shutdown, 1, true);
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
    }
}

fn call(
    address: SocketAddr,
    command: WorkerCommand,
    version: u32,
    control: bool,
) -> Result<(u16, Value)> {
    let request_id = format!("writer_{}", REQUESTS.fetch_add(1, Ordering::Relaxed));
    let response = post(
        address,
        control,
        if control { CONTROL } else { SQL },
        &serde_json::to_value(WorkerRequest {
            version,
            request_id: request_id.clone(),
            command,
        })?,
    )?;
    if response.0 == 200 {
        assert_eq!(response.1["version"], version);
        assert_eq!(response.1["request_id"], request_id);
    }
    Ok(response)
}

fn post(address: SocketAddr, control: bool, token: &str, payload: &Value) -> Result<(u16, Value)> {
    let response = reqwest::blocking::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .timeout(Duration::from_secs(10))
        .build()?
        .post(format!(
            "http://{address}/{}",
            if control { "control" } else { "sql" }
        ))
        .bearer_auth(token)
        .json(payload)
        .send()?;
    Ok((response.status().as_u16(), response.json()?))
}

fn assert_state(response: &Value, session: &str, owner: bool, previous: u64) -> u64 {
    let state = &response["session_state"];
    assert_eq!(state["session"], session);
    assert_eq!(state["owner"], owner);
    assert!(state["binding"].is_object());
    let sequence = state["sequence"].as_u64().unwrap();
    assert!(sequence > previous);
    sequence
}

#[test]
fn capability_is_opt_in_bounded_and_private() -> Result<()> {
    let worker = Worker::new(true)?;
    let (status, response) = worker.call(WorkerCommand::WriterProfileStatus, 2, false)?;
    assert_eq!(status, 200);
    assert_eq!(response.as_object().unwrap().len(), 5);
    assert_eq!(
        response["result"],
        json!({ "protocol": 2, "writer_profile": true, "max_inflight": 6,
        "control_reserve": 1, "sql_total": 5, "general_sql": 3, "owner_sql": 1, "cleanup_sql": 1 })
    );
    assert!(response.get("session_state").is_none());
    let request = json!({ "version": 2, "request_id": "forged", "command": { "command": "writer_profile_status" },
        "session_state": { "owner": true, "sequence": 999 } });
    assert_eq!(post(worker.address, false, SQL, &request)?.0, 400);
    assert_eq!(
        post(
            worker.address,
            false,
            CONTROL,
            &json!({ "version": 2, "request_id": "wrong_token",
        "command": { "command": "writer_profile_status" } })
        )?
        .0,
        403
    );
    assert!(WorkerServer::bind_with_limits_and_writer_profile(
        "127.0.0.1:0".parse()?,
        worker.fixture.runtime.clone(),
        CONTROL.into(),
        SQL.into(),
        WorkerLimits {
            max_inflight: 3,
            control_reserve: 1,
            ..WorkerLimits::default()
        },
        true
    )
    .is_err());
    Ok(())
}

#[test]
fn disabled_profile_rejects_v2_without_breaking_legacy_shape() -> Result<()> {
    let worker = Worker::new(false)?;
    assert_eq!(
        worker.call(WorkerCommand::WriterProfileStatus, 2, false)?.0,
        400
    );
    assert_eq!(
        worker
            .call(
                WorkerCommand::OpenSession {
                    database: worker.fixture.identifier.clone()
                },
                2,
                false
            )?
            .0,
        400
    );
    let (session, opened) = worker.open(1)?;
    assert!(opened.get("session_state").is_none());
    let (_, response) = worker.call(
        WorkerCommand::Query {
            session: session.clone(),
            sql: "SELECT 7".into(),
        },
        1,
        false,
    )?;
    assert_eq!(response["result"], json!([[7]]));
    assert!(response.get("session_state").is_none());
    assert_eq!(
        worker
            .call(WorkerCommand::CloseSession { session }, 1, false)?
            .1["ok"],
        true
    );
    Ok(())
}

#[test]
fn native_owner_constraint_commit_and_close_have_fresh_bound_state() -> Result<()> {
    let worker = Worker::new(true)?;
    let (session, opened) = worker.open(2)?;
    assert_eq!(opened["session_state"]["session"], session);
    assert_eq!(opened["session_state"]["owner"], false);
    assert_eq!(
        opened["session_state"]["binding"],
        serde_json::to_value(&worker.fixture.lease.binding)?
    );
    let setup = worker.execute(
        &session,
        "CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(7)",
    )?;
    assert_eq!(setup["ok"], true);
    let initial = opened["session_state"]["sequence"].as_u64().unwrap();
    let begun = worker.execute(&session, "BEGIN IMMEDIATE")?;
    assert_eq!(begun["ok"], true);
    let sequence = assert_state(&begun, &session, true, initial);
    let failed = worker.execute(&session, "INSERT INTO items VALUES(7)")?;
    assert_eq!(failed["ok"], false);
    let sequence = assert_state(&failed, &session, true, sequence);
    let committed = worker.execute(&session, "COMMIT")?;
    assert_eq!(committed["ok"], true);
    let sequence = assert_state(&committed, &session, false, sequence);
    let (_, closed) = worker.call(
        WorkerCommand::CloseSession {
            session: session.clone(),
        },
        2,
        false,
    )?;
    assert_eq!(closed["ok"], true);
    assert_state(&closed, &session, false, sequence);
    let (_, metrics) = worker.call(WorkerCommand::GetMetrics, 1, true)?;
    assert_eq!(metrics["result"]["sessions"], 0);
    Ok(())
}

#[test]
fn general_saturation_preserves_owner_commit_and_waiter_cleanup() -> Result<()> {
    let worker = Worker::new(true)?;
    let (owner, _) = worker.open(2)?;
    let waiters = (0..3)
        .map(|_| worker.open(2).map(|opened| opened.0))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(worker.execute(&owner, "BEGIN IMMEDIATE")?["ok"], true);
    let (finished, received) = std::sync::mpsc::channel();
    let calls = waiters
        .iter()
        .map(|session| {
            let address = worker.address;
            let session = session.clone();
            let finished = finished.clone();
            std::thread::spawn(move || {
                let response = call(
                    address,
                    WorkerCommand::Execute {
                        session: session.clone(),
                        sql: "BEGIN IMMEDIATE".into(),
                    },
                    2,
                    false,
                );
                finished.send((session, response)).unwrap();
            })
        })
        .collect::<Vec<_>>();
    worker.await_general(3)?;
    let (status, _) = worker.call(
        WorkerCommand::Query {
            session: "f".repeat(64),
            sql: "SELECT 1".into(),
        },
        2,
        false,
    )?;
    assert_eq!(status, 429);
    let (_, closed) = worker.call(
        WorkerCommand::CloseSession {
            session: waiters[0].clone(),
        },
        2,
        false,
    )?;
    assert_eq!(closed["ok"], true);
    assert_eq!(closed["session_state"]["owner"], false);
    assert_eq!(worker.execute(&owner, "COMMIT")?["ok"], true);
    for _ in 0..waiters.len() {
        let (session, response) = received.recv_timeout(Duration::from_secs(10))?;
        let (status, response) = response?;
        assert_eq!(status, 200);
        if session == waiters[0] {
            assert_eq!(response["ok"], false);
        } else {
            assert_eq!(response["ok"], true);
            assert_eq!(worker.execute(&session, "ROLLBACK")?["ok"], true);
            assert_eq!(
                worker
                    .call(WorkerCommand::CloseSession { session }, 2, false)?
                    .1["ok"],
                true
            );
        }
    }
    for operation in calls {
        operation.join().unwrap();
    }
    assert_eq!(
        worker
            .call(WorkerCommand::CloseSession { session: owner }, 2, false)?
            .1["ok"],
        true
    );
    Ok(())
}
