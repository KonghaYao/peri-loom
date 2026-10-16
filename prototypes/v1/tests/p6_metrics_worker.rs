#[allow(dead_code)]
#[path = "p4_runtime_helpers/clock_gate.rs"]
mod clock_gate;
#[allow(dead_code)]
#[path = "p4_runtime_helpers/remote.rs"]
mod remote;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{ensure, Result};
use peri_loom_v1::ownership::types::AccessRole;
use peri_loom_v1::worker_rpc::protocol::{WorkerCommand, WorkerRequest};
use peri_loom_v1::worker_rpc::server::{WorkerLimits, WorkerServer};
use serde_json::Value;

const CONTROL: &str = "p6_control_credential____________________";
const SQL: &str = "p6_sql_credential________________________";

struct Worker {
    fixture: remote::Fixture,
    address: SocketAddr,
    server: Option<JoinHandle<Result<()>>>,
}

impl Worker {
    fn new(role: AccessRole, limits: WorkerLimits) -> Result<Self> {
        let fixture = remote::Fixture::new(role, 60_000)?;
        fixture.acquire(role)?;
        let server = WorkerServer::bind_with_limits(
            "127.0.0.1:0".parse()?,
            fixture.runtime.clone(),
            CONTROL.into(),
            SQL.into(),
            limits,
        )?;
        let address = server.local_addr()?;
        let server = Some(std::thread::spawn(move || server.run()));
        Ok(Self {
            fixture,
            address,
            server,
        })
    }

    fn call(&self, command: WorkerCommand, control: bool) -> Result<(u16, Value)> {
        call(self.address, command, control)
    }

    fn session(&self) -> Result<String> {
        let (status, body) = self.call(
            WorkerCommand::OpenSession {
                database: self.fixture.identifier.clone(),
            },
            false,
        )?;
        ensure!(status == 200 && body["ok"] == true, "open failed: {body}");
        Ok(body["result"]["session"].as_str().unwrap().to_owned())
    }
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

fn call(address: SocketAddr, command: WorkerCommand, control: bool) -> Result<(u16, Value)> {
    let response = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?
        .post(format!(
            "http://{address}/{}",
            if control { "control" } else { "sql" }
        ))
        .bearer_auth(if control { CONTROL } else { SQL })
        .json(&WorkerRequest {
            version: 1,
            request_id: "same_request".into(),
            command,
        })
        .send()?;
    Ok((response.status().as_u16(), response.json()?))
}

#[test]
fn authenticated_metrics_real_sql_checkpoint_and_no_native_dedup() -> Result<()> {
    let worker = Worker::new(AccessRole::Writer, WorkerLimits::default())?;
    let session = worker.session()?;
    assert_eq!(
        worker.call(WorkerCommand::GetMetrics, false)?.1["ok"],
        false
    );
    worker.call(
        WorkerCommand::Execute {
            session: session.clone(),
            sql: "CREATE TABLE items(value INTEGER)".into(),
        },
        false,
    )?;
    for _ in 0..2 {
        let response = worker
            .call(
                WorkerCommand::Execute {
                    session: session.clone(),
                    sql: "INSERT INTO items VALUES(1)".into(),
                },
                false,
            )?
            .1;
        assert_eq!(response["ok"], true);
    }
    let response = worker
        .call(
            WorkerCommand::Query {
                session: session.clone(),
                sql: "SELECT count(*) FROM items".into(),
            },
            false,
        )?
        .1;
    assert_eq!(response["result"][0][0], 2);
    assert_eq!(
        worker
            .call(
                WorkerCommand::Checkpoint {
                    session: session.clone()
                },
                false
            )?
            .1["ok"],
        true
    );
    let report = worker.call(WorkerCommand::GetMetrics, true)?.1;
    assert_eq!(report["ok"], true);
    let metrics = &report["result"]["metrics"];
    assert!(metrics["query"]["total"].as_u64().unwrap() > 0);
    assert!(metrics["execute"]["total"].as_u64().unwrap() >= 3);
    assert!(metrics["checkpoint"]["total"].as_u64().unwrap() >= 1);
    assert!(metrics["checkpoint"]["latency_total_ns"].as_u64().unwrap() > 0);
    assert_eq!(report["result"]["sessions"], 1);
    assert_eq!(report["result"]["resources"]["active_sessions"], 1);
    assert!(report["result"]["metrics"].get("cache_hits").is_none());
    assert_eq!(
        worker
            .call(WorkerCommand::CloseSession { session }, false)?
            .1["ok"],
        true
    );
    assert_eq!(
        worker.call(WorkerCommand::GetMetrics, true)?.1["result"]["sessions"],
        0
    );
    Ok(())
}

#[test]
fn sql_lane_rejects_before_engine_and_control_metrics_stays_available() -> Result<()> {
    let worker = Worker::new(
        AccessRole::Writer,
        WorkerLimits {
            max_inflight: 4,
            control_reserve: 3,
            ..WorkerLimits::default()
        },
    )?;
    let session = worker.session()?;
    assert_eq!(
        worker
            .call(
                WorkerCommand::Execute {
                    session: session.clone(),
                    sql: "CREATE TABLE items(value INTEGER)".into()
                },
                false
            )?
            .1["ok"],
        true
    );
    worker.fixture.gate.arm(1);
    let address = worker.address;
    let operation = std::thread::spawn(move || {
        call(
            address,
            WorkerCommand::Execute {
                session,
                sql: "INSERT INTO items VALUES(1)".into(),
            },
            false,
        )
    });
    worker.fixture.gate.wait_entered();
    let metrics = worker.call(WorkerCommand::GetMetrics, true)?.1;
    assert_eq!(metrics["ok"], true);
    assert_eq!(metrics["result"]["sql_inflight"], 1);
    // One ownership sample backs both fields, so the count can never disagree with
    // the list it summarises.
    assert_eq!(
        metrics["result"]["ownership_failures_count"],
        metrics["result"]["ownership_failures"]
            .as_array()
            .map(|failures| failures.len())
            .unwrap_or_default()
    );
    let (status, _) = worker.call(
        WorkerCommand::Query {
            session: "unknown".into(),
            sql: "SELECT 1".into(),
        },
        false,
    )?;
    assert_eq!(status, 429);
    worker.fixture.gate.release();
    assert_eq!(operation.join().unwrap()?.1["ok"], true);
    Ok(())
}

#[test]
fn body_sql_session_limits_and_later_reuse() -> Result<()> {
    let worker = Worker::new(
        AccessRole::Writer,
        WorkerLimits {
            max_sessions: 1,
            ..WorkerLimits::default()
        },
    )?;
    let session = worker.session()?;
    assert_eq!(
        worker
            .call(
                WorkerCommand::OpenSession {
                    database: worker.fixture.identifier.clone()
                },
                false
            )?
            .1["ok"],
        false
    );
    let oversized = format!("SELECT 1{}", " ".repeat(64 * 1024));
    assert_eq!(
        worker
            .call(
                WorkerCommand::Query {
                    session: session.clone(),
                    sql: oversized
                },
                false
            )?
            .1["ok"],
        false
    );
    let mut stream = TcpStream::connect(worker.address)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    write!(stream, "POST /sql HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {SQL}\r\nContent-Length: 536870912\r\n\r\n")?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    assert!(response.starts_with("HTTP/1.1 400"));
    assert_eq!(
        worker
            .call(
                WorkerCommand::Query {
                    session,
                    sql: "SELECT 1".into()
                },
                false
            )?
            .1["result"][0][0],
        1
    );
    Ok(())
}

#[test]
fn worker_role_and_local_expiry_regressions_remain_enforced() -> Result<()> {
    let worker = Worker::new(AccessRole::ReadOnly, WorkerLimits::default())?;
    let session = worker.session()?;
    assert_eq!(
        worker
            .call(
                WorkerCommand::Execute {
                    session: session.clone(),
                    sql: "INSERT INTO items VALUES(8)".into()
                },
                false
            )?
            .1["ok"],
        false
    );
    assert_eq!(
        worker
            .call(
                WorkerCommand::Query {
                    session: session.clone(),
                    sql: "SELECT id FROM items".into()
                },
                false
            )?
            .1["result"][0][0],
        7
    );
    worker
        .fixture
        .worker_clock
        .0
        .store(60_000, Ordering::SeqCst);
    assert_eq!(
        worker
            .call(
                WorkerCommand::Query {
                    session,
                    sql: "SELECT id FROM items".into()
                },
                false
            )?
            .1["ok"],
        false
    );
    Ok(())
}
