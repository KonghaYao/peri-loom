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
use peri_loom_v1::ownership::types::{AccessRole, OwnershipCommand};
use peri_loom_v1::worker_rpc::protocol::{WorkerCommand, WorkerRequest};
use peri_loom_v1::worker_rpc::server::{WorkerLimits, WorkerServer};
use serde_json::{json, Value};

const CONTROL: &str = "writer_control_credential__________________";
const SQL: &str = "writer_sql_credential______________________";
static REQUESTS: AtomicU64 = AtomicU64::new(0);

#[test]
#[ignore = "requires PERI_CAPACITY_TRACE=1; run explicitly with --ignored --nocapture"]
fn opt_in_worker_capacity_page_maps_loaded_queue_before_execute_or_query() -> Result<()> {
    assert_eq!(std::env::var("PERI_CAPACITY_TRACE").as_deref(), Ok("1"));
    let worker = Worker::new(true)?;
    let (session, _) = worker.open(2)?;
    let (_, before) = worker.call(WorkerCommand::GetMetrics, 1, true)?;
    let trace = &before["result"]["capacity_trace"];
    let mapping = trace["runtimeMappings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|mapping| mapping["databaseId"] == json!(worker.fixture.identifier))
        .unwrap();
    let ring = trace["databases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|ring| ring["queueInstance"] == mapping["queueInstance"])
        .unwrap();
    assert_eq!(mapping["status"], "available");
    assert_eq!(ring["status"], "available");
    assert_eq!(ring["sequence"], 0);
    assert_eq!(ring["lost"], 0);
    assert_eq!(ring["epoch"], json!(worker.fixture.lease.binding.epoch));
    let stream_id = ring["streamId"].as_str().unwrap().to_owned();
    let command = WorkerCommand::GetCapacityTrace {
        stream_id: stream_id.clone(),
        after_sequence: 0,
        max_events: 128,
    };
    let (_, denied) = worker.call(command.clone(), 1, false)?;
    assert_eq!(denied["ok"], false);
    let (_, page) = worker.call(command, 1, true)?;
    assert_eq!(page["ok"], true);
    let result = &page["result"];
    assert_eq!(result["status"], "available");
    assert_eq!(result["stream"]["status"], "available");
    assert_eq!(result["stream"]["databaseId"], mapping["databaseId"]);
    assert_eq!(result["stream"]["epoch"], mapping["epoch"]);
    assert_eq!(result["stream"]["queueInstance"], mapping["queueInstance"]);
    assert_eq!(result["stream"]["streamId"], json!(stream_id));
    assert_eq!(result["stream"]["sequence"], 0);
    assert_eq!(result["stream"]["lost"], 0);
    assert_eq!(result["stream"]["dropped"], 0);
    assert_eq!(result["stream"]["prefixGap"], false);
    assert_eq!(result["stream"]["events"], json!([]));
    assert_eq!(result["afterSequence"], 0);
    assert_eq!(result["nextSequence"], 0);
    assert_eq!(result["hasMore"], false);
    for field in ["pid", "startUnixMicros", "generation"] {
        assert_eq!(result["source"][field], trace["source"][field]);
    }
    assert!(serde_json::to_vec(result)?.len() <= 64 * 1024);
    let (_, gap) = worker.call(
        WorkerCommand::GetCapacityTrace {
            stream_id: stream_id.clone(),
            after_sequence: 1,
            max_events: 128,
        },
        1,
        true,
    )?;
    assert_eq!(gap["result"]["status"], "sequence_gap");
    assert_eq!(gap["result"]["stream"]["prefixGap"], true);
    let (_, missing) = worker.call(
        WorkerCommand::GetCapacityTrace {
            stream_id: "NOT-LIVE-missing-stream".into(),
            after_sequence: 0,
            max_events: 128,
        },
        1,
        true,
    )?;
    assert_eq!(missing["result"]["status"], "stream_unavailable");
    let (_, after) = worker.call(WorkerCommand::GetMetrics, 1, true)?;
    for operation in ["execute", "query"] {
        assert!(before["result"]["metrics"][operation]["total"]
            .as_u64()
            .is_some());
        assert_eq!(
            after["result"]["metrics"][operation]["total"],
            before["result"]["metrics"][operation]["total"]
        );
    }
    assert_eq!(after["result"]["sessions"], 1);
    let (_, closed) = worker.call(WorkerCommand::CloseSession { session }, 2, false)?;
    assert_eq!(closed["ok"], true);
    worker
        .fixture
        .runtime
        .command(OwnershipCommand::RevokeDatabase {
            database: worker.fixture.identifier.clone(),
            expected_epoch: worker.fixture.lease.binding.epoch,
        })?;
    let (_, retired) = worker.call(
        WorkerCommand::GetCapacityTrace {
            stream_id: stream_id.clone(),
            after_sequence: 0,
            max_events: 128,
        },
        1,
        true,
    )?;
    assert_eq!(retired["ok"], false);
    let (_, retired_gap) = worker.call(
        WorkerCommand::GetCapacityTrace {
            stream_id,
            after_sequence: u64::MAX,
            max_events: 128,
        },
        1,
        true,
    )?;
    assert_eq!(retired_gap["ok"], true);
    assert_eq!(retired_gap["result"]["status"], "sequence_gap");
    assert_eq!(retired_gap["result"]["stream"]["prefixGap"], true);
    assert_eq!(retired_gap["result"]["stream"]["gap"], true);
    assert_eq!(retired_gap["result"]["stream"]["lost"], 0);
    assert_eq!(retired_gap["result"]["afterSequence"], json!(u64::MAX));
    assert_eq!(
        retired_gap["result"]["source"]["generation"],
        trace["source"]["generation"]
    );
    println!("CAPACITY_PRE_EXECUTE_PAGE live_queue_mapping=1 open_session=1 execute_delta=0 query_delta=0 cursor_gap_preserved=1 unavailable_preserved=1 retired_mapping_rejected=1 retired_gap_preserved=1");
    Ok(())
}

#[test]
#[ignore = "requires PERI_CAPACITY_TRACE=1; run explicitly with --ignored --nocapture"]
fn opt_in_native_capacity_trace_joins_reserved_cleanup_to_fresh_ack() -> Result<()> {
    assert_eq!(
        std::env::var("PERI_CAPACITY_TRACE").as_deref(),
        Ok("1"),
        "capacity trace opt-in test requires PERI_CAPACITY_TRACE=1"
    );
    assert!(peri_loom_v1::capacity_trace::enabled());
    let worker = Worker::new(true)?;
    let (session, _) = worker.open(2)?;
    assert_eq!(
        worker.execute(&session, "CREATE TABLE capacity_test(value INTEGER)")?["ok"],
        true
    );
    let begin = worker.execute(&session, "BEGIN IMMEDIATE")?;
    assert_eq!(begin["session_state"]["owner"], true);
    assert_eq!(
        worker.execute(&session, "INSERT INTO capacity_test VALUES (1)")?["ok"],
        true
    );
    let committed = worker.execute(&session, "COMMIT")?;
    assert_eq!(committed["ok"], true);
    assert_eq!(committed["session_state"]["owner"], false);
    assert!(
        committed["session_state"]["sequence"].as_u64().unwrap()
            > begin["session_state"]["sequence"].as_u64().unwrap()
    );
    let (_, metrics) = worker.call(WorkerCommand::GetMetrics, 1, true)?;
    let trace = &metrics["result"]["capacity_trace"];
    assert_eq!(trace["enabled"], true);
    let mapping = trace["runtimeMappings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|mapping| mapping["databaseId"] == json!(worker.fixture.identifier))
        .unwrap();
    let database = trace["databases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|database| database["queueInstance"] == mapping["queueInstance"])
        .unwrap();
    assert!(database["events"].is_null());
    let stream_id = database["streamId"].as_str().unwrap().to_owned();
    let command = WorkerCommand::GetCapacityTrace {
        stream_id: stream_id.clone(),
        after_sequence: 0,
        max_events: 128,
    };
    let (_, denied) = worker.call(command.clone(), 1, false)?;
    assert_eq!(denied["ok"], false);
    let (_, page) = worker.call(command, 1, true)?;
    assert_eq!(page["ok"], true);
    assert_eq!(page["result"]["enabled"], true);
    assert!(serde_json::to_vec(&page)?.len() < 64 * 1024);
    let events = page["result"]["stream"]["events"].as_array().unwrap();
    let finish = events
        .iter()
        .find(|event| {
            event["kind"] == "cleanup_finish" && event["requestId"] == committed["request_id"]
        })
        .unwrap();
    assert_eq!(finish["success"], true);
    assert_eq!(finish["reservedLane"], "owner_sql");
    assert_eq!(finish["operation"], "COMMIT");
    assert_eq!(
        finish["terminalSequence"],
        committed["session_state"]["sequence"]
    );
    assert!(events.iter().any(|event| event["kind"] == "release"
        && event["controlId"] == finish["controlId"]
        && event["requestId"] == committed["request_id"]));
    let serialized = serde_json::to_string(&page["result"])?;
    assert!(!serialized.contains(&session));
    assert!(!serialized.contains(CONTROL));
    assert!(!serialized.contains(SQL));
    let (_, invalid) = worker.call(
        WorkerCommand::GetCapacityTrace {
            stream_id,
            after_sequence: 0,
            max_events: 0,
        },
        1,
        true,
    )?;
    assert_eq!(invalid["ok"], false);
    println!(
        "CAPACITY_TRACE_OPT_IN_ASSERTIONS_COMPLETE enabled=1 reserved_cleanup_ack_join=verified"
    );
    Ok(())
}

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
fn readonly_describe_issues_fresh_writer_state_before_and_after_native_changes() -> Result<()> {
    let worker = Worker::new(true)?;
    let (session, opened) = worker.open(2)?;
    let mut previous = opened["session_state"]["sequence"].as_u64().unwrap();
    for _ in 0..2 {
        let (status, described) = worker.call(
            WorkerCommand::Describe {
                session: session.clone(),
                sql: "SELECT 1".into(),
            },
            2,
            false,
        )?;
        assert_eq!(status, 200);
        assert_eq!(described["ok"], true);
        assert_eq!(described["result"]["is_readonly"], true);
        previous = assert_state(&described, &session, false, previous);
    }
    let executed = worker.execute(&session, "CREATE TABLE described(id INTEGER)")?;
    assert_eq!(executed["ok"], true);
    previous = assert_state(&executed, &session, false, previous);
    let (_, closed) = worker.call(
        WorkerCommand::CloseSession {
            session: session.clone(),
        },
        2,
        false,
    )?;
    assert_eq!(closed["ok"], true);
    assert_state(&closed, &session, false, previous);
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
    let (status, rejected) = worker.call(
        WorkerCommand::Query {
            session: "f".repeat(64),
            sql: "SELECT 1".into(),
        },
        2,
        false,
    )?;
    assert_eq!(status, 429);
    assert_eq!(rejected["admission"], "not_admitted");
    assert_eq!(rejected["execution_completed"], false);
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
