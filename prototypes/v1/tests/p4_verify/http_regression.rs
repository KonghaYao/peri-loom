use super::{
    identifier,
    process_support::{Cluster, AUTHORITY_CONTROL, WORKER_A, WORKER_CONTROL},
};
use anyhow::{ensure, Result};
use peri_loom_v1::{
    authority_rpc::protocol::{AuthorityCommand, RpcRequest},
    ownership::types::OwnershipCommand,
    worker_rpc::protocol::{WorkerCommand, WorkerRequest},
};
use std::{
    io::{Read, Write},
    net::TcpStream,
    thread,
    time::Duration,
};

fn fragmented(address: &str, path: &str, token: &str, body: &[u8]) -> Result<()> {
    let mut stream = TcpStream::connect(address)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    stream.write_all(format!("POST {path} HTTP/1.1\r\n").as_bytes())?;
    thread::sleep(Duration::from_millis(50));
    stream.write_all(format!("Host: {address}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes())?;
    let midpoint = body.len() / 2;
    stream.write_all(&body[..midpoint])?;
    thread::sleep(Duration::from_millis(100));
    stream.write_all(&body[midpoint..])?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    let text = String::from_utf8(response)?;
    ensure!(
        text.starts_with("HTTP/1.1 200 "),
        "fragmented authenticated request was rejected: {text}"
    );
    let body = text.split_once("\r\n\r\n").unwrap().1;
    let response: serde_json::Value = serde_json::from_str(body)?;
    ensure!(
        response["ok"] == true,
        "fragmented RPC did not complete successfully"
    );
    Ok(())
}

#[test]
fn valid_fragmented_authority_and_worker_http_requests_do_not_fail_with_eagain() -> Result<()> {
    let mut cluster = Cluster::new()?;
    let mut worker = cluster.worker("verify-a", WORKER_A)?;
    let authority_body = serde_json::to_vec(&RpcRequest {
        version: 1,
        request_id: "fragmented-authority".into(),
        command: AuthorityCommand::GetDatabaseState {
            database: identifier("fragmented"),
        },
    })?;
    fragmented(
        &cluster.authority.address,
        "/",
        AUTHORITY_CONTROL,
        &authority_body,
    )?;
    let worker_body = serde_json::to_vec(&WorkerRequest {
        version: 1,
        request_id: "fragmented-worker".into(),
        command: WorkerCommand::Ownership {
            action: OwnershipCommand::GetWorkerState,
        },
    })?;
    fragmented(&worker.address, "/control", WORKER_CONTROL, &worker_body)?;
    worker.worker(true, WorkerCommand::Shutdown)?;
    assert!(worker.wait()?.success());
    cluster.control.call(AuthorityCommand::Shutdown)?;
    assert!(cluster.authority.wait()?.success());
    Ok(())
}
