use super::harness::{self, Harness};
use anyhow::{ensure, Result};
use peri_loom_v1::authority_rpc::client::AuthorityClient;
use peri_loom_v1::authority_rpc::protocol::{AuthorityCommand, RpcRequest};
use peri_loom_v1::authority_rpc::startup::AuthorityStartup;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

fn packet(stream: &mut TcpStream) -> Result<RpcRequest> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        ensure!(header.len() < 16_384, "test header too large");
        let mut byte = [0];
        stream.read_exact(&mut byte)?;
        header.push(byte[0]);
    }
    let header = String::from_utf8(header)?;
    let length: usize = header
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().ok())
                .flatten()
        })
        .ok_or_else(|| anyhow::anyhow!("test content length missing"))?;
    ensure!(length < 32 * 1024 * 1024, "test body too large");
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn fake_call(make_reply: impl FnOnce(RpcRequest) -> String + Send + 'static) -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let endpoint = format!("http://{}/", listener.local_addr()?);
    let thread = std::thread::spawn(move || -> Result<()> {
        let (mut stream, _) = listener.accept()?;
        let request = packet(&mut stream)?;
        let body = make_reply(request);
        write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body)?;
        Ok(())
    });
    let credential = harness::token()?;
    let client = AuthorityClient::new(&endpoint, credential.clone())?;
    let result = client.call(AuthorityCommand::Shutdown);
    thread
        .join()
        .map_err(|_| anyhow::anyhow!("test peer panicked"))??;
    let error = result.expect_err("malformed peer must fail").to_string();
    assert!(!error.contains(&credential));
    Ok(error)
}

#[test]
fn strict_response_identity_unknown_duplicate_and_missing_fields() -> Result<()> {
    fake_call(|request| {
        serde_json::json!({"version":1,"request_id":request.request_id,"ok":true,"result":null,"error":null,"precondition_failed":false,"extra":true}).to_string()
    })?;
    fake_call(|request| {
        format!("{{\"version\":1,\"version\":1,\"request_id\":\"{}\",\"ok\":true,\"result\":null,\"error\":null,\"precondition_failed\":false}}", request.request_id)
    })?;
    fake_call(|request| {
        serde_json::json!({"version":1,"request_id":request.request_id,"ok":true,"error":null,"precondition_failed":false}).to_string()
    })?;
    fake_call(|_| {
        serde_json::json!({"version":1,"request_id":"wrong","ok":true,"result":null,"error":null,"precondition_failed":false}).to_string()
    })?;
    Ok(())
}

#[test]
fn response_null_and_typed_preconditions_are_preserved() -> Result<()> {
    for conflict in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let client = AuthorityClient::new(
            &format!("http://{}/", listener.local_addr()?),
            harness::token()?,
        )?;
        let thread = std::thread::spawn(move || -> Result<()> {
            let (mut stream, _) = listener.accept()?;
            let request = packet(&mut stream)?;
            let body = serde_json::json!({"version":1,"request_id":request.request_id,"ok":!conflict,"result":null,"error":if conflict {Some("hidden backend detail")} else {None},"precondition_failed":conflict}).to_string();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )?;
            Ok(())
        });
        let response = client.call(AuthorityCommand::Shutdown);
        if conflict {
            assert!(response
                .unwrap_err()
                .downcast_ref::<peri_loom_v1::object_store::PreconditionFailed>()
                .is_some());
        } else {
            assert_eq!(response?, serde_json::Value::Null);
        }
        thread
            .join()
            .map_err(|_| anyhow::anyhow!("test peer panicked"))??;
    }
    Ok(())
}

#[test]
fn endpoint_validation_and_lost_mutating_reply_have_no_retry() -> Result<()> {
    for endpoint in [
        "http://example.com/",
        "http://127.0.0.1/other",
        "https://127.0.0.1/",
        "http://user@127.0.0.1/",
        "http://127.0.0.1/?token=bad",
    ] {
        assert!(AuthorityClient::new(endpoint, harness::token()?).is_err());
    }
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let client = AuthorityClient::new(
        &format!("http://{}/", listener.local_addr()?),
        harness::token()?,
    )?;
    let thread = std::thread::spawn(move || -> Result<()> {
        let (mut stream, _) = listener.accept()?;
        let request = packet(&mut stream)?;
        assert!(matches!(request.command, AuthorityCommand::Shutdown));
        drop(stream);
        listener.set_nonblocking(true)?;
        std::thread::sleep(Duration::from_millis(150));
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
        Ok(())
    });
    assert!(client.call(AuthorityCommand::Shutdown).is_err());
    thread
        .join()
        .map_err(|_| anyhow::anyhow!("test peer panicked"))??;
    Ok(())
}

#[test]
fn startup_exclusive_dirty_reconciliation_and_clean_restart() -> Result<()> {
    let directory = harness::private_directory()?;
    let startup = AuthorityStartup::acquire(directory.path(), false)?;
    assert!(AuthorityStartup::acquire(directory.path(), false).is_err());
    drop(startup);
    assert!(AuthorityStartup::acquire(directory.path(), false).is_err());
    drop(AuthorityStartup::acquire(directory.path(), true)?);
    assert!(AuthorityStartup::acquire(directory.path(), false).is_err());
    let backend = std::sync::Arc::new(peri_loom_v1::object_store::MemoryObjectStore::default());
    let mut service = Harness::at(directory.path(), backend.clone(), true)?;
    let lease = service.grant(harness::identifier("term-restart")?, "worker-a", 1, 0)?;
    service.shutdown()?;
    let mut restarted = Harness::at(directory.path(), backend, false)?;
    let state = restarted.control.call(AuthorityCommand::GetDatabaseState {
        database: lease.binding.database.clone(),
    })?;
    assert!(state["binding"].is_null());
    assert!(restarted
        .worker
        .call(AuthorityCommand::ValidateLease {
            binding: lease.binding,
            role: peri_loom_v1::ownership::types::AccessRole::Writer,
        })
        .is_err());
    restarted.shutdown()
}

#[test]
fn committed_grant_with_lost_reply_can_be_explicitly_replayed() -> Result<()> {
    let mut service = Harness::new()?;
    let grant = service.grant_request(harness::identifier("reply-loss")?, "worker-a", 1, 0)?;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let endpoint = format!("http://{}/", listener.local_addr()?);
    let control = service.control.clone();
    let expected_id = grant.request_id.clone();
    let thread = std::thread::spawn(move || -> Result<serde_json::Value> {
        let (mut stream, _) = listener.accept()?;
        let request = packet(&mut stream)?;
        assert_eq!(request.request_id, expected_id);
        let committed = control.call(request.command)?;
        drop(stream);
        Ok(committed)
    });
    let client = AuthorityClient::new(&endpoint, harness::token()?)?;
    assert!(client
        .call(AuthorityCommand::Grant {
            request: grant.clone()
        })
        .is_err());
    let committed = thread
        .join()
        .map_err(|_| anyhow::anyhow!("test proxy panicked"))??;
    let replay = service
        .control
        .call(AuthorityCommand::Grant { request: grant })?;
    assert_eq!(committed, replay);
    service.shutdown()
}

#[test]
fn real_http_rejects_wrong_method_path_unknown_fields_and_body_limit() -> Result<()> {
    let mut service = Harness::new()?;
    let http = reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()?;
    let request = serde_json::json!({"version":1,"request_id":harness::token()?,"command":{"command":"get_database_state","database":harness::identifier("http")?},"unknown":true});
    let forged = serde_json::json!({"version":1,"request_id":harness::token()?,"role":"control","command":{"command":"shutdown"}});
    let response = http
        .post(&service.endpoint)
        .bearer_auth(&service.worker_token)
        .json(&forged)
        .send()?;
    assert!(!response.status().is_success());
    for (method, path, body) in [
        (reqwest::Method::GET, "", b"{}".to_vec()),
        (reqwest::Method::POST, "other", b"{}".to_vec()),
        (reqwest::Method::POST, "", serde_json::to_vec(&request)?),
    ] {
        let response = http
            .request(method, format!("{}{path}", service.endpoint))
            .bearer_auth(&service.control_token)
            .body(body)
            .send()?;
        if response.status().is_success() {
            let body: serde_json::Value = response.json()?;
            assert_eq!(body["ok"], false);
        }
    }
    let address = url::Url::parse(&service.endpoint)?.socket_addrs(|| None)?[0];
    let mut stream = TcpStream::connect(address)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(stream, "POST / HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nContent-Length: 33554433\r\n\r\n", service.control_token)?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    assert!(!response.starts_with("HTTP/1.1 200 "));
    service.shutdown()
}
