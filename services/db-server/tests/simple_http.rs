//! Real single-process Simple smoke: HTTP, crash recovery, instance lock, and offline transfer.
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use reqwest::{Client, Method, StatusCode};
use serde_json::{json, Value};

struct Server {
    child: Child,
    url: String,
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}
fn spawn(data: &Path) -> Server {
    let port = free_port();
    let child = Command::new(env!("CARGO_BIN_EXE_peri-loom"))
        .args(["serve", "--mode", "simple", "--data-dir"])
        .arg(data)
        .arg("--listen")
        .arg(format!("127.0.0.1:{port}"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    Server {
        child,
        url: format!("http://127.0.0.1:{port}"),
    }
}
async fn ready(server: &mut Server, client: &Client) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(Some(status)) = server.child.try_wait() {
            panic!("Simple process exited during startup: {status}");
        }
        if let Ok(response) = client.get(format!("{}/readyz", server.url)).send().await {
            if response.status() == StatusCode::OK {
                return;
            }
        }
        assert!(Instant::now() < deadline, "Simple startup timed out");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
async fn request(
    client: &Client,
    url: &str,
    method: Method,
    path: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = client.request(method, format!("{url}{path}"));
    if let Some(token) = token {
        builder = builder.bearer_auth(token);
    }
    if let Some(body) = body {
        builder = builder.json(&body);
    }
    let response = builder.send().await.unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    let body = serde_json::from_str(&text).unwrap_or_else(|_| json!({"raw": text}));
    (status, body)
}
async fn accepted(
    client: &Client,
    url: &str,
    token: &str,
    method: Method,
    path: &str,
    body: Option<Value>,
) -> Value {
    let (status, value) = request(client, url, method, path, Some(token), body).await;
    assert_eq!(status, StatusCode::ACCEPTED, "operation request: {value}");
    let id = value["operation_id"].as_str().expect("operation id");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (status, operation) = request(
            client,
            url,
            Method::GET,
            &format!("/api/v1/operations/{id}"),
            Some(token),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        match operation["state"].as_str() {
            Some("SUCCEEDED") => return value,
            Some("FAILED") | Some("CANCELLED") => panic!("operation {id} failed: {operation}"),
            _ => {
                assert!(Instant::now() < deadline, "operation {id} timed out");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}
async fn query(
    client: &Client,
    url: &str,
    token: &str,
    db: &str,
    sql: &str,
    params: Vec<Value>,
) -> Value {
    let (status, body) = request(
        client,
        url,
        Method::POST,
        &format!("/data/v1/databases/{db}/query"),
        Some(token),
        Some(json!({"sql":sql,"params":params})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query failed: {body}");
    body
}
async fn session_query(client: &Client, url: &str, token: &str, session: &str, sql: &str) -> Value {
    let (status, body) = request(
        client,
        url,
        Method::POST,
        &format!("/data/v1/sessions/{session}/query"),
        Some(token),
        Some(json!({"sql":sql})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "session query failed: {body}");
    body
}

async fn offline_success(mut command: Command, label: &str) {
    let mut child = command
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "{label} failed");
            return;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("{label} timed out");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_binary_crash_recovery_and_offline_transfer() {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("instance");
    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let mut server = spawn(&data);
    ready(&mut server, &client).await;
    let (_, deployment) = request(
        &client,
        &server.url,
        Method::GET,
        "/api/v1/deployment",
        None,
        None,
    )
    .await;
    assert_eq!(deployment["mode"], "simple");
    assert_eq!(deployment["capabilities"]["remote_durability_lsn"], false);
    let unknown = client
        .get(format!("{}/api/v1/no-such-route", server.url))
        .send()
        .await
        .unwrap();
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
    assert!(!unknown
        .headers()
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .contains("text/html"));
    let unauth_metrics = client
        .get(format!("{}/metrics", server.url))
        .send()
        .await
        .unwrap();
    assert_ne!(unauth_metrics.status(), StatusCode::OK);
    let initial: Value =
        serde_json::from_slice(&std::fs::read(data.join("secrets/initial-admin.json")).unwrap())
            .unwrap();
    let (status, login) = request(
        &client,
        &server.url,
        Method::POST,
        "/api/v1/auth/login",
        None,
        Some(json!({"username":initial["username"],"password":initial["password"]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "admin login failed");
    let token = login["access_token"].as_str().unwrap().to_owned();
    drop(initial);
    let created = accepted(
        &client,
        &server.url,
        &token,
        Method::POST,
        "/api/v1/databases",
        Some(json!({"name":"smoke"})),
    )
    .await;
    let db = created["database_id"].as_str().unwrap().to_owned();
    query(
        &client,
        &server.url,
        &token,
        &db,
        "CREATE TABLE t(v INTEGER)",
        vec![],
    )
    .await;
    query(
        &client,
        &server.url,
        &token,
        &db,
        "INSERT INTO t VALUES (?1)",
        vec![json!(41)],
    )
    .await;
    let rows = query(&client, &server.url, &token, &db, "SELECT v FROM t", vec![]).await;
    assert_eq!(rows["rows"], json!([[41]]));
    assert!(
        rows["wal_lsn"].is_null(),
        "local mode must not claim remote LSN"
    );
    for version in ["v2", "v3"] {
        let (status, response) = request(&client, &server.url, Method::POST, &format!("/db/{db}/{version}/pipeline"), Some(&token), Some(json!({
            "requests":[{"type":"execute","stmt":{"sql":"SELECT ?1 AS n","args":[{"type":"integer","value":"42"}]}}]
        }))).await;
        assert_eq!(status, StatusCode::OK, "Hrana {version}: {response}");
        assert_eq!(response["results"][0]["type"], "ok");
        assert_eq!(
            response["results"][0]["response"]["result"]["rows"][0][0]["value"],
            "42"
        );
    }
    let ndjson_response = client
        .post(format!("{}/data/v1/databases/{db}/query", server.url))
        .bearer_auth(&token)
        .header("accept", "application/x-ndjson")
        .json(&json!({"sql":"SELECT v FROM t"}))
        .send()
        .await
        .unwrap();
    assert_eq!(ndjson_response.status(), StatusCode::OK);
    assert!(ndjson_response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .starts_with("application/x-ndjson"));
    let lines: Vec<Value> = ndjson_response
        .text()
        .await
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lines.first().unwrap()["type"], "header");
    assert!(lines
        .iter()
        .any(|line| line["type"] == "row" && line["values"] == json!([41])));
    assert_eq!(lines.last().unwrap()["type"], "trailer");
    let (status, created_token) = request(
        &client,
        &server.url,
        Method::POST,
        "/api/v1/tokens",
        Some(&token),
        Some(json!({"name":"crash-survivor"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let api_token = created_token["token"].as_str().unwrap().to_owned();
    let (status, opened) = request(
        &client,
        &server.url,
        Method::POST,
        &format!("/data/v1/databases/{db}/sessions"),
        Some(&token),
        None,
    )
    .await;
    assert!(status.is_success(), "open session failed: {opened}");
    let session = opened["session_id"].as_str().unwrap();
    session_query(&client, &server.url, &token, session, "BEGIN").await;
    session_query(
        &client,
        &server.url,
        &token,
        session,
        "INSERT INTO t VALUES (99)",
    )
    .await;
    let own = session_query(
        &client,
        &server.url,
        &token,
        session,
        "SELECT COUNT(*) FROM t",
    )
    .await;
    assert_eq!(own["rows"], json!([[2]]));
    let (status, second_opened) = request(
        &client,
        &server.url,
        Method::POST,
        &format!("/data/v1/databases/{db}/sessions"),
        Some(&token),
        None,
    )
    .await;
    assert!(status.is_success());
    let second_session = second_opened["session_id"].as_str().unwrap();
    let other_path = format!("/data/v1/sessions/{second_session}/query");
    let other_read = request(
        &client,
        &server.url,
        Method::POST,
        &other_path,
        Some(&token),
        Some(json!({"sql":"SELECT COUNT(*) FROM t"})),
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), other_read)
            .await
            .is_err(),
        "another session observed an uncommitted transaction"
    );
    // 第二实例不得取得同一个目录锁。
    let mut contender = spawn(&data);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = contender.child.try_wait().unwrap() {
            assert!(!status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "second instance did not reject data-dir lock"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    server.child.kill().unwrap(); // Child::kill on Unix sends SIGKILL, not graceful shutdown.
    server.child.wait().unwrap();
    let mut restarted = spawn(&data);
    ready(&mut restarted, &client).await;
    let (status, _) = request(
        &client,
        &restarted.url,
        Method::GET,
        "/api/v1/auth/me",
        Some(&token),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "JWT signing identity changed across crash"
    );
    let recovered = query(
        &client,
        &restarted.url,
        &token,
        &db,
        "SELECT v FROM t ORDER BY v",
        vec![],
    )
    .await;
    assert_eq!(recovered["rows"], json!([[41]]));
    let token_response = client
        .post(format!("{}/data/v1/databases/{db}/query", restarted.url))
        .header("x-api-token", &api_token)
        .json(&json!({"sql":"SELECT v FROM t"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        token_response.status(),
        StatusCode::OK,
        "API token did not survive crash"
    );
    let (status, audit) = request(
        &client,
        &restarted.url,
        Method::GET,
        "/api/v1/audit",
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(audit["items"]
        .as_array()
        .is_some_and(|items| !items.is_empty()));
    let (status, operation) = request(
        &client,
        &restarted.url,
        Method::GET,
        &format!(
            "/api/v1/operations/{}",
            created["operation_id"].as_str().unwrap()
        ),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(operation["state"], "SUCCEEDED");
    let (status, _) = request(
        &client,
        &restarted.url,
        Method::POST,
        &format!("/api/v1/databases/{db}/move"),
        Some(&token),
        Some(json!({"target_worker_id":"x"})),
    )
    .await;
    assert!(status == StatusCode::NOT_IMPLEMENTED || status == StatusCode::BAD_REQUEST);
    // Export and import are offline operations; the imported instance must serve the same SQL data.
    restarted.child.kill().unwrap();
    restarted.child.wait().unwrap();
    let archive = temp.path().join("export.tar.zst");
    let mut export = Command::new(env!("CARGO_BIN_EXE_peri-loom"));
    export
        .arg("export")
        .arg("--data-dir")
        .arg(&data)
        .arg("--output")
        .arg(&archive);
    offline_success(export, "offline export").await;
    let imported = temp.path().join("imported");
    let mut import = Command::new(env!("CARGO_BIN_EXE_peri-loom"));
    import
        .arg("import")
        .arg("--input")
        .arg(&archive)
        .arg("--data-dir")
        .arg(&imported);
    offline_success(import, "offline import").await;
    let mut imported_server = spawn(&imported);
    ready(&mut imported_server, &client).await;
    let imported_rows = query(
        &client,
        &imported_server.url,
        &token,
        &db,
        "SELECT v FROM t",
        vec![],
    )
    .await;
    assert_eq!(imported_rows["rows"], json!([[41]]));
    let broken = accepted(
        &client,
        &imported_server.url,
        &token,
        Method::POST,
        "/api/v1/databases",
        Some(json!({"name":"broken"})),
    )
    .await;
    let broken_id = broken["database_id"].as_str().unwrap();
    let broken_dir = imported.join("databases").join(broken_id);
    std::fs::create_dir_all(&broken_dir).unwrap();
    std::fs::write(broken_dir.join("main.db"), b"this is not a sqlite database").unwrap();
    let (broken_status, _) = request(
        &client,
        &imported_server.url,
        Method::POST,
        &format!("/data/v1/databases/{broken_id}/query"),
        Some(&token),
        Some(json!({"sql":"SELECT name FROM sqlite_schema"})),
    )
    .await;
    assert!(!broken_status.is_success(), "corrupt database was accepted");
    let healthy = query(
        &client,
        &imported_server.url,
        &token,
        &db,
        "SELECT v FROM t",
        vec![],
    )
    .await;
    assert_eq!(healthy["rows"], json!([[41]]));
    accepted(
        &client,
        &imported_server.url,
        &token,
        Method::POST,
        &format!("/api/v1/databases/{db}/backup"),
        None,
    )
    .await;
    let (status, snapshots) = request(
        &client,
        &imported_server.url,
        Method::GET,
        &format!("/api/v1/snapshots?database_id={db}"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let snapshot = snapshots
        .as_array()
        .and_then(|items| items.first())
        .expect("snapshot record");
    let snapshot_id = snapshot["id"].as_str().unwrap();
    let manifest_key = snapshot["object_key"].as_str().unwrap();
    let metadata = catalog::SqliteCatalog::connect(imported.join("catalog/metadata.db"))
        .await
        .unwrap();
    let backups = metadata
        .list_backup_jobs(db.parse().unwrap(), 10)
        .await
        .unwrap();
    assert_eq!(backups.len(), 1);
    assert_eq!(backups[0].state, "SUCCEEDED");
    assert_eq!(backups[0].snapshot_id.as_deref(), Some(snapshot_id));
    metadata.close().await.unwrap();
    query(
        &client,
        &imported_server.url,
        &token,
        &db,
        "INSERT INTO t VALUES (50)",
        vec![],
    )
    .await;
    accepted(
        &client,
        &imported_server.url,
        &token,
        Method::POST,
        &format!("/api/v1/databases/{db}/restore"),
        Some(json!({"snapshot_id":snapshot_id})),
    )
    .await;
    let restored = query(
        &client,
        &imported_server.url,
        &token,
        &db,
        "SELECT v FROM t",
        vec![],
    )
    .await;
    assert_eq!(restored["rows"], json!([[41]]));
    // Corrupt the stored manifest; verified restore must fail before replacing live files.
    std::fs::write(imported.join("objects").join(manifest_key), b"corrupt").unwrap();
    let (status, submitted) = request(
        &client,
        &imported_server.url,
        Method::POST,
        &format!("/api/v1/databases/{db}/restore"),
        Some(&token),
        Some(json!({"snapshot_id":snapshot_id})),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let operation_id = submitted["operation_id"].as_str().unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (_, operation) = request(
            &client,
            &imported_server.url,
            Method::GET,
            &format!("/api/v1/operations/{operation_id}"),
            Some(&token),
            None,
        )
        .await;
        if operation["state"] == "FAILED" {
            break;
        }
        assert!(Instant::now() < deadline, "corrupted restore did not fail");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let still_live = query(
        &client,
        &imported_server.url,
        &token,
        &db,
        "SELECT v FROM t",
        vec![],
    )
    .await;
    assert_eq!(still_live["rows"], json!([[41]]));
}
