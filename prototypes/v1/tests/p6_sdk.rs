use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::sdk::{Client, ClientOptions, DatabaseId, ManagementRequest};
use serde_json::{json, Value};

const CREDENTIAL: &str = "synthetic-api-credential__________________";
const SESSION: &str = "0123456789abcdef0123456789abcdef";

fn database() -> DatabaseId {
    DatabaseId::new(Namespace::new("tenant", "database").unwrap(), "main").unwrap()
}

fn request(stream: &mut TcpStream) -> (String, Value) {
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut bytes = Vec::new();
    let mut byte = [0; 1];
    while !bytes.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).unwrap();
        bytes.push(byte[0]);
        assert!(bytes.len() < 8192);
    }
    let header = String::from_utf8(bytes).unwrap();
    assert!(header.contains(&format!("Bearer {CREDENTIAL}")));
    let length = header
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    let mut body = vec![0; length];
    stream.read_exact(&mut body).unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    let correlation = header.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("x-request-id")
            .then(|| value.trim())
    });
    assert_eq!(correlation, body["request_id"].as_str());
    assert!(correlation.is_some());
    (header.split_whitespace().nth(1).unwrap().into(), body)
}

fn server(
    count: usize,
    mut handler: impl FnMut(&str, &Value) -> Option<Value> + Send + 'static,
) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(8);
        for _ in 0..count {
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "SDK did not issue expected request"
                        );
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("test listener failed: {error}"),
                }
            };
            let (path, body) = request(&mut stream);
            if let Some(reply) = handler(&path, &body) {
                let bytes = serde_json::to_vec(&reply).unwrap();
                let status = match reply["error"]["code"].as_str() {
                    Some("quota_exceeded") => "429 Too Many Requests",
                    Some("unauthenticated") => "401 Unauthorized",
                    _ => "200 OK",
                };
                write!(stream, "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n", bytes.len()).unwrap();
                stream.write_all(&bytes).unwrap();
            }
        }
        thread::sleep(Duration::from_millis(150));
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
            "SDK retried or reopened implicitly"
        );
    });
    (endpoint, task)
}

fn success(body: &Value, result: Value) -> Value {
    json!({"version":1,"request_id":body["request_id"],"ok":true,"result":result,"error":null})
}

#[test]
fn sdk_header_correlated_early_rejections_never_replay() {
    for code in ["quota_exceeded", "unauthenticated"] {
        for action in ["open", "execute", "query"] {
            let (endpoint, task) = server(
                if action == "open" { 1 } else { 2 },
                move |path, body| {
                    if action != "open" && path.ends_with("/open") {
                        Some(success(body, json!({"session":SESSION,"readonly":false})))
                    } else {
                        Some(
                            json!({"version":1,"request_id":body["request_id"],"ok":false,"result":null,"error":{"code":code}}),
                        )
                    }
                },
            );
            let client = Client::new(&endpoint, CREDENTIAL.into()).unwrap();
            let error = if action == "open" {
                client
                    .open(&database())
                    .expect_err("expected typed rejection")
            } else {
                let session = client.open(&database()).unwrap();
                let error = if action == "execute" {
                    session.execute("COMMIT", Some("early-reject")).unwrap_err()
                } else {
                    session
                        .query(
                            "INSERT INTO items VALUES(1) RETURNING id",
                            Some("early-reject"),
                        )
                        .unwrap_err()
                };
                assert_eq!(error.request_id, "early-reject");
                error
            };
            assert_eq!(error.code, code);
            assert!(!error.outcome_unknown);
            assert!(!error.execution_completed && !error.may_have_committed);
            task.join().unwrap();
        }
    }
}

#[test]
fn sdk_mismatched_early_rejection_id_remains_unknown_and_poisoned() {
    for code in ["quota_exceeded", "unauthenticated"] {
        let (endpoint, task) = server(2, move |path, body| {
            if path.ends_with("/open") {
                Some(success(body, json!({"session":SESSION,"readonly":false})))
            } else {
                Some(
                    json!({"version":1,"request_id":"wrong-response-id","ok":false,"result":null,"error":{"code":code}}),
                )
            }
        });
        let session = Client::new(&endpoint, CREDENTIAL.into())
            .unwrap()
            .open(&database())
            .unwrap();
        let error = session.query("COMMIT", Some("early-reject")).unwrap_err();
        assert_eq!(error.code, "outcome_unknown");
        assert!(error.outcome_unknown);
        assert_eq!(error.request_id, "early-reject");
        assert_eq!(
            session
                .execute("COMMIT", Some("no-replay"))
                .unwrap_err()
                .code,
            "session_unusable"
        );
        task.join().unwrap();
    }
}

#[test]
fn sdk_native_wire_contract_and_lossless_integer_cells() {
    let (endpoint, task) = server(6, |path, body| {
        Some(success(
            body,
            match path {
                "/v1/sql/open" => json!({"session":SESSION,"readonly":false}),
                "/v1/sql/execute" => json!({"executed":true}),
                "/v1/sql/query" => json!([[9223372036854775807_i64, "native"]]),
                "/v1/databases/restore-point" => {
                    assert_eq!(body["sequence"], 3);
                    json!({"restored":true})
                }
                "/v1/organizations/create" => {
                    assert!(body.get("database").is_none());
                    json!({"created":true})
                }
                "/v1/sql/close" => json!({"closed":true}),
                _ => panic!("unrecognized SDK path"),
            },
        ))
    });
    let client = Client::new(&endpoint, CREDENTIAL.into()).unwrap();
    assert!(!format!("{client:?}").contains(CREDENTIAL));
    let session = client.open(&database()).unwrap();
    session.execute("BEGIN; COMMIT", Some("execute-1")).unwrap();
    let rows = session
        .query(
            "INSERT INTO items VALUES(1) RETURNING id",
            Some("query-write"),
        )
        .unwrap();
    assert_eq!(rows[0][0].as_i64(), Some(i64::MAX));
    let mut restore = ManagementRequest::new(database());
    restore.sequence = Some(3);
    client
        .management("databases/restore-point", restore)
        .unwrap();
    client
        .management(
            "organizations/create",
            ManagementRequest {
                name: Some("local".into()),
                ..ManagementRequest::default()
            },
        )
        .unwrap();
    session.close().unwrap();
    session.close().unwrap();
    task.join().unwrap();
}

#[test]
fn sdk_execute_and_write_query_lost_ack_never_replay() {
    for action in ["execute", "query"] {
        let action_name = action.to_string();
        let (endpoint, task) = server(2, move |path, body| {
            if path.ends_with("/open") {
                Some(success(body, json!({"session":SESSION,"readonly":false})))
            } else {
                assert_eq!(path, format!("/v1/sql/{action_name}"));
                None
            }
        });
        let client = Client::new(&endpoint, CREDENTIAL.into()).unwrap();
        let session = client.open(&database()).unwrap();
        let error = if action == "execute" {
            session.execute("COMMIT", Some("lost-ack")).unwrap_err()
        } else {
            session
                .query("INSERT INTO items VALUES(1) RETURNING id", Some("lost-ack"))
                .unwrap_err()
        };
        assert!(error.outcome_unknown);
        assert_eq!(error.request_id, "lost-ack");
        assert!(!error.to_string().contains(CREDENTIAL));
        assert_eq!(
            session
                .execute("COMMIT", Some("no-replay"))
                .unwrap_err()
                .code,
            "session_unusable"
        );
        task.join().unwrap();
    }
}

#[test]
fn sdk_session_moved_requires_explicit_new_session() {
    let (endpoint, task) = server(2, |path, body| {
        if path.ends_with("/open") {
            Some(success(body, json!({"session":SESSION,"readonly":true})))
        } else {
            Some(
                json!({"version":1,"request_id":body["request_id"],"ok":false,"result":null,"error":{"code":"session_moved"}}),
            )
        }
    });
    let session = Client::new(&endpoint, CREDENTIAL.into())
        .unwrap()
        .open(&database())
        .unwrap();
    assert!(session.is_readonly());
    assert_eq!(
        session.query("SELECT 1", Some("moved")).unwrap_err().code,
        "session_moved"
    );
    assert_eq!(
        session
            .query("SELECT 1", Some("no-reconnect"))
            .unwrap_err()
            .code,
        "session_unusable"
    );
    task.join().unwrap();
}

#[test]
fn sdk_terminal_sql_failure_allows_explicit_rollback_without_replay() {
    let (endpoint, task) = server(4, |path, body| {
        if path.ends_with("/open") {
            return Some(success(body, json!({"session":SESSION,"readonly":false})));
        }
        assert_eq!(body["session"], SESSION);
        match body["sql"].as_str().unwrap() {
            "invalid SQL" => Some(
                json!({"version":1,"request_id":body["request_id"],"ok":false,"result":null,"error":{"code":"sql_error","execution_completed":true,"may_have_committed":true}}),
            ),
            "ROLLBACK" => Some(success(body, json!({"executed":true}))),
            "SELECT 1" => Some(success(body, json!([[1]]))),
            _ => panic!("unexpected replay"),
        }
    });
    let session = Client::new(&endpoint, CREDENTIAL.into())
        .unwrap()
        .open(&database())
        .unwrap();
    let failure = session
        .execute("invalid SQL", Some("terminal"))
        .unwrap_err();
    assert_eq!(failure.code, "sql_error");
    assert!(failure.execution_completed && failure.may_have_committed && !failure.outcome_unknown);
    session
        .execute("ROLLBACK", Some("explicit-rollback"))
        .unwrap();
    assert_eq!(
        session.query("SELECT 1", Some("explicit-reuse")).unwrap(),
        vec![vec![json!(1)]]
    );
    task.join().unwrap();
}

#[test]
fn sdk_endpoint_validation_limits_and_malformed_ack() {
    for endpoint in [
        "http://localhost:8080",
        "http://192.0.2.1",
        "ftp://127.0.0.1",
        "https://user:password@example.test",
        "https://example.test/private",
    ] {
        assert_eq!(
            Client::new(endpoint, CREDENTIAL.into()).unwrap_err().code,
            "invalid_endpoint"
        );
    }
    let client = Client::with_options(
        "http://127.0.0.1:1",
        CREDENTIAL.into(),
        ClientOptions {
            max_request_bytes: 1,
            ..ClientOptions::default()
        },
    )
    .unwrap();
    assert_eq!(client.open(&database()).unwrap_err().code, "request_limit");
    let (endpoint, task) = server(2, |path, body| {
        Some(success(
            body,
            if path.ends_with("/open") {
                json!({"session":SESSION,"readonly":false})
            } else {
                json!({"executed":false})
            },
        ))
    });
    let session = Client::new(&endpoint, CREDENTIAL.into())
        .unwrap()
        .open(&database())
        .unwrap();
    let error = session.execute("COMMIT", Some("bad-ack")).unwrap_err();
    assert!(error.outcome_unknown);
    assert_eq!(
        session
            .query("SELECT 1", Some("must-not-run"))
            .unwrap_err()
            .code,
        "session_unusable"
    );
    task.join().unwrap();
}
