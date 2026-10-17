use anyhow::Result;
use peri_loom_v1::object_store::{
    GroupWrite, LocalObjectStore, MemoryObjectStore, ObjectStore, PreconditionFailed, PutCondition,
    S3ObjectStore,
};
use sha2::{Digest, Sha256};
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    process::Command,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

#[path = "../src/object_store/http_mock.rs"]
mod http_mock;
use http_mock::{config, mock, mock_with_retries, read_request, reply, verify_signature};

#[path = "../src/object_store/error_tests.rs"]
mod error_tests;

fn contract(store: &dyn ObjectStore) -> Result<()> {
    assert!(store.get("tenant/db/HEAD")?.is_none());
    let first = store.put("tenant/db/HEAD", b"one", PutCondition::Absent)?;
    assert!(!first.is_empty());
    let object = store.get("tenant/db/HEAD")?.unwrap();
    assert_eq!(object.bytes, b"one");
    assert_eq!(object.etag, first);
    let error = store
        .put("tenant/db/HEAD", b"bad", PutCondition::Absent)
        .unwrap_err();
    assert!(error.downcast_ref::<PreconditionFailed>().is_some());
    let second = store.put("tenant/db/HEAD", b"two", PutCondition::Match(first.clone()))?;
    assert_ne!(first, second);
    assert!(store
        .put("tenant/db/HEAD", b"bad", PutCondition::Match(first.clone()))
        .unwrap_err()
        .is::<PreconditionFailed>());
    assert!(store
        .delete("tenant/db/HEAD", Some(&first))
        .unwrap_err()
        .is::<PreconditionFailed>());
    store.put("tenant/db2/HEAD", b"other", PutCondition::Any)?;
    assert_eq!(store.list("tenant/db/")?, vec!["tenant/db/HEAD"]);
    store.delete("tenant/db/HEAD", Some(&second))?;
    store.delete("tenant/db/HEAD", None)?;
    assert!(store.get("tenant/db/HEAD")?.is_none());
    for key in [
        "", "/root", "a/../b", "a/./b", "a//b", "a\\b", "a?b", "a%2Fb", "a/", "é",
    ] {
        assert!(store.get(key).is_err(), "{key}");
        assert!(store.put(key, b"bad", PutCondition::Any).is_err(), "{key}");
        assert!(store.delete(key, None).is_err(), "{key}");
    }
    assert!(store.list("../").is_err());
    Ok(())
}

#[test]
fn memory_contract() -> Result<()> {
    contract(&MemoryObjectStore::default())
}

#[test]
fn local_contract_and_reopen() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open(directory.path())?;
    contract(&store)?;
    let etag = store.put("tenant/db/HEAD", b"persisted", PutCondition::Absent)?;
    drop(store);
    let reopened = LocalObjectStore::open(directory.path())?;
    assert_eq!(reopened.get("tenant/db/HEAD")?.unwrap().etag, etag);
    assert_eq!(reopened.get("tenant/db/HEAD")?.unwrap().bytes, b"persisted");
    Ok(())
}

#[test]
fn concurrent_memory_cas_has_one_winner() -> Result<()> {
    let store = Arc::new(MemoryObjectStore::default());
    let etag = store.put("db/HEAD", b"initial", PutCondition::Absent)?;
    let barrier = Arc::new(Barrier::new(16));
    let workers: Vec<_> = (0..16)
        .map(|_| {
            let store = store.clone();
            let barrier = barrier.clone();
            let etag = etag.clone();
            thread::spawn(move || {
                barrier.wait();
                match store.put("db/HEAD", b"new", PutCondition::Match(etag)) {
                    Ok(_) => true,
                    Err(error) => {
                        assert!(error.is::<PreconditionFailed>());
                        false
                    }
                }
            })
        })
        .collect();
    assert_eq!(
        workers
            .into_iter()
            .map(|worker| usize::from(worker.join().unwrap()))
            .sum::<usize>(),
        1
    );
    Ok(())
}

#[test]
fn local_process_worker() -> Result<()> {
    let Ok(root) = std::env::var("PERI_OBJECT_STORE_TEST_ROOT") else {
        return Ok(());
    };
    let etag = std::env::var("PERI_OBJECT_STORE_TEST_ETAG")?;
    let store = LocalObjectStore::open(&root)?;
    let result = store.put("db/HEAD", b"new", PutCondition::Match(etag));
    match result {
        Ok(_) => println!("CAS_WINNER"),
        Err(error) => assert!(error.is::<PreconditionFailed>()),
    }
    Ok(())
}

#[test]
fn local_cas_is_atomic_across_processes() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open(directory.path())?;
    let etag = store.put("db/HEAD", b"old", PutCondition::Absent)?;
    let children: Vec<_> = (0..8)
        .map(|_| {
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "local_process_worker", "--nocapture"])
                .env("PERI_OBJECT_STORE_TEST_ROOT", directory.path())
                .env("PERI_OBJECT_STORE_TEST_ETAG", &etag)
                .stdout(std::process::Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    let mut winners = 0;
    for child in children {
        let output = child.wait_with_output()?;
        assert!(output.status.success(), "{:?}", output);
        winners += usize::from(String::from_utf8(output.stdout)?.contains("CAS_WINNER"));
    }
    assert_eq!(winners, 1);
    Ok(())
}

#[test]
fn local_corruption_is_rejected() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open(directory.path())?;
    store.put("db/HEAD", b"payload", PutCondition::Absent)?;
    let path = directory
        .path()
        .join(format!("{}.obj", blake3::hash(b"db/HEAD").to_hex()));
    let mut envelope: serde_json::Value = serde_json::from_slice(&std::fs::read(&path)?)?;
    envelope["bytes"] = serde_json::json!([0, 1]);
    std::fs::write(&path, serde_json::to_vec(&envelope)?)?;
    assert!(store.get("db/HEAD").is_err());
    assert!(store.list("db/").is_err());
    assert!(store
        .put("db/HEAD", b"overwrite", PutCondition::Any)
        .is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn local_symlink_escape_is_rejected() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    let store = LocalObjectStore::open(directory.path())?;
    let target = outside.path().join("secret");
    std::fs::write(&target, b"untouched")?;
    let path = directory
        .path()
        .join(format!("{}.obj", blake3::hash(b"db/HEAD").to_hex()));
    std::os::unix::fs::symlink(&target, path)?;
    assert!(store.get("db/HEAD").is_err());
    assert!(store.put("db/HEAD", b"bad", PutCondition::Any).is_err());
    assert_eq!(std::fs::read(&target)?, b"untouched");
    let link = outside.path().join("root");
    std::os::unix::fs::symlink(directory.path(), &link)?;
    assert!(LocalObjectStore::open(link).is_err());
    Ok(())
}

#[test]
fn s3_signed_conditional_put_get_delete() -> Result<()> {
    let (store, worker) = mock(|listener| {
        for operation in 0..5 {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_request(&mut stream);
            verify_signature(&request);
            assert_eq!(request.target, "/bucket/tenant/db/a-_.~");
            match operation {
                0 => {
                    assert_eq!(request.method, "PUT");
                    assert_eq!(request.headers["if-none-match"], "*");
                    assert_eq!(request.body, b"payload");
                    reply(&mut stream, 200, &[("ETag", "\"first\"")], b"");
                }
                1 => {
                    assert_eq!(request.method, "GET");
                    reply(
                        &mut stream,
                        200,
                        &[
                            ("ETag", "\"first\""),
                            (
                                "x-amz-meta-peri-sha256",
                                "239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5",
                            ),
                        ],
                        b"payload",
                    );
                }
                2 => {
                    assert_eq!(request.headers["if-match"], "\"first\"");
                    reply(
                        &mut stream,
                        412,
                        &[],
                        b"<Error><Code>PreconditionFailed</Code></Error>",
                    );
                }
                3 => {
                    assert_eq!(request.method, "DELETE");
                    assert_eq!(request.headers["if-match"], "\"first\"");
                    reply(&mut stream, 204, &[], b"");
                }
                _ => reply(
                    &mut stream,
                    404,
                    &[],
                    b"<Error><Code>NoSuchKey</Code></Error>",
                ),
            }
        }
    });
    let key = "tenant/db/a-_.~";
    let etag = store.put(key, b"payload", PutCondition::Absent)?;
    assert_eq!(etag, "\"first\"");
    assert_eq!(store.get(key)?.unwrap().bytes, b"payload");
    assert!(store
        .put(key, b"bad", PutCondition::Match(etag.clone()))
        .unwrap_err()
        .is::<PreconditionFailed>());
    store.delete(key, Some(&etag))?;
    assert!(store.get(key)?.is_none());
    worker.join().unwrap();
    Ok(())
}

#[test]
fn s3_list_paginates_and_escapes_tokens() -> Result<()> {
    let (store, worker) = mock(|listener| {
        for page in 0..2 {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_request(&mut stream);
            verify_signature(&request);
            if page == 0 {
                assert_eq!(
                    request.target,
                    "/bucket?encoding-type=url&list-type=2&prefix=tenant%2Fdb%2F"
                );
                reply(&mut stream, 200, &[], b"<ListBucketResult><EncodingType>url</EncodingType><IsTruncated>true</IsTruncated><Contents><Key>tenant%2Fdb%2Fa</Key></Contents><NextContinuationToken>a+/=&amp;x</NextContinuationToken></ListBucketResult>");
            } else {
                assert_eq!(request.target, "/bucket?continuation-token=a%2B%2F%3D%26x&encoding-type=url&list-type=2&prefix=tenant%2Fdb%2F");
                reply(&mut stream, 200, &[], b"<ListBucketResult><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated><Contents><Key>tenant%2Fdb%2Fb</Key></Contents></ListBucketResult>");
            }
        }
    });
    assert_eq!(store.list("tenant/db/")?, ["tenant/db/a", "tenant/db/b"]);
    worker.join().unwrap();
    Ok(())
}

#[test]
fn s3_retry_and_checksum_failure() -> Result<()> {
    let (store, worker) = mock(|listener| {
        for attempt in 0..2 {
            let (mut stream, _) = listener.accept().unwrap();
            verify_signature(&read_request(&mut stream));
            if attempt == 0 {
                reply(&mut stream, 503, &[], b"busy");
            } else {
                reply(
                    &mut stream,
                    200,
                    &[
                        ("ETag", "\"etag\""),
                        ("x-amz-meta-peri-sha256", "incorrect"),
                    ],
                    b"corrupt",
                );
            }
        }
    });
    assert!(store
        .get("db/HEAD")
        .unwrap_err()
        .to_string()
        .contains("checksum mismatch"));
    worker.join().unwrap();
    Ok(())
}

#[test]
fn s3_timeout_and_secret_redaction() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut settings = config(format!("http://{}", listener.local_addr()?));
    settings.timeout = Duration::from_millis(40);
    settings.retries = 0;
    assert!(!format!("{settings:?}").contains("never-log-this-secret"));
    assert!(!format!("{settings:?}").contains("session-token"));
    let worker = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        thread::sleep(Duration::from_millis(150));
    });
    let error = S3ObjectStore::new(settings)?.get("db/HEAD").unwrap_err();
    assert!(error.to_string().contains("timeout"));
    assert!(!format!("{error:?}").contains("never-log-this-secret"));
    worker.join().unwrap();
    Ok(())
}

/// Accept one mock connection, giving the client `idle` to open it first. Tests use it
/// to bound a server thread by observed traffic instead of guessing a request count.
fn accept_with_timeout(listener: &TcpListener, idle: Duration) -> Option<TcpStream> {
    let deadline = Instant::now() + idle;
    loop {
        match listener.accept() {
            Ok((stream, _)) => return Some(stream),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return None;
                }
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("mock accept failed: {error}"),
        }
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn group_bodies(count: usize) -> Vec<GroupWrite> {
    (0..count)
        .map(|index| {
            GroupWrite::absent(
                format!("group/body/{index}"),
                format!("body-{index}").into_bytes(),
            )
        })
        .collect()
}

fn group_root() -> GroupWrite {
    GroupWrite::new(
        "group/root",
        b"root-body".to_vec(),
        PutCondition::Match("\"old-commit\"".into()),
    )
}

#[test]
fn s3_group_uploads_immutable_bodies_with_bounded_parallelism() -> Result<()> {
    const BODIES: usize = 9;
    const LIMIT: usize = 4;
    let inflight = Arc::new(AtomicUsize::new(0));
    let max_inflight = Arc::new(AtomicUsize::new(0));
    let bodies_acked = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let server = (
        inflight.clone(),
        max_inflight.clone(),
        bodies_acked.clone(),
        seen.clone(),
    );
    let (store, worker) = mock(move |listener| {
        let (inflight, max_inflight, bodies_acked, seen) = server;
        listener.set_nonblocking(true).unwrap();
        let mut handlers = Vec::new();
        while handlers.len() < BODIES + 1 {
            let Some(stream) = accept_with_timeout(&listener, Duration::from_secs(5)) else {
                break;
            };
            let inflight = inflight.clone();
            let max_inflight = max_inflight.clone();
            let bodies_acked = bodies_acked.clone();
            let seen = seen.clone();
            handlers.push(thread::spawn(move || {
                let active = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                max_inflight.fetch_max(active, Ordering::SeqCst);
                let mut stream = stream;
                let request = read_request(&mut stream);
                verify_signature(&request);
                assert_eq!(request.method, "PUT");
                seen.lock().unwrap().push(request.target.clone());
                if request.target == "/bucket/group/root" {
                    assert_eq!(request.headers["if-match"], "\"old-commit\"");
                    assert_eq!(request.body, b"root-body");
                    // Every body was confirmed before the client could send this CAS.
                    assert_eq!(bodies_acked.load(Ordering::SeqCst), BODIES);
                    inflight.fetch_sub(1, Ordering::SeqCst);
                    reply(&mut stream, 200, &[("ETag", "\"root\"")], b"");
                    return;
                }
                let index: usize = request
                    .target
                    .strip_prefix("/bucket/group/body/")
                    .expect("unexpected group target")
                    .parse()
                    .expect("invalid body index");
                let body = format!("body-{index}");
                assert_eq!(request.body, body.as_bytes());
                assert_eq!(request.headers["if-none-match"], "*");
                assert_eq!(
                    request.headers["x-amz-meta-peri-sha256"],
                    sha256_hex(body.as_bytes())
                );
                // Hold this connection until the batch of bodies the client still owes is
                // connected. A serialized upload never gets there, so the peak below is
                // exactly the client's own bound and not a timing artefact. The expected
                // batch follows from the bodies already answered, not from the order in
                // which these handler threads happened to start.
                let expected =
                    LIMIT.min(BODIES.saturating_sub(bodies_acked.load(Ordering::SeqCst)));
                // Strictly below the client's own request timeout, so a serialized
                // implementation fails on the peak assertion instead of on a timeout.
                let deadline = Instant::now() + Duration::from_secs(1);
                while inflight.load(Ordering::SeqCst) < expected && Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(2));
                }
                // Decrement before replying: the next connection of this worker can only
                // start after the reply reaches it, so the peak stays a real bound.
                inflight.fetch_sub(1, Ordering::SeqCst);
                bodies_acked.fetch_add(1, Ordering::SeqCst);
                let etag = format!("\"body-{index}\"");
                reply(&mut stream, 200, &[("ETag", &etag)], b"");
            }));
        }
        for handler in handlers {
            handler.join().unwrap();
        }
        assert_eq!(max_inflight.load(Ordering::SeqCst), LIMIT);
    });
    let mut writes = group_bodies(BODIES);
    writes.push(group_root());
    let etags = store.put_group(&writes)?;
    let expected: Vec<String> = (0..BODIES)
        .map(|index| format!("\"body-{index}\""))
        .chain([String::from("\"root\"")])
        .collect();
    assert_eq!(etags, expected);
    assert!(store.put_group(&[])?.is_empty());
    worker.join().unwrap();
    let mut targets = seen.lock().unwrap().clone();
    targets.sort();
    targets.dedup();
    assert_eq!(targets.len(), BODIES + 1);
    Ok(())
}

#[test]
fn s3_group_dependent_bodies_keep_serial_order() -> Result<()> {
    let observed = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    let server = observed.clone();
    let (store, worker) = mock_with_retries(0, move |listener| {
        let observed = server;
        listener.set_nonblocking(true).unwrap();
        while let Some(mut stream) = accept_with_timeout(&listener, Duration::from_millis(600)) {
            let request = read_request(&mut stream);
            verify_signature(&request);
            assert_eq!(request.method, "PUT");
            observed.lock().unwrap().push((
                request.target.clone(),
                String::from_utf8_lossy(&request.body).into_owned(),
            ));
            let etag = format!("\"{}\"", String::from_utf8_lossy(&request.body));
            match request.target.as_str() {
                "/bucket/group/dup" => {
                    assert_eq!(request.headers["if-none-match"], "*");
                    reply(&mut stream, 200, &[("ETag", &etag)], b"");
                }
                "/bucket/group/cas" => {
                    assert_eq!(request.headers["if-match"], "\"v0\"");
                    reply(
                        &mut stream,
                        412,
                        &[],
                        b"<Error><Code>PreconditionFailed</Code></Error>",
                    );
                }
                "/bucket/group/root" => {
                    assert_eq!(request.headers["if-match"], "\"old\"");
                    reply(&mut stream, 200, &[("ETag", "\"root\"")], b"");
                }
                other => panic!("unexpected group target {other}"),
            }
        }
    });
    // A repeated key is order-sensitive even when both writes are immutable bodies: the
    // adapter must not reorder it, and the etags stay aligned with `writes`.
    let writes = [
        GroupWrite::absent("group/dup", b"first".to_vec()),
        GroupWrite::absent("group/dup", b"second".to_vec()),
        GroupWrite::new(
            "group/root",
            b"root-body".to_vec(),
            PutCondition::Match("\"old\"".into()),
        ),
    ];
    let etags = store.put_group(&writes)?;
    assert_eq!(etags, vec!["\"first\"", "\"second\"", "\"root\""]);
    // A conditional body is not independent: the serial path stops at it, so the later
    // body and the visible write are never attempted.
    let writes = [
        GroupWrite::new(
            "group/cas",
            b"cas-body".to_vec(),
            PutCondition::Match("\"v0\"".into()),
        ),
        GroupWrite::absent("group/body/0", b"body-0".to_vec()),
        GroupWrite::new(
            "group/root",
            b"root-body".to_vec(),
            PutCondition::Match("\"old\"".into()),
        ),
    ];
    let error = store.put_group(&writes).unwrap_err();
    assert!(error.is::<PreconditionFailed>(), "{error}");
    worker.join().unwrap();
    let observed = observed.lock().unwrap().clone();
    let targets: Vec<&str> = observed.iter().map(|(target, _)| target.as_str()).collect();
    assert_eq!(
        targets,
        [
            "/bucket/group/dup",
            "/bucket/group/dup",
            "/bucket/group/root",
            "/bucket/group/cas",
        ],
        "{observed:?}"
    );
    let bodies: Vec<&str> = observed.iter().map(|(_, body)| body.as_str()).collect();
    assert_eq!(
        bodies,
        ["first", "second", "root-body", "cas-body"],
        "{observed:?}"
    );
    Ok(())
}

#[test]
fn s3_group_root_condition_conflict_is_typed_and_not_atomic() -> Result<()> {
    const BODIES: usize = 3;
    let observed = Arc::new(Mutex::new(Vec::<String>::new()));
    let server = observed.clone();
    let (store, worker) = mock_with_retries(0, move |listener| {
        let observed = server;
        listener.set_nonblocking(true).unwrap();
        // Driven by the expected requests, not by an idle window: the mock answers
        // `Connection: close`, so the client opens a fresh connection per request, and between the
        // last body and the root CAS it also joins three upload threads. An idle window would have
        // to guess how long that takes, and under parallel suites the guess is what fails — the
        // requests this case counts are then never sent, and the failure surfaces as a transport
        // error instead of as a missing request. A shortfall still fails the assertions below.
        while observed.lock().unwrap().len() < BODIES + 1 {
            let Some(mut stream) = accept_with_timeout(&listener, Duration::from_secs(5)) else {
                break;
            };
            let request = read_request(&mut stream);
            verify_signature(&request);
            assert_eq!(request.method, "PUT");
            observed.lock().unwrap().push(request.target.clone());
            if request.target == "/bucket/group/root" {
                assert_eq!(request.headers["if-match"], "\"old-commit\"");
                reply(
                    &mut stream,
                    412,
                    &[],
                    b"<Error><Code>PreconditionFailed</Code></Error>",
                );
            } else {
                assert!(request.target.starts_with("/bucket/group/body/"));
                reply(&mut stream, 200, &[("ETag", "\"body\"")], b"");
            }
        }
        // The refused CAS ends the group; anything after it is the anomaly this case exists to
        // notice, so it is reported as one rather than as a client-side transport failure.
        assert!(
            accept_with_timeout(&listener, Duration::from_millis(600)).is_none(),
            "a request arrived after the refused CAS"
        );
    });
    // The adapter never claims group atomicity on S3, and a caller must be able to keep
    // relying on that declaration.
    assert!(!store.group_rejection_publishes_nothing());
    let mut writes = group_bodies(BODIES);
    writes.push(group_root());
    let error = store.put_group(&writes).unwrap_err();
    assert!(error.is::<PreconditionFailed>(), "{error}");
    worker.join().unwrap();
    let observed = observed.lock().unwrap().clone();
    // Every body was attempted exactly once before the CAS was refused: a rejected group
    // leaves the refused CAS unpublished, not untouched bodies.
    assert_eq!(observed.len(), BODIES + 1, "{observed:?}");
    assert_eq!(
        observed.last().map(String::as_str),
        Some("/bucket/group/root")
    );
    Ok(())
}

#[test]
fn s3_group_single_write_is_one_conditional_put() -> Result<()> {
    let requests = Arc::new(AtomicUsize::new(0));
    let server = requests.clone();
    let (store, worker) = mock_with_retries(0, move |listener| {
        let requests = server;
        listener.set_nonblocking(true).unwrap();
        while let Some(mut stream) = accept_with_timeout(&listener, Duration::from_millis(600)) {
            let request = read_request(&mut stream);
            verify_signature(&request);
            requests.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.target, "/bucket/group/root");
            assert_eq!(request.headers["if-match"], "\"old-commit\"");
            assert_eq!(request.body, b"root-body");
            reply(&mut stream, 200, &[("ETag", "\"root\"")], b"");
        }
    });
    let etags = store.put_group(&[group_root()])?;
    assert_eq!(etags, vec!["\"root\""]);
    worker.join().unwrap();
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn s3_group_body_failure_never_publishes_root() -> Result<()> {
    const BODIES: usize = 3;
    let attempts = Arc::new(Mutex::new(Vec::<String>::new()));
    let roots = Arc::new(AtomicUsize::new(0));
    let server = (attempts.clone(), roots.clone());
    // No retries: a repeated key would be visible as an extra connection below.
    let (store, worker) = mock_with_retries(0, move |listener| {
        let (attempts, roots) = server;
        listener.set_nonblocking(true).unwrap();
        while let Some(mut stream) = accept_with_timeout(&listener, Duration::from_millis(600)) {
            let request = read_request(&mut stream);
            verify_signature(&request);
            assert_eq!(request.method, "PUT");
            if request.target == "/bucket/group/root" {
                // The mock accepts a root publish; only the client's own fail-closed
                // behaviour keeps an unconfirmed body out of the group.
                roots.fetch_add(1, Ordering::SeqCst);
                reply(&mut stream, 200, &[("ETag", "\"root\"")], b"");
                continue;
            }
            assert!(request.target.starts_with("/bucket/group/body/"));
            assert_eq!(request.headers["if-none-match"], "*");
            let first = {
                let mut attempts = attempts.lock().unwrap();
                let first = attempts.is_empty();
                attempts.push(request.target.clone());
                first
            };
            if first {
                reply(
                    &mut stream,
                    500,
                    &[],
                    b"<Error><Code>InternalError</Code></Error>",
                );
            } else {
                reply(&mut stream, 200, &[("ETag", "\"body\"")], b"");
            }
        }
        assert_eq!(roots.load(Ordering::SeqCst), 0);
    });
    let mut writes = group_bodies(BODIES);
    writes.push(group_root());
    let error = store.put_group(&writes).unwrap_err();
    assert!(error.to_string().contains("500"), "{error}");
    let recorded: Vec<String> = attempts.lock().unwrap().clone();
    // `retries == 0` and one write per key: more attempts would mean the group issued a
    // write the configured policy never allowed. Fewer is the intended early stop.
    assert!(!recorded.is_empty());
    assert!(recorded.len() <= BODIES, "{recorded:?}");
    let mut distinct = recorded.clone();
    distinct.sort();
    distinct.dedup();
    assert_eq!(distinct.len(), recorded.len(), "{recorded:?}");
    worker.join().unwrap();
    Ok(())
}

#[test]
fn s3_group_unknown_body_response_fails_closed() -> Result<()> {
    const BODIES: usize = 3;
    let attempts = Arc::new(Mutex::new(Vec::<String>::new()));
    let roots = Arc::new(AtomicUsize::new(0));
    let server = (attempts.clone(), roots.clone());
    let (store, worker) = mock_with_retries(0, move |listener| {
        let (attempts, roots) = server;
        listener.set_nonblocking(true).unwrap();
        let mut truncated = false;
        while let Some(mut stream) = accept_with_timeout(&listener, Duration::from_millis(600)) {
            let request = read_request(&mut stream);
            verify_signature(&request);
            assert_eq!(request.method, "PUT");
            if request.target == "/bucket/group/root" {
                roots.fetch_add(1, Ordering::SeqCst);
                reply(&mut stream, 200, &[("ETag", "\"root\"")], b"");
                continue;
            }
            attempts.lock().unwrap().push(request.target.clone());
            if !truncated {
                truncated = true;
                // Partial reply: the headers promise 64 bytes and the socket delivers 7
                // before closing, so the body read is an Unknown outcome, not a success.
                write!(
                    stream,
                    "HTTP/1.1 200 Mock\r\nContent-Length: 64\r\nConnection: close\r\n\r\npartial"
                )
                .unwrap();
                drop(stream);
                continue;
            }
            reply(&mut stream, 200, &[("ETag", "\"body\"")], b"");
        }
        assert_eq!(roots.load(Ordering::SeqCst), 0);
    });
    let mut writes = group_bodies(BODIES);
    writes.push(group_root());
    let error = store.put_group(&writes).unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("network") || message.contains("timeout"),
        "{message}"
    );
    let recorded: Vec<String> = attempts.lock().unwrap().clone();
    assert!(!recorded.is_empty());
    assert!(recorded.len() <= BODIES, "{recorded:?}");
    let mut distinct = recorded.clone();
    distinct.sort();
    distinct.dedup();
    assert_eq!(distinct.len(), recorded.len(), "{recorded:?}");
    worker.join().unwrap();
    Ok(())
}

// Prepared, not yet executed: these two pin the fallback predicate for inputs the
// executed tests do not cover (a conditional body that is not the first write, and a
// body sharing the CAS key). They are added only after the next build slot runs them.
#[test]
fn s3_group_conditional_body_keeps_serial_order_from_its_position() -> Result<()> {
    let observed = Arc::new(Mutex::new(Vec::<String>::new()));
    let server = observed.clone();
    let (store, worker) = mock_with_retries(0, move |listener| {
        let observed = server;
        listener.set_nonblocking(true).unwrap();
        while let Some(mut stream) = accept_with_timeout(&listener, Duration::from_millis(600)) {
            let request = read_request(&mut stream);
            verify_signature(&request);
            assert_eq!(request.method, "PUT");
            observed.lock().unwrap().push(request.target.clone());
            match request.target.as_str() {
                "/bucket/group/body/0" => {
                    assert_eq!(request.headers["if-none-match"], "*");
                    reply(&mut stream, 200, &[("ETag", "\"body-0\"")], b"");
                }
                "/bucket/group/cas" => {
                    assert_eq!(request.headers["if-match"], "\"v0\"");
                    reply(
                        &mut stream,
                        412,
                        &[],
                        b"<Error><Code>PreconditionFailed</Code></Error>",
                    );
                }
                other => panic!("write after the conditional body was attempted: {other}"),
            }
        }
    });
    let writes = [
        GroupWrite::absent("group/body/0", b"body-0".to_vec()),
        GroupWrite::new(
            "group/cas",
            b"cas-body".to_vec(),
            PutCondition::Match("\"v0\"".into()),
        ),
        GroupWrite::absent("group/body/1", b"body-1".to_vec()),
        GroupWrite::new(
            "group/root",
            b"root-body".to_vec(),
            PutCondition::Match("\"old\"".into()),
        ),
    ];
    let error = store.put_group(&writes).unwrap_err();
    assert!(error.is::<PreconditionFailed>(), "{error}");
    worker.join().unwrap();
    let observed = observed.lock().unwrap().clone();
    assert_eq!(
        observed,
        vec!["/bucket/group/body/0", "/bucket/group/cas"],
        "{observed:?}"
    );
    Ok(())
}

#[test]
fn s3_group_body_sharing_the_cas_key_keeps_serial_order() -> Result<()> {
    let observed = Arc::new(Mutex::new(Vec::<String>::new()));
    let server = observed.clone();
    let (store, worker) = mock_with_retries(0, move |listener| {
        let observed = server;
        listener.set_nonblocking(true).unwrap();
        while let Some(mut stream) = accept_with_timeout(&listener, Duration::from_millis(600)) {
            let request = read_request(&mut stream);
            verify_signature(&request);
            assert_eq!(request.method, "PUT");
            assert_eq!(request.target, "/bucket/group/shared");
            let body = String::from_utf8_lossy(&request.body).into_owned();
            observed.lock().unwrap().push(body.clone());
            match body.as_str() {
                "body" => {
                    assert_eq!(request.headers["if-none-match"], "*");
                    reply(&mut stream, 200, &[("ETag", "\"first\"")], b"");
                }
                "root-body" => {
                    assert_eq!(request.headers["if-match"], "\"old\"");
                    reply(&mut stream, 200, &[("ETag", "\"second\"")], b"");
                }
                other => panic!("unexpected group body {other}"),
            }
        }
    });
    let writes = [
        GroupWrite::absent("group/shared", b"body".to_vec()),
        GroupWrite::new(
            "group/shared",
            b"root-body".to_vec(),
            PutCondition::Match("\"old\"".into()),
        ),
    ];
    let etags = store.put_group(&writes)?;
    assert_eq!(etags, vec!["\"first\"", "\"second\""]);
    worker.join().unwrap();
    let observed = observed.lock().unwrap().clone();
    assert_eq!(observed, vec!["body", "root-body"], "{observed:?}");
    Ok(())
}

#[test]
fn s3_group_parallel_body_failures_report_every_key() -> Result<()> {
    const BODIES: usize = 6;
    // One body per worker: the batch the client has in flight before it can observe a
    // reply, and therefore the failures its early stop cannot avoid collecting.
    const BATCH: usize = 4;
    let failed_keys = Arc::new(Mutex::new(Vec::<String>::new()));
    let roots = Arc::new(AtomicUsize::new(0));
    let server = (failed_keys.clone(), roots.clone());
    // No retries: a re-attempt would land in the tail loop below, be answered 200, and
    // turn a body recorded here into a success. Each body must fail exactly once for the
    // keys below to be the failures the error has to name.
    let (store, worker) = mock_with_retries(0, move |listener| {
        let (failed_keys, roots) = server;
        listener.set_nonblocking(true).unwrap();
        // Hold the first batch unanswered so that no body can fail — and stop the group —
        // before the whole batch is in flight. One deadline for the batch, well below the
        // client's own 2 s request timeout, so an implementation that serializes its
        // bodies fails on the count below instead of on a request timeout.
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut held = Vec::new();
        while held.len() < BATCH {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let Some(mut stream) = accept_with_timeout(&listener, remaining) else {
                break;
            };
            let request = read_request(&mut stream);
            verify_signature(&request);
            assert_eq!(request.method, "PUT");
            assert!(
                request.target.starts_with("/bucket/group/body/"),
                "group write issued before the bodies: {}",
                request.target
            );
            held.push((stream, request.target));
        }
        assert_eq!(
            held.len(),
            BATCH,
            "client did not keep a full batch in flight"
        );
        for (mut stream, target) in held {
            failed_keys.lock().unwrap().push(target);
            // Every body in the batch fails, so the group has several failures to report
            // and none of them may be dropped from the error it returns.
            reply(
                &mut stream,
                500,
                &[],
                b"<Error><Code>InternalError</Code></Error>",
            );
        }
        // Anything arriving now is a write the failed group must not have issued.
        while let Some(mut stream) = accept_with_timeout(&listener, Duration::from_millis(600)) {
            let request = read_request(&mut stream);
            verify_signature(&request);
            if request.target == "/bucket/group/root" {
                roots.fetch_add(1, Ordering::SeqCst);
            } else {
                failed_keys.lock().unwrap().push(request.target.clone());
            }
            reply(&mut stream, 200, &[("ETag", "\"late\"")], b"");
        }
        assert_eq!(roots.load(Ordering::SeqCst), 0);
    });
    let mut writes = group_bodies(BODIES);
    writes.push(group_root());
    let error = store.put_group(&writes).unwrap_err();
    worker.join().unwrap();

    // Fail-closed: the visible write of the group was never attempted, and the error does
    // not claim otherwise.
    assert_eq!(roots.load(Ordering::SeqCst), 0);
    // Early stop kept the group to the batch already dispatched: bodies beyond it were
    // never handed out, so they cannot appear below either.
    let failed = failed_keys.lock().unwrap().clone();
    assert_eq!(failed.len(), BATCH, "{failed:?}");
    // Every failed body is named in the error chain...
    let chain: Vec<String> = error.chain().map(|cause| cause.to_string()).collect();
    let reported = chain.join(" | ");
    for target in &failed {
        let key = target.strip_prefix("/bucket/").expect("body target");
        assert!(reported.contains(key), "{key} missing from {chain:?}");
    }
    // ...while the first failure is still the error the backend produced, not a wrapper
    // around several of them.
    assert!(reported.contains("500"), "{chain:?}");
    assert!(!reported.contains("group/root"), "{chain:?}");
    Ok(())
}
