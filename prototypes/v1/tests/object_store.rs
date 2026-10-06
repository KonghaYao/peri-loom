use anyhow::Result;
use peri_loom_v1::object_store::{
    LocalObjectStore, MemoryObjectStore, ObjectStore, PreconditionFailed, PutCondition,
    S3ObjectStore,
};
use std::{
    net::TcpListener,
    process::Command,
    sync::{Arc, Barrier},
    thread,
    time::Duration,
};

#[path = "../src/object_store/http_mock.rs"]
mod http_mock;
use http_mock::{config, mock, read_request, reply, verify_signature};

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
