#[path = "../src/object_store/signing.rs"]
#[allow(dead_code)]
mod signing;

#[path = "../src/object_store/http_mock.rs"]
#[allow(dead_code)]
mod http_mock;

#[path = "minio/p3.rs"]
mod p3;

#[path = "minio/p4.rs"]
mod p4;

use anyhow::{anyhow, ensure, Result};
use peri_loom_v1::{
    database::RemoteDatabase,
    namespace::Namespace,
    object_store::{ObjectStore, PreconditionFailed, PutCondition, S3Config, S3ObjectStore},
};
use reqwest::{blocking::Client, Method, StatusCode, Url};
use std::{
    collections::BTreeMap,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn enabled() -> bool {
    std::env::var("PERI_MINIO_TEST").as_deref() == Ok("1")
        || std::env::var("PERI_S3_TEST").as_deref() == Ok("1")
}

fn optional_env(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => {
            ensure!(!value.is_empty(), "{name} must not be empty");
            Ok(Some(value))
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => Err(anyhow!("{name} must be valid Unicode")),
    }
}

fn config(bucket: String) -> Result<S3Config> {
    let endpoint = optional_env("PERI_S3_TEST_ENDPOINT")?;
    let access_key = optional_env("PERI_S3_TEST_ACCESS_KEY")?;
    let secret_key = optional_env("PERI_S3_TEST_SECRET_KEY")?;
    let (endpoint, access_key, secret_key) = match (endpoint, access_key, secret_key) {
        (None, None, None) => (
            "http://127.0.0.1:19000".into(),
            "peri-test-access".into(),
            "peri-test-secret-isolated-only".into(),
        ),
        (Some(endpoint), Some(access_key), Some(secret_key)) => (endpoint, access_key, secret_key),
        _ => return Err(anyhow!("PERI_S3_TEST_ENDPOINT, PERI_S3_TEST_ACCESS_KEY and PERI_S3_TEST_SECRET_KEY must be supplied together")),
    };
    Ok(S3Config {
        endpoint,
        region: "us-east-1".into(),
        bucket,
        access_key,
        secret_key,
        session_token: None,
        timeout: Duration::from_secs(10),
        retries: 2,
    })
}

fn raw_status(
    config: &S3Config,
    method: Method,
    key: Option<&str>,
    bytes: &[u8],
    mut headers: BTreeMap<String, String>,
) -> Result<StatusCode> {
    let mut destination =
        Url::parse(&config.endpoint).map_err(|_| anyhow!("invalid test S3 endpoint"))?;
    ensure!(
        matches!(destination.scheme(), "http" | "https")
            && destination.host_str().is_some()
            && destination.username().is_empty()
            && destination.password().is_none()
            && destination.query().is_none()
            && destination.fragment().is_none(),
        "invalid test S3 endpoint"
    );
    {
        let mut segments = destination
            .path_segments_mut()
            .map_err(|_| anyhow!("invalid test S3 URL path"))?;
        segments.pop_if_empty().push(&config.bucket);
        if let Some(key) = key {
            for component in key.split('/') {
                segments.push(component);
            }
        }
    }
    let path = destination.path().to_owned();
    let origin = destination.origin().ascii_serialization();
    let host = origin
        .split_once("://")
        .ok_or_else(|| anyhow!("invalid test S3 origin"))?
        .1;
    headers.insert("host".into(), host.into());
    headers.insert(
        "x-amz-date".into(),
        chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string(),
    );
    headers.insert("x-amz-content-sha256".into(), signing::sha256(bytes));
    if let Some(token) = &config.session_token {
        headers.insert("x-amz-security-token".into(), token.clone());
    }
    let authorization = signing::authorization(
        method.as_str(),
        &path,
        "",
        &headers,
        signing::Credentials {
            access_key: &config.access_key,
            secret_key: &config.secret_key,
            region: &config.region,
        },
    );
    let mut request = Client::builder()
        .no_proxy()
        .timeout(config.timeout)
        .build()
        .map_err(|_| anyhow!("test S3 client initialization failed"))?
        .request(method, destination)
        .body(bytes.to_vec())
        .header("authorization", authorization);
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let response = request
        .send()
        .map_err(|_| anyhow!("test S3 setup transport failed"))?;
    Ok(response.status())
}

fn raw(
    config: &S3Config,
    method: Method,
    key: Option<&str>,
    bytes: &[u8],
    headers: BTreeMap<String, String>,
) -> Result<()> {
    let status = raw_status(config, method, key, bytes, headers)?;
    ensure!(
        status.is_success(),
        "isolated S3 test setup request failed: HTTP {}",
        status.as_u16()
    );
    Ok(())
}

fn ensure_fresh_bucket(config: &S3Config) -> Result<()> {
    let status = raw_status(config, Method::HEAD, None, b"", BTreeMap::new())?;
    ensure!(
        status == StatusCode::NOT_FOUND,
        "test bucket is not confirmed absent; refusing create or cleanup (HTTP {})",
        status.as_u16()
    );
    Ok(())
}

struct TestBucket {
    config: S3Config,
    store: Arc<S3ObjectStore>,
}

#[test]
#[ignore = "requires explicit PERI_S3_TEST=1 and S3 test credentials"]
fn p2_s3_authority_refs_pitr_and_gc_with_empty_worker_cache() -> Result<()> {
    use peri_loom_v1::{authority::Authority, capabilities::Capabilities, gc::GarbageCollector};
    ensure!(enabled(), "real S3 tests require an explicit opt-in");
    let fixture = TestBucket::new()?;
    let namespace = Namespace::new("tenant", "capabilities")?;
    let authority = Authority::open(fixture.store.clone(), namespace.clone())?;
    let capabilities = Capabilities::new(authority.clone());
    let cache = tempfile::tempdir()?;
    let database = RemoteDatabase::open(
        authority.branch_store("main")?,
        namespace.clone(),
        cache.path(),
        0,
    )?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    let point = capabilities.snapshot("main", "before")?;
    assert!(point.sequence > 0);
    database.execute("INSERT INTO items VALUES(2)")?;
    database.checkpoint()?;
    drop(database);
    std::fs::remove_dir_all(cache.path())?;
    capabilities.clone("before", "copy")?;
    let copied_cache = tempfile::tempdir()?;
    let copied = RemoteDatabase::open(
        authority.branch_store("copy")?,
        namespace.clone(),
        copied_cache.path(),
        0,
    )?;
    assert_eq!(copied.query("SELECT id FROM items")?.len(), 1);
    copied.execute("INSERT INTO items VALUES(3)")?;
    drop(copied);
    std::fs::remove_dir_all(copied_cache.path())?;
    let retained = capabilities.pitr("main", point.sequence)?;
    capabilities.restore_point("main", &retained)?;
    capabilities.delete_branch("copy")?;
    capabilities.delete_snapshot("before")?;
    capabilities.retain_after("main", point.sequence)?;
    GarbageCollector::new(authority.clone(), Duration::ZERO).collect()?;
    drop(capabilities);
    drop(authority);
    let restarted = Authority::open(fixture.store.clone(), namespace.clone())?;
    let final_cache = tempfile::tempdir()?;
    let restored = RemoteDatabase::open(
        restarted.branch_store("main")?,
        namespace,
        final_cache.path(),
        0,
    )?;
    assert_eq!(restored.query("SELECT id FROM items")?.len(), 1);
    assert_eq!(
        restored.query("PRAGMA integrity_check")?[0][0].to_string(),
        "ok"
    );
    eprintln!("PASS real S3 P2: root CAS, snapshot clone COW, native commit PITR, ref deletion and GC, authority restart and fresh-cache recovery");
    Ok(())
}

impl TestBucket {
    fn new() -> Result<Self> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let temporary = tempfile::tempdir()?;
        let salt = blake3::hash(temporary.path().as_os_str().as_encoded_bytes()).to_hex();
        let config = config(format!(
            "peri-p1-test-{}-{nonce:x}-{}",
            std::process::id(),
            &salt[..12]
        ))?;
        let store = Arc::new(S3ObjectStore::new(config.clone())?);
        ensure_fresh_bucket(&config)?;
        raw(&config, Method::PUT, None, b"", BTreeMap::new())?;
        Ok(Self { config, store })
    }
}

impl Drop for TestBucket {
    fn drop(&mut self) {
        if let Ok(keys) = self.store.list("tenant/") {
            for key in keys {
                let _ = self.store.delete(&key, None);
            }
        }
        let _ = raw(&self.config, Method::DELETE, None, b"", BTreeMap::new());
    }
}

#[test]
#[ignore = "requires PERI_S3_TEST=1 or PERI_MINIO_TEST=1 and explicit test S3 configuration"]
fn minio_network_object_store_conformance() -> Result<()> {
    ensure!(
        enabled(),
        "real S3 tests require explicit PERI_S3_TEST=1 or PERI_MINIO_TEST=1"
    );
    let fixture = TestBucket::new()?;
    let store = fixture.store.clone();
    let key = "tenant/db/HEAD";
    assert!(store.get(key)?.is_none());
    let initial = store.put(key, b"initial-generation-0", PutCondition::Absent)?;
    assert!(store
        .put(key, b"conflict", PutCondition::Absent)
        .unwrap_err()
        .is::<PreconditionFailed>());
    let barrier = Arc::new(Barrier::new(16));
    let workers: Vec<_> = (0..16)
        .map(|generation| {
            let store = store.clone();
            let initial = initial.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                let bytes = format!("generation-{generation}");
                match store.put(key, bytes.as_bytes(), PutCondition::Match(initial)) {
                    Ok(etag) => Some((etag, bytes)),
                    Err(error) => {
                        assert!(error.is::<PreconditionFailed>(), "{error:#}");
                        None
                    }
                }
            })
        })
        .collect();
    let winners: Vec<_> = workers
        .into_iter()
        .filter_map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(winners.len(), 1);
    let current = store.get(key)?.unwrap();
    assert_eq!(current.etag, winners[0].0);
    assert_eq!(current.bytes, winners[0].1.as_bytes());
    assert!(store
        .delete(key, Some(&initial))
        .unwrap_err()
        .is::<PreconditionFailed>());
    assert_eq!(store.get(key)?.unwrap().etag, current.etag);
    store.delete(key, Some(&current.etag))?;
    assert!(store.get(key)?.is_none());
    let writers: Vec<_> = (0..8)
        .map(|shard| {
            let store = store.clone();
            thread::spawn(move || -> Result<()> {
                for index in (shard..1005).step_by(8) {
                    store.put(
                        &format!("tenant/db/pages/{index:04}"),
                        b"page",
                        PutCondition::Absent,
                    )?;
                }
                Ok(())
            })
        })
        .collect();
    for writer in writers {
        writer.join().unwrap()?;
    }
    store.put("tenant/other/pages/0000", b"isolated", PutCondition::Any)?;
    let keys = store.list("tenant/db/pages/")?;
    assert_eq!(keys.len(), 1005);
    assert_eq!(keys.first().unwrap(), "tenant/db/pages/0000");
    assert_eq!(keys.last().unwrap(), "tenant/db/pages/1004");
    assert_eq!(store.get(&keys[500])?.unwrap().bytes, b"page");
    raw(
        &fixture.config,
        Method::PUT,
        Some("tenant/db/corrupt"),
        b"corrupt",
        BTreeMap::from([(
            "x-amz-meta-peri-sha256".into(),
            signing::sha256(b"expected"),
        )]),
    )?;
    assert!(store
        .get("tenant/db/corrupt")
        .unwrap_err()
        .to_string()
        .contains("checksum mismatch"));
    assert!(store
        .put("tenant/db/../other", b"bad", PutCondition::Any)
        .is_err());
    eprintln!("PASS real S3: signed requests, 16-way conditional CAS, conditional delete, 1005-key pagination, checksum and namespace isolation");
    Ok(())
}

#[test]
#[ignore = "requires PERI_S3_TEST=1 or PERI_MINIO_TEST=1 and explicit test S3 configuration"]
fn minio_network_fresh_cache_sql_recovery() -> Result<()> {
    ensure!(
        enabled(),
        "real S3 tests require explicit PERI_S3_TEST=1 or PERI_MINIO_TEST=1"
    );
    let fixture = TestBucket::new()?;
    let namespace = Namespace::new("tenant", "database")?;
    let cache = tempfile::tempdir()?;
    let database = RemoteDatabase::open(fixture.store.clone(), namespace.clone(), cache.path(), 0)?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")?;
    database.checkpoint()?;
    database.execute("BEGIN; INSERT INTO items VALUES(1, 'remote'); INSERT INTO items VALUES(2, 'wal-tail'); COMMIT;")?;
    assert!(database.io().wal_lsn() > database.storage().manifest().applied_lsn);
    drop(database);
    cache.close()?;
    let fresh = tempfile::tempdir()?;
    let database = RemoteDatabase::open(fixture.store.clone(), namespace.clone(), fresh.path(), 0)?;
    let rows = database.query("SELECT count(*) FROM items")?;
    assert!(matches!(
        rows[0][0],
        turso_core::Value::Numeric(turso_core::Numeric::Integer(2))
    ));
    assert!(!fresh.path().join("database").exists());
    assert!(!fresh.path().join("database-wal").exists());
    database.checkpoint()?;
    drop(database);
    fresh.close()?;
    let third = tempfile::tempdir()?;
    let database = RemoteDatabase::open(fixture.store.clone(), namespace, third.path(), 0)?;
    let rows = database.query("SELECT count(*) FROM items")?;
    assert!(matches!(
        rows[0][0],
        turso_core::Value::Numeric(turso_core::Numeric::Integer(2))
    ));
    let integrity = database.query("PRAGMA integrity_check")?;
    assert!(matches!(&integrity[0][0], turso_core::Value::Text(text) if text.as_str() == "ok"));
    eprintln!("PASS real S3 SQL: delete all local cache, replay remote WAL, query, checkpoint, delete cache again and integrity_check");
    Ok(())
}

#[test]
fn setup_signs_destination_host_and_encoded_url_path() -> Result<()> {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let mut settings = http_mock::config(format!("http://{address}/base/"));
    settings.retries = 0;
    let worker = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request = http_mock::read_request(&mut stream);
        assert_eq!(request.headers["host"], address.to_string());
        assert_eq!(request.target, "/base/bucket/tenant/db/file%20name");
        http_mock::verify_signature(&request);
        http_mock::reply(&mut stream, 200, &[], b"");
    });
    raw(
        &settings,
        Method::PUT,
        Some("tenant/db/file name"),
        b"payload",
        BTreeMap::new(),
    )?;
    worker.join().unwrap();
    Ok(())
}

#[test]
fn existing_bucket_preflight_refuses_mutation() -> Result<()> {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let settings = http_mock::config(format!("http://{}", listener.local_addr()?));
    let worker = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request = http_mock::read_request(&mut stream);
        assert_eq!(request.method, "HEAD");
        http_mock::verify_signature(&request);
        http_mock::reply(&mut stream, 200, &[], b"");
    });
    assert!(ensure_fresh_bucket(&settings).is_err());
    worker.join().unwrap();
    Ok(())
}
