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
    fs,
    io::Write,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
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

fn retain_bucket_setting(value: Option<&str>) -> Result<bool> {
    ensure!(
        matches!(value, None | Some("0") | Some("1")),
        "PERI_S3_TEST_RETAIN must be 0 or 1"
    );
    Ok(value == Some("1"))
}

fn announce_retained_bucket(retain: bool, bucket: &str, output: &mut impl Write) -> Result<()> {
    if retain {
        writeln!(output, "retained isolated S3 test bucket: {bucket}")?;
    }
    Ok(())
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
    retain: bool,
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
        let retain = retain_bucket_setting(optional_env("PERI_S3_TEST_RETAIN")?.as_deref())?;
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let temporary = tempfile::tempdir()?;
        let salt = blake3::hash(temporary.path().as_os_str().as_encoded_bytes()).to_hex();
        let config = config(format!(
            "peri-p1-test-{}-{nonce:x}-{}",
            std::process::id(),
            &salt[..12]
        ))?;
        let store = Arc::new(S3ObjectStore::new(config.clone())?);
        announce_retained_bucket(retain, &config.bucket, &mut std::io::stderr().lock())?;
        ensure_fresh_bucket(&config)?;
        raw(&config, Method::PUT, None, b"", BTreeMap::new())?;
        Ok(Self {
            config,
            store,
            retain,
        })
    }
}

impl Drop for TestBucket {
    fn drop(&mut self) {
        if self.retain {
            return;
        }
        if let Ok(keys) = self.store.list("tenant/") {
            for key in keys {
                let _ = self.store.delete(&key, None);
            }
        }
        let _ = raw(&self.config, Method::DELETE, None, b"", BTreeMap::new());
    }
}

#[test]
fn retain_bucket_requires_explicit_opt_in() {
    assert!(!retain_bucket_setting(None).unwrap());
    assert!(!retain_bucket_setting(Some("0")).unwrap());
    assert!(retain_bucket_setting(Some("1")).unwrap());
    assert!(retain_bucket_setting(Some("")).is_err());
    assert!(retain_bucket_setting(Some("true")).is_err());
}

#[test]
fn retained_bucket_notice_only_contains_generated_identifier() -> Result<()> {
    let mut output = Vec::new();
    announce_retained_bucket(false, "peri-p1-test-123", &mut output)?;
    assert!(output.is_empty());
    announce_retained_bucket(true, "peri-p1-test-123", &mut output)?;
    assert_eq!(
        output,
        b"retained isolated S3 test bucket: peri-p1-test-123\n"
    );
    Ok(())
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

fn private_microbench_dir(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_dir()
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.permissions().mode() & 0o7777 == 0o700,
        "PERI_LOCAL_BENCH_EVIDENCE_DIR must be an existing private 0700 directory"
    );
    Ok(())
}

/// Invariant context every microbenchmark row shares. Bundled so the recording
/// call carries only the fields that differ between rows.
struct Microbench<'attention, Output> {
    started: Instant,
    evidence: &'attention mut Output,
    bucket: &'attention str,
    s3_retries: usize,
}

impl<'attention, Output> Microbench<'attention, Output> {
    fn new(
        started: Instant,
        evidence: &'attention mut Output,
        bucket: &'attention str,
        s3_retries: usize,
    ) -> Self {
        Self {
            started,
            evidence,
            bucket,
            s3_retries,
        }
    }
}

fn microbench_record<T, Output: Write>(
    meta: Microbench<'_, Output>,
    operation: &str,
    index: usize,
    expected: serde_json::Value,
    action: impl FnOnce() -> Result<(T, serde_json::Value)>,
) -> Result<T> {
    let Microbench {
        started,
        evidence,
        bucket,
        s3_retries,
    } = meta;
    ensure!(
        started.elapsed() < Duration::from_secs(90),
        "microbenchmark exceeded 90s"
    );
    let operation_start = Instant::now();
    let result = action();
    let latency_ns = operation_start.elapsed().as_nanos();
    let within_budget = started.elapsed() < Duration::from_secs(90);
    let (correct, observed) = match &result {
        Ok((_, observed)) => (within_budget, observed.clone()),
        Err(_) => (false, serde_json::Value::Null),
    };
    serde_json::to_writer(
        &mut *evidence,
        &serde_json::json!({
            "bucket": bucket,
            "s3_retries": s3_retries,
            "namespace": "tenant/local-s3-sql-microbenchmark",
            "operation": operation,
            "index": index,
            "latency_ns": latency_ns,
            "expected": expected,
            "observed": observed,
            "correct": correct
        }),
    )?;
    writeln!(evidence)?;
    evidence.flush()?;
    ensure!(within_budget, "microbenchmark exceeded 90s");
    Ok(result?.0)
}

fn microbench_integer(
    rows: Vec<Vec<turso_core::Value>>,
    expected: i64,
) -> Result<serde_json::Value> {
    ensure!(
        matches!(rows.as_slice(), [row] if matches!(row.as_slice(), [turso_core::Value::Numeric(turso_core::Numeric::Integer(value))] if *value == expected)),
        "microbenchmark SQL integer result mismatch"
    );
    Ok(serde_json::json!(expected))
}

#[test]
#[ignore = "requires explicit local microbenchmark opt-in, retained bucket, private evidence directory and S3 credentials"]
fn local_s3_sql_microbenchmark() -> Result<()> {
    ensure!(
        optional_env("PERI_LOCAL_MICROBENCH")?.as_deref() == Some("1"),
        "PERI_LOCAL_MICROBENCH=1 required"
    );
    ensure!(
        retain_bucket_setting(optional_env("PERI_S3_TEST_RETAIN")?.as_deref())?,
        "PERI_S3_TEST_RETAIN=1 required"
    );
    for name in [
        "PERI_S3_TEST_ENDPOINT",
        "PERI_S3_TEST_ACCESS_KEY",
        "PERI_S3_TEST_SECRET_KEY",
    ] {
        ensure!(optional_env(name)?.is_some(), "{name} required");
    }
    let evidence_dir = optional_env("PERI_LOCAL_BENCH_EVIDENCE_DIR")?
        .ok_or_else(|| anyhow!("PERI_LOCAL_BENCH_EVIDENCE_DIR required"))?;
    private_microbench_dir(Path::new(&evidence_dir))?;
    let (mut evidence, _) = tempfile::Builder::new()
        .prefix("local-s3-sql-")
        .suffix(".jsonl")
        .tempfile_in(&evidence_dir)?
        .keep()?;
    ensure!(
        evidence.metadata()?.permissions().mode() & 0o7777 == 0o600,
        "microbenchmark evidence must be 0600"
    );

    let started = Instant::now();
    ensure!(
        started.elapsed() < Duration::from_secs(90),
        "microbenchmark exceeded 90s"
    );
    let fixture = TestBucket::new()?;
    let s3_retries = fixture.config.retries;
    let store = fixture.store.clone();
    let namespace = Namespace::new("tenant", "local-s3-sql-microbenchmark")?;
    let cache = Path::new(&evidence_dir).join("active-cache");
    fs::create_dir(&cache)?;
    fs::set_permissions(&cache, fs::Permissions::from_mode(0o700))?;
    let database = microbench_record(
        Microbench::new(started, &mut evidence, &fixture.config.bucket, s3_retries),
        "open",
        0,
        serde_json::json!("opened"),
        || {
            Ok((
                RemoteDatabase::open(store.clone(), namespace.clone(), &cache, 0)?,
                serde_json::json!("opened"),
            ))
        },
    )?;
    microbench_record(
        Microbench::new(started, &mut evidence, &fixture.config.bucket, s3_retries),
        "setup",
        0,
        serde_json::json!("created"),
        || {
            database
                .execute("CREATE TABLE ledger(id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)")?;
            Ok(((), serde_json::json!("created")))
        },
    )?;
    for id in 1..=16 {
        microbench_record(
            Microbench::new(started, &mut evidence, &fixture.config.bucket, s3_retries),
            "serial_insert",
            id,
            serde_json::json!({"id": id, "amount": id}),
            || {
                database.execute(&format!(
                    "BEGIN; INSERT INTO ledger(id, amount) VALUES({id}, {id}); COMMIT;"
                ))?;
                Ok(((), serde_json::json!({"id": id, "amount": id})))
            },
        )?;
    }
    for index in 1..=64 {
        let id = (index - 1) % 16 + 1;
        microbench_record(
            Microbench::new(started, &mut evidence, &fixture.config.bucket, s3_retries),
            "serial_point_read",
            index,
            serde_json::json!({"id": id, "amount": id}),
            || {
                let rows = database.query(&format!("SELECT amount FROM ledger WHERE id={id}"))?;
                Ok((
                    (),
                    serde_json::json!({"id": id, "amount": microbench_integer(rows, id as i64)?}),
                ))
            },
        )?;
    }
    drop(database);
    ensure!(
        started.elapsed() < Duration::from_secs(90),
        "microbenchmark exceeded 90s"
    );
    let cold_cache = Path::new(&evidence_dir).join("cold-cache");
    fs::create_dir(&cold_cache)?;
    fs::set_permissions(&cold_cache, fs::Permissions::from_mode(0o700))?;
    let cold = microbench_record(
        Microbench::new(started, &mut evidence, &fixture.config.bucket, s3_retries),
        "cold_open",
        0,
        serde_json::json!("opened"),
        || {
            Ok((
                RemoteDatabase::open(store, namespace, &cold_cache, 0)?,
                serde_json::json!("opened"),
            ))
        },
    )?;
    for (operation, sql, expected) in [
        ("cold_count", "SELECT count(*) FROM ledger", 16),
        ("cold_sum", "SELECT sum(amount) FROM ledger", 136),
    ] {
        microbench_record(
            Microbench::new(started, &mut evidence, &fixture.config.bucket, s3_retries),
            operation,
            0,
            serde_json::json!(expected),
            || {
                let rows = cold.query(sql)?;
                Ok(((), microbench_integer(rows, expected)?))
            },
        )?;
    }
    let expected_ledger: Vec<_> = (1..=16).map(|id| vec![id, id]).collect();
    microbench_record(
        Microbench::new(started, &mut evidence, &fixture.config.bucket, s3_retries),
        "cold_ledger",
        0,
        serde_json::json!(expected_ledger),
        || {
            let rows = cold.query("SELECT id, amount FROM ledger ORDER BY id")?;
            let actual: Vec<Vec<i64>> = rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|value| match value {
                            turso_core::Value::Numeric(turso_core::Numeric::Integer(value)) => {
                                Ok(value)
                            }
                            _ => Err(anyhow!("microbenchmark ledger value mismatch")),
                        })
                        .collect()
                })
                .collect::<Result<_>>()?;
            ensure!(actual == expected_ledger, "microbenchmark ledger mismatch");
            Ok(((), serde_json::json!(actual)))
        },
    )?;
    microbench_record(
        Microbench::new(started, &mut evidence, &fixture.config.bucket, s3_retries),
        "integrity_check",
        0,
        serde_json::json!("ok"),
        || {
            let rows = cold.query("PRAGMA integrity_check")?;
            ensure!(
                matches!(rows.as_slice(), [row] if matches!(row.as_slice(), [turso_core::Value::Text(value)] if value.as_str() == "ok")),
                "microbenchmark integrity check failed"
            );
            Ok(((), serde_json::json!("ok")))
        },
    )?;
    eprintln!("PASS local S3 SQL microbenchmark: 16 serial inserts, 64 serial point reads, cold-cache full ledger/count/sum/integrity, S3 retries={s3_retries}; bucket {}", fixture.config.bucket);
    Ok(())
}

#[test]
fn microbench_evidence_requires_existing_private_directory() -> Result<()> {
    let directory = tempfile::tempdir()?;
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))?;
    private_microbench_dir(directory.path())?;
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o755))?;
    assert!(private_microbench_dir(directory.path()).is_err());
    assert!(private_microbench_dir(&directory.path().join("missing")).is_err());
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
