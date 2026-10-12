use std::path::Path;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{bail, ensure, Result};
use peri_loom_v1::database::RemoteDatabase;
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::{LocalObjectStore, ObjectStore, PutCondition, StoredObject};
use peri_loom_v1::wal::RemoteWal;
use turso_core::{Numeric, Value};

// Only the actual fallback used by LocalObjectStore is injected: append_wal's default returns None,
// so the SQL WAL path writes the chunk and publishes the index using put(".../wal/index").
struct PublishedThenError {
    inner: LocalObjectStore,
    state: AtomicU8, // 0=normal; 1=armed; 2=published/lost ACK; 3=readback performed.
    fail_readback: bool,
    published: AtomicUsize,
    readbacks: AtomicUsize,
}

impl ObjectStore for PublishedThenError {
    fn supports_strict_fencing(&self) -> bool {
        self.inner.supports_strict_fencing()
    }
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        if key.ends_with("/wal/index") && self.state.load(Ordering::SeqCst) == 2 {
            self.readbacks.fetch_add(1, Ordering::SeqCst);
            self.state.store(3, Ordering::SeqCst);
            if self.fail_readback {
                bail!("injected index readback unavailable after publish");
            }
        }
        self.inner.get(key)
    }
    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        let etag = self.inner.put(key, bytes, condition)?;
        if key.ends_with("/wal/index")
            && self
                .state
                .compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.published.fetch_add(1, Ordering::SeqCst);
            // Here the real LocalObjectStore CAS has completed, including on-disk publication.
            // The caller loses only its acknowledgement.
            bail!("injected WAL index acknowledgement lost after publish");
        }
        Ok(etag)
    }
    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

fn namespace() -> Namespace {
    Namespace::new("tenant", "after-publish").unwrap()
}
fn open(store: Arc<dyn ObjectStore>, cache: &Path) -> Result<RemoteDatabase> {
    RemoteDatabase::open(store, namespace(), cache, 0)
}
fn count(db: &RemoteDatabase) -> Result<i64> {
    match &db.query("SELECT count(*) FROM items")?[0][0] {
        Value::Numeric(Numeric::Integer(n)) => Ok(*n),
        other => bail!("unexpected SQL count: {other:?}"),
    }
}

fn run(fail_readback: bool) -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(PublishedThenError {
        inner: LocalObjectStore::open(objects.path())?,
        state: AtomicU8::new(0),
        fail_readback,
        published: AtomicUsize::new(0),
        readbacks: AtomicUsize::new(0),
    });
    let db = open(store.clone(), cache.path())?;
    db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")?;
    db.checkpoint()?;
    ensure!(count(&db)? == 0, "baseline row count");
    let prior_lsn = RemoteWal::new(store.clone())
        .metadata(&namespace())?
        .last_lsn;
    store.state.store(1, Ordering::SeqCst);
    let committed = db.execute("INSERT INTO items VALUES(1)");
    eprintln!("AFTER_PUBLISH readback_fail={fail_readback} sql={committed:?} published={} readbacks={} state={}",
        store.published.load(Ordering::SeqCst), store.readbacks.load(Ordering::SeqCst), store.state.load(Ordering::SeqCst));
    ensure!(
        store.published.load(Ordering::SeqCst) == 1,
        "did not inject in real index publish"
    );
    ensure!(
        store.readbacks.load(Ordering::SeqCst) >= 1,
        "WAL receipt readback did not run"
    );
    let status = db.writer_admission_status()?;
    eprintln!("AFTER_PUBLISH admission_quarantined={}", status.quarantined);
    if fail_readback {
        let error = committed.expect_err("an unverified post-publication result must be unsettled");
        let detail = format!("{error:#}");
        ensure!(
            detail.contains("unsettled"),
            "missing unsettled error: {detail}"
        );
        ensure!(
            detail.contains("injected index readback unavailable after publish"),
            "missing injected cause: {detail}"
        );
        ensure!(
            status.quarantined,
            "failed receipt readback should quarantine the writer"
        );
        ensure!(
            db.execute("INSERT INTO items VALUES(2)").is_err(),
            "quarantined writer dispatched another commit"
        );
    } else {
        committed?;
        ensure!(!status.quarantined, "verified receipt should permit writer");
    }
    store.state.store(0, Ordering::SeqCst);
    let published_lsn = RemoteWal::new(store.clone())
        .metadata(&namespace())?
        .last_lsn;
    ensure!(
        published_lsn > prior_lsn,
        "fault occurred before durable index publish"
    );
    eprintln!("AFTER_PUBLISH prior_lsn={prior_lsn} published_lsn={published_lsn}");
    drop(db);
    cache.close()?;
    let fresh = tempfile::tempdir()?;
    let reopened = open(
        Arc::new(LocalObjectStore::open(objects.path())?),
        fresh.path(),
    )?;
    let recovered_count = count(&reopened)?;
    eprintln!("AFTER_PUBLISH cold_reopen_count={recovered_count}");
    ensure!(
        recovered_count == 1,
        "cold recovery did not replay exact published prefix"
    );
    let ids = reopened.query("SELECT id FROM items ORDER BY id")?;
    ensure!(
        ids.len() == 1 && matches!(&ids[0][0], Value::Numeric(Numeric::Integer(1))),
        "cold recovery included an unexpected write: {ids:?}"
    );
    let cold_lsn = RemoteWal::new(Arc::new(LocalObjectStore::open(objects.path())?))
        .metadata(&namespace())?
        .last_lsn;
    ensure!(
        cold_lsn == published_lsn,
        "cold store advanced WAL without a second successful writer"
    );
    eprintln!("AFTER_PUBLISH cold_reopen_ids={ids:?} cold_lsn={cold_lsn}");
    ensure!(
        !reopened.writer_admission_status()?.quarantined,
        "fresh writer still quarantined"
    );
    Ok(())
}

#[test]
fn published_index_and_recoverable_receipt_settle_successfully() -> Result<()> {
    run(false)
}
#[test]
fn published_index_but_unavailable_receipt_is_unsettled_and_quarantined() -> Result<()> {
    run(true)
}
