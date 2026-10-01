//! 模拟 WAL sync 失败；测试成功响应必须等可靠同步确认。
use engine_adapter::{EngineAdapter, EngineOpenConfig};
use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use turso_core::io::FileSyncType;
use turso_core::{
    Buffer, Clock, Completion, CompletionError, File, MonotonicInstant, OpenFlags,
    WallClockInstant, IO,
};

struct FaultIO {
    inner: Arc<dyn IO>,
    fail: Arc<AtomicBool>,
    wal_syncs: Arc<AtomicUsize>,
}
impl Clock for FaultIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}
impl IO for FaultIO {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        if path.ends_with("-wal") {
            Ok(Arc::new(FaultFile {
                inner,
                fail: self.fail.clone(),
                wal_syncs: self.wal_syncs.clone(),
            }))
        } else {
            Ok(inner)
        }
    }
    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.inner.remove_file(path)
    }
    fn step(&self) -> turso_core::Result<()> {
        self.inner.step()
    }
    fn file_id(&self, path: &str) -> turso_core::Result<turso_core::io::FileId> {
        self.inner.file_id(path)
    }
}
struct FaultFile {
    inner: Arc<dyn File>,
    fail: Arc<AtomicBool>,
    wal_syncs: Arc<AtomicUsize>,
}
impl File for FaultFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
        self.inner.pread(pos, c)
    }
    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.inner.pwrite(pos, buffer, c)
    }
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> turso_core::Result<Completion> {
        self.wal_syncs.fetch_add(1, Ordering::SeqCst);
        if self.fail.swap(false, Ordering::SeqCst) {
            c.error(CompletionError::IOError(
                ErrorKind::Other,
                "injected WAL sync failure",
            ));
            return Ok(c);
        }
        self.inner.sync(c, sync_type)
    }
    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
        self.inner.truncate(len, c)
    }
}

#[test]
fn autocommit_and_explicit_commit_wait_for_wal_sync() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("main.db");
    let fail = Arc::new(AtomicBool::new(false));
    let wal_syncs = Arc::new(AtomicUsize::new(0));
    let io: Arc<dyn IO> = Arc::new(FaultIO {
        inner: Arc::new(turso_core::UnixIO::new().unwrap()),
        fail: fail.clone(),
        wal_syncs: wal_syncs.clone(),
    });
    let db = EngineAdapter::open(
        io,
        EngineOpenConfig {
            db_path: path,
            durable_io: None,
            owner_epoch: 0,
        },
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.enforce_full_sync();
    conn.execute("CREATE TABLE t(v INTEGER)").unwrap();
    let before = wal_syncs.load(Ordering::SeqCst);
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    assert!(
        wal_syncs.load(Ordering::SeqCst) > before,
        "autocommit returned without WAL sync"
    );
    conn.begin().unwrap();
    conn.execute("INSERT INTO t VALUES (2)").unwrap();
    let before = wal_syncs.load(Ordering::SeqCst);
    conn.commit().unwrap();
    assert!(
        wal_syncs.load(Ordering::SeqCst) > before,
        "explicit COMMIT returned without WAL sync"
    );
    fail.store(true, Ordering::SeqCst);
    assert!(
        conn.execute("INSERT INTO t VALUES (3)").is_err(),
        "failed WAL sync acknowledged a write"
    );
}

#[test]
fn power_loss_model_discards_unsynced_wal_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("main.db");
    let stable = dir.path().join("stable");
    std::fs::create_dir(&stable).unwrap();
    let fail = Arc::new(AtomicBool::new(false));
    let io: Arc<dyn IO> = Arc::new(FaultIO {
        inner: Arc::new(turso_core::UnixIO::new().unwrap()),
        fail: fail.clone(),
        wal_syncs: Arc::new(AtomicUsize::new(0)),
    });
    let db = EngineAdapter::open(
        io,
        EngineOpenConfig {
            db_path: path.clone(),
            durable_io: None,
            owner_epoch: 0,
        },
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.enforce_full_sync();
    conn.execute("CREATE TABLE t(v INTEGER)").unwrap();
    conn.execute("INSERT INTO t VALUES (11)").unwrap();
    // 稳定介质模型：只保留已完成 sync 后的 DB/WAL 字节，之后的写入在掉电时丢弃。
    for name in ["main.db", "main.db-wal"] {
        let source = dir.path().join(name);
        if source.exists() {
            std::fs::copy(&source, stable.join(name)).unwrap();
        }
    }
    fail.store(true, Ordering::SeqCst);
    assert!(conn.execute("INSERT INTO t VALUES (22)").is_err());
    drop(conn);
    drop(db);
    for name in ["main.db", "main.db-wal", "main.db-shm"] {
        let _ = std::fs::remove_file(dir.path().join(name));
    }
    for name in ["main.db", "main.db-wal"] {
        let source = stable.join(name);
        if source.exists() {
            std::fs::copy(source, dir.path().join(name)).unwrap();
        }
    }
    let reopened = EngineAdapter::open(
        Arc::new(turso_core::UnixIO::new().unwrap()),
        EngineOpenConfig {
            db_path: path,
            durable_io: None,
            owner_epoch: 0,
        },
    )
    .unwrap();
    let values = reopened
        .connect()
        .unwrap()
        .query("SELECT v FROM t ORDER BY v")
        .unwrap();
    match values {
        engine_adapter::QueryOutcome::Rows(rows) => {
            assert_eq!(rows.rows, vec![vec![domain::value::SqlValue::Integer(11)]])
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn explicit_commit_sync_failure_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("main.db");
    let fail = Arc::new(AtomicBool::new(false));
    let io: Arc<dyn IO> = Arc::new(FaultIO {
        inner: Arc::new(turso_core::UnixIO::new().unwrap()),
        fail: fail.clone(),
        wal_syncs: Arc::new(AtomicUsize::new(0)),
    });
    let db = EngineAdapter::open(
        io,
        EngineOpenConfig {
            db_path: path,
            durable_io: None,
            owner_epoch: 0,
        },
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.enforce_full_sync();
    conn.execute("CREATE TABLE t(v INTEGER)").unwrap();
    conn.begin().unwrap();
    conn.execute("INSERT INTO t VALUES (3)").unwrap();
    fail.store(true, Ordering::SeqCst);
    assert!(
        conn.commit().is_err(),
        "explicit COMMIT acknowledged failed WAL sync"
    );
}
