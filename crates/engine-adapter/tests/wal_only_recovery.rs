//! 回归测试：**只有 Remote WAL、本地主库为空**的恢复（架构 §11.1 RPO = 0 / §11.2）。
//!
//! # 线上故障
//!
//! 场景：在某节点上新建库并写入若干事务（HTTP 200，响应里的 `wal_lsn` 持续推进），
//! 这个节点随后被替换，库在**另一个从没承载过它的节点**上被唤醒。新节点的目录里只有
//! Worker 从 Remote WAL 回放出来的 `db-wal`：主库文件 `db` 是 0 字节（page 1 只存在于旧
//! 节点的本地文件里 —— 它是引擎自己 fsync 进 `db` 的，从来不在 Remote WAL 中）。
//!
//! 引擎的硬规则是「WAL 有帧 + 主库零页 ⇒ 这份 WAL 不属于该库」，于是它把 WAL 直接删掉，
//! 库变成空的：已 quorum durable 的 20 行数据全部读不到（RPO 违规）。
//!
//! 本测试不用 docker：用真引擎 + 抓取 append 的替身扮演 Remote WAL，把线上这条路径
//! 完整走一遍 —— 唯一的差别是恢复发生在同一个进程里。

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use domain::error::Result;
use domain::{DatabaseId, Lsn};
use engine_adapter::{
    ensure_base_db_for_wal, replay_wal_segments, BaseDbState, DurableIoConfig, EngineAdapter,
    EngineOpenConfig, PlatformDurableIO, QueryOutcome, RemoteWalAppender, DEFAULT_APPEND_TIMEOUT,
};
use turso_core::{UnixIO, IO};
use wal_client::{AppendOutcome, AppendWalRequest, WalClient, WalClientConfig, WalSegment};

/// 扮演 Remote WAL 的替身：把每次 append 的字节按 `file_offset` 存下来，
/// 之后就能像 Worker 那样把它回放到一个新节点的目录里。
///
/// `baseline_lsn` 是「接管这条 WAL 流之前远端已有的末端」：新节点恢复后第一次 append
/// 必须从这里接续，否则 `start_lsn` 会与远端已有区间重叠（服务端必然拒绝）。
#[derive(Debug)]
struct RecordingRemoteWal {
    baseline_lsn: u64,
    appends: Mutex<Vec<AppendWalRequest>>,
}

impl RecordingRemoteWal {
    fn new(baseline_lsn: u64) -> Self {
        Self {
            baseline_lsn,
            appends: Mutex::new(Vec::new()),
        }
    }

    /// 已 durable 的末端 LSN（= 最后一批 append 的结束位置，没有 append 时是 baseline）。
    fn head_lsn(&self) -> u64 {
        self.appends
            .lock()
            .expect("测试用锁不得中毒")
            .last()
            .map_or(self.baseline_lsn, |request| {
                request.start_lsn.get() + request.bytes.len() as u64
            })
    }

    /// 已记录的全部 append（模拟 `read_range` 返回的段）。
    fn segments(&self) -> Vec<WalSegment> {
        self.appends
            .lock()
            .expect("测试用锁不得中毒")
            .iter()
            .map(|request| WalSegment {
                start_lsn: request.start_lsn,
                file_offset: request.file_offset,
                reset_wal: request.reset_wal,
                data: request.bytes.clone(),
            })
            .collect()
    }

    fn append_count(&self) -> usize {
        self.appends.lock().expect("测试用锁不得中毒").len()
    }
}

#[async_trait]
impl RemoteWalAppender for RecordingRemoteWal {
    async fn append(&self, request: AppendWalRequest) -> Result<AppendOutcome> {
        let durable_lsn = request.start_lsn.get() + request.bytes.len() as u64;
        self.appends.lock().expect("测试用锁不得中毒").push(request);
        Ok(AppendOutcome {
            durable_lsn: Lsn::new(durable_lsn),
            acked_replicas: vec!["test-quorum".to_string()],
            latency: Duration::ZERO,
            deduplicated: false,
        })
    }

    async fn last_lsn(&self, _database_id: &DatabaseId) -> Result<Option<Lsn>> {
        Ok(Some(Lsn::new(self.head_lsn())))
    }

    fn describe(&self) -> String {
        "recording-remote-wal".to_string()
    }
}

/// 建一个 DB Process 的 durable IO（真 UnixIO + 注入的 Remote WAL 替身）。
fn durable_io(
    database_id: DatabaseId,
    epoch: u64,
    appender: Arc<RecordingRemoteWal>,
) -> Arc<PlatformDurableIO> {
    let client = Arc::new(
        WalClient::new(WalClientConfig {
            endpoints: vec!["http://127.0.0.1:1".to_string()],
            ..Default::default()
        })
        .expect("构造测试用 WalClient"),
    );
    Arc::new(PlatformDurableIO::new_with_appender(
        Arc::new(UnixIO::new().expect("UnixIO")) as Arc<dyn IO>,
        DurableIoConfig::new(
            database_id,
            epoch,
            client,
            tokio::runtime::Handle::current(),
            DEFAULT_APPEND_TIMEOUT,
        ),
        appender as Arc<dyn RemoteWalAppender>,
    ))
}

/// 打开一个库并返回（适配器，连接）。
fn open(
    durable: Arc<PlatformDurableIO>,
    db_path: &std::path::Path,
    epoch: u64,
) -> (Arc<EngineAdapter>, engine_adapter::EngineConnection) {
    let adapter = EngineAdapter::open(
        Arc::clone(&durable) as Arc<dyn IO>,
        EngineOpenConfig {
            db_path: db_path.to_path_buf(),
            durable_io: Some(Arc::clone(&durable)),
            owner_epoch: epoch,
        },
    )
    .expect("打开数据库");
    let conn = adapter.connect().expect("建立连接");
    (adapter, conn)
}

fn rows(outcome: QueryOutcome) -> Vec<Vec<domain::value::SqlValue>> {
    let QueryOutcome::Rows(result) = outcome else {
        panic!("期望结果集，实际 {outcome:?}");
    };
    result.rows
}

/// 主用例：写入 20 行 → 只带走 Remote WAL 的字节 → 在新目录（主库为空）恢复 → 数据必须
/// 一行不少地读回来，并且恢复后的库还能继续写（durable LSN 与文件偏移都不错位）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_only_recovery_keeps_committed_rows() {
    let database_id = DatabaseId::new_v7();

    // ---- 旧节点：建库、写入 20 行（含一次 schema 变更，制造多个 page 1 帧）----
    let origin = tempfile::tempdir().expect("临时目录");
    let origin_db = origin.path().join("db");
    let remote = Arc::new(RecordingRemoteWal::new(0));
    let (adapter, conn) = open(
        durable_io(database_id, 1, Arc::clone(&remote)),
        &origin_db,
        1,
    );
    conn.execute("CREATE TABLE t_rpo (id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
        .expect("建表");
    for id in 1..=20 {
        conn.execute(&format!(
            "INSERT INTO t_rpo (id, v) VALUES ({id}, 'row-{id}')"
        ))
        .expect("写入");
    }
    // schema 变更：库头（page 1）会被再次写进 WAL，page 1 帧因此不止一帧。
    conn.execute("CREATE INDEX idx_rpo_v ON t_rpo (v)")
        .expect("建索引");
    assert!(
        origin_db.metadata().expect("主库元数据").len() >= 4096,
        "旧节点上主库文件必须已有首页（page 1 是引擎直接写进文件的）"
    );
    drop(conn);
    drop(adapter);

    let segments = remote.segments();
    assert!(
        segments.len() > 20,
        "写入必须真的产生 Remote WAL 段，实际 {}",
        segments.len()
    );
    let remote_head = remote.head_lsn();

    // ---- 新节点：目录里只有回放出来的 WAL，主库文件根本不存在 ----
    let fresh = tempfile::tempdir().expect("临时目录");
    let fresh_db = fresh.path().join("db");
    let fresh_wal = fresh.path().join("db-wal");
    let replayed = replay_wal_segments(&fresh_wal, &segments).expect("回放 Remote WAL 段");
    assert_eq!(replayed, remote_head, "回放末端必须等于 Remote WAL 末端");
    assert!(
        !fresh_db.exists(),
        "本用例的前提是新节点没有主库文件（只有 Remote WAL）"
    );

    // 修复：主库为空 + WAL 有帧 ⇒ 用 WAL 的 page 1 重建库头。
    let state = ensure_base_db_for_wal(&fresh_db, &fresh_wal).expect("重建库头");
    assert!(
        matches!(state, BaseDbState::Rebuilt { .. }),
        "必须走重建分支，实际 {state:?}"
    );
    assert!(
        fresh_db.metadata().expect("主库元数据").len() >= 4096,
        "重建后主库必须至少有一页，否则引擎会丢弃整份 WAL"
    );

    // ---- 恢复后的库必须能打开、能读回全部已提交数据 ----
    // 新进程接手同一条 WAL 流：远端末端就是它的记账起点。
    let remote_after = Arc::new(RecordingRemoteWal::new(remote_head));
    let (_adapter2, conn2) = open(
        durable_io(database_id, 2, Arc::clone(&remote_after)),
        &fresh_db,
        2,
    );

    let count = rows(conn2.query("SELECT count(*) FROM t_rpo").expect("查询行数"));
    assert_eq!(
        count,
        vec![vec![domain::value::SqlValue::Integer(20)]],
        "20 行已提交数据必须一行不少地恢复出来"
    );
    let all = rows(
        conn2
            .query("SELECT id, v FROM t_rpo ORDER BY id")
            .expect("查询全部行"),
    );
    let expected: Vec<Vec<domain::value::SqlValue>> = (1..=20)
        .map(|id| {
            vec![
                domain::value::SqlValue::Integer(id),
                domain::value::SqlValue::text(format!("row-{id}")),
            ]
        })
        .collect();
    assert_eq!(all, expected, "恢复出来的行内容必须与写入时逐行一致");

    // 索引（第二次 page 1 写入之后的 schema）同样必须生效：重复主键要报约束冲突，
    // 说明恢复出来的不只是数据页，库头里的 schema 也是完整的。
    let duplicate = conn2.execute("INSERT INTO t_rpo (id, v) VALUES (1, 'dup')");
    assert!(
        duplicate.is_err(),
        "主键索引必须随恢复一起回来，实际 {duplicate:?}"
    );

    // ---- 恢复不是「假成功」：恢复后的库还能继续写，且 WAL 流按偏移接续 ----
    let wal_len_before = fresh_wal.metadata().expect("WAL 元数据").len();
    conn2
        .execute("INSERT INTO t_rpo (id, v) VALUES (21, 'row-21')")
        .expect("恢复后继续写入");
    let appended = remote_after
        .appends
        .lock()
        .expect("测试用锁不得中毒")
        .clone();
    let last = appended.last().expect("恢复后的写入必须有 append");
    assert_eq!(
        last.start_lsn.get(),
        remote_head,
        "恢复后的第一批 append 必须接在远端末端之后"
    );
    assert_eq!(
        last.file_offset, wal_len_before,
        "恢复后的 append 必须落在本地 WAL 的真实末端（偏移错位会让远端与本地永久分叉）"
    );
    assert_eq!(
        remote_after.append_count(),
        1,
        "一次提交只应产生一次 append"
    );

    let count_after = rows(conn2.query("SELECT count(*) FROM t_rpo").expect("查询行数"));
    assert_eq!(
        count_after,
        vec![vec![domain::value::SqlValue::Integer(21)]],
        "恢复后的写入必须可见"
    );
    assert!(
        fresh_wal.metadata().expect("WAL 元数据").len() > wal_len_before,
        "新提交必须落到本地 WAL"
    );
}

/// 反向用例：**没有任何 WAL** 的新库不该被本函数干扰（正常的首次启动路径）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn brand_new_database_needs_no_repair() {
    let database_id = DatabaseId::new_v7();
    let dir = tempfile::tempdir().expect("临时目录");
    let db_path = dir.path().join("db");

    let state = ensure_base_db_for_wal(&db_path, &engine_adapter::wal_path_for(&db_path))
        .expect("新库无需修复");
    assert_eq!(state, BaseDbState::NoWalFrames);

    let remote = Arc::new(RecordingRemoteWal::new(0));
    let (_adapter, conn) = open(durable_io(database_id, 1, remote), &db_path, 1);
    conn.execute("CREATE TABLE fresh (id INTEGER PRIMARY KEY)")
        .expect("建表");
    conn.execute("INSERT INTO fresh (id) VALUES (7)")
        .expect("写入");
    assert_eq!(
        rows(conn.query("SELECT id FROM fresh").expect("查询")),
        vec![vec![domain::value::SqlValue::Integer(7)]]
    );
}
