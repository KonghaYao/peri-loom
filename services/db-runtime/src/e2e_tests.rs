//! db-runtime 的端到端测试（无外部依赖）。
//!
//! 覆盖四件最容易被改坏的事：
//! 1. 握手 -> `Execute` 真的能跑通（真引擎 + 真 UDS + 真 commit durability 路径）；
//! 2. 会话空闲超时 / 事务最大存活返回**冻结错误码**；
//! 3. 重启（新进程）后的事务返回 `TRANSACTION_LOST`，绝不假成功；
//! 4. deadline 与流式分块的帧序。
//!
//! 测试用「本地 ACK 的 appender」替代 Remote WAL：这样既不依赖真实 WAL 集群，
//! 又完整走一遍 `PlatformDurableIO` 的 commit 结算路径（append 必须成功才提交）。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use domain::error::ErrorCode;
use engine_adapter::RemoteWalAppender;
use protocol::framing::{read_frame, write_frame};
use protocol::runtime_local as rt;
use tokio::net::UnixStream;
use wal_client::{AppendOutcome, AppendWalRequest};

use crate::config::RuntimeConfig;

/// 测试库 id：必须是合法 UUID（`--database-id` 是 Catalog 主键，见 host.rs 的解析约束）。
const TEST_DB_ID: &str = "018f0b3a-1c2d-7e3f-8a4b-5c6d7e8f9a0b";
use crate::host::Host;
use crate::server;

/// 本地 ACK 的 append 实现：字节"写"到哪里不重要，重要的是 commit 必须拿到确认。
#[derive(Debug)]
struct LocalAckAppender;

#[async_trait::async_trait]
impl RemoteWalAppender for LocalAckAppender {
    async fn append(&self, request: AppendWalRequest) -> domain::error::Result<AppendOutcome> {
        Ok(AppendOutcome {
            durable_lsn: request.start_lsn.saturating_add(request.bytes.len() as u64),
            acked_replicas: vec!["local-test".to_string()],
            latency: Duration::from_millis(0),
            deduplicated: false,
        })
    }

    fn describe(&self) -> String {
        "local-ack-test".to_string()
    }
}

/// 可切换的 append 实现：默认本地 ACK；`fail(true)` 之后一律返回「未 durable」。
///
/// 现场故障链要求**先有成功的 commit，再有失败的 commit**：只有已提交的字节存在，
/// 引擎随后的回卷才会越过最后一个 commit 边界而被判成字节流违规（fail-stop）。
#[derive(Debug)]
struct SwitchableAppender {
    failing: std::sync::atomic::AtomicBool,
}

impl SwitchableAppender {
    fn new() -> Self {
        Self {
            failing: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// 让后续所有 append 都拿不到 durable 确认（模拟 Remote WAL 不可用）。
    fn fail_next_appends(&self) {
        self.failing
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

#[async_trait::async_trait]
impl RemoteWalAppender for SwitchableAppender {
    async fn append(&self, request: AppendWalRequest) -> domain::error::Result<AppendOutcome> {
        if self.failing.load(std::sync::atomic::Ordering::Acquire) {
            return Err(domain::error::PlatformError::new(
                ErrorCode::WalNotDurable,
                "测试注入：Remote WAL 未确认 quorum durable",
            ));
        }
        Ok(AppendOutcome {
            durable_lsn: request.start_lsn.saturating_add(request.bytes.len() as u64),
            acked_replicas: vec!["local-test".to_string()],
            latency: Duration::from_millis(0),
            deduplicated: false,
        })
    }

    fn describe(&self) -> String {
        "switchable-test".to_string()
    }
}

/// 一套测试环境：临时目录 + 已启动的宿主 + 服务任务。
struct TestEnv {
    dir: tempfile::TempDir,
    host: Arc<Host>,
    task: tokio::task::JoinHandle<anyhow::Result<i32>>,
    epoch: u64,
}

impl TestEnv {
    /// 启动一个真实宿主（真数据库文件 + 本地 ACK durability）。
    async fn start(epoch: u64) -> Self {
        Self::start_with(epoch, |_| {}).await
    }

    async fn start_with<F: FnOnce(&mut RuntimeConfig)>(epoch: u64, tweak: F) -> Self {
        Self::start_in(tempfile::tempdir().expect("tempdir"), epoch, tweak).await
    }

    /// 启动宿主并注入自定义 append 实现（覆盖 durability 失败路径）。
    async fn start_with_appender(epoch: u64, appender: Arc<dyn RemoteWalAppender>) -> Self {
        Self::start_in_with_appender(
            tempfile::tempdir().expect("tempdir"),
            epoch,
            appender,
            |_| {},
        )
        .await
    }

    /// 在**已有的数据目录**里启动宿主（模拟「同一个工作集被新进程接管」：failover 后
    /// 新节点只有 Worker 回放出来的本地文件）。
    async fn start_in<F: FnOnce(&mut RuntimeConfig)>(
        dir: tempfile::TempDir,
        epoch: u64,
        tweak: F,
    ) -> Self {
        Self::start_in_with_appender(dir, epoch, Arc::new(LocalAckAppender), tweak).await
    }

    async fn start_in_with_appender<F: FnOnce(&mut RuntimeConfig)>(
        dir: tempfile::TempDir,
        epoch: u64,
        appender: Arc<dyn RemoteWalAppender>,
        tweak: F,
    ) -> Self {
        let mut config = config_for(dir.path(), epoch);
        tweak(&mut config);
        let host = Host::open(config, Some(appender))
            .await
            .expect("宿主启动失败");
        // 复用目录时必须先清掉上一代 socket 文件：否则 `wait_for_socket` 会立刻
        // 看到这个指向死进程的文件而误判就绪（Worker 的 supervisor 同样先清理）。
        let _ = std::fs::remove_file(socket_path(dir.path()));
        let task = tokio::spawn(server::run(Arc::clone(&host)));
        wait_for_socket(&socket_path(dir.path())).await;
        Self {
            dir,
            host,
            task,
            epoch,
        }
    }

    fn socket(&self) -> PathBuf {
        socket_path(self.dir.path())
    }

    /// 本地 WAL 路径（引擎按 `<db_path>-wal` 推导）。
    fn wal_path(&self) -> PathBuf {
        let mut path = self
            .dir
            .path()
            .join("data")
            .join("test.db")
            .into_os_string();
        path.push("-wal");
        PathBuf::from(path)
    }

    /// 连上 UDS 并完成握手（DB Process 先发 Hello，客户端回 HelloAck）。
    async fn connect(&self) -> Client {
        let stream = UnixStream::connect(self.socket()).await.expect("连接 UDS");
        let mut client = Client {
            stream,
            epoch: self.epoch,
            session_id: String::new(),
        };
        client.handshake().await;
        client
    }

    async fn shutdown(self) {
        self.task.abort();
    }

    /// 停机但把数据目录交还给调用方（模拟「进程死了，本地文件还在」）。
    async fn shutdown_keeping_dir(self) -> tempfile::TempDir {
        self.task.abort();
        self.dir
    }
}

fn socket_path(dir: &Path) -> PathBuf {
    dir.join("run").join("db.sock")
}

fn config_for(dir: &Path, epoch: u64) -> RuntimeConfig {
    // 用固定的假端点构造 WalClient（本地 ACK 实现下永远不会连它），
    // 避免测试依赖真实 WAL 集群，同时保持生产代码里"端点必填"的约束。
    RuntimeConfig {
        worker_id: "worker-test".to_string(),
        database_id: TEST_DB_ID.to_string(),
        owner_epoch: epoch,
        db_path: dir.join("data").join("test.db"),
        wal_path: None,
        socket_path: socket_path(dir),
        data_dir: dir.join("data"),
        run_dir: dir.join("run"),
        cpu_milli: 1000,
        memory_mib: 256,
        disk_mib: 1024,
        process_slots: 1,
        log_level: "info".to_string(),
        wal_cluster: Some("1@127.0.0.1:19201".to_string()),
        read_only: false,
        snapshot_id: String::new(),
        base_lsn: 0,
        wal_client_endpoints: None,
    }
}

async fn wait_for_socket(path: &Path) {
    for _ in 0..100 {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("socket 未在预期时间内出现：{}", path.display());
}

/// 极简客户端：只用 protocol::framing 收发帧。
struct Client {
    stream: UnixStream,
    /// 本进程的 owner epoch（握手时校验，并在 HelloAck 里回显）。
    epoch: u64,
    /// 最近一次 OpenSession 得到的会话 id（空 = 无会话执行）。
    session_id: String,
}

impl Client {
    /// 完成握手：读 DB Process 的 Hello -> 回 HelloAck。
    async fn handshake(&mut self) {
        let frame = read_frame(&mut self.stream)
            .await
            .expect("读 Hello")
            .expect("连接被关闭");
        let Some(rt::frame::Message::Hello(hello)) = frame.message else {
            panic!("期望 Hello，实际 {:?}", frame.message);
        };
        assert_eq!(hello.database_id, TEST_DB_ID);
        assert_eq!(
            hello.owner_epoch, self.epoch,
            "Hello 的 epoch 必须与配置一致"
        );
        let ack = rt::Frame {
            seq: 1,
            reply_to_seq: frame.seq,
            request_id: String::new(),
            database_id: hello.database_id.clone(),
            owner_epoch: hello.owner_epoch,
            session_id: String::new(),
            transaction_id: String::new(),
            deadline_unix_ms: 0,
            error: None,
            message: Some(rt::frame::Message::HelloAck(rt::HelloAck {
                accepted: true,
                worker_id: "worker-test".to_string(),
                dispatcher_epoch: hello.owner_epoch,
                reject_reason: String::new(),
            })),
        };
        write_frame(&mut self.stream, &ack)
            .await
            .expect("回 HelloAck");
    }

    async fn send(&mut self, frame: rt::Frame) {
        write_frame(&mut self.stream, &frame)
            .await
            .expect("写帧失败");
    }

    /// 读一帧直到拿到 `reply_to_seq == seq` 的响应（跳过通知类帧）。
    async fn recv_reply(&mut self, seq: u64) -> rt::Frame {
        for _ in 0..64 {
            let frame = read_frame(&mut self.stream)
                .await
                .expect("读帧失败")
                .expect("连接被关闭");
            if frame.reply_to_seq == seq {
                return frame;
            }
            // 通知帧（SessionExpiredNotice 等）不参与请求响应匹配。
        }
        panic!("等待响应 {seq} 超时（帧数过多）");
    }

    fn frame(&self, seq: u64, request_id: &str, message: rt::frame::Message) -> rt::Frame {
        rt::Frame {
            seq,
            reply_to_seq: 0,
            request_id: request_id.to_string(),
            database_id: TEST_DB_ID.to_string(),
            owner_epoch: self.epoch,
            // 会话内请求自动带上当前会话；无会话执行时该字段为空。
            session_id: self.session_id.clone(),
            transaction_id: String::new(),
            deadline_unix_ms: 0,
            error: None,
            message: Some(message),
        }
    }

    /// 执行一条语句并返回响应帧。
    async fn execute(&mut self, seq: u64, sql: &str) -> rt::Frame {
        let mut frame = self.frame(
            seq,
            &format!("req-{seq}"),
            rt::frame::Message::Execute(rt::ExecuteRequest {
                sql: sql.to_string(),
                params: Vec::new(),
                atomic: false,
                want_stream: false,
                inline_row_limit: 0,
            }),
        );
        frame.session_id = self.session_id.clone();
        self.send(frame).await;
        self.recv_reply(seq).await
    }
}

/// 从响应帧里取出结果集。
fn result_of(frame: &rt::Frame) -> &protocol::data::ResultSet {
    let Some(rt::frame::Message::ExecuteResponse(response)) = frame.message.as_ref() else {
        panic!("期望 ExecuteResponse，实际 {:?}", frame.message);
    };
    response.result.as_ref().expect("期望带结果集")
}

fn error_code_of(frame: &rt::Frame) -> ErrorCode {
    let error = frame.error.as_ref().expect("期望错误帧");
    protocol::convert::error_code_from_proto(error.code)
}

// ---------------------------------------------------------------- 用例

/// Hello 握手通过后，CREATE / INSERT / SELECT 必须能跑通。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_round_trip_over_uds() {
    let env = TestEnv::start(1).await;
    let mut client = env.connect().await;

    let created = client
        .execute(10, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .await;
    assert!(created.error.is_none(), "建表失败：{:?}", created.error);

    let inserted = client
        .execute(
            11,
            "INSERT INTO t (id, name) VALUES (1, 'alice'), (2, 'bob')",
        )
        .await;
    assert!(inserted.error.is_none(), "写入失败：{:?}", inserted.error);
    // 写入必须真的推进了 durable 末端：本地 ACK 的 append 走的是 PlatformDurableIO 的
    // commit 结算路径，"成功"意味着远程侧已确认。
    assert!(
        env.host.durable_lsn() > 0,
        "commit 之后 durable LSN 必须推进"
    );

    let selected = client
        .execute(12, "SELECT id, name FROM t ORDER BY id")
        .await;
    assert!(selected.error.is_none(), "查询失败：{:?}", selected.error);
    let result = result_of(&selected);
    assert_eq!(result.columns.len(), 2, "列数不对：{:?}", result.columns);
    assert_eq!(result.rows.len(), 2, "行数不对：{:?}", result.rows);

    env.shutdown().await;
}

/// 流式执行必须按 Header -> RowBatch* -> End 的顺序回帧。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_execute_emits_expected_frame_sequence() {
    let env = TestEnv::start(1).await;
    let mut client = env.connect().await;
    client
        .execute(10, "CREATE TABLE s (id INTEGER PRIMARY KEY)")
        .await;
    for id in 0..200 {
        client
            .execute(11, &format!("INSERT INTO s (id) VALUES ({id})"))
            .await;
    }

    let seq = 12;
    let frame = client.frame(
        seq,
        "req-stream",
        rt::frame::Message::Execute(rt::ExecuteRequest {
            sql: "SELECT id FROM s".to_string(),
            params: Vec::new(),
            atomic: false,
            want_stream: true,
            inline_row_limit: 0,
        }),
    );
    client.send(frame).await;

    let header = client.recv_reply(seq).await;
    assert!(
        matches!(header.message, Some(rt::frame::Message::StreamHeader(_))),
        "首帧必须是 StreamHeader：{:?}",
        header.message
    );

    let mut total_rows = 0u64;
    loop {
        let frame = client.recv_reply(seq).await;
        match frame.message {
            Some(rt::frame::Message::Rows(batch)) => total_rows += batch.rows.len() as u64,
            Some(rt::frame::Message::StreamEnd(end)) => {
                assert_eq!(end.affected_rows, 0, "SELECT 不应报告受影响行数");
                break;
            }
            other => panic!("流中出现意外帧：{other:?}"),
        }
    }
    assert_eq!(total_rows, 200, "流式返回的行数必须与写入一致");

    env.shutdown().await;
}

/// 会话空闲超时：下一次请求必须拿到 SESSION_IDLE_TIMEOUT（而不是 SESSION_LOST）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_idle_timeout_returns_frozen_code() {
    let env = TestEnv::start(1).await;
    let mut client = env.connect().await;

    client.session_id = String::new();
    client
        .send(client.frame(
            10,
            "req-open",
            rt::frame::Message::OpenSession(rt::OpenSessionRequest {
                idle_timeout_ms: 50,
                max_transaction_lifetime_ms: 0,
            }),
        ))
        .await;
    let reply = client.recv_reply(10).await;
    let Some(rt::frame::Message::OpenSessionResponse(response)) = reply.message else {
        panic!("期望 OpenSessionResponse，实际 {:?}", reply.message);
    };
    assert!(!response.session_id.is_empty());
    client.session_id = response.session_id.clone();

    // 空闲超过 50ms 后必须被判定为超时。
    tokio::time::sleep(Duration::from_millis(200)).await;
    let late = client.execute(11, "SELECT 1").await;
    assert_eq!(
        error_code_of(&late),
        ErrorCode::SessionIdleTimeout,
        "超时会话必须回 SESSION_IDLE_TIMEOUT"
    );

    env.shutdown().await;
}

/// 事务最大存活：超时后的 Commit 必须拿到 TRANSACTION_MAX_LIFETIME_EXCEEDED。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transaction_max_lifetime_returns_frozen_code() {
    let env = TestEnv::start(1).await;
    let mut client = env.connect().await;

    client
        .send(client.frame(
            10,
            "req-open",
            rt::frame::Message::OpenSession(rt::OpenSessionRequest {
                idle_timeout_ms: 0,
                max_transaction_lifetime_ms: 0,
            }),
        ))
        .await;
    let reply = client.recv_reply(10).await;
    let Some(rt::frame::Message::OpenSessionResponse(response)) = reply.message else {
        panic!("期望 OpenSessionResponse");
    };
    client.session_id = response.session_id.clone();

    // 事务只允许存活 50ms。
    client
        .send(client.frame(
            11,
            "req-begin",
            rt::frame::Message::Begin(rt::BeginRequest {
                read_only: false,
                max_lifetime_ms: 50,
            }),
        ))
        .await;
    let reply = client.recv_reply(11).await;
    let Some(rt::frame::Message::TransactionResponse(txn)) = reply.message else {
        panic!(
            "期望 TransactionResponse，实际 {:?} / 错误 {:?}",
            reply.message, reply.error
        );
    };
    assert!(!txn.transaction_id.is_empty());

    tokio::time::sleep(Duration::from_millis(200)).await;

    client
        .send(client.frame(
            12,
            "req-commit",
            rt::frame::Message::Commit(rt::CommitRequest {}),
        ))
        .await;
    let reply = client.recv_reply(12).await;
    assert_eq!(
        error_code_of(&reply),
        ErrorCode::TransactionMaxLifetimeExceeded,
        "超时事务必须回 TRANSACTION_MAX_LIFETIME_EXCEEDED"
    );

    env.shutdown().await;
}

/// 事务内执行 + 正常提交：Commit 必须返回已 durable 的 LSN。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_transaction_commit_reports_wal_lsn() {
    let env = TestEnv::start(1).await;
    let mut client = env.connect().await;
    client
        .execute(10, "CREATE TABLE tx (id INTEGER PRIMARY KEY)")
        .await;

    client
        .send(client.frame(
            11,
            "req-open",
            rt::frame::Message::OpenSession(rt::OpenSessionRequest {
                idle_timeout_ms: 0,
                max_transaction_lifetime_ms: 0,
            }),
        ))
        .await;
    let reply = client.recv_reply(11).await;
    let Some(rt::frame::Message::OpenSessionResponse(response)) = reply.message else {
        panic!("期望 OpenSessionResponse");
    };
    client.session_id = response.session_id.clone();

    client
        .send(client.frame(
            12,
            "req-begin",
            rt::frame::Message::Begin(rt::BeginRequest {
                read_only: false,
                max_lifetime_ms: 0,
            }),
        ))
        .await;
    let reply = client.recv_reply(12).await;
    let Some(rt::frame::Message::TransactionResponse(txn)) = reply.message else {
        panic!(
            "期望 TransactionResponse，实际 {:?} / 错误 {:?}",
            reply.message, reply.error
        );
    };

    let inside = client.execute(13, "INSERT INTO tx (id) VALUES (7)").await;
    assert!(inside.error.is_none(), "事务内写入失败：{:?}", inside.error);

    let mut commit = client.frame(
        14,
        "req-commit",
        rt::frame::Message::Commit(rt::CommitRequest {}),
    );
    commit.transaction_id = txn.transaction_id.clone();
    client.send(commit).await;
    let reply = client.recv_reply(14).await;
    let Some(rt::frame::Message::CommitResponse(response)) = reply.message else {
        panic!("期望 CommitResponse，实际 {:?}", reply.message);
    };
    assert!(response.wal_lsn > 0, "提交后必须报告已 durable 的 LSN");

    let selected = client.execute(15, "SELECT id FROM tx").await;
    assert_eq!(result_of(&selected).rows.len(), 1, "提交的数据必须可见");

    env.shutdown().await;
}

/// 进程重启（新宿主）后，原会话/事务必须回 TRANSACTION_LOST，不得假成功。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_after_restart_is_transaction_lost() {
    let env = TestEnv::start(1).await;
    let mut first = env.connect().await;
    first
        .send(first.frame(
            10,
            "req-open",
            rt::frame::Message::OpenSession(rt::OpenSessionRequest {
                idle_timeout_ms: 0,
                max_transaction_lifetime_ms: 0,
            }),
        ))
        .await;
    let reply = first.recv_reply(10).await;
    let Some(rt::frame::Message::OpenSessionResponse(response)) = reply.message else {
        panic!("期望 OpenSessionResponse");
    };
    let session_id = response.session_id.clone();

    // 换一个"新进程"（新宿主 = 新的会话表），模拟重启后的调度。
    let restarted = TestEnv::start_with(2, |_| {}).await;
    let mut second = restarted.connect().await;
    let mut commit = second.frame(
        20,
        "req-commit",
        rt::frame::Message::Commit(rt::CommitRequest {}),
    );
    commit.session_id = session_id;
    second.send(commit).await;
    let reply = second.recv_reply(20).await;
    assert_eq!(
        error_code_of(&reply),
        ErrorCode::TransactionLost,
        "重启后的事务必须回 TRANSACTION_LOST"
    );

    env.shutdown().await;
    restarted.shutdown().await;
}

/// 已过期的 deadline：请求还未执行就必须回 DeadlineExceeded。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_deadline_returns_deadline_exceeded() {
    let env = TestEnv::start(1).await;
    let mut client = env.connect().await;

    let mut frame = client.frame(
        10,
        "req-deadline",
        rt::frame::Message::Execute(rt::ExecuteRequest {
            sql: "CREATE TABLE never (id INTEGER)".to_string(),
            params: Vec::new(),
            atomic: false,
            want_stream: false,
            inline_row_limit: 0,
        }),
    );
    frame.deadline_unix_ms = crate::session::now_ms().saturating_sub(1);
    client.send(frame).await;
    let reply = client.recv_reply(10).await;
    assert_eq!(error_code_of(&reply), ErrorCode::DeadlineExceeded);

    // 语句不得被执行：表不存在。
    let check = client
        .execute(11, "SELECT name FROM sqlite_master WHERE name = 'never'")
        .await;
    assert_eq!(result_of(&check).rows.len(), 0, "过期请求不得真正执行");

    env.shutdown().await;
}

/// durable IO fail-stop 之后**进程必须退出**（架构 §11.1 / §12.1）。
///
/// 现场故障链：Remote WAL 不可用 -> commit 拿不到 durable 确认 -> 引擎回卷本地 WAL ->
/// 回卷越过了最后一个 commit 边界 -> `PlatformDurableIO` 判定本地字节流不可信并 fail-stop。
/// 此后本进程再也写不进任何东西，唯一正确的动作是退出：Worker 会把它当异常退出并重启一个
/// 干净进程（本用例不测 Worker，只钉住"退出码非零 + 明确原因"这一半）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_fail_stop_exits_process() {
    let appender = Arc::new(SwitchableAppender::new());
    let env =
        TestEnv::start_with_appender(1, Arc::clone(&appender) as Arc<dyn RemoteWalAppender>).await;
    let mut client = env.connect().await;

    // 先有一条成功的 commit：本地 WAL 里因此存在"已提交字节"（回卷的禁区）。
    let created = client
        .execute(10, "CREATE TABLE t_fs (id INTEGER PRIMARY KEY, v TEXT)")
        .await;
    assert!(created.error.is_none(), "基线建表失败：{:?}", created.error);
    let inserted = client
        .execute(11, "INSERT INTO t_fs (v) VALUES ('baseline')")
        .await;
    assert!(
        inserted.error.is_none(),
        "基线写入失败：{:?}",
        inserted.error
    );

    // Remote WAL 变不可用：这一次写入不可能拿到 durable 确认。
    appender.fail_next_appends();
    let failed = client
        .execute(12, "INSERT INTO t_fs (v) VALUES ('must-fail')")
        .await;
    assert!(
        failed.error.is_some(),
        "远程 WAL 不可用时写入不得成功：{:?}",
        failed.error
    );
    // 与现场一致的重试（验收脚本 durability 场景同样反复重试）：失败的写入让引擎回卷
    // 本地 WAL 游标，后续写入便落在「早于最后一个 commit 边界」的偏移上 -> 帧解析违规。
    // 一旦退出决策成立就立刻停止发请求：进程正在退出，连接会被关闭。
    for seq in 13..23u64 {
        if env.task.is_finished() {
            break;
        }
        let retry = client
            .execute(seq, "INSERT INTO t_fs (v) VALUES ('after-recovery')")
            .await;
        assert!(
            retry.error.is_some(),
            "durable IO 已停摆时写入不得成功：{:?}",
            retry.error
        );
        if crate::fatal::decide_exit(&crate::fatal::DurableReport::capture(&env.host.durable))
            != crate::fatal::ExitDecision::Continue
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // 等看门狗给出退出决策：进程必须以 durable IO fail-stop 的退出码结束。
    let code = tokio::time::timeout(Duration::from_secs(5), env.task)
        .await
        .expect("fail-stop 后必须迅速退出（不得停在原地继续服务）")
        .expect("服务任务不应 panic")
        .expect("服务循环不应报错");
    assert_eq!(
        code,
        crate::host::EXIT_DURABLE_STOP,
        "fail-stop 必须以专用非零退出码退出（实际 {code}）"
    );
    assert_eq!(
        env.host.state(),
        crate::host::HostState::Draining,
        "退出前必须停止接受新请求"
    );
    // 退出前要把原因写清楚（日志已打印；这里确认原因确实是不可恢复的 fail-stop）。
    let reason = env.host.durable.last_error().expect("fail-stop 原因");
    assert!(
        crate::fatal::is_unrecoverable_fail_stop(&reason),
        "退出原因必须是不可恢复的 fail-stop：{reason}"
    );
}

/// 可恢复的 durability 失败（远程 append 超时）**不得**导致进程退出：
/// 写请求照样失败，但进程仍然有服务能力（架构 §11.1 只要求不假成功）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transient_durability_failure_does_not_exit_process() {
    let env = TestEnv::start_with_appender(1, Arc::new(NeverAckAppender)).await;
    let mut client = env.connect().await;

    // append 永不返回 -> 超过 append_timeout 后写请求失败（耗时受 append 超时支配）。
    let created = client
        .execute(10, "CREATE TABLE t_slow (id INTEGER PRIMARY KEY)")
        .await;
    assert!(
        created.error.is_some(),
        "未拿到 durable 确认的写入不得成功：{:?}",
        created.error
    );

    let report = crate::fatal::DurableReport::capture(&env.host.durable);
    assert!(report.fenced, "durability 失败必须 fence WAL 写入流");
    assert_eq!(
        crate::fatal::decide_exit(&report),
        crate::fatal::ExitDecision::Continue,
        "可恢复失败不得触发退出：{:?}",
        report.last_error
    );
    // 进程仍在服务：服务任务没有结束（`join` 立刻返回说明它已经退出）。
    assert!(
        !env.task.is_finished(),
        "可恢复的 durability 失败不得让进程退出"
    );
    // 只读语句照样可用（证明"进程还在，只是写不了"）。
    let selected = client.execute(11, "SELECT 1").await;
    assert!(
        selected.error.is_none(),
        "读请求应继续可用：{:?}",
        selected.error
    );

    env.shutdown().await;
}

/// 永不返回的 append 实现：触发 append 超时（可恢复类故障）。
#[derive(Debug)]
struct NeverAckAppender;

#[async_trait::async_trait]
impl RemoteWalAppender for NeverAckAppender {
    async fn append(&self, _request: AppendWalRequest) -> domain::error::Result<AppendOutcome> {
        std::future::pending::<()>().await;
        unreachable!()
    }

    fn describe(&self) -> String {
        "never-ack-test".to_string()
    }
}

/// 健康检查要带上身份、状态与已 durable 的 LSN。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_reports_identity_and_lsn() {
    let env = TestEnv::start(3).await;
    let mut client = env.connect().await;
    client
        .send(client.frame(
            10,
            "req-health",
            rt::frame::Message::Health(rt::HealthRequest {}),
        ))
        .await;
    let reply = client.recv_reply(10).await;
    let Some(rt::frame::Message::HealthResponse(health)) = reply.message else {
        panic!("期望 HealthResponse，实际 {:?}", reply.message);
    };
    assert_eq!(health.database_id, TEST_DB_ID);
    assert_eq!(health.state, "RUNNING");
    assert_eq!(health.active_sessions, 0);
    env.shutdown().await;
}

/// 从结果集里取第一行第一列的整数（本文件只用来断言 count）。
fn first_integer(frame: &rt::Frame) -> i64 {
    let row = result_of(frame).rows.first().expect("至少一行结果");
    match row.values.first().and_then(|value| value.kind.as_ref()) {
        Some(protocol::data::value::Kind::Integer(value)) => *value,
        other => panic!("期望整数，实际 {other:?}"),
    }
}

/// 回归（架构 §11.1 RPO = 0 / §11.2 failover 恢复）：**只有 Remote WAL、本地主库为空**
/// 的冷启动，已提交的数据必须一行不少地查得到。
///
/// 线上丢数据的路径正是这个组合：Worker 只搬 WAL 字节（它不解包引擎格式），库头
/// （page 1）从来没有进过 Remote WAL —— 它是引擎自己 fsync 进旧节点的 `db` 文件的。
/// 新节点上于是出现「WAL 有帧 + 主库零页」，引擎按 SQLite 的规则认定「这份 WAL 不属于
/// 这个库」，删掉 WAL 后开了个空库：HTTP 200、`wal_lsn` 一路推进，数据却在重启后消失。
///
/// 本用例不依赖 docker：本地文件 + 真引擎 + 真 UDS，把「新节点接管同一份 WAL」走完。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_start_with_empty_base_db_keeps_committed_rows() {
    // ---- 旧节点：建表 + 写入 20 行 ----
    let env = TestEnv::start(1).await;
    let mut client = env.connect().await;
    let created = client
        .execute(
            10,
            "CREATE TABLE t_rpo (id INTEGER PRIMARY KEY, v TEXT NOT NULL)",
        )
        .await;
    assert!(created.error.is_none(), "建表失败：{:?}", created.error);
    for id in 1..=20u64 {
        let inserted = client
            .execute(
                100 + id,
                &format!("INSERT INTO t_rpo (id, v) VALUES ({id}, 'row-{id}')"),
            )
            .await;
        assert!(
            inserted.error.is_none(),
            "写入 {id} 失败：{:?}",
            inserted.error
        );
    }
    assert_eq!(
        first_integer(&client.execute(200, "SELECT count(*) FROM t_rpo").await),
        20
    );

    // ---- 进程消失、工作集留在磁盘上（Worker 会把 WAL 字节留在新节点）----
    let dir = env.shutdown_keeping_dir().await;
    let db_path = dir.path().join("data").join("test.db");
    let wal_path = engine_adapter::wal_path_for(&db_path);
    let wal_len = std::fs::metadata(&wal_path)
        .expect("本地 WAL 必须存在")
        .len();
    assert!(wal_len > 0, "commit 必须写进本地 WAL");
    assert!(
        db_path.exists(),
        "旧节点上主库首页存在（这就是新节点缺的那一页）"
    );

    // 新节点只有回放出来的 WAL：主库文件为空（与线上现场一致：db=0B、db-wal 有字节）。
    std::fs::remove_file(&db_path).expect("移除主库文件");
    let shm_path = std::path::PathBuf::from(format!("{}-shm", db_path.display()));
    let _ = std::fs::remove_file(&shm_path);

    // ---- 新节点冷启动（新 epoch）：必须恢复出全部已提交数据 ----
    let env = TestEnv::start_in(dir, 2, |_| {}).await;
    let mut client = env.connect().await;
    let count = client.execute(10, "SELECT count(*) FROM t_rpo").await;
    assert!(
        count.error.is_none(),
        "主库为空时也必须恢复出表：{:?}",
        count.error
    );
    assert_eq!(
        first_integer(&count),
        20,
        "已 quorum durable 的 20 行必须一行不少地恢复出来"
    );
    let ids = client.execute(11, "SELECT id FROM t_rpo ORDER BY id").await;
    let restored: Vec<i64> = (0..20)
        .map(|index| {
            let row = &result_of(&ids).rows[index];
            match row.values.first().and_then(|value| value.kind.as_ref()) {
                Some(protocol::data::value::Kind::Integer(value)) => *value,
                other => panic!("期望整数，实际 {other:?}"),
            }
        })
        .collect();
    assert_eq!(restored, (1..=20).collect::<Vec<i64>>());

    // 恢复不是「假成功」：接手后的库还能继续写、写的内容也读得回来。
    let inserted = client
        .execute(12, "INSERT INTO t_rpo (id, v) VALUES (21, 'row-21')")
        .await;
    assert!(
        inserted.error.is_none(),
        "恢复后写入失败：{:?}",
        inserted.error
    );
    assert!(
        env.host.durable_lsn() > 0,
        "恢复后的 commit 同样必须拿到 durable 确认"
    );
    assert_eq!(
        first_integer(&client.execute(13, "SELECT count(*) FROM t_rpo").await),
        21
    );
    assert!(
        std::fs::metadata(&wal_path).expect("本地 WAL").len() > wal_len,
        "新提交必须落到同一份本地 WAL 的末端"
    );
    env.shutdown().await;
}

/// 快照协调帧：必须给出「确定且自洽」的快照点（架构 §11.4）。
///
/// 自洽的含义在这里被断言死：回帧的 `base_lsn` = 已 durable 的末端，且本地 WAL 的
/// `[0, base_lsn)` 确实存在 —— Worker 就是按这个长度裁剪上传的。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_request_reports_durable_prefix() {
    let env = TestEnv::start(1).await;
    let mut client = env.connect().await;
    client
        .execute(10, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await;
    for id in 1..=5 {
        let reply = client
            .execute(10 + id, &format!("INSERT INTO t (id) VALUES ({id})"))
            .await;
        assert!(reply.error.is_none(), "写入失败：{:?}", reply.error);
    }

    client
        .send(client.frame(
            20,
            "req-snapshot",
            rt::frame::Message::SnapshotRequest(rt::SnapshotRequest {
                snapshot_id: "snap-1".to_string(),
                // 0 = 「由 DB Process 报告」；manifest 的 base_lsn 取回帧值
                base_lsn: 0,
                object_key: String::new(),
            }),
        ))
        .await;
    let reply = client.recv_reply(20).await;
    assert!(reply.error.is_none(), "快照协调失败：{:?}", reply.error);
    let Some(rt::frame::Message::SnapshotResponse(response)) = reply.message else {
        panic!("期望 SnapshotResponse，实际 {:?}", reply.message);
    };
    assert_eq!(response.snapshot_id, "snap-1");
    assert_eq!(
        response.base_lsn,
        env.host.durable_lsn(),
        "快照基线必须是已 quorum durable 的末端"
    );
    assert!(response.base_lsn > 0, "写过数据之后基线必须前进");
    assert!(
        std::fs::metadata(env.wal_path()).expect("本地 WAL").len() >= response.base_lsn,
        "本地 WAL 必须覆盖 [0, base_lsn) 这段 durable 字节流"
    );

    // 快照不阻塞写入：拿到快照点之后继续写，LSN 继续前进。
    let written = client.execute(21, "INSERT INTO t (id) VALUES (99)").await;
    assert!(
        written.error.is_none(),
        "快照后写入失败：{:?}",
        written.error
    );
    assert!(env.host.durable_lsn() > response.base_lsn);

    env.shutdown().await;
}

/// 本地字节流与 durable 记账对不上时必须**显式失败**，绝不回一个「看似正常」的基线。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_request_rejects_wal_shorter_than_durable_lsn() {
    let env = TestEnv::start(1).await;
    let mut client = env.connect().await;
    client
        .execute(10, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await;
    let reply = client.execute(11, "INSERT INTO t (id) VALUES (1)").await;
    assert!(reply.error.is_none(), "写入失败：{:?}", reply.error);

    // 模拟「本地 WAL 被截断 / 与 durable 记账脱节」
    let wal = env.wal_path();
    assert!(std::fs::metadata(&wal).expect("本地 WAL").len() > 0);
    std::fs::write(&wal, b"").expect("截断本地 WAL");

    client
        .send(client.frame(
            20,
            "req-snapshot",
            rt::frame::Message::SnapshotRequest(rt::SnapshotRequest {
                snapshot_id: "snap-bad".to_string(),
                base_lsn: 0,
                object_key: String::new(),
            }),
        ))
        .await;
    let reply = client.recv_reply(20).await;
    assert_eq!(
        error_code_of(&reply),
        ErrorCode::InternalError,
        "对不上的字节流不能报成功：{:?}",
        reply.message
    );
    env.shutdown().await;
}
