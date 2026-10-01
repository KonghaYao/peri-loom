//! Worker Data Dispatcher <-> DB Process 的本地 UDS 传输层（架构 §17.7）。
//!
//! 冻结 wire format：`UnixStream + 4 字节大端长度前缀 + protobuf Frame`
//! （实现见 `protocol::framing`，本模块不重复实现编解码）。
//!
//! 本模块负责三件事：
//!
//! 1. **连接与握手**：连上 db-runtime 的 UDS 后交换 Hello / HelloAck，双方核对
//!    `database_id + owner_epoch`。epoch 不一致必须立刻判定失败 —— 这是防止「已被
//!    取代的旧 Owner 继续服务」的第一道本地防线（架构 §11.3）。
//! 2. **连接复用**：每个 DB 维护一组连接。读半连接（response 流）由 `reader` 互斥量
//!    独占（一个请求的响应必须被完整消费），写半连接（`writer`）只做短临界区写入，
//!    因此 **CancelNotice 可以在流式响应进行中从同一连接插队发出**。
//! 3. **背压**：本层不排队、不聚合。写入 `await` 到内核 buffer 满自然挂起；读取逐帧
//!    `await`，调用方（gRPC 流）不消费则不会读下一帧。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use protocol::framing::LocalFrameCodec;
use protocol::runtime_local as rt;
use tokio::net::UnixStream;
use tokio::sync::{Mutex, OwnedMutexGuard};
use tokio_util::codec::Framed;

use crate::error::{Result, WorkerError};
use crate::paths::socket_path;
use crate::registry::LocalDbRegistry;

/// 帧流（读半）。
pub type FrameStream = SplitStream<Framed<UnixStream, LocalFrameCodec>>;
/// 帧写半。
pub type FrameSink = SplitSink<Framed<UnixStream, LocalFrameCodec>, rt::Frame>;

/// 握手信息（DB Process 上报的自身状态）。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HelloInfo {
    /// 引擎版本。
    pub engine_version: String,
    /// 恢复时携带的快照 id。
    pub snapshot_id: String,
    /// 恢复到的 LSN。
    pub recovered_lsn: u64,
    /// DB Process 自报的 PID。
    pub pid: i64,
}

/// 连接池配置（从 WorkerConfig 派生，单独成结构便于单测）。
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// 本节点 worker id（握手时用于自报身份）。
    pub worker_id: String,
    /// UDS 所在目录（`WORKER_RUN_DIR`）。
    pub run_dir: PathBuf,
    /// 单个 DB 的最大并发连接数。
    pub max_connections_per_db: usize,
    /// 建链超时。
    pub connect_timeout: Duration,
    /// 握手超时。
    pub handshake_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            worker_id: "worker-1".to_string(),
            run_dir: PathBuf::from("/run/db-platform"),
            max_connections_per_db: 4,
            connect_timeout: Duration::from_secs(2),
            handshake_timeout: Duration::from_secs(5),
        }
    }
}

/// 一条到 DB Process 的 UDS 连接。
#[derive(Debug)]
pub struct UdsConnection {
    inner: Arc<ConnInner>,
}

#[derive(Debug)]
struct ConnInner {
    database_id: String,
    owner_epoch: u64,
    socket: PathBuf,
    /// 读半连接：被「正在接收响应」的请求独占。
    reader: Arc<Mutex<FrameStream>>,
    /// 写半连接：短临界区（发送请求帧 / 取消通知）。
    writer: Mutex<FrameSink>,
    /// 帧序号分配器（每个 Frame 的唯一 seq）。
    seq: AtomicU64,
    /// 连接是否已损坏（I/O 错误 / 握手失败 / 对端关闭）。
    broken: AtomicBool,
    hello: HelloInfo,
}

impl UdsConnection {
    /// 建立连接并完成握手。
    ///
    /// 失败时连接立即作废（不会进入池），避免把半死连接留给后续请求。
    pub async fn connect(
        socket: &Path,
        database_id: &str,
        owner_epoch: u64,
        worker_id: &str,
        connect_timeout: Duration,
        handshake_timeout: Duration,
    ) -> Result<Arc<Self>> {
        let stream = tokio::time::timeout(connect_timeout, UnixStream::connect(socket))
            .await
            .map_err(|_| {
                WorkerError::Uds(format!(
                    "连接 {} 超时（{}ms）",
                    socket.display(),
                    connect_timeout.as_millis()
                ))
            })?
            .map_err(|err| WorkerError::Uds(format!("连接 {} 失败：{err}", socket.display())))?;

        let mut framed = Framed::new(stream, LocalFrameCodec::new());
        let hello = handshake(
            &mut framed,
            database_id,
            owner_epoch,
            worker_id,
            handshake_timeout,
        )
        .await?;

        let (sink, stream) = framed.split();
        Ok(Arc::new(Self {
            inner: Arc::new(ConnInner {
                database_id: database_id.to_string(),
                owner_epoch,
                socket: socket.to_path_buf(),
                reader: Arc::new(Mutex::new(stream)),
                writer: Mutex::new(sink),
                seq: AtomicU64::new(1),
                broken: AtomicBool::new(false),
                hello,
            }),
        }))
    }

    /// 所属数据库 id。
    pub fn database_id(&self) -> &str {
        &self.inner.database_id
    }

    /// 建链时协商的 owner epoch。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn owner_epoch(&self) -> u64 {
        self.inner.owner_epoch
    }

    /// 本地 socket 路径。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn socket(&self) -> &Path {
        &self.inner.socket
    }

    /// 握手信息。
    pub fn hello(&self) -> &HelloInfo {
        &self.inner.hello
    }

    /// 连接是否已损坏。
    pub fn is_broken(&self) -> bool {
        self.inner.broken.load(Ordering::Acquire)
    }

    /// 标记连接损坏（对端异常时由调用方触发，必须停止复用）。
    pub fn mark_broken(&self) {
        self.inner.broken.store(true, Ordering::Release);
    }

    /// 分配一个新的帧序号。
    pub fn next_seq(&self) -> u64 {
        self.inner.seq.fetch_add(1, Ordering::AcqRel)
    }

    /// 构造一个属于本连接的请求帧（自动填充 db_id / epoch / seq）。
    pub fn frame_for(&self, request_id: &str, message: rt::frame::Message) -> rt::Frame {
        rt::Frame {
            seq: self.next_seq(),
            reply_to_seq: 0,
            request_id: request_id.to_string(),
            database_id: self.inner.database_id.clone(),
            owner_epoch: self.inner.owner_epoch,
            session_id: String::new(),
            transaction_id: String::new(),
            deadline_unix_ms: 0,
            error: None,
            message: Some(message),
        }
    }

    /// 占用读半连接，返回一个 lease。会等待直到有空闲。
    pub async fn lease(self: &Arc<Self>) -> Result<ConnectionLease> {
        if self.is_broken() {
            return Err(WorkerError::Uds(format!(
                "连接 {} 已损坏",
                self.inner.socket.display()
            )));
        }
        let guard = Arc::clone(&self.inner.reader).lock_owned().await;
        Ok(ConnectionLease {
            conn: Arc::clone(self),
            reader: guard,
        })
    }

    /// 尝试占用读半连接；被占用时返回 `None`（用于连接池挑选空闲连接）。
    pub fn try_lease(self: &Arc<Self>) -> Option<ConnectionLease> {
        if self.is_broken() {
            return None;
        }
        let guard = Arc::clone(&self.inner.reader).try_lock_owned().ok()?;
        Some(ConnectionLease {
            conn: Arc::clone(self),
            reader: guard,
        })
    }

    /// 发送一帧（不等待响应）。
    ///
    /// 写半连接只在发送期间被锁定，因此流式响应进行中依然可以插入 CancelNotice。
    pub async fn send(&self, frame: rt::Frame) -> Result<()> {
        if self.is_broken() {
            return Err(WorkerError::Uds("连接已损坏，拒绝发送".into()));
        }
        let mut sink = self.inner.writer.lock().await;
        if let Err(err) = sink.send(frame).await {
            self.mark_broken();
            return Err(WorkerError::Uds(format!("发送帧失败：{err}")));
        }
        Ok(())
    }
}

/// 一次「请求 -> 响应」期间的连接租约（独占读半）。
#[derive(Debug)]
pub struct ConnectionLease {
    conn: Arc<UdsConnection>,
    reader: OwnedMutexGuard<FrameStream>,
}

impl ConnectionLease {
    /// 所属连接。
    pub fn connection(&self) -> &Arc<UdsConnection> {
        &self.conn
    }

    /// 发送请求帧。
    pub async fn send(&self, frame: rt::Frame) -> Result<()> {
        self.conn.send(frame).await
    }

    /// 读取下一帧；对端正常关闭时返回 `Ok(None)`。
    ///
    /// `deadline` 为 `None` 表示不限时（长流由上游 gRPC deadline 控制）。
    pub async fn recv(&mut self, deadline: Option<Instant>) -> Result<Option<rt::Frame>> {
        let next = async { self.reader.next().await };
        let item = match deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline.into(), next).await {
                Ok(item) => item,
                Err(_) => {
                    self.conn.mark_broken();
                    return Err(WorkerError::Uds("等待 DB Process 响应超时".into()));
                }
            },
            None => next.await,
        };

        match item {
            None => {
                self.conn.mark_broken();
                Ok(None)
            }
            Some(Err(err)) => {
                self.conn.mark_broken();
                Err(WorkerError::Uds(format!("读取帧失败：{err}")))
            }
            Some(Ok(frame)) => Ok(Some(frame)),
        }
    }

    /// 读取下一帧并要求它存在（对端提前关闭即错误）。
    pub async fn recv_required(&mut self, deadline: Option<Instant>) -> Result<rt::Frame> {
        self.recv(deadline).await?.ok_or_else(|| {
            WorkerError::Uds(format!(
                "DB Process {} 在响应中途关闭连接",
                self.conn.database_id()
            ))
        })
    }
}

/// 握手：Dispatcher 先说 Hello，也兼容 DB Process 主动先说 Hello（两种顺序都接受）。
async fn handshake(
    framed: &mut Framed<UnixStream, LocalFrameCodec>,
    database_id: &str,
    owner_epoch: u64,
    worker_id: &str,
    timeout: Duration,
) -> Result<HelloInfo> {
    let hello = rt::Frame {
        seq: 1,
        reply_to_seq: 0,
        request_id: String::new(),
        database_id: database_id.to_string(),
        owner_epoch,
        session_id: String::new(),
        transaction_id: String::new(),
        deadline_unix_ms: 0,
        error: None,
        message: Some(rt::frame::Message::Hello(rt::Hello {
            database_id: database_id.to_string(),
            owner_epoch,
            process_id: worker_id.to_string(),
            pid: std::process::id() as i64,
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            local_socket: String::new(),
            snapshot_id: String::new(),
            recovered_lsn: 0,
            read_only: false,
        })),
    };
    framed
        .send(hello)
        .await
        .map_err(|err| WorkerError::Uds(format!("发送 Hello 失败：{err}")))?;

    let deadline = Instant::now() + timeout;
    // 允许跳过少量非握手帧（例如 DB Process 启动后立刻推送的 Health 通知）
    for _ in 0..8 {
        let frame = match tokio::time::timeout_at(deadline.into(), framed.next()).await {
            Ok(Some(Ok(frame))) => frame,
            Ok(Some(Err(err))) => {
                return Err(WorkerError::Uds(format!("握手期间读帧失败：{err}")));
            }
            Ok(None) => {
                return Err(WorkerError::Uds(
                    "DB Process 在握手完成前关闭了连接".to_string(),
                ));
            }
            Err(_) => {
                return Err(WorkerError::Uds(format!(
                    "等待 HelloAck 超时（{}ms）",
                    timeout.as_millis()
                )));
            }
        };

        if let Some(err) = frame.error.as_ref() {
            return Err(WorkerError::Uds(format!(
                "DB Process 拒绝握手：{}",
                err.message
            )));
        }

        match frame.message {
            Some(rt::frame::Message::HelloAck(ack)) => {
                if !ack.accepted {
                    // epoch 不一致 = 本 Dispatcher 持有的所有权已过期（或反之）
                    return Err(WorkerError::NotOwner {
                        db_id: database_id.to_string(),
                        requested: owner_epoch,
                        local: ack.dispatcher_epoch,
                    });
                }
                if ack.dispatcher_epoch != 0 && ack.dispatcher_epoch != owner_epoch {
                    return Err(WorkerError::EpochStale {
                        db_id: database_id.to_string(),
                        requested: owner_epoch,
                        local: ack.dispatcher_epoch,
                    });
                }
                return Ok(HelloInfo::default());
            }
            // DB Process 主动握手：回 HelloAck 确认
            Some(rt::frame::Message::Hello(peer)) => {
                if peer.database_id != database_id || peer.owner_epoch != owner_epoch {
                    let reject = rt::Frame {
                        seq: 0,
                        reply_to_seq: frame.seq,
                        request_id: frame.request_id.clone(),
                        database_id: database_id.to_string(),
                        owner_epoch,
                        session_id: String::new(),
                        transaction_id: String::new(),
                        deadline_unix_ms: 0,
                        error: None,
                        message: Some(rt::frame::Message::HelloAck(rt::HelloAck {
                            accepted: false,
                            worker_id: worker_id.to_string(),
                            dispatcher_epoch: owner_epoch,
                            reject_reason: format!(
                                "握手身份不一致：db={} epoch={}",
                                peer.database_id, peer.owner_epoch
                            ),
                        })),
                    };
                    let _ = framed.send(reject).await;
                    return Err(WorkerError::NotOwner {
                        db_id: database_id.to_string(),
                        requested: owner_epoch,
                        local: peer.owner_epoch,
                    });
                }
                let ack = rt::Frame {
                    seq: 0,
                    reply_to_seq: frame.seq,
                    request_id: frame.request_id.clone(),
                    database_id: database_id.to_string(),
                    owner_epoch,
                    session_id: String::new(),
                    transaction_id: String::new(),
                    deadline_unix_ms: 0,
                    error: None,
                    message: Some(rt::frame::Message::HelloAck(rt::HelloAck {
                        accepted: true,
                        worker_id: worker_id.to_string(),
                        dispatcher_epoch: owner_epoch,
                        reject_reason: String::new(),
                    })),
                };
                framed
                    .send(ack)
                    .await
                    .map_err(|err| WorkerError::Uds(format!("回复 HelloAck 失败：{err}")))?;
                return Ok(HelloInfo {
                    engine_version: peer.engine_version,
                    snapshot_id: peer.snapshot_id,
                    recovered_lsn: peer.recovered_lsn,
                    pid: peer.pid,
                });
            }
            // 其它帧：忽略继续等（保持握手健壮）
            _ => continue,
        }
    }

    Err(WorkerError::Uds("握手帧过多，未收到 HelloAck".to_string()))
}

// ---------------------------------------------------------------- 会话绑定

/// 会话 -> 连接绑定（显式会话必须固定在同一个 DB Process 连接上，事务状态才有效）。
#[derive(Debug, Clone)]
pub struct SessionBinding {
    /// 数据库 id。
    pub database_id: String,
    /// 会话 id。
    pub session_id: String,
    /// 绑定的连接。
    pub connection: Arc<UdsConnection>,
}

// ---------------------------------------------------------------- 连接池

/// 每 DB 的 UDS 连接池 + 会话绑定表。
#[derive(Debug)]
pub struct DbConnectionPool {
    cfg: PoolConfig,
    registry: Arc<LocalDbRegistry>,
    /// db_id -> 该 DB 的连接集合（懒创建）。
    databases: DashMap<String, Arc<Mutex<Vec<Arc<UdsConnection>>>>>,
    /// session_id -> 绑定连接。
    sessions: DashMap<String, SessionBinding>,
}

impl DbConnectionPool {
    /// 构造连接池。
    pub fn new(cfg: PoolConfig, registry: Arc<LocalDbRegistry>) -> Self {
        Self {
            cfg,
            registry,
            databases: DashMap::new(),
            sessions: DashMap::new(),
        }
    }

    /// 配置（只读）。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn config(&self) -> &PoolConfig {
        &self.cfg
    }

    /// 当前缓存的连接数（诊断 / 测试用；不阻塞，拿不到锁时返回 0）。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn cached_connection_count(&self, db_id: &str) -> usize {
        self.databases
            .get(db_id)
            .map(|entry| entry.try_lock().map(|guards| guards.len()).unwrap_or(0))
            .unwrap_or(0)
    }

    /// 当前绑定的会话数。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// 解析本地 socket 路径。
    pub fn socket_of(&self, db_id: &str) -> PathBuf {
        socket_path(&self.cfg.run_dir, db_id)
    }

    /// 取一个可用连接租约（无空闲连接时新建；达到上限则等待最老的连接释放）。
    pub async fn lease(&self, db_id: &str) -> Result<ConnectionLease> {
        let entry = self.registry.check_serving(db_id)?;
        let socket = self.socket_of(db_id);
        let slot = self.db_slot(db_id);

        // 最多两轮：第一轮复用空闲连接，第二轮等待或新建
        for attempt in 0..2 {
            let candidate = {
                let mut connections = slot.lock().await;
                connections.retain(|conn| !conn.is_broken());
                if let Some(idle) = connections.iter().find_map(|conn| conn.try_lease()) {
                    return Ok(idle);
                }
                if connections.len() < self.cfg.max_connections_per_db {
                    None
                } else {
                    // 达到上限：挑一个连接等它空闲（近似的 FIFO）
                    connections.first().cloned()
                }
            };

            match candidate {
                Some(conn) => {
                    // 等待该连接空闲：这是天然的并发背压（同 DB 并发超过上限时排队）
                    let lease = match conn.lease().await {
                        Ok(lease) => lease,
                        Err(err) if attempt == 0 => {
                            tracing::debug!(db_id = %db_id, error = %err, "复用连接失败，重试");
                            continue;
                        }
                        Err(err) => return Err(err),
                    };
                    return Ok(lease);
                }
                None => {
                    let conn = self.connect(db_id, &socket, entry.owner_epoch).await?;
                    let mut connections = slot.lock().await;
                    connections.push(Arc::clone(&conn));
                    return conn
                        .try_lease()
                        .ok_or_else(|| WorkerError::Uds("新建连接无法租用".to_string()));
                }
            }
        }

        Err(WorkerError::Uds(format!("为 {db_id} 获取连接失败")))
    }

    /// 在指定连接上取租约（会话固定连接的场景）。
    pub async fn lease_on(&self, conn: &Arc<UdsConnection>) -> Result<ConnectionLease> {
        conn.lease().await
    }

    /// 取一条（可能是缓存复用的）连接，用于握手信息查询 / 优雅关闭通知。
    pub async fn connection(&self, db_id: &str) -> Result<Arc<UdsConnection>> {
        let entry = self.registry.check_serving(db_id)?;
        let socket = self.socket_of(db_id);
        let slot = self.db_slot(db_id);
        {
            let mut connections = slot.lock().await;
            connections.retain(|conn| !conn.is_broken());
            if let Some(conn) = connections.first() {
                return Ok(Arc::clone(conn));
            }
        }
        let conn = self.connect(db_id, &socket, entry.owner_epoch).await?;
        slot.lock().await.push(Arc::clone(&conn));
        Ok(conn)
    }

    /// 强制新建一条连接（显式会话必须独占连接，避免事务状态被其它请求干扰）。
    pub async fn new_connection(&self, db_id: &str) -> Result<Arc<UdsConnection>> {
        let entry = self.registry.check_serving(db_id)?;
        let socket = self.socket_of(db_id);
        let conn = self.connect(db_id, &socket, entry.owner_epoch).await?;
        let slot = self.db_slot(db_id);
        let mut connections = slot.lock().await;
        connections.retain(|existing| !existing.is_broken());
        connections.push(Arc::clone(&conn));
        Ok(conn)
    }

    /// 底层建链（含握手）。
    async fn connect(&self, db_id: &str, socket: &Path, epoch: u64) -> Result<Arc<UdsConnection>> {
        UdsConnection::connect(
            socket,
            db_id,
            epoch,
            &self.cfg.worker_id,
            self.cfg.connect_timeout,
            self.cfg.handshake_timeout,
        )
        .await
    }

    /// 把一条已建好的连接放入池中（Process Supervisor 在 READY 校验后调用，
    /// 避免「注册表尚未标记可服务 -> 池拒绝建链」的死锁）。
    pub async fn adopt(self: &Arc<Self>, conn: Arc<UdsConnection>) {
        let slot = self.db_slot(conn.database_id());
        let mut connections = slot.lock().await;
        connections.retain(|existing| !existing.is_broken());
        if !connections
            .iter()
            .any(|existing| Arc::ptr_eq(existing, &conn))
        {
            connections.push(conn);
        }
    }

    /// 绑定会话到连接。
    pub fn bind_session(&self, binding: SessionBinding) {
        self.sessions.insert(binding.session_id.clone(), binding);
    }

    /// 查询会话连接。
    pub fn session(&self, session_id: &str) -> Option<SessionBinding> {
        self.sessions.get(session_id).map(|entry| entry.clone())
    }

    /// 解绑会话（CloseSession / 连接断开 / 进程退出）。
    pub fn unbind_session(&self, session_id: &str) -> Option<SessionBinding> {
        self.sessions.remove(session_id).map(|(_, value)| value)
    }

    /// 丢弃某个 DB 的全部连接与会话（进程退出、重启、迁移后必须调用）。
    pub fn drop_database(&self, db_id: &str) {
        if let Some((_, slot)) = self.databases.remove(db_id) {
            let removed = slot.try_lock().map(|guards| guards.len()).unwrap_or(0);
            tracing::debug!(db_id = %db_id, connections = removed, "清理本地 UDS 连接");
        }
        self.sessions
            .retain(|_key, binding| binding.database_id != db_id);
    }

    /// 按会话 id 反查连接（Cancel 需要向会话所在连接下发通知）。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn connection_for_session(&self, session_id: &str) -> Option<Arc<UdsConnection>> {
        self.sessions
            .get(session_id)
            .map(|binding| Arc::clone(&binding.connection))
    }

    /// 遍历某个 DB 的缓存连接（Cancel 查找在途请求所属连接用）。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn connections_of(&self, db_id: &str) -> Vec<Arc<UdsConnection>> {
        self.databases
            .get(db_id)
            .map(|slot| match slot.try_lock() {
                Ok(guards) => guards.clone(),
                Err(_) => Vec::new(),
            })
            .unwrap_or_default()
    }

    fn db_slot(&self, db_id: &str) -> Arc<Mutex<Vec<Arc<UdsConnection>>>> {
        if let Some(slot) = self.databases.get(db_id) {
            return Arc::clone(&slot);
        }
        let slot = Arc::new(Mutex::new(Vec::new()));
        self.databases
            .entry(db_id.to_string())
            .or_insert_with(|| Arc::clone(&slot))
            .clone()
    }
}

/// 连接统计（诊断）。
#[allow(dead_code)]
// 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
#[derive(Clone, Debug, Default)]
pub struct PoolStats {
    /// 缓存的 DB 数。
    pub databases: usize,
    /// 缓存的会话数。
    pub sessions: usize,
    /// 每 DB 连接数。
    pub connections: HashMap<String, usize>,
}

#[cfg(test)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::LocalDatabase;
    use domain::lifecycle::LifecycleState;
    use domain::resources::ResourceBudget;
    use domain::time::now_unix_ms;
    use tokio::net::UnixListener;

    /// 进程内伪造 db-runtime：完成握手后按 `responder` 回帧。
    ///
    /// 只依赖本机 UDS，不需要网络与外部进程，因此默认参与测试。
    struct FakeRuntime {
        socket: PathBuf,
        handle: tokio::task::JoinHandle<()>,
    }

    impl Drop for FakeRuntime {
        fn drop(&mut self) {
            self.handle.abort();
            let _ = std::fs::remove_file(&self.socket);
        }
    }

    /// 行为：给定请求帧，返回要回写的帧（None = 主动断开）。
    type Responder = fn(&rt::Frame) -> Option<rt::Frame>;

    fn registry_with(db_id: &str, epoch: u64, socket: &Path) -> Arc<LocalDbRegistry> {
        let registry = Arc::new(LocalDbRegistry::new());
        registry.register(LocalDatabase {
            database_id: db_id.to_string(),
            state: LifecycleState::Warm,
            pid: Some(4242),
            local_socket: socket.to_path_buf(),
            owner_epoch: epoch,
            budget: ResourceBudget::new(1000, 256, 64, 1024, 1, 100),
            started_at_unix_ms: now_unix_ms(),
            last_activity_unix_ms: now_unix_ms(),
            crash_count: 0,
            cgroup: None,
            restored_from: None,
            read_only: false,
        });
        registry
    }

    fn pool_for(
        dir: &Path,
        db_id: &str,
        epoch: u64,
    ) -> (Arc<DbConnectionPool>, Arc<LocalDbRegistry>) {
        let registry = registry_with(db_id, epoch, &dir.join(format!("{db_id}.sock")));
        let pool = DbConnectionPool::new(
            PoolConfig {
                run_dir: dir.to_path_buf(),
                connect_timeout: Duration::from_millis(500),
                handshake_timeout: Duration::from_millis(500),
                ..Default::default()
            },
            Arc::clone(&registry),
        );
        (Arc::new(pool), registry)
    }

    fn echo_responder(frame: &rt::Frame) -> Option<rt::Frame> {
        let reply = rt::Frame {
            seq: 0,
            reply_to_seq: frame.seq,
            request_id: frame.request_id.clone(),
            database_id: frame.database_id.clone(),
            owner_epoch: frame.owner_epoch,
            message: Some(rt::frame::Message::HelloAck(rt::HelloAck {
                accepted: true,
                worker_id: "fake-worker".into(),
                dispatcher_epoch: frame.owner_epoch,
                reject_reason: String::new(),
            })),
            ..Default::default()
        };
        Some(reply)
    }

    #[test]
    fn pool_config_defaults_are_local_only() {
        let cfg = PoolConfig::default();
        assert_eq!(cfg.max_connections_per_db, 4);
        assert!(cfg.connect_timeout <= Duration::from_secs(5));
        assert_eq!(cfg.worker_id, "worker-1");
    }

    #[test]
    fn frame_layout_uses_registry_socket_path() {
        let pool = DbConnectionPool::new(
            PoolConfig {
                run_dir: PathBuf::from("/run/db-platform"),
                ..Default::default()
            },
            Arc::new(LocalDbRegistry::new()),
        );
        assert_eq!(
            pool.socket_of("db-1"),
            PathBuf::from("/run/db-platform/sockets/db-1.sock")
        );
        // 穿越尝试被消毒
        assert_eq!(
            pool.socket_of("../../x"),
            PathBuf::from("/run/db-platform/sockets/.._.._x.sock")
        );
    }

    #[tokio::test]
    async fn lease_requires_registered_serving_database() {
        let dir = tempfile::tempdir().unwrap();
        let pool = DbConnectionPool::new(
            PoolConfig {
                run_dir: dir.path().to_path_buf(),
                ..Default::default()
            },
            Arc::new(LocalDbRegistry::new()),
        );

        // 未注册 -> 拒绝（NOT_OWNER：请求被路由到了错误的 Worker）
        let err = pool.lease("db-1").await.unwrap_err();
        assert_eq!(err.code(), domain::error::ErrorCode::NotOwner);
    }

    #[tokio::test]
    async fn handshake_and_lease_over_real_uds() {
        // 伪造运行时的 socket 必须落在 PoolConfig.run_dir/sockets 下
        let run_dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(run_dir.path().join(crate::paths::SOCKET_SUBDIR)).unwrap();
        let db_id = "db-handshake";
        let runtime = spawn_fake_runtime_at(
            run_dir
                .path()
                .join(crate::paths::SOCKET_SUBDIR)
                .join(format!("{db_id}.sock")),
            db_id,
            11,
            echo_responder,
        )
        .await;

        let (pool, _registry) = pool_for(run_dir.path(), db_id, 11);
        let mut lease = pool.lease(db_id).await.expect("建链并握手");
        assert_eq!(lease.connection().owner_epoch(), 11);
        assert_eq!(lease.connection().database_id(), db_id);
        assert!(!lease.connection().is_broken());

        // 发一帧（伪造运行时回 HelloAck），验证帧收发
        let frame = lease
            .connection()
            .frame_for("req-1", rt::frame::Message::Health(rt::HealthRequest {}));
        lease.send(frame).await.unwrap();
        let reply = lease
            .recv(Some(Instant::now() + Duration::from_secs(1)))
            .await
            .unwrap()
            .expect("应收到回帧");
        assert_eq!(reply.request_id, "req-1");
        drop(lease);

        // 连接复用：第二次租约应复用同一条连接
        let again = pool.lease(db_id).await.unwrap();
        assert_eq!(pool.cached_connection_count(db_id), 1);
        drop(again);
        drop(runtime);
    }

    #[tokio::test]
    async fn handshake_rejects_epoch_mismatch() {
        let run_dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(run_dir.path().join(crate::paths::SOCKET_SUBDIR)).unwrap();
        let db_id = "db-epoch";
        let _runtime = spawn_fake_runtime_at(
            run_dir
                .path()
                .join(crate::paths::SOCKET_SUBDIR)
                .join(format!("{db_id}.sock")),
            db_id,
            // 对端 epoch 与本端注册表不一致
            99,
            echo_responder,
        )
        .await;

        let (pool, _registry) = pool_for(run_dir.path(), db_id, 11);
        let err = pool.lease(db_id).await.unwrap_err();
        assert_eq!(err.code(), domain::error::ErrorCode::EpochMismatch);
    }

    #[tokio::test]
    async fn drop_database_clears_connections_and_sessions() {
        let run_dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(run_dir.path().join(crate::paths::SOCKET_SUBDIR)).unwrap();
        let db_id = "db-drop";
        let _runtime = spawn_fake_runtime_at(
            run_dir
                .path()
                .join(crate::paths::SOCKET_SUBDIR)
                .join(format!("{db_id}.sock")),
            db_id,
            3,
            echo_responder,
        )
        .await;

        let (pool, _registry) = pool_for(run_dir.path(), db_id, 3);
        let conn = pool.new_connection(db_id).await.expect("建链");
        pool.bind_session(SessionBinding {
            database_id: db_id.to_string(),
            session_id: "sess-1".into(),
            connection: Arc::clone(&conn),
        });
        assert_eq!(pool.session_count(), 1);
        assert!(pool.connection_for_session("sess-1").is_some());

        pool.drop_database(db_id);
        assert_eq!(pool.session_count(), 0);
        assert!(pool.session("sess-1").is_none());
        assert_eq!(pool.cached_connection_count(db_id), 0);
        assert!(pool.connections_of(db_id).is_empty());
    }

    #[tokio::test]
    async fn connect_to_missing_socket_fails_fast() {
        let run_dir = tempfile::tempdir().unwrap();
        let (pool, _registry) = pool_for(run_dir.path(), "db-absent", 1);
        let started = Instant::now();
        let err = pool.lease("db-absent").await.unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(matches!(err, WorkerError::Uds(_)), "实际错误：{err}");
    }

    /// 在指定路径启动伪造运行时（测试里 socket 路径需要与连接池推导一致）。
    async fn spawn_fake_runtime_at(
        socket: PathBuf,
        db_id: &str,
        epoch: u64,
        responder: Responder,
    ) -> FakeRuntime {
        let _ = std::fs::remove_file(&socket);
        let listener = UnixListener::bind(&socket).unwrap();
        let db_id = db_id.to_string();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let db_id = db_id.clone();
                tokio::spawn(async move {
                    while let Ok(Some(frame)) = protocol::framing::read_frame(&mut stream).await {
                        if matches!(frame.message, Some(rt::frame::Message::Hello(_))) {
                            let ack = rt::Frame {
                                seq: 0,
                                reply_to_seq: frame.seq,
                                request_id: frame.request_id.clone(),
                                database_id: db_id.clone(),
                                owner_epoch: epoch,
                                message: Some(rt::frame::Message::HelloAck(rt::HelloAck {
                                    accepted: true,
                                    worker_id: "fake-worker".into(),
                                    dispatcher_epoch: epoch,
                                    reject_reason: String::new(),
                                })),
                                ..Default::default()
                            };
                            if protocol::framing::write_frame(&mut stream, &ack)
                                .await
                                .is_err()
                            {
                                break;
                            }
                            continue;
                        }
                        match responder(&frame) {
                            Some(reply) => {
                                if protocol::framing::write_frame(&mut stream, &reply)
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                });
            }
        });
        // 让 accept 循环先跑起来，避免 connect 早于 bind（listen 已完成，connect 不会失败）
        tokio::task::yield_now().await;
        FakeRuntime { socket, handle }
    }
}
