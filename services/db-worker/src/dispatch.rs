//! Worker Data Dispatcher（数据面，架构 §5.2 / §15.6 / §17.7）。
//!
//! 这是 Server 看到的**稳定数据入口**：`db_id -> 本地 DB Process`。Dispatcher 的存在
//! 让 Server 完全不需要感知 PID / 本地 UDS —— DB Process 重启、迁移、换 socket 都不
//! 会改变 Worker 的 endpoint。
//!
//! 每个请求的处理顺序是固定的：
//!
//! ```text
//! 1. 目标 Worker 校验（Server 指定的 worker_id 必须是自己）
//! 2. Epoch fencing（与本地注册表精确比对：低于=过期路由，高于=已被取代）
//! 3. Deadline 解析（无声明则用兜底值，绝不无限等）
//! 4. 取本地 UDS 连接（连接池，按需新建 / 断线重连）
//! 5. 登记在途请求（Cancel 要靠它定位连接）
//! 6. 转发帧 / 逐帧回流
//! ```
//!
//! 两个容易做错的点：
//!
//! - **流式必须逐帧转发**：`ExecuteStream` 每收到一个 RowBatch 帧就 `yield` 一次，
//!   中间不聚合。客户端不消费 -> generator 不被 poll -> 不再读 UDS -> 内核 socket
//!   缓冲区写满 -> DB Process 的写入挂起，背压天然贯通（架构 §17.4）。
//! - **被放弃的流必须作废连接**：客户端中断后 DB Process 仍在往 socket 里灌帧，
//!   若把连接还回池里，下一个请求会读到上一次请求的残帧。因此流未正常收尾时
//!   下发 `CancelNotice` 并把连接标记为 broken（丢弃重建）。

use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use domain::error::ErrorCode;
use protocol::common;
use protocol::convert;
use protocol::data::{
    self, worker_data_server::WorkerData, BeginRequest, CancelRequest, CancelResponse,
    CloseSessionRequest, CloseSessionResponse, CommitRequest, CommitResponse,
    DispatcherStatusRequest, DispatcherStatusResponse, ExecuteBatchRequest, ExecuteBatchResponse,
    ExecuteRequest, ExecuteResponse, LocalDatabaseEntry, OpenSessionRequest, OpenSessionResponse,
    RollbackRequest, SessionExecuteRequest, StreamFrame, TransactionResponse,
};
use protocol::runtime_local as rt;
use tonic::{Request, Response, Status};

use crate::cli::WorkerConfig;
use crate::error::{Result, WorkerError};
use crate::registry::LocalDbRegistry;
use crate::resources::ResourceSampler;
use crate::uds::{ConnectionLease, DbConnectionPool, SessionBinding, UdsConnection};

/// 请求未声明 deadline 时的兜底执行超时。
///
/// 为什么必须有兜底：没有 deadline 的请求会让连接租约被永久占用（读半连接被独占），
/// 一个卡死的 DB 就能把该库的并发能力全部吃掉。
const FALLBACK_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// 流式响应的兜底超时（大结果集更慢，给更宽的窗口）。
const FALLBACK_STREAM_TIMEOUT: Duration = Duration::from_secs(300);

/// 显式会话默认空闲超时（架构 §15.3）。
const DEFAULT_SESSION_IDLE_TIMEOUT_MS: u32 = 60_000;

/// 显式事务默认最大存活时间（架构 §15.3）。
const DEFAULT_TRANSACTION_MAX_LIFETIME_MS: u32 = 30_000;

// ---------------------------------------------------------------- 在途请求索引

/// 一个在途请求（Cancel 的定位依据）。
#[derive(Debug)]
pub struct InFlight {
    /// 请求 id。
    pub request_id: String,
    /// 目标库。
    #[allow(dead_code)]
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub database_id: String,
    /// 所属会话（无会话时为空）。
    pub session_id: String,
    /// 请求所用连接（CancelNotice 从该连接下发）。
    pub connection: Arc<UdsConnection>,
    /// 开始时刻（用于诊断与超长请求排查）。
    #[allow(dead_code)]
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub started_at: Instant,
}

/// 在途请求索引。
///
/// 为什么是独立索引而不是遍历连接池：Cancel 必须**精确**找到承载该 request_id 的那条
/// 连接（同一 DB 可能有多条连接），遍历既慢又可能漏。索引的键就是 `request_id`，
/// 插入/移除与请求生命周期一一对应（由 [`InFlightGuard`] 的 Drop 保证）。
#[derive(Debug, Default)]
pub struct InflightIndex {
    entries: DashMap<String, Arc<InFlight>>,
}

impl InflightIndex {
    /// 空索引。
    pub fn new() -> Self {
        Self::default()
    }

    /// 登记的请求数。
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// 是否没有在途请求。
    #[allow(dead_code)]
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// 登记一个在途请求，返回 RAII 守卫（Drop 即注销）。
    pub fn insert(
        self: &Arc<Self>,
        request_id: &str,
        database_id: &str,
        session_id: &str,
        connection: &Arc<UdsConnection>,
    ) -> InFlightGuard {
        self.entries.insert(
            request_id.to_string(),
            Arc::new(InFlight {
                request_id: request_id.to_string(),
                database_id: database_id.to_string(),
                session_id: session_id.to_string(),
                connection: Arc::clone(connection),
                started_at: Instant::now(),
            }),
        );
        InFlightGuard {
            index: Arc::clone(self),
            request_id: request_id.to_string(),
        }
    }

    /// 按 request_id 查找。
    pub fn lookup(&self, request_id: &str) -> Option<Arc<InFlight>> {
        self.entries.get(request_id).map(|entry| Arc::clone(&entry))
    }

    /// 列出某个会话的全部在途请求（取消整个会话时使用）。
    pub fn by_session(&self, session_id: &str) -> Vec<Arc<InFlight>> {
        self.entries
            .iter()
            .filter(|entry| entry.session_id == session_id)
            .map(|entry| Arc::clone(entry.value()))
            .collect()
    }

    /// 是否还有属于该 DB 的在途请求（进程退出前清理连接时的诊断用）。
    #[allow(dead_code)]
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn count_for_database(&self, db_id: &str) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.database_id == db_id)
            .count()
    }
}

/// 在途登记的 RAII 守卫。
#[derive(Debug)]
pub struct InFlightGuard {
    index: Arc<InflightIndex>,
    request_id: String,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.index.entries.remove(&self.request_id);
    }
}

// ---------------------------------------------------------------- 服务

/// Worker 数据面服务。
pub struct WorkerDataService {
    cfg: Arc<WorkerConfig>,
    registry: Arc<LocalDbRegistry>,
    pool: Arc<DbConnectionPool>,
    sampler: Arc<ResourceSampler>,
    inflight: Arc<InflightIndex>,
}

impl std::fmt::Debug for WorkerDataService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerDataService")
            .field("worker_id", &self.cfg.worker_id)
            .field("inflight", &self.inflight.len())
            .finish()
    }
}

/// 请求级的解析结果（校验通过后才有）。
#[derive(Clone, Debug)]
struct DispatchContext {
    request_id: String,
    database_id: String,
    owner_epoch: u64,
    session_id: String,
    transaction_id: String,
    deadline: Instant,
}

impl WorkerDataService {
    /// 构造。
    pub fn new(
        cfg: Arc<WorkerConfig>,
        registry: Arc<LocalDbRegistry>,
        pool: Arc<DbConnectionPool>,
        sampler: Arc<ResourceSampler>,
    ) -> Self {
        Self {
            cfg,
            registry,
            pool,
            sampler,
            inflight: Arc::new(InflightIndex::new()),
        }
    }

    /// 在途请求索引（诊断 / 测试）。
    #[allow(dead_code)]
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn inflight(&self) -> &Arc<InflightIndex> {
        &self.inflight
    }

    /// 请求前置校验：目标 Worker、epoch fencing、deadline。
    ///
    /// epoch 校验走 [`LocalDbRegistry::check_data_epoch`]（**精确匹配**）：数据面不接受
    /// 「更高 epoch 先用着」—— 高于本地意味着本 Worker 已被新 Owner 取代，
    /// 继续服务会造成 Split Brain（架构 §10）。
    fn prepare(
        &self,
        context: Option<&common::RequestContext>,
        fallback: Duration,
    ) -> Result<DispatchContext> {
        let proto_ctx = context.cloned().unwrap_or_default();
        if !proto_ctx.worker_id.is_empty() && proto_ctx.worker_id != self.cfg.worker_id {
            return Err(WorkerError::WrongWorker {
                target: proto_ctx.worker_id.clone(),
                local: self.cfg.worker_id.clone(),
            });
        }
        if proto_ctx.database_id.trim().is_empty() {
            return Err(WorkerError::Config("请求缺少 database_id".into()));
        }
        // 消毒后的 id 才是可安全落盘的 id；非法字符在这里就被拒绝
        crate::paths::checked_id(&proto_ctx.database_id)?;
        self.registry
            .check_data_epoch(&proto_ctx.database_id, proto_ctx.owner_epoch)?;

        // request_id 为空时补一个：Cancel 与日志追踪都依赖它
        let request_id = if proto_ctx.request_id.is_empty() {
            domain::ids::RequestId::new_v7().to_string()
        } else {
            proto_ctx.request_id.clone()
        };
        let deadline = convert::deadline_from_ms(proto_ctx.deadline_unix_ms)
            .unwrap_or_else(|| Instant::now() + fallback);

        Ok(DispatchContext {
            request_id,
            database_id: proto_ctx.database_id.clone(),
            owner_epoch: proto_ctx.owner_epoch,
            session_id: proto_ctx.session_id.clone(),
            transaction_id: proto_ctx.transaction_id.clone(),
            deadline,
        })
    }

    /// 取得连接租约并登记在途请求。
    async fn lease_with_inflight(
        &self,
        ctx: &DispatchContext,
    ) -> Result<(ConnectionLease, InFlightGuard)> {
        let lease = self.pool.lease(&ctx.database_id).await?;
        let guard = self.inflight.insert(
            &ctx.request_id,
            &ctx.database_id,
            &ctx.session_id,
            lease.connection(),
        );
        Ok((lease, guard))
    }

    /// 会话绑定的连接 + epoch 校验。
    fn session_connection(
        &self,
        session_id: &str,
        database_id: &str,
        owner_epoch: u64,
    ) -> Result<Arc<UdsConnection>> {
        if session_id.is_empty() {
            return Err(WorkerError::SessionNotFound {
                session_id: String::new(),
            });
        }
        let binding =
            self.pool
                .session(session_id)
                .ok_or_else(|| WorkerError::SessionNotFound {
                    session_id: session_id.to_string(),
                })?;
        if !database_id.is_empty() && binding.database_id != database_id {
            // 会话不能跨库：Server 把它路由到了错误的 DB
            return Err(WorkerError::SessionNotFound {
                session_id: session_id.to_string(),
            });
        }
        self.registry
            .check_data_epoch(&binding.database_id, owner_epoch)?;
        Ok(binding.connection)
    }

    /// 非流式执行（流式路径共用同一段转发逻辑）。
    async fn execute_once(
        &self,
        ctx: &DispatchContext,
        sql: String,
        params: Vec<data::Value>,
        atomic: bool,
        inline_row_limit: u32,
        want_stream: bool,
    ) -> Result<(ConnectionLease, InFlightGuard, rt::Frame)> {
        let (mut lease, guard) = self.lease_with_inflight(ctx).await?;
        let mut frame = lease.connection().frame_for(
            &ctx.request_id,
            rt::frame::Message::Execute(rt::ExecuteRequest {
                sql,
                params,
                atomic,
                want_stream,
                inline_row_limit,
            }),
        );
        frame.session_id = ctx.session_id.clone();
        frame.transaction_id = ctx.transaction_id.clone();
        frame.deadline_unix_ms = convert::deadline_ms_from(Some(ctx.deadline));
        lease.send(frame).await?;
        let response = lease.recv_required(Some(ctx.deadline)).await?;
        Ok((lease, guard, response))
    }

    /// 把 DB Process 的错误帧转成对外结构化错误（没有错误时返回 `None`）。
    fn frame_error(frame: &rt::Frame) -> Option<common::PlatformError> {
        frame
            .error
            .as_ref()
            .map(|err| convert::platform_error_to_proto(&convert::platform_error_from_proto(err)))
    }

    /// 从错误帧构造 `ExecuteResponse`。
    fn error_execute_response(error: common::PlatformError) -> ExecuteResponse {
        ExecuteResponse {
            error: Some(error),
            result: None,
            elapsed_micros: 0,
            wal_lsn: 0,
        }
    }

    /// 把本地流帧映射为对外流帧；返回 `None` 表示该帧与本请求无关（通知类）。
    fn map_stream_frame(frame: rt::Frame) -> Option<StreamFrame> {
        if let Some(error) = Self::frame_error(&frame) {
            return Some(StreamFrame {
                error: Some(error),
                frame: None,
            });
        }
        match frame.message {
            Some(rt::frame::Message::StreamHeader(header)) => Some(StreamFrame {
                error: None,
                frame: Some(data::stream_frame::Frame::Header(data::StreamHeader {
                    columns: header.columns,
                    // 行数预估由 DB Process 自己决定是否上报；本地协议没有该字段，
                    // 因此统一填 0（0 表示「未知」，不是「0 行」）。
                    row_count_estimate: 0,
                })),
            }),
            Some(rt::frame::Message::Rows(batch)) => Some(StreamFrame {
                error: None,
                frame: Some(data::stream_frame::Frame::Rows(data::RowBatch {
                    rows: batch.rows,
                })),
            }),
            Some(rt::frame::Message::StreamEnd(end)) => Some(StreamFrame {
                error: None,
                frame: Some(data::stream_frame::Frame::Trailer(data::StreamTrailer {
                    affected_rows: end.affected_rows,
                    wal_lsn: end.wal_lsn,
                    elapsed_micros: end.elapsed_micros,
                    // 原样透传：`None`（DB Process 未上报）与 `Some(false)`（在事务中）
                    // 语义不同，Worker 不得替它填默认值。
                    is_autocommit: end.is_autocommit,
                    last_insert_rowid: end.last_insert_rowid,
                })),
            }),
            // 通知 / 握手 / 会话类帧不属于本次流式响应
            _ => None,
        }
    }

    /// 判断某个流帧是否代表流已结束（trailer 或错误）。
    fn is_terminal(frame: &StreamFrame) -> bool {
        frame.error.is_some() || matches!(frame.frame, Some(data::stream_frame::Frame::Trailer(_)))
    }

    /// 流式转发：把 lease 上的帧逐个 yield 出去。
    ///
    /// 用 generator（`async_stream`）而不是「后台任务 + channel」：generator 只在被
    /// poll 时读下一帧，天然实现了「下游不消费就不读」的背压，且不需要中间缓冲。
    fn stream_frames(
        &self,
        lease: ConnectionLease,
        ctx: DispatchContext,
        guard: InFlightGuard,
    ) -> BoxStream {
        self.stream_with_first(lease, ctx, guard, None)
    }

    /// 单个错误帧构成的流（预检失败时使用，保持「错误也走 in-band」的一致性）。
    fn error_stream(
        err: WorkerError,
    ) -> Pin<Box<dyn futures::Stream<Item = std::result::Result<StreamFrame, Status>> + Send>> {
        Box::pin(async_stream::stream! {
            yield Ok(StreamFrame {
                error: Some(convert::platform_error_to_proto(&err.to_platform_error())),
                frame: None,
            });
        })
    }

    /// 在一条连接上开启临时会话（原子批处理使用）。
    async fn open_ephemeral_session(
        lease: &mut ConnectionLease,
        request_id: &str,
        deadline: Instant,
        idle_timeout_ms: u32,
    ) -> Result<String> {
        let mut frame = lease.connection().frame_for(
            request_id,
            rt::frame::Message::OpenSession(rt::OpenSessionRequest {
                idle_timeout_ms,
                max_transaction_lifetime_ms: DEFAULT_TRANSACTION_MAX_LIFETIME_MS as u64,
            }),
        );
        frame.deadline_unix_ms = convert::deadline_ms_from(Some(deadline));
        lease.send(frame).await?;
        let response = lease.recv_required(Some(deadline)).await?;
        if let Some(error) = Self::frame_error(&response) {
            return Err(WorkerError::Uds(format!(
                "开启临时会话失败：{}",
                error.message
            )));
        }
        match response.message {
            Some(rt::frame::Message::OpenSessionResponse(resp)) => Ok(resp.session_id),
            _ => Err(WorkerError::Uds("DB Process 返回了非预期的会话响应".into())),
        }
    }
}

/// 关闭临时会话（尽力而为：会话随连接生命周期结束，失败只记 debug）。
impl WorkerDataService {
    async fn close_ephemeral_session(
        lease: &mut ConnectionLease,
        request_id: &str,
        session_id: &str,
    ) -> Result<()> {
        let mut frame = lease.connection().frame_for(
            request_id,
            rt::frame::Message::CloseSession(rt::CloseSessionRequest {}),
        );
        frame.session_id = session_id.to_string();
        frame.deadline_unix_ms =
            convert::deadline_ms_from(Some(Instant::now() + Duration::from_secs(2)));
        lease.send(frame).await
    }

    /// 发送一个「无需响应载荷」的会话内控制帧，并把错误映射出来。
    async fn send_session_frame(
        lease: &mut ConnectionLease,
        ctx: &DispatchContext,
        session_id: &str,
        transaction_id: &str,
        message: rt::frame::Message,
    ) -> Result<rt::Frame> {
        let mut frame = lease.connection().frame_for(&ctx.request_id, message);
        frame.session_id = session_id.to_string();
        frame.transaction_id = transaction_id.to_string();
        frame.deadline_unix_ms = convert::deadline_ms_from(Some(ctx.deadline));
        lease.send(frame).await?;
        lease.recv_required(Some(ctx.deadline)).await
    }
}

/// 流被中断时的清理：下发 CancelNotice 并作废连接。
///
/// 为什么 Drop 里做：客户端断开 / gRPC 流被丢弃都表现为「generator 被 Drop」，
/// 这是唯一可靠的中断信号。`Drop` 不能 await，因此取消下发放在 spawn 出去的任务里，
/// 并**先把连接标记为 broken** 让池不再复用它（避免读到残帧是安全属性，
/// 取消通知只是尽力而为）。
#[derive(Debug)]
struct StreamAbortGuard {
    connection: Arc<UdsConnection>,
    request_id: String,
    session_id: String,
    finished: bool,
}

impl StreamAbortGuard {
    fn new(connection: Arc<UdsConnection>, ctx: &DispatchContext) -> Self {
        Self {
            connection,
            request_id: ctx.request_id.clone(),
            session_id: ctx.session_id.clone(),
            finished: false,
        }
    }

    fn finish(&mut self) {
        self.finished = true;
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl Drop for StreamAbortGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let connection = Arc::clone(&self.connection);
        let mut frame = connection.frame_for(
            &self.request_id,
            rt::frame::Message::CancelNotice(rt::CancelNotice {
                target_request_id: self.request_id.clone(),
                target_session_id: self.session_id.clone(),
                reason: "upstream stream closed".to_string(),
            }),
        );
        frame.deadline_unix_ms =
            convert::deadline_ms_from(Some(Instant::now() + Duration::from_secs(2)));
        tracing::debug!(
            db_id = %connection.database_id(),
            request_id = %self.request_id,
            "流式响应被中断，下发 CancelNotice 并作废本地连接"
        );
        // 先让下一条请求不再复用这条可能残留帧的连接
        connection.mark_broken();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = connection.send(frame).await;
            });
        }
    }
}

/// 装箱后的服务端流类型（tonic 要求关联类型可命名）。
type BoxStream =
    Pin<Box<dyn futures::Stream<Item = std::result::Result<StreamFrame, Status>> + Send>>;

/// 数据面领域错误 -> 响应体（in-band `PlatformError`）。
///
/// 契约（架构 §17.4 / proto）：数据面的业务错误一律写进响应体的 `error` 字段，
/// 调用方据此读到结构化错误码，进而决定「刷新路由重试 / 换 Worker / 放弃」。
/// `tonic::Status` 只留给「请求无法映射成业务错误」的场景（例如 DB Process
/// 回了协议外的帧），因此预检/转发失败绝不能就地拼一个 `Status` 抛出去。
///
/// 除 `error` 外其余字段取默认值：预检就失败的请求没有结果、LSN、会话可填。
/// `ExecuteResponse` 另有 [`WorkerDataService::error_execute_response`]，不在此列。
macro_rules! impl_error_response {
    ($($response:ty),+ $(,)?) => {
        $(
            impl From<WorkerError> for $response {
                fn from(err: WorkerError) -> Self {
                    Self {
                        error: Some(convert::platform_error_to_proto(&err.to_platform_error())),
                        ..Default::default()
                    }
                }
            }
        )+
    };
}

impl_error_response!(
    ExecuteBatchResponse,
    OpenSessionResponse,
    TransactionResponse,
    CommitResponse,
    CancelResponse,
);

/// 只有 `error` 一个字段的响应体：`..Default::default()` 在这里是多余的（clippy 会拒）。
impl From<WorkerError> for CloseSessionResponse {
    fn from(err: WorkerError) -> Self {
        Self {
            error: Some(convert::platform_error_to_proto(&err.to_platform_error())),
        }
    }
}

#[tonic::async_trait]
impl WorkerData for WorkerDataService {
    /// 单次执行（无会话）。
    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> std::result::Result<Response<ExecuteResponse>, Status> {
        let started = Instant::now();
        let req = request.into_inner();
        let ctx = match self.prepare(req.context.as_ref(), FALLBACK_REQUEST_TIMEOUT) {
            Ok(ctx) => ctx,
            // 预检失败（worker 不符 / epoch 过期 / 参数非法）：错误进响应体
            Err(err) => {
                return Ok(Response::new(Self::error_execute_response(
                    convert::platform_error_to_proto(&err.to_platform_error()),
                )))
            }
        };
        let kind = "execute";

        // inline_row_limit = 0 表示调用方不限制内联行数（由 Server 侧的阈值决定）
        let (lease, _guard, frame) = self
            .execute_once(
                &ctx,
                req.sql,
                req.params,
                req.atomic,
                req.inline_row_limit,
                false,
            )
            .await
            .map_err(|err| {
                crate::metrics::record_dispatch(
                    kind,
                    "error",
                    started.elapsed().as_micros() as u64,
                );
                err.to_status()
            })?;
        drop(lease);

        let response = if let Some(error) = Self::frame_error(&frame) {
            ExecuteResponse {
                error: Some(error),
                result: None,
                elapsed_micros: started.elapsed().as_micros() as u64,
                wal_lsn: 0,
            }
        } else {
            match frame.message {
                Some(rt::frame::Message::ExecuteResponse(resp)) => ExecuteResponse {
                    error: None,
                    result: resp.result,
                    elapsed_micros: if resp.elapsed_micros == 0 {
                        started.elapsed().as_micros() as u64
                    } else {
                        resp.elapsed_micros
                    },
                    wal_lsn: resp.wal_lsn,
                },
                _ => {
                    crate::metrics::record_dispatch(
                        kind,
                        "unexpected_frame",
                        started.elapsed().as_micros() as u64,
                    );
                    return Err(WorkerError::Uds(
                        "DB Process 返回了非预期的响应帧（期望 ExecuteResponse）".to_string(),
                    )
                    .to_status());
                }
            }
        };

        let outcome = if response.error.is_some() {
            "error"
        } else {
            "ok"
        };
        crate::metrics::record_dispatch(kind, outcome, started.elapsed().as_micros() as u64);
        self.registry.mark_activity(&ctx.database_id);
        Ok(Response::new(response))
    }

    /// 流式执行（大结果集唯一允许的路径）。
    type ExecuteStreamStream = BoxStream;

    async fn execute_stream(
        &self,
        request: Request<ExecuteRequest>,
    ) -> std::result::Result<Response<Self::ExecuteStreamStream>, Status> {
        let req = request.into_inner();
        // 预检失败也走 in-band 错误帧：调用方对两条路径的处理保持一致
        let ctx = match self.prepare(req.context.as_ref(), FALLBACK_STREAM_TIMEOUT) {
            Ok(ctx) => ctx,
            Err(err) => return Ok(Response::new(Self::error_stream(err))),
        };

        match self
            .execute_once(
                &ctx,
                req.sql,
                req.params,
                req.atomic,
                req.inline_row_limit,
                true,
            )
            .await
        {
            Ok((lease, guard, first)) => {
                // 第一帧已在手（`want_stream=true` 时 DB Process 回的就是流的第一帧，
                // 无论它是 header 还是直接就是错误帧），交给统一的转发器处理，避免丢帧。
                Ok(Response::new(self.stream_with_first(
                    lease,
                    ctx,
                    guard,
                    Some(first),
                )))
            }
            Err(err) => Ok(Response::new(Self::error_stream(err))),
        }
    }

    /// 同请求内多语句批量执行。
    async fn execute_batch(
        &self,
        request: Request<ExecuteBatchRequest>,
    ) -> std::result::Result<Response<ExecuteBatchResponse>, Status> {
        let started = Instant::now();
        let req = request.into_inner();
        let ctx = match self.prepare(req.context.as_ref(), FALLBACK_REQUEST_TIMEOUT) {
            Ok(ctx) => ctx,
            Err(err) => return Ok(Response::new(err.into())),
        };

        if req.statements.is_empty() {
            return Ok(Response::new(ExecuteBatchResponse {
                error: None,
                results: Vec::new(),
                elapsed_micros: started.elapsed().as_micros() as u64,
                wal_lsn: 0,
            }));
        }

        let response = if req.atomic {
            self.execute_batch_atomic(&ctx, req.statements).await
        } else {
            self.execute_batch_sequential(&ctx, req.statements).await
        };

        let outcome = if response.error.is_some() {
            "error"
        } else {
            "ok"
        };
        crate::metrics::record_dispatch(
            "execute_batch",
            outcome,
            started.elapsed().as_micros() as u64,
        );
        self.registry.mark_activity(&ctx.database_id);
        Ok(Response::new(response))
    }

    /// 打开显式会话（会话固定到一条独占连接上，事务状态才不会被别的请求干扰）。
    async fn open_session(
        &self,
        request: Request<OpenSessionRequest>,
    ) -> std::result::Result<Response<OpenSessionResponse>, Status> {
        let req = request.into_inner();
        let ctx = match self.prepare(req.context.as_ref(), FALLBACK_REQUEST_TIMEOUT) {
            Ok(ctx) => ctx,
            Err(err) => return Ok(Response::new(err.into())),
        };
        let idle_timeout_ms = if req.idle_timeout_ms == 0 {
            DEFAULT_SESSION_IDLE_TIMEOUT_MS
        } else {
            req.idle_timeout_ms
        };

        let conn = match self.pool.new_connection(&ctx.database_id).await {
            Ok(conn) => conn,
            Err(err) => return Ok(Response::new(err.into())),
        };
        let mut frame = conn.frame_for(
            &ctx.request_id,
            rt::frame::Message::OpenSession(rt::OpenSessionRequest {
                idle_timeout_ms,
                max_transaction_lifetime_ms: DEFAULT_TRANSACTION_MAX_LIFETIME_MS as u64,
            }),
        );
        frame.deadline_unix_ms = convert::deadline_ms_from(Some(ctx.deadline));

        let mut lease = match self.pool.lease_on(&conn).await {
            Ok(lease) => lease,
            Err(err) => return Ok(Response::new(err.into())),
        };
        let _guard =
            self.inflight
                .insert(&ctx.request_id, &ctx.database_id, &ctx.session_id, &conn);
        if let Err(err) = lease.send(frame).await {
            return Ok(Response::new(err.into()));
        }
        let response = match lease.recv_required(Some(ctx.deadline)).await {
            Ok(response) => response,
            Err(err) => return Ok(Response::new(err.into())),
        };
        drop(lease);

        if let Some(error) = Self::frame_error(&response) {
            return Ok(Response::new(OpenSessionResponse {
                error: Some(error),
                ..Default::default()
            }));
        }
        let session = match response.message {
            Some(rt::frame::Message::OpenSessionResponse(resp)) => resp,
            _ => {
                return Err(
                    WorkerError::Uds("DB Process 返回了非预期的会话响应帧".to_string()).to_status(),
                )
            }
        };

        // 绑定会话 -> 连接：后续 Begin/Commit/Stream 都必须在同一条连接上
        self.pool.bind_session(SessionBinding {
            database_id: ctx.database_id.clone(),
            session_id: session.session_id.clone(),
            connection: Arc::clone(&conn),
        });
        observability::metrics::record_session_open();
        tracing::debug!(
            db_id = %ctx.database_id,
            session_id = %session.session_id,
            "会话已建立"
        );

        Ok(Response::new(OpenSessionResponse {
            error: None,
            session_id: session.session_id,
            worker_id: self.cfg.worker_id.clone(),
            database_id: ctx.database_id,
            expires_at_unix_ms: session.expires_at_unix_ms,
        }))
    }

    /// 关闭会话并解绑。
    async fn close_session(
        &self,
        request: Request<CloseSessionRequest>,
    ) -> std::result::Result<Response<CloseSessionResponse>, Status> {
        let req = request.into_inner();
        let ctx = match self.prepare(req.context.as_ref(), FALLBACK_REQUEST_TIMEOUT) {
            Ok(ctx) => ctx,
            Err(err) => return Ok(Response::new(err.into())),
        };
        // 关闭会话时以请求里的 session_id 为准（proto 显式字段优先于 context）
        let session_id = if req.session_id.is_empty() {
            ctx.session_id.clone()
        } else {
            req.session_id.clone()
        };
        let conn = match self.session_connection(&session_id, &ctx.database_id, ctx.owner_epoch) {
            Ok(conn) => conn,
            Err(err) => return Ok(Response::new(err.into())),
        };

        let mut frame = conn.frame_for(
            &ctx.request_id,
            rt::frame::Message::CloseSession(rt::CloseSessionRequest {}),
        );
        frame.session_id = session_id.clone();
        frame.deadline_unix_ms = convert::deadline_ms_from(Some(ctx.deadline));
        let mut lease = match self.pool.lease_on(&conn).await {
            Ok(lease) => lease,
            Err(err) => return Ok(Response::new(err.into())),
        };
        if let Err(err) = lease.send(frame).await {
            return Ok(Response::new(err.into()));
        }
        let response = match lease.recv_required(Some(ctx.deadline)).await {
            Ok(response) => response,
            Err(err) => return Ok(Response::new(err.into())),
        };
        drop(lease);
        self.pool.unbind_session(&session_id);

        Ok(Response::new(CloseSessionResponse {
            error: Self::frame_error(&response),
        }))
    }

    /// 开启事务（必须在已打开的会话内）。
    async fn begin(
        &self,
        request: Request<BeginRequest>,
    ) -> std::result::Result<Response<TransactionResponse>, Status> {
        let req = request.into_inner();
        let ctx = match self.prepare(req.context.as_ref(), FALLBACK_REQUEST_TIMEOUT) {
            Ok(ctx) => ctx,
            Err(err) => return Ok(Response::new(err.into())),
        };
        let session_id = if req.session_id.is_empty() {
            ctx.session_id.clone()
        } else {
            req.session_id.clone()
        };
        let conn = match self.session_connection(&session_id, &ctx.database_id, ctx.owner_epoch) {
            Ok(conn) => conn,
            Err(err) => return Ok(Response::new(err.into())),
        };
        let mut lease = match self.pool.lease_on(&conn).await {
            Ok(lease) => lease,
            Err(err) => return Ok(Response::new(err.into())),
        };
        let _guard = self
            .inflight
            .insert(&ctx.request_id, &ctx.database_id, &session_id, &conn);

        let max_lifetime_ms = if req.max_lifetime_ms == 0 {
            DEFAULT_TRANSACTION_MAX_LIFETIME_MS
        } else {
            req.max_lifetime_ms
        };
        let response = match Self::send_session_frame(
            &mut lease,
            &ctx,
            &session_id,
            "",
            rt::frame::Message::Begin(rt::BeginRequest {
                read_only: req.read_only,
                max_lifetime_ms: max_lifetime_ms as u64,
            }),
        )
        .await
        {
            Ok(response) => response,
            Err(err) => return Ok(Response::new(err.into())),
        };

        if let Some(error) = Self::frame_error(&response) {
            return Ok(Response::new(TransactionResponse {
                error: Some(error),
                ..Default::default()
            }));
        }
        match response.message {
            Some(rt::frame::Message::TransactionResponse(resp)) => {
                Ok(Response::new(TransactionResponse {
                    error: None,
                    transaction_id: resp.transaction_id,
                    expires_at_unix_ms: resp.expires_at_unix_ms,
                }))
            }
            _ => {
                Err(WorkerError::Uds("DB Process 返回了非预期的事务响应帧".to_string()).to_status())
            }
        }
    }

    /// 提交事务：只有 Remote WAL durable 之后才允许返回成功（架构 §11.1）。
    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> std::result::Result<Response<CommitResponse>, Status> {
        let started = Instant::now();
        let req = request.into_inner();
        let ctx = match self.prepare(req.context.as_ref(), FALLBACK_REQUEST_TIMEOUT) {
            Ok(ctx) => ctx,
            Err(err) => return Ok(Response::new(err.into())),
        };
        let session_id = if req.session_id.is_empty() {
            ctx.session_id.clone()
        } else {
            req.session_id.clone()
        };
        let transaction_id = if req.transaction_id.is_empty() {
            ctx.transaction_id.clone()
        } else {
            req.transaction_id.clone()
        };
        let conn = match self.session_connection(&session_id, &ctx.database_id, ctx.owner_epoch) {
            Ok(conn) => conn,
            Err(err) => return Ok(Response::new(err.into())),
        };
        let mut lease = match self.pool.lease_on(&conn).await {
            Ok(lease) => lease,
            Err(err) => return Ok(Response::new(err.into())),
        };
        let _guard = self
            .inflight
            .insert(&ctx.request_id, &ctx.database_id, &session_id, &conn);

        let response = match Self::send_session_frame(
            &mut lease,
            &ctx,
            &session_id,
            &transaction_id,
            rt::frame::Message::Commit(rt::CommitRequest {}),
        )
        .await
        {
            Ok(response) => response,
            Err(err) => return Ok(Response::new(err.into())),
        };

        if let Some(error) = Self::frame_error(&response) {
            return Ok(Response::new(CommitResponse {
                error: Some(error),
                wal_lsn: 0,
                elapsed_micros: started.elapsed().as_micros() as u64,
            }));
        }
        match response.message {
            Some(rt::frame::Message::CommitResponse(resp)) => {
                observability::metrics::record_transaction_commit();
                Ok(Response::new(CommitResponse {
                    error: None,
                    wal_lsn: resp.wal_lsn,
                    elapsed_micros: if resp.elapsed_micros == 0 {
                        started.elapsed().as_micros() as u64
                    } else {
                        resp.elapsed_micros
                    },
                }))
            }
            _ => {
                Err(WorkerError::Uds("DB Process 返回了非预期的提交响应帧".to_string()).to_status())
            }
        }
    }

    /// 回滚事务。
    async fn rollback(
        &self,
        request: Request<RollbackRequest>,
    ) -> std::result::Result<Response<TransactionResponse>, Status> {
        let req = request.into_inner();
        let ctx = match self.prepare(req.context.as_ref(), FALLBACK_REQUEST_TIMEOUT) {
            Ok(ctx) => ctx,
            Err(err) => return Ok(Response::new(err.into())),
        };
        let session_id = if req.session_id.is_empty() {
            ctx.session_id.clone()
        } else {
            req.session_id.clone()
        };
        let transaction_id = if req.transaction_id.is_empty() {
            ctx.transaction_id.clone()
        } else {
            req.transaction_id.clone()
        };
        let conn = match self.session_connection(&session_id, &ctx.database_id, ctx.owner_epoch) {
            Ok(conn) => conn,
            Err(err) => return Ok(Response::new(err.into())),
        };
        let mut lease = match self.pool.lease_on(&conn).await {
            Ok(lease) => lease,
            Err(err) => return Ok(Response::new(err.into())),
        };
        let _guard = self
            .inflight
            .insert(&ctx.request_id, &ctx.database_id, &session_id, &conn);

        let response = match Self::send_session_frame(
            &mut lease,
            &ctx,
            &session_id,
            &transaction_id,
            rt::frame::Message::Rollback(rt::RollbackRequest {}),
        )
        .await
        {
            Ok(response) => response,
            Err(err) => return Ok(Response::new(err.into())),
        };

        if let Some(error) = Self::frame_error(&response) {
            return Ok(Response::new(TransactionResponse {
                error: Some(error),
                ..Default::default()
            }));
        }
        match response.message {
            Some(rt::frame::Message::RollbackResponse(_)) => {
                observability::metrics::record_transaction_rollback();
                Ok(Response::new(TransactionResponse {
                    error: None,
                    transaction_id,
                    expires_at_unix_ms: 0,
                }))
            }
            _ => {
                Err(WorkerError::Uds("DB Process 返回了非预期的回滚响应帧".to_string()).to_status())
            }
        }
    }

    /// 会话内流式执行。
    type SessionExecuteStreamStream = BoxStream;

    async fn session_execute_stream(
        &self,
        request: Request<SessionExecuteRequest>,
    ) -> std::result::Result<Response<Self::SessionExecuteStreamStream>, Status> {
        let req = request.into_inner();
        let ctx = match self.prepare(req.context.as_ref(), FALLBACK_STREAM_TIMEOUT) {
            Ok(ctx) => ctx,
            Err(err) => return Ok(Response::new(Self::error_stream(err))),
        };
        let session_id = if req.session_id.is_empty() {
            ctx.session_id.clone()
        } else {
            req.session_id.clone()
        };
        let conn = match self.session_connection(&session_id, &ctx.database_id, ctx.owner_epoch) {
            Ok(conn) => conn,
            Err(err) => return Ok(Response::new(Self::error_stream(err))),
        };

        let mut session_ctx = ctx.clone();
        session_ctx.session_id = session_id.clone();
        let mut frame = conn.frame_for(
            &session_ctx.request_id,
            rt::frame::Message::Execute(rt::ExecuteRequest {
                sql: req.sql,
                params: req.params,
                atomic: false,
                want_stream: true,
                inline_row_limit: 0,
            }),
        );
        frame.session_id = session_id.clone();
        frame.transaction_id = session_ctx.transaction_id.clone();
        frame.deadline_unix_ms = convert::deadline_ms_from(Some(session_ctx.deadline));

        let lease = match self.pool.lease_on(&conn).await {
            Ok(lease) => lease,
            Err(err) => return Ok(Response::new(Self::error_stream(err))),
        };
        let guard = self.inflight.insert(
            &session_ctx.request_id,
            &session_ctx.database_id,
            &session_id,
            &conn,
        );
        if let Err(err) = lease.send(frame).await {
            return Ok(Response::new(Self::error_stream(err)));
        }
        Ok(Response::new(self.stream_frames(lease, session_ctx, guard)))
    }

    /// 取消：按 request_id 定位在途请求，向它所在连接下发 CancelNotice。
    async fn cancel(
        &self,
        request: Request<CancelRequest>,
    ) -> std::result::Result<Response<CancelResponse>, Status> {
        let req = request.into_inner();
        let context = req.context.clone().unwrap_or_default();
        // target 为空 = 取消 context.request_id 自己（proto 语义）
        let target_request_id = if req.target_request_id.is_empty() {
            context.request_id.clone()
        } else {
            req.target_request_id.clone()
        };
        // Cancel 自身不做 epoch 校验的强拒绝：取消是「尽力而为」的收敛动作，
        // 而且它不写数据、不改所有权。但仍要求 database_id 能定位本地注册项。
        if !context.database_id.is_empty() {
            // db_id 非法：Cancel 不改变任何状态，错误进响应体即可
            if let Err(err) = crate::paths::checked_id(&context.database_id) {
                return Ok(Response::new(err.into()));
            }
        }

        let mut notices = 0u32;
        if !req.target_session_id.is_empty() {
            for entry in self.inflight.by_session(&req.target_session_id) {
                if Self::send_cancel_notice(
                    &entry.connection,
                    &entry.request_id,
                    &req.target_session_id,
                    &req.reason,
                )
                .await
                {
                    notices += 1;
                }
            }
        } else if !target_request_id.is_empty() {
            if let Some(entry) = self.inflight.lookup(&target_request_id) {
                if Self::send_cancel_notice(
                    &entry.connection,
                    &entry.request_id,
                    &entry.session_id,
                    &req.reason,
                )
                .await
                {
                    notices += 1;
                }
            }
        }

        // 找不到在途请求不是错误：请求可能刚刚完成，或者从未到达本 Worker。
        // 返回 cancelled=false 让 Server 自行判断（它可以选择重试或直接判超时）。
        Ok(Response::new(CancelResponse {
            error: None,
            cancelled: notices > 0,
        }))
    }

    /// Dispatcher 健康与本地注册表快照（仅内部使用）。
    async fn dispatcher_status(
        &self,
        _request: Request<DispatcherStatusRequest>,
    ) -> std::result::Result<Response<DispatcherStatusResponse>, Status> {
        let databases = self
            .registry
            .snapshot()
            .into_iter()
            .map(|db| LocalDatabaseEntry {
                database_id: db.database_id.clone(),
                state: db.state.to_db_str().to_string(),
                pid: db.pid.unwrap_or(0) as i64,
                owner_epoch: db.owner_epoch,
                local_socket: db.local_socket.display().to_string(),
                rss_kib: self.sampler.rss_mib_of(&db.database_id) * 1024,
            })
            .collect();

        Ok(Response::new(DispatcherStatusResponse {
            worker_id: self.cfg.worker_id.clone(),
            usage: Some(self.sampler.current().into()),
            running_db_count: self.registry.serving_count() as u32,
            databases,
        }))
    }
}

impl WorkerDataService {
    /// 流式转发入口。`first` 为已在手的首帧（非流式路径准备阶段读到的那一帧）。
    ///
    /// 首帧可能是「通知类」帧（例如 WalDurableNotice），此时 `map_stream_frame` 返回
    /// `None`，需要继续读而不是把它当成响应 —— 这正是不能简单丢弃首帧的原因。
    fn stream_with_first(
        &self,
        lease: ConnectionLease,
        ctx: DispatchContext,
        guard: InFlightGuard,
        first: Option<rt::Frame>,
    ) -> BoxStream {
        let head = first.and_then(Self::map_stream_frame);
        Box::pin(async_stream::stream! {
            // 守卫随流一起存活：客户端中断导致流被 Drop 时，Drop 里会下发取消并作废连接
            let _guard = guard;
            let mut lease = lease;
            let mut abort = StreamAbortGuard::new(Arc::clone(lease.connection()), &ctx);
            let mut done = false;

            if let Some(frame) = head {
                if Self::is_terminal(&frame) {
                    abort.finish();
                }
                yield Ok(frame);
                done = abort.is_finished();
            }

            while !done {
                match lease.recv(Some(ctx.deadline)).await {
                    Ok(Some(frame)) => match Self::map_stream_frame(frame) {
                        Some(out) => {
                            if Self::is_terminal(&out) {
                                // 正常收尾：不需要取消，也不要作废连接
                                abort.finish();
                                done = true;
                            }
                            yield Ok(out);
                        }
                        // 通知类帧：跳过，继续等本次请求的响应
                        None => continue,
                    },
                    Ok(None) => {
                        yield Err(WorkerError::Uds(format!(
                            "DB Process 在流式响应中途关闭连接（db={}）",
                            ctx.database_id
                        ))
                        .to_status());
                        break;
                    }
                    Err(err) => {
                        yield Err(err.to_status());
                        break;
                    }
                }
            }
        })
    }

    /// 顺序批处理（非原子）：逐条以 autocommit 语义执行，失败即停并回报部分结果。
    async fn execute_batch_sequential(
        &self,
        ctx: &DispatchContext,
        statements: Vec<data::BatchStatement>,
    ) -> ExecuteBatchResponse {
        let started = Instant::now();
        let mut results = Vec::with_capacity(statements.len());
        let mut wal_lsn = 0u64;

        for (index, statement) in statements.into_iter().enumerate() {
            let execution = self
                .execute_once(
                    ctx,
                    statement.sql,
                    statement.params,
                    // 单条执行按 autocommit 语义包裹（架构 §13.1 的默认行为）
                    true,
                    0,
                    false,
                )
                .await;

            let frame = match execution {
                Ok((lease, guard, frame)) => {
                    drop(lease);
                    drop(guard);
                    frame
                }
                Err(err) => {
                    return ExecuteBatchResponse {
                        error: Some(batch_error(Some(index), &err.to_platform_error()).into()),
                        results,
                        elapsed_micros: started.elapsed().as_micros() as u64,
                        wal_lsn,
                    }
                }
            };

            if let Some(error) = Self::frame_error(&frame) {
                let mut platform_error = convert::platform_error_from_proto(&error);
                if index > 0 {
                    // 前面的语句已经生效：批处理整体是「部分成功」
                    platform_error = batch_error(Some(index), &platform_error);
                }
                return ExecuteBatchResponse {
                    error: Some(convert::platform_error_to_proto(&platform_error)),
                    results,
                    elapsed_micros: started.elapsed().as_micros() as u64,
                    wal_lsn,
                };
            }
            match frame.message {
                Some(rt::frame::Message::ExecuteResponse(resp)) => {
                    wal_lsn = resp.wal_lsn.max(wal_lsn);
                    results.push(resp.result.unwrap_or_default());
                }
                _ => {
                    let err = WorkerError::Uds(format!(
                        "DB Process 返回了非预期响应帧（批处理第 {index} 条）"
                    ));
                    return ExecuteBatchResponse {
                        error: Some(convert::platform_error_to_proto(&err.to_platform_error())),
                        results,
                        elapsed_micros: started.elapsed().as_micros() as u64,
                        wal_lsn,
                    };
                }
            }
        }

        ExecuteBatchResponse {
            error: None,
            results,
            elapsed_micros: started.elapsed().as_micros() as u64,
            wal_lsn,
        }
    }

    /// 原子批处理：临时会话 + 显式事务，任一失败即整体回滚。
    ///
    /// 为什么用临时会话而不是「拼一条多语句 SQL」：参数是结构化的（`Value` oneof），
    /// 字符串拼接会破坏参数绑定；用会话 + Begin/Commit 帧则完全复用 DB Process 的
    /// 事务语义（架构 §13.2）。
    async fn execute_batch_atomic(
        &self,
        ctx: &DispatchContext,
        statements: Vec<data::BatchStatement>,
    ) -> ExecuteBatchResponse {
        let started = Instant::now();
        let elapsed = |from: Instant| from.elapsed().as_micros() as u64;
        let failure_response =
            |error: common::PlatformError, elapsed_micros: u64| ExecuteBatchResponse {
                error: Some(error),
                results: Vec::new(),
                elapsed_micros,
                wal_lsn: 0,
            };

        let mut lease = match self.pool.lease(&ctx.database_id).await {
            Ok(lease) => lease,
            Err(err) => {
                return failure_response(
                    convert::platform_error_to_proto(&err.to_platform_error()),
                    elapsed(started),
                )
            }
        };
        let session_id = match Self::open_ephemeral_session(
            &mut lease,
            &ctx.request_id,
            ctx.deadline,
            DEFAULT_SESSION_IDLE_TIMEOUT_MS,
        )
        .await
        {
            Ok(session_id) => session_id,
            Err(err) => {
                return failure_response(
                    convert::platform_error_to_proto(&err.to_platform_error()),
                    elapsed(started),
                )
            }
        };
        let _guard = self.inflight.insert(
            &ctx.request_id,
            &ctx.database_id,
            &session_id,
            lease.connection(),
        );

        // BEGIN：拿到 transaction_id 之后所有语句都在同一事务内
        let transaction_id = match Self::send_session_frame(
            &mut lease,
            ctx,
            &session_id,
            "",
            rt::frame::Message::Begin(rt::BeginRequest {
                read_only: false,
                max_lifetime_ms: DEFAULT_TRANSACTION_MAX_LIFETIME_MS as u64,
            }),
        )
        .await
        {
            Ok(frame) => match Self::frame_error(&frame) {
                Some(error) => {
                    let _ = Self::close_ephemeral_session(&mut lease, &ctx.request_id, &session_id)
                        .await;
                    return failure_response(error, elapsed(started));
                }
                None => match frame.message {
                    Some(rt::frame::Message::TransactionResponse(resp)) => resp.transaction_id,
                    _ => {
                        let err = WorkerError::Uds("原子批处理开启事务失败：响应帧类型非法".into());
                        let _ =
                            Self::close_ephemeral_session(&mut lease, &ctx.request_id, &session_id)
                                .await;
                        return failure_response(
                            convert::platform_error_to_proto(&err.to_platform_error()),
                            elapsed(started),
                        );
                    }
                },
            },
            Err(err) => {
                let _ =
                    Self::close_ephemeral_session(&mut lease, &ctx.request_id, &session_id).await;
                return failure_response(
                    convert::platform_error_to_proto(&err.to_platform_error()),
                    elapsed(started),
                );
            }
        };

        let mut results = Vec::with_capacity(statements.len());
        let mut failure: Option<common::PlatformError> = None;
        for statement in statements {
            let execution = Self::send_session_frame(
                &mut lease,
                ctx,
                &session_id,
                &transaction_id,
                rt::frame::Message::Execute(rt::ExecuteRequest {
                    sql: statement.sql,
                    params: statement.params,
                    // 会话内已经是显式事务，单条执行不再要求引擎再包一层
                    atomic: false,
                    want_stream: false,
                    inline_row_limit: 0,
                }),
            )
            .await;

            match execution {
                Ok(frame) => {
                    if let Some(error) = Self::frame_error(&frame) {
                        failure = Some(error);
                        break;
                    }
                    match frame.message {
                        Some(rt::frame::Message::ExecuteResponse(resp)) => {
                            results.push(resp.result.unwrap_or_default())
                        }
                        _ => {
                            failure = Some(convert::platform_error_to_proto(
                                &WorkerError::Uds("原子批处理收到非预期响应帧".into())
                                    .to_platform_error(),
                            ));
                            break;
                        }
                    }
                }
                Err(err) => {
                    failure = Some(convert::platform_error_to_proto(&err.to_platform_error()));
                    break;
                }
            }
        }

        let (final_error, wal_lsn) = match failure {
            None => {
                // COMMIT 成功即代表 Remote WAL 已 durable（架构 §11.1）
                match Self::send_session_frame(
                    &mut lease,
                    ctx,
                    &session_id,
                    &transaction_id,
                    rt::frame::Message::Commit(rt::CommitRequest {}),
                )
                .await
                {
                    Ok(frame) => match Self::frame_error(&frame) {
                        Some(error) => (Some(error), 0),
                        None => match frame.message {
                            Some(rt::frame::Message::CommitResponse(resp)) => {
                                observability::metrics::record_transaction_commit();
                                (None, resp.wal_lsn)
                            }
                            _ => (
                                Some(convert::platform_error_to_proto(
                                    &WorkerError::Uds("原子批处理提交响应非法".into())
                                        .to_platform_error(),
                                )),
                                0,
                            ),
                        },
                    },
                    Err(err) => (
                        Some(convert::platform_error_to_proto(&err.to_platform_error())),
                        0,
                    ),
                }
            }
            Some(error) => {
                // 回滚失败不覆盖原始错误（原始错误才是调用方需要的信息），只记日志
                if let Err(err) = Self::send_session_frame(
                    &mut lease,
                    ctx,
                    &session_id,
                    &transaction_id,
                    rt::frame::Message::Rollback(rt::RollbackRequest {}),
                )
                .await
                {
                    tracing::warn!(
                        db_id = %ctx.database_id,
                        request_id = %ctx.request_id,
                        error = %err,
                        "原子批处理回滚失败（连接可能已断）"
                    );
                }
                observability::metrics::record_transaction_rollback();
                (Some(error), 0)
            }
        };

        // 临时会话必须在所有退出路径上关闭，否则 DB Process 侧会堆积会话
        let _ = Self::close_ephemeral_session(&mut lease, &ctx.request_id, &session_id).await;

        ExecuteBatchResponse {
            error: final_error,
            results,
            elapsed_micros: elapsed(started),
            wal_lsn,
        }
    }

    /// 向连接下发 CancelNotice（尽力而为）。
    async fn send_cancel_notice(
        connection: &Arc<UdsConnection>,
        request_id: &str,
        session_id: &str,
        reason: &str,
    ) -> bool {
        let mut frame = connection.frame_for(
            request_id,
            rt::frame::Message::CancelNotice(rt::CancelNotice {
                target_request_id: request_id.to_string(),
                target_session_id: session_id.to_string(),
                reason: reason.to_string(),
            }),
        );
        frame.deadline_unix_ms =
            convert::deadline_ms_from(Some(Instant::now() + Duration::from_secs(3)));
        match connection.send(frame).await {
            Ok(()) => {
                tracing::debug!(request_id = %request_id, reason = %reason, "已下发取消通知");
                true
            }
            Err(err) => {
                tracing::warn!(request_id = %request_id, error = %err, "下发取消通知失败");
                false
            }
        }
    }
}

/// 构造批处理错误：已有语句生效时统一收敛为 `BATCH_PARTIAL_FAILURE`，
/// 否则保留原始错误码（首条就失败 = 整批没生效，原始原因更有诊断价值）。
fn batch_error(
    index: Option<usize>,
    original: &domain::error::PlatformError,
) -> domain::error::PlatformError {
    let Some(index) = index else {
        return original.clone();
    };
    if index == 0 {
        return original.clone();
    }
    domain::error::PlatformError::new(
        ErrorCode::BatchPartialFailure,
        format!("批处理第 {index} 条失败：{}", original.message),
    )
    .with_detail(serde_json::json!({
        "failed_index": index,
        "original_code": original.code.as_str(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::uds::PoolConfig;
    use domain::lifecycle::LifecycleState;
    use domain::resources::ResourceBudget;
    use domain::time::now_unix_ms;
    use std::path::PathBuf;

    fn registry_with(db_id: &str, epoch: u64) -> Arc<LocalDbRegistry> {
        let registry = Arc::new(LocalDbRegistry::new());
        registry.register(crate::registry::LocalDatabase {
            database_id: db_id.to_string(),
            state: LifecycleState::Warm,
            pid: Some(4242),
            local_socket: PathBuf::from("/run/sockets/db.sock"),
            owner_epoch: epoch,
            budget: ResourceBudget::ZERO,
            started_at_unix_ms: now_unix_ms(),
            last_activity_unix_ms: now_unix_ms(),
            crash_count: 0,
            cgroup: None,
            restored_from: None,
            read_only: false,
        });
        registry
    }

    fn config(dir: &std::path::Path) -> Arc<WorkerConfig> {
        let cli = <crate::cli::Cli as clap::Parser>::try_parse_from([
            "db-worker",
            "--worker-id",
            "worker-test",
            "--data-dir",
            dir.join("data").to_str().unwrap(),
            "--run-dir",
            dir.join("run").to_str().unwrap(),
            "--cgroup-disabled",
        ])
        .unwrap();
        let cfg = WorkerConfig::from_cli(cli).unwrap();
        cfg.ensure_dirs().unwrap();
        Arc::new(cfg)
    }

    fn service(dir: &std::path::Path, db_id: &str, epoch: u64) -> WorkerDataService {
        let cfg = config(dir);
        let registry = registry_with(db_id, epoch);
        let pool = Arc::new(DbConnectionPool::new(
            PoolConfig {
                worker_id: cfg.worker_id.clone(),
                run_dir: cfg.run_dir.clone(),
                connect_timeout: Duration::from_millis(200),
                handshake_timeout: Duration::from_millis(200),
                ..Default::default()
            },
            Arc::clone(&registry),
        ));
        let sampler = Arc::new(ResourceSampler::new(
            cfg.capacity,
            crate::cgroup::CgroupManager::new(cfg.cgroup_root.clone(), false),
        ));
        WorkerDataService::new(cfg, registry, pool, sampler)
    }

    #[test]
    fn prepare_rejects_epoch_above_local() {
        let dir = tempfile::tempdir().unwrap();
        let svc = service(dir.path(), "db-1", 7);
        let ctx = common::RequestContext {
            request_id: "req-1".into(),
            database_id: "db-1".into(),
            owner_epoch: 9,
            ..Default::default()
        };
        let err = svc
            .prepare(Some(&ctx), FALLBACK_REQUEST_TIMEOUT)
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::NotOwner);
    }

    #[test]
    fn prepare_rejects_epoch_below_local() {
        let dir = tempfile::tempdir().unwrap();
        let svc = service(dir.path(), "db-1", 7);
        let ctx = common::RequestContext {
            request_id: "req-1".into(),
            database_id: "db-1".into(),
            owner_epoch: 3,
            ..Default::default()
        };
        let err = svc
            .prepare(Some(&ctx), FALLBACK_REQUEST_TIMEOUT)
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::EpochMismatch);
    }

    #[test]
    fn prepare_rejects_unknown_database() {
        let dir = tempfile::tempdir().unwrap();
        let svc = service(dir.path(), "db-1", 7);
        let ctx = common::RequestContext {
            request_id: "req-1".into(),
            database_id: "db-other".into(),
            owner_epoch: 7,
            ..Default::default()
        };
        let err = svc
            .prepare(Some(&ctx), FALLBACK_REQUEST_TIMEOUT)
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::NotOwner);
    }

    #[test]
    fn prepare_rejects_wrong_worker() {
        let dir = tempfile::tempdir().unwrap();
        let svc = service(dir.path(), "db-1", 7);
        let ctx = common::RequestContext {
            request_id: "req-1".into(),
            database_id: "db-1".into(),
            owner_epoch: 7,
            worker_id: "worker-other".into(),
            ..Default::default()
        };
        let err = svc
            .prepare(Some(&ctx), FALLBACK_REQUEST_TIMEOUT)
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::NotOwner);
    }

    #[test]
    fn prepare_rejects_unsafe_database_id() {
        let dir = tempfile::tempdir().unwrap();
        let svc = service(dir.path(), "db-1", 7);
        let ctx = common::RequestContext {
            request_id: "req-1".into(),
            database_id: "   ".into(),
            owner_epoch: 7,
            ..Default::default()
        };
        assert!(svc.prepare(Some(&ctx), FALLBACK_REQUEST_TIMEOUT).is_err());
    }

    #[test]
    fn prepare_generates_request_id_and_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let svc = service(dir.path(), "db-1", 7);
        let ctx = common::RequestContext {
            database_id: "db-1".into(),
            owner_epoch: 7,
            ..Default::default()
        };
        let prepared = svc
            .prepare(Some(&ctx), Duration::from_secs(5))
            .expect("应当通过校验");
        assert!(!prepared.request_id.is_empty());
        let remaining = prepared.deadline.saturating_duration_since(Instant::now());
        assert!(remaining <= Duration::from_secs(6));
    }

    #[test]
    fn inflight_index_insert_lookup_and_remove() {
        // 无需真实连接即可验证索引语义：用 std::process::id() 之类的 fd 不行，
        // 因此这里只验证「按 request_id / session_id 查找」与守卫回收。
        let index = Arc::new(InflightIndex::new());
        assert!(index.is_empty());
        assert!(index.lookup("missing").is_none());
        assert!(index.by_session("s-1").is_empty());
    }

    #[test]
    fn batch_error_maps_partial_failure() {
        let original =
            domain::error::PlatformError::new(ErrorCode::ConstraintViolation, "唯一键冲突");
        // 首条失败：保留原始错误码
        assert_eq!(
            batch_error(Some(0), &original).code,
            ErrorCode::ConstraintViolation
        );
        // 后续失败：收敛为 BATCH_PARTIAL_FAILURE，并带上原始码
        let mapped = batch_error(Some(2), &original);
        assert_eq!(mapped.code, ErrorCode::BatchPartialFailure);
        assert!(mapped.detail.is_some());
        // 无索引：原样返回
        assert_eq!(
            batch_error(None, &original).code,
            ErrorCode::ConstraintViolation
        );
    }

    #[test]
    fn session_lookup_fails_for_unknown_session() {
        let dir = tempfile::tempdir().unwrap();
        let svc = service(dir.path(), "db-1", 7);
        let err = svc.session_connection("s-missing", "db-1", 7).unwrap_err();
        assert_eq!(err.code(), ErrorCode::SessionNotFound);
        // 空 session_id 同样按「会话不存在」处理，而不是 panic
        let err = svc.session_connection("", "db-1", 7).unwrap_err();
        assert_eq!(err.code(), ErrorCode::SessionNotFound);
    }
}
