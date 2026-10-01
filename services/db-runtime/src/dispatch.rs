//! 帧分发：把 Dispatcher 的请求路由到会话 / 引擎，并回帧（架构 §17.7）。
//!
//! 处理流程（每个请求一视同仁）：
//!
//! ```text
//! 分帧 -> 记 span -> 解析 deadline -> 登记取消 -> 路由（会话 / 无会话）
//!      -> spawn_blocking 跑引擎（引擎会在调用线程上驱动 IO，禁止占用 tokio worker）
//!      -> 回 ExecuteResponse，或 StreamHeader/RowBatch*/StreamEnd
//! ```
//!
//! 背压：所有写出都直接 `await` socket；单连接上多个请求各自 spawn，互不排队，
//! 唯一的串行点是**同一会话**的状态锁（一个会话一条引擎连接，事务语义要求如此）。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use domain::error::{ErrorCode, PlatformError};
use engine_adapter::{EngineConnection, QueryOutcome};
use protocol::convert;
use protocol::data;
use protocol::framing::{self, FramingError};
use protocol::runtime_local as rt;
use tokio::net::unix::OwnedWriteHalf;

use crate::cancel::CancelFlag;
use crate::config::DEFAULT_REQUEST_DEADLINE;
use crate::frame;
use crate::host::{is_read_only_statement, read_only_rejection, rss_kib, Host, HostState};
use crate::session::{
    now_ms, session_lost, transaction_lost, ExpiryReason, SessionLookup, SessionState,
};

/// 每个 `RowBatch` 携带的最大行数。
///
/// 取 128 是"帧不要太小"与"首行尽快到达"之间的折中：太小则帧头开销占比高，
/// 太大则单帧可能超过对端的读缓冲并拉长首行延迟。
const ROWS_PER_BATCH: usize = 128;

/// 流式判定的阈值（字节）：超过它就改走分块，避免一个超大帧把 UDS 写死。
const STREAM_BYTES_THRESHOLD: usize = 1024 * 1024;

/// 连接写出端：一帧的字节不会被另一帧切开。
pub struct FrameWriter {
    out: OwnedWriteHalf,
}

impl FrameWriter {
    /// 包住 UDS 写半边。
    #[must_use]
    pub fn new(out: OwnedWriteHalf) -> Self {
        Self { out }
    }

    /// 写一帧；写不下去就挂起调用方（内核 socket buffer 满 = 天然背压）。
    pub async fn send(&mut self, frame: &rt::Frame) -> Result<(), FramingError> {
        framing::write_frame(&mut self.out, frame).await
    }
}

/// 连接上下文（同一连接上的所有请求共享）。
pub struct ConnectionCtx {
    /// 宿主。
    pub host: Arc<Host>,
    /// 共享写出端。
    pub writer: Arc<tokio::sync::Mutex<FrameWriter>>,
    /// 对端标识（日志）。
    pub peer: String,
    /// 本连接的响应帧计数（诊断用）。
    seq: AtomicU64,
}

impl ConnectionCtx {
    /// 新建连接上下文。
    #[must_use]
    pub fn new(host: Arc<Host>, writer: FrameWriter, peer: String) -> Arc<Self> {
        Arc::new(Self {
            host,
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
            peer,
            seq: AtomicU64::new(0),
        })
    }

    /// 写出一帧（自动分配本连接的响应序号）。
    pub async fn write(&self, frame: &rt::Frame) -> Result<(), FramingError> {
        let mut writer = self.writer.lock().await;
        writer.send(frame).await
    }

    /// 下一帧响应序号。
    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::AcqRel) + 1
    }
}

/// 单帧入口：不阻塞调用方（由 server 逐帧 spawn）。
pub async fn dispatch(ctx: Arc<ConnectionCtx>, request: rt::Frame) {
    let span = tracing::info_span!(
        "runtime_request",
        db_id = %request.database_id,
        session_id = %request.session_id,
        request_id = %request.request_id,
        owner_epoch = request.owner_epoch,
        operation = operation_name(&request),
    );
    let _entered = span.enter();

    let drained = ctx.host.state() == HostState::Draining;
    let is_control = matches!(
        request.message,
        Some(rt::frame::Message::Health(_)) | Some(rt::frame::Message::Shutdown(_))
    );
    if drained && !is_control {
        let err = PlatformError::new(
            ErrorCode::WorkerDraining,
            "DB Process 正在优雅停机，不再接受新请求",
        );
        reply_error(&ctx, &request, &err).await;
        return;
    }

    let Some(message) = request.message.clone() else {
        // 没有 message 的帧无法处理；单向通知（reply_to_seq=0）直接忽略。
        if request.reply_to_seq != 0 {
            let err = PlatformError::invalid_argument("帧没有携带任何 message");
            reply_error(&ctx, &request, &err).await;
        }
        return;
    };

    match message {
        rt::frame::Message::Execute(req) => handle_execute(ctx, request, req).await,
        rt::frame::Message::OpenSession(req) => handle_open_session(ctx, request, req).await,
        rt::frame::Message::CloseSession(_) => handle_close_session(ctx, request).await,
        rt::frame::Message::Begin(req) => handle_begin(ctx, request, req).await,
        rt::frame::Message::Commit(_) => handle_commit(ctx, request).await,
        rt::frame::Message::Rollback(_) => handle_rollback(ctx, request).await,
        rt::frame::Message::Cancel(req) => {
            handle_cancel(
                &ctx,
                &request.session_id,
                &req.target_request_id,
                &req.reason,
            );
        }
        rt::frame::Message::CancelNotice(notice) => {
            handle_cancel(
                &ctx,
                &notice.target_session_id,
                &notice.target_request_id,
                &notice.reason,
            );
        }
        rt::frame::Message::Health(_) => handle_health(ctx, request).await,
        rt::frame::Message::Shutdown(req) => handle_shutdown(ctx, request, req).await,
        rt::frame::Message::Hello(hello) => handle_peer_hello(ctx, request, hello).await,
        rt::frame::Message::SnapshotRequest(req) => handle_snapshot(ctx, request, req).await,
        // 双向含义的帧（流帧 / ACK / durable 通知）：本进程不会把它们当请求处理，
        // 单向通知直接忽略，避免与对端产生回帧风暴。
        other => {
            tracing::debug!(?other, "忽略本进程不处理的本地帧");
        }
    }
}

/// 帧的操作名（低基数，用于 span）。
fn operation_name(request: &rt::Frame) -> &'static str {
    match request.message {
        Some(rt::frame::Message::Execute(_)) => "execute",
        Some(rt::frame::Message::OpenSession(_)) => "open_session",
        Some(rt::frame::Message::CloseSession(_)) => "close_session",
        Some(rt::frame::Message::Begin(_)) => "begin",
        Some(rt::frame::Message::Commit(_)) => "commit",
        Some(rt::frame::Message::Rollback(_)) => "rollback",
        Some(rt::frame::Message::Cancel(_)) | Some(rt::frame::Message::CancelNotice(_)) => "cancel",
        Some(rt::frame::Message::Health(_)) => "health",
        Some(rt::frame::Message::Shutdown(_)) => "shutdown",
        Some(rt::frame::Message::Hello(_)) => "hello",
        _ => "other",
    }
}

// ------------------------------------------------------------------ 执行

/// 一次执行的结果（与协议无关的中间形态）。
struct ExecOutcome {
    /// 行结果（`None` = 非查询语句）。
    result: Option<domain::ResultSet>,
    /// 受影响行数。
    affected_rows: u64,
    /// 已 durable 的末端 LSN。
    wal_lsn: u64,
}

impl ExecOutcome {
    /// 结果集估算字节数（决定是否需要分块）。
    fn estimated_size(&self) -> usize {
        self.result.as_ref().map_or(0, |rs| rs.estimated_size())
    }
}

async fn handle_execute(ctx: Arc<ConnectionCtx>, request: rt::Frame, req: rt::ExecuteRequest) {
    let host = Arc::clone(&ctx.host);
    let started = Instant::now();

    if host.read_only() && !is_read_only_statement(&req.sql) {
        reply_error(&ctx, &request, &read_only_rejection()).await;
        return;
    }

    // 取消登记：语句边界检查要用它，Dispatch 的 CancelNotice 也靠它定位。
    let _guard = host
        .cancels
        .register(&request.request_id, &request.session_id);
    let flag = host
        .cancels
        .flag_for(&request.request_id)
        .unwrap_or_default();
    if flag.is_cancelled() {
        reply_error(&ctx, &request, &cancelled_error(flag.reason())).await;
        return;
    }

    let deadline = request_deadline(&request);
    if deadline.is_zero() {
        // 截止时间在到达时就已过期：不执行任何语句，直接回 DeadlineExceeded。
        let err = PlatformError::new(
            ErrorCode::DeadlineExceeded,
            format!("请求 {} 在到达时已超过截止时间", request.request_id),
        );
        reply_error(&ctx, &request, &err).await;
        return;
    }
    let outcome = run_with_deadline(
        deadline,
        execute_route(&ctx, &request, &req, &flag),
        &request.request_id,
    )
    .await;

    match outcome {
        Ok(exec) => {
            emit_result(&ctx, &request, &req, exec, started).await;
        }
        Err(err) => {
            tracing::debug!(error = %err, "执行失败");
            reply_error(&ctx, &request, &err).await;
        }
    }
}

/// 按是否有会话选择执行路径。
async fn execute_route(
    ctx: &Arc<ConnectionCtx>,
    request: &rt::Frame,
    req: &rt::ExecuteRequest,
    flag: &CancelFlag,
) -> Result<ExecOutcome, PlatformError> {
    if request.session_id.is_empty() {
        execute_stateless(ctx, req, flag).await
    } else {
        execute_in_session(ctx, request, req, flag).await
    }
}

/// 无会话执行：每次新建一条连接（Stateless by Default）。
async fn execute_stateless(
    ctx: &Arc<ConnectionCtx>,
    req: &rt::ExecuteRequest,
    flag: &CancelFlag,
) -> Result<ExecOutcome, PlatformError> {
    let adapter = Arc::clone(&ctx.host.adapter);
    let sql = req.sql.clone();
    let atomic = req.atomic;
    let flag = flag.clone();
    tokio::task::spawn_blocking(move || {
        let conn = adapter.connect()?;
        exec_on_conn(&conn, &sql, atomic, false, &flag)
    })
    .await
    .map_err(|err| internal_error(format!("执行线程异常：{err}")))?
}

/// 会话内执行：拿到会话状态锁后交给阻塞线程。
async fn execute_in_session(
    ctx: &Arc<ConnectionCtx>,
    request: &rt::Frame,
    req: &rt::ExecuteRequest,
    flag: &CancelFlag,
) -> Result<ExecOutcome, PlatformError> {
    let session = match ctx.host.sessions.lookup(&request.session_id) {
        SessionLookup::Found(session) => session,
        SessionLookup::Expired { reason, .. } => {
            return Err(reason.to_error(&request.session_id));
        }
        SessionLookup::Unknown => {
            // 进程重启 / fencing 之后的常见情形：会话已经不存在。
            return Err(session_lost(
                &request.session_id,
                "会话不存在（进程可能已重启或发生过 failover）",
            ));
        }
    };

    let state = session.state();
    let guard = state.lock_owned().await;
    let sql = req.sql.clone();
    let atomic = req.atomic;
    let flag = flag.clone();
    let session_ref = Arc::clone(&session);
    tokio::task::spawn_blocking(move || {
        let mut guard = guard;
        exec_on_session(&session_ref, &mut guard, &sql, atomic, &flag)
    })
    .await
    .map_err(|err| internal_error(format!("执行线程异常：{err}")))?
}

/// 会话内执行的阻塞体。
fn exec_on_session(
    session: &crate::session::Session,
    state: &mut SessionState,
    sql: &str,
    atomic: bool,
    flag: &CancelFlag,
) -> Result<ExecOutcome, PlatformError> {
    // 惰性事务超时：reaper 可能还没跑到，但过期事务绝不能再执行语句。
    if let Some(txn) = state.transaction.as_ref() {
        if now_ms() >= txn.expires_at_ms {
            let _ = state.conn.rollback();
            state.transaction = None;
            session.set_transaction_deadline(0);
            return Err(ExpiryReason::TransactionLifetimeExceeded.to_error(session.id()));
        }
    }
    let in_txn = state.transaction.is_some();
    // 只读事务里的写语句必须被拒绝：客户端开只读事务是为了拿到"这次事务里不会有写"的
    // 承诺，放行写入会让只读副本/读扩展节点产生写入。
    if let Some(txn) = state.transaction.as_ref() {
        if txn.read_only && !is_read_only_statement(sql) {
            return Err(PlatformError::new(
                ErrorCode::PermissionDenied,
                "只读事务中不允许执行写语句",
            ));
        }
    }
    let outcome = exec_on_conn(&state.conn, sql, atomic, in_txn, flag);
    session.touch(now_ms());
    outcome
}

/// 在一条引擎连接上执行 SQL（会话路径与无会话路径共用）。
fn exec_on_conn(
    conn: &EngineConnection,
    sql: &str,
    atomic: bool,
    in_txn: bool,
    flag: &CancelFlag,
) -> Result<ExecOutcome, PlatformError> {
    // 语句边界检查 ①：进入引擎之前。
    if flag.is_cancelled() {
        return Err(cancelled_error(flag.reason()));
    }

    // `atomic` 且当前没有显式事务时，把这条语句包成一个显式事务，
    // 这样中途失败/取消不会留下半截写入（引擎的 autocommit 只覆盖单条语句，
    // 而客户端用 `atomic` 表达的语义是"这条语句要么全做要么全不做"）。
    let wrapped = atomic && !in_txn && !conn.in_transaction();
    if wrapped {
        conn.begin()?;
    }

    let outcome = match conn.query(sql) {
        Ok(outcome) => outcome,
        Err(err) => {
            if wrapped {
                let _ = conn.rollback();
            }
            return Err(err);
        }
    };
    let wal_lsn = if wrapped {
        conn.commit()?
    } else {
        conn.durable_lsn()
    };

    let exec = match outcome {
        QueryOutcome::Rows(result) => ExecOutcome {
            affected_rows: result.affected_rows,
            result: Some(result),
            wal_lsn,
        },
        QueryOutcome::Affected { rows } => ExecOutcome {
            result: None,
            affected_rows: rows,
            wal_lsn,
        },
    };

    // 语句边界检查 ②：结果已经产出，但客户端已经不要了 —— 丢弃结果并回 CANCELLED。
    // 已经提交的写入不回滚（那是客户端的事务决策），这里只保证"不再继续往下跑"。
    if flag.is_cancelled() {
        return Err(cancelled_error(flag.reason()));
    }
    Ok(exec)
}

/// 按协议回结果：小结果一次性回，流式 / 大结果分块回。
async fn emit_result(
    ctx: &Arc<ConnectionCtx>,
    request: &rt::Frame,
    req: &rt::ExecuteRequest,
    exec: ExecOutcome,
    started: Instant,
) {
    let elapsed_micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    // `inline_row_limit = 0` 在 Dispatcher 侧表示"调用方不限制内联行数"，
    // 此时只有显式 want_stream 才分块 —— 否则会给出对端没有预期到的流。
    let over_limit =
        req.inline_row_limit > 0 && exec.estimated_size() > req.inline_row_limit as usize;
    let stream = req.want_stream || over_limit || exec.estimated_size() > STREAM_BYTES_THRESHOLD;

    if !stream {
        let result = exec.result.clone().map(data::ResultSet::from);
        let response = rt::ExecuteResponse {
            result,
            affected_rows: exec.affected_rows,
            elapsed_micros,
            wal_lsn: exec.wal_lsn,
        };
        send(
            ctx,
            frame::reply(
                request,
                ctx.next_seq(),
                rt::frame::Message::ExecuteResponse(response),
            ),
        )
        .await;
        return;
    }

    let columns: Vec<data::ColumnMeta> = exec
        .result
        .as_ref()
        .map(|rs| {
            rs.columns
                .iter()
                .cloned()
                .map(data::ColumnMeta::from)
                .collect()
        })
        .unwrap_or_default();
    let seq = ctx.next_seq();
    send(
        ctx,
        frame::stream_reply(
            request,
            seq,
            rt::frame::Message::StreamHeader(rt::StreamHeader { columns }),
        ),
    )
    .await;

    let mut rows_sent = 0u64;
    if let Some(result) = exec.result.as_ref() {
        for chunk in result.rows.chunks(ROWS_PER_BATCH) {
            rows_sent += chunk.len() as u64;
            let batch = rt::RowBatch {
                rows: chunk.iter().cloned().map(convert::row_to_proto).collect(),
                rows_sent,
            };
            let seq = ctx.next_seq();
            send(
                ctx,
                frame::stream_reply(request, seq, rt::frame::Message::Rows(batch)),
            )
            .await;
        }
    }

    let end = rt::StreamEnd {
        affected_rows: exec.affected_rows,
        wal_lsn: exec.wal_lsn,
        elapsed_micros,
    };
    let seq = ctx.next_seq();
    send(
        ctx,
        frame::stream_reply(request, seq, rt::frame::Message::StreamEnd(end)),
    )
    .await;
}

// ------------------------------------------------------------------ 会话

async fn handle_open_session(
    ctx: Arc<ConnectionCtx>,
    request: rt::Frame,
    req: rt::OpenSessionRequest,
) {
    let adapter = Arc::clone(&ctx.host.adapter);
    // `connect()` 只在引擎内建连接对象，不做磁盘 IO；但仍然放到阻塞线程，
    // 避免任何一次引擎内部实现变化把 tokio worker 拖住。
    let conn = match tokio::task::spawn_blocking(move || adapter.connect()).await {
        Ok(Ok(conn)) => conn,
        Ok(Err(err)) => {
            reply_error(&ctx, &request, &err).await;
            return;
        }
        Err(err) => {
            let err = internal_error(format!("建连线程异常：{err}"));
            reply_error(&ctx, &request, &err).await;
            return;
        }
    };

    let session =
        ctx.host
            .sessions
            .open(conn, req.idle_timeout_ms, req.max_transaction_lifetime_ms);
    let response = rt::OpenSessionResponse {
        session_id: session.id().to_string(),
        expires_at_unix_ms: session.expires_at_ms(),
    };
    tracing::debug!(
        session_id = %session.id(),
        idle_timeout_ms = session.idle_timeout().as_millis() as u64,
        "会话已打开"
    );
    send(
        &ctx,
        frame::reply(
            &request,
            ctx.next_seq(),
            rt::frame::Message::OpenSessionResponse(response),
        ),
    )
    .await;
}

async fn handle_close_session(ctx: Arc<ConnectionCtx>, request: rt::Frame) {
    let closed = ctx.host.sessions.close(&request.session_id).await;
    send(
        &ctx,
        frame::reply(
            &request,
            ctx.next_seq(),
            rt::frame::Message::CloseSessionResponse(rt::CloseSessionResponse { closed }),
        ),
    )
    .await;
}

async fn handle_begin(ctx: Arc<ConnectionCtx>, request: rt::Frame, req: rt::BeginRequest) {
    let session = match ctx.host.sessions.lookup(&request.session_id) {
        SessionLookup::Found(session) => session,
        SessionLookup::Expired { reason, .. } => {
            reply_error(&ctx, &request, &reason.to_error(&request.session_id)).await;
            return;
        }
        SessionLookup::Unknown => {
            let err = session_lost(&request.session_id, "会话不存在");
            reply_error(&ctx, &request, &err).await;
            return;
        }
    };
    if ctx.host.read_only() && !req.read_only {
        reply_error(&ctx, &request, &read_only_rejection()).await;
        return;
    }

    let lifetime = if req.max_lifetime_ms == 0 {
        session.max_transaction_lifetime()
    } else {
        // 只能收紧：超过会话上限的请求会被夹到上限。
        Duration::from_millis(req.max_lifetime_ms).min(session.max_transaction_lifetime())
    };
    let transaction_id = uuid::Uuid::now_v7().to_string();
    let expires_at_ms = now_ms() + lifetime.as_millis() as u64;

    let state = session.state();
    let guard = state.lock_owned().await;
    let read_only = req.read_only;
    let txn_id = transaction_id.clone();
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        let mut guard = guard;
        if guard.transaction.is_some() {
            return Err(PlatformError::new(
                ErrorCode::TransactionStateInvalid,
                "会话上已经有一个在途事务，必须先 Commit / Rollback",
            ));
        }
        guard.conn.begin()?;
        guard.transaction = Some(crate::session::Transaction {
            id: txn_id,
            read_only,
            expires_at_ms,
        });
        Ok::<(), PlatformError>(())
    })
    .await;

    match result {
        Ok(Ok(())) => {
            session.set_transaction_deadline(expires_at_ms);
            session.touch(now_ms());
            tracing::debug!(
                session_id = %session.id(),
                transaction_id = %transaction_id,
                elapsed_micros = started.elapsed().as_micros() as u64,
                "事务已开启"
            );
            let response = rt::TransactionResponse {
                transaction_id,
                expires_at_unix_ms: expires_at_ms,
            };
            send(
                &ctx,
                frame::reply(
                    &request,
                    ctx.next_seq(),
                    rt::frame::Message::TransactionResponse(response),
                ),
            )
            .await;
        }
        Ok(Err(err)) => reply_error(&ctx, &request, &err).await,
        Err(err) => {
            let err = internal_error(format!("开始事务线程异常：{err}"));
            reply_error(&ctx, &request, &err).await;
        }
    }
}

async fn handle_commit(ctx: Arc<ConnectionCtx>, request: rt::Frame) {
    let session = match ctx.host.sessions.lookup(&request.session_id) {
        SessionLookup::Found(session) => session,
        // 会话已终结 / 不存在：在途事务肯定已经不在，必须明确回 TRANSACTION_LOST。
        SessionLookup::Expired { reason, .. } => {
            let err = PlatformError::new(
                reason.error_code(),
                format!("会话 {} 已终结，事务丢失", request.session_id),
            );
            reply_error(&ctx, &request, &err).await;
            return;
        }
        SessionLookup::Unknown => {
            let err = transaction_lost(&request.session_id, "会话不存在（进程重启或 failover）");
            reply_error(&ctx, &request, &err).await;
            return;
        }
    };

    let state = session.state();
    let guard = state.lock_owned().await;
    let expected_txn = request.transaction_id.clone();
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        let mut guard = guard;
        let Some(txn) = guard.transaction.as_ref() else {
            return Err(PlatformError::transaction_lost(
                "会话上没有在途事务（可能已超时回滚或进程已重启）",
            ));
        };
        if !expected_txn.is_empty() && txn.id != expected_txn {
            return Err(PlatformError::transaction_lost(format!(
                "事务 id 不匹配：请求 {expected_txn}"
            )));
        }
        if now_ms() >= txn.expires_at_ms {
            let _ = guard.conn.rollback();
            guard.transaction = None;
            return Err(
                ExpiryReason::TransactionLifetimeExceeded.to_error("事务已超过最大存活时间")
            );
        }
        let lsn = guard.conn.commit()?;
        guard.transaction = None;
        Ok(lsn)
    })
    .await;

    match result {
        Ok(Ok(wal_lsn)) => {
            session.set_transaction_deadline(0);
            session.touch(now_ms());
            let response = rt::CommitResponse {
                wal_lsn,
                elapsed_micros: started.elapsed().as_micros() as u64,
            };
            send(
                &ctx,
                frame::reply(
                    &request,
                    ctx.next_seq(),
                    rt::frame::Message::CommitResponse(response),
                ),
            )
            .await;
        }
        Ok(Err(err)) => {
            // 提交失败时事务已经在引擎侧结束（回滚或 IO 失败），清掉本进程的记录。
            session.set_transaction_deadline(0);
            reply_error(&ctx, &request, &err).await;
        }
        Err(err) => {
            let err = internal_error(format!("提交线程异常：{err}"));
            reply_error(&ctx, &request, &err).await;
        }
    }
}

async fn handle_rollback(ctx: Arc<ConnectionCtx>, request: rt::Frame) {
    let session = match ctx.host.sessions.lookup(&request.session_id) {
        SessionLookup::Found(session) => session,
        SessionLookup::Expired { reason, .. } => {
            let err = PlatformError::new(
                reason.error_code(),
                format!("会话 {} 已终结，事务丢失", request.session_id),
            );
            reply_error(&ctx, &request, &err).await;
            return;
        }
        SessionLookup::Unknown => {
            let err = transaction_lost(&request.session_id, "会话不存在（进程重启或 failover）");
            reply_error(&ctx, &request, &err).await;
            return;
        }
    };

    let state = session.state();
    let guard = state.lock_owned().await;
    let result = tokio::task::spawn_blocking(move || {
        let mut guard = guard;
        if guard.transaction.take().is_none() {
            return Err(PlatformError::transaction_lost(
                "会话上没有在途事务（可能已超时回滚或进程已重启）",
            ));
        }
        guard.conn.rollback()?;
        Ok::<(), PlatformError>(())
    })
    .await;

    match result {
        Ok(Ok(())) => {
            session.set_transaction_deadline(0);
            session.touch(now_ms());
            send(
                &ctx,
                frame::reply(
                    &request,
                    ctx.next_seq(),
                    rt::frame::Message::RollbackResponse(rt::RollbackResponse {
                        rolled_back: true,
                    }),
                ),
            )
            .await;
        }
        Ok(Err(err)) => {
            session.set_transaction_deadline(0);
            reply_error(&ctx, &request, &err).await;
        }
        Err(err) => {
            let err = internal_error(format!("回滚线程异常：{err}"));
            reply_error(&ctx, &request, &err).await;
        }
    }
}

// ------------------------------------------------------------------ 取消 / 健康 / 停机

/// 取消：`target_request_id` 为空 = 取消该会话上的全部在途请求。
///
/// 取消是**单向**语义：本进程不回 CancelResponse，被取消的请求自己会以
/// `CANCELLED` 错误帧收尾——这样"谁取消的"与"请求的结果"始终只有一处出口。
fn handle_cancel(
    ctx: &Arc<ConnectionCtx>,
    target_session_id: &str,
    target_request_id: &str,
    reason: &str,
) {
    let registry = &ctx.host.cancels;
    let by_request = if target_request_id.is_empty() {
        false
    } else {
        registry.cancel(target_request_id)
    };
    let by_session = if target_session_id.is_empty() {
        0
    } else {
        registry.cancel_session(target_session_id)
    };
    tracing::info!(
        target_request_id,
        target_session_id,
        reason,
        by_request,
        by_session,
        in_flight = registry.len(),
        "收到取消请求"
    );
}

async fn handle_health(ctx: Arc<ConnectionCtx>, request: rt::Frame) {
    let response = rt::HealthResponse {
        database_id: ctx.host.database_id_text().to_string(),
        owner_epoch: ctx.host.config.owner_epoch,
        state: ctx.host.state().as_str().to_string(),
        rss_kib: rss_kib(),
        opened_connections: ctx.host.opened_connections.load(Ordering::Acquire),
        active_sessions: ctx.host.sessions.active_count() as u64,
        applied_lsn: ctx.host.durable_lsn(),
    };
    send(
        &ctx,
        frame::reply(
            &request,
            ctx.next_seq(),
            rt::frame::Message::HealthResponse(response),
        ),
    )
    .await;
}

async fn handle_shutdown(ctx: Arc<ConnectionCtx>, request: rt::Frame, req: rt::ShutdownRequest) {
    send(
        &ctx,
        frame::reply(
            &request,
            ctx.next_seq(),
            rt::frame::Message::ShutdownResponse(rt::ShutdownResponse { closing: true }),
        ),
    )
    .await;
    tracing::info!(graceful = req.graceful, "收到 Shutdown 请求");
    if !req.graceful {
        // 非优雅停机：不等待在途请求，直接以异常退出码结束（由 main 决定退出码）。
        ctx.host.exit_code.store(1, Ordering::Release);
    }
    ctx.host.shutdown.notify_waiters();
}

async fn handle_peer_hello(ctx: Arc<ConnectionCtx>, request: rt::Frame, hello: rt::Hello) {
    let verdict = crate::fencing::verify_peer_hello(
        &hello,
        ctx.host.database_id_text(),
        ctx.host.config.owner_epoch,
    );
    let (accepted, reason) = match verdict {
        crate::fencing::FenceVerdict::Accept => (true, String::new()),
        crate::fencing::FenceVerdict::Fenced { reason } => (false, reason),
    };
    let ack = rt::HelloAck {
        accepted,
        worker_id: ctx.host.config.worker_id.clone(),
        dispatcher_epoch: ctx.host.config.owner_epoch,
        reject_reason: reason.clone(),
    };
    send(
        &ctx,
        frame::reply(&request, ctx.next_seq(), rt::frame::Message::HelloAck(ack)),
    )
    .await;
    if !accepted {
        // 对端声明的身份与本进程不符：本进程不是这条连接的合法 Owner，立即退出。
        tracing::error!(reason = %reason, "握手身份不一致，触发 fencing 退出");
        ctx.host.fenced(&reason);
    }
}

/// 快照协调（架构 §11.4）：给出一个**一致快照点**，但不生成也不上传快照。
///
/// ## 为什么由本进程给这个点
///
/// 快照的抓取与上传在 Worker 侧（它有对象存储凭据，本进程没有），但「哪些字节已经
/// quorum durable」只有持有引擎的进程知道，因此本帧的契约是：
///
/// * 回帧的 `base_lsn` = **本进程此刻已经 durable 的末端 LSN**（exclusive）。Worker 据此
///   写 manifest，恢复时 Remote WAL 从该 LSN 继续 replay（架构 §11.2）；
/// * 「本地文件自洽」的保证 = 本地 WAL 文件的前 `base_lsn` 个字节就是那一份 durable
///   字节流。平台不变量：本地 WAL 的文件偏移即 LSN（`host::restore_from_remote` 就是把
///   回放得到的 LSN 直接当文件偏移播种的），且本地写总是先于远端确认落盘，因此
///   `[0, base_lsn)` 恰好是「完整提交前缀」，快照点之后引擎追加的字节不属于这个快照。
///   这里把该不变量**显式校验**（文件长度必须 >= base_lsn），不一致就报错而不是给一个
///   看似正常、实则对不上的 base_lsn。
///
/// ## 为什么不去 checkpoint 主库
///
/// 本平台的引擎把变更留在 WAL 里（主库只含库头），而 checkpoint 只能由引擎自己发起
/// （engine-adapter 不暴露入口）；TRUNCATE checkpoint 还会把本地 WAL 截断、让 Remote WAL
/// 进入新一代，那是另一条恢复路径。因此快照 = 「主库 + WAL 的 durable 前缀」这一对文件，
/// 由 Worker 原样上传。
///
/// 写入不被本帧阻塞：这里只读一个原子计数器与本地 WAL 的长度，不与会话/引擎抢锁。
async fn handle_snapshot(ctx: Arc<ConnectionCtx>, request: rt::Frame, req: rt::SnapshotRequest) {
    let base_lsn = ctx.host.durable_lsn();
    let wal_path = ctx.host.config.resolved_wal_path();
    // 只看长度，不做任何遍历/读取：快照重活（压缩、上传）在 Worker 侧。
    let wal_bytes = match std::fs::metadata(&wal_path) {
        Ok(meta) => meta.len(),
        // WAL 尚不存在 = 建库后还没有任何提交，等价于空文件（base_lsn 也必为 0）。
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
        Err(err) => {
            let err = PlatformError::new(
                ErrorCode::InternalError,
                format!("读取本地 WAL 元数据失败（{}）：{err}", wal_path.display()),
            );
            reply_error(&ctx, &request, &err).await;
            return;
        }
    };
    if wal_bytes < base_lsn {
        // 本地字节流比 durable 记账还短：本地 WAL 与 Remote WAL 已经不可能逐字节对应，
        // 这时候给出任何 base_lsn 都会让 Worker 上传一份不自洽的快照，必须显式失败。
        let err = PlatformError::new(
            ErrorCode::InternalError,
            format!(
                "本地 WAL 只有 {wal_bytes} 字节，短于已 durable 的 {base_lsn} 字节，拒绝报告快照点"
            ),
        );
        reply_error(&ctx, &request, &err).await;
        return;
    }

    tracing::info!(
        snapshot_id = %req.snapshot_id,
        base_lsn,
        wal_bytes,
        pending_bytes = wal_bytes.saturating_sub(base_lsn),
        "已给出快照点（base_lsn 之后的字节尚未 quorum durable，不进快照）"
    );
    // checksum / size_bytes 留空：artifact 的权威元数据由 Worker 写 manifest 时产出，
    // 本进程没有对象存储通道，报一个自己没算过的摘要只会制造第二份真相。
    send(
        &ctx,
        frame::reply(
            &request,
            ctx.next_seq(),
            rt::frame::Message::SnapshotResponse(rt::SnapshotResponse {
                snapshot_id: req.snapshot_id,
                base_lsn,
                checksum: String::new(),
                size_bytes: 0,
            }),
        ),
    )
    .await;
}

// ------------------------------------------------------------------ 公共小工具

/// 解析并计算本次请求的剩余预算。
fn request_deadline(request: &rt::Frame) -> Duration {
    if request.deadline_unix_ms == 0 {
        return DEFAULT_REQUEST_DEADLINE;
    }
    let now = now_ms();
    if request.deadline_unix_ms <= now {
        // 已经过期：不执行，立刻回 DeadlineExceeded。
        Duration::ZERO
    } else {
        Duration::from_millis(request.deadline_unix_ms - now).min(DEFAULT_REQUEST_DEADLINE)
    }
}

/// 给任意处理 future 套上 deadline。
async fn run_with_deadline<F, T>(
    budget: Duration,
    future: F,
    request_id: &str,
) -> Result<T, PlatformError>
where
    F: std::future::Future<Output = Result<T, PlatformError>>,
{
    match tokio::time::timeout(budget, future).await {
        Ok(result) => result,
        Err(_) => Err(PlatformError::new(
            ErrorCode::DeadlineExceeded,
            format!("请求 {request_id} 超过截止时间"),
        )),
    }
}

/// 写一帧，失败只记日志（连接断了就没什么可回的）。
async fn send(ctx: &Arc<ConnectionCtx>, frame: rt::Frame) {
    if let Err(err) = ctx.write(&frame).await {
        tracing::debug!(peer = %ctx.peer, error = %err, "回帧失败（对端可能已断开）");
    }
}

async fn reply_error(ctx: &Arc<ConnectionCtx>, request: &rt::Frame, error: &PlatformError) {
    let frame = frame::error_reply(request, ctx.next_seq(), error);
    send(ctx, frame).await;
}

/// 取消错误（保留首次取消原因，便于定位是谁取消的）。
fn cancelled_error(reason: Option<String>) -> PlatformError {
    PlatformError::new(
        ErrorCode::Cancelled,
        reason.unwrap_or_else(|| "请求已被取消".to_string()),
    )
}

fn internal_error(message: impl Into<String>) -> PlatformError {
    PlatformError::internal(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 过期的 deadline 必须立刻判定为 0 预算，而不是被夹成默认值。
    #[test]
    fn expired_deadline_yields_zero_budget() {
        let request = rt::Frame {
            deadline_unix_ms: now_ms().saturating_sub(1),
            ..Default::default()
        };
        assert_eq!(request_deadline(&request), Duration::ZERO);
    }

    /// 未声明 deadline -> 兜底值；声明得过远 -> 夹到兜底值（不允许无限等）。
    #[test]
    fn deadline_is_bounded_by_default() {
        let request = rt::Frame::default();
        assert_eq!(request_deadline(&request), DEFAULT_REQUEST_DEADLINE);

        let request = rt::Frame {
            deadline_unix_ms: now_ms() + 3_600_000,
            ..Default::default()
        };
        assert_eq!(request_deadline(&request), DEFAULT_REQUEST_DEADLINE);
    }
}
