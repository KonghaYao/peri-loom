//! `platform.wal.v1.RemoteWal` 的 gRPC 实现（架构 §17.8 对外契约）。
//!
//! 分层：本模块只做「proto <-> 内部类型」的翻译与指标埋点，所有 Raft 语义
//! （fencing、幂等、quorum durable、超时）都在 [`crate::raft_group`] 内完成，
//! 保证「同一语义只有一处实现」。
//!
//! # 错误返回约定
//!
//! proto 为每个响应都定义了 `platform.common.v1.PlatformError error` 字段，因此：
//! - **业务错误**（非 leader、epoch 落后、未 durable、区间被 Trim）走 in-band 的
//!   `error` 字段，客户端不需要解析 gRPC status 就能拿到结构化错误码；
//! - `tonic::Status` 只用于「请求根本没进入业务语义」的情况（协议级失败），
//!   例如 streaming 响应构造失败。
//!
//! # ReadRange 的读一致性
//!
//! `ReadRange` **不要求本节点是 leader**：它的用途是 failover replay 与冷启动恢复，
//! 从任一已 apply 该区间的副本读都可以。副本若落后（区间末端尚未 apply）会返回
//! `WAL_NOT_DURABLE`（可重试），客户端应重试该副本或换副本，而不是把它当作数据丢失。

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use observability::metrics::{metric_names, record_wal_fenced_rejected};
use protocol::common as proto_common;
use protocol::convert::RequestContext as PlatformRequestContext;
use protocol::wal;
use tonic::codegen::tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use crate::config::WalConfig;
use crate::error::{WalError, WalResult};
use crate::raft_group::{AppendParams, WalHandle};
use crate::state_machine::{ReadRangeCursor, DEFAULT_CHUNK_BYTES, MAX_CHUNK_BYTES};

/// 服务端允许的最大请求消息（Append 批次）与响应消息字节数。
///
/// tonic 默认解码上限是 4 MiB，WAL 批次（一次 checkpoint 后的批量写入 / 大事务）
/// 很容易超过：不放开会在客户端侧看到 `message too large` 这类与业务无关的失败。
/// 上限与 peer 帧上限保持同一量级（64 MiB），避免单条消息把节点内存打满。
const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Remote WAL gRPC 服务。
pub struct WalService {
    handle: WalHandle,
    config: Arc<WalConfig>,
}

impl WalService {
    /// 由 Raft Group 句柄与配置构造。
    pub fn new(handle: WalHandle, config: Arc<WalConfig>) -> Self {
        Self { handle, config }
    }

    /// 本进程承载的 shard 标识（Health 用）。
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)]
    pub fn shard_id(&self) -> &str {
        &self.config.shard_id
    }
}

/// 取两个 deadline 中更紧的一个（0 表示未设置）。
///
/// 为什么需要两次取值：AppendRequest 同时有顶层 `deadline_unix_ms` 与 `context.deadline_unix_ms`，
/// 调用方（Worker / Server）可能只填其中一个。取较紧者可以保证无论哪一侧先超时，
/// 服务端都不会比调用方等得更久。
fn tighter_deadline(a: u64, b: u64) -> u64 {
    match (a, b) {
        (0, 0) => 0,
        (0, b) => b,
        (a, 0) => a,
        (a, b) => a.min(b),
    }
}

/// 从 proto 请求上下文抽取追踪与 deadline 信息。
fn context_of(context: Option<proto_common::RequestContext>) -> PlatformRequestContext {
    context
        .map(PlatformRequestContext::from)
        .unwrap_or_default()
}

/// 把内部错误收敛成 proto 错误体。
fn error_body(err: &WalError) -> proto_common::PlatformError {
    err.to_proto()
}

/// `ReadRange` 的惰性响应流：每次 poll 只把**下一块**数据编码成 proto。
///
/// 数据来源是 [`ReadRangeCursor`] 持有的 `Bytes` 切片（状态机内存的引用计数切片），
/// 因此流本身不驻留区间数据；`to_vec()` 只发生在真正要把该块写上线的时候 ——
/// prost 的 `bytes` 字段拥有所有权，这一次复制无法避免，但它不再是「整段先复制一遍、
/// 每块再复制一遍」。
struct WalChunkStream {
    /// 分块游标（区间已校验，产出必然是连续的）。
    cursor: ReadRangeCursor,
}

impl Stream for WalChunkStream {
    type Item = Result<wal::WalChunk, Status>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.cursor.next_chunk() {
            None => Poll::Ready(None),
            Some(chunk) => Poll::Ready(Some(Ok(wal::WalChunk {
                error: None,
                start_lsn: chunk.start_lsn,
                data: chunk.data.to_vec(),
                last: chunk.last,
                wal_file_offset: chunk.file_offset,
                reset_wal: chunk.reset_wal,
            }))),
        }
    }
}

/// 记录 Append 结果指标。
///
/// 标签只使用**有界**取值（outcome / reason），不用 database_id：
/// DB 数量在一个 shard 上可能上万，把它作为标签会让 Prometheus 时间序列爆炸。
fn record_append_metrics(outcome: &'static str, reason: &'static str, micros: u64) {
    metrics::histogram!(
        metric_names::WAL_APPEND_LATENCY_MICROS,
        "outcome" => outcome,
    )
    .record(micros as f64);
    metrics::counter!(metric_names::WAL_APPEND_TOTAL, "outcome" => outcome).increment(1);
    if reason != "none" {
        metrics::counter!(metric_names::WAL_APPEND_ERROR_TOTAL, "reason" => reason).increment(1);
    }
}

/// 错误 -> 指标 reason 标签（有界枚举，禁止自由文本）。
const fn error_reason(err: &WalError) -> &'static str {
    match err {
        WalError::NotLeader { .. } => "not_leader",
        WalError::StaleEpoch { .. } => "stale_epoch",
        WalError::EpochNotMonotonic { .. } => "epoch_not_monotonic",
        WalError::IdempotencyConflict { .. } => "idempotency_conflict",
        WalError::InvalidArgument(_) => "invalid_argument",
        WalError::DbNotFound(_) => "db_not_found",
        WalError::NotDurable(_) => "not_durable",
        WalError::RangeTrimmed { .. } => "range_trimmed",
        WalError::AppendTimeout { .. } => "timeout",
        WalError::Storage(_) => "storage",
        WalError::Raft(_) => "raft",
        WalError::Internal(_) => "internal",
    }
}

/// 记录一次 fencing 拒绝（架构 §11.3 / §16：旧 owner_epoch 的写入必须可见可告警）。
///
/// 只有 owner_epoch 冲突才是 fencing：
/// - [`WalError::StaleEpoch`]：旧 Owner 的 Append 被 Storage-level Fencing 拒绝
///   （预检拒绝与状态机 apply 拒绝都是这一个错误类型）；
/// - [`WalError::EpochNotMonotonic`]：SetOwnerEpoch 回退被拒（等价于放行旧 Owner）。
///
/// 其它失败（非 leader / 超时 / 存储故障）**不**计入本计数器：混进来会让
/// 「fencing 是否真的在生效」这个信号被无关故障淹没，也会破坏「拒绝率」的分母语义。
fn record_fencing_rejection(kind: &'static str, err: &WalError) {
    if matches!(
        err,
        WalError::StaleEpoch { .. } | WalError::EpochNotMonotonic { .. }
    ) {
        record_wal_fenced_rejected(kind, error_reason(err));
    }
}

#[tonic::async_trait]
impl wal::remote_wal_server::RemoteWal for WalService {
    /// 追加 WAL 数据：返回成功 = quorum durable（架构 §11.1）。
    async fn append(
        &self,
        request: Request<wal::AppendRequest>,
    ) -> Result<Response<wal::AppendResponse>, Status> {
        let started = Instant::now();
        let request = request.into_inner();
        let context = context_of(request.context.clone());
        let deadline_unix_ms = tighter_deadline(
            request.deadline_unix_ms,
            protocol::convert::deadline_ms_from(context.deadline),
        );

        let span = observability::RequestSpan::builder()
            .external_request_id(context.request_id.clone())
            .trace_id(context.trace_id.clone())
            .db_id(request.database_id.clone())
            .owner_epoch(request.owner_epoch)
            .build()
            .start();
        let _entered = span.enter();

        let bytes_len = request.wal_bytes.len();
        let params = AppendParams {
            database_id: request.database_id,
            owner_epoch: request.owner_epoch,
            start_lsn: request.start_lsn,
            wal_file_offset: request.wal_file_offset,
            reset_wal: request.reset_wal,
            bytes: request.wal_bytes,
            append_id: request.append_id,
            contains_commit_frame: request.contains_commit_frame,
            deadline_unix_ms,
            trace_id: context.trace_id.clone(),
        };

        let response = match self.handle.append(params).await {
            Ok(ack) => {
                let micros = started.elapsed().as_micros() as u64;
                let outcome = if ack.deduplicated {
                    "deduplicated"
                } else {
                    "ok"
                };
                record_append_metrics(outcome, "none", micros);
                span.record_wal_lsn(ack.durable_lsn);
                if ack.idempotency_window_evicted {
                    // 幂等窗口开始逐出最旧的键：极长重试链里的旧 append_id 之后可能
                    // 不再被识别为重复写。proto 的 AppendResponse 没有承载该标记的
                    // 字段，因此这里用结构化日志把 detail 暴露给运维/告警
                    // （db_id 由当前 span 提供，无需重复克隆参数）。
                    warn!(
                        idempotency_window_evicted = true,
                        "幂等键窗口已满，逐出最旧的键（该键对应的重试将不再被去重）"
                    );
                }
                debug!(
                    durable_lsn = ack.durable_lsn,
                    deduplicated = ack.deduplicated,
                    acked_replicas = ack.acked_replicas.len(),
                    bytes = bytes_len,
                    "Append 已 quorum durable"
                );
                wal::AppendResponse {
                    error: None,
                    durable_lsn: ack.durable_lsn,
                    acked_replicas: ack.acked_replicas,
                    append_latency_micros: micros,
                    deduplicated: ack.deduplicated,
                }
            }
            Err(err) => {
                let micros = started.elapsed().as_micros() as u64;
                // 未 durable 一律不返回成功：outcome=error 让告警能直接按此聚合
                record_append_metrics("error", error_reason(&err), micros);
                // 旧 owner_epoch 的 Append 被拒必须单独可观测（§11.3 / §16 验收）
                record_fencing_rejection("append", &err);
                warn!(error = %err, "Append 被拒绝");
                wal::AppendResponse {
                    error: Some(error_body(&err)),
                    // 明确回 0：避免调用方把「上一次的 LSN」误当成本次结果
                    durable_lsn: 0,
                    acked_replicas: Vec::new(),
                    append_latency_micros: micros,
                    deduplicated: false,
                }
            }
        };
        Ok(Response::new(response))
    }

    /// Server streaming 响应类型：装箱以隐藏具体流实现。
    type ReadRangeStream = Pin<Box<dyn Stream<Item = Result<wal::WalChunk, Status>> + Send>>;

    /// 读取 `[start_lsn, end_lsn)` 区间的 WAL（分块下发）。
    ///
    /// # 为什么是惰性流
    ///
    /// 区间校验（Trim 水位 / durable 末端 / 段间空洞）在取得游标时一次完成，
    /// 数据本身**不在**这里物化：游标只持有状态机内 `Bytes` 的切片，每个 chunk 在
    /// [`WalChunkStream::poll_next`] 里才被编码成 proto。这样服务一个大区间时服务端
    /// 的峰值内存是 O(单块) 而不是 O(区间)，也不会先复制整段再逐块复制一次
    /// （旧实现把整个区间的数据 `to_vec()` 进一个 `Vec`，等于把区间放大了一倍）。
    ///
    /// 校验必须前置的理由：若先发数据再发现区间非法，客户端只能丢弃已收到的数据并
    /// 重试整个区间；先在锁内判定完，流一旦开始产出数据就一定能走完。
    async fn read_range(
        &self,
        request: Request<wal::ReadRangeRequest>,
    ) -> Result<Response<Self::ReadRangeStream>, Status> {
        let request = request.into_inner();
        let max_chunk = if request.max_chunk_bytes == 0 {
            DEFAULT_CHUNK_BYTES
        } else {
            (request.max_chunk_bytes as usize).min(MAX_CHUNK_BYTES)
        };

        let cursor = self.handle.read_range_cursor(
            &request.database_id,
            request.start_lsn,
            request.end_lsn,
            max_chunk,
        );

        match cursor {
            Ok(cursor) => {
                debug!(
                    db_id = %request.database_id,
                    start_lsn = request.start_lsn,
                    end_lsn = request.end_lsn,
                    max_chunk_bytes = max_chunk,
                    "ReadRange 开始流式下发"
                );
                Ok(Response::new(Box::pin(WalChunkStream { cursor })))
            }
            Err(err) => {
                // 读失败也用 chunk 承载错误：proto 的 WalChunk 定义了 error 字段，
                // 让客户端在同一个流里拿到结构化错误码（与 unary 方法一致）。
                let body = error_body(&err);
                warn!(
                    db_id = %request.database_id,
                    start_lsn = request.start_lsn,
                    end_lsn = request.end_lsn,
                    error = %err,
                    "ReadRange 失败"
                );
                let chunk = wal::WalChunk {
                    error: Some(body),
                    start_lsn: request.start_lsn,
                    data: Vec::new(),
                    last: true,
                    wal_file_offset: 0,
                    reset_wal: false,
                };
                Ok(Response::new(Box::pin(tonic::codegen::tokio_stream::iter(
                    vec![Ok(chunk)],
                ))))
            }
        }
    }

    /// 设置 / 提升 owner epoch（fencing：拒绝所有更低 epoch 的 Append）。
    async fn set_owner_epoch(
        &self,
        request: Request<wal::SetOwnerEpochRequest>,
    ) -> Result<Response<wal::SetOwnerEpochResponse>, Status> {
        let request = request.into_inner();
        let context = context_of(request.context.clone());
        let deadline_unix_ms = tighter_deadline(
            request.deadline_unix_ms,
            protocol::convert::deadline_ms_from(context.deadline),
        );

        let result = self
            .handle
            .set_owner_epoch(
                &request.database_id,
                request.owner_epoch,
                &request.worker_id,
                &request.reason,
                deadline_unix_ms,
                context.trace_id.clone(),
            )
            .await;

        let response = match result {
            Ok((applied_epoch, known_lsn)) => wal::SetOwnerEpochResponse {
                error: None,
                applied_epoch,
                known_lsn,
            },
            Err(err) => {
                debug!(error = %err, "SetOwnerEpoch 失败");
                // epoch 回退被拒 = fencing 拒绝（§11.3）：与 Append 同一计数器，用 kind 区分
                record_fencing_rejection("set_owner_epoch", &err);
                wal::SetOwnerEpochResponse {
                    error: Some(error_body(&err)),
                    applied_epoch: 0,
                    known_lsn: 0,
                }
            }
        };
        Ok(Response::new(response))
    }

    /// 截断 `lsn` 之前的 WAL（必须携带 snapshot_id）。
    async fn trim_before_lsn(
        &self,
        request: Request<wal::TrimBeforeLsnRequest>,
    ) -> Result<Response<wal::TrimBeforeLsnResponse>, Status> {
        let request = request.into_inner();
        let result = self
            .handle
            .trim_before_lsn(
                &request.database_id,
                request.lsn,
                &request.snapshot_id,
                request.deadline_unix_ms,
                String::new(),
            )
            .await;
        let response = match result {
            Ok(trimmed_before_lsn) => wal::TrimBeforeLsnResponse {
                error: None,
                trimmed_before_lsn,
            },
            Err(err) => {
                debug!(error = %err, "TrimBeforeLsn 失败");
                wal::TrimBeforeLsnResponse {
                    error: Some(error_body(&err)),
                    trimmed_before_lsn: 0,
                }
            }
        };
        Ok(Response::new(response))
    }

    /// 查询某 DB 的 WAL 状态。
    async fn get_wal_status(
        &self,
        request: Request<wal::GetWalStatusRequest>,
    ) -> Result<Response<wal::GetWalStatusResponse>, Status> {
        let request = request.into_inner();
        let response = match self.handle.wal_status(&request.database_id) {
            Ok(status) => wal::GetWalStatusResponse {
                error: None,
                has_data: status.has_data,
                first_lsn: status.first_lsn,
                last_lsn: status.last_lsn,
                owner_epoch: status.owner_epoch,
                append_count: status.append_count,
            },
            Err(err) => wal::GetWalStatusResponse {
                error: Some(error_body(&err)),
                has_data: false,
                first_lsn: 0,
                last_lsn: 0,
                owner_epoch: 0,
                append_count: 0,
            },
        };
        Ok(Response::new(response))
    }

    /// 节点与 shard 的 Raft 运行态（用于运维探测与客户端选 leader）。
    async fn health(
        &self,
        _request: Request<wal::HealthRequest>,
    ) -> Result<Response<wal::HealthResponse>, Status> {
        let health = self.handle.health();
        Ok(Response::new(wal::HealthResponse {
            healthy: true,
            is_leader: health.is_leader,
            term: health.term,
            // 0 表示「暂无 leader（选举中）」：客户端据此换节点重试
            leader_id: if health.leader_id == 0 {
                String::new()
            } else {
                health.leader_id.to_string()
            },
            commit_index: health.commit_index,
            applied_index: health.applied_index,
            node_id: self.config.node_id.to_string(),
            shard_id: self.config.shard_id.clone(),
        }))
    }

    /// 增加 Raft 成员（运维）。endpoint 会随 conf change 一起复制，
    /// 保证所有副本对「新成员怎么连」有同一份认知。
    async fn add_member(
        &self,
        request: Request<wal::AddMemberRequest>,
    ) -> Result<Response<wal::AddMemberResponse>, Status> {
        let request = request.into_inner();
        let result = self
            .handle
            .add_member(request.member_id, &request.endpoint, String::new())
            .await;
        let response = match result {
            Ok(()) => wal::AddMemberResponse { error: None },
            Err(err) => {
                debug!(error = %err, "AddMember 失败");
                wal::AddMemberResponse {
                    error: Some(error_body(&err)),
                }
            }
        };
        Ok(Response::new(response))
    }

    /// 移除 Raft 成员（运维）。
    async fn remove_member(
        &self,
        request: Request<wal::RemoveMemberRequest>,
    ) -> Result<Response<wal::RemoveMemberResponse>, Status> {
        let request = request.into_inner();
        let result = self
            .handle
            .remove_member(request.member_id, String::new())
            .await;
        let response = match result {
            Ok(()) => wal::RemoveMemberResponse { error: None },
            Err(err) => {
                debug!(error = %err, "RemoveMember 失败");
                wal::RemoveMemberResponse {
                    error: Some(error_body(&err)),
                }
            }
        };
        Ok(Response::new(response))
    }
}

/// 构造 gRPC server（含消息大小上限），供主程序注册到 tonic。
pub fn server(service: WalService) -> wal::remote_wal_server::RemoteWalServer<WalService> {
    wal::remote_wal_server::RemoteWalServer::new(service)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES)
}

/// 启动 gRPC server 的辅助：绑定 -> serve（fail fast 绑定，错误从启动路径返回）。
///
/// 绑定用同步 `TcpListener`：端口占用必须在启动阶段暴露，而不是让服务"起来了但没人能连上"。
/// 传入 `SocketAddr` 的端口为 0 时由内核分配，返回值给出真实地址（集成测试用）。
pub fn bind_listener(addr: std::net::SocketAddr) -> WalResult<tokio::net::TcpListener> {
    let listener = std::net::TcpListener::bind(addr)
        .map_err(|err| WalError::Internal(format!("绑定 gRPC 监听地址 {addr} 失败：{err}")))?;
    listener
        .set_nonblocking(true)
        .map_err(|err| WalError::Internal(format!("设置 gRPC 监听非阻塞失败：{err}")))?;
    tokio::net::TcpListener::from_std(listener)
        .map_err(|err| WalError::Internal(format!("注册 gRPC 监听失败：{err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tighter_deadline_prefers_earliest_set_value() {
        assert_eq!(tighter_deadline(0, 0), 0, "两侧都未设置 -> 不设 deadline");
        assert_eq!(tighter_deadline(0, 100), 100);
        assert_eq!(tighter_deadline(50, 0), 50);
        assert_eq!(tighter_deadline(50, 100), 50);
        assert_eq!(tighter_deadline(100, 50), 50);
    }

    #[test]
    fn error_reason_is_bounded_enum() {
        // 标签值必须来自固定集合：任何自由文本都会让指标基数失控
        let cases = [
            WalError::NotLeader {
                shard: "shard-0".into(),
                leader: Some(2),
            },
            WalError::StaleEpoch {
                recorded: 9,
                requested: 8,
            },
            WalError::EpochNotMonotonic {
                recorded: 9,
                requested: 9,
            },
            WalError::IdempotencyConflict {
                append_id: "a".into(),
                recorded_start_lsn: 1,
                requested_start_lsn: 2,
            },
            WalError::InvalidArgument("x".into()),
            WalError::DbNotFound("db".into()),
            WalError::NotDurable("x".into()),
            WalError::RangeTrimmed {
                start_lsn: 1,
                trimmed_before_lsn: 2,
            },
            WalError::AppendTimeout {
                timeout_ms: 1,
                log_index: None,
            },
            WalError::Storage("x".into()),
            WalError::Raft("x".into()),
            WalError::Internal("x".into()),
        ];
        for err in &cases {
            let reason = error_reason(err);
            assert!(!reason.is_empty());
            assert!(
                reason.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "标签值必须是 ASCII 小写枚举：{reason}"
            );
        }
    }

    #[test]
    fn context_maps_deadline_and_trace_id() {
        let raw = proto_common::RequestContext {
            request_id: "req-1".into(),
            trace_id: "trace-1".into(),
            deadline_unix_ms: crate::time::now_unix_ms() + 5_000,
            ..Default::default()
        };
        let ctx = context_of(Some(raw));
        assert_eq!(ctx.request_id, "req-1");
        assert_eq!(ctx.trace_id, "trace-1");
        assert!(
            ctx.deadline.is_some(),
            "非 0 deadline 必须转成进程内 deadline"
        );
        assert!(context_of(None).deadline.is_none());
    }

    #[test]
    fn error_body_uses_platform_error_codes() {
        let err = WalError::NotLeader {
            shard: "shard-0".into(),
            leader: Some(7),
        };
        let body = error_body(&err);
        assert_eq!(body.code, proto_common::ErrorCode::WalNotLeader as i32);
        assert!(body.detail_json.contains("leader_id"));
    }
}
