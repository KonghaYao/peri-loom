//! `WalClient` 实现：端点轮换 + 有界重试 + fencing 短路 + leader 缓存。
//!
//! 重试循环的骨架只有一条：**要么拿到 leader 的成功响应，要么返回错误**。
//! 任何一条分支都不允许把「未确认 durable」当成成功（架构 §11.1）。

use std::sync::Arc;
use std::time::{Duration, Instant};

use domain::error::{ErrorCode, PlatformError, Result};
use domain::{DatabaseId, Lsn, WorkerId};
use protocol::convert::{deadline_ms_from, RequestContext};
use protocol::wal;
use protocol::wal::remote_wal_client::RemoteWalClient;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use tracing::Instrument;

use crate::channel::ChannelPool;
use crate::config::WalClientConfig;
use crate::endpoint::{AttemptPlan, EndpointSet};
use crate::error::{
    is_fencing_code, is_success_error, platform_error_from_proto, AttemptFailure, Disposition,
    FailureLog,
};
use crate::metrics;
use crate::model::{AppendOutcome, AppendWalRequest, WalHealth, WalSegment, WalStatus};

/// 第一次重试前的退避基数。
const BACKOFF_BASE: Duration = Duration::from_millis(5);
/// 退避上限：重试次数很少（默认 4 次），上限只用于防止将来调大 max_attempts 后退避失控。
const BACKOFF_CAP: Duration = Duration::from_millis(160);

/// Remote WAL 客户端。
///
/// `Clone` 是廉价的：内部共享同一个连接池与 leader 缓存，多个调用方（DB 进程的写路径、
/// failover 恢复路径）应复用同一个实例，让 leader 缓存真正生效。
#[derive(Clone, Debug)]
pub struct WalClient {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    config: WalClientConfig,
    endpoints: EndpointSet,
    channels: ChannelPool,
}

/// 重试循环的元参数。
#[derive(Clone, Copy)]
struct RunOptions {
    /// 操作名（日志与错误消息使用）。
    op: &'static str,
    /// 成功是否证明该端点就是 leader。
    ///
    /// `append` / `set_owner_epoch` / `trim_before` 必须是 leader 才能成功，因此可以把
    /// 该端点缓存为下次的起点；`read_range` / `status` 副本也能服务，把它们当成 leader
    /// 只会让后续写路径多打一次 `WAL_NOT_LEADER`。
    proves_leader: bool,
}

impl WalClient {
    /// 构造客户端。
    ///
    /// 只做配置校验（端点形态、尝试次数），**不建立连接**：WAL 副本可能在客户端启动后
    /// 才就绪，构造期失败会让整个 Worker 起不来。连接在首次调用时惰性建立。
    pub fn new(config: WalClientConfig) -> Result<Self> {
        if config.endpoints.is_empty() {
            return Err(PlatformError::invalid_argument(
                "wal-client: endpoints 不能为空，必须给出全部 WAL 副本的 gRPC 地址",
            ));
        }
        if config.max_attempts == 0 {
            return Err(PlatformError::invalid_argument(
                "wal-client: max_attempts 必须 >= 1（该值含首次尝试）",
            ));
        }
        for endpoint in &config.endpoints {
            validate_endpoint(endpoint)?;
        }

        let endpoints =
            EndpointSet::new(config.endpoints.clone(), config.allow_unlisted_leader_hint);
        let channels = ChannelPool::new(config.connect_timeout);
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                endpoints,
                channels,
            }),
        })
    }

    /// 配置的 WAL 副本端点（去重后，保持配置顺序）。
    pub fn endpoints(&self) -> &[String] {
        self.inner.endpoints.as_slice()
    }

    /// 设置 / 提升 owner epoch（storage-level fencing，架构 §11.3）。
    ///
    /// 成功返回服务端已应用的 epoch。出现以下情况会把所有权问题直接暴露给调用方，
    /// 而不是静默继续：
    /// - 服务端已有更高 epoch（说明其它 Owner 已经接管该 DB）-> `WAL_APPEND_REJECTED`；
    /// - 服务端返回的 `applied_epoch` 低于请求值（fence 没生效）-> 视为未生效并重试。
    pub async fn set_owner_epoch(
        &self,
        db: &DatabaseId,
        epoch: u64,
        worker: &WorkerId,
    ) -> Result<u64> {
        let span = tracing::info_span!(
            // 字段名与 observability::context::field_names 一致（db_id / owner_epoch / wal_lsn / worker_id）
            "wal_client.set_owner_epoch",
            db_id = %db,
            owner_epoch = epoch,
            wal_lsn = tracing::field::Empty,
            worker_id = %worker,
        );
        self.set_owner_epoch_inner(db, epoch, worker)
            .instrument(span)
            .await
    }

    async fn set_owner_epoch_inner(
        &self,
        db: &DatabaseId,
        epoch: u64,
        worker: &WorkerId,
    ) -> Result<u64> {
        let timeout = self.inner.config.request_timeout;

        self.run_unary(
            RunOptions {
                op: "set_owner_epoch",
                proves_leader: true,
            },
            move |mut client| {
                // 每次尝试都用**本次尝试的时刻**重新生成 deadline（见 append_inner 的说明）：
                // 复用首次构造的请求会让重试带着已过期的 deadline 上线，服务端立即放弃。
                let deadline = Instant::now() + timeout;
                let request = request_with_timeout(
                    self.set_owner_epoch_proto_request(db, epoch, worker, deadline),
                    timeout,
                );
                async move { client.set_owner_epoch(request).await }
            },
            move |endpoint, response: wal::SetOwnerEpochResponse| {
                if let Some(error) = response.error.as_ref() {
                    if !is_success_error(Some(error)) {
                        let error = platform_error_from_proto(error);
                        // 服务端把当前 epoch 放在响应体里时，用它补全 fencing detail
                        if is_fencing_code(error.code) {
                            let actual = (response.applied_epoch > 0)
                                .then_some(response.applied_epoch);
                            return Err(AttemptFailure::fencing_from_server(
                                endpoint,
                                error,
                                Some(epoch),
                                actual,
                            ));
                        }
                        return Err(AttemptFailure::from_server(endpoint, error, Some(epoch)));
                    }
                }
                match response.applied_epoch.cmp(&epoch) {
                    std::cmp::Ordering::Equal => Ok(epoch),
                    // 服务端记录了一个更高的 epoch：别的 Owner 已经接管，本节点不得继续写
                    std::cmp::Ordering::Greater => {
                        let error = PlatformError::new(
                            ErrorCode::EpochMismatch,
                            format!(
                                "服务端已记录 epoch {}，高于本次请求的 {epoch}，本节点不再是该 DB 的有效 Owner",
                                response.applied_epoch
                            ),
                        );
                        Err(AttemptFailure::fencing_from_server(
                            endpoint,
                            error,
                            Some(epoch),
                            Some(response.applied_epoch),
                        ))
                    }
                    // fence 未生效（可能落在旧副本上）：换端点重试
                    std::cmp::Ordering::Less => Err(AttemptFailure::Incomplete {
                        endpoint: endpoint.to_owned(),
                        message: format!(
                            "服务端回报 applied_epoch={} 低于请求的 {epoch}，fence 未生效",
                            response.applied_epoch
                        ),
                    }),
                }
            },
            |_failure| {},
        )
        .await
    }

    /// 追加一批 WAL 数据。
    ///
    /// **返回 `Ok` 即代表本批次已 quorum durable**（架构 §11.1）：调用方据此才允许向
    /// Client 返回 Commit Success。任何未确认 durable 的情况都返回 `Err`。
    pub async fn append(&self, req: AppendWalRequest) -> Result<AppendOutcome> {
        if req.append_id.trim().is_empty() {
            // 没有幂等键的重试会在「已提交但 ACK 丢失」时重复写入 WAL
            return Err(PlatformError::invalid_argument(
                "wal-client: append_id 不能为空（它决定重试是否幂等）",
            ));
        }

        let span = tracing::info_span!(
            "wal_client.append",
            db_id = %req.database_id,
            owner_epoch = req.owner_epoch,
            wal_lsn = req.start_lsn.get(),
            // AppendWalRequest 不携带 worker_id（Owner 身份由 epoch 表达），
            // 但字段必须存在以保持 span schema 稳定（observability 按字段检索）
            worker_id = "",
            append_id = %req.append_id,
            wal_bytes = req.bytes.len(),
        );
        let started = Instant::now();
        let result = self.append_inner(&req, started).instrument(span).await;

        metrics::record_append_latency_micros(elapsed_micros(started.elapsed()));
        if result.is_ok() {
            metrics::record_append_success();
        }
        result
    }

    async fn append_inner(
        &self,
        req: &AppendWalRequest,
        started: Instant,
    ) -> Result<AppendOutcome> {
        let timeout = self.inner.config.request_timeout;
        let expected_min_durable = req
            .start_lsn
            .get()
            .saturating_add(u64::try_from(req.bytes.len()).unwrap_or(u64::MAX));
        let owner_epoch = req.owner_epoch;
        let start_lsn = req.start_lsn;

        self.run_unary(
            RunOptions {
                op: "append",
                proves_leader: true,
            },
            move |mut client| {
                // 每次尝试都按**当前时刻**重新生成请求副本（deadline_unix_ms 与
                // context.deadline 一起）：复用首次构造的 deadline 会让重试带上已经过期的
                // 截止时间，服务端据此算出 wait_budget=0 后立即失败 —— 重试变成白打一次
                // RPC，写路径直接拿到 WAL_NOT_DURABLE，而实际上 leader 完全健康。
                // 其余字段（append_id / start_lsn / bytes）保持逐字节相同以保证幂等去重。
                let deadline = Instant::now() + timeout;
                let request =
                    request_with_timeout(self.append_proto_request(req, deadline), timeout);
                async move { client.append(request).await }
            },
            move |endpoint, response: wal::AppendResponse| {
                if let Some(error) = response.error.as_ref() {
                    if !is_success_error(Some(error)) {
                        return Err(AttemptFailure::from_server(
                            endpoint,
                            platform_error_from_proto(error),
                            Some(owner_epoch),
                        ));
                    }
                }
                // durable_lsn 是「已 durable 的末端 LSN（exclusive）」，必须覆盖本批次
                // 的全部字节；否则服务端只是「收到了」，还没 durable。
                if response.durable_lsn < expected_min_durable {
                    return Err(AttemptFailure::Incomplete {
                        endpoint: endpoint.to_owned(),
                        message: format!(
                            "服务端回报 durable_lsn={} 未覆盖本批次 [{start_lsn}, {expected_min_durable})",
                            response.durable_lsn
                        ),
                    });
                }
                Ok(AppendOutcome {
                    durable_lsn: Lsn::new(response.durable_lsn),
                    acked_replicas: response.acked_replicas,
                    latency: started.elapsed(),
                    deduplicated: response.deduplicated,
                })
            },
            |failure| metrics::record_append_error(failure.metric_reason()),
        )
        .await
    }

    /// 读取 `[from, to)` 区间的 WAL 数据，按 `start_lsn` 升序返回。
    ///
    /// 用 server-streaming 增量消费：冷启动 / failover replay 的区间可能远大于单条消息
    /// 上限，一次性收集会打爆内存。流中途失败时从最后一段续读（同一 LSN 覆盖写），
    /// 因此换端点重试不会产生重复段。
    pub async fn read_range(&self, db: &DatabaseId, from: Lsn, to: Lsn) -> Result<Vec<WalSegment>> {
        if to < from {
            return Err(PlatformError::invalid_argument(format!(
                "wal-client: read_range 的区间非法：[{from}, {to})"
            )));
        }
        if to == from {
            // 空区间不需要打网络
            return Ok(Vec::new());
        }

        let span = tracing::info_span!(
            "wal_client.read_range",
            db_id = %db,
            owner_epoch = tracing::field::Empty,
            wal_lsn = from.get(),
            worker_id = "",
            to_lsn = to.get(),
        );
        self.read_range_inner(db, from, to).instrument(span).await
    }

    async fn read_range_inner(
        &self,
        db: &DatabaseId,
        from: Lsn,
        to: Lsn,
    ) -> Result<Vec<WalSegment>> {
        let config = &self.inner.config;
        let mut plan = self.inner.endpoints.plan(config.max_attempts);
        let mut log = FailureLog::default();
        let mut segments: Vec<WalSegment> = Vec::new();
        // 断点：重试时从「最后收到的段」重读，重复段由 insert_segment 覆盖
        let mut cursor = from;

        while let Some(endpoint) = plan.next() {
            let attempt = plan.attempts();
            if attempt > 1 {
                sleep_backoff(attempt - 1).await;
            }

            let deadline = Instant::now() + config.request_timeout;
            let request = wal::ReadRangeRequest {
                database_id: db.to_string(),
                start_lsn: cursor.get(),
                end_lsn: to.get(),
                // 0 = 由服务端决定分块大小（proto 注释：server 侧 chunk 上限）
                max_chunk_bytes: 0,
                deadline_unix_ms: deadline_ms_from(Some(deadline)),
            };

            let failure = match self.inner.channels.get(&endpoint).await {
                Err(failure) => failure,
                Ok(channel) => {
                    let mut client = RemoteWalClient::new(channel);
                    // 流式响应不能用 channel / 请求级整体 deadline（会把正常的长流砍断），
                    // 因此这里只发 header，逐 chunk 的超时在 consume_stream 内控制。
                    let request = Request::new(request.clone());
                    match tokio::time::timeout(config.request_timeout, client.read_range(request))
                        .await
                    {
                        Err(_elapsed) => AttemptFailure::transport(
                            endpoint.clone(),
                            format!(
                                "read_range 首包超时（{}ms）",
                                config.request_timeout.as_millis()
                            ),
                        ),
                        Ok(Err(status)) => AttemptFailure::from_status(endpoint.clone(), &status),
                        Ok(Ok(response)) => {
                            match self
                                .consume_stream(&endpoint, response.into_inner(), &mut segments)
                                .await
                            {
                                Err(failure) => failure,
                                Ok(()) => {
                                    // 流正常结束 ≠ 区间读完：服务端可能中途截断（副本落后、
                                    // 实现有 bug）。必须校验 segments **恰好覆盖** [from, to)，
                                    // 否则调用方会把「短读」当成「该区间就这么长」，
                                    // failover replay 会静默丢数据（架构 §11.1 的 RPO 承诺）。
                                    match coverage_failure(from, to, &segments) {
                                        Some(message) => AttemptFailure::Incomplete {
                                            endpoint: endpoint.clone(),
                                            message,
                                        },
                                        None => {
                                            self.inner.endpoints.finish(&plan);
                                            tracing::debug!(
                                                db_id = %db,
                                                endpoint = %endpoint,
                                                segments = segments.len(),
                                                from = from.get(),
                                                to = to.get(),
                                                "read_range 完成"
                                            );
                                            return Ok(segments);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            };

            // 续读起点：最后一段的 start_lsn（该段会被重读并覆盖，保证不丢字节）。
            // 夹到请求末端以内：病态服务端返回越界数据时，下一次请求仍必须是合法区间 ——
            // 否则客户端会拿到 INVALID_ARGUMENT（终态、不重试），而不是契约要求的
            // 「耗尽后 WAL_NOT_DURABLE」。
            if let Some(last) = segments.last() {
                cursor = Lsn::new(last.start_lsn.get().min(to.get()));
            }
            if let Some(error) =
                self.handle_failure(&mut plan, &endpoint, failure, &mut log, "read_range")
            {
                return Err(error);
            }
        }

        self.inner.endpoints.finish(&plan);
        Err(log.exhausted_error("read_range", plan.attempts()))
    }

    /// 消费一个 streaming 响应，把 chunk 合并进 `segments`（按 `start_lsn` 升序）。
    async fn consume_stream(
        &self,
        endpoint: &str,
        mut stream: tonic::Streaming<wal::WalChunk>,
        segments: &mut Vec<WalSegment>,
    ) -> std::result::Result<(), AttemptFailure> {
        let request_timeout = self.inner.config.request_timeout;
        loop {
            // 逐 chunk 限时：server 侧卡住时不能让恢复路径无限等待
            let next = tokio::time::timeout(request_timeout, stream.message()).await;
            let chunk = match next {
                Err(_elapsed) => {
                    return Err(AttemptFailure::transport(
                        endpoint,
                        format!("WAL chunk 等待超时（{}ms）", request_timeout.as_millis()),
                    ))
                }
                Ok(Err(status)) => return Err(AttemptFailure::from_status(endpoint, &status)),
                // 流正常结束
                Ok(Ok(None)) => return Ok(()),
                Ok(Ok(Some(chunk))) => chunk,
            };

            if let Some(error) = chunk.error.as_ref() {
                if !is_success_error(Some(error)) {
                    return Err(AttemptFailure::from_server(
                        endpoint,
                        platform_error_from_proto(error),
                        None,
                    ));
                }
            }

            let is_last = chunk.last;
            // 空 chunk 不是数据段（`last` 标记常常就是一个空 chunk），不得生成空 WalSegment
            if !chunk.data.is_empty() {
                insert_segment(
                    segments,
                    WalSegment {
                        start_lsn: Lsn::new(chunk.start_lsn),
                        file_offset: chunk.wal_file_offset,
                        reset_wal: chunk.reset_wal,
                        data: bytes::Bytes::from(chunk.data),
                    },
                );
            }
            if is_last {
                return Ok(());
            }
        }
    }

    /// 截断 `lsn` 之前的 WAL（该区间已被 snapshot 覆盖）。
    ///
    /// 必须提供 `snapshot_id`：没有快照就截断等于永久丢数据，客户端在本地先拦一道。
    pub async fn trim_before(&self, db: &DatabaseId, lsn: Lsn, snapshot_id: &str) -> Result<()> {
        if snapshot_id.trim().is_empty() {
            return Err(PlatformError::invalid_argument(
                "wal-client: trim_before 必须提供 snapshot_id（防止截断未被快照覆盖的 WAL）",
            ));
        }

        let span = tracing::info_span!(
            "wal_client.trim_before",
            db_id = %db,
            owner_epoch = tracing::field::Empty,
            wal_lsn = lsn.get(),
            worker_id = "",
            snapshot_id = %snapshot_id,
        );
        self.trim_before_inner(db, lsn, snapshot_id)
            .instrument(span)
            .await
    }

    async fn trim_before_inner(&self, db: &DatabaseId, lsn: Lsn, snapshot_id: &str) -> Result<()> {
        let timeout = self.inner.config.request_timeout;

        self.run_unary(
            RunOptions {
                op: "trim_before",
                proves_leader: true,
            },
            move |mut client| {
                // 与 append / set_owner_epoch 同理：重试必须重新生成 deadline，
                // 否则第二次尝试送上的是一个早已过期的截止时间。
                let deadline = Instant::now() + timeout;
                let request = request_with_timeout(
                    self.trim_before_proto_request(db, lsn, snapshot_id, deadline),
                    timeout,
                );
                async move { client.trim_before_lsn(request).await }
            },
            move |endpoint, response: wal::TrimBeforeLsnResponse| {
                if let Some(error) = response.error.as_ref() {
                    if !is_success_error(Some(error)) {
                        return Err(AttemptFailure::from_server(
                            endpoint,
                            platform_error_from_proto(error),
                            None,
                        ));
                    }
                }
                if response.trimmed_before_lsn > lsn.get() {
                    // 服务端截断得比请求更多：客户端无法回填，只能告警（服务端是水位权威）
                    tracing::warn!(
                        db_id = %db,
                        requested = lsn.get(),
                        trimmed_before = response.trimmed_before_lsn,
                        "WAL 截断水位超出请求值，请检查 snapshot 覆盖范围"
                    );
                }
                Ok(())
            },
            |_failure| {},
        )
        .await
    }

    /// 查询某 DB 的 WAL 状态。
    pub async fn status(&self, db: &DatabaseId) -> Result<WalStatus> {
        let span = tracing::info_span!(
            "wal_client.status",
            db_id = %db,
            owner_epoch = tracing::field::Empty,
            wal_lsn = tracing::field::Empty,
            worker_id = "",
        );
        self.status_inner(db).instrument(span).await
    }

    async fn status_inner(&self, db: &DatabaseId) -> Result<WalStatus> {
        let request = wal::GetWalStatusRequest {
            database_id: db.to_string(),
        };
        let timeout = self.inner.config.request_timeout;

        self.run_unary(
            RunOptions {
                op: "status",
                // 读请求副本也能服务，不能据此认定它是 leader
                proves_leader: false,
            },
            move |mut client| {
                let request = request_with_timeout(request.clone(), timeout);
                async move { client.get_wal_status(request).await }
            },
            move |endpoint, response: wal::GetWalStatusResponse| {
                if let Some(error) = response.error.as_ref() {
                    if !is_success_error(Some(error)) {
                        return Err(AttemptFailure::from_server(
                            endpoint,
                            platform_error_from_proto(error),
                            None,
                        ));
                    }
                }
                Ok(WalStatus {
                    has_data: response.has_data,
                    first_lsn: Lsn::new(response.first_lsn),
                    last_lsn: Lsn::new(response.last_lsn),
                    owner_epoch: response.owner_epoch,
                })
            },
            |_failure| {},
        )
        .await
    }

    /// 探测整个 WAL 组（优先返回 leader 的视角）。
    ///
    /// 语义：
    /// - 联系到 leader -> `healthy` 取 leader 自评，`is_leader = true`；
    /// - 只有副本应答 -> `healthy = false`（没有 leader 就无法接受写入），
    ///   `leader_id` 取副本视图，便于排障；
    /// - 全部端点不可达 -> `Err(WalNotDurable)`。
    ///
    /// 探测覆盖**每一个**配置端点（每端点最多一次），不受 `max_attempts` 限制：诊断不能
    /// 因为尝试预算小于副本数就漏报副本。
    pub async fn health(&self) -> Result<WalHealth> {
        let span = tracing::info_span!(
            "wal_client.health",
            db_id = "",
            owner_epoch = tracing::field::Empty,
            wal_lsn = tracing::field::Empty,
            worker_id = "",
        );
        self.health_inner().instrument(span).await
    }

    async fn health_inner(&self) -> Result<WalHealth> {
        let config = &self.inner.config;
        let mut plan = self.inner.endpoints.plan_all();
        let mut follower_view: Option<wal::HealthResponse> = None;
        let mut reachable = 0u32;
        let mut probed = 0u32;

        while let Some(endpoint) = plan.next() {
            probed += 1;
            let Ok(channel) = self.inner.channels.get(&endpoint).await else {
                continue;
            };
            let mut client = RemoteWalClient::new(channel);
            let request = request_with_timeout(wal::HealthRequest {}, config.request_timeout);
            let Ok(Ok(response)) = tokio::time::timeout(
                config.request_timeout,
                // health 不需要 leader 语义，任何副本都能回答
                client.health(request),
            )
            .await
            else {
                continue;
            };

            reachable += 1;
            let response = response.into_inner();
            if response.is_leader {
                self.inner.endpoints.prefer(&endpoint);
                self.inner.endpoints.finish(&plan);
                return Ok(WalHealth {
                    healthy: response.healthy,
                    is_leader: true,
                    term: response.term,
                    leader_id: leader_id_of(&response),
                    node_id: response.node_id,
                });
            }
            if follower_view.is_none() {
                follower_view = Some(response);
            }
        }

        self.inner.endpoints.finish(&plan);
        match follower_view {
            Some(response) => Ok(WalHealth {
                // 没有 leader：写入路径不可用，绝不能报 healthy
                healthy: false,
                is_leader: false,
                term: response.term,
                leader_id: leader_id_of(&response),
                node_id: response.node_id,
            }),
            None => Err(PlatformError::new(
                ErrorCode::WalNotDurable,
                format!("wal-client: {probed} 个 WAL 端点全部不可达（成功应答 {reachable} 个）"),
            )),
        }
    }

    /// 一元 RPC 的统一执行器：端点轮换 + 有界退避 + fencing 短路 + leader 缓存。
    ///
    /// `call` 每次尝试都重新 clone 请求体，保证重试携带完全相同的幂等字段；
    /// `finish` 把 proto 响应翻成业务结果或失败分类。
    async fn run_unary<Raw, R, F, Fut, C, O>(
        &self,
        options: RunOptions,
        mut call: F,
        mut finish: C,
        mut on_failure: O,
    ) -> Result<R>
    where
        F: FnMut(RemoteWalClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<tonic::Response<Raw>, tonic::Status>>,
        C: FnMut(&str, Raw) -> std::result::Result<R, AttemptFailure>,
        O: FnMut(&AttemptFailure),
    {
        let config = &self.inner.config;
        let mut plan = self.inner.endpoints.plan(config.max_attempts);
        let mut log = FailureLog::default();

        while let Some(endpoint) = plan.next() {
            let attempt = plan.attempts();
            if attempt > 1 {
                sleep_backoff(attempt - 1).await;
            }
            tracing::debug!(
                op = options.op,
                endpoint = %endpoint,
                attempt,
                "wal-client 尝试"
            );

            let failure = match self.inner.channels.get(&endpoint).await {
                Err(failure) => failure,
                Ok(channel) => {
                    let client = RemoteWalClient::new(channel);
                    // 本地超时兜底：`grpc-timeout` 只是给服务端的 hint，服务端不守约
                    // （或 TCP 半开）时客户端必须自己放弃这次尝试
                    match tokio::time::timeout(config.request_timeout, call(client)).await {
                        Err(_elapsed) => AttemptFailure::transport(
                            endpoint.clone(),
                            format!(
                                "{} 请求超时（{}ms）",
                                options.op,
                                config.request_timeout.as_millis()
                            ),
                        ),
                        Ok(Err(status)) => AttemptFailure::from_status(endpoint.clone(), &status),
                        Ok(Ok(response)) => match finish(&endpoint, response.into_inner()) {
                            Ok(value) => {
                                if options.proves_leader {
                                    // 只有 leader 才能让这些 RPC 成功，缓存下来避免下次试错
                                    self.inner.endpoints.prefer(&endpoint);
                                }
                                self.inner.endpoints.finish(&plan);
                                return Ok(value);
                            }
                            Err(failure) => failure,
                        },
                    }
                }
            };

            on_failure(&failure);
            tracing::debug!(
                op = options.op,
                endpoint = %endpoint,
                attempt,
                reason = failure.metric_reason(),
                "wal-client 尝试失败：{}",
                failure.describe()
            );

            if let Some(error) =
                self.handle_failure(&mut plan, &endpoint, failure, &mut log, options.op)
            {
                return Err(error);
            }
        }

        self.inner.endpoints.finish(&plan);
        Err(log.exhausted_error(options.op, plan.attempts()))
    }

    /// 统一处理一次失败。
    ///
    /// 返回 `Some(error)` 表示必须立即终止（fencing / 终态错误），调用方直接把它回传给
    /// 上层；返回 `None` 表示已记入失败日志、可以继续换端点重试。
    fn handle_failure(
        &self,
        plan: &mut AttemptPlan,
        endpoint: &str,
        failure: AttemptFailure,
        log: &mut FailureLog,
        op: &str,
    ) -> Option<PlatformError> {
        // leader 提示：默认只接受配置列表内的端点（列表外的一律忽略并告警），
        // 否则一条提示就能让写路径拨号到运维未授权的地址（见 `EndpointSet::accepts_hint`）。
        if let Some(hint) = failure.hint() {
            if self.inner.endpoints.accepts_hint(hint) {
                if !plan.tried().iter().any(|tried| tried.as_str() == hint) {
                    self.inner.endpoints.prefer(hint);
                }
                plan.promote(hint);
            } else {
                tracing::warn!(
                    op,
                    endpoint,
                    hint,
                    "忽略配置列表之外的 leader 提示（allow_unlisted_leader_hint=false）"
                );
            }
        }

        if failure.disposition() == Disposition::Terminal {
            self.inner.endpoints.finish(plan);
            tracing::warn!(
                op,
                endpoint,
                "wal-client 终态失败，不再重试：{}",
                failure.describe()
            );
            return Some(failure.terminal_error());
        }

        if failure.is_leader_related() {
            // 该端点自证不是 leader（或不再是 owner），缓存作废
            self.inner.endpoints.forget_leader(endpoint);
        }
        log.push(failure);
        None
    }

    /// 构造 `set_owner_epoch` 的 proto 请求。
    ///
    /// `deadline` 由调用方按**本次尝试**的时刻生成（见 [`WalClient::run_unary`] 的说明），
    /// 因此这里不再自己取 `Instant::now()`。
    fn set_owner_epoch_proto_request(
        &self,
        db: &DatabaseId,
        epoch: u64,
        worker: &WorkerId,
        deadline: Instant,
    ) -> wal::SetOwnerEpochRequest {
        wal::SetOwnerEpochRequest {
            database_id: db.to_string(),
            owner_epoch: epoch,
            worker_id: worker.as_str().to_owned(),
            // reason 是运维可读的自由文本，客户端不编造语义
            reason: String::new(),
            deadline_unix_ms: deadline_ms_from(Some(deadline)),
            context: Some(self.request_context(db, epoch, worker.as_str(), deadline)),
        }
    }

    /// 构造 append 的 proto 请求。
    ///
    /// 除 deadline 外的字段在一次重试链里必须**逐字节相同**（服务端按
    /// `(db_id, start_lsn, append_id)` 幂等去重）；`deadline` 则由调用方按本次尝试的
    /// 时刻生成。
    fn append_proto_request(
        &self,
        req: &AppendWalRequest,
        deadline: Instant,
    ) -> wal::AppendRequest {
        wal::AppendRequest {
            database_id: req.database_id.to_string(),
            owner_epoch: req.owner_epoch,
            start_lsn: req.start_lsn.get(),
            wal_bytes: req.bytes.to_vec(),
            append_id: req.append_id.clone(),
            contains_commit_frame: req.contains_commit_frame,
            deadline_unix_ms: deadline_ms_from(Some(deadline)),
            context: Some(self.request_context(&req.database_id, req.owner_epoch, "", deadline)),
            wal_file_offset: req.file_offset,
            reset_wal: req.reset_wal,
        }
    }

    /// 构造 `trim_before` 的 proto 请求（`deadline` 由调用方按本次尝试的时刻生成）。
    fn trim_before_proto_request(
        &self,
        db: &DatabaseId,
        lsn: Lsn,
        snapshot_id: &str,
        deadline: Instant,
    ) -> wal::TrimBeforeLsnRequest {
        wal::TrimBeforeLsnRequest {
            database_id: db.to_string(),
            lsn: lsn.get(),
            snapshot_id: snapshot_id.to_owned(),
            deadline_unix_ms: deadline_ms_from(Some(deadline)),
        }
    }

    /// 构造随 RPC 下发的请求上下文（proto 要求所有内部 RPC 必须携带）。
    fn request_context(
        &self,
        db: &DatabaseId,
        owner_epoch: u64,
        worker_id: &str,
        deadline: Instant,
    ) -> protocol::common::RequestContext {
        RequestContext {
            request_id: uuid::Uuid::now_v7().to_string(),
            // trace_id 由 observability 的 subscriber 注入到日志后端；
            // 传输层不伪造 W3C traceparent，避免与真实 trace 冲突
            trace_id: String::new(),
            tenant_id: String::new(),
            database_id: db.to_string(),
            owner_epoch,
            deadline: Some(deadline),
            session_id: String::new(),
            transaction_id: String::new(),
            worker_id: worker_id.to_owned(),
            idempotency_key: String::new(),
        }
        .into()
    }
}

/// 退避时长（纯函数，便于测试）。
fn backoff_delay(failed_attempts: u32) -> Duration {
    let shift = failed_attempts.saturating_sub(1).min(5);
    (BACKOFF_BASE * 2u32.pow(shift)).min(BACKOFF_CAP)
}

/// 有界指数退避：5ms / 10ms / 20ms …（不超过 [`BACKOFF_CAP`]）。
async fn sleep_backoff(failed_attempts: u32) {
    tokio::time::sleep(backoff_delay(failed_attempts)).await;
}

/// 构造带 `grpc-timeout` 的一元请求。
fn request_with_timeout<T>(message: T, timeout: Duration) -> Request<T> {
    let mut request = Request::new(message);
    request.set_timeout(timeout);
    request
}

/// 把一段 WAL 合并进结果集：按 `start_lsn` 升序，同 LSN 覆盖（重试续读的去重点）。
fn insert_segment(segments: &mut Vec<WalSegment>, segment: WalSegment) {
    match segments.binary_search_by_key(&segment.start_lsn.get(), |existing| {
        existing.start_lsn.get()
    }) {
        Ok(index) => segments[index] = segment,
        Err(index) => segments.insert(index, segment),
    }
}

/// 校验已收到的段是否**严格连续且恰好覆盖** `[from, to)`。
///
/// 返回 `Some(原因)` 表示这次读取不可信，调用方必须按失败处理（换端点重试，耗尽后报
/// `WAL_NOT_DURABLE`），绝不能把部分数据当成完整区间返回：failover replay 会把返回的
/// 字节直接回放进本地 WAL，少一段等于**永久丢数据**，而且不报错。
///
/// 三条判定缺一不可（只比总长度的检查会被「空洞 + 重叠」凑出同样的长度骗过）：
/// 1. 首段起点必须等于 `from`（服务端从别处开始返回说明它没有按请求区间裁剪）；
/// 2. 相邻段必须首尾相接（`start + len == next.start`，既无空洞也无重叠）；
/// 3. 字节总数必须等于 `to - from`（截断的流必须被发现）。
fn coverage_failure(from: Lsn, to: Lsn, segments: &[WalSegment]) -> Option<String> {
    let expected = to.get().saturating_sub(from.get());
    let mut cursor = from.get();
    let mut total: u64 = 0;
    for (index, segment) in segments.iter().enumerate() {
        if segment.start_lsn.get() != cursor {
            return Some(format!(
                "区间 [{from}, {to}) 的第 {} 段起点 {} 与前一段末端 {cursor} 不连续（存在空洞或重叠），本批数据不可信",
                index + 1,
                segment.start_lsn.get()
            ));
        }
        let len = u64::try_from(segment.data.len()).unwrap_or(u64::MAX);
        cursor = cursor.saturating_add(len);
        total = total.saturating_add(len);
    }
    if total != expected {
        return Some(format!(
            "区间 [{from}, {to}) 应为 {expected} 字节，实际只收到 {total} 字节（流被截断），本批数据不可信"
        ));
    }
    None
}

/// 服务端没给出 leader_id 时退回 node_id（排障时至少要有一个可对照的节点标识）。
fn leader_id_of(response: &wal::HealthResponse) -> String {
    if response.leader_id.is_empty() {
        response.node_id.clone()
    } else {
        response.leader_id.clone()
    }
}

fn elapsed_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// 校验端点形态：必须是 `http(s)://host[:port]`。
///
/// `tonic::Endpoint::from_shared` 对缺少 scheme 的字符串不会报错（`http::Uri` 允许
/// 相对路径），错误会推迟到首次连接才暴露 —— 写路径上才发现配置错是不可接受的。
fn validate_endpoint(endpoint: &str) -> Result<()> {
    let invalid = |reason: &str| {
        PlatformError::invalid_argument(format!(
            "wal-client: 端点 {endpoint:?} 非法（{reason}），示例：http://wal-1:9200"
        ))
    };
    if endpoint.trim() != endpoint || endpoint.is_empty() {
        return Err(invalid("不得包含首尾空白"));
    }
    let authority = if let Some(rest) = endpoint.strip_prefix("http://") {
        rest
    } else if let Some(rest) = endpoint.strip_prefix("https://") {
        rest
    } else {
        return Err(invalid("必须带 http:// 或 https:// scheme"));
    };
    let host = authority.split(['/', '?', '#']).next().unwrap_or_default();
    if host.is_empty() {
        return Err(invalid("缺少 host"));
    }
    Endpoint::from_shared(endpoint.to_owned())
        .map_err(|err| invalid(&format!("URI 解析失败：{err}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(endpoints: Vec<&str>) -> WalClientConfig {
        WalClientConfig {
            endpoints: endpoints.into_iter().map(str::to_owned).collect(),
            connect_timeout: Duration::from_millis(50),
            request_timeout: Duration::from_millis(100),
            max_attempts: 4,
            allow_unlisted_leader_hint: false,
        }
    }

    fn segment(start_lsn: u64, data: &'static [u8]) -> WalSegment {
        WalSegment {
            start_lsn: Lsn::new(start_lsn),
            file_offset: start_lsn,
            reset_wal: false,
            data: bytes::Bytes::from_static(data),
        }
    }

    #[test]
    fn default_config_matches_frozen_values() {
        let config = WalClientConfig::default();
        assert!(config.endpoints.is_empty());
        assert_eq!(config.max_attempts, 4);
        assert_eq!(config.connect_timeout, Duration::from_secs(1));
        assert_eq!(config.request_timeout, Duration::from_secs(5));
        // leader 提示默认只接受配置列表内的端点（越权拨号的开关必须显式打开）
        assert!(!config.allow_unlisted_leader_hint);
    }

    #[test]
    fn new_rejects_empty_endpoints() {
        let error = WalClient::new(WalClientConfig::default()).expect_err("空端点必须被拒绝");
        assert_eq!(error.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn new_rejects_zero_attempts() {
        let mut config = config(vec!["http://wal-1:9200"]);
        config.max_attempts = 0;
        let error = WalClient::new(config).expect_err("max_attempts=0 必须被拒绝");
        assert_eq!(error.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn new_rejects_endpoint_without_scheme() {
        // 缺 scheme 的地址在 tonic 里要等到连接时才报错，必须在构造期拦下
        for invalid in [
            "wal-1:9200",
            "http://",
            "ftp://wal-1:9200",
            " http://wal-1:9200",
        ] {
            let error = WalClient::new(config(vec![invalid]))
                .err()
                .unwrap_or_else(|| panic!("端点 {invalid} 必须被拒绝"));
            assert_eq!(error.code, ErrorCode::InvalidArgument, "端点 {invalid}");
        }
    }

    #[test]
    fn endpoints_are_deduped_but_order_preserved() {
        let client = WalClient::new(config(vec![
            "http://wal-1:9200",
            "http://wal-2:9200",
            "http://wal-1:9200",
        ]))
        .expect("构造成功");
        assert_eq!(
            client.endpoints(),
            ["http://wal-1:9200", "http://wal-2:9200"]
        );
    }

    #[test]
    fn backoff_is_bounded_exponential() {
        assert_eq!(backoff_delay(1), Duration::from_millis(5));
        assert_eq!(backoff_delay(2), Duration::from_millis(10));
        assert_eq!(backoff_delay(3), Duration::from_millis(20));
        assert_eq!(backoff_delay(4), Duration::from_millis(40));
        // 上限：无限调大 max_attempts 也不会退避到不可控
        assert_eq!(backoff_delay(100), BACKOFF_CAP);
        assert_eq!(backoff_delay(0), BACKOFF_BASE);
    }

    #[test]
    fn insert_segment_keeps_ascending_order_and_dedupes() {
        let mut segments = vec![
            WalSegment {
                start_lsn: Lsn::new(100),
                file_offset: 0,
                reset_wal: false,
                data: bytes::Bytes::from_static(b"b"),
            },
            WalSegment {
                start_lsn: Lsn::new(200),
                file_offset: 10,
                reset_wal: false,
                data: bytes::Bytes::from_static(b"c"),
            },
        ];
        insert_segment(
            &mut segments,
            WalSegment {
                start_lsn: Lsn::new(50),
                file_offset: 0,
                reset_wal: true,
                data: bytes::Bytes::from_static(b"a"),
            },
        );
        // 续读重放同一段：覆盖而不是重复插入
        insert_segment(
            &mut segments,
            WalSegment {
                start_lsn: Lsn::new(200),
                file_offset: 10,
                reset_wal: false,
                data: bytes::Bytes::from_static(b"cc"),
            },
        );

        let lsns: Vec<u64> = segments.iter().map(|s| s.start_lsn.get()).collect();
        assert_eq!(lsns, [50, 100, 200]);
        assert_eq!(segments[2].data, bytes::Bytes::from_static(b"cc"));
    }

    #[test]
    fn read_range_rejects_inverted_interval() {
        let client = WalClient::new(config(vec!["http://wal-1:9200"])).unwrap();
        let error = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(client.read_range(&DatabaseId::new_v7(), Lsn::new(200), Lsn::new(100)))
            .expect_err("区间反了必须报错");
        assert_eq!(error.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn read_range_of_empty_interval_needs_no_network() {
        let client = WalClient::new(config(vec!["http://127.0.0.1:1"])).unwrap();
        let segments = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(client.read_range(&DatabaseId::new_v7(), Lsn::new(100), Lsn::new(100)))
            .expect("空区间必须直接返回");
        assert!(segments.is_empty());
    }

    /// 恰好覆盖 `[100, 106)` 的两段：必须通过。
    #[test]
    fn coverage_check_accepts_exact_cover() {
        assert_eq!(
            coverage_failure(
                Lsn::new(100),
                Lsn::new(106),
                &[segment(100, b"ab"), segment(102, b"cdef")]
            ),
            None
        );
    }

    /// 截断的流：数据是连续的，但总长度不足 —— 这是最危险的一种（看起来「正常结束」）。
    #[test]
    fn coverage_check_rejects_truncated_stream() {
        let reason = coverage_failure(
            Lsn::new(100),
            Lsn::new(106),
            &[segment(100, b"ab"), segment(102, b"cd")],
        )
        .unwrap_or_else(|| panic!("长度不足必须被判为失败"));
        assert!(reason.contains("被截断"), "{reason}");
    }

    /// 空洞 + 重叠能凑出与区间相同的总长度，只比长度的检查会被骗过。
    #[test]
    fn coverage_check_rejects_gap_even_when_total_length_matches() {
        // [100,105) + [110,115)：总长 10 == 115-105，但中间 [105,110) 是空洞
        let reason = coverage_failure(
            Lsn::new(100),
            Lsn::new(110),
            &[segment(100, b"aaaaa"), segment(110, b"bbbbb")],
        )
        .unwrap_or_else(|| panic!("空洞必须被判为失败"));
        assert!(reason.contains("不连续"), "{reason}");
    }

    /// 首段起点不等于请求起点：服务端没按区间裁剪，必须拒绝而不是「按偏移猜」。
    #[test]
    fn coverage_check_rejects_wrong_start() {
        assert!(coverage_failure(Lsn::new(100), Lsn::new(102), &[segment(90, b"ab")]).is_some());
        // 一段数据都没有（空流）也必须被发现
        assert!(coverage_failure(Lsn::new(100), Lsn::new(101), &[]).is_some());
    }

    /// 多出的字节同样不可接受：调用方按 [from, to) 回放，多出来的部分会写到错误偏移。
    #[test]
    fn coverage_check_rejects_overlong_stream() {
        assert!(coverage_failure(Lsn::new(100), Lsn::new(102), &[segment(100, b"abc")]).is_some());
    }

    #[test]
    fn append_requires_idempotency_key() {
        let client = WalClient::new(config(vec!["http://wal-1:9200"])).unwrap();
        let error = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(client.append(AppendWalRequest {
                database_id: DatabaseId::new_v7(),
                owner_epoch: 1,
                start_lsn: Lsn::ZERO,
                file_offset: 0,
                reset_wal: false,
                bytes: bytes::Bytes::from_static(b"x"),
                contains_commit_frame: true,
                append_id: "   ".into(),
            }))
            .expect_err("空 append_id 必须被拒绝");
        assert_eq!(error.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn trim_requires_snapshot_id() {
        let client = WalClient::new(config(vec!["http://wal-1:9200"])).unwrap();
        let error = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(client.trim_before(&DatabaseId::new_v7(), Lsn::new(10), ""))
            .expect_err("缺 snapshot_id 必须被拒绝");
        assert_eq!(error.code, ErrorCode::InvalidArgument);
    }

    /// span 字段名必须与 observability 的字段常量一致（按字段检索依赖这一点）。
    #[test]
    fn span_field_names_match_observability_contract() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../observability/src/context.rs");
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("读取 {} 失败：{err}", path.display()));
        for field in [
            crate::field_names::DB_ID,
            crate::field_names::OWNER_EPOCH,
            crate::field_names::WAL_LSN,
            crate::field_names::WORKER_ID,
        ] {
            assert!(
                source.contains(&format!("\"{field}\"")),
                "span 字段 {field} 未出现在 observability 契约中"
            );
        }
    }
}
