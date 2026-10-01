//! 进程内 mock Remote WAL 服务。
//!
//! 用真实 tonic server 绑定 127.0.0.1 的临时端口：不依赖任何外部进程，但走的是**真实
//! gRPC 编解码与 HTTP/2 链路**，因此能覆盖「响应体里的 in-band PlatformError」、
//! 「server-streaming chunk」这些纯逻辑测试覆盖不到的部分。
//!
//! 每个 RPC 的行为由脚本（action 列表）驱动：按调用序号取用，用完后重复最后一条，
//! 这样可以精确构造「先失败、后成功」这类 durability 场景。

#![allow(dead_code)] // 各测试文件只用到自己需要的部分

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use protocol::common;
use protocol::wal;
use protocol::wal::remote_wal_server::{RemoteWal, RemoteWalServer};
use tonic::transport::server::TcpIncoming;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use wal_client::{WalClient, WalClientConfig};

/// mock 服务：脚本 + 请求记录。
#[derive(Clone)]
pub struct MockWal {
    inner: Arc<Inner>,
}

struct Inner {
    append_actions: Mutex<Vec<AppendAction>>,
    /// 收到的 append 请求（用于断言重试期间 append_id 不变）
    appends: Mutex<Vec<wal::AppendRequest>>,
    read_actions: Mutex<Vec<Vec<ReadStep>>>,
    reads: Mutex<Vec<wal::ReadRangeRequest>>,
    epoch_actions: Mutex<Vec<EpochAction>>,
    epochs: Mutex<Vec<wal::SetOwnerEpochRequest>>,
    trim_actions: Mutex<Vec<TrimAction>>,
    trims: Mutex<Vec<wal::TrimBeforeLsnRequest>>,
    status: Mutex<wal::GetWalStatusResponse>,
    /// `GetWalStatus` 的调用次数（用于断言「终态错误不得重试」）
    status_calls: Mutex<usize>,
    health: Mutex<wal::HealthResponse>,
}

/// `Append` 的一次行为。
#[derive(Clone)]
pub enum AppendAction {
    /// 成功；`durable_lsn = None` 时按 `start_lsn + bytes.len()` 返回（与 wal-service 契约一致）。
    Ok {
        durable_lsn: Option<u64>,
        deduplicated: bool,
        acked: Vec<String>,
    },
    /// 返回 in-band 平台错误。
    Err {
        code: i32,
        message: &'static str,
        /// 拥有所有权的 detail：调用方需要用 `format!` 才能拼出端点提示
        detail_json: String,
    },
    /// 挂起（用于构造客户端超时）。
    Stall(Duration),
}

impl AppendAction {
    /// 成功且 quorum 副本齐全的默认行为。
    pub fn ok() -> Self {
        AppendAction::Ok {
            durable_lsn: None,
            deduplicated: false,
            acked: vec!["wal-1".into(), "wal-2".into()],
        }
    }

    /// 返回指定平台错误码。
    pub fn err(code: i32, message: &'static str) -> Self {
        AppendAction::Err {
            code,
            message,
            detail_json: String::new(),
        }
    }

    /// 返回带 detail 的平台错误。
    pub fn err_with_detail(
        code: i32,
        message: &'static str,
        detail_json: impl Into<String>,
    ) -> Self {
        AppendAction::Err {
            code,
            message,
            detail_json: detail_json.into(),
        }
    }
}

/// `ReadRange` 流的一步。
#[derive(Clone)]
pub enum ReadStep {
    /// 一个数据 chunk。
    Chunk {
        start_lsn: u64,
        file_offset: u64,
        reset_wal: bool,
        data: &'static [u8],
    },
    /// 结束标记（`last = true`）。
    Last { start_lsn: u64 },
    /// 携带 in-band 平台错误的 chunk（服务端在流中途发现自己不是 leader 等）。
    ErrorChunk {
        code: i32,
        message: &'static str,
        /// 拥有所有权的 detail：调用方需要用 `format!` 才能拼出端点提示
        detail_json: String,
    },
    /// 流中途的 gRPC 级失败。
    Fail(Code, &'static str),
}

impl ReadStep {
    /// 数据 chunk 的简写。
    pub fn chunk(start_lsn: u64, file_offset: u64, data: &'static [u8]) -> Self {
        ReadStep::Chunk {
            start_lsn,
            file_offset,
            reset_wal: false,
            data,
        }
    }
}

/// `SetOwnerEpoch` 的一次行为。
#[derive(Clone)]
pub enum EpochAction {
    /// 成功应用指定 epoch。
    Ok(u64),
    /// 返回 in-band 平台错误。
    Err {
        code: i32,
        message: &'static str,
        detail_json: &'static str,
        applied_epoch: u64,
    },
}

/// `TrimBeforeLsn` 的一次行为。
#[derive(Clone)]
pub enum TrimAction {
    /// 成功；`0` 表示按请求的 LSN 回显水位。
    Ok(u64),
    /// 返回 in-band 平台错误。
    Err { code: i32, message: &'static str },
}

/// mock 服务句柄。
pub struct MockHandle {
    /// `http://127.0.0.1:<port>`。
    pub endpoint: String,
    /// 脚本与记录。
    pub mock: MockWal,
}

/// 启动一个 mock 服务，返回可用的客户端端点。
pub async fn spawn(name: &str) -> MockHandle {
    let mock = MockWal::new(name);
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse::<SocketAddr>().expect("addr"))
        .expect("bind 临时端口");
    let addr = incoming.local_addr().expect("local_addr");
    let service = RemoteWalServer::new(mock.clone());
    tokio::spawn(async move {
        // 测试结束时随 runtime 一起销毁；serve 的错误在测试里没有继续处理的意义
        let _ = Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await;
    });
    MockHandle {
        endpoint: format!("http://{addr}"),
        mock,
    }
}

/// 一个「连不上」的端点（先占端口再释放，保证端口空闲）。
pub fn dead_endpoint() -> String {
    let socket = std::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("bind");
    let addr = socket.local_addr().expect("addr");
    drop(socket);
    format!("http://{addr}")
}

/// 构造指向给定端点的客户端。
pub fn client(endpoints: Vec<String>, max_attempts: u32) -> WalClient {
    client_with_timeout(endpoints, max_attempts, Duration::from_millis(500))
}

/// 构造客户端并指定请求超时。
pub fn client_with_timeout(
    endpoints: Vec<String>,
    max_attempts: u32,
    request_timeout: Duration,
) -> WalClient {
    client_with_options(endpoints, max_attempts, request_timeout, false)
}

/// 构造客户端，并显式决定是否接受配置列表之外的 leader 提示。
///
/// 默认（`allow_unlisted_leader_hint = false`）时配置列表就是可拨号端点闭集；
/// 需要构造「提示端点确实不在本地配置里」的场景时显式打开。
pub fn client_with_options(
    endpoints: Vec<String>,
    max_attempts: u32,
    request_timeout: Duration,
    allow_unlisted_leader_hint: bool,
) -> WalClient {
    WalClient::new(WalClientConfig {
        endpoints,
        // 建链超时留足余量：测试机可能在高并发编译等高负载下运行
        connect_timeout: Duration::from_millis(1500),
        request_timeout,
        max_attempts,
        allow_unlisted_leader_hint,
    })
    .expect("构造 WalClient")
}

/// 构造 in-band 平台错误。
pub fn platform_error(code: i32, message: &str, detail_json: &str) -> common::PlatformError {
    common::PlatformError {
        code,
        message: message.to_owned(),
        retryable: false,
        request_id: String::new(),
        detail_json: detail_json.to_owned(),
        route_retry_count: 0,
    }
}

impl MockWal {
    /// 新建 mock：默认所有 RPC 都成功。
    pub fn new(name: &str) -> Self {
        Self {
            inner: Arc::new(Inner {
                append_actions: Mutex::new(vec![AppendAction::ok()]),
                appends: Mutex::new(Vec::new()),
                read_actions: Mutex::new(Vec::new()),
                reads: Mutex::new(Vec::new()),
                epoch_actions: Mutex::new(vec![EpochAction::Ok(1)]),
                epochs: Mutex::new(Vec::new()),
                trim_actions: Mutex::new(vec![TrimAction::Ok(0)]),
                trims: Mutex::new(Vec::new()),
                status: Mutex::new(wal::GetWalStatusResponse {
                    error: None,
                    has_data: true,
                    first_lsn: 10,
                    last_lsn: 20,
                    owner_epoch: 7,
                    append_count: 3,
                }),
                status_calls: Mutex::new(0),
                health: Mutex::new(wal::HealthResponse {
                    healthy: true,
                    is_leader: true,
                    term: 5,
                    leader_id: name.to_owned(),
                    node_id: name.to_owned(),
                    commit_index: 100,
                    applied_index: 100,
                    shard_id: "0".into(),
                }),
            }),
        }
    }

    /// 设置 append 脚本（每次调用依次取用，用尽后重复最后一条）。
    pub fn script_append(&self, actions: Vec<AppendAction>) {
        assert!(!actions.is_empty());
        *self.inner.append_actions.lock().unwrap() = actions;
    }

    /// 设置 ReadRange 脚本（每次调用依次取用，用尽后重复最后一组）。
    pub fn script_read(&self, actions: Vec<Vec<ReadStep>>) {
        assert!(!actions.is_empty());
        *self.inner.read_actions.lock().unwrap() = actions;
    }

    /// 设置 SetOwnerEpoch 脚本。
    pub fn script_epoch(&self, actions: Vec<EpochAction>) {
        assert!(!actions.is_empty());
        *self.inner.epoch_actions.lock().unwrap() = actions;
    }

    /// 设置 Trim 脚本。
    pub fn script_trim(&self, actions: Vec<TrimAction>) {
        assert!(!actions.is_empty());
        *self.inner.trim_actions.lock().unwrap() = actions;
    }

    /// 设置 health 响应（是否 leader、是否健康、leader_id）。
    pub fn set_health(&self, is_leader: bool, healthy: bool, leader_id: &str) {
        let mut health = self.inner.health.lock().unwrap();
        health.is_leader = is_leader;
        health.healthy = healthy;
        health.leader_id = leader_id.to_owned();
    }

    /// 让 `GetWalStatus` 返回指定平台错误码（覆盖默认的成功响应）。
    pub fn set_status_error(&self, code: i32, message: &str) {
        let mut status = self.inner.status.lock().unwrap();
        *status = wal::GetWalStatusResponse {
            error: Some(platform_error(code, message, "")),
            has_data: false,
            first_lsn: 0,
            last_lsn: 0,
            owner_epoch: 0,
            append_count: 0,
        };
    }

    /// `GetWalStatus` 调用次数。
    pub fn status_calls(&self) -> usize {
        *self.inner.status_calls.lock().unwrap()
    }

    /// 收到的 append 请求。
    pub fn appends(&self) -> Vec<wal::AppendRequest> {
        self.inner.appends.lock().unwrap().clone()
    }

    /// append 调用次数。
    pub fn append_calls(&self) -> usize {
        self.inner.appends.lock().unwrap().len()
    }

    /// 收到的 read_range 请求。
    pub fn reads(&self) -> Vec<wal::ReadRangeRequest> {
        self.inner.reads.lock().unwrap().clone()
    }

    /// read_range 调用次数。
    pub fn read_calls(&self) -> usize {
        self.inner.reads.lock().unwrap().len()
    }

    /// 收到的 SetOwnerEpoch 请求。
    pub fn epochs(&self) -> Vec<wal::SetOwnerEpochRequest> {
        self.inner.epochs.lock().unwrap().clone()
    }

    /// 收到的 TrimBeforeLsn 请求。
    pub fn trims(&self) -> Vec<wal::TrimBeforeLsnRequest> {
        self.inner.trims.lock().unwrap().clone()
    }

    /// 取下一个 append 行为（用尽后重复最后一条）。
    fn next_append_action(&self) -> AppendAction {
        let mut actions = self.inner.append_actions.lock().unwrap();
        if actions.len() > 1 {
            actions.remove(0)
        } else {
            actions[0].clone()
        }
    }

    fn next_read_action(&self) -> Vec<ReadStep> {
        let mut actions = self.inner.read_actions.lock().unwrap();
        if actions.is_empty() {
            return Vec::new();
        }
        if actions.len() > 1 {
            actions.remove(0)
        } else {
            actions[0].clone()
        }
    }

    fn next_epoch_action(&self) -> EpochAction {
        let mut actions = self.inner.epoch_actions.lock().unwrap();
        if actions.len() > 1 {
            actions.remove(0)
        } else {
            actions[0].clone()
        }
    }

    fn next_trim_action(&self) -> TrimAction {
        let mut actions = self.inner.trim_actions.lock().unwrap();
        if actions.len() > 1 {
            actions.remove(0)
        } else {
            actions[0].clone()
        }
    }
}

#[async_trait]
impl RemoteWal for MockWal {
    async fn append(
        &self,
        request: Request<wal::AppendRequest>,
    ) -> Result<Response<wal::AppendResponse>, Status> {
        let request = request.into_inner();
        self.inner.appends.lock().unwrap().push(request.clone());

        let response = match self.next_append_action() {
            AppendAction::Ok {
                durable_lsn,
                deduplicated,
                acked,
            } => {
                let durable = durable_lsn.unwrap_or_else(|| {
                    request
                        .start_lsn
                        .saturating_add(u64::try_from(request.wal_bytes.len()).unwrap_or(0))
                });
                wal::AppendResponse {
                    error: None,
                    durable_lsn: durable,
                    acked_replicas: acked,
                    append_latency_micros: 42,
                    deduplicated,
                }
            }
            AppendAction::Err {
                code,
                message,
                detail_json,
            } => wal::AppendResponse {
                error: Some(platform_error(code, message, &detail_json)),
                durable_lsn: 0,
                acked_replicas: Vec::new(),
                append_latency_micros: 0,
                deduplicated: false,
            },
            AppendAction::Stall(duration) => {
                tokio::time::sleep(duration).await;
                return Err(Status::deadline_exceeded("mock stall"));
            }
        };
        Ok(Response::new(response))
    }

    type ReadRangeStream = futures::stream::Iter<std::vec::IntoIter<Result<wal::WalChunk, Status>>>;

    async fn read_range(
        &self,
        request: Request<wal::ReadRangeRequest>,
    ) -> Result<Response<Self::ReadRangeStream>, Status> {
        let request = request.into_inner();
        self.inner.reads.lock().unwrap().push(request);

        let steps = self.next_read_action();
        let mut chunks: Vec<Result<wal::WalChunk, Status>> = Vec::with_capacity(steps.len());
        for step in steps {
            match step {
                ReadStep::Chunk {
                    start_lsn,
                    file_offset,
                    reset_wal,
                    data,
                } => chunks.push(Ok(wal::WalChunk {
                    error: None,
                    start_lsn,
                    data: data.to_vec(),
                    last: false,
                    wal_file_offset: file_offset,
                    reset_wal,
                })),
                ReadStep::Last { start_lsn } => chunks.push(Ok(wal::WalChunk {
                    error: None,
                    start_lsn,
                    data: Vec::new(),
                    last: true,
                    wal_file_offset: 0,
                    reset_wal: false,
                })),
                ReadStep::ErrorChunk {
                    code,
                    message,
                    detail_json,
                } => chunks.push(Ok(wal::WalChunk {
                    error: Some(platform_error(code, message, &detail_json)),
                    start_lsn: 0,
                    data: Vec::new(),
                    last: false,
                    wal_file_offset: 0,
                    reset_wal: false,
                })),
                ReadStep::Fail(code, message) => {
                    chunks.push(Err(Status::new(code, message)));
                }
            }
        }
        Ok(Response::new(futures::stream::iter(chunks)))
    }

    async fn set_owner_epoch(
        &self,
        request: Request<wal::SetOwnerEpochRequest>,
    ) -> Result<Response<wal::SetOwnerEpochResponse>, Status> {
        let request = request.into_inner();
        self.inner.epochs.lock().unwrap().push(request);

        let response = match self.next_epoch_action() {
            EpochAction::Ok(epoch) => wal::SetOwnerEpochResponse {
                error: None,
                applied_epoch: epoch,
                known_lsn: 0,
            },
            EpochAction::Err {
                code,
                message,
                detail_json,
                applied_epoch,
            } => wal::SetOwnerEpochResponse {
                error: Some(platform_error(code, message, detail_json)),
                applied_epoch,
                known_lsn: 0,
            },
        };
        Ok(Response::new(response))
    }

    async fn trim_before_lsn(
        &self,
        request: Request<wal::TrimBeforeLsnRequest>,
    ) -> Result<Response<wal::TrimBeforeLsnResponse>, Status> {
        let request = request.into_inner();
        self.inner.trims.lock().unwrap().push(request.clone());

        let response = match self.next_trim_action() {
            // 0 表示按请求的 LSN 回显水位
            TrimAction::Ok(0) => wal::TrimBeforeLsnResponse {
                error: None,
                trimmed_before_lsn: request.lsn,
            },
            TrimAction::Ok(lsn) => wal::TrimBeforeLsnResponse {
                error: None,
                trimmed_before_lsn: lsn,
            },
            TrimAction::Err { code, message } => wal::TrimBeforeLsnResponse {
                error: Some(platform_error(code, message, "")),
                trimmed_before_lsn: 0,
            },
        };
        Ok(Response::new(response))
    }

    async fn get_wal_status(
        &self,
        _request: Request<wal::GetWalStatusRequest>,
    ) -> Result<Response<wal::GetWalStatusResponse>, Status> {
        *self.inner.status_calls.lock().unwrap() += 1;
        Ok(Response::new(self.inner.status.lock().unwrap().clone()))
    }

    async fn health(
        &self,
        _request: Request<wal::HealthRequest>,
    ) -> Result<Response<wal::HealthResponse>, Status> {
        Ok(Response::new(self.inner.health.lock().unwrap().clone()))
    }

    async fn add_member(
        &self,
        _request: Request<wal::AddMemberRequest>,
    ) -> Result<Response<wal::AddMemberResponse>, Status> {
        Ok(Response::new(wal::AddMemberResponse { error: None }))
    }

    async fn remove_member(
        &self,
        _request: Request<wal::RemoveMemberRequest>,
    ) -> Result<Response<wal::RemoveMemberResponse>, Status> {
        Ok(Response::new(wal::RemoveMemberResponse { error: None }))
    }
}
