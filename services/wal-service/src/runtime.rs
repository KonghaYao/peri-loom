//! 节点装配：把配置变成「一个跑起来的 WAL 节点」。
//!
//! 为什么把装配从 `main` 里抽出来：
//! 1. `main` 只负责「读配置 -> 装观测 -> 起节点 -> 等信号」，不掺业务装配细节；
//! 2. 集成测试需要**在同一进程内**起一个完整的单节点服务（gRPC + peer + ops），
//!    并与真实端口交互；装配逻辑只有一份，测试与服务不会走偏。
//!
//! 启动顺序（每一层的失败都必须在启动阶段暴露，而不是运行期才炸）：
//!
//! ```text
//! 1. 打开 raft-engine（数据目录、成员表）
//! 2. 绑定 peer / gRPC / ops 三个监听（fail fast）
//! 3. 起 Peer 传输层（入站 accept + 出站发送）
//! 4. 起 Raft Group（事件循环，恢复状态机与日志）
//! 5. 起 gRPC server 与 Ops HTTP
//! ```

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::WalConfig;
use crate::error::{WalError, WalResult};
use crate::ops::{self, OpsState};
use crate::peer::{PeerRegistry, PeerTransport, PEER_OUTBOUND_CAPACITY};
use crate::raft_group::{RaftGroup, WalHandle};
use crate::service::{self, WalService};
use crate::storage::{group_id_from_shard, WalStorage};

/// 已启动的 WAL 节点。
pub struct WalNode {
    /// Raft Group 对外句柄（gRPC 层的唯一入口）。
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)]
    pub handle: WalHandle,
    /// 本地持久化层（ops 指标读取用）。
    #[allow(dead_code)]
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub storage: Arc<WalStorage>,
    /// 客户端 gRPC 实际监听地址（端口配 0 时是内核分配的端口）。
    pub grpc_addr: SocketAddr,
    /// peer 实际监听地址。
    pub peer_addr: SocketAddr,
    /// Ops HTTP 实际监听地址。
    pub ops_addr: SocketAddr,
    shutdown: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
}

impl WalNode {
    /// 请求停机并等待所有后台任务退出。
    pub async fn shutdown(mut self) {
        self.shutdown.cancel();
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
    }

    /// 停机令牌（供外部在异常时触发全局停机）。
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }
}

/// 按配置装配并启动一个完整节点。
///
/// 单节点集群（`WAL_CLUSTER` 只含本节点或留空）走的是**完全相同**的代码路径：
/// 单节点 Raft 立即当选，Append 语义退化为「本地 fsync 即 quorum durable」，
/// 客户端看到的接口与错误语义不变（便于本地开发与集成测试）。
pub async fn start(config: Arc<WalConfig>) -> WalResult<WalNode> {
    config.validate()?;
    let members = config.members()?;

    // ---- 1) 本地持久化层 ----
    let group_id = group_id_from_shard(&config.shard_id);
    let member_ids: Vec<u64> = members.iter().map(|member| member.node_id).collect();
    let storage = Arc::new(WalStorage::open(&config.data_dir, group_id, &member_ids)?);
    info!(
        node_id = config.node_id,
        shard = %config.shard_id,
        group_id,
        data_dir = %config.data_dir.display(),
        voters = ?storage.conf_state().voters,
        "本地 Raft 日志已打开"
    );

    // ---- 2) 绑定监听（fail fast）----
    // peer 监听由 PeerTransport::spawn 自己绑定（同一份 fail-fast 逻辑只有一处）
    let grpc_listener = service::bind_listener(config.listen)?;
    let grpc_addr = grpc_listener
        .local_addr()
        .map_err(|err| WalError::Internal(format!("读取 gRPC 监听地址失败：{err}")))?;

    let ops_listener = service::bind_listener(config.ops_listen)?;
    let ops_addr = ops_listener
        .local_addr()
        .map_err(|err| WalError::Internal(format!("读取 Ops 监听地址失败：{err}")))?;

    // ---- 3) 成员表 + Peer 传输层 ----
    let peers = PeerRegistry::from_members(&members);

    let shutdown = CancellationToken::new();
    // 出站通道由装配层创建：Raft Group 持发送端，Peer 传输层持接收端
    let (outbound_tx, outbound_rx) = mpsc::channel(PEER_OUTBOUND_CAPACITY);

    // 先建 Raft Group（它创建事件通道），再建 Peer 传输层（需要事件发送端）。
    let (group, handle) =
        RaftGroup::new(config.clone(), storage.clone(), peers.clone(), outbound_tx)?;
    let transport = PeerTransport::spawn(
        config.clone(),
        peers.clone(),
        handle.events_sender(),
        outbound_rx,
        shutdown.clone(),
    )?;
    let peer_addr = transport.local_addr();
    // 端口配 0（内核分配）时，真实端口要到绑定之后才知道：把本节点在成员表里的地址
    // 更新成实际地址，否则日志/指标/成员变更里看到的都是「:0」，无法据此排查连接问题。
    if config.peer_listen.port() == 0 {
        peers.insert(config.node_id, peer_addr.to_string());
    }

    // ---- 4) Raft Group 事件循环 ----
    let group_shutdown = shutdown.clone();
    let group_task = tokio::spawn(async move {
        match group.run(group_shutdown.clone()).await {
            Ok(()) => info!("Raft Group 已停止"),
            Err(err) => {
                // Raft 是唯一的写入路径：它退出后本节点无法再接受任何 Append，
                // 继续对外提供 gRPC 只会让客户端拿到误导性的错误，因此直接全局停机。
                error!(error = %err, "Raft Group 异常退出，触发停机");
                group_shutdown.cancel();
            }
        }
    });

    // ---- 5) gRPC 与 Ops ----
    let prometheus = match ops::install_metrics_recorder() {
        Ok(handle) => handle,
        // 指标不是业务路径：装不上只降级，不影响 WAL 服务本身
        Err(err) => {
            warn!(error = %err, "/metrics 将不可用（Prometheus recorder 已被占用）");
            metrics_exporter_prometheus::PrometheusBuilder::new()
                .build_recorder()
                .handle()
        }
    };

    let wal_service = WalService::new(handle.clone(), config.clone());
    let grpc_shutdown = shutdown.clone();
    let incoming = incoming_stream(grpc_listener, grpc_shutdown.clone());
    let grpc_task = tokio::spawn(async move {
        let result = tonic::transport::Server::builder()
            .add_service(service::server(wal_service))
            .serve_with_incoming_shutdown(incoming, grpc_shutdown.cancelled())
            .await;
        match result {
            Ok(()) => info!("gRPC server 已停止"),
            Err(err) => error!(error = %err, "gRPC server 异常退出"),
        }
    });

    let ops_state = OpsState::new(handle.clone(), storage.clone(), config.clone(), prometheus);
    let ops_shutdown = shutdown.clone();
    let ops_task = tokio::spawn(async move {
        ops::serve(ops_listener, ops_state, ops_shutdown).await;
    });

    info!(
        grpc_addr = %grpc_addr,
        peer_addr = %peer_addr,
        ops_addr = %ops_addr,
        node_id = config.node_id,
        shard = %config.shard_id,
        "wal-service 已就绪"
    );

    Ok(WalNode {
        handle,
        storage,
        grpc_addr,
        peer_addr,
        ops_addr,
        shutdown,
        tasks: vec![
            group_task,
            grpc_task,
            ops_task,
            tokio::spawn(async move { transport.join().await }),
        ],
    })
}

/// 把 TCP listener 变成 tonic 需要的连接流。
///
/// accept 出错（fd 耗尽等）不应终止 server：记录后继续接受，否则一次瞬时错误会让
/// 整个节点永久停止服务。停机时流结束，tonic 进入 graceful shutdown。
fn incoming_stream(
    listener: tokio::net::TcpListener,
    shutdown: CancellationToken,
) -> impl futures::Stream<Item = Result<TcpStream, std::io::Error>> + Send + 'static {
    futures::stream::unfold(listener, move |listener| {
        let shutdown = shutdown.clone();
        async move {
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => return None,
                    accepted = listener.accept() => match accepted {
                        Ok((stream, _peer)) => {
                            // 内部 RPC：禁用 Nagle，避免小请求被攒批放大延迟
                            let _ = stream.set_nodelay(true);
                            return Some((Ok(stream), listener));
                        }
                        Err(err) => {
                            warn!(error = %err, "接受 gRPC 连接失败");
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "需要联网能力与磁盘：默认只做编译守护，用 --ignored 显式运行"]
    async fn single_node_starts_and_serves_health() {
        // 集成测试（默认忽略）：起一个单节点实例，验证三个监听都真的可用。
        // 具体业务语义（Append / ReadRange / fencing）由 service.rs 下的集成测试覆盖。
        let dir = tempfile::Builder::new()
            .prefix("wal-runtime-test")
            .tempdir()
            .expect("创建临时目录");
        let config = Arc::new(WalConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            peer_listen: "127.0.0.1:0".parse().unwrap(),
            ops_listen: "127.0.0.1:0".parse().unwrap(),
            data_dir: dir.keep(),
            ..WalConfig::default()
        });

        let node = start(config).await.expect("单节点必须能启动");
        assert_ne!(node.grpc_addr.port(), 0);
        assert_ne!(node.peer_addr.port(), 0);
        assert_ne!(node.ops_addr.port(), 0);

        // 直接用 TCP 发一个最小 HTTP/1.1 请求：不引入额外的 HTTP 客户端依赖
        let body = http_get(node.ops_addr, "/healthz").await;
        assert!(body.contains("200 OK"), "healthz 必须 200：{body}");
        assert!(body.contains("ok"), "healthz 应返回 ok：{body}");

        node.shutdown().await;
    }

    /// 极简 HTTP/1.1 GET（仅测试用，避免为测试引入 HTTP 客户端依赖）。
    async fn http_get(addr: SocketAddr, path: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = TcpStream::connect(addr).await.expect("连接 Ops 端口");
        let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.expect("写请求");
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.expect("读响应");
        String::from_utf8_lossy(&response).into_owned()
    }
}
