//! 节点间 Raft 通信（Peer Transport）。
//!
//! # 为什么不用 gRPC / 不注册到 tonic server
//!
//! 1. `proto/platform/*.proto` 是**冻结的对外契约**，里面没有 peer service。raft 的内部
//!    消息（`eraftpb::Message`，含 AppendEntries / RequestVote 等）格式由 raft-proto 定义，
//!    属于第三方实现细节；把它固化成平台 gRPC 契约意味着「升级 raft 版本 = 变更平台 API」，
//!    而且会让内部复制流量与客户端契约流量共用一个端口，无法独立限流、排障与灰度。
//! 2. 因此 peer 流量走 WAL_PEER_LISTEN 上的**独立 TCP 端口** + 极简 length-delimited 帧：
//!
//!    ```text
//!    4 字节大端长度前缀 | protobuf 编码的 raft::eraftpb::Message
//!    ```
//!
//!    两端必须是同一个 Raft Group 的成员（集群私网），因此不做 TLS/认证 —— 该端口的
//!    访问控制由部署层（network policy / 安全组）保证，与架构 §17.11 的内部网络假设一致。
//!
//! # 为什么用 rust-protobuf（而不是 prost）编解码
//!
//! `Cargo.toml` 里 raft 走的是默认 `protobuf-codec`（未启用 `prost-codec`），
//! `raft::eraftpb::Message` 只实现 `protobuf::Message`。要让 prost 也能用它，必须
//! 打开 raft 的 `prost-codec` feature —— 这会引入第二套 raft-proto 生成代码并改动
//! 依赖特性（本 crate 不允许改 Cargo.lock / 依赖清单）。所以 peer 编解码与 raft 自身
//! 使用同一个 codec。prost 仅用于 WAL Service **自己的**命令与快照编码（command.rs /
//! state_machine.rs），两者互不影响。
//!
//! # 可靠性取舍
//!
//! 本层**不保证投递**：发送队列满、链路断开时直接丢消息，由 Raft 自身的心跳重传与
//! 日志回溯补偿（raft 的一致性不依赖消息可靠投递）。若在这里做无界排队或阻塞重试，
//! 反而会把 raft 事件循环拖住，放大故障。

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use protobuf::Message as PbMessage;
use raft::eraftpb::Message;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::{ClusterMember, WalConfig};
use crate::error::{WalError, WalResult};
use crate::raft_group::GroupEvent;

/// 单帧长度上限（64 MiB，与本地 UDS framing 的上限保持一致）。
///
/// 长度前缀理论上可以声明 4 GiB：若不设上限，对端只要发 4 字节头就能让本端按声明长度
/// 预分配内存，构成 OOM 攻击面。超限一律断开连接。
pub const MAX_PEER_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// 单条 peer 链路的发送队列容量。
///
/// 队列满即丢（见模块注释的可靠性取舍）：容量取足够大以吸收一次心跳间隔内的突发
/// 复制消息，但不大到让内存被单个失联副本吃掉。
const PEER_CHANNEL_CAPACITY: usize = 4096;

/// 出站总队列容量（`raft_group` -> peer 传输层）。
///
/// 所有目标 peer 共用这一条队列，由发送 loop 分发到各链路。队列满同样丢弃：
/// Raft 的重传机制会补上，阻塞 raft 事件循环才是真正的故障放大。
pub const PEER_OUTBOUND_CAPACITY: usize = 4096;

/// 建连超时：SYN 被黑洞丢弃时不能让链路任务永久挂住。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// 重连退避上下限。
const RECONNECT_BACKOFF_MIN: Duration = Duration::from_millis(50);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(2);

// ---------------------------------------------------------------- 成员表

/// Raft 成员 ID -> peer 监听地址。
///
/// 启动时由 `WAL_CLUSTER` 初始化，之后随成员变更（AddMember / RemoveMember）更新，
/// 因此 `raft_group` 与 peer 传输层共享**同一份**表（`Arc` 内部可变）。
#[derive(Clone, Default)]
pub struct PeerRegistry {
    inner: Arc<RwLock<BTreeMap<u64, String>>>,
}

impl PeerRegistry {
    /// 空成员表。
    pub fn new() -> Self {
        Self::default()
    }

    /// 由启动参数中的成员表构造。
    pub fn from_members(members: &[ClusterMember]) -> Self {
        let registry = Self::new();
        for member in members {
            registry.insert(member.node_id, member.endpoint.clone());
        }
        registry
    }

    /// 写入 / 更新成员地址；返回是否真的发生了变化（endpoint 变更视为变化）。
    ///
    /// 返回变化标志让调用方（raft_group）能判断是否需要重建 peer 链路。
    pub fn insert(&self, node_id: u64, endpoint: String) -> bool {
        let mut members = self.inner.write();
        match members.get(&node_id) {
            Some(existing) if *existing == endpoint => false,
            _ => {
                members.insert(node_id, endpoint);
                true
            }
        }
    }

    /// 移除成员；返回被移除的地址。
    pub fn remove(&self, node_id: u64) -> Option<String> {
        self.inner.write().remove(&node_id)
    }

    /// 查询成员地址。
    pub fn get(&self, node_id: u64) -> Option<String> {
        self.inner.read().get(&node_id).cloned()
    }

    /// 是否是当前成员。
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)]
    pub fn contains(&self, node_id: u64) -> bool {
        self.inner.read().contains_key(&node_id)
    }

    /// 成员数量。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// 是否为空。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// 成员表快照（升序）。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn members(&self) -> Vec<(u64, String)> {
        self.inner
            .read()
            .iter()
            .map(|(id, endpoint)| (*id, endpoint.clone()))
            .collect()
    }
}

// ---------------------------------------------------------------- 帧编解码

/// 编码一帧：`4 字节大端长度前缀 | protobuf payload`。
///
/// 返回完整帧（前缀 + 载荷），便于调用方一次 `write_all`。
pub fn encode_frame(message: &Message) -> WalResult<Vec<u8>> {
    let payload = message
        .write_to_bytes()
        .map_err(|err| WalError::Internal(format!("Raft 消息编码失败：{err}")))?;
    if payload.len() > MAX_PEER_FRAME_BYTES {
        return Err(WalError::InvalidArgument(format!(
            "Raft 消息 {length} 字节超过单帧上限 {MAX_PEER_FRAME_BYTES} 字节",
            length = payload.len()
        )));
    }
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// 解码一帧的**载荷**（不含长度前缀）。
pub fn decode_frame(payload: &[u8]) -> WalResult<Message> {
    if payload.len() > MAX_PEER_FRAME_BYTES {
        return Err(WalError::InvalidArgument(format!(
            "Raft 消息载荷 {} 字节超过单帧上限 {MAX_PEER_FRAME_BYTES} 字节",
            payload.len()
        )));
    }
    // 载荷来自网络：解码失败必须是错误而不是 panic（对端版本不匹配 / 字节流错位）
    <Message as PbMessage>::parse_from_bytes(payload)
        .map_err(|err| WalError::InvalidArgument(format!("Raft 消息解码失败：{err}")))
}

/// 解析长度前缀（大端 u32）并做上限校验。
pub fn decode_length_prefix(prefix: [u8; 4]) -> WalResult<usize> {
    let length = u32::from_be_bytes(prefix) as usize;
    if length > MAX_PEER_FRAME_BYTES {
        return Err(WalError::InvalidArgument(format!(
            "对端声明的帧长度 {length} 超过上限 {MAX_PEER_FRAME_BYTES}"
        )));
    }
    Ok(length)
}

/// 写出一帧（长度前缀 + 载荷）。
pub async fn write_frame<W>(writer: &mut W, message: &Message) -> WalResult<()>
where
    W: AsyncWrite + Unpin,
{
    let frame = encode_frame(message)?;
    writer
        .write_all(&frame)
        .await
        .map_err(|err| WalError::Internal(format!("写入 peer 帧失败：{err}")))
}

/// 读取一帧；对端正常关闭返回 `None`。
pub async fn read_frame<R>(reader: &mut R) -> WalResult<Option<Message>>
where
    R: AsyncRead + Unpin,
{
    let mut prefix = [0u8; 4];
    // 首字节即 EOF 说明连接被对端正常关闭；读到一半断开属于异常
    let read = reader
        .read(&mut prefix[..1])
        .await
        .map_err(|err| WalError::Internal(format!("读取 peer 帧前缀失败：{err}")))?;
    if read == 0 {
        return Ok(None);
    }
    reader
        .read_exact(&mut prefix[1..])
        .await
        .map_err(|err| WalError::Internal(format!("读取 peer 帧前缀失败：{err}")))?;

    let length = decode_length_prefix(prefix)?;
    if length == 0 {
        return Err(WalError::InvalidArgument("对端发送了空帧".into()));
    }
    let mut payload = vec![0u8; length];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|err| WalError::Internal(format!("读取 peer 帧载荷失败：{err}")))?;
    decode_frame(&payload).map(Some)
}

// ---------------------------------------------------------------- 传输层

/// Peer 传输层句柄。
///
/// 生命周期与进程一致：`spawn` 之后 accept loop 与发送 loop 各自在后台任务里运行，
/// 停机时由 [`CancellationToken`] 统一收口。
pub struct PeerTransport {
    local_addr: SocketAddr,
    accept_task: JoinHandle<()>,
    send_task: JoinHandle<()>,
}

impl PeerTransport {
    /// 绑定 WAL_PEER_LISTEN 并启动两条 loop。
    ///
    /// 绑定用**同步** `std::net::TcpListener`：端口被占用这类错误必须在启动阶段
    /// 直接抛出（fail fast），而不是在一个后台任务里悄悄失败导致集群「少一个副本」。
    ///
    /// `outbound` 由装配层创建并交给 Raft Group 持有发送端 —— 通道两端必须来自
    /// 同一次创建（早期实现里两端分别创建会让消息永远到不了对端）。
    pub fn spawn(
        config: Arc<WalConfig>,
        peers: PeerRegistry,
        events: mpsc::Sender<GroupEvent>,
        outbound: mpsc::Receiver<(u64, Message)>,
        shutdown: CancellationToken,
    ) -> WalResult<Self> {
        let listener = std::net::TcpListener::bind(config.peer_listen).map_err(|err| {
            WalError::Internal(format!(
                "绑定 peer 监听地址 {} 失败：{err}",
                config.peer_listen
            ))
        })?;
        let local_addr = listener
            .local_addr()
            .map_err(|err| WalError::Internal(format!("读取 peer 监听地址失败：{err}")))?;
        listener
            .set_nonblocking(true)
            .map_err(|err| WalError::Internal(format!("设置 peer 监听非阻塞失败：{err}")))?;
        let listener = TcpListener::from_std(listener)
            .map_err(|err| WalError::Internal(format!("注册 peer 监听失败：{err}")))?;

        let accept_task = tokio::spawn(accept_loop(listener, events.clone(), shutdown.clone()));
        let send_task = tokio::spawn(sender_loop(outbound, peers, events, shutdown));

        info!(local_addr = %local_addr, "Peer 传输层已启动");
        Ok(Self {
            local_addr,
            accept_task,
            send_task,
        })
    }

    /// 实际绑定的地址（端口配 0 时由内核分配，集成测试据此取真实端口）。
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// 等待两条 loop 退出（停机路径使用）。
    pub async fn join(self) {
        // 任一 loop 退出都说明 shutdown 已被触发或监听器不可用，直接等两者结束
        let _ = self.accept_task.await;
        let _ = self.send_task.await;
    }
}

/// 接受入站连接：每个连接一个读 loop，读到消息就投给 Raft Group 事件循环。
async fn accept_loop(
    listener: TcpListener,
    events: mpsc::Sender<GroupEvent>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, peer_addr) = match accepted {
                    Ok(accepted) => accepted,
                    Err(err) => {
                        // accept 失败通常是 fd 耗尽等瞬时问题：记录后继续，
                        // 不退出 loop（退出等于该节点永久失去入站复制能力）
                        warn!(error = %err, "接受 peer 连接失败");
                        continue;
                    }
                };
                // 内部流量：禁用 Nagle，避免小的心跳/投票消息被攒批延迟
                let _ = stream.set_nodelay(true);
                let events = events.clone();
                let shutdown = shutdown.clone();
                tokio::spawn(async move {
                    if let Err(err) = inbound_loop(stream, events, shutdown).await {
                        debug!(peer = %peer_addr, error = %err, "peer 入站连接结束");
                    }
                });
            }
        }
    }
    debug!("peer accept loop 已退出");
}

/// 单条入站连接的读 loop。
async fn inbound_loop(
    mut stream: TcpStream,
    events: mpsc::Sender<GroupEvent>,
    shutdown: CancellationToken,
) -> WalResult<()> {
    loop {
        let message = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            frame = read_frame(&mut stream) => frame?,
        };
        let Some(message) = message else {
            return Ok(());
        };
        // 背压：await 投递。Raft Group 处理慢时让 TCP 接收窗口自然收敛，
        // 而不是在这里无界堆积内存。
        if events
            .send(GroupEvent::Message(Box::new(message)))
            .await
            .is_err()
        {
            // 事件循环已退出（进程停机）
            return Ok(());
        }
    }
}

/// 出站发送 loop：按 node_id 维护链路，链路断开由各自的 writer 任务自行重连。
async fn sender_loop(
    mut queue: mpsc::Receiver<(u64, Message)>,
    peers: PeerRegistry,
    events: mpsc::Sender<GroupEvent>,
    shutdown: CancellationToken,
) {
    let mut links: BTreeMap<u64, PeerLink> = BTreeMap::new();
    loop {
        let (to, message) = tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            item = queue.recv() => match item {
                Some(item) => item,
                None => break,
            },
        };

        // 目标不在成员表：丢弃。raft 只应给成员发消息，走到这里说明成员表刚被变更，
        // 保留旧链路会让被移除的节点继续收到复制流量。
        let Some(endpoint) = peers.get(to) else {
            if links.remove(&to).is_some() {
                debug!(node_id = to, "成员已移除，关闭 peer 链路");
            }
            continue;
        };

        match links.get(&to) {
            // endpoint 未变化：复用链路
            Some(link) if link.endpoint == endpoint => {}
            // endpoint 变化（成员表更新）：重建链路
            Some(_) => {
                debug!(node_id = to, endpoint = %endpoint, "peer 地址已变化，重建链路");
                let link = PeerLink::spawn(to, endpoint.clone(), events.clone(), shutdown.clone());
                links.insert(to, link);
            }
            None => {
                let link = PeerLink::spawn(to, endpoint.clone(), events.clone(), shutdown.clone());
                links.insert(to, link);
            }
        }

        if let Some(link) = links.get(&to) {
            match link.sender.try_send(message) {
                Ok(()) => {}
                // 队列满：丢弃并由 raft 重传补偿（见模块注释的可靠性取舍）
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!(
                        node_id = to,
                        "peer 发送队列已满，丢弃一条 Raft 消息（将由重传补偿）"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    debug!(node_id = to, "peer 链路任务已退出，移除链路");
                    links.remove(&to);
                }
            }
        }
    }
    // 丢弃 links 会关闭各 writer 任务的队列，任务随之退出
    debug!("peer 发送 loop 已退出");
}

/// 到单个 peer 的链路：一个后台 writer 任务 + 有界队列。
struct PeerLink {
    endpoint: String,
    sender: mpsc::Sender<Message>,
}

impl PeerLink {
    fn spawn(
        node_id: u64,
        endpoint: String,
        events: mpsc::Sender<GroupEvent>,
        shutdown: CancellationToken,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(PEER_CHANNEL_CAPACITY);
        let link_endpoint = endpoint.clone();
        tokio::spawn(async move {
            writer_task(node_id, link_endpoint, receiver, events, shutdown).await;
        });
        Self { endpoint, sender }
    }
}

/// 单 peer 的写任务：连接 -> 发送 -> 断开后指数退避重连。
async fn writer_task(
    node_id: u64,
    endpoint: String,
    mut queue: mpsc::Receiver<Message>,
    events: mpsc::Sender<GroupEvent>,
    shutdown: CancellationToken,
) {
    let mut backoff = RECONNECT_BACKOFF_MIN;
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(&endpoint)).await {
            Ok(Ok(stream)) => {
                let _ = stream.set_nodelay(true);
                backoff = RECONNECT_BACKOFF_MIN;
                match pump(stream, &mut queue, &shutdown).await {
                    Ok(()) => {
                        debug!(node_id, endpoint = %endpoint, "peer 连接已关闭");
                    }
                    Err(err) => {
                        debug!(node_id, endpoint = %endpoint, error = %err, "peer 连接中断");
                    }
                }
                notify_unreachable(&events, node_id).await;
            }
            Ok(Err(err)) => {
                warn!(node_id, endpoint = %endpoint, error = %err, "连接 peer 失败");
            }
            Err(_) => {
                warn!(node_id, endpoint = %endpoint, "连接 peer 超时");
            }
        }

        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
    }
}

/// 在一条已建立的连接上泵消息。
///
/// 同时监视读半边：对端进程退出会关闭连接（EOF），此时立刻结束本任务触发重连，
/// 而不是等到下一次写失败。注意半开连接（对端主机断电、无 FIN）无法在此发现，
/// 由 Raft 的选举/心跳超时兜底。
async fn pump(
    stream: TcpStream,
    queue: &mut mpsc::Receiver<Message>,
    shutdown: &CancellationToken,
) -> WalResult<()> {
    let (mut reader, mut writer) = stream.into_split();
    let (closed_tx, mut closed_rx) = mpsc::channel::<()>(1);

    // 监视任务：只做「读到 EOF 就通知」，不解析内容 —— 对端连上我们之后的写方向
    // 是它自己的连接，这里读到任何字节都属于协议误用，直接当作断开处理。
    tokio::spawn(async move {
        let mut scratch = [0u8; 64];
        loop {
            match reader.read(&mut scratch).await {
                Ok(0) => break,
                Ok(_) => {}
                Err(_) => break,
            }
        }
        let _ = closed_tx.send(()).await;
    });

    loop {
        let message = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            _ = closed_rx.recv() => return Ok(()),
            item = queue.recv() => match item {
                Some(message) => message,
                // 发送侧丢弃了队列（进程停机或链路被替换）
                None => return Ok(()),
            },
        };
        write_frame(&mut writer, &message).await?;
    }
}

/// 告知 Raft Group 某个 peer 不可达（用于 `RawNode::report_unreachable`）。
///
/// 投递失败说明事件循环已退出，此时无需再通知。
async fn notify_unreachable(events: &mpsc::Sender<GroupEvent>, node_id: u64) {
    let _ = events.send(GroupEvent::PeerUnreachable(node_id)).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft::eraftpb::Entry;

    fn sample_message() -> Message {
        let mut entry = Entry::default();
        entry.set_index(7);
        entry.set_term(3);
        entry.data = bytes::Bytes::from_static(b"wal-payload");

        let mut message = Message::default();
        message.set_msg_type(raft::eraftpb::MessageType::MsgAppend);
        message.set_from(1);
        message.set_to(2);
        message.set_term(3);
        message.set_index(6);
        message.set_log_term(2);
        message.set_commit(5);
        message.entries = vec![entry].into();
        message
    }

    #[test]
    fn frame_roundtrip_preserves_message() {
        let message = sample_message();
        let frame = encode_frame(&message).unwrap();
        // 4 字节前缀 + 载荷
        assert_eq!(
            u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize,
            frame.len() - 4
        );
        let decoded = decode_frame(&frame[4..]).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn length_prefix_rejects_oversize_declaration() {
        // 声明一个 1 GiB 的帧：必须在分配内存前拒绝
        let prefix = (1024u32 * 1024 * 1024).to_be_bytes();
        assert!(decode_length_prefix(prefix).is_err());
        assert_eq!(decode_length_prefix(1024u32.to_be_bytes()).unwrap(), 1024);
    }

    #[test]
    fn decode_rejects_garbage_payload() {
        // 任意字节（非 protobuf）必须返回错误而不是 panic
        assert!(decode_frame(&[0xff, 0xff, 0xff, 0xff]).is_err());
        // 空载荷解码为默认消息在 protobuf 语义下合法，但不含任何路由信息，
        // 因此由上层的 raft 逻辑（未知 from）丢弃，这里只保证不 panic
        let _ = decode_frame(&[]);
    }

    #[test]
    fn registry_tracks_membership_changes() {
        let registry = PeerRegistry::from_members(&[
            ClusterMember {
                node_id: 1,
                endpoint: "wal-1:9201".into(),
            },
            ClusterMember {
                node_id: 2,
                endpoint: "wal-2:9201".into(),
            },
        ]);
        assert_eq!(registry.len(), 2);
        assert!(registry.contains(2));
        assert_eq!(registry.get(2).as_deref(), Some("wal-2:9201"));
        // 相同地址不算变化，避免无谓重建链路
        assert!(!registry.insert(2, "wal-2:9201".into()));
        assert!(registry.insert(2, "wal-2b:9201".into()));
        assert_eq!(registry.get(2).as_deref(), Some("wal-2b:9201"));
        assert_eq!(registry.remove(2).as_deref(), Some("wal-2b:9201"));
        assert!(!registry.contains(2));
        assert_eq!(registry.members().len(), 1);
    }

    #[tokio::test]
    async fn frames_survive_duplex_transport() {
        // 用内存 duplex 替代真实网络：本层不依赖外部进程，默认就能跑
        let (mut client, mut server) = tokio::io::duplex(64 * 1024);
        let message = sample_message();
        write_frame(&mut client, &message).await.unwrap();
        let decoded = read_frame(&mut server).await.unwrap().unwrap();
        assert_eq!(decoded, message);
    }

    #[tokio::test]
    async fn read_frame_reports_clean_eof() {
        // 对端直接关闭：必须是 Ok(None)（正常关闭）而不是错误
        let (client, mut server) = tokio::io::duplex(1024);
        drop(client);
        assert!(read_frame(&mut server).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn read_frame_rejects_truncated_payload() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        // 声明 16 字节但只写 4 字节，然后关闭
        client.write_all(&16u32.to_be_bytes()).await.unwrap();
        client.write_all(&[1, 2, 3, 4]).await.unwrap();
        drop(client);
        assert!(read_frame(&mut server).await.is_err());
    }

    #[tokio::test]
    async fn read_frame_rejects_oversize_declaration() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        client.write_all(&(u32::MAX).to_be_bytes()).await.unwrap();
        assert!(read_frame(&mut server).await.is_err());
    }

    #[tokio::test]
    async fn registry_is_shared_between_clones() {
        // raft_group 与 peer 传输层共享同一份成员表：克隆必须看到同一份数据
        let registry = PeerRegistry::new();
        let clone = registry.clone();
        assert!(clone.insert(3, "wal-3:9201".into()));
        assert_eq!(registry.get(3).as_deref(), Some("wal-3:9201"));
        assert!(registry.remove(3).is_some());
        assert!(clone.is_empty());
    }
}
