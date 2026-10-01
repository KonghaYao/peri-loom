//! Raft Group 驱动：单 shard = 单 Raft Group（架构 §17.8）。
//!
//! 本模块把 raft-rs 的 `RawNode` 包成「可被 gRPC 并发调用」的服务：
//!
//! ```text
//! gRPC handler ──propose(oneshot)──> 本模块事件循环 ──> RawNode::propose
//!                                          │
//!                          Ready: persist(fsync) -> send msgs -> apply -> advance
//!                                          │
//!                                          └─> 状态机(apply) -> 唤醒 oneshot
//! ```
//!
//! 为什么必须把所有 Raft 操作串行化到一个 loop 里：raft-rs 明确要求
//! 「`ready()` 到 `advance()` 之间不得调用 `step` / `propose` / `campaign`」。
//! 因此 gRPC 线程只能通过 channel 投递事件，绝不允许直接碰 `RawNode`。
//!
//! 关键顺序（崩溃安全）：**先把日志条目与 HardState fsync 落盘，再发消息、再 apply**。
//! 反过来会在崩溃后出现「已对客户端承诺 durable、但日志不在盘上」的窗口。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use raft::eraftpb::{ConfChange, EntryType, Message};
use raft::{Config as RaftConfig, RawNode, StateRole};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::command::{WalCommand, SM_SNAPSHOT_APPLY_INTERVAL};
use crate::config::WalConfig;
use crate::error::{WalError, WalResult};
use crate::logging::raft_logger;
use crate::peer::PeerRegistry;
use crate::state_machine::{ApplyOutcome, ReadRangeCursor, StateMachine, WalChunk, WalStatus};
use crate::storage::WalStorage;

/// Raft Group 事件队列容量。
///
/// 队列里流动的是「peer 收到的消息 + gRPC 提案」：容量太小会让 peer 读任务频繁等待，
/// 太大则让事件循环的延迟被放大。1024 与单次选举/批量复制的消息量同量级。
const GROUP_EVENT_CAPACITY: usize = 1024;

/// 提交给 Raft Group 的事件（保证在 loop 里串行处理）。
#[derive(Debug)]
pub enum GroupEvent {
    /// 来自其它节点的 Raft 消息。
    Message(Box<Message>),
    /// 某个 peer 不可达（发送失败），用于让 raft 推进 recent_active 判断。
    PeerUnreachable(u64),
    /// 来自 gRPC 的提案。
    Proposal(Proposal),
}

/// 提案载荷。
///
/// 两类提案都走同一条「提案 -> Ready -> apply -> 唤醒等待方」的路径，
/// 因此共用 [`Proposal`] 与等待通道，避免成员变更另起一套状态跟踪。
#[derive(Debug)]
pub enum ProposalPayload {
    /// 状态机命令（SetEpoch / Append / Trim）。
    Command(WalCommand),
    /// Raft 成员变更（AddMember / RemoveMember）。
    ///
    /// 用 `Box` 是因为 `ConfChange` 比命令大得多，而 `GroupEvent` 会在队列里成批搬运。
    ConfChange(Box<ConfChange>),
}

/// 一条待提交的提案。
#[derive(Debug)]
pub struct Proposal {
    /// 提案载荷。
    pub payload: ProposalPayload,
    /// 追踪 ID（写入 Raft Entry context，便于跨节点排查）。
    pub trace_id: String,
    /// 结果回调。
    pub reply: oneshot::Sender<WalResult<ProposalAck>>,
}

/// 提案结果。
#[derive(Debug, Clone)]
pub struct ProposalAck {
    /// 状态机 apply 结果。
    pub outcome: ApplyOutcome,
    /// 已确认该条日志的副本（leader 视角；follower 恒为空）。
    pub acked_replicas: Vec<String>,
}

/// Raft Group 运行态（原子量，供 Health / readyz / metrics 读取）。
#[derive(Debug)]
pub struct GroupStatus {
    /// 本节点 ID。
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)]
    pub node_id: u64,
    /// WAL Shard 标识。
    pub shard_id: String,
    is_leader: AtomicBool,
    leader_id: AtomicU64,
    term: AtomicU64,
    commit_index: AtomicU64,
    applied_index: AtomicU64,
}

/// `GroupStatus` 的一份无锁快照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupHealth {
    /// 本节点当前是否为 leader。
    pub is_leader: bool,
    /// 已知 leader（0 = 选举中）。
    pub leader_id: u64,
    /// 当前 term。
    pub term: u64,
    /// commit index。
    pub commit_index: u64,
    /// applied index。
    pub applied_index: u64,
}

impl GroupStatus {
    fn new(node_id: u64, shard_id: String) -> Self {
        Self {
            node_id,
            shard_id,
            is_leader: AtomicBool::new(false),
            leader_id: AtomicU64::new(0),
            term: AtomicU64::new(0),
            commit_index: AtomicU64::new(0),
            applied_index: AtomicU64::new(0),
        }
    }

    /// 读取快照。
    pub fn snapshot(&self) -> GroupHealth {
        GroupHealth {
            is_leader: self.is_leader.load(Ordering::Acquire),
            leader_id: self.leader_id.load(Ordering::Acquire),
            term: self.term.load(Ordering::Acquire),
            commit_index: self.commit_index.load(Ordering::Acquire),
            applied_index: self.applied_index.load(Ordering::Acquire),
        }
    }

    /// 集群是否已经选出了 leader（readiness 判定）。
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)]
    pub fn has_leader(&self) -> bool {
        self.leader_id.load(Ordering::Acquire) != 0
    }
}

/// Append 请求参数（proto 层解包后传入，避免本模块依赖 proto 细节）。
#[derive(Debug, Clone)]
pub struct AppendParams {
    /// 目标 DB。
    pub database_id: String,
    /// 写入者 epoch。
    pub owner_epoch: u64,
    /// 首字节 LSN。
    pub start_lsn: u64,
    /// 本地 WAL 文件偏移。
    pub wal_file_offset: u64,
    /// 是否开启新一代 WAL。
    pub reset_wal: bool,
    /// WAL 字节。
    pub bytes: Vec<u8>,
    /// 幂等键。
    pub append_id: String,
    /// 是否包含 commit frame。
    pub contains_commit_frame: bool,
    /// 调用方 deadline（Unix 毫秒，0 表示未设置）。
    pub deadline_unix_ms: u64,
    /// 追踪 ID。
    pub trace_id: String,
}

/// Append 结果。
#[derive(Debug, Clone)]
pub struct AppendAck {
    /// 已 durable 的末端 LSN（exclusive）。
    pub durable_lsn: u64,
    /// 是否幂等命中。
    pub deduplicated: bool,
    /// 本次 Append 是否触发了幂等窗口逐出（最旧的键被丢弃）。
    ///
    /// proto 的 `AppendResponse` 没有可承载该标记的字段，调用方（gRPC 层）据此打日志 /
    /// 告警：窗口一旦开始逐出，极长重试链里的旧 append_id 就可能失去去重保护。
    pub idempotency_window_evicted: bool,
    /// 参与确认的副本。
    pub acked_replicas: Vec<String>,
}

/// 对外的 Wal 句柄：gRPC 层只用它，不接触 `RawNode`。
#[derive(Clone)]
pub struct WalHandle {
    events: mpsc::Sender<GroupEvent>,
    state_machine: Arc<RwLock<StateMachine>>,
    status: Arc<GroupStatus>,
    config: Arc<WalConfig>,
}

impl WalHandle {
    /// 事件发送端：peer 传输层把对端消息投给 Raft Group 的唯一入口。
    ///
    /// 通道两端都由 [`RaftGroup::new`] 创建，装配层只从这里取发送端，
    /// 避免出现「两端来自不同 channel、消息永远到不了事件循环」的接线错误。
    pub fn events_sender(&self) -> mpsc::Sender<GroupEvent> {
        self.events.clone()
    }

    /// 本节点当前是否为 leader。
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)]
    pub fn is_leader(&self) -> bool {
        self.status.snapshot().is_leader
    }

    /// 运行态快照。
    pub fn health(&self) -> GroupHealth {
        self.status.snapshot()
    }

    /// Append：fencing 预检 -> 提案 -> 等 committed + applied（带超时）。
    pub async fn append(&self, params: AppendParams) -> WalResult<AppendAck> {
        // 空 append_id 的写请求没有幂等语义：ACK 丢失后的重试会重复写 WAL。
        // 状态机里也有同样一道（保证三副本判定一致），这里提前拦是为了不白跑一轮 Raft。
        if params.append_id.is_empty() {
            return Err(WalError::InvalidArgument(
                "Append 的 append_id 不能为空：它是重试幂等的唯一依据".into(),
            ));
        }
        self.ensure_leader()?;
        self.precheck_epoch(&params.database_id, params.owner_epoch)?;

        let command = WalCommand::append(crate::command::AppendCommand {
            database_id: params.database_id.clone(),
            owner_epoch: params.owner_epoch,
            start_lsn: params.start_lsn,
            wal_file_offset: params.wal_file_offset,
            reset_wal: params.reset_wal,
            bytes: params.bytes.clone(),
            append_id: params.append_id.clone(),
            contains_commit_frame: params.contains_commit_frame,
        });

        let wait = self.wait_budget(params.deadline_unix_ms);
        let outcome = self
            .propose(ProposalPayload::Command(command), params.trace_id, wait)
            .await?;
        match outcome.outcome {
            ApplyOutcome::Append {
                durable_lsn,
                deduplicated,
                idempotency_window_evicted,
            } => Ok(AppendAck {
                durable_lsn,
                deduplicated,
                idempotency_window_evicted,
                acked_replicas: outcome.acked_replicas,
            }),
            other => Err(WalError::Internal(format!(
                "Append 提案返回了非 Append 结果：{other:?}"
            ))),
        }
    }

    /// 设置 / 提升 owner epoch（fencing）。
    pub async fn set_owner_epoch(
        &self,
        database_id: &str,
        owner_epoch: u64,
        worker_id: &str,
        reason: &str,
        deadline_unix_ms: u64,
        trace_id: String,
    ) -> WalResult<(u64, u64)> {
        self.ensure_leader()?;
        if database_id.is_empty() {
            return Err(WalError::InvalidArgument("database_id 不能为空".into()));
        }
        if owner_epoch == 0 {
            return Err(WalError::InvalidArgument(
                "owner_epoch 不能为 0：0 表示无 Owner".into(),
            ));
        }
        let command = WalCommand::set_epoch(crate::command::SetEpochCommand {
            database_id: database_id.to_owned(),
            owner_epoch,
            worker_id: worker_id.to_owned(),
            reason: reason.to_owned(),
        });
        let outcome = self
            .propose(
                ProposalPayload::Command(command),
                trace_id,
                self.wait_budget(deadline_unix_ms),
            )
            .await?;
        match outcome.outcome {
            ApplyOutcome::SetEpoch {
                applied_epoch,
                known_lsn,
            } => Ok((applied_epoch, known_lsn)),
            other => Err(WalError::Internal(format!(
                "SetOwnerEpoch 提案返回了非预期结果：{other:?}"
            ))),
        }
    }

    /// 截断 `before_lsn` 之前的数据（必须带 snapshot_id）。
    pub async fn trim_before_lsn(
        &self,
        database_id: &str,
        before_lsn: u64,
        snapshot_id: &str,
        deadline_unix_ms: u64,
        trace_id: String,
    ) -> WalResult<u64> {
        self.ensure_leader()?;
        if snapshot_id.is_empty() {
            return Err(WalError::InvalidArgument(
                "TrimBeforeLsn 必须携带 snapshot_id（防止误截断）".into(),
            ));
        }
        let command = WalCommand::trim(crate::command::TrimCommand {
            database_id: database_id.to_owned(),
            before_lsn,
            snapshot_id: snapshot_id.to_owned(),
        });
        let outcome = self
            .propose(
                ProposalPayload::Command(command),
                trace_id,
                self.wait_budget(deadline_unix_ms),
            )
            .await?;
        match outcome.outcome {
            ApplyOutcome::Trim { trimmed_before_lsn } => Ok(trimmed_before_lsn),
            other => Err(WalError::Internal(format!(
                "Trim 提案返回了非预期结果：{other:?}"
            ))),
        }
    }

    /// 增加一个 Raft 成员（运维操作）。
    ///
    /// `endpoint` 会随 conf change 一起复制：其它副本必须知道「新成员怎么连」，
    /// 否则成员表只在提交方的内存里正确，重启后新成员就联系不上了。
    pub async fn add_member(
        &self,
        node_id: u64,
        endpoint: &str,
        trace_id: String,
    ) -> WalResult<()> {
        self.ensure_leader()?;
        if node_id == 0 {
            return Err(WalError::InvalidArgument(
                "node_id 0 是 raft 保留值（RawNode 会 panic），不能作为成员".into(),
            ));
        }
        let endpoint = endpoint.trim();
        if endpoint.is_empty() {
            return Err(WalError::InvalidArgument(
                "AddMember 必须携带 endpoint：否则其它副本无法连接新成员".into(),
            ));
        }
        let mut conf_change = ConfChange::default();
        conf_change.set_change_type(raft::eraftpb::ConfChangeType::AddNode);
        conf_change.set_node_id(node_id);
        conf_change.set_context(bytes::Bytes::copy_from_slice(endpoint.as_bytes()));
        self.propose_membership(conf_change, trace_id).await
    }

    /// 移除一个 Raft 成员（运维操作）。
    ///
    /// 允许移除本节点（集群缩容的最后一步）：成员变更一旦提交，本节点就不再是成员，
    /// 事件循环会随即停止（见 `RaftGroup::run`）。此时进程应被下线，而不是继续空转。
    pub async fn remove_member(&self, node_id: u64, trace_id: String) -> WalResult<()> {
        self.ensure_leader()?;
        if node_id == 0 {
            return Err(WalError::InvalidArgument("node_id 0 不是合法成员".into()));
        }
        let mut conf_change = ConfChange::default();
        conf_change.set_change_type(raft::eraftpb::ConfChangeType::RemoveNode);
        conf_change.set_node_id(node_id);
        self.propose_membership(conf_change, trace_id).await
    }

    /// 提交成员变更并等待其 apply。
    async fn propose_membership(&self, conf_change: ConfChange, trace_id: String) -> WalResult<()> {
        // 成员变更是运维动作，不携带业务 deadline，统一用 append 超时兜底：
        // 没有超时的等待会让 gRPC 调用永久挂住（与 Append 同样的理由）。
        let wait = self.config.append_timeout();
        let outcome = self
            .propose(
                ProposalPayload::ConfChange(Box::new(conf_change)),
                trace_id,
                wait,
            )
            .await?;
        match outcome.outcome {
            ApplyOutcome::Membership { .. } => Ok(()),
            other => Err(WalError::Internal(format!(
                "成员变更提案返回了非预期结果：{other:?}"
            ))),
        }
    }

    /// 读取区间（从**本节点已 apply** 的状态机读，零拷贝切片）。
    ///
    /// 注意：本节点若是 follower，其 applied 进度可能落后于 leader，
    /// 此时会返回 `WAL_NOT_DURABLE`（retryable）。调用方应重试到 leader，
    /// 或稍后重试（与架构 §11.2 的 replay 语义一致）。
    // 组件内部 API：gRPC 路径走 `read_range_cursor`；本方法供诊断端点与测试使用
    #[allow(dead_code)]
    pub fn read_range(
        &self,
        database_id: &str,
        start_lsn: u64,
        end_lsn: u64,
        max_chunk_bytes: usize,
    ) -> WalResult<Vec<WalChunk>> {
        self.state_machine
            .read()
            .read_range(database_id, start_lsn, end_lsn, max_chunk_bytes)
    }

    /// 读取区间，返回**惰性**分块游标（gRPC 流式路径用）。
    ///
    /// 与 [`WalHandle::read_range`] 共用同一套分块语义，但只做区间校验并把各段的
    /// `Bytes` 切片交给游标，数据在流真正 poll 时才按块取出 —— 服务端不会为了服务一个
    /// 大区间 ReadRange 先把整段数据搬进内存（内存占用 O(单块) 而不是 O(区间)）。
    pub fn read_range_cursor(
        &self,
        database_id: &str,
        start_lsn: u64,
        end_lsn: u64,
        max_chunk_bytes: usize,
    ) -> WalResult<ReadRangeCursor> {
        self.state_machine.read().read_range_cursor(
            database_id,
            start_lsn,
            end_lsn,
            max_chunk_bytes,
        )
    }

    /// 查询 DB 的 WAL 状态。
    pub fn wal_status(&self, database_id: &str) -> WalResult<WalStatus> {
        self.state_machine.read().status(database_id)
    }

    /// 把提案投给 Raft Group，并等待 committed + applied。
    async fn propose(
        &self,
        payload: ProposalPayload,
        trace_id: String,
        wait: Duration,
    ) -> WalResult<ProposalAck> {
        let (reply, receiver) = oneshot::channel();
        let event = GroupEvent::Proposal(Proposal {
            payload,
            trace_id,
            reply,
        });
        self.events
            .send(event)
            .await
            .map_err(|_| WalError::Internal("Raft Group 事件循环已退出".into()))?;

        match tokio::time::timeout(wait, receiver).await {
            Ok(Ok(result)) => result,
            // 事件循环丢弃了 oneshot：进程正在关闭或提案被覆盖
            Ok(Err(_)) => Err(WalError::Internal(
                "提案等待通道被丢弃（Raft Group 已停止）".into(),
            )),
            // 超时 = 未确认 quorum durable：绝不能返回成功（架构 §11.1）
            Err(_) => Err(WalError::AppendTimeout {
                timeout_ms: wait.as_millis() as u64,
                log_index: None,
            }),
        }
    }

    /// 计算等待预算：`WAL_APPEND_TIMEOUT_MS` 与调用方 deadline 取较早者。
    fn wait_budget(&self, deadline_unix_ms: u64) -> Duration {
        let limit = self.config.append_timeout();
        if deadline_unix_ms == 0 {
            return limit;
        }
        let now = crate::time::now_unix_ms();
        let remaining = deadline_unix_ms.saturating_sub(now);
        limit.min(Duration::from_millis(remaining))
    }

    /// 非 leader 一律拒绝写：客户端应换节点重试（proto 契约 WAL_NOT_LEADER）。
    fn ensure_leader(&self) -> WalResult<()> {
        let health = self.status.snapshot();
        if health.is_leader {
            return Ok(());
        }
        Err(WalError::NotLeader {
            shard: self.status.shard_id.clone(),
            leader: (health.leader_id != 0).then_some(health.leader_id),
        })
    }

    /// fencing 预检：命中时直接拒绝，避免把注定失败的命令写进 Raft 日志。
    ///
    /// 这只是**优化**：权威判定在状态机 apply 里（所有副本一致），
    /// 因此预检与 apply 之间的并发 `SetOwnerEpoch` 不会破坏正确性。
    fn precheck_epoch(&self, database_id: &str, owner_epoch: u64) -> WalResult<()> {
        let state_machine = self.state_machine.read();
        if let Ok(status) = state_machine.status(database_id) {
            if owner_epoch < status.owner_epoch {
                return Err(WalError::StaleEpoch {
                    recorded: status.owner_epoch,
                    requested: owner_epoch,
                });
            }
        }
        Ok(())
    }
}

/// 待确认的提案。
struct PendingProposal {
    /// 提案所在 term（用于识别被新 leader 覆盖的日志位置）。
    term: u64,
    /// 结果回调。
    reply: oneshot::Sender<WalResult<ProposalAck>>,
}

/// Raft Group 事件循环。
pub struct RaftGroup {
    node: RawNode<WalStorage>,
    storage: Arc<WalStorage>,
    state_machine: Arc<RwLock<StateMachine>>,
    status: Arc<GroupStatus>,
    config: Arc<WalConfig>,
    peers: PeerRegistry,
    events: mpsc::Receiver<GroupEvent>,
    outbound: mpsc::Sender<(u64, Message)>,
    pending: BTreeMap<u64, PendingProposal>,
    applied_index: u64,
    last_snapshot_index: u64,
    last_snapshot_at: Instant,
    /// 上一次在 `handle_ready` 里观察到的 leader/term 视图（用于检测 leadership 变化）。
    last_leadership: LeadershipView,
    /// 本节点是否已被成员变更移除（缩容）：为 true 时事件循环应停止。
    self_removed: bool,
}

/// 一次 `handle_ready` 后观察到的 leadership 视图。
///
/// 为什么要把「term / leader / 是否 leader」三个值一起记下来：
/// 未决提案只有在**同一任 leader 的同一 term 内**才可能提交。leader 换了、
/// term 变了、或者本节点从 leader 掉成 follower，之前写进日志但尚未提交的提案
/// 都可能永远不会提交，必须立刻回错（否则调用方要等到 deadline 才拿到超时，
/// 白白拖慢 failover）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LeadershipView {
    term: u64,
    leader_id: u64,
    is_leader: bool,
}

impl LeadershipView {
    /// 从 Raft 节点取当前视图。
    fn of(node: &RawNode<WalStorage>) -> Self {
        Self {
            term: node.raft.term,
            leader_id: node.raft.leader_id,
            is_leader: node.raft.state == StateRole::Leader,
        }
    }
}

impl RaftGroup {
    /// 组装 Raft Group（含恢复：状态机快照 + 日志 replay 起点）。
    ///
    /// 事件通道在内部创建：`WalHandle` 必须拿到发送端，而本结构持有接收端，
    /// 两者必须来自同一个 channel（早期实现里分开构造会让句柄指向一个已断开的通道）。
    pub fn new(
        config: Arc<WalConfig>,
        storage: Arc<WalStorage>,
        peers: PeerRegistry,
        outbound: mpsc::Sender<(u64, Message)>,
    ) -> WalResult<(Self, WalHandle)> {
        let (events_tx, events) = mpsc::channel(GROUP_EVENT_CAPACITY);

        // 恢复状态机：优先用快照（缩短 replay），否则从 0 开始 replay 整个日志。
        let (state_machine, applied_index) = match storage.load_state_machine() {
            Some(snapshot) => {
                let applied = snapshot.applied_index;
                (StateMachine::from_snapshot(snapshot), applied)
            }
            None => (StateMachine::new(), 0),
        };
        let persisted_last = storage.persisted_last_index();
        if applied_index > persisted_last {
            // 不可能同时成立：apply 只在日志落盘之后发生。若出现说明数据目录被替换/损坏。
            return Err(WalError::Storage(format!(
                "状态机快照 applied_index={applied_index} 超过本地日志末端 {persisted_last}：数据目录与集群不匹配"
            )));
        }

        let mut raft_config = RaftConfig::new(config.node_id);
        raft_config.election_tick = config.election_tick;
        raft_config.heartbeat_tick = config.heartbeat_tick;
        raft_config.applied = applied_index;
        // 单条 MsgAppend 的最大载荷：WAL 批次通常几十 KB ~ 数 MB，
        // 上限太小会拖慢复制，太大则单条消息占用过多内存。
        raft_config.max_size_per_msg = 1024 * 1024;
        raft_config.max_inflight_msgs = 256;
        // 生产级一致性设置：租约/预投票可以避免分区节点重入时打断稳定 leader。
        raft_config.check_quorum = true;
        raft_config.pre_vote = true;
        raft_config.validate()?;

        // raft 只接受 slog Logger：这里用桥接到 tracing 的 Drain（见 logging.rs），
        // 保证 raft 内部日志与平台其它服务走同一条采集管线。
        let logger = raft_logger(config.node_id, &config.shard_id);
        let node = RawNode::new(&raft_config, storage.as_ref().clone(), &logger)
            .map_err(|err| WalError::Raft(format!("初始化 RawNode 失败：{err}")))?;

        let state_machine = Arc::new(RwLock::new(state_machine));
        let status = Arc::new(GroupStatus::new(config.node_id, config.shard_id.clone()));
        let handle = WalHandle {
            events: events_tx,
            state_machine: state_machine.clone(),
            status: status.clone(),
            config: config.clone(),
        };

        // 在把 node 移进事件循环结构之前先取一次视图（字段求值顺序要求）
        let initial_leadership = LeadershipView::of(&node);
        let group = Self {
            node,
            storage,
            state_machine,
            status,
            config,
            peers,
            events,
            outbound,
            pending: BTreeMap::new(),
            applied_index,
            last_snapshot_index: applied_index,
            last_snapshot_at: Instant::now(),
            last_leadership: initial_leadership,
            self_removed: false,
        };
        Ok((group, handle))
    }
}

impl RaftGroup {
    /// 事件循环主入口。
    pub async fn run(mut self, shutdown: CancellationToken) -> WalResult<()> {
        let mut ticker = tokio::time::interval(self.config.tick_interval());
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut snapshot_ticker = tokio::time::interval(self.config.trim_interval());
        snapshot_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // 主动发起一次选举：3 副本同时启动时不必等满一个 election timeout。
        // 单节点集群会立刻当选，本地测试无需任何额外步骤。
        if let Err(err) = self.node.campaign() {
            warn!(error = %err, "启动选举失败（将由 election timeout 重试）");
        }
        info!(
            node_id = self.config.node_id,
            shard = %self.config.shard_id,
            applied_index = self.applied_index,
            voters = ?self.storage.conf_state().voters,
            "Raft Group 已启动"
        );

        loop {
            // 事件处理完必须重新把 Ready 处理干净，因此这两步在同一个循环里
            while self.node.has_ready() {
                self.handle_ready().await?;
            }

            // 本节点已被成员变更移除：成员表已提交，继续运行没有意义（也不会再被选为 leader）
            if self.self_removed {
                warn!(
                    node_id = self.config.node_id,
                    "本节点已被移出 Raft 集群，停止 Raft Group（应从部署中下线）"
                );
                break;
            }

            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    info!("收到停机信号，Raft Group 退出");
                    break;
                }
                _ = ticker.tick() => {
                    self.node.tick();
                }
                _ = snapshot_ticker.tick() => {
                    self.maybe_snapshot(SnapshotTrigger::Periodic).await;
                }
                event = self.events.recv() => {
                    match event {
                        Some(event) => self.handle_event(event).await,
                        None => {
                            warn!("事件通道已关闭，Raft Group 退出");
                            break;
                        }
                    }
                }
            }
        }

        // 停机前落一次快照：下次启动可以少 replay 一段日志。
        // 用 Shutdown 触发：只要 apply 进度超过上次快照就落盘（见 maybe_snapshot），
        // 不受周期阈值的限制 —— 否则「刚写过快照就停机」会把最后这段进度丢掉。
        self.maybe_snapshot(SnapshotTrigger::Shutdown).await;
        // 停机后所有未决提案都不可能再提交；给它们一个明确的错误，
        // 而不是让调用方等到 deadline 才发现「节点没了」。
        self.fail_all_pending(WalError::Internal("节点正在停机".into()));
        Ok(())
    }

    /// 处理事件。
    async fn handle_event(&mut self, event: GroupEvent) {
        match event {
            GroupEvent::Message(message) => {
                if let Err(err) = self.node.step(*message) {
                    // 旧 term / 未知 peer 的消息属于正常噪声，不应影响本地状态
                    debug!(error = %err, "忽略无法处理的 Raft 消息");
                }
            }
            GroupEvent::PeerUnreachable(node_id) => {
                self.node.report_unreachable(node_id);
            }
            GroupEvent::Proposal(proposal) => self.handle_proposal(proposal),
        }
    }

    /// 提交提案。非 leader 立即回错（WAL_NOT_LEADER）。
    fn handle_proposal(&mut self, proposal: Proposal) {
        if self.node.raft.state != StateRole::Leader {
            let _ = proposal.reply.send(Err(WalError::NotLeader {
                shard: self.config.shard_id.clone(),
                leader: self.known_leader(),
            }));
            return;
        }
        let term = self.node.raft.term;
        let context = proposal.trace_id.clone().into_bytes();
        let (kind, result) = match proposal.payload {
            ProposalPayload::Command(command) => {
                let kind = command.kind_name();
                let data = command.encode();
                (kind, self.node.propose(context, data))
            }
            ProposalPayload::ConfChange(conf_change) => (
                "conf_change",
                self.node.propose_conf_change(context, *conf_change),
            ),
        };

        match result {
            Ok(()) => {
                let index = self.node.raft.raft_log.last_index();
                if let Some(previous) = self.pending.insert(
                    index,
                    PendingProposal {
                        term,
                        reply: proposal.reply,
                    },
                ) {
                    // 同一 index 出现两次提案：只可能发生在索引被覆盖后（换 leader），
                    // 旧的等待者已经失效，必须立即回错而不是让它等到超时。
                    let _ = previous.reply.send(Err(WalError::NotLeader {
                        shard: self.config.shard_id.clone(),
                        leader: self.known_leader(),
                    }));
                }
                debug!(index, term, kind, "提案已写入 Raft 日志");
            }
            Err(err) => {
                // 成员变更的非法输入（成员已存在 / 不存在）属于调用方错误，
                // 不能笼统报内部错误，否则运维无法区分「参数错」与「集群故障」。
                let mapped = match err {
                    raft::Error::ConfChangeError(message) => WalError::InvalidArgument(message),
                    other => other.into(),
                };
                let _ = proposal.reply.send(Err(mapped));
            }
        }
    }

    /// 处理一轮 Ready（可能带 LightReady）。
    async fn handle_ready(&mut self) -> WalResult<()> {
        let mut ready = self.node.ready();
        // 本实现不做快照传输（见 storage.rs），收到快照说明集群里跑着不兼容的版本
        if !ready.snapshot().is_empty() {
            return Err(WalError::Storage(
                "收到 Raft Snapshot，但本服务不支持快照传输（不做日志 compaction）".into(),
            ));
        }

        // ---- 1) 落盘：日志条目 + HardState 必须在发消息与 apply 之前 ----
        let entries = ready.take_entries();
        let hard_state = ready.hs().cloned();
        if !entries.is_empty() || hard_state.is_some() {
            let storage = self.storage.clone();
            let write = tokio::task::spawn_blocking(move || {
                storage.persist_ready(&entries, hard_state.as_ref())
            })
            .await;
            match write {
                Ok(result) => result?,
                Err(err) => return Err(WalError::Internal(format!("持久化任务 panic：{err}"))),
            }
        }

        // ---- 2) 发送消息（落盘之后才允许发出）----
        for message in ready.take_messages() {
            self.send_message(message).await;
        }
        for message in ready.take_persisted_messages() {
            self.send_message(message).await;
        }

        // ---- 3) apply 已提交条目 ----
        self.apply_committed(ready.take_committed_entries()).await?;

        // ---- 4) advance：随后可能产生新的 committed entries / 消息 ----
        let mut light = self.node.advance(ready);
        let messages = light.take_messages();
        let committed = light.take_committed_entries();
        for message in messages {
            self.send_message(message).await;
        }
        self.apply_committed(committed).await?;
        self.node.advance_apply();

        // ---- 5) leadership 变化检查 ----
        // 放在 apply 之后：本批已提交的条目里可能就带着未决提案的成功结果，
        // 先让它们落地，再把**仍然未决**的提案判成 NotLeader。
        self.fail_pending_on_leadership_change();

        self.refresh_status();
        Ok(())
    }

    /// leadership（term / leader / 本节点是否 leader）变化时，立即失败所有未决提案。
    ///
    /// 为什么必须在 `handle_ready` 里做：未决提案只可能在**当前 term、当前 leader**
    /// 下提交。一旦 term 前进、leader 换人、或本节点从 leader 掉成 follower，之前写进
    /// 日志但还没提交的提案就可能永远不会提交 —— 若不在此刻回错，调用方要等到
    /// deadline 才拿到超时（`WAL_NOT_DURABLE`），failover 的恢复路径会被无谓地拖慢；
    /// 而回错成 `WAL_NOT_LEADER` 会让客户端立刻换端点重试（同一 append_id 重试是幂等的）。
    fn fail_pending_on_leadership_change(&mut self) {
        let current = LeadershipView::of(&self.node);
        if current == self.last_leadership {
            return;
        }
        let previous = std::mem::replace(&mut self.last_leadership, current);
        if self.pending.is_empty() {
            return;
        }
        warn!(
            node_id = self.config.node_id,
            previous_term = previous.term,
            term = current.term,
            previous_leader = previous.leader_id,
            leader = current.leader_id,
            was_leader = previous.is_leader,
            pending = self.pending.len(),
            "leadership 发生变化，失败所有未决提案"
        );
        self.fail_all_pending(WalError::NotLeader {
            shard: self.config.shard_id.clone(),
            leader: (current.leader_id != 0).then_some(current.leader_id),
        });
    }

    /// 应用一批已提交条目。
    async fn apply_committed(&mut self, entries: Vec<raft::eraftpb::Entry>) -> WalResult<()> {
        for entry in entries {
            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    let command = match WalCommand::decode(&entry.data) {
                        Ok(command) => command,
                        Err(err) => {
                            // 单条命令解不开不是「可以跳过」的噪声：副本会因此发散。
                            // 这里返回错误终止 Group（触发进程退出告警），绝不静默忽略。
                            error!(
                                index = entry.get_index(),
                                error = %err,
                                "Raft 日志命令解码失败"
                            );
                            return Err(WalError::Internal(format!(
                                "Raft 日志 index={} 命令解码失败：{err}",
                                entry.get_index()
                            )));
                        }
                    };
                    let index = entry.get_index();
                    let term = entry.get_term();
                    let result = {
                        let mut state_machine = self.state_machine.write();
                        state_machine.apply(index, term, &command)
                    };
                    self.applied_index = index;
                    self.resolve_pending(index, term, &result);
                }
                EntryType::EntryConfChange => {
                    let conf_change =
                        match <ConfChange as protobuf::Message>::parse_from_bytes(&entry.data) {
                            Ok(conf_change) => conf_change,
                            Err(err) => {
                                return Err(WalError::Internal(format!(
                                    "成员变更条目解码失败：{err}"
                                )))
                            }
                        };
                    let conf_state = self
                        .node
                        .apply_conf_change(&conf_change)
                        .map_err(|err| WalError::Raft(format!("apply_conf_change 失败：{err}")))?;
                    // 成员表必须立即落盘：否则重启后成员表回退，可能与已提交的配置冲突
                    self.storage.persist_conf_state(&conf_state)?;
                    self.apply_member_endpoint(&conf_change);
                    self.applied_index = entry.get_index();
                    info!(
                        voters = ?conf_state.voters,
                        node_id = conf_change.get_node_id(),
                        "成员变更已生效"
                    );
                    // 唤醒提案方：成员变更是运维操作，调用方必须知道它到底生效没有
                    let outcome = Ok(ApplyOutcome::Membership {
                        voters: conf_state.voters.clone(),
                    });
                    self.resolve_pending(entry.get_index(), entry.get_term(), &outcome);
                }
                other => {
                    // ConfChangeV2 / 未知类型：本服务只提议 ConfChange v1，出现即版本不匹配
                    return Err(WalError::Internal(format!(
                        "不支持的 Raft 日志条目类型：{other:?}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// 唤醒等待该日志索引的提案方。
    fn resolve_pending(&mut self, index: u64, term: u64, result: &WalResult<ApplyOutcome>) {
        let Some(pending) = self.pending.remove(&index) else {
            return;
        };
        if pending.term != term {
            // 该位置被新 leader 的日志覆盖：原提案不可能再提交
            let _ = pending.reply.send(Err(WalError::NotLeader {
                shard: self.config.shard_id.clone(),
                leader: self.known_leader(),
            }));
            return;
        }
        let acked_replicas = if self.node.raft.state == StateRole::Leader {
            self.node
                .raft
                .prs()
                .iter()
                .filter(|(_, progress)| progress.matched >= index)
                .map(|(id, _)| id.to_string())
                .collect()
        } else {
            Vec::new()
        };
        let ack = result.clone().map(|outcome| ProposalAck {
            outcome,
            acked_replicas,
        });
        let _ = pending.reply.send(ack);
    }

    /// 把消息交给 Peer 传输层。
    async fn send_message(&mut self, message: Message) {
        let to = message.get_to();
        if to == 0 || to == self.config.node_id {
            // 发给自己在 raft 内部已短路，走到这里说明是异常消息
            debug!(to, "忽略目标为本节点的 Raft 消息");
            return;
        }
        if let Err(err) = self.outbound.send((to, message)).await {
            warn!(to, error = %err, "Peer 发送队列已关闭，丢弃 Raft 消息（将由重传补偿）");
        }
    }

    /// 从成员变更的 context 里取出 endpoint 并更新 peer 表。
    fn apply_member_endpoint(&mut self, conf_change: &ConfChange) {
        use raft::eraftpb::ConfChangeType;
        let node_id = conf_change.get_node_id();
        match conf_change.get_change_type() {
            ConfChangeType::AddNode | ConfChangeType::AddLearnerNode => {
                if conf_change.context.is_empty() {
                    warn!(node_id, "成员变更未携带 endpoint，后续无法主动连接该节点");
                    return;
                }
                match std::str::from_utf8(&conf_change.context) {
                    Ok(endpoint) => {
                        self.peers.insert(node_id, endpoint.to_owned());
                    }
                    Err(err) => warn!(node_id, error = %err, "成员变更 endpoint 不是合法 UTF-8"),
                }
            }
            ConfChangeType::RemoveNode => {
                self.peers.remove(node_id);
                if node_id == self.config.node_id {
                    // 自己被移除：raft 不会再给我们发消息，本进程应立即停止服务
                    self.self_removed = true;
                }
            }
        }
    }

    /// 刷新对外状态（Health / readyz / metrics 读取）。
    fn refresh_status(&self) {
        // raft::Status 的 leader / term 分别来自 SoftState 与 HardState
        // （Status 本身只有 id / hs / ss / applied / progress 字段）。
        let status = self.node.status();
        let leader_id = status.ss.leader_id;
        let term = status.hs.term;
        self.status
            .is_leader
            .store(self.node.raft.state == StateRole::Leader, Ordering::Release);
        self.status.leader_id.store(leader_id, Ordering::Release);
        self.status.term.store(term, Ordering::Release);
        self.status
            .commit_index
            .store(self.node.raft.raft_log.committed, Ordering::Release);
        self.status
            .applied_index
            .store(self.applied_index, Ordering::Release);
    }

    /// 已写入 Raft 日志但未落盘到 apply 的日志末端（诊断用）。
    fn known_leader(&self) -> Option<u64> {
        let leader = self.status.leader_id.load(Ordering::Acquire);
        (leader != 0).then_some(leader)
    }

    /// 失去 leadership 时，所有未决提案都不再可能提交。
    fn fail_all_pending(&mut self, reason: WalError) {
        for (_, pending) in std::mem::take(&mut self.pending) {
            let _ = pending.reply.send(Err(reason.clone()));
        }
    }

    /// 写状态机快照。
    ///
    /// 触发条件由 [`should_snapshot`] 决定：周期触发需要「时间 + apply 进度」双阈值，
    /// 停机触发只要 apply 进度超过上次快照就写（见函数注释）。
    async fn maybe_snapshot(&mut self, trigger: SnapshotTrigger) {
        if trigger == SnapshotTrigger::Periodic
            && self.last_snapshot_at.elapsed() < self.config.trim_interval()
        {
            // 定时器可能因为调度抖动提前到点：时间阈值未到就不写
            return;
        }
        if !should_snapshot(trigger, self.applied_index, self.last_snapshot_index) {
            return;
        }

        let snapshot = self.state_machine.read().to_snapshot();
        let storage = self.storage.clone();
        let applied = self.applied_index;
        let write =
            tokio::task::spawn_blocking(move || storage.save_state_machine(&snapshot)).await;
        match write {
            Ok(Ok(())) => {
                self.last_snapshot_index = applied;
                self.last_snapshot_at = Instant::now();
                debug!(applied_index = applied, "状态机快照已落盘");
            }
            Ok(Err(err)) => {
                warn!(error = %err, "状态机快照写入失败（不影响正确性，仅影响重启速度）")
            }
            Err(err) => warn!(error = %err, "状态机快照任务 panic"),
        }
    }
}

/// 状态机快照的触发原因。
///
/// 为什么要显式区分而不是一个 `force: bool`：两者的**跳过条件完全不同**。
/// 早期实现把「停机」当成「非强制」，于是停机路径套用了「距离上次快照不足
/// trim_interval 就跳过」的时间阈值 —— 只要进程在两次定时快照之间停机，
/// 最后一次 apply 的进度就永远进不了快照，下次启动必须多 replay 一段日志。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotTrigger {
    /// 周期触发（定时器）：apply 进度与时间双阈值都满足才写，避免高频写盘。
    Periodic,
    /// 停机前的最后一次：只要 apply 进度超过上次快照就写一次。
    Shutdown,
}

/// 是否需要写快照。
///
/// - 没有新进度（`applied_index <= last_snapshot_index`）一律不写；
/// - [`SnapshotTrigger::Shutdown`]：只要比上次快照新就写（停机后不再有机会补写）；
/// - [`SnapshotTrigger::Periodic`]：还需要积累 `SM_SNAPSHOT_APPLY_INTERVAL` 条 apply，
///   否则短时间大量 apply 时等于每条都写一次盘。
fn should_snapshot(trigger: SnapshotTrigger, applied_index: u64, last_snapshot_index: u64) -> bool {
    if applied_index <= last_snapshot_index {
        return false;
    }
    match trigger {
        SnapshotTrigger::Shutdown => true,
        SnapshotTrigger::Periodic => {
            applied_index.saturating_sub(last_snapshot_index) >= SM_SNAPSHOT_APPLY_INTERVAL
        }
    }
}

/// 测试辅助构造器。
///
/// 只服务于**探测类端点**的单元测试（Health / readyz / metrics）：这些端点读的是
/// 句柄里的原子状态与状态机，不需要真的跑起 Raft 事件循环。放在本模块是因为
/// [`WalHandle`] 的字段是私有的，只有本模块能构造出一个「离线句柄」。
#[cfg(test)]
pub mod tests_support {
    use super::*;
    use crate::state_machine::StateMachine;

    /// 构造一个不驱动 Raft 的句柄（状态机为空、无 leader）。
    pub fn offline_handle(config: Arc<WalConfig>) -> WalHandle {
        let (events, _receiver) = mpsc::channel(1);
        WalHandle {
            events,
            state_machine: Arc::new(RwLock::new(StateMachine::new())),
            status: Arc::new(GroupStatus::new(config.node_id, config.shard_id.clone())),
            config,
        }
    }

    /// 构造一个「本节点为 leader」的离线句柄，用于验证 readiness 判定。
    pub fn offline_leader_handle(config: Arc<WalConfig>) -> WalHandle {
        let handle = offline_handle(config);
        handle.status.is_leader.store(true, Ordering::Release);
        handle
            .status
            .leader_id
            .store(handle.config.node_id, Ordering::Release);
        handle.status.term.store(1, Ordering::Release);
        handle
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // 重构后这两个名字不再经 super::* 带入，测试需要显式导入
    use crate::config::ClusterMember;
    use crate::peer::PEER_OUTBOUND_CAPACITY;
    use crate::state_machine::WalStatus;

    fn test_config() -> Arc<WalConfig> {
        Arc::new(WalConfig::default())
    }

    /// 在临时目录上装配一个**不驱动事件循环**的 RaftGroup。
    ///
    /// 只用于直接验证事件循环内部的状态转移（leadership 变化、停机快照），
    /// 不涉及真实的网络与选举 —— 那些由 e2e_tests 覆盖。
    fn test_group(name: &str) -> (tempfile::TempDir, Arc<WalStorage>, RaftGroup) {
        let dir = tempfile::Builder::new()
            .prefix(name)
            .tempdir()
            .expect("创建临时目录");
        let storage =
            Arc::new(WalStorage::open(dir.path(), 1, &[1]).expect("打开 raft-engine 数据目录"));
        let peers = PeerRegistry::from_members(&[ClusterMember {
            node_id: 1,
            endpoint: "127.0.0.1:9201".to_owned(),
        }]);
        let (outbound, _rx) = mpsc::channel(PEER_OUTBOUND_CAPACITY);
        let (group, _handle) = RaftGroup::new(test_config(), storage.clone(), peers, outbound)
            .expect("装配 Raft Group");
        (dir, storage, group)
    }

    #[test]
    fn offline_handle_reports_no_leader() {
        let handle = tests_support::offline_handle(test_config());
        let health = handle.health();
        assert!(!health.is_leader);
        assert_eq!(health.leader_id, 0);
        // 无 leader 时写路径必须在**提案之前**就被拒（WAL_NOT_LEADER）
        let err = futures::executor::block_on(handle.append(AppendParams {
            database_id: "db-1".into(),
            owner_epoch: 1,
            start_lsn: 0,
            wal_file_offset: 0,
            reset_wal: false,
            bytes: vec![1, 2, 3],
            append_id: "ap-1".into(),
            contains_commit_frame: false,
            deadline_unix_ms: 0,
            trace_id: "t-1".into(),
        }))
        .expect_err("非 leader 必须拒绝 Append");
        assert!(matches!(err, WalError::NotLeader { .. }));
        assert_eq!(err.error_code(), domain::error::ErrorCode::WalNotLeader);
    }

    #[test]
    fn empty_state_machine_reports_no_data_for_unknown_db() {
        // 状态查询是只读探测：新建库在首次写入前「没有数据」是合法答案，不是错误。
        // 若这里报 DbNotFound，Worker 的冷启动准备会把新库误判成存储故障。
        let handle = tests_support::offline_handle(test_config());
        let status = handle.wal_status("db-missing").expect("未知 DB 不应报错");
        assert!(!status.has_data);
        assert_eq!(status.last_lsn, 0);
        assert_eq!(status.owner_epoch, 0);
    }

    #[test]
    fn read_range_on_unknown_db_yields_no_data() {
        // 未知 DB = 尚无 WAL 数据。恢复端读一个空区间应当得到空结果。
        //
        // 注意区分：**非空区间**读未知 DB 仍然会失败（durable 末端为 0，请求末端 10 超出），
        // 因此「没有数据」不会被误当成「这段数据是空的」而静默丢数据。
        let handle = tests_support::offline_handle(test_config());
        let empty = handle
            .read_range("db-missing", 0, 0, 1024)
            .expect("空区间读未知 DB 应返回空结果");
        assert!(
            empty.iter().all(|chunk| chunk.data.is_empty()),
            "未知 DB 的空区间不得返回任何数据: {empty:?}"
        );
        let err = handle
            .read_range("db-missing", 0, 10, 1024)
            .expect_err("非空区间读未知 DB 必须失败（不能假装区间为空）");
        assert!(matches!(err, WalError::NotDurable(_)), "实际: {err:?}");
    }

    #[test]
    fn health_snapshot_reflects_leader_state() {
        let handle = tests_support::offline_leader_handle(test_config());
        let health = handle.health();
        assert!(health.is_leader);
        assert_eq!(health.leader_id, 1);
        assert_eq!(health.term, 1);
    }

    #[test]
    fn status_snapshot_is_shared_across_clones() {
        let handle = tests_support::offline_handle(test_config());
        let clone = handle.clone();
        handle.status.leader_id.store(3, Ordering::Release);
        assert_eq!(clone.health().leader_id, 3, "句柄克隆必须共享同一份运行态");
    }

    #[test]
    fn wal_status_type_is_stable() {
        // 契约字段顺序/语义是 gRPC 与 ops 共同依赖的，改动必须显式（编译期守护）
        let status = WalStatus {
            has_data: false,
            first_lsn: 0,
            last_lsn: 0,
            owner_epoch: 0,
            append_count: 0,
        };
        assert!(!status.has_data);
    }

    /// leadership 变化（term 前进 / leader 换人 / 本节点掉下 leader）时必须立刻把
    /// **未决提案**判成 `WAL_NOT_LEADER`，而不是让调用方等到 deadline 才发现。
    #[tokio::test]
    async fn leadership_change_fails_pending_proposals_immediately() {
        let (_dir, _storage, mut group) = test_group("wal-leadership");
        assert!(group.pending.is_empty(), "刚装配的 group 不应有未决提案");

        let (reply, answer) = oneshot::channel();
        group.pending.insert(7, PendingProposal { term: 1, reply });
        // term 前进 = 领导权已换代：旧 term 下写的提案不可能再提交
        group.node.raft.term = 2;
        group.fail_pending_on_leadership_change();

        let error = answer
            .await
            .expect("未决提案必须收到明确错误")
            .expect_err("失去 leadership 后提案不得被当成成功");
        assert!(
            matches!(error, WalError::NotLeader { .. }),
            "必须报 WAL_NOT_LEADER（客户端据此换端点，同一 append_id 重试是幂等的）：{error}"
        );
        assert_eq!(error.error_code(), domain::error::ErrorCode::WalNotLeader);
        assert!(group.pending.is_empty(), "回错后不得残留未决提案");

        // 视图没再变化时不得重复回错（幂等的事件循环）
        let (reply, mut answer) = oneshot::channel();
        group.pending.insert(8, PendingProposal { term: 2, reply });
        group.fail_pending_on_leadership_change();
        assert!(
            group.pending.contains_key(&8),
            "leadership 无变化时不得清空未决提案"
        );
        assert!(
            answer.try_recv().is_err(),
            "未发生 leadership 变化就不该给调用方回错"
        );
    }

    /// 停机快照的触发规则：只要 apply 进度超过上次快照就落一次，不受周期阈值限制。
    #[test]
    fn shutdown_snapshot_ignores_periodic_thresholds() {
        // 没有新进度：任何触发都不写
        assert!(!should_snapshot(SnapshotTrigger::Shutdown, 10, 10));
        assert!(!should_snapshot(SnapshotTrigger::Periodic, 10, 10));
        // 有进度但不足周期阈值：周期触发跳过，停机触发必须写（否则这段进度永远进不了快照）
        assert!(!should_snapshot(SnapshotTrigger::Periodic, 11, 10));
        assert!(should_snapshot(SnapshotTrigger::Shutdown, 11, 10));
        // 远超阈值的周期触发
        assert!(should_snapshot(
            SnapshotTrigger::Periodic,
            10 + SM_SNAPSHOT_APPLY_INTERVAL,
            10
        ));
    }

    /// 停机分支必须真的把快照落盘（不只是纯函数判定）。
    #[tokio::test]
    async fn shutdown_writes_snapshot_when_there_is_progress() {
        let (_dir, storage, mut group) = test_group("wal-shutdown-snapshot");
        assert!(
            storage.load_state_machine().is_none(),
            "初始不应有状态机快照"
        );

        // 模拟「已经 apply 了一批命令，但还没到周期快照的阈值」
        group.applied_index = 3;
        group.maybe_snapshot(SnapshotTrigger::Shutdown).await;

        assert_eq!(group.last_snapshot_index, 3, "快照水位必须跟上 apply 进度");
        assert!(
            storage.load_state_machine().is_some(),
            "停机分支必须把状态机快照写进 raft-engine（下次启动少 replay 一段日志）"
        );

        // 没有新进度时不必重复写
        group.maybe_snapshot(SnapshotTrigger::Shutdown).await;
        assert_eq!(group.last_snapshot_index, 3);
    }
}
