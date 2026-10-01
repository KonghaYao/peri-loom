//! Raft Storage 实现：raft-engine 作为持久化 Raft 日志后端（架构 §17.8）。
//!
//! 为什么自己写 Storage 而不直接用 raft-engine 的封装：
//! raft-engine 只提供「按 Raft Group 分区的日志/KV 读写」，不提供 `raft::Storage`
//! 实现 —— 而 `raft::Storage` 的语义（`entries` 的越界规则、`term(compacted)` 的
//! 返回值、`snapshot()` 的错误类型）直接决定 raft 内部是否走 fatal 分支。
//! 自己实现可以把这些边界写清楚，而不是靠猜测第三方封装的容忍度。
//!
//! 持久化布局（同一 Raft Group 命名空间内）：
//!
//! ```text
//! <raft log entries>            日志条目，键由 raft-engine 内部按 index 维护
//! wal-hard-state   -> HardState vote/term/commit，每次 Ready 与日志一起原子落盘
//! wal-conf-state   -> ConfState  当前成员表，成员变更后立即落盘
//! wal-state-machine-> 状态机快照（重启加速，见 state_machine.rs）
//! ```
//!
//! 崩溃一致性：日志条目与 HardState 必须在**同一个** `LogBatch` 里写入并 fsync，
//! 否则会出现「投过票但没落日志」或「提交了但日志缺失」这类违反 Raft 假设的状态。

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use prost::Message as _;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::{Error as RaftError, GetEntriesContext, RaftState, Storage, StorageError};
use raft_engine::{Config as EngineConfig, Engine, LogBatch, MessageExt, ReadableSize};
use sha2::{Digest, Sha256};

use crate::command::{CONF_STATE_KEY, HARD_STATE_KEY, SM_SNAPSHOT_KEY};
use crate::error::{WalError, WalResult};
use crate::state_machine::StateMachineSnapshot;

/// raft-entry 的 `MessageExt` 适配：告诉 raft-engine 用哪个字段做日志索引。
///
/// 注意这里用的是 rust-protobuf（`raft-proto` 生成的类型实现的是 `protobuf::Message`，
/// 而非 `prost::Message`）：workspace 里 raft 走的是默认 `protobuf-codec`，
/// 强行启用 `prost-codec` 会引入第二套 raft-proto 生成代码并改动 Cargo.lock。
pub struct WalEntry;

impl MessageExt for WalEntry {
    type Entry = Entry;

    fn index(entry: &Entry) -> u64 {
        entry.get_index()
    }
}

/// 由 shard id 派生稳定的 Raft Group ID（同一 shard 重启后必须落在同一个键空间）。
pub fn group_id_from_shard(shard_id: &str) -> u64 {
    let digest = Sha256::digest(shard_id.as_bytes());
    let mut raw = [0u8; 8];
    raw.copy_from_slice(&digest[..8]);
    // 0 在 raft-engine 里是可用的，但留 0 给「未设置」，避免误用
    u64::from_be_bytes(raw) | 1
}

/// 基于 raft-engine 的 Raft Storage。
///
/// 内部状态全部挂在 `Arc` 上：`RawNode<WalStorage>` 持有的是同一个句柄的克隆，
/// 因此 gRPC/Peer 侧读到的 HardState / ConfState 与 Raft 内部看到的一致
/// （若按值克隆，两边会各持一份 RwLock 而静默发散）。
#[derive(Clone)]
pub struct WalStorage {
    inner: Arc<WalStorageInner>,
}

struct WalStorageInner {
    engine: Arc<Engine>,
    group_id: u64,
    /// 最近一次落盘的 HardState（内存镜像，供 `initial_state` 同步读取）。
    hard_state: RwLock<HardState>,
    /// 当前成员表（内存镜像）。
    conf_state: RwLock<ConfState>,
    /// 路径，仅用于日志与诊断。
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)]
    path: String,
}

impl WalStorage {
    /// 打开（或创建）数据目录并恢复 HardState / ConfState。
    ///
    /// `initial_members` 只在**首次启动**（KV 中没有成员表）时使用：已有成员表说明
    /// 集群已经历过成员变更，必须沿用落库的版本，否则会与 Raft 已提交的配置冲突。
    pub fn open(data_dir: &Path, group_id: u64, initial_members: &[u64]) -> WalResult<Self> {
        std::fs::create_dir_all(data_dir)?;
        // WAL 承载的是 commit 关键路径数据，保留足够的 append 队列空间再触发 purge
        let config = EngineConfig {
            dir: data_dir.to_string_lossy().into_owned(),
            purge_threshold: ReadableSize::gb(64),
            ..EngineConfig::default()
        };
        let engine = Engine::open(config).map_err(|err| {
            WalError::Storage(format!(
                "打开 raft-engine（dir={}）失败：{err}",
                data_dir.display()
            ))
        })?;
        let engine = Arc::new(engine);

        let hard_state = engine
            .get_message::<HardState>(group_id, HARD_STATE_KEY)
            .map_err(|err| WalError::Storage(format!("读取 HardState 失败：{err}")))?
            .unwrap_or_default();

        let conf_state = match engine
            .get_message::<ConfState>(group_id, CONF_STATE_KEY)
            .map_err(|err| WalError::Storage(format!("读取 ConfState 失败：{err}")))?
        {
            Some(state) => state,
            None => {
                // 首次启动：用启动参数里的成员表初始化（单节点集群同样走这条路径）
                let mut state = ConfState::default();
                state.set_voters(initial_members.to_vec());
                let mut batch = LogBatch::default();
                batch
                    .put_message(group_id, CONF_STATE_KEY.to_vec(), &state)
                    .map_err(|err| WalError::Storage(format!("写入初始 ConfState 失败：{err}")))?;
                engine.write(&mut batch, true)?;
                state
            }
        };

        if conf_state.voters.is_empty() {
            return Err(WalError::Storage(format!(
                "Raft Group {group_id} 的成员表为空：无法参与选举（检查 WAL_CLUSTER 与数据目录是否匹配）"
            )));
        }

        Ok(Self {
            inner: Arc::new(WalStorageInner {
                engine,
                group_id,
                hard_state: RwLock::new(hard_state),
                conf_state: RwLock::new(conf_state),
                path: data_dir.to_string_lossy().into_owned(),
            }),
        })
    }

    /// Raft Group ID。
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)]
    pub fn group_id(&self) -> u64 {
        self.inner.group_id
    }
    /// 数据目录。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn path(&self) -> &str {
        &self.inner.path
    }

    /// 当前成员表快照。
    pub fn conf_state(&self) -> ConfState {
        self.inner.conf_state.read().clone()
    }

    /// 当前 HardState 快照。
    pub fn hard_state(&self) -> HardState {
        self.inner.hard_state.read().clone()
    }

    /// 已持久化的 commit index。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn persisted_commit(&self) -> u64 {
        self.inner.hard_state.read().get_commit()
    }

    /// 落盘一次 Ready：日志条目 + HardState（原子、同步）。
    ///
    /// Raft 契约要求：必须先持久化日志与 HardState，之后才允许把消息发出去。
    /// 因此本函数返回之前一定已经 fsync 完成（`sync = true`）。
    pub fn persist_ready(
        &self,
        entries: &[Entry],
        hard_state: Option<&HardState>,
    ) -> WalResult<()> {
        if entries.is_empty() && hard_state.is_none() {
            return Ok(());
        }
        let group_id = self.inner.group_id;
        let mut batch = LogBatch::default();
        if !entries.is_empty() {
            batch
                .add_entries::<WalEntry>(group_id, entries)
                .map_err(|err| WalError::Storage(format!("追加日志条目失败：{err}")))?;
        }
        if let Some(hard_state) = hard_state {
            batch
                .put_message(group_id, HARD_STATE_KEY.to_vec(), hard_state)
                .map_err(|err| WalError::Storage(format!("写入 HardState 失败：{err}")))?;
        }
        self.inner.engine.write(&mut batch, true)?;
        if let Some(hard_state) = hard_state {
            *self.inner.hard_state.write() = hard_state.clone();
        }
        Ok(())
    }

    /// 落盘成员表变更（成员变更必须立即持久化，否则重启后成员表会回退）。
    pub fn persist_conf_state(&self, conf_state: &ConfState) -> WalResult<()> {
        let group_id = self.inner.group_id;
        let mut batch = LogBatch::default();
        batch
            .put_message(group_id, CONF_STATE_KEY.to_vec(), conf_state)
            .map_err(|err| WalError::Storage(format!("写入 ConfState 失败：{err}")))?;
        self.inner.engine.write(&mut batch, true)?;
        *self.inner.conf_state.write() = conf_state.clone();
        Ok(())
    }

    /// 读取状态机快照；不存在或损坏都返回 `None`（退化为从 Raft 日志全量 replay）。
    ///
    /// 快照损坏时**不能**直接终止进程：Raft 日志本身是权威数据，丢弃快照后
    /// 从 index 0 replay 仍然可以得到正确状态。这里只记录告警。
    pub fn load_state_machine(&self) -> Option<StateMachineSnapshot> {
        let raw = self
            .inner
            .engine
            .get(self.inner.group_id, SM_SNAPSHOT_KEY)?;
        match StateMachineSnapshot::decode(raw.as_slice()) {
            Ok(snapshot) => Some(snapshot),
            Err(err) => {
                tracing::warn!(
                    group_id = self.inner.group_id,
                    error = %err,
                    "状态机快照解码失败，将退化为从 Raft 日志全量 replay"
                );
                None
            }
        }
    }

    /// 写入状态机快照（同步落盘），用于缩短重启后的 replay 长度。
    ///
    /// 命名与 [`WalStorage::load_state_machine`] 对应：快照的语义是「状态机在某索引的
    /// 一致视图」，不是 Raft 层的 `Snapshot`（本服务不做日志 compaction，见 storage 模块注释）。
    pub fn save_state_machine(&self, snapshot: &StateMachineSnapshot) -> WalResult<()> {
        let mut raw = Vec::with_capacity(snapshot.encoded_len());
        snapshot
            .encode(&mut raw)
            .map_err(|err| WalError::Storage(format!("状态机快照编码失败：{err}")))?;
        let mut batch = LogBatch::default();
        batch
            .put(self.inner.group_id, SM_SNAPSHOT_KEY.to_vec(), raw)
            .map_err(|err| WalError::Storage(format!("写入状态机快照失败：{err}")))?;
        self.inner.engine.write(&mut batch, true)?;
        Ok(())
    }

    /// 已持久化的最早日志索引（无日志时为 1）。
    pub fn persisted_first_index(&self) -> u64 {
        self.inner
            .engine
            .first_index(self.inner.group_id)
            .unwrap_or(1)
    }

    /// 已持久化的最大日志索引（无日志时为 0）。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn persisted_last_index(&self) -> u64 {
        self.inner
            .engine
            .last_index(self.inner.group_id)
            .unwrap_or(0)
    }

    /// 引擎占用字节数（指标用）。
    pub fn used_bytes(&self) -> usize {
        self.inner.engine.get_used_size()
    }
}

impl Storage for WalStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState::new(self.hard_state(), self.conf_state()))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        if high <= low {
            return Ok(Vec::new());
        }
        let last = self.persisted_last_index();
        if high > last + 1 {
            // raft 保证不会越界取（slice 会先按 last_index 收敛），走到这里说明
            // 本地日志与 raft 内部状态不一致：报 Unavailable 而不是返回短数组。
            return Err(RaftError::Store(StorageError::Unavailable));
        }
        let max_size = max_size.into().map(|value| value as usize);
        let mut entries = Vec::with_capacity((high - low) as usize);
        self.inner
            .engine
            .fetch_entries_to::<WalEntry>(self.inner.group_id, low, high, max_size, &mut entries)
            .map_err(|err| match err {
                raft_engine::Error::EntryCompacted => RaftError::Store(StorageError::Compacted),
                raft_engine::Error::EntryNotFound => RaftError::Store(StorageError::Unavailable),
                other => RaftError::Store(StorageError::Other(Box::new(other))),
            })?;
        Ok(entries)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == 0 {
            // raft 的 dummy entry：term 0
            return Ok(0);
        }
        match self
            .inner
            .engine
            .get_entry::<WalEntry>(self.inner.group_id, idx)
        {
            Ok(Some(entry)) => Ok(entry.get_term()),
            Ok(None) => Err(RaftError::Store(StorageError::Unavailable)),
            Err(err) => Err(RaftError::Store(StorageError::Other(Box::new(err)))),
        }
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.persisted_first_index())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.persisted_last_index())
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        // 本服务不做 Raft 日志 compaction（见 storage 模块注释与 REPORT 的偏差说明），
        // 因此永远不需要给落后的副本发送 Snapshot。
        // 若 raft 真的走到这里（说明有成员的日志落后到需要快照），返回
        // `SnapshotTemporarilyUnavailable` 让 raft 稍后重试 —— **绝不能**返回空
        // Snapshot，raft 对 `metadata.index == 0` 会直接 fatal! panic。
        Err(RaftError::Store(
            StorageError::SnapshotTemporarilyUnavailable,
        ))
    }
}

/// 便捷：把 `Bytes` 日志载荷转成 Raft Entry data（`Entry.data` 是 `Bytes`）。
// 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
#[allow(dead_code)]
pub fn entry_data(bytes: &[u8]) -> Bytes {
    Bytes::copy_from_slice(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::{AppendCommand, WalCommand};
    use crate::state_machine::StateMachineSnapshot;

    fn temp_dir(name: &str) -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix(name)
            .tempdir()
            .expect("创建临时目录")
    }

    fn sample_entry(index: u64, term: u64, payload: &[u8]) -> Entry {
        let mut entry = Entry::default();
        entry.set_index(index);
        entry.set_term(term);
        entry.data = Bytes::copy_from_slice(payload);
        entry
    }

    #[test]
    fn group_id_is_stable_and_non_zero() {
        let a = group_id_from_shard("shard-0");
        let b = group_id_from_shard("shard-0");
        let c = group_id_from_shard("shard-1");
        assert_eq!(a, b, "同一 shard 必须映射到同一 Raft Group");
        assert_ne!(a, c, "不同 shard 不得共享键空间");
        assert_ne!(a, 0);
    }

    #[test]
    fn entries_survive_restart() {
        let dir = temp_dir("wal-storage-restart");
        let group = group_id_from_shard("shard-0");
        {
            let storage = WalStorage::open(dir.path(), group, &[1]).unwrap();
            assert_eq!(storage.persisted_last_index(), 0);
            let mut hard_state = HardState::default();
            hard_state.set_term(3);
            hard_state.set_vote(1);
            hard_state.set_commit(2);
            storage
                .persist_ready(
                    &[sample_entry(1, 1, b"first"), sample_entry(2, 3, b"second")],
                    Some(&hard_state),
                )
                .unwrap();
        }
        // 重新打开：日志与 HardState 都必须还在（重启不丢已 commit 的日志）
        let storage = WalStorage::open(dir.path(), group, &[1]).unwrap();
        assert_eq!(storage.persisted_first_index(), 1);
        assert_eq!(storage.persisted_last_index(), 2);
        assert_eq!(storage.persisted_commit(), 2);
        assert_eq!(storage.hard_state().get_term(), 3);
        assert_eq!(storage.hard_state().get_vote(), 1);

        let entries = storage
            .entries(1, 3, None, GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].data.as_ref(), b"first");
        assert_eq!(entries[1].get_term(), 3);
        assert_eq!(storage.term(2).unwrap(), 3);
    }

    #[test]
    fn conf_state_is_written_once_and_reused() {
        let dir = temp_dir("wal-storage-conf");
        let group = group_id_from_shard("shard-0");
        {
            let storage = WalStorage::open(dir.path(), group, &[1, 2, 3]).unwrap();
            assert_eq!(storage.conf_state().voters, vec![1, 2, 3]);
        }
        {
            // 重启时以落库的成员表为准，忽略启动参数（可能已过时）
            let storage = WalStorage::open(dir.path(), group, &[1]).unwrap();
            assert_eq!(storage.conf_state().voters, vec![1, 2, 3]);
            let mut new_conf = ConfState::default();
            new_conf.set_voters(vec![1, 2, 3, 4]);
            storage.persist_conf_state(&new_conf).unwrap();
        }
        let storage = WalStorage::open(dir.path(), group, &[1]).unwrap();
        assert_eq!(storage.conf_state().voters, vec![1, 2, 3, 4]);
    }

    #[test]
    fn entries_out_of_range_reports_unavailable() {
        let dir = temp_dir("wal-storage-range");
        let group = group_id_from_shard("shard-0");
        let storage = WalStorage::open(dir.path(), group, &[1]).unwrap();
        storage
            .persist_ready(&[sample_entry(1, 1, b"x")], None)
            .unwrap();
        let err = storage
            .entries(1, 5, None, GetEntriesContext::empty(false))
            .unwrap_err();
        assert_eq!(err, RaftError::Store(StorageError::Unavailable));
        // 空区间是合法查询
        assert!(storage
            .entries(3, 3, None, GetEntriesContext::empty(false))
            .unwrap()
            .is_empty());
        // dummy entry 的 term 固定为 0（raft 依赖该约定）
        assert_eq!(storage.term(0).unwrap(), 0);
    }

    #[test]
    fn state_machine_snapshot_roundtrip_through_engine() {
        let dir = temp_dir("wal-storage-sm");
        let group = group_id_from_shard("shard-0");
        let storage = WalStorage::open(dir.path(), group, &[1]).unwrap();
        assert!(storage.load_state_machine().is_none());

        let mut sm = crate::state_machine::StateMachine::new();
        sm.apply(
            1,
            1,
            &WalCommand::append(AppendCommand {
                database_id: "db-1".into(),
                owner_epoch: 1,
                start_lsn: 0,
                wal_file_offset: 0,
                reset_wal: false,
                bytes: b"payload".to_vec(),
                append_id: "ap-1".into(),
                contains_commit_frame: true,
            }),
        )
        .unwrap();
        storage.save_state_machine(&sm.to_snapshot()).unwrap();

        let restored = crate::state_machine::StateMachine::from_snapshot(
            storage.load_state_machine().expect("快照必须能读回"),
        );
        assert_eq!(restored.applied_index(), 1);
        assert_eq!(
            restored.read_range("db-1", 0, 7, 1024).unwrap()[0]
                .data
                .as_ref(),
            b"payload"
        );
    }

    #[test]
    fn corrupt_state_machine_snapshot_degrades_to_full_replay() {
        let dir = temp_dir("wal-storage-sm-corrupt");
        let group = group_id_from_shard("shard-0");
        let storage = WalStorage::open(dir.path(), group, &[1]).unwrap();
        // 直接塞一个非法快照：必须降级为 None（从 Raft 日志 replay），而不是 panic
        let mut batch = LogBatch::default();
        batch
            .put(group, SM_SNAPSHOT_KEY.to_vec(), b"not-a-protobuf".to_vec())
            .unwrap();
        storage.inner.engine.write(&mut batch, true).unwrap();
        assert!(storage.load_state_machine().is_none());
    }

    #[test]
    fn empty_conf_state_is_rejected() {
        let dir = temp_dir("wal-storage-empty-conf");
        let group = group_id_from_shard("shard-0");
        // 成员表为空说明配置缺失：必须启动即失败，避免起一个永远选不出 leader 的节点
        assert!(WalStorage::open(dir.path(), group, &[]).is_err());
    }

    #[test]
    fn snapshot_request_is_temporarily_unavailable_not_empty() {
        let dir = temp_dir("wal-storage-snapshot");
        let group = group_id_from_shard("shard-0");
        let storage = WalStorage::open(dir.path(), group, &[1]).unwrap();
        let err = storage.snapshot(1, 2).unwrap_err();
        assert_eq!(
            err,
            RaftError::Store(StorageError::SnapshotTemporarilyUnavailable),
            "返回空 Snapshot 会让 raft 直接 panic"
        );
    }

    #[test]
    fn state_machine_snapshot_type_is_stable() {
        // 快照类型必须能独立编解码（跨进程重启的唯一格式契约）
        let snapshot = StateMachineSnapshot::default();
        let mut buf = Vec::new();
        snapshot.encode(&mut buf).unwrap();
        assert!(StateMachineSnapshot::decode(buf.as_slice()).is_ok());
    }
}
