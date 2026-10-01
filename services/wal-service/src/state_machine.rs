//! WAL Shard 状态机：Raft apply 的唯一目标，也是 `ReadRange` 的唯一数据来源。
//!
//! 状态形状（架构 §17.8）：
//!
//! ```text
//! db_id -> { owner_epoch, segments: [ { start_lsn, end_lsn, file_offset, reset_wal, log_index } ] }
//! ```
//!
//! 关键语义：
//! 1. **epoch 单调**：`SetEpoch` 必须严格递增；`Append` 携带的 epoch 不得旧于已记录值
//!    （架构 §11.3 Storage-level Fencing：旧 Owner 的写入在 WAL Service 内被拒）。
//! 2. **Append 幂等**：同一 `(db, append_id)` 只生效一次，重复提案返回 `deduplicated=true`，
//!    且不得重复推进 LSN（多副本 replay 时同一条命令会各自 apply 一次，必须收敛到同一状态）。
//! 3. **确定性**：本模块**不得**读取时钟、随机数、文件系统或网络；相同命令序列在
//!    3 个副本上必须得到完全相同的内存状态（否则副本发散）。
//!
//! 内存 vs 持久化：状态机把 WAL 字节保存在内存里（`Bytes`，chunk 切片零拷贝）。
//! 数据的**权威持久化**是 Raft 日志（raft-engine，quorum durable）；状态机快照只是
//! 重启加速手段，快照缺失时从 Raft 日志 replay 重建（见 storage.rs / raft_group.rs）。

use std::collections::{HashMap, VecDeque};

use bytes::Bytes;

use crate::command::{wal_command::Kind, AppendCommand, SetEpochCommand, TrimCommand, WalCommand};
use crate::error::{WalError, WalResult};

/// `ReadRange` 默认 chunk 大小（1 MiB）。
pub const DEFAULT_CHUNK_BYTES: usize = 1024 * 1024;

/// `ReadRange` 单 chunk 上限（8 MiB）：客户端可以要求更小，但不能要求更大，
/// 否则一个请求就能让服务端分配超大缓冲。
pub const MAX_CHUNK_BYTES: usize = 8 * 1024 * 1024;

/// 单 DB 幂等键窗口的容量上限。
///
/// 幂等键只需要覆盖「客户端重试窗口」，不需要永久保存；超出上限时按**插入顺序**
/// 逐出最旧的键，保留最近 [`MAX_IDEMPOTENCY_ENTRIES`] 条（见 [`IdempotencyWindow`]）。
const MAX_IDEMPOTENCY_ENTRIES: usize = 4096;

/// 一段连续 WAL 数据（一次 Append 产生一段）。
#[derive(Debug, Clone)]
pub struct Segment {
    /// 本段首字节对应的 LSN。
    pub start_lsn: u64,
    /// 本段末端 LSN（exclusive）。
    pub end_lsn: u64,
    /// 本段首字节在本地 WAL 文件中的偏移。
    pub file_offset: u64,
    /// 本段是否开启新一代 WAL。
    pub reset_wal: bool,
    /// 产生本段的 Raft 日志索引（Trim / 运维溯源用）。
    pub log_index: u64,
    /// WAL 字节（`Bytes` 切片零拷贝）。
    data: Bytes,
}

impl Segment {
    /// 本段字节数。
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)]
    pub fn len(&self) -> u64 {
        self.end_lsn.saturating_sub(self.start_lsn)
    }

    /// 本段是否为空。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn is_empty(&self) -> bool {
        self.end_lsn <= self.start_lsn
    }

    /// 本段数据。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn data(&self) -> &Bytes {
        &self.data
    }
}

/// 单 DB 的幂等键窗口：有界、按插入顺序逐出、可确定性导出。
///
/// 为什么不是一个裸 `HashMap`：
/// 1. **逐出顺序**。窗口满时必须丢弃**最旧**的键（保留最近 N 条），而 `HashMap`
///    不保留插入顺序；旧实现「满了就整表清空」会让窗口内**全部**键同时失去去重能力，
///    被清掉的键再重试就会被判成「覆盖已 durable 区间」而拒绝 —— 明明已经 durable
///    的批次却返回失败（客户端视为终态，不会重试）。
/// 2. **快照可重复构造**。`HashMap` 的迭代顺序取决于实例的随机种子，同一个命令序列在
///    三个副本上会导出**不同字节**的状态机快照；快照必须只由命令序列决定。
///    这里用 `order`（插入顺序 = apply 顺序 = 三副本一致）作为唯一的导出顺序。
///
/// `order` 与 `entries` 的键集合始终一致（`insert` 是唯一写入点）。
#[derive(Debug, Default, Clone)]
struct IdempotencyWindow {
    /// 按插入顺序排列的键，队首最旧（逐出与快照裁剪都以它为准）。
    order: VecDeque<String>,
    /// 幂等键 -> (start_lsn, end_lsn)。
    entries: HashMap<String, (u64, u64)>,
}

impl IdempotencyWindow {
    /// 查询一个幂等键。
    fn get(&self, append_id: &str) -> Option<(u64, u64)> {
        self.entries.get(append_id).copied()
    }

    /// 当前键数。
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.entries.len()
    }

    /// 清空（epoch 换代时调用：上一代 Owner 的键不再有意义）。
    fn clear(&mut self) {
        self.order.clear();
        self.entries.clear();
    }

    /// 记录一个幂等键，返回本次是否发生了窗口逐出（有最旧键被丢弃）。
    fn insert(&mut self, append_id: String, value: (u64, u64)) -> bool {
        if let Some(existing) = self.entries.get_mut(&append_id) {
            // 同键重复插入不改变插入位置（它仍是窗口里的一条），只刷新记录值
            *existing = value;
            return false;
        }
        self.order.push_back(append_id.clone());
        self.entries.insert(append_id, value);

        let mut evicted = false;
        while self.entries.len() > MAX_IDEMPOTENCY_ENTRIES {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.entries.remove(&oldest);
            evicted = true;
        }
        evicted
    }

    /// 按插入顺序取最近 `limit` 条（快照导出用；顺序确定，保证快照可重复构造）。
    fn recent(&self, limit: usize) -> impl Iterator<Item = (&str, (u64, u64))> {
        let skip = self.order.len().saturating_sub(limit);
        self.order.iter().skip(skip).filter_map(move |append_id| {
            self.entries
                .get(append_id)
                .map(|value| (append_id.as_str(), *value))
        })
    }

    /// 从快照恢复：按快照中的顺序重建插入顺序，并继续套用容量上限
    /// （快照来自旧版本 / 被人工篡改时可能超限，不能在恢复路径上失去有界性）。
    fn from_entries(entries: impl IntoIterator<Item = (String, u64, u64)>) -> Self {
        let mut window = Self::default();
        for (append_id, start_lsn, end_lsn) in entries {
            window.insert(append_id, (start_lsn, end_lsn));
        }
        window
    }
}

/// 单 DB 的 WAL 状态。
#[derive(Debug, Default, Clone)]
pub struct DbWalState {
    /// 当前 Owner Epoch。
    pub owner_epoch: u64,
    /// 当前 Owner Worker（仅诊断）。
    pub worker_id: String,
    /// 已 durable 的末端 LSN（exclusive），不因 Trim 回退。
    pub durable_end_lsn: u64,
    /// Trim 水位：此 LSN 之前的数据已被快照覆盖、可丢弃。
    pub trimmed_before_lsn: u64,
    /// 本 DB 累计成功的 Append 次数（幂等命中不计入）。
    pub append_count: u64,
    /// 按 LSN 升序的段列表。
    pub segments: Vec<Segment>,
    /// 最近一次 Append 的幂等键（跨重启保留在快照里，用于尾部重试去重）。
    pub last_append_id: String,
    /// 最近一次 Append 的 start_lsn（与 `last_append_id` 配对校验）。
    pub last_append_start_lsn: u64,
    /// 当前 epoch 内的幂等键（有界、按插入顺序逐出，且**进快照**）。
    appends: IdempotencyWindow,
}

impl DbWalState {
    /// 「尚无 WAL 数据」的默认视图。
    ///
    /// 用于未登记 DB 的只读探测：新建库在首次写入之前不存在状态机条目，
    /// 但读状态/读空区间都是合法请求，返回默认视图即可。
    #[must_use]
    pub fn empty() -> Self {
        Self {
            owner_epoch: 0,
            worker_id: String::new(),
            durable_end_lsn: 0,
            trimmed_before_lsn: 0,
            segments: Vec::new(),
            appends: IdempotencyWindow::default(),
            last_append_id: String::new(),
            last_append_start_lsn: 0,
            append_count: 0,
        }
    }

    /// 是否已经有过数据（用于 `GetWalStatus.has_data`）。
    pub fn has_data(&self) -> bool {
        self.durable_end_lsn > 0 || !self.segments.is_empty()
    }

    /// 当前可读区间起点。
    pub fn first_lsn(&self) -> u64 {
        match self.segments.first() {
            Some(segment) => segment.start_lsn.max(self.trimmed_before_lsn),
            None => self.trimmed_before_lsn,
        }
    }

    /// 当前可读区间末端（exclusive）。
    pub fn last_lsn(&self) -> u64 {
        self.durable_end_lsn
    }

    /// 应用 `SetEpoch`。
    fn apply_set_epoch(&mut self, command: &SetEpochCommand) -> WalResult<ApplyOutcome> {
        if command.owner_epoch < self.owner_epoch {
            // 回退会被旧 Owner 当成「仍然有效」，等价于放行 split-brain 写 —— 必须拒绝。
            return Err(WalError::EpochNotMonotonic {
                recorded: self.owner_epoch,
                requested: command.owner_epoch,
            });
        }
        if command.owner_epoch == self.owner_epoch {
            // 持平 = 同一个 Owner 重复确立所有权，必须幂等成功。
            //
            // 为什么不能报错：Worker 崩溃后用**同一个** epoch 重启该 DB 是最常见的恢复路径
            // （epoch 只在所有权**变更**时递增）。若持平即失败，重启必然卡在「确立所有权」
            // 这一步，DB 永远起不来。
            //
            // 这不削弱 fencing：控制面保证同一时刻只有一个 Worker 持有该 epoch，
            // 而任何**更低** epoch 的写入仍会被 apply_append 拒绝。
            return Ok(ApplyOutcome::SetEpoch {
                applied_epoch: self.owner_epoch,
                known_lsn: self.durable_end_lsn,
            });
        }
        self.owner_epoch = command.owner_epoch;
        self.worker_id = command.worker_id.clone();
        // epoch 变了，上一代 Owner 的幂等键不再有意义（旧 Owner 的下一次 Append
        // 必然被 fencing 拒绝），清空可避免键表无限增长。
        self.appends.clear();
        self.last_append_id.clear();
        self.last_append_start_lsn = 0;
        Ok(ApplyOutcome::SetEpoch {
            applied_epoch: self.owner_epoch,
            known_lsn: self.durable_end_lsn,
        })
    }

    /// 应用 `Append`。
    fn apply_append(&mut self, command: &AppendCommand, log_index: u64) -> WalResult<ApplyOutcome> {
        if command.append_id.is_empty() {
            // 空 append_id 等于「调用方放弃幂等承诺」：一旦 ACK 丢失，重试就会重复写入
            // WAL（甚至撞上「覆盖已 durable 区间」）。客户端的 `append` 已经拦了一道，
            // 服务端必须同样拒绝，否则绕过客户端的写者（其它语言 / 手写 gRPC 客户端）
            // 仍能把不可重试的写送进来。
            return Err(WalError::InvalidArgument(
                "Append 的 append_id 不能为空：它是重试幂等的唯一依据".into(),
            ));
        }
        if command.owner_epoch == 0 {
            return Err(WalError::InvalidArgument(
                "Append 的 owner_epoch 不能为 0：0 表示无 Owner，必须先用 SetOwnerEpoch 确立写者"
                    .into(),
            ));
        }
        if command.owner_epoch < self.owner_epoch {
            // ★ Storage-level Fencing：旧 Owner 的 Append 在此被拒（架构 §11.3）
            return Err(WalError::StaleEpoch {
                recorded: self.owner_epoch,
                requested: command.owner_epoch,
            });
        }
        if command.owner_epoch > self.owner_epoch {
            // proto 契约要求 `epoch >= 已记录` 才接受；更高的 epoch 说明 Control Plane
            // 已经指派了新 Owner，这里前移本地视图并清掉上一代幂等键。
            self.owner_epoch = command.owner_epoch;
            self.worker_id.clear();
            self.appends.clear();
            self.last_append_id.clear();
            self.last_append_start_lsn = 0;
        }

        // --- 幂等：同一 (db, append_id) 只生效一次 ---
        // append_id 非空已在上方校验；这里只需按 (append_id, start_lsn) 判定重复。
        if let Some((recorded_start, _)) = self.appends.get(&command.append_id) {
            if recorded_start != command.start_lsn {
                return Err(WalError::IdempotencyConflict {
                    append_id: command.append_id.clone(),
                    recorded_start_lsn: recorded_start,
                    requested_start_lsn: command.start_lsn,
                });
            }
            return Ok(ApplyOutcome::Append {
                durable_lsn: self.durable_end_lsn,
                deduplicated: true,
                idempotency_window_evicted: false,
            });
        }
        if self.last_append_id == command.append_id {
            if self.last_append_start_lsn != command.start_lsn {
                return Err(WalError::IdempotencyConflict {
                    append_id: command.append_id.clone(),
                    recorded_start_lsn: self.last_append_start_lsn,
                    requested_start_lsn: command.start_lsn,
                });
            }
            return Ok(ApplyOutcome::Append {
                durable_lsn: self.durable_end_lsn,
                deduplicated: true,
                idempotency_window_evicted: false,
            });
        }

        let len = command.bytes.len() as u64;
        let end_lsn = command.start_lsn.saturating_add(len);

        if len > 0 {
            // 覆盖已 durable 数据是绝对不允许的：要么客户端 LSN 算错，要么发生了
            // 本应被 fencing 拦住的重复写。宁可拒绝也不能让持久化数据被改写。
            if command.start_lsn < self.durable_end_lsn {
                return Err(WalError::InvalidArgument(format!(
                    "Append 的 start_lsn={} 落在已 durable 区间（< {}）内，拒绝覆盖",
                    command.start_lsn, self.durable_end_lsn
                )));
            }
            self.segments.push(Segment {
                start_lsn: command.start_lsn,
                end_lsn,
                file_offset: command.wal_file_offset,
                reset_wal: command.reset_wal,
                log_index,
                data: Bytes::copy_from_slice(&command.bytes),
            });
            self.durable_end_lsn = end_lsn;
            self.append_count += 1;
        }

        // 幂等键入窗：窗口满时逐出**最旧的**一条（保留最近 N 条），
        // 而不是整表清空 —— 整表清空会让窗口内所有键同时失去去重能力。
        let idempotency_window_evicted = self
            .appends
            .insert(command.append_id.clone(), (command.start_lsn, end_lsn));
        self.last_append_id = command.append_id.clone();
        self.last_append_start_lsn = command.start_lsn;

        Ok(ApplyOutcome::Append {
            durable_lsn: self.durable_end_lsn,
            deduplicated: false,
            idempotency_window_evicted,
        })
    }

    /// 应用 `Trim`。
    fn apply_trim(&mut self, command: &TrimCommand) -> WalResult<ApplyOutcome> {
        if command.snapshot_id.is_empty() {
            // 没有 snapshot_id 就无法证明该区间已被快照覆盖，误截断会直接丢数据
            return Err(WalError::InvalidArgument(
                "TrimBeforeLsn 必须携带 snapshot_id".into(),
            ));
        }
        // 截断水位不得超过已 durable 末端：超出部分本就不存在，夹到末端即可
        let target = command.before_lsn.min(self.durable_end_lsn);
        if target > self.trimmed_before_lsn {
            self.trimmed_before_lsn = target;
            self.drop_trimmed_segments();
        }
        Ok(ApplyOutcome::Trim {
            trimmed_before_lsn: self.trimmed_before_lsn,
        })
    }

    /// 丢弃水位以下的段，并把跨界段裁剪到水位起点。
    fn drop_trimmed_segments(&mut self) {
        let watermark = self.trimmed_before_lsn;
        // 整段低于水位：直接丢弃
        let keep_from = self
            .segments
            .partition_point(|segment| segment.end_lsn <= watermark);
        if keep_from > 0 {
            self.segments.drain(..keep_from);
        }
        if let Some(segment) = self.segments.first_mut() {
            if segment.start_lsn < watermark && watermark < segment.end_lsn {
                // 跨界段：丢弃前缀字节，同时按丢弃长度推进 file_offset，
                // 保证后续 ReadRange 报出的 file_offset 仍对应本地 WAL 文件真实位置。
                let drop_bytes = (watermark - segment.start_lsn) as usize;
                let remaining = segment.data.slice(drop_bytes..);
                segment.file_offset += drop_bytes as u64;
                segment.start_lsn = watermark;
                segment.data = remaining;
            }
        }
    }

    /// 校验 `[start_lsn, end_lsn)` 并构造惰性分块游标。
    ///
    /// 分两步的理由：区间合法性（Trim 水位 / durable 末端 / 段间空洞）必须**在发出
    /// 第一个 chunk 之前**判定完，否则客户端会先收到一段数据、再收到错误，只能重试
    /// 整个区间；而数据本身不需要预先物化 —— 游标只持有各段的 `Bytes` 切片（引用计数，
    /// 零拷贝），chunk 在 poll 时按块切片产出（见 [`ReadRangeCursor::next_chunk`]）。
    fn read_cursor(
        &self,
        start_lsn: u64,
        end_lsn: u64,
        max_chunk_bytes: usize,
    ) -> WalResult<ReadRangeCursor> {
        let max_chunk = max_chunk_bytes.clamp(1, MAX_CHUNK_BYTES);
        let mut planned = VecDeque::new();

        if end_lsn < start_lsn {
            return Err(WalError::InvalidArgument(format!(
                "ReadRange 区间非法：start_lsn={start_lsn} > end_lsn={end_lsn}"
            )));
        }
        if end_lsn == start_lsn {
            // 空区间是合法请求（例如 base_lsn == durable 末端）：游标会产出一个终止 chunk
            return Ok(ReadRangeCursor::empty(start_lsn, max_chunk));
        }
        if start_lsn < self.trimmed_before_lsn {
            return Err(WalError::RangeTrimmed {
                start_lsn,
                trimmed_before_lsn: self.trimmed_before_lsn,
            });
        }
        if end_lsn > self.durable_end_lsn {
            return Err(WalError::NotDurable(format!(
                "请求末端 {} 超过已 durable 末端 {}",
                end_lsn, self.durable_end_lsn
            )));
        }

        let mut cursor = start_lsn;
        let first_index = self
            .segments
            .partition_point(|segment| segment.end_lsn <= start_lsn);
        for segment in &self.segments[first_index..] {
            if cursor >= end_lsn {
                break;
            }
            if segment.end_lsn <= cursor {
                continue;
            }
            if segment.start_lsn > cursor {
                // 段与段之间存在空洞（例如调用方跳着写 LSN）：绝不静默返回不连续数据
                return Err(WalError::NotDurable(format!(
                    "区间 [{start_lsn}, {end_lsn}) 在 lsn={cursor} 处不连续"
                )));
            }
            // 只保留区间内的部分（头部可能被请求起点裁掉，尾部可能被请求末端裁掉）
            let stop = end_lsn.min(segment.end_lsn);
            let offset_in_segment = (cursor - segment.start_lsn) as usize;
            planned.push_back(PlannedSegment {
                start_lsn: cursor,
                end_lsn: stop,
                file_offset: segment.file_offset + offset_in_segment as u64,
                // 只有区间恰好从段首开始时才继承该段的 reset 语义：
                // 中途开始的回放方不应截断本地 WAL。
                reset_wal: cursor == segment.start_lsn && segment.reset_wal,
                data: segment
                    .data
                    .slice(offset_in_segment..offset_in_segment + (stop - cursor) as usize),
            });
            cursor = stop;
        }
        if cursor < end_lsn {
            return Err(WalError::NotDurable(format!(
                "区间 [{start_lsn}, {end_lsn}) 末端未 durable"
            )));
        }
        Ok(ReadRangeCursor::new(start_lsn, planned, max_chunk))
    }

    /// 读取 `[start_lsn, end_lsn)`，按 `max_chunk_bytes` 分块（一次性收集）。
    ///
    /// 与流式路径共用同一个游标实现（语义只有一处），供不需要流式的调用方与测试使用。
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)]
    fn read_range(
        &self,
        start_lsn: u64,
        end_lsn: u64,
        max_chunk_bytes: usize,
    ) -> WalResult<Vec<WalChunk>> {
        Ok(self
            .read_cursor(start_lsn, end_lsn, max_chunk_bytes)?
            .collect_all())
    }
}

/// 未登记 DB 的只读默认视图（见 `DbWalState::empty` 的说明）。
///
/// 用进程级静态量而不是每次构造临时值：读取路径返回的是 `&DbWalState`，
/// 临时值会在语句结束被释放。
static EMPTY_WAL_STATE: std::sync::LazyLock<DbWalState> =
    std::sync::LazyLock::new(DbWalState::empty);

/// 区间读取计划中的一段：已裁剪到请求区间，数据是状态机内 `Bytes` 的零拷贝切片。
#[derive(Debug, Clone)]
struct PlannedSegment {
    /// 本段（裁剪后）首字节 LSN。
    start_lsn: u64,
    /// 本段（裁剪后）末端 LSN（exclusive）。
    end_lsn: u64,
    /// `start_lsn` 对应的本地 WAL 文件偏移。
    file_offset: u64,
    /// `start_lsn` 是否恰好是一代 WAL 的段首（决定回放方是否截断本地 WAL）。
    reset_wal: bool,
    /// 段数据。
    data: Bytes,
}

/// `ReadRange` 的惰性分块游标。
///
/// 语义（与旧的一次性实现逐字节一致）：
/// - chunk 按 LSN 升序产出，每块最多 `max_chunk_bytes`（已夹到 `[1, MAX_CHUNK_BYTES]`）；
/// - **分块不跨段**：每个 chunk 只携带一个段的 `(file_offset, reset_wal)`，跨段会让
///   回放端无法判断这批字节该写到本地 WAL 的哪个偏移；
/// - 最后一个 chunk 带 `last = true`；空区间产出一个空终止 chunk。
///
/// 数据用 `Bytes` 切片持有，因此「取出游标」不会把整个区间的数据复制或物化到内存里。
#[derive(Debug, Clone)]
pub struct ReadRangeCursor {
    /// 请求区间起点（空区间时用于产出终止 chunk）。
    request_start_lsn: u64,
    /// 已校验连续的待发送段（队首为当前段）。
    segments: VecDeque<PlannedSegment>,
    /// 当前段内已产出的字节数。
    offset_in_segment: u64,
    /// 单块字节上限（已夹到 `[1, MAX_CHUNK_BYTES]`）。
    max_chunk_bytes: usize,
    /// 是否已经产出终止 chunk（含空区间的终止 chunk）。
    finished: bool,
}

impl ReadRangeCursor {
    fn new(
        request_start_lsn: u64,
        segments: VecDeque<PlannedSegment>,
        max_chunk_bytes: usize,
    ) -> Self {
        Self {
            request_start_lsn,
            segments,
            offset_in_segment: 0,
            max_chunk_bytes,
            finished: false,
        }
    }

    /// 空区间游标：只产出一个终止 chunk。
    fn empty(request_start_lsn: u64, max_chunk_bytes: usize) -> Self {
        Self::new(request_start_lsn, VecDeque::new(), max_chunk_bytes)
    }

    /// 产出下一个 chunk；`None` 表示区间已读完。
    pub fn next_chunk(&mut self) -> Option<WalChunk> {
        if self.finished {
            return None;
        }
        let Some(segment) = self.segments.front() else {
            // 空区间：仍然要产出一个终止 chunk，让流式客户端知道「这里就是末端」
            self.finished = true;
            return Some(WalChunk {
                start_lsn: self.request_start_lsn,
                file_offset: 0,
                reset_wal: false,
                data: Bytes::new(),
                last: true,
            });
        };

        let pos = segment.start_lsn + self.offset_in_segment;
        let take = ((segment.end_lsn - pos) as usize).min(self.max_chunk_bytes);
        let offset_in_segment = (pos - segment.start_lsn) as usize;
        let chunk = WalChunk {
            start_lsn: pos,
            // 只有 chunk 恰好落在段首时才继承该段的偏移与 reset 语义
            file_offset: segment.file_offset + (pos - segment.start_lsn),
            reset_wal: pos == segment.start_lsn && segment.reset_wal,
            data: segment
                .data
                .slice(offset_in_segment..offset_in_segment + take),
            last: false,
        };

        let advanced_to = pos + take as u64;
        if advanced_to >= segment.end_lsn {
            self.segments.pop_front();
            self.offset_in_segment = 0;
            if self.segments.is_empty() {
                self.finished = true;
                return Some(WalChunk {
                    last: true,
                    ..chunk
                });
            }
        } else {
            self.offset_in_segment += take as u64;
        }
        Some(chunk)
    }

    /// 是否已产出终止 chunk。
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)]
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// 一次性收集全部 chunk（旧 `read_range` 的返回形态）。
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)]
    fn collect_all(mut self) -> Vec<WalChunk> {
        let mut chunks = Vec::new();
        while let Some(chunk) = self.next_chunk() {
            chunks.push(chunk);
        }
        chunks
    }
}

/// `ReadRange` 返回的一个分块。
#[derive(Debug, Clone)]
pub struct WalChunk {
    /// 本 chunk 首字节对应的 LSN。
    pub start_lsn: u64,
    /// 本 chunk 首字节在本地 WAL 文件中的偏移。
    pub file_offset: u64,
    /// 本 chunk 是否开启新一代 WAL。
    pub reset_wal: bool,
    /// 数据（段内切片，零拷贝）。
    pub data: Bytes,
    /// 是否为本次读取的最后一个 chunk。
    pub last: bool,
}

/// apply 成功后的结果（等待方据此填充 gRPC 响应）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// `SetEpoch` 生效。
    SetEpoch {
        /// 生效后的 epoch。
        applied_epoch: u64,
        /// 该 DB 已知的 durable 末端 LSN。
        known_lsn: u64,
    },
    /// `Append` 生效（或幂等命中）。
    Append {
        /// 已 durable 的末端 LSN（exclusive）。
        durable_lsn: u64,
        /// 是否幂等命中（此前已提交过）。
        deduplicated: bool,
        /// 本次 Append 是否触发幂等窗口逐出（最旧的键被丢弃）。
        ///
        /// 窗口逐出会削弱极长重试链的去重能力，因此不能静默发生：调用方据此打点 /
        /// 告警（proto 的 `AppendResponse` 没有承载该标记的字段，故不跨 wire 传递）。
        idempotency_window_evicted: bool,
    },
    /// `Trim` 生效。
    Trim {
        /// 截断后的水位。
        trimmed_before_lsn: u64,
    },
    /// Raft 成员变更生效。
    ///
    /// 成员变更由 Raft 配置层处理（**不进入**本状态机，也不改变任何 DB 的 WAL 状态）；
    /// 这里复用一个返回类型，是为了让 AddMember / RemoveMember 与其它提案共享同一条
    /// 「提案 -> apply -> 唤醒等待方」的路径，避免成员变更另起一套状态跟踪。
    Membership {
        /// 生效后的 voter 列表。
        voters: Vec<u64>,
    },
}

/// `GetWalStatus` 的查询结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalStatus {
    /// 是否已有数据。
    pub has_data: bool,
    /// 可读区间起点。
    pub first_lsn: u64,
    /// 已 durable 末端（exclusive）。
    pub last_lsn: u64,
    /// 当前 owner epoch。
    pub owner_epoch: u64,
    /// 累计 Append 次数。
    pub append_count: u64,
}

/// WAL Shard 状态机。
#[derive(Debug, Default, Clone)]
pub struct StateMachine {
    dbs: HashMap<String, DbWalState>,
    applied_index: u64,
    applied_term: u64,
    apply_count: u64,
}

impl StateMachine {
    /// 空状态机（新 shard）。
    pub fn new() -> Self {
        Self::default()
    }

    /// 已 apply 的 Raft 日志索引（重启时作为 `Config.applied`）。
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)]
    pub fn applied_index(&self) -> u64 {
        self.applied_index
    }

    /// 已 apply 的 Raft 日志 term。
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    #[allow(dead_code)]
    pub fn applied_term(&self) -> u64 {
        self.applied_term
    }

    /// 累计 apply 命令数。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn apply_count(&self) -> u64 {
        self.apply_count
    }

    /// 当前承载的 DB 数量。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn database_count(&self) -> usize {
        self.dbs.len()
    }

    /// 应用一条 Raft 日志命令。
    ///
    /// 无论成功还是被拒（fencing / 参数非法），**推进 apply 进度**都是确定性的：
    /// 三个副本必须对「第 N 条日志命令」做同样的事，因此拒绝也要落在状态机里
    /// （调用方从命令是否被提交的角度看是成功的，只是业务结果被拒）。
    pub fn apply(
        &mut self,
        index: u64,
        term: u64,
        command: &WalCommand,
    ) -> WalResult<ApplyOutcome> {
        self.applied_index = index;
        self.applied_term = term;
        self.apply_count += 1;

        let Some(kind) = &command.kind else {
            // 空命令：允许（旧版本 / 未来的 no-op 探活），对状态无副作用
            return Ok(ApplyOutcome::Trim {
                trimmed_before_lsn: 0,
            });
        };

        match kind {
            Kind::SetEpoch(set) => {
                if set.database_id.is_empty() {
                    return Err(WalError::InvalidArgument(
                        "SetOwnerEpoch 缺少 database_id".into(),
                    ));
                }
                let state = self.dbs.entry(set.database_id.clone()).or_default();
                state.apply_set_epoch(set)
            }
            Kind::Append(append) => {
                if append.database_id.is_empty() {
                    return Err(WalError::InvalidArgument("Append 缺少 database_id".into()));
                }
                let state = self.dbs.entry(append.database_id.clone()).or_default();
                state.apply_append(append, index)
            }
            Kind::Trim(trim) => {
                if trim.database_id.is_empty() {
                    return Err(WalError::InvalidArgument("Trim 缺少 database_id".into()));
                }
                let Some(state) = self.dbs.get_mut(&trim.database_id) else {
                    return Err(WalError::DbNotFound(trim.database_id.clone()));
                };
                state.apply_trim(trim)
            }
        }
    }

    /// 查询某 DB 的 WAL 状态。
    pub fn status(&self, database_id: &str) -> WalResult<WalStatus> {
        // 未登记的 DB 返回「尚无数据」而不是错误：状态查询是只读探测，
        // 一个刚创建、还没写过任何 WAL 的库本来就该是这个答案。
        // （若这里报 DbNotFound，Worker 的冷启动准备会把新建库误判成存储故障。）
        let Some(state) = self.dbs.get(database_id) else {
            return Ok(WalStatus {
                has_data: false,
                first_lsn: 0,
                last_lsn: 0,
                owner_epoch: 0,
                append_count: 0,
            });
        };
        Ok(WalStatus {
            has_data: state.has_data(),
            first_lsn: state.first_lsn(),
            last_lsn: state.last_lsn(),
            owner_epoch: state.owner_epoch,
            append_count: state.append_count,
        })
    }

    /// 读取 `[start_lsn, end_lsn)`，一次性返回全部 chunk。
    // 组件内部 API：gRPC 流式路径走 `read_range_cursor`；本方法供诊断端点与测试使用
    #[allow(dead_code)]
    pub fn read_range(
        &self,
        database_id: &str,
        start_lsn: u64,
        end_lsn: u64,
        max_chunk_bytes: usize,
    ) -> WalResult<Vec<WalChunk>> {
        let state = self
            .dbs
            .get(database_id)
            // 未登记的 DB = 还没有任何 WAL 数据。恢复端读一个空区间是合法请求，
            // 返回空结果；把它当成 DbNotFound 会让新建库的冷启动直接失败。
            .unwrap_or(&EMPTY_WAL_STATE);
        state.read_range(start_lsn, end_lsn, max_chunk_bytes)
    }

    /// 读取 `[start_lsn, end_lsn)`，返回**惰性**分块游标（gRPC 流式路径用）。
    ///
    /// 与 [`StateMachine::read_range`] 共用同一套分块语义，区别只是 chunk 在
    /// [`ReadRangeCursor::next_chunk`] 时才被切出来，因此服务端不需要先把整个区间的
    /// 数据物化到内存里（大区间 ReadRange 的内存占用从 O(区间) 降到 O(单块)）。
    pub fn read_range_cursor(
        &self,
        database_id: &str,
        start_lsn: u64,
        end_lsn: u64,
        max_chunk_bytes: usize,
    ) -> WalResult<ReadRangeCursor> {
        let state = self
            .dbs
            .get(database_id)
            // 未登记的 DB = 还没有任何 WAL 数据。恢复端读一个空区间是合法请求，
            // 返回空结果；把它当成 DbNotFound 会让新建库的冷启动直接失败。
            .unwrap_or(&EMPTY_WAL_STATE);
        state.read_cursor(start_lsn, end_lsn, max_chunk_bytes)
    }

    /// 导出快照（写入 raft-engine KV，用于缩短重启 replay）。
    pub fn to_snapshot(&self) -> StateMachineSnapshot {
        StateMachineSnapshot {
            applied_index: self.applied_index,
            applied_term: self.applied_term,
            apply_count: self.apply_count,
            databases: self
                .dbs
                .iter()
                .map(|(id, state)| DbSnapshot {
                    database_id: id.clone(),
                    owner_epoch: state.owner_epoch,
                    worker_id: state.worker_id.clone(),
                    durable_end_lsn: state.durable_end_lsn,
                    trimmed_before_lsn: state.trimmed_before_lsn,
                    append_count: state.append_count,
                    last_append_id: state.last_append_id.clone(),
                    last_append_start_lsn: state.last_append_start_lsn,
                    // ★ 幂等键窗口必须进快照：否则「快照 + 日志 replay」的重启路径会丢掉
                    // 键表，重启后任何非最后一条 append_id 的重试都会被下方的
                    // 「覆盖已 durable 区间」判定拒绝（INVALID_ARGUMENT，客户端视为终态、
                    // 不再重试），而该批次其实早已 durable。
                    // 导出顺序 = 插入顺序（三副本一致），并按上限裁剪最新的 N 条，
                    // 保证同一个命令序列导出的快照字节完全相同。
                    appends: state
                        .appends
                        .recent(MAX_IDEMPOTENCY_ENTRIES)
                        .map(
                            |(append_id, (start_lsn, end_lsn))| IdempotencyEntrySnapshot {
                                append_id: append_id.to_owned(),
                                start_lsn,
                                end_lsn,
                            },
                        )
                        .collect(),
                    segments: state
                        .segments
                        .iter()
                        .map(|segment| SegmentSnapshot {
                            start_lsn: segment.start_lsn,
                            end_lsn: segment.end_lsn,
                            file_offset: segment.file_offset,
                            reset_wal: segment.reset_wal,
                            log_index: segment.log_index,
                            data: segment.data.to_vec(),
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    /// 从快照恢复。快照损坏（protobuf 解析失败）由调用方决定是报错终止还是退回全量 replay。
    pub fn from_snapshot(snapshot: StateMachineSnapshot) -> Self {
        let mut dbs = HashMap::with_capacity(snapshot.databases.len());
        for db in snapshot.databases {
            let mut state = DbWalState {
                owner_epoch: db.owner_epoch,
                worker_id: db.worker_id,
                durable_end_lsn: db.durable_end_lsn,
                trimmed_before_lsn: db.trimmed_before_lsn,
                append_count: db.append_count,
                last_append_id: db.last_append_id,
                last_append_start_lsn: db.last_append_start_lsn,
                segments: Vec::with_capacity(db.segments.len()),
                // 键表按快照中的顺序重建（快照顺序 = 原插入顺序），继续套用容量上限
                appends: IdempotencyWindow::from_entries(
                    db.appends
                        .into_iter()
                        .map(|entry| (entry.append_id, entry.start_lsn, entry.end_lsn)),
                ),
            };
            state.segments = db
                .segments
                .into_iter()
                .map(|segment| Segment {
                    start_lsn: segment.start_lsn,
                    end_lsn: segment.end_lsn,
                    file_offset: segment.file_offset,
                    reset_wal: segment.reset_wal,
                    log_index: segment.log_index,
                    data: Bytes::from(segment.data),
                })
                .collect();
            dbs.insert(db.database_id, state);
        }
        Self {
            dbs,
            applied_index: snapshot.applied_index,
            applied_term: snapshot.applied_term,
            apply_count: snapshot.apply_count,
        }
    }
}

// ---------------------------------------------------------------- 快照编码
//
// 与 command.rs 同理：状态机快照是 WAL Service 内部结构，不进对外 proto 契约。
// 用 prost 手写结构体拿到紧凑二进制，避免 JSON/base64 带来的体积与性能开销。

/// 状态机快照。
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct StateMachineSnapshot {
    /// 快照对应的 Raft 日志索引（重启时作为 `Config.applied`）。
    #[prost(uint64, tag = "1")]
    pub applied_index: u64,
    /// 快照对应的 Raft 日志 term。
    #[prost(uint64, tag = "2")]
    pub applied_term: u64,
    /// 累计 apply 命令数。
    #[prost(uint64, tag = "3")]
    pub apply_count: u64,
    /// 各 DB 的快照。
    #[prost(message, repeated, tag = "4")]
    pub databases: Vec<DbSnapshot>,
}

/// 单 DB 快照。
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct DbSnapshot {
    /// DB 标识。
    #[prost(string, tag = "1")]
    pub database_id: String,
    /// owner epoch。
    #[prost(uint64, tag = "2")]
    pub owner_epoch: u64,
    /// owner worker（诊断）。
    #[prost(string, tag = "3")]
    pub worker_id: String,
    /// durable 末端 LSN。
    #[prost(uint64, tag = "4")]
    pub durable_end_lsn: u64,
    /// Trim 水位。
    #[prost(uint64, tag = "5")]
    pub trimmed_before_lsn: u64,
    /// 累计 Append 次数。
    #[prost(uint64, tag = "6")]
    pub append_count: u64,
    /// 最近一次 Append 的幂等键（跨重启保留尾部去重能力）。
    #[prost(string, tag = "7")]
    pub last_append_id: String,
    /// 最近一次 Append 的 start_lsn。
    #[prost(uint64, tag = "8")]
    pub last_append_start_lsn: u64,
    /// 段列表。
    #[prost(message, repeated, tag = "9")]
    pub segments: Vec<SegmentSnapshot>,
    /// 幂等键窗口（按插入顺序，最多 `MAX_IDEMPOTENCY_ENTRIES` 条）。
    ///
    /// 缺失（旧版本快照 / 字段未写）时退化为空窗口，此时尾部重试仍由
    /// `last_append_id` 兜底；非尾部重试只能依赖 Raft 日志 replay 重建键表。
    #[prost(message, repeated, tag = "10")]
    pub appends: Vec<IdempotencyEntrySnapshot>,
}

/// 单条幂等键快照。
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct IdempotencyEntrySnapshot {
    /// 客户端幂等键。
    #[prost(string, tag = "1")]
    pub append_id: String,
    /// 首次使用该键时的 start_lsn。
    #[prost(uint64, tag = "2")]
    pub start_lsn: u64,
    /// 该次 Append 的末端 LSN（exclusive）。
    #[prost(uint64, tag = "3")]
    pub end_lsn: u64,
}

/// 单段快照。
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct SegmentSnapshot {
    /// 段起点 LSN。
    #[prost(uint64, tag = "1")]
    pub start_lsn: u64,
    /// 段末端 LSN（exclusive）。
    #[prost(uint64, tag = "2")]
    pub end_lsn: u64,
    /// 段首字节在本地 WAL 文件中的偏移。
    #[prost(uint64, tag = "3")]
    pub file_offset: u64,
    /// 是否开启新一代 WAL。
    #[prost(bool, tag = "4")]
    pub reset_wal: bool,
    /// 产生该段的 Raft 日志索引。
    #[prost(uint64, tag = "5")]
    pub log_index: u64,
    /// 段数据。
    #[prost(bytes = "vec", tag = "6")]
    pub data: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message as _;

    fn set_epoch(db: &str, epoch: u64) -> WalCommand {
        WalCommand::set_epoch(SetEpochCommand {
            database_id: db.to_owned(),
            owner_epoch: epoch,
            worker_id: "worker-1".to_owned(),
            reason: "test".to_owned(),
        })
    }

    fn append(db: &str, epoch: u64, start_lsn: u64, offset: u64, bytes: &[u8]) -> WalCommand {
        WalCommand::append(AppendCommand {
            database_id: db.to_owned(),
            owner_epoch: epoch,
            start_lsn,
            wal_file_offset: offset,
            reset_wal: false,
            bytes: bytes.to_vec(),
            append_id: format!("ap-{start_lsn}"),
            contains_commit_frame: false,
        })
    }

    fn append_with_id(
        db: &str,
        epoch: u64,
        start_lsn: u64,
        offset: u64,
        bytes: &[u8],
        append_id: &str,
    ) -> WalCommand {
        WalCommand::append(AppendCommand {
            database_id: db.to_owned(),
            owner_epoch: epoch,
            start_lsn,
            wal_file_offset: offset,
            reset_wal: false,
            bytes: bytes.to_vec(),
            append_id: append_id.to_owned(),
            contains_commit_frame: false,
        })
    }

    fn collect(sm: &StateMachine, db: &str, start: u64, end: u64, chunk: usize) -> Vec<u8> {
        let mut out = Vec::new();
        for item in sm.read_range(db, start, end, chunk).unwrap() {
            out.extend_from_slice(&item.data);
        }
        out
    }

    #[test]
    fn set_epoch_is_monotonic_and_idempotent() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 5)).unwrap();

        // 持平 = 同一个 Owner 重复确立所有权 -> 幂等成功。
        // Worker 崩溃后用同一个 epoch 重启该 DB 是最常见的恢复路径，
        // 若持平即失败，重启会卡在「确立所有权」这一步，DB 永远起不来。
        let same = sm.apply(2, 1, &set_epoch("db-1", 5)).unwrap();
        assert!(matches!(
            same,
            ApplyOutcome::SetEpoch {
                applied_epoch: 5,
                ..
            }
        ));

        // 回退仍然必须拒绝（否则旧 Owner 会被当成“仍然有效”，等于放行 split-brain 写）
        let outcome = sm.apply(3, 1, &set_epoch("db-1", 3)).unwrap_err();
        assert!(matches!(
            outcome,
            WalError::EpochNotMonotonic {
                recorded: 5,
                requested: 3
            }
        ));
        // 被拒的命令仍然推进 apply 进度（确定性要求）
        assert_eq!(sm.applied_index(), 3);
        sm.apply(4, 1, &set_epoch("db-1", 6)).unwrap();
        assert_eq!(sm.status("db-1").unwrap().owner_epoch, 6);
    }

    #[test]
    fn append_with_stale_epoch_is_fenced() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 7)).unwrap();
        sm.apply(2, 1, &append("db-1", 7, 0, 0, b"hello")).unwrap();

        // 旧 Owner（epoch 6）不得再写入：Storage-level Fencing
        let outcome = sm
            .apply(3, 1, &append("db-1", 6, 5, 5, b"stale"))
            .unwrap_err();
        assert!(matches!(
            outcome,
            WalError::StaleEpoch {
                recorded: 7,
                requested: 6
            }
        ));
        // 拒绝必须无副作用：数据长度与内容都不变
        assert_eq!(sm.status("db-1").unwrap().last_lsn, 5);
        assert_eq!(collect(&sm, "db-1", 0, 5, 1024), b"hello");
    }

    #[test]
    fn append_requires_non_zero_epoch() {
        let mut sm = StateMachine::new();
        let outcome = sm.apply(1, 1, &append("db-1", 0, 0, 0, b"x")).unwrap_err();
        assert!(matches!(outcome, WalError::InvalidArgument(_)));
        assert_eq!(sm.status("db-1").unwrap().last_lsn, 0);
    }

    #[test]
    fn append_is_idempotent_per_append_id() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();
        let first = sm
            .apply(2, 1, &append_with_id("db-1", 1, 0, 0, b"abc", "ap-x"))
            .unwrap();
        assert_eq!(
            first,
            ApplyOutcome::Append {
                durable_lsn: 3,
                deduplicated: false,
                idempotency_window_evicted: false
            }
        );

        // 同一条命令（Raft 重放 / 客户端重试）必须只生效一次
        let second = sm
            .apply(3, 1, &append_with_id("db-1", 1, 0, 0, b"abc", "ap-x"))
            .unwrap();
        assert_eq!(
            second,
            ApplyOutcome::Append {
                durable_lsn: 3,
                deduplicated: true,
                idempotency_window_evicted: false
            }
        );
        assert_eq!(
            sm.status("db-1").unwrap().last_lsn,
            3,
            "重复提案不得推进 LSN"
        );
        assert_eq!(sm.status("db-1").unwrap().append_count, 1);
    }

    #[test]
    fn idempotency_survives_other_appends_in_between() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();
        sm.apply(2, 1, &append_with_id("db-1", 1, 0, 0, b"aa", "ap-1"))
            .unwrap();
        sm.apply(3, 1, &append_with_id("db-1", 1, 2, 2, b"bb", "ap-2"))
            .unwrap();
        // 迟到的重试（乱序到达）仍然被去重，并返回当前 durable 末端
        let out = sm
            .apply(4, 1, &append_with_id("db-1", 1, 0, 0, b"aa", "ap-1"))
            .unwrap();
        assert_eq!(
            out,
            ApplyOutcome::Append {
                durable_lsn: 4,
                deduplicated: true,
                idempotency_window_evicted: false
            }
        );
        assert_eq!(collect(&sm, "db-1", 0, 4, 1024), b"aabb");
    }

    #[test]
    fn reused_append_id_with_different_lsn_is_rejected() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();
        sm.apply(2, 1, &append_with_id("db-1", 1, 0, 0, b"aa", "ap-1"))
            .unwrap();
        let err = sm
            .apply(3, 1, &append_with_id("db-1", 1, 8, 8, b"bb", "ap-1"))
            .unwrap_err();
        assert!(matches!(err, WalError::IdempotencyConflict { .. }));
    }

    #[test]
    fn empty_append_id_is_rejected() {
        // 没有幂等键的写请求无法安全重试：ACK 丢失后的重试会重复写入 WAL
        // （甚至撞上「覆盖已 durable 区间」）。服务端必须与客户端一致地拒绝它，
        // 而不是悄悄按「不去重」处理 —— 那会让调用方以为重试是安全的。
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();
        let err = sm
            .apply(2, 1, &append_with_id("db-1", 1, 0, 0, b"aa", ""))
            .unwrap_err();
        assert!(matches!(err, WalError::InvalidArgument(_)), "{err}");
        // 拒绝必须无副作用：没有写入任何字节，也没有推进 durable 末端
        assert_eq!(sm.status("db-1").unwrap().last_lsn, 0);
        assert_eq!(sm.status("db-1").unwrap().append_count, 0);
    }

    #[test]
    fn overlapping_append_is_rejected() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();
        sm.apply(2, 1, &append("db-1", 1, 0, 0, b"hello")).unwrap();
        // start_lsn 回退到已 durable 区间内：绝不允许覆盖
        let err = sm
            .apply(3, 1, &append_with_id("db-1", 1, 3, 3, b"xx", "ap-over"))
            .unwrap_err();
        assert!(matches!(err, WalError::InvalidArgument(_)));
        assert_eq!(collect(&sm, "db-1", 0, 5, 1024), b"hello");
    }

    #[test]
    fn append_with_higher_epoch_promotes_and_resets_idempotency() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();
        sm.apply(2, 1, &append_with_id("db-1", 1, 0, 0, b"aa", "ap-1"))
            .unwrap();
        // proto 契约要求 epoch >= 已记录即接受；更高的 epoch 意味着 Control Plane 已换 Owner
        let out = sm
            .apply(3, 1, &append_with_id("db-1", 2, 2, 2, b"bb", "ap-1"))
            .unwrap();
        assert_eq!(
            out,
            ApplyOutcome::Append {
                durable_lsn: 4,
                deduplicated: false,
                idempotency_window_evicted: false
            },
            "epoch 切换后旧幂等键必须失效"
        );
        assert_eq!(sm.status("db-1").unwrap().owner_epoch, 2);
    }

    #[test]
    fn trim_drops_data_and_affects_read_range() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();
        sm.apply(2, 1, &append("db-1", 1, 0, 0, b"0123456789"))
            .unwrap();
        sm.apply(3, 1, &append("db-1", 1, 10, 10, b"abcdefghij"))
            .unwrap();

        let trim = WalCommand::trim(TrimCommand {
            database_id: "db-1".into(),
            before_lsn: 4,
            snapshot_id: "snap-1".into(),
        });
        let out = sm.apply(4, 1, &trim).unwrap();
        assert_eq!(
            out,
            ApplyOutcome::Trim {
                trimmed_before_lsn: 4
            }
        );

        let status = sm.status("db-1").unwrap();
        assert_eq!(status.first_lsn, 4);
        assert_eq!(status.last_lsn, 20);
        assert_eq!(collect(&sm, "db-1", 4, 20, 1024), b"456789abcdefghij");

        // 水位以下的数据已丢弃：必须报错而不是静默返回短数据
        let err = sm.read_range("db-1", 0, 20, 1024).unwrap_err();
        assert!(matches!(err, WalError::RangeTrimmed { .. }));
    }

    #[test]
    fn trim_requires_snapshot_id() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();
        sm.apply(2, 1, &append("db-1", 1, 0, 0, b"0123")).unwrap();
        let trim = WalCommand::trim(TrimCommand {
            database_id: "db-1".into(),
            before_lsn: 2,
            snapshot_id: String::new(),
        });
        assert!(matches!(
            sm.apply(3, 1, &trim).unwrap_err(),
            WalError::InvalidArgument(_)
        ));
        assert_eq!(sm.status("db-1").unwrap().first_lsn, 0);
    }

    #[test]
    fn trim_unknown_db_is_rejected() {
        let mut sm = StateMachine::new();
        let trim = WalCommand::trim(TrimCommand {
            database_id: "db-x".into(),
            before_lsn: 2,
            snapshot_id: "snap-1".into(),
        });
        assert!(matches!(
            sm.apply(1, 1, &trim).unwrap_err(),
            WalError::DbNotFound(_)
        ));
    }

    #[test]
    fn read_range_is_ordered_and_chunked() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();
        sm.apply(2, 1, &append("db-1", 1, 0, 0, b"aaaa")).unwrap();
        sm.apply(3, 1, &append("db-1", 1, 4, 4, b"bbbb")).unwrap();
        let chunks = sm.read_range("db-1", 0, 8, 3).unwrap();
        // 3 字节一分块，且**分块不跨段**：每个 chunk 只携带一个 (file_offset, reset_wal)，
        // 跨段会让回放端无法判断这批字节该写到本地 WAL 的哪个偏移，语义会歧义。
        // 因此 aaaa|bbbb -> [aaa][a] + [bbb][b]
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0].start_lsn, 0);
        assert_eq!(chunks[0].data.as_ref(), b"aaa");
        assert_eq!(chunks[1].start_lsn, 3);
        assert_eq!(chunks[1].data.as_ref(), b"a");
        assert_eq!(chunks[2].start_lsn, 4);
        assert_eq!(chunks[2].data.as_ref(), b"bbb");
        assert_eq!(chunks[3].start_lsn, 7);
        assert_eq!(chunks[3].data.as_ref(), b"b");
        // 同一段内 chunk 的 file_offset 必须连续推进（回放端据此定位写入偏移）
        assert_eq!(chunks[1].file_offset, chunks[0].file_offset + 3);
        assert_eq!(chunks[3].file_offset, chunks[2].file_offset + 3);
        assert!(chunks[3].last, "最后一个 chunk 必须带 last 标记");
        assert!(!chunks[0].last);

        // 分块必须保持 LSN 严格递增且能拼回原始字节
        let mut expected = 0u64;
        let mut restored = Vec::new();
        for chunk in &chunks {
            assert_eq!(chunk.start_lsn, expected);
            expected += chunk.data.len() as u64;
            restored.extend_from_slice(&chunk.data);
        }
        assert_eq!(restored, b"aaaabbbb");
    }

    #[test]
    fn read_range_reports_file_offset_and_reset_wal() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();
        sm.apply(2, 1, &append_with_id("db-1", 1, 0, 100, b"aaaa", "ap-0"))
            .unwrap();
        let reset = WalCommand::append(AppendCommand {
            database_id: "db-1".into(),
            owner_epoch: 1,
            start_lsn: 4,
            wal_file_offset: 0,
            reset_wal: true,
            bytes: b"bbbb".to_vec(),
            append_id: "ap-1".into(),
            contains_commit_frame: false,
        });
        sm.apply(3, 1, &reset).unwrap();

        let chunks = sm.read_range("db-1", 0, 8, 4).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].file_offset, 100, "偏移必须随段一起保存");
        assert!(!chunks[0].reset_wal);
        assert_eq!(chunks[1].start_lsn, 4);
        assert_eq!(chunks[1].file_offset, 0);
        assert!(
            chunks[1].reset_wal,
            "新一代 WAL 的段必须把 reset_wal 传给回放方"
        );

        // 从段中间读取时不得谎报 reset_wal（回放方不应中途截断本地 WAL）
        let mid = sm.read_range("db-1", 4, 8, 2).unwrap();
        assert_eq!(mid[0].file_offset, 0);
        assert!(mid[0].reset_wal);
        let mid2 = sm.read_range("db-1", 2, 8, 4).unwrap();
        assert_eq!(mid2[0].file_offset, 102);
        assert!(!mid2[0].reset_wal);
        // 第二个 chunk 恰好落在一代的段首：必须把 reset_wal 传下去
        assert!(mid2[1].reset_wal);
        assert_eq!(mid2[1].file_offset, 0);
    }

    #[test]
    fn read_range_rejects_undurable_and_gap() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();
        sm.apply(2, 1, &append("db-1", 1, 0, 0, b"abcd")).unwrap();
        // 末端超过 durable 末端
        assert!(matches!(
            sm.read_range("db-1", 0, 9, 1024).unwrap_err(),
            WalError::NotDurable(_)
        ));
        // 未知 DB（新建库，尚无 WAL）：按「无数据」处理，返回空结果而不是错误。
        // 原因：冷启动准备会在第一次写入之前探测 WAL，把「还没写过」当成存储故障
        // 会让新库永远起不来。
        // 空区间会产出一个终止帧（游标约定），但其中不含任何数据字节
        let unknown = sm.read_range("db-x", 0, 0, 1024).unwrap();
        assert!(
            unknown.iter().all(|chunk| chunk.data.is_empty()),
            "未知 DB 的空区间不得返回任何数据: {unknown:?}"
        );
        // 空区间合法
        let empty = sm.read_range("db-1", 4, 4, 1024).unwrap();
        assert_eq!(empty.len(), 1);
        assert!(empty[0].last);
        assert!(empty[0].data.is_empty());
    }

    #[test]
    fn read_range_rejects_holes() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();
        sm.apply(2, 1, &append("db-1", 1, 0, 0, b"abcd")).unwrap();
        // 跳着写：LSN 8 开始，丢掉了 [4,8)
        sm.apply(3, 1, &append("db-1", 1, 8, 4, b"efgh")).unwrap();
        // 单独读后一段是允许的
        assert_eq!(collect(&sm, "db-1", 8, 12, 1024), b"efgh");
        // 跨越空洞必须报错，绝不返回不连续数据
        assert!(matches!(
            sm.read_range("db-1", 0, 12, 1024).unwrap_err(),
            WalError::NotDurable(_)
        ));
    }

    #[test]
    fn chunk_size_is_clamped() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();
        sm.apply(2, 1, &append("db-1", 1, 0, 0, b"0123456789"))
            .unwrap();
        // 客户端请求 0 -> 按 1 字节兜底，不会死循环也不会返回空 chunk
        let chunks = sm.read_range("db-1", 0, 10, 0).unwrap();
        assert_eq!(chunks.len(), 10);
        assert!(chunks.iter().all(|chunk| chunk.data.len() == 1));
        // 超过上限被夹到 MAX_CHUNK_BYTES
        let chunks = sm.read_range("db-1", 0, 10, usize::MAX).unwrap();
        assert_eq!(chunks.len(), 1);
    }

    /// 惰性游标与一次性 `read_range` 必须产出**完全相同**的 chunk 序列：
    /// gRPC 流式路径与本地一次性读取共用同一套分块/偏移/reset 语义。
    #[test]
    fn lazy_cursor_matches_eager_read_range() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();
        sm.apply(2, 1, &append("db-1", 1, 0, 0, b"aaaa")).unwrap();
        sm.apply(3, 1, &append("db-1", 1, 4, 4, b"bbbb")).unwrap();
        sm.apply(4, 1, &append("db-1", 1, 8, 8, b"cccc")).unwrap();

        for (start, end, chunk) in [(0u64, 12u64, 3usize), (0, 12, 5), (2, 9, 4), (3, 3, 4)] {
            let eager = sm.read_range("db-1", start, end, chunk).unwrap();
            let mut cursor = sm.read_range_cursor("db-1", start, end, chunk).unwrap();
            let mut lazy = Vec::new();
            while let Some(item) = cursor.next_chunk() {
                lazy.push(item);
            }
            assert!(cursor.is_finished(), "耗尽后游标必须自报结束");
            assert!(cursor.next_chunk().is_none(), "结束后不得再产出 chunk");
            assert_eq!(
                lazy.len(),
                eager.len(),
                "区间 [{start},{end}) chunk={chunk} 的分块数必须一致"
            );
            for (lazy_chunk, eager_chunk) in lazy.iter().zip(eager.iter()) {
                assert_eq!(lazy_chunk.start_lsn, eager_chunk.start_lsn);
                assert_eq!(lazy_chunk.file_offset, eager_chunk.file_offset);
                assert_eq!(lazy_chunk.reset_wal, eager_chunk.reset_wal);
                assert_eq!(lazy_chunk.last, eager_chunk.last);
                assert_eq!(lazy_chunk.data, eager_chunk.data);
            }
        }

        // 空区间：游标仍要产出一个终止 chunk（流式端据此结束）
        let mut cursor = sm.read_range_cursor("db-1", 12, 12, 4).unwrap();
        let chunk = cursor.next_chunk().expect("空区间必须有终止 chunk");
        assert!(chunk.last && chunk.data.is_empty());
        assert!(cursor.next_chunk().is_none());
    }

    /// 大区间必须能一块一块地取出来（不预先物化整段），且拼回后与原文一致。
    #[test]
    fn read_cursor_yields_large_range_block_by_block() {
        const CHUNK: usize = 1024 * 1024;
        const TOTAL: usize = 8 * CHUNK;
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();
        // 用可校验的模式填充，避免「长度对但内容错」也能通过
        let payload: Vec<u8> = (0..TOTAL).map(|i| (i % 251) as u8).collect();
        sm.apply(2, 1, &append("db-1", 1, 0, 0, &payload)).unwrap();

        let mut cursor = sm
            .read_range_cursor("db-1", 0, TOTAL as u64, CHUNK)
            .unwrap();
        let mut restored = Vec::with_capacity(TOTAL);
        let mut produced = 0usize;
        let mut expected_lsn = 0u64;
        while let Some(chunk) = cursor.next_chunk() {
            produced += 1;
            assert_eq!(chunk.start_lsn, expected_lsn, "chunk 必须严格连续");
            assert!(chunk.data.len() <= CHUNK, "单块不得超过请求上限");
            assert_eq!(
                chunk.last,
                expected_lsn + chunk.data.len() as u64 == TOTAL as u64,
                "只有最后一块带 last 标记"
            );
            expected_lsn += chunk.data.len() as u64;
            restored.extend_from_slice(&chunk.data);
        }
        assert_eq!(produced, TOTAL / CHUNK, "8MiB / 1MiB 必须切成 8 块");
        assert_eq!(restored, payload, "拼接结果必须与写入字节完全一致");
    }

    #[test]
    fn snapshot_roundtrip_restores_state() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 3)).unwrap();
        sm.apply(2, 1, &append_with_id("db-1", 3, 0, 0, b"hello", "ap-1"))
            .unwrap();
        sm.apply(3, 1, &append("db-2", 3, 0, 0, b"world")).unwrap();
        let trim = WalCommand::trim(TrimCommand {
            database_id: "db-1".into(),
            before_lsn: 2,
            snapshot_id: "snap-1".into(),
        });
        sm.apply(4, 1, &trim).unwrap();

        let bytes = sm.to_snapshot().encode_to_vec();
        let restored =
            StateMachine::from_snapshot(StateMachineSnapshot::decode(bytes.as_slice()).unwrap());
        assert_eq!(restored.applied_index(), 4);
        assert_eq!(restored.database_count(), 2);
        assert_eq!(collect(&restored, "db-1", 2, 5, 1024), b"llo");
        assert_eq!(collect(&restored, "db-2", 0, 5, 1024), b"world");
        // 这里只覆盖**尾部重试**：ap-1 恰好是最后一条 Append，因此除幂等键窗口外
        // 还能被 `last_append_id` 兜底命中。非尾部键的重试见
        // `retry_of_non_last_append_is_idempotent_after_snapshot_roundtrip`。
        let mut restored_after_restart = restored.clone();
        let out =
            restored_after_restart.apply(5, 1, &append_with_id("db-1", 3, 0, 0, b"hello", "ap-1"));
        // 注意：start_lsn=0 已经低于 durable 末端，这里应命中 last_append_id 去重
        assert_eq!(
            out.unwrap(),
            ApplyOutcome::Append {
                durable_lsn: 5,
                deduplicated: true,
                idempotency_window_evicted: false
            }
        );
    }

    /// 快照 roundtrip 之后，**非最后一条** append_id 的重试仍必须幂等。
    ///
    /// 为什么这条测试是必须的：WAL 节点重启走的是「状态机快照 + Raft 日志 replay」，
    /// 若幂等键表不进快照，重启后对「更早批次」的重试就查不到键，会掉进
    /// `start_lsn < durable_end_lsn` 分支被当成「覆盖已 durable 区间」拒绝
    /// （INVALID_ARGUMENT，客户端视为终态、不再重试），而该批次其实早已 durable ——
    /// 调用方会看到一个无法自愈的写失败。
    #[test]
    fn retry_of_non_last_append_is_idempotent_after_snapshot_roundtrip() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();
        sm.apply(2, 1, &append_with_id("db-1", 1, 0, 0, b"aa", "ap-1"))
            .unwrap();
        sm.apply(3, 1, &append_with_id("db-1", 1, 2, 2, b"bb", "ap-2"))
            .unwrap();
        sm.apply(4, 1, &append_with_id("db-1", 1, 4, 4, b"cc", "ap-3"))
            .unwrap();
        assert_eq!(sm.status("db-1").unwrap().last_lsn, 6);

        let restored = StateMachine::from_snapshot(sm.to_snapshot());
        let mut restarted = restored;
        // ap-1 既不是最后一条、start_lsn 也落在已 durable 区间内：
        // 只有幂等键表随快照一起恢复，才可能命中。
        let out = restarted
            .apply(5, 1, &append_with_id("db-1", 1, 0, 0, b"aa", "ap-1"))
            .expect("重启后的重试必须仍然是幂等命中，而不是被当成覆盖写");
        assert_eq!(
            out,
            ApplyOutcome::Append {
                durable_lsn: 6,
                deduplicated: true,
                idempotency_window_evicted: false
            }
        );
        // 幂等命中不得重复推进 LSN，也不得重复计数
        assert_eq!(restarted.status("db-1").unwrap().last_lsn, 6);
        assert_eq!(restarted.status("db-1").unwrap().append_count, 3);
        assert_eq!(collect(&restarted, "db-1", 0, 6, 1024), b"aabbcc");

        // 同一 append_id 换 start_lsn 仍然必须报冲突（幂等键的校验语义不因重启而放松）
        let err = restarted
            .apply(6, 1, &append_with_id("db-1", 1, 8, 8, b"aa", "ap-1"))
            .unwrap_err();
        assert!(matches!(err, WalError::IdempotencyConflict { .. }), "{err}");
    }

    /// 快照必须可重复构造：同一个命令序列导出的字节完全相同。
    ///
    /// 幂等键表若用 `HashMap` 的迭代顺序导出，进程间的随机种子会让两个副本
    /// 导出不同的快照字节（快照必须只由命令序列决定）。
    #[test]
    fn snapshot_bytes_are_reproducible() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();
        for (index, append_id) in ["ap-c", "ap-a", "ap-b", "ap-d"].iter().enumerate() {
            let start = index as u64 * 2;
            sm.apply(
                index as u64 + 2,
                1,
                &append_with_id("db-1", 1, start, start, b"xy", append_id),
            )
            .unwrap();
        }
        let first = sm.to_snapshot().encode_to_vec();
        let reencoded = StateMachine::from_snapshot(sm.to_snapshot()).to_snapshot();
        assert_eq!(
            first,
            reencoded.encode_to_vec(),
            "同状态导出的快照必须逐字节相同（含幂等键窗口的顺序）"
        );
    }

    /// 幂等窗口满时按插入顺序逐出最旧的键，而不是整表清空。
    #[test]
    fn idempotency_window_evicts_oldest_and_keeps_recent() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();

        // 填满窗口（每条 1 字节）
        let total = MAX_IDEMPOTENCY_ENTRIES as u64 + 1;
        let mut evicted_at = Vec::new();
        for i in 0..total {
            let out = sm
                .apply(
                    i + 2,
                    1,
                    &append_with_id("db-1", 1, i, i, b"x", &format!("ap-{i}")),
                )
                .unwrap();
            match out {
                ApplyOutcome::Append {
                    idempotency_window_evicted,
                    ..
                } => {
                    if idempotency_window_evicted {
                        evicted_at.push(i);
                    }
                }
                other => panic!("期望 Append 结果，实际 {other:?}"),
            }
        }
        assert_eq!(
            evicted_at,
            vec![MAX_IDEMPOTENCY_ENTRIES as u64],
            "只有写入第 N+1 条时才会逐出，且只逐出一条"
        );

        // 最旧的键（ap-0）已被逐出：它的重试落在已 durable 区间内，只能被拒绝
        // （这正是「窗口有界」的必然代价，行为必须显式而非静默改写数据）
        let err = sm
            .apply(total + 2, 1, &append_with_id("db-1", 1, 0, 0, b"x", "ap-0"))
            .unwrap_err();
        assert!(matches!(err, WalError::InvalidArgument(_)), "{err}");

        // 最近的键必须全部保留：窗口是「逐出最旧」，不是「整表清空」
        for i in (total - MAX_IDEMPOTENCY_ENTRIES as u64)..total {
            let start = i;
            let out = sm
                .apply(
                    total + 3 + i,
                    1,
                    &append_with_id("db-1", 1, start, start, b"x", &format!("ap-{i}")),
                )
                .unwrap();
            assert!(
                matches!(
                    out,
                    ApplyOutcome::Append {
                        deduplicated: true,
                        ..
                    }
                ),
                "ap-{i} 仍在窗口内，必须命中幂等"
            );
        }
        assert_eq!(sm.status("db-1").unwrap().last_lsn, total);
    }

    #[test]
    fn snapshot_roundtrip_preserves_large_payload() {
        let mut sm = StateMachine::new();
        sm.apply(1, 1, &set_epoch("db-1", 1)).unwrap();
        let payload: Vec<u8> = (0..70_000u32).map(|i| (i % 251) as u8).collect();
        sm.apply(2, 1, &append("db-1", 1, 0, 0, &payload)).unwrap();
        let restored = StateMachine::from_snapshot(sm.to_snapshot());
        assert_eq!(
            collect(&restored, "db-1", 0, 70_000, 64 * 1024).len(),
            70_000
        );
        assert_eq!(collect(&restored, "db-1", 0, 70_000, 64 * 1024), payload);
    }
}
