//! 本地 WAL 字节流的帧解析（架构 §17.3：**只有本 crate 理解 Turso/SQLite WAL 内部格式**）。
//!
//! TursoDB / SQLite 的 WAL 文件是固定布局的字节流：
//!
//! ```text
//! [0..32)                       WAL header：0..4 magic(0x377f0682/0x377f0683，大端)，
//!                               8..12 page_size（大端）
//! [32 + i*frame_size, ...)      第 i 个 frame：24 字节 frame header + page_size 字节页面
//!   frame header 0..4           page_number（1 起；0 视为非法）
//!   frame header 4..8           db_size（nTruncate）——**非 0 表示本帧是 commit frame**
//! ```
//!
//! 本模块只做「字节 → commit 边界」的判定，不持有任何 IO、不认识 Turso 的 WAL 类型：
//! `PlatformDurableIO` 把本地 `pwrite` 的字节喂给它，它回答「从哪个文件偏移开始的
//! 哪些字节需要在本地写成功后被送到 Remote WAL」。
//!
//! 三条不变量（`PlatformDurableIO` 的 durability 契约依赖它们）：
//!
//! 1. **未提交字节留在 pending**：`[pending_start, cursor)` 与本地 WAL 文件的同一区间
//!    逐字节一致；只有 commit frame 才会把整段交给调用方去 append，因此 Remote WAL
//!    永远只包含「已提交前缀」，不会出现半个事务。
//! 2. **回卷只允许丢弃未提交字节**：引擎 rollback 后会在同一偏移重写帧，此时丢弃
//!    `[pos, cursor)` 是安全的——它们位于最后一个已 append 的 commit 边界之后。
//!    任何回卷越过 `pending_start`（= 已送出的字节）都是协议违规，必须 fail-stop。
//! 3. **不连续写入即故障**：偏移出现空洞意味着本地 WAL 字节流已经不可信，继续 append
//!    会让 Remote WAL 与本地 WAL 永久分叉，因此直接报错而不是猜测。

use bytes::Bytes;
use thiserror::Error;

/// WAL header 大小（字节）。
pub const WAL_HEADER_SIZE: usize = 32;
/// WAL frame header 大小（字节）。
pub const WAL_FRAME_HEADER_SIZE: usize = 24;
/// 校验和按小端字节序计算的 WAL magic。
pub const WAL_MAGIC_LE: u32 = 0x377f_0682;
/// 校验和按大端字节序计算的 WAL magic。
pub const WAL_MAGIC_BE: u32 = 0x377f_0683;
/// 最小合法 page size。
pub const MIN_PAGE_SIZE: u32 = 512;
/// 最大合法 page size。
pub const MAX_PAGE_SIZE: u32 = 65536;

/// WAL 字节流不符合帧协议时的错误。
///
/// 这些错误一律**不可恢复**：本地 WAL 与 Remote WAL 的字节级对应关系已经无法保证，
/// 继续写入等于把不可信字节推进 durability 边界。
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WalStreamError {
    /// 写入偏移与解析游标不连续（字节流出现空洞或重复）。
    #[error(
        "本地 WAL 写入偏移不连续：write_pos={pos}，解析游标={cursor}（本地 WAL 字节流被破坏）"
    )]
    NonSequential {
        /// 本次写入的文件偏移。
        pos: u64,
        /// 解析器当前期待的偏移。
        cursor: u64,
    },
    /// 回卷越过了最后一个已提交边界：已送出的字节不允许被覆盖。
    #[error(
        "本地 WAL 回卷到 {pos}，早于最后一个 commit 边界 {pending_start}：已提交字节不允许被覆盖"
    )]
    RewindPastCommit {
        /// 本次写入的文件偏移。
        pos: u64,
        /// 最后一个 commit 边界（`pending[0]` 的文件偏移）。
        pending_start: u64,
    },
    /// 截断落在未提交区间中间。
    #[error("本地 WAL 截断到 {len}，落在未提交区间 [{pending_start}, {cursor}) 中间")]
    TruncateInsidePending {
        /// 目标长度。
        len: u64,
        /// 未提交区间起点。
        pending_start: u64,
        /// 未提交区间终点。
        cursor: u64,
    },
    /// WAL header 非法。
    #[error("本地 WAL header 非法：magic={magic:#010x} page_size={page_size}")]
    InvalidHeader {
        /// 实际读到的 magic。
        magic: u32,
        /// 实际读到的 page size。
        page_size: u32,
    },
    /// 帧头非法（当前只把 page_number == 0 视为非法）。
    #[error("本地 WAL 帧头非法：offset={offset} page_number=0")]
    InvalidFrame {
        /// 该帧的文件偏移。
        offset: u64,
    },
}

/// 一段需要在本地写成功后送到 Remote WAL 的字节。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableBatch {
    /// 这批字节在本地 WAL 文件内的起始偏移（`AppendRequest.wal_file_offset`）。
    pub file_offset: u64,
    /// 字节内容（一定以 commit frame 结尾）。
    pub data: Bytes,
    /// 本批是否开启新一代本地 WAL（`AppendRequest.reset_wal`）。
    pub reset_wal: bool,
}

/// 一次写入之前的解析器状态，用于本地写失败时回滚。
///
/// 只在本次写入没有产生 [`DurableBatch`] 时才允许回滚：一旦字节被交给调用方去 append，
/// 它们就代表「已经承诺要进入 Remote WAL 的提交前缀」，撤销会让两侧字节流分叉。
#[derive(Debug, Clone, Copy)]
pub struct WalWriteCheckpoint {
    pending_start: u64,
    cursor: u64,
    pending_len: usize,
    page_size: Option<u32>,
    needs_reset_append: bool,
}

/// WAL 帧解析器（非线程安全，由调用方用锁保护）。
#[derive(Debug)]
pub struct WalFrameParser {
    /// `pending[0]` 对应的本地 WAL 文件偏移。
    pending_start: u64,
    /// 下一个待解析字节的文件偏移；恒等于 `pending_start + pending.len()`。
    cursor: u64,
    /// 当前 WAL 代的 page size；尚未读到 header 时为 `None`。
    page_size: Option<u32>,
    /// 尚未提交的 WAL 字节。
    pending: Vec<u8>,
    /// 下一次 append 是否要带 `reset_wal`（新的一代本地 WAL 从 `file_offset` 重新开始）。
    needs_reset_append: bool,
}

impl Default for WalFrameParser {
    fn default() -> Self {
        Self::new()
    }
}

impl WalFrameParser {
    /// 新建解析器：空流，且第一次 append 会标记 `reset_wal`（空文件从头写即为新一代）。
    #[must_use]
    pub fn new() -> Self {
        Self {
            pending_start: 0,
            cursor: 0,
            page_size: None,
            pending: Vec::new(),
            needs_reset_append: true,
        }
    }

    /// 恢复播种：把解析游标对齐到「已经回放完成的本地 WAL 末端」。
    ///
    /// `file_offset` 必须是帧边界（本地 WAL 已回放的长度）；`durable_lsn` 的推进由
    /// `PlatformDurableIO` 负责，这里只管字节流位置。`reset_wal` 为 true 表示
    /// 下一个 commit 批次要重新开启一代（对应恢复后从头写 WAL 的场景）。
    pub fn seed(&mut self, file_offset: u64, page_size: u32, reset_wal: bool) {
        self.pending_start = file_offset;
        self.cursor = file_offset;
        self.page_size = Some(page_size);
        self.pending.clear();
        self.needs_reset_append = reset_wal;
    }

    /// 当前解析游标（下一个待解析字节的文件偏移）。
    #[must_use]
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// 未提交区间的起点（= 最后一个已交出 commit 边界的终点）。
    #[must_use]
    pub fn pending_start(&self) -> u64 {
        self.pending_start
    }

    /// 已解析出的 page size（未读到 header 时为 `None`）。
    #[must_use]
    pub fn page_size(&self) -> Option<u32> {
        self.page_size
    }

    /// 取回滚点。
    #[must_use]
    pub fn checkpoint(&self) -> WalWriteCheckpoint {
        WalWriteCheckpoint {
            pending_start: self.pending_start,
            cursor: self.cursor,
            pending_len: self.pending.len(),
            page_size: self.page_size,
            needs_reset_append: self.needs_reset_append,
        }
    }

    /// 回滚到写入之前的状态（仅用于「本次写入没有产生 commit 批次」的本地写失败）。
    pub fn rollback_write(&mut self, checkpoint: WalWriteCheckpoint) {
        self.pending_start = checkpoint.pending_start;
        self.cursor = checkpoint.cursor;
        self.pending.truncate(checkpoint.pending_len);
        self.page_size = checkpoint.page_size;
        self.needs_reset_append = checkpoint.needs_reset_append;
        debug_assert!(
            self.cursor >= self.pending_start && self.cursor <= self.write_end(),
            "回滚后必须保持 pending_start <= 解析游标 <= 写入末端 不变量"
        );
    }

    /// 处理一次本地 WAL 写入，返回本批次需要送到 Remote WAL 的字节（按提交顺序）。
    ///
    /// 语义要点：
    /// - 只有完整解析到 **commit frame** 才会产生批次；非 commit 帧只累积在 pending 里；
    /// - 一个批次总是从 `pending_start` 开始到 commit frame 结束，因此包含 WAL header
    ///   与事务内的全部非提交帧，可被远端按 `wal_file_offset` 原样回放；
    /// - 一帧的写入如果跨越两次 `pwrite`（Turso 逐帧写时不会，但 `pwritev` 会分批），
    ///   只需保持偏移连续即可。
    pub fn on_write(
        &mut self,
        pos: u64,
        bytes: &[u8],
    ) -> Result<Vec<DurableBatch>, WalStreamError> {
        if bytes.is_empty() {
            return Ok(Vec::new());
        }

        if pos == 0 {
            // 偏移 0 只可能是 WAL header；写 header 即代表本地 WAL 重新从头写（新的一代）。
            if !is_wal_header(bytes) {
                return Err(WalStreamError::InvalidHeader {
                    magic: read_u32_be(bytes, 0).unwrap_or(0),
                    page_size: read_u32_be(bytes, 8).unwrap_or(0),
                });
            }
            self.start_new_generation();
        }

        // 「写入末端」= pending_start + pending.len()：它才是下一次写入的期望偏移。
        // `cursor` 只是**解析**游标，落后于写入末端是常态（帧没写全），因此两者不能混用。
        let write_end = self.write_end();
        if pos == write_end {
            self.pending.extend_from_slice(bytes);
        } else if pos < write_end && pos >= self.pending_start {
            // rollback / statement 失败后引擎在同样的偏移重写帧：丢弃未提交区间再写。
            // 解析游标同时回退到重写点 —— 被丢弃的字节不能继续算作「已解析」。
            self.pending.truncate((pos - self.pending_start) as usize);
            self.cursor = self.cursor.min(pos);
            self.pending.extend_from_slice(bytes);
        } else if pos < self.pending_start {
            return Err(WalStreamError::RewindPastCommit {
                pos,
                pending_start: self.pending_start,
            });
        } else {
            return Err(WalStreamError::NonSequential {
                pos,
                cursor: write_end,
            });
        }

        self.scan_frames()
    }

    /// 当前写入末端（= 未提交区间的终点，也是下一次写入的期望偏移）。
    #[must_use]
    pub fn write_end(&self) -> u64 {
        self.pending_start + self.pending.len() as u64
    }

    /// 处理一次本地 WAL 截断。
    ///
    /// - 截断到 0：本地 WAL 进入新一代（Turso 的 TRUNCATE checkpoint 会这么做）；
    /// - 截断到当前游标：合法且常见（写入新 header 后清掉上一代的孤儿帧）；
    /// - 其它位置：会切开未提交区间，属于协议违规。
    pub fn on_truncate(&mut self, len: u64) -> Result<(), WalStreamError> {
        if len == 0 {
            self.start_new_generation();
            return Ok(());
        }
        if len == self.cursor || len == self.write_end() {
            return Ok(());
        }
        if len == self.pending_start && self.pending.is_empty() {
            self.cursor = len;
            return Ok(());
        }
        Err(WalStreamError::TruncateInsidePending {
            len,
            pending_start: self.pending_start,
            cursor: self.cursor,
        })
    }

    /// 进入新一代本地 WAL：丢弃所有未提交字节，下一次 append 带 `reset_wal`。
    fn start_new_generation(&mut self) {
        self.pending_start = 0;
        self.cursor = 0;
        self.pending.clear();
        self.page_size = None;
        self.needs_reset_append = true;
    }

    /// 从 pending 中扫描出所有完整的 commit frame。
    fn scan_frames(&mut self) -> Result<Vec<DurableBatch>, WalStreamError> {
        let mut batches = Vec::new();

        if self.page_size.is_none() {
            debug_assert_eq!(
                self.pending_start, 0,
                "只有新一代（pending_start == 0）才可能尚未读到 WAL header"
            );
            if self.pending.len() < WAL_HEADER_SIZE {
                // header 还没写全（正常实现不会发生，但不能猜测 page size）。
                return Ok(batches);
            }
            let magic = read_u32_be(&self.pending, 0).expect("header 已保证长度");
            let page_size = read_u32_be(&self.pending, 8).expect("header 已保证长度");
            if !is_wal_magic(magic) || !is_valid_page_size(page_size) {
                return Err(WalStreamError::InvalidHeader { magic, page_size });
            }
            self.page_size = Some(page_size);
            self.cursor = self.pending_start + WAL_HEADER_SIZE as u64;
        }

        let page_size = self.page_size.expect("上一步已确定 page size") as u64;
        let frame_size = WAL_FRAME_HEADER_SIZE as u64 + page_size;
        let stream_end = self.pending_start + self.pending.len() as u64;

        while self.cursor + frame_size <= stream_end {
            let offset = (self.cursor - self.pending_start) as usize;
            let page_number = read_u32_be(&self.pending[offset..], 0).expect("帧头已保证长度");
            let db_size = read_u32_be(&self.pending[offset..], 4).expect("帧头已保证长度");
            if page_number == 0 {
                return Err(WalStreamError::InvalidFrame {
                    offset: self.cursor,
                });
            }
            self.cursor += frame_size;

            if db_size != 0 {
                // commit frame：把 [pending_start, cursor) 整段交给调用方 append。
                let len = (self.cursor - self.pending_start) as usize;
                let data = Bytes::copy_from_slice(&self.pending[..len]);
                batches.push(DurableBatch {
                    file_offset: self.pending_start,
                    data,
                    reset_wal: std::mem::take(&mut self.needs_reset_append),
                });
                self.pending.drain(..len);
                self.pending_start = self.cursor;
            }
        }

        Ok(batches)
    }
}

/// 是否是合法的 WAL magic（大端读取）。
#[must_use]
pub fn is_wal_magic(magic: u32) -> bool {
    magic == WAL_MAGIC_LE || magic == WAL_MAGIC_BE
}

/// page size 是否在 SQLite 允许范围内（512..=65536 且为 2 的幂）。
#[must_use]
pub fn is_valid_page_size(page_size: u32) -> bool {
    (MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&page_size) && page_size.is_power_of_two()
}

/// 一段字节是否是（至少以）一个合法 WAL header 开头。
#[must_use]
pub fn is_wal_header(bytes: &[u8]) -> bool {
    if bytes.len() < WAL_HEADER_SIZE {
        return false;
    }
    let magic = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let page_size = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    is_wal_magic(magic) && is_valid_page_size(page_size)
}

fn read_u32_be(bytes: &[u8], offset: usize) -> Option<u32> {
    let slice = bytes.get(offset..offset + 4)?;
    Some(u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE_SIZE: u32 = 4096;
    const FRAME_SIZE: usize = WAL_FRAME_HEADER_SIZE + PAGE_SIZE as usize;

    fn wal_header() -> Vec<u8> {
        let mut header = vec![0u8; WAL_HEADER_SIZE];
        header[0..4].copy_from_slice(&WAL_MAGIC_LE.to_be_bytes());
        header[4..8].copy_from_slice(&3_007_000u32.to_be_bytes());
        header[8..12].copy_from_slice(&PAGE_SIZE.to_be_bytes());
        header[16..20].copy_from_slice(&0x1122_3344u32.to_be_bytes());
        header[20..24].copy_from_slice(&0x5566_7788u32.to_be_bytes());
        header
    }

    fn frame(page_number: u32, db_size: u32) -> Vec<u8> {
        let mut frame = vec![0u8; FRAME_SIZE];
        frame[0..4].copy_from_slice(&page_number.to_be_bytes());
        frame[4..8].copy_from_slice(&db_size.to_be_bytes());
        // 页面内容用 page_number 填充，便于断言「送出的字节就是这些字节」。
        for (i, byte) in frame[WAL_FRAME_HEADER_SIZE..].iter_mut().enumerate() {
            *byte = (page_number as usize + i) as u8;
        }
        frame
    }

    #[test]
    fn header_alone_produces_no_batch() {
        let mut parser = WalFrameParser::new();
        let batches = parser.on_write(0, &wal_header()).expect("header 合法");
        assert!(batches.is_empty(), "只有 header 不能触发 remote append");
        assert_eq!(parser.cursor(), WAL_HEADER_SIZE as u64);
        assert_eq!(parser.page_size(), Some(PAGE_SIZE));
    }

    #[test]
    fn only_commit_frame_triggers_append() {
        let mut parser = WalFrameParser::new();
        assert!(parser.on_write(0, &wal_header()).unwrap().is_empty());

        // 非 commit 帧：只能累积在 pending 中。
        let first = frame(1, 0);
        assert!(
            parser
                .on_write(WAL_HEADER_SIZE as u64, &first)
                .unwrap()
                .is_empty(),
            "db_size == 0 的帧不是 commit frame，不得触发 append"
        );

        // commit 帧：一次 append，覆盖 header + 两个帧。
        let second = frame(2, 2);
        let batches = parser
            .on_write((WAL_HEADER_SIZE + FRAME_SIZE) as u64, &second)
            .unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.file_offset, 0, "批次必须从 WAL 起点开始（含 header）");
        assert_eq!(batch.data.len(), WAL_HEADER_SIZE + 2 * FRAME_SIZE);
        assert!(batch.reset_wal, "本地 WAL 的第一代从偏移 0 开始");
        assert_eq!(&batch.data[..WAL_HEADER_SIZE], wal_header().as_slice());
        assert_eq!(
            &batch.data[WAL_HEADER_SIZE..WAL_HEADER_SIZE + FRAME_SIZE],
            first.as_slice()
        );
        assert_eq!(
            &batch.data[WAL_HEADER_SIZE + FRAME_SIZE..],
            second.as_slice()
        );
        assert_eq!(
            parser.pending_start(),
            batch.file_offset + batch.data.len() as u64
        );

        // 第二个事务：批次从上一个 commit 边界开始，且不再是 reset。
        let third = frame(3, 0);
        let fourth = frame(4, 3);
        assert!(parser
            .on_write((WAL_HEADER_SIZE + 2 * FRAME_SIZE) as u64, &third)
            .unwrap()
            .is_empty());
        let batches = parser
            .on_write((WAL_HEADER_SIZE + 3 * FRAME_SIZE) as u64, &fourth)
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0].file_offset,
            (WAL_HEADER_SIZE + 2 * FRAME_SIZE) as u64
        );
        assert!(!batches[0].reset_wal);
        assert_eq!(batches[0].data.len(), 2 * FRAME_SIZE);
    }

    #[test]
    fn vectored_write_with_multiple_commits_yields_batches_in_order() {
        let mut parser = WalFrameParser::new();
        // 一次写入包含：header + 事务1(2 帧) + 事务2(1 帧)
        let mut stream = wal_header();
        stream.extend_from_slice(&frame(1, 0));
        stream.extend_from_slice(&frame(2, 2));
        stream.extend_from_slice(&frame(3, 3));

        let batches = parser.on_write(0, &stream).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].file_offset, 0);
        assert_eq!(batches[0].data.len(), WAL_HEADER_SIZE + 2 * FRAME_SIZE);
        assert!(batches[0].reset_wal);
        assert_eq!(
            batches[1].file_offset,
            (WAL_HEADER_SIZE + 2 * FRAME_SIZE) as u64
        );
        assert_eq!(batches[1].data.len(), FRAME_SIZE);
        assert!(!batches[1].reset_wal, "同一代内只有第一批带 reset_wal");
        assert_eq!(parser.pending_start(), stream.len() as u64);
    }

    #[test]
    fn rewound_write_discards_uncommitted_bytes() {
        let mut parser = WalFrameParser::new();
        parser.on_write(0, &wal_header()).unwrap();
        let aborted = frame(1, 0);
        parser.on_write(WAL_HEADER_SIZE as u64, &aborted).unwrap();

        // 事务回滚后引擎在同样偏移重写同一帧。
        let mut rewritten = frame(1, 0);
        rewritten[WAL_FRAME_HEADER_SIZE] = 0xAB;
        assert!(parser
            .on_write(WAL_HEADER_SIZE as u64, &rewritten)
            .unwrap()
            .is_empty());

        let batches = parser
            .on_write((WAL_HEADER_SIZE + FRAME_SIZE) as u64, &frame(2, 2))
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(
            &batches[0].data[WAL_HEADER_SIZE..WAL_HEADER_SIZE + FRAME_SIZE],
            rewritten.as_slice(),
            "被回滚的字节不得进入 Remote WAL"
        );
    }

    #[test]
    fn rewind_before_commit_boundary_is_rejected() {
        let mut parser = WalFrameParser::new();
        parser.on_write(0, &wal_header()).unwrap();
        parser
            .on_write(WAL_HEADER_SIZE as u64, &frame(1, 1))
            .unwrap();

        // 回卷越过已经交出（并即将 append）的 commit 边界：必须 fail-stop。
        let err = parser
            .on_write(WAL_HEADER_SIZE as u64, &frame(1, 0))
            .unwrap_err();
        assert!(matches!(err, WalStreamError::RewindPastCommit { .. }));
    }

    #[test]
    fn non_sequential_write_is_rejected() {
        let mut parser = WalFrameParser::new();
        parser.on_write(0, &wal_header()).unwrap();
        let err = parser
            .on_write((WAL_HEADER_SIZE + FRAME_SIZE) as u64, &frame(2, 0))
            .unwrap_err();
        assert!(matches!(err, WalStreamError::NonSequential { .. }));
    }

    #[test]
    fn new_header_starts_new_generation() {
        let mut parser = WalFrameParser::new();
        parser.on_write(0, &wal_header()).unwrap();
        parser
            .on_write(WAL_HEADER_SIZE as u64, &frame(1, 1))
            .unwrap();

        // 新一代：重新从偏移 0 写 header。
        assert!(parser.on_write(0, &wal_header()).unwrap().is_empty());
        assert_eq!(parser.pending_start(), 0);
        let batches = parser
            .on_write(WAL_HEADER_SIZE as u64, &frame(1, 1))
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert!(
            batches[0].reset_wal,
            "新一代的第一个 commit 必须带 reset_wal，供恢复时先清空本地 WAL"
        );
        assert_eq!(batches[0].file_offset, 0);
    }

    #[test]
    fn truncate_semantics() {
        let mut parser = WalFrameParser::new();
        parser.on_write(0, &wal_header()).unwrap();

        // header 写完后 Turso 会截断到 WAL_HEADER_SIZE（清掉上一代孤儿帧），这是 no-op。
        parser.on_truncate(WAL_HEADER_SIZE as u64).unwrap();

        // 未提交区间中间的截断必须被拒绝。
        parser
            .on_write(WAL_HEADER_SIZE as u64, &frame(1, 0))
            .unwrap();
        let err = parser.on_truncate(WAL_HEADER_SIZE as u64 + 8).unwrap_err();
        assert!(matches!(err, WalStreamError::TruncateInsidePending { .. }));

        // 截断到 0 = 新一代。
        parser.on_truncate(0).unwrap();
        assert_eq!(parser.cursor(), 0);
        assert_eq!(parser.pending_start(), 0);
        assert!(parser.page_size().is_none());
        let batches = parser.on_write(0, &wal_header()).unwrap();
        assert!(batches.is_empty());
        let batches = parser
            .on_write(WAL_HEADER_SIZE as u64, &frame(1, 1))
            .unwrap();
        assert!(batches[0].reset_wal);
    }

    #[test]
    fn invalid_header_or_frame_is_rejected() {
        let mut parser = WalFrameParser::new();
        let mut bad = wal_header();
        bad[8..12].copy_from_slice(&1000u32.to_be_bytes()); // 非 2 的幂
        assert!(matches!(
            parser.on_write(0, &bad).unwrap_err(),
            WalStreamError::InvalidHeader { .. }
        ));

        let mut parser = WalFrameParser::new();
        parser.on_write(0, &wal_header()).unwrap();
        let err = parser
            .on_write(WAL_HEADER_SIZE as u64, &frame(0, 0))
            .unwrap_err();
        assert!(matches!(err, WalStreamError::InvalidFrame { .. }));
    }

    #[test]
    fn rollback_write_restores_checkpoint() {
        let mut parser = WalFrameParser::new();
        parser.on_write(0, &wal_header()).unwrap();
        let checkpoint = parser.checkpoint();
        parser
            .on_write(WAL_HEADER_SIZE as u64, &frame(1, 0))
            .unwrap();
        assert_eq!(parser.cursor(), (WAL_HEADER_SIZE + FRAME_SIZE) as u64);

        parser.rollback_write(checkpoint);
        assert_eq!(parser.cursor(), WAL_HEADER_SIZE as u64);
        // 回滚后重新写入同一帧，仍然没有 commit。
        assert!(parser
            .on_write(WAL_HEADER_SIZE as u64, &frame(1, 0))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn seed_resumes_after_restored_wal() {
        let mut parser = WalFrameParser::new();
        parser.seed(1024, PAGE_SIZE, false);
        assert_eq!(parser.cursor(), 1024);
        // 恢复后引擎从回放末端继续追加帧。
        let batches = parser.on_write(1024, &frame(9, 9)).unwrap();
        assert_eq!(batches.len(), 1);
        assert!(!batches[0].reset_wal);
        assert_eq!(batches[0].file_offset, 1024);
        assert_eq!(batches[0].data.len(), FRAME_SIZE);
    }
}
