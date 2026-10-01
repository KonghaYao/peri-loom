//! TursoDB / SQLite WAL **帧格式**的最小解析（架构 §17.3）。
//!
//! 本模块只做一件事：在 WAL 文件的字节流上识别帧边界，回答「哪些字节属于一次 commit」。
//! 它**不理解页内容**，也不接触 IO，因此可以脱离引擎单独测试。
//!
//! ```text
//! WAL 头（32B）：0..4 magic | 4..8 格式版本 | 8..12 page_size | 12..16 checkpoint seq | ...
//! 帧（24B + page_size）：0..4 page_number | 4..8 db_size(nTruncate) | 8..12 salt1 | ...
//! ```
//!
//! 两条与实现强相关的结论（对照 turso_core 0.8.1 `storage/sqlite3_ondisk.rs`）：
//!
//! * **字段一律按大端解析**。Turso 用 `to_be_bytes` / `from_be_bytes` 读写
//!   `magic` / `page_size` / `page_number` / `db_size`；magic 的最低位只影响
//!   **校验和**的字节序（`0x377f0682` = 本机序、`0x377f0683` = 大端），不影响字段编码。
//! * **`db_size != 0` 的帧是 commit frame**（SQLite 语义：只有事务最后一帧写 nTruncate）。
//!   非 commit 帧没有任何 durability 含义，绝不允许触发远程 append。

use std::fmt;

/// WAL 头长度。
pub const WAL_HEADER_SIZE: usize = 32;
/// 单个帧头长度（帧 = 帧头 + page_size 字节页数据）。
pub const WAL_FRAME_HEADER_SIZE: usize = 24;
/// 本机（小端主机）写出的 WAL magic：低位为 0，校验和按本机字节序。
pub const WAL_MAGIC_LE: u32 = 0x377f0682;
/// 大端主机写出的 WAL magic：低位为 1，校验和按大端。
pub const WAL_MAGIC_BE: u32 = 0x377f0683;

/// SQLite 允许的最小页大小。
pub const MIN_PAGE_SIZE: u32 = 512;
/// SQLite 允许的最大页大小。
pub const MAX_PAGE_SIZE: u32 = 65536;

/// 是否为合法的 WAL magic。
#[must_use]
pub const fn is_wal_magic(magic: u32) -> bool {
    magic == WAL_MAGIC_LE || magic == WAL_MAGIC_BE
}

/// 是否为合法的页大小（2 的幂，512..=65536）。
#[must_use]
pub fn is_valid_page_size(page_size: u32) -> bool {
    (MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&page_size) && page_size.is_power_of_two()
}

/// 大端读取 u32（调用方保证长度足够）。
#[must_use]
pub fn read_u32_be(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

/// 帧头里我们关心的两个字段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    /// 页号。
    pub page_number: u32,
    /// 事务提交后的数据库页数；0 表示该帧不是事务最后一帧。
    pub db_size: u32,
}

impl FrameHeader {
    /// 是否为 commit frame（架构 §11.1 的唯一提交边界）。
    #[must_use]
    pub const fn is_commit(&self) -> bool {
        self.db_size != 0
    }
}

/// 解析 24 字节帧头。
#[must_use]
pub fn parse_frame_header(bytes: &[u8]) -> Option<FrameHeader> {
    if bytes.len() < WAL_FRAME_HEADER_SIZE {
        return None;
    }
    Some(FrameHeader {
        page_number: read_u32_be(&bytes[0..4]),
        db_size: read_u32_be(&bytes[4..8]),
    })
}

/// 解析 32 字节 WAL 头，返回 page_size（magic / page_size 非法时返回 `None`）。
#[must_use]
pub fn parse_wal_header_page_size(bytes: &[u8]) -> Option<u32> {
    if bytes.len() < WAL_HEADER_SIZE {
        return None;
    }
    if !is_wal_magic(read_u32_be(&bytes[0..4])) {
        return None;
    }
    let page_size = read_u32_be(&bytes[8..12]);
    is_valid_page_size(page_size).then_some(page_size)
}

/// WAL 字节格式无法识别。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalFormatError {
    /// 期望 WAL 头，但 magic 不是 `0x377f0682` / `0x377f0683`。
    BadMagic {
        /// 该头的绝对偏移。
        offset: u64,
        /// 实际读到的 magic。
        magic: u32,
    },
    /// WAL 头里的 page_size 不是 2 的幂（或超出 512..=65536）。
    BadPageSize {
        /// 该头的绝对偏移。
        offset: u64,
        /// 实际读到的页大小。
        page_size: u32,
    },
}

impl fmt::Display for WalFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalFormatError::BadMagic { offset, magic } => write!(
                f,
                "WAL 头 magic 非法：offset={offset}, magic=0x{magic:08x}（期望 0x377f0682 / 0x377f0683）"
            ),
            WalFormatError::BadPageSize { offset, page_size } => write!(
                f,
                "WAL 头 page_size 非法：offset={offset}, page_size={page_size}"
            ),
        }
    }
}

impl std::error::Error for WalFormatError {}

/// WAL 帧解析游标（纯状态，不含字节）。
///
/// 语义：`offset` 之前的字节已经**确定**是完整的头 + 整数个帧；从 `offset` 起需要更多字节。
/// `page_size == 0` 表示还不知道页大小（此时除了解析 WAL 头什么都不能做）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalCursor {
    /// 已解析出的 page_size；0 表示未知。
    pub page_size: u32,
    /// 下一个待解析字节的绝对偏移。
    pub offset: u64,
    /// `offset` 处应当是 WAL 头（文件起点 / 新的一代）。
    pub expect_header: bool,
}

impl Default for WalCursor {
    fn default() -> Self {
        Self::new()
    }
}

impl WalCursor {
    /// 文件起点状态：等待 WAL 头。
    #[must_use]
    pub const fn new() -> Self {
        Self {
            page_size: 0,
            offset: 0,
            expect_header: true,
        }
    }

    /// 回到一代 WAL 的起点（truncate 到 0 或重写 WAL 头时使用）。
    pub fn restart(&mut self) {
        self.page_size = 0;
        self.offset = 0;
        self.expect_header = true;
    }

    /// 文件被截断到 `len` 之后同步游标。
    ///
    /// * `len == 0`：一代 WAL 被清空，等待新的 WAL 头，页大小未知。
    /// * `len <= WAL_HEADER_SIZE`：只保留（或截到只剩）WAL 头，帧从头开始。
    pub fn truncate_to(&mut self, len: u64) {
        if self.offset > len {
            self.offset = len;
        }
        if len < WAL_HEADER_SIZE as u64 {
            self.page_size = 0;
            self.expect_header = true;
        } else if len == WAL_HEADER_SIZE as u64 {
            self.expect_header = false;
        }
    }

    /// 直接把游标放到「已存在的完整 WAL 的末端」（failover 恢复后打开数据库的场景）。
    ///
    /// 恢复出来的 WAL 已经是若干代完整字节（见 [`crate::recovery`]），因此不需要重新解析
    /// 历史帧，只需要一个正确的 `page_size` 与对齐位置。
    pub fn resume_at(&mut self, offset: u64, page_size: u32) {
        self.offset = offset;
        self.page_size = page_size;
        self.expect_header = false;
    }

    /// 一个帧的总长度（帧头 + 页数据）；页大小未知时为 `None`。
    #[must_use]
    pub fn frame_size(&self) -> Option<u64> {
        if self.page_size == 0 {
            None
        } else {
            Some(WAL_FRAME_HEADER_SIZE as u64 + u64::from(self.page_size))
        }
    }
}

/// 在 `[base, base + buf.len())` 这段连续字节上推进游标，返回本次扫描到的
/// **最后一个 commit frame 的结束偏移**（绝对偏移；没有 commit frame 时返回 `None`）。
///
/// 约定：
/// * 调用方保证 `base <= cursor.offset <= base + buf.len()`：`pending` 缓冲区必须从
///   上一次解析位置开始连续，不能有空洞（有空洞时调用方必须先补齐字节）。
/// * 只有**完整**的头 / 帧才会推进游标；半截数据保留给下一次调用。
/// * 非 commit 帧不会产生返回值，因此非 commit 帧永远不会触发远程 append。
pub fn scan_frames(
    cursor: &mut WalCursor,
    base: u64,
    buf: &[u8],
) -> Result<Option<u64>, WalFormatError> {
    debug_assert!(cursor.offset >= base, "游标不能早于缓冲区起点");
    let available = base + buf.len() as u64;
    let mut last_commit: Option<u64> = None;

    loop {
        if cursor.offset >= available {
            break;
        }
        let rel = (cursor.offset - base) as usize;
        if cursor.expect_header {
            if available - cursor.offset < WAL_HEADER_SIZE as u64 {
                // 头还没写全（可能被拆成两次 pwrite），等下一次
                break;
            }
            let header = &buf[rel..rel + WAL_HEADER_SIZE];
            let magic = read_u32_be(&header[0..4]);
            if !is_wal_magic(magic) {
                return Err(WalFormatError::BadMagic {
                    offset: cursor.offset,
                    magic,
                });
            }
            let page_size = read_u32_be(&header[8..12]);
            if !is_valid_page_size(page_size) {
                return Err(WalFormatError::BadPageSize {
                    offset: cursor.offset,
                    page_size,
                });
            }
            cursor.page_size = page_size;
            cursor.expect_header = false;
            cursor.offset += WAL_HEADER_SIZE as u64;
            continue;
        }

        let Some(frame_size) = cursor.frame_size() else {
            // 还不知道页大小：必须等 WAL 头。调用方负责先补齐/探测页大小。
            break;
        };
        if available - cursor.offset < frame_size {
            break;
        }
        let frame_header = parse_frame_header(&buf[rel..rel + WAL_FRAME_HEADER_SIZE])
            .expect("长度已检查，帧头必然可解析");
        cursor.offset += frame_size;
        if frame_header.is_commit() {
            last_commit = Some(cursor.offset);
        }
    }

    Ok(last_commit)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wal_header(page_size: u32) -> Vec<u8> {
        let mut header = vec![0u8; WAL_HEADER_SIZE];
        header[0..4].copy_from_slice(&WAL_MAGIC_LE.to_be_bytes());
        header[4..8].copy_from_slice(&3007000u32.to_be_bytes());
        header[8..12].copy_from_slice(&page_size.to_be_bytes());
        header
    }

    fn frame(page_number: u32, db_size: u32, page_size: u32) -> Vec<u8> {
        let mut frame = vec![0xABu8; WAL_FRAME_HEADER_SIZE + page_size as usize];
        frame[0..4].copy_from_slice(&page_number.to_be_bytes());
        frame[4..8].copy_from_slice(&db_size.to_be_bytes());
        frame
    }

    #[test]
    fn only_commit_frame_produces_boundary() {
        let page_size = 512u32;
        let mut bytes = wal_header(page_size);
        // 两个非 commit 帧（db_size = 0）
        bytes.extend_from_slice(&frame(1, 0, page_size));
        bytes.extend_from_slice(&frame(2, 0, page_size));
        // 一个 commit 帧
        bytes.extend_from_slice(&frame(2, 3, page_size));
        // 提交边界 = 最后一个 commit frame 的**结束偏移**（必须在上面的 commit 帧写入之后取）。
        let expected_commit_end = bytes.len() as u64;

        let mut cursor = WalCursor::new();
        let boundary = scan_frames(&mut cursor, 0, &bytes).expect("合法 WAL 应当解析成功");
        assert_eq!(
            boundary,
            Some(expected_commit_end),
            "只有 commit frame 才能产生提交边界"
        );
        assert_eq!(cursor.page_size, page_size);
        assert_eq!(cursor.offset, bytes.len() as u64);
    }

    #[test]
    fn non_commit_frames_alone_never_produce_boundary() {
        let page_size = 1024u32;
        let mut bytes = wal_header(page_size);
        bytes.extend_from_slice(&frame(1, 0, page_size));
        bytes.extend_from_slice(&frame(2, 0, page_size));

        let mut cursor = WalCursor::new();
        let boundary = scan_frames(&mut cursor, 0, &bytes).expect("合法 WAL 应当解析成功");
        assert_eq!(boundary, None, "非 commit 帧不得触发远程 append");
    }

    #[test]
    fn partial_frame_waits_for_more_bytes() {
        let page_size = 512u32;
        let mut bytes = wal_header(page_size);
        let commit_frame = frame(7, 9, page_size);
        let commit_end = (bytes.len() + commit_frame.len()) as u64;
        bytes.extend_from_slice(&commit_frame[..commit_frame.len() - 10]);

        let mut cursor = WalCursor::new();
        assert_eq!(scan_frames(&mut cursor, 0, &bytes).unwrap(), None);
        let parsed_after_partial = cursor.offset;
        assert_eq!(parsed_after_partial, WAL_HEADER_SIZE as u64);

        // 补齐剩下的字节后必须能识别出 commit 边界
        bytes.extend_from_slice(&commit_frame[commit_frame.len() - 10..]);
        assert_eq!(
            scan_frames(&mut cursor, 0, &bytes).unwrap(),
            Some(commit_end)
        );
    }

    #[test]
    fn header_written_in_two_parts_is_reassembled() {
        let page_size = 4096u32;
        let header = wal_header(page_size);
        let mut cursor = WalCursor::new();
        assert_eq!(scan_frames(&mut cursor, 0, &header[..16]).unwrap(), None);
        assert_eq!(cursor.offset, 0, "半截头不得推进游标");
        assert_eq!(scan_frames(&mut cursor, 0, &header).unwrap(), None);
        assert_eq!(cursor.offset, WAL_HEADER_SIZE as u64);
        assert_eq!(cursor.page_size, page_size);
    }

    #[test]
    fn non_zero_base_offset_is_respected() {
        let page_size = 512u32;
        let mut cursor = WalCursor::new();
        cursor.resume_at(4096, page_size);
        let commit_frame = frame(3, 5, page_size);
        let boundary = scan_frames(&mut cursor, 4096, &commit_frame).unwrap();
        assert_eq!(boundary, Some(4096 + commit_frame.len() as u64));
    }

    #[test]
    fn bad_magic_is_rejected() {
        let mut bytes = wal_header(512);
        bytes[0] = 0x00;
        let mut cursor = WalCursor::new();
        assert!(matches!(
            scan_frames(&mut cursor, 0, &bytes),
            Err(WalFormatError::BadMagic { .. })
        ));
    }

    #[test]
    fn bad_page_size_is_rejected() {
        let mut bytes = wal_header(512);
        bytes[8..12].copy_from_slice(&1000u32.to_be_bytes());
        let mut cursor = WalCursor::new();
        assert!(matches!(
            scan_frames(&mut cursor, 0, &bytes),
            Err(WalFormatError::BadPageSize { .. })
        ));
        assert!(!is_valid_page_size(1000));
        assert!(is_valid_page_size(65536));
        assert!(!is_valid_page_size(131072));
    }

    #[test]
    fn truncate_resets_cursor_semantics() {
        let mut cursor = WalCursor::new();
        cursor.resume_at(8192, 4096);
        cursor.truncate_to(0);
        assert!(cursor.expect_header, "truncate 到 0 后必须重新等待 WAL 头");
        assert_eq!(cursor.page_size, 0);
        assert_eq!(cursor.offset, 0);

        cursor.resume_at(8192, 4096);
        cursor.truncate_to(WAL_HEADER_SIZE as u64);
        assert!(!cursor.expect_header);
        assert_eq!(cursor.offset, WAL_HEADER_SIZE as u64);
        assert_eq!(cursor.page_size, 4096, "只截掉帧时页大小仍然有效");
    }

    #[test]
    fn page_size_is_read_from_header_not_guessed() {
        let bytes = wal_header(65536);
        assert_eq!(parse_wal_header_page_size(&bytes), Some(65536));
        assert_eq!(parse_wal_header_page_size(&bytes[..31]), None);
    }
}
