//! 本地 WAL 恢复（failover 冷启动，架构 §11.2 / §15.1）。
//!
//! 恢复的目标只有一句话：**让本地 WAL 与 Remote WAL 逐字节一致**，这样引擎重新打开
//! 数据库时看到的就是「最后一次 quorum durable 提交之后」的状态，不会凭空多出或丢失事务。
//!
//! ```text
//! Remote WAL（唯一可信来源）                本地（重建目标）
//!   read_range 返回 [WalSegment]   ──►   <db_path>-wal
//!     start_lsn / file_offset              按 file_offset 原样写回
//!     reset_wal / data                     遇到 reset_wal 先截断，开启新一代
//! ```
//!
//! 三条纪律：
//!
//! 1. **偏移即事实**：段只能写到它声明的 `file_offset`。远端段之间出现空洞，说明我们
//!    手上的字节流不完整；这时宁可让恢复失败（DB 停在恢复中），也不能跳着写 ——
//!    否则本地 WAL 会包含一个「缺了中间帧」的事务，引擎读出来的页是错误的。
//! 2. **按 LSN 顺序落盘**：`file_offset` 的连续性只有在段按 `start_lsn` 升序处理时才成立，
//!    因此这里自己排序，不假设调用方的顺序。
//! 3. **先清干净再写**：上一代的 WAL / SHM 残留必须删除（见
//!    [`prepare_local_wal_for_restore`]），否则新字节会与旧字节混在同一文件里。

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use domain::error::{ErrorCode, PlatformError, Result};
use wal_client::WalSegment;

use crate::durable::WAL_SUFFIX;
use crate::wal_frames::{
    is_valid_page_size, is_wal_magic, MAX_PAGE_SIZE, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE,
};

/// `snapshot_files` 里主库文件的逻辑名。
pub const SNAPSHOT_KEY_DB: &str = "db";
/// `snapshot_files` 里本地 WAL 的逻辑名。
pub const SNAPSHOT_KEY_WAL: &str = "wal";

/// WAL header 里 checkpoint 世代号（`checkpoint_seq`）的偏移（大端 u32）。
///
/// 每次 WAL 重启（checkpoint 已经把全部帧回填进主库文件之后的 `restart`）都会 +1。
/// 因此 `0` 的唯一含义是：**这份 WAL 是该库创建以来的第一代，没有 checkpoint 把页面
/// 提前搬进主库文件** —— 恢复路径据此判断「纯 WAL 是否覆盖整库历史」。
const WAL_CHECKPOINT_SEQ_OFFSET: usize = 12;

/// 数据库主文件对应的本地 WAL 路径。
///
/// Turso/SQLite 约定：`<db_path>` 的 WAL 是 `<db_path>-wal`（同一个目录、同一个文件名前缀）。
#[must_use]
pub fn wal_path_for(db_path: &Path) -> PathBuf {
    // 用 OsString 拼接而不是格式化字符串：非 UTF-8 路径也不能在这里被静默改写。
    let mut path = db_path.as_os_str().to_os_string();
    path.push(WAL_SUFFIX);
    PathBuf::from(path)
}

/// 恢复前需要处理的本地文件清单：`(逻辑名, 路径)`。
///
/// 只返回**真实存在**的文件：不存在的文件交给恢复流程创建，列出来只会让调用方多做一次
/// 无用的搬运。逻辑名用于日志与对象命名，取值见 [`SNAPSHOT_KEY_DB`] / [`SNAPSHOT_KEY_WAL`]。
#[must_use]
pub fn snapshot_files(db_path: &Path) -> Vec<(String, PathBuf)> {
    let mut files = Vec::new();
    if db_path.is_file() {
        files.push((SNAPSHOT_KEY_DB.to_string(), db_path.to_path_buf()));
    }
    let wal_path = wal_path_for(db_path);
    if wal_path.is_file() {
        files.push((SNAPSHOT_KEY_WAL.to_string(), wal_path));
    }
    files
}

/// 清理上一代残留，为回放做准备（幂等：文件不存在时什么也不做）。
///
/// 除了 WAL 本身还要删 `-shm`：SHM 是由上一代 WAL 内容导出的共享内存索引，只删 WAL
/// 会把这份索引与新写回的 WAL 头对不上，引擎可能据此读到错误的页。恢复是「推倒重来」，
/// 两份都清掉才是安全的起点。
pub fn prepare_local_wal_for_restore(wal_path: &Path) -> Result<()> {
    remove_if_exists(wal_path)?;
    remove_if_exists(&shm_path_for(wal_path))?;
    Ok(())
}

/// 主库文件与本地 WAL 的配对状态（[`ensure_base_db_for_wal`] 的结果）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BaseDbState {
    /// 主库文件已有完整的首页：引擎可以直接把 WAL 应用到它上面。
    AlreadyPaged {
        /// 主库文件当前长度（字节）。
        len: u64,
    },
    /// 本地 WAL 没有可用帧（新建库 / 空 WAL）：没有需要修复的配对。
    NoWalFrames,
    /// 主库原来为空（或只有一个被写坏的半页），已用 WAL 里的 page 1 重建首页。
    Rebuilt {
        /// 重建所用的页大小（取自 WAL header）。
        page_size: u32,
        /// 提供 page 1 镜像的那一帧在 WAL 文件里的偏移。
        frame_offset: u64,
    },
}

/// 恢复的最后一道保险：让「主库文件 + 本地 WAL」成为引擎能接受的组合。
///
/// # 为什么必须有这一步
///
/// Turso/SQLite 有一条硬规则（turso_core `database.rs` 的 orphan-WAL 判定）：**WAL 里
/// 有帧、而主库文件是零页时，引擎判定「这份 WAL 不属于该库」并把它删掉**。
/// 平台的「只有 Remote WAL、没有快照」恢复路径恰好会造出这个组合 —— Worker 只把 WAL
/// 字节回放进 `db-wal`，主库文件（page 1，也就是库头）从来没人创建（它在旧节点上，
/// 是引擎自己 `allocate_page1` 写进 `db` 的，不在 Remote WAL 里）。结果就是：远端
/// quorum 已经 durable 的事务，在新节点上被引擎连 WAL 一起丢弃（RPO 违规）。
///
/// 修复动作只有一个：主库为空、而 WAL 有帧时，把 WAL 里 page 1 那一帧的页面镜像写进
/// 主库文件。WAL 的帧携带**完整页数据**（含 page 1 即库头），且 page size 与 WAL header
/// 自洽，因此重建出来的库头一定和这份 WAL 匹配。
///
/// # 安全边界（宁可不启动，也不给一个「看起来成功」的空库）
///
/// 1. **只在纯 WAL 覆盖整库历史时才重建**：WAL header 的 checkpoint 世代号必须为 0。
///    非 0 说明这份 WAL 是 checkpoint 重启后的新一代，checkpoint 之前的页面只存在于主库
///    文件里（是 checkpoint 把它们搬进去的，WAL 里不会再有）；此时用 page 1 拼出来的库
///    会缺页 —— 那种「成功」是假的，必须直接失败。
/// 2. **找不到 page 1 帧就失败**：没有库头就没有库，宁可启动失败也不能凭空造一个。
/// 3. **WAL header 非法就失败**：本地文件已不可信，任何猜测都是拿数据冒险。
///
/// 三条边界都不会降低可用性下限：这三个分支在没有本函数时**必然**以「引擎丢弃 WAL、
/// 库变成空的」收尾（见本函数文档第一段），失败在这里比那样的「成功」更接近真相。
///
/// # Errors
///
/// 以上 1~3 任一不满足，或本地文件读写失败时返回错误。
pub fn ensure_base_db_for_wal(db_path: &Path, wal_path: &Path) -> Result<BaseDbState> {
    // page size 的上界是 65536：主库文件只要有这么多字节，就一定含有一个完整首页，
    // 无需再去看 WAL（恢复路径上绝大多数情况都走这条快速路径）。
    let db_len = file_len_if_exists(db_path)?;
    if db_len >= u64::from(MAX_PAGE_SIZE) {
        return Ok(BaseDbState::AlreadyPaged { len: db_len });
    }

    let mut wal = match OpenOptions::new().read(true).open(wal_path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(BaseDbState::NoWalFrames)
        }
        Err(err) => return Err(storage_error(wal_path, &err)),
    };
    let mut header = [0u8; WAL_HEADER_SIZE];
    if wal.read_exact(&mut header).is_err() {
        // 比 WAL header 还短：不可能含任何帧。
        return Ok(BaseDbState::NoWalFrames);
    }
    let page_size = read_u32_be(&header, 8);
    if !is_wal_magic(read_u32_be(&header, 0)) || !is_valid_page_size(page_size) {
        return Err(broken_wal_state(
            wal_path,
            format!("WAL header 非法（magic/page_size 不合法，page_size={page_size}）"),
        ));
    }
    let frame_size = (WAL_FRAME_HEADER_SIZE + page_size as usize) as u64;
    let wal_len = file_len_if_exists(wal_path)?;
    if wal_len < WAL_HEADER_SIZE as u64 + frame_size {
        return Ok(BaseDbState::NoWalFrames);
    }
    // 主库文件已经有完整首页（或更长）：引擎自己能处理，不要动它。
    if db_len >= u64::from(page_size) {
        return Ok(BaseDbState::AlreadyPaged { len: db_len });
    }

    let generation = read_u32_be(&header, WAL_CHECKPOINT_SEQ_OFFSET);
    if generation != 0 {
        return Err(broken_wal_state(
            wal_path,
            format!(
                "本地 WAL 是 checkpoint 之后重启的第 {generation} 代（主库为空时它不含 checkpoint 之前的页面）"
            ),
        ));
    }

    let frame_offset = first_page1_frame_offset(&mut wal, wal_path, wal_len, frame_size)?
        .ok_or_else(|| {
            broken_wal_state(
                wal_path,
                "本地 WAL 里没有 page 1 帧，无法重建库头（该 WAL 不覆盖整库历史）".to_string(),
            )
        })?;

    let page = read_frame_page(&mut wal, wal_path, frame_offset, page_size)?;
    write_base_page(db_path, &page)?;
    tracing::warn!(
        db_path = %db_path.display(),
        wal_path = %wal_path.display(),
        page_size,
        frame_offset,
        "主库文件为空而本地 WAL 有帧：已用 WAL 的 page 1 重建库头（否则引擎会丢弃整份 WAL）"
    );
    Ok(BaseDbState::Rebuilt {
        page_size,
        frame_offset,
    })
}

/// 顺序扫描帧，返回第一帧 `page_number == 1` 的偏移。
///
/// 「第一帧」而不是「最后一帧」：恢复出的本地 WAL 从文件的第一个字节就是可信流，
/// 越早的帧越不可能落在被截断/未提交的尾部；主库首页只是给引擎一个合法库头，
/// 读取仍然以 WAL 帧为准（引擎对任何在 WAL 里有帧的页面都从 WAL 读）。
fn first_page1_frame_offset(
    wal: &mut std::fs::File,
    wal_path: &Path,
    wal_len: u64,
    frame_size: u64,
) -> Result<Option<u64>> {
    let mut frame_header = [0u8; WAL_FRAME_HEADER_SIZE];
    let mut frame_offset = WAL_HEADER_SIZE as u64;
    while frame_offset + frame_size <= wal_len {
        wal.seek(SeekFrom::Start(frame_offset))
            .map_err(|err| storage_error(wal_path, &err))?;
        wal.read_exact(&mut frame_header)
            .map_err(|err| storage_error(wal_path, &err))?;
        match read_u32_be(&frame_header, 0) {
            // page_number == 0 是非法帧：帧链到此结束，后面都是残留字节。
            0 => return Ok(None),
            1 => return Ok(Some(frame_offset)),
            _ => frame_offset += frame_size,
        }
    }
    Ok(None)
}

/// 读出某一帧携带的页面镜像（帧头之后紧跟 `page_size` 字节的完整页）。
fn read_frame_page(
    wal: &mut std::fs::File,
    wal_path: &Path,
    frame_offset: u64,
    page_size: u32,
) -> Result<Vec<u8>> {
    wal.seek(SeekFrom::Start(frame_offset + WAL_FRAME_HEADER_SIZE as u64))
        .map_err(|err| storage_error(wal_path, &err))?;
    let mut page = vec![0u8; page_size as usize];
    wal.read_exact(&mut page)
        .map_err(|err| storage_error(wal_path, &err))?;
    Ok(page)
}

/// 就地写主库首页（先截断到 0 再写整页）。
///
/// 之所以允许覆盖：调用点已经确认主库文件是空的，或者是**比一页还短的半页**
/// （上一次重建被崩溃打断留下的残片）—— 两种情况下文件里都没有可用内容。
/// 写完 `sync_all`：恢复结果必须在引擎打开它之前真正落盘。
fn write_base_page(db_path: &Path, page: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(db_path)
        .map_err(|err| storage_error(db_path, &err))?;
    file.write_all(page)
        .map_err(|err| storage_error(db_path, &err))?;
    file.sync_all().map_err(|err| storage_error(db_path, &err))
}

/// 文件长度；不存在时返回 0。
fn file_len_if_exists(path: &Path) -> Result<u64> {
    match std::fs::metadata(path) {
        Ok(meta) => Ok(meta.len()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(err) => Err(storage_error(path, &err)),
    }
}

/// 大端读取 4 字节（越界返回 0：调用点都已经先把 header 填满）。
fn read_u32_be(bytes: &[u8], offset: usize) -> u32 {
    let Some(slice) = bytes.get(offset..offset + 4) else {
        return 0;
    };
    u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]])
}

/// 「主库与 WAL 无法配对」类错误：重试同样的本地文件只会得到同样结果。
fn broken_wal_state(wal_path: &Path, reason: String) -> PlatformError {
    PlatformError::new(
        ErrorCode::InvalidArgument,
        format!(
            "主库文件为空且没有可用的基础数据（{}）：{reason}；拒绝以空库启动",
            wal_path.display()
        ),
    )
}

/// 按 `file_offset` 把 WAL 段写回本地文件，返回**写入结束偏移**。
///
/// 处理规则（任何一条不满足都直接报错，不做「跳过空洞」这类猜测）：
///
/// 1. 段按 `start_lsn` 升序处理 —— 调用方乱序传入也能得到正确结果，且不会因为顺序
///    不同而写出不同的文件；
/// 2. `reset_wal = true` 的段先截断文件到 0（开启新一代 WAL），写入末端随之归零；
/// 3. 每段的 `file_offset` 必须等于当前写入末端：**大于**末端是空洞，**小于**末端是重叠，
///    两者都意味着本地 WAL 与 Remote WAL 无法逐字节对应，只能失败。
///
/// 返回的偏移可以直接作为 `PlatformDurableIO` 恢复播种参数里的 `file_offset`。
pub fn replay_wal_segments(wal_path: &Path, segments: &[WalSegment]) -> Result<u64> {
    let mut ordered: Vec<&WalSegment> = segments.iter().collect();
    ordered.sort_by_key(|segment| segment.start_lsn);
    replay_ordered(wal_path, &ordered)
}

/// 只重放**最后一个** `reset_wal = true` 段及其之后的部分（该段即「本代起点」）。
///
/// 为什么需要它：`reset_wal` 意味着这一代 WAL 从偏移 0 重新开始（引擎重新初始化了
/// WAL 头）。更早的段属于上一代，它们的偏移已经被截断操作覆盖；重放它们只会先写后截，
/// 既浪费时间，又有把旧字节当成新数据的风险。没有 reset 段时等价于全量重放
/// （此时整份 Remote WAL 就是一代）。
pub fn replay_wal_segments_from_generation_start(
    wal_path: &Path,
    segments: &[WalSegment],
) -> Result<u64> {
    let mut ordered: Vec<&WalSegment> = segments.iter().collect();
    ordered.sort_by_key(|segment| segment.start_lsn);
    let generation_start = ordered
        .iter()
        .rposition(|segment| segment.reset_wal)
        .unwrap_or(0);
    replay_ordered(wal_path, &ordered[generation_start..])
}

/// 已按 LSN 排好序的回放主干。
fn replay_ordered(wal_path: &Path, segments: &[&WalSegment]) -> Result<u64> {
    let mut file = open_wal_for_restore(wal_path)?;
    // 起点是文件当前长度：这样「恢复到一半被打断后续传」也能接上；
    // 若首个段带 reset_wal，下面会先截断到 0，重新从偏移 0 开始。
    let mut written = file
        .metadata()
        .map_err(|err| storage_error(wal_path, &err))?
        .len();
    for segment in segments {
        if segment.reset_wal {
            file.set_len(0)
                .map_err(|err| storage_error(wal_path, &err))?;
            written = 0;
        }
        if segment.file_offset != written {
            return Err(inconsistent_segment(segment, written));
        }
        file.seek(SeekFrom::Start(segment.file_offset))
            .map_err(|err| storage_error(wal_path, &err))?;
        file.write_all(&segment.data)
            .map_err(|err| storage_error(wal_path, &err))?;
        written = segment.file_offset + segment.data.len() as u64;
    }
    // 先落盘再交给引擎：恢复出的 WAL 一旦被引擎读到，就必须已经在磁盘上。
    file.sync_all()
        .map_err(|err| storage_error(wal_path, &err))?;
    Ok(written)
}

/// 以「创建 + 读写」方式打开 WAL（父目录不存在时补建）。
fn open_wal_for_restore(wal_path: &Path) -> Result<std::fs::File> {
    if let Some(parent) = wal_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|err| storage_error(wal_path, &err))?;
        }
    }
    OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        // 不截断：文件里可能已有需要续传的前缀，由 reset_wal 段决定是否丢弃。
        .truncate(false)
        .open(wal_path)
        .map_err(|err| storage_error(wal_path, &err))
}

/// 本地 WAL 的 `-shm` 同伴文件路径。
fn shm_path_for(wal_path: &Path) -> PathBuf {
    let stripped = wal_path
        .as_os_str()
        .to_string_lossy()
        .strip_suffix(WAL_SUFFIX)
        .map_or_else(
            || wal_path.as_os_str().to_os_string(),
            |base| PathBuf::from(base).into_os_string(),
        );
    let mut path = stripped;
    path.push("-shm");
    PathBuf::from(path)
}

/// 删除文件；不存在时视为成功（清理是幂等的）。
fn remove_if_exists(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(storage_error(path, &err)),
    }
}

/// 段偏移不连续：区分「空洞」与「重叠」，因为两者的排障方向完全不同。
fn inconsistent_segment(segment: &WalSegment, expected: u64) -> PlatformError {
    let kind = if segment.file_offset > expected {
        "空洞"
    } else {
        "重叠"
    };
    // 用 InvalidArgument 而不是存储类错误码：本地 IO 没坏，坏的是回放输入本身，
    // 重试同样的输入只会得到同样的结果（继续写下去会让本地 WAL 与远端永久分叉）。
    PlatformError::new(
        ErrorCode::InvalidArgument,
        format!(
            "WAL 段不连续（{kind}）：start_lsn={} file_offset={} 但本地写入末端={expected}，拒绝静默跳过",
            segment.start_lsn, segment.file_offset
        ),
    )
}

/// 本地文件不可用（打不开 / 读不了 / 写不进）。
fn storage_error(path: &Path, err: &std::io::Error) -> PlatformError {
    PlatformError::new(
        ErrorCode::StorageUnavailable,
        format!("本地 WAL 恢复失败（{}）：{err}", path.display()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use domain::Lsn;

    fn segment(
        start_lsn: u64,
        file_offset: u64,
        reset_wal: bool,
        data: &'static [u8],
    ) -> WalSegment {
        WalSegment {
            start_lsn: Lsn::new(start_lsn),
            file_offset,
            reset_wal,
            data: Bytes::from_static(data),
        }
    }

    #[test]
    fn wal_path_appends_suffix() {
        assert_eq!(
            wal_path_for(Path::new("/var/lib/platform/demo.db")),
            PathBuf::from("/var/lib/platform/demo.db-wal")
        );
    }

    #[test]
    fn prepare_is_idempotent_and_removes_shm() {
        let dir = tempfile::tempdir().expect("临时目录");
        let wal_path = dir.path().join("demo.db-wal");
        let shm_path = dir.path().join("demo.db-shm");
        std::fs::write(&wal_path, b"stale").expect("写残留 WAL");
        std::fs::write(&shm_path, b"stale").expect("写残留 SHM");

        prepare_local_wal_for_restore(&wal_path).expect("清理残留");
        assert!(!wal_path.exists(), "残留 WAL 必须被删除");
        assert!(!shm_path.exists(), "残留 SHM 必须被删除");
        // 再调用一次（文件已不存在）也必须成功。
        prepare_local_wal_for_restore(&wal_path).expect("重复清理");
    }

    #[test]
    fn snapshot_files_lists_only_existing_files() {
        let dir = tempfile::tempdir().expect("临时目录");
        let db_path = dir.path().join("demo.db");
        assert!(
            snapshot_files(&db_path).is_empty(),
            "什么都不存在时不得虚报"
        );

        std::fs::write(&db_path, b"db").expect("写主库");
        let only_db = snapshot_files(&db_path);
        assert_eq!(only_db.len(), 1);
        assert_eq!(only_db[0].0, SNAPSHOT_KEY_DB);

        std::fs::write(wal_path_for(&db_path), b"wal").expect("写 WAL");
        let both = snapshot_files(&db_path);
        assert_eq!(both.len(), 2);
        assert_eq!(both[1].0, SNAPSHOT_KEY_WAL);
        assert_eq!(both[1].1, wal_path_for(&db_path));
    }

    #[test]
    fn replay_writes_segments_at_their_offsets() {
        let dir = tempfile::tempdir().expect("临时目录");
        let wal_path = dir.path().join("demo.db-wal");
        // 故意乱序传入：函数必须自己按 start_lsn 排序。
        let segments = vec![
            segment(4, 4, false, b"BBBB"),
            segment(0, 0, false, b"AAAA"),
            segment(8, 8, false, b"CCCC"),
        ];

        let end = replay_wal_segments(&wal_path, &segments).expect("回放");
        assert_eq!(end, 12, "返回写入结束偏移");
        assert_eq!(std::fs::read(&wal_path).expect("读回"), b"AAAABBBBCCCC");
    }

    #[test]
    fn replay_reset_segment_truncates_previous_generation() {
        let dir = tempfile::tempdir().expect("临时目录");
        let wal_path = dir.path().join("demo.db-wal");
        std::fs::write(&wal_path, b"OLD-GARBAGE").expect("写上一代残留");

        // 新一代 WAL：从偏移 0 重新开始，旧字节必须全部失效。
        let end = replay_wal_segments(&wal_path, &[segment(100, 0, true, b"NEW")]).expect("回放");
        assert_eq!(end, 3);
        assert_eq!(std::fs::read(&wal_path).expect("读回"), b"NEW");
    }

    #[test]
    fn replay_rejects_hole_and_overlap() {
        let dir = tempfile::tempdir().expect("临时目录");
        let wal_path = dir.path().join("demo.db-wal");

        let hole = vec![segment(0, 0, false, b"AAAA"), segment(4, 8, false, b"BBBB")];
        let err = replay_wal_segments(&wal_path, &hole).expect_err("空洞必须报错");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("空洞"), "实际：{}", err.message);

        let overlap = vec![segment(0, 0, false, b"AAAA"), segment(4, 2, false, b"BBBB")];
        let err = replay_wal_segments(&wal_path, &overlap).expect_err("重叠必须报错");
        assert!(err.message.contains("重叠"), "实际：{}", err.message);

        // 报错后不得留下「半个」文件内容（第一段已写入，但恢复结果是失败态，调用方不会采用）。
        assert!(wal_path.exists());
    }

    #[test]
    fn replay_from_generation_start_skips_older_generations() {
        let dir = tempfile::tempdir().expect("临时目录");
        let wal_path = dir.path().join("demo.db-wal");
        let segments = vec![
            segment(0, 0, true, b"AAAA"),
            segment(4, 4, false, b"BB"),
            // 新一代从这里开始：偏移归零。
            segment(6, 0, true, b"CC"),
            segment(8, 2, false, b"DD"),
        ];

        let end = replay_wal_segments_from_generation_start(&wal_path, &segments).expect("回放");
        assert_eq!(end, 4, "只保留最后一代：CC + DD");
        assert_eq!(std::fs::read(&wal_path).expect("读回"), b"CCDD");
    }

    #[test]
    fn replay_from_generation_start_without_reset_is_full_replay() {
        let dir = tempfile::tempdir().expect("临时目录");
        let wal_path = dir.path().join("demo.db-wal");
        let segments = vec![segment(0, 0, false, b"AAAA"), segment(4, 4, false, b"BB")];

        let end = replay_wal_segments_from_generation_start(&wal_path, &segments).expect("回放");
        assert_eq!(end, 6);
        assert_eq!(std::fs::read(&wal_path).expect("读回"), b"AAAABB");
    }

    // ---------------------------------------------------------- 库头重建

    const PAGE_SIZE: u32 = 4096;
    const FRAME_SIZE: usize = WAL_FRAME_HEADER_SIZE + PAGE_SIZE as usize;

    /// 构造一份最小 WAL：header + 每页一帧（页面内容按 page_number 填充，便于断言搬运）。
    fn wal_bytes(generation: u32, page_numbers: &[u32]) -> Vec<u8> {
        let mut out = vec![0u8; WAL_HEADER_SIZE];
        out[0..4].copy_from_slice(&crate::wal_frames::WAL_MAGIC_LE.to_be_bytes());
        out[8..12].copy_from_slice(&PAGE_SIZE.to_be_bytes());
        out[12..16].copy_from_slice(&generation.to_be_bytes());
        for page_number in page_numbers {
            let mut frame = vec![0u8; FRAME_SIZE];
            frame[0..4].copy_from_slice(&page_number.to_be_bytes());
            frame[4..8].copy_from_slice(&7u32.to_be_bytes()); // db_size：本帧是提交帧
            for (index, byte) in frame[WAL_FRAME_HEADER_SIZE..].iter_mut().enumerate() {
                *byte = (*page_number as usize + index) as u8;
            }
            out.extend_from_slice(&frame);
        }
        out
    }

    /// 第 `index` 帧携带的页面镜像（与 `wal_bytes` 的填充规则一致）。
    fn frame_page(page_number: u32) -> Vec<u8> {
        (0..PAGE_SIZE as usize)
            .map(|index| (page_number as usize + index) as u8)
            .collect()
    }

    /// 线上故障的回归：主库为空 + 本地 WAL 有帧 —— 必须用 WAL 的 page 1 重建库头，
    /// 否则引擎会认定「WAL 不属于这个库」并把它删掉（已提交的数据丢失）。
    #[test]
    fn rebuilds_empty_base_page_from_wal_page1_frame() {
        let dir = tempfile::tempdir().expect("临时目录");
        let db_path = dir.path().join("db");
        let wal_path = wal_path_for(&db_path);
        std::fs::write(&wal_path, wal_bytes(0, &[1, 2, 2])).expect("写 WAL");

        let state = ensure_base_db_for_wal(&db_path, &wal_path).expect("重建库头");
        assert_eq!(
            state,
            BaseDbState::Rebuilt {
                page_size: PAGE_SIZE,
                frame_offset: WAL_HEADER_SIZE as u64,
            }
        );
        assert_eq!(
            std::fs::read(&db_path).expect("读主库"),
            frame_page(1),
            "主库首页必须逐字节等于 WAL 里 page 1 帧的页面镜像"
        );
        // 幂等：再跑一次不得改动已经合法的库头。
        assert_eq!(
            ensure_base_db_for_wal(&db_path, &wal_path).expect("再跑一次"),
            BaseDbState::AlreadyPaged {
                len: u64::from(PAGE_SIZE)
            }
        );
        assert_eq!(std::fs::read(&db_path).expect("读主库"), frame_page(1));
    }

    /// `page_number == 1` 出现在后续帧（首帧是别的页）时也要能找到。
    #[test]
    fn finds_page1_frame_anywhere_in_the_stream() {
        let dir = tempfile::tempdir().expect("临时目录");
        let db_path = dir.path().join("db");
        let wal_path = wal_path_for(&db_path);
        std::fs::write(&wal_path, wal_bytes(0, &[3, 5, 1, 2])).expect("写 WAL");

        let state = ensure_base_db_for_wal(&db_path, &wal_path).expect("重建库头");
        assert_eq!(
            state,
            BaseDbState::Rebuilt {
                page_size: PAGE_SIZE,
                frame_offset: WAL_HEADER_SIZE as u64 + 2 * FRAME_SIZE as u64,
            }
        );
        assert_eq!(std::fs::read(&db_path).expect("读主库"), frame_page(1));
    }

    /// 主库已经有整页时不得动它（快照恢复出来的主库就是这样）。
    #[test]
    fn keeps_existing_base_page_untouched() {
        let dir = tempfile::tempdir().expect("临时目录");
        let db_path = dir.path().join("db");
        let wal_path = wal_path_for(&db_path);
        std::fs::write(&db_path, b"EXISTING-BASE").expect("写主库");
        std::fs::write(&wal_path, wal_bytes(0, &[1, 2])).expect("写 WAL");

        // 文件短于页大小：先补齐到一整页（模拟引擎写出的合法首页）。
        let mut base = std::fs::read(&db_path).expect("读主库");
        base.resize(PAGE_SIZE as usize, 7);
        std::fs::write(&db_path, &base).expect("写整页主库");

        assert_eq!(
            ensure_base_db_for_wal(&db_path, &wal_path).expect("无需修复"),
            BaseDbState::AlreadyPaged {
                len: u64::from(PAGE_SIZE)
            }
        );
        assert_eq!(std::fs::read(&db_path).expect("读主库"), base);
    }

    /// 上一次重建被崩溃打断留下的半页必须被整页覆盖（否则引擎会读到坏库头）。
    #[test]
    fn rewrites_torn_base_page() {
        let dir = tempfile::tempdir().expect("临时目录");
        let db_path = dir.path().join("db");
        let wal_path = wal_path_for(&db_path);
        std::fs::write(&db_path, b"SQL").expect("写半页");
        std::fs::write(&wal_path, wal_bytes(0, &[1, 2])).expect("写 WAL");

        assert!(matches!(
            ensure_base_db_for_wal(&db_path, &wal_path).expect("重建"),
            BaseDbState::Rebuilt { .. }
        ));
        assert_eq!(std::fs::read(&db_path).expect("读主库"), frame_page(1));
    }

    /// 没有 WAL / WAL 里放不下一帧：新建库的正常情形，什么都不做。
    #[test]
    fn empty_or_frame_less_wal_is_noop() {
        let dir = tempfile::tempdir().expect("临时目录");
        let db_path = dir.path().join("db");

        // 文件不存在。
        assert_eq!(
            ensure_base_db_for_wal(&db_path, &wal_path_for(&db_path)).expect("无 WAL"),
            BaseDbState::NoWalFrames
        );
        // 只有半截 header。
        let wal_path = wal_path_for(&db_path);
        std::fs::write(&wal_path, [0u8; 8]).expect("写半截 header");
        assert_eq!(
            ensure_base_db_for_wal(&db_path, &wal_path).expect("半截 header"),
            BaseDbState::NoWalFrames
        );
        // header 合法，但连一帧都放不下。
        let mut short = wal_bytes(0, &[]);
        short.truncate(WAL_HEADER_SIZE + 8);
        std::fs::write(&wal_path, &short).expect("写无帧 WAL");
        assert_eq!(
            ensure_base_db_for_wal(&db_path, &wal_path).expect("无帧"),
            BaseDbState::NoWalFrames
        );
        assert!(!db_path.exists(), "无事可做时不得凭空创建主库文件");
    }

    /// checkpoint 重启后的新一代 WAL 不含 checkpoint 之前的页面：宁可不启动，
    /// 也不能给出一个「缺页但看起来成功」的库。
    #[test]
    fn rejects_restarted_wal_generation() {
        let dir = tempfile::tempdir().expect("临时目录");
        let db_path = dir.path().join("db");
        let wal_path = wal_path_for(&db_path);
        std::fs::write(&wal_path, wal_bytes(1, &[1, 2])).expect("写第二代 WAL");

        let err = ensure_base_db_for_wal(&db_path, &wal_path).expect_err("必须拒绝");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("checkpoint"), "实际：{}", err.message);
        assert!(!db_path.exists(), "拒绝时不得写出主库文件");
    }

    /// WAL 里没有 page 1 帧（该 WAL 不覆盖整库历史）同样必须拒绝。
    #[test]
    fn rejects_wal_without_page1_frame() {
        let dir = tempfile::tempdir().expect("临时目录");
        let db_path = dir.path().join("db");
        let wal_path = wal_path_for(&db_path);
        std::fs::write(&wal_path, wal_bytes(0, &[2, 3, 2])).expect("写 WAL");

        let err = ensure_base_db_for_wal(&db_path, &wal_path).expect_err("必须拒绝");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("page 1"), "实际：{}", err.message);
    }

    /// WAL header 非法（本地文件已经不可信）时不得瞎猜。
    #[test]
    fn rejects_invalid_wal_header() {
        let dir = tempfile::tempdir().expect("临时目录");
        let db_path = dir.path().join("db");
        let wal_path = wal_path_for(&db_path);
        std::fs::write(&wal_path, vec![0xAB; FRAME_SIZE * 2]).expect("写垃圾");

        let err = ensure_base_db_for_wal(&db_path, &wal_path).expect_err("必须拒绝");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("WAL header"), "实际：{}", err.message);
    }

    /// 主库文件已经远超一页（任何 page size 下都够）：不去读 WAL，直接放行。
    #[test]
    fn large_base_file_short_circuits() {
        let dir = tempfile::tempdir().expect("临时目录");
        let db_path = dir.path().join("db");
        let wal_path = wal_path_for(&db_path);
        // WAL 故意写成垃圾：既然主库有页面，恢复路径就不该碰它。
        std::fs::write(&wal_path, b"GARBAGE").expect("写垃圾 WAL");
        std::fs::write(&db_path, vec![0u8; 1 << 20]).expect("写大主库");

        assert_eq!(
            ensure_base_db_for_wal(&db_path, &wal_path).expect("放行"),
            BaseDbState::AlreadyPaged { len: 1 << 20 }
        );
    }
}
