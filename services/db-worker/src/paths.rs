//! 本地路径约定（Worker 与 DB Process 共享的布局）。
//!
//! ```text
//! WORKER_DATA_DIR/<db_id>/                    DB 工作集根目录
//!   ├── db                                    主库文件（db-runtime 的 `--db-path`）
//!   ├── db-wal                                本地 WAL（db-runtime 的 `--wal-path`）
//!   ├── .worker-work-set.json                 Worker 记录的工作集元数据（epoch/snapshot/lsn）
//!   └── .worker-snapshots.json                Snapshot ledger（本 Worker 已知的最近快照）
//! WORKER_RUN_DIR/sockets/<db_id>.sock         本地 UDS（db-runtime 监听，Dispatcher 连接）
//! ```
//!
//! **主库 `db` 是文件，不是目录**：引擎把 `--db-path` 当主库**文件**打开，并按
//! `<db_path>-wal` 打开 WAL（Turso/SQLite 约定）。历史版本把 `db` 建成目录，导致
//! spawn 出来的 DB Process 在 open 时立刻 `EISDIR` 退出（"is a directory"）。
//! 布局里凡是本进程自己创建目录的地方，都只能建到 `<db_id>/` 这一层。
//!
//! **WAL 的位置不是自由选择**：`--wal-path` 必须等于 `<db_path>-wal`。Worker 回放的
//! WAL 字节与 db-runtime 播种（`seed_wal_stream`）都写这个路径，而引擎只会打开
//! 它自己推导出的 `<db_path>-wal`；两者不一致时，恢复回放的数据对引擎不可见。
//!
//! **id 消毒**：`db_id` 来自控制面（不可信输入），直接拼进路径会被 `../` 逃逸出
//! 数据目录。所有落盘路径必须经过 [`sanitize_id`]。真实 db_id 是 UUID，消毒是恒等
//! 变换；它保护的是「id 被构造成恶意字符串」的情况。

use std::path::{Path, PathBuf};

use crate::error::{Result, WorkerError};

/// 主库文件名（`<db_id>/db`；worker 把它当**文件**传给 db-runtime 的 `--db-path`）。
pub const DB_FILE_NAME: &str = "db";
/// 本地 WAL 的文件名（`<db_id>/db-wal`）：[`DB_FILE_NAME`] + [`WAL_SUFFIX`]。
///
/// 快照 manifest 里 WAL 条目的 `relative_path` 必须取这个值：下载端按
/// `relative_path` 原样还原文件，而引擎只会打开它自己推导出的 `<db_path>-wal`，
/// 写成逻辑名（例如 `wal`）会让恢复出来的字节对引擎不可见。
pub const WAL_FILE_NAME: &str = "db-wal";
/// 本地 WAL 的文件名后缀（`<db_path>-wal`，与引擎 `wal_path_for` 的推导一致）。
pub const WAL_SUFFIX: &str = "-wal";
/// WAL 共享内存索引的文件名后缀（`<db_path>-shm`，引擎自行维护的派生物）。
pub const SHM_SUFFIX: &str = "-shm";
/// 工作集元数据文件名（Worker 私有，不上传快照）。
pub const WORK_SET_META_FILE: &str = ".worker-work-set.json";
/// Snapshot ledger 文件名（Worker 私有，不上传快照）。
pub const SNAPSHOT_LEDGER_FILE: &str = ".worker-snapshots.json";
/// 快照上传用临时文件（冻结的 WAL 前缀）的文件名前缀：`.snapshot-<snapshot_id>.wal`。
///
/// 落下临时文件而不是直接上传活的 `db-wal`：快照点之后引擎仍会向 WAL 追加，直接按
/// 文件上传会把「尚未 quorum durable 的字节」也带上（见 `restore::freeze_wal_prefix`）。
pub const FROZEN_WAL_PREFIX: &str = ".snapshot-";
/// 冻结 WAL 临时文件的后缀。
pub const FROZEN_WAL_SUFFIX: &str = ".wal";
/// UDS socket 子目录名。
pub const SOCKET_SUBDIR: &str = "sockets";

/// 消毒一个 id，使其可安全用作单层路径/目录名。
///
/// 规则：仅保留 `[A-Za-z0-9._-]`，其余字符替换为 `_`；空串或全为点的结果回落到
/// `invalid-id`。`.` 与 `..` 显式拒绝（目录穿越）。
pub fn sanitize_id(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() || out == "." || out == ".." || out.chars().all(|c| c == '.') {
        return "invalid-id".to_string();
    }
    // 路径分隔符已被替换，因此这里再做一次断言式检查，避免未来放宽字符集时失效
    if out.contains('/') || out.contains('\\') {
        return "invalid-id".to_string();
    }
    out
}

/// 校验 db_id 非空；返回消毒后的可安全拼接值。
pub fn checked_id(db_id: &str) -> Result<String> {
    if db_id.trim().is_empty() {
        return Err(WorkerError::Config("database_id 不能为空".into()));
    }
    Ok(sanitize_id(db_id))
}

/// DB 工作集根目录：`<data_dir>/<db_id>`。
pub fn db_dir(data_dir: &Path, db_id: &str) -> PathBuf {
    data_dir.join(sanitize_id(db_id))
}

/// engine 主库文件：`<data_dir>/<db_id>/db`（db-runtime 的 `--db-path`）。
pub fn db_path(data_dir: &Path, db_id: &str) -> PathBuf {
    db_dir(data_dir, db_id).join(DB_FILE_NAME)
}

/// 本地 WAL 文件：`<data_dir>/<db_id>/db-wal`（db-runtime 的 `--wal-path`）。
///
/// 必须由主库路径 + [`WAL_SUFFIX`] 推导（见模块文档）：引擎打开的就是这个路径。
pub fn wal_path(data_dir: &Path, db_id: &str) -> PathBuf {
    let mut path = db_path(data_dir, db_id).into_os_string();
    path.push(WAL_SUFFIX);
    PathBuf::from(path)
}

/// 工作集元数据文件路径。
pub fn work_set_meta_path(data_dir: &Path, db_id: &str) -> PathBuf {
    db_dir(data_dir, db_id).join(WORK_SET_META_FILE)
}

/// 冻结 WAL 临时文件路径：`<data_dir>/<db_id>/.snapshot-<snapshot_id>.wal`。
///
/// 与源文件同目录（同一文件系统，复制不跨设备），且 [`is_worker_private_file`] 会把它
/// 排除在快照内容之外 —— 否则并发快照可能把另一份临时文件也传上去。
pub fn frozen_wal_path(data_dir: &Path, db_id: &str, snapshot_id: &str) -> PathBuf {
    db_dir(data_dir, db_id).join(format!(
        "{FROZEN_WAL_PREFIX}{}{FROZEN_WAL_SUFFIX}",
        sanitize_id(snapshot_id)
    ))
}

/// Snapshot ledger 路径。
pub fn snapshot_ledger_path(data_dir: &Path, db_id: &str) -> PathBuf {
    db_dir(data_dir, db_id).join(SNAPSHOT_LEDGER_FILE)
}

/// 本地 UDS 路径：`<run_dir>/sockets/<db_id>.sock`。
///
/// 注意 UDS 路径长度上限（Linux 约 108 字节含结尾 NUL），由
/// [`socket_path_fits`] 在启动时校验，避免运行期才报 `EINVAL`。
pub fn socket_path(run_dir: &Path, db_id: &str) -> PathBuf {
    run_dir
        .join(SOCKET_SUBDIR)
        .join(format!("{}.sock", sanitize_id(db_id)))
}

/// UDS 路径是否在平台允许的长度内。
pub fn socket_path_fits(path: &Path) -> bool {
    path.as_os_str().len() < 108
}

/// Worker 私有的元数据文件（不上传快照、不参与 WAL 回放）。
///
/// 除两份固定元数据外，还包括快照上传期的临时文件（`FROZEN_WAL_PREFIX` 前缀）：
/// 它是**另一份** WAL 字节的派生物，属于一次快照的内部状态，不是工作集内容。
pub fn is_worker_private_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    matches!(name, WORK_SET_META_FILE | SNAPSHOT_LEDGER_FILE) || name.starts_with(FROZEN_WAL_PREFIX)
}

/// 引擎派生的本地文件（`db-wal` / `db-shm`）：不属于快照内容。
///
/// Remote WAL 才是 WAL 的权威副本，本地 WAL 只是回放派生物；`-shm` 更是由 WAL 内容
/// 导出的索引，上传它只会让恢复端拿到对不上的索引。
pub fn is_engine_derived_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let Some(stem) = name
        .strip_suffix(WAL_SUFFIX)
        .or_else(|| name.strip_suffix(SHM_SUFFIX))
    else {
        return false;
    };
    stem == DB_FILE_NAME
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_blocks_traversal() {
        assert_eq!(sanitize_id("db-1"), "db-1");
        assert_eq!(sanitize_id("../../etc/passwd"), ".._.._etc_passwd");
        assert_eq!(sanitize_id(".."), "invalid-id");
        assert_eq!(sanitize_id("."), "invalid-id");
        assert_eq!(sanitize_id(""), "invalid-id");
        assert_eq!(sanitize_id("..."), "invalid-id");
        assert_eq!(sanitize_id("a/b\\c"), "a_b_c");
        assert_eq!(sanitize_id("db id\u{0}"), "db_id_");
    }

    #[test]
    fn traversal_cannot_escape_data_dir() {
        let root = Path::new("/var/lib/db-platform");
        let escaped = db_dir(root, "../../etc");
        assert!(escaped.starts_with(root), "路径逃逸：{escaped:?}");
        assert_eq!(escaped, root.join(".._.._etc"));
    }

    #[test]
    fn layout_is_stable() {
        let data = Path::new("/data");
        let run = Path::new("/run");
        assert_eq!(db_dir(data, "db-1"), Path::new("/data/db-1"));
        assert_eq!(db_path(data, "db-1"), Path::new("/data/db-1/db"));
        // WAL 必须紧跟主库路径（引擎按 <db_path>-wal 打开它）
        assert_eq!(wal_path(data, "db-1"), Path::new("/data/db-1/db-wal"));
        assert_eq!(
            wal_path(data, "db-1").file_name().unwrap(),
            WAL_FILE_NAME,
            "快照 manifest 用的 WAL relative_path 必须与引擎实际打开的文件名一致"
        );
        assert_eq!(
            socket_path(run, "db-1"),
            Path::new("/run/sockets/db-1.sock")
        );
        assert!(is_worker_private_file(&work_set_meta_path(data, "db-1")));
        assert!(!is_worker_private_file(Path::new("/data/db-1/db/main.db")));
    }

    #[test]
    fn frozen_wal_temp_file_is_private_and_stays_in_db_dir() {
        let data = Path::new("/data");
        let frozen = frozen_wal_path(data, "db-1", "db-1-e1-42");
        assert_eq!(frozen, Path::new("/data/db-1/.snapshot-db-1-e1-42.wal"));
        // 临时文件既不能进快照（私有），也不能被当成引擎的 WAL 派生物
        assert!(is_worker_private_file(&frozen));
        assert!(!is_engine_derived_file(&frozen));
        // snapshot_id 来自上一跳，必须消毒后才能拼进文件名
        let evil = frozen_wal_path(data, "db-1", "../../escape");
        assert!(evil.starts_with(db_dir(data, "db-1")), "路径逃逸：{evil:?}");
    }

    #[test]
    fn engine_derived_files_are_recognized() {
        assert!(is_engine_derived_file(Path::new("/data/db-1/db-wal")));
        assert!(is_engine_derived_file(Path::new("/data/db-1/db-shm")));
        // 主库本体、以及其它前缀的 -wal / -shm 都不算「引擎派生物」
        assert!(!is_engine_derived_file(Path::new("/data/db-1/db")));
        assert!(!is_engine_derived_file(Path::new("/data/db-1/other-wal")));
        assert!(!is_engine_derived_file(Path::new("/data/db-1")));
    }

    #[test]
    fn socket_path_length_is_checkable() {
        assert!(socket_path_fits(Path::new(
            "/run/db-platform/sockets/db.sock"
        )));
        let long = format!("/run/{}/db.sock", "x".repeat(200));
        assert!(!socket_path_fits(Path::new(&long)));
    }
}
