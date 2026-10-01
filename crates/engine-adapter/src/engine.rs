//! `EngineAdapter` / `EngineConnection` —— 平台侧唯一的引擎入口（架构 §17.3）。
//!
//! 本模块把 turso_core 的 `Database` / `Connection` / `Statement` 收敛成三个平台语义：
//!
//! * [`EngineAdapter::open`] 用**调用方给定的 IO**（生产环境必定是
//!   [`PlatformDurableIO`]）打开数据库。IO 一旦在 open 时固定，后续所有 WAL 写入都
//!   走同一条 durability 路径，不存在「绕开 durable IO 的第二个句柄」。
//! * [`EngineConnection::query`] / [`EngineConnection::execute`] 把结果映射成
//!   [`QueryOutcome`]（`Rows` 或 `Affected`），列元信息取自 statement 自身，
//!   元信息缺失时退化为「未知类型」而不是失败。
//! * [`EngineConnection::commit`] 返回**已 quorum durable 的末端 LSN**：COMMIT 只让帧
//!   进入本地 WAL，真正推进 LSN 的是 [`PlatformDurableIO`] 拿到远程 append 确认的时刻；
//!   拿不到确认时 COMMIT 会直接以错误返回（引擎侧看到的是 IO 失败）。
//!
//! 并发模型：`Statement` 是**每次调用局部创建**的（引擎的编译产物只属于那条语句），
//! 因此连接本身不需要用锁保护可变状态；`Arc<Connection>` 允许同一连接被多线程共享，
//! 但事务语义仍由引擎的 autocommit 状态机保证 —— 调用方不要并发在同一连接上做
//! BEGIN/COMMIT（db-runtime 每个会话独占一个连接）。
//!
//! 阻塞提醒：turso 的 `run_*` 系列方法会在调用线程上驱动 IO（并等待
//! `PlatformDurableIO` 的远程 append 结果），因此**不得在 tokio worker 线程上直接调用**
//! 本模块的查询方法（见 [`crate::durable`] 的线程模型说明）。

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use domain::error::{ErrorCode, PlatformError, Result};
use domain::value::{ColumnMeta, ResultSet, SqlValue};
use turso_core::{Connection, Database, Numeric, OpenOptions, SqliteDialect, Statement, Value, IO};

use crate::durable::PlatformDurableIO;
use crate::error::map_engine_error;

/// 本 crate 适配的引擎版本（= 绑定的 turso_core 版本字符串）。
///
/// 只做镜像而不各写一份字面量：版本一旦在 `Cargo.toml` 里被改动，
/// [`crate::TURSO_CORE_VERSION`] 与这里必须同时变，避免出现「报告一个版本、实际跑另一个」。
pub const ENGINE_VERSION: &str = crate::TURSO_CORE_VERSION;

/// 打开引擎所需的参数。
pub struct EngineOpenConfig {
    /// 数据库主文件路径（WAL 为 `<db_path>-wal`，见 [`crate::recovery::wal_path_for`]）。
    pub db_path: PathBuf,
    /// 正在使用的 durable IO；`Some` 时 [`EngineAdapter::durable_lsn`] 才有真实取值。
    pub durable_io: Option<Arc<PlatformDurableIO>>,
    /// 写 Owner epoch；open 时记录，便于把引擎侧错误与 fencing 身份对上号。
    pub owner_epoch: u64,
}

impl fmt::Debug for EngineOpenConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // PlatformDurableIO 不实现 Debug（它持有一个 IO 实现），这里只报告「有没有」。
        f.debug_struct("EngineOpenConfig")
            .field("db_path", &self.db_path)
            .field("durable_io", &self.durable_io.is_some())
            .field("owner_epoch", &self.owner_epoch)
            .finish()
    }
}

/// 打开后的引擎句柄：一个数据库文件对应一个实例。
pub struct EngineAdapter {
    db: Arc<Database>,
    db_path: PathBuf,
    durable_io: Option<Arc<PlatformDurableIO>>,
    owner_epoch: u64,
}

impl EngineAdapter {
    /// 在给定 IO 上打开（或创建）数据库。
    ///
    /// `io` 就是引擎后续所有读写（含 WAL）的通道：生产环境必须传
    /// [`PlatformDurableIO`]，否则 commit 不再经过远程 WAL，durability 契约被绕过。
    pub fn open(io: Arc<dyn IO>, config: EngineOpenConfig) -> Result<Arc<Self>> {
        let path = db_path_to_str(&config.db_path)?;
        // 方言在 open 时固定：平台只提供 SQLite SQL 与 sqlite_schema 语义。
        let options = OpenOptions::new(Arc::new(SqliteDialect));
        let db = Database::open(io, path, options).map_err(|err| map_engine_error(&err))?;
        Ok(Arc::new(Self {
            db,
            db_path: config.db_path,
            durable_io: config.durable_io,
            owner_epoch: config.owner_epoch,
        }))
    }

    /// 新建一条连接。
    ///
    /// 取 `self: &Arc<Self>` 是因为连接需要持有 adapter 才能回答 `commit` 后的 durable LSN。
    pub fn connect(self: &Arc<Self>) -> Result<EngineConnection> {
        let conn = self.db.connect().map_err(|err| map_engine_error(&err))?;
        Ok(EngineConnection {
            conn,
            adapter: Arc::clone(self),
        })
    }

    /// 数据库主文件路径。
    #[must_use]
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// 写 Owner epoch。
    #[must_use]
    pub const fn owner_epoch(&self) -> u64 {
        self.owner_epoch
    }

    /// 已经 quorum durable 的末端 LSN（exclusive）。
    ///
    /// 没有挂 durable IO（例如纯本地测试）时返回 0：此时没有任何远程确认，
    /// 语义上等价于「尚未确认任何字节」，而不是「已全部确认」。
    #[must_use]
    pub fn durable_lsn(&self) -> u64 {
        self.durable_io.as_ref().map_or(0, |io| io.durable_lsn())
    }

    /// 引擎版本字符串。
    #[must_use]
    pub const fn engine_version(&self) -> &'static str {
        ENGINE_VERSION
    }
}

/// 一条语句的执行结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryOutcome {
    /// 返回行的语句（SELECT / RETURNING / PRAGMA 查询）。
    Rows(ResultSet),
    /// 不返回行的语句（INSERT / UPDATE / DELETE / DDL）。
    Affected {
        /// 受影响行数（DDL 与无行变更时为 0）。
        rows: u64,
    },
}

/// 引擎连接：语句执行、事务控制与 durable LSN 查询。
pub struct EngineConnection {
    conn: Arc<Connection>,
    adapter: Arc<EngineAdapter>,
}

impl EngineConnection {
    /// 执行 SQL 到结束（可含多条语句），丢弃行，只报告受影响行数。
    ///
    /// 用于 DDL 与写入路径；需要结果集时用 [`Self::query`]。
    pub fn execute(&self, sql: &str) -> Result<QueryOutcome> {
        self.conn
            .execute(sql)
            .map_err(|err| map_engine_error(&err))?;
        Ok(QueryOutcome::Affected {
            rows: self.changed_rows(),
        })
    }

    /// 执行查询并组装结果集。
    ///
    /// 行数据在 `run_collect_rows` 里被驱动到结束 —— 这一步会等待
    /// [`PlatformDurableIO`] 的远程 append 结果：如果 commit frame 没拿到 quorum
    /// durable，这里返回的是错误（引擎的 IO 失败），绝不返回「看起来成功」的结果集。
    pub fn query(&self, sql: &str) -> Result<QueryOutcome> {
        let mut stmt = self.conn.query(sql).map_err(|err| map_engine_error(&err))?;
        let Some(stmt) = stmt.as_mut() else {
            // 空语句 / 纯注释：没有执行任何东西。
            return Ok(QueryOutcome::Affected { rows: 0 });
        };
        let columns = column_metadata(stmt);
        let rows = stmt
            .run_collect_rows()
            .map_err(|err| map_engine_error(&err))?;
        if columns.is_empty() {
            // 无列 = 非查询语句：按受影响行数上报（`n_change` 负数按 0 处理）。
            return Ok(QueryOutcome::Affected {
                rows: u64::try_from(stmt.n_change().max(0)).unwrap_or(u64::MAX),
            });
        }
        let mut result = ResultSet::new(columns);
        for row in rows {
            result
                .rows
                .push(row.iter().map(to_sql_value).collect::<Vec<SqlValue>>());
        }
        Ok(QueryOutcome::Rows(result))
    }

    /// 开启显式事务。
    pub fn begin(&self) -> Result<()> {
        self.run_control("BEGIN")
    }

    /// 提交事务，返回提交后的 durable LSN（exclusive 末端）。
    ///
    /// 引擎的 `COMMIT` 只保证 commit frame 写进本地 WAL；远程确认由
    /// [`PlatformDurableIO`] 在写入路径上完成。因此这里读到的是**已经 durable 的末端**：
    /// 若远程 append 未确认，`COMMIT` 本身就已经以 IO 错误失败了，不会走到这一步。
    pub fn commit(&self) -> Result<u64> {
        self.run_control("COMMIT")?;
        Ok(self.adapter.durable_lsn())
    }

    /// 回滚事务。
    pub fn rollback(&self) -> Result<()> {
        self.run_control("ROLLBACK")
    }

    /// 当前是否处于显式事务中。
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        !self.conn.get_auto_commit()
    }

    /// 最近一次 INSERT 的 rowid。
    #[must_use]
    pub fn last_insert_rowid(&self) -> i64 {
        self.conn.last_insert_rowid()
    }

    /// 已经 quorum durable 的末端 LSN（与 [`EngineAdapter::durable_lsn`] 同源）。
    #[must_use]
    pub fn durable_lsn(&self) -> u64 {
        self.adapter.durable_lsn()
    }

    /// 引擎侧最近一次语句改动行数（负数按 0 处理）。
    fn changed_rows(&self) -> u64 {
        u64::try_from(self.conn.changes().max(0)).unwrap_or(u64::MAX)
    }

    /// 事务控制语句：引擎没有单独的 begin/commit/rollback API，走 SQL 命令。
    fn run_control(&self, statement: &str) -> Result<()> {
        self.conn
            .execute(statement)
            .map_err(|err| map_engine_error(&err))
    }
}

/// 平台值 → 引擎值。
///
/// BLOB 走 [`Value::from_blob`]：`ValueBlob` 在本工程（stable 构建）就是 `Vec<u8>`，
/// 因此这条路径不会失败，也就不需要把「内存不足」伪装成 NULL。
#[must_use]
pub fn to_turso_value(value: &SqlValue) -> Value {
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(number) => Value::from_i64(*number),
        SqlValue::Real(number) => Value::from_f64(*number),
        SqlValue::Text(text) => Value::build_text(text.clone()),
        SqlValue::Blob(bytes) => Value::from_blob(bytes.clone()),
    }
}

/// 引擎值 → 平台值。
#[must_use]
pub fn to_sql_value(value: &Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::Numeric(Numeric::Integer(number)) => SqlValue::Integer(*number),
        Value::Numeric(Numeric::Float(number)) => SqlValue::Real(f64::from(*number)),
        Value::Text(text) => SqlValue::Text(text.as_str().to_string()),
        Value::Blob(_) => SqlValue::Blob(value.to_blob().unwrap_or_default().to_vec()),
    }
}

/// 列元信息。
///
/// 声明类型取不到时退化为「未知类型」（`ColumnMeta::unknown`）而不是报错：
/// 表达式列、连接表列在引擎里本来就没有 decltype，这类查询必须照常返回结果。
fn column_metadata(stmt: &Statement) -> Vec<ColumnMeta> {
    (0..stmt.num_columns())
        .map(|index| {
            let name = stmt.get_column_name(index).to_string();
            match stmt.get_column_decltype(index) {
                Some(decl_type) if !decl_type.is_empty() => ColumnMeta::new(name, decl_type, true),
                _ => ColumnMeta::unknown(name),
            }
        })
        .collect()
}

/// 数据库路径 → 引擎要求的 `&str`（非 UTF-8 路径直接拒绝，避免静默改写成别的文件）。
fn db_path_to_str(path: &Path) -> Result<&str> {
    path.to_str().ok_or_else(|| {
        PlatformError::new(
            ErrorCode::InvalidArgument,
            format!("数据库路径不是合法 UTF-8，无法交给引擎：{}", path.display()),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_core::UnixIO;

    /// 内存中的值转换必须与引擎值一一对应（BLOB 用 Deref 到 `&[u8]` 的路径读取）。
    #[test]
    fn value_conversion_round_trips() {
        for value in [
            SqlValue::Null,
            SqlValue::Integer(-7),
            SqlValue::Real(1.5),
            SqlValue::Text("平台".to_string()),
            SqlValue::Blob(vec![0, 1, 2, 255]),
        ] {
            assert_eq!(to_sql_value(&to_turso_value(&value)), value, "值 {value:?}");
        }
    }

    /// 版本字符串必须与 `Cargo.toml` 的 pin 一致（架构 §17.3 禁止 floating semver）。
    #[test]
    fn engine_version_is_pinned() {
        assert_eq!(ENGINE_VERSION, crate::TURSO_CORE_VERSION);
        let manifest =
            std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.toml"))
                .expect("workspace Cargo.toml 必须可读");
        let pinned = format!("turso_core = \"={ENGINE_VERSION}\"");
        assert!(
            manifest.contains(&pinned),
            "workspace Cargo.toml 必须把 turso_core pin 成 {ENGINE_VERSION}（期望出现 `{pinned}`）"
        );
    }

    /// 端到端：建表 → 插入 → 查询 → 事务回滚。用 UnixIO（无远程 WAL）验证引擎适配本身，
    /// durability 语义由 durable_io 的测试覆盖。
    #[test]
    fn open_execute_query_and_transaction() {
        let dir = tempfile::tempdir().expect("临时目录");
        let db_path = dir.path().join("demo.db");
        let io: Arc<dyn IO> = Arc::new(UnixIO::new().expect("UnixIO"));
        let adapter = EngineAdapter::open(
            io,
            EngineOpenConfig {
                db_path,
                durable_io: None,
                owner_epoch: 7,
            },
        )
        .expect("打开数据库");
        assert_eq!(adapter.engine_version(), ENGINE_VERSION);
        assert_eq!(adapter.durable_lsn(), 0, "没有 durable IO 时不得谎报 LSN");

        let conn = adapter.connect().expect("建立连接");
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .expect("建表");

        let inserted = conn
            .execute("INSERT INTO t (name) VALUES ('a'), ('b')")
            .expect("插入");
        assert_eq!(inserted, QueryOutcome::Affected { rows: 2 });
        assert_eq!(conn.last_insert_rowid(), 2);

        let selected = conn
            .query("SELECT id, name FROM t ORDER BY id")
            .expect("查询");
        let QueryOutcome::Rows(result) = selected else {
            panic!("SELECT 必须返回行");
        };
        assert_eq!(result.column_count(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(
            result.rows,
            vec![
                vec![SqlValue::Integer(1), SqlValue::text("a")],
                vec![SqlValue::Integer(2), SqlValue::text("b")],
            ]
        );

        // 事务：回滚后新行不得存在。
        assert!(!conn.in_transaction());
        conn.begin().expect("BEGIN");
        assert!(conn.in_transaction());
        conn.execute("INSERT INTO t (name) VALUES ('c')")
            .expect("事务内插入");
        conn.rollback().expect("ROLLBACK");
        assert!(!conn.in_transaction());
        let QueryOutcome::Rows(after) = conn.query("SELECT count(*) FROM t").expect("计数")
        else {
            panic!("count(*) 必须返回行");
        };
        assert_eq!(after.rows, vec![vec![SqlValue::Integer(2)]]);
    }
}
