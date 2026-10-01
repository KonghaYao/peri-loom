//! durability 子系统的聚合模块（架构 §11.1 / §17.3）。
//!
//! 这里把三件事挂到同一条路径 `crate::durable::*` 下，使 lib.rs 的对外导出成为一份
//! 自洽的清单：
//!
//! * [`frame`] —— WAL 帧格式解析（唯一理解 Turso/SQLite WAL 内部格式的地方）；
//! * [`gate`] —— 本地写 + 远程 append 的双边完成门（commit 结算规则）；
//! * [`remote`] —— 远程 append 的注入点（trait + 生产实现 [`WalClientAppender`]）；
//! * [`PlatformDurableIO`] —— durability 的唯一实现位置。其实现在 crate 根目录的
//!   `durable_io.rs`，用 `#[path]` 挂进来：文件位置保持不动，但对外的模块路径统一为
//!   `durable`，避免「同一概念在 `crate::durable_io` 与 `crate::durable` 两处各说各话」。
//!
//! 注意：`PlatformDurableIO` 只通过 [`remote::RemoteWalAppender`] 与远程 WAL 交互，
//! 该 trait 也只有一份定义（在 [`remote`] 里）—— 否则注入实现的类型与
//! [`PlatformDurableIO::new_with_appender`] 期待的类型会分叉。

pub mod frame;
pub mod gate;
pub mod remote;

#[path = "../durable_io.rs"]
mod io;

pub use self::io::{
    DurableIoConfig, PlatformDurableIO, WalStreamSeed, DEFAULT_APPEND_TIMEOUT, WAL_SUFFIX,
};
pub use self::remote::{RemoteWalAppender, WalClientAppender};
