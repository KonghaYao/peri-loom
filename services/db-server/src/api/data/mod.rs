//! `/data/v1/*` SQL / Data HTTP API（架构 §17.4 / §13）。
//!
//! 与 `/api/v1/*` 的分工：Management 面管**数据库对象**，Data 面管**DBA 会话**。两者
//! 共享同一套错误体、审计与路由能力，但数据面额外承担两条硬约束：
//!
//! 1. **大结果集不得在 Server 内存里堆积**：默认内联 JSON，超过
//!    `INLINE_RESULT_LIMIT_BYTES` 或客户端声明 `Accept: application/x-ndjson` 时
//!    走 NDJSON 流式（见 [`stream`]）；
//! 2. **客户端断开 1s 内把 Cancel 传到 Worker**：由 `CancelGuard` 随响应体生命周期
//!    自动完成，见 [`crate::clients::CancelGuard`]。

mod query;
mod session;
// 兼容层（`api::hrana`）要复用这里的帧收集辅助：一次执行、两种出口的语义
// 只应有一份实现。
pub(crate) mod stream;

// glob 重导出：`#[utoipa::path]` 生成的 `__path_*` 标记与函数同模块，
// 逐个重导出会让 `data::__path_query_database` 找不到（api::mod 的 paths(...) 依赖它）。
pub use query::*;
pub use session::*;

use axum::routing::{delete, post};
use axum::Router;

use crate::state::AppState;

/// Data 面路由表。
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/data/v1/databases/{db_id}/query", post(query_database))
        .route("/data/v1/databases/{db_id}/batch", post(batch_database))
        .route("/data/v1/databases/{db_id}/sessions", post(open_session))
        .route("/data/v1/sessions/{session_id}/query", post(session_query))
        .route("/data/v1/sessions/{session_id}", delete(close_session))
}
