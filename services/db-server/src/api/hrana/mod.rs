//! TursoDB / libsql 客户端兼容端点：**Hrana over HTTP v2（JSON）**。
//!
//! # 为什么需要这一层
//!
//! 架构 §17.4 把出口冻结为平台自己的 HTTP（`/api/v1/*` + `/data/v1/*`），但那是
//! **平台自有契约**：`turso_core` 是纯嵌入式库（`crates/engine-adapter` 走进程内
//! FFI），仓库里没有任何 Hrana / libsql / sqld 的服务端实现。因此"外部 TursoDB /
//! libsql 客户端直连"不是已有能力的开关，而是**新增的一层协议适配**：
//!
//! ```text
//! @libsql/client / turso CLI
//!   └─ POST /db/{db_id}/v2/pipeline        <- 本模块（Hrana v2 JSON）
//!        └─ DbRouter / SessionRegistry      <- 复用既有的路由、租约、fencing、透明 Wake
//!             └─ WorkerData(ExecuteStream / SessionExecuteStream)
//!                  └─ DB Process（turso_core）
//! ```
//!
//! 客户端 URL 必须形如 `http://<host>/db/<db_id>/`（**结尾斜杠不能省**）：客户端用
//! `new URL("v2/pipeline", baseUrl)` 拼路径，少了斜杠会把 `db_id` 当目录吃掉。
//!
//! # 会话（baton）与平台显式会话的映射
//!
//! Hrana 的跨请求状态全靠服务端签发的 `baton`。判定规则只有一条：**pipeline 里出现了
//! `close` 就是"本次用完即弃"，否则必须留住会话**。再加上"批处理必须落在同一条连接上"：
//!
//! | 客户端行为 | 请求形状 | 本端处理 |
//! |---|---|---|
//! | `client.execute()` | `[execute, close]` | 无会话（走 `ExecuteStream` 的透明重试快路径） |
//! | `client.batch()` | `[store_sql…, batch, close]` | 瞬态会话：开一条连接跑完即关，`baton=null` |
//! | `client.transaction()` | `[store_sql, batch]`（**无 close**） | 持久会话，回传 `baton` |
//! | 事务的后续语句 | `{baton, [execute, close]}` | 复用该会话执行后关闭 |
//!
//! `baton` 就是平台会话 ID 本身：会话不进 Catalog、Server 重启即失效（与
//! `/data/v1/sessions` 完全同一套语义），失效时返回明确错误而不是静默新建一条新连接
//! ——静默新建会让客户端以为事务还在，把数据写到事务外。
//!
//! # 与 `/data/v1` 的差别（刻意的，不是遗漏）
//!
//! - **一次性 JSON**：Hrana v2 没有流式出口，整个结果集必须在同一个响应体里，
//!   因此这里对结果集有估算上限（[`MAX_RESULT_BYTES`]），超限明确报错而不是无上限缓存；
//! - **权限仍是 `db:write`**：与 `/data/v1` 同档。平台不解析 SQL，无法可靠区分
//!   `SELECT` 与 `WITH ... DELETE`，给只读主体放行就等于开了越权旁路；
//! - **`last_insert_rowid` 恒为 `null`**：平台结果集契约里没有这个字段
//!   （见 `domain::value::ResultSet`），如实报告"未知"而不是猜一个可能属于别的连接的值。

pub mod sql;
pub mod wire;

use std::collections::HashMap;

use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use domain::error::ErrorCode;
use domain::ids::DatabaseId;
use domain::value::{ColumnMeta, ResultSet, SqlValue};
use futures::StreamExt;

use crate::api::data::stream;
use crate::api::{db_id, MAX_REQUEST_BODY_BYTES};
use crate::auth::{permission, Principal};
use crate::clients::status_to_api_error;
use crate::error::{ApiError, ApiResult};
use crate::middleware::current_request_id;
use crate::router::StreamTarget;
use crate::state::{AppState, SessionBinding};

/// 单条语句结果的**估算**内存上限。
///
/// Hrana v2 是**一次性 JSON** 协议：整个结果集必须装在同一个响应体里，没有流式出口
/// （平台的 NDJSON 出口在 `/data/v1/*`）。所以这里必须有上限，否则一条
/// `select * from 大表` 就能把 Server 的内存吃光。取值与请求体上限一致（16 MiB）：
/// 超过它的结果集更合理的做法是加 `LIMIT` 分页。
///
/// 注意这是**估算值**上限：实际比较的是 `api::data::stream::approximate_bytes` 的估算
/// 结果，标量一律按 8 字节计；而 Hrana JSON 的真实编码（`{"type":"integer","value":"1"}`
/// 这类包装、Blob 的 base64）要大一至数倍，因此编码后的响应体可能明显超过本值。
/// 它防的是「一个大结果集把内存吃光」，不是精确的响应体预算。
pub const MAX_RESULT_BYTES: usize = MAX_REQUEST_BODY_BYTES;

/// 单个会话最多缓存多少条 `store_sql` 的 SQL 文本。
///
/// 客户端每条语句都会重新 `store_sql`，正常用量远低于这个数；上限只为防止
/// 恶意客户端把会话当成无限容量的存储。
pub const MAX_STORED_SQL_PER_SESSION: usize = 1024;

/// Hrana 兼容端点路由表。
///
/// 额外挂一层[错误体形状适配](hrana_error_shape)：认证 / 权限 / 请求体超限这类失败由
/// 中间件与提取器**直接生成响应**，根本不经过 [`pipeline`]，而 Hrana 客户端只认顶层
/// `message`（见该函数注释）。放在这一层是因为它是唯一能覆盖全部失败来源的位置。
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/db/{db_id}/v2/pipeline", post(pipeline))
        .layer(axum::middleware::from_fn(hrana_error_shape))
}

/// `POST /db/{db_id}/v2/pipeline`。
///
/// 错误体形如由 [`hrana_error_shape`] 统一适配，这里只管业务。
pub async fn pipeline(
    State(state): State<AppState>,
    principal: Principal,
    Path(raw_db_id): Path<String>,
    Json(request): Json<wire::PipelineRequest>,
) -> Response {
    match run(&state, &principal, &raw_db_id, request).await {
        Ok(body) => Json(body).into_response(),
        Err(err) => err.into_response(),
    }
}

/// 错误体形状适配层：把平台冻结信封改写成 Hrana 客户端能读懂的形状。
///
/// 平台冻结的错误体是 `{"error":{code,message,…}}`，而 Hrana 客户端**只在响应顶层**
/// 看到 `message` 时才把它当业务错误（`hrana-client` 的 `errorFromResponse`：
/// `content-type === "application/json"` 且 `"message" in body`），否则退化成
/// `Server returned HTTP status 401` —— 真实原因就此丢失。因此这里保留冻结信封，
/// 同时把 `message` / `code` 提到顶层：是**加字段**，不是改契约。
///
/// 为什么必须是响应层而不是 handler 内部：401 / 403 / 413 由认证中间件与提取器直接
/// 返回，handler 根本没有机会插手；只有包裹整条响应链才能全覆盖。
async fn hrana_error_shape(request: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let response = next.run(request).await;
    if response.status().is_success() {
        // 成功路径一律原样透传：结果集可能很大，绝不能在这里缓冲一遍。
        return response;
    }
    reshape_error_body(response).await
}

/// 单次错误响应的正文上限（错误体本该很小，超过就退回通用文案）。
const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;

async fn reshape_error_body(response: Response) -> Response {
    let status = response.status();
    let (parts, body) = response.into_parts();
    let bytes = match axum::body::to_bytes(body, MAX_ERROR_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return (
                status,
                Json(serde_json::json!({
                    "message": format!("请求处理失败（HTTP {status}），且错误正文超出可读上限"),
                })),
            )
                .into_response();
        }
    };
    // 解析不出 JSON 的：提取器在进入 handler 之前直接拒绝时，正文是 `text/plain`
    // （`DefaultBodyLimit` 的 413，`Json` 语法错误 / 反序列化失败的 400 / 422）。
    // 原样透传的话，客户端只会读到 `Server returned HTTP status 413`，上面承诺的
    // "覆盖请求体超限"就成了空话。这里合成最小信封，文案只按状态码语义构造，
    // **不回显原始正文**（可能夹带用户输入或内部细节）。
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return rebuild_json_error(
            parts,
            serde_json::json!({ "message": non_json_error_message(status) }),
        );
    };
    // 能解析成 JSON 的：只改写平台自己的错误信封，其余形状原样返回，避免把别的 JSON
    // 错误包装坏。这里按 JSON 取值而不是反序列化成 `ErrorEnvelope`：本层**不该**依赖
    // 信封的内部结构，信封加字段时也不该让这里失效。
    let Some(error) = value.get("error") else {
        return Response::from_parts(parts, axum::body::Body::from(bytes));
    };
    let Some(message) = error.get("message").and_then(serde_json::Value::as_str) else {
        return Response::from_parts(parts, axum::body::Body::from(bytes));
    };
    let body = serde_json::json!({
        "message": message,
        "code": error.get("code").cloned().unwrap_or(serde_json::Value::Null),
        "error": error,
    });
    rebuild_json_error(parts, body)
}

/// 用新正文重建错误响应：强制**精确**的 `content-type`，并丢掉过期的 `content-length`。
///
/// 客户端用的是**精确匹配**（`content-type === "application/json"`），不能带 charset；
/// 长度交给 hyper 按新正文重算，留着旧值会截断响应。
fn rebuild_json_error(parts: axum::http::response::Parts, body: serde_json::Value) -> Response {
    let mut rebuilt = Response::from_parts(
        parts,
        axum::body::Body::from(serde_json::to_vec(&body).unwrap_or_default()),
    );
    rebuilt.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    rebuilt.headers_mut().remove(axum::http::header::CONTENT_LENGTH);
    rebuilt
}

/// 非 JSON 错误体（提取器拒绝）的兜底文案。
///
/// 只按状态码语义构造，**不读也不回显原始正文**：那里面可能夹带用户输入或内部细节，
/// 而客户端需要的只是一句能读的失败原因。
fn non_json_error_message(status: axum::http::StatusCode) -> String {
    match status {
        axum::http::StatusCode::PAYLOAD_TOO_LARGE => "请求体超过服务端上限".to_string(),
        axum::http::StatusCode::UNSUPPORTED_MEDIA_TYPE => {
            "请求的 content-type 必须是 application/json".to_string()
        }
        axum::http::StatusCode::UNPROCESSABLE_ENTITY => {
            "请求体不是合法的 Hrana v2 请求：字段缺失或类型不符".to_string()
        }
        _ => format!("请求处理失败（HTTP {status}）"),
    }
}

/// 一次 pipeline 请求的完整处理。
async fn run(
    state: &AppState,
    principal: &Principal,
    raw_db_id: &str,
    request: wire::PipelineRequest,
) -> ApiResult<wire::PipelineResponse> {
    // 数据面一律 `db:write`，与 /data/v1 同档（理由见模块头）。
    principal.require(permission::DB_WRITE)?;
    let database_id = db_id(raw_db_id)?;
    if request.requests.is_empty() {
        return Err(ApiError::invalid_argument("requests 不能为空"));
    }

    let closes = request
        .requests
        .iter()
        .any(|item| matches!(item, wire::StreamRequest::Close));
    let batches = request
        .requests
        .iter()
        .any(|item| matches!(item, wire::StreamRequest::Batch { .. }));
    // 没有 close 说明客户端还会带 baton 回来；有 batch 说明这些步骤必须在同一条连接上。
    let needs_session = !closes || batches;

    let mut session = match request.baton.as_deref() {
        Some(baton) => Some(resume_binding(state, database_id, baton).await?),
        None if needs_session => Some(crate::api::data::open_binding(state, database_id).await?.0),
        None => None,
    };
    if let Some(binding) = session.as_ref() {
        ensure_binding_route(state, binding).await?;
    }

    let mut pipeline = Pipeline {
        state,
        database_id,
        request_id: current_request_id(),
        session: session.take(),
        local_sql: HashMap::new(),
    };

    let mut results = Vec::with_capacity(request.requests.len());
    for item in request.requests {
        results.push(pipeline.handle(item).await);
    }

    Ok(wire::PipelineResponse {
        baton: pipeline.session.as_ref().map(|binding| binding.session_id.clone()),
        // 本实现不做连接迁移，客户端应继续用自己配置的地址。
        base_url: None,
        results,
    })
}

/// pipeline 的执行上下文：把「当前会话 / 本请求的 SQL 缓存」收在一处。
struct Pipeline<'a> {
    state: &'a AppState,
    database_id: DatabaseId,
    request_id: String,
    /// 当前会话；`close` 之后置 `None`（后续语句退化为无会话执行）。
    session: Option<SessionBinding>,
    /// 本请求内 `store_sql` 的临时缓存（无会话时使用，随请求结束丢弃）。
    local_sql: HashMap<i64, String>,
}

impl Pipeline<'_> {
    /// 处理单条 pipeline 请求；失败只影响这一条。
    async fn handle(&mut self, item: wire::StreamRequest) -> wire::StreamResult {
        match item {
            wire::StreamRequest::Close => {
                self.close_session().await;
                wire::StreamResult::ok(wire::StreamResponse::Close)
            }
            wire::StreamRequest::StoreSql { sql_id, sql } => match self.store_sql(sql_id, sql) {
                Ok(()) => wire::StreamResult::ok(wire::StreamResponse::StoreSql),
                Err(err) => wire::StreamResult::error(wire_error(&err)),
            },
            wire::StreamRequest::CloseSql { sql_id } => {
                self.forget_sql(sql_id);
                wire::StreamResult::ok(wire::StreamResponse::CloseSql)
            }
            wire::StreamRequest::Execute { stmt } => match self.exec(&stmt).await {
                Ok(result) => wire::StreamResult::ok(wire::StreamResponse::Execute { result }),
                Err(err) => wire::StreamResult::error(wire_error(&err)),
            },
            wire::StreamRequest::Batch { batch } => match self.exec_batch(&batch).await {
                Ok(result) => wire::StreamResult::ok(wire::StreamResponse::Batch { result }),
                Err(err) => wire::StreamResult::error(wire_error(&err)),
            },
            wire::StreamRequest::Sequence { sql, sql_id } => {
                match self.exec_sequence(sql.as_deref(), sql_id).await {
                    Ok(()) => wire::StreamResult::ok(wire::StreamResponse::Sequence),
                    Err(err) => wire::StreamResult::error(wire_error(&err)),
                }
            }
            // 以下两者都在"必须实现的最小集合"之外：报明确错误，而不是回一个空结果
            // 让调用方以为成功了。
            wire::StreamRequest::Describe { .. } => wire::StreamResult::error(wire_error(
                &ApiError::not_implemented(
                    "Hrana v2 兼容层暂未实现 describe；列元数据可从 execute 的 cols 里取",
                ),
            )),
            wire::StreamRequest::GetAutocommit => wire::StreamResult::error(wire_error(
                &ApiError::not_implemented("get_autocommit 是 Hrana v3 能力，本端点只实现 v2"),
            )),
        }
    }

    // ---------------------------------------------------------------- 语句执行

    /// 解析语句文本与绑定参数。
    fn prepare(&self, stmt: &wire::Stmt) -> ApiResult<(String, Vec<protocol::data::Value>)> {
        let mut sql = self.resolve_sql(stmt.sql.as_deref(), stmt.sql_id)?;
        if sql.trim().is_empty() {
            return Err(ApiError::invalid_argument("sql 不能为空"));
        }
        // 单语句语义：DB Process 的 prepare 只看第一条语句，把多语句文本原样送下去就是
        // **静默丢弃后半段**。宁可在入口拒绝，也不让调用方以为都执行了。
        // `CREATE TRIGGER` 的语句体内含 `;`（见 sql::is_trigger_definition），不参与切分。
        if !sql::is_trigger_definition(&sql) {
            let count = sql::split_statements(&sql).len();
            if count > 1 {
                return Err(ApiError::invalid_argument(format!(
                    "execute 一次只接受一条语句，检测到 {count} 条；请改用 executeMultiple() 或 batch()"
                )));
            }
        }

        if !stmt.args.is_empty() && !stmt.named_args.is_empty() {
            return Err(ApiError::invalid_argument(
                "args 与 named_args 不能同时非空：位置取值顺序无法判定",
            ));
        }
        if stmt.named_args.is_empty() {
            let params = stmt
                .args
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    wire::value_from_wire(value)
                        .map(protocol::data::Value::from)
                        .map_err(|err| ApiError::invalid_argument(format!("args[{index}]: {err}")))
                })
                .collect::<ApiResult<Vec<_>>>()?;
            return Ok((sql, params));
        }

        let named = stmt
            .named_args
            .iter()
            .map(|arg| {
                wire::value_from_wire(&arg.value)
                    .map(|value| (arg.name.clone(), value))
                    .map_err(|err| {
                        ApiError::invalid_argument(format!("named_args[{}]: {err}", arg.name))
                    })
            })
            .collect::<ApiResult<Vec<_>>>()?;
        let bind = sql::bind_named_args(&sql, &named).map_err(ApiError::invalid_argument)?;
        sql = bind.sql;
        Ok((sql, bind.args.into_iter().map(protocol::data::Value::from).collect()))
    }

    /// 执行一条语句并转成线上结果。
    async fn exec(&self, stmt: &wire::Stmt) -> ApiResult<wire::StmtResult> {
        let (sql, params) = self.prepare(stmt)?;
        let result = self.run_statement(sql, params).await?;
        Ok(stmt_result(&result, stmt.want_rows))
    }

    /// 执行一条语句，取回完整结果集。
    async fn run_statement(
        &self,
        sql: String,
        params: Vec<protocol::data::Value>,
    ) -> ApiResult<ResultSet> {
        let target = match self.session.as_ref() {
            // 会话内执行不做透明重试：会话 pin 在特定 Worker 上，重试到别的 Worker
            // 只会拿到一个必然失败的会话。
            Some(binding) => StreamTarget::Session {
                session_id: binding.session_id.clone(),
                sql,
                params,
            },
            None => StreamTarget::Stateless { sql, params },
        };

        let (route, mut frames, mut guard) = self
            .state
            .router
            .open_stream(self.database_id, target, &self.request_id, None)
            .await?;
        let worker_id = route.worker_id.to_string();

        let mut columns: Vec<ColumnMeta> = Vec::new();
        let mut rows: Vec<Vec<SqlValue>> = Vec::new();
        let mut affected_rows = 0u64;
        let mut buffered_bytes = 0usize;

        while let Some(item) = frames.next().await {
            let frame = item.map_err(|status| status_to_api_error(status, &worker_id))?;
            if let Some(error) = stream::proto_error(frame.error.as_ref()) {
                return Err(ApiError::from(error));
            }
            match frame.frame {
                Some(protocol::data::stream_frame::Frame::Header(header)) => {
                    columns = header.columns.iter().map(stream::column_meta).collect();
                }
                Some(protocol::data::stream_frame::Frame::Rows(batch)) => {
                    for row in batch.rows {
                        let values = protocol::convert::row_from_proto(row);
                        buffered_bytes += stream::approximate_bytes(&values);
                        rows.push(values);
                    }
                    if buffered_bytes > MAX_RESULT_BYTES {
                        return Err(ApiError::new(
                            ErrorCode::ResultTooLarge,
                            format!(
                                "结果集超过 {MAX_RESULT_BYTES} 字节：Hrana v2 只能在单个响应体里回传完整结果，\
                                 请加 LIMIT 分页，或改用 /data/v1 的 NDJSON 流式出口"
                            ),
                        ));
                    }
                }
                Some(protocol::data::stream_frame::Frame::Trailer(trailer)) => {
                    affected_rows = trailer.affected_rows;
                }
                None => {}
            }
        }
        // 执行已正常结束：解除守卫，不再向 Worker 发表 Cancel。
        guard.disarm();

        Ok(ResultSet {
            columns,
            rows,
            affected_rows,
            truncated: false,
        })
    }

    /// 批处理：逐步执行 + 条件求值。
    ///
    /// **失败不中止批处理**：客户端生成的步骤里，回滚本身就是靠
    /// `not(ok, step:COMMIT)` 触发的（见 `client.batch()` 的实际请求），
    /// 一旦遇到错误就停，回滚步骤永远不会被执行。
    async fn exec_batch(&self, batch: &wire::Batch) -> ApiResult<wire::BatchResult> {
        let total = batch.steps.len();
        let mut completed = vec![false; total];
        let mut step_results: Vec<Option<wire::StmtResult>> = vec![None; total];
        let mut step_errors: Vec<Option<wire::WireError>> = vec![None; total];

        for (index, step) in batch.steps.iter().enumerate() {
            if let Some(condition) = step.condition.as_ref() {
                let run = eval_condition(condition, &completed, &step_errors, total)?;
                if !run {
                    // 被跳过：两边都留 null（客户端要求两个数组等长）。
                    continue;
                }
            }
            completed[index] = true;
            match self.exec(&step.stmt).await {
                Ok(result) => step_results[index] = Some(result),
                Err(err) => step_errors[index] = Some(wire_error(&err)),
            }
        }

        Ok(wire::BatchResult {
            step_results,
            step_errors,
        })
    }

    /// 多语句顺序执行（`executeMultiple`）：只回报成败，不回报结果集。
    async fn exec_sequence(&self, sql: Option<&str>, sql_id: Option<i64>) -> ApiResult<()> {
        let text = self.resolve_sql(sql, sql_id)?;
        if text.trim().is_empty() {
            return Err(ApiError::invalid_argument("sql 不能为空"));
        }
        // `CREATE TRIGGER` 的语句体内含 `;`，按 `;` 切分会把一条合法语句切坏；
        // 这类语句整段下发（DB Process 的 prepare 能整段吃下触发器定义）。
        let statements = if sql::is_trigger_definition(&text) {
            vec![text]
        } else {
            sql::split_statements(&text)
        };
        if statements.is_empty() {
            return Err(ApiError::invalid_argument("sql 不包含可执行的语句"));
        }
        for statement in statements {
            self.run_statement(statement, Vec::new()).await?;
        }
        Ok(())
    }

    // ---------------------------------------------------------------- SQL 缓存

    /// 解析 `stmt.sql` / `stmt.sql_id`。
    fn resolve_sql(&self, sql: Option<&str>, sql_id: Option<i64>) -> ApiResult<String> {
        match (sql, sql_id) {
            (Some(text), None) => Ok(text.to_string()),
            (None, Some(id)) => self
                .local_sql
                .get(&id)
                .cloned()
                .or_else(|| {
                    self.session
                        .as_ref()
                        .and_then(|binding| self.state.sessions.sql_cache_get(&binding.session_id, id))
                })
                .ok_or_else(|| {
                    ApiError::invalid_argument(format!(
                        "sql_id {id} 未在本请求或当前会话中通过 store_sql 注册"
                    ))
                }),
            (Some(_), Some(_)) => Err(ApiError::invalid_argument(
                "stmt 的 sql 与 sql_id 只能给一个",
            )),
            (None, None) => Err(ApiError::invalid_argument("stmt 必须给出 sql 或 sql_id")),
        }
    }

    /// 缓存 SQL 文本：有会话就挂在会话上（客户端可能下一个请求才用它），否则只在本请求内有效。
    fn store_sql(&mut self, sql_id: i64, sql: String) -> ApiResult<()> {
        match self.session.as_ref() {
            Some(binding) => {
                let cache = self.state.sessions.sql_cache(&binding.session_id);
                if cache.len() >= MAX_STORED_SQL_PER_SESSION && !cache.contains_key(&sql_id) {
                    return Err(ApiError::new(
                        ErrorCode::ResourceExhausted,
                        format!("单个会话最多缓存 {MAX_STORED_SQL_PER_SESSION} 条 SQL"),
                    ));
                }
                cache.insert(sql_id, sql);
            }
            None => {
                self.local_sql.insert(sql_id, sql);
            }
        }
        Ok(())
    }

    /// 释放缓存的 SQL 文本。
    fn forget_sql(&mut self, sql_id: i64) {
        self.local_sql.remove(&sql_id);
        if let Some(binding) = self.session.as_ref() {
            if let Some(cache) = self.state.sessions.sql_cache_existing(&binding.session_id) {
                cache.remove(&sql_id);
            }
        }
    }

    /// 关闭并注销当前会话。
    async fn close_session(&mut self) {
        if let Some(binding) = self.session.take() {
            close_binding(self.state, &binding).await;
        }
    }
}

// -------------------------------------------------------------------- 会话

/// 用客户端带回的 baton 找回会话。
///
/// baton 就是平台会话 ID（见模块头）。找不到 / 已过期 / 不属于本库都返回明确错误：
/// 静默新建一条连接会让客户端以为事务还在，把后续语句写到事务之外。
async fn resume_binding(
    state: &AppState,
    database_id: DatabaseId,
    baton: &str,
) -> ApiResult<SessionBinding> {
    let binding = crate::api::data::live_binding(state, baton)?;
    if binding.database_id != database_id {
        return Err(ApiError::invalid_argument(
            "baton 属于另一个数据库（会话不能在库之间复用）",
        ));
    }
    Ok(binding)
}

/// 执行前校验会话 pin 的 Worker / epoch 仍然有效。
async fn ensure_binding_route(state: &AppState, binding: &SessionBinding) -> ApiResult<()> {
    let route = state
        .router
        .resolve_target(binding.database_id, None)
        .await?;
    if route.worker_id != binding.worker_id || route.owner_epoch != binding.owner_epoch {
        state.sessions.remove(&binding.session_id);
        return Err(ApiError::new(
            ErrorCode::SessionLost,
            "会话所属数据库已发生 failover，会话不可恢复；请重新开始事务",
        ));
    }
    Ok(())
}

/// 关闭会话：先尽力通知 Worker，再注销本地绑定。
async fn close_binding(state: &AppState, binding: &SessionBinding) {
    if let Err(err) = state
        .router
        .close_session(&binding.database_id, &binding.worker_id, &binding.session_id)
        .await
    {
        // 失败不致命：Worker 侧还有空闲计时兜底。但必须留下日志，否则"关闭变慢"
        // 会变成无头案。
        tracing::warn!(
            session_id = %binding.session_id,
            worker_id = %binding.worker_id,
            code = err.code().as_str(),
            "通知 Worker 关闭 Hrana 会话失败（依赖 Worker 侧空闲回收）"
        );
    }
    state.sessions.remove(&binding.session_id);
}

// -------------------------------------------------------------------- 辅助

/// 求值批处理步骤条件。
fn eval_condition(
    condition: &wire::BatchCond,
    completed: &[bool],
    step_errors: &[Option<wire::WireError>],
    total_steps: usize,
) -> ApiResult<bool> {
    Ok(match condition {
        wire::BatchCond::Ok { step } => {
            let index = step_index(*step, total_steps)?;
            completed[index] && step_errors[index].is_none()
        }
        wire::BatchCond::Error { step } => {
            let index = step_index(*step, total_steps)?;
            completed[index] && step_errors[index].is_some()
        }
        wire::BatchCond::Not { cond } => !eval_condition(cond, completed, step_errors, total_steps)?,
        wire::BatchCond::And { conds } => {
            for cond in conds {
                if !eval_condition(cond, completed, step_errors, total_steps)? {
                    return Ok(false);
                }
            }
            true
        }
        wire::BatchCond::Or { conds } => {
            for cond in conds {
                if eval_condition(cond, completed, step_errors, total_steps)? {
                    return Ok(true);
                }
            }
            false
        }
        // v2 下客户端不会发这个条件（它是 v3 能力）。真收到就报错，
        // 而不是随便给个真假让客户端的控制流走向未知分支。
        wire::BatchCond::IsAutocommit => {
            return Err(ApiError::not_implemented(
                "batch 的 is_autocommit 条件需要 Hrana v3，本端点只实现 v2",
            ));
        }
    })
}

/// 条件里的步骤下标越界时直接报错（属于协议违规，不能当成 `false` 静默放行）。
fn step_index(step: usize, total_steps: usize) -> ApiResult<usize> {
    if step >= total_steps {
        return Err(ApiError::invalid_argument(format!(
            "batch 条件引用了不存在的步骤 {step}（共 {total_steps} 步）"
        )));
    }
    Ok(step)
}

/// 领域结果集 -> 线上语句结果。
fn stmt_result(result: &ResultSet, want_rows: bool) -> wire::StmtResult {
    if !want_rows {
        // 客户端没要行（BEGIN / COMMIT / DDL）。列与行返回空数组，但字段必须在 ——
        // 客户端的解码器对 `cols` / `rows` 是必填。
        return wire::StmtResult::empty(result.affected_rows);
    }
    wire::StmtResult {
        cols: result
            .columns
            .iter()
            .map(|column| wire::WireCol {
                name: column.name.clone(),
                decltype: column.type_name.clone(),
            })
            .collect(),
        rows: result
            .rows
            .iter()
            .map(|row| row.iter().map(wire::value_to_wire).collect())
            .collect(),
        affected_row_count: result.affected_rows,
        // 平台结果集契约里没有 last_insert_rowid，如实报告"未知"。
        last_insert_rowid: None,
    }
}

/// 平台错误 -> 线上错误（`code` 用平台错误码字面量，客户端会原样透出）。
fn wire_error(err: &ApiError) -> wire::WireError {
    wire::WireError::new(err.code().as_str(), err.error.message.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cond_ok(step: usize) -> wire::BatchCond {
        wire::BatchCond::Ok { step }
    }

    #[test]
    fn batch_condition_reads_completed_steps_only() {
        let mut completed = vec![false, false];
        let errors: Vec<Option<wire::WireError>> = vec![None, Some(wire::WireError::new("X", "boom"))];
        // 还没执行的步骤：既不是 ok 也不是 error。
        assert!(!eval_condition(&cond_ok(0), &completed, &errors, 2).unwrap());
        assert!(!eval_condition(&wire::BatchCond::Error { step: 1 }, &completed, &errors, 2).unwrap());

        completed[0] = true;
        completed[1] = true;
        assert!(eval_condition(&cond_ok(0), &completed, &errors, 2).unwrap());
        assert!(eval_condition(&wire::BatchCond::Error { step: 1 }, &completed, &errors, 2).unwrap());
        assert!(!eval_condition(&cond_ok(1), &completed, &errors, 2).unwrap());
    }

    #[test]
    fn batch_condition_supports_not_and_or() {
        let completed = vec![true, false];
        let errors: Vec<Option<wire::WireError>> = vec![None, None];
        // `client.batch()` 生成的回滚条件就是 not(ok(COMMIT))。
        let rollback = wire::BatchCond::Not {
            cond: Box::new(cond_ok(1)),
        };
        assert!(eval_condition(&rollback, &completed, &errors, 2).unwrap());
        let any = wire::BatchCond::Or {
            conds: vec![cond_ok(1), cond_ok(0)],
        };
        assert!(eval_condition(&any, &completed, &errors, 2).unwrap());
        let all = wire::BatchCond::And {
            conds: vec![cond_ok(0), cond_ok(1)],
        };
        assert!(!eval_condition(&all, &completed, &errors, 2).unwrap());
    }

    #[test]
    fn out_of_range_step_is_a_protocol_error_not_false() {
        let err = eval_condition(&cond_ok(9), &[true], &[None], 1).expect_err("越界必须报错");
        assert_eq!(err.code(), ErrorCode::InvalidArgument);
    }

    #[test]
    fn is_autocommit_condition_is_refused_loudly() {
        let err = eval_condition(&wire::BatchCond::IsAutocommit, &[], &[], 0)
            .expect_err("v2 不支持 is_autocommit");
        assert_eq!(err.code(), ErrorCode::NotImplemented);
    }

    #[test]
    fn want_rows_false_keeps_empty_arrays_but_reports_affected_rows() {
        let result = ResultSet {
            columns: vec![ColumnMeta::new("x", "INTEGER", true)],
            rows: vec![vec![SqlValue::Integer(1)]],
            affected_rows: 7,
            truncated: false,
        };
        let without_rows = stmt_result(&result, false);
        assert!(without_rows.cols.is_empty());
        assert!(without_rows.rows.is_empty());
        assert_eq!(without_rows.affected_row_count, 7);

        let with_rows = stmt_result(&result, true);
        assert_eq!(with_rows.cols.len(), 1);
        assert_eq!(with_rows.rows.len(), 1);
        assert_eq!(with_rows.rows[0][0], serde_json::json!({"type":"integer","value":"1"}));
    }

    /// 冻结信封 -> Hrana 客户端能读懂的形状。
    ///
    /// 客户端只在**响应顶层**有 `message`、且 `content-type` **精确等于** `application/json`
    /// 时才认业务错误（`errorFromResponse`），否则退化成 `Server returned HTTP status 401`。
    /// 所以这里既断言正文形状，也断言头里没有 `; charset=utf-8`。
    #[tokio::test]
    async fn error_body_shape_is_readable_by_the_sdk() {
        // 认证中间件 / 提取器直接生成的失败响应（401 不经过 handler）也走这条适配。
        let response =
            reshape_error_body(ApiError::unauthenticated("凭据无效").into_response()).await;
        assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("application/json"),
            "客户端是精确匹配，带 charset 会让它读不到结构化错误"
        );
        let bytes = http_body_util::BodyExt::collect(response.into_body())
            .await
            .expect("读取响应体")
            .to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON 响应体");
        // 客户端只在顶层有 message 时才认业务错误；冻结信封同时保留。
        assert_eq!(body["message"], "凭据无效");
        assert_eq!(body["code"], "UNAUTHENTICATED");
        assert_eq!(body["error"]["code"], "UNAUTHENTICATED");
    }

    /// 提取器直接生成的 `text/plain` 拒绝响应（典型是 `DefaultBodyLimit` 触发的 413）
    /// 也要变成客户端能读懂的 JSON 信封，而不是被原样透传回
    /// `Server returned HTTP status 413`。
    #[tokio::test]
    async fn non_json_rejection_is_wrapped_into_a_readable_envelope() {
        // 请求体超限时 axum 直接返回 text/plain，根本不经过 handler。
        let response = (
            axum::http::StatusCode::PAYLOAD_TOO_LARGE,
            [(axum::http::header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            "length limit exceeded",
        )
            .into_response();
        let response = reshape_error_body(response).await;

        assert_eq!(response.status(), axum::http::StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("application/json"),
            "客户端是精确匹配，text/plain 会让它读不到结构化错误"
        );
        let bytes = http_body_util::BodyExt::collect(response.into_body())
            .await
            .expect("读取响应体")
            .to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON 响应体");
        let message = body["message"].as_str().expect("顶层 message");
        assert_eq!(message, "请求体超过服务端上限");
        // 原始正文可能夹带用户输入或内部细节，不能回显。
        assert!(!message.contains("length limit exceeded"));
    }
}
