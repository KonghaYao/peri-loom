//! Hrana over HTTP **v2 / v3** 的线上格式（JSON）。
//!
//! 字段名与标签完全按 `@libsql/hrana-client` 的 `shared/json_encode.js` /
//! `shared/json_decode.js` 对齐：这套编码是客户端唯一认得的契约，"看起来差不多"
//! 就会在运行时炸成 `ProtoError`。三个最容易写错的地方：
//!
//! 1. **值编码不对称**：`integer` 的 `value` 是**字符串**（`{"type":"integer","value":"42"}`，
//!    因为 JSON 数字承载不了完整 i64），`float` 的 `value` 是 **JSON 数字**，
//!    而 `blob` 的字段名是 `base64` 而不是 `value`；
//! 2. **`results` 与 `requests` 必须等长、且逐项 `type` 同名**（客户端对不上长度直接抛错）；
//! 3. **`step_results` / `step_errors` 必须等长**，被条件跳过的步骤两边都填 `null`。
//!
//! 本模块同时描述 v2 与 v3 两套线上格式。官方 SDK 在 HTTP 上只发自己认得的那个版本，
//! 既不探测也不降级：`@libsql/client` 全程只打 `/v2/pipeline`，而
//! `@tursodatabase/serverless` 自发布起（0.1.0）就只打 v3，从不请求 `/v2/pipeline` ——
//! 0.1.0 / 0.1.1 只用 `/v3/cursor`，0.1.2 起 `/v3/pipeline` 才进入调用路径
//! （`exec()` / `executeMultiple()` / `close()`），0.2.0 起 `prepare()` 触发 `describe`。
//! 所以服务端必须两套都认、`/v3/pipeline` 与 `/v3/cursor` 也一个都不能少，
//! 不能指望新客户端退回 v2。
//!
//! 两套格式在**值编码与 `stmt` 形状上完全同构**（第 1 条对 v3 同样成立），
//! 差异只在信封与响应形态：
//!
//! - **v3 cursor**：请求体是单个 `batch`（没有 `requests` 数组，见 [`CursorRequest`]），
//!   响应是 NDJSON 的 step 条目流 —— 首行 [`CursorHeader`]，其后逐行 [`CursorEntry`]；
//! - **v3 pipeline**：比 v2 多一个 `get_autocommit` 请求 / 响应（见
//!   [`StreamResponse::GetAutocommit`]），v2 下不会出现。

use domain::value::SqlValue;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;

/// 客户端发来的 pipeline 请求体。
#[derive(Debug, Deserialize)]
pub struct PipelineRequest {
    /// 会话句柄：服务端在上一次响应里签发，客户端原样带回来。
    #[serde(default)]
    pub baton: Option<String>,
    /// 顺序执行的请求列表。
    pub requests: Vec<StreamRequest>,
}

/// pipeline 里的单个请求（JSON 用 `type` 区分）。
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamRequest {
    /// 执行单条语句。
    Execute {
        /// 语句（SQL 文本或 `sql_id`）。
        stmt: Stmt,
    },
    /// 步骤批处理（含条件求值）。
    Batch {
        /// 步骤列表。
        batch: Batch,
    },
    /// 收尾：服务端必须销毁会话并把 `baton` 置空。
    Close,
    /// 一次执行多条语句（`@libsql/client` 的 `executeMultiple`）。
    Sequence {
        /// SQL 文本。
        #[serde(default)]
        sql: Option<String>,
        /// 已 `store_sql` 的语句 ID。
        #[serde(default)]
        sql_id: Option<i64>,
    },
    /// 语句描述（只 prepare，不执行）。
    Describe {
        /// SQL 文本。
        #[serde(default)]
        sql: Option<String>,
        /// 已 `store_sql` 的语句 ID。
        #[serde(default)]
        sql_id: Option<i64>,
    },
    /// 把 SQL 文本缓存到会话上（`batch` / `transaction` 必经此路）。
    StoreSql {
        /// 客户端分配的 ID。
        sql_id: i64,
        /// SQL 文本。
        sql: String,
    },
    /// 释放缓存的 SQL 文本。
    CloseSql {
        /// 客户端分配的 ID。
        sql_id: i64,
    },
    /// 查询是否处于 autocommit（v3 能力，v2 下不会出现）。
    GetAutocommit,
}

/// 一条待执行语句。
#[derive(Debug, Deserialize)]
pub struct Stmt {
    /// SQL 文本（与 `sql_id` 二选一）。
    #[serde(default)]
    pub sql: Option<String>,
    /// 会话上已缓存 SQL 的 ID（与 `sql` 二选一）。
    #[serde(default)]
    pub sql_id: Option<i64>,
    /// 位置绑定参数。
    #[serde(default)]
    pub args: Vec<serde_json::Value>,
    /// 命名绑定参数（`name` **不带** `:` / `@` / `$` 前缀）。
    #[serde(default)]
    pub named_args: Vec<NamedArg>,
    /// 是否需要回传行数据；缺省按 `true`（与规范一致）。
    #[serde(default = "default_want_rows")]
    pub want_rows: bool,
}

/// 命名绑定参数。
#[derive(Debug, Deserialize)]
pub struct NamedArg {
    /// 参数名（不含前缀）。
    pub name: String,
    /// 参数值（Hrana 值编码）。
    pub value: serde_json::Value,
}

/// 批处理。
#[derive(Debug, Deserialize)]
pub struct Batch {
    /// 步骤列表。
    pub steps: Vec<BatchStep>,
}

/// 批处理步骤。
#[derive(Debug, Deserialize)]
pub struct BatchStep {
    /// 执行条件；缺省表示无条件执行。
    #[serde(default)]
    pub condition: Option<BatchCond>,
    /// 本步骤的语句。
    pub stmt: Stmt,
}

/// 批处理步骤条件（可递归）。
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BatchCond {
    /// 第 `step` 步已执行且成功。
    Ok {
        /// 步骤下标。
        step: usize,
    },
    /// 第 `step` 步已执行但失败。
    Error {
        /// 步骤下标。
        step: usize,
    },
    /// 取反。
    Not {
        /// 子条件。
        cond: Box<BatchCond>,
    },
    /// 全部成立。
    And {
        /// 子条件列表。
        conds: Vec<BatchCond>,
    },
    /// 任一成立。
    Or {
        /// 子条件列表。
        conds: Vec<BatchCond>,
    },
    /// 是否处于 autocommit（v3 能力）。
    IsAutocommit,
}

// ------------------------------------------------------------------ 响应

/// 返回给客户端的 pipeline 响应体。
#[derive(Debug, Serialize)]
pub struct PipelineResponse {
    /// 会话句柄；`null` 表示本次 pipeline 已收尾，后续请求应重新开始。
    pub baton: Option<String>,
    /// 服务端建议的基址。本实现不迁移连接，恒为 `null`。
    pub base_url: Option<String>,
    /// 与请求逐项对应的结果。
    pub results: Vec<StreamResult>,
}

/// 单个请求的结果：成功或失败。
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResult {
    /// 成功。
    Ok {
        /// 与请求 `type` 同名的响应体。
        response: StreamResponse,
    },
    /// 失败（只影响这一条请求）。
    Error {
        /// 错误详情。
        error: WireError,
    },
}

/// 成功响应体（`type` 必须与请求 `type` 一致）。
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResponse {
    /// `close` 的响应。
    Close,
    /// `execute` 的响应。
    Execute {
        /// 语句结果。
        result: StmtResult,
    },
    /// `describe` 的响应。
    Describe {
        /// 语句描述。
        result: DescribeResult,
    },
    /// `batch` 的响应。
    Batch {
        /// 逐步结果。
        result: BatchResult,
    },
    /// `sequence` 的响应（无负载）。
    Sequence,
    /// `store_sql` 的响应（无负载）。
    StoreSql,
    /// `close_sql` 的响应（无负载）。
    CloseSql,
    /// `get_autocommit` 的响应。
    GetAutocommit {
        /// 该连接此刻是否处于 autocommit（`false` = 在事务中）。
        ///
        /// 取值来自 DB Process 回传的流式 trailer，不由 Server 推断（见
        /// [`crate::state::SessionRegistry`] 的 `autocommit` 字段说明）。
        is_autocommit: bool,
    },
}

/// 语句结果。
#[derive(Debug, Clone, Serialize)]
pub struct StmtResult {
    /// 列元数据（`want_rows=false` 时为空数组）。
    pub cols: Vec<WireCol>,
    /// 行数据（`want_rows=false` 时为空数组）。
    pub rows: Vec<Vec<serde_json::Value>>,
    /// 受影响行数。
    pub affected_row_count: u64,
    /// 最后一次插入的 rowid。
    ///
    /// 来自同一引擎连接的语句结束 trailer，以字符串承载完整 i64 精度；
    /// 未上报才为 `null`，不能补零或猜一个可能属于其它连接的值。
    pub last_insert_rowid: Option<String>,
}

/// 列描述。
#[derive(Debug, Clone, Serialize)]
pub struct WireCol {
    /// 列名。
    pub name: String,
    /// 声明类型（可能为空字符串）。
    pub decltype: String,
}

/// `describe` 结果。字段名遵循 Hrana JSON 解码器的蛇形拼写。
#[derive(Debug, Clone, Serialize)]
pub struct DescribeResult {
    /// 参数名数组；匿名参数的 name 为 null。
    pub params: Vec<DescribeParam>,
    /// 结果列。
    pub cols: Vec<WireCol>,
    /// 是否为 EXPLAIN。
    #[serde(rename = "is_explain")]
    pub is_explain: bool,
    /// 语句是否只读。
    #[serde(rename = "is_readonly")]
    pub is_readonly: bool,
}

/// describe 参数。
#[derive(Debug, Clone, Serialize)]
pub struct DescribeParam {
    /// 参数名。
    pub name: Option<String>,
}

/// 批处理结果。
#[derive(Debug, Clone, Serialize)]
pub struct BatchResult {
    /// 逐步结果；被条件跳过或失败的步骤为 `null`。
    pub step_results: Vec<Option<StmtResult>>,
    /// 逐步错误；成功或被跳过的步骤为 `null`。
    pub step_errors: Vec<Option<WireError>>,
}

/// 客户端可识别的错误（`message` 必填，`code` 可选）。
#[derive(Debug, Clone, Serialize)]
pub struct WireError {
    /// 人类可读信息。
    pub message: String,
    /// 平台错误码（`ErrorCode::as_str()`）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

impl WireError {
    /// 由平台错误码与消息构造。
    #[must_use]
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: Some(code.to_string()),
        }
    }
}

impl StreamResult {
    /// 成功结果。
    #[must_use]
    pub fn ok(response: StreamResponse) -> Self {
        Self::Ok { response }
    }

    /// 失败结果。
    #[must_use]
    pub fn error(error: WireError) -> Self {
        Self::Error { error }
    }
}

impl StmtResult {
    /// 无行结果（DDL / DML，或客户端没要行）。
    #[must_use]
    pub fn empty(affected_row_count: u64) -> Self {
        Self {
            cols: Vec::new(),
            rows: Vec::new(),
            affected_row_count,
            last_insert_rowid: None,
        }
    }
}

fn default_want_rows() -> bool {
    true
}

// ------------------------------------------------------------------ 值映射

/// Hrana 值 -> 领域值。
///
/// # Errors
/// 结构不是对象、缺 `type`、字段类型不符或 base64 / 整数非法时返回可读原因。
pub fn value_from_wire(value: &serde_json::Value) -> Result<SqlValue, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "绑定值必须是 {\"type\":...} 形式的对象".to_string())?;
    let kind = object
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "绑定值缺少字符串字段 type".to_string())?;
    match kind {
        "null" => Ok(SqlValue::Null),
        "integer" => {
            // Hrana 用字符串承载 64 位整数（JSON 数字只有 f64 精度）。
            let raw = object
                .get("value")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "integer 的 value 必须是字符串".to_string())?;
            raw.parse::<i64>()
                .map(SqlValue::Integer)
                .map_err(|err| format!("integer 的 value 不是合法 64 位整数: {err}"))
        }
        "float" => object
            .get("value")
            .and_then(serde_json::Value::as_f64)
            .map(SqlValue::Real)
            .ok_or_else(|| "float 的 value 必须是 JSON 数字".to_string()),
        "text" => object
            .get("value")
            .and_then(serde_json::Value::as_str)
            .map(|text| SqlValue::Text(text.to_string()))
            .ok_or_else(|| "text 的 value 必须是字符串".to_string()),
        "blob" => {
            let encoded = object
                .get("base64")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "blob 的字段名是 base64（不是 value）且必须是字符串".to_string())?;
            decode_base64(encoded)
                .map(SqlValue::Blob)
                .map_err(|err| format!("blob 的 base64 非法: {err}"))
        }
        other => Err(format!("不支持的绑定值类型: {other}")),
    }
}

/// 领域值 -> Hrana 值。
#[must_use]
pub fn value_to_wire(value: &SqlValue) -> serde_json::Value {
    match value {
        SqlValue::Null => json!({ "type": "null" }),
        SqlValue::Integer(v) => json!({ "type": "integer", "value": v.to_string() }),
        SqlValue::Real(v) if v.is_finite() => json!({ "type": "float", "value": v }),
        // NaN / ±Infinity 不是合法 JSON，Hrana 的 float 承载不了；
        // 用 null 如实表达"此协议无法表示该值"，而不是替换成 0 之类的假值。
        SqlValue::Real(_) => json!({ "type": "null" }),
        SqlValue::Text(v) => json!({ "type": "text", "value": v }),
        SqlValue::Blob(v) => json!({ "type": "blob", "base64": encode_base64(v) }),
    }
}

fn encode_base64(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn decode_base64(encoded: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(encoded)
}

// ------------------------------------------------------------------ v3 cursor

/// `POST /db/{db_id}/v3/cursor` 的请求体。
///
/// 与 pipeline 的差别：cursor **只**提交一个批处理，没有 `requests` 数组、没有 `items`
/// 的逐条错误信封 —— 步骤级失败改由响应流里的 `step_error` 条目表达（见
/// [`CursorEntry::StepError`]）。因此不复用 [`PipelineRequest`]。
#[derive(Debug, Deserialize)]
pub struct CursorRequest {
    /// 会话句柄；`null` 表示新建。
    #[serde(default)]
    pub baton: Option<String>,
    /// 要执行的批处理。
    pub batch: Batch,
}

/// cursor 响应 NDJSON 的**第一行**（头）。
///
/// 客户端先把第一行读成游标响应、再逐行读后续条目，所以这一行必须最先写、
/// 且自身也以 `\n` 结尾。
#[derive(Debug, Serialize)]
pub struct CursorHeader {
    /// 会话句柄。
    pub baton: Option<String>,
    /// 服务端建议的基址。本实现不迁移连接，恒为 `null`。
    pub base_url: Option<String>,
}

/// cursor 响应流里的一行条目（首行之后）。
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CursorEntry {
    /// 某步骤开始产出。
    StepBegin {
        /// 步骤下标（与请求 `batch.steps` 的下标一致）。
        step: usize,
        /// 列元数据。
        cols: Vec<WireCol>,
    },
    /// 某步骤的一行数据。
    Row {
        /// 步骤下标。
        step: usize,
        /// 行数据（Hrana 值编码）。
        row: Vec<serde_json::Value>,
    },
    /// 某步骤正常结束。
    StepEnd {
        /// 步骤下标。
        step: usize,
        /// 受影响行数。
        affected_row_count: u64,
        /// 最后一次插入的 rowid；同 [`StmtResult::last_insert_rowid`]，未上报才为 `null`。
        last_insert_rowid: Option<String>,
    },
    /// 某步骤失败。
    ///
    /// **不能**把步骤级失败升级成 HTTP 错误：客户端生成的批处理里，回滚靠的是
    /// `error` / `not(ok, …)` 这类条件（见 `client.batch()` 与 `transaction()`），
    /// 一旦整请求失败，回滚步骤永远不会被执行，事务也就留在半途。
    StepError {
        /// 步骤下标。
        step: usize,
        /// 错误详情。
        error: WireError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_round_trip_keeps_types() {
        let cases = [
            SqlValue::Null,
            SqlValue::Integer(i64::MIN),
            SqlValue::Integer(9_007_199_254_740_993),
            SqlValue::Real(1.5),
            SqlValue::Text("你好".to_string()),
            SqlValue::Blob(vec![0, 1, 2, 255]),
        ];
        for value in cases {
            let wire = value_to_wire(&value);
            let back = value_from_wire(&wire).expect("回解必须成功");
            assert_eq!(back, value, "wire={wire}");
        }
    }

    #[test]
    fn integer_is_encoded_as_string_and_blob_uses_base64_field() {
        // 这两条是客户端解码器的硬要求，写错就是运行时 ProtoError。
        let integer = value_to_wire(&SqlValue::Integer(42));
        assert_eq!(integer["value"], json!("42"));
        let blob = value_to_wire(&SqlValue::Blob(vec![1, 2, 3]));
        assert_eq!(blob["base64"], json!("AQID"));
        assert!(blob.get("value").is_none());
    }

    #[test]
    fn non_finite_floats_degrade_to_null_instead_of_invalid_json() {
        assert_eq!(
            value_to_wire(&SqlValue::Real(f64::NAN)),
            json!({"type":"null"})
        );
        assert_eq!(
            value_to_wire(&SqlValue::Real(f64::INFINITY)),
            json!({"type":"null"})
        );
    }

    #[test]
    fn malformed_values_report_readable_reasons() {
        assert!(value_from_wire(&json!("42")).unwrap_err().contains("对象"));
        assert!(value_from_wire(&json!({"type":"integer","value":42}))
            .unwrap_err()
            .contains("字符串"));
        assert!(value_from_wire(&json!({"type":"blob","value":"AQID"}))
            .unwrap_err()
            .contains("base64"));
        assert!(value_from_wire(&json!({"type":"uuid"}))
            .unwrap_err()
            .contains("不支持"));
    }

    #[test]
    fn integer_beyond_i64_is_rejected_not_truncated() {
        let err = value_from_wire(&json!({"type":"integer","value":"9223372036854775808"}))
            .expect_err("溢出必须报错");
        assert!(err.contains("64 位整数"), "{err}");
    }

    // -------------------------------------------------------------- v3 线格式

    #[test]
    fn cursor_request_parses_the_shape_the_sdk_actually_sends() {
        // 原样照抄 `@tursodatabase/serverless` 的 `client.batch()` 发出的 JSON：
        // 顶层只有 baton + batch（没有 `requests`），条件走 `is_autocommit`。
        // baton=null 表示新建会话，不能当成缺字段直接报错。
        let raw = json!({
            "baton": null,
            "batch": {
                "steps": [{
                    "stmt": {"sql": "SELECT 1", "args": [], "named_args": [], "want_rows": false},
                    "condition": {"type": "is_autocommit"}
                }]
            }
        });
        let request: CursorRequest = serde_json::from_value(raw).expect("cursor 请求必须可解析");

        assert!(request.baton.is_none(), "baton=null 应解成 None");
        assert_eq!(request.batch.steps.len(), 1);
        let step = &request.batch.steps[0];
        assert_eq!(step.stmt.sql.as_deref(), Some("SELECT 1"));
        assert!(step.stmt.sql_id.is_none());
        assert!(step.stmt.args.is_empty());
        assert!(step.stmt.named_args.is_empty());
        // 显式传了 want_rows=false 就必须是 false，不能被缺省值 true 盖掉。
        assert!(!step.stmt.want_rows);
        assert!(matches!(step.condition, Some(BatchCond::IsAutocommit)));
    }

    #[test]
    fn batch_cond_is_autocommit_parses_from_its_bare_tag() {
        // 变体无负载，SDK 只发 `{"type":"is_autocommit"}`，多要求一个字段就解析不了。
        let cond: BatchCond =
            serde_json::from_value(json!({"type": "is_autocommit"})).expect("条件必须可解析");
        assert!(matches!(cond, BatchCond::IsAutocommit));
    }

    #[test]
    fn cursor_entry_tags_and_fields_match_the_protocol() {
        let cols = vec![WireCol {
            name: "x".to_string(),
            decltype: "INTEGER".to_string(),
        }];
        let cases = [
            (
                CursorEntry::StepBegin { step: 0, cols },
                json!({"type":"step_begin","step":0,"cols":[{"name":"x","decltype":"INTEGER"}]}),
            ),
            (
                CursorEntry::Row {
                    step: 1,
                    row: vec![json!({"type":"integer","value":"7"})],
                },
                json!({"type":"row","step":1,"row":[{"type":"integer","value":"7"}]}),
            ),
            (
                CursorEntry::StepEnd {
                    step: 2,
                    affected_row_count: 3,
                    last_insert_rowid: None,
                },
                json!({"type":"step_end","step":2,"affected_row_count":3,"last_insert_rowid":null}),
            ),
            (
                CursorEntry::StepError {
                    step: 3,
                    error: WireError {
                        message: "boom".to_string(),
                        code: Some("INTERNAL".to_string()),
                    },
                },
                json!({"type":"step_error","step":3,"error":{"message":"boom","code":"INTERNAL"}}),
            ),
        ];
        for (entry, expected) in cases {
            assert_eq!(
                serde_json::to_value(&entry).expect("序列化必须成功"),
                expected
            );
        }
    }

    #[test]
    fn cursor_step_end_always_carries_last_insert_rowid() {
        // 客户端逐键读这两个字段；`last_insert_rowid` 为 None 时被 skip 就会读成
        // undefined，而不是"未知"。
        let value = serde_json::to_value(CursorEntry::StepEnd {
            step: 0,
            affected_row_count: 0,
            last_insert_rowid: None,
        })
        .expect("序列化必须成功");
        assert_eq!(value["affected_row_count"], json!(0));
        assert!(
            value.get("last_insert_rowid").is_some(),
            "last_insert_rowid 即使为 None 也必须以 null 出现: {value}"
        );
    }

    #[test]
    fn get_autocommit_false_still_carries_the_field() {
        // 客户端读 `results[].response.is_autocommit`；字段被 skip 会读成 undefined
        // （假值），于是"在事务中"看起来和"在 autocommit"一样。两种取值都要发字段。
        for is_autocommit in [true, false] {
            let value = serde_json::to_value(StreamResponse::GetAutocommit { is_autocommit })
                .expect("序列化必须成功");
            assert_eq!(
                value,
                json!({"type": "get_autocommit", "is_autocommit": is_autocommit}),
                "is_autocommit={is_autocommit}"
            );
        }
    }
}
