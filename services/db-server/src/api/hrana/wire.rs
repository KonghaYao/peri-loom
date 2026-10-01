//! Hrana over HTTP **v2** 的线上格式（JSON）。
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
//! 只实现 v2：客户端在 HTTP 上只用 v2 + JSON（不探测版本、不降级），
//! `get_autocommit` / `cursor` 这些 v3 能力在 v2 下不会出现。

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
    /// 语句描述（v2 未实现，返回明确错误而不是空结果）。
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
    /// 平台的结果集契约里没有这个字段（见 `domain::value::ResultSet`），
    /// 因此**恒为 `null`**：宁可如实报告"未知"，也不猜一个可能属于其它连接的值。
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
        assert_eq!(value_to_wire(&SqlValue::Real(f64::NAN)), json!({"type":"null"}));
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
}
