//! NDJSON（`application/x-ndjson`）编码（架构 §17.4 Data API 契约）。
//!
//! 行格式（每行一个独立 JSON 对象，客户端逐行解析即可）：
//!
//! ```text
//! {"type":"header","columns":[{"name":"id","type_name":"INTEGER","nullable":false}]}
//! {"type":"row","values":[1,"alice"]}
//! {"type":"trailer","affected_rows":0,"wal_lsn":4096,"elapsed_micros":1200}
//! {"type":"error","error":{"code":"SQL_ERROR","message":"...","request_id":"...","retryable":false}}
//! ```
//!
//! 设计取舍：
//! - **行值用数组而不是对象**：SQL 允许同名列（`SELECT 1 AS a, 2 AS a`），
//!   对象形式会静默丢列。列名放在 header 行里，顺序即数组下标。
//! - 先发 header 再发 row：客户端不必缓存整个结果集就能建表。
//! - 出错时发 `error` 行并终止：流已经开始写出，无法再改 HTTP 状态码，
//!   只能把结构化错误放进流内（与冻结错误体共用同一份 `ErrorBody`）。

use bytes::Bytes;
use domain::error::PlatformError;
use domain::value::{ColumnMeta, SqlValue};
use serde::Serialize;

use crate::error::ErrorBody;

/// NDJSON 行的类型标签。
pub const LINE_TYPE_HEADER: &str = "header";
/// 数据行。
pub const LINE_TYPE_ROW: &str = "row";
/// 结束行。
pub const LINE_TYPE_TRAILER: &str = "trailer";
/// 错误行。
pub const LINE_TYPE_ERROR: &str = "error";

/// header 行里的列描述。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct NdjsonColumn {
    /// 列名（可能重复，因此值用数组承载）。
    pub name: String,
    /// 类型名（引擎上报的字面量）。
    pub type_name: String,
    /// 是否可空。
    pub nullable: bool,
}

/// 把单个 [`SqlValue`] 编码为 JSON。
///
/// 编码规则与内联 JSON 响应完全一致，避免同一次查询在两种模式下值语义不同：
/// `Blob` 用 base64 字符串（`{"$blob":"..."}` 会破坏「值是 JSON 原语」的直觉，故用
/// 前缀对象显式标注，客户端无需类型推断）；`Null` 直接是 JSON `null`。
#[must_use]
pub fn encode_value(value: &SqlValue) -> serde_json::Value {
    match value {
        SqlValue::Null => serde_json::Value::Null,
        SqlValue::Integer(v) => serde_json::Value::from(*v),
        SqlValue::Real(v) => serde_json::Number::from_f64(*v)
            .map(serde_json::Value::Number)
            // NaN / Infinity 不是合法 JSON：降级为 null，避免整行序列化失败
            .unwrap_or(serde_json::Value::Null),
        SqlValue::Text(v) => serde_json::Value::String(v.clone()),
        // Blob 用 base64 + 显式标注，客户端可无歧义还原二进制
        SqlValue::Blob(v) => serde_json::json!({ "$blob": base64_encode(v) }),
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// header 行。
#[must_use]
pub fn header_line(columns: &[ColumnMeta]) -> Bytes {
    let columns: Vec<NdjsonColumn> = columns
        .iter()
        .map(|column| NdjsonColumn {
            name: column.name.clone(),
            type_name: column.type_name.clone(),
            nullable: column.nullable,
        })
        .collect();
    line(&serde_json::json!({
        "type": LINE_TYPE_HEADER,
        "columns": columns,
    }))
}

/// 数据行（值按列顺序排列）。
#[must_use]
pub fn row_line(values: &[SqlValue]) -> Bytes {
    let encoded: Vec<serde_json::Value> = values.iter().map(encode_value).collect();
    line(&serde_json::json!({
        "type": LINE_TYPE_ROW,
        "values": encoded,
    }))
}

/// 结束行。
#[must_use]
pub fn trailer_line(
    affected_rows: u64,
    wal_lsn: impl Into<Option<u64>>,
    elapsed_micros: u64,
) -> Bytes {
    line(&serde_json::json!({
        "type": LINE_TYPE_TRAILER,
        "affected_rows": affected_rows,
        "wal_lsn": wal_lsn.into(),
        "elapsed_micros": elapsed_micros,
    }))
}

/// 错误行：流已开始写出时唯一能表达错误的方式。
#[must_use]
pub fn error_line(body: &ErrorBody) -> Bytes {
    line(&serde_json::json!({
        "type": LINE_TYPE_ERROR,
        "error": body,
    }))
}

/// 把 JSON 值编码成一行（追加 `\n`）。
#[must_use]
pub fn line(value: &serde_json::Value) -> Bytes {
    let mut buffer = serde_json::to_vec(value).unwrap_or_else(|_| {
        // serde_json 对上述结构不会失败；兜底成结构化错误而不是 panic
        br#"{"type":"error","error":{"code":"INTERNAL_ERROR","message":"ndjson encode failed","request_id":"","retryable":false,"route_retry_count":0}}"#.to_vec()
    });
    buffer.push(b'\n');
    Bytes::from(buffer)
}

/// 由 `PlatformError` 构造错误行（补齐 request_id，保持与冻结错误体一致）。
#[must_use]
pub fn error_line_from(error: &PlatformError, request_id: &str) -> Bytes {
    let body = ErrorBody {
        code: error.code.as_str().to_string(),
        message: error.message.clone(),
        request_id: request_id.to_string(),
        retryable: error.retryable,
        detail: error.detail.clone(),
        route_retry_count: error.route_retry_count,
    };
    error_line(&body)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(bytes: &Bytes) -> serde_json::Value {
        let text = std::str::from_utf8(bytes).expect("utf-8");
        assert!(text.ends_with('\n'), "NDJSON 行必须以换行结尾");
        assert_eq!(text.matches('\n').count(), 1, "一行只能有一个换行");
        serde_json::from_str(text.trim_end()).expect("合法 JSON")
    }

    #[test]
    fn header_line_lists_columns_in_order() {
        let columns = vec![
            ColumnMeta::new("id", "INTEGER", false),
            ColumnMeta::new("name", "TEXT", true),
        ];
        let value = parse(&header_line(&columns));
        assert_eq!(value["type"], "header");
        assert_eq!(value["columns"][0]["name"], "id");
        assert_eq!(value["columns"][1]["type_name"], "TEXT");
        assert_eq!(value["columns"][1]["nullable"], true);
    }

    #[test]
    fn row_line_keeps_values_positional_so_duplicate_names_survive() {
        let row = vec![SqlValue::Integer(7), SqlValue::text("x"), SqlValue::Null];
        let value = parse(&row_line(&row));
        assert_eq!(value["type"], "row");
        assert_eq!(value["values"][0], 7);
        assert_eq!(value["values"][1], "x");
        assert!(value["values"][2].is_null());
    }

    #[test]
    fn blob_is_encoded_with_explicit_marker() {
        let row = vec![SqlValue::blob(vec![0xde, 0xad])];
        let value = parse(&row_line(&row));
        assert_eq!(value["values"][0]["$blob"], "3q0=");
    }

    #[test]
    fn non_finite_real_degrades_to_null_instead_of_failing() {
        let row = vec![SqlValue::Real(f64::NAN), SqlValue::Real(f64::INFINITY)];
        let value = parse(&row_line(&row));
        assert!(value["values"][0].is_null());
        assert!(value["values"][1].is_null());
    }

    #[test]
    fn trailer_line_carries_durability_fields() {
        let value = parse(&trailer_line(3, 8192, 900));
        assert_eq!(value["type"], "trailer");
        assert_eq!(value["affected_rows"], 3);
        assert_eq!(value["wal_lsn"], 8192);
        assert_eq!(value["elapsed_micros"], 900);
    }

    #[test]
    fn error_line_uses_frozen_error_shape() {
        let err = PlatformError::new(domain::error::ErrorCode::SqlError, "boom");
        let value = parse(&error_line_from(&err, "req-9"));
        assert_eq!(value["type"], "error");
        assert_eq!(value["error"]["code"], "SQL_ERROR");
        assert_eq!(value["error"]["request_id"], "req-9");
        assert_eq!(value["error"]["retryable"], false);
    }
}
