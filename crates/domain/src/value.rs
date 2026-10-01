//! SQL 结果值、列元数据与结果集。
//!
//! 值类型对标 SQLite/TursoDB 的动态类型：NULL / INTEGER / REAL / TEXT / BLOB。
//!
//! `SqlValue` 的相等性对 `Real` 采用 [`f64::total_cmp`] 语义：`NaN == NaN`、
//! `0.0 != -0.0`，从而保证测试与缓存键的确定性（普通 `PartialEq` 会让 `NaN != NaN`）。

use std::fmt;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

/// 一个 SQL 值。
///
/// serde 采用 untagged 表示，直接映射到 JSON 原生类型（`null` / number / string / array），
/// 便于 `/data/v1` 的 NDJSON 与 JSON 输出。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SqlValue {
    /// SQL NULL。
    Null,
    /// 64 位整数。
    Integer(i64),
    /// 64 位浮点。
    Real(f64),
    /// UTF-8 文本。
    Text(String),
    /// 二进制。
    Blob(Vec<u8>),
}

impl SqlValue {
    /// 文本值。
    #[must_use]
    pub fn text(value: impl Into<String>) -> Self {
        SqlValue::Text(value.into())
    }

    /// 二进制值。
    #[must_use]
    pub fn blob(value: impl Into<Vec<u8>>) -> Self {
        SqlValue::Blob(value.into())
    }

    /// 是否为 NULL。
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, SqlValue::Null)
    }

    /// 是否为数值类型。
    #[must_use]
    pub const fn is_numeric(&self) -> bool {
        matches!(self, SqlValue::Integer(_) | SqlValue::Real(_))
    }

    /// 取整数（仅 INTEGER）。
    #[must_use]
    pub const fn as_i64(&self) -> Option<i64> {
        match self {
            SqlValue::Integer(value) => Some(*value),
            _ => None,
        }
    }

    /// 取浮点（INTEGER 会提升为 f64；REAL 原样返回）。
    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            SqlValue::Integer(value) => Some(*value as f64),
            SqlValue::Real(value) => Some(*value),
            _ => None,
        }
    }

    /// 取文本。
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            SqlValue::Text(value) => Some(value),
            _ => None,
        }
    }

    /// 取二进制。
    #[must_use]
    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            SqlValue::Blob(value) => Some(value),
            _ => None,
        }
    }

    /// SQL 类型名（用于列元数据与错误信息）。
    #[must_use]
    pub const fn type_name(&self) -> &'static str {
        match self {
            SqlValue::Null => "NULL",
            SqlValue::Integer(_) => "INTEGER",
            SqlValue::Real(_) => "REAL",
            SqlValue::Text(_) => "TEXT",
            SqlValue::Blob(_) => "BLOB",
        }
    }

    /// 值占用的近似字节数，用于结果集截断判定（BLOB 按原始长度，TEXT 按 UTF-8 长度）。
    #[must_use]
    pub fn estimated_size(&self) -> usize {
        match self {
            SqlValue::Null => 0,
            SqlValue::Integer(_) | SqlValue::Real(_) => 8,
            SqlValue::Text(value) => value.len(),
            SqlValue::Blob(value) => value.len(),
        }
    }
}

impl PartialEq for SqlValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SqlValue::Null, SqlValue::Null) => true,
            (SqlValue::Integer(a), SqlValue::Integer(b)) => a == b,
            // total_cmp：NaN 等于自身，-0.0 不等于 0.0，保证结果可复现
            (SqlValue::Real(a), SqlValue::Real(b)) => a.total_cmp(b).is_eq(),
            (SqlValue::Text(a), SqlValue::Text(b)) => a == b,
            (SqlValue::Blob(a), SqlValue::Blob(b)) => a == b,
            _ => false,
        }
    }
}

// total_cmp 是全序（自反、传递），因此 Eq 成立。
impl Eq for SqlValue {}

impl Hash for SqlValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            SqlValue::Null => state.write_u8(0),
            SqlValue::Integer(value) => {
                state.write_u8(1);
                value.hash(state);
            }
            SqlValue::Real(value) => {
                state.write_u8(2);
                // 与 total_cmp 相等性一致：相等值必然同 bits
                state.write_u64(value.to_bits());
            }
            SqlValue::Text(value) => {
                state.write_u8(3);
                value.hash(state);
            }
            SqlValue::Blob(value) => {
                state.write_u8(4);
                value.hash(state);
            }
        }
    }
}

impl fmt::Display for SqlValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlValue::Null => f.write_str("NULL"),
            SqlValue::Integer(value) => write!(f, "{value}"),
            // {:?} 保证整数值输出 "1.0"、极大/极小值走科学计数法，避免与 INTEGER 混淆
            SqlValue::Real(value) => write!(f, "{value:?}"),
            SqlValue::Text(value) => f.write_str(value),
            // 采用 SQLite blob literal 形式：x'deadbeef'
            SqlValue::Blob(value) => write!(f, "x'{}'", hex::encode(value)),
        }
    }
}

impl From<i64> for SqlValue {
    fn from(value: i64) -> Self {
        SqlValue::Integer(value)
    }
}

impl From<i32> for SqlValue {
    fn from(value: i32) -> Self {
        SqlValue::Integer(i64::from(value))
    }
}

impl From<f64> for SqlValue {
    fn from(value: f64) -> Self {
        SqlValue::Real(value)
    }
}

impl From<String> for SqlValue {
    fn from(value: String) -> Self {
        SqlValue::Text(value)
    }
}

impl From<&str> for SqlValue {
    fn from(value: &str) -> Self {
        SqlValue::Text(value.to_string())
    }
}

impl From<bool> for SqlValue {
    fn from(value: bool) -> Self {
        SqlValue::Integer(i64::from(value))
    }
}

impl<T: Into<SqlValue>> From<Option<T>> for SqlValue {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(inner) => inner.into(),
            None => SqlValue::Null,
        }
    }
}

/// 列元数据。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMeta {
    /// 列名（alias 后的名字）。
    pub name: String,
    /// 声明类型名（TursoDB / SQL 侧的类型，可能为空字符串）。
    pub type_name: String,
    /// 是否可空。
    pub nullable: bool,
}

impl ColumnMeta {
    /// 构造列元数据。
    #[must_use]
    pub fn new(name: impl Into<String>, type_name: impl Into<String>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
            nullable,
        }
    }

    /// 未知类型名（例如表达式结果）的占位元数据。
    #[must_use]
    pub fn unknown(name: impl Into<String>) -> Self {
        Self::new(name, "", true)
    }
}

/// 查询结果集（行优先）。
///
/// `truncated` 表示结果因大小上限被截断（见 `RESULT_TOO_LARGE` 契约）：
/// 截断是**显式**语义，调用方必须能看到而不是被静默丢弃。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultSet {
    /// 列元数据。
    pub columns: Vec<ColumnMeta>,
    /// 行数据，每行长度与 `columns` 一致。
    pub rows: Vec<Vec<SqlValue>>,
    /// 受影响行数（INSERT / UPDATE / DELETE）；查询为 0。
    pub affected_rows: u64,
    /// 是否被截断。
    pub truncated: bool,
}

impl ResultSet {
    /// 空结果集。
    #[must_use]
    pub fn new(columns: Vec<ColumnMeta>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
            affected_rows: 0,
            truncated: false,
        }
    }

    /// 只有受影响行数的结果（DML）。
    #[must_use]
    pub fn with_affected_rows(affected_rows: u64) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows,
            truncated: false,
        }
    }

    /// 追加一行（调用方保证列数一致；此处不 panic，长度不符时忽略）。
    pub fn push_row(&mut self, row: Vec<SqlValue>) {
        if row.len() == self.columns.len() || self.columns.is_empty() {
            self.rows.push(row);
        }
    }

    /// 标记结果被截断。
    pub fn mark_truncated(&mut self) {
        self.truncated = true;
    }

    /// 列数。
    #[must_use]
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// 行数。
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// 是否无行。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// 按列名取值（找不到列返回 `None`）。
    #[must_use]
    pub fn value_by_name(&self, row: usize, column: &str) -> Option<&SqlValue> {
        let index = self.columns.iter().position(|c| c.name == column)?;
        self.rows.get(row)?.get(index)
    }

    /// 所有单元格的近似总字节数（用于结果大小上限判定）。
    #[must_use]
    pub fn estimated_size(&self) -> usize {
        self.rows
            .iter()
            .flat_map(|row| row.iter())
            .map(SqlValue::estimated_size)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_equality_is_total_cmp_based() {
        // NaN == NaN（测试确定性）
        assert_eq!(SqlValue::Real(f64::NAN), SqlValue::Real(f64::NAN));
        // ±0.0 在 total_cmp 下不相等
        assert_ne!(SqlValue::Real(0.0), SqlValue::Real(-0.0));
        assert_eq!(SqlValue::Real(1.5), SqlValue::Real(1.5));
        assert_ne!(SqlValue::Real(1.5), SqlValue::Real(1.6));
    }

    #[test]
    fn cross_type_values_are_never_equal() {
        assert_ne!(SqlValue::Integer(1), SqlValue::Real(1.0));
        assert_ne!(SqlValue::Text("1".into()), SqlValue::Integer(1));
        assert_ne!(SqlValue::Null, SqlValue::Integer(0));
        assert_ne!(
            SqlValue::Blob(vec![1, 2]),
            SqlValue::Text("\u{1}\u{2}".into())
        );
        assert_eq!(SqlValue::Null, SqlValue::Null);
        assert_eq!(SqlValue::Text("a".into()), SqlValue::text("a"));
        assert_eq!(
            SqlValue::Blob(vec![0xde, 0xad]),
            SqlValue::blob(vec![0xde, 0xad])
        );
    }

    #[test]
    fn hashing_matches_equality_for_nan() {
        use std::collections::HashMap;
        let mut map: HashMap<SqlValue, &str> = HashMap::new();
        map.insert(SqlValue::Real(f64::NAN), "nan");
        // 相等性一致 => 同一 key 能命中
        assert_eq!(map.get(&SqlValue::Real(f64::NAN)), Some(&"nan"));
        assert_eq!(map.get(&SqlValue::Real(0.0)), None);
    }

    #[test]
    fn accessors_and_type_names() {
        assert!(SqlValue::Null.is_null());
        assert_eq!(SqlValue::Null.type_name(), "NULL");
        assert_eq!(SqlValue::Integer(7).as_i64(), Some(7));
        assert_eq!(SqlValue::Integer(7).as_f64(), Some(7.0));
        assert_eq!(SqlValue::Real(1.5).as_f64(), Some(1.5));
        assert_eq!(SqlValue::Real(1.5).as_i64(), None);
        assert_eq!(SqlValue::text("hi").as_str(), Some("hi"));
        assert_eq!(SqlValue::blob(vec![1]).as_blob(), Some(&[1u8][..]));
        assert!(SqlValue::Integer(1).is_numeric());
        assert!(!SqlValue::Null.is_numeric());
        assert_eq!(SqlValue::Integer(1).estimated_size(), 8);
        assert_eq!(SqlValue::text("abc").estimated_size(), 3);
        assert_eq!(SqlValue::Null.estimated_size(), 0);
    }

    #[test]
    fn display_is_deterministic() {
        assert_eq!(SqlValue::Null.to_string(), "NULL");
        assert_eq!(SqlValue::Integer(-3).to_string(), "-3");
        assert_eq!(SqlValue::Real(1.0).to_string(), "1.0");
        assert_eq!(SqlValue::Real(-0.5).to_string(), "-0.5");
        assert_eq!(SqlValue::text("abc").to_string(), "abc");
        assert_eq!(SqlValue::blob(vec![0xde, 0xad]).to_string(), "x'dead'");
    }

    #[test]
    fn from_conversions() {
        assert_eq!(SqlValue::from(1i64), SqlValue::Integer(1));
        assert_eq!(SqlValue::from(1i32), SqlValue::Integer(1));
        assert_eq!(SqlValue::from(1.0f64), SqlValue::Real(1.0));
        assert_eq!(SqlValue::from("x"), SqlValue::text("x"));
        assert_eq!(SqlValue::from(String::from("x")), SqlValue::text("x"));
        assert_eq!(SqlValue::from(true), SqlValue::Integer(1));
        assert_eq!(SqlValue::from(Option::<i64>::None), SqlValue::Null);
        assert_eq!(SqlValue::from(Some(5i64)), SqlValue::Integer(5));
    }

    #[test]
    fn json_representation_is_native() {
        let row = vec![
            SqlValue::Null,
            SqlValue::Integer(42),
            SqlValue::Real(1.5),
            SqlValue::text("abc"),
            SqlValue::blob(vec![1, 2, 3]),
        ];
        let json = serde_json::to_value(&row).unwrap();
        assert_eq!(json, serde_json::json!([null, 42, 1.5, "abc", [1, 2, 3]]));
        let back: Vec<SqlValue> = serde_json::from_value(json).unwrap();
        assert_eq!(back, row);
    }

    #[test]
    fn result_set_helpers() {
        let mut rs = ResultSet::new(vec![
            ColumnMeta::new("id", "INTEGER", false),
            ColumnMeta::unknown("name"),
        ]);
        rs.push_row(vec![SqlValue::Integer(1), SqlValue::text("a")]);
        rs.push_row(vec![SqlValue::Integer(2), SqlValue::text("b")]);
        // 列数不符的行被忽略，不 panic
        rs.push_row(vec![SqlValue::Integer(3)]);
        assert_eq!(rs.row_count(), 2);
        assert_eq!(rs.column_count(), 2);
        assert!(!rs.is_empty());
        assert_eq!(rs.value_by_name(1, "name"), Some(&SqlValue::text("b")));
        assert_eq!(rs.value_by_name(0, "missing"), None);
        rs.mark_truncated();
        assert!(rs.truncated);
        assert!(rs.estimated_size() >= 4);

        let dml = ResultSet::with_affected_rows(3);
        assert_eq!(dml.affected_rows, 3);
        assert!(dml.is_empty());
    }

    #[test]
    fn result_set_serde_round_trip() {
        let mut rs = ResultSet::new(vec![ColumnMeta::new("v", "TEXT", true)]);
        rs.push_row(vec![SqlValue::text("x")]);
        let json = serde_json::to_string(&rs).unwrap();
        let back: ResultSet = serde_json::from_str(&json).unwrap();
        assert_eq!(back, rs);
    }
}
