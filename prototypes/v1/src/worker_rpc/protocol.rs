use serde::de::Deserializer;
use serde::ser::{SerializeMap, SerializeSeq, Serializer};
use serde::{Deserialize, Serialize};

use turso_core::{Numeric, Value as SqlValue};

use crate::ownership::types::{LeaseBinding, OwnershipCommand};
use crate::runtime::DatabaseId;
use crate::session::StatementColumn;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerRequest {
    pub version: u32,
    pub request_id: String,
    pub command: WorkerCommand,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkerCommand {
    WriterProfileStatus,
    Ownership {
        action: OwnershipCommand,
    },
    OpenSession {
        database: DatabaseId,
    },
    OpenOwnedSession {
        database: DatabaseId,
        owner_incarnation: String,
    },
    CloseOwnedSessions {
        binding: LeaseBinding,
        owner_incarnation: String,
        open_request_id: String,
    },
    Execute {
        session: String,
        sql: String,
    },
    Query {
        session: String,
        sql: String,
    },
    Describe {
        session: String,
        sql: String,
    },
    // Reads the connection state a protocol condition asks about, on the session
    // it belongs to, without preparing or stepping SQL.
    SessionAutocommit {
        session: String,
    },
    Statement {
        session: String,
        sql: String,
        #[serde(default)]
        args: Vec<StatementArg>,
        #[serde(default)]
        named_args: Vec<NamedStatementArg>,
        // Absent means the caller expects the rows, so a caller that predates
        // this field keeps the full result.
        #[serde(default = "want_rows_default")]
        want_rows: bool,
    },
    // One statement prepared, bound and stepped with the engine's read-only
    // verdict required before every step. A statement the engine classifies as
    // writing before it runs answers `requires_intent` with an engine verdict;
    // transaction/context refusals have distinct reasons and cannot seed a
    // cross-session write hint. No writing instruction runs on this path.
    GuardedReadonlyStatement {
        session: String,
        sql: String,
        #[serde(default)]
        args: Vec<StatementArg>,
        #[serde(default)]
        named_args: Vec<NamedStatementArg>,
        #[serde(default = "want_rows_default")]
        want_rows: bool,
    },
    // One implicit session open fused with the first guarded read-only statement.
    // The worker opens exactly as `open_session` and then runs the statement in the
    // same request's lease window, so the authority validation is paid once. A
    // statement the engine classifies as writing is refused before it steps, which
    // is what makes the whole command unable to commit.
    OpenGuardedStatement {
        database: DatabaseId,
        sql: String,
        #[serde(default)]
        args: Vec<StatementArg>,
        #[serde(default)]
        named_args: Vec<NamedStatementArg>,
        #[serde(default = "want_rows_default")]
        want_rows: bool,
        // The caller knows the session ends with this statement (a pipeline whose
        // last entry is `close`), so the close joins the same request and lease
        // window. The field is absent from the protocol's first version: an older
        // worker rejects the whole body instead of ignoring a flag it cannot honor.
        #[serde(default)]
        close_after_statement: bool,
    },
    // The same guarded statement, repeated for a caller-known contiguous block of
    // read-only work. Each statement keeps its own budget, deadline check and
    // engine verdict; the block only removes round trips, never evidence.
    GuardedReadonlyBatch {
        session: String,
        statements: Vec<BatchStatement>,
        // Remaining pipeline result budget in JSON bytes. The worker stops after
        // the statement that crosses it and reports how many it decided.
        max_result_bytes: usize,
    },
    // Feature discovery for a caller that must know whether this worker parses
    // the batch command before it sends one. Answered before any session exists;
    // an older worker rejects the request as invalid before dispatch.
    Capabilities,
    OpenReadCursor {
        session: String,
        sql: String,
        #[serde(default)]
        args: Vec<StatementArg>,
        #[serde(default)]
        named_args: Vec<NamedStatementArg>,
    },
    OpenReadCursorFirst {
        session: String,
        sql: String,
        #[serde(default)]
        args: Vec<StatementArg>,
        #[serde(default)]
        named_args: Vec<NamedStatementArg>,
        max_rows: usize,
    },
    FetchReadCursor {
        session: String,
        max_rows: usize,
    },
    CloseReadCursor {
        session: String,
    },
    OpenWriteCursor {
        session: String,
        sql: String,
        #[serde(default)]
        args: Vec<StatementArg>,
        #[serde(default)]
        named_args: Vec<NamedStatementArg>,
    },
    OpenWriteCursorFirst {
        session: String,
        sql: String,
        #[serde(default)]
        args: Vec<StatementArg>,
        #[serde(default)]
        named_args: Vec<NamedStatementArg>,
        max_rows: usize,
    },
    FetchWriteCursor {
        session: String,
        max_rows: usize,
    },
    CloseWriteCursor {
        session: String,
    },
    Checkpoint {
        session: String,
    },
    Compact {
        session: String,
    },
    Diagnostics {
        session: String,
    },
    CloseSession {
        session: String,
    },
    GetMetrics,
    GetCapacityTrace {
        stream_id: String,
        after_sequence: u64,
        max_events: usize,
    },
    Shutdown,
    ShutdownWithin {
        remaining_ms: u64,
    },
}

/// Only emitted before dispatch; absence never establishes non-execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionRejection {
    pub error: String,
    pub execution_completed: bool,
    pub admission: AdmissionOutcome,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionOutcome {
    NotAdmitted,
}

impl AdmissionRejection {
    pub fn encode(reason: &str) -> Vec<u8> {
        serde_json::to_vec(&Self {
            error: reason.into(),
            execution_completed: false,
            admission: AdmissionOutcome::NotAdmitted,
        })
        .expect("admission response serializes")
    }
}

fn want_rows_default() -> bool {
    true
}

/// One entry of `guarded_readonly_batch`. The defaults mirror
/// `GuardedReadonlyStatement`, so a statement that names only its SQL behaves the
/// same on both commands.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchStatement {
    pub sql: String,
    #[serde(default)]
    pub args: Vec<StatementArg>,
    #[serde(default)]
    pub named_args: Vec<NamedStatementArg>,
    #[serde(default = "want_rows_default")]
    pub want_rows: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum StatementArg {
    Null,
    Integer { value: String },
    Float { value: f64 },
    Text { value: String },
    Blob { base64: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedStatementArg {
    pub name: String,
    pub value: StatementArg,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionState {
    pub session: String,
    pub binding: LeaseBinding,
    pub owner: bool,
    pub sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerResponse {
    pub version: u32,
    pub request_id: String,
    pub ok: bool,
    pub result: Option<WorkerResult>,
    pub error: Option<String>,
    // 失败时 true 表示执行已收束（不保证回滚），false 表示未进入执行；缺失表示无证据。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_completed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_state: Option<SessionState>,
}

/// One row-bearing worker answer. Cursor pages keep the engine rows and let the
/// reply serializer walk them directly: building a `serde_json::Value` tree first
/// copies every cell (a whole page of text) one extra time before it is encoded.
#[derive(Clone, Debug)]
pub enum WorkerResult {
    Json(serde_json::Value),
    ReadCursorPage(ReadCursorPage),
}

impl WorkerResult {
    /// Whether this answer closes a cursor. Only the write-cursor path consults it,
    /// where a lost reply must be settled by a delivered done page.
    pub fn done(&self) -> bool {
        match self {
            WorkerResult::Json(value) => {
                value.get("done").and_then(|done| done.as_bool()) == Some(true)
            }
            WorkerResult::ReadCursorPage(page) => page.done,
        }
    }
}

impl Serialize for WorkerResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            WorkerResult::Json(value) => value.serialize(serializer),
            WorkerResult::ReadCursorPage(page) => page.serialize(serializer),
        }
    }
}

/// Responses are only decoded by tests and control tooling, which read the page
/// shape as a plain JSON value; the borrowed page exists only on the encode side.
impl<'de> Deserialize<'de> for WorkerResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde_json::Value::deserialize(deserializer).map(WorkerResult::Json)
    }
}

#[derive(Clone, Debug)]
pub struct ReadCursorPage {
    /// Present on the first page only, matching the fused open answer.
    pub cols: Option<Vec<StatementColumn>>,
    pub rows: Vec<Vec<SqlValue>>,
    pub done: bool,
    pub autocommit: Option<bool>,
}

impl Serialize for ReadCursorPage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Same key order as the tree it replaces: `json!` objects are sorted, and
        // keeping the reply byte-compatible makes the two shapes interchangeable
        // for every decoder in tests and tooling.
        let mut map = serializer.serialize_map(None)?;
        if let Some(autocommit) = self.autocommit {
            map.serialize_entry("autocommit", &autocommit)?;
        }
        if let Some(cols) = &self.cols {
            map.serialize_entry("cols", &Columns(cols))?;
        }
        map.serialize_entry("done", &self.done)?;
        map.serialize_entry("rows", &Rows(&self.rows))?;
        map.end()
    }
}

struct Columns<'a>(&'a [StatementColumn]);

impl Serialize for Columns<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for column in self.0 {
            seq.serialize_element(&Column {
                name: column.name.as_str(),
                decltype: column.decltype.as_deref(),
            })?;
        }
        seq.end()
    }
}

struct Column<'a> {
    name: &'a str,
    decltype: Option<&'a str>,
}

impl Serialize for Column<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("decltype", &self.decltype)?;
        map.serialize_entry("name", &self.name)?;
        map.end()
    }
}

struct Rows<'a>(&'a [Vec<SqlValue>]);

impl Serialize for Rows<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for row in self.0 {
            seq.serialize_element(&Row(row))?;
        }
        seq.end()
    }
}

struct Row<'a>(&'a [SqlValue]);

impl Serialize for Row<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for value in self.0 {
            seq.serialize_element(&Cell(value))?;
        }
        seq.end()
    }
}

struct Cell<'a>(&'a SqlValue);

impl Serialize for Cell<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Exactly the mapping of `sessions::value`, but written straight into the
        // encoder instead of materializing one `serde_json::Value` per cell.
        match self.0 {
            SqlValue::Null => serializer.serialize_unit(),
            SqlValue::Numeric(Numeric::Integer(integer)) => serializer.serialize_i64(*integer),
            SqlValue::Numeric(Numeric::Float(float)) => {
                let value = f64::from(*float);
                if value.is_finite() {
                    serializer.serialize_f64(value)
                } else {
                    serializer.serialize_unit()
                }
            }
            SqlValue::Text(text) => serializer.serialize_str(text.as_str()),
            SqlValue::Blob(bytes) => serializer.collect_seq(bytes.iter()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn reference_rows(rows: &[Vec<SqlValue>], done: bool) -> serde_json::Value {
        json!({
            "rows": rows
                .iter()
                .map(|row| row.iter().map(super::super::sessions::value).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            "done": done,
        })
    }

    fn sample_rows() -> Vec<Vec<SqlValue>> {
        vec![
            vec![
                SqlValue::Null,
                SqlValue::from_i64(-7),
                SqlValue::from_f64(1.5),
                SqlValue::from_f64(f64::INFINITY),
                SqlValue::build_text("quote \" backslash \\ newline \n tab \t unicode 中文"),
                SqlValue::build_text(""),
                SqlValue::Blob(vec![0, 1, 127, 255]),
            ],
            vec![SqlValue::from_i64(0), SqlValue::build_text("plain")],
        ]
    }

    #[test]
    fn borrowed_page_encodes_exactly_like_the_value_tree() {
        let rows = sample_rows();
        let page = ReadCursorPage {
            cols: None,
            rows: rows.clone(),
            done: true,
            autocommit: None,
        };
        let borrowed = serde_json::to_vec(&page).expect("borrowed page encodes");
        let tree = serde_json::to_vec(&reference_rows(&rows, true)).expect("tree encodes");
        assert_eq!(borrowed, tree);
    }

    #[test]
    fn borrowed_first_page_encodes_exactly_like_the_value_tree() {
        let rows = sample_rows();
        let columns = vec![
            crate::session::StatementColumn {
                name: "id".to_string(),
                decltype: Some("INTEGER".to_string()),
            },
            crate::session::StatementColumn {
                name: "body".to_string(),
                decltype: None,
            },
        ];
        let page = ReadCursorPage {
            cols: Some(columns.clone()),
            rows: rows.clone(),
            done: false,
            autocommit: Some(true),
        };
        let borrowed = serde_json::to_vec(&page).expect("borrowed page encodes");
        let tree = serde_json::to_vec(&json!({
            "cols": columns
                .iter()
                .map(|column| json!({"name": column.name, "decltype": column.decltype}))
                .collect::<Vec<_>>(),
            "rows": rows
                .iter()
                .map(|row| row.iter().map(super::super::sessions::value).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            "done": false,
            "autocommit": true,
        }))
        .expect("tree encodes");
        assert_eq!(borrowed, tree);
    }

    #[test]
    fn worker_result_decodes_and_reports_done() {
        let decoded: WorkerResult =
            serde_json::from_str(r#"{"rows":[],"done":true}"#).expect("decode");
        assert!(decoded.done());
        assert!(matches!(decoded, WorkerResult::Json(_)));
        let page = WorkerResult::ReadCursorPage(ReadCursorPage {
            cols: None,
            rows: vec![vec![SqlValue::Null]],
            done: false,
            autocommit: None,
        });
        assert!(!page.done());
        assert_eq!(
            serde_json::to_vec(&page).expect("encode"),
            br#"{"done":false,"rows":[[null]]}"#
        );
    }
}
