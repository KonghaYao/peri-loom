#!/usr/bin/env python3
"""Strict Hrana v2 pipeline encoding, decoding, and outcome classification."""
import base64
import json
import re
from dataclasses import dataclass, field

MAX_RESPONSE_BYTES = 32 * 1024 * 1024
DATABASE_PART = re.compile(r"^[A-Za-z0-9._-]+$")
STEP_LIMIT = 64


class ProtocolError(ValueError):
    pass


class Rejected(ProtocolError):
    """The server rejected the pipeline before any statement was dispatched."""

    def __init__(self, code, status):
        super().__init__(code)
        self.code = code
        self.status = status
        self.may_have_committed = False
        self.outcome_unknown = False


class OutcomeUnknown(ProtocolError):
    """The request may have been executed; SQL must never be replayed."""

    def __init__(self, code, status=None, detail=""):
        super().__init__(code)
        self.code = code
        self.status = status
        self.detail = detail
        self.may_have_committed = True
        self.outcome_unknown = True


@dataclass
class Step:
    step_id: str
    sql: str = ""
    args: list = field(default_factory=list)
    want_rows: bool = True
    kind: str = "execute"


@dataclass
class StepResult:
    step_id: str
    kind: str
    ok: bool
    response_type: str = ""
    result: dict = field(default_factory=dict)
    error_code: str = ""
    error_message: str = ""
    may_have_committed: bool = False

    def rows(self):
        return decode_rows(self.result)

    def affected_row_count(self):
        value = self.result.get("affected_row_count")
        return value if isinstance(value, int) and not isinstance(value, bool) else None

    def last_insert_rowid(self):
        value = self.result.get("last_insert_rowid")
        if value is None:
            return None
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                raise ProtocolError("last_insert_rowid_not_integer") from None
        raise ProtocolError("last_insert_rowid_type")


@dataclass
class PipelineReply:
    status: int
    baton: str | None
    results: list
    close_acknowledged: bool
    response_complete: bool
    body_bytes: int
    cleanup_error: dict | None = None
    rejected: Rejected | None = None

    @property
    def ok(self):
        return self.status == 200 and all(result.ok for result in self.results)


def pipeline_path(database):
    parts = database.split("/") if isinstance(database, str) else []
    if len(parts) != 3 or any(not part or not DATABASE_PART.match(part)
                              for part in parts):
        raise ProtocolError("database_must_be_tenant/database/branch")
    return "/db/%s/%s/%s/v2/pipeline" % tuple(parts)


def integer_arg(value):
    if not isinstance(value, int) or isinstance(value, bool):
        raise ProtocolError("integer_arg_type")
    return {"type": "integer", "value": str(value)}


def text_arg(value):
    if not isinstance(value, str):
        raise ProtocolError("text_arg_type")
    return {"type": "text", "value": value}


def blob_arg(value):
    if not isinstance(value, (bytes, bytearray)):
        raise ProtocolError("blob_arg_type")
    return {"type": "blob", "base64": base64.b64encode(bytes(value)).decode("ascii")}


def null_arg():
    return {"type": "null"}


def encode_pipeline(baton, steps):
    if not isinstance(steps, list) or not steps or len(steps) > STEP_LIMIT:
        raise ProtocolError("pipeline_step_count")
    requests = []
    for step in steps:
        if not isinstance(step, Step):
            raise ProtocolError("pipeline_step_type")
        if step.kind == "close":
            requests.append({"type": "close"})
        elif step.kind == "execute":
            if not step.sql:
                raise ProtocolError("pipeline_sql_missing")
            requests.append({
                "type": "execute",
                "stmt": {
                    "sql": step.sql,
                    "args": step.args,
                    "want_rows": bool(step.want_rows),
                },
            })
        else:
            raise ProtocolError("pipeline_step_kind: " + str(step.kind))
    document = {"baton": baton or "", "requests": requests}
    return json.dumps(document, separators=(",", ":")).encode()


def decode_cell(cell):
    if not isinstance(cell, dict) or "type" not in cell:
        raise ProtocolError("cell_not_object")
    kind = cell.get("type")
    if kind == "null":
        return None
    if kind in {"integer", "float", "text"}:
        return cell.get("value")
    if kind == "blob":
        encoded = cell.get("base64")
        if not isinstance(encoded, str):
            raise ProtocolError("blob_base64")
        try:
            return base64.b64decode(encoded, validate=True)
        except ValueError:
            raise ProtocolError("blob_base64_invalid") from None
    raise ProtocolError("cell_type: " + str(kind))


def decode_rows(result):
    rows = result.get("rows")
    if not isinstance(rows, list):
        raise ProtocolError("rows_not_list")
    return [[decode_cell(cell) for cell in row] for row in rows]


def _error_fields(payload):
    error = payload.get("error")
    if not isinstance(error, dict):
        raise ProtocolError("error_not_object")
    code = error.get("code")
    message = error.get("message")
    if not isinstance(code, str) or not code:
        raise ProtocolError("error_code")
    if message is not None and not isinstance(message, str):
        raise ProtocolError("error_message")
    return error, code


def _parse_result(step, payload):
    if not isinstance(payload, dict):
        raise ProtocolError("step_result_not_object")
    kind = payload.get("type")
    if kind == "ok":
        response = payload.get("response")
        if not isinstance(response, dict):
            raise ProtocolError("step_response_not_object")
        response_type = response.get("type")
        if response_type not in {"execute", "close"}:
            raise ProtocolError("step_response_type: " + str(response_type))
        if response_type == "close":
            return StepResult(step.step_id, step.kind, True, "close")
        result = response.get("result")
        if not isinstance(result, dict):
            raise ProtocolError("execute_result_not_object")
        for key in ("cols", "rows", "affected_row_count"):
            if key not in result:
                raise ProtocolError("execute_result_missing: " + key)
        if not isinstance(result["cols"], list) or not isinstance(result["rows"], list):
            raise ProtocolError("execute_result_shape")
        affected = result["affected_row_count"]
        if not isinstance(affected, int) or isinstance(affected, bool) or affected < 0:
            raise ProtocolError("affected_row_count")
        if "last_insert_rowid" not in result:
            raise ProtocolError("last_insert_rowid_missing")
        return StepResult(step.step_id, step.kind, True, "execute", result)
    if kind == "error":
        error, code = _error_fields(payload)
        message = error.get("message") or code
        may_have_committed = error.get("may_have_committed")
        if may_have_committed is None:
            may_have_committed = step.kind != "close" and step.want_rows is False
        if not isinstance(may_have_committed, bool):
            raise ProtocolError("error_may_have_committed_type")
        return StepResult(step.step_id, step.kind, False, "error",
                          error_code=code, error_message=message,
                          may_have_committed=may_have_committed)
    raise ProtocolError("step_result_type: " + str(kind))


def parse_pipeline_response(status, body, steps):
    """Decode a fully-read HTTP response.

    Non-200 responses either become Rejected (proved pre-dispatch) or
    OutcomeUnknown. Transport failures are classified by the client, never here.
    """
    if not isinstance(status, int) or status < 100 or status > 599:
        raise OutcomeUnknown("invalid_http_status")
    if len(body) > MAX_RESPONSE_BYTES:
        raise OutcomeUnknown("response_too_large", status)
    text = body.decode("utf-8", errors="replace")
    try:
        document = json.loads(text)
    except json.JSONDecodeError:
        if 400 <= status < 500:
            code = "http_%d" % status
            raise Rejected(code, status) from None
        raise OutcomeUnknown("response_not_json", status) from None
    if not isinstance(document, dict):
        raise OutcomeUnknown("response_not_object", status)
    allowed = {"baton", "results", "error", "cleanup_error"}
    if set(document) - allowed:
        raise OutcomeUnknown("response_unknown_field", status)
    if status != 200:
        if "error" not in document:
            raise OutcomeUnknown("http_%d" % status, status)
        error, code = _error_fields(document)
        unknown = bool(error.get("outcome_unknown")) \
            or bool(error.get("partial_execution_possible"))
        if 400 <= status < 500 and not unknown:
            raise Rejected(code, status)
        raise OutcomeUnknown(code, status)
    results = document.get("results")
    if not isinstance(results, list):
        raise ProtocolError("pipeline_results_missing")
    if len(results) != len(steps):
        raise ProtocolError("pipeline_result_count")
    parsed = [_parse_result(step, payload)
              for step, payload in zip(steps, results)]
    baton = document.get("baton")
    if baton is not None and (not isinstance(baton, str) or not baton):
        raise ProtocolError("pipeline_baton")
    cleanup = document.get("cleanup_error")
    if cleanup is not None and not isinstance(cleanup, dict):
        raise ProtocolError("pipeline_cleanup_error")
    close_acknowledged = any(step.kind == "close" and result.ok
                             and result.response_type == "close"
                             for step, result in zip(steps, parsed))
    return PipelineReply(
        status=status,
        baton=baton,
        results=parsed,
        close_acknowledged=close_acknowledged,
        response_complete=True,
        body_bytes=len(body),
        cleanup_error=cleanup,
    )
