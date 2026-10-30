"""Hrana v3 wire client for the Issue 28 performance plan.

The module deliberately implements only the shapes the plan needs: JSON and
protobuf pipelines, plus JSON and protobuf cursors. SQL requests are never
retried here; transport failures are returned as evidence.
"""
from __future__ import annotations

import http.client
import json
import ssl
import struct
import time
import urllib.parse
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


class HranaError(ValueError):
    """The peer replied with bytes this client cannot classify."""


class FatalTransportError(RuntimeError):
    """Authentication or connection failure that makes the run meaningless."""


@dataclass(frozen=True)
class Target:
    endpoint: str
    project_api_key: str
    global_api_key: str
    database: dict
    ca_file: str | None = None

    @classmethod
    def from_file(cls, path: str) -> "Target":
        with open(path, encoding="utf-8") as handle:
            document = json.load(handle)
        database = document.get("database")
        if not isinstance(database, dict):
            raise HranaError("connection file has no database")
        endpoint = document.get("endpoint")
        if not isinstance(endpoint, str):
            raise HranaError("connection file has no endpoint")
        return cls(
            endpoint=endpoint.rstrip("/"),
            project_api_key=_required_secret(document, "project_api_key"),
            global_api_key=_required_secret(document, "global_api_key"),
            database=database,
            ca_file=document.get("ca_file"),
        )

    def path(self, suffix: str) -> str:
        namespace = self.database.get("namespace") or {}
        tenant = namespace.get("tenant")
        database = namespace.get("database")
        branch = self.database.get("branch")
        if not all(isinstance(value, str) and value for value in (tenant, database, branch)):
            raise HranaError("connection database identity is incomplete")
        return "/db/%s/%s/%s/%s" % (
            urllib.parse.quote(tenant, safe=""),
            urllib.parse.quote(database, safe=""),
            urllib.parse.quote(branch, safe=""),
            suffix.lstrip("/"),
        )

    def public_identity(self) -> dict:
        return {
            "endpoint": self.endpoint,
            "database": self.database,
            "tls": self.endpoint.startswith("https://"),
        }


def _required_secret(document: dict, key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str) or not value:
        raise HranaError("connection file has no %s" % key)
    return value


# -- protobuf wire helpers -------------------------------------------------


def varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("varint cannot be negative")
    output = bytearray()
    while True:
        chunk = value & 0x7F
        value >>= 7
        if value:
            output.append(chunk | 0x80)
        else:
            output.append(chunk)
            return bytes(output)


def field_bytes(number: int, value: bytes) -> bytes:
    return varint(number << 3 | 2) + varint(len(value)) + value


def field_varint(number: int, value: int) -> bytes:
    return varint(number << 3) + varint(value)


def field_fixed64(number: int, value: float) -> bytes:
    return varint(number << 3 | 1) + struct.pack("<d", value)


def encode_value(value: Any) -> bytes:
    if value is None:
        return field_bytes(1, b"")
    if isinstance(value, bool):
        return field_varint(2, 1 if value else 0)
    if isinstance(value, int):
        # The v3 schema carries integers as zigzag-encoded int64.
        return field_varint(2, (value << 1) ^ (value >> 63))
    if isinstance(value, float):
        return field_fixed64(3, value)
    if isinstance(value, str):
        return field_bytes(4, value.encode("utf-8"))
    if isinstance(value, (bytes, bytearray)):
        return field_bytes(5, bytes(value))
    raise TypeError("unsupported Hrana value %s" % type(value).__name__)


def encode_statement(sql: str, want_rows: bool, args: Iterable[Any]) -> bytes:
    statement = field_bytes(1, sql.encode("utf-8"))
    for value in args:
        statement += field_bytes(3, encode_value(value))
    if want_rows:
        statement += field_varint(5, 1)
    return statement


def encode_pipeline(requests: list[dict], baton: str | None = None) -> bytes:
    output = bytearray()
    if baton:
        output += field_bytes(1, baton.encode("utf-8"))
    for request in requests:
        if request["type"] == "close":
            output += field_bytes(2, field_bytes(1, b""))
            continue
        if request["type"] != "execute":
            raise ValueError("unsupported pipeline request %r" % request["type"])
        statement = encode_statement(
            request["sql"], bool(request.get("want_rows")), request.get("args") or ())
        output += field_bytes(2, field_bytes(2, field_bytes(1, statement)))
    return bytes(output)


def encode_cursor(
        sql: str, want_rows: bool, args: Iterable[Any], baton: str | None = None) -> bytes:
    statement = encode_statement(sql, want_rows, args)
    output = bytearray()
    if baton:
        output += field_bytes(1, baton.encode("utf-8"))
    batch_step = field_bytes(2, statement)
    output += field_bytes(2, field_bytes(1, batch_step))
    return bytes(output)


@dataclass(frozen=True)
class ProtoField:
    number: int
    wire: int
    value: Any


def read_fields(data: bytes) -> list[ProtoField]:
    fields: list[ProtoField] = []
    index = 0
    length = len(data)
    while index < length:
        header = 0
        shift = 0
        while True:
            if index >= length:
                raise HranaError("truncated protobuf header")
            byte = data[index]
            index += 1
            header |= (byte & 0x7F) << shift
            shift += 7
            if not byte & 0x80:
                break
        number = header >> 3
        wire = header & 7
        if number == 0:
            raise HranaError("invalid protobuf field number")
        if wire == 0:
            value = 0
            shift = 0
            while True:
                if index >= length:
                    raise HranaError("truncated protobuf varint")
                byte = data[index]
                index += 1
                value |= (byte & 0x7F) << shift
                shift += 7
                if not byte & 0x80:
                    break
            fields.append(ProtoField(number, wire, value))
        elif wire == 1:
            if index + 8 > length:
                raise HranaError("truncated protobuf fixed64")
            fields.append(ProtoField(number, wire, struct.unpack("<d", data[index:index + 8])[0]))
            index += 8
        elif wire == 2:
            size = 0
            shift = 0
            while True:
                if index >= length:
                    raise HranaError("truncated protobuf length")
                byte = data[index]
                index += 1
                size |= (byte & 0x7F) << shift
                shift += 7
                if not byte & 0x80:
                    break
            if index + size > length:
                raise HranaError("truncated protobuf payload")
            fields.append(ProtoField(number, wire, data[index:index + size]))
            index += size
        elif wire == 5:
            if index + 4 > length:
                raise HranaError("truncated protobuf fixed32")
            fields.append(ProtoField(number, wire, data[index:index + 4]))
            index += 4
        else:
            raise HranaError("unsupported protobuf wire type %d" % wire)
    return fields


def _one(fields: list[ProtoField], number: int) -> ProtoField | None:
    for field in fields:
        if field.number == number:
            return field
    return None


def _text(field: ProtoField | None) -> str | None:
    if field is None or field.wire != 2:
        return None
    return field.value.decode("utf-8", "replace")


@dataclass
class PipelineResult:
    ok: bool
    response_type: str = ""
    rows: int = 0
    rows_read: int = 0
    rows_written: int = 0
    affected_row_count: int = 0
    query_ms: float | None = None
    error_code: str = ""
    error_message: str = ""


@dataclass
class PipelineReply:
    baton: str | None
    results: list[PipelineResult] = field(default_factory=list)


def parse_pipeline_json(body: bytes) -> PipelineReply:
    try:
        document = json.loads(body)
    except json.JSONDecodeError as error:
        raise HranaError("invalid JSON pipeline reply: %s" % error) from None
    results: list[PipelineResult] = []
    for entry in document.get("results") or []:
        if entry.get("type") == "ok":
            response = entry.get("response") or {}
            if response.get("type") == "execute":
                result = response.get("result") or {}
                rows = result.get("rows")
                results.append(PipelineResult(
                    ok=True,
                    response_type="execute",
                    rows=len(rows) if isinstance(rows, list) else 0,
                    rows_read=int(result.get("rows_read") or 0),
                    rows_written=int(result.get("rows_written") or 0),
                    affected_row_count=int(result.get("affected_row_count") or 0),
                    query_ms=_float_or_none(result.get("query_duration_ms")),
                ))
            else:
                results.append(PipelineResult(ok=True, response_type=str(response.get("type") or "")))
        elif entry.get("type") == "error":
            error = entry.get("error") or {}
            results.append(PipelineResult(
                ok=False,
                response_type="error",
                error_code=str(error.get("code") or ""),
                error_message=str(error.get("message") or ""),
            ))
        else:
            results.append(PipelineResult(ok=False, response_type="unknown"))
    return PipelineReply(baton=document.get("baton"), results=results)


def parse_pipeline_protobuf(body: bytes) -> PipelineReply:
    results: list[PipelineResult] = []
    baton: str | None = None
    for field in read_fields(body):
        if field.number == 1:
            baton = _text(field)
        elif field.number == 3:
            results.append(_parse_pipeline_result(field.value))
    return PipelineReply(baton=baton, results=results)


def _parse_pipeline_result(data: bytes) -> PipelineResult:
    fields = read_fields(data)
    ok_field = _one(fields, 1)
    error_field = _one(fields, 2)
    if error_field is not None:
        error_fields = read_fields(error_field.value)
        return PipelineResult(
            ok=False,
            response_type="error",
            error_code=_text(_one(error_fields, 1)) or "",
            error_message=_text(_one(error_fields, 2)) or "",
        )
    if ok_field is None:
        return PipelineResult(ok=False, response_type="unknown")
    response_fields = read_fields(ok_field.value)
    execute = _one(response_fields, 2)
    if execute is None:
        return PipelineResult(ok=True, response_type="close")
    execute_fields = read_fields(execute.value)
    statement = _one(execute_fields, 1)
    if statement is None or statement.wire != 2:
        return PipelineResult(ok=False, response_type="unknown")
    statement_fields = read_fields(statement.value)
    rows = [item for item in statement_fields if item.number == 2 and item.wire == 2]
    return PipelineResult(
        ok=True,
        response_type="execute",
        rows=len(rows),
        rows_read=int(_one(statement_fields, 6).value) if _one(statement_fields, 6) else 0,
        rows_written=int(_one(statement_fields, 7).value) if _one(statement_fields, 7) else 0,
        affected_row_count=int(_one(statement_fields, 3).value) if _one(statement_fields, 3) else 0,
        query_ms=float(_one(statement_fields, 8).value) if _one(statement_fields, 8) else None,
    )


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@dataclass
class CursorReply:
    baton: str | None
    rows: int
    frames: int
    errors: list[str] = field(default_factory=list)


def parse_cursor_json(body: bytes) -> CursorReply:
    rows = 0
    frames = 0
    errors: list[str] = []
    baton: str | None = None
    for line in body.splitlines():
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError as error:
            raise HranaError("invalid JSON cursor entry: %s" % error) from None
        frames += 1
        if baton is None and "baton" in entry:
            baton = entry.get("baton")
        kind = entry.get("type")
        if kind == "row":
            rows += 1
        elif kind == "step_error":
            error = entry.get("error") or {}
            errors.append(str(error.get("code") or "step_error"))
    return CursorReply(baton=baton, rows=rows, frames=frames, errors=errors)


def parse_cursor_protobuf(body: bytes) -> CursorReply:
    rows = 0
    frames = 0
    errors: list[str] = []
    baton: str | None = None
    index = 0
    length = len(body)
    while index < length:
        size = 0
        shift = 0
        while True:
            if index >= length:
                raise HranaError("truncated cursor frame length")
            byte = body[index]
            index += 1
            size |= (byte & 0x7F) << shift
            shift += 7
            if not byte & 0x80:
                break
        if index + size > length:
            raise HranaError("truncated cursor frame")
        frame = body[index:index + size]
        index += size
        frames += 1
        if frames == 1:
            baton = _text(_one(read_fields(frame), 1))
            continue
        entry_fields = read_fields(frame)
        if _one(entry_fields, 4) is not None:
            rows += 1
        elif _one(entry_fields, 3) is not None:
            item = read_fields(_one(entry_fields, 3).value)
            failure = read_fields(item[0].value) if item else []
            if len(failure) >= 2:
                error_fields = read_fields(failure[1].value)
                errors.append(_text(_one(error_fields, 1)) or "step_error")
            else:
                errors.append("step_error")
    return CursorReply(baton=baton, rows=rows, frames=frames, errors=errors)


# -- transport -------------------------------------------------------------


@dataclass
class RawResponse:
    status: int
    headers: dict
    body: bytes
    started_ns: int
    headers_ns: int
    completed_ns: int
    request_bytes: int
    connection_id: int
    connection_reused: bool
    request_id: str = ""

    @property
    def duration_ms(self) -> float:
        return max(0, self.completed_ns - self.started_ns) / 1_000_000

    @property
    def ttfb_ms(self) -> float:
        return max(0, self.headers_ns - self.started_ns) / 1_000_000


class HranaClient:
    def __init__(self, target: Target, timeout: float = 120.0):
        self.target = target
        self.timeout = timeout
        self._connection: http.client.HTTPConnection | None = None
        self._connection_id = 0
        self._next_connection_id = 0
        self._last_used = 0.0

    def _connect(self) -> tuple[http.client.HTTPConnection, int]:
        split = urllib.parse.urlsplit(self.target.endpoint)
        if split.scheme == "https":
            context = ssl.create_default_context(cafile=self.target.ca_file)
            connection = http.client.HTTPSConnection(split.hostname, split.port or 443,
                                                     timeout=self.timeout, context=context)
        elif split.scheme == "http":
            connection = http.client.HTTPConnection(split.hostname, split.port or 80,
                                                    timeout=self.timeout)
        else:
            raise FatalTransportError("unsupported endpoint scheme")
        self._next_connection_id += 1
        return connection, self._next_connection_id

    def _ensure_connection(self) -> tuple[http.client.HTTPConnection, int, bool]:
        # Keep-alive sockets can be closed by the server while a long scenario
        # is measuring another protocol. Drop an idle socket before use so a
        # stale connection is never mistaken for a SQL outcome.
        if self._connection is not None and time.monotonic() - self._last_used > 5.0:
            self._drop_connection()
        reused = self._connection is not None
        if self._connection is None:
            self._connection, self._connection_id = self._connect()
        return self._connection, self._connection_id, reused

    def _drop_connection(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            finally:
                self._connection = None

    def request(self, path: str, body: bytes, content_type: str) -> RawResponse:
        connection, connection_id, reused = self._ensure_connection()
        request_id = uuid.uuid4().hex
        headers = {
            "Authorization": "Bearer " + self.target.project_api_key,
            "Content-Type": content_type,
            "Accept": content_type,
            "X-Request-ID": request_id,
            "Connection": "keep-alive",
        }
        started = _monotonic_ns()
        try:
            connection.request("POST", path, body=body, headers=headers)
            response = connection.getresponse()
            headers_at = _monotonic_ns()
            chunks = []
            while True:
                chunk = response.read(65536)
                if not chunk:
                    break
                chunks.append(chunk)
            completed = _monotonic_ns()
            status = response.status
            response_headers = dict(response.getheaders())
            will_close = response.will_close
        except (OSError, http.client.HTTPException) as error:
            self._drop_connection()
            raise FatalTransportError("%s: %s" % (type(error).__name__, error)) from None
        if will_close:
            self._drop_connection()
        else:
            self._last_used = time.monotonic()
        if status in (401, 403):
            raise FatalTransportError("authentication rejected with HTTP %d" % status)
        return RawResponse(
            status=status,
            headers=response_headers,
            body=b"".join(chunks),
            started_ns=started,
            headers_ns=headers_at,
            completed_ns=completed,
            request_bytes=len(body),
            connection_id=connection_id,
            connection_reused=reused,
            request_id=request_id,
        )

    def pipeline_json(self, requests: list[dict], baton: str | None = None) -> RawResponse:
        encoded = []
        for request in requests:
            if request["type"] == "close":
                encoded.append({"type": "close"})
                continue
            if request["type"] != "execute":
                raise ValueError("unsupported pipeline request %r" % request["type"])
            statement: dict[str, Any] = {
                "sql": request["sql"],
                "want_rows": bool(request.get("want_rows")),
            }
            values = list(request.get("args") or ())
            if values:
                statement["args"] = [_json_argument(value) for value in values]
            encoded.append({"type": "execute", "stmt": statement})
        document: dict[str, Any] = {"requests": encoded}
        if baton:
            document["baton"] = baton
        return self.request(self.target.path("v3/pipeline"),
                            json.dumps(document, ensure_ascii=False).encode("utf-8"),
                            "application/json")

    def pipeline_protobuf(self, requests: list[dict], baton: str | None = None) -> RawResponse:
        return self.request(self.target.path("v3-protobuf/pipeline"),
                            encode_pipeline(requests, baton),
                            "application/x-protobuf")

    def cursor_json(self, sql: str, args: Iterable[Any] = (),
                    want_rows: bool = True, baton: str | None = None) -> RawResponse:
        statement: dict[str, Any] = {"sql": sql, "want_rows": want_rows}
        values = list(args)
        if values:
            statement["args"] = [_json_argument(value) for value in values]
        document: dict[str, Any] = {"batch": {"steps": [{"stmt": statement}]}}
        if baton:
            document["baton"] = baton
        return self.request(self.target.path("v3/cursor"),
                            json.dumps(document, ensure_ascii=False).encode("utf-8"),
                            "application/json")

    def cursor_protobuf(self, sql: str, args: Iterable[Any] = (),
                        want_rows: bool = True, baton: str | None = None) -> RawResponse:
        return self.request(self.target.path("v3-protobuf/cursor"),
                            encode_cursor(sql, want_rows, args, baton),
                            "application/x-protobuf")

    def close_cursor(self, baton: str, codec: str) -> RawResponse:
        """Close the session left behind by a cursor stream."""
        if not baton:
            raise HranaError("cursor reply has no baton to close")
        if codec == "json":
            return self.pipeline_json([{"type": "close"}], baton=baton)
        return self.pipeline_protobuf([{"type": "close"}], baton=baton)

    def close(self) -> None:
        self._drop_connection()


def _json_argument(value: Any) -> dict:
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "integer", "value": "1" if value else "0"}
    if isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    if isinstance(value, float):
        return {"type": "real", "value": value}
    if isinstance(value, str):
        return {"type": "text", "value": value}
    if isinstance(value, (bytes, bytearray)):
        import base64
        return {"type": "blob", "value": base64.b64encode(bytes(value)).decode("ascii")}
    raise TypeError("unsupported Hrana argument %s" % type(value).__name__)


def _monotonic_ns() -> int:
    import time
    return time.monotonic_ns()


def redact_error(message: str, target: Target) -> str:
    redacted = message
    for secret in (target.project_api_key, target.global_api_key):
        if secret:
            redacted = redacted.replace(secret, "<REDACTED>")
    return redacted
