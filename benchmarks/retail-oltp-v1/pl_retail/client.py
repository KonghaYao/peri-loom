"""Hrana v3 JSON client used by the retail benchmark driver.

Only the shapes the contract needs are implemented: one execute per HTTP
request, one independent baton per worker, connection reuse and TLS
verification.  SQL is never retried here; the reply and its phase timings are
returned as evidence even when the outcome is unknown.
"""
from __future__ import annotations

import http.client
import json
import ssl
import time
import urllib.parse
import uuid
from dataclasses import dataclass
from typing import Any, Callable


class HranaProtocolError(ValueError):
    """The peer's bytes cannot be classified under the Hrana contract."""


class FatalTransportError(RuntimeError):
    """Authentication or connection failure that makes a fixture meaningless."""


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
            raise HranaProtocolError("connection file has no database")
        endpoint = document.get("endpoint")
        if not isinstance(endpoint, str) or not endpoint:
            raise HranaProtocolError("connection file has no endpoint")
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
            raise HranaProtocolError("connection database identity is incomplete")
        return "/db/%s/%s/%s/%s" % (
            urllib.parse.quote(tenant, safe=""),
            urllib.parse.quote(database, safe=""),
            urllib.parse.quote(branch, safe=""),
            suffix.lstrip("/"),
        )

    def identity(self) -> dict:
        namespace = self.database.get("namespace") or {}
        return {
            "tenant": namespace.get("tenant"),
            "database": namespace.get("database"),
            "branch": self.database.get("branch"),
            "tls": self.endpoint.startswith("https://"),
        }


def _required_secret(document: dict, key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str) or not value:
        raise HranaProtocolError("connection file has no %s" % key)
    return value


@dataclass(frozen=True)
class StepResult:
    ok: bool
    result: dict[str, Any] | None = None
    error_code: str = ""
    error_message: str = ""
    rows: list[list[Any]] | None = None
    affected_row_count: int | None = None
    last_insert_rowid: int | None = None


@dataclass(frozen=True)
class Reply:
    status: int
    baton: str | None
    results: tuple[StepResult, ...]
    error: dict[str, Any] | None
    cleanup_error: dict[str, Any] | None
    transport_error: str | None
    started_ns: int
    completed_ns: int
    ttfb_ns: int
    request_id: str
    connection_id: int
    connection_reused: bool
    request_bytes: int
    response_bytes: int

    @property
    def latency_ms(self) -> float:
        return max(0, self.completed_ns - self.started_ns) / 1_000_000

    @property
    def code(self) -> str:
        if self.error:
            return str(self.error.get("code") or "")
        for step in self.results:
            if not step.ok:
                return step.error_code
        return ""

    @property
    def message(self) -> str:
        if self.error:
            return str(self.error.get("message") or self.error.get("code") or "")
        for step in self.results:
            if not step.ok:
                return step.error_message
        return ""

    def first_error_step(self) -> StepResult | None:
        for step in self.results:
            if not step.ok:
                return step
        return None

    def evidence(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "code": self.code,
            "message": self.message,
            "request_id": self.request_id,
            "connection_id": self.connection_id,
            "connection_reused": self.connection_reused,
            "request_bytes": self.request_bytes,
            "response_bytes": self.response_bytes,
            "started_ns": self.started_ns,
            "ttfb_ns": self.ttfb_ns,
            "completed_ns": self.completed_ns,
            "latency_ms": round(self.latency_ms, 6),
            "partial_execution_possible": (
                None if not self.error else self.error.get("partial_execution_possible")
            ),
            "outcome_unknown": None if not self.error else self.error.get("outcome_unknown"),
            "retryable": None if not self.error else self.error.get("retryable"),
            "operation_id": None if not self.error else self.error.get("operation_id"),
            "cleanup_error": self.cleanup_error,
            "transport_error": self.transport_error,
        }


def classify_transport(reply: Reply) -> tuple[str, str]:
    """Return ``(kind, reason)`` where kind is ``ok``/``known_not_executed``/``unknown``.

    ``known_not_executed`` is only returned for a platform reply that proves no
    statement was dispatched.  Everything ambiguous or unproven is ``unknown``.
    """
    if reply.transport_error:
        return "unknown", "transport_failure:%s" % reply.transport_error
    if reply.error:
        code = str(reply.error.get("code") or "")
        if reply.error.get("outcome_unknown") is True:
            return "unknown", "service_unknown:%s" % code
        if code in {"quota_exceeded", "rate_limited"} and reply.error.get(
            "partial_execution_possible"
        ) is False:
            return "known_not_executed", code
        if code in {"invalid_arguments", "invalid_baton", "expired_baton", "baton_busy"}:
            return "known_not_executed", code
        if reply.status >= 500:
            return "unknown", "server_error:%s" % (code or reply.status)
        if reply.error.get("partial_execution_possible") is False:
            return "known_not_executed", code or "refused_before_dispatch"
        return "unknown", "unproven_failure:%s" % (code or reply.status)
    step = reply.first_error_step()
    if step is not None:
        message = step.error_message.lower()
        if "unique constraint failed" in message or "constraint failed" in message:
            return "known_failure", "constraint:%s" % step.error_code
        if "syntax error" in message:
            return "known_failure", "syntax:%s" % step.error_code
        return "unknown", "statement_unproven:%s" % (step.error_code or "unknown")
    if reply.status != 200:
        return "unknown", "http_%d" % reply.status
    return "ok", ""


class HranaClient:
    def __init__(
        self,
        target: Target,
        timeout: float = 120.0,
        recorder: Callable[[dict[str, Any]], None] | None = None,
    ):
        self.target = target
        self.timeout = timeout
        self._connection: http.client.HTTPConnection | None = None
        self._connection_id = 0
        self._next_connection_id = 0
        self._last_used = 0.0
        self._recorder = recorder

    # -- transport ---------------------------------------------------------

    def _connect(self) -> tuple[http.client.HTTPConnection, int]:
        split = urllib.parse.urlsplit(self.target.endpoint)
        if split.scheme == "https":
            if not self.target.ca_file:
                raise FatalTransportError("TLS endpoint has no ca_file; refusing to disable verification")
            context = ssl.create_default_context(cafile=self.target.ca_file)
            connection = http.client.HTTPSConnection(
                split.hostname, split.port or 443, timeout=self.timeout, context=context
            )
        elif split.scheme == "http":
            connection = http.client.HTTPConnection(
                split.hostname, split.port or 80, timeout=self.timeout
            )
        else:
            raise FatalTransportError("unsupported endpoint scheme")
        self._next_connection_id += 1
        return connection, self._next_connection_id

    def _drop_connection(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            finally:
                self._connection = None

    def _ensure_connection(self) -> tuple[http.client.HTTPConnection, int, bool]:
        if self._connection is not None and time.monotonic() - self._last_used > 5.0:
            self._drop_connection()
        reused = self._connection is not None
        if self._connection is None:
            self._connection, self._connection_id = self._connect()
        return self._connection, self._connection_id, reused

    def request(self, path: str, body: bytes, content_type: str = "application/json") -> Reply:
        connection, connection_id, reused = self._ensure_connection()
        request_id = uuid.uuid4().hex
        headers = {
            "Authorization": "Bearer " + self.target.project_api_key,
            "Content-Type": content_type,
            "Accept": content_type,
            "X-Request-ID": request_id,
            "Connection": "keep-alive",
        }
        started = time.monotonic_ns()
        transport_error: str | None = None
        status = 0
        response_headers: dict[str, str] = {}
        chunks: list[bytes] = []
        ttfb = 0
        try:
            connection.request("POST", path, body=body, headers=headers)
            response = connection.getresponse()
            ttfb = time.monotonic_ns()
            while True:
                chunk = response.read(65536)
                if not chunk:
                    break
                chunks.append(chunk)
            completed = time.monotonic_ns()
            status = response.status
            response_headers = dict(response.getheaders())
            will_close = response.will_close
        except (OSError, http.client.HTTPException) as error:
            completed = time.monotonic_ns()
            self._drop_connection()
            transport_error = "%s: %s" % (type(error).__name__, error)
            will_close = True
        if transport_error is None:
            if will_close:
                self._drop_connection()
            else:
                self._last_used = time.monotonic()
        body_bytes = b"".join(chunks)
        if status in (401, 403) and not transport_error:
            raise FatalTransportError("authentication rejected with HTTP %d" % status)
        reply = _parse_reply(
            status=status,
            body=body_bytes,
            baton=None,
            transport_error=transport_error,
            started_ns=started,
            completed_ns=completed,
            ttfb_ns=ttfb,
            request_id=request_id,
            connection_id=connection_id,
            connection_reused=reused,
            request_bytes=len(body),
            response_headers=response_headers,
        )
        if self._recorder is not None:
            entry = reply.evidence()
            entry["path"] = path
            entry["content_type"] = content_type
            self._recorder(entry)
        return reply

    # -- Hrana requests ----------------------------------------------------

    def pipeline(self, requests: list[dict], baton: str | None = None) -> Reply:
        encoded: list[dict[str, Any]] = []
        for request in requests:
            kind = request["type"]
            if kind == "close":
                encoded.append({"type": "close"})
                continue
            if kind != "execute":
                raise ValueError("unsupported pipeline request %r" % kind)
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
        body = json.dumps(document, ensure_ascii=False).encode("utf-8")
        return self.request(self.target.path("v3/pipeline"), body)

    def execute(
        self,
        sql: str,
        args: tuple | list = (),
        want_rows: bool = False,
        baton: str | None = None,
    ) -> Reply:
        return self.pipeline(
            [{"type": "execute", "sql": sql, "args": list(args), "want_rows": want_rows}],
            baton=baton,
        )

    def open(self) -> Reply:
        """Open a stream without dispatching business SQL.

        A ``describe`` is a protocol question about a statement, not an
        execution, so it allocates a baton without counting as business work.
        """
        document = {"requests": [{"type": "describe", "sql": "SELECT 1"}]}
        return self.request(
            self.target.path("v3/pipeline"),
            json.dumps(document, ensure_ascii=False).encode("utf-8"),
        )

    def close(self, baton: str) -> Reply:
        return self.pipeline([{"type": "close"}], baton=baton)

    def close_quietly(self) -> None:
        self._drop_connection()



def _parse_reply(
    *,
    status: int,
    body: bytes,
    baton: str | None,
    transport_error: str | None,
    started_ns: int,
    completed_ns: int,
    ttfb_ns: int,
    request_id: str,
    connection_id: int,
    connection_reused: bool,
    request_bytes: int,
    response_headers: dict[str, str],
) -> Reply:
    results: list[StepResult] = []
    error: dict[str, Any] | None = None
    cleanup_error: dict[str, Any] | None = None
    if transport_error is None and body:
        try:
            document = json.loads(body)
        except json.JSONDecodeError as exc:
            raise HranaProtocolError("invalid JSON reply: %s" % exc) from None
        if isinstance(document, dict):
            baton = document.get("baton") if isinstance(document.get("baton"), str) else baton
            failure = document.get("error")
            if isinstance(failure, dict):
                error = failure
            cleanup = document.get("cleanup_error")
            if isinstance(cleanup, dict):
                cleanup_error = cleanup
            for entry in document.get("results") or []:
                results.append(_parse_step(entry))
    return Reply(
        status=status,
        baton=baton,
        results=tuple(results),
        error=error,
        cleanup_error=cleanup_error,
        transport_error=transport_error,
        started_ns=started_ns,
        completed_ns=completed_ns,
        ttfb_ns=ttfb_ns,
        request_id=request_id,
        connection_id=connection_id,
        connection_reused=connection_reused,
        request_bytes=request_bytes,
        response_bytes=len(body),
    )


def _parse_step(entry: Any) -> StepResult:
    if not isinstance(entry, dict):
        return StepResult(ok=False, error_code="invalid_step", error_message="step is not an object")
    if entry.get("type") == "error":
        failure = entry.get("error") or {}
        return StepResult(
            ok=False,
            error_code=str(failure.get("code") or ""),
            error_message=str(failure.get("message") or ""),
        )
    if entry.get("type") != "ok":
        return StepResult(ok=False, error_code="invalid_step", error_message="unknown step type")
    response = entry.get("response") or {}
    if response.get("type") != "execute":
        return StepResult(ok=True, result={"response_type": response.get("type")})
    result = response.get("result") or {}
    rows = result.get("rows")
    if not isinstance(rows, list):
        rows = None
    rowid = result.get("last_insert_rowid")
    return StepResult(
        ok=True,
        result=result,
        rows=rows,
        affected_row_count=_int_or_none(result.get("affected_row_count")),
        last_insert_rowid=_int_or_none(rowid),
    )


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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
