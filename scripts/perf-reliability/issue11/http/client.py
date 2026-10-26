#!/usr/bin/env python3
"""Real HTTP/TLS transport for the Issue 11 Hrana runner.

The transport, not the SDK, owns request and connection counters. SQL is never
retried here: any timeout, truncated body, connection error or unknown reply is
returned as an unknown outcome with the connection closed.
"""
import http.client
import ipaddress
from dataclasses import dataclass, field
from pathlib import Path
import socket
import ssl
import threading
import time
import urllib.parse

from issue11.http import protocol

MAX_RESPONSE_BYTES = 32 * 1024 * 1024
REDACTED_AUTHORIZATION = "Bearer <REDACTED>"


class ClientError(ValueError):
    pass


@dataclass
class Exchange:
    request_id: str
    pipeline_correlation_id: str
    step_ids: list
    connection_id: int
    connections_created: int
    reused_connection: bool
    request_bytes: int
    status: int | None
    response_bytes: int
    response_complete: bool
    response_connection_close: bool
    connection_reusable_after: bool
    baton_in: str
    baton_out: str | None
    close_acknowledged: bool
    outcome: str
    error_code: str = ""
    error_type: str = ""
    started_ns: int = 0
    connected_ns: int = 0
    sent_ns: int = 0
    headers_ns: int = 0
    completed_ns: int = 0
    spans: list = field(default_factory=list)
    redacted_headers: dict = field(default_factory=dict)

    def duration_ns(self):
        return max(0, self.completed_ns - self.started_ns)

    def as_record(self, scenario, sample_index, phase, tls):
        return {
            "schema_version": 1,
            "record_type": "http_request",
            "request_id": self.request_id,
            "pipeline_correlation_id": self.pipeline_correlation_id,
            "step_ids": self.step_ids,
            "scenario": scenario,
            "sample_index": sample_index,
            "phase": phase,
            "connection_id": self.connection_id,
            "connections_created": self.connections_created,
            "reused_connection": self.reused_connection,
            "tls": tls,
            "request_bytes": self.request_bytes,
            "status": self.status,
            "response_bytes": self.response_bytes,
            "response_complete": self.response_complete,
            "response_connection_close": self.response_connection_close,
            "connection_reusable_after": self.connection_reusable_after,
            "baton_in": self.baton_in,
            "baton_out": self.baton_out,
            "close_acknowledged": self.close_acknowledged,
            "outcome": self.outcome,
            "error_code": self.error_code,
            "error_type": self.error_type,
            "started_ns": self.started_ns,
            "connected_ns": self.connected_ns,
            "sent_ns": self.sent_ns,
            "headers_ns": self.headers_ns,
            "completed_ns": self.completed_ns,
            "duration_ns": self.duration_ns(),
            "spans": self.spans,
            "request_headers_redacted": self.redacted_headers,
        }


def _is_loopback(host):
    if host in {"localhost", "::1"}:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class HranaClient:
    def __init__(self, endpoint, credential, ca_file=None, timeout_seconds=30.0,
                 allow_plaintext_loopback=False):
        if not isinstance(credential, str) or not credential:
            raise ClientError("credential_missing")
        if "\n" in credential or "\r" in credential:
            raise ClientError("credential_invalid")
        parsed = urllib.parse.urlsplit(endpoint)
        if parsed.scheme not in {"https", "http"} or not parsed.hostname:
            raise ClientError("endpoint_invalid")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ClientError("endpoint_must_not_contain_userinfo_query_or_fragment")
        if parsed.scheme == "http":
            if not allow_plaintext_loopback or not _is_loopback(parsed.hostname):
                raise ClientError("https_required_outside_loopback_diagnostic")
        if timeout_seconds <= 0:
            raise ClientError("timeout_invalid")
        self.endpoint = endpoint.rstrip("/")
        self._parsed = parsed
        self._credential = credential
        self._timeout = timeout_seconds
        self._allow_plaintext = allow_plaintext_loopback
        self._tls = parsed.scheme == "https"
        self._lock = threading.Lock()
        self._connection = None
        self._connection_id = 0
        self._requests_on_connection = 0
        self._connections_created = 0
        self._requests_sent = 0
        self._context = None
        if self._tls:
            self._context = ssl.create_default_context()
            if ca_file:
                cafile = Path(ca_file)
                if not cafile.is_file():
                    raise ClientError("ca_file_missing")
                self._context.load_verify_locations(cafile=str(cafile))
        elif ca_file:
            raise ClientError("ca_file_without_tls")

    def __repr__(self):
        return "<HranaClient %s>" % self._parsed.netloc

    @property
    def tls(self):
        return self._tls

    def counters(self):
        with self._lock:
            return {
                "http_requests": self._requests_sent,
                "connections_created": self._connections_created,
                "connection_reuse_ratio": (
                    (self._requests_sent - self._connections_created)
                    / self._requests_sent if self._requests_sent else 0.0
                ),
            }

    def _connection_object(self):
        host = self._parsed.hostname
        port = self._parsed.port or (443 if self._tls else 80)
        if self._tls:
            return http.client.HTTPSConnection(host, port, timeout=self._timeout,
                                               context=self._context)
        return http.client.HTTPConnection(host, port, timeout=self._timeout)

    def _connect(self):
        """Return (connection, reused, connection_id, connections_created)."""
        with self._lock:
            if self._connection is not None:
                return (self._connection, self._requests_on_connection > 0,
                        self._connection_id, self._connections_created)
            self._connection = self._connection_object()
            self._connection_id += 1
            self._requests_on_connection = 0
            self._connections_created += 1
            return (self._connection, False, self._connection_id,
                    self._connections_created)

    def _drop_connection(self):
        connection = self._connection
        self._connection = None
        self._requests_on_connection = 0
        if connection is not None:
            try:
                connection.close()
            except OSError:
                pass

    def close(self):
        self._drop_connection()

    def post(self, payload, path, request_id, correlation_id, steps, baton_in=""):
        if not isinstance(payload, (bytes, bytearray)) or not payload:
            raise ClientError("payload_missing")
        if not steps or not all(isinstance(step, protocol.Step) for step in steps):
            raise ClientError("steps_missing")
        step_ids = [step.step_id for step in steps]
        headers = {
            "Authorization": "Bearer " + self._credential,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Request-ID": request_id,
            "X-Pipeline-Correlation-ID": correlation_id,
            "X-Hrana-Step-IDs": ",".join(step_ids),
        }
        redacted = dict(headers)
        redacted["Authorization"] = REDACTED_AUTHORIZATION
        started = time.monotonic_ns()
        try:
            connection, reused, connection_id, created = self._connect()
        except OSError as failure:
            completed = time.monotonic_ns()
            exchange = Exchange(
                request_id=request_id,
                pipeline_correlation_id=correlation_id,
                step_ids=list(step_ids),
                connection_id=0,
                connections_created=self._connections_created,
                reused_connection=False,
                request_bytes=len(payload),
                status=None,
                response_bytes=0,
                response_complete=False,
                response_connection_close=False,
                connection_reusable_after=False,
                baton_in=baton_in,
                baton_out=None,
                close_acknowledged=False,
                outcome="outcome_unknown",
                error_code="connect_failed",
                error_type=type(failure).__name__,
                started_ns=started,
                connected_ns=completed,
                sent_ns=completed,
                headers_ns=completed,
                completed_ns=completed,
                spans=[],
                redacted_headers=redacted,
            )
            return None, exchange, protocol.OutcomeUnknown("connect_failed")
        connected = time.monotonic_ns()
        if not reused:
            try:
                connection.connect()
            except (OSError, ssl.SSLError) as failure:
                self._drop_connection()
                completed = time.monotonic_ns()
                exchange = Exchange(
                    request_id=request_id,
                    pipeline_correlation_id=correlation_id,
                    step_ids=list(step_ids),
                    connection_id=connection_id,
                    connections_created=created,
                    reused_connection=False,
                    request_bytes=len(payload),
                    status=None,
                    response_bytes=0,
                    response_complete=False,
                    response_connection_close=False,
                    connection_reusable_after=False,
                    baton_in=baton_in,
                    baton_out=None,
                    close_acknowledged=False,
                    outcome="outcome_unknown",
                    error_code="tls_or_tcp_connect_failed",
                    error_type=type(failure).__name__,
                    started_ns=started,
                    connected_ns=completed,
                    sent_ns=completed,
                    headers_ns=completed,
                    completed_ns=completed,
                    spans=[],
                    redacted_headers=redacted,
                )
                return None, exchange, protocol.OutcomeUnknown(
                    "tls_or_tcp_connect_failed")
            connected = time.monotonic_ns()
        spans = []
        if not reused:
            spans.append({"phase": "client.connect",
                          "duration_ns": max(0, connected - started)})
        with self._lock:
            self._requests_sent += 1
        try:
            connection.request("POST", path, body=bytes(payload), headers=headers)
        except (OSError, http.client.HTTPException, ssl.SSLError) as failure:
            self._drop_connection()
            completed = time.monotonic_ns()
            exchange = self._failure_exchange(
                request_id, correlation_id, step_ids, connection_id, created,
                reused, payload, baton_in, redacted, started, connected,
                connected, completed, "write_failed", failure, spans)
            return None, exchange, protocol.OutcomeUnknown("write_failed")
        sent = time.monotonic_ns()
        spans.append({"phase": "client.write", "duration_ns": max(0, sent - connected)})
        try:
            response = connection.getresponse()
        except (OSError, http.client.HTTPException, ssl.SSLError) as failure:
            self._drop_connection()
            completed = time.monotonic_ns()
            exchange = self._failure_exchange(
                request_id, correlation_id, step_ids, connection_id, created,
                reused, payload, baton_in, redacted, started, connected, sent,
                completed, "response_headers_failed", failure, spans)
            return None, exchange, protocol.OutcomeUnknown("response_headers_failed")
        headers_at = time.monotonic_ns()
        spans.append({"phase": "client.wait", "duration_ns": max(0, headers_at - sent)})
        try:
            body = self._read_body(response)
        except protocol.OutcomeUnknown as failure:
            self._drop_connection()
            completed = time.monotonic_ns()
            exchange = self._failure_exchange(
                request_id, correlation_id, step_ids, connection_id, created,
                reused, payload, baton_in, redacted, started, connected, sent,
                completed, failure.code, failure, spans)
            exchange.status = getattr(response, "status", None)
            exchange.response_bytes = 0
            return None, exchange, failure
        completed = time.monotonic_ns()
        spans.append({"phase": "client.read", "duration_ns": max(0, completed - headers_at)})
        response_close = bool(response.will_close)
        reusable_after = (not response_close and connection.sock is not None)
        if not reusable_after:
            self._drop_connection()
        else:
            with self._lock:
                self._requests_on_connection += 1
        exchange = Exchange(
            request_id=request_id,
            pipeline_correlation_id=correlation_id,
            step_ids=list(step_ids),
            connection_id=connection_id,
            connections_created=created,
            reused_connection=reused,
            request_bytes=len(payload),
            status=response.status,
            response_bytes=len(body),
            response_complete=True,
            response_connection_close=response_close,
            connection_reusable_after=reusable_after,
            baton_in=baton_in,
            baton_out=None,
            close_acknowledged=False,
            outcome="success",
            started_ns=started,
            connected_ns=connected,
            sent_ns=sent,
            headers_ns=headers_at,
            completed_ns=completed,
            spans=spans,
            redacted_headers=redacted,
        )
        try:
            reply = protocol.parse_pipeline_response(response.status, body, steps)
        except protocol.Rejected as failure:
            exchange.outcome = "rejected"
            exchange.error_code = failure.code
            return None, exchange, failure
        except protocol.OutcomeUnknown as failure:
            self._drop_connection()
            exchange.outcome = "outcome_unknown"
            exchange.error_code = failure.code
            exchange.connection_reusable_after = False
            return None, exchange, failure
        except protocol.ProtocolError as failure:
            self._drop_connection()
            exchange.outcome = "outcome_unknown"
            exchange.error_code = "invalid_response"
            exchange.error_type = type(failure).__name__
            exchange.connection_reusable_after = False
            return None, exchange, protocol.OutcomeUnknown("invalid_response",
                                                           response.status)
        exchange.baton_out = reply.baton
        exchange.close_acknowledged = reply.close_acknowledged
        if not reply.ok:
            exchange.outcome = "error"
            for result in reply.results:
                if not result.ok:
                    exchange.error_code = result.error_code
                    break
        return reply, exchange, None

    def _read_body(self, response):
        chunks = []
        total = 0
        while True:
            try:
                chunk = response.read(64 * 1024)
            except (OSError, http.client.HTTPException, ssl.SSLError) as failure:
                raise protocol.OutcomeUnknown("response_read_failed",
                                              getattr(response, "status", None),
                                              type(failure).__name__) from None
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_RESPONSE_BYTES:
                raise protocol.OutcomeUnknown("response_too_large",
                                              getattr(response, "status", None))
            chunks.append(chunk)
        remaining = getattr(response, "length", None)
        if remaining not in (None, 0):
            raise protocol.OutcomeUnknown("response_truncated",
                                          getattr(response, "status", None))
        return b"".join(chunks)

    def _failure_exchange(self, request_id, correlation_id, step_ids,
                          connection_id, created, reused, payload, baton_in,
                          redacted, started, connected, sent, completed,
                          code, failure, spans):
        return Exchange(
            request_id=request_id,
            pipeline_correlation_id=correlation_id,
            step_ids=list(step_ids),
            connection_id=connection_id,
            connections_created=created,
            reused_connection=reused,
            request_bytes=len(payload),
            status=None,
            response_bytes=0,
            response_complete=False,
            response_connection_close=False,
            connection_reusable_after=False,
            baton_in=baton_in,
            baton_out=None,
            close_acknowledged=False,
            outcome="outcome_unknown",
            error_code=code,
            error_type=type(failure).__name__,
            started_ns=started,
            connected_ns=connected,
            sent_ns=sent,
            headers_ns=sent,
            completed_ns=completed,
            spans=list(spans),
            redacted_headers=redacted,
        )
