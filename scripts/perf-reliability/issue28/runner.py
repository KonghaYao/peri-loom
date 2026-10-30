"""Execution engine for the Issue 28 performance plan."""
from __future__ import annotations

import concurrent.futures
import json
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field

from .contract import ScenarioSpec
from .fixture import Fixture, MetricsApi, SqlApi
from .hrana import (
    FatalTransportError,
    HranaClient,
    HranaError,
    Target,
    parse_cursor_json,
    parse_cursor_protobuf,
    parse_pipeline_json,
    parse_pipeline_protobuf,
    redact_error,
)


@dataclass
class Measurement:
    scenario: str
    protocol: str
    kind: str
    phase: str
    round_index: int
    sample_index: int
    status: int
    duration_ms: float
    ttfb_ms: float
    request_bytes: int
    response_bytes: int
    rows: int
    rows_read: int
    rows_written: int
    affected_row_count: int
    query_ms: float | None
    outcome: str
    error_code: str = ""
    detail: str = ""
    operation: str = ""
    concurrency: int = 0
    baton_reused: bool = False
    connection_reused: bool = False
    request_id: str = ""
    baton_out: str = ""
    cleanup_ms: float = 0.0
    cleanup_error: str = ""

    def as_record(self) -> dict:
        record = self.__dict__.copy()
        return record


@dataclass
class ConcurrencyMetrics:
    level: int
    before: dict
    after: dict
    drained: dict
    drain_ms: float
    requests: int
    status_counts: dict
    error_codes: dict


@dataclass
class RunResult:
    measurements: list[Measurement] = field(default_factory=list)
    concurrency: list[ConcurrencyMetrics] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    fatal: str = ""

    @property
    def timed(self) -> list[Measurement]:
        return [item for item in self.measurements if item.phase == "timed"]


class Runner:
    def __init__(
            self,
            target: Target,
            fixture: Fixture,
            scenarios: list[ScenarioSpec],
            rounds: int = 3,
            warmup: int = 1,
            samples: int = 5,
            metrics: MetricsApi | None = None):
        self.target = target
        self.fixture = fixture
        self.scenarios = scenarios
        self.rounds = rounds
        self.warmup = warmup
        self.samples = samples
        self.metrics = metrics
        self.result = RunResult()
        self._clients: dict[str, HranaClient] = {}
        self._concurrency_clients: list[HranaClient] = []
        self._concurrency_lock = threading.Lock()

    def run(self) -> RunResult:
        try:
            for spec in self.scenarios:
                if spec.kind == "concurrency":
                    self._run_concurrency(spec)
                    continue
                for protocol in spec.protocols:
                    if spec.kind == "baton":
                        self._run_baton(spec, protocol)
                    elif spec.kind == "write":
                        self._run_write(spec, protocol)
                    else:
                        self._run_standard(spec, protocol)
        except FatalTransportError as error:
            self.result.fatal = self._redact(str(error))
            self.result.warnings.append("fatal transport error: " + self.result.fatal)
        finally:
            for client in self._clients.values():
                client.close()
            with self._concurrency_lock:
                for client in self._concurrency_clients:
                    client.close()
            self._clients.clear()
        return self.result

    # -- standard reads ----------------------------------------------------

    def _run_standard(self, spec: ScenarioSpec, protocol) -> None:
        client = self._client(protocol.name)
        for round_index in range(1, self.rounds + 1):
            for phase, count in (("warmup", self.warmup), ("timed", self.samples)):
                for sample_index in range(1, count + 1):
                    measurement = self._request_once(
                        spec=spec,
                        protocol=protocol,
                        client=client,
                        sql=spec.sql,
                        args=spec.args,
                        want_rows=spec.want_rows,
                        phase=phase,
                        round_index=round_index,
                        sample_index=sample_index,
                    )
                    if measurement is None:
                        return
                    if measurement.cleanup_error:
                        self.result.warnings.append(
                            "%s/%s cursor close failed: %s" % (
                                spec.key, protocol.name, measurement.cleanup_error))
                    self.result.measurements.append(measurement)
        self._progress(spec, protocol)

    def _run_write(self, spec: ScenarioSpec, protocol) -> None:
        client = self._client(protocol.name)
        for round_index in range(1, self.rounds + 1):
            for phase, count in (("warmup", self.warmup), ("timed", self.samples)):
                for sample_index in range(1, count + 1):
                    table = "issue28_w_%s" % uuid.uuid4().hex[:12]
                    sql = _write_sql(spec.write_operation, table)
                    try:
                        self._setup_write(client, spec.write_operation, table)
                        measurement = self._request_once(
                            spec=spec,
                            protocol=protocol,
                            client=client,
                            sql=sql,
                            args=(),
                            want_rows=False,
                            phase=phase,
                            round_index=round_index,
                            sample_index=sample_index,
                            operation=spec.write_operation,
                        )
                        if measurement is None:
                            return
                        self.result.measurements.append(measurement)
                        if not measurement.outcome.startswith("ok"):
                            # A write with a lost reply may have committed. Stop
                            # the scenario instead of issuing another write.
                            self.result.warnings.append(
                                "%s/%s write stopped after %s" % (
                                    spec.key, protocol.name, measurement.outcome))
                            self._progress(spec, protocol)
                            return
                    finally:
                        self._cleanup_write(client, table)
        self._progress(spec, protocol)

    def _run_baton(self, spec: ScenarioSpec, protocol) -> None:
        client = self._client(protocol.name)
        first_error = ""
        for round_index in range(1, self.rounds + 1):
            for phase, count in (("warmup", self.warmup), ("timed", self.samples)):
                for cycle in range(1, count + 1):
                    baton = ""
                    try:
                        for index in range(spec.baton_executes):
                            measurement = self._request_once(
                                spec=spec,
                                protocol=protocol,
                                client=client,
                                sql=spec.sql,
                                args=spec.args,
                                want_rows=spec.want_rows,
                                phase=phase,
                                round_index=round_index,
                                sample_index=(cycle - 1) * spec.baton_executes + index + 1,
                                baton=baton,
                                close_stream=False,
                                baton_reused=bool(baton),
                            )
                            if measurement is None:
                                return
                            self.result.measurements.append(measurement)
                            if measurement.baton_out:
                                baton = measurement.baton_out
                            if not measurement.outcome.startswith("ok") and not first_error:
                                first_error = measurement.error_code or measurement.outcome
                            if not baton:
                                break
                    finally:
                        if baton:
                            try:
                                closed = client.pipeline_json(
                                    [{"type": "close"}], baton=baton)
                                closed_reply = parse_pipeline_json(closed.body)
                                if any(not item.ok for item in closed_reply.results):
                                    self.result.warnings.append(
                                        "baton close rejected for %s" % spec.key)
                            except FatalTransportError as error:
                                self.result.warnings.append(
                                    "baton close transport failure for %s: %s" % (
                                        spec.key, type(error).__name__))
                            except HranaError:
                                self.result.warnings.append(
                                    "baton close decode failure for %s" % spec.key)
                            except Exception as error:  # noqa: BLE001 - evidence must survive
                                self.result.warnings.append(
                                    "baton close failed for %s: %s" % (
                                        spec.key, type(error).__name__))
                    if first_error:
                        break
                if first_error:
                    break
            if first_error:
                break
        self._progress(spec, protocol)

    def _progress(self, spec: ScenarioSpec, protocol) -> None:
        timed = [item for item in self.result.measurements
                 if item.scenario == spec.key and item.protocol == protocol.name
                 and item.phase == "timed"]
        if not timed:
            return
        durations = sorted(item.duration_ms for item in timed)
        print("  %-24s %-18s n=%-3d median=%7.2fms p95=%7.2fms" % (
            spec.key, protocol.name, len(durations), _median(durations),
            _p95(durations)), file=sys.stderr)

    # -- writes ------------------------------------------------------------

    def _setup_write(self, client: HranaClient, operation: str, table: str) -> None:
        if operation == "create":
            return
        self._untimed_sql(client, _write_sql("create", table))
        if operation == "delete":
            self._untimed_sql(
                client, "INSERT INTO \"%s\" (payload) VALUES ('issue28')" % table)

    def _cleanup_write(self, client: HranaClient, table: str) -> None:
        try:
            self._untimed_sql(client, 'DROP TABLE IF EXISTS "%s"' % table)
        except Exception as error:  # noqa: BLE001 - cleanup must be visible
            self.result.warnings.append(
                "cleanup failed for %s: %s" % (table, type(error).__name__))

    def _untimed_sql(self, client: HranaClient, sql: str) -> None:
        raw = client.pipeline_json([
            {"type": "execute", "sql": sql, "want_rows": False, "args": ()},
            {"type": "close"},
        ])
        if raw.status != 200:
            raise HranaError("setup HTTP %d" % raw.status)
        reply = parse_pipeline_json(raw.body)
        failure = next((item for item in reply.results if not item.ok), None)
        if failure is not None:
            raise HranaError("setup %s" % failure.error_code)

    # -- concurrency -------------------------------------------------------

    def _run_concurrency(self, spec: ScenarioSpec) -> None:
        if self.metrics is None:
            self.result.warnings.append("concurrency skipped: no metrics client")
            return
        protocol = spec.protocols[0]
        warmup_client = self._client(protocol.name)
        self._request_once(
            spec=spec, protocol=protocol, client=warmup_client,
            sql=spec.sql, args=spec.args, want_rows=spec.want_rows,
            phase="warmup", round_index=1, sample_index=1,
            concurrency=0,
        )
        for level in spec.concurrency_levels:
            before = self._metrics_snapshot("before %d" % level)
            started = time.monotonic()
            local = threading.local()
            with concurrent.futures.ThreadPoolExecutor(max_workers=level) as pool:
                futures = [
                    pool.submit(self._concurrent_one, spec, protocol, local, level, index)
                    for index in range(100)
                ]
                measurements = []
                for future in concurrent.futures.as_completed(futures):
                    try:
                        measurements.append(future.result())
                    except FatalTransportError:
                        raise
                    except Exception as error:  # noqa: BLE001 - keep evidence
                        self.result.warnings.append(
                            "concurrency level %d request failed: %s" % (
                                level, type(error).__name__))
            elapsed_ms = (time.monotonic() - started) * 1000.0
            with self._concurrency_lock:
                clients = list(self._concurrency_clients)
                self._concurrency_clients.clear()
            for client in clients:
                client.close()
            after = self._metrics_snapshot("after %d" % level)
            drained, drain_ms = self._drain_metrics(level)
            status_counts: dict[str, int] = {}
            error_codes: dict[str, int] = {}
            for item in measurements:
                self.result.measurements.append(item)
                status_counts[str(item.status)] = status_counts.get(str(item.status), 0) + 1
                if item.error_code:
                    error_codes[item.error_code] = error_codes.get(item.error_code, 0) + 1
            self.result.concurrency.append(ConcurrencyMetrics(
                level=level,
                before=before,
                after=after,
                drained=drained,
                drain_ms=drain_ms,
                requests=len(measurements),
                status_counts=status_counts,
                error_codes=error_codes,
            ))
            durations = sorted(item.duration_ms for item in measurements)
            print("  X1 concurrency=%-2d n=%d elapsed=%.0fms p50=%.2fms p95=%.2fms p99=%.2fms 429=%d" % (
                level, len(durations), elapsed_ms, _p50(durations), _p95(durations),
                _p99(durations), status_counts.get("429", 0)), file=sys.stderr)

    def _concurrent_one(self, spec, protocol, local, level: int, index: int) -> Measurement:
        client = getattr(local, "client", None)
        if client is None:
            client = HranaClient(self.target)
            local.client = client
            with self._concurrency_lock:
                self._concurrency_clients.append(client)
        measurement = self._request_once(
            spec=spec, protocol=protocol, client=client,
            sql=spec.sql, args=spec.args, want_rows=spec.want_rows,
            phase="timed", round_index=1, sample_index=index + 1,
            concurrency=level,
        )
        if measurement is None:
            raise HranaError("concurrency request produced no measurement")
        return measurement

    def _metrics_snapshot(self, label: str) -> dict:
        try:
            return self.metrics.snapshot()
        except Exception as error:  # noqa: BLE001 - metrics are evidence, not control
            self.result.warnings.append(
                "metrics snapshot %s failed: %s" % (label, type(error).__name__))
            return {}

    def _drain_metrics(self, level: int) -> tuple[dict, float]:
        started = time.monotonic()
        snapshot: dict = {}
        for _ in range(20):
            snapshot = self._metrics_snapshot("drain %d" % level)
            if snapshot.get("active_permits") == 0 and snapshot.get("unknown_held_permits") == 0:
                break
            time.sleep(0.5)
        return snapshot, (time.monotonic() - started) * 1000.0

    # -- requests ----------------------------------------------------------

    def _request_once(
            self,
            *,
            spec: ScenarioSpec,
            protocol,
            client: HranaClient,
            sql: str,
            args,
            want_rows: bool,
            phase: str,
            round_index: int,
            sample_index: int,
            operation: str = "",
            concurrency: int = 0,
            baton: str | None = None,
            baton_reused: bool = False,
            close_stream: bool = True) -> Measurement | None:
        try:
            if protocol.mode == "pipeline":
                requests = _pipeline_requests(spec, sql, args, want_rows, close_stream)
                if protocol.codec == "json":
                    raw = client.pipeline_json(requests, baton=baton)
                    reply = parse_pipeline_json(raw.body) if raw.status == 200 else None
                else:
                    raw = client.pipeline_protobuf(requests, baton=baton)
                    reply = parse_pipeline_protobuf(raw.body) if raw.status == 200 else None
                return self._pipeline_measurement(
                    spec, protocol, raw, reply, phase, round_index, sample_index,
                    operation, concurrency, baton_reused)
            if protocol.mode == "cursor":
                if protocol.codec == "json":
                    raw = client.cursor_json(sql, args=args, want_rows=want_rows, baton=baton)
                    reply = parse_cursor_json(raw.body) if raw.status == 200 else None
                else:
                    raw = client.cursor_protobuf(sql, args=args, want_rows=want_rows, baton=baton)
                    reply = parse_cursor_protobuf(raw.body) if raw.status == 200 else None
                return self._cursor_measurement(
                    spec, protocol, client, raw, reply, phase, round_index,
                    sample_index, operation, concurrency)
            raise HranaError("unsupported protocol mode %r" % protocol.mode)
        except HranaError as error:
            self.result.warnings.append(
                "%s/%s decode failure: %s" % (spec.key, protocol.name, type(error).__name__))
            return Measurement(
                scenario=spec.key, protocol=protocol.name, kind=spec.kind,
                phase=phase, round_index=round_index, sample_index=sample_index,
                status=0, duration_ms=0.0, ttfb_ms=0.0,
                request_bytes=0, response_bytes=0, rows=0, rows_read=0,
                rows_written=0, affected_row_count=0, query_ms=None,
                outcome="decode_error", error_code=type(error).__name__,
                detail=self._redact(str(error)), operation=operation,
                concurrency=concurrency, baton_reused=baton_reused)
        except FatalTransportError as error:
            if "authentication rejected" in str(error):
                raise
            # A dead keep-alive socket is not a SQL outcome. Do not retry the
            # request; record it and let the next sample use a fresh socket.
            self.result.warnings.append(
                "%s/%s transport error: %s" % (
                    spec.key, protocol.name, type(error).__name__))
            return Measurement(
                scenario=spec.key, protocol=protocol.name, kind=spec.kind,
                phase=phase, round_index=round_index, sample_index=sample_index,
                status=0, duration_ms=0.0, ttfb_ms=0.0,
                request_bytes=0, response_bytes=0, rows=0, rows_read=0,
                rows_written=0, affected_row_count=0, query_ms=None,
                outcome=("outcome_unknown" if spec.kind == "write"
                         else "transport_error"),
                error_code=type(error).__name__,
                detail=self._redact(str(error)), operation=operation,
                concurrency=concurrency, baton_reused=baton_reused)

    def _pipeline_measurement(self, spec, protocol, raw, reply, phase,
                              round_index, sample_index, operation, concurrency,
                              baton_reused) -> Measurement:
        error_code = ""
        detail = ""
        if raw.status != 200:
            error_code, detail = _http_error(raw)
            rows = rows_read = rows_written = affected = 0
            query_ms = None
            outcome = "http_error"
        elif reply is None:
            error_code, detail = "empty_reply", ""
            rows = rows_read = rows_written = affected = 0
            query_ms = None
            outcome = "decode_error"
        else:
            failures = [item for item in reply.results if not item.ok]
            executes = [item for item in reply.results if item.ok and item.response_type == "execute"]
            rows = sum(item.rows for item in executes)
            rows_read = sum(item.rows_read for item in executes)
            rows_written = sum(item.rows_written for item in executes)
            affected = sum(item.affected_row_count for item in executes)
            durations = [item.query_ms for item in executes if item.query_ms is not None]
            if len(durations) == 1:
                query_ms = durations[0]
            elif durations:
                query_ms = sum(durations)
            else:
                query_ms = None
            if failures:
                error_code = failures[0].error_code or "error"
                detail = failures[0].error_message or ""
                outcome = "expected_limit" if _is_result_limit(error_code) else "error"
            else:
                outcome = "ok"
            if spec.expected_rows is not None and spec.kind != "batch":
                if rows != spec.expected_rows:
                    outcome = "mismatch"
                    error_code = "row_count_mismatch"
                    detail = "expected %d rows, got %d" % (spec.expected_rows, rows)
            if spec.kind == "batch" and spec.expected_rows is not None:
                if rows != spec.expected_rows:
                    outcome = "mismatch"
                    error_code = "row_count_mismatch"
                    detail = "expected %d rows, got %d" % (spec.expected_rows, rows)
            if spec.kind == "write" and operation in ("insert", "delete") and affected != 1:
                outcome = "mismatch"
                error_code = "affected_row_count_mismatch"
                detail = "expected 1 affected row, got %d" % affected
        return Measurement(
            scenario=spec.key, protocol=protocol.name, kind=spec.kind,
            phase=phase, round_index=round_index, sample_index=sample_index,
            status=raw.status, duration_ms=raw.duration_ms, ttfb_ms=raw.ttfb_ms,
            request_bytes=raw.request_bytes, response_bytes=len(raw.body),
            rows=rows, rows_read=rows_read, rows_written=rows_written,
            affected_row_count=affected, query_ms=query_ms, outcome=outcome,
            error_code=error_code, detail=self._redact(detail), operation=operation,
            concurrency=concurrency, baton_reused=baton_reused,
            connection_reused=raw.connection_reused, request_id=raw.request_id,
            baton_out=(reply.baton or "") if reply is not None else "")

    def _cursor_measurement(self, spec, protocol, client, raw, reply, phase,
                            round_index, sample_index, operation, concurrency) -> Measurement:
        error_code = ""
        detail = ""
        cleanup_ms = 0.0
        cleanup_error = ""
        if raw.status != 200:
            error_code, detail = _http_error(raw)
            rows = 0
            outcome = "http_error"
        elif reply is None:
            error_code, detail = "empty_reply", ""
            rows = 0
            outcome = "decode_error"
        else:
            rows = reply.rows
            if reply.errors:
                error_code = reply.errors[0]
                detail = ", ".join(reply.errors[:3])
                outcome = "expected_limit" if _is_result_limit(error_code) else "error"
            else:
                outcome = "ok"
            if spec.expected_rows is not None and rows != spec.expected_rows:
                outcome = "mismatch"
                error_code = "row_count_mismatch"
                detail = "expected %d rows, got %d" % (spec.expected_rows, rows)
        if reply is not None and reply.baton:
            started = time.monotonic_ns()
            try:
                closed = client.close_cursor(reply.baton, protocol.codec)
                cleanup_ms = (time.monotonic_ns() - started) / 1_000_000.0
                if closed.status != 200:
                    cleanup_error = "cursor close HTTP %d" % closed.status
                else:
                    closed_reply = (parse_pipeline_json(closed.body)
                                    if protocol.codec == "json"
                                    else parse_pipeline_protobuf(closed.body))
                    if any(not item.ok for item in closed_reply.results):
                        cleanup_error = "cursor close rejected"
            except FatalTransportError as error:
                if "authentication rejected" in str(error):
                    raise
                cleanup_ms = (time.monotonic_ns() - started) / 1_000_000.0
                cleanup_error = type(error).__name__
            except (HranaError, OSError) as error:
                cleanup_ms = (time.monotonic_ns() - started) / 1_000_000.0
                cleanup_error = type(error).__name__
        return Measurement(
            scenario=spec.key, protocol=protocol.name, kind=spec.kind,
            phase=phase, round_index=round_index, sample_index=sample_index,
            status=raw.status, duration_ms=raw.duration_ms, ttfb_ms=raw.ttfb_ms,
            request_bytes=raw.request_bytes, response_bytes=len(raw.body),
            rows=rows, rows_read=0, rows_written=0, affected_row_count=0,
            query_ms=None, outcome=outcome, error_code=error_code,
            detail=self._redact(detail), operation=operation,
            concurrency=concurrency, connection_reused=raw.connection_reused,
            request_id=raw.request_id, cleanup_ms=cleanup_ms,
            cleanup_error=cleanup_error)

    def _client(self, protocol_name: str) -> HranaClient:
        client = self._clients.get(protocol_name)
        if client is None:
            client = HranaClient(self.target)
            self._clients[protocol_name] = client
        return client

    def _redact(self, text: str) -> str:
        return redact_error(text, self.target)


def _pipeline_requests(spec: ScenarioSpec, sql: str, args, want_rows: bool,
                       close_stream: bool) -> list[dict]:
    if spec.kind == "batch":
        requests = [{
            "type": "execute",
            "sql": spec.sql,
            "want_rows": spec.want_rows,
            "args": spec.args,
        } for _ in range(spec.batch_size)]
    else:
        requests = [{
            "type": "execute",
            "sql": sql,
            "want_rows": want_rows,
            "args": tuple(args),
        }]
    if close_stream:
        requests.append({"type": "close"})
    return requests


def _write_sql(operation: str, table: str) -> str:
    quoted = '"%s"' % table
    if operation == "create":
        return "CREATE TABLE %s (id INTEGER PRIMARY KEY, payload TEXT)" % quoted
    if operation == "insert":
        return "INSERT INTO %s (payload) VALUES ('issue28')" % quoted
    if operation == "delete":
        return "DELETE FROM %s" % quoted
    if operation == "drop":
        return "DROP TABLE %s" % quoted
    raise ValueError("unknown write operation %r" % operation)


def _http_error(raw) -> tuple[str, str]:
    body = raw.body[:512]
    try:
        document = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        text = body.decode("utf-8", "replace").strip()
        code = "http_%d" % raw.status
        if raw.status == 413 and text:
            code = text
        return code, text[:200]
    error = document.get("error")
    if isinstance(error, dict):
        return str(error.get("code") or "http_%d" % raw.status), str(error.get("message") or "")
    if isinstance(error, str):
        return error, error
    return "http_%d" % raw.status, ""


def _is_result_limit(code: str) -> bool:
    lowered = code.lower()
    return "result_limit" in lowered or "too_large" in lowered or "413" in lowered


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _percentile(values: list[float], percent: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    import math
    rank = max(1, math.ceil(percent / 100.0 * len(ordered)))
    return ordered[rank - 1]


def _p50(values: list[float]) -> float:
    return _percentile(values, 50)


def _p95(values: list[float]) -> float:
    return _percentile(values, 95)


def _p99(values: list[float]) -> float:
    return _percentile(values, 99)
