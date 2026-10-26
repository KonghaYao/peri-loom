"""离线契约夹具与 loopback mock；绝不是真实性能证据。

默认标记 synthetic；contract_fixture=True 仅测试完整 formal gate 逻辑。
"""
import contextlib
import http.server
import io
import json
import os
from pathlib import Path
import re
import sys
import threading

PERF_ROOT = Path(__file__).resolve().parent
if str(PERF_ROOT) not in sys.path:
    sys.path.insert(0, str(PERF_ROOT))

from issue11 import contract, evidence_io  # noqa: E402

TEST_CREDENTIAL = "test-only-credential-6c0f4b9a"
ZERO_SHA256 = "0" * 64


def _decode_arg(argument):
    kind = argument.get("type")
    if kind == "null":
        return None
    if kind == "integer":
        return int(argument.get("value"))
    if kind == "float":
        return float(argument.get("value"))
    if kind == "text":
        return argument.get("value")
    if kind == "blob":
        import base64
        return base64.b64decode(argument.get("base64", ""))
    raise ValueError("unsupported arg type")


class MockState:
    def __init__(self, rows=1000, mode="normal"):
        self.rows = rows
        self.mode = mode
        self.batons = {}
        self.lock = threading.Lock()
        self.requests = []
        self.closed_baton_rejections = 0
        self.unknown_responses = 0
        self.truncated_responses = 0
        self.step_errors = 0
        self.baton_counter = 0
        self.connection_ports = set()

    def next_baton(self):
        self.baton_counter += 1
        return "baton-%d" % self.baton_counter

    def record(self, handler, body):
        with self.lock:
            self.requests.append({
                "path": handler.path,
                "authorization": handler.headers.get("Authorization"),
                "correlation": handler.headers.get("X-Pipeline-Correlation-ID"),
                "step_ids": handler.headers.get("X-Hrana-Step-IDs"),
                "request_id": handler.headers.get("X-Request-ID"),
                "content_type": handler.headers.get("Content-Type"),
                "body": body,
                "client_port": handler.client_address[1],
                "connection": handler.headers.get("Connection"),
            })
            self.connection_ports.add(handler.client_address[1])


class MockHandler(http.server.BaseHTTPRequestHandler):
    def setup(self):
        import socket
        super().setup()
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        return

    def _send(self, status, payload, content_type="application/json"):
        body = payload if isinstance(payload, bytes) else json.dumps(
            payload, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _execute(self, statement, state):
        import sqlite3
        if not hasattr(state, "connection"):
            state.connection = sqlite3.connect(":memory:", check_same_thread=False)
            state.connection.executescript(contract.read_sql("sql/00-schema.sql"))
            if state.rows:
                state.connection.executescript(contract.read_sql(contract.fixture_relative(state.rows)))
        sql = statement.get("sql", "")
        args = [_decode_arg(argument) for argument in statement.get("args", [])]
        cursor = state.connection.execute(sql, args)
        def cell(value):
            return {"type": "integer" if isinstance(value, int) else "text", "value": str(value)}
        rows = [[cell(value) for value in row] for row in cursor.fetchall()] if cursor.description else []
        return {"cols": [{"name": field[0], "decltype": None} for field in cursor.description or []],
                "rows": rows, "affected_row_count": max(0, cursor.rowcount),
                "last_insert_rowid": str(cursor.lastrowid) if cursor.lastrowid else None}

    def do_POST(self):
        state = self.server.state
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        state.record(self, body)
        if self.path != "/db/tenant/db/main/v2/pipeline":
            self._send(404, b"not found\n", "text/plain")
            return
        if self.headers.get("Authorization") != "Bearer " + TEST_CREDENTIAL:
            self._send(401, b"unauthenticated\n", "text/plain")
            return
        if self.headers.get("Content-Type") != "application/json":
            self._send(415, b"JSON required\n", "text/plain")
            return
        try:
            pipeline = json.loads(body)
        except json.JSONDecodeError:
            self._send(400, b"invalid pipeline\n", "text/plain")
            return
        requests = pipeline.get("requests")
        if not isinstance(requests, list) or not requests:
            self._send(400, b"invalid pipeline\n", "text/plain")
            return
        baton_in = pipeline.get("baton") or ""
        with state.lock:
            if state.mode == "reject":
                self._send(409, b"baton busy; do not automatically retry SQL\n",
                           "text/plain")
                return
            if baton_in and baton_in not in state.batons:
                state.closed_baton_rejections += 1
                self._send(403, b"closed baton\n", "text/plain")
                return
            baton = baton_in or state.next_baton()
            state.batons.setdefault(baton, {"closed": False})
            results = []
            closed = False
            for entry in requests:
                kind = entry.get("type")
                if kind == "close":
                    state.batons.pop(baton, None)
                    closed = True
                    results.append({"type": "ok",
                                    "response": {"type": "close"}})
                    continue
                if kind != "execute":
                    self._send(400, b"unsupported Hrana request\n", "text/plain")
                    return
                try:
                    result = self._execute(entry.get("stmt", {}), state)
                except ValueError as failure:
                    state.step_errors += 1
                    results.append({"type": "error", "error": {
                        "code": "constraint" if "constraint" in str(failure)
                        else "unsupported_sql",
                        "message": str(failure)}})
                    continue
                results.append({"type": "ok", "response": {
                    "type": "execute", "result": result}})
        if state.mode == "unknown_after_execute":
            state.unknown_responses += 1
            self._send(504, {"error": {
                "code": "pipeline_timeout",
                "partial_execution_possible": True,
                "outcome_unknown": True,
                "retryable": False,
            }})
            return
        if state.mode == "truncate_after_execute":
            state.truncated_responses += 1
            partial = b'{"baton":"' + baton.encode() + b'","results":'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", "4096")
            self.end_headers()
            self.wfile.write(partial)
            self.wfile.flush()
            self.close_connection = True
            return
        document = {"results": results}
        if not closed:
            document["baton"] = baton
        self._send(200, document)


class MockHranaServer:
    def __init__(self, rows=1000, mode="normal"):
        self.state = MockState(rows=rows, mode=mode)
        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0),
                                                     MockHandler)
        self.httpd.state = self.state
        self.thread = threading.Thread(target=self.httpd.serve_forever,
                                       daemon=True)

    @property
    def endpoint(self):
        return "http://127.0.0.1:%d" % self.httpd.server_address[1]

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)
        return False


def base_environment(evidence_root, formal=False, diagnostics=None,
                     synthetic=False):
    commit = contract.load_acceptance()["baseline_commit"]
    turso = contract.load_workloads()["turso_commit"]
    arch = "amd64" if formal else "arm64"
    if formal:
        storage = {
            "root_type": "xfs",
            "persistent": True,
            "durable": True,
            "retention": "fixed",
            "store_root": "/perf/store",
            "cache_root": "/perf/cache",
            "evidence_root": "/perf/evidence",
            "metrics_snapshot": "/perf/metrics",
        }
        backend = {"kind": "local", "durability": "fsync", "root": "/perf/store"}
        encryption = {"at_rest": True, "key_broker": True}
        tls = {"enabled": True, "verified": True, "ca_sha256": ZERO_SHA256}
        mounts = [{"path": "/perf/evidence", "type": "bind", "readonly": False}]
        virtualization = "none"
        evidence_class = "formal"
    else:
        storage = {
            "root_type": "apfs-diagnostic",
            "persistent": False,
            "durable": False,
            "retention": "ephemeral",
            "store_root": str(Path(evidence_root) / "store"),
            "cache_root": str(Path(evidence_root) / "cache"),
            "evidence_root": str(Path(evidence_root).resolve()),
            "metrics_snapshot": str(Path(evidence_root).resolve() / "metrics"),
        }
        backend = {"kind": "local", "durability": "diagnostic",
                   "root": str(Path(evidence_root).resolve())}
        encryption = {"at_rest": False, "key_broker": False}
        tls = {"enabled": False, "verified": False, "ca_sha256": ZERO_SHA256}
        mounts = [{"path": str(Path(evidence_root).resolve()),
                   "type": "host-directory", "readonly": False}]
        virtualization = "macos-native"
        evidence_class = "diagnostic"
    return {
        "schema": "issue11-environment",
        "schema_version": 1,
        "evidence_class": evidence_class,
        "source": {
            "commit": commit,
            "branch": "test",
            "dirty": False,
            "turso_gitlink": turso,
            "sdk": {"package": "test-sdk", "version": "test",
                    "lock_sha256": ZERO_SHA256},
        },
        "image": {
            "digest": "sha256:" + ZERO_SHA256,
            "tag": "synthetic-test",
            "os": "linux" if formal else "darwin",
            "arch": arch,
            "arch_native": True,
            "built_from_commit": commit,
        },
        "runtime": {
            "python": "3.12",
            "gateway_binary_sha256": ZERO_SHA256,
            "worker_binary_sha256": ZERO_SHA256,
            "worker_arch": arch,
            "gateway_arch": arch,
        },
        "host": {
            "os": "linux" if formal else "darwin",
            "arch": arch,
            "kernel": "test",
            "machine": arch,
            "virtualization": virtualization,
        },
        "limits": {"cpu_cores": 8, "memory_bytes": 16 * 1024 ** 3, "pids": 1024},
        "storage": storage,
        "backend": backend,
        "encryption": encryption,
        "tls": tls,
        "quotas": {"max_result_bytes": 8 * 1024 * 1024, "max_streams": 256,
                   "max_pipeline_steps": 64},
        "mounts": mounts,
        "diagnostics": diagnostics or {
            "PERI_WORK_METRICS": False,
            "PERI_DIAG_GROUP_PHASE": False,
            "PERI_ADMISSION_TRACE": False,
            "PERI_MAINTENANCE_SCHEDULE": False,
            "sql_open_observer": False,
            "PERI_SQL_OPEN_OBSERVER": False,
            "PERI_SQL_OPEN_OBSERVER_PHASE_THRESHOLD_MS": 20,
            "PERI_SQL_OPEN_OBSERVER_PHASE_SAMPLE_PERMILLE": 1000,
        },
        "recording": {
            "started_at_unix_ms": 1,
            "runner_sha256": ZERO_SHA256,
            "contract_sha256": ZERO_SHA256,
            "endpoint": "https://perf.invalid:9443",
            "authorization": "Bearer <REDACTED>",
        },
        "synthetic_test_fixture": synthetic,
    }


def write_jsonl(path, records):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as output:
        for record in records:
            output.write(json.dumps(record, separators=(",", ":"),
                                    sort_keys=True) + "\n")
        output.flush()
        os.fsync(output.fileno())


def write_run(run_directory, timed, warmup=200, formal=False,
              diagnostics=None, summary=None, close=False, spacing_ns=10_000,
              scenario="H1", arm="A", pair_index=0, state="fresh",
              contract_fixture=False):
    """Build a complete synthetic run; every record is explicitly test-only."""
    run_directory = Path(run_directory)
    run_directory.mkdir(parents=True, exist_ok=True)
    environment = base_environment(run_directory, formal=formal,
                                   diagnostics=diagnostics, synthetic=not contract_fixture)
    for name, value in {
        "PERI_SQL_OPEN_OBSERVER": False,
        "PERI_SQL_OPEN_OBSERVER_PHASE_THRESHOLD_MS": 20,
        "PERI_SQL_OPEN_OBSERVER_PHASE_SAMPLE_PERMILLE": 1000,
    }.items():
        environment["diagnostics"].setdefault(name, value)
    if contract_fixture:
        environment["image"]["tag"] = "contract-logic-only-not-performance"
    evidence_io.write_json(run_directory / "run.json", {
        "scenario": scenario, "arm": arm, "pair_index": pair_index,
        "state": state, "profile": "formal" if formal else "quick", "concurrency": 1,
        "evidence_extension_version": 2, "database": "tenant/db/main",
        "started_at_unix_ms": 1_000_000_000 + pair_index * 10_000_000
        + contract.load_acceptance()["sampling"]["formal"]["pair_order"][pair_index].index(arm) * 4_000_000,
        "completed_at_unix_ms": 1_003_900_000 + pair_index * 10_000_000
        + contract.load_acceptance()["sampling"]["formal"]["pair_order"][pair_index].index(arm) * 4_000_000,
        "diagnostics": environment["diagnostics"],
        "matched_state": {"data_sha256": contract.fixture_digest(1000),
                          "metadata_sha256": ZERO_SHA256},
    })
    evidence_io.write_json(run_directory / "environment.json", environment)
    operations = []
    requests = []
    attribution = []
    clock = "client:test"
    issued = 1_000_000
    duration = 6_250_000 if arm == "A" else 5_000_000
    index = 0
    total = warmup + timed
    for phase, count in (("warmup", warmup), ("timed", timed)):
        for sample in range(count):
            request_id = "req-%s-%d" % (phase, sample)
            correlation = "corr-%s-%d" % (phase, sample)
            step_id = "step-%s-%d" % (phase, sample)
            started = issued
            completed = started + duration
            requests.append({
                "schema_version": 1,
                "record_type": "http_request",
                "request_id": request_id,
                "pipeline_correlation_id": correlation,
                "step_ids": [step_id],
                "scenario": scenario,
                "sample_index": sample,
                "phase": phase,
                "connection_id": 1,
                "connections_created": 1,
                "reused_connection": index > 0,
                "tls": environment["tls"]["enabled"],
                "request_bytes": 200,
                "status": 200,
                "response_bytes": 200,
                "response_complete": True,
                "response_connection_close": False,
                "connection_reusable_after": True,
                "baton_in": "baton-test",
                "baton_out": "baton-test",
                "close_step": close,
                "close_acknowledged": close,
                "outcome": "success",
                "error_code": "",
                "error_type": "",
                "started_ns": started,
                "connected_ns": started,
                "sent_ns": started + 1,
                "headers_ns": started + 2,
                "completed_ns": completed,
                "duration_ns": duration,
                "request_headers_redacted": {
                    "Authorization": "Bearer <REDACTED>",
                    "Content-Type": "application/json",
                },
            })
            spans = [{"span_id": "client-%d" % index, "parent_span_id": None,
                      "process": "client", "phase": "client.wait", "clock_id": clock,
                      "duration_ns": duration, "outcome": "success"}]
            for process in ("gateway", "worker", "authority", "object_store"):
                root_id = "%s-%d" % (process, index)
                spans.extend([
                    {"span_id": root_id, "parent_span_id": None,
                     "process": process, "phase": process + ".root",
                     "clock_id": process + ":test", "duration_ns": duration,
                     "outcome": "success"},
                    {"span_id": root_id + "-phase", "parent_span_id": root_id,
                     "process": process, "phase": process + ".work",
                     "clock_id": process + ":test", "duration_ns": duration,
                     "outcome": "success"},
                ])
            attribution.append({
                "schema_version": 1,
                "record_type": "request",
                "request_id": request_id,
                "pipeline_correlation_id": correlation,
                "scenario": scenario,
                "sample_index": sample,
                "phase": phase,
                "step_ids": [step_id],
                "total_ns": duration,
                "spans": spans,
                "process_coverage": ["client", "gateway", "worker",
                                     "authority", "object_store"],
                "server_evidence": True,
                "outcome": "success",
            })
            operations.append({
                "schema_version": 1,
                "record_type": "operation",
                "op_id": "op-%s-%d" % (phase, sample),
                "scenario": scenario,
                "sample_index": sample,
                "phase": phase,
                "issued_ns": started,
                "completed_ns": completed,
                "duration_ns": duration,
                "outcome": "success",
                "http_request_ids": [request_id],
                "pipeline_correlation_ids": [correlation],
                "step_ids": [step_id],
                "verification": {"ok": True, "kind": "readback"},
                "may_have_committed": False,
                "retry_count": 0,
                "synthetic_test_fixture": not contract_fixture,
            })
            issued = completed + spacing_ns
            index += 1
    if scenario == "H4":
        import copy
        split_operations, split_requests, split_attribution = [], [], []
        for operation, request, document in zip(operations, requests, attribution):
            for index, action in enumerate(("insert", "select")):
                op, req, doc = (copy.deepcopy(item) for item in (operation, request, document))
                suffix = "-" + action
                start = operation["issued_ns"] + operation["duration_ns"] * index // 2
                end = operation["issued_ns"] + operation["duration_ns"] * (index + 1) // 2
                req.update(request_id=request["request_id"] + suffix,
                           pipeline_correlation_id=request["pipeline_correlation_id"] + suffix,
                           step_ids=[request["step_ids"][0] + suffix],
                           started_ns=start, completed_ns=end, duration_ns=end-start,
                           connected_ns=start, sent_ns=start+1, headers_ns=start+2,
                           reused_connection=request["reused_connection"] or index == 1)
                op.update(op_id=operation["op_id"]+suffix, action=action,
                          issued_ns=start, completed_ns=end, duration_ns=end-start,
                          http_request_ids=[req["request_id"]],
                          pipeline_correlation_ids=[req["pipeline_correlation_id"]], step_ids=req["step_ids"])
                doc.update(request_id=req["request_id"], pipeline_correlation_id=req["pipeline_correlation_id"],
                           step_ids=req["step_ids"], total_ns=end-start)
                for span in doc["spans"]:
                    span["span_id"] += suffix
                    if span.get("parent_span_id"):
                        span["parent_span_id"] += suffix
                    span["duration_ns"] = end-start
                split_operations.append(op)
                split_requests.append(req)
                split_attribution.append(doc)
        operations, requests, attribution = split_operations, split_requests, split_attribution
    from test_issue11_v2_support import recovery_fixture, extended_fixture
    recovery = recovery_fixture(run_directory.name, scenario, operations, requests,
                                evidence_io.read_json(run_directory / "run.json"))
    write_jsonl(run_directory / "operations.jsonl", operations)
    write_jsonl(run_directory / "requests.jsonl", requests)
    write_jsonl(run_directory / "request-attribution.jsonl", attribution)
    periodic = []
    kinds = ("audit_head", "journal_compact", "receipt_roll", "checkpoint",
             "index_compact")
    for kind in kinds:
        pair_id = "pair-" + kind
        periodic.append({
            "schema_version": 1,
            "record_type": "periodic_event",
            "event_id": pair_id + "-start",
            "process": "gateway",
            "clock_id": "gateway:test",
            "kind": kind,
            "event_phase": "start",
            "pair_id": pair_id,
            "started_monotonic_ns": 1,
            "duration_ns": 0,
            "outcome": "running",
        })
        periodic.append({
            "schema_version": 1,
            "record_type": "periodic_event",
            "event_id": pair_id + "-end",
            "process": "gateway",
            "clock_id": "gateway:test",
            "kind": kind,
            "event_phase": "end",
            "pair_id": pair_id,
            "started_monotonic_ns": 2,
            "duration_ns": 1000,
            "outcome": "success",
        })
    import copy
    periodic = [dict(event, event_id=event["event_id"] + "-%d" % cycle,
                     pair_id=event["pair_id"] + "-%d" % cycle)
                for cycle in range(3) for event in periodic]
    write_jsonl(run_directory / "periodic-events.jsonl", periodic)
    metrics_directory = run_directory / "metrics"
    metrics_directory.mkdir(exist_ok=True)
    snapshot_path = metrics_directory / "worker.json"
    evidence_io.write_json(snapshot_path, {
        "schema_version": 1,
        "work_metrics_enabled": True,
        "generated_at_unix_ms": 1,
        "process": {"pid": 4321},
        "counters": {
            "work.wal_workload.append_calls": timed,
            "work.metadata.root_publish_attempts": timed,
            "object_store.put.total": timed,
            "object_store.wal_durability.total": timed,
        },
    })
    evidence_io.write_json(run_directory / "metrics-snapshot-index.json", {
        "schema": 1,
        "available": True,
        "snapshots": [{"path": "metrics/worker.json",
                       "sha256": contract.digest(snapshot_path),
                       "process": "worker"}],
    })
    evidence_io.write_json(run_directory / "resources.json", {
        "schema": 1,
        "client": {"pid": os.getpid(), "rss_bytes": 1, "cpu_seconds": 0.1},
        "product_processes": [{
            "pid": 4321, "rss_bytes": 1024, "group_joined": True,
            "exit_code": 0,
        }],
        "product_processes_accounted": True,
        "permit_residual": 0,
        "extended_evidence": extended_fixture(),
        "failure": None,
    })
    evidence_io.write_json(run_directory / "oracle.json", {
        "schema": 1,
        "fixture_scale": 1000,
        "fixture_rows": 1000,
        "fixture_sha256": contract.fixture_digest(1000),
        "initial_content_sha256": contract.fixture_digest(1000),
        "setup_executed": False,
        "verified_operations": timed,
        "recovery": recovery,
        "mismatches": [],
    })
    evidence_io.write_json(run_directory / "summary.json", summary or {
        "schema": 1,
        "status": "RUN_COMPLETED",
        "display_only": True,
        "synthetic_test_fixture": not contract_fixture,
    })
    evidence_io.write_manifests(run_directory, run_directory.name)
    return run_directory


def write_campaign(root, runs, profile="formal", pair_order=None,
                   scenarios=None, contract_fixture=False):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    pair_order = pair_order or ["AB", "BA", "AB", "BA", "AB"]
    campaign = {
        "schema": 1,
        "id": "synthetic-test-campaign",
        "profile": profile,
        "pair_order": pair_order,
        "synthetic_test_fixture": not contract_fixture,
    }
    if scenarios is not None:
        campaign["scenarios"] = scenarios
    evidence_io.write_json(root / "campaign.json", campaign)
    evidence_io.write_json(root / "campaign-identity.json", {
        "schema": 1,
        "runner_version": "test",
        "synthetic_test_fixture": not contract_fixture,
    })
    ledger = root / "campaign-runs.jsonl"
    with ledger.open("w") as output:
        for record in runs:
            output.write(json.dumps(record, separators=(",", ":")) + "\n")
        output.flush()
        os.fsync(output.fileno())
    return root


def runner_arguments(server, evidence_root, scenario="H1", samples=5,
                     warmup=2, extra=None):
    arguments = [
        "--scenario", scenario,
        "--profile", "quick",
        "--endpoint", server.endpoint,
        "--database", "tenant/db/main",
        "--credential-env", "ISSUE11_TEST_TOKEN",
        "--diagnostic-plaintext",
        "--evidence-root", str(evidence_root),
        "--samples", str(samples),
        "--warmup", str(warmup),
        "--synthetic-test-fixture",
    ]
    arguments.extend(extra or [])
    return arguments


@contextlib.contextmanager
def run_runner(arguments):
    from issue11.http import runner
    os.environ["ISSUE11_TEST_TOKEN"] = TEST_CREDENTIAL
    stdout = io.StringIO()
    stderr = io.StringIO()
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = runner.main(arguments)
        yield code, stdout.getvalue(), stderr.getvalue()
    finally:
        os.environ.pop("ISSUE11_TEST_TOKEN", None)
