#!/usr/bin/env python3
"""Bounded native worker benchmark; no builds, no credential output."""
import argparse
import concurrent.futures
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import selectors
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]


class ProtocolError(ValueError):
    pass


def validate_response(response, identifier, operation):
    if not isinstance(response, dict) or type(response.get("id")) is not int or response["id"] != identifier:
        raise ProtocolError("response_identity_or_object")
    if type(response.get("ok")) is not bool or not isinstance(response.get("error", ""), str):
        raise ProtocolError("response_outcome_schema")
    if response["ok"] and response.get("error", ""):
        raise ProtocolError("contradictory_success_error")
    if not response["ok"]:
        return
    if operation == "open":
        if not isinstance(response.get("session"), str) or not 1 <= len(response["session"]) <= 128:
            raise ProtocolError("open_session_schema")
    elif operation == "query":
        rows = response.get("result")
        if not isinstance(rows, list):
            raise ProtocolError("query_result_schema")
        for row in rows:
            if not isinstance(row, list):
                raise ProtocolError("query_row_schema")
            for value in row:
                scalar = value is None or type(value) in (str, int) or (type(value) is float and math.isfinite(value))
                blob = isinstance(value, list) and all(type(byte) is int and 0 <= byte <= 255 for byte in value)
                if not scalar and not blob:
                    raise ProtocolError("query_cell_schema")
    elif operation in {"exec", "close", "checkpoint", "compact"}:
        result = response.get("result")
        if not isinstance(result, dict) or result.get("ok") is not True:
            raise ProtocolError("mutation_result_schema")


def digest(path):
    with open(path, "rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def source_inventory():
    paths = [ROOT / "prototypes/v1/Cargo.toml", ROOT / "prototypes/v1/Cargo.lock",
             ROOT / "prototypes/v1/deploy/verify-image.py", ROOT / "prototypes/v1/deploy/Dockerfile"]
    for directory in ["prototypes/v1/src", "prototypes/v1/platform", "prototypes/v1/controller", "scripts/perf-reliability"]:
        paths.extend(path for path in (ROOT / directory).rglob("*")
                     if path.is_file() and not {"node_modules", "target", "__pycache__"}.intersection(path.parts)
                     and path.suffix in {".rs", ".toml", ".py", ".go", ".mod", ".sum", ".ts", ".tsx", ".mjs"})
    turso = ROOT / "prototypes/p0/turso"
    tracked = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=turso)
    suffixes = {".rs", ".toml", ".lock", ".c", ".h", ".y", ".in", ".py", ".sh", ".json"}
    paths.extend(turso / os.fsdecode(name) for name in tracked.split(b"\0") if name
                 and (turso / os.fsdecode(name)).is_file() and Path(os.fsdecode(name)).suffix in suffixes)
    return {str(path.relative_to(ROOT)): digest(path) for path in sorted(set(paths))}


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def summary(records, seconds):
    latencies = [item["latency_ms"] for item in records]
    success = [item["latency_ms"] for item in records if item["outcome"] == "ACK"]
    return {"attempts": len(records), "wall_seconds": seconds,
            "attempts_per_second": len(records) / seconds if seconds else None,
            "ack_per_second": len(success) / seconds if seconds else None,
            "ACK": len(success), "Unknown": sum(item["outcome"] == "Unknown" for item in records),
            "errors": sum(item["outcome"] == "Error" for item in records),
            "all_latency_ms": {name: percentile(latencies, value) for name, value in
                               [("p50", .5), ("p95", .95), ("p99", .99)]},
            "ack_latency_ms": {name: percentile(success, value) for name, value in
                               [("p50", .5), ("p95", .95), ("p99", .99)]}}


class Worker:
    def __init__(self, binary, store, cache, timeout, deadline):
        self.timeout = timeout
        self.deadline = deadline
        self.buffer = b""
        self.sequence = 0
        self.usage = None
        self.process = subprocess.Popen([str(binary), "worker", str(store), str(cache)],
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.DEVNULL, start_new_session=True)
        self.group = self.process.pid
        os.set_blocking(self.process.stdin.fileno(), False)
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)

    def send(self, operation, **arguments):
        self.sequence += 1
        request = {"id": self.sequence, "op": operation, **arguments}
        self.last_operation = operation
        payload = json.dumps(request).encode() + b"\n"
        if len(payload) > 1024 * 1024:
            raise ValueError("bounded_request_size")
        limit = min(self.deadline, time.monotonic() + self.timeout)
        self.request_deadline = limit
        with selectors.DefaultSelector() as writable:
            writable.register(self.process.stdin, selectors.EVENT_WRITE)
            offset = 0
            while offset < len(payload):
                remaining = limit - time.monotonic()
                if remaining <= 0 or not writable.select(remaining):
                    raise TimeoutError("bounded_send_timeout")
                try:
                    offset += os.write(self.process.stdin.fileno(), payload[offset:])
                except BlockingIOError:
                    continue

    def receive(self):
        limit = min(self.deadline, self.request_deadline)
        while b"\n" not in self.buffer:
            remaining = limit - time.monotonic()
            if remaining <= 0 or not self.selector.select(remaining):
                raise TimeoutError("bounded_response_timeout")
            chunk = os.read(self.process.stdout.fileno(), 65536)
            if not chunk:
                raise EOFError("worker_eof")
            self.buffer += chunk
            if len(self.buffer) > 1024 * 1024:
                raise ValueError("bounded_response_size")
        line, self.buffer = self.buffer.split(b"\n", 1)
        response = json.loads(line)
        validate_response(response, self.sequence, self.last_operation)
        return response

    def request(self, operation, **arguments):
        started = time.monotonic()
        try:
            self.send(operation, **arguments)
            response = self.receive()
            error = response.get("error", "")
            outcome = "ACK" if response.get("ok") is True else (
                "Unknown" if operation in {"exec", "checkpoint", "compact"} or "unknown" in error.lower() else "Error")
            category = next((tag for tag in ["busy", "conflict", "timeout", "epoch", "lock", "unknown"]
                             if tag in error.lower()), None)
            record = {"operation": operation, "outcome": outcome, "error_category": category,
                      "negative_response": response.get("ok") is not True,
                      "error_sha256": hashlib.sha256(error.encode()).hexdigest() if error else None}
        except (TimeoutError, EOFError, BrokenPipeError, OSError, ValueError) as failure:
            response = None
            record = {"operation": operation, "outcome": "Unknown", "error_category": type(failure).__name__}
        record["latency_ms"] = (time.monotonic() - started) * 1000
        return response, record

    def require(self, operation, **arguments):
        response, record = self.request(operation, **arguments)
        if record["outcome"] != "ACK":
            raise RuntimeError("precondition_" + operation + "_" + record["outcome"])
        return response

    def stop(self, crash=False):
        if self.usage is not None:
            return self.usage
        if crash:
            try:
                os.killpg(self.group, signal.SIGKILL)
            except ProcessLookupError:
                pass
        try:
            self.process.stdin.close()
        except BrokenPipeError:
            pass
        limit = time.monotonic() + 2
        while True:
            waited, status, usage = os.wait4(self.process.pid, os.WNOHANG)
            if waited:
                self.process.returncode = os.waitstatus_to_exitcode(status)
                self.usage = {"pid": waited, "exit_code": self.process.returncode,
                              "user_cpu_seconds": usage.ru_utime, "system_cpu_seconds": usage.ru_stime,
                              "peak_rss_bytes": usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024)}
                break
            if time.monotonic() >= limit:
                try:
                    os.killpg(self.group, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            time.sleep(.01)
        try:
            os.killpg(self.group, signal.SIGKILL)
        except ProcessLookupError:
            pass
        limit = time.monotonic() + 2
        while True:
            try:
                members = process_group_members(self.group)
            except Exception as failure:
                self.usage["group_proof_failure"] = type(failure).__name__
                members = [{"pid": self.group, "state": "UNPROVEN"}]
            live = [member for member in members if not member["state"].startswith("Z")]
            if not live or time.monotonic() >= limit:
                break
            time.sleep(.02)
        self.usage["group_members_at_cleanup"] = members
        self.usage["group_live_remaining"] = live
        self.usage["group_joined"] = not live
        self.selector.close()
        self.process.stdout.close()
        return self.usage


def process_group_members(group):
    output = subprocess.check_output(["ps", "-A", "-o", "pid=,pgid=,stat="], text=True, timeout=2)
    return [{"pid": int(fields[0]), "state": fields[2]} for line in output.splitlines()
            if len(fields := line.split()) == 3 and int(fields[1]) == group]


def stop_workers(workers):
    resources = []
    for worker in workers:
        try:
            resources.append(worker.stop())
        except Exception as failure:
            resources.append({"pid": worker.process.pid, "group_joined": False,
                              "cleanup_failure": type(failure).__name__})
    return resources


def payload_matches(actual, expected, uncertain=None):
    uncertain = uncertain or {}
    return (isinstance(actual, list) and len(actual) == len(expected)
            and all(isinstance(row, list) and len(row) == 2 and type(row[0]) is int and row[0] == index
                    and row[1] in uncertain.get(index, [expected[index]])
                    for index, row in enumerate(actual)))


def final_status(result):
    reasons = []
    modes = result.get("benchmarks", [])
    if [mode["mode"] for mode in modes] != ["read", "write", "mixed"]:
        reasons.append("mode_count_or_order")
    for mode in modes:
        if mode.get("setup_failure") or not mode.get("completed_budget"):
            reasons.append(mode["mode"] + "_incomplete")
        if mode.get("Unknown", 0) or mode.get("errors", 0):
            reasons.append(mode["mode"] + "_non_ACK")
        if mode.get("ACK") != result["load"]["operations"] * result["load"]["concurrency"]:
            reasons.append(mode["mode"] + "_ACK_count")
        if len(mode.get("oracles", [])) != result["load"]["concurrency"] or not all(oracle["pass"] for oracle in mode.get("oracles", [])):
            reasons.append(mode["mode"] + "_oracle")
        if len(mode.get("resources", [])) < result["load"]["concurrency"] or not all(resource.get("group_joined") for resource in mode.get("resources", [])):
            reasons.append(mode["mode"] + "_cleanup")
        if not any(type(sample.get("combined_rss_bytes")) is int and sample["combined_rss_bytes"] > 0
                   for sample in mode.get("rss_samples", [])):
            reasons.append(mode["mode"] + "_rss_unmeasured")
    recovery = result.get("reliability", {})
    if recovery.get("failure") or len(recovery.get("checks", [])) != 3 or not all(check["pass"] for check in recovery.get("checks", [])):
        reasons.append("reliability_missing_or_failed")
    if len(recovery.get("resources", [])) != 4 or not all(resource.get("group_joined") for resource in recovery.get("resources", [])):
        reasons.append("reliability_cleanup")
    if not result.get("source_unchanged") or not result.get("frozen_binary_unchanged"):
        reasons.append("freeze_changed")
    if "build_provenance" in result and not all(result["build_provenance"].get(key) for key in ["binary_matches", "sources_match"]):
        reasons.append("build_provenance_mismatch")
    return {"pass": not reasons, "status": "PASS" if not reasons else "FAIL_OR_NOTRUN", "reasons": reasons}


def db_id(name):
    return {"namespace": {"tenant": "perf_" + name, "database": "db"}, "branch": "main"}


def opened(worker, identifier):
    return worker.require("open", database=identifier, epoch=0)["session"]


def collect_rss(workers, samples, done):
    while not done.wait(.1):
        try:
            result = subprocess.run(["ps", "-o", "rss=", "-p", ",".join(str(worker.process.pid) for worker in workers)],
                                    capture_output=True, text=True, timeout=1)
            values = [int(value) for value in result.stdout.split()] if result.returncode == 0 else []
            samples.append({"monotonic": time.monotonic(),
                            "combined_rss_bytes": sum(values) * 1024 if len(values) == len(workers) and all(value > 0 for value in values) else None})
        except (OSError, ValueError, subprocess.TimeoutExpired):
            samples.append({"monotonic": time.monotonic(), "combined_rss_bytes": None})


def benchmark(binary, store, run, args, mode, name, deadline):
    workers, sessions, records, rss = [], [], [], []
    done = threading.Event()
    sampler = None
    result = {"mode": mode, "concurrency": args.concurrency, "sharding": "one independent DB per worker",
              "rows_per_db": args.rows, "payload_bytes": args.payload_bytes, "records": records}
    try:
        for lane in range(args.concurrency):
            worker = Worker(binary, store, run / (name + str(lane)), args.timeout, deadline)
            workers.append(worker)
            session = opened(worker, db_id(name + str(lane)))
            sessions.append(session)
            worker.require("exec", session=session, sql="CREATE TABLE bench(id INTEGER PRIMARY KEY, payload TEXT)")
            values = ",".join(f"({index},'{('x' * args.payload_bytes)}')" for index in range(args.rows))
            worker.require("exec", session=session, sql="INSERT INTO bench VALUES " + values)
            worker.require("checkpoint", session=session)
            initial = worker.require("query", session=session, sql="SELECT id,payload FROM bench ORDER BY id")["result"]
            if not payload_matches(initial, {index: "x" * args.payload_bytes for index in range(args.rows)}):
                raise RuntimeError("initial_payload_oracle")
        sampler = threading.Thread(target=collect_rss, args=(workers, rss, done))
        sampler.start()
        barrier = threading.Barrier(args.concurrency)

        def lane_work(lane):
            lane_records = []
            expected_payloads = {index: "x" * args.payload_bytes for index in range(args.rows)}
            barrier.wait(timeout=args.timeout)
            for index in range(args.operations):
                if time.monotonic() >= deadline:
                    break
                write = mode == "write" or (mode == "mixed" and index % 2 == 1)
                sql = (f"UPDATE bench SET payload='{('y' if (index // args.rows) % 2 == 0 else 'z') * args.payload_bytes}' WHERE id={index % args.rows}"
                       if write else f"SELECT payload FROM bench WHERE id={index % args.rows}")
                response, record = workers[lane].request("exec" if write else "query", session=sessions[lane], sql=sql)
                record.update(lane=lane, index=index, kind="write" if write else "read")
                if write and record["outcome"] == "ACK":
                    expected_payloads[index % args.rows] = ("y" if (index // args.rows) % 2 == 0 else "z") * args.payload_bytes
                if not write and record["outcome"] == "ACK" and response.get("result") != [[expected_payloads[index % args.rows]]]:
                    record.update(outcome="Error", error_category="read_oracle_mismatch")
                lane_records.append(record)
                if record["outcome"] == "Unknown":
                    break
            return lane_records

        started = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            for lane_records in pool.map(lane_work, range(args.concurrency)):
                records.extend(lane_records)
        elapsed = time.monotonic() - started
        done.set()
        sampler.join()
        result.update(summary(records, elapsed))
        result["completed_budget"] = len(records) == args.operations * args.concurrency
        result["oracles"] = []
        for lane in range(args.concurrency):
            expected = {index: "x" * args.payload_bytes for index in range(args.rows)}
            uncertain = {}
            lane_records = [record for record in records if record["lane"] == lane]
            for record in lane_records:
                if record["kind"] == "write":
                    index = record["index"]
                    row = index % args.rows
                    payload = ("y" if (index // args.rows) % 2 == 0 else "z") * args.payload_bytes
                    if record["outcome"] == "ACK":
                        expected[row] = payload
                    elif record["outcome"] == "Unknown":
                        uncertain[row] = [expected[row], payload]
            if any(record["outcome"] == "Unknown" for record in lane_records):
                workers[lane].stop(crash=True)
                oracle_worker = Worker(binary, store, run / (name + str(lane) + "oracle"), args.timeout, deadline)
                workers.append(oracle_worker)
                oracle_session = opened(oracle_worker, db_id(name + str(lane)))
            else:
                oracle_worker, oracle_session = workers[lane], sessions[lane]
            response, observation = oracle_worker.request("query", session=oracle_session,
                                                          sql="SELECT id,payload FROM bench ORDER BY id")
            actual = response.get("result") if response and observation["outcome"] == "ACK" else None
            valid = payload_matches(actual, expected, uncertain)
            result["oracles"].append({"lane": lane, "pass": valid, "expected_rowcount": args.rows,
                                      "actual_rows": actual, "expected_payloads": expected,
                                      "unknown_allowed_payloads": uncertain, "observation": observation})
    except (RuntimeError, OSError, ValueError) as failure:
        result["setup_failure"] = str(failure) if isinstance(failure, RuntimeError) else type(failure).__name__
    finally:
        done.set()
        if sampler:
            sampler.join(timeout=2)
            if sampler.is_alive():
                sampler.join()
        result["resources"] = stop_workers(workers)
        result["rss_samples"] = rss
    result["cpu_scope"] = "whole worker lifetime including setup; not phase-isolated"
    return result


def reliability(binary, store, run, args, name, deadline):
    identifier = db_id(name)
    records, checks, workers = [], [], []
    result = {"records": records, "checks": checks}

    def new(cache):
        started = time.monotonic()
        worker = Worker(binary, store, run / cache, args.timeout, deadline)
        workers.append(worker)
        session = opened(worker, identifier)
        records.append({"stage": cache, "spawn_to_open_ms": (time.monotonic() - started) * 1000})
        return worker, session

    try:
        worker, session = new(name + "cold")
        worker.require("exec", session=session, sql="CREATE TABLE durable(id INTEGER PRIMARY KEY, payload TEXT)")
        worker.require("exec", session=session, sql="INSERT INTO durable VALUES(1,'acked_without_checkpoint')")
        warm, record = worker.request("open", database=identifier, epoch=0)
        record["stage"] = "warm_open_loaded_db"
        records.append(record)
        if warm and warm.get("ok"):
            worker.require("close", session=warm["session"])
        worker.require("exec", session=session, sql="BEGIN IMMEDIATE")
        worker.require("exec", session=session, sql="INSERT INTO durable VALUES(2,'uncommitted')")
        worker.stop(crash=True)
        worker, session = new(name + "cold")
        recovered = worker.require("query", session=session, sql="SELECT id,payload FROM durable ORDER BY id")["result"]
        checks.append({"stage": "same_cache_restart", "actual": recovered,
                       "expected": [[1, "acked_without_checkpoint"]], "pass": recovered == [[1, "acked_without_checkpoint"]]})
        worker.stop(crash=True)
        worker, session = new(name + "fresh")
        remote = worker.require("query", session=session, sql="SELECT id,payload FROM durable ORDER BY id")["result"]
        checks.append({"stage": "fresh_cache_remote_durability", "actual": remote,
                       "expected": [[1, "acked_without_checkpoint"]], "pass": remote == [[1, "acked_without_checkpoint"]]})
        worker.send("exec", session=session, sql="INSERT INTO durable VALUES(3,'inflight_unknown')")
        worker.stop(crash=True)
        records.append({"stage": "kill_after_send_before_receive", "outcome": "Unknown", "retried": False})
        worker, session = new(name + "unknown")
        observed = worker.require("query", session=session, sql="SELECT id,payload FROM durable ORDER BY id")["result"]
        checks.append({"stage": "unknown_reconcile", "actual": observed,
                       "pass": observed in ([[1, "acked_without_checkpoint"]], [[1, "acked_without_checkpoint"], [3, "inflight_unknown"]]),
                       "acked_present": [1, "acked_without_checkpoint"] in observed,
                       "uncommitted_absent": all(row[0] != 2 for row in observed),
                       "unknown_present": any(row[0] == 3 for row in observed)})
    except (RuntimeError, OSError, ValueError) as failure:
        result["failure"] = str(failure) if isinstance(failure, RuntimeError) else type(failure).__name__
    finally:
        result["resources"] = stop_workers(workers)
    return result


def main():
    signal.signal(signal.SIGTERM, lambda signum, frame: (_ for _ in ()).throw(KeyboardInterrupt()))
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, default=ROOT / "prototypes/v1/target/debug/peri-loom-v1")
    parser.add_argument("--backend", choices=["local", "s3"], default="local")
    parser.add_argument("--build-provenance", type=Path)
    parser.add_argument("--operations", type=int, default=100)
    parser.add_argument("--rows", type=int, default=64)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--max-seconds", type=float, default=120)
    parser.add_argument("--round", choices=["smoke", "baseline", "repeat"], default="smoke")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    for value, minimum, maximum in [(args.operations, 1, 1000), (args.rows, 1, 256),
                                    (args.payload_bytes, 1, 1024), (args.concurrency, 1, 4),
                                    (args.timeout, .1, 20), (args.max_seconds, 1, 300)]:
        if not minimum <= value <= maximum:
            parser.error("resource bounds exceeded")
    if args.output.exists():
        parser.error("output exists; evidence must not be overwritten")
    if args.backend == "s3":
        for key in ["S3_ENDPOINT", "S3_BUCKET", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY"]:
            if not os.environ.get(key):
                parser.error("missing private environment: " + key)
    os.umask(0o077)
    run = Path(tempfile.mkdtemp(prefix="peri-perf-"))
    binary = run / "native"
    before = source_inventory()
    original_hash = digest(args.binary)
    shutil.copy2(args.binary, binary)
    binary.chmod(0o500)
    if digest(binary) != original_hash:
        raise RuntimeError("binary_changed_during_copy")
    result = {"schema": 1, "round": args.round, "backend": args.backend,
              "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "host": platform.platform(), "python": platform.python_version(), "run_directory": str(run),
              "binary_origin": str(args.binary.resolve()), "binary_sha256": original_hash,
              "binary_mtime_ns": args.binary.stat().st_mtime_ns,
              "source_inventory": before, "compiled_source_identity": "unattested: separate source snapshot and binary digest",
              "load": vars(args) | {"binary": str(args.binary), "output": str(args.output),
                                     "build_provenance": str(args.build_provenance) if args.build_provenance else None},
              "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "turso_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT / "prototypes/p0/turso", text=True).strip(),
              "limitations": ["stdio native worker, not HTTP/SDK/gateway serverless deployment",
                               "closed-loop finite-count clients; no coordinated-omission correction",
                               "one database per lane; no same-database concurrent-write claim",
                               "shared host/external object store; round labels do not certify isolation",
                               "CPU whole lifetime, RSS sampled aggregate/individual wait4 high-water"]}
    if args.build_provenance:
        provenance = json.loads(args.build_provenance.read_text())
        selected_sources = {name: value for name, value in provenance["source_before"].items() if not name.startswith("scripts/perf-reliability/")}
        actual_sources = {name: value for name, value in before.items() if not name.startswith("scripts/perf-reliability/")}
        result["build_provenance"] = {"path": str(args.build_provenance), "sha256": digest(args.build_provenance),
                                      "binary_matches": provenance.get("pass") is True and provenance.get("frozen_binary_sha256") == original_hash,
                                      "sources_match": selected_sources == actual_sources,
                                      "kind": provenance.get("build_kind"), "cargo_artifact_fresh": provenance.get("cargo_artifact", {}).get("fresh")}
        if result["build_provenance"]["binary_matches"] and result["build_provenance"]["sources_match"]:
            result["compiled_source_identity"] = "offline cached build selected with source/artifact evidence; not clean-build attestation"
    started = time.monotonic()
    deadline = started + args.max_seconds
    store = "s3" if args.backend == "s3" else run / "objects"
    name = uuid.uuid4().hex[:16]
    result["namespace_prefix"] = "perf_" + name
    result["benchmarks"] = []
    try:
        for mode in ["read", "write", "mixed"]:
            if time.monotonic() >= deadline:
                break
            result["benchmarks"].append(benchmark(binary, store, run, args, mode, name + mode, deadline))
        if time.monotonic() < deadline:
            result["reliability"] = reliability(binary, store, run, args, name + "reliability", deadline)
    finally:
        result["wall_seconds"] = time.monotonic() - started
        result["source_unchanged"] = source_inventory() == before
        result["frozen_binary_unchanged"] = digest(binary) == original_hash
        result.update(final_status(result))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x") as output:
            json.dump(result, output, indent=2)
            output.write("\n")
    print(json.dumps({"output": str(args.output), "wall_seconds": result["wall_seconds"],
                      "round": args.round, "source_unchanged": result["source_unchanged"], "status": result["status"]}))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
