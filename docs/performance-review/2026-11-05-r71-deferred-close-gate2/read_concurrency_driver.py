#!/usr/bin/env python3
"""Optional c8 read driver for R71 Gate 2.

Same F1-shaped request as f1_driver.py, issued by eight concurrent workers over
their own keep-alive connections. This is a mechanism check, not a regression
gate: it exists to see whether the deferred account's per-database bound turns
into inline closes (expected, bound = 4) and whether nothing is refused. The
total request count is capped so the whole run fits in one default admission
burst (three tokens per request); the driver still records every 429/503.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import pathlib
import sys
import threading
import time

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[2] / "scripts" / "perf-reliability"))

import gate2_common  # noqa: E402
from issue28.hrana import (  # noqa: E402
    FatalTransportError,
    HranaClient,
    HranaError,
    Target,
    parse_pipeline_json,
)

REQUESTS = [
    {"type": "execute", "sql": "SELECT 1", "want_rows": True},
    {"type": "close"},
]


def run_request(client: HranaClient) -> dict:
    started = time.perf_counter_ns()
    record = {"duration_ns": 0, "status": 0, "bytes": 0, "ok": False, "error": ""}
    try:
        raw = client.pipeline_json(REQUESTS)
    except (FatalTransportError, HranaError, OSError) as error:
        record["duration_ns"] = time.perf_counter_ns() - started
        record["error"] = type(error).__name__
        return record
    record["duration_ns"] = time.perf_counter_ns() - started
    record["status"] = raw.status
    record["bytes"] = len(raw.body)
    if raw.status != 200:
        record["error"] = "http_%d" % raw.status
        return record
    try:
        reply = parse_pipeline_json(raw.body)
    except HranaError:
        record["error"] = "decode"
        return record
    rejected = [item for item in reply.results if not item.ok]
    if rejected:
        record["error"] = rejected[0].error_code or "rejected"
        return record
    executes = [item for item in reply.results
                if item.ok and item.response_type == "execute"]
    if len(executes) != 1 or executes[0].rows != 1:
        record["error"] = "row_count"
        return record
    record["ok"] = True
    return record


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection-file", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--leg", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--requests", type=int, default=160)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--initial-refill-seconds", type=float, default=1.2)
    parser.add_argument("--settle-seconds", type=float, default=2.0)
    parser.add_argument("--settle-attempts", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=120.0)
    arguments = parser.parse_args()

    out = pathlib.Path(arguments.out)
    out.mkdir(parents=True, exist_ok=True)
    target = Target.from_file(arguments.connection_file)
    local = threading.local()

    def client() -> HranaClient:
        if not hasattr(local, "client"):
            local.client = HranaClient(target, timeout=arguments.timeout)
        return local.client

    def one(index: int) -> dict:
        record = run_request(client())
        record.update({"leg": arguments.leg, "index": index})
        return record

    def run(count: int) -> list[dict]:
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=arguments.concurrency) as pool:
            return list(pool.map(one, range(count)))

    run(arguments.concurrency)  # warm-up connections and servers
    time.sleep(arguments.initial_refill_seconds)
    before = gate2_common.metrics_snapshot(target)
    started = time.perf_counter_ns()
    records = run(arguments.requests)
    wall_ns = time.perf_counter_ns() - started
    for _ in range(arguments.settle_attempts):
        time.sleep(arguments.settle_seconds)
        after = gate2_common.metrics_snapshot(target)
        deferred = after.get("deferred_session_close")
        if deferred is None or int(deferred.get("pending") or 0) == 0:
            break

    failures = [record for record in records if not record["ok"]]
    durations = [record["duration_ns"] / 1e6 for record in records]
    summary = {
        "leg": arguments.leg,
        "label": arguments.label,
        "payload": "f1-select-1-json-pipeline-close-c8",
        "concurrency": arguments.concurrency,
        "requests": len(records),
        "ok": len(records) - len(failures),
        "failures": len(failures),
        "first_failure": failures[0] if failures else None,
        "wall_ms": wall_ns / 1e6,
        "request_ms": gate2_common.percentiles(durations),
        "wire_bytes_total": sum(record["bytes"] for record in records),
        "window": gate2_common.window_summary(before, after),
        "window_files": {"before": "window-before.json",
                         "after": "window-after.json"},
    }
    with open(out / "raw.jsonl", "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
    gate2_common.write_json(out / "window-before.json", before)
    gate2_common.write_json(out / "window-after.json", after)
    gate2_common.write_json(out / "summary.json", summary)
    print(json.dumps(summary["window"], indent=2, sort_keys=True))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
