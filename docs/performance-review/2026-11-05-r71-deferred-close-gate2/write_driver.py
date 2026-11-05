#!/usr/bin/env python3
"""W1-insert-shaped write driver for R71 Gate 2.

One request is a Hrana JSON pipeline `[execute INSERT, close]` with no baton:
the implicit open fuses the write statement, and the close entry retires the
session. Every session in this leg therefore ran an `execute` that may commit,
which is exactly the case the deferred close must keep synchronous.

The table is created once before the window and dropped after it; both are
ordinary write sessions and sit outside the timed window. Each timed insert
must report `affected_row_count == 1`. The driver snapshots `/v1/metrics`
around the timed blocks and waits for the deferred account to drain before the
closing snapshot, mirroring the read driver. The connection file is never
printed.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
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


def pipeline_insert(client: HranaClient, sql: str, want_rows: bool) -> dict:
    started = time.perf_counter_ns()
    record = {"duration_ns": 0, "status": 0, "bytes": 0, "ok": False,
              "error": "", "affected": 0}
    requests = [
        {"type": "execute", "sql": sql, "want_rows": want_rows},
        {"type": "close"},
    ]
    try:
        raw = client.pipeline_json(requests)
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
    if len(executes) != 1:
        record["error"] = "execute_count"
        return record
    record["affected"] = executes[0].affected_row_count
    if want_rows:
        record["ok"] = executes[0].rows == 1
        if not record["ok"]:
            record["error"] = "row_count"
        return record
    record["ok"] = executes[0].affected_row_count == 1
    if not record["ok"]:
        record["error"] = "affected_row_count"
    return record


def untimed(client: HranaClient, sql: str) -> None:
    raw = client.pipeline_json([
        {"type": "execute", "sql": sql, "want_rows": False},
        {"type": "close"},
    ])
    if raw.status != 200:
        raise SystemExit("setup HTTP %d" % raw.status)
    reply = parse_pipeline_json(raw.body)
    failure = next((item for item in reply.results if not item.ok), None)
    if failure is not None:
        raise SystemExit("setup %s" % failure.error_code)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection-file", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--leg", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--blocks", type=int, default=5)
    parser.add_argument("--block-size", type=int, default=20)
    parser.add_argument("--block-gap-seconds", type=float, default=0.75)
    parser.add_argument("--initial-refill-seconds", type=float, default=1.2)
    parser.add_argument("--settle-seconds", type=float, default=2.0)
    parser.add_argument("--settle-attempts", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=120.0)
    arguments = parser.parse_args()

    out = pathlib.Path(arguments.out)
    out.mkdir(parents=True, exist_ok=True)
    target = Target.from_file(arguments.connection_file)
    client = HranaClient(target, timeout=arguments.timeout)
    table = '"issue28_g2_write_%s"' % arguments.leg.replace("-", "_")

    untimed(client, "DROP TABLE IF EXISTS %s" % table)
    untimed(client, "CREATE TABLE %s (id INTEGER PRIMARY KEY, payload TEXT)" % table)
    time.sleep(arguments.initial_refill_seconds)
    before = gate2_common.metrics_snapshot(target)

    records: list[dict] = []
    blocks: list[dict] = []
    failures = 0
    first_failure = None
    sql = "INSERT INTO %s (payload) VALUES ('issue28')" % table
    for index in range(1, arguments.blocks + 1):
        started = time.perf_counter_ns()
        durations: list[float] = []
        sizes = 0
        block_failures = 0
        for offset in range(arguments.block_size):
            record = pipeline_insert(client, sql, want_rows=False)
            record.update({"leg": arguments.leg, "block": index, "index": offset})
            records.append(record)
            durations.append(record["duration_ns"] / 1e6)
            sizes += record["bytes"]
            if not record["ok"]:
                failures += 1
                block_failures += 1
                if first_failure is None:
                    first_failure = {"block": index, "index": offset,
                                     "error": record["error"],
                                     "status": record["status"]}
        wall_ns = time.perf_counter_ns() - started
        blocks.append({
            "block": index,
            "requests": arguments.block_size,
            "failures": block_failures,
            "wall_ms": wall_ns / 1e6,
            "median_ms": sorted(durations)[len(durations) // 2],
            "wire_bytes": sizes,
        })
        print("block %d median=%.3fms failures=%d" % (
            index, blocks[-1]["median_ms"], block_failures), file=sys.stderr)
        if index != arguments.blocks:
            time.sleep(arguments.block_gap_seconds)

    for _ in range(arguments.settle_attempts):
        time.sleep(arguments.settle_seconds)
        after = gate2_common.metrics_snapshot(target)
        deferred = after.get("deferred_session_close")
        if deferred is None or int(deferred.get("pending") or 0) == 0:
            break

    untimed(client, "DROP TABLE IF EXISTS %s" % table)

    durations = [record["duration_ns"] / 1e6 for record in records]
    summary = {
        "leg": arguments.leg,
        "label": arguments.label,
        "payload": "w1-insert-json-pipeline-close",
        "table": table,
        "blocks": arguments.blocks,
        "block_size": arguments.block_size,
        "requests": len(records),
        "ok": len(records) - failures,
        "failures": failures,
        "first_failure": first_failure,
        "request_ms": gate2_common.percentiles(durations),
        "affected_rows_total": sum(record["affected"] for record in records),
        "wire_bytes_total": sum(record["bytes"] for record in records),
        "wire_bytes_per_request": sum(record["bytes"] for record in records)
        / len(records) if records else None,
        "block_summaries": blocks,
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
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
