#!/usr/bin/env python3
"""F1-shaped read driver for R71 Gate 2.

One request is a Hrana JSON pipeline `[execute SELECT 1 want_rows, close]` with
no baton: the implicit open fuses the statement into `open_session`, and the
close entry retires the session it just opened. That is the Issue 28
`F1.select-1` json-pipeline shape and the same request Gate 1b timed.

The driver runs one warm-up block, snapshots `/v1/metrics`, runs the timed
blocks over one keep-alive connection, waits for the deferred close account to
drain, then snapshots metrics again. Only the timed window feeds the Gate 2
counters. Default admission quotas are not relaxed: one pipeline request
spends three tokens (open, statement, close) against a per-project bucket with
a 500 burst at 500/s, so the driver pauses only BETWEEN blocks to refill; the
requests inside a block stay consecutive.

Every request is recorded in raw.jsonl with its wall clock, HTTP status and
response bytes. A failed request is counted, never dropped. The connection file
is read for the endpoint and keys and is never printed.
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
    parser.add_argument("--blocks", type=int, default=5)
    parser.add_argument("--block-size", type=int, default=100)
    parser.add_argument("--warmup-blocks", type=int, default=1)
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

    records: list[dict] = []
    blocks: list[dict] = []
    timed_failures = 0
    warmup_failures = 0
    first_failure = None

    def run_block(index: int, warmup: bool) -> dict:
        nonlocal timed_failures, warmup_failures, first_failure
        started = time.perf_counter_ns()
        durations: list[float] = []
        sizes = 0
        block_failures = 0
        for offset in range(arguments.block_size):
            record = run_request(client)
            record.update({"leg": arguments.leg, "block": index,
                           "index": offset, "warmup": warmup})
            records.append(record)
            durations.append(record["duration_ns"] / 1e6)
            sizes += record["bytes"]
            if not record["ok"]:
                if warmup:
                    warmup_failures += 1
                else:
                    timed_failures += 1
                block_failures += 1
                if first_failure is None:
                    first_failure = {"block": index, "index": offset,
                                     "error": record["error"],
                                     "status": record["status"]}
        wall_ns = time.perf_counter_ns() - started
        summary = {
            "block": index,
            "warmup": warmup,
            "requests": arguments.block_size,
            "failures": block_failures,
            "wall_ms": wall_ns / 1e6,
            "median_ms": sorted(durations)[len(durations) // 2],
            "wire_bytes": sizes,
        }
        blocks.append(summary)
        return summary

    for index in range(1, arguments.warmup_blocks + 1):
        summary = run_block(-index, True)
        print("warmup block %d median=%.3fms failures=%d" % (
            index, summary["median_ms"], summary["failures"]), file=sys.stderr)

    time.sleep(arguments.initial_refill_seconds)
    before = gate2_common.metrics_snapshot(target)

    for index in range(1, arguments.blocks + 1):
        summary = run_block(index, False)
        print("block %d median=%.3fms failures=%d" % (
            index, summary["median_ms"], summary["failures"]), file=sys.stderr)
        if index != arguments.blocks:
            time.sleep(arguments.block_gap_seconds)

    # Let the account finish the closes it owns before the closing snapshot:
    # pending is the only completion signal the account publishes.
    settle_seconds = arguments.settle_seconds
    for _ in range(arguments.settle_attempts):
        time.sleep(settle_seconds)
        after = gate2_common.metrics_snapshot(target)
        deferred = after.get("deferred_session_close")
        if deferred is None or int(deferred.get("pending") or 0) == 0:
            break
    else:
        after = gate2_common.metrics_snapshot(target)

    timed = [record for record in records if not record["warmup"]]
    durations = [record["duration_ns"] / 1e6 for record in timed]
    percentages = gate2_common.percentiles(durations)
    summary = {
        "leg": arguments.leg,
        "label": arguments.label,
        "payload": "f1-select-1-json-pipeline-close",
        "blocks": arguments.blocks,
        "block_size": arguments.block_size,
        "warmup_blocks": arguments.warmup_blocks,
        "requests": len(timed),
        "ok": len(timed) - timed_failures,
        "failures": timed_failures,
        "warmup_failures": warmup_failures,
        "first_failure": first_failure,
        "request_ms": percentages,
        "wire_bytes_total": sum(record["bytes"] for record in timed),
        "wire_bytes_per_request": sum(record["bytes"] for record in timed)
        / len(timed) if timed else None,
        "block_summaries": [block for block in blocks if not block["warmup"]],
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
    return 0 if timed_failures == 0 and warmup_failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
