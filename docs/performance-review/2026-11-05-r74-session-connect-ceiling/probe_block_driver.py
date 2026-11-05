#!/usr/bin/env python3
"""R74 Stage B block driver.

One block = 100 consecutive F1.select-1 json-pipeline requests. The block value
is the wall clock of the whole block divided by 100. The connection file is
private and is never printed.

The client is the same Hrana client the Issue 28 harness uses, so the request
shape is identical (execute SELECT 1 + close in one v3 pipeline).
"""
from __future__ import annotations

import argparse
import json
import pathlib
import statistics
import sys
import time

REPO = pathlib.Path("/Users/kk/code/peri-loom")
sys.path.insert(0, str(REPO / "scripts" / "perf-reliability" / "issue28"))

from hrana import HranaClient, Target, parse_pipeline_json  # noqa: E402

REQUEST = [
    {"type": "execute", "sql": "SELECT 1", "want_rows": True, "args": ()},
    {"type": "close"},
]


def one(client: HranaClient) -> str:
    raw = client.pipeline_json(REQUEST)
    if raw.status != 200:
        return "status=%d" % raw.status
    reply = parse_pipeline_json(raw.body)
    if len(reply.results) < 2:
        return "results=%d" % len(reply.results)
    if not reply.results[0].ok:
        return "error=%s:%s" % (reply.results[0].error_code, reply.results[0].error_message)
    if reply.results[0].rows != 1:
        return "rows=%d" % reply.results[0].rows
    if reply.results[1].response_type != "close":
        return "second=%s" % reply.results[1].response_type
    return ""


def block(client: HranaClient, size: int) -> tuple[int, list[int], list[str]]:
    requests: list[int] = []
    failures: list[str] = []
    started = time.perf_counter_ns()
    for _ in range(size):
        request_started = time.perf_counter_ns()
        error = one(client)
        requests.append(time.perf_counter_ns() - request_started)
        if error:
            failures.append(error)
            if len(failures) >= 20:
                break
    elapsed = time.perf_counter_ns() - started
    return elapsed, requests, failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection-file", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--blocks", type=int, default=30)
    parser.add_argument("--block-size", type=int, default=100)
    parser.add_argument("--warmup-blocks", type=int, default=1)
    parser.add_argument("--gap-ms", type=float, default=50.0)
    arguments = parser.parse_args()

    target = Target.from_file(arguments.connection_file)
    client = HranaClient(target)
    failures: list[str] = []
    warmup: list[int] = []
    blocks: list[int] = []
    requests: list[int] = []
    try:
        for _ in range(arguments.warmup_blocks):
            elapsed, _, block_failures = block(client, arguments.block_size)
            warmup.append(elapsed)
            failures.extend(block_failures)
            time.sleep(arguments.gap_ms / 1000.0)
        for _ in range(arguments.blocks):
            elapsed, block_requests, block_failures = block(client, arguments.block_size)
            blocks.append(elapsed)
            requests.extend(block_requests)
            failures.extend(block_failures)
            if block_failures:
                break
            time.sleep(arguments.gap_ms / 1000.0)
    finally:
        client.close()

    block_average_ns = [value // arguments.block_size for value in blocks]
    document = {
        "label": arguments.label,
        "blocks_planned": arguments.blocks,
        "block_size": arguments.block_size,
        "warmup_block_average_ns": [value // arguments.block_size for value in warmup],
        "block_average_ns": block_average_ns,
        "request_ns": requests,
        "failures": failures,
        "block_median_ns": int(statistics.median(block_average_ns)) if block_average_ns else None,
        "request_p50_ns": int(statistics.median(requests)) if requests else None,
        "request_p95_ns": sorted(requests)[int(0.95 * (len(requests) - 1))] if requests else None,
    }
    pathlib.Path(arguments.out).write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: document[key] for key in (
        "label", "blocks_planned", "block_size", "block_median_ns",
        "request_p50_ns", "request_p95_ns")}) + " failures=%d" % len(failures))
    return 0 if not failures and len(blocks) == arguments.blocks else 1


if __name__ == "__main__":
    raise SystemExit(main())
