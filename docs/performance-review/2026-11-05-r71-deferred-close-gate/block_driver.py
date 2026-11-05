#!/usr/bin/env python3
"""Block-mean driver for R71 Gate 1b.

One sample is the wall clock of 100 consecutive Hrana JSON pipeline requests,
each `[execute SELECT 1 want_rows, close]`, divided by 100. A leg runs one
warm-up block (discarded) plus the requested number of timed blocks over a
single keep-alive connection.

Default quotas are NOT relaxed. Each pipeline request spends three admission
tokens (implicit open, statement, close) against a per-project bucket of
`requests_per_second` with a burst of the same size (500 at 500/s), so 100
back-to-back requests spend 300 tokens. The driver therefore pauses only
BETWEEN blocks long enough for the bucket to refill; the 100 requests inside a
block stay consecutive and their wall clock is the sample. The pauses are
recorded and are never inside a timed block.

The driver writes one raw record per block as soon as the block finishes, so a
crash keeps the evidence of the blocks that already ran. Failed requests are
counted, never dropped; a leg with a failure is reported as failed and excluded
from the decision only by saying so in the analysis, not by hiding it.

Only durations, sizes and status codes are recorded. The connection file is
read for the endpoint and key and is never printed.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import statistics
import sys
import time

REQUESTS = [
    {"type": "execute", "sql": "SELECT 1", "want_rows": True},
    {"type": "close"},
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection-file", required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--out", required=True, help="leg evidence directory")
    parser.add_argument("--side", required=True, help="a or b")
    parser.add_argument("--leg", required=True)
    parser.add_argument("--round", required=True)
    parser.add_argument("--block-size", type=int, default=100)
    parser.add_argument("--blocks", type=int, default=30)
    parser.add_argument("--warmup-blocks", type=int, default=1)
    parser.add_argument("--block-gap-seconds", type=float, default=0.75,
                        help="refill the default admission bucket between blocks")
    parser.add_argument("--initial-refill-seconds", type=float, default=1.2,
                        help="refill after fixture seeding before the first block")
    parser.add_argument("--timeout", type=float, default=120.0)
    arguments = parser.parse_args()

    sys.path.insert(0, str(pathlib.Path(arguments.repo) / "scripts" / "perf-reliability"))
    from issue28.hrana import (  # noqa: E402  (path is set above)
        FatalTransportError,
        HranaClient,
        HranaError,
        Target,
        parse_pipeline_json,
    )

    out = pathlib.Path(arguments.out)
    out.mkdir(parents=True, exist_ok=True)
    raw_path = out / "raw.jsonl"
    target = Target.from_file(arguments.connection_file)
    client = HranaClient(target, timeout=arguments.timeout)

    blocks: list[dict] = []
    failures = 0
    first_failure = None

    def run_block(index: int, warmup: bool) -> dict | None:
        nonlocal failures, first_failure
        started = time.perf_counter_ns()
        statuses: dict[str, int] = {}
        codes: dict[str, int] = {}
        requests_ok = 0
        for _ in range(arguments.block_size):
            try:
                raw = client.pipeline_json(REQUESTS)
            except (FatalTransportError, HranaError) as error:
                failures += 1
                if first_failure is None:
                    first_failure = {"block": index, "kind": "transport",
                                     "detail": type(error).__name__}
                return None
            statuses[str(raw.status)] = statuses.get(str(raw.status), 0) + 1
            if raw.status != 200:
                failures += 1
                if first_failure is None:
                    first_failure = {"block": index, "kind": "http", "status": raw.status}
                continue
            try:
                reply = parse_pipeline_json(raw.body)
            except HranaError as error:
                failures += 1
                if first_failure is None:
                    first_failure = {"block": index, "kind": "decode",
                                     "detail": type(error).__name__}
                continue
            rejected = [item for item in reply.results if not item.ok]
            if rejected:
                failures += 1
                if first_failure is None:
                    first_failure = {"block": index, "kind": "rejected",
                                     "code": rejected[0].error_code}
                continue
            requests_ok += 1
        elapsed_ns = time.perf_counter_ns() - started
        block = {
            "leg": arguments.leg,
            "side": arguments.side,
            "round": arguments.round,
            "block": index,
            "warmup": warmup,
            "requests": arguments.block_size,
            "requests_ok": requests_ok,
            "seconds": elapsed_ns / 1_000_000_000,
            "per_request_ms": elapsed_ns / 1_000_000 / arguments.block_size,
            "statuses": statuses,
        }
        blocks.append(block)
        with open(raw_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(block, ensure_ascii=False) + "\n")
        print("%s %s block %d%s %.3f ms/request" % (
            arguments.leg, arguments.side, index,
            " (warmup)" if warmup else "", block["per_request_ms"]), flush=True)
        return block

    total_blocks = arguments.warmup_blocks + arguments.blocks
    time.sleep(arguments.initial_refill_seconds)
    for index in range(total_blocks):
        if index > 0:
            time.sleep(arguments.block_gap_seconds)
        run_block(index, index < arguments.warmup_blocks)
    client.close()

    timed = [block["per_request_ms"] for block in blocks if not block["warmup"]]
    warmup = [block["per_request_ms"] for block in blocks if block["warmup"]]
    summary = {
        "leg": arguments.leg,
        "side": arguments.side,
        "round": arguments.round,
        "block_size": arguments.block_size,
        "blocks": arguments.blocks,
        "block_gap_seconds": arguments.block_gap_seconds,
        "initial_refill_seconds": arguments.initial_refill_seconds,
        "timed_blocks": len(timed),
        "warmup_blocks": len(warmup),
        "warmup_median_ms": round(statistics.median(warmup), 4) if warmup else None,
        "median_ms": round(statistics.median(timed), 4) if timed else None,
        "mean_ms": round(statistics.fmean(timed), 4) if timed else None,
        "stdev_ms": round(statistics.stdev(timed), 4) if len(timed) > 1 else None,
        "min_ms": round(min(timed), 4) if timed else None,
        "max_ms": round(max(timed), 4) if timed else None,
        "failures": failures,
        "first_failure": first_failure,
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
                                      encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False), flush=True)

    if failures:
        print("leg %s has %d failed requests" % (arguments.leg, failures), file=sys.stderr)
        return 1
    if len(timed) != arguments.blocks:
        print("leg %s produced %d/%d timed blocks" % (arguments.leg, len(timed), arguments.blocks),
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
