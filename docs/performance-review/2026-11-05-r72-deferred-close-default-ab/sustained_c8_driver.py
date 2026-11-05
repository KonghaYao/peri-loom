#!/usr/bin/env python3
"""Sustained c8 driver for R71d.

One request is the same Hrana JSON pipeline as the interleaved P1 blocks:

    {"requests":[{"type":"execute","stmt":{"sql":"SELECT 1",
     "want_rows":true}},{"type":"close"}]}

The implicit open fuses the statement into `open_session`; the close entry
retires the session it opened. Eight workers each own one keep-alive
connection.

The default quota is not relaxed. One pipeline request charges the open and
statement actions (close is a control action), so a block of 8 x 25 = 200
requests spends 400 tokens against the 500-token burst; a 1.2 s gap refills
600 tokens. The block shape is the pre-registered "equivalent steady-state
design" in criteria.txt, not an attempt to hide a c8 problem: throughput is
computed only from the timed block wall clocks, and every request's latency is
recorded.

Calibration: one request per worker, then a 1.5 s refill. Warm-up: 4 blocks
(100 requests per worker). Timed: 16 blocks (400 requests per worker). Every
leg therefore sends 500 requests per worker.

The connection file is read for the endpoint and key and is never printed.
"""
from __future__ import annotations

import argparse
import collections
import concurrent.futures
import json
import pathlib
import statistics
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


def percentile(values: list[float], percent: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * percent / 100.0))]


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
    parser.add_argument("--label", default="sustained-c8")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--requests-per-client", type=int, default=500)
    parser.add_argument("--warmup-per-client", type=int, default=100)
    parser.add_argument("--requests-per-block", type=int, default=200,
                        help="200 requests = 8 clients x 25")
    parser.add_argument("--block-gap-seconds", type=float, default=1.2)
    parser.add_argument("--initial-refill-seconds", type=float, default=1.5)
    parser.add_argument("--settle-seconds", type=float, default=2.0)
    parser.add_argument("--settle-attempts", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=120.0)
    arguments = parser.parse_args()

    if arguments.requests_per_client != 500:
        parser.error("requests-per-client must be 500 (frozen)")
    if arguments.warmup_per_client != 100:
        parser.error("warmup-per-client must be 100 (frozen)")
    if arguments.requests_per_block % arguments.concurrency != 0:
        parser.error("requests-per-block must divide evenly across clients")
    per_client_block = arguments.requests_per_block // arguments.concurrency
    total_blocks = arguments.requests_per_client // per_client_block
    warmup_blocks = arguments.warmup_per_client // per_client_block
    timed_blocks = total_blocks - warmup_blocks
    if timed_blocks <= 0:
        parser.error("no timed blocks")

    out = pathlib.Path(arguments.out)
    out.mkdir(parents=True, exist_ok=True)
    target = Target.from_file(arguments.connection_file)

    clients = [HranaClient(target, timeout=arguments.timeout)
               for _ in range(arguments.concurrency)]
    records: list[dict] = []
    records_lock = threading.Lock()
    blocks: list[dict] = []
    start_barrier = threading.Barrier(arguments.concurrency + 1)
    end_barrier = threading.Barrier(arguments.concurrency + 1)
    stop = threading.Event()
    worker_errors: list[str] = []

    def worker(index: int) -> None:
        client = clients[index]
        try:
            for block_index in range(total_blocks):
                start_barrier.wait()
                if stop.is_set():
                    end_barrier.wait()
                    return
                local: list[dict] = []
                for offset in range(per_client_block):
                    record = run_request(client)
                    record.update({"worker": index, "block": block_index,
                                   "offset": offset})
                    local.append(record)
                with records_lock:
                    records.extend(local)
                end_barrier.wait()
        except threading.BrokenBarrierError as error:
            worker_errors.append("worker %d barrier: %s" % (index, error))
            stop.set()
        except Exception as error:  # noqa: BLE001 - keep the evidence
            worker_errors.append("worker %d: %s: %s"
                                 % (index, type(error).__name__, error))
            stop.set()
            try:
                end_barrier.abort()
            except Exception:
                pass

    def run_block(index: int, warmup: bool) -> None:
        start = time.perf_counter_ns()
        try:
            start_barrier.wait()
            end_barrier.wait()
        except threading.BrokenBarrierError:
            return
        wall_ns = time.perf_counter_ns() - start
        block_records = [item for item in records if item["block"] == index]
        failures = [item for item in block_records if not item["ok"]]
        block = {
            "block": index,
            "warmup": warmup,
            "requests": len(block_records),
            "failures": len(failures),
            "first_failure": failures[0] if failures else None,
            "wall_ms": wall_ns / 1e6,
            "throughput_qps": round(len(block_records) / (wall_ns / 1e9), 3)
            if wall_ns else None,
        }
        blocks.append(block)
        with open(out / "blocks.jsonl", "a", encoding="utf-8") as handle:
            handle.write(json.dumps(block, ensure_ascii=False) + "\n")
        print("block %02d %s n=%d %.1fms %.1f qps failures=%d" % (
            index, "warmup" if warmup else "timed", len(block_records),
            block["wall_ms"], block["throughput_qps"] or 0, len(failures)),
            flush=True)

    # Calibration: one request per worker, before the metrics window.
    calibration = []
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=arguments.concurrency) as pool:
        calibration = list(pool.map(lambda client: run_request(client), clients))
    calibration_failures = [item for item in calibration if not item["ok"]]

    time.sleep(arguments.initial_refill_seconds)
    before = gate2_common.metrics_snapshot(target)

    # Start the workers; the main thread releases each block in order.
    threads = [threading.Thread(target=worker, args=(index,), daemon=True)
               for index in range(arguments.concurrency)]
    for thread in threads:
        thread.start()

    for block_index in range(total_blocks):
        run_block(block_index, block_index < warmup_blocks)
        if stop.is_set():
            try:
                start_barrier.abort()
                end_barrier.abort()
            except Exception:
                pass
            break
        if block_index + 1 < total_blocks:
            time.sleep(arguments.block_gap_seconds)

    for thread in threads:
        thread.join(timeout=arguments.timeout)
    for client in clients:
        try:
            client.close()
        except Exception:
            pass

    # Settle the deferred account before the after-snapshot.
    after = {}
    for _ in range(arguments.settle_attempts):
        time.sleep(arguments.settle_seconds)
        after = gate2_common.metrics_snapshot(target)
        deferred = after.get("deferred_session_close")
        if deferred is None or int(deferred.get("pending") or 0) == 0:
            break
    if not after:
        after = gate2_common.metrics_snapshot(target)

    with open(out / "raw.jsonl", "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
    (out / "calibration.json").write_text(
        json.dumps(calibration, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    gate2_common.write_json(out / "window-before.json", before)
    gate2_common.write_json(out / "window-after.json", after)

    block_by_index = {item["block"]: item for item in blocks}
    timed = [item for item in records
             if item["block"] in block_by_index
             and not block_by_index[item["block"]]["warmup"]]
    timed_failures = [item for item in timed if not item["ok"]]
    warmup_failures = [item for item in records
                       if item["block"] in block_by_index
                       and block_by_index[item["block"]]["warmup"]
                       and not item["ok"]]
    timed_blocks_list = [item for item in blocks if not item["warmup"]]
    timed_wall_ms = sum(item["wall_ms"] for item in timed_blocks_list)
    durations = [item["duration_ns"] / 1e6 for item in timed]
    statuses = collections.Counter(str(item["status"]) for item in timed)
    error_codes = collections.Counter(item["error"] for item in timed
                                      if item["error"])
    deferred = gate2_common.deferred_status_delta(before, after)
    summary = {
        "leg": arguments.leg,
        "label": arguments.label,
        "payload": "f1-select-1-json-pipeline-implicit-open-close-c8",
        "concurrency": arguments.concurrency,
        "requests_per_client": arguments.requests_per_client,
        "warmup_per_client": arguments.warmup_per_client,
        "requests_per_block": arguments.requests_per_block,
        "per_client_block": per_client_block,
        "block_gap_seconds": arguments.block_gap_seconds,
        "initial_refill_seconds": arguments.initial_refill_seconds,
        "total_blocks": total_blocks,
        "warmup_blocks": warmup_blocks,
        "timed_blocks": timed_blocks,
        "calibration": {"requests": len(calibration),
                        "failures": len(calibration_failures),
                        "first_failure": calibration_failures[0]
                        if calibration_failures else None},
        "blocks": blocks,
        "timed": {
            "requests": len(timed),
            "failures": len(timed_failures),
            "first_failure": timed_failures[0] if timed_failures else None,
            "wall_ms": timed_wall_ms,
            "throughput_qps": round(len(timed) / (timed_wall_ms / 1000.0), 3)
            if timed_wall_ms else None,
            "p50_ms": round(statistics.median(durations), 4) if durations else None,
            "p95_ms": round(percentile(durations, 95), 4) if durations else None,
            "mean_ms": round(statistics.fmean(durations), 4) if durations else None,
            "min_ms": round(min(durations), 4) if durations else None,
            "max_ms": round(max(durations), 4) if durations else None,
            "status_counts": dict(statuses),
            "error_codes": dict(error_codes),
        },
        "warmup": {"requests": len(records) - len(timed),
                   "failures": len(warmup_failures)},
        "worker_errors": worker_errors,
        "window": gate2_common.window_summary(before, after),
        "deferred_session_close": deferred,
        "session_seats_after": int(after.get("session_seats") or 0),
        "active_permits_after": int(after.get("active_permits") or 0),
        "cleanup_ledger_after": after.get("cleanup_ledger") or {},
    }
    gate2_common.write_json(out / "summary.json", summary)
    print(json.dumps({"leg": arguments.leg,
                      "timed": summary["timed"],
                      "warmup": summary["warmup"],
                      "deferred": deferred}, ensure_ascii=False), flush=True)

    bad = (calibration_failures or timed_failures or warmup_failures
           or worker_errors or stop.is_set())
    return 0 if not bad else 1


if __name__ == "__main__":
    raise SystemExit(main())
