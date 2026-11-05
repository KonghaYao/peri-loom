#!/usr/bin/env python3
"""Dual-stack block-mean driver for the R71c short interleaved campaign.

Two isolated stacks (A = flag off, B = PERI_DEFERRED_SESSION_CLOSE=1) run
concurrently. One triple is three 20-request blocks: A1 (stack A), A2 (stack A)
and B (stack B), executed in a seeded random order. A block sample is the wall
clock of its 20 consecutive Hrana JSON pipeline requests divided by 20.

A block with any transport error, non-200 response or rejected pipeline result
fails; the whole triple becomes VOID, is recorded, and is retried after an
extra pause. Void triples never enter a statistic.

One keep-alive connection per stack. The connection files are read for their
endpoints and keys and are never printed.

Usage:
  interleaved_driver.py --a-connection FILE --b-connection FILE --out DIR
                        [--rounds 1] [--triples 30] [--block-size 20]
"""
from __future__ import annotations

import argparse
import collections
import json
import pathlib
import random
import statistics
import sys
import time

REQUESTS = [
    {"type": "execute", "sql": "SELECT 1", "want_rows": True},
    {"type": "close"},
]
BLOCKS = ("A1", "A2", "B")
SIDE_OF = {"A1": "a", "A2": "a", "B": "b"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--a-connection", required=True)
    parser.add_argument("--b-connection", required=True)
    parser.add_argument("--repo", default=None,
                        help="repository root (default: five levels above this file)")
    parser.add_argument("--out", required=True)
    parser.add_argument("--seed", type=int, default=20261105)
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--triples", type=int, default=30)
    parser.add_argument("--block-size", type=int, default=20)
    parser.add_argument("--block-gap-seconds", type=float, default=0.75)
    parser.add_argument("--round-gap-seconds", type=float, default=5.0)
    parser.add_argument("--initial-refill-seconds", type=float, default=1.2)
    parser.add_argument("--retry-pause-seconds", type=float, default=3.0)
    parser.add_argument("--max-attempts", type=int, default=6)
    parser.add_argument("--timeout", type=float, default=120.0)
    arguments = parser.parse_args()

    repo = pathlib.Path(arguments.repo) if arguments.repo else \
        pathlib.Path(__file__).resolve().parents[5]
    sys.path.insert(0, str(repo / "scripts" / "perf-reliability"))
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
    triples_path = out / "triples.jsonl"
    void_path = out / "void.jsonl"
    for stale in (raw_path, triples_path, void_path, out / "summary.json"):
        if stale.exists():
            stale.unlink()

    clients = {
        "a": HranaClient(Target.from_file(arguments.a_connection),
                         timeout=arguments.timeout),
        "b": HranaClient(Target.from_file(arguments.b_connection),
                         timeout=arguments.timeout),
    }
    rng = random.Random(arguments.seed)
    first_block = True
    void_triples = 0
    retry_pause = arguments.retry_pause_seconds

    def run_block(round_index: int, triple_index: int, attempt: int, name: str,
                  position: int, pause: float) -> dict | None:
        nonlocal first_block
        if pause > 0:
            time.sleep(pause)
        side = SIDE_OF[name]
        client = clients[side]
        started = time.perf_counter_ns()
        statuses: collections.Counter[str] = collections.Counter()
        requests_ok = 0
        failure = None
        completed = 0
        for _ in range(arguments.block_size):
            try:
                raw = client.pipeline_json(REQUESTS)
            except (FatalTransportError, HranaError) as error:
                failure = {"kind": "transport", "detail": type(error).__name__}
                break
            completed += 1
            statuses[str(raw.status)] += 1
            if raw.status != 200:
                failure = {"kind": "http", "status": raw.status}
                break
            try:
                reply = parse_pipeline_json(raw.body)
            except HranaError as error:
                failure = {"kind": "decode", "detail": type(error).__name__}
                break
            rejected = [item for item in reply.results if not item.ok]
            if rejected:
                failure = {"kind": "rejected", "code": rejected[0].error_code}
                break
            requests_ok += 1
        elapsed_ns = time.perf_counter_ns() - started
        block = {
            "round": round_index,
            "triple": triple_index,
            "attempt": attempt,
            "block": name,
            "side": side,
            "position": position,
            "requests": arguments.block_size,
            "requests_ok": requests_ok,
            "requests_attempted": completed,
            "seconds": elapsed_ns / 1_000_000_000,
            "per_request_ms": elapsed_ns / 1_000_000 / arguments.block_size,
            "statuses": dict(statuses),
            "failure": failure,
        }
        with open(raw_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(block, ensure_ascii=False) + "\n")
        print("%s r%d t%02d attempt%d %s pos%d %s %.3f ms/request%s" % (
            time.strftime("%H:%M:%S"), round_index, triple_index, attempt, name,
            position, "FAIL" if failure else "ok", block["per_request_ms"],
            (" " + json.dumps(failure)) if failure else ""), flush=True)
        first_block = False
        return None if failure else block

    for round_index in range(1, arguments.rounds + 1):
        for triple_index in range(arguments.triples):
            attempt = 0
            while True:
                attempt += 1
                order = list(BLOCKS)
                rng.shuffle(order)
                results: dict[str, dict | None] = {}
                for position, name in enumerate(order):
                    pause = arguments.block_gap_seconds
                    if first_block:
                        pause = arguments.initial_refill_seconds
                    elif position == 0 and triple_index == 0 and attempt == 1:
                        # Only the first block after a completed round pays the
                        # longer round pause; every other triple starts with
                        # the normal block gap.
                        pause = arguments.round_gap_seconds
                    if attempt > 1:
                        pause = max(pause, retry_pause)
                    results[name] = run_block(round_index, triple_index, attempt,
                                              name, position, pause)
                failed = [name for name in BLOCKS if results.get(name) is None]
                if not failed:
                    break
                void_triples += 1
                record = {
                    "round": round_index,
                    "triple": triple_index,
                    "attempt": attempt,
                    "order": order,
                    "failed_blocks": failed,
                }
                with open(void_path, "a", encoding="utf-8") as handle:
                    handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                print("VOID r%d t%02d attempt%d failed=%s; retry in %.1fs" % (
                    round_index, triple_index, attempt, ",".join(failed),
                    retry_pause), flush=True)
                if attempt >= arguments.max_attempts:
                    print("triple r%d t%02d failed %d attempts; aborting" % (
                        round_index, triple_index, attempt), file=sys.stderr)
                    return 1
                time.sleep(retry_pause)
                retry_pause = min(retry_pause * 2.0, 60.0)

            a1 = results["A1"]["per_request_ms"]
            a2 = results["A2"]["per_request_ms"]
            b = results["B"]["per_request_ms"]
            a_mean = (a1 + a2) / 2.0
            triple = {
                "round": round_index,
                "triple": triple_index,
                "order": order,
                "a1_ms": a1,
                "a2_ms": a2,
                "b_ms": b,
                "a_mean_ms": a_mean,
                "zero_ratio": a2 / a1,
                "effect": b / a_mean,
                "paired_diff_ms": b - a_mean,
            }
            with open(triples_path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(triple, ensure_ascii=False) + "\n")

    for client in clients.values():
        client.close()

    triples = [json.loads(line) for line in
               triples_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    blocks = [json.loads(line) for line in
              raw_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    by_round = {}
    for round_index in range(1, arguments.rounds + 1):
        subset = [item for item in triples if item["round"] == round_index]
        by_round[str(round_index)] = {
            "triples": len(subset),
            "median_effect": round(statistics.median(
                [item["effect"] for item in subset]), 6) if subset else None,
            "median_zero_ratio": round(statistics.median(
                [item["zero_ratio"] for item in subset]), 6) if subset else None,
        }
    summary = {
        "seed": arguments.seed,
        "rounds": arguments.rounds,
        "triples_per_round": arguments.triples,
        "block_size": arguments.block_size,
        "block_gap_seconds": arguments.block_gap_seconds,
        "round_gap_seconds": arguments.round_gap_seconds,
        "initial_refill_seconds": arguments.initial_refill_seconds,
        "valid_triples": len(triples),
        "void_triples": void_triples,
        "blocks": len(blocks),
        "failed_blocks": sum(1 for item in blocks if item["failure"]),
        "requests_total": sum(item["requests_attempted"] for item in blocks),
        "a_requests": sum(item["requests_attempted"] for item in blocks
                          if item["side"] == "a"),
        "b_requests": sum(item["requests_attempted"] for item in blocks
                          if item["side"] == "b"),
        "median_effect": round(statistics.median(
            [item["effect"] for item in triples]), 6) if triples else None,
        "median_zero_ratio": round(statistics.median(
            [item["zero_ratio"] for item in triples]), 6) if triples else None,
        "by_round": by_round,
    }
    (out / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    if len(triples) != arguments.rounds * arguments.triples:
        print("incomplete: %d/%d valid triples" % (
            len(triples), arguments.rounds * arguments.triples), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
