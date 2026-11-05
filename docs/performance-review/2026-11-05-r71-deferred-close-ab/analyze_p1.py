#!/usr/bin/env python3
"""Analyze the R71 frozen A/B P1 campaign.

Reads `legs/p1/<leg>/raw.jsonl` (one record per 100-request block) written by
block_driver.py. The frozen decision is:

  P0.1  a2/a1 positional ratio of the flag-off A legs inside 1.00 +/- 0.05.
  P1    merged B/A block-median ratio <= 0.92, both rounds same direction,
        and the paired bootstrap 95% CI of the per-block B-A difference
        excludes 0 (upper bound < 0).

All resampling uses 10,000 percentile-bootstrap draws with the frozen seed
20261105. Nothing here re-reads raw data with a different protocol.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import random
import statistics
import sys

DRAWS = 10_000
SEED = 20261105
LEGS = ("r1-a1", "r1-b1", "r1-b2", "r1-a2", "r2-a1", "r2-b1", "r2-b2", "r2-a2")


def percentile(values: list[float], percent: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * percent / 100.0))]


def ratio_ci(left: list[float], right: list[float]) -> dict:
    generator = random.Random(SEED)
    ratios = []
    for _ in range(DRAWS):
        left_sample = [left[generator.randrange(len(left))] for _ in range(len(left))]
        right_sample = [right[generator.randrange(len(right))] for _ in range(len(right))]
        ratios.append(statistics.median(right_sample) / statistics.median(left_sample))
    ratios.sort()
    return {
        "ratio": round(statistics.median(right) / statistics.median(left), 5),
        "ci95": [round(ratios[int(0.025 * len(ratios))], 5),
                 round(ratios[min(len(ratios) - 1, int(0.975 * len(ratios)))], 5)],
        "draws": DRAWS,
    }


def paired_ci(pairs: list[tuple[float, float]]) -> dict:
    generator = random.Random(SEED)
    differences = [right - left for left, right in pairs]
    draws = []
    count = len(differences)
    for _ in range(DRAWS):
        sample = [differences[generator.randrange(count)] for _ in range(count)]
        draws.append(statistics.median(sample))
    draws.sort()
    left_items = [left for left, _ in pairs]
    return {
        "pairs": count,
        "median_difference_ms": round(statistics.median(differences), 6),
        "mean_difference_ms": round(statistics.fmean(differences), 6),
        "ci95_ms": [round(draws[int(0.025 * len(draws))], 6),
                    round(draws[min(len(draws) - 1, int(0.975 * len(draws)))], 6)],
        "percent_of_paired_a_median": round(
            statistics.median(differences) / statistics.median(left_items) * 100.0, 3),
        "draws": DRAWS,
    }


def load_legs(root: pathlib.Path) -> list[dict]:
    legs = []
    for name in LEGS:
        directory = root / "legs" / "p1" / name
        summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
        raw = [json.loads(line) for line in
               (directory / "raw.jsonl").read_text(encoding="utf-8").splitlines()
               if line.strip()]
        timed = [block["per_request_ms"] for block in raw if not block["warmup"]]
        legs.append({
            "leg": name,
            "side": summary["side"],
            "round": summary["round"],
            "blocks": len(timed),
            "median_ms": round(statistics.median(timed), 4),
            "mean_ms": round(statistics.fmean(timed), 4),
            "p95_ms": round(percentile(timed, 95), 4),
            "stdev_ms": round(statistics.stdev(timed), 4),
            "min_ms": round(min(timed), 4),
            "max_ms": round(max(timed), 4),
            "failures": summary.get("failures", 0),
            "first_failure": summary.get("first_failure"),
            "values": timed,
            "requests": summary.get("block_size", 100) * len(timed),
        })
    return legs


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(pathlib.Path(__file__).resolve().parent))
    arguments = parser.parse_args()
    root = pathlib.Path(arguments.root)

    legs = load_legs(root)
    by_name = {leg["leg"]: leg for leg in legs}
    a_blocks = [value for leg in legs if leg["side"] == "a" for value in leg["values"]]
    b_blocks = [value for leg in legs if leg["side"] == "b" for value in leg["values"]]
    a1_blocks = [value for name in ("r1-a1", "r2-a1") for value in by_name[name]["values"]]
    a2_blocks = [value for name in ("r1-a2", "r2-a2") for value in by_name[name]["values"]]

    merged = ratio_ci(a_blocks, b_blocks)
    positional = ratio_ci(a1_blocks, a2_blocks)

    rounds = {}
    pairs = []
    for round_name, a1, b1, b2, a2 in (("r1", "r1-a1", "r1-b1", "r1-b2", "r1-a2"),
                                       ("r2", "r2-a1", "r2-b1", "r2-b2", "r2-a2")):
        a_values = by_name[a1]["values"] + by_name[a2]["values"]
        b_values = by_name[b1]["values"] + by_name[b2]["values"]
        rounds[round_name] = ratio_ci(a_values, b_values)
        for left, right in ((by_name[a1]["values"], by_name[b1]["values"]),
                            (by_name[a2]["values"], by_name[b2]["values"])):
            pairs.extend(zip(left, right))
    paired = paired_ci(pairs)

    complete = all(leg["blocks"] == 30 for leg in legs) and not any(
        leg["failures"] for leg in legs)
    same_direction = all(value["ratio"] < 1.0 for value in rounds.values())
    aa_ok = 0.95 <= positional["ratio"] <= 1.05
    p1_ok = (merged["ratio"] <= 0.92 and same_direction
             and paired["ci95_ms"][1] < 0)
    verdict = "PASS" if (complete and aa_ok and p1_ok) else "FAIL"
    reasons = []
    if not complete:
        reasons.append("incomplete legs or driver failures")
    if not aa_ok:
        reasons.append("P0.1 positional A/A ratio outside 1.00 +/- 0.05: power insufficient")
    if merged["ratio"] > 0.92:
        reasons.append("merged B/A ratio above 0.92")
    if not same_direction:
        reasons.append("rounds not same direction")
    if paired["ci95_ms"][1] >= 0:
        reasons.append("paired bootstrap 95% CI does not exclude 0")

    report = {
        "complete": complete,
        "legs": [{key: leg[key] for key in
                  ("leg", "side", "round", "blocks", "median_ms", "mean_ms",
                   "p95_ms", "stdev_ms", "min_ms", "max_ms", "failures",
                   "requests")} for leg in legs],
        "sides": {
            "a": {"blocks": len(a_blocks), "median_ms": round(statistics.median(a_blocks), 4),
                  "mean_ms": round(statistics.fmean(a_blocks), 4),
                  "p95_ms": round(percentile(a_blocks, 95), 4)},
            "b": {"blocks": len(b_blocks), "median_ms": round(statistics.median(b_blocks), 4),
                  "mean_ms": round(statistics.fmean(b_blocks), 4),
                  "p95_ms": round(percentile(b_blocks, 95), 4)},
        },
        "p0_1_positional_a2_over_a1": positional,
        "p0_2_positive_control_citation": {
            "source": "Gate 1b campaign 3 (spin 100us probe)",
            "merged_b_over_a": 1.13258,
            "ci95": [1.08304, 1.20891],
            "rounds": {"r1": 1.13602, "r2": 1.12804},
            "rerun_this_round": False,
        },
        "merged_b_over_a": merged,
        "round_ratios": {name: value["ratio"] for name, value in rounds.items()},
        "round_details": rounds,
        "paired_b_minus_a": paired,
        "same_direction": same_direction,
        "reasons": reasons,
        "verdict": verdict,
    }
    (root / "analysis-p1.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print("| leg | side | round | blocks | median ms | mean ms | p95 ms | stdev | failures |")
    print("|---|---|---|---:|---:|---:|---:|---:|---:|")
    for leg in legs:
        print("| %s | %s | %s | %d | %.4f | %.4f | %.4f | %.4f | %d |"
              % (leg["leg"], leg["side"], leg["round"], leg["blocks"], leg["median_ms"],
                 leg["mean_ms"], leg["p95_ms"], leg["stdev_ms"], leg["failures"]))
    print()
    print("side A pooled: median %.4f ms, mean %.4f ms, p95 %.4f ms (%d blocks)"
          % (report["sides"]["a"]["median_ms"], report["sides"]["a"]["mean_ms"],
             report["sides"]["a"]["p95_ms"], len(a_blocks)))
    print("side B pooled: median %.4f ms, mean %.4f ms, p95 %.4f ms (%d blocks)"
          % (report["sides"]["b"]["median_ms"], report["sides"]["b"]["mean_ms"],
             report["sides"]["b"]["p95_ms"], len(b_blocks)))
    print("P0.1 a2/a1 = %.5f (CI %s)" % (positional["ratio"], positional["ci95"]))
    print("P1 merged B/A = %.5f (CI %s), rounds %s"
          % (merged["ratio"], merged["ci95"], report["round_ratios"]))
    print("paired B-A median %.6f ms (CI %s, %.3f%% of paired A median, n=%d)"
          % (paired["median_difference_ms"], paired["ci95_ms"],
             paired["percent_of_paired_a_median"], paired["pairs"]))
    print("VERDICT:", verdict, ("; ".join(reasons) if reasons else "all frozen checks pass"))
    return 0 if verdict == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
