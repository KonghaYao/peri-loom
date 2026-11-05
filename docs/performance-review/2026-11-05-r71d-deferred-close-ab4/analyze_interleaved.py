#!/usr/bin/env python3
"""Analyze the R71d short interleaved campaign (P0 zero control and P1).

Reads legs/<campaign>/driver/triples.jsonl written by interleaved_driver.py.
Every statistic is computed from this round's triples only; the previous
round's files are never read.

Frozen decisions (criteria.txt):

  P0.1  median(A2/A1) within 1.00 +/- 0.05 AND its bootstrap 95% CI covers 1.
        Evaluated on the 30 triples of this round (see criteria.txt sample-size
        reconciliation).
  P0.2  positive control: median(B / mean(A1,A2)) >= 1.08 (B slower); cited
        from R71b, not rerun this round.
  P1    median(B / mean(A1,A2)) <= 0.90, bootstrap 95% CI upper bound < 1.0,
        and at least 2/3 of the triples (20 of 30) have B faster.

All bootstrap intervals are percentile intervals with 10,000 draws and seed
20261105.
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


def percentile(values: list[float], percent: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * percent / 100.0))]


def bootstrap_median(values: list[float], draws: int = DRAWS,
                     seed: int = SEED) -> dict:
    generator = random.Random(seed)
    samples = []
    count = len(values)
    for _ in range(draws):
        sample = [values[generator.randrange(count)] for _ in range(count)]
        samples.append(statistics.median(sample))
    samples.sort()
    return {
        "median": round(statistics.median(values), 6),
        "ci95": [round(samples[int(0.025 * draws)], 6),
                 round(samples[min(draws - 1, int(0.975 * draws))], 6)],
        "draws": draws,
    }


def describe(values: list[float]) -> dict:
    return {
        "count": len(values),
        "median": round(statistics.median(values), 6),
        "mean": round(statistics.fmean(values), 6),
        "p95": round(percentile(values, 95), 6),
        "min": round(min(values), 6),
        "max": round(max(values), 6),
        "stdev": round(statistics.stdev(values), 6) if len(values) > 1 else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign", choices=("p1", "p0"), required=True)
    parser.add_argument("--root", default=str(pathlib.Path(__file__).resolve().parent))
    arguments = parser.parse_args()
    root = pathlib.Path(arguments.root)
    driver = root / "legs" / arguments.campaign / "driver"
    triples = [json.loads(line) for line in
               (driver / "triples.jsonl").read_text(encoding="utf-8").splitlines()
               if line.strip()]
    summary = json.loads((driver / "summary.json").read_text(encoding="utf-8"))
    rounds = sorted({item["round"] for item in triples})
    expected = 30 if arguments.campaign == "p1" else 30
    complete = len(triples) == expected and summary["valid_triples"] == expected

    zero_values = [item["zero_ratio"] for item in triples]
    effect_values = [item["effect"] for item in triples]
    diff_values = [item["paired_diff_ms"] for item in triples]
    a1_values = [item["a1_ms"] for item in triples]
    a2_values = [item["a2_ms"] for item in triples]
    b_values = [item["b_ms"] for item in triples]

    report = {
        "campaign": arguments.campaign,
        "complete": complete,
        "valid_triples": len(triples),
        "void_triples": summary["void_triples"],
        "failed_blocks": summary["failed_blocks"],
        "requests_total": summary["requests_total"],
        "a_requests": summary["a_requests"],
        "b_requests": summary["b_requests"],
        "blocks": {
            "A1": describe(a1_values),
            "A2": describe(a2_values),
            "B": describe(b_values),
        },
        "zero_control": {
            "per_triple": bootstrap_median(zero_values),
            "pass": None,
        },
        "effect": {
            "per_triple": bootstrap_median(effect_values),
            "paired_difference_ms": bootstrap_median(diff_values),
            "rounds": {},
            "pass": None,
        },
    }

    by_round = {name: [item for item in triples if item["round"] == name]
                for name in rounds}
    for name, subset in by_round.items():
        report["effect"]["rounds"][str(name)] = {
            "triples": len(subset),
            "effect": bootstrap_median([item["effect"] for item in subset]),
            "zero_ratio": bootstrap_median([item["zero_ratio"] for item in subset]),
            "paired_difference_ms": bootstrap_median(
                [item["paired_diff_ms"] for item in subset]),
        }

    report["direction_triples_below_one"] = sum(
        1 for value in effect_values if value < 1.0)
    report["direction_triples_above_one"] = sum(
        1 for value in effect_values if value >= 1.0)

    zero = report["zero_control"]["per_triple"]
    zero["pass"] = (0.95 <= zero["median"] <= 1.05
                    and zero["ci95"][0] <= 1.0 <= zero["ci95"][1])
    report["zero_control"]["pass"] = zero["pass"]

    effect = report["effect"]["per_triple"]
    round_improvements = [report["effect"]["rounds"][str(name)]["effect"]["median"] < 1.0
                          for name in rounds]
    if arguments.campaign == "p1":
        effect["pass"] = (effect["median"] <= 0.90
                          and effect["ci95"][1] < 1.0
                          and report["direction_triples_below_one"] >= 20)
        report["effect"]["pass"] = effect["pass"]
        report["verdict"] = ("PASS" if complete and zero["pass"] and effect["pass"]
                             else "FAIL")
    else:
        effect["pass"] = effect["median"] >= 1.08
        report["effect"]["pass"] = effect["pass"]
        report["verdict"] = "PASS" if complete and effect["pass"] else "FAIL"

    out_path = root / ("analysis-%s.json" % arguments.campaign)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n",
                        encoding="utf-8")

    print("campaign %s: valid triples %d/%d void=%d failed_blocks=%d"
          % (arguments.campaign, len(triples), expected,
             summary["void_triples"], summary["failed_blocks"]))
    print("| block | n | median ms | mean ms | p95 ms | min | max |")
    print("|---|---:|---:|---:|---:|---:|---:|")
    for name in ("A1", "A2", "B"):
        item = report["blocks"][name]
        print("| %s | %d | %.4f | %.4f | %.4f | %.4f | %.4f |"
              % (name, item["count"], item["median"], item["mean"], item["p95"],
                 item["min"], item["max"]))
    print("zero control A2/A1: median %.5f CI [%.5f, %.5f] pass=%s"
          % (zero["median"], zero["ci95"][0], zero["ci95"][1], zero["pass"]))
    for name in rounds:
        item = report["effect"]["rounds"][str(name)]["zero_ratio"]
        print("  round %s zero ratio median %.5f CI [%.5f, %.5f]"
              % (name, item["median"], item["ci95"][0], item["ci95"][1]))
    print("effect B/mean(A1,A2): median %.5f CI [%.5f, %.5f] pass=%s"
          % (effect["median"], effect["ci95"][0], effect["ci95"][1], effect["pass"]))
    diff = report["effect"]["paired_difference_ms"]
    print("paired difference B - mean(A1,A2): median %.6f ms CI [%.6f, %.6f]"
          % (diff["median"], diff["ci95"][0], diff["ci95"][1]))
    for name in rounds:
        item = report["effect"]["rounds"][str(name)]["effect"]
        print("  round %s effect median %.5f CI [%.5f, %.5f]"
              % (name, item["median"], item["ci95"][0], item["ci95"][1]))
    print("VERDICT:", report["verdict"])
    return 0 if report["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
