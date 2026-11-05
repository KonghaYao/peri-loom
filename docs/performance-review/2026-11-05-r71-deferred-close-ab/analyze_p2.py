#!/usr/bin/env python3
"""Analyze the R71 P2 regression gate (F1, L1, X1 c8).

Reads the standard Issue 28 harness output (`harness/raw.jsonl` and
`harness/summary.json`) plus `driver.txt` (which carries the X1 elapsed time).
The frozen rule is: a reproducible degradation is

  p50/p95:  merged B/A > 1.10 AND both round ratios > 1.0
  X1 c8:    merged B/A throughput < 0.90 AND both round ratios < 1.0

merging = pooled median (or throughput median) per side; a round ratio uses
that round's two A legs and two B legs. Improvements are reported as supporting
evidence only and never feed the P1 decision.
"""
from __future__ import annotations

import argparse
import collections
import json
import pathlib
import re
import statistics
import sys

LEGS = ("r1-a1", "r1-b1", "r1-b2", "r1-a2", "r2-a1", "r2-b1", "r2-b2", "r2-a2")
SCENARIOS = ("f1", "l1", "x1")


def percentile(values: list[float], percent: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * percent / 100.0))]


def side_of(leg: str) -> str:
    return "a" if "-a" in leg else "b"


def round_of(leg: str) -> str:
    return leg.split("-", 1)[0]


def parse_x1_elapsed(driver_text: str, level: int = 8) -> float | None:
    pattern = re.compile(r"X1 concurrency=\s*%d\s+n=\d+/\d+\s+elapsed=([0-9.]+)ms"
                         % level)
    match = pattern.search(driver_text)
    return float(match.group(1)) if match else None


def load_leg(root: pathlib.Path, scenario: str, leg: str) -> dict:
    directory = root / "legs" / "p2" / scenario / leg
    harness = directory / "harness"
    raw = [json.loads(line) for line in
           (harness / "raw.jsonl").read_text(encoding="utf-8").splitlines()
           if line.strip()]
    summary = json.loads((harness / "summary.json").read_text(encoding="utf-8"))
    driver = (directory / "driver.txt").read_text(encoding="utf-8")
    record = {
        "leg": leg,
        "side": side_of(leg),
        "round": round_of(leg),
        "driver_exit": json.loads((directory / "exit-code.json").read_text())["driver"],
        "warnings": summary.get("warnings") or [],
        "fatal": summary.get("fatal") or "",
        "row_verdicts": {row["scenario"]: row["verdict"]
                         for row in summary.get("rows") or []},
    }
    if scenario in ("f1", "l1"):
        timed = [item for item in raw if item.get("phase") == "timed"]
        durations = [item["duration_ms"] for item in timed]
        sizes = collections.Counter(item["response_bytes"] for item in timed)
        errors = [item for item in timed
                  if item.get("status") != 200 or item.get("outcome") != "ok"]
        record.update({
            "requests": len(timed),
            "median_ms": round(statistics.median(durations), 4) if durations else None,
            "mean_ms": round(statistics.fmean(durations), 4) if durations else None,
            "p95_ms": round(percentile(durations, 95), 4) if durations else None,
            "min_ms": round(min(durations), 4) if durations else None,
            "max_ms": round(max(durations), 4) if durations else None,
            "failures": len(errors),
            "failure_sample": errors[:3],
            "response_bytes": dict(sorted(sizes.items())),
            "bytes_median": statistics.median(list(sizes.elements())) if sizes else None,
            "durations": durations,
        })
    else:
        timed = [item for item in raw
                 if item.get("phase") == "timed" and item.get("concurrency") == 8]
        durations = [item["duration_ms"] for item in timed]
        errors = [item for item in timed
                  if item.get("status") != 200 or item.get("outcome") != "ok"]
        elapsed_ms = parse_x1_elapsed(driver)
        record.update({
            "requests": len(timed),
            "median_ms": round(statistics.median(durations), 4) if durations else None,
            "p95_ms": round(percentile(durations, 95), 4) if durations else None,
            "failures": len(errors),
            "failure_sample": errors[:3],
            "elapsed_ms": elapsed_ms,
            "throughput_qps": round(100 / (elapsed_ms / 1000.0), 3)
            if elapsed_ms else None,
            "durations": durations,
        })
    return record


def summarize(root: pathlib.Path) -> dict:
    report = {"scenarios": {}, "degradations": [], "verdict": "PASS"}
    for scenario in SCENARIOS:
        legs = [load_leg(root, scenario, leg) for leg in LEGS]
        by_leg = {leg["leg"]: leg for leg in legs}
        entry = {"legs": [{key: value for key, value in leg.items()
                           if key not in ("durations", "failure_sample")}
                          for leg in legs], "checks": {}}
        if scenario in ("f1", "l1"):
            for metric in ("median_ms", "p95_ms"):
                sides = {}
                for side in ("a", "b"):
                    values = [leg[metric] for leg in legs if leg["side"] == side]
                    sides[side] = statistics.median(values)
                rounds = {}
                for round_name in ("r1", "r2"):
                    a_values = [leg[metric] for leg in legs
                                if leg["round"] == round_name and leg["side"] == "a"]
                    b_values = [leg[metric] for leg in legs
                                if leg["round"] == round_name and leg["side"] == "b"]
                    rounds[round_name] = round(statistics.median(b_values)
                                               / statistics.median(a_values), 5)
                merged = round(sides["b"] / sides["a"], 5)
                reproducible = merged > 1.10 and all(value > 1.0 for value in rounds.values())
                entry["checks"][metric] = {
                    "sides": {key: round(value, 4) for key, value in sides.items()},
                    "merged_b_over_a": merged,
                    "round_ratios": rounds,
                    "reproducible_degradation": reproducible,
                }
                if reproducible:
                    report["degradations"].append("%s %s >10%% reproducible" % (scenario, metric))
            bytes_by_side = {
                side: collections.Counter()
                for side in ("a", "b")
            }
            for leg in legs:
                bytes_by_side[leg["side"]].update(
                    {int(key): value for key, value in leg["response_bytes"].items()})
            entry["wire_bytes"] = {
                side: {str(key): value for key, value in sorted(counter.items())}
                for side, counter in bytes_by_side.items()}
            entry["wire_bytes_identical"] = bytes_by_side["a"] == bytes_by_side["b"]
        else:
            sides = {}
            for side in ("a", "b"):
                values = [leg["throughput_qps"] for leg in legs if leg["side"] == side]
                sides[side] = statistics.median(values)
            rounds = {}
            for round_name in ("r1", "r2"):
                a_values = [leg["throughput_qps"] for leg in legs
                            if leg["round"] == round_name and leg["side"] == "a"]
                b_values = [leg["throughput_qps"] for leg in legs
                            if leg["round"] == round_name and leg["side"] == "b"]
                rounds[round_name] = round(statistics.median(b_values)
                                           / statistics.median(a_values), 5)
            merged = round(sides["b"] / sides["a"], 5)
            reproducible = merged < 0.90 and all(value < 1.0 for value in rounds.values())
            entry["checks"]["throughput_qps"] = {
                "sides": {key: round(value, 3) for key, value in sides.items()},
                "merged_b_over_a": merged,
                "round_ratios": rounds,
                "reproducible_degradation": reproducible,
            }
            if reproducible:
                report["degradations"].append("x1 throughput >10% reproducible")
        failures = sum(leg["failures"] for leg in legs)
        entry["failures"] = failures
        entry["driver_exits"] = {leg["leg"]: leg["driver_exit"] for leg in legs}
        entry["warnings"] = {leg["leg"]: leg["warnings"] for leg in legs
                             if leg["warnings"]}
        entry["fatal"] = {leg["leg"]: leg["fatal"] for leg in legs if leg["fatal"]}
        entry["row_verdicts"] = {leg["leg"]: leg["row_verdicts"] for leg in legs}
        report["scenarios"][scenario] = entry
    if report["degradations"]:
        report["verdict"] = "FAIL"
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(pathlib.Path(__file__).resolve().parent))
    arguments = parser.parse_args()
    root = pathlib.Path(arguments.root)
    report = summarize(root)
    (root / "analysis-p2.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print("| scenario | metric | A | B | merged B/A | r1 | r2 | verdict |")
    print("|---|---|---:|---:|---:|---:|---:|---|")
    for scenario, entry in report["scenarios"].items():
        for metric, check in entry["checks"].items():
            print("| %s | %s | %.4f | %.4f | %.5f | %.5f | %.5f | %s |"
                  % (scenario, metric, check["sides"]["a"], check["sides"]["b"],
                     check["merged_b_over_a"], check["round_ratios"]["r1"],
                     check["round_ratios"]["r2"],
                     "DEGRADED" if check["reproducible_degradation"] else "ok"))
        print("| %s | failures | — | — | — | — | — | %d |" % (scenario, entry["failures"]))
    for scenario in ("f1", "l1"):
        entry = report["scenarios"][scenario]
        print("%s wire bytes identical across sides: %s  A=%s B=%s"
              % (scenario, entry["wire_bytes_identical"],
                 entry["wire_bytes"]["a"], entry["wire_bytes"]["b"]))
    print("VERDICT:", report["verdict"],
          ("; " + "; ".join(report["degradations"])) if report["degradations"] else "")
    return 0 if report["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
