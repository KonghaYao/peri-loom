#!/usr/bin/env python3
"""Analyze the R71d P2 gate: F1, L1, standard X1 c8, sustained c8.

Reads the per-leg evidence written by run_p2.sh. The frozen rules are in
criteria.txt:

  F1/L1 p50/p95: merged B/A must be < 1.10.
  Standard X1 c8 throughput: merged B/A must be > 0.90.
  Sustained c8 throughput: merged B/A must be > 0.90, and every leg must be
  free of request failures. Sustained c8 p50/p95 are reported as diagnostics,
  not pass conditions.

Merging uses the median of the four per-leg values per side; a round ratio
uses that round's two A legs and two B legs. Improvements are reported as
supporting evidence only. A leg with request failures (other than the known
standard-X1 c16 429 level) is invalid and fails the gate.

The connection file is never read here.
"""
from __future__ import annotations

import json
import pathlib
import re
import statistics
import sys

LEGS = ("r1-a1", "r1-b1", "r1-b2", "r1-a2", "r2-a1", "r2-b1", "r2-b2", "r2-a2")
SCENARIOS = ("f1", "l1", "x1", "c8")


def percentile(values: list[float], percent: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * percent / 100.0))]


def side_of(leg: str) -> str:
    return "a" if "-a" in leg else "b"


def round_of(leg: str) -> str:
    return leg.split("-", 1)[0]


def read_json(path: pathlib.Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def parse_x1_levels(driver_text: str) -> dict:
    pattern = re.compile(
        r"X1 concurrency=\s*(\d+)\s+n=(\d+)/(\d+)\s+elapsed=([0-9.]+)ms"
        r"\s+p50=([0-9.]+)ms\s+p95=([0-9.]+)ms\s+p99=([0-9.]+)ms\s+429=(\d+)")
    levels = {}
    for match in pattern.finditer(driver_text):
        level = int(match.group(1))
        levels[level] = {
            "level": level,
            "requests": int(match.group(2)),
            "planned": int(match.group(3)),
            "elapsed_ms": float(match.group(4)),
            "p50_ms": float(match.group(5)),
            "p95_ms": float(match.group(6)),
            "p99_ms": float(match.group(7)),
            "status_429": int(match.group(8)),
        }
    return levels


def load_leg(root: pathlib.Path, scenario: str, leg: str) -> dict:
    directory = root / "legs" / "p2" / scenario / leg
    harness = directory / "harness"
    record = {
        "leg": leg,
        "side": side_of(leg),
        "round": round_of(leg),
        "driver_exit": read_json(directory / "exit-code.json") or {},
        "present": harness.is_dir(),
    }
    if not harness.is_dir():
        record["invalid"] = "harness directory missing"
        return record

    if scenario in ("f1", "l1"):
        raw = [json.loads(line) for line in
               (harness / "raw.jsonl").read_text(encoding="utf-8").splitlines()
               if line.strip()]
        summary = read_json(harness / "summary.json") or {}
        timed = [item for item in raw if item.get("phase") == "timed"]
        durations = [item["duration_ms"] for item in timed]
        errors = [item for item in timed
                  if item.get("status") != 200 or item.get("outcome") != "ok"]
        record.update({
            "requests": len(timed),
            "median_ms": round(statistics.median(durations), 4) if durations else None,
            "p95_ms": round(percentile(durations, 95), 4) if durations else None,
            "mean_ms": round(statistics.fmean(durations), 4) if durations else None,
            "failures": len(errors),
            "failure_sample": errors[:3],
            "warnings": summary.get("warnings") or [],
            "fatal": summary.get("fatal") or "",
            "row_verdicts": {row["scenario"]: row["verdict"]
                             for row in summary.get("rows") or []},
            "durations": durations,
        })
        if len(timed) != 125:
            record["invalid"] = "expected 125 timed requests, got %d" % len(timed)
        elif errors:
            record["invalid"] = "%d timed failures" % len(errors)
        elif summary.get("fatal"):
            record["invalid"] = "harness fatal: " + str(summary.get("fatal"))
    elif scenario == "x1":
        summary = read_json(harness / "summary.json") or {}
        driver = (directory / "driver.txt").read_text(encoding="utf-8")
        levels = parse_x1_levels(driver)
        concurrency = {row["level"]: row
                       for row in summary.get("concurrency") or []}
        c8 = levels.get(8) or {}
        c8_metrics = concurrency.get(8) or {}
        c16 = levels.get(16) or {}
        c16_metrics = concurrency.get(16) or {}
        record.update({
            "levels": levels,
            "c8": c8,
            "c8_metrics": c8_metrics,
            "c8_429": c8.get("status_429"),
            "c8_errors": c8_metrics.get("errors"),
            "c16": c16,
            "c16_429": c16.get("status_429"),
            "c16_errors": c16_metrics.get("errors"),
            "fixture_complete": (summary.get("fixture") or {}).get("complete"),
            "warnings": summary.get("warnings") or [],
            "fatal": summary.get("fatal") or "",
            "throughput_qps": round(1000.0 * c8["requests"] / c8["elapsed_ms"], 3)
            if c8.get("requests") and c8.get("elapsed_ms") else None,
            "median_ms": c8.get("p50_ms"),
            "p95_ms": c8.get("p95_ms"),
            "failures": (c8_metrics.get("errors") or 0) + (c8.get("status_429") or 0),
        })
        bad = []
        if not summary:
            bad.append("summary missing")
        if summary.get("fatal"):
            bad.append("fatal: " + str(summary.get("fatal")))
        if (summary.get("fixture") or {}).get("complete") is not True:
            bad.append("fixture incomplete")
        for level in (1, 4, 8):
            item = levels.get(level)
            metrics = concurrency.get(level) or {}
            if item is None:
                bad.append("level %d missing" % level)
                continue
            if item.get("requests") != 100 or item.get("planned") != 100:
                bad.append("level %d requests %s/%s" % (level, item.get("requests"),
                                                        item.get("planned")))
            if item.get("status_429"):
                bad.append("level %d has %d 429" % (level, item["status_429"]))
            if metrics.get("errors"):
                bad.append("level %d has %d errors" % (level, metrics["errors"]))
        if bad:
            record["invalid"] = "; ".join(bad)
    else:  # c8 sustained
        summary = read_json(harness / "summary.json") or {}
        timed = summary.get("timed") or {}
        record.update({
            "requests": timed.get("requests"),
            "throughput_qps": timed.get("throughput_qps"),
            "median_ms": timed.get("p50_ms"),
            "p95_ms": timed.get("p95_ms"),
            "mean_ms": timed.get("mean_ms"),
            "failures": timed.get("failures"),
            "warmup_failures": (summary.get("warmup") or {}).get("failures"),
            "calibration_failures": (summary.get("calibration") or {}).get("failures"),
            "worker_errors": summary.get("worker_errors") or [],
            "blocks": summary.get("blocks") or [],
            "deferred": summary.get("deferred_session_close"),
            "window": summary.get("window"),
            "timed_detail": timed,
        })
        if not summary:
            record["invalid"] = "summary missing"
        elif timed.get("requests") != 3200:
            record["invalid"] = "expected 3200 timed requests, got %r" % timed.get("requests")
        elif timed.get("failures"):
            record["invalid"] = "%r timed failures" % timed.get("failures")
        elif (summary.get("warmup") or {}).get("failures"):
            record["invalid"] = "warmup failures"
        elif (summary.get("calibration") or {}).get("failures"):
            record["invalid"] = "calibration failures"
        elif summary.get("worker_errors"):
            record["invalid"] = "worker errors"
    return record


def ratio_by_round(legs: list[dict], metric: str) -> dict:
    rounds = {}
    for round_name in ("r1", "r2"):
        a_values = [leg[metric] for leg in legs
                    if leg["round"] == round_name and leg["side"] == "a"
                    and leg.get(metric) is not None]
        b_values = [leg[metric] for leg in legs
                    if leg["round"] == round_name and leg["side"] == "b"
                    and leg.get(metric) is not None]
        if a_values and b_values:
            rounds[round_name] = round(statistics.median(b_values)
                                       / statistics.median(a_values), 5)
        else:
            rounds[round_name] = None
    return rounds


def summarize_metric(legs: list[dict], metric: str, degraded_when_lower: bool,
                     require_both_rounds: bool = False) -> dict:
    sides = {}
    for side in ("a", "b"):
        values = [leg[metric] for leg in legs
                  if leg["side"] == side and leg.get(metric) is not None]
        sides[side] = statistics.median(values) if values else None
    rounds = ratio_by_round(legs, metric)
    merged = (round(sides["b"] / sides["a"], 5)
              if sides["a"] and sides["b"] else None)
    round_values = [value for value in rounds.values() if value is not None]
    if degraded_when_lower:
        reproducible = (merged is not None and merged <= 0.90)
        if require_both_rounds:
            reproducible = (reproducible and len(round_values) == 2
                            and all(value < 1.0 for value in round_values))
    else:
        reproducible = (merged is not None and merged >= 1.10)
        if require_both_rounds:
            reproducible = (reproducible and len(round_values) == 2
                            and all(value > 1.0 for value in round_values))
    return {
        "sides": {key: (round(value, 4) if value is not None else None)
                  for key, value in sides.items()},
        "merged_b_over_a": merged,
        "round_ratios": rounds,
        "reproducible_degradation": reproducible,
    }


def summarize(root: pathlib.Path) -> dict:
    report = {"scenarios": {}, "degradations": [], "invalid_legs": [],
              "verdict": "PASS"}
    for scenario in SCENARIOS:
        legs = [load_leg(root, scenario, leg) for leg in LEGS]
        entry = {"legs": [{key: value for key, value in leg.items()
                           if key not in ("durations", "blocks", "levels")}
                          for leg in legs], "checks": {}}
        invalid = [leg["leg"] + ": " + leg["invalid"] for leg in legs
                   if leg.get("invalid")]
        entry["invalid_legs"] = invalid
        if invalid:
            report["invalid_legs"].extend("%s/%s" % (scenario, item)
                                          for item in invalid)
        if scenario in ("f1", "l1"):
            for metric in ("median_ms", "p95_ms"):
                check = summarize_metric(legs, metric, degraded_when_lower=False)
                entry["checks"][metric] = check
                if check["reproducible_degradation"]:
                    report["degradations"].append(
                        "%s %s >10%% reproducible" % (scenario, metric))
        elif scenario == "x1":
            check = summarize_metric(legs, "throughput_qps",
                                     degraded_when_lower=True)
            entry["checks"]["throughput_qps"] = check
            if check["reproducible_degradation"]:
                report["degradations"].append(
                    "x1 c8 throughput >10% reproducible")
            entry["c8_diagnostics"] = {
                leg["leg"]: {"p50_ms": leg.get("median_ms"),
                             "p95_ms": leg.get("p95_ms"),
                             "c8_429": leg.get("c8_429"),
                             "c16_429": leg.get("c16_429"),
                             "c16_errors": leg.get("c16_errors"),
                             "driver_exit": leg.get("driver_exit")}
                for leg in legs}
        else:
            check = summarize_metric(legs, "throughput_qps",
                                     degraded_when_lower=True)
            entry["checks"]["throughput_qps"] = check
            if check["reproducible_degradation"]:
                report["degradations"].append(
                    "c8 throughput >10% reproducible")
            entry["diagnostics"] = {
                leg["leg"]: {"p50_ms": leg.get("median_ms"),
                             "p95_ms": leg.get("p95_ms"),
                             "throughput_qps": leg.get("throughput_qps"),
                             "failures": leg.get("failures")}
                for leg in legs}
            for metric in ("median_ms", "p95_ms"):
                entry["checks"][metric] = summarize_metric(
                    legs, metric, degraded_when_lower=False)
        entry["driver_exits"] = {leg["leg"]: leg.get("driver_exit") for leg in legs}
        entry["warnings"] = {leg["leg"]: leg.get("warnings") for leg in legs
                             if leg.get("warnings")}
        report["scenarios"][scenario] = entry
    if report["degradations"] or report["invalid_legs"]:
        report["verdict"] = "FAIL"
    return report


def main() -> int:
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else __file__).resolve()
    if root.is_file():
        root = root.parent
    report = summarize(root)
    (root / "analysis-p2.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print("| scenario | metric | A | B | merged B/A | r1 | r2 | verdict |")
    print("|---|---|---:|---:|---:|---:|---:|---|")
    for scenario, entry in report["scenarios"].items():
        for metric, check in entry["checks"].items():
            verdict = "DEGRADED" if check["reproducible_degradation"] else "ok"
            print("| %s | %s | %s | %s | %s | %s | %s | %s |" % (
                scenario, metric, check["sides"]["a"], check["sides"]["b"],
                check["merged_b_over_a"], check["round_ratios"].get("r1"),
                check["round_ratios"].get("r2"), verdict))
    for item in report["invalid_legs"]:
        print("INVALID LEG: " + item)
    for item in report["degradations"]:
        print("DEGRADATION: " + item)
    print("VERDICT: " + report["verdict"])
    return 0 if report["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
