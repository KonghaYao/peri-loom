#!/usr/bin/env python3
"""v1.2.7 release A/B aggregate.

Reads legs/<scenario>/<leg>/ (summary.json, raw.jsonl, metrics-before.json,
metrics-after.json, launcher-stop.json, stop-exit.txt) and prints:

  1. per-leg rows for every protocol / concurrency level
  2. leg validity (fixture, outcomes, status, stop proof)
  3. primary metric per round and pooled against the frozen 1.10 regression
     threshold and the 0.90 improvement threshold
  4. p95/p99, secondary protocols and response bytes (report-only)
  5. verdict

Exit code: 0 = all scenarios PASS/IMPROVED, 1 = regression (FAIL),
2 = unconfirmable/invalid evidence.
"""

import json
import math
import pathlib
import statistics
import sys

HERE = pathlib.Path(__file__).resolve().parent
THRESHOLD = 1.10
IMPROVE = 0.90
EXPECTED_FIXTURE = {"threads": 201, "messages": 8625, "content_chars": 20177069}
ROUNDS = {
    "round1": {"A": ("r1a1", "r1a2"), "B": ("r1b1", "r1b2")},
    "round2": {"A": ("r2a1", "r2a2"), "B": ("r2b1", "r2b2")},
}
SCENARIOS = {
    "W1.insert": {
        "protocols": ("json-pipeline", "protobuf-pipeline"),
        "main": "json-pipeline",
        "samples": 5,
        "expected_rows": None,
        "kind": "write",
    },
    "F1.select-1": {
        "protocols": ("json-pipeline", "protobuf-pipeline",
                      "json-cursor", "protobuf-cursor"),
        "main": "json-pipeline",
        "samples": 5,
        "expected_rows": 1,
        "kind": "read",
    },
    "X1.concurrency": {
        "protocols": ("json-pipeline",),
        "main": "json-pipeline",
        "levels": (8, 16),
        "samples": None,
        "expected_rows": None,
        "kind": "concurrency",
    },
    "H1.content-20mb": {
        "protocols": ("json-cursor", "protobuf-cursor"),
        "main": "json-cursor",
        "samples": 3,
        "expected_rows": 8625,
        "kind": "read",
    },
}


def pctl(values, pct):
    if not values:
        return None
    values = sorted(values)
    rank = max(1, math.ceil(pct / 100.0 * len(values)))
    return values[rank - 1]


def med(values):
    return statistics.median(values) if values else None


def fmt(value):
    return "—" if value is None else "%.3f" % value


def load_leg(scenario, leg):
    base = HERE / "legs" / scenario / leg
    entry = {"dir": base, "scenario": scenario, "leg": leg, "problems": [], "rows": {}}
    required = ("summary.json", "raw.jsonl", "metrics-before.json",
                "metrics-after.json", "launcher-stop.json")
    for name in required:
        if not (base / name).exists():
            entry["problems"].append("missing " + name)
    if entry["problems"]:
        return entry

    summary = json.loads((base / "summary.json").read_text())
    entry["summary"] = summary
    entry["before"] = json.loads((base / "metrics-before.json").read_text())
    entry["after"] = json.loads((base / "metrics-after.json").read_text())

    fixture = summary.get("fixture") or {}
    for key, expected in EXPECTED_FIXTURE.items():
        if fixture.get(key) != expected:
            entry["problems"].append("fixture %s=%r expected=%d" % (
                key, fixture.get(key), expected))
    if not fixture.get("complete"):
        entry["problems"].append("fixture incomplete")
    if summary.get("warnings"):
        entry["problems"].append("warnings: %r" % (summary["warnings"],))
    if summary.get("fatal"):
        entry["problems"].append("fatal: %r" % (summary["fatal"],))

    if SCENARIOS[scenario]["kind"] == "concurrency":
        levels = {}
        for row in summary.get("concurrency", []):
            levels[row["level"]] = row
            if row.get("verdict") != "PASS":
                entry["problems"].append("level %d verdict %r" % (
                    row["level"], row.get("verdict")))
            if row.get("status_429"):
                entry["problems"].append("level %d 429=%d" % (
                    row["level"], row["status_429"]))
            if row.get("errors"):
                entry["problems"].append("level %d errors=%d" % (
                    row["level"], row["errors"]))
            if row.get("requests") != row.get("planned"):
                entry["problems"].append("level %d requests=%r planned=%r" % (
                    row["level"], row.get("requests"), row.get("planned")))
        entry["levels"] = levels
        for level in (1, 4, 8, 16):
            if level not in levels:
                entry["problems"].append("missing concurrency level %d" % level)
    else:
        by_protocol = {}
        for row in summary.get("rows", []):
            by_protocol[row["protocol"]] = row
            bad = {k: v for k, v in row.get("outcomes", {}).items()
                   if k not in ("ok", "expected_limit")}
            if bad:
                entry["problems"].append("%s outcomes %s" % (row["protocol"], bad))
            bad_status = {str(k): v for k, v in row.get("status_counts", {}).items()
                          if str(k) != "200"}
            if bad_status:
                entry["problems"].append("%s status %s" % (row["protocol"], bad_status))
            expected = SCENARIOS[scenario]["expected_rows"]
            if expected is not None and row.get("rows") != expected:
                entry["problems"].append("%s rows=%r expected=%d" % (
                    row["protocol"], row.get("rows"), expected))
            if row.get("samples") != SCENARIOS[scenario]["samples"]:
                entry["problems"].append("%s samples=%r expected=%d" % (
                    row["protocol"], row.get("samples"),
                    SCENARIOS[scenario]["samples"]))
        entry["rows"] = by_protocol
        for protocol in SCENARIOS[scenario]["protocols"]:
            if protocol not in by_protocol:
                entry["problems"].append("missing protocol " + protocol)

    # p99 is not in summary.json; recompute from the timed raw samples.
    timed = {}
    for line in (base / "raw.jsonl").read_text().splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            entry["problems"].append("bad raw line")
            continue
        if record.get("phase") != "timed":
            continue
        key = record.get("protocol")
        if SCENARIOS[scenario]["kind"] == "concurrency":
            key = (key, record.get("concurrency"))
        timed.setdefault(key, []).append(record["duration_ms"])
    entry["timed"] = timed

    stop_doc = json.loads((base / "launcher-stop.json").read_text())
    payload = stop_doc.get("payload")
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, dict):
        entry["problems"].append("launcher stop payload missing")
    else:
        if payload.get("outcome") != "clean":
            entry["problems"].append("stop=%r" % (payload.get("outcome"),))
        if payload.get("unresolved_operations"):
            entry["problems"].append("unresolved_operations=true")
    exit_file = base / "stop-exit.txt"
    if exit_file.exists() and exit_file.read_text().strip() != "0":
        entry["problems"].append("stop exit=%r" % exit_file.read_text().strip())
    return entry


def timed_values(data, scenario, leg, protocol, level=None):
    entry = data[(scenario, leg)]
    if entry["problems"]:
        return []
    key = (protocol, level) if level is not None else protocol
    return entry.get("timed", {}).get(key, [])


def side_values(data, scenario, round_name, side, protocol, key, level=None):
    values = []
    for leg in ROUNDS[round_name][side]:
        entry = data[(scenario, leg)]
        if entry["problems"]:
            continue
        if level is None:
            row = entry["rows"].get(protocol)
            if row is not None and row.get(key) is not None:
                values.append(row[key])
        else:
            row = entry["levels"].get(level)
            if row is not None and row.get(key) is not None:
                values.append(row[key])
    return values


def ratio_for(data, scenario, round_name, protocol, key, level=None):
    base = side_values(data, scenario, round_name, "A", protocol, key, level)
    cand = side_values(data, scenario, round_name, "B", protocol, key, level)
    if not base or not cand:
        return None, None, None
    base_med = med(base)
    cand_med = med(cand)
    return base_med, cand_med, cand_med / base_med if base_med else None


def pooled_ratio(data, scenario, protocol, key, level=None):
    base = []
    cand = []
    for round_name in ROUNDS:
        base += side_values(data, scenario, round_name, "A", protocol, key, level)
        cand += side_values(data, scenario, round_name, "B", protocol, key, level)
    if not base or not cand:
        return None, None, None
    base_med = med(base)
    cand_med = med(cand)
    return base_med, cand_med, cand_med / base_med if base_med else None


def verdict(round_ratios, pooled):
    if any(ratio is None for ratio in round_ratios) or pooled is None:
        return "UNCONFIRMED"
    if all(ratio > THRESHOLD for ratio in round_ratios) and pooled > THRESHOLD:
        return "FAIL"
    if all(ratio < IMPROVE for ratio in round_ratios) and pooled < IMPROVE:
        return "IMPROVED"
    return "PASS"


def main():
    data = {}
    for scenario in SCENARIOS:
        for sides in ROUNDS.values():
            for legs in sides.values():
                for leg in legs:
                    data[(scenario, leg)] = load_leg(scenario, leg)

    print("\n## 1. 逐腿（全部协议 / 并发档）\n")
    print("| 场景 | leg | 侧 | 协议/档 | 样本 | median | p95 | p99 | min | max | outcomes | rows |")
    print("|---|---|---|---|---:|---:|---:|---:|---:|---:|---|---:|")
    for scenario in SCENARIOS:
        for round_name, sides in ROUNDS.items():
            for side, legs in sides.items():
                for leg in legs:
                    entry = data[(scenario, leg)]
                    if entry["problems"]:
                        print("| %s | %s | %s | — | — | — | — | — | — | — | 无效 | — |"
                              % (scenario, leg, side))
                        continue
                    if SCENARIOS[scenario]["kind"] == "concurrency":
                        for level in (1, 4, 8, 16):
                            row = entry["levels"][level]
                            values = entry["timed"].get((SCENARIOS[scenario]["main"], level), [])
                            print("| %s | %s | %s | c%d | %d | %.3f | %.3f | %s | %.3f | %.3f | 429=%d | — |"
                                  % (scenario, leg, side, level, row["requests"],
                                     row["median_ms"], row["p95_ms"], fmt(pctl(values, 99)),
                                     row["min_ms"], row["max_ms"], row["status_429"]))
                    else:
                        for protocol in SCENARIOS[scenario]["protocols"]:
                            row = entry["rows"][protocol]
                            values = entry["timed"].get(protocol, [])
                            print("| %s | %s | %s | %s | %d | %.3f | %.3f | %s | %.3f | %.3f | %s | %d |"
                                  % (scenario, leg, side, protocol, row["samples"],
                                     row["median_ms"], row["p95_ms"], fmt(pctl(values, 99)),
                                     row["min_ms"], row["max_ms"],
                                     ",".join("%s=%d" % item for item in sorted(row["outcomes"].items())),
                                     row["rows"]))

    print("\n## 2. 腿有效性（冻结规则）\n")
    for scenario in SCENARIOS:
        for leg in (leg for sides in ROUNDS.values() for legs in sides.values() for leg in legs):
            entry = data[(scenario, leg)]
            status = "OK" if not entry["problems"] else "; ".join(entry["problems"])
            print("- %s/%s: %s" % (scenario, leg, status))

    print("\n## 3. 主指标（冻结判据：两轮比值都 >1.10 且合并 >1.10 才判 FAIL；"
          "两轮都 <0.90 且合并 <0.90 才判 IMPROVED）\n")
    print("| 场景 | 指标 | 轮 | 基线中位(ms) | 候选中位(ms) | 比值(候选/基线) |")
    print("|---|---|---|---:|---:|---:|")
    verdicts = {}
    for scenario, spec in SCENARIOS.items():
        targets = [(spec["main"], None)]
        if spec["kind"] == "concurrency":
            targets = [(spec["main"], level) for level in spec["levels"]]
        for protocol, level in targets:
            label = protocol if level is None else "c%d" % level
            round_ratios = []
            for round_name in ROUNDS:
                base, cand, ratio = ratio_for(data, scenario, round_name, protocol,
                                              "median_ms", level)
                round_ratios.append(ratio)
                print("| %s | %s | %s | %s | %s | %s |" % (
                    scenario, label, round_name, fmt(base), fmt(cand),
                    "—" if ratio is None else "%.3f" % ratio))
            base, cand, pooled = pooled_ratio(data, scenario, protocol, "median_ms", level)
            print("| %s | %s | 合并 | %s | %s | %s |" % (
                scenario, label, fmt(base), fmt(cand),
                "—" if pooled is None else "%.3f" % pooled))
            verdicts[(scenario, label)] = verdict(round_ratios, pooled)

    print("\n## 4. p95 / p99（报告项，不判）\n")
    print("| 场景 | 指标 | 轮 | 基线 p95 | 候选 p95 | 比值 | 基线 p99 | 候选 p99 | 比值 |")
    print("|---|---|---|---:|---:|---:|---:|---:|---:|")
    for scenario, spec in SCENARIOS.items():
        targets = [(spec["main"], None)]
        if spec["kind"] == "concurrency":
            targets = [(spec["main"], level) for level in spec["levels"]]
        for protocol, level in targets:
            label = protocol if level is None else "c%d" % level
            for round_name in ROUNDS:
                base95 = side_values(data, scenario, round_name, "A", protocol, "p95_ms", level)
                cand95 = side_values(data, scenario, round_name, "B", protocol, "p95_ms", level)
                base99 = pctl(sum((timed_values(data, scenario, leg, protocol, level)
                                   for leg in ROUNDS[round_name]["A"]), []), 99)
                cand99 = pctl(sum((timed_values(data, scenario, leg, protocol, level)
                                   for leg in ROUNDS[round_name]["B"]), []), 99)
                b95, c95 = med(base95), med(cand95)
                print("| %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
                    scenario, label, round_name, fmt(b95), fmt(c95),
                    "—" if not b95 else "%.3f" % (c95 / b95),
                    fmt(base99), fmt(cand99),
                    "—" if not base99 else "%.3f" % (cand99 / base99)))

    print("\n## 5. 次协议与字节（报告项，不判）\n")
    print("| 场景 | 协议 | 轮 | 基线中位 | 候选中位 | 比值 | 基线 p95 | 候选 p95 | 基线字节 | 候选字节 |")
    print("|---|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for scenario, spec in SCENARIOS.items():
        if spec["kind"] == "concurrency":
            continue
        for protocol in spec["protocols"]:
            for round_name in ROUNDS:
                base, cand, ratio = ratio_for(data, scenario, round_name, protocol, "median_ms")
                b95, c95, _ = ratio_for(data, scenario, round_name, protocol, "p95_ms")
                base_bytes = side_values(data, scenario, round_name, "A", protocol,
                                         "response_bytes")
                cand_bytes = side_values(data, scenario, round_name, "B", protocol,
                                         "response_bytes")
                print("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
                    scenario, protocol, round_name, fmt(base), fmt(cand),
                    "—" if ratio is None else "%.3f" % ratio, fmt(b95), fmt(c95),
                    fmt(med(base_bytes)), fmt(med(cand_bytes))))

    print("\n## 6. 判决\n")
    for (scenario, label), result in verdicts.items():
        print("- %s / %s: **%s**" % (scenario, label, result))
    failed = [key for key, result in verdicts.items() if result in ("FAIL", "UNCONFIRMED")]
    if failed:
        print("\n未通过/不可确认：%r" % (failed,))
        return 1 if any(verdicts[key] == "FAIL" for key in failed) else 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
