#!/usr/bin/env python3
"""R65 aggregate: frozen-criteria regression verdict for the post-R64 merge gate.

Reads legs/<scenario>/<leg>/ (summary.json, metrics-before.json, metrics-after.json,
launcher-stop.json) and prints:

  1. per-leg rows for every protocol
  2. leg validity (invariants)
  3. main metric per round and pooled against the frozen 1.10 regression threshold
  4. secondary protocols, p95, query_ms and response_bytes (report-only)
  5. mechanism snapshot (deferred_durability on B legs only)
  6. verdict

Exit code: 0 = no regression (PASS), 1 = regression (FAIL), 2 = unconfirmable.
"""

import json
import pathlib
import statistics
import sys

HERE = pathlib.Path(__file__).resolve().parent
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
    },
    "F1.select-1": {
        "protocols": ("json-pipeline", "protobuf-pipeline", "json-cursor", "protobuf-cursor"),
        "main": "json-pipeline",
        "samples": 5,
        "expected_rows": 1,
    },
    "H1.content-20mb": {
        "protocols": ("json-cursor", "protobuf-cursor"),
        "main": "json-cursor",
        "samples": 3,
        "expected_rows": 8625,
    },
}
THRESHOLD = 1.10


def load_leg(scenario, leg):
    base = HERE / "legs" / scenario / leg
    entry = {"dir": base, "scenario": scenario, "leg": leg, "problems": [], "rows": {}}
    for name in ("summary.json", "metrics-before.json", "metrics-after.json", "launcher-stop.json"):
        if not (base / name).exists():
            entry["problems"].append("missing " + name)
    if entry["problems"]:
        return entry
    summary = json.loads((base / "summary.json").read_text())
    entry["summary"] = summary
    entry["before"] = json.loads((base / "metrics-before.json").read_text())
    entry["after"] = json.loads((base / "metrics-after.json").read_text())
    fixture = summary.get("fixture") or {}
    if not fixture.get("complete"):
        entry["problems"].append("fixture incomplete")
    if summary.get("warnings"):
        entry["problems"].append("warnings: %r" % (summary["warnings"],))
    if summary.get("fatal"):
        entry["problems"].append("fatal: %r" % (summary["fatal"],))
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
            entry["problems"].append(
                "%s rows=%r expected=%d" % (row["protocol"], row.get("rows"), expected))
        if row.get("samples") != SCENARIOS[scenario]["samples"]:
            entry["problems"].append(
                "%s samples=%r expected=%d" % (row["protocol"], row.get("samples"),
                                                SCENARIOS[scenario]["samples"]))
    for protocol in SCENARIOS[scenario]["protocols"]:
        if protocol not in by_protocol:
            entry["problems"].append("missing protocol " + protocol)
    entry["rows"] = by_protocol
    doc = json.loads((base / "launcher-stop.json").read_text())
    payload = json.loads(doc["payload"])
    if payload.get("outcome") != "clean" or payload.get("unresolved_operations"):
        entry["problems"].append("stop=%r unresolved=%r" % (
            payload.get("outcome"), payload.get("unresolved_operations")))
    return entry


def med(values):
    return statistics.median(values)


def fmt(value):
    return "—" if value is None else "%.3f" % value


def side_values(data, scenario, round_name, side, protocol, key):
    values = []
    for leg in ROUNDS[round_name][side]:
        entry = data[(scenario, leg)]
        if entry["problems"]:
            continue
        row = entry["rows"].get(protocol)
        if row is not None and row.get(key) is not None:
            values.append(row[key])
    return values


def ratio_for(data, scenario, round_name, protocol, key):
    base = side_values(data, scenario, round_name, "A", protocol, key)
    cand = side_values(data, scenario, round_name, "B", protocol, key)
    if not base or not cand:
        return None, None, None
    base_med = med(base)
    cand_med = med(cand)
    return base_med, cand_med, cand_med / base_med if base_med else float("inf")


def main():
    data = {}
    for scenario in SCENARIOS:
        for sides in ROUNDS.values():
            for legs in sides.values():
                for leg in legs:
                    data[(scenario, leg)] = load_leg(scenario, leg)

    print("\n## 1. 逐腿（全部协议）\n")
    print("| 场景 | leg | 侧 | 协议 | 样本 | median | p95 | min | max | outcomes | rows |")
    print("|---|---|---|---|---:|---:|---:|---:|---:|---|---:|")
    for scenario in SCENARIOS:
        for round_name, sides in ROUNDS.items():
            for side, legs in sides.items():
                for leg in legs:
                    entry = data[(scenario, leg)]
                    if entry["problems"]:
                        print("| %s | %s | %s | — | — | — | — | — | — | 无效 | — |"
                              % (scenario, leg, side))
                        continue
                    for protocol in SCENARIOS[scenario]["protocols"]:
                        row = entry["rows"][protocol]
                        print("| %s | %s | %s | %s | %d | %.3f | %.3f | %.3f | %.3f | %s | %d |"
                              % (scenario, leg, side, protocol, row["samples"], row["median_ms"],
                                 row["p95_ms"], row["min_ms"], row["max_ms"],
                                 ",".join("%s=%d" % item for item in sorted(row["outcomes"].items())),
                                 row["rows"]))

    print("\n## 2. 腿有效性（冻结规则）\n")
    for scenario in SCENARIOS:
        for leg in (leg for sides in ROUNDS.values() for legs in sides.values() for leg in legs):
            entry = data[(scenario, leg)]
            status = "OK" if not entry["problems"] else "; ".join(entry["problems"])
            print("- %s/%s: %s" % (scenario, leg, status))

    print("\n## 3. 主指标（冻结判据：两轮比值都 >1.10 且合并 >1.10 才判回归）\n")
    print("| 场景 | 轮 | 基线中位(ms) | 候选中位(ms) | 比值(候选/基线) |")
    print("|---|---|---:|---:|---:|")
    round_results = {}
    pooled_results = {}
    for scenario in SCENARIOS:
        main_protocol = SCENARIOS[scenario]["main"]
        for round_name in ROUNDS:
            base, cand, ratio = ratio_for(data, scenario, round_name, main_protocol, "median_ms")
            round_results[(scenario, round_name)] = ratio if ratio is not None else None
            print("| %s | %s | %s | %s | %s |" % (
                scenario, round_name, fmt(base), fmt(cand),
                "—" if ratio is None else "%.3f" % ratio))
        pooled_base = side_values(data, scenario, "round1", "A", main_protocol, "median_ms") + \
            side_values(data, scenario, "round2", "A", main_protocol, "median_ms")
        pooled_cand = side_values(data, scenario, "round1", "B", main_protocol, "median_ms") + \
            side_values(data, scenario, "round2", "B", main_protocol, "median_ms")
        if pooled_base and pooled_cand:
            pooled_ratio = med(pooled_cand) / med(pooled_base)
        else:
            pooled_ratio = None
        pooled_results[scenario] = pooled_ratio
        print("| %s | 合并 | %s | %s | %s |" % (
            scenario, fmt(med(pooled_base)) if pooled_base else "—",
            fmt(med(pooled_cand)) if pooled_cand else "—",
            "—" if pooled_ratio is None else "%.3f" % pooled_ratio))

    print("\n### 主指标 p95（报告项，不判）\n")
    print("| 场景 | 轮 | 基线 p95 | 候选 p95 | 比值 |")
    print("|---|---|---:|---:|---:|")
    for scenario in SCENARIOS:
        main_protocol = SCENARIOS[scenario]["main"]
        for round_name in ROUNDS:
            base, cand, ratio = ratio_for(data, scenario, round_name, main_protocol, "p95_ms")
            print("| %s | %s | %s | %s | %s |" % (
                scenario, round_name, fmt(base), fmt(cand),
                "—" if ratio is None else "%.3f" % ratio))

    print("\n## 4. 次协议与字节（报告项，不判）\n")
    print("| 场景 | 协议 | 轮 | 基线中位 | 候选中位 | 比值 | 基线 p95 | 候选 p95 |")
    print("|---|---|---|---:|---:|---:|---:|---:|")
    for scenario in SCENARIOS:
        for protocol in SCENARIOS[scenario]["protocols"]:
            for round_name in ROUNDS:
                base, cand, ratio = ratio_for(data, scenario, round_name, protocol, "median_ms")
                pbase, pcand, _ = ratio_for(data, scenario, round_name, protocol, "p95_ms")
                print("| %s | %s | %s | %s | %s | %s | %s | %s |" % (
                    scenario, protocol, round_name, fmt(base), fmt(cand),
                    "—" if ratio is None else "%.3f" % ratio, fmt(pbase), fmt(pcand)))
    print("\n| 场景 | 协议 | 基线 query_ms | 候选 query_ms | 基线 response_bytes | 候选 response_bytes |")
    print("|---|---|---:|---:|---:|---:|")
    for scenario in SCENARIOS:
        for protocol in SCENARIOS[scenario]["protocols"]:
            qb = [data[(scenario, leg)]["rows"][protocol]["query_ms"]
                  for round_name in ROUNDS for leg in ROUNDS[round_name]["A"]
                  if not data[(scenario, leg)]["problems"]]
            qc = [data[(scenario, leg)]["rows"][protocol]["query_ms"]
                  for round_name in ROUNDS for leg in ROUNDS[round_name]["B"]
                  if not data[(scenario, leg)]["problems"]]
            bb = [data[(scenario, leg)]["rows"][protocol]["response_bytes"]
                  for round_name in ROUNDS for leg in ROUNDS[round_name]["A"]
                  if not data[(scenario, leg)]["problems"]]
            bc = [data[(scenario, leg)]["rows"][protocol]["response_bytes"]
                  for round_name in ROUNDS for leg in ROUNDS[round_name]["B"]
                  if not data[(scenario, leg)]["problems"]]
            qb = [v for v in qb if v is not None]
            qc = [v for v in qc if v is not None]
            print("| %s | %s | %s | %s | %s | %s |" % (
                scenario, protocol,
                fmt(med(qb)) if qb else "—",
                fmt(med(qc)) if qc else "—",
                "—" if not bb else "%.0f" % med(bb),
                "—" if not bc else "%.0f" % med(bc)))

    print("\n## 5. 机制快照（B 腿应为合入后的 4096 上限，A 腿无该指标）\n")
    print("| 场景 | leg | 侧 | deferred_durability |")
    print("|---|---|---|---|")
    for scenario in SCENARIOS:
        for round_name, sides in ROUNDS.items():
            for side, legs in sides.items():
                for leg in legs:
                    entry = data[(scenario, leg)]
                    if entry["problems"]:
                        continue
                    deferred = entry["after"].get("deferred_durability")
                    if deferred is None:
                        text = "absent"
                    else:
                        text = "maximum=%s pending_max=%s inline_total=%s" % (
                            deferred.get("maximum"), deferred.get("pending_max"),
                            deferred.get("inline_total"))
                    print("| %s | %s | %s | %s |" % (scenario, leg, side, text))

    print("\n## 6. 判决输入\n")
    unconfirmable = False
    regression = False
    for scenario in SCENARIOS:
        ratios = [round_results[(scenario, round_name)] for round_name in ROUNDS]
        valid_rounds = [value for value in ratios if value is not None]
        if len(valid_rounds) < 2:
            status = "不可确认（有效轮次 %d/2）" % len(valid_rounds)
            unconfirmable = True
        else:
            both = all(value > THRESHOLD for value in valid_rounds)
            pooled = pooled_results[scenario]
            hit = both and pooled is not None and pooled > THRESHOLD
            regression = regression or hit
            status = ("回归 FAIL（两轮与合并均 >1.10）" if hit else "无回归 PASS")
        print("- %s: 轮次比值 %s，合并比值 %s ⇒ %s" % (
            scenario,
            ", ".join("—" if value is None else "%.3f" % value for value in ratios),
            "—" if pooled_results[scenario] is None else "%.3f" % pooled_results[scenario],
            status))
    if unconfirmable:
        print("- **总判决: 不可确认**")
        return 2
    if regression:
        print("- **总判决: 回归 FAIL**")
        return 1
    print("- **总判决: 无回归 PASS**")
    return 0


if __name__ == "__main__":
    sys.exit(main())
