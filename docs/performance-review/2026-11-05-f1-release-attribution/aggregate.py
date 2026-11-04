#!/usr/bin/env python3
"""F1.select-1 / json-pipeline release attribution aggregate.

Adapted from docs/performance-review/2026-11-04-v1.2.7-release/aggregate.py.
Frozen sections 1-5 are preserved (same validity checks, same FAIL/IMPROVED
thresholds). The only parameter changes are:
  * the window contains only F1.select-1 / json-pipeline (task scope);
  * each leg carries 20 timed samples instead of the frozen 5 (documented
    deviation in perf-protocol.md, verdict rule unchanged).

Section 6 adds the task's pre-registered reproducibility criteria:
  reproducible = both round ratios > 1.10
                 AND pooled ratio > 1.10
                 AND each round's two sides either have disjoint sample ranges
                     or differ by more than 3x the within-leg standard deviation.
Section 7 prints the resulting verdict.
"""

import json
import math
import pathlib
import random
import statistics
import sys

HERE = pathlib.Path(__file__).resolve().parent
THRESHOLD = 1.10
IMPROVE = 0.90
SEED = 20261105
SAMPLES = 20
EXPECTED_FIXTURE = {"threads": 201, "messages": 8625, "content_chars": 20177069}
ROUNDS = {
    "round1": {"A": ("r1a1", "r1a2"), "B": ("r1b1", "r1b2")},
    "round2": {"A": ("r2a1", "r2a2"), "B": ("r2b1", "r2b2")},
}
SCENARIOS = {
    "F1.select-1": {
        "protocols": ("json-pipeline",),
        "main": "json-pipeline",
        "samples": SAMPLES,
        "expected_rows": 1,
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


def fmt(value, digits=3):
    return "—" if value is None else ("%%.%df" % digits) % value


def stdev(values):
    return statistics.stdev(values) if len(values) > 1 else 0.0


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

    timed = {}
    for line in (base / "raw.jsonl").read_text().splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            entry["problems"].append("bad raw line")
            continue
        if record.get("phase") != "timed":
            continue
        timed.setdefault(record.get("protocol"), []).append(record["duration_ms"])
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


def timed_values(data, scenario, leg, protocol):
    entry = data[(scenario, leg)]
    if entry["problems"]:
        return []
    return entry.get("timed", {}).get(protocol, [])


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


def side_samples(data, scenario, round_name, side, protocol):
    values = []
    for leg in ROUNDS[round_name][side]:
        values += timed_values(data, scenario, leg, protocol)
    return values


def side_leg_samples(data, scenario, round_name, side, protocol):
    return [(leg, timed_values(data, scenario, leg, protocol))
            for leg in ROUNDS[round_name][side]]


def ratio_for(data, scenario, round_name, protocol, key):
    base = side_values(data, scenario, round_name, "A", protocol, key)
    cand = side_values(data, scenario, round_name, "B", protocol, key)
    if not base or not cand:
        return None, None, None
    base_med = med(base)
    cand_med = med(cand)
    return base_med, cand_med, cand_med / base_med if base_med else None


def pooled_ratio(data, scenario, protocol, key):
    base = []
    cand = []
    for round_name in ROUNDS:
        base += side_values(data, scenario, round_name, "A", protocol, key)
        cand += side_values(data, scenario, round_name, "B", protocol, key)
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


def pooled_within_leg_sd(leg_samples):
    """Pooled within-leg standard deviation across the legs of one side."""
    numerator, denominator = 0.0, 0
    for _, values in leg_samples:
        if len(values) < 2:
            continue
        numerator += (len(values) - 1) * statistics.variance(values)
        denominator += len(values) - 1
    return math.sqrt(numerator / denominator) if denominator else 0.0


def permutation_pvalue(a, b, permutations=20000):
    """Two-sided permutation test on the difference of medians (b - a)."""
    observed = statistics.median(b) - statistics.median(a)
    pool = list(a) + list(b)
    n_a = len(a)
    rng = random.Random(SEED)
    hits = 0
    for _ in range(permutations):
        rng.shuffle(pool)
        diff = statistics.median(pool[n_a:]) - statistics.median(pool[:n_a])
        if abs(diff) >= abs(observed) - 1e-12:
            hits += 1
    return (hits + 1) / (permutations + 1), observed


def bootstrap_median_diff(a, b, resamples=10000):
    rng = random.Random(SEED + 1)
    diffs = []
    for _ in range(resamples):
        sample_a = [a[rng.randrange(len(a))] for _ in range(len(a))]
        sample_b = [b[rng.randrange(len(b))] for _ in range(len(b))]
        diffs.append(statistics.median(sample_b) - statistics.median(sample_a))
    diffs.sort()
    return diffs[int(0.025 * len(diffs))], diffs[int(0.975 * len(diffs)) - 1]


def describe(values):
    return {
        "n": len(values),
        "min": min(values),
        "p25": pctl(values, 25),
        "median": med(values),
        "p75": pctl(values, 75),
        "max": max(values),
    }


def main():
    data = {}
    for scenario in SCENARIOS:
        for sides in ROUNDS.values():
            for legs in sides.values():
                for leg in legs:
                    data[(scenario, leg)] = load_leg(scenario, leg)

    print("\n## 1. 逐腿（json-pipeline）\n")
    print("| 场景 | leg | 侧 | 样本 | median | p95 | p99 | min | max | outcomes | rows |")
    print("|---|---|---|---:|---:|---:|---:|---:|---:|---|---:|")
    for scenario in SCENARIOS:
        for round_name, sides in ROUNDS.items():
            for side, legs in sides.items():
                for leg in legs:
                    entry = data[(scenario, leg)]
                    if entry["problems"]:
                        print("| %s | %s | %s | — | — | — | — | — | — | 无效 | — |"
                              % (scenario, leg, side))
                        continue
                    row = entry["rows"]["json-pipeline"]
                    values = entry["timed"].get("json-pipeline", [])
                    print("| %s | %s | %s | %d | %.3f | %.3f | %s | %.3f | %.3f | %s | %d |"
                          % (scenario, leg, side, row["samples"], row["median_ms"],
                             row["p95_ms"], fmt(pctl(values, 99)), row["min_ms"],
                             row["max_ms"],
                             ",".join("%s=%d" % item for item in sorted(row["outcomes"].items())),
                             row["rows"]))

    print("\n## 2. 腿有效性（冻结规则）\n")
    for scenario in SCENARIOS:
        for leg in (leg for sides in ROUNDS.values() for legs in sides.values() for leg in legs):
            entry = data[(scenario, leg)]
            status = "OK" if not entry["problems"] else "; ".join(entry["problems"])
            print("- %s/%s: %s" % (scenario, leg, status))

    print("\n## 3. 主指标（冻结判据：两轮比值都 >1.10 且合并 >1.10 才判 FAIL）\n")
    print("| 场景 | 指标 | 轮 | 基线中位(ms) | 候选中位(ms) | 比值(候选/基线) |")
    print("|---|---|---|---:|---:|---:|")
    verdicts = {}
    round_ratios = {}
    for scenario, spec in SCENARIOS.items():
        protocol = spec["main"]
        ratios = []
        for round_name in ROUNDS:
            base, cand, ratio = ratio_for(data, scenario, round_name, protocol, "median_ms")
            ratios.append(ratio)
            print("| %s | %s | %s | %s | %s | %s |" % (
                scenario, protocol, round_name, fmt(base), fmt(cand),
                "—" if ratio is None else "%.3f" % ratio))
        base, cand, pooled = pooled_ratio(data, scenario, protocol, "median_ms")
        print("| %s | %s | 合并 | %s | %s | %s |" % (
            scenario, protocol, fmt(base), fmt(cand),
            "—" if pooled is None else "%.3f" % pooled))
        verdicts[(scenario, protocol)] = verdict(ratios, pooled)
        round_ratios[(scenario, protocol)] = (ratios, pooled)

    print("\n## 4. p95 / p99（报告项，不判）\n")
    print("| 场景 | 指标 | 轮 | 基线 p95 | 候选 p95 | 比值 | 基线 p99 | 候选 p99 | 比值 |")
    print("|---|---|---|---:|---:|---:|---:|---:|---:|")
    for scenario, spec in SCENARIOS.items():
        protocol = spec["main"]
        for round_name in ROUNDS:
            base95 = side_values(data, scenario, round_name, "A", protocol, "p95_ms")
            cand95 = side_values(data, scenario, round_name, "B", protocol, "p95_ms")
            base99 = pctl(sum((timed_values(data, scenario, leg, protocol)
                               for leg in ROUNDS[round_name]["A"]), []), 99)
            cand99 = pctl(sum((timed_values(data, scenario, leg, protocol)
                               for leg in ROUNDS[round_name]["B"]), []), 99)
            b95, c95 = med(base95), med(cand95)
            print("| %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
                scenario, protocol, round_name, fmt(b95), fmt(c95),
                "—" if not b95 else "%.3f" % (c95 / b95),
                fmt(base99), fmt(cand99),
                "—" if not base99 else "%.3f" % (cand99 / base99)))

    print("\n## 5. 字节与逐量计数（报告项，不判）\n")
    print("| 场景 | 轮 | 基线字节 | 候选字节 | 基线 samples | 候选 samples |")
    print("|---|---|---:|---:|---:|---:|")
    for scenario, spec in SCENARIOS.items():
        protocol = spec["main"]
        for round_name in ROUNDS:
            base_bytes = side_values(data, scenario, round_name, "A", protocol, "response_bytes")
            cand_bytes = side_values(data, scenario, round_name, "B", protocol, "response_bytes")
            base_n = side_values(data, scenario, round_name, "A", protocol, "samples")
            cand_n = side_values(data, scenario, round_name, "B", protocol, "samples")
            print("| %s | %s | %s | %s | %s | %s |" % (
                scenario, round_name, fmt(med(base_bytes), 2), fmt(med(cand_bytes), 2),
                ",".join(str(int(item)) for item in base_n),
                ",".join(str(int(item)) for item in cand_n)))

    print("\n## 6. 可复现性判据（本任务预注册，样本级）\n")
    reproducible_rounds = []
    for scenario, spec in SCENARIOS.items():
        protocol = spec["main"]
        for round_name in ROUNDS:
            a = side_samples(data, scenario, round_name, "A", protocol)
            b = side_samples(data, scenario, round_name, "B", protocol)
            a_legs = side_leg_samples(data, scenario, round_name, "A", protocol)
            b_legs = side_leg_samples(data, scenario, round_name, "B", protocol)
            if not a or not b:
                reproducible_rounds.append(False)
                print("### %s %s: 样本缺失" % (scenario, round_name))
                continue
            da, db = describe(a), describe(b)
            sd_a = pooled_within_leg_sd(a_legs)
            sd_b = pooled_within_leg_sd(b_legs)
            sd = max(sd_a, sd_b)
            diff = db["median"] - da["median"]
            non_overlap = (max(a) < min(b)) or (max(b) < min(a))
            gt3sd = abs(diff) > 3.0 * sd
            ratio = ratio_for(data, scenario, round_name, protocol, "median_ms")[2]
            p_value, observed = permutation_pvalue(a, b)
            ci_low, ci_high = bootstrap_median_diff(a, b)
            reproducible_rounds.append(bool(ratio is not None and ratio > THRESHOLD
                                            and (non_overlap or gt3sd)))
            print("### %s %s" % (scenario, round_name))
            print("- A(n=%d): min=%.3f p25=%.3f median=%.3f p75=%.3f max=%.3f sd_within_leg=%.3f"
                  % (da["n"], da["min"], da["p25"], da["median"], da["p75"], da["max"], sd_a))
            print("- B(n=%d): min=%.3f p25=%.3f median=%.3f p75=%.3f max=%.3f sd_within_leg=%.3f"
                  % (db["n"], db["min"], db["p25"], db["median"], db["p75"], db["max"], sd_b))
            print("- leg_medians A=%s" % ",".join("%.3f" % med(v) for _, v in a_legs if v))
            print("- leg_medians B=%s" % ",".join("%.3f" % med(v) for _, v in b_legs if v))
            print("- median_diff(B-A)=%.3f ms; non_overlap=%s; 3*sd=%.3f; >3sd=%s; round_ratio=%s"
                  % (diff, non_overlap, 3.0 * sd, gt3sd,
                     "—" if ratio is None else "%.3f" % ratio))
            print("- permutation_p=%.5f (observed diff %.3f ms); bootstrap_95%%CI=[%.3f, %.3f]"
                  % (p_value, observed, ci_low, ci_high))

        ratios, pooled = round_ratios[(scenario, protocol)]
        same_direction = len(ratios) == 2 and all(item is not None for item in ratios) \
            and (ratios[0] > 1.0) == (ratios[1] > 1.0)
        all_above = len(ratios) == 2 and all(item is not None and item > THRESHOLD
                                             for item in ratios)
        reproducible = (same_direction and all_above and pooled is not None
                        and pooled > THRESHOLD and all(reproducible_rounds))
        noise_dominant = (pooled is not None and 0.95 <= pooled <= 1.10) or not same_direction
        print("\n### 汇总")
        print("- round_ratios=%s pooled=%s" % (
            ",".join("—" if item is None else "%.3f" % item for item in ratios),
            "—" if pooled is None else "%.3f" % pooled))
        print("- same_direction=%s all_rounds_above_1.10=%s"
              % (same_direction, all_above))
        print("- per_round_separation=%s" % reproducible_rounds)
        if reproducible:
            label = "可复现（候选更慢）"
        elif noise_dominant:
            label = "不可复现/噪声主导"
        else:
            label = "部分复现"
        print("- **判决：%s**" % label)

    print("\n## 7. 冻结判决（与 2026-11-04 报告同规则）\n")
    for (scenario, label), result in verdicts.items():
        print("- %s / %s: **%s**" % (scenario, label, result))
    failed = [key for key, result in verdicts.items() if result in ("FAIL", "UNCONFIRMED")]
    if failed:
        print("\n未通过/不可确认：%r" % (failed,))
        return 1 if any(verdicts[key] == "FAIL" for key in failed) else 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
