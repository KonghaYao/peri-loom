"""R64 aggregate: frozen-criteria verdict for the R62 candidate re-evaluation.

Reads ./legs/<leg>/ (summary.json, raw.jsonl, metrics-before.json, metrics-after.json) and prints:
  1. per-leg table
  2. Gate 0 mechanism checks
  3. runtime invariants
  4. main metric per round and pooled, against the frozen thresholds
  5. guards, and c1 tail as report-only items

Usage: python3 aggregate.py [--dir <evidence dir>]
"""

import json
import math
import pathlib
import statistics
import sys

HERE = pathlib.Path(__file__).resolve().parent
ROUNDS = {
    "round1": {"A": ["r1a1", "r1a2"], "B": ["r1b1", "r1b2"]},
    "round2": {"A": ["r2a1", "r2a2"], "B": ["r2b1", "r2b2"]},
}
LEVELS = (1, 4, 8, 16)
MAIN_UNITS = [(8, "p95_ms"), (8, "p99_ms"), (16, "p95_ms"), (16, "p99_ms")]
GUARD_UNITS = [("c1", 1, "median_ms"), ("c4", 4, "median_ms"), ("c4", 4, "p95_ms")]


def load(leg):
    base = HERE / "legs" / leg
    summary = json.loads((base / "summary.json").read_text())
    before = json.loads((base / "metrics-before.json").read_text())
    after = json.loads((base / "metrics-after.json").read_text())
    raw = [json.loads(line) for line in (base / "raw.jsonl").read_text().splitlines()]
    raw = [r for r in raw if "request_id" in r and r.get("phase") == "timed"]
    levels = {c["level"]: c for c in summary["concurrency"]}
    return {"summary": summary, "before": before, "after": after, "raw": raw, "levels": levels}


def med(values):
    return statistics.median(values)


def fmt(value, unit=""):
    return f"{value:.2f}{unit}"


def main():
    data = {leg: load(leg) for side in ROUNDS.values() for legs in side.values() for leg in legs}

    print("\n## 1. 逐腿\n")
    print("| leg | 侧 | c1 p50/p95/p99 | c4 p50/p95 | c8 p50/p95/p99 | c16 p50/p95/p99 | errors | 429 |")
    print("|---|---|---|---|---|---|---:|---:|")
    for round_name, sides in ROUNDS.items():
        for side, legs in sides.items():
            for leg in legs:
                d = data[leg]
                lv = d["levels"]
                cell = lambda n: (f"{lv[n]['median_ms']:.2f}/{lv[n]['p95_ms']:.2f}/"
                                  f"{lv[n]['p99_ms']:.2f}")
                print(f"| {leg} | {side} | {cell(1)} | {lv[4]['median_ms']:.2f}/{lv[4]['p95_ms']:.2f} | "
                      f"{cell(8)} | {cell(16)} | {lv[16]['errors'] + lv[8]['errors']} | "
                      f"{sum(lv[n]['status_429'] for n in LEVELS)} |")

    print("\n## 2. Gate 0（机制核对）\n")
    gate_ok = True
    for round_name, sides in ROUNDS.items():
        for leg in sides["B"]:
            deferred = data[leg]["after"].get("deferred_durability", {})
            maximum = deferred.get("maximum")
            pending_max = deferred.get("pending_max")
            inline = deferred.get("inline_total")
            ok = maximum == 4096 and isinstance(pending_max, int) and pending_max > 256 and inline == 0
            gate_ok = gate_ok and ok
            print(f"- {leg}(B): maximum={maximum} pending_max={pending_max} inline_total={inline} "
                  f"=> {'OK' if ok else 'FAIL'}")
    for round_name, sides in ROUNDS.items():
        for leg in sides["A"]:
            present = "deferred_durability" in data[leg]["after"]
            gate_ok = gate_ok and not present
            print(f"- {leg}(A): deferred_durability present={present} => {'OK' if not present else 'FAIL'}")

    print("\n## 3. 运行性不变量\n")
    invariants_ok = True
    for leg, d in data.items():
        bad = []
        for level in LEVELS:
            cell = d["levels"][level]
            if cell["errors"] or cell["status_429"] or cell["rejection_reasons"]:
                bad.append(f"c{level}:errors={cell['errors']},429={cell['status_429']},"
                           f"rejections={cell['rejection_reasons']}")
        rows_bad = sum(1 for r in d["raw"] if r.get("rows") != 1)
        if rows_bad:
            bad.append(f"rows!=1:{rows_bad}")
        if not d["summary"]["fixture"].get("complete"):
            bad.append("fixture incomplete")
        stop_path = pathlib.Path(f"/tmp/peri-perf/run-r64-{leg}/launcher-stop.json")
        if stop_path.exists():
            payload = json.loads(json.loads(stop_path.read_text())["payload"])
            if payload.get("outcome") != "clean" or payload.get("unresolved_operations"):
                bad.append(f"stop={payload.get('outcome')}/{payload.get('unresolved_operations')}")
        else:
            bad.append("launcher-stop.json missing")
        invariants_ok = invariants_ok and not bad
        print(f"- {leg}: {'OK' if not bad else '; '.join(bad)}")

    print("\n## 4. 主指标（硬门：每轮四单元 <= 90%，两轮同向，合并 <= 90%）\n")
    print("| 轮 | 单元 | 基线中位 | 候选中位 | 比值 | ≤90% |")
    print("|---|---|---:|---:|---:|---|")
    main_pass = True
    for round_name, sides in ROUNDS.items():
        round_ok = True
        for level, key in MAIN_UNITS:
            base = med([data[leg]["levels"][level][key] for leg in sides["A"]])
            cand = med([data[leg]["levels"][level][key] for leg in sides["B"]])
            ratio = cand / base if base else float("inf")
            ok = ratio <= 0.90
            round_ok = round_ok and ok
            print(f"| {round_name} | c{level} {key[:3]} | {base:.2f} | {cand:.2f} | {ratio:.3f} | "
                  f"{'YES' if ok else 'NO'} |")
        main_pass = main_pass and round_ok
        print(f"| {round_name} | **本轮** | — | — | — | {'PASS' if round_ok else 'FAIL'} |")
    print("| 合并 | 单元 | 基线中位 | 候选中位 | 比值 | ≤90% |")
    print("|---|---|---:|---:|---:|---|")
    all_a = [leg for sides in ROUNDS.values() for leg in sides["A"]]
    all_b = [leg for sides in ROUNDS.values() for leg in sides["B"]]
    for level, key in MAIN_UNITS:
        base = med([data[leg]["levels"][level][key] for leg in all_a])
        cand = med([data[leg]["levels"][level][key] for leg in all_b])
        ratio = cand / base if base else float("inf")
        ok = ratio <= 0.90
        main_pass = main_pass and ok
        print(f"| 合并 | c{level} {key[:3]} | {base:.2f} | {cand:.2f} | {ratio:.3f} | "
              f"{'YES' if ok else 'NO'} |")

    print("\n## 5. 护栏\n")
    guards_pass = True
    print("| 护栏 | 轮 | 基线 | 候选 | 比值 | ≤110% |")
    print("|---|---|---:|---:|---:|---|")
    for label, level, key in GUARD_UNITS:
        for round_name, sides in ROUNDS.items():
            base = med([data[leg]["levels"][level][key] for leg in sides["A"]])
            cand = med([data[leg]["levels"][level][key] for leg in sides["B"]])
            ratio = cand / base if base else float("inf")
            ok = ratio <= 1.10
            guards_pass = guards_pass and ok
            print(f"| {label} {key[:3]} | {round_name} | {base:.2f} | {cand:.2f} | {ratio:.3f} | "
                  f"{'YES' if ok else 'NO'} |")
        base = med([data[leg]["levels"][level][key] for leg in all_a])
        cand = med([data[leg]["levels"][level][key] for leg in all_b])
        ratio = cand / base if base else float("inf")
        ok = ratio <= 1.10
        guards_pass = guards_pass and ok
        print(f"| {label} {key[:3]} | 合并 | {base:.2f} | {cand:.2f} | {ratio:.3f} | "
              f"{'YES' if ok else 'NO'} |")

    print("\n### 报告项（不判）：c1 尾部\n")
    print("| leg | 侧 | p95 | p99 | max | >2ms |")
    print("|---|---|---:|---:|---:|---:|")
    for round_name, sides in ROUNDS.items():
        for side, legs in sides.items():
            for leg in legs:
                c1 = data[leg]["levels"][1]
                spikes = sum(1 for r in data[leg]["raw"]
                             if r.get("concurrency") == 1 and r["duration_ms"] > 2)
                print(f"| {leg} | {side} | {c1['p95_ms']:.2f} | {c1['p99_ms']:.2f} | "
                      f"{c1['max_ms']:.2f} | {spikes} |")
    for side, legs in (("A", all_a), ("B", all_b)):
        spikes = sum(1 for leg in legs for r in data[leg]["raw"]
                     if r.get("concurrency") == 1 and r["duration_ms"] > 2)
        print(f"- 合并 {side}: c1 >2ms = {spikes}/200")

    print("\n## 6. 判决输入\n")
    print(f"- Gate 0: {'PASS' if gate_ok else 'FAIL'}")
    print(f"- 运行性不变量: {'PASS' if invariants_ok else 'FAIL'}")
    print(f"- 主指标: {'PASS' if main_pass else 'FAIL'}")
    print(f"- 护栏: {'PASS' if guards_pass else 'FAIL'}")
    verdict = gate_ok and invariants_ok and main_pass and guards_pass
    print(f"- **总判决: {'合入候选' if verdict else '不合入'}**")
    return 0 if verdict else 1


if __name__ == "__main__":
    sys.exit(main())
