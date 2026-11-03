#!/usr/bin/env python3
"""Release A/B comparison: v1.1.27 image (base) vs same image + R37 platform (cand)."""
import json
import os
import statistics
import sys

ROOT = os.environ.get("REL_AB_ROOT", "/tmp/r37/rel-ab2")
SIDES = {"base": ["base-r1", "base-r2"], "cand": ["cand-r1", "cand-r2"]}


def load(side):
    runs = []
    for name in SIDES[side]:
        try:
            runs.append(json.load(open("%s/%s/results/summary.json" % (ROOT, name))))
        except FileNotFoundError:
            pass
    return runs


def key(row):
    return (row["scenario"], row["protocol"])


def main():
    data = {side: load(side) for side in SIDES}
    for side, runs in data.items():
        print(side, "runs:", len(runs), [r["metadata"]["duration_seconds"] for r in runs])

    base, cand = data["base"], data["cand"]
    if not base or not cand:
        print("missing side")
        return 1

    brows = {key(r): r for run in base for r in run["rows"]}
    crows = {key(r): r for run in cand for r in run["rows"]}
    order = [key(r) for r in base[0]["rows"]]
    print("\n## per-scenario")
    print("%-24s %-18s %8s %8s %8s %8s %8s %s" % (
        "scenario", "protocol", "A_med", "B_med", "delta%", "A_p95", "B_p95", "verdict/regression"))
    regressions = []
    improvements = []
    for k in order:
        a = brows.get(k)
        b = crows.get(k)
        if not a or not b:
            print("missing", k)
            continue
        a_rounds = []
        b_rounds = []
        for run in base:
            for row in run["rows"]:
                if key(row) == k:
                    a_rounds.extend(row["round_medians_ms"])
        for run in cand:
            for row in run["rows"]:
                if key(row) == k:
                    b_rounds.extend(row["round_medians_ms"])
        a_med = statistics.median(a_rounds)
        b_med = statistics.median(b_rounds)
        delta = (b_med - a_med) / a_med * 100.0
        a_p95 = max(r["p95_ms"] for run in base for r in run["rows"] if key(r) == k)
        b_p95 = max(r["p95_ms"] for run in cand for r in run["rows"] if key(r) == k)
        # same direction in both cycles?
        a_cycle = [statistics.median([r["median_ms"] for r in run["rows"] if key(r) == k]) for run in base]
        b_cycle = [statistics.median([r["median_ms"] for r in run["rows"] if key(r) == k]) for run in cand]
        directions = [(bc - ac) for ac, bc in zip(a_cycle, b_cycle)]
        consistent = all(d > 0 for d in directions) or all(d < 0 for d in directions)
        mark = ""
        if delta > 10 and consistent:
            mark = "REGRESSION"
            regressions.append((k, delta))
        elif delta < -10 and consistent:
            mark = "improve"
            improvements.append((k, delta))
        if a["rows"] != b["rows"] or a["response_bytes"] != b["response_bytes"]:
            mark += " INVARIANT"
        print("%-24s %-18s %8.2f %8.2f %+8.1f %8.2f %8.2f %s/%s %s" % (
            k[0], k[1], a_med, b_med, delta, a_p95, b_p95,
            a.get("verdict", ""), b.get("verdict", ""), mark))

    print("\n## concurrency")
    for level in [1, 4, 8, 16]:
        vals = {}
        for side, runs in data.items():
            rows = [c for run in runs for c in run["concurrency"] if c["level"] == level]
            vals[side] = rows
        if not vals["base"] or not vals["cand"]:
            continue
        def agg(rows, field):
            return statistics.median([r[field] for r in rows])
        a50, b50 = agg(vals["base"], "median_ms"), agg(vals["cand"], "median_ms")
        a95, b95 = agg(vals["base"], "p95_ms"), agg(vals["cand"], "p95_ms")
        a99, b99 = agg(vals["base"], "p99_ms"), agg(vals["cand"], "p99_ms")
        errs = sum(r["errors"] for r in vals["cand"])
        s429 = sum(r["status_429"] for r in vals["cand"])
        seats = max(r["session_seats"] for r in vals["cand"])
        print("c%-2d p50 %7.2f -> %7.2f (%+6.1f%%)  p95 %7.2f -> %7.2f (%+6.1f%%)  p99 %7.2f -> %7.2f (%+6.1f%%)  cand errors=%d 429=%d seats=%d" % (
            level, a50, b50, (b50 - a50) / a50 * 100, a95, b95, (b95 - a95) / a95 * 100,
            a99, b99, (b99 - a99) / a99 * 100, errs, s429, seats))

    print("\n## summary")
    print("regressions(>10%, consistent):", regressions or "none")
    print("improvements(>10%, consistent):", len(improvements), improvements[:12])

    print("\n## metadata")
    for side, runs in data.items():
        for run in runs:
            m = run["metadata"]
            print(side, m.get("git_revision"), m.get("peri_image_version"), m.get("platform_label"),
                  "dry/warmup/samples", m.get("rounds"), m.get("warmup"), m.get("samples"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
