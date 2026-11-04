#!/usr/bin/env python3
"""Summarize the batch regression gate: medians per side and per scenario.

For X1.concurrency the harness reports one row per concurrency level; each row is
summarized separately (name includes the concurrency level).
"""
import json
import statistics as st
import sys
from pathlib import Path

root = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/peri-perf/batchreg")
groups = {}
for summary in sorted(root.glob("*/summary.json")):
    data = json.loads(summary.read_text())
    leg_dir = summary.parent.name
    side = "base" if ".base" in leg_dir else "cand"
    if data.get("rows"):
        for row in data["rows"]:
            name = f"{row['scenario']} {row['protocol']}"
            level = row.get("concurrency") or row.get("concurrency_level")
            if level is not None:
                name += f" c={level}"
            groups.setdefault(name, {}).setdefault(side, []).append(row["median_ms"])
            groups[name].setdefault("meta", {}).setdefault(side, []).append(
                (row.get("response_bytes"), row.get("status_counts"), row.get("verdict"))
            )
    # X1 keeps its per-level rows in a top-level "concurrency" array.
    for row in data.get("concurrency") or []:
        name = "X1.concurrency json-pipeline c=%s" % row["level"]
        groups.setdefault(name, {}).setdefault(side, []).append(row["median_ms"])
        groups[name].setdefault("meta", {}).setdefault(side, []).append(
            (None, {"200": row.get("requests", 0) - row.get("errors", 0)}, row.get("verdict"))
        )

flag = False
for name in sorted(groups):
    sides = groups[name]
    line = [name]
    for side in ("base", "cand"):
        vals = [v for v in sides.get(side, []) if v is not None]
        if vals:
            line.append(f"{side} n={len(vals)} median={st.median(vals):.2f}")
    if sides.get("base") and sides.get("cand"):
        b = st.median(sides["base"])
        c = st.median(sides["cand"])
        if b:
            delta = 100 * (c - b) / b
            line.append(f"delta={delta:+.2f}%")
            if delta > 10:
                line.append("REGRESSION?")
                flag = True
    print("  ".join(line))
    for side in ("base", "cand"):
        meta = sides.get("meta", {}).get(side, [])
        if meta:
            bytes_set = sorted({m[0] for m in meta if m[0] is not None})
            statuses = [m[1] for m in meta]
            verdicts = sorted({str(m[2]) for m in meta if m[2] is not None})
            bad = [s for s in statuses if s and any(code != "200" for code in s)]
            print(f"    {side}: response_bytes={bytes_set} non200={bad} verdicts={verdicts}")

print("\nregression gate:", "REVIEW >10% cells" if flag else "no cell >10%")
