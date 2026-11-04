#!/usr/bin/env python3
"""Summarize the R52 regression gate: medians per side and per scenario."""
import json
import statistics as st
import sys
from pathlib import Path

root = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/peri-perf/r52reg")
groups = {}
for summary in sorted(root.glob("*/summary.json")):
    data = json.loads(summary.read_text())
    leg_dir = summary.parent.name
    side = "base" if ".base" in leg_dir else "cand"
    for row in data.get("rows", []):
        name = f"{row['scenario']} {row['protocol']}"
        groups.setdefault(name, {}).setdefault(side, []).append(row["median_ms"])
        groups[name].setdefault("meta", {}).setdefault(side, []).append(
            (row.get("response_bytes"), row.get("status_counts"), row.get("verdict"))
        )

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
            line.append(f"delta={100*(c-b)/b:+.2f}%")
    print("  ".join(line))
    for side in ("base", "cand"):
        meta = sides.get("meta", {}).get(side, [])
        if meta:
            bytes_set = sorted({m[0] for m in meta if m[0] is not None})
            statuses = [m[1] for m in meta]
            bad = [s for s in statuses if s and any(code != "200" for code in s)]
            print(f"    {side}: response_bytes={bytes_set} non200={bad}")
