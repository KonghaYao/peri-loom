#!/usr/bin/env python3
"""Parse the R74WORKER segment summaries and write segmentation.json."""
from __future__ import annotations

import json
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent
LEG = ROOT / "stage-b" / "legs" / "segmentation"


def main() -> int:
    text = ""
    for path in sorted(LEG.glob("worker-*.log")):
        text += path.read_text(encoding="utf-8", errors="replace")
    milestones = []
    current = None
    for line in text.splitlines():
        match = re.search(r"milestone=(\d+) started=(\d+) completed=(\d+)", line)
        if match:
            if current:
                milestones.append(current)
            current = {
                "milestone": int(match.group(1)),
                "started": int(match.group(2)),
                "completed": int(match.group(3)),
                "segments": {},
            }
            continue
        match = re.search(
            r"seg=(\S+) n=(\d+) mean_us=([\d.]+) p50_us=([\d.]+) p95_us=([\d.]+)", line)
        if match and current is not None:
            current["segments"][match.group(1)] = {
                "n": int(match.group(2)),
                "mean_us": float(match.group(3)),
                "p50_us": float(match.group(4)),
                "p95_us": float(match.group(5)),
            }
    if current:
        milestones.append(current)
    if not milestones:
        raise SystemExit("no R74WORKER milestone found")
    last = milestones[-1]
    segments = last["segments"]
    total = segments["open_total"]["p50_us"]
    authority = segments["validate_authority"]["p50_us"]
    non_authority = total - authority
    connect_configure = segments["connect"]["p50_us"] + segments["configure"]["p50_us"]
    stage_a_f1_p50_ns = 1_096_792
    document = {
        "milestones": [entry["milestone"] for entry in milestones],
        "last": last,
        "count_assertion": {
            "open_total_n": segments["open_total"]["n"],
            "completed": last["completed"],
            "all_segment_counts_equal_completed": all(
                item["n"] == last["completed"] for item in segments.values()),
        },
        "derived": {
            "open_total_p50_us": total,
            "validate_authority_p50_us": authority,
            "validate_authority_share": authority / total,
            "non_authority_p50_us": non_authority,
            "connect_plus_configure_p50_us": connect_configure,
            "stage_a_f1_p50_ns": stage_a_f1_p50_ns,
            "connect_configure_share_of_stage_a_f1": connect_configure * 1000 / stage_a_f1_p50_ns,
            "full_non_authority_share_of_stage_a_f1": non_authority * 1000 / stage_a_f1_p50_ns,
        },
    }
    (ROOT / "stage-b" / "segmentation.json").write_text(
        json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(document["derived"], indent=2))
    print(json.dumps(document["count_assertion"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
