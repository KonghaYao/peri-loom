#!/usr/bin/env python3
"""C7 response-shape check for the F1/L1 regression legs.

Every F1/L1 leg replays the same request sequence, so the two arms must return
the same rows and the same response bytes. The response byte count includes the
`query_duration_ms` field, whose digit width can change by a byte or two; the
comparison therefore uses the modal (response_bytes, rows, rows_read,
rows_written, affected_row_count) tuple over each leg's timed samples, exactly
as R71d reported the 15,627 L1 mode.

Writes response-bytes.json and prints one row per leg.
"""
from __future__ import annotations

import collections
import json
import pathlib
import sys


def read_json(path: pathlib.Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def modal_tuple(directory: pathlib.Path) -> dict | None:
    raw_path = directory / "harness" / "raw.jsonl"
    if not raw_path.exists():
        return None
    counts = collections.Counter()
    rows_seen = collections.Counter()
    for line in raw_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        if item.get("phase") != "timed":
            continue
        key = (
            item.get("response_bytes"),
            item.get("rows"),
            item.get("rows_read"),
            item.get("rows_written"),
            item.get("affected_row_count"),
        )
        counts[key] += 1
        rows_seen[(item.get("rows"), item.get("rows_read"))] += 1
    if not counts:
        return None
    key, count = counts.most_common(1)[0]
    return {
        "response_bytes": key[0],
        "rows": key[1],
        "rows_read": key[2],
        "rows_written": key[3],
        "affected_row_count": key[4],
        "mode_samples": count,
        "timed_samples": sum(counts.values()),
        "distinct_response_bytes": sorted({item[0] for item in counts}),
        "distinct_rows": sorted({item[0] for item in rows_seen}),
    }


def main() -> int:
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else __file__).resolve()
    if root.is_file():
        root = root.parent
    report = {"scenarios": {}, "passed": True, "failures": []}
    for scenario in ("f1", "l1"):
        base = root / "legs" / "p2" / scenario
        if not base.is_dir():
            continue
        legs = {}
        for directory in sorted(path for path in base.iterdir() if path.is_dir()):
            identity = {}
            leg_txt = directory / "leg.txt"
            if leg_txt.exists():
                for line in leg_txt.read_text(encoding="utf-8").splitlines():
                    if "=" in line:
                        key, value = line.split("=", 1)
                        identity[key.strip()] = value.strip()
            legs[directory.name] = {
                "side": identity.get("side"),
                "switch": identity.get("switch"),
                "mode": modal_tuple(directory),
            }
        entries = [entry["mode"] for entry in legs.values() if entry["mode"]]
        modes = {json.dumps(entry["mode"] if entry else None, sort_keys=True)
                 for entry in legs.values()}
        # Keep only the compared fields for the equality decision.
        tuples = {
            (entry["mode"]["response_bytes"], entry["mode"]["rows"],
             entry["mode"]["rows_read"], entry["mode"]["rows_written"],
             entry["mode"]["affected_row_count"])
            for entry in legs.values() if entry["mode"]
        }
        equal = len(tuples) == 1 and len(entries) == len(legs)
        if not equal:
            report["passed"] = False
            report["failures"].append(
                "%s response shape differs across legs: %s" % (scenario, sorted(tuples)))
        report["scenarios"][scenario] = {
            "legs": legs,
            "tuples": sorted(list(item) for item in tuples),
            "equal": equal,
        }
    (root / "response-bytes.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("| scenario | leg | side | switch | response_bytes mode | rows | mode samples/timed |")
    print("|---|---|---|---|---:|---:|---:|")
    for scenario, entry in report["scenarios"].items():
        for leg, detail in entry["legs"].items():
            mode = detail["mode"] or {}
            print("| %s | %s | %s | %s | %s | %s | %s/%s |" % (
                scenario, leg, detail.get("side"), detail.get("switch"),
                mode.get("response_bytes"), mode.get("rows"),
                mode.get("mode_samples"), mode.get("timed_samples")))
    print("RESPONSE-SHAPE:", "PASS" if report["passed"] else "FAIL")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
