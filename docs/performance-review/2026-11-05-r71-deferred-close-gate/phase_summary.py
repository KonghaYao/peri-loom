#!/usr/bin/env python3
"""Summarize the platform request-phase stream for Gate 1a.

Input is the private `request-phase-observation.jsonl` written when the platform
runs with `PERI_SQL_OPEN_OBSERVER=1`. Only phase durations are read: request ids,
actions and outcome labels are already credential-free, and nothing here prints
anything else from the stream.

The `--after-bytes` option skips the records that existed before the timed
window started (the file is append-only and created with O_EXCL), so fixture
discovery and dry-run traffic do not enter the timed distribution. A partial
line at the boundary is discarded.

Sampling is a diagnostic: the observer slows requests down and the absolute
durations are not end-to-end performance. The output says so.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys


def percentile(values: list[int], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, min(len(ordered), int(round(fraction * len(ordered) + 0.5))))
    return ordered[rank - 1]


def stats(values: list[int]) -> dict | None:
    if not values:
        return None
    ordered = sorted(values)
    return {
        "n": len(values),
        "p50_us": round(percentile(values, 0.50) / 1000.0, 3),
        "p90_us": round(percentile(values, 0.90) / 1000.0, 3),
        "mean_us": round(sum(values) / len(values) / 1000.0, 3),
        "min_us": round(ordered[0] / 1000.0, 3),
        "max_us": round(ordered[-1] / 1000.0, 3),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase_file")
    parser.add_argument("--after-bytes", type=int, default=0,
                        help="skip records that started before this byte offset")
    parser.add_argument("--out")
    arguments = parser.parse_args()

    payload = pathlib.Path(arguments.phase_file).read_bytes()
    window_bytes = payload[arguments.after_bytes:]
    # The file is append-only, so the marker is a byte offset. When it lands
    # mid-line the record spanning it started before the window and is dropped
    # up to the next newline; when it lands exactly between lines (the usual
    # case, because stat and append are serialized by the same file) the first
    # record belongs to the window and is kept.
    if arguments.after_bytes > 0 and payload[arguments.after_bytes - 1:arguments.after_bytes] != b"\n":
        cut = window_bytes.find(b"\n")
        window_bytes = b"" if cut < 0 else window_bytes[cut + 1:]

    by_action: dict[str, list[dict]] = {}
    close_line = None
    skipped = 0
    for raw in window_bytes.splitlines():
        if not raw.strip():
            continue
        try:
            record = json.loads(raw)
        except json.JSONDecodeError:
            skipped += 1
            continue
        if record.get("action") == "observer":
            close_line = record
            continue
        by_action.setdefault(record.get("action", "?"), []).append(record)

    summary: dict = {
        "phase_file": str(arguments.phase_file),
        "after_bytes": arguments.after_bytes,
        "window_bytes": len(window_bytes),
        "unparsed_lines": skipped,
        "observer": close_line,
        "actions": {},
        "note": "diagnostic phase capture; absolute durations are not end-to-end performance",
    }

    for action in sorted(by_action):
        records = by_action[action]
        phases: dict[str, list[int]] = {}
        outcomes: dict[str, int] = {}
        totals: list[int] = []
        for record in records:
            outcomes[record.get("outcome", "?")] = outcomes.get(record.get("outcome", "?"), 0) + 1
            if isinstance(record.get("total_ns"), int):
                totals.append(record["total_ns"])
            for span in record.get("spans") or []:
                phases.setdefault(span.get("phase", "?"), []).append(int(span.get("ns", 0)))
        summary["actions"][action] = {
            "requests": len(records),
            "outcomes": outcomes,
            "total": stats(totals),
            "phases": {phase: stats(values) for phase, values in sorted(phases.items())},
        }

    rendered = json.dumps(summary, indent=2, ensure_ascii=False)
    if arguments.out:
        pathlib.Path(arguments.out).write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    sys.exit(main())
