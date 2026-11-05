#!/usr/bin/env python3
"""Gate 0: prove the merged settlement fix is present, and attribute refusals.

For every leg written by this campaign:
  * /v1/metrics must publish the operation_settle account;
  * a full local settlement queue must appear as retry_total / retry_success_total
    or refused_total{queue_full}, and must not appear as a 502 caused by queue
    fullness;
  * any errors_total movement is listed per leg so an unexpected
    outcome_unknown cannot hide.

This is a mechanism check. It does not change any performance criterion.
"""
from __future__ import annotations

import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent


def read_json(path: pathlib.Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def delta(before: dict | None, after: dict | None, section: str, key: str) -> int:
    if not before or not after:
        return 0
    left = (before.get(section) or {}) if section else before
    right = (after.get(section) or {}) if section else after
    return int(right.get(key) or 0) - int(left.get(key) or 0)


def map_delta(before: dict | None, after: dict | None, section: str) -> dict:
    if not before or not after:
        return {}
    left = before.get(section) or {}
    right = after.get(section) or {}
    moved = {}
    for key in sorted(set(left) | set(right)):
        value = int(right.get(key) or 0) - int(left.get(key) or 0)
        if value:
            moved[key] = value
    return moved


def legs(root: pathlib.Path):
    p1 = root / "legs" / "p1"
    if p1.is_dir():
        for directory in sorted(path for path in p1.iterdir()
                                if path.is_dir() and (path / "leg.txt").exists()):
            yield "p1/" + directory.name, directory
    for scenario in ("f1", "l1", "x1", "c8"):
        base = root / "legs" / "p2" / scenario
        if not base.is_dir():
            continue
        for directory in sorted(path for path in base.iterdir()
                                if path.is_dir() and (path / "leg.txt").exists()):
            yield "p2/%s/%s" % (scenario, directory.name), directory


def main() -> int:
    root = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT
    records = []
    failures = []
    for name, directory in legs(root):
        before = read_json(directory / "metrics-before.json")
        after = read_json(directory / "metrics-after.json")
        record = {"leg": name}
        if before is None or after is None:
            record["invalid"] = "missing metrics"
            failures.append(name + ": missing metrics")
            records.append(record)
            continue
        settle_before = before.get("operation_settle")
        settle_after = after.get("operation_settle")
        record["operation_settle_before"] = settle_before is not None
        record["operation_settle_after"] = settle_after is not None
        record["retry_total"] = delta(before, after, "operation_settle", "retry_total")
        record["retry_success_total"] = delta(
            before, after, "operation_settle", "retry_success_total")
        record["exhausted_total"] = delta(
            before, after, "operation_settle", "exhausted_total")
        record["refused_total"] = {}
        refused = {}
        left = (before.get("operation_settle") or {}).get("refused_total") or {}
        right = (after.get("operation_settle") or {}).get("refused_total") or {}
        for reason in sorted(set(left) | set(right)):
            value = int(right.get(reason) or 0) - int(left.get(reason) or 0)
            if value:
                refused[reason] = value
        record["refused_total"] = refused
        record["errors_total"] = map_delta(before, after, "errors_total")
        record["rejection_reasons_total"] = map_delta(
            before, after, "rejection_reasons_total")
        if settle_before is None or settle_after is None:
            failures.append(name + ": operation_settle missing")
        queue_full = int(refused.get("queue_full") or 0)
        if queue_full and int(record["retry_total"]) < queue_full:
            failures.append(name + ": queue_full refusals without matching retries")
        if int(record["errors_total"].get("outcome_unknown") or 0) and queue_full:
            failures.append(name + ": queue_full surfaced as outcome_unknown")
        records.append(record)

    result = {
        "legs": records,
        "failures": failures,
        "passed": not failures,
        "note": "queue_full must be attributed to operation_settle retries, not to "
                "outcome_unknown; every leg must publish operation_settle",
    }
    (root / "gate0.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("gate0 legs=%d failures=%d" % (len(records), len(failures)))
    for failure in failures:
        print("  FAIL", failure)
    for record in records:
        if (record.get("retry_total") or record.get("refused_total")
                or record.get("errors_total")):
            print("  ", record["leg"], "retry", record.get("retry_total"),
                  "refused", record.get("refused_total"),
                  "errors", record.get("errors_total"))
    print("GATE0:", "PASS" if not failures else "FAIL")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
