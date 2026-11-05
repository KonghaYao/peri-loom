#!/usr/bin/env python3
"""R72 Gate 0 and C1 mechanism check.

Two parts:

1. The short Gate 0 legs under legs/gate0/{a,b}:
   * A (explicit PERI_DEFERRED_SESSION_CLOSE=0) publishes no
     deferred_session_close key before or after;
   * B (completely unset, the R72 default) publishes the key with
     enqueued_total > 0;
   * both arms publish operation_settle and have retry_total == 0;
   * B leaves failed/retried/unresolved at zero and pending at zero.

2. Every P1/P2 measurement leg must also publish operation_settle with
   retry_total == 0, and queue_full refusals (if any) must be attributed to
   retries rather than to outcome_unknown. This mirrors R71d's Gate 0 account
   check.

Writes gate0.json and prints a machine-readable per-leg summary.
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


def leg_name(directory: pathlib.Path) -> str:
    return directory.name


def measurement_legs(root: pathlib.Path):
    p1 = root / "legs" / "p1"
    if p1.is_dir():
        for directory in sorted(path for path in p1.iterdir()
                                if path.is_dir() and (path / "leg.txt").exists()):
            yield "p1/" + leg_name(directory), directory
    for scenario in ("f1", "l1", "x1", "c8"):
        base = root / "legs" / "p2" / scenario
        if not base.is_dir():
            continue
        for directory in sorted(path for path in base.iterdir()
                                if path.is_dir() and (path / "leg.txt").exists()):
            yield "p2/%s/%s" % (scenario, leg_name(directory)), directory


def check_measurement_leg(name: str, directory: pathlib.Path) -> tuple[dict, list[str]]:
    before = read_json(directory / "metrics-before.json")
    after = read_json(directory / "metrics-after.json")
    record = {"leg": name, "kind": "measurement"}
    failures: list[str] = []
    if before is None or after is None:
        record["invalid"] = "missing metrics"
        failures.append(name + ": missing metrics")
        return record, failures
    settle_before = before.get("operation_settle")
    settle_after = after.get("operation_settle")
    record["operation_settle_before"] = settle_before is not None
    record["operation_settle_after"] = settle_after is not None
    record["retry_total"] = delta(before, after, "operation_settle", "retry_total")
    record["retry_success_total"] = delta(
        before, after, "operation_settle", "retry_success_total")
    record["exhausted_total"] = delta(
        before, after, "operation_settle", "exhausted_total")
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
    return record, failures


def check_gate0_leg(name: str, directory: pathlib.Path) -> tuple[dict, list[str]]:
    identity = {}
    leg_txt = directory / "leg.txt"
    if leg_txt.exists():
        for line in leg_txt.read_text(encoding="utf-8").splitlines():
            if "=" in line:
                key, value = line.split("=", 1)
                identity[key.strip()] = value.strip()
    switch = identity.get("switch", "unknown")
    before = read_json(directory / "metrics-before.json")
    after = read_json(directory / "metrics-after.json")
    record = {"leg": name, "kind": "gate0", "switch": switch}
    failures: list[str] = []
    if before is None or after is None:
        record["invalid"] = "missing metrics"
        failures.append(name + ": missing metrics")
        return record, failures
    present_before = "deferred_session_close" in before
    present_after = "deferred_session_close" in after
    record["deferred_present_before"] = present_before
    record["deferred_present_after"] = present_after
    record["operation_settle_before"] = before.get("operation_settle") is not None
    record["operation_settle_after"] = after.get("operation_settle") is not None
    record["retry_total"] = delta(before, after, "operation_settle", "retry_total")
    record["errors_total"] = map_delta(before, after, "errors_total")
    deferred = after.get("deferred_session_close") or {}
    record["deferred_enqueued_total"] = delta(
        before, after, "deferred_session_close", "enqueued_total")
    record["deferred_inline_total"] = delta(
        before, after, "deferred_session_close", "inline_total")
    record["deferred_failed_total"] = delta(
        before, after, "deferred_session_close", "failed_total")
    record["deferred_retried_total"] = delta(
        before, after, "deferred_session_close", "retried_total")
    record["deferred_unresolved_total"] = int(deferred.get("unresolved_total") or 0)
    record["deferred_pending"] = int(deferred.get("pending") or 0)
    record["deferred_pending_max"] = int(deferred.get("pending_max") or 0)
    if before.get("operation_settle") is None or after.get("operation_settle") is None:
        failures.append(name + ": operation_settle missing")
    if int(record["retry_total"]) != 0:
        failures.append(name + ": operation_settle.retry_total != 0")
    if switch == "explicit-0":
        if present_before or present_after:
            failures.append(name + ": explicit-0 arm published deferred_session_close")
    else:
        if not present_after:
            failures.append(name + ": default arm has no deferred_session_close")
        if int(record["deferred_enqueued_total"]) <= 0:
            failures.append(name + ": default arm enqueued_total <= 0")
        if int(record["deferred_failed_total"]) != 0:
            failures.append(name + ": default arm failed_total != 0")
        if int(record["deferred_retried_total"]) != 0:
            failures.append(name + ": default arm retried_total != 0")
        if int(record["deferred_unresolved_total"]) != 0:
            failures.append(name + ": default arm unresolved_total != 0")
        if int(record["deferred_pending"]) != 0:
            failures.append(name + ": default arm pending != 0")
        if int(record["deferred_pending_max"]) > 4:
            failures.append(name + ": default arm pending_max > 4")
    if int(record["errors_total"].get("outcome_unknown") or 0):
        failures.append(name + ": outcome_unknown in Gate 0 window")
    return record, failures


def main() -> int:
    root = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT
    records = []
    failures = []

    gate0 = root / "legs" / "gate0"
    if gate0.is_dir():
        for directory in sorted(path for path in gate0.iterdir()
                                if path.is_dir() and (path / "leg.txt").exists()):
            record, leg_failures = check_gate0_leg("gate0/" + leg_name(directory),
                                                   directory)
            records.append(record)
            failures.extend(leg_failures)

    for name, directory in measurement_legs(root):
        record, leg_failures = check_measurement_leg(name, directory)
        records.append(record)
        failures.extend(leg_failures)

    result = {
        "legs": records,
        "failures": failures,
        "passed": not failures,
        "note": "C1: explicit-0 publishes no deferred key; default arm publishes "
                "deferred_session_close with enqueued_total > 0; both arms "
                "operation_settle.retry_total == 0. Queue-full must be attributed "
                "to retries, never to outcome_unknown.",
    }
    (root / "gate0.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("gate0 legs=%d failures=%d" % (len(records), len(failures)))
    for failure in failures:
        print("  FAIL", failure)
    for record in records:
        if record["kind"] == "gate0":
            print("  ", record["leg"], "switch", record["switch"],
                  "deferred_present", record.get("deferred_present_after"),
                  "enqueued", record.get("deferred_enqueued_total"),
                  "pending_max", record.get("deferred_pending_max"),
                  "retry", record.get("retry_total"),
                  "errors", record.get("errors_total"))
        elif record.get("retry_total") or record.get("refused_total") \
                or record.get("errors_total"):
            print("  ", record["leg"], "retry", record.get("retry_total"),
                  "refused", record.get("refused_total"),
                  "errors", record.get("errors_total"))
    print("GATE0:", "PASS" if not failures else "FAIL")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
