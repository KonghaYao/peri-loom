#!/usr/bin/env python3
"""Build metrics.json: the consolidated per-leg metric view of the R72 campaign.

Reads each leg's leg.txt, metrics-before/metrics-after, audit-verify.json and
the analysis-p1/analysis-p2 outputs. The raw per-leg samples stay under
legs/<campaign>/<leg>/harness/raw.jsonl; this file is the compact index a
reviewer can diff without opening hundreds of files.

Writes metrics.json at the evidence root.
"""
from __future__ import annotations

import json
import pathlib
import sys


def read_json(path: pathlib.Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def read_int(path: pathlib.Path):
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, ValueError):
        return None


def identity(directory: pathlib.Path) -> dict:
    values = {}
    path = directory / "leg.txt"
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def delta(before: dict | None, after: dict | None, section: str, key: str) -> int:
    if before is None or after is None:
        return 0
    left = (before.get(section) or {}) if section else before
    right = (after.get(section) or {}) if section else after
    return int(right.get(key) or 0) - int(left.get(key) or 0)


def map_delta(before: dict | None, after: dict | None, section: str) -> dict:
    if before is None or after is None:
        return {}
    left = before.get(section) or {}
    right = after.get(section) or {}
    moved = {}
    for key in sorted(set(left) | set(right)):
        value = int(right.get(key) or 0) - int(left.get(key) or 0)
        if value:
            moved[key] = value
    return moved


def leg_record(directory: pathlib.Path, campaign: str, scenario: str) -> dict:
    ident = identity(directory)
    before = read_json(directory / "metrics-before.json")
    after = read_json(directory / "metrics-after.json") or {}
    audit = read_json(directory / "audit-verify.json") or {}
    deferred = after.get("deferred_session_close")
    deferred_delta = None
    if deferred is not None:
        deferred_delta = {
            field: delta(before, after, "deferred_session_close", field)
            for field in ("enqueued_total", "inline_total", "failed_total",
                          "retried_total", "unresolved_total", "flushed_total")
        }
        deferred_delta["pending"] = int(deferred.get("pending") or 0)
        deferred_delta["pending_max"] = int(deferred.get("pending_max") or 0)
    record = {
        "leg": ident.get("leg", directory.name),
        "campaign": campaign,
        "scenario": scenario,
        "side": ident.get("side"),
        "flag": ident.get("flag"),
        "switch": ident.get("switch"),
        "container": ident.get("container"),
        "volume": ident.get("volume"),
        "driver_exit": (read_json(directory / "exit-code.json") or {}).get("driver"),
        "settle_exit": (read_json(directory / "settle-exit-code.json") or {}).get("settle"),
        "stop_exit": (directory / "stop-exit.txt").read_text().strip()
        if (directory / "stop-exit.txt").exists() else None,
        "deferred_session_close": deferred_delta,
        "errors_total": map_delta(before, after, "errors_total"),
        "rejection_reasons_total": map_delta(before, after, "rejection_reasons_total"),
        "operation_settle_retry_total": delta(before, after, "operation_settle", "retry_total"),
        "operation_settle_retry_success_total": delta(
            before, after, "operation_settle", "retry_success_total"),
        "operation_settle_exhausted_total": delta(
            before, after, "operation_settle", "exhausted_total"),
        "operation_settle_refused_total": (after.get("operation_settle") or {}).get(
            "refused_total"),
        "session_seats": int(after.get("session_seats") or 0),
        "active_permits": int(after.get("active_permits") or 0),
        "unknown_held_permits": int(after.get("unknown_held_permits") or 0),
        "audit_records": audit.get("records"),
        "audit_chain_ok": audit.get("chain_ok"),
        "audit_head_matches_tail": audit.get("head_matches_tail"),
        "audit_count_before": read_int(directory / "audit-count-before.txt"),
        "audit_count_after": read_int(directory / "audit-count-after.txt"),
    }
    record["audit_delta"] = (
        record["audit_count_after"] - record["audit_count_before"]
        if record["audit_count_before"] is not None
        and record["audit_count_after"] is not None else None)
    return record


def main() -> int:
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else __file__).resolve()
    if root.is_file():
        root = root.parent
    p1_analysis = read_json(root / "analysis-p1.json") or {}
    p2_analysis = read_json(root / "analysis-p2.json") or {}
    p2_legs = {}
    for scenario, entry in (p2_analysis.get("scenarios") or {}).items():
        for leg in entry.get("legs") or []:
            p2_legs[(scenario, leg.get("leg"))] = leg
    records = []
    for directory in sorted((root / "legs" / "gate0").glob("*")):
        if directory.is_dir() and (directory / "leg.txt").exists():
            records.append(leg_record(directory, "gate0", "gate0"))
    for directory in sorted((root / "legs" / "p1").glob("*")):
        if directory.is_dir() and (directory / "leg.txt").exists():
            record = leg_record(directory, "p1", "block-mean")
            record["p1_block_mean_ms"] = None
            record["p1_effect_median"] = (p1_analysis.get("effect") or {}).get(
                "per_triple", {}).get("median")
            records.append(record)
    for scenario in ("f1", "l1", "x1", "c8"):
        for directory in sorted((root / "legs" / "p2" / scenario).glob("*")):
            if directory.is_dir() and (directory / "leg.txt").exists():
                record = leg_record(directory, "p2", scenario)
                analysis_leg = p2_legs.get((scenario, record["leg"])) or {}
                record["median_ms"] = analysis_leg.get("median_ms")
                record["p95_ms"] = analysis_leg.get("p95_ms")
                record["throughput_qps"] = analysis_leg.get("throughput_qps")
                record["failures"] = analysis_leg.get("failures")
                records.append(record)
    document = {
        "campaign": "R72 deferred session close default enable — independent verification",
        "legs_total": len(records),
        "legs": records,
    }
    (root / "metrics.json").write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("metrics.json legs=%d" % len(records))
    return 0


if __name__ == "__main__":
    sys.exit(main())
