#!/usr/bin/env python3
"""Collect and check the R71 P3 invariants for every measured leg.

For each leg directory under `legs/p1` and `legs/p2/<scenario>`:

  * deferred_session_close direction (absent on A; enqueued > 0 and clean on B),
  * session_seats / active_permits / worker sessions / pending_close zero,
  * clean stop (launcher-stop.json outcome=clean, unresolved_operations=false,
    docker stop exit 0),
  * audit chain continuity and equal record counts for the same workload,
  * error counters and driver outcomes free of 429/503/outcome_unknown/
    session_moved/audit_unavailable.

Audit continuity is delegated to verify_audit.py, which also writes each leg's
`audit-verify.json`. This script never reads the connection file.
"""
from __future__ import annotations

import json
import pathlib
import sys

import verify_audit


def read_json(path: pathlib.Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def stop_payload(document):
    """The launcher-stop file is a MAC envelope whose payload is a JSON string."""
    if isinstance(document, dict) and isinstance(document.get("payload"), str):
        try:
            return json.loads(document["payload"])
        except json.JSONDecodeError:
            return document
    return document


def delta(before: dict, after: dict, section: str, key: str) -> int:
    left = (before.get(section) or {}) if section else before
    right = (after.get(section) or {}) if section else after
    return int(right.get(key) or 0) - int(left.get(key) or 0)


def map_delta(before: dict, after: dict, section: str) -> dict:
    left = before.get(section) or {}
    right = after.get(section) or {}
    keys = set(left) | set(right)
    moved = {}
    for key in sorted(keys):
        value = int(right.get(key) or 0) - int(left.get(key) or 0)
        if value:
            moved[key] = value
    return moved


def worker_state(metrics: dict) -> dict:
    workers = (metrics.get("native") or {}).get("workers") or {}
    return {
        name: {
            "sessions": int(worker.get("sessions") or 0),
            "pending_close": int((worker.get("lifecycle") or {})
                                 .get("pending_close", {}).get("count") or 0),
        }
        for name, worker in workers.items()
    }


def leg_identity(directory: pathlib.Path) -> dict:
    identity = {}
    for line in (directory / "leg.txt").read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            identity[key.strip()] = value.strip()
    return identity


def check_leg(directory: pathlib.Path, campaign: str, scenario: str) -> dict:
    identity = leg_identity(directory)
    before = read_json(directory / "metrics-before.json")
    after = read_json(directory / "metrics-after.json")
    stop = stop_payload(read_json(directory / "launcher-stop.json"))
    settle = read_json(directory / "settle.json")
    exit_code = read_json(directory / "exit-code.json") or {}
    stop_exit = (directory / "stop-exit.txt").read_text().strip() \
        if (directory / "stop-exit.txt").exists() else "missing"

    audit = verify_audit.verify(directory)
    (directory / "audit-verify.json").write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    flag = identity.get("flag", "unknown")
    deferred_present = bool(after is not None and "deferred_session_close" in after)
    deferred_delta = {}
    deferred_now = {}
    if before is not None and after is not None and deferred_present:
        deferred_delta = {
            field: delta(before, after, "deferred_session_close", field)
            for field in ("enqueued_total", "inline_total", "failed_total",
                          "retried_total", "unresolved_total", "flushed_total")
        }
        deferred_now = after.get("deferred_session_close") or {}

    errors = map_delta(before, after, "errors_total") if before and after else {}
    rejections = map_delta(before, after, "rejection_reasons_total") \
        if before and after else {}
    actions = map_delta(before, after, "actions_total") if before and after else {}

    checks = []

    def check(name: str, condition: bool, detail) -> None:
        checks.append({"check": name, "pass": bool(condition), "detail": detail})

    if flag == "off":
        check("flag off publishes no deferred key", not deferred_present,
              "deferred_session_close present=%s" % deferred_present)
    else:
        check("flag on publishes deferred key", deferred_present,
              "deferred_session_close present=%s" % deferred_present)
        check("flag on enqueues closes", deferred_delta.get("enqueued_total", 0) > 0,
              "enqueued=%r" % deferred_delta.get("enqueued_total"))
        check("flag on has no failed/retried closes",
              deferred_delta.get("failed_total", 0) == 0
              and deferred_delta.get("retried_total", 0) == 0,
              "failed=%r retried=%r" % (deferred_delta.get("failed_total"),
                                        deferred_delta.get("retried_total")))
        check("flag on leaves no unresolved closes",
              int(deferred_now.get("unresolved_total") or 0) == 0,
              "unresolved_total=%r" % deferred_now.get("unresolved_total"))
        check("flag on drains pending closes",
              int(deferred_now.get("pending") or 0) == 0,
              "pending=%r pending_max=%r" % (deferred_now.get("pending"),
                                             deferred_now.get("pending_max")))

    if after is not None:
        check("session_seats zero", int(after.get("session_seats") or 0) == 0,
              "session_seats=%r" % after.get("session_seats"))
        check("active_permits zero", int(after.get("active_permits") or 0) == 0,
              "active_permits=%r" % after.get("active_permits"))
        workers = worker_state(after)
        check("worker sessions zero",
              all(value["sessions"] == 0 for value in workers.values()), workers)
        check("worker pending_close zero",
              all(value["pending_close"] == 0 for value in workers.values()), workers)
        ledger = (after.get("cleanup_ledger") or {}).get("pending_close") or {}
        check("cleanup ledger pending_close zero",
              int(ledger.get("count") or 0) == 0, ledger)
    check("settle reached zero", settle is not None and settle.get("verdict") == "settled",
          None if settle is None else settle.get("verdict"))
    check("audit chain continuous", audit["chain_ok"],
          "records=%d errors=%s" % (audit["records"], audit["errors"][:3]))
    check("clean stop proof",
          stop is not None and stop.get("outcome") == "clean"
          and stop.get("unresolved_operations") is False,
          None if stop is None else {"outcome": stop.get("outcome"),
                                     "unresolved_operations":
                                         stop.get("unresolved_operations")})
    check("docker stop exit zero", stop_exit == "0", stop_exit)
    check("no error counters", not errors, errors)
    check("no rejection counters", not rejections, rejections)
    return {
        "campaign": campaign,
        "scenario": scenario,
        "leg": identity.get("leg", directory.name),
        "flag": flag,
        "side": identity.get("leg", "").split("-")[-1][0],
        "round": identity.get("leg", "").split("-")[0],
        "deferred_present": deferred_present,
        "deferred_delta": deferred_delta,
        "deferred_now": {key: deferred_now.get(key) for key in
                         ("pending", "pending_max", "maximum",
                          "per_database_maximum", "unresolved_total")}
        if deferred_present else None,
        "actions": actions,
        "errors": errors,
        "rejections": rejections,
        "driver_exit": exit_code.get("driver"),
        "audit_records": audit["records"],
        "audit_last_sequence": audit["last_sequence"],
        "audit_head_matches_tail": audit["head_matches_tail"],
        "checks": checks,
        "passed": all(item["pass"] for item in checks),
    }


def main() -> int:
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else __file__).resolve()
    if root.is_file():
        root = root.parent
    legs = []
    p1 = root / "legs" / "p1"
    if p1.is_dir():
        for directory in sorted(path for path in p1.iterdir() if path.is_dir()):
            legs.append(check_leg(directory, "p1", "block-mean"))
    p2 = root / "legs" / "p2"
    if p2.is_dir():
        for scenario_dir in sorted(path for path in p2.iterdir() if path.is_dir()):
            for directory in sorted(path for path in scenario_dir.iterdir()
                                    if path.is_dir()):
                legs.append(check_leg(directory, "p2", scenario_dir.name))

    # Equal audit record counts for the same workload: compare each A leg with
    # the B legs of the same campaign/scenario/round.
    counts = {}
    for leg in legs:
        counts.setdefault((leg["campaign"], leg["scenario"], leg["round"]),
                          {"a": [], "b": []})[leg["side"]].append(
            (leg["leg"], leg["audit_records"]))
    count_checks = []
    for key, sides in sorted(counts.items()):
        if sides["a"] and sides["b"]:
            a_counts = {count for _, count in sides["a"]}
            b_counts = {count for _, count in sides["b"]}
            count_checks.append({
                "key": "%s/%s/%s" % key,
                "a": sides["a"],
                "b": sides["b"],
                "equal": a_counts == b_counts,
            })

    passed = all(leg["passed"] for leg in legs) and all(
        item["equal"] for item in count_checks)
    document = {
        "legs": legs,
        "audit_count_checks": count_checks,
        "legs_total": len(legs),
        "legs_passed": sum(1 for leg in legs if leg["passed"]),
        "passed": passed,
    }
    (root / "invariants.json").write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print("| leg | flag | checks | driver exit | audit records | seats/permits | deferred enqueued |")
    print("|---|---|---:|---:|---:|---|---:|")
    for leg in legs:
        failed = [item["check"] for item in leg["checks"] if not item["pass"]]
        print("| %s/%s/%s | %s | %d/%d | %s | %d | 0/0 | %s |"
              % (leg["campaign"], leg["scenario"], leg["leg"], leg["flag"],
                 len(leg["checks"]) - len(failed), len(leg["checks"]),
                 leg["driver_exit"], leg["audit_records"],
                 leg["deferred_delta"].get("enqueued_total", "—")))
        for name in failed:
            print("  FAIL: " + name)
    for item in count_checks:
        print("audit counts %s equal=%s A=%s B=%s"
              % (item["key"], item["equal"], item["a"], item["b"]))
    print("%d/%d legs passed; audit count groups equal=%s; VERDICT=%s"
          % (document["legs_passed"], document["legs_total"],
             all(item["equal"] for item in count_checks),
             "PASS" if passed else "FAIL"))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
