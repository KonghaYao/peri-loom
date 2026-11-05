#!/usr/bin/env python3
"""Collect and check the R71b P3 invariants.

P1: the two interleaved stacks under legs/p1/a and legs/p1/b. The A stack
performs twice the B stack's requests by design, so equality is not required;
the audit growth must track the request ratio (2:1) and no unexplained gap is
accepted.

P2: every leg under legs/p2/<scenario>/<leg>; A and B legs of the same
scenario/round carry the same workload, so their audit growth must match
within a two-record tolerance.

The positive-control stacks under legs/p0 are not candidate legs and are not
checked here.

Continuity is delegated to verify_audit.py, which also writes each leg's
audit-verify.json. The connection file is never read.
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


def read_int(path: pathlib.Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, ValueError):
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
    moved = {}
    for key in sorted(set(left) | set(right)):
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
    count_before = read_int(directory / "audit-count-before.txt")
    count_after = read_int(directory / "audit-count-after.txt")
    audit_delta = (count_after - count_before
                   if count_before is not None and count_after is not None else None)

    audit = verify_audit.verify(directory)
    (directory / "audit-verify.json").write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    flag = identity.get("flag", "unknown")
    present_before = bool(before is not None and "deferred_session_close" in before)
    present_after = bool(after is not None and "deferred_session_close" in after)
    deferred_delta = {}
    deferred_now = {}
    if before is not None and after is not None and present_after:
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
        check("flag off publishes no deferred key",
              not present_before and not present_after,
              "before=%s after=%s" % (present_before, present_after))
    else:
        check("flag on publishes deferred key", present_after,
              "deferred_session_close present=%s" % present_after)
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
    check("audit count delta recorded", audit_delta is not None and audit_delta > 0,
          "before=%r after=%r delta=%r"
          % (count_before, count_after, audit_delta))
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
        "side": identity.get("side") or identity.get("leg", "").split("-")[-1][0],
        "round": identity.get("round", ""),
        "deferred_present": present_after,
        "deferred_delta": deferred_delta,
        "deferred_now": {key: deferred_now.get(key) for key in
                         ("pending", "pending_max", "maximum",
                          "per_database_maximum", "unresolved_total")}
        if present_after else None,
        "actions": actions,
        "errors": errors,
        "rejections": rejections,
        "driver_exit": exit_code.get("driver"),
        "audit_records": audit["records"],
        "audit_count_before": count_before,
        "audit_count_after": count_after,
        "audit_delta": audit_delta,
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
        for directory in sorted(path for path in p1.iterdir()
                                if path.is_dir() and (path / "leg.txt").exists()):
            legs.append(check_leg(directory, "p1", "block-mean"))
    p2 = root / "legs" / "p2"
    if p2.is_dir():
        for scenario_dir in sorted(path for path in p2.iterdir() if path.is_dir()):
            for directory in sorted(path for path in scenario_dir.iterdir()
                                    if path.is_dir() and (path / "leg.txt").exists()):
                legs.append(check_leg(directory, "p2", scenario_dir.name))

    count_checks = []
    p1_legs = [leg for leg in legs if leg["campaign"] == "p1"]
    driver_summary = read_json(root / "legs" / "p1" / "driver" / "summary.json") or {}
    if p1_legs:
        a_leg = next((leg for leg in p1_legs if leg["flag"] == "off"), None)
        b_leg = next((leg for leg in p1_legs if leg["flag"] == "on"), None)
        a_requests = driver_summary.get("a_requests")
        b_requests = driver_summary.get("b_requests")
        if a_leg and b_leg and a_leg["audit_delta"] and b_leg["audit_delta"]:
            ratio = a_leg["audit_delta"] / b_leg["audit_delta"]
            request_ratio = (a_requests / b_requests
                             if a_requests and b_requests else None)
            explained = (request_ratio is not None
                         and abs(ratio - request_ratio) <= 0.05 * request_ratio)
            count_checks.append({
                "key": "p1/block-mean",
                "a": [(a_leg["leg"], a_leg["audit_delta"])],
                "b": [(b_leg["leg"], b_leg["audit_delta"])],
                "a_requests": a_requests,
                "b_requests": b_requests,
                "count_ratio": round(ratio, 6),
                "request_ratio": round(request_ratio, 6) if request_ratio else None,
                "equal": explained,
            })
        else:
            count_checks.append({
                "key": "p1/block-mean", "a": [], "b": [],
                "equal": False, "detail": "missing audit delta",
            })

    groups = {}
    for leg in legs:
        if leg["campaign"] != "p2":
            continue
        groups.setdefault((leg["scenario"], leg["round"]), {"a": [], "b": []})[
            leg["side"]].append((leg["leg"], leg["audit_delta"]))
    for key, sides in sorted(groups.items()):
        deltas = [value for _, value in sides["a"] + sides["b"]
                  if value is not None]
        equal = (bool(sides["a"]) and bool(sides["b"])
                 and len(deltas) == len(sides["a"]) + len(sides["b"])
                 and max(deltas) - min(deltas) <= 2)
        count_checks.append({
            "key": "%s/%s" % key,
            "a": sides["a"],
            "b": sides["b"],
            "equal": equal,
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

    print("| leg | flag | checks | driver exit | audit records | audit delta | deferred enqueued |")
    print("|---|---|---:|---:|---:|---:|---:|")
    for leg in legs:
        failed = [item["check"] for item in leg["checks"] if not item["pass"]]
        print("| %s/%s/%s | %s | %d/%d | %s | %d | %s | %s |"
              % (leg["campaign"], leg["scenario"], leg["leg"], leg["flag"],
                 len(leg["checks"]) - len(failed), len(leg["checks"]),
                 leg["driver_exit"], leg["audit_records"],
                 leg["audit_delta"],
                 leg["deferred_delta"].get("enqueued_total", "—")))
        for name in failed:
            print("  FAIL: " + name)
    for item in count_checks:
        print("audit counts %s equal=%s A=%s B=%s%s"
              % (item["key"], item["equal"], item["a"], item["b"],
                 (" ratio=%s requests=%s/%s" % (
                     item.get("count_ratio"), item.get("a_requests"),
                     item.get("b_requests"))) if "count_ratio" in item else ""))
    print("%d/%d legs passed; audit count groups equal=%s; VERDICT=%s"
          % (document["legs_passed"], document["legs_total"],
             all(item["equal"] for item in count_checks),
             "PASS" if passed else "FAIL"))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
