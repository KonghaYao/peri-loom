#!/usr/bin/env python3
"""R72b: aggregate the 16 L1 legs and check the frozen criteria.

Reads only the evidence written by the unmodified R72 run_p2.sh under
legs/p2/l1/<leg>/. Never reads a connection file. Writes summary.json,
metrics.json, raw.jsonl, analysis.txt and per-leg audit-verify.json.

Frozen criteria: criteria-r72b.json (see criteria-r72b-freeze.txt).
"""
from __future__ import annotations

import collections
import datetime
import hashlib
import json
import math
import pathlib
import random
import statistics
import sys

HERE = pathlib.Path(__file__).resolve().parent
R72 = HERE.parent / "2026-11-05-r72-deferred-close-default-ab"
sys.path.insert(0, str(R72))
import verify_audit  # noqa: E402  (read-only reuse of the R72 structural check)

PAIRS = [
    ("p1", "p1-a", "p1-b", "A_then_B"),
    ("p2", "p2-a", "p2-b", "B_then_A"),
    ("p3", "p3-a", "p3-b", "B_then_A"),
    ("p4", "p4-a", "p4-b", "A_then_B"),
    ("p5", "p5-a", "p5-b", "B_then_A"),
    ("p6", "p6-a", "p6-b", "A_then_B"),
    ("p7", "p7-a", "p7-b", "A_then_B"),
    ("p8", "p8-a", "p8-b", "B_then_A"),
]
LEG_SEQUENCE = [
    "p1-a", "p1-b", "p2-b", "p2-a", "p3-b", "p3-a", "p4-a", "p4-b",
    "p5-b", "p5-a", "p6-a", "p6-b", "p7-a", "p7-b", "p8-b", "p8-a",
]
LEG_SIDE = {leg: ("a" if leg.endswith("-a") else "b") for leg in LEG_SEQUENCE}
LEG_PAIR = {name: pair for pair, a, b, _ in PAIRS for name in (a, b)}
ARM_OF = {"a": "explicit-0", "b": "unset-default"}
EXPECTED_PLATFORM_SHA = "e07e6da8331d6d40b9f68663899739c75d20f13c8379d685960b74b418c621ba"
EXPECTED_WORKER_SHA = "b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff"
EXPECTED_IMAGE = "sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff"
BOOTSTRAP_SEED = 20261105
BOOTSTRAP_DRAWS = 10000
POOLED_DRAWS = 2000


def read_json(path: pathlib.Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def read_text(path: pathlib.Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


def percentile(values: list[float], percent: float) -> float:
    """Same rank definition as R72 analyze_p2.py / issue28 report.py."""
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * percent / 100.0))]


def median(values):
    return statistics.median(values) if values else None


def ratio(numerator, denominator):
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def bootstrap_median(values: list[float]) -> dict:
    rng = random.Random(BOOTSTRAP_SEED)
    draws = []
    n = len(values)
    for _ in range(BOOTSTRAP_DRAWS):
        draws.append(statistics.median([values[rng.randrange(n)] for _ in range(n)]))
    draws.sort()
    return {
        "draws": BOOTSTRAP_DRAWS,
        "seed": BOOTSTRAP_SEED,
        "median": statistics.median(draws),
        "ci95_low": draws[int(0.025 * len(draws))],
        "ci95_high": draws[min(len(draws) - 1, int(0.975 * len(draws)))],
    }


def bootstrap_pooled_ratio(a: list[float], b: list[float]) -> dict:
    rng = random.Random(BOOTSTRAP_SEED)
    ratios = []
    for _ in range(POOLED_DRAWS):
        ra = [a[rng.randrange(len(a))] for _ in range(len(a))]
        rb = [b[rng.randrange(len(b))] for _ in range(len(b))]
        ratios.append(percentile(rb, 95) / percentile(ra, 95))
    ratios.sort()
    return {
        "draws": POOLED_DRAWS,
        "seed": BOOTSTRAP_SEED,
        "median": statistics.median(ratios),
        "ci95_low": ratios[int(0.025 * len(ratios))],
        "ci95_high": ratios[min(len(ratios) - 1, int(0.975 * len(ratios)))],
    }


def stop_payload(document):
    """The launcher-stop file is a MAC envelope whose payload is a JSON string."""
    if isinstance(document, dict) and isinstance(document.get("payload"), str):
        try:
            return json.loads(document["payload"])
        except json.JSONDecodeError:
            return document
    return document


def leg_identity(directory: pathlib.Path) -> dict:
    identity = {}
    for line in read_text(directory / "leg.txt").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            identity[key.strip()] = value.strip()
    return identity


def metrics_delta(before: dict | None, after: dict | None, section: str) -> dict:
    if not before or not after:
        return {}
    left = before.get(section) or {}
    right = after.get(section) or {}
    moved = {}
    for key in sorted(set(left) | set(right)):
        if isinstance(left.get(key), dict) or isinstance(right.get(key), dict):
            continue
        value = int(right.get(key) or 0) - int(left.get(key) or 0)
        if value:
            moved[key] = value
    return moved


def metric_delta(before: dict | None, after: dict | None, key: str):
    if not before or not after:
        return None
    return int(after.get(key) or 0) - int(before.get(key) or 0)


def worker_state(metrics: dict | None) -> dict:
    workers = ((metrics or {}).get("native") or {}).get("workers") or {}
    result = {}
    for name, worker in workers.items():
        result[name] = {
            "sessions": int(worker.get("sessions") or 0),
            "pending_close": int((worker.get("lifecycle") or {}).get("pending_close", {}).get("count") or 0),
        }
    return result


def parse_env_line(text: str, prefix: str) -> str | None:
    for line in text.splitlines():
        if line.strip().startswith(prefix):
            return line.strip()[len(prefix):].strip()
    return None


def load_leg(leg: str) -> dict:
    directory = HERE / "legs" / "p2" / "l1" / leg
    harness = directory / "harness"
    if not directory.is_dir():
        return {
            "leg": leg, "pair": LEG_PAIR[leg], "side": LEG_SIDE[leg],
            "arm": ARM_OF[LEG_SIDE[leg]], "order_index": LEG_SEQUENCE.index(leg),
            "port": None, "volume": None, "container": None,
            "timed": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None,
            "mean_ms": None, "min_ms": None, "max_ms": None,
            "failures": 0, "failure_sample": [],
            "response_mode": {}, "driver_exit": None, "settle_exit": None,
            "settle_verdict": None, "stop_exit": None, "stop_outcome": None,
            "unresolved_operations": None, "audit_records": 0,
            "audit_chain_ok": False, "audit_errors": [], "audit_delta": None,
            "errors_delta": {}, "rejection_delta": {}, "actions_delta": {},
            "operation_settle": {}, "session_seats": None,
            "active_permits": None, "unknown_held_permits": None,
            "worker_sessions": {}, "worker_pending_close": {},
            "deferred_present_after": False, "deferred_delta": {},
            "deferred_now": {}, "seed_line": "", "row_verdicts": {},
            "summary_duration_seconds": None, "platform_sha_check": False,
            "worker_sha_check": False, "image_check": False,
            "valid": False, "invalid_reasons": ["leg directory missing (not run)"],
            "_durations": [],
        }
    identity = leg_identity(directory)
    before = read_json(directory / "metrics-before.json")
    after = read_json(directory / "metrics-after.json")
    settle = read_json(directory / "settle.json")
    stop = stop_payload(read_json(directory / "launcher-stop.json"))
    exit_code = read_json(directory / "exit-code.json") or {}
    settle_code = read_json(directory / "settle-exit-code.json") or {}
    stop_exit = read_text(directory / "stop-exit.txt").strip()
    audit_delta = None
    try:
        audit_delta = (int(read_text(directory / "audit-count-after.txt").strip())
                       - int(read_text(directory / "audit-count-before.txt").strip()))
    except ValueError:
        pass
    seed_line = read_text(directory / "seed.txt").strip().splitlines()
    seed_line = seed_line[-1] if seed_line else ""

    raw = []
    raw_error = ""
    try:
        raw = [json.loads(line) for line in read_text(harness / "raw.jsonl").splitlines() if line.strip()]
    except json.JSONDecodeError as error:
        raw_error = "raw.jsonl invalid: %s" % error
    timed = [item for item in raw if item.get("phase") == "timed"]
    durations = [float(item.get("duration_ms") or 0.0) for item in timed]
    failures = [item for item in timed
                if item.get("status") != 200
                or item.get("outcome") != "ok"
                or item.get("error_code")]
    shapes = collections.Counter(
        (item.get("response_bytes"), item.get("rows"), item.get("rows_read"),
         item.get("rows_written"), item.get("affected_row_count"))
        for item in timed)
    shape_mode = shapes.most_common(1)[0] if shapes else (None, 0)
    rows_bad = [item for item in timed
                if item.get("rows") != 40 or item.get("rows_read") != 201]
    summary = read_json(harness / "summary.json") or {}
    row_verdicts = {row.get("scenario"): row.get("verdict")
                    for row in summary.get("rows") or []}

    audit = verify_audit.verify(directory)
    (directory / "audit-verify.json").write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    deferred_after_present = bool(after is not None and "deferred_session_close" in after)
    deferred_before_present = bool(before is not None and "deferred_session_close" in before)
    deferred_delta = {}
    if before is not None and after is not None and deferred_after_present:
        left = before.get("deferred_session_close") or {}
        right = after.get("deferred_session_close") or {}
        for field in ("enqueued_total", "failed_total", "retried_total",
                      "unresolved_total", "flushed_total", "inline_total"):
            deferred_delta[field] = int(right.get(field) or 0) - int(left.get(field) or 0)
    deferred_now = (after or {}).get("deferred_session_close") or {}

    errors_delta = metrics_delta(before, after, "errors_total")
    rejections_delta = metrics_delta(before, after, "rejection_reasons_total")
    actions_delta = metrics_delta(before, after, "actions_total")
    operation_settle = (after or {}).get("operation_settle") or {}
    cleanup_ledger = (after or {}).get("cleanup_ledger") or {}
    workers = worker_state(after)

    invalid = []
    if raw_error:
        invalid.append(raw_error)
    if len(timed) != 500:
        invalid.append("expected 500 timed samples, got %d" % len(timed))
    if failures:
        invalid.append("%d timed request failures (first: %s)" % (
            len(failures),
            {key: failures[0].get(key) for key in ("status", "outcome", "error_code", "detail")}))
    if rows_bad:
        invalid.append("%d timed samples with rows/rows_read != 40/201" % len(rows_bad))
    if shape_mode[0] is None:
        invalid.append("no timed response shape")
    elif shape_mode[0][0] != 15627:
        invalid.append("response_bytes mode %r != 15627" % (shape_mode[0][0],))
    if exit_code.get("driver") != 0:
        invalid.append("driver exit %r" % exit_code.get("driver"))
    if summary.get("fatal"):
        invalid.append("harness fatal: %s" % summary.get("fatal"))
    if summary.get("warnings"):
        invalid.append("harness warnings: %s" % summary.get("warnings"))
    if settle_code.get("settle") != 0 or (settle or {}).get("verdict") != "settled":
        invalid.append("settle exit=%r verdict=%r" % (
            settle_code.get("settle"), (settle or {}).get("verdict")))
    if stop_exit != "0":
        invalid.append("docker stop exit %r" % stop_exit)
    if (stop or {}).get("outcome") != "clean":
        invalid.append("launcher stop outcome %r" % (stop or {}).get("outcome"))
    if (stop or {}).get("unresolved_operations") is not False:
        invalid.append("launcher unresolved_operations %r" % (stop or {}).get("unresolved_operations"))
    fixture = summary.get("fixture") or {}
    fixture_ok = (fixture.get("threads") == 201
                  and fixture.get("messages") == 8625
                  and fixture.get("content_chars") == 20177069
                  and fixture.get("focus_messages") == 8625)
    if not fixture_ok:
        invalid.append("fixture counts %r" % fixture)
    if "threads=201 messages=8625 chars=20177069 focus=8625" not in seed_line:
        invalid.append("fixture seed line %r" % seed_line)
    if not audit.get("chain_ok"):
        invalid.append("audit chain: %s" % audit.get("errors", [])[:3])
    if audit_delta is None or audit_delta <= 0:
        invalid.append("audit delta %r" % audit_delta)
    if after is None:
        invalid.append("metrics-after missing")
    else:
        if int(after.get("session_seats") or 0) != 0:
            invalid.append("session_seats=%r" % after.get("session_seats"))
        if int(after.get("active_permits") or 0) != 0:
            invalid.append("active_permits=%r" % after.get("active_permits"))
        if int(after.get("unknown_held_permits") or 0) != 0:
            invalid.append("unknown_held_permits=%r" % after.get("unknown_held_permits"))
        if any(value["sessions"] != 0 for value in workers.values()):
            invalid.append("worker sessions %r" % workers)
        if any(value["pending_close"] != 0 for value in workers.values()):
            invalid.append("worker pending_close %r" % workers)
        if int((cleanup_ledger.get("pending_close") or {}).get("count") or 0) != 0:
            invalid.append("cleanup ledger pending_close %r" % cleanup_ledger.get("pending_close"))
        if errors_delta:
            invalid.append("error counters %r" % errors_delta)
        if int(operation_settle.get("exhausted_total") or 0) != 0:
            invalid.append("operation_settle exhausted_total=%r" % operation_settle.get("exhausted_total"))
        if operation_settle.get("refused_total"):
            invalid.append("operation_settle refused_total=%r" % operation_settle.get("refused_total"))
        side = LEG_SIDE[leg]
        if side == "a":
            if deferred_before_present or deferred_after_present:
                invalid.append("A arm publishes deferred_session_close")
        else:
            if not deferred_after_present:
                invalid.append("B arm does not publish deferred_session_close")
            else:
                if int(deferred_delta.get("enqueued_total") or 0) <= 0:
                    invalid.append("B deferred enqueued_total delta=%r" % deferred_delta.get("enqueued_total"))
                if int(deferred_delta.get("failed_total") or 0) != 0:
                    invalid.append("B deferred failed_total delta=%r" % deferred_delta.get("failed_total"))
                if int(deferred_delta.get("retried_total") or 0) != 0:
                    invalid.append("B deferred retried_total delta=%r" % deferred_delta.get("retried_total"))
                if int(deferred_now.get("unresolved_total") or 0) != 0:
                    invalid.append("B deferred unresolved_total=%r" % deferred_now.get("unresolved_total"))
                if int(deferred_now.get("pending") or 0) != 0:
                    invalid.append("B deferred pending=%r" % deferred_now.get("pending"))
                if int(deferred_now.get("pending_max") or 0) > 4:
                    invalid.append("B deferred pending_max=%r" % deferred_now.get("pending_max"))

    binaries = read_text(directory / "binaries.txt")
    return {
        "leg": leg,
        "pair": LEG_PAIR[leg],
        "side": LEG_SIDE[leg],
        "arm": ARM_OF[LEG_SIDE[leg]],
        "order_index": LEG_SEQUENCE.index(leg),
        "port": identity.get("port"),
        "volume": identity.get("volume"),
        "container": identity.get("container"),
        "timed": len(timed),
        "p50_ms": round(statistics.median(durations), 4) if durations else None,
        "p95_ms": round(percentile(durations, 95), 4) if durations else None,
        "p99_ms": round(percentile(durations, 99), 4) if durations else None,
        "mean_ms": round(statistics.fmean(durations), 4) if durations else None,
        "min_ms": round(min(durations), 4) if durations else None,
        "max_ms": round(max(durations), 4) if durations else None,
        "failures": len(failures),
        "failure_sample": failures[:3],
        "response_mode": {
            "response_bytes": shape_mode[0][0] if shape_mode[0] else None,
            "rows": shape_mode[0][1] if shape_mode[0] else None,
            "rows_read": shape_mode[0][2] if shape_mode[0] else None,
            "rows_written": shape_mode[0][3] if shape_mode[0] else None,
            "affected_row_count": shape_mode[0][4] if shape_mode[0] else None,
            "mode_samples": shape_mode[1],
            "distinct_response_bytes": sorted({key[0] for key in shapes}),
        },
        "driver_exit": exit_code.get("driver"),
        "settle_exit": settle_code.get("settle"),
        "settle_verdict": (settle or {}).get("verdict"),
        "stop_exit": stop_exit,
        "stop_outcome": (stop or {}).get("outcome"),
        "unresolved_operations": (stop or {}).get("unresolved_operations"),
        "audit_records": audit.get("records"),
        "audit_chain_ok": audit.get("chain_ok"),
        "audit_errors": audit.get("errors", [])[:3],
        "audit_delta": audit_delta,
        "errors_delta": errors_delta,
        "rejection_delta": rejections_delta,
        "actions_delta": actions_delta,
        "operation_settle": {key: operation_settle.get(key) for key in
                             ("retry_total", "retry_success_total", "exhausted_total",
                              "refused_total", "retry_budget_ms")},
        "session_seats": (after or {}).get("session_seats"),
        "active_permits": (after or {}).get("active_permits"),
        "unknown_held_permits": (after or {}).get("unknown_held_permits"),
        "worker_sessions": {name: value["sessions"] for name, value in workers.items()},
        "worker_pending_close": {name: value["pending_close"] for name, value in workers.items()},
        "deferred_present_after": deferred_after_present,
        "deferred_delta": deferred_delta,
        "deferred_now": {key: deferred_now.get(key) for key in
                          ("enqueued_total", "failed_total", "retried_total",
                           "unresolved_total", "pending", "pending_max", "maximum",
                           "per_database_maximum")},
        "seed_line": seed_line,
        "row_verdicts": row_verdicts,
        "summary_duration_seconds": (summary.get("metadata") or {}).get("duration_seconds"),
        "platform_sha_check": EXPECTED_PLATFORM_SHA in binaries,
        "worker_sha_check": EXPECTED_WORKER_SHA in binaries,
        "image_check": EXPECTED_IMAGE in binaries,
        "valid": not invalid,
        "invalid_reasons": invalid,
        "_durations": durations,
    }


def main() -> int:
    generated = datetime.datetime.now(datetime.timezone.utc).isoformat()
    criteria_sha = hashlib.sha256((HERE / "criteria-r72b.json").read_bytes()).hexdigest()
    frozen = read_text(HERE / "criteria-r72b-freeze.txt")
    frozen_sha = parse_env_line(frozen, "criteria_sha256:") or ""
    freeze_ok = criteria_sha == frozen_sha

    legs = [load_leg(leg) for leg in LEG_SEQUENCE]
    by_leg = {leg["leg"]: leg for leg in legs}

    pairs = []
    for pair, a_leg, b_leg, order in PAIRS:
        a = by_leg[a_leg]
        b = by_leg[b_leg]
        pairs.append({
            "pair": pair,
            "order": order,
            "a_leg": a_leg,
            "b_leg": b_leg,
            "a_p50_ms": a["p50_ms"], "b_p50_ms": b["p50_ms"],
            "a_p95_ms": a["p95_ms"], "b_p95_ms": b["p95_ms"],
            "p50_ratio": round(ratio(b["p50_ms"], a["p50_ms"]), 5)
            if ratio(b["p50_ms"], a["p50_ms"]) is not None else None,
            "p95_ratio": round(ratio(b["p95_ms"], a["p95_ms"]), 5)
            if ratio(b["p95_ms"], a["p95_ms"]) is not None else None,
            "valid": a["valid"] and b["valid"],
        })

    valid_pairs = [pair for pair in pairs if pair["valid"]]
    p95_ratios = [pair["p95_ratio"] for pair in valid_pairs if pair["p95_ratio"] is not None]
    p50_ratios = [pair["p50_ratio"] for pair in valid_pairs if pair["p50_ratio"] is not None]
    primary = median(p95_ratios)
    auxiliary = median(p50_ratios)
    first_half = [pair for pair in valid_pairs if int(pair["pair"][1:]) <= 4]
    second_half = [pair for pair in valid_pairs if int(pair["pair"][1:]) > 4]

    a_durations = [value for leg in legs if leg["side"] == "a" for value in leg["_durations"]]
    b_durations = [value for leg in legs if leg["side"] == "b" for value in leg["_durations"]]
    pooled = {
        "a_samples": len(a_durations),
        "b_samples": len(b_durations),
        "a_p50_ms": round(statistics.median(a_durations), 4) if a_durations else None,
        "b_p50_ms": round(statistics.median(b_durations), 4) if b_durations else None,
        "a_p95_ms": round(percentile(a_durations, 95), 4) if a_durations else None,
        "b_p95_ms": round(percentile(b_durations, 95), 4) if b_durations else None,
        "p50_ratio": round(statistics.median(b_durations) / statistics.median(a_durations), 5)
        if a_durations and b_durations else None,
        "p95_ratio": round(percentile(b_durations, 95) / percentile(a_durations, 95), 5)
        if a_durations and b_durations else None,
    }
    if valid_pairs and a_durations and b_durations:
        pooled["bootstrap"] = bootstrap_pooled_ratio(a_durations, b_durations)
    else:
        pooled["bootstrap"] = None

    invalid_legs = {leg["leg"]: leg["invalid_reasons"] for leg in legs if not leg["valid"]}
    invariant_failures = sorted({reason for reasons in invalid_legs.values() for reason in reasons})
    quota_blocked = any(
        int((leg["errors_delta"] or {}).get("quota_exceeded") or 0) > 0
        for leg in legs)
    executed_legs = [leg["leg"] for leg in legs if leg["timed"] > 0]
    env_checks = {
        "criteria_freeze_matches": freeze_ok,
        "all_16_legs_present_with_500_samples": all(leg["timed"] == 500 for leg in legs),
        "platform_sha_recorded_in_every_leg": all(leg["platform_sha_check"] for leg in legs),
        "worker_sha_recorded_in_every_leg": all(leg["worker_sha_check"] for leg in legs),
        "image_digest_recorded_in_every_leg": all(leg["image_check"] for leg in legs),
        "all_legs_valid": not invalid_legs,
    }

    if primary is not None and auxiliary is not None and len(valid_pairs) == len(PAIRS) and freeze_ok:
        verdict = "PASS" if (primary < 1.10 and auxiliary < 1.10) else "FAIL"
        blocked_reason = ""
    else:
        verdict = "ENVIRONMENT_BLOCKED"
        if not freeze_ok:
            blocked_reason = "criteria freeze does not match criteria-r72b.json"
        elif len(valid_pairs) < len(PAIRS) and quota_blocked:
            blocked_reason = ("the faster B leg exhausted the default project quota "
                              "requests_per_second=500 before 500 timed requests; "
                              "see quota-conflict.txt. No unbiased 8-pair estimate is possible.")
        elif len(valid_pairs) < len(PAIRS):
            blocked_reason = ("the frozen protocol did not yield 8 valid pairs "
                              "(%d/8); no primary statistic can be computed" % len(valid_pairs))
        else:
            blocked_reason = "measurement incomplete"

    summary = {
        "campaign": "R72b L1 c1 p95 non-inferiority retest",
        "generated_at_utc": generated,
        "criteria_file": "criteria-r72b.json",
        "criteria_sha256": criteria_sha,
        "criteria_frozen_at_utc": parse_env_line(frozen, "frozen_at_utc:"),
        "criteria_freeze_matches": freeze_ok,
        "candidate_commit": "56dc9142d62c8ce22445b2b8aa90ed00fa8fbba6",
        "design": {
            "pairs": len(PAIRS),
            "legs": len(LEG_SEQUENCE),
            "leg_sequence": LEG_SEQUENCE,
            "per_leg_timed_samples": 500,
            "workload": "L1.list-20 json-pipeline c1",
        },
        "primary": {
            "metric": "median of the 8 per-pair p95(B)/p95(A) ratios",
            "value": round(primary, 5) if primary is not None else None,
            "threshold": 1.10,
            "pass": primary is not None and primary < 1.10,
            "bootstrap_over_pairs": bootstrap_median(p95_ratios) if p95_ratios else None,
        },
        "auxiliary": {
            "metric": "median of the 8 per-pair p50(B)/p50(A) ratios",
            "value": round(auxiliary, 5) if auxiliary is not None else None,
            "threshold": 1.10,
            "pass": auxiliary is not None and auxiliary < 1.10,
            "bootstrap_over_pairs": bootstrap_median(p50_ratios) if p50_ratios else None,
        },
        "halves": {
            "first_4_pairs_p95_median": round(median([pair["p95_ratio"] for pair in first_half]), 5)
            if first_half else None,
            "last_4_pairs_p95_median": round(median([pair["p95_ratio"] for pair in second_half]), 5)
            if second_half else None,
            "first_4_pairs_p50_median": round(median([pair["p50_ratio"] for pair in first_half]), 5)
            if first_half else None,
            "last_4_pairs_p50_median": round(median([pair["p50_ratio"] for pair in second_half]), 5)
            if second_half else None,
        },
        "pooled": pooled,
        "pairs": pairs,
        "legs": [{key: value for key, value in leg.items() if not key.startswith("_")}
                 for leg in legs],
        "invariants": {
            "checks": env_checks,
            "invalid_legs": invalid_legs,
            "failure_reasons": invariant_failures,
        },
        "blocked_reason": blocked_reason,
        "executed_legs": executed_legs,
        "not_run_legs": [leg["leg"] for leg in legs if leg["timed"] == 0],
        "frozen_rule_note": ("invalid legs are recorded mechanically; the campaign did not "
                             "complete 8 valid pairs, so the p95 non-inferiority threshold "
                             "was not evaluated and no p95 FAIL is claimed"),
        "verdict": verdict,
    }
    (HERE / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    metrics = {
        "generated_at_utc": generated,
        "legs": {
            leg["leg"]: {
                "side": leg["side"], "arm": leg["arm"],
                "actions_delta": leg["actions_delta"],
                "errors_delta": leg["errors_delta"],
                "rejection_delta": leg["rejection_delta"],
                "operation_settle": leg["operation_settle"],
                "session_seats": leg["session_seats"],
                "active_permits": leg["active_permits"],
                "unknown_held_permits": leg["unknown_held_permits"],
                "worker_sessions": leg["worker_sessions"],
                "worker_pending_close": leg["worker_pending_close"],
                "deferred_present_after": leg["deferred_present_after"],
                "deferred_delta": leg["deferred_delta"],
                "deferred_now": leg["deferred_now"],
                "audit_records": leg["audit_records"],
                "audit_chain_ok": leg["audit_chain_ok"],
                "audit_delta": leg["audit_delta"],
                "response_mode": leg["response_mode"],
            } for leg in legs
        },
    }
    (HERE / "metrics.json").write_text(
        json.dumps(metrics, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    with open(HERE / "raw.jsonl", "w", encoding="utf-8") as handle:
        for leg in legs:
            directory = HERE / "legs" / "p2" / "l1" / leg["leg"] / "harness" / "raw.jsonl"
            for line in read_text(directory).splitlines():
                if not line.strip():
                    continue
                item = json.loads(line)
                item["leg"] = leg["leg"]
                item["pair"] = leg["pair"]
                item["side"] = leg["side"]
                item["arm"] = leg["arm"]
                handle.write(json.dumps(item, ensure_ascii=False) + "\n")

    lines = []
    lines.append("R72b L1 c1 p95 retest")
    lines.append("generated: %s" % generated)
    lines.append("criteria sha256: %s (freeze match: %s)" % (criteria_sha, freeze_ok))
    lines.append("")
    lines.append("| pair | order | A leg | B leg | A p50 | B p50 | p50 B/A | A p95 | B p95 | p95 B/A | valid |")
    lines.append("|---|---|---|---|---:|---:|---:|---:|---:|---:|---|")
    for pair in pairs:
        lines.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            pair["pair"], pair["order"], pair["a_leg"], pair["b_leg"],
            pair["a_p50_ms"], pair["b_p50_ms"], pair["p50_ratio"],
            pair["a_p95_ms"], pair["b_p95_ms"], pair["p95_ratio"], pair["valid"]))
    lines.append("")
    lines.append("primary p95 ratio median: %s (threshold < 1.10) -> %s" % (
        summary["primary"]["value"], summary["primary"]["pass"]))
    lines.append("auxiliary p50 ratio median: %s (threshold < 1.10) -> %s" % (
        summary["auxiliary"]["value"], summary["auxiliary"]["pass"]))
    lines.append("halves: %s" % json.dumps(summary["halves"], sort_keys=True))
    lines.append("pooled: %s" % json.dumps(pooled, sort_keys=True))
    lines.append("")
    lines.append("| leg | side | port | timed | p50 | p95 | p99 | failures | audit delta | invalid |")
    lines.append("|---|---|---|---:|---:|---:|---:|---:|---:|---|")
    for leg in legs:
        lines.append("| %s | %s | %s | %d | %s | %s | %s | %d | %s | %s |" % (
            leg["leg"], leg["side"], leg["port"], leg["timed"], leg["p50_ms"],
            leg["p95_ms"], leg["p99_ms"], leg["failures"], leg["audit_delta"],
            "; ".join(leg["invalid_reasons"]) or ""))
    lines.append("")
    lines.append("VERDICT: %s" % verdict)
    if invalid_legs:
        lines.append("INVALID LEGS: %s" % json.dumps(invalid_legs, ensure_ascii=False))
    (HERE / "analysis.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("\n".join(lines))
    return 0 if verdict == "PASS" else (2 if verdict == "ENVIRONMENT_BLOCKED" else 1)


if __name__ == "__main__":
    sys.exit(main())
