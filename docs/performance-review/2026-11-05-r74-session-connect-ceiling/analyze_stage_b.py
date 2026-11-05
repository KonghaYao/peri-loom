#!/usr/bin/env python3
"""R74 Stage B aggregation and verdict, using only the frozen criteria.

Reads stage-b/legs/r<round>/<seq><arm>/ and writes analysis.json + summary.md.
Never reads the connection file.
"""
from __future__ import annotations

import json
import pathlib
import statistics

ROOT = pathlib.Path(__file__).resolve().parent
LEGS = ROOT / "stage-b" / "legs"
ORDER = [(1, 1, "a"), (1, 2, "b"), (1, 3, "b"), (1, 4, "a"),
         (2, 5, "b"), (2, 6, "a"), (2, 7, "a"), (2, 8, "b")]


def read(path: pathlib.Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def leg_record(round_index: int, seq: int, arm: str) -> dict:
    directory = LEGS / ("r%d" % round_index) / ("%d%s" % (seq, arm))
    driver = read(directory / "driver.json")
    record = {
        "round": round_index, "seq": seq, "arm": arm.upper(),
        "directory": str(directory.relative_to(ROOT)),
        "present": driver is not None,
        "invalid": False, "invalid_reasons": [],
    }
    if driver is None:
        record["invalid"] = True
        record["invalid_reasons"].append("driver.json missing")
        return record
    blocks = driver.get("block_average_ns") or []
    requests = driver.get("request_ns") or []
    record.update({
        "blocks": len(blocks),
        "block_median_ns": int(statistics.median(blocks)) if blocks else None,
        "block_mean_ns": int(statistics.mean(blocks)) if blocks else None,
        "request_p50_ns": int(statistics.median(requests)) if requests else None,
        "request_p95_ns": sorted(requests)[int(0.95 * (len(requests) - 1))] if requests else None,
        "failures": driver.get("failures") or [],
    })
    if len(blocks) != 30:
        record["invalid"] = True
        record["invalid_reasons"].append("blocks=%d" % len(blocks))
    if driver.get("failures"):
        record["invalid"] = True
        record["invalid_reasons"].append("driver failures")
    after = read(directory / "metrics-after.json") or {}
    immediate = read(directory / "metrics-immediate.json") or {}
    for label, metrics in (("after", after), ("immediate", immediate)):
        errors = metrics.get("errors_total") or {}
        rejections = {key: value for key, value in (metrics.get("rejection_reasons_total") or {}).items() if value}
        # The frozen invalid_leg rule names outcome_unknown / 502 / 503 /
        # audit_unavailable / broken audit chain. A default-quota rate-limit 429
        # from the optional X1 c16 observation is not one of them; it is recorded
        # separately and never excuses a judged block failure.
        invalidating_errors = {key: value for key, value in errors.items() if key != "quota_exceeded"}
        invalidating_rejections = {key: value for key, value in rejections.items() if key != "rate_limit"}
        record["%s_errors" % label] = errors
        record["%s_rejections" % label] = rejections
        record["%s_rate_limit_429" % label] = rejections.get("rate_limit", 0)
        if invalidating_errors:
            record["invalid"] = True
            record["invalid_reasons"].append("%s errors: %s" % (label, invalidating_errors))
        if invalidating_rejections:
            record["invalid"] = True
            record["invalid_reasons"].append("%s rejections: %s" % (label, invalidating_rejections))
    record["settled"] = {
        "session_seats": after.get("session_seats"),
        "active_permits": after.get("active_permits"),
        "unknown_held_permits": after.get("unknown_held_permits"),
    }
    verify_text = ""
    verify_path = directory / "audit-verify.txt"
    if verify_path.exists():
        verify_text = verify_path.read_text(encoding="utf-8").strip()
    record["audit_verify"] = verify_text
    if "chain_ok" in verify_text and "chain_ok=True" not in verify_text and "OK" not in verify_text:
        record["invalid"] = True
        record["invalid_reasons"].append("audit chain not continuous")
    probe = []
    probe_path = directory / "probe.txt"
    if probe_path.exists():
        probe = [line for line in probe_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    record["probe_lines"] = probe
    stop = read(directory / "launcher-stop.json") or {}
    payload = stop.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            payload = None
    if isinstance(payload, dict):
        record["launcher_stop"] = {
            "outcome": payload.get("outcome"),
            "reason": payload.get("reason"),
            "unresolved_operations": payload.get("unresolved_operations"),
        }
        if payload.get("unresolved_operations") is not False:
            record["invalid"] = True
            record["invalid_reasons"].append("launcher stop unresolved")
    return record


def main() -> int:
    records = [leg_record(*item) for item in ORDER]
    rounds = {}
    for round_index in (1, 2):
        side = {}
        for arm in ("A", "B"):
            values = [record["block_median_ns"] for record in records
                      if record["round"] == round_index and record["arm"] == arm
                      and record["block_median_ns"]]
            side[arm] = {
                "legs": values,
                "statistic_ns": int(statistics.median(values)) if values else None,
            }
        improvement = None
        if side["A"]["statistic_ns"] and side["B"]["statistic_ns"]:
            improvement = 1.0 - side["B"]["statistic_ns"] / side["A"]["statistic_ns"]
        rounds[round_index] = {"side": side, "improvement": improvement}

    valid = [record for record in records if not record["invalid"]]
    improvements = [rounds[1]["improvement"], rounds[2]["improvement"]]
    primary = min(improvements) if all(value is not None for value in improvements) else None
    if len(valid) != len(records) or primary is None:
        verdict = "INVALID_OR_INCOMPLETE"
    elif primary >= 0.20:
        verdict = "CONTINUE_IMPLEMENT"
    elif primary >= 0.10:
        verdict = "BELOW_F1_20PCT_GATE"
    else:
        verdict = "CLOSE_DIRECTION"

    document = {
        "criteria_sha256": (ROOT / "criteria.sha256").read_text(encoding="utf-8").split()[0]
        if (ROOT / "criteria.sha256").exists() else None,
        "records": records,
        "rounds": {str(key): value for key, value in rounds.items()},
        "primary_result": primary,
        "verdict": verdict,
        "regression_observations": {
            "note": "observations only, not judged",
            "legs": [record["directory"] for record in records if record["seq"] in (7, 8)],
        },
    }
    (ROOT / "stage-b" / "analysis.json").write_text(
        json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "primary_result": primary,
        "verdict": verdict,
        "rounds": {str(key): value["improvement"] for key, value in rounds.items()},
        "invalid": [record["directory"] for record in records if record["invalid"]],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
