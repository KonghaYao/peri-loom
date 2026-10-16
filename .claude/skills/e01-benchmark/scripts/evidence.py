"""只归约输入测量；没有 adapter 或足压证据时不产生性能 PASS。"""

import math

from contract import FAMILIES, VERSION, digest, exact, integer, require
from model import generate

OUTCOMES = ("success", "rejected", "timeout", "known_failure", "unknown", "not_dispatched")
VERDICTS = ("match", "mismatch", "not_checked")
RECORD_FIELDS = ("op_id", "outcome", "planned_ns", "dispatched_ns", "completed_ns",
                 "result_sha256", "oracle", "error_class", "error_message")


def template(config):
    bundle = generate(config)
    return {"version": VERSION, "config_sha256": digest(config),
            "dataset_sha256": bundle["dataset_sha256"], "queries_sha256": bundle["queries_sha256"],
            "target": "native", "sync_mode": config["sync_mode"], "clock": "monotonic_ns",
            "records": []}


def quantiles(values):
    ordered = sorted(values)
    def percentile(fraction):
        return ordered[math.ceil(len(ordered) * fraction) - 1] if ordered else None
    return {"samples": len(ordered), "p50_ms": percentile(0.50),
            "p95_ms": percentile(0.95) if len(ordered) >= 100 else None,
            "p99_ms": percentile(0.99) if len(ordered) >= 1000 else None}


def summarize(config, raw):
    expected_raw = template(config)
    exact(raw, expected_raw)
    for key in ("version", "config_sha256", "dataset_sha256", "queries_sha256", "clock", "sync_mode"):
        require(raw[key] == expected_raw[key], "evidence_identity_mismatch")
    require(raw["target"] in ("native", "platform"), "invalid_target")
    require(type(raw["records"]) is list and len(raw["records"]) <= 1024, "invalid_records")
    bundle = generate(config)
    operations = {q["op_id"]: q for q in bundle["queries"]}
    seen, mismatches, digest_mismatches, verified = set(), 0, 0, 0
    counters = {q: {s: 0 for s in OUTCOMES} for q in FAMILIES}
    latency = {q: [] for q in FAMILIES}
    success_latency = {q: [] for q in FAMILIES}
    dispatch_lag = {q: [] for q in FAMILIES}
    for record in raw["records"]:
        exact(record, RECORD_FIELDS)
        identity = record["op_id"]
        require(type(identity) is str and identity in operations and identity not in seen,
                "unknown_or_duplicate_operation")
        seen.add(identity)
        family, outcome = operations[identity]["family"], record["outcome"]
        require(outcome in OUTCOMES and record["oracle"] in VERDICTS, "invalid_outcome")
        for name, limit in (("error_class", 64), ("error_message", 200)):
            value = record[name]
            require(value is None or (type(value) is str and len(value) <= limit), "invalid_error_detail")
        counters[family][outcome] += 1
        planned, dispatched, completed = (record[k] for k in ("planned_ns", "dispatched_ns", "completed_ns"))
        require(integer(planned, 0, 2**63 - 1), "invalid_planned_time")
        if outcome == "not_dispatched":
            require(dispatched is None and completed is None and record["result_sha256"] is None,
                    "invalid_not_dispatched")
            require(record["oracle"] == "not_checked", "not_dispatched_cannot_be_checked")
            continue
        require(integer(dispatched, planned, 2**63 - 1)
                and integer(completed, dispatched, 2**63 - 1), "invalid_measurement_time")
        elapsed = (completed - planned) / 1000000
        latency[family].append(elapsed)
        dispatch_lag[family].append((dispatched - planned) / 1000000)
        if outcome == "success":
            checksum = record["result_sha256"]
            require(type(checksum) is str and len(checksum) == 64
                    and all(c in "0123456789abcdef" for c in checksum), "invalid_result_digest")
            require(record["oracle"] in ("match", "mismatch"), "success_requires_oracle_verdict")
            if record["oracle"] == "mismatch":
                mismatches += 1
            if checksum != operations[identity]["expected_sha256"]:
                digest_mismatches += 1
            if record["oracle"] == "match" and checksum == operations[identity]["expected_sha256"]:
                success_latency[family].append(elapsed)
                verified += 1
        else:
            require(record["result_sha256"] is None, "failure_result_must_be_null")
            require(record["oracle"] == "not_checked", "failure_cannot_be_checked")
    missing = len(operations) - len(seen)
    failures = sum(sum(value for key, value in counts.items() if key != "success")
                   for counts in counters.values())
    correctness = "NOT MEASURED"
    if raw["records"]:
        if mismatches or digest_mismatches:
            correctness = "FAIL_STOP"
        elif missing or failures:
            correctness = "INCOMPLETE"
        else:
            correctness = "ORACLE_VERIFIED"
    verified_ops = verified
    return {"version": VERSION, "config_sha256": digest(config), "raw_sha256": digest(raw),
            "target": raw["target"], "mode": "smoke", "correctness": correctness,
            "missing_records": missing, "oracle_mismatches": mismatches,
            "digest_mismatches": digest_mismatches, "ops_total": len(operations),
            "oracle_verified_ops": verified_ops,
            "families": {q: {"outcomes": counters[q], "all_dispatched_latency": quantiles(latency[q]),
                             "verified_success_latency": quantiles(success_latency[q]),
                             "dispatch_lag": quantiles(dispatch_lag[q])} for q in FAMILIES},
            "scale_coverage": "SMOKE_ONLY", "performance": "NOT MEASURED",
            "overload_safety": "NOT MEASURED", "capacity": "NOT MEASURED",
            "evidence_trust": "TARGET_REPORTED_ROWS_COMPARED_TO_LOCAL_ORACLE"}
