"""Synthetic extension-v2 fixtures. Never product/recovery evidence."""
import hashlib

from issue11 import contract
from issue11.recovery_v2 import digest, expected_writes


def extended_fixture():
    return {
        "capacity": {"offered_rps": 10, "successful_rps": 10, "concurrency": 1,
                     "duration_seconds": 60, "arrival_fraction_of_capacity": 0.5},
        "queueing": {"wait_p50_ms": 0, "wait_p95_ms": 0, "wait_p99_ms": 0, "queue_depth_max": 0},
        "overload": {"offered": 10, "completed": 4, "rejected_before_execution": 6, "offered_concurrency": 8,
                     "rejection_ratio": 0.6, "all_request_p50_ms": 1,
                     "all_request_p95_ms": 1, "all_request_p99_ms": 1,
                     "failure": 0, "outcome_unknown": 0},
        "quotas": {"configured": {"max_concurrent": 4}, "observed": 4, "violations": 0},
        "confidence_intervals": {
            "method": "synthetic-contract-test", "confidence": 0.95,
            "p50_lower": 0, "p50_upper": 0, "p99_lower": 0, "p99_upper": 0,
            "paired_p50_difference_lower_ms": 0, "paired_p50_difference_upper_ms": 0,
            "paired_p99_difference_lower_ms": 0, "paired_p99_difference_upper_ms": 0,
        },
    }


def recovery_fixture(run_id, scenario, operations, requests, metadata):
    revisions, sequence = {}, 0
    for operation in operations:
        writing = scenario in {"H3", "H4", "H6", "H7"} and (
            scenario != "H4" or operation.get("action") == "insert")
        if not writing:
            continue
        row_id = 1 + operation["sample_index"] % 1000 if scenario == "H3" else 10_000_000 + sequence
        sequence += 1
        value = revisions.get(row_id, 0) + 1 if scenario == "H3" else row_id
        revisions[row_id] = value
        text = contract.fixture_payload(row_id) if scenario == "H3" else contract.insert_payload(row_id)
        operation["write_effect"] = {
            "request_id": operation["http_request_ids"][0],
            "kind": "update" if scenario == "H3" else "insert",
            "table": "perf_kv" if scenario == "H3" else "perf_events",
            "id": row_id, "value": value, "payload_sha256": hashlib.sha256(text.encode()).hexdigest(),
        }
    problems = []
    writes, rows = expected_writes({"record": {"scenario": scenario},
        "operations": operations, "requests": requests, "oracle": {"fixture_scale": 1000}}, problems)
    assert not problems, problems
    return {
        "schema": 2, "run_id": run_id, "database": metadata["database"],
        "acknowledged_operations_sha256": digest(writes),
        "acked_writes": len(writes), "recovered_writes": len(writes),
        "mismatches": 0, "rows": rows,
        "restart": {"before_instance": "synthetic-before", "after_instance": "synthetic-after",
                    "method": "process_crash", "stopped_at_unix_ms": metadata["completed_at_unix_ms"] + 1,
                    "verified_at_unix_ms": metadata["completed_at_unix_ms"] + 2},
    }
