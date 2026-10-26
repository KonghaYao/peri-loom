"""Bind restart observations to the exact acknowledged workload, not counts.

The manifest proves internal consistency only. Restart instance identities and
the provenance of observations must still be supplied by the run supervisor.
"""
import hashlib
import json

from issue11 import contract
from issue11.gate_common import _is_int

WRITE_SCENARIOS = {"H3", "H4", "H6", "H7"}


def digest(value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False).encode()
    return hashlib.sha256(encoded).hexdigest()


def expected_writes(view, problems):
    """Derive final fixture values and an identity digest from raw ACK records."""
    scenario = view.get("record", {}).get("scenario")
    requests = {item.get("request_id"): item for item in view.get("requests", [])}
    writes, rows, seen = [], {}, set()
    operations = sorted(view.get("operations", []), key=lambda op: op.get("issued_ns", 0))
    for operation in operations:
        if operation.get("phase") not in {"warmup", "timed"}:
            continue
        writing = scenario in WRITE_SCENARIOS and (scenario != "H4" or operation.get("action") == "insert")
        effect = operation.get("write_effect")
        if not writing:
            if effect is not None:
                problems.append(("FAIL", "read_has_write_effect"))
            continue
        if not isinstance(effect, dict):
            if operation.get("outcome") == "success":
                problems.append(("INCOMPLETE", "acknowledged_write_identity_missing"))
            continue
        identity = operation.get("op_id")
        request_id = effect.get("request_id")
        request = requests.get(request_id, {})
        references = operation.get("http_request_ids", [])
        if not isinstance(identity, str) or not identity or identity in seen \
                or not isinstance(references, list) or not references or request_id != references[0] \
                or request.get("outcome") != "success" \
                or request.get("response_complete") is not True:
            problems.append(("FAIL", "acknowledged_write_request_mismatch"))
            continue
        seen.add(identity)
        row_id, value = effect.get("id"), effect.get("value")
        kind = "update" if scenario == "H3" else "insert"
        table = "perf_kv" if scenario == "H3" else "perf_events"
        if effect.get("kind") != kind or effect.get("table") != table \
                or not _is_int(row_id) or row_id < 1 or not _is_int(value):
            problems.append(("FAIL", "acknowledged_write_effect_invalid"))
            continue
        key = (table, row_id)
        if kind == "update":
            expected = rows.get(key, {}).get("value", 0) + 1
            payload = contract.fixture_payload(row_id)
            if row_id > view.get("oracle", {}).get("fixture_scale", 1000):
                problems.append(("FAIL", "update_key_outside_fixture"))
        else:
            expected = row_id
            payload = contract.insert_payload(row_id)
            if key in rows:
                problems.append(("FAIL", "duplicate_acknowledged_insert_key"))
        payload_hash = hashlib.sha256(payload.encode()).hexdigest()
        if value != expected or effect.get("payload_sha256") != payload_hash:
            problems.append(("FAIL", "acknowledged_write_value_mismatch"))
        row = {"table": table, "id": row_id, "value": expected,
               "payload_sha256": payload_hash}
        rows[key] = row
        writes.append({"op_id": identity, "request_id": request_id,
                       "kind": kind, **row})
    return writes, [rows[key] for key in sorted(rows)]


def validate_recovery(view, problems):
    metadata = view.get("metadata", view.get("record", {}))
    if metadata.get("evidence_extension_version") != 2:
        problems.append(("INCOMPLETE", "evidence_extension_v2_missing"))
    writes, expected = expected_writes(view, problems)
    proof = view.get("oracle", {}).get("recovery")
    if not isinstance(proof, dict) or proof.get("schema") != 2:
        problems.append(("INCOMPLETE", "recovery_identity_proof_missing"))
        return
    if proof.get("run_id") != view.get("run_id") \
            or proof.get("database") != metadata.get("database") \
            or not isinstance(metadata.get("database"), str) or not metadata["database"]:
        problems.append(("FAIL", "recovery_run_or_database_mismatch"))
    if proof.get("acknowledged_operations_sha256") != digest(writes):
        problems.append(("FAIL", "recovery_acknowledged_identity_mismatch"))
    if proof.get("acked_writes") != len(writes) or proof.get("recovered_writes") != len(writes):
        problems.append(("FAIL", "recovery_acknowledged_count_mismatch"))
    recovered = proof.get("rows")
    valid_rows = isinstance(recovered, list) and all(isinstance(row, dict)
        and _is_int(row.get("id")) and _is_int(row.get("value")) for row in recovered)
    if not valid_rows or recovered != expected:
        problems.append(("FAIL", "recovery_final_values_mismatch"))
    if not writes:
        return
    restart = proof.get("restart", {})
    if not isinstance(restart, dict):
        problems.append(("INCOMPLETE", "restart_evidence_missing"))
        return
    before, after = restart.get("before_instance"), restart.get("after_instance")
    stopped, verified = restart.get("stopped_at_unix_ms"), restart.get("verified_at_unix_ms")
    end = metadata.get("completed_at_unix_ms")
    if not isinstance(before, str) or not before or not isinstance(after, str) or not after \
            or before == after or restart.get("method") not in {"graceful", "process_crash", "storage_fault"} \
            or not all(_is_int(value) for value in (stopped, verified, end)) \
            or not end <= stopped < verified:
        problems.append(("FAIL", "restart_identity_or_window_invalid"))
