#!/usr/bin/env python3
"""Issue 8 S0/S1/S2 workload schedules, payload model, and sample budgets."""
import hashlib
import json

import perf_contract

ROWS = 64
PAYLOAD_BYTES = 256
S0_MODES = ("read", "write", "mixed")
REQUIRED_RECORD_FIELDS = ("run", "pair", "arm", "case", "lane", "seq", "kind", "phase",
                          "planned_at", "issued_at", "completed_at", "duration_ns",
                          "outcome", "ack_type", "durable_ack", "oracle")
BUCKETS = {"S0": ("read", "write", "mixed"), "S1": ("write", "all"), "S2": ("read", "write", "all")}


class WorkloadError(ValueError):
    pass


def payload_value(index, rows=ROWS, payload_bytes=PAYLOAD_BYTES):
    return ("y" if (index // rows) % 2 == 0 else "z") * payload_bytes


def write_sql(index, rows=ROWS, payload_bytes=PAYLOAD_BYTES):
    return ("UPDATE bench SET payload='" + payload_value(index, rows, payload_bytes)
            + "' WHERE id=" + str(index % rows))


def read_sql(index, rows=ROWS):
    return "SELECT payload FROM bench WHERE id=" + str(index % rows)


def operation_kind(case_id, index):
    if case_id == "S1":
        return "write"
    if case_id == "S2":
        return "write" if index % 2 == 1 else "read"
    raise WorkloadError("case_not_implemented: " + str(case_id))


def operation(case_id, mode, index):
    if case_id == "S0":
        if mode not in S0_MODES:
            raise WorkloadError("s0_mode_unknown: " + str(mode))
        kind = "write" if mode == "write" or (mode == "mixed" and index % 2 == 1) else "read"
    else:
        kind = operation_kind(case_id, index)
    write = kind == "write"
    return {"kind": kind, "operation": "exec" if write else "query", "row": index % ROWS,
            "sql": write_sql(index) if write else read_sql(index),
            "payload": payload_value(index) if write else None}


def case_plan(profile, case_id, concurrency):
    parameters = perf_contract.validate_case_parameters(case_id, concurrency=concurrency)
    if case_id not in profile.get("case_set", []):
        raise WorkloadError("case_not_allowed_for_profile: " + str(case_id) + "/" + profile["name"])
    if case_id == "S0":
        operations = 100
        warmup = 0
    else:
        operations = profile["min_samples_per_lane"]
        if case_id == "S2":
            operations *= 2
        warmup = profile["warmup_operations"]
    return {"case": case_id, "rows": parameters["rows"], "payload_bytes": parameters["payload_bytes"],
            "concurrency": concurrency, "required_operations_per_lane": operations,
            "minimum_samples_per_bucket": profile["min_samples_per_lane"] if case_id != "S0" else None,
            "warmup_operations": warmup, "minimum_seconds": profile["min_seconds"],
            "max_seconds_per_arm": profile["max_seconds_per_arm"], "buckets": BUCKETS[case_id],
            "setup_insert_chunks": parameters["setup_insert_chunks"]}


class PayloadModel:
    def __init__(self, rows=ROWS, payload_bytes=PAYLOAD_BYTES):
        self.rows = rows
        self.payload_bytes = payload_bytes
        self.values = ["x" * payload_bytes for _ in range(rows)]
        self.uncertain = {}

    def write(self, row, payload):
        if not 0 <= row < self.rows:
            raise WorkloadError("payload_model_row_out_of_range")
        self.values[row] = payload
        self.uncertain.pop(row, None)

    def uncertain_write(self, row, payload):
        self.uncertain[row] = [self.values[row], payload]
        self.values[row] = payload

    def expected(self, row):
        return self.values[row]

    def allowed(self, row):
        return self.uncertain.get(row, [self.values[row]])

    def snapshot(self):
        return list(self.values)

    def digest(self):
        return hashlib.sha256(json.dumps(self.values, separators=(",", ":")).encode()).hexdigest()


def verify_read(response, model, row):
    if not isinstance(response, dict) or response.get("ok") is not True:
        return False
    return response.get("result") == [[model.expected(row)]]
