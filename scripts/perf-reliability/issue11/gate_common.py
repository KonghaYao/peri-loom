"""Issue11 gate_common；按证据职责隔离。"""
import json
import math
from pathlib import Path
from issue11 import evidence_io
PASS = "PASS"
PHASE_PASS = "PHASE_PASS"
INCOMPLETE = "INCOMPLETE"
NOT_RUN = "NOT_RUN"
FAIL = "FAIL"

STATUSES = (PASS, PHASE_PASS, INCOMPLETE, NOT_RUN, FAIL)
REQUIRED_RUN = evidence_io.REQUIRED_RUN_ARTIFACTS
PERIODIC_KINDS = {
    "audit_head",
    "journal_compact",
    "receipt_roll",
    "checkpoint",
    "index_compact",
}
METRIC_COUNTERS = {
    "work.wal_workload.append_calls",
    "work.metadata.root_publish_attempts",
    "object_store.put.total",
    "object_store.wal_durability.total",
}
PROCESS_NAMES = {"client", "gateway", "worker", "authority", "object_store"}
OUTCOMES = {"success", "error", "rejected", "outcome_unknown"}


class GateError(ValueError):
    pass


def _read_json(path):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as failure:
        raise GateError("json_unreadable: " + str(path) + ": "
                        + type(failure).__name__) from None


def _read_jsonl(path):
    records = []
    try:
        lines = Path(path).read_text().splitlines()
    except OSError as failure:
        raise GateError("jsonl_unreadable: " + str(path) + ": "
                        + type(failure).__name__) from None
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            raise GateError("jsonl_invalid: " + str(path) + ":"
                            + str(line_number)) from None
        if not isinstance(record, dict):
            raise GateError("jsonl_not_object: " + str(path) + ":"
                            + str(line_number))
        records.append(record)
    return records


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) \
        and math.isfinite(value)


def _percentile(values, fraction):
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[min(index, len(ordered) - 1)]


def _section(status, reasons=None, **extra):
    document = {"status": status, "reasons": list(reasons or [])}
    document.update(extra)
    return document
