"""Strict evidence parser and scorer for PL-RETAIL-OLTP 1.0.0.

The scorer is deliberately fail-closed: every required digest, window,
repetition, terminal and verification result must be present and internally
consistent.  ``summary`` is a convenience view only; it never carries
acceptance authority.  Only ``score`` may return exit code 0, and only for a
PASS scorecard.
"""
from __future__ import annotations

import hashlib
import json
import math
import sys
from pathlib import Path
from typing import Any, Iterable

from .contract import Contract, driver_digest, load_contract
from .errors import Status

SUCCESS_TERMINALS = {"read_ok", "committed_ack"}
KNOWN_NON_SUCCESS_TERMINALS = {"known_failure_rolled_back", "session_retired"}
UNKNOWN_TERMINALS = {"service_unknown", "transport_unknown", "cleanup_unconfirmed"}
UNEXPECTED_TERMINALS = {"unexpected_error"}
ALL_TERMINALS = (
    SUCCESS_TERMINALS
    | KNOWN_NON_SUCCESS_TERMINALS
    | UNKNOWN_TERMINALS
    | UNEXPECTED_TERMINALS
)

EXPECTED_HTTP_CALLS = {
    "product_stock": 3,
    "order_detail": 3,
    "customer_orders": 3,
    "stock_report": 3,
    "checkout": 13,
}
READ_CLASSES = ("product_stock", "order_detail", "customer_orders", "stock_report")
BUSINESS_CLASSES = READ_CLASSES + ("checkout",)

# Everything the contract lists under evidence.required_per_round except
# scorecard.json: the scorecard is an output of scoring, so requiring it as an
# input would be circular.  Stream files are only required where a business
# stream exists.
REQUIRED_ROUND_FILES = (
    "config-before.json",
    "config-after.json",
    "initial-data-manifest.json",
    "ledger.json",
    "verify-pre.json",
    "verify-post.json",
    "verify-restart.json",
    "shutdown.json",
    "restart.json",
)
STREAM_ROUND_FILES = ("attempts.jsonl", "http.jsonl", "resource.jsonl", "sessions.jsonl")
PERFORMANCE_LEVELS = ("U1", "U4", "U8", "U16", "H8")

EXIT_CODES = {
    Status.PASS: 0,
    Status.FAIL: 1,
    Status.INVALID: 2,
    Status.INCONCLUSIVE: 3,
    Status.BLOCKED: 4,
    Status.NOT_RUN: 5,
}

STATUS_ORDER = [
    Status.NOT_RUN,
    Status.PASS,
    Status.INCONCLUSIVE,
    Status.FAIL,
    Status.BLOCKED,
    Status.INVALID,
]


class EvidenceError(RuntimeError):
    pass


# -- small helpers ---------------------------------------------------------


def _read_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _read_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as error:
                raise EvidenceError("%s:%d: %s" % (path, line_number, error)) from error
            if not isinstance(value, dict):
                raise EvidenceError("%s:%d: record is not an object" % (path, line_number))
            yield value


def _nearest_rank(values: list[float], quantile: float) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    index = max(1, math.ceil(quantile * len(ordered))) - 1
    return ordered[min(index, len(ordered) - 1)]


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _stats(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"n": 0, "p50": None, "p95": None, "p99": None, "max": None}
    return {
        "n": len(values),
        "p50": round(_nearest_rank(values, 0.50), 6),
        "p95": round(_nearest_rank(values, 0.95), 6),
        "p99": round(_nearest_rank(values, 0.99), 6),
        "max": round(max(values), 6),
    }


def _status_rank(status: Status) -> int:
    try:
        return STATUS_ORDER.index(status)
    except ValueError:
        return len(STATUS_ORDER)


def _merge_status(statuses: Iterable[Status]) -> Status:
    current = Status.NOT_RUN
    for status in statuses:
        if _status_rank(status) > _status_rank(current):
            current = status
    return current


# -- evidence manifest -----------------------------------------------------


def _verify_manifest(root: Path, violations: list[str]) -> dict[str, dict[str, Any]]:
    manifest_path = root / "evidence-manifest.json"
    if not manifest_path.is_file():
        violations.append("evidence-manifest.json is missing")
        return {}
    try:
        document = _read_json(manifest_path)
    except (OSError, json.JSONDecodeError) as error:
        violations.append("evidence-manifest.json is unreadable: %s" % error)
        return {}
    entries = document.get("files")
    if not isinstance(entries, list) or not entries:
        violations.append("evidence-manifest.json has no files")
        return {}
    recorded: dict[str, dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            violations.append("manifest entry is not an object")
            continue
        relative = entry.get("path")
        digest = entry.get("sha256")
        size = entry.get("bytes")
        if not relative or not digest or size is None:
            violations.append("manifest entry lacks path/bytes/sha256: %r" % entry)
            continue
        path = root / relative
        if not path.is_file():
            violations.append("%s: recorded file is missing" % relative)
            continue
        data = path.read_bytes()
        if len(data) != int(size):
            violations.append(
                "%s: size %d != recorded %d" % (relative, len(data), int(size))
            )
            continue
        actual = hashlib.sha256(data).hexdigest()
        if actual != digest:
            # Keep the entry visible so the round can still be identified, but
            # the bundle is already INVALID and the message names the file.
            violations.append("%s: sha256 mismatch" % relative)
            recorded[relative] = entry
            continue
        if entry.get("complete") is False:
            violations.append("%s: manifest marks the file incomplete" % relative)
            continue
        if entry.get("kind") == "jsonl":
            records = sum(1 for line in data.splitlines() if line.strip())
            if records != int(entry.get("records", -1)):
                violations.append(
                    "%s: record count %d != recorded %d"
                    % (relative, records, int(entry.get("records", -1)))
                )
                continue
        recorded[relative] = entry
    return recorded


# -- per-round parsing -----------------------------------------------------


class RoundResult:
    def __init__(self, round_id: str, level: str, concurrency: int):
        self.round_id = round_id
        self.level = level
        self.concurrency = concurrency
        self.status = Status.PASS
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.metrics: dict[str, Any] = {}
        self.goodput: float | None = None
        self.checkout_goodput: float | None = None
        self.successes = 0
        self.attempts = 0
        self.terminals: dict[str, int] = {}
        self.latency: dict[str, dict[str, Any]] = {}
        # Raw per-class successful samples (milliseconds).  The pooled p99 is
        # recomputed across repetitions from these, so they must survive the
        # per-round statistics.
        self.raw_success: dict[str, list[float]] = {name: [] for name in BUSINESS_CLASSES}
        self.slo_violations: list[str] = []
        self.verify: dict[str, Any] = {}
        self.config: dict[str, Any] = {}
        self.recovery: dict[str, Any] = {}
        self.record: dict[str, Any] = {}

    def fail(self, message: str) -> None:
        self.errors.append(message)
        self.status = Status.FAIL

    def invalid(self, message: str) -> None:
        self.errors.append(message)
        self.status = Status.INVALID

    def as_dict(self) -> dict[str, Any]:
        return {
            "round_id": self.round_id,
            "level": self.level,
            "concurrency": self.concurrency,
            "status": self.status.value,
            "business_goodput": self.goodput,
            "checkout_goodput": self.checkout_goodput,
            "attempts": self.attempts,
            "successes": self.successes,
            "terminals": self.terminals,
            "latency": self.latency,
            "slo_violations": self.slo_violations,
            "verify": self.verify,
            "config": self.config,
            "recovery": self.recovery,
            "errors": self.errors,
            "warnings": self.warnings,
        }


def _require_files(
    root: Path, recorded: dict[str, dict[str, Any]], relative: str, result: RoundResult
) -> Path | None:
    path = root / relative
    if relative not in recorded:
        result.invalid("%s: not covered by evidence-manifest" % relative)
        return None
    if not path.is_file():
        result.invalid("%s: file is missing" % relative)
        return None
    return path


def _expected_rounds(contract: Contract, run: dict[str, Any]) -> list[str]:
    if run.get("mode") != "full":
        return [str(item) for item in run.get("rounds", [])]
    rounds: list[str] = []
    repetitions = int(run.get("repetitions", 0))
    levels = contract.document["matrix"]["levels"]
    order = contract.document["matrix"].get("order") or list(levels)
    for repetition in range(1, repetitions + 1):
        for name in order:
            if name not in PERFORMANCE_LEVELS:
                continue
            rounds.append("r%02d-%s" % (repetition, name))
        rounds.append("r%02d-L1" % repetition)
        rounds.append("r%02d-H1" % repetition)
        rounds.append("r%02d-IDLE" % repetition)
    return rounds


def _load_round(
    root: Path,
    recorded: dict[str, dict[str, Any]],
    round_id: str,
    contract: Contract,
    run: dict[str, Any],
) -> RoundResult:
    level = round_id.split("-", 1)[1] if "-" in round_id else "UNKNOWN"
    concurrency = 0
    levels = contract.document["matrix"]["levels"]
    if level in levels:
        concurrency = int(levels[level]["concurrency"])
    result = RoundResult(round_id, level, concurrency)
    relative = "rounds/%s/round.json" % round_id
    path = _require_files(root, recorded, relative, result)
    if path is None:
        return result
    try:
        record = _read_json(path)
    except (OSError, json.JSONDecodeError) as error:
        result.invalid("round.json is unreadable: %s" % error)
        return result
    result.record = record
    result.status = Status.PASS

    directory = "rounds/%s" % round_id
    for name in REQUIRED_ROUND_FILES:
        _require_files(root, recorded, "%s/%s" % (directory, name), result)
    if level in PERFORMANCE_LEVELS:
        for name in STREAM_ROUND_FILES:
            _require_files(root, recorded, "%s/%s" % (directory, name), result)

    status = record.get("status")
    if status == "blocked":
        result.status = Status.BLOCKED
        result.errors.append("round was blocked: %s" % record.get("abort_reason"))
        return result
    if status == "invalid":
        result.invalid(
            "round is invalid: status=%r reason=%r"
            % (status, record.get("abort_reason"))
        )
        return result
    if status != "completed":
        result.fail(
            "round did not complete: status=%r reason=%r"
            % (status, record.get("abort_reason"))
        )
        return result

    config = record.get("config") or {}
    result.config = {
        "before": _audit_summary(config.get("before")),
        "after": _audit_summary(config.get("after")),
    }
    for phase in ("before", "after"):
        audit = config.get(phase)
        if not isinstance(audit, dict):
            result.invalid("config.%s is missing" % phase)
            continue
        if audit.get("violations"):
            result.invalid("config.%s violations: %s" % (phase, audit["violations"]))
        if audit.get("gaps"):
            result.invalid("config.%s gaps: %s" % (phase, audit["gaps"]))
        if audit.get("ok") is not True:
            result.invalid("config.%s is not ok" % phase)

    verify = record.get("verify") or {}
    result.verify = {
        name: _verify_summary(verify.get(name)) for name in ("pre", "post", "restart")
    }
    if not verify.get("pre"):
        result.invalid("verify.pre is missing")
    elif not verify["pre"].get("ok"):
        result.fail("pre-stream verification failed")
    if result.level in PERFORMANCE_LEVELS:
        if not verify.get("post"):
            result.invalid("verify.post is missing")
        elif not verify["post"].get("ok"):
            result.fail("post-stream verification failed")
        if not verify.get("restart"):
            if run.get("mode") == "full":
                result.invalid("verify.restart is missing for a full performance round")
        elif not verify["restart"].get("ok"):
            result.fail("post-restart verification failed")
    if result.level == "H1":
        if not verify.get("restart"):
            result.invalid("verify.restart is missing for H1")
        elif not verify["restart"].get("ok"):
            result.fail("H1 post-restart verification failed")
    result.verify["violations"] = _verify_violations(verify)

    if result.level in PERFORMANCE_LEVELS:
        _score_performance_round(root, recorded, contract, result, record)
    elif result.level == "L1":
        _score_load_round(result, record, contract)
    elif result.level == "H1":
        _score_recovery_round(result, record)
    elif result.level == "IDLE":
        _score_idle_round(root, recorded, contract, result, record)
    else:
        result.invalid("unknown round level %r" % result.level)
    return result


def _audit_summary(audit: Any) -> dict[str, Any]:
    if not isinstance(audit, dict):
        return {"present": False}
    return {
        "present": True,
        "ok": audit.get("ok"),
        "violations": audit.get("violations", []),
        "gaps": audit.get("gaps", []),
        "checks": audit.get("checks"),
        "schema": audit.get("schema", {}).get("ok") if isinstance(audit.get("schema"), dict) else None,
        "oracle": audit.get("oracle", {}).get("ok") if isinstance(audit.get("oracle"), dict) else None,
    }


def _verify_summary(verify: Any) -> dict[str, Any]:
    if not isinstance(verify, dict):
        return {"present": False}
    invariants = verify.get("invariants") or {}
    ledger = verify.get("ledger") or {}
    return {
        "present": True,
        "ok": verify.get("ok"),
        "phase": verify.get("phase"),
        "tables": verify.get("tables", {}),
        "invariant_violations": invariants.get("violations", []),
        "ledger": {
            "confirmed": ledger.get("confirmed"),
            "seen": ledger.get("seen"),
            "missing": ledger.get("missing", []),
            "unexpected": ledger.get("unexpected", []),
            "rejected_absent": ledger.get("rejected_absent"),
        },
        "query_errors": verify.get("query_errors", []),
    }


def _verify_violations(verify: dict[str, Any]) -> list[str]:
    violations: list[str] = []
    for name in ("pre", "post", "restart"):
        document = verify.get(name)
        if not isinstance(document, dict):
            continue
        if document.get("present") is False:
            continue
        if not document.get("ok"):
            violations.append("%s: ok=false" % name)
        if document.get("query_errors"):
            violations.append("%s: query_errors=%r" % (name, document["query_errors"]))
        invariants = document.get("invariants") or {}
        if invariants.get("violations"):
            violations.append(
                "%s: invariant violations=%r" % (name, invariants["violations"][:8])
            )
        ledger = document.get("ledger") or {}
        if ledger.get("missing"):
            violations.append("%s: missing ledger keys=%r" % (name, ledger["missing"][:8]))
        if ledger.get("unexpected"):
            violations.append(
                "%s: unexpected ledger keys=%r" % (name, ledger["unexpected"][:8])
            )
        if ledger.get("rejected_absent") is not True:
            violations.append("%s: rejected keys are present in the data" % name)
    return violations


def _score_performance_round(
    root: Path,
    recorded: dict[str, dict[str, Any]],
    contract: Contract,
    result: RoundResult,
    record: dict[str, Any],
) -> None:
    relative = "rounds/%s/attempts.jsonl" % result.round_id
    path = _require_files(root, recorded, relative, result)
    if path is None:
        return
    for extra in ("http.jsonl", "resource.jsonl", "sessions.jsonl"):
        _require_files(root, recorded, "rounds/%s/%s" % (result.round_id, extra), result)

    timeline = record.get("timeline") or {}
    t0 = timeline.get("t0_ns")
    t1 = timeline.get("t1_ns")
    window = float(contract.document["matrix"]["window_seconds"])
    if not isinstance(t0, int) or not isinstance(t1, int) or t1 <= t0:
        result.invalid("timeline t0/t1 is missing or not monotonic")
        return
    duration = (t1 - t0) / 1e9
    if abs(duration - window) > 1.0:
        result.invalid(
            "measurement window is %.3fs, contract requires %.3fs" % (duration, window)
        )
        return

    attempts: list[dict[str, Any]] = []
    try:
        for attempt in _read_jsonl(path):
            attempts.append(attempt)
    except (OSError, EvidenceError) as error:
        result.invalid("attempts.jsonl is unreadable: %s" % error)
        return

    counts = record.get("counts") or {}
    observed_import = counts.get("rows_after_import")
    expected_import = counts.get("rows_expected_after_import")
    if isinstance(observed_import, int) and isinstance(expected_import, int):
        if observed_import != expected_import:
            result.invalid(
                "import produced %d rows, contract fixes %d"
                % (observed_import, expected_import)
            )

    warmups_expected = (
        int(contract.document["matrix"]["warmups_per_worker"]) * result.concurrency
    )
    warmups = 0
    measure = 0
    successes = 0
    http_ids: set[str] = set()
    seen_attempts: set[str] = set()
    by_class: dict[str, dict[str, list[float]]] = {
        name: {"success": [], "failure": [], "all": []} for name in BUSINESS_CLASSES
    }
    terminals: dict[str, int] = {}
    unexpected = 0
    unknown = 0
    stopped = False
    for attempt in attempts:
        attempt_id = attempt.get("attempt_id")
        if not attempt_id:
            result.invalid("attempt without attempt_id")
            continue
        if attempt_id in seen_attempts:
            result.invalid("duplicate attempt_id %s" % attempt_id)
            continue
        seen_attempts.add(attempt_id)
        business = attempt.get("business")
        terminal = attempt.get("terminal")
        window_name = attempt.get("window")
        if business not in EXPECTED_HTTP_CALLS:
            result.invalid("%s: unknown business %r" % (attempt_id, business))
            continue
        if terminal not in ALL_TERMINALS:
            result.invalid("%s: unknown terminal %r" % (attempt_id, terminal))
            continue
        if window_name not in {"warmup", "measure"}:
            result.invalid("%s: unknown window %r" % (attempt_id, window_name))
            continue
        if attempt.get("success") != (terminal in SUCCESS_TERMINALS):
            result.invalid("%s: success flag disagrees with terminal" % attempt_id)
        if int(attempt.get("http_calls") or 0) != EXPECTED_HTTP_CALLS[business]:
            result.invalid(
                "%s: %s used %r HTTP calls, contract fixes %d"
                % (attempt_id, business, attempt.get("http_calls"), EXPECTED_HTTP_CALLS[business])
            )
        started_ns = attempt.get("started_ns")
        finished_ns = attempt.get("finished_ns")
        if not isinstance(started_ns, int) or not isinstance(finished_ns, int):
            result.invalid("%s: start/finish timestamps are missing" % attempt_id)
            continue
        if finished_ns < started_ns:
            result.invalid("%s: finished before started" % attempt_id)
            continue
        phases = attempt.get("phases") or []
        if not phases:
            result.invalid("%s: no HTTP phase evidence" % attempt_id)
        for phase in phases:
            if isinstance(phase, dict) and phase.get("request_id"):
                http_ids.add(str(phase["request_id"]))
        if terminal in SUCCESS_TERMINALS:
            last = phases[-1] if phases else {}
            if not isinstance(last, dict) or last.get("label") != "commit":
                result.invalid("%s: successful attempt does not end with COMMIT" % attempt_id)
            elif last.get("status") != 200 or last.get("code"):
                result.invalid("%s: COMMIT did not succeed" % attempt_id)
        if attempt.get("stop_fixture"):
            stopped = True
        terminals[terminal] = terminals.get(terminal, 0) + 1
        if terminal in UNKNOWN_TERMINALS or terminal in UNEXPECTED_TERMINALS:
            if terminal in UNKNOWN_TERMINALS:
                unknown += 1
            else:
                unexpected += 1
        if window_name == "warmup":
            warmups += 1
            continue
        measure += 1
        if not (t0 <= started_ns < t1):
            result.invalid(
                "%s: measure attempt started outside the advertised window" % attempt_id
            )
            continue
        latency = (finished_ns - started_ns) / 1e6
        by_class[business]["all"].append(latency)
        if terminal in SUCCESS_TERMINALS:
            by_class[business]["success"].append(latency)
            successes += 1
        else:
            by_class[business]["failure"].append(latency)

    if "http.jsonl" in recorded:
        try:
            recorded_ids = {
                str(item.get("request_id"))
                for item in _read_jsonl(root / ("rounds/%s/http.jsonl" % result.round_id))
                if item.get("request_id")
            }
        except (OSError, EvidenceError) as error:
            result.invalid("http.jsonl is unreadable: %s" % error)
            recorded_ids = set()
        missing = http_ids - recorded_ids
        if missing:
            result.invalid(
                "http.jsonl misses %d request ids (first: %s)"
                % (len(missing), sorted(missing)[0])
            )
    if warmups != warmups_expected:
        result.invalid(
            "warmup count %d != %d (one worker did not complete its warmups)"
            % (warmups, warmups_expected)
        )
    if measure <= 0:
        result.invalid("no measured attempts")
    if successes <= 0:
        result.invalid("empty success trace")
    if stopped:
        result.fail("a worker stopped the fixture during the round")
    if unknown:
        result.fail("%d attempt(s) ended in an unknown state" % unknown)
    if unexpected:
        result.fail("%d unexpected business error(s)" % unexpected)
    for business in BUSINESS_CLASSES:
        if not by_class[business]["success"]:
            result.fail("business class %s has no successful sample" % business)

    result.attempts = measure
    result.successes = successes
    result.terminals = terminals
    result.latency = {
        name: {
            "success": _stats(by_class[name]["success"]),
            "failure": _stats(by_class[name]["failure"]),
            "all": _stats(by_class[name]["all"]),
        }
        for name in BUSINESS_CLASSES
    }
    result.raw_success = {
        name: list(by_class[name]["success"]) for name in BUSINESS_CLASSES
    }

    goodput_success = 0
    checkout_success = 0
    for attempt in attempts:
        if attempt.get("window") != "measure":
            continue
        started_ns = attempt.get("started_ns")
        finished_ns = attempt.get("finished_ns")
        if not isinstance(started_ns, int) or not isinstance(finished_ns, int):
            continue
        if not (t0 <= started_ns < t1):
            continue
        if attempt.get("terminal") not in SUCCESS_TERMINALS:
            continue
        if finished_ns > t1:
            continue
        goodput_success += 1
        if attempt.get("business") == "checkout":
            checkout_success += 1
    if not any(result.latency[name]["success"]["n"] for name in BUSINESS_CLASSES):
        result.invalid(
            "the timed window contains no successful attempt: the success trace is empty"
        )
    result.goodput = round(goodput_success / duration, 6)
    result.checkout_goodput = round(checkout_success / duration, 6)

    _check_round_slo(contract, result)


def _check_round_slo(contract: Contract, result: RoundResult) -> None:
    slo = contract.document["slo"]
    deadline = float(slo["business_deadline_ms"])
    for business in READ_CLASSES:
        success = result.latency[business]["success"]
        if success["n"] == 0:
            continue
        if success["p95"] is not None and success["p95"] > float(slo["read_p95_ms"]):
            result.slo_violations.append(
                "%s p95 %.3fms > %.3fms"
                % (business, success["p95"], float(slo["read_p95_ms"]))
            )
        if success["p99"] is not None and success["p99"] > float(slo["read_p99_ms"]):
            result.slo_violations.append(
                "%s p99 %.3fms > %.3fms"
                % (business, success["p99"], float(slo["read_p99_ms"]))
            )
    checkout = result.latency["checkout"]["success"]
    if checkout["n"]:
        if checkout["p95"] is not None and checkout["p95"] > float(slo["checkout_p95_ms"]):
            result.slo_violations.append(
                "checkout p95 %.3fms > %.3fms"
                % (checkout["p95"], float(slo["checkout_p95_ms"]))
            )
        if checkout["p99"] is not None and checkout["p99"] > float(slo["checkout_p99_ms"]):
            result.slo_violations.append(
                "checkout p99 %.3fms > %.3fms"
                % (checkout["p99"], float(slo["checkout_p99_ms"]))
            )
    for business in BUSINESS_CLASSES:
        success = result.latency[business]["success"]
        if success["max"] is not None and success["max"] > deadline:
            result.slo_violations.append(
                "%s max %.3fms > business deadline %.3fms"
                % (business, success["max"], deadline)
            )
    if result.slo_violations:
        result.fail("; ".join(result.slo_violations))


def _score_load_round(result: RoundResult, record: dict[str, Any], contract: Contract) -> None:
    load = record.get("load")
    if not isinstance(load, dict):
        result.invalid("L1 has no load report")
        return
    if load.get("ok") is not True:
        result.fail("L1 load did not complete")
    expected = contract.document["dataset"].get("tables") or contract.document["dataset"].get("rows") or {}
    actual = load.get("rows") or {}
    for table, count in expected.items():
        if int(actual.get(table, -1)) != int(count):
            result.fail(
                "L1 loaded %r rows into %s, contract requires %r"
                % (actual.get(table), table, count)
            )
    result.metrics = {"rows": actual, "elapsed_ms": load.get("elapsed_ms")}


def _score_recovery_round(result: RoundResult, record: dict[str, Any]) -> None:
    shutdown = record.get("shutdown") or {}
    restart = record.get("restart") or {}
    result.recovery = {
        "shutdown": {
            "status": shutdown.get("status"),
            "ok": shutdown.get("ok"),
            "clean_proof": shutdown.get("clean_proof"),
            "duration_ms": shutdown.get("duration_ms"),
        },
        "restart": {
            "status": restart.get("status"),
            "ok": restart.get("ok"),
            "duration_ms": restart.get("duration_ms"),
            "ready": restart.get("ready"),
        },
    }
    if shutdown.get("configured") is False:
        result.status = Status.BLOCKED
        result.errors.append("lifecycle is not configured; recovery cannot be established")
        return
    clean = (shutdown.get("clean_proof") or {}).get("matched") is True
    if shutdown.get("ok") is not True or not clean:
        result.fail("clean shutdown proof is missing or does not match")
    if shutdown.get("duration_ms") is not None and shutdown["duration_ms"] > 30_000:
        result.fail("clean shutdown exceeded 30 seconds")
    if restart.get("ok") is not True:
        result.fail("restart did not become ready")
    if restart.get("duration_ms") is not None and restart["duration_ms"] > 30_000:
        result.fail("restart exceeded 30 seconds")


def _score_idle_round(
    root: Path,
    recorded: dict[str, dict[str, Any]],
    contract: Contract,
    result: RoundResult,
    record: dict[str, Any],
) -> None:
    _require_files(root, recorded, "rounds/%s/round.json" % result.round_id, result)
    idle = record.get("idle") or {}
    cycles = idle.get("cycles")
    if isinstance(cycles, list) and cycles:
        required = int(idle.get("cycles_required") or 10)
        if len(cycles) != required or idle.get("ok") is not True:
            result.invalid(
                "idle completed %d of %d cycles" % (len(cycles), required)
            )
        for cycle in cycles:
            if not isinstance(cycle, dict):
                result.invalid("idle cycle is not an object")
                continue
            if float(cycle.get("quiet_seconds") or 0) < 120.0:
                result.invalid(
                    "idle cycle %r had %.3fs of silence, contract requires 120s"
                    % (cycle.get("cycle"), float(cycle.get("quiet_seconds") or 0))
                )
            unload = cycle.get("unload") or {}
            if unload.get("status") != "unloaded":
                result.invalid(
                    "idle cycle %r cannot prove unload: %r"
                    % (cycle.get("cycle"), unload)
                )
            trigger = cycle.get("trigger") or {}
            if trigger.get("success") is not True:
                result.fail("idle cycle %r trigger did not succeed" % cycle.get("cycle"))
            if float(trigger.get("latency_ms") or 1e9) > float(
                contract.document["slo"]["business_deadline_ms"]
            ):
                result.fail(
                    "idle cycle %r exceeded the business deadline" % cycle.get("cycle")
                )
        seconds = float(cycles[-1].get("quiet_seconds") or 0) if isinstance(cycles[-1], dict) else 0.0
        unload = cycles[-1].get("unload") or {} if isinstance(cycles[-1], dict) else {}
        trigger = cycles[-1].get("trigger") or {} if isinstance(cycles[-1], dict) else {}
    else:
        seconds = float(idle.get("seconds") or 0)
        if seconds < 120.0:
            result.invalid("idle silence was %.3fs, contract requires 120s" % seconds)
        unload = idle.get("unload") or {}
        if unload.get("status") not in {"ok", "unloaded"}:
            result.invalid("unload cannot be proven: %r" % unload)
        trigger = record.get("trigger") or {}
        if trigger.get("success") is not True:
            result.fail("first business after idle did not succeed")
        if float(trigger.get("latency_ms") or 1e9) > float(
            contract.document["slo"]["business_deadline_ms"]
        ):
            result.fail("first business after idle exceeded the business deadline")
    post = (record.get("verify") or {}).get("post")
    if not post:
        result.invalid("idle round has no post-stream verification")
    elif not post.get("ok"):
        result.fail("idle round post-stream verification failed")
    result.metrics = {"idle_seconds": seconds, "unload": unload, "trigger": trigger}


# -- level aggregation -----------------------------------------------------


def _aggregate_level(
    level: str,
    rounds: list[RoundResult],
    contract: Contract,
) -> dict[str, Any]:
    statuses = [item.status for item in rounds]
    status = _merge_status(statuses)
    goodputs = [item.goodput for item in rounds if item.goodput is not None]
    checkouts = [item.checkout_goodput for item in rounds if item.checkout_goodput is not None]
    pooled: dict[str, dict[str, Any]] = {}
    statistics = contract.document["statistics"]
    minimum = int(
        statistics.get("min_pooled_success_samples")
        or statistics.get("sample_threshold_per_class")
        or 1000
    )
    sample_short = False
    for business in BUSINESS_CLASSES:
        values: list[float] = []
        for item in rounds:
            values.extend(item.raw_success.get(business, []))
        if len(values) < minimum:
            sample_short = True
        pooled[business] = {
            "n": len(values),
            "p50": round(_nearest_rank(values, 0.50), 6) if values else None,
            "p95": round(_nearest_rank(values, 0.95), 6) if values else None,
            "p99": round(_nearest_rank(values, 0.99), 6) if values else None,
            "max": round(max(values), 6) if values else None,
        }
    if sample_short:
        if status == Status.PASS:
            status = Status.INCONCLUSIVE
    slo = contract.document["slo"]
    for business in READ_CLASSES:
        value = pooled[business]["p99"]
        if value is not None and value > float(slo["read_p99_ms"]):
            status = Status.FAIL
    value = pooled["checkout"]["p99"]
    if value is not None and value > float(slo["checkout_p99_ms"]):
        status = Status.FAIL
    return {
        "level": level,
        "concurrency": rounds[0].concurrency if rounds else 0,
        "status": status.value,
        "repetitions": [item.as_dict() for item in rounds],
        "median_business_goodput": round(_median(goodputs), 6) if goodputs else None,
        "median_checkout_goodput": round(_median(checkouts), 6) if checkouts else None,
        "pooled_success": pooled,
        "pooled_minimum": minimum,
        "pooled_sample_short": sample_short,
        "slo_violations": [
            violation for item in rounds for violation in item.slo_violations
        ],
    }


# -- main scoring ----------------------------------------------------------


def score_run(
    root: Path,
    contract: Contract,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(root)
    violations: list[str] = []
    recorded = _verify_manifest(root, violations)
    run_path = root / "run.json"
    if not run_path.is_file():
        violations.append("run.json is missing")
        run: dict[str, Any] = {}
    else:
        try:
            run = _read_json(run_path)
        except (OSError, json.JSONDecodeError) as error:
            violations.append("run.json is unreadable: %s" % error)
            run = {}
    if run.get("driver_sha256") != driver_digest():
        violations.append("driver digest does not match the driver that runs the scorer")
    contract_block = run.get("contract") or {}
    if contract_block.get("sha256") != contract.sha256:
        violations.append("contract digest recorded by the run does not match benchmark.json")
    if contract.document.get("certified_driver_sha256") is not None:
        violations.append("certified_driver_sha256 must stay null until certification")
    mode = str(run.get("mode") or "unknown")
    if mode not in {"full", "smoke"}:
        violations.append("run mode %r is not full or smoke" % mode)
    if mode == "smoke" and run.get("certifiable") is True:
        violations.append("smoke runs cannot be certifiable")

    listed_files = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file()
        and path.name not in {"evidence-manifest.json", "SHA256SUMS"}
    }
    for stray in sorted(listed_files - set(recorded)):
        violations.append("%s: file exists but the manifest does not cover it" % stray)

    listed = {entry for entry in run.get("rounds") or []}
    on_disk = {
        path.parent.name
        for path in root.glob("rounds/*/round.json")
    }
    for stray in sorted(on_disk - listed):
        violations.append("rounds/%s: evidence exists but run.json does not list it" % stray)
    for missing in sorted(listed - on_disk):
        violations.append("rounds/%s: run.json lists it but no evidence exists" % missing)

    expected = _expected_rounds(contract, run)
    rounds: list[RoundResult] = []
    for round_id in expected:
        rounds.append(_load_round(root, recorded, round_id, contract, run))

    level_order = list(contract.document["matrix"]["levels"])
    levels: dict[str, Any] = {}
    for name in level_order:
        subset = [item for item in rounds if item.level == name]
        if subset:
            levels[name] = _aggregate_level(name, subset, contract)
    qualified = [
        (name, document["median_business_goodput"])
        for name, document in levels.items()
        if name in {"U1", "U4", "U8", "U16"}
        and document["status"] == Status.PASS.value
        and document["median_business_goodput"] is not None
    ]
    qualified_level = max(qualified, key=lambda item: item[1]) if qualified else None

    performance_rounds = [item for item in rounds if item.level in level_order]
    correctness = [
        item for item in rounds
        if item.level in PERFORMANCE_LEVELS + ("L1", "H1")
    ]
    evidence_violations = list(violations)
    for item in rounds:
        if item.verify.get("violations"):
            evidence_violations.extend(
                "%s: %s" % (item.round_id, value) for value in item.verify["violations"]
            )
    evidence_status = Status.PASS
    if evidence_violations:
        evidence_status = Status.INVALID

    performance_status = Status.NOT_RUN
    if performance_rounds:
        performance_status = _merge_status(item.status for item in performance_rounds)

    correctness_status = Status.NOT_RUN
    correctness_errors: list[str] = []
    if correctness:
        statuses = []
        for item in correctness:
            statuses.append(item.status)
            for document in item.verify.values():
                if isinstance(document, dict) and document.get("ok") is False:
                    correctness_errors.append(item.round_id)
        correctness_status = _merge_status(statuses)
        if correctness_errors:
            correctness_status = Status.FAIL

    recovery_status = Status.NOT_RUN
    recovery_errors: list[str] = []
    recovery_rounds = [item for item in rounds if item.level == "H1"] + [
        item for item in rounds if item.level in level_order
    ]
    if recovery_rounds:
        statuses = []
        for item in recovery_rounds:
            if item.level in level_order:
                shutdown = item.record.get("shutdown") or {}
                restart = item.record.get("restart") or {}
                if shutdown.get("configured") is False or restart.get("configured") is False:
                    if run.get("mode") == "full":
                        recovery_errors.append(item.round_id)
                    continue
                if shutdown.get("ok") is not True:
                    recovery_errors.append("%s shutdown" % item.round_id)
                if restart.get("ok") is not True:
                    recovery_errors.append("%s restart" % item.round_id)
            statuses.append(item.status)
        recovery_status = _merge_status(statuses)
        if recovery_errors:
            recovery_status = Status.FAIL

    load_rounds = [item for item in rounds if item.level == "L1"]
    load_status = _merge_status(item.status for item in load_rounds) if load_rounds else Status.NOT_RUN
    idle_rounds = [item for item in rounds if item.level == "IDLE"]
    idle_status = _merge_status(item.status for item in idle_rounds) if idle_rounds else Status.NOT_RUN

    cleanup_ab_status = Status.NOT_RUN
    cleanup_ab_violations: list[str] = []
    cleanup_ab_rounds: list[dict[str, Any]] = []
    if mode == "full":
        required_ab = int(contract.document["matrix"]["repetitions"])
        counted = 0
        for path in sorted(root.glob("rounds/*/cleanup-ab.json")):
            relative = path.relative_to(root).as_posix()
            if relative not in recorded:
                cleanup_ab_violations.append("%s: not covered by evidence-manifest" % relative)
                continue
            try:
                document = _read_json(path)
            except (OSError, json.JSONDecodeError) as error:
                cleanup_ab_violations.append("%s: unreadable: %s" % (relative, error))
                continue
            counted += 1
            comparison = document.get("comparison") or {}
            summary = {
                "round_id": document.get("round_id"),
                "verdict": comparison.get("verdict"),
                "violations": list(comparison.get("violations") or []),
                "scope": comparison.get("scope"),
            }
            cleanup_ab_rounds.append(summary)
            for violation in summary["violations"]:
                cleanup_ab_violations.append("%s: %s" % (summary["round_id"], violation))
            if summary["verdict"] != "PASS":
                cleanup_ab_violations.append(
                    "%s: close-retirement A/B verdict %r" % (summary["round_id"], summary["verdict"])
                )
        if counted < required_ab:
            cleanup_ab_violations.append(
                "close-retirement A/B ran %d of %d required repetitions" % (counted, required_ab)
            )
        cleanup_ab_status = Status.PASS if not cleanup_ab_violations else Status.FAIL

    dimension_statuses = [
        performance_status,
        correctness_status,
        recovery_status,
        load_status,
        idle_status,
        evidence_status,
        cleanup_ab_status,
    ]
    run_warnings: list[str] = []
    if mode != "full":
        run_warnings.append(
            "mode is %r: only a full three-round matrix can be certified" % mode
        )
    if not contract.document.get("certified_driver_sha256"):
        run_warnings.append(
            "certified_driver_sha256 is null: the driver is not certified, so this "
            "scorecard records measurements only"
        )
    if contract.document.get("driver", {}).get("legacy_reference_present") is False:
        run_warnings.append(
            "reference_driver.py is absent from this package; the historical digest "
            "is recorded but the bytes could not be re-verified"
        )

    overall = _merge_status(dimension_statuses)
    if mode == "smoke" and overall == Status.PASS:
        overall = Status.INCONCLUSIVE
    if mode == "full":
        for required, value in (
            ("recovery", recovery_status),
            ("load", load_status),
            ("idle", idle_status),
        ):
            if value in {Status.NOT_RUN, Status.BLOCKED}:
                if value == Status.BLOCKED:
                    overall = Status.BLOCKED
                elif overall in {Status.PASS, Status.INCONCLUSIVE}:
                    overall = Status.INVALID

    scorecard = {
        "schema": "pl-retail-oltp-scorecard/1.0.0",
        "contract": {
            "id": contract.document.get("contract"),
            "version": contract.version,
            "profile": contract.profile,
            "sha256": contract.sha256,
        },
        "run": {
            "run_id": run.get("run_id"),
            "mode": mode,
            "scale": run.get("scale"),
            "repetitions": run.get("repetitions"),
            "window_seconds": run.get("window_seconds"),
            "driver_sha256": run.get("driver_sha256"),
            "certified_driver_sha256": contract.document.get("certified_driver_sha256"),
            # A bundle may pass every dimension and still not be certifiable:
            # the contract keeps certified_driver_sha256 null until the four
            # P0 deliverables are accepted, so no driver is certified yet.
            "driver_certified": bool(
                contract.document.get("certified_driver_sha256")
                and contract.document.get("certified_driver_sha256") == run.get("driver_sha256")
            ),
            "certifiable": bool(
                mode == "full"
                and overall == Status.PASS
                and contract.document.get("certified_driver_sha256")
                and contract.document.get("certified_driver_sha256") == run.get("driver_sha256")
            ),
        },
        "status": overall.value,
        "dimensions": {
            "performance": {
                "status": performance_status.value,
                "qualified_uniform_level": qualified_level[0] if qualified_level else None,
                "qualified_uniform_business_goodput": qualified_level[1] if qualified_level else None,
            },
            "correctness": {
                "status": correctness_status.value,
                "failed_rounds": correctness_errors,
            },
            "recovery": {
                "status": recovery_status.value,
                "failures": recovery_errors,
            },
            "load": {"status": load_status.value},
            "idle": {"status": idle_status.value},
            "cleanup_ab": {
                "status": cleanup_ab_status.value,
                "rounds": cleanup_ab_rounds,
                "violations": cleanup_ab_violations,
            },
            "evidence": {
                "status": evidence_status.value,
                "violations": evidence_violations,
                "files_checked": len(recorded),
            },
        },
        "levels": levels,
        "load_rounds": [item.as_dict() for item in load_rounds],
        "idle_rounds": [item.as_dict() for item in idle_rounds],
        "recovery_rounds": [item.as_dict() for item in recovery_rounds if item.level == "H1"],
        "failures": [
            "evidence: %s" % violation for violation in violations
        ]
        + [
            "%s: %s" % (item.round_id, error)
            for item in rounds
            for error in item.errors
        ],
        "warnings": run_warnings
        + [
            "%s: %s" % (item.round_id, warning)
            for item in rounds
            for warning in item.warnings
        ],
    }
    if details is not None:
        details["rounds"] = {item.round_id: item.as_dict() for item in rounds}
        details["recorded_files"] = sorted(recorded)
    return scorecard


def emit_scorecards(
    root: Path, scorecard: dict[str, Any], details: dict[str, Any]
) -> dict[str, Any]:
    """Write the per-round and run-level scorecard.json into the bundle.

    The contract lists ``scorecard.json`` under the per-round evidence.  It is
    an output of scoring, so the write happens here and the manifest is
    finalized afterwards; re-scoring the same bundle is idempotent because the
    documents are deterministic.
    """
    from .evidence import EvidenceWriter

    writer = EvidenceWriter(root)
    written: list[str] = []
    rounds = details.get("rounds") or {}
    for round_id in sorted(rounds):
        document = rounds[round_id]
        writer.write_json("rounds/%s/scorecard.json" % round_id, document)
        written.append("rounds/%s/scorecard.json" % round_id)
    writer.write_json("scorecard.json", scorecard)
    written.append("scorecard.json")
    manifest = writer.finalize()
    return {"written": written, "manifest": manifest}


def print_summary(scorecard: dict[str, Any]) -> None:
    print("PL-RETAIL-OLTP scorecard")
    print("contract: %(version)s profile: %(profile)s" % scorecard["contract"])
    print("status: %s" % scorecard["status"])
    print("certifiable: %s" % scorecard["run"]["certifiable"])
    for name, document in scorecard["dimensions"].items():
        print("  %-12s %s" % (name, document["status"]))
    for level, document in scorecard["levels"].items():
        median = document["median_business_goodput"]
        checkout = document["median_checkout_goodput"]
        print(
            "  %-4s status=%-12s median=%s checkout=%s"
            % (level, document["status"], median, checkout)
        )
    if scorecard["failures"]:
        print("failures:")
        for failure in scorecard["failures"][:20]:
            print("  - %s" % failure)


def _main(argv: list[str]) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Score a PL-RETAIL-OLTP evidence bundle")
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--contract", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--summary", action="store_true", help="print instead of only writing JSON")
    args = parser.parse_args(argv)
    contract = load_contract(args.contract)
    scorecard = score_run(Path(args.evidence), contract)
    if args.output:
        Path(args.output).write_text(
            json.dumps(scorecard, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    if args.summary or not args.output:
        print(json.dumps(scorecard, indent=2, sort_keys=True))
    return EXIT_CODES[Status(scorecard["status"])]


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main(sys.argv[1:]))
