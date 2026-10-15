"""Read-only, fail-closed verifier for v1 stress runner evidence.

Usage: python3 evidence.py /path/to/run_<16-hex-digits>
Only an allowlisted aggregate is written to stdout; diagnostics never echo input.
This verifies runner accounting, not durable correctness or a commercial SLA.
"""

import argparse
import contextlib
import hashlib
import json
import math
import os
import re
import stat
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path


RUN_ID = re.compile(r"[0-9a-f]{16}\Z")
STATUSES = {"COMPLETE", "FAIL", "UNRESOLVED", "SKIP", "NOT MEASURED"}
NOT_MEASURED = "NOT MEASURED"
REQUIRED_STAGES = {f"P{index}" for index in range(7)}
GIT_HASH = re.compile(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})\Z")
RFC3339_NANO = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})\Z")
MAX_RUN_BYTES = 16 * 1024 * 1024
MAX_OPERATIONS_BYTES = 64 * 1024 * 1024
MAX_LINE_BYTES = 8 * 1024 * 1024


class InvalidEvidence(ValueError):
    pass


def require(condition):
    if not condition:
        raise InvalidEvidence("evidence validation failed")


def integer(value, minimum=0):
    require(type(value) is int and value >= minimum)
    return value


def number(value):
    require(type(value) in (int, float) and math.isfinite(value))
    return value


def mapping(value):
    require(type(value) is dict)
    return value


def quantiles(values):
    ordered = sorted(values)
    count = len(ordered)
    return {
        "count": count,
        "p50_ns": ordered[(count * 50 + 99) // 100 - 1] if count else None,
        "p95_ns": ordered[(count * 95 + 99) // 100 - 1] if count else None,
        "p99_ns": ordered[(count * 99 + 99) // 100 - 1] if count else None,
        "max_ns": ordered[-1] if count else None,
    }


def equal_rate(actual, count, elapsed):
    require(elapsed > 0)
    require(math.isclose(number(actual), count * 1e9 / elapsed, rel_tol=1e-12, abs_tol=1e-12))


def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result)
        result[key] = value
    return result


def reject_constant(value):
    raise InvalidEvidence("evidence validation failed")


def parse_json(data):
    return json.loads(data, object_pairs_hook=unique_pairs, parse_constant=reject_constant)


def fingerprint(info):
    return info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_nlink, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def trusted_directory(info, private=False):
    require(stat.S_ISDIR(info.st_mode) and info.st_uid in (0, os.getuid()))
    mode = stat.S_IMODE(info.st_mode)
    if private:
        require(info.st_uid == os.getuid() and mode == 0o700)
    else:
        require(mode & 0o022 == 0 or info.st_uid == 0 and mode & stat.S_ISVTX != 0)


def read_evidence(directory):
    """Keep descriptors open through verification; reject changed paths before publishing."""
    require(".." not in Path(directory).parts)
    parts = Path(os.path.abspath(directory)).parts
    require(len(parts) > 1)
    with contextlib.ExitStack() as stack:
        root = os.open(parts[0], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        stack.callback(os.close, root)
        chain = [(root, None, None, fingerprint(os.fstat(root)))]
        trusted_directory(os.fstat(root))
        for index, component in enumerate(parts[1:], 1):
            parent = chain[-1][0]
            descriptor = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            stack.callback(os.close, descriptor)
            info = os.fstat(descriptor)
            trusted_directory(info, private=index == len(parts) - 1)
            chain.append((descriptor, parent, component, fingerprint(info)))
        directory_fd = chain[-1][0]
        opened = []
        for name, maximum in (("run.json", MAX_RUN_BYTES), ("operations.jsonl", MAX_OPERATIONS_BYTES)):
            descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
            stack.callback(os.close, descriptor)
            info = os.fstat(descriptor)
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
                    and stat.S_IMODE(info.st_mode) == 0o600 and 0 <= info.st_size <= maximum)
            opened.append((name, descriptor, maximum, fingerprint(info)))
        data = {}
        for name, descriptor, maximum, before in opened:
            with os.fdopen(os.dup(descriptor), "rb") as stream:
                body = stream.read(maximum + 1)
            require(len(body) <= maximum and len(body) == before[5])
            data[name] = body
        report = mapping(parse_json(data["run.json"]))
        operations = []
        for line in data["operations.jsonl"].splitlines(keepends=True):
            require(len(line) <= MAX_LINE_BYTES and line.endswith(b"\n"))
            operations.append(mapping(parse_json(line)))
        for descriptor, parent, name, before in chain:
            require(fingerprint(os.fstat(descriptor)) == before)
            if parent is not None:
                require(fingerprint(os.stat(name, dir_fd=parent, follow_symlinks=False)) == before)
        for name, descriptor, maximum, before in opened:
            require(fingerprint(os.fstat(descriptor)) == before)
            require(fingerprint(os.stat(name, dir_fd=directory_fd, follow_symlinks=False)) == before)
        hashes = {name: hashlib.sha256(body).hexdigest() for name, body in data.items()}
        return report, operations, hashes


def valid_observed_at(value):
    if type(value) is not str or RFC3339_NANO.fullmatch(value) is None:
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def embedded_prerequisites(report):
    """Check embedded claims only; this is not independent gate attestation."""
    gate = report.get("joint_gate")
    source = report.get("source")
    preflight = report.get("preflight_correctness")
    if not isinstance(gate, dict) or not isinstance(source, dict):
        return False
    stages = gate.get("stages")
    git_hash = source.get("git_hash")
    return (type(gate.get("version")) is int and gate["version"] == 1
            and gate.get("joint_pass") is True and gate.get("confirmed_by") == "main"
            and type(git_hash) is str and GIT_HASH.fullmatch(git_hash) is not None
            and gate.get("source_git") == git_hash
            and valid_observed_at(gate.get("observed_at"))
            and type(stages) is list and all(type(stage) is str for stage in stages)
            and REQUIRED_STAGES.issubset(stages)
            and type(source.get("dirty")) is bool and source["dirty"] is False
            and type(source.get("untracked_content_provenance")) is str
            and bool(source["untracked_content_provenance"].strip())
            and type(preflight) is list and bool(preflight)
            and all(type(check) is dict and check.get("status") == "PASS" for check in preflight))


def verify(directory):
    directory = Path(directory)
    report, raw_records, hashes = read_evidence(directory)
    identifier = report.get("run_id")
    require(type(identifier) is str and RUN_ID.fullmatch(identifier) is not None)
    require(directory.name == "run_" + identifier and report.get("version") == 1)
    require(report.get("status") in {"COMPLETE", "FAIL", "UNRESOLVED"})
    prerequisites_present = embedded_prerequisites(report)
    scenarios = report.get("scenarios")
    require(type(scenarios) is list)
    phases = {}
    for scenario_index, item in enumerate(scenarios):
        item = mapping(item)
        require(item.get("status") in {"COMPLETE", "FAIL", "UNRESOLVED", "SKIP"} and type(item.get("scenario")) is str)
        require(report["status"] != "COMPLETE" or item["status"] in {"COMPLETE", "SKIP"})
        entries = item.get("phases", [])
        require(type(entries) is list)
        for phase_index, entry in enumerate(entries):
            entry = mapping(entry)
            name, phase = item["scenario"], entry.get("phase")
            require(entry.get("scenario") == name and type(phase) is str)
            concurrency = integer(entry.get("concurrency"), 1)
            key = name, phase, concurrency
            require(entry.get("status") in STATUSES and key not in phases)
            require(type(entry.get("short_gate_not_benchmark", False)) is bool)
            require(not entry.get("started_at_observed") or "short_gate_not_benchmark" in entry)
            phases[key] = (scenario_index, phase_index, item["status"], entry, [], [], Counter())

    active_phase = {}
    for record in raw_records:
        if record.get("type") not in ("operation", "client_admission_drop", "session_cleanup"):
            continue
        label = record.get("scenario"), record.get("phase")
        if record["type"] == "session_cleanup":
            candidates = [key for key in phases if key[:2] == label]
            key = active_phase.get(label)
            if key is None and len(candidates) == 1:
                key = candidates[0]
        else:
            key = (*label, record.get("concurrency"))
            active_phase[label] = key
        require(key in phases)
        state = phases[key]
        entry, operations, drops, cleanups = state[3:]
        require(entry.get("started_at_observed"))
        if record["type"] == "session_cleanup":
            failure = record.get("failure")
            if failure is not None:
                failure = mapping(failure)
                require(type(failure.get("class")) is str and type(failure.get("unknown")) is bool)
                cleanups["cleanup_" + failure["class"]] += 1
                if failure["unknown"]:
                    cleanups["__unknown"] += 1
            continue
        sequence = integer(record.get("offered_sequence"))
        business_id = record.get("business_operation_id")
        require(type(business_id) is str and re.fullmatch(r"stress_" + identifier + r"_[0-9]{12}", business_id) is not None)
        require(integer(record.get("concurrency"), 1) == entry["concurrency"])
        if record["type"] == "operation":
            require(type(record.get("acked")) is bool and type(record.get("correct")) is bool)
            operations.append(record)
        else:
            drops.append(record)
        require(sequence >= 0)

    output = []
    has_unverified_offers = False
    for scenario_index, phase_index, scenario_status, entry, operations, drops, cleanups in phases.values():
        status = entry["status"]
        started = entry.get("started_at_observed")
        if not started:
            require(entry.get("measurement_status") == "unavailable_not_started")
            require(not operations and not drops and not cleanups)
            require(integer(entry.get("unknown")) >= 0 and integer(entry.get("cleanup_unknown")) >= 0)
            require(status != "COMPLETE")
            output.append({"scenario_index": scenario_index, "phase_index": phase_index,
                           "status": status, "benchmark": NOT_MEASURED})
            continue
        require(type(started) is str and type(entry.get("finished_at_observed")) is str)
        require(status in {"COMPLETE", "FAIL", "UNRESOLVED"})
        sequences = [record["offered_sequence"] for record in operations + drops]
        require(len(sequences) == len(set(sequences)))
        require(len({record["business_operation_id"] for record in operations + drops}) == len(sequences))
        attempted = len(operations)
        recorded_offers = attempted + len(drops)
        queue_expired = integer(entry.get("client_queue_expired"))
        offered = integer(entry.get("offered"))
        require(offered == recorded_offers + queue_expired)
        require(integer(entry.get("client_admission_dropped")) == len(drops))
        has_unverified_offers |= queue_expired > 0
        require(integer(entry.get("attempted")) == attempted)
        require(integer(entry.get("completed_workflows")) == attempted)
        planned = integer(entry.get("planned_offers"))
        require(integer(entry.get("scheduled_offers_not_emitted")) == planned - offered and planned >= offered)
        acked = sum(record["acked"] for record in operations)
        require(integer(entry.get("acked_workflows")) == acked)
        require(integer(entry.get("correctness_failures")) == sum(record["acked"] and not record["correct"] for record in operations))
        errors = Counter(cleanups)
        errors.pop("__unknown", None)
        unknown = 0
        timeouts = 0
        for record in operations:
            failure = record.get("failure")
            if record["acked"]:
                require(failure is None)
            else:
                failure = mapping(failure)
                require(type(failure.get("class")) is str and type(failure.get("unknown")) is bool and type(failure.get("timed_out")) is bool)
                errors[failure["class"]] += 1
                unknown += failure["unknown"]
                timeouts += failure["timed_out"]
            for field in ("http_workflow_ns", "client_queue_ns"):
                integer(record.get(field))
        require(integer(entry.get("unknown")) == unknown)
        require(integer(entry.get("timeouts")) == timeouts)
        require(integer(entry.get("cleanup_unknown")) == cleanups["__unknown"])
        reported_errors = mapping(entry.get("errors_by_class"))
        require(all(type(key) is str and type(value) is int and value >= 0 for key, value in reported_errors.items()))
        require({key: value for key, value in reported_errors.items() if value} == dict(errors))
        require(status == "UNRESOLVED" or unknown == 0 and cleanups["__unknown"] == 0)
        require(not unknown or status == "UNRESOLVED")
        require(scenario_status != "COMPLETE" or status == "COMPLETE")
        require(status != "COMPLETE" or not entry.get("reason"))
        elapsed = integer(entry.get("elapsed_ns"), 1)
        offering = integer(entry.get("offering_elapsed_ns"), 1)
        equal_rate(entry.get("observed_offered_per_second"), offered, offering)
        equal_rate(entry.get("acked_per_second_including_drain"), acked, elapsed)
        all_latency = quantiles([record["http_workflow_ns"] for record in operations])
        ack_latency = quantiles([record["http_workflow_ns"] for record in operations if record["acked"]])
        queue_latency = quantiles([record["client_queue_ns"] for record in operations])
        for key, computed in (("all_workflow_latency", all_latency), ("acked_workflow_latency", ack_latency), ("client_queue_latency", queue_latency)):
            require(mapping(entry.get(key)) == computed)
        require(entry.get("actual_admission_rejection_observed") is (errors.get("admission", 0) > 0))
        short = entry["short_gate_not_benchmark"]
        settings = mapping(report.get("settings"))
        require(short == (integer(settings.get("smoke_ns")) < 10_000_000_000 or integer(settings.get("measure_ns")) < 30_000_000_000))
        kind = "measure" if entry["phase"].endswith("measure") else "gate"
        require(integer(entry.get("requested_duration_ns"), 1) >= (30_000_000_000 if kind == "measure" and not short else 1))
        benchmark = (prerequisites_present and kind == "measure" and not short and queue_expired == 0
                     and status == scenario_status == report["status"] == "COMPLETE")
        output.append({"scenario_index": scenario_index, "phase_index": phase_index, "status": status,
                       "kind": kind, "reported_offered": offered, "recorded_offers": recorded_offers,
                       "reported_queue_expired": queue_expired,
                       "expired_offer_identity": NOT_MEASURED if queue_expired else "NOT APPLICABLE",
                       "offered_evidence": "RECONCILED_ONLY" if queue_expired else "RAW_RECORDS",
                       "attempted": attempted, "acked": acked,
                       "unknown": unknown, "cleanup_unknown": cleanups["__unknown"],
                       "client_admission_dropped": len(drops), "errors_by_class": NOT_MEASURED if not errors else sum(errors.values()),
                       "benchmark": {"acked_per_second": acked * 1e9 / elapsed,
                                     "offered_per_second": offered * 1e9 / offering,
                                     "all_workflow_latency": all_latency, "acked_workflow_latency": ack_latency,
                                     "client_queue_latency": queue_latency} if benchmark else NOT_MEASURED})
    return {"validation": "PARTIAL" if has_unverified_offers or not prerequisites_present else "ACCOUNTING_PASS",
            "embedded_prerequisites": "PRESENT_UNVERIFIED" if prerequisites_present else "INCOMPLETE",
            "independent_gate_verification": NOT_MEASURED,
            "source_binary_provenance": NOT_MEASURED,
            "untracked_content_verification": NOT_MEASURED,
            "limitation": "Embedded gate is an unverified operator attestation; source Git is not binary provenance, and untracked content is not independently verified. Benchmark is runner accounting only, not certified performance.",
            "run_status": report["status"], "run_id": identifier,
            "sha256": hashes,
            "phases": output, "durability_oracle": NOT_MEASURED, "cpu_rss": NOT_MEASURED,
            "storage_network": NOT_MEASURED, "recovery": NOT_MEASURED}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Verify stress evidence without connecting to the system")
    parser.add_argument("directory", help="run_<id> evidence directory")
    args = parser.parse_args(argv)
    try:
        result = verify(args.directory)
    except (InvalidEvidence, OSError, UnicodeError, ValueError, TypeError, KeyError, RecursionError, OverflowError):
        print("Invalid or incomplete evidence; no metrics published.", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
