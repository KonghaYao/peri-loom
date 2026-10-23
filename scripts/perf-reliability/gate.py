#!/usr/bin/env python3
"""Issue 8 gate: recompute native thresholds from raw evidence only.

Attribution is mandatory by default; --allow-missing-attribution is a
diagnostic exception capped at PHASE_PASS. PHASE_PASS is never a formal PASS:
per acceptance.status_rules.any_not_pass_exit_nonzero, only an overall PASS
exits 0, so PHASE_PASS, INCOMPLETE, NOT_RUN and FAIL all exit nonzero. A
formal PASS additionally requires
--trusted-artifacts, an external operator-supplied arm->sha256 list. That list
is the operator trust root: identity, artifacts, run manifests and the list
itself are all writable by the evidence producer, so the gate can only prove
internal consistency and that the list lives outside the evidence root. It
cannot prove the list is trustworthy; a wrong binary paired with a wrong
external list remains a residual trust boundary, not a solved provenance
problem. G1 is a phase gate: its highest status is PHASE_PASS, and only a
complete G2 evaluation can produce an overall PASS.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import platform
import re
import statistics
import sys

import campaign as campaign_module
import evidence as evidence_module
import issue8_workload as workload
import perf_contract

PASS = "PASS"
PHASE_PASS = "PHASE_PASS"
NOT_RUN = "NOT_RUN"
INCOMPLETE = "INCOMPLETE"
FAIL = "FAIL"
STATUS_PRIORITY = {NOT_RUN: 1, INCOMPLETE: 2, FAIL: 3}
HEX64 = re.compile(r"^[0-9a-f]{64}$")
REQUIRED_ENVIRONMENT = ("run_id", "pair", "arm", "case", "concurrency", "profile",
                        "pair_order", "timed_started_at_ns", "timed_completed_at_ns",
                        "warmup_operations", "required_operations_per_lane", "minimum_seconds",
                        "max_seconds_per_arm", "budget_exhausted", "contract_sha256",
                        "workload_sha256", "campaign_sha256", "source_inventory_sha256",
                        "arm_version", "arm_commit", "binary_sha256", "initial_state_sha256",
                        "post_warmup_state_sha256", "metrics_snapshot_requested",
                        "host_environment", "roots_validation",
                        "run_started_at_unix_ms", "timed_started_at_unix_ms",
                        "timed_completed_at_unix_ms", "run_completed_at_unix_ms",
                        "rows", "payload_bytes")
REQUIRED_HOST_FIELDS = ("os", "arch", "kernel", "machine")
FORBIDDEN_FILESYSTEMS = {"tmpfs", "ramfs", "devtmpfs", "overlay"}
# Frozen from prototypes/v1/docs/metrics-exit-snapshot.md (schema_version 1)
# and PublishStage::ALL in prototypes/v1/src/work_metrics.rs.
METRICS_PHASES = ("snapshot", "clone", "mutate", "prepare", "receipt", "validate",
                  "index", "seal", "encode", "group", "recover")
# Counter names are the serialized work_metrics paths from the same sources.
METRICS_COUNTERS = ("work.wal_workload.append_calls",
                    "work.metadata.root_publish_attempts",
                    "object_store.put.total",
                    "object_store.wal_durability.total")
METRICS_GENERATED_AT_MIN_MS = 1_600_000_000_000  # 2020-09-13
METRICS_GENERATED_AT_MAX_MS = 4_102_444_800_000  # 2100-01-01
# The metrics snapshot is written by the worker as it exits, after the timed
# window. A producer cannot be expected to align wall and monotonic clocks
# better than a few seconds, so the window checks use this tolerance.
METRICS_WINDOW_TOLERANCE_MS = 5_000
# Sampling interval used when resources.json omits interval_seconds. It is also
# an upper bound on a declared interval: a producer cannot claim a sparse
# sampler and then satisfy density with a handful of samples.
RESOURCE_INTERVAL_FALLBACK_SECONDS = 1.0
EPHEMERAL_ROOTS = tuple(Path(path).resolve()
                        for path in ("/tmp", "/var/tmp", "/dev/shm"))
ROOT_LABELS = ("store", "cache")
LIMITS = {"issues": 20}


class GateError(ValueError):
    pass


def combine(statuses):
    values = [status for status in statuses if status != PASS]
    if not values:
        return PASS
    return max(values, key=lambda status: STATUS_PRIORITY[status])


def status_exit_code(status, status_rules):
    """Only a full PASS may exit 0, per acceptance.status_rules.

    The frozen contract states ``any_not_pass_exit_nonzero``. If that rule is
    missing or contradicted, fail closed (nonzero) rather than guessing.
    """
    rule = status_rules.get("any_not_pass_exit_nonzero") if isinstance(status_rules, dict) else None
    if rule is not True:
        return 1
    return 0 if status == PASS else 1


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def _is_int(value):
    return type(value) is int


def _is_number(value):
    return type(value) in (int, float)


def _is_hex(value):
    return isinstance(value, str) and bool(HEX64.match(value))


# G2 subsumes G1. The effective tier is never below any tier a producer
# declared: the CLI may ask for a stricter review, never a weaker one.
TIER_RANK = {"G1": 1, "G2": 2}


def _strictest_tier(*tiers):
    declared = [tier for tier in tiers if tier is not None]
    if not declared:
        return None
    return max(declared, key=lambda tier: TIER_RANK[tier])


def _failure_present(value):
    """A failure marker is any non-empty value; missing/null/empty means none."""
    if value is None or value is False:
        return False
    if isinstance(value, str):
        return value != ""
    if isinstance(value, (dict, list, tuple, set)):
        return bool(value)
    return True


def _empty_or_zero(value):
    return value is None or value is False or value == 0 or value == "" \
        or value == [] or value == {}


def _add(reasons, reason):
    if len(reasons) < LIMITS["issues"]:
        reasons.append(reason)


def _milliseconds(value):
    return None if value is None else value / 1_000_000


def _latency(durations):
    return {"count": len(durations), "p50_ns": percentile(durations, .5),
            "p95_ns": percentile(durations, .95), "p99_ns": percentile(durations, .99),
            "p50_ms": _milliseconds(percentile(durations, .5)),
            "p95_ms": _milliseconds(percentile(durations, .95)),
            "p99_ms": _milliseconds(percentile(durations, .99))}


def _buckets(case, lane):
    if case == "S1":
        buckets = {"write": lane["buckets"]["write"]}
        buckets["all"] = buckets["write"]
    else:
        buckets = {"read": lane["buckets"]["read"], "write": lane["buckets"]["write"]}
        buckets["all"] = {"attempts": buckets["read"]["attempts"] + buckets["write"]["attempts"],
                          "ack_durations": buckets["read"]["ack_durations"] + buckets["write"]["ack_durations"],
                          "attempt_durations": buckets["read"]["attempt_durations"]
                          + buckets["write"]["attempt_durations"]}
    return {name: {"attempts": value["attempts"], "acks": len(value["ack_durations"]),
                   "ack": _latency(value["ack_durations"]),
                   "attempt": _latency(value["attempt_durations"])}
            for name, value in buckets.items()}


def _run_span(environment):
    return (environment["timed_completed_at_ns"] - environment["timed_started_at_ns"]) / 1_000_000_000


def _result_sha256(payload):
    return hashlib.sha256(json.dumps([[payload]], separators=(",", ":"),
                                     sort_keys=True).encode()).hexdigest()


def _payload_matches(actual, expected, uncertain):
    if not isinstance(actual, list) or len(actual) != len(expected):
        return False
    for index, row in enumerate(actual):
        if not isinstance(row, list) or len(row) != 2 or not _is_int(row[0]) or row[0] != index:
            return False
        if row[1] not in uncertain.get(index, [expected[index]]):
            return False
    return True


def _empty_lane(rows, payload_bytes):
    return {"attempts": 0, "acks": 0, "errors": 0, "unknowns": 0, "mismatches": 0,
            "warmup_attempts": 0, "warmup_mismatches": 0,
            "expected": ["x" * payload_bytes for _ in range(rows)], "uncertain": {},
            "post_warmup_expected": None, "post_warmup_uncertain": None,
            "buckets": {"read": {"attempts": 0, "ack_durations": [], "attempt_durations": []},
                        "write": {"attempts": 0, "ack_durations": [], "attempt_durations": []}},
            "timed_sequences": [], "warmup_sequences": [], "ack_types": {},
            "warmup_completed_at": [], "timed_issued_at": [],
            "kind_indices": {"warmup": {"read": [], "write": []},
                             "timed": {"read": [], "write": []}},
            "schedule_indices": {"warmup": [], "timed": []}}


def _resolved(path):
    return Path(path).expanduser().resolve()


def _is_within(path, root):
    return path == root or path.is_relative_to(root)


def _validate_host_environment(host, problems):
    """Return True when the declared host differs from the gate host."""
    if not isinstance(host, dict):
        problems.append("host_environment_type")
        return False
    missing = [name for name in REQUIRED_HOST_FIELDS
               if not isinstance(host.get(name), str) or not host[name].strip()]
    if missing:
        problems.append("host_environment_missing:" + ",".join(missing))
    if "cpu_count" in host and (not _is_int(host["cpu_count"]) or host["cpu_count"] <= 0):
        problems.append("host_environment_cpu_count")
    if missing:
        return False
    return host["os"] != platform.system() or host["arch"] != platform.machine()


def _validate_roots_validation(document, evidence_root, problems):
    """Reject tmpfs/overlay, unverified persistence, and ephemeral evidence roots."""
    if not isinstance(document, dict):
        problems.append("roots_validation_type")
        return
    if document.get("persistent_mounts_verified") is not True:
        problems.append("persistent_mounts_not_verified")
    roots = document.get("roots")
    if not isinstance(roots, dict):
        problems.append("roots_missing")
        roots = {}
    filesystems = document.get("filesystems")
    if filesystems is not None and not isinstance(filesystems, dict):
        problems.append("filesystems_type")
        filesystems = None
    forbidden = sorted(name for name, value in (filesystems or {}).items()
                       if value in FORBIDDEN_FILESYSTEMS)
    if forbidden:
        problems.append("forbidden_filesystem:" + ",".join(forbidden))
    evidence = _resolved(evidence_root) if evidence_root is not None else None
    resolved_roots = {}
    for label in ROOT_LABELS:
        entry = roots.get(label)
        if isinstance(entry, dict):
            path = entry.get("path")
            filesystem = entry.get("filesystem")
            if not isinstance(path, str) or not path:
                problems.append("root_path_missing:" + label)
                continue
            if not isinstance(filesystem, str) or not filesystem:
                problems.append("root_filesystem_missing:" + label)
            elif filesystem in FORBIDDEN_FILESYSTEMS:
                problems.append("root_filesystem_forbidden:" + label + ":" + filesystem)
            if entry.get("persistent") is not True:
                problems.append("root_not_persistent:" + label)
            if entry.get("verified") is not True:
                problems.append("root_not_verified:" + label)
        elif isinstance(entry, str) and entry:
            path = entry
            filesystem = (filesystems or {}).get(label)
            if not isinstance(filesystem, str) or not filesystem:
                problems.append("root_filesystem_missing:" + label)
            elif filesystem in FORBIDDEN_FILESYSTEMS:
                problems.append("root_filesystem_forbidden:" + label + ":" + filesystem)
            if document.get("persistent_mounts_verified") is not True:
                problems.append("root_not_persistent:" + label)
            if not isinstance(filesystem, str) or not filesystem:
                problems.append("root_not_verified:" + label)
        else:
            problems.append("root_missing:" + label)
            continue
        if not Path(path).is_absolute():
            problems.append("root_not_absolute:" + label)
        resolved = _resolved(path)
        for ephemeral in EPHEMERAL_ROOTS:
            if _is_within(resolved, ephemeral):
                problems.append("root_on_ephemeral_path:" + label)
                break
        if evidence is not None and _is_within(resolved, evidence):
            problems.append("root_inside_evidence:" + label)
        resolved_roots[label] = resolved
    if len(resolved_roots) == len(ROOT_LABELS):
        first, second = (resolved_roots[label] for label in ROOT_LABELS)
        if _is_within(first, second) or _is_within(second, first):
            problems.append("roots_not_distinct")


def _validate_environment(environment, campaign, policy, run_directory, contract_sha, workload_sha,
                          evidence_root=None, allow_foreign_environment=False):
    problems = []
    for key in REQUIRED_ENVIRONMENT:
        if key not in environment:
            problems.append("missing_" + key)
    if problems:
        raise evidence_module.EvidenceError("environment_schema: " + ",".join(problems))
    # The runner records any run-level failure here, including failures after
    # the timed window. A failed run is not evidence, even if its files parse.
    if _failure_present(environment.get("failure")):
        problems.append("environment_failure")
    if environment["run_id"] != run_directory.name:
        problems.append("run_id_directory_mismatch")
    if environment["profile"] != policy["name"]:
        problems.append("profile_mismatch")
    if not isinstance(environment["case"], str) or environment["case"] not in {"S1", "S2"}:
        problems.append("case_outside_w0a")
    if not isinstance(environment["arm"], str) or environment["arm"] not in campaign["arms"]:
        problems.append("arm")
    if not _is_int(environment["rows"]) or not _is_int(environment["payload_bytes"]):
        problems.append("fixture_shape_type")
    elif environment["rows"] != 64 or environment["payload_bytes"] != 256:
        problems.append("fixture_shape_outside_contract")
    if not _is_int(environment["concurrency"]) or environment["concurrency"] not in {1, 2}:
        problems.append("concurrency")
    if not _is_int(environment["pair"]) or not 0 <= environment["pair"] < policy["pairs"]:
        problems.append("pair")
    if environment["arm"] not in campaign["arms"]:
        problems.append("arm")
    if not _is_int(environment["timed_started_at_ns"]) or not _is_int(environment["timed_completed_at_ns"]):
        problems.append("timed_window_type")
    elif environment["timed_completed_at_ns"] <= environment["timed_started_at_ns"]:
        problems.append("timed_window_order")
    wall_keys = ("run_started_at_unix_ms", "timed_started_at_unix_ms",
                 "timed_completed_at_unix_ms", "run_completed_at_unix_ms")
    for key in wall_keys:
        if not _is_int(environment[key]) or environment[key] <= 0:
            problems.append(key)
    if all(_is_int(environment[key]) and environment[key] > 0 for key in wall_keys):
        if not (environment["run_started_at_unix_ms"] <= environment["timed_started_at_unix_ms"]
                <= environment["timed_completed_at_unix_ms"] <= environment["run_completed_at_unix_ms"]):
            problems.append("wall_window_order")
    for key in ("minimum_seconds", "max_seconds_per_arm"):
        if not _is_number(environment[key]) or environment[key] < 0:
            problems.append(key)
    for key in ("warmup_operations", "required_operations_per_lane"):
        if not _is_int(environment[key]) or environment[key] < 0:
            problems.append(key)
    if type(environment["budget_exhausted"]) is not bool:
        problems.append("budget_exhausted")
    if type(environment["metrics_snapshot_requested"]) is not bool:
        problems.append("metrics_snapshot_requested")
    if not isinstance(environment["pair_order"], str) or environment["pair_order"] not in {"AB", "BA"}:
        problems.append("pair_order")
    for key in ("contract_sha256", "workload_sha256", "campaign_sha256", "arm_version", "arm_commit"):
        if not isinstance(environment[key], str):
            problems.append(key + "_type")
    for key in ("binary_sha256", "initial_state_sha256", "post_warmup_state_sha256",
                "source_inventory_sha256"):
        if not _is_hex(environment[key]):
            problems.append(key + "_sha256")
    for key in ("arm_version", "arm_commit"):
        if isinstance(environment[key], str) and not environment[key]:
            problems.append(key + "_empty")
    if environment["contract_sha256"] != contract_sha:
        problems.append("contract_hash_mismatch")
    if environment["workload_sha256"] != workload_sha:
        problems.append("workload_hash_mismatch")
    if environment["campaign_sha256"] != campaign_module.campaign_identity(campaign):
        problems.append("campaign_hash_mismatch")
    if environment["warmup_operations"] != policy["warmup_operations"]:
        problems.append("warmup_plan_mismatch")
    minimum_required = policy["min_samples_per_lane"]
    if environment["case"] == "S2":
        minimum_required *= 2
    if _is_int(environment["required_operations_per_lane"]) \
            and environment["required_operations_per_lane"] < minimum_required:
        problems.append("operation_plan_below_contract")
    if _is_number(environment["minimum_seconds"]) \
            and environment["minimum_seconds"] < policy["min_seconds"]:
        problems.append("duration_plan_below_contract")
    if _is_number(environment["minimum_seconds"]) and _is_number(environment["max_seconds_per_arm"]) \
            and environment["minimum_seconds"] > environment["max_seconds_per_arm"]:
        problems.append("duration_plan_above_arm_budget")
    derived = perf_contract.load_derived()
    maximum_budget = derived["profiles"].get(policy["name"], {}).get(
        "maximum_seconds_per_arm", policy["max_seconds_per_arm"])
    if _is_number(environment["max_seconds_per_arm"]) \
            and environment["max_seconds_per_arm"] > maximum_budget:
        problems.append("arm_budget_above_contract")
    try:
        campaign_module.check_arm_evidence(campaign, environment["arm"], environment)
    except campaign_module.CampaignError as failure:
        problems.append(str(failure))
    if campaign_module.arm_metadata_problems(campaign, environment["arm"]):
        problems.append("arm_metadata_incomplete: " + environment["arm"])
    foreign_environment = _validate_host_environment(environment["host_environment"], problems)
    _validate_roots_validation(environment["roots_validation"], evidence_root, problems)
    if foreign_environment and not allow_foreign_environment:
        problems.append("foreign_environment_not_allowed")
    if problems:
        raise evidence_module.EvidenceError("environment_invalid: " + ",".join(problems))
    return foreign_environment


def _validate_record(record, environment, policy, lanes, sequences, integrity, correctness):
    required = ("run", "pair", "arm", "case", "lane", "seq", "kind", "phase", "row",
                "schedule_index", "planned_at", "issued_at", "completed_at", "duration_ns",
                "outcome", "ack_type", "durable_ack", "oracle")
    for key in required:
        if key not in record:
            _add(integrity, "record_missing_" + key)
            return
    if record["run"] != environment["run_id"] or record["pair"] != environment["pair"] \
            or record["arm"] != environment["arm"] or record["case"] != environment["case"]:
        _add(integrity, "record_identity_mismatch")
    if not _is_int(record["lane"]) or not 0 <= record["lane"] < environment["concurrency"]:
        _add(integrity, "record_lane")
        return
    if not isinstance(record["phase"], str) or record["phase"] not in {"warmup", "timed"}:
        _add(integrity, "record_phase")
        return
    if not isinstance(record["kind"], str) or record["kind"] not in {"read", "write"}:
        _add(integrity, "record_kind")
        return
    for key in ("planned_at", "issued_at", "completed_at", "duration_ns", "seq",
                "row", "schedule_index"):
        if not _is_int(record[key]) or record[key] < 0:
            _add(integrity, "record_" + key)
            return
    # The frozen schedule is a pure function of the per-lane schedule index:
    # warmup starts at 0 and timed continues at warmup_operations. Comparing
    # seq->index here keeps a hot-row rewrite from passing as a valid schedule.
    lane = lanes[record["lane"]]
    expected_index = record["seq"] if record["phase"] == "warmup" \
        else environment["warmup_operations"] + record["seq"]
    if record["schedule_index"] != expected_index:
        _add(integrity, "record_schedule_index_mismatch")
    if record["kind"] != workload.operation_kind(environment["case"],
                                                 record["schedule_index"]):
        _add(integrity, "record_kind_schedule_mismatch")
    lane["schedule_indices"][record["phase"]].append(record["schedule_index"])
    lane["kind_indices"][record["phase"]][record["kind"]].append(record["schedule_index"])
    rows, payload_bytes = environment["rows"], environment["payload_bytes"]
    if record["row"] != record["schedule_index"] % rows:
        _add(integrity, "record_row_schedule_mismatch")
    if not record["planned_at"] <= record["issued_at"] <= record["completed_at"]:
        _add(integrity, "record_time_order")
    if record["duration_ns"] != record["completed_at"] - record["issued_at"]:
        _add(integrity, "record_duration_mismatch")
    if record["duration_ns"] <= 0:
        _add(integrity, "record_duration_nonpositive")
    key = (record["lane"], record["phase"])
    if record["seq"] in sequences.setdefault(key, set()):
        _add(integrity, "record_duplicate_seq")
    sequences[key].add(record["seq"])
    if record["phase"] == "timed" and lane["post_warmup_expected"] is None:
        lane["post_warmup_expected"] = list(lane["expected"])
        lane["post_warmup_uncertain"] = {row: list(values) for row, values in lane["uncertain"].items()}
    if record["phase"] == "warmup" and lane["timed_sequences"]:
        _add(integrity, "warmup_after_timed")
    if record["phase"] == "warmup":
        lane["warmup_completed_at"].append(record["completed_at"])
    else:
        lane["timed_issued_at"].append(record["issued_at"])
    payload = workload.payload_value(record["schedule_index"], rows, payload_bytes)
    if not isinstance(record["outcome"], str) or record["outcome"] not in {"ACK", "Error", "Unknown"}:
        _add(integrity, "record_outcome")
        return
    if type(record["durable_ack"]) is not bool:
        _add(integrity, "record_durable_ack")
    if not isinstance(record["ack_type"], str):
        _add(integrity, "record_ack_type")
        return
    if record["outcome"] == "ACK":
        if not record["ack_type"]:
            _add(integrity, "record_ack_type_missing")
        if record["ack_type"] == "commit":
            _add(correctness, "commit_ack_in_statement_case")
        if record["durable_ack"] is True:
            _add(correctness, "statement_ack_claimed_durable")
        expected_ack = "read_result" if record["kind"] == "read" else "statement_autocommit"
        if record["ack_type"] != expected_ack:
            _add(correctness, "unexpected_ack_type")
    elif record["ack_type"]:
        _add(integrity, "record_non_ack_ack_type")
    if record["kind"] == "read":
        if not isinstance(record["oracle"], str) or record["oracle"] not in {"match", "mismatch"}:
            _add(integrity, "record_read_oracle_schema")
        elif record["oracle"] == "mismatch":
            _add(correctness, "read_oracle_mismatch")
        if record["outcome"] == "ACK":
            if not _is_hex(record.get("observed_sha256")):
                _add(integrity, "observed_sha256_missing")
            elif record["observed_sha256"] != _result_sha256(lane["expected"][record["row"]]):
                _add(correctness, "read_observed_hash_mismatch")
    elif record["oracle"] != "not_applicable":
        _add(integrity, "record_write_oracle_schema")
    if record["kind"] == "write":
        if record["outcome"] == "ACK":
            lane["expected"][record["row"]] = payload
            lane["uncertain"].pop(record["row"], None)
        elif record["outcome"] == "Unknown":
            lane["uncertain"][record["row"]] = [lane["expected"][record["row"]], payload]
    if record["phase"] == "warmup":
        lane["warmup_attempts"] += 1
        lane["warmup_sequences"].append(record["seq"])
        if record["outcome"] != "ACK":
            _add(correctness, record["outcome"].lower() + "_during_warmup")
        if record["kind"] == "read" and record["oracle"] == "mismatch":
            lane["warmup_mismatches"] += 1
        return
    lane["attempts"] += 1
    lane["timed_sequences"].append(record["seq"])
    lane["ack_types"][record["ack_type"]] = lane["ack_types"].get(record["ack_type"], 0) + 1
    bucket = lane["buckets"][record["kind"]]
    bucket["attempts"] += 1
    bucket["attempt_durations"].append(record["duration_ns"])
    if record["outcome"] == "ACK":
        lane["acks"] += 1
        bucket["ack_durations"].append(record["duration_ns"])
    elif record["outcome"] == "Unknown":
        lane["unknowns"] += 1
        _add(correctness, "unknown_result")
    else:
        lane["errors"] += 1
        _add(correctness, "error_result")
    if record["kind"] == "read" and record["oracle"] == "mismatch":
        lane["mismatches"] += 1


def _oracle_actuals(oracle, section):
    value = oracle.get(section) if isinstance(oracle, dict) else None
    lanes = value.get("lanes") if isinstance(value, dict) else None
    if not isinstance(lanes, list):
        return None
    actuals = {}
    for entry in lanes:
        if not isinstance(entry, dict) or not _is_int(entry.get("lane")):
            return None
        actuals[entry["lane"]] = entry.get("actual")
    return actuals


def _oracle_content(oracle, lanes, environment):
    available, reasons = True, []
    initial = _oracle_actuals(oracle, "initial")
    post = _oracle_actuals(oracle, "post_warmup")
    final = _oracle_actuals(oracle, "final")
    if initial is None or post is None or final is None:
        return False, ["oracle_content_missing"]
    initial_expected = ["x" * environment["payload_bytes"] for _ in range(environment["rows"])]
    for lane_number, lane in lanes.items():
        if lane["post_warmup_expected"] is None:
            lane["post_warmup_expected"] = list(lane["expected"])
            lane["post_warmup_uncertain"] = {row: list(values) for row, values in lane["uncertain"].items()}
        if lane_number not in initial or lane_number not in post or lane_number not in final:
            available = False
            reasons.append("oracle_lane_missing:" + str(lane_number))
            continue
        if initial[lane_number] is None or post[lane_number] is None or final[lane_number] is None:
            available = False
            reasons.append("oracle_content_unavailable:" + str(lane_number))
            continue
        if not _payload_matches(initial[lane_number], initial_expected, {}):
            _add(reasons, "oracle_initial_content_mismatch")
        if not _payload_matches(post[lane_number], lane["post_warmup_expected"],
                                lane["post_warmup_uncertain"] or {}):
            _add(reasons, "oracle_post_warmup_content_mismatch")
        if not _payload_matches(final[lane_number], lane["expected"], lane["uncertain"]):
            _add(reasons, "oracle_final_content_mismatch")
    for lane_number in range(environment["concurrency"]):
        if lane_number not in initial or lane_number not in post or lane_number not in final:
            available = False
            _add(reasons, "oracle_lane_missing:" + str(lane_number))
    return available, reasons


def _sampling_interval(sampler):
    """Read the sampler interval, capped at a documented 1 Hz floor.

    A producer cannot make the density check vacuous by declaring a large
    interval; a missing or malformed interval falls back to the same floor.
    """
    declared = sampler.get("interval_seconds")
    if not _is_number(declared) or not math.isfinite(declared) or declared <= 0:
        return RESOURCE_INTERVAL_FALLBACK_SECONDS
    return min(float(declared), RESOURCE_INTERVAL_FALLBACK_SECONDS)


def _window_sample_entries(samples, environment):
    """Return (index, sample) pairs whose monotonic time is inside the timed window.

    Density, coverage and pid/root binding only count samples taken while the
    timed workload ran. Samples from setup/teardown may exist but cannot stand
    in for missing in-window measurements.
    """
    started = environment.get("timed_started_at_ns")
    completed = environment.get("timed_completed_at_ns")
    if not _is_int(started) or not _is_int(completed):
        return []
    return [(index, sample) for index, sample in enumerate(samples)
            if isinstance(sample, dict) and _is_int(sample.get("monotonic_ns"))
            and started <= sample["monotonic_ns"] <= completed]


def _resource_raw_reasons(sampler, environment):
    """Recompute peak/window from raw samples; the summary fields are not evidence.

    Every sample must be a successful measurement: a partial ps failure,
    missing process root, or null RSS means the resource evidence is unproven
    even when a later window happens to be measurable. The sample stream must
    also be dense enough inside the timed window, not just internally
    self-consistent or stretched across setup and teardown.
    """
    samples = sampler.get("samples")
    if not isinstance(samples, list) or not samples:
        return ["raw_samples_missing"]
    reasons = []
    if sampler.get("sample_count") != len(samples):
        reasons.append("resource_sample_count_mismatch")
    window_indices = {index for index, _ in _window_sample_entries(samples, environment)}
    for index, sample in enumerate(samples):
        if not isinstance(sample, dict):
            reasons.append("raw_sample_not_object:" + str(index))
            continue
        if _failure_present(sample.get("failure")):
            reasons.append("raw_sample_failure:" + str(index))
        if _failure_present(sample.get("missing_roots")):
            reasons.append("raw_sample_missing_roots:" + str(index))
        if not _non_negative_integer(sample.get("rss_bytes")):
            reasons.append("raw_sample_rss:" + str(index))
        if not _non_negative_integer(sample.get("monotonic_ns")):
            reasons.append("raw_sample_monotonic:" + str(index))
        pids = sample.get("pids")
        if not isinstance(pids, list) or any(not _is_int(pid) or pid <= 0 for pid in pids):
            reasons.append("raw_sample_pids:" + str(index))
        elif index in window_indices and not pids:
            # In-window samples are the only ones that may support the timed
            # measurements; an empty process list is not a successful sample.
            reasons.append("raw_sample_pids_empty:" + str(index))
        if index in window_indices:
            roots = sample.get("roots")
            if not isinstance(roots, list) or any(not _is_int(pid) or pid <= 0 for pid in roots):
                reasons.append("raw_sample_roots:" + str(index))
            elif not roots:
                reasons.append("raw_sample_roots_empty:" + str(index))
    if reasons:
        return reasons
    timestamps = [sample["monotonic_ns"] for sample in samples]
    if any(later <= earlier for earlier, later in zip(timestamps, timestamps[1:])):
        reasons.append("raw_sample_monotonic_order")
    window_size = max(1, math.ceil(len(samples) * 0.1))
    window = samples[-window_size:]
    window_rss = [sample["rss_bytes"] for sample in window]
    peak = max(sample["rss_bytes"] for sample in samples)
    if sampler.get("peak_rss_bytes") != peak:
        reasons.append("rss_peak_summary_mismatch")
    if sampler.get("last_window_rss_bytes") != window_rss:
        reasons.append("rss_window_summary_mismatch")
    # Density and coverage use the same monotonic clock as timed_started_at_ns
    # (ResourceSampler.sample_once and issue8_runner both use monotonic_ns).
    interval = _sampling_interval(sampler)
    timed_started = environment["timed_started_at_ns"]
    timed_completed = environment["timed_completed_at_ns"]
    interval_ns = int(interval * 1_000_000_000)
    timed_seconds = (timed_completed - timed_started) / 1_000_000_000
    required = math.ceil(timed_seconds / interval)
    window_samples = [sample for _, sample in _window_sample_entries(samples, environment)]
    # One missing sample is tolerated for scheduling jitter, but an empty
    # window is never coverage: 1811 samples stacked before/after the window
    # must not satisfy density.
    minimum_window_samples = max(1, required - 1)
    if len(window_samples) < minimum_window_samples:
        reasons.append("resource_sample_density_low")
    if not window_samples:
        reasons.append("resource_sample_coverage_short")
        reasons.append("resource_sample_window_start_late")
        reasons.append("resource_sample_window_end_early")
        return reasons
    first_ns = window_samples[0]["monotonic_ns"]
    last_ns = window_samples[-1]["monotonic_ns"]
    # The first and last in-window samples must bracket the timed window; the
    # old check compared the first/last sample of the whole stream, so samples
    # outside the window could fake both endpoints.
    if (last_ns - first_ns) + interval_ns < timed_completed - timed_started:
        reasons.append("resource_sample_coverage_short")
    if first_ns > timed_started + interval_ns:
        reasons.append("resource_sample_window_start_late")
    if last_ns < timed_completed - interval_ns:
        reasons.append("resource_sample_window_end_early")
    window_timestamps = [sample["monotonic_ns"] for sample in window_samples]
    if any(later - earlier > 2 * interval_ns
           for earlier, later in zip(window_timestamps, window_timestamps[1:])):
        reasons.append("resource_sample_window_gap")
    return reasons


def _resource_window_binding_reasons(sampler, environment, lane_pids):
    """Bind in-window samples to the worker processes the run reports.

    Sample `roots` are the process roots the sampler was constructed with (the
    initial worker pids), not filesystem paths; store/cache paths are covered
    by `roots_validation`. Each lane's worker pid must be observed in at least
    one in-window sample, and one in-window sample must observe all worker
    roots together, otherwise a producer can hide a worker by splitting pids
    across samples. Requiring `roots` to equal the lane pids rejects samples
    that describe a different process tree.
    """
    samples = sampler.get("samples")
    if not isinstance(samples, list):
        return []
    worker_pids = set()
    for pids in lane_pids.values():
        worker_pids.update(pids)
    if not worker_pids:
        return []
    window_entries = _window_sample_entries(samples, environment)
    reasons = []
    observed = set()
    for _, sample in window_entries:
        pids = sample.get("pids")
        if isinstance(pids, list):
            observed.update(pid for pid in pids if _is_int(pid))
    for pid in sorted(worker_pids - observed):
        reasons.append("resource_window_pid_unsampled:" + str(pid))
    together = any(
        worker_pids <= {pid for pid in sample.get("pids", []) if _is_int(pid)}
        for _, sample in window_entries if isinstance(sample.get("pids"), list))
    if not together:
        reasons.append("resource_window_pids_not_together")
    for index, sample in window_entries:
        roots = sample.get("roots")
        if not isinstance(roots, list) or any(not _is_int(pid) or pid <= 0 for pid in roots):
            continue
        if set(roots) != worker_pids:
            reasons.append("raw_sample_roots_mismatch:" + str(index))
    return reasons


def _resources_status(resources, concurrency, environment):
    reasons = []
    if not isinstance(resources, dict):
        return NOT_RUN, ["resources_not_object"], {}
    if resources.get("status") != "MEASURED":
        reasons.append("resources_unmeasured")
    sampler = resources.get("sampler")
    if not isinstance(sampler, dict):
        return NOT_RUN, reasons + ["resource_sampler_missing"], {}
    if sampler.get("status") != "MEASURED":
        reasons.append("rss_unmeasured")
    failures = sampler.get("failures")
    if not isinstance(failures, list):
        reasons.append("sampler_failures_schema")
    elif failures:
        reasons.append("sampler_failures")
    reasons.extend(_resource_raw_reasons(sampler, environment))
    cpu = sampler.get("cpu_seconds")
    if not _is_number(cpu) or cpu < 0:
        reasons.append("cpu_missing")
    disk = resources.get("disk")
    if not isinstance(disk, dict) or not disk:
        reasons.append("disk_missing")
    else:
        for name in ROOT_LABELS:
            if name not in disk:
                reasons.append("disk_missing:" + name)
        for name, entry in disk.items():
            if not isinstance(entry, dict) or not _is_int(entry.get("growth_bytes")):
                reasons.append("disk_growth_missing:" + name)
                continue
            if entry["growth_bytes"] < 0:
                reasons.append("disk_growth_negative:" + name)
            start, end = entry.get("start_bytes"), entry.get("end_bytes")
            if _is_int(start) and _is_int(end) and end - start != entry["growth_bytes"]:
                reasons.append("disk_growth_inconsistent:" + name)
    processes = resources.get("processes")
    lane_pids = {}
    if not isinstance(processes, list) or not processes:
        reasons.append("processes_missing")
    else:
        # One worker process per concurrent lane is the floor implied by the
        # run's concurrency. More is allowed (forks), fewer is unaccounted.
        if len(processes) < concurrency:
            reasons.append("process_count_below_concurrency")
        pids = []
        for process in processes:
            if not isinstance(process, dict):
                reasons.append("process_not_object")
                continue
            pid = process.get("pid")
            if not _is_int(pid) or pid <= 0:
                reasons.append("process_pid_invalid")
            else:
                pids.append(pid)
            lane = process.get("lane")
            if not _is_int(lane) or not 0 <= lane < concurrency:
                reasons.append("process_lane_invalid")
            elif _is_int(pid) and pid > 0:
                lane_pids.setdefault(lane, set()).add(pid)
            if process.get("group_joined") is not True:
                reasons.append("process_group_not_joined")
            if not _is_int(process.get("exit_code")) or process["exit_code"] != 0:
                reasons.append("process_exit_nonzero")
            if _failure_present(process.get("cleanup_failure")):
                reasons.append("process_cleanup_failure")
            if _failure_present(process.get("group_proof_failure")):
                reasons.append("process_group_proof_failure")
            if not _empty_or_zero(process.get("group_live_remaining")):
                reasons.append("process_group_live_remaining")
        if len(pids) != len(set(pids)):
            reasons.append("process_pid_duplicate")
        for lane in range(concurrency):
            if not lane_pids.get(lane):
                reasons.append("process_lane_missing:" + str(lane))
        reasons.extend(_resource_window_binding_reasons(sampler, environment, lane_pids))
        # A process report must correspond to the process tree the sampler
        # actually observed. This is not provenance, but it stops a report from
        # naming an unrelated pid while the samples describe another set.
        samples = sampler.get("samples")
        sample_pids = set()
        if isinstance(samples, list):
            for sample in samples:
                if isinstance(sample, dict) and isinstance(sample.get("pids"), list):
                    sample_pids.update(pid for pid in sample["pids"] if _is_int(pid))
        for pid in pids:
            if pid not in sample_pids:
                reasons.append("process_pid_not_sampled:" + str(pid))
    if resources.get("process_group_join_proof") is not True:
        reasons.append("group_join_proof_missing")
    if resources.get("all_processes_accounted") is not True:
        reasons.append("processes_not_accounted")
    return (NOT_RUN, reasons, lane_pids) if reasons else (PASS, [], lane_pids)


def _oracle_status(oracle):
    if not isinstance(oracle, dict):
        return NOT_RUN, ["oracle_missing"]
    if oracle.get("pass") is False:
        return FAIL, ["oracle_failed"]
    if oracle.get("pass") is not True:
        return NOT_RUN, ["oracle_pass_not_proven"]
    missing, failed = [], []
    for section in ("initial", "post_warmup", "final"):
        value = oracle.get(section)
        if not isinstance(value, dict):
            missing.append("oracle_" + section)
        elif value.get("pass") is False:
            failed.append("oracle_" + section + "_failed")
        elif value.get("pass") is not True:
            missing.append("oracle_" + section)
    if failed:
        return FAIL, failed
    if missing:
        return NOT_RUN, missing
    return PASS, []


def _non_negative_integer(value):
    return type(value) is int and value >= 0


def _validate_metrics_document(document):
    """Validate one worker exit snapshot against the documented schema.

    This is deliberately structural: it proves the file is the metrics export
    the runner asked for, not a placeholder. It does not re-derive counters,
    and it does not require group_phases aggregation (the worker reports it as
    unavailable by design).
    """
    if not isinstance(document, dict):
        return ["metrics_snapshot_not_object"], {}
    reasons = []
    if type(document.get("schema_version")) is not int or document.get("schema_version") != 1:
        reasons.append("metrics_snapshot_schema_version")
    if document.get("work_metrics_enabled") is not True:
        reasons.append("metrics_snapshot_work_metrics_disabled")
    generated = document.get("generated_at_unix_ms")
    if type(generated) is not int or not METRICS_GENERATED_AT_MIN_MS <= generated <= METRICS_GENERATED_AT_MAX_MS:
        reasons.append("metrics_snapshot_generated_at")
    process = document.get("process")
    if not isinstance(process, dict):
        reasons.append("metrics_snapshot_process_missing")
    elif not _non_negative_integer(process.get("pid")) or process["pid"] <= 0:
        reasons.append("metrics_snapshot_process_pid")
    phases = document.get("phases")
    if not isinstance(phases, list):
        reasons.append("metrics_snapshot_phases")
    else:
        names = []
        for index, phase in enumerate(phases):
            if not isinstance(phase, dict) or not isinstance(phase.get("name"), str) or not phase["name"]:
                reasons.append("metrics_snapshot_phase_entry:" + str(index))
                continue
            names.append(phase["name"])
            for field in ("count", "total_ns", "max_ns"):
                if not _non_negative_integer(phase.get(field)):
                    reasons.append("metrics_snapshot_phase_negative:" + phase["name"] + ":" + field)
        for name in sorted({value for value in names if names.count(value) > 1}):
            reasons.append("metrics_snapshot_phase_duplicate:" + name)
        for name in METRICS_PHASES:
            if name not in names:
                reasons.append("metrics_snapshot_phase_missing:" + name)
    counters = document.get("counters")
    values = {}
    if not isinstance(counters, list) or not counters:
        reasons.append("metrics_snapshot_counters")
    else:
        counter_names = []
        for index, counter in enumerate(counters):
            if not isinstance(counter, dict) or not isinstance(counter.get("name"), str) \
                    or not counter["name"] or not _non_negative_integer(counter.get("value")):
                name = counter.get("name") if isinstance(counter, dict) else None
                value = counter.get("value") if isinstance(counter, dict) else None
                if isinstance(name, str) and name and type(value) is int and value < 0:
                    reasons.append("metrics_snapshot_counter_negative:" + name)
                else:
                    reasons.append("metrics_snapshot_counter_entry:" + str(index))
                continue
            counter_names.append(counter["name"])
            values[counter["name"]] = counter["value"]
        for name in sorted({value for value in counter_names if counter_names.count(value) > 1}):
            reasons.append("metrics_snapshot_counter_duplicate:" + name)
        for name in METRICS_COUNTERS:
            if name not in values:
                reasons.append("metrics_snapshot_counter_missing:" + name)
    group_phases = document.get("group_phases")
    if not isinstance(group_phases, dict) or type(group_phases.get("available")) is not bool:
        reasons.append("metrics_snapshot_group_phases")
    return reasons, values


def _metrics_status(run_directory, environment, lanes=None, lane_pids=None):
    """Evidence decides attribution; the request flag is only informational.

    A snapshot must belong to this run and this lane: its process.pid has to be
    one of the worker pids recorded for the lane, and its generation time has
    to fall inside the run's wall-clock window, not before timed execution.
    """
    notes = [] if environment["metrics_snapshot_requested"] else ["metrics_snapshot_not_requested"]
    path = run_directory / "metrics-snapshot-index.json"
    if not path.is_file():
        return NOT_RUN, notes + ["metrics_snapshot_index_missing"], []
    try:
        index = evidence_module.read_json(path)
    except evidence_module.EvidenceError:
        return NOT_RUN, notes + ["metrics_snapshot_index_unreadable"], []
    snapshots = index.get("snapshots") if isinstance(index, dict) else None
    if not isinstance(snapshots, list):
        return NOT_RUN, notes + ["metrics_snapshot_index_schema"], []
    reasons = []
    for lane in range(environment["concurrency"]):
        matches = [item for item in snapshots
                   if isinstance(item, dict) and item.get("lane") == lane
                   and item.get("copied") is True]
        if not matches:
            reasons.append("metrics_snapshot_missing_lane:" + str(lane))
            continue
        item = matches[0]
        name = item.get("file")
        if not isinstance(name, str) or Path(name).name != name:
            reasons.append("metrics_snapshot_path")
            continue
        candidate = run_directory / name
        if not candidate.is_file():
            reasons.append("metrics_snapshot_file_missing")
            continue
        if item.get("sha256") != perf_contract.digest(candidate):
            reasons.append("metrics_snapshot_hash_mismatch")
            continue
        try:
            document = evidence_module.read_json(candidate)
        except evidence_module.EvidenceError:
            reasons.append("metrics_snapshot_unreadable:lane" + str(lane))
            continue
        document_reasons, counters = _validate_metrics_document(document)
        reasons.extend(reason + ":lane" + str(lane) for reason in document_reasons)
        process = document.get("process") if isinstance(document, dict) else None
        pid = process.get("pid") if isinstance(process, dict) else None
        if lane_pids is not None:
            expected_pids = lane_pids.get(lane) or set()
            if not expected_pids:
                reasons.append("metrics_snapshot_worker_pid_missing:lane" + str(lane))
            elif not _is_int(pid) or pid not in expected_pids:
                reasons.append("metrics_snapshot_worker_pid_mismatch:lane" + str(lane))
        generated = document.get("generated_at_unix_ms") if isinstance(document, dict) else None
        if _is_int(generated) and all(_is_int(environment[key]) for key in
                                      ("run_started_at_unix_ms", "timed_started_at_unix_ms",
                                       "run_completed_at_unix_ms")):
            tolerance = METRICS_WINDOW_TOLERANCE_MS
            if generated < environment["run_started_at_unix_ms"] - tolerance:
                reasons.append("metrics_snapshot_before_run_start:lane" + str(lane))
            if generated < environment["timed_started_at_unix_ms"]:
                reasons.append("metrics_snapshot_before_timed_start:lane" + str(lane))
            if generated > environment["run_completed_at_unix_ms"] + tolerance:
                reasons.append("metrics_snapshot_after_run_window:lane" + str(lane))
        # append_calls counts sync_wal calls that carried a pending batch. Each
        # ACK'd autocommit write in this harness performs one, and warmup calls
        # only add to the count, so timed writes are a safe lower bound.
        if lanes is not None and "work.wal_workload.append_calls" in counters:
            writes = len(lanes[lane]["buckets"]["write"]["ack_durations"])
            if counters["work.wal_workload.append_calls"] < writes:
                reasons.append("metrics_snapshot_wal_append_below_writes:lane" + str(lane))
    return (NOT_RUN, reasons + notes, snapshots) if reasons else (PASS, notes, snapshots)


def _denominator_problems(campaign, official):
    """The denominator is anchored to the frozen contract, never to campaign self-declaration."""
    problems = []
    expected = campaign.get("expected_denominator")
    if not isinstance(expected, dict):
        problems.append("expected_denominator_missing")
    else:
        for key in ("version", "commit"):
            if expected.get(key) != official[key]:
                problems.append("expected_denominator_" + key)
    labels = [label for label, arm in campaign["arms"].items() if arm.get("role") == "denominator"]
    if len(labels) != 1:
        problems.append("denominator_arm_count")
    for label in labels:
        for key in ("version", "commit"):
            if campaign["arms"][label].get(key) != official[key]:
                problems.append("denominator_arm_" + key + ":" + label)
    return problems


def _load_identity(evidence_root, campaign, contract_sha, workload_sha, profile_name):
    path = evidence_root / "campaign-identity.json"
    if not path.is_file():
        raise evidence_module.EvidenceError("campaign_identity_missing")
    identity = evidence_module.read_json(path)
    if not isinstance(identity, dict) or identity.get("schema") != 1:
        raise evidence_module.EvidenceError("campaign_identity_schema")
    if _failure_present(identity.get("failure")):
        raise evidence_module.EvidenceError("identity_failure")
    if identity.get("campaign_sha256") != campaign_module.campaign_identity(campaign):
        raise evidence_module.EvidenceError("campaign_identity_hash_mismatch")
    if identity.get("contract_sha256") != contract_sha:
        raise evidence_module.EvidenceError("identity_contract_hash_mismatch")
    if identity.get("workload_sha256") != workload_sha:
        raise evidence_module.EvidenceError("identity_workload_hash_mismatch")
    if identity.get("profile") != profile_name:
        raise evidence_module.EvidenceError("identity_profile_mismatch")
    identity_tier = identity.get("tier")
    if not isinstance(identity_tier, str) or identity_tier not in TIER_RANK:
        raise evidence_module.EvidenceError("identity_tier")
    source_before = identity.get("source_before_sha256")
    source_after = identity.get("source_after_sha256")
    if not _is_hex(source_before) or source_before != source_after \
            or identity.get("source_unchanged") is not True:
        raise evidence_module.EvidenceError("source_inventory_unstable")
    arms = identity.get("arms")
    if not isinstance(arms, dict) or set(arms) != set(campaign["arms"]):
        raise evidence_module.EvidenceError("identity_arms_schema")
    frozen = {}
    for label in sorted(campaign["arms"]):
        entry = arms.get(label)
        if not isinstance(entry, dict) or not _is_hex(entry.get("sha256")):
            raise evidence_module.EvidenceError("identity_arm_schema:" + label)
        if _failure_present(entry.get("failure")):
            raise evidence_module.EvidenceError("identity_arm_failure:" + label)
        directory = evidence_root / "artifacts" / label
        files = sorted(path for path in directory.rglob("*") if path.is_file()) \
            if directory.is_dir() else []
        if not files:
            raise evidence_module.EvidenceError("artifact_missing:" + label)
        digests = {perf_contract.digest(path): path for path in files}
        if entry["sha256"] not in digests:
            raise evidence_module.EvidenceError("artifact_hash_mismatch:" + label)
        frozen[label] = {"sha256": entry["sha256"], "file": str(digests[entry["sha256"]])}
    return {"source_sha256": source_before, "arms": frozen,
            "tier": identity_tier,
            "metrics_requested": identity.get("metrics_snapshot_requested") is True}


def _load_trusted_artifacts(path, evidence_root=None):
    if evidence_root is not None:
        trusted_path = _resolved(path)
        root = _resolved(evidence_root)
        if _is_within(trusted_path, root):
            raise evidence_module.EvidenceError("trusted_artifacts_inside_evidence")
    document = evidence_module.read_json(Path(path))
    if not isinstance(document, dict):
        raise evidence_module.EvidenceError("trusted_artifacts_schema")
    values = dict(document)
    if "schema" in values and values.pop("schema") != 1:
        raise evidence_module.EvidenceError("trusted_artifacts_schema")
    return values


def _validate_trusted_artifacts(trusted, campaign, identity, profile_name, evidence_root=None):
    """The trusted list is the external root: identity/artifact self-consistency
    alone cannot prove which binary produced a run."""
    if trusted is None:
        if profile_name == "formal":
            raise evidence_module.EvidenceError("trusted_artifacts_required")
        return
    values = _load_trusted_artifacts(trusted, evidence_root=evidence_root) \
        if isinstance(trusted, (str, Path)) else trusted
    if not isinstance(values, dict):
        raise evidence_module.EvidenceError("trusted_artifacts_schema")
    if set(values) != set(campaign["arms"]):
        raise evidence_module.EvidenceError("trusted_artifacts_arms")
    for label, expected in values.items():
        if not _is_hex(expected):
            raise evidence_module.EvidenceError("trusted_artifacts_sha256:" + label)
        if identity["arms"][label]["sha256"] != expected:
            raise evidence_module.EvidenceError("trusted_artifact_mismatch:" + label)


def _validate_run_identity(run, identity):
    environment = run["environment"]
    arm = run["arm"]
    if environment["binary_sha256"] != identity["arms"][arm]["sha256"]:
        raise evidence_module.EvidenceError("binary_sha256_artifact_mismatch: " + run["run_id"])
    if environment["source_inventory_sha256"] != identity["source_sha256"]:
        raise evidence_module.EvidenceError("source_inventory_sha256_mismatch: " + run["run_id"])


def _schedule_problems(lane_number, lane, environment, integrity):
    """Validate the frozen schedule in operations.jsonl file order.

    Comparing sorted sets would accept a permutation that preserves coverage but
    turns the intended round-robin access pattern into a hot-row batch.
    """
    rows = environment["rows"]
    for phase, offset in (("warmup", 0), ("timed", environment["warmup_operations"])):
        indices = lane["schedule_indices"][phase]
        expected = list(range(offset, offset + len(indices)))
        if indices != expected:
            _add(integrity, "schedule_index_gap:lane" + str(lane_number) + ":" + phase)
            continue
        if len(indices) >= rows:
            if {index % rows for index in indices} != set(range(rows)):
                _add(integrity, "row_coverage_incomplete:lane" + str(lane_number) + ":" + phase)
        if environment["case"] == "S1" and len(indices) >= rows:
            writes = {index % rows for index in lane["kind_indices"][phase]["write"]}
            if writes != set(range(rows)):
                _add(integrity, "s1_write_row_coverage:lane" + str(lane_number) + ":" + phase)
        if environment["case"] == "S2" and len(indices) >= 2 * rows:
            # The frozen S2 schedule reads even rows and writes odd rows; one
            # full round is 32 reads + 32 writes covering all 64 rows. Reading
            # or writing all rows per kind is not the contract.
            read_indices = lane["kind_indices"][phase]["read"]
            write_indices = lane["kind_indices"][phase]["write"]
            expected_reads = {row for row in range(rows)
                              if workload.operation_kind("S2", row) == "read"}
            expected_writes = {row for row in range(rows)
                               if workload.operation_kind("S2", row) == "write"}
            expected_read_count = sum(
                1 for index in range(offset, offset + len(indices))
                if workload.operation_kind("S2", index) == "read")
            if {index % rows for index in read_indices} != expected_reads \
                    or {index % rows for index in write_indices} != expected_writes \
                    or expected_reads | expected_writes != set(range(rows)) \
                    or len(read_indices) != expected_read_count \
                    or len(write_indices) != len(indices) - expected_read_count:
                _add(integrity, "s2_read_write_row_coverage:lane" + str(lane_number) + ":" + phase)


def load_run(run_directory, campaign, policy, contract_sha, workload_sha,
             evidence_root=None, allow_foreign_environment=False):
    run_directory = Path(run_directory)
    evidence_module.verify_manifest(run_directory)
    environment = evidence_module.read_json(run_directory / "environment.json")
    if not isinstance(environment, dict):
        raise evidence_module.EvidenceError("environment_not_object")
    foreign_environment = _validate_environment(
        environment, campaign, policy, run_directory, contract_sha, workload_sha,
        evidence_root=evidence_root, allow_foreign_environment=allow_foreign_environment)
    summary_path = run_directory / "summary.json"
    if summary_path.is_file():
        summary = evidence_module.read_json(summary_path)
        if isinstance(summary, dict) and _failure_present(summary.get("failure")):
            raise evidence_module.EvidenceError("run_summary_failure")
    operation_path = run_directory / "operations.jsonl"
    if not operation_path.is_file():
        raise evidence_module.EvidenceError("operations_missing")
    lanes = {lane: _empty_lane(environment["rows"], environment["payload_bytes"])
             for lane in range(environment["concurrency"])}
    sequences, integrity, correctness = {}, [], []
    timed_issued_min, timed_completed_max = None, None
    try:
        with operation_path.open("rb") as source:
            for line_number, line in enumerate(source, 1):
                if not line.strip():
                    _add(integrity, "blank_record_line")
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    _add(integrity, "record_json:" + str(line_number))
                    continue
                if not isinstance(record, dict):
                    _add(integrity, "record_not_object")
                    continue
                _validate_record(record, environment, policy, lanes, sequences, integrity, correctness)
                if record.get("phase") == "timed" and _is_int(record.get("issued_at")) \
                        and _is_int(record.get("completed_at")):
                    if timed_issued_min is None or record["issued_at"] < timed_issued_min:
                        timed_issued_min = record["issued_at"]
                    if timed_completed_max is None or record["completed_at"] > timed_completed_max:
                        timed_completed_max = record["completed_at"]
    except OSError as failure:
        raise evidence_module.EvidenceError("operations_unreadable: " + type(failure).__name__) from None
    shortage = []
    span = _run_span(environment)
    if span < policy["min_seconds"]:
        _add(shortage, "duration_below_minimum")
    # A campaign may tighten the contract floor but never relax it, and the
    # actual evidence must still satisfy the value the campaign declared.
    if span < environment["minimum_seconds"]:
        _add(shortage, "duration_below_declared_plan")
    if span > environment["max_seconds_per_arm"] + 5:
        _add(integrity, "timed_budget_overrun")
    if environment["budget_exhausted"]:
        _add(shortage, "arm_budget_exhausted")
    if timed_issued_min is None:
        _add(shortage, "no_timed_records")
    else:
        if timed_issued_min < environment["timed_started_at_ns"] or \
                timed_completed_max > environment["timed_completed_at_ns"]:
            _add(integrity, "record_outside_timed_window")
        actual_span = (timed_completed_max - timed_issued_min) / 1_000_000_000
        if span - actual_span > max(1.0, span * 0.1):
            _add(integrity, "timed_window_span_mismatch")
    for lane_number, lane in lanes.items():
        if lane["timed_sequences"] != list(range(len(lane["timed_sequences"]))):
            _add(integrity, "timed_sequence_gap:lane" + str(lane_number))
        if lane["warmup_sequences"] != list(range(len(lane["warmup_sequences"]))):
            _add(integrity, "warmup_sequence_gap:lane" + str(lane_number))
        # Warmup must finish before the lane's first timed operation starts,
        # independent of file order. Raw timestamps are self-declared, but an
        # internally inconsistent phase story is still unverifiable evidence.
        if lane["warmup_completed_at"] and lane["timed_issued_at"]:
            if max(lane["warmup_completed_at"]) > min(lane["timed_issued_at"]):
                _add(integrity, "warmup_completed_after_timed_start:lane" + str(lane_number))
            if max(lane["warmup_completed_at"]) > environment["timed_started_at_ns"]:
                _add(integrity, "warmup_completed_after_declared_start:lane" + str(lane_number))
        if any(later < earlier for earlier, later in
               zip(lane["timed_issued_at"], lane["timed_issued_at"][1:])):
            _add(integrity, "timed_record_time_regressed:lane" + str(lane_number))
        _schedule_problems(lane_number, lane, environment, integrity)
        if lane["warmup_attempts"] != policy["warmup_operations"]:
            _add(shortage, "warmup_incomplete:lane" + str(lane_number))
        if lane["attempts"] < environment["required_operations_per_lane"]:
            _add(shortage, "samples_below_declared_plan:lane" + str(lane_number))
        if lane["attempts"] < policy["min_samples_per_lane"]:
            _add(shortage, "samples_below_minimum:lane" + str(lane_number))
        if environment["case"] == "S1":
            if lane["buckets"]["write"]["attempts"] < policy["min_samples_per_lane"]:
                _add(shortage, "write_samples_below_minimum:lane" + str(lane_number))
        else:
            for kind in ("read", "write"):
                if lane["buckets"][kind]["attempts"] < policy["min_samples_per_lane"]:
                    _add(shortage, kind + "_samples_below_minimum:lane" + str(lane_number))
    resources_status, resources_reasons, lane_pids = _resources_status(
        evidence_module.read_json(run_directory / "resources.json"),
        environment["concurrency"], environment)
    oracle_document = evidence_module.read_json(run_directory / "oracle.json")
    oracle_status, oracle_reasons = _oracle_status(oracle_document)
    content_available, content_reasons = _oracle_content(oracle_document, lanes, environment)
    if not content_available:
        oracle_status = NOT_RUN
        oracle_reasons = oracle_reasons + content_reasons
    elif content_reasons:
        for reason in content_reasons:
            _add(correctness, reason)
    metrics_status, metrics_reasons, snapshots = _metrics_status(
        run_directory, environment, lanes, lane_pids)
    if oracle_status == FAIL:
        _add(correctness, "oracle_failed")
    lanes_metrics = {}
    aggregate = {"read": {"ack_durations": [], "attempt_durations": []},
                 "write": {"ack_durations": [], "attempt_durations": []}}
    for lane_number, lane in lanes.items():
        buckets = _buckets(environment["case"], lane)
        for name, value in buckets.items():
            value["ack_per_second"] = value["acks"] / span if span > 0 else None
            value["attempts_per_second"] = value["attempts"] / span if span > 0 else None
        for kind in ("read", "write"):
            aggregate[kind]["ack_durations"].extend(lane["buckets"][kind]["ack_durations"])
            aggregate[kind]["attempt_durations"].extend(lane["buckets"][kind]["attempt_durations"])
        lanes_metrics[lane_number] = {
            "attempts": lane["attempts"], "acks": lane["acks"], "errors": lane["errors"],
            "unknowns": lane["unknowns"], "mismatches": lane["mismatches"],
            "warmup_attempts": lane["warmup_attempts"], "warmup_mismatches": lane["warmup_mismatches"],
            "ack_per_second": lane["acks"] / span if span > 0 else None,
            "attempts_per_second": lane["attempts"] / span if span > 0 else None,
            "ack_types": lane["ack_types"], "buckets": buckets}
    combined = {}
    if environment["case"] == "S1":
        combined["write"] = {"attempts": sum(lane["buckets"]["write"]["attempts"] for lane in lanes.values()),
                             "ack_durations": aggregate["write"]["ack_durations"],
                             "attempt_durations": aggregate["write"]["attempt_durations"]}
        combined["all"] = combined["write"]
    else:
        combined["read"] = {"attempts": sum(lane["buckets"]["read"]["attempts"] for lane in lanes.values()),
                            "ack_durations": aggregate["read"]["ack_durations"],
                            "attempt_durations": aggregate["read"]["attempt_durations"]}
        combined["write"] = {"attempts": sum(lane["buckets"]["write"]["attempts"] for lane in lanes.values()),
                             "ack_durations": aggregate["write"]["ack_durations"],
                             "attempt_durations": aggregate["write"]["attempt_durations"]}
        combined["all"] = {"attempts": combined["read"]["attempts"] + combined["write"]["attempts"],
                           "ack_durations": combined["read"]["ack_durations"] + combined["write"]["ack_durations"],
                           "attempt_durations": combined["read"]["attempt_durations"]
                           + combined["write"]["attempt_durations"]}
    metrics = {name: {"attempts": value["attempts"], "acks": len(value["ack_durations"]),
                      "ack": _latency(value["ack_durations"]),
                      "attempt": _latency(value["attempt_durations"]),
                      "ack_per_second": len(value["ack_durations"]) / span if span > 0 else None,
                      "attempts_per_second": value["attempts"] / span if span > 0 else None}
               for name, value in sorted(combined.items())}
    return {"run_id": environment["run_id"], "directory": str(run_directory),
            "case": environment["case"], "concurrency": environment["concurrency"],
            "pair": environment["pair"], "arm": environment["arm"],
            "role": campaign["arms"][environment["arm"]]["role"],
            "arm_version": environment["arm_version"], "arm_commit": environment["arm_commit"],
            "foreign_environment": foreign_environment,
            "profile": environment["profile"], "environment": environment,
            "span_seconds": span, "budget_exhausted": environment["budget_exhausted"],
            "metrics": metrics, "lanes": lanes_metrics,
            "integrity": integrity, "correctness": correctness, "shortage": shortage,
            "resources_status": resources_status, "resources_reasons": resources_reasons,
            "oracle_status": oracle_status, "oracle_reasons": oracle_reasons,
            "metrics_snapshot_status": metrics_status, "metrics_snapshot_reasons": metrics_reasons,
            "metrics_snapshots": snapshots}


def _absolute_failures(run, thresholds, bucket):
    metric = run["metrics"].get(bucket)
    if metric is None:
        return ["bucket_missing:" + bucket]
    failures = []
    for percentile_key, threshold_key in (("p50", "p50_ms_max"), ("p95", "p95_ms_max"),
                                         ("p99", "p99_ms_max")):
        maximum = thresholds.get(threshold_key)
        if maximum is None:
            continue
        actual = metric["ack"][percentile_key + "_ms"]
        if actual is None or actual > maximum:
            failures.append(bucket + "_" + percentile_key)
    minimum = thresholds.get("ack_per_second_min")
    if minimum is not None and (metric["ack_per_second"] is None or metric["ack_per_second"] < minimum):
        failures.append(bucket + "_ack_per_second")
    return failures


def _absolute_status(run, thresholds, bucket):
    if run["integrity"]:
        return NOT_RUN
    if run["shortage"]:
        return INCOMPLETE
    return FAIL if _absolute_failures(run, thresholds, bucket) else PASS


def _pair_ratios(candidate, denominator, bucket):
    problems = []
    if candidate["integrity"] or denominator["integrity"]:
        return None, None, ["paired_evidence_integrity"]
    if candidate["shortage"] or denominator["shortage"]:
        return None, None, ["paired_sample_insufficient"]
    candidate_metric, denominator_metric = candidate["metrics"].get(bucket), denominator["metrics"].get(bucket)
    if candidate_metric is None or denominator_metric is None:
        return None, None, ["paired_bucket_missing"]
    p50_candidate = candidate_metric["ack"]["p50_ns"]
    p50_denominator = denominator_metric["ack"]["p50_ns"]
    throughput_candidate = candidate_metric["ack_per_second"]
    throughput_denominator = denominator_metric["ack_per_second"]
    if not p50_candidate or not p50_denominator or not throughput_candidate or not throughput_denominator:
        return None, None, ["paired_metric_missing"]
    return p50_denominator / p50_candidate, throughput_candidate / throughput_denominator, problems


def _threshold_config(acceptance, tier, case, concurrency):
    if tier == "G1" and (case, concurrency) != ("S1", 2):
        return None
    if tier == "G2":
        key = {"S1": "S1_C" + str(concurrency), "S2": "S2"}.get(case)
    else:
        key = "S1_C2"
    return acceptance["native"][tier].get(key)


def _evaluate(evidence_root, tier="G2", scope="native", profile_name=None, policy=None,
              require_attribution=False, allow_missing_attribution=False, acceptance=None,
              trusted_artifacts=None, allow_foreign_environment=False, strict_policy=True):
    if require_attribution and allow_missing_attribution:
        raise GateError("attribution_flags_conflict")
    evidence_root = Path(evidence_root)
    acceptance = acceptance or perf_contract.load_acceptance()
    reasons = []
    tier_mismatch = False
    tier_downgrade = False
    declared_tier = None
    effective_tier = tier
    phase_only = effective_tier != "G2"
    try:
        if strict_policy and acceptance != perf_contract.load_acceptance():
            raise perf_contract.ContractError("acceptance_injection_rejected")
        contract_sha = perf_contract.contract_identity()["sha256sums_sha256"]
        workload_sha = perf_contract.digest(perf_contract.CONTRACT_ROOT / "workloads.json")
        campaign_path = evidence_root / "campaign.json"
        if not campaign_path.is_file():
            raise campaign_module.CampaignError("campaign_evidence_missing")
        campaign = campaign_module.load_campaign(campaign_path)
        denominator_problems = _denominator_problems(campaign, perf_contract.official_denominator())
        if denominator_problems:
            raise campaign_module.CampaignError(
                "official_denominator_mismatch: " + ",".join(denominator_problems))
        run_directories = sorted(path for path in (evidence_root / "runs").glob("*") if path.is_dir())
        if not run_directories:
            raise evidence_module.EvidenceError("no_run_directories")
        if profile_name is None:
            environment = evidence_module.read_json(run_directories[0] / "environment.json")
            profile_name = environment.get("profile")
        frozen_policy = perf_contract.profile(profile_name, acceptance=acceptance)
        if policy is None:
            policy = frozen_policy
        elif strict_policy and policy != frozen_policy:
            # The production entry point must not accept a relaxed policy from
            # a caller. Tests that need small-sample arithmetic use the
            # explicitly named _evaluate_arithmetic_for_tests helper instead.
            raise perf_contract.ContractError(
                "policy_injection_rejected: policy differs from frozen profile " + str(profile_name))
        if profile_name not in {"quick", "formal"}:
            raise perf_contract.ContractError("gate_profile_unsupported: " + str(profile_name))
        if policy.get("name") != profile_name:
            raise perf_contract.ContractError("gate_policy_profile_mismatch")
        campaign_module.validate_profile(campaign, policy)
        identity = _load_identity(evidence_root, campaign, contract_sha, workload_sha,
                                  profile_name)
        campaign_tier = campaign.get("tier")
        declared_tiers = {value for value in (campaign_tier, identity["tier"]) if value is not None}
        tier_mismatch = len(declared_tiers) > 1
        declared_tier = _strictest_tier(campaign_tier, identity["tier"])
        if declared_tier is None:
            raise campaign_module.CampaignError("tier_declaration_missing")
        effective_tier = _strictest_tier(tier, declared_tier)
        phase_only = effective_tier != "G2"
        tier_downgrade = TIER_RANK[tier] < TIER_RANK[declared_tier]
        if tier_mismatch:
            reasons.append("tier_declaration_mismatch:" + ",".join(sorted(declared_tiers)))
        if tier_downgrade:
            reasons.append("tier_downgrade_rejected:requested=" + tier
                           + ":declared=" + declared_tier)
        # Coverage and thresholds always follow the strictest declared tier, so
        # runs a lower CLI tier would skip are still audited for integrity.
        campaign_module.validate_coverage(campaign, effective_tier)
        _validate_trusted_artifacts(trusted_artifacts, campaign, identity, profile_name,
                                    evidence_root=evidence_root)
    except (evidence_module.EvidenceError, perf_contract.ContractError,
            campaign_module.CampaignError, OSError, ValueError) as failure:
        failure_reasons = [type(failure).__name__ + ": " + str(failure)]
        if tier_mismatch:
            failure_reasons.append("tier_declaration_mismatch")
        if tier_downgrade:
            failure_reasons.append("tier_downgrade_rejected:requested=" + tier
                                   + ":declared=" + str(declared_tier))
        report = {"schema": 1, "status": NOT_RUN, "exit_code": 1,
                  "formal_pass": False, "tier": effective_tier,
                  "requested_tier": tier, "declared_tier": declared_tier,
                  "tier_mismatch": tier_mismatch,
                  "tier_downgrade_rejected": tier_downgrade,
                  "phase_only": effective_tier != "G2", "total_pass_tier": "G2",
                  "scope": scope, "profile": profile_name, "reasons": failure_reasons,
                  "sections": {}, "runs": {}}
        return report
    runs, evidence_errors = {}, []
    for run_directory in run_directories:
        try:
            run = load_run(run_directory, campaign, policy, contract_sha, workload_sha,
                           evidence_root=evidence_root,
                           allow_foreign_environment=allow_foreign_environment)
            _validate_run_identity(run, identity)
        except (evidence_module.EvidenceError, perf_contract.ContractError,
                campaign_module.CampaignError) as failure:
            evidence_errors.append({"directory": run_directory.name, "error": str(failure)})
            continue
        key = (run["case"], run["concurrency"], run["pair"], run["arm"])
        if key in runs:
            evidence_errors.append({"directory": run_directory.name, "error": "duplicate_run"})
            continue
        runs[key] = run
    if evidence_errors:
        _add(reasons, "evidence_errors")
    candidate_label = next(label for label, arm in campaign["arms"].items() if arm["role"] == "candidate")
    denominator_label = next(label for label, arm in campaign["arms"].items() if arm["role"] == "denominator")
    required_runs = campaign_module.REQUIRED_RUNS[effective_tier]
    native_checks, paired, paired_groups, required_objects, missing = [], [], [], [], []
    for case, concurrency in required_runs:
        thresholds = _threshold_config(acceptance, effective_tier, case, concurrency)
        bucket = "write" if case == "S1" else "all"
        ratios, problem_statuses = [], []
        for pair in range(policy["pairs"]):
            candidate = runs.get((case, concurrency, pair, candidate_label))
            denominator = runs.get((case, concurrency, pair, denominator_label))
            if candidate is None or denominator is None:
                missing.append(case + "/C" + str(concurrency) + "/pair" + str(pair))
                continue
            required_objects.extend([candidate, denominator])
            expected_order = policy["order"][pair]
            observed_orders = {candidate["environment"]["pair_order"],
                               denominator["environment"]["pair_order"]}
            if observed_orders != {expected_order}:
                _add(reasons, "pair_order_mismatch:pair" + str(pair))
                native_checks.append(NOT_RUN)
                continue
            pairing_fields = ("minimum_seconds", "max_seconds_per_arm", "warmup_operations",
                              "required_operations_per_lane", "rows", "payload_bytes")
            mismatched = [field for field in pairing_fields
                          if candidate["environment"][field] != denominator["environment"][field]]
            if mismatched:
                _add(reasons, "arm_plan_mismatch:" + ",".join(mismatched))
                native_checks.append(NOT_RUN)
                continue
            if candidate["environment"]["initial_state_sha256"] != denominator["environment"]["initial_state_sha256"]:
                _add(reasons, "initial_state_mismatch:pair" + str(pair))
                native_checks.append(NOT_RUN)
                continue
            if candidate["environment"]["post_warmup_state_sha256"] != denominator["environment"]["post_warmup_state_sha256"]:
                _add(reasons, "post_warmup_state_mismatch:pair" + str(pair))
                native_checks.append(NOT_RUN)
                continue
            native_checks.append(_absolute_status(candidate, thresholds or {}, bucket))
            if not candidate["shortage"] and not candidate["integrity"]:
                for failure in _absolute_failures(candidate, thresholds or {}, bucket):
                    _add(reasons, "candidate_absolute_failure:" + failure)
            p50_ratio, throughput_ratio, problems = _pair_ratios(candidate, denominator, bucket)
            bucket_ratios = {}
            for name in (("read", "write", "all") if case == "S2" else ("write", "all")):
                bucket_p50, bucket_throughput, bucket_problems = _pair_ratios(
                    candidate, denominator, name)
                bucket_ratios[name] = {"p50_speedup": bucket_p50,
                                       "ack_throughput_speedup": bucket_throughput,
                                       "problems": bucket_problems}
            if problems:
                problem_statuses.append(combine([
                    INCOMPLETE if problem == "paired_sample_insufficient" else NOT_RUN
                    for problem in problems]))
            ratios.append({"pair": pair, "p50_speedup": p50_ratio,
                           "ack_throughput_speedup": throughput_ratio,
                           "problems": problems, "buckets": bucket_ratios})
            paired.append({"case": case, "concurrency": concurrency, "pair": pair,
                           "bucket": bucket, "p50_speedup": p50_ratio,
                           "ack_throughput_speedup": throughput_ratio,
                           "buckets": bucket_ratios,
                           "problems": problems})
        p50_minimum = (thresholds or {}).get("paired_p50_speedup_min")
        throughput_minimum = (thresholds or {}).get("paired_ack_throughput_speedup_min")
        if case == "S2":
            throughput_minimum = acceptance["native"]["G2"]["S2"]["paired_ack_throughput_speedup_min"]
        p50_values = [ratio["p50_speedup"] for ratio in ratios if ratio["p50_speedup"] is not None]
        throughput_values = [ratio["ack_throughput_speedup"] for ratio in ratios
                             if ratio["ack_throughput_speedup"] is not None]
        median_p50 = statistics.median(p50_values) if len(p50_values) == len(ratios) and ratios else None
        median_throughput = (statistics.median(throughput_values)
                             if len(throughput_values) == len(ratios) and ratios else None)
        relative_checks = []
        if p50_minimum is not None or throughput_minimum is not None:
            if problem_statuses:
                relative_checks.extend(problem_statuses)
            elif not ratios or median_p50 is None or median_throughput is None:
                relative_checks.append(NOT_RUN)
            else:
                if p50_minimum is not None and median_p50 < p50_minimum:
                    _add(reasons, "paired_p50_speedup_below_minimum")
                    relative_checks.append(FAIL)
                if throughput_minimum is not None and median_throughput < throughput_minimum:
                    _add(reasons, "paired_ack_speedup_below_minimum")
                    relative_checks.append(FAIL)
                if not relative_checks:
                    relative_checks.append(PASS)
        native_checks.extend(relative_checks)
        paired_groups.append({"case": case, "concurrency": concurrency, "bucket": bucket,
                              "pair_count": len(ratios), "median_p50_speedup": median_p50,
                              "median_ack_throughput_speedup": median_throughput,
                              "p50_minimum": p50_minimum, "throughput_minimum": throughput_minimum,
                              "relative_status": combine(relative_checks) if relative_checks else PASS,
                              "ratios": ratios})
    run_statuses = []
    for run in required_objects:
        if run["integrity"]:
            run_statuses.append(NOT_RUN)
        elif run["correctness"]:
            run_statuses.append(FAIL)
        elif run["shortage"]:
            run_statuses.append(INCOMPLETE)
        else:
            run_statuses.append(PASS)
    native_reasons = []
    if missing:
        native_reasons.append("missing_runs: " + ",".join(missing))
    if evidence_errors:
        native_reasons.append("evidence_errors")
    native_reasons.extend(reasons)
    native_status = combine(native_checks + run_statuses + ([NOT_RUN] if missing else []))
    if evidence_errors:
        native_status = combine([native_status, NOT_RUN])
    correctness_statuses, correctness_reasons = [], []
    for run in required_objects:
        if run["correctness"] or run["oracle_status"] == FAIL:
            correctness_statuses.append(FAIL)
        elif run["oracle_status"] == NOT_RUN:
            correctness_statuses.append(NOT_RUN)
        else:
            correctness_statuses.append(PASS)
        correctness_reasons.extend(run["correctness"])
        correctness_reasons.extend(run["oracle_reasons"])
    if missing or not required_objects or evidence_errors:
        correctness_statuses.append(NOT_RUN)
    correctness_status = combine(correctness_statuses)
    resource_statuses, resource_reasons = [], []
    for run in required_objects:
        resource_statuses.append(run["resources_status"])
        resource_reasons.extend(run["resources_reasons"])
    if missing or not required_objects or evidence_errors:
        resource_statuses.append(NOT_RUN)
    resource_status = combine(resource_statuses)
    metrics_statuses = [run["metrics_snapshot_status"] for run in required_objects]
    requested = identity["metrics_requested"] or any(
        run["environment"]["metrics_snapshot_requested"] for run in required_objects)
    # Attribution is mandatory for every quick/formal campaign by default.
    # metrics_snapshot_requested only says what the runner expected; it is not
    # authorization to skip the evidence. The sole exception is the explicit
    # diagnostic waiver, which caps the overall status at PHASE_PASS.
    attribution_required = require_attribution or not allow_missing_attribution
    attribution_status = PASS if metrics_statuses and all(
        status == PASS for status in metrics_statuses) else NOT_RUN
    attribution_reasons = [reason for run in required_objects for reason in run["metrics_snapshot_reasons"]]
    if missing or not required_objects or evidence_errors:
        attribution_status = NOT_RUN
    if allow_missing_attribution:
        # A waiver never turns attribution into a pass, even when snapshots
        # happen to exist: the section was explicitly excluded from the gate.
        attribution_status = NOT_RUN
        attribution_reasons.append("attribution_waived")
    sections = {
        "native": {"status": native_status, "checks": native_checks, "reasons": native_reasons,
                   "paired": paired, "paired_groups": paired_groups},
        "correctness": {"status": correctness_status, "reasons": sorted(set(correctness_reasons))},
        "resources": {"status": resource_status, "reasons": sorted(set(resource_reasons))},
        "attribution": {"status": attribution_status, "required": attribution_required,
                        "requested": requested, "waived": allow_missing_attribution,
                        "reasons": sorted(set(attribution_reasons))},
    }
    considered = [native_status, correctness_status, resource_status]
    if attribution_required:
        considered.append(attribution_status)
    if scope == "full":
        sections["product"] = {"status": NOT_RUN, "reasons": ["product_scope_not_in_w0a"]}
        sections["s3_nonregression"] = {"status": NOT_RUN, "reasons": ["s3_scope_not_in_w0a"]}
        considered.extend([NOT_RUN, NOT_RUN])
    overall = combine(considered)
    # Broken raw evidence (ordering, schedule, manifests, environment,
    # resources) is not a performance failure: it is unverifiable evidence and
    # must dominate any downstream correctness FAIL with NOT_RUN. Integrity is
    # audited for every loaded run, not only the runs the requested tier uses.
    if evidence_errors or missing or not required_objects \
            or resource_status == NOT_RUN \
            or tier_mismatch or tier_downgrade \
            or any(run["integrity"] for run in runs.values()):
        overall = NOT_RUN
    foreign_environment = any(run["foreign_environment"] for run in required_objects) \
        if required_objects else False
    waivers = []
    if allow_missing_attribution:
        waivers.append({"section": "attribution", "status": attribution_status,
                        "reasons": sorted(set(attribution_reasons)),
                        "limit": "overall_capped_at_" + PHASE_PASS})
    if foreign_environment:
        waivers.append({"section": "environment", "status": PHASE_PASS,
                        "reasons": ["foreign_environment_allowed"],
                        "limit": "overall_capped_at_" + PHASE_PASS})
    if overall == PASS and (phase_only or allow_missing_attribution or foreign_environment
                            or not policy.get("total_pass")):
        overall = PHASE_PASS
    exit_code = status_exit_code(overall, acceptance.get("status_rules"))
    report = {"schema": 1, "status": overall, "exit_code": exit_code,
              "formal_pass": overall == PASS,
              "total_pass_tier": "G2", "phase_only": phase_only,
              "exit_code_basis": "acceptance.status_rules.any_not_pass_exit_nonzero",
              "evidence_root": str(evidence_root.resolve()), "tier": effective_tier,
              "requested_tier": tier, "declared_tier": declared_tier,
              "tier_mismatch": tier_mismatch, "tier_downgrade_rejected": tier_downgrade,
              "scope": scope,
              "profile": profile_name, "contract_sha256": contract_sha,
              "allow_foreign_environment": allow_foreign_environment,
              "foreign_environment": foreign_environment,
              "campaign_sha256": campaign_module.campaign_identity(campaign),
              "summary_ignored": True, "sections": sections, "waivers": waivers,
              "runs": {case + "/C" + str(concurrency) + "/pair" + str(pair) + "/" + arm:
                       {name: value for name, value in run.items()
                        if name not in {"environment", "metrics_snapshots"}}
                       for (case, concurrency, pair, arm), run in runs.items()},
              "paired": paired, "evidence_errors": evidence_errors}
    if overall == PHASE_PASS:
        report["status_note"] = "PHASE_PASS is not a formal PASS; the exit code is nonzero"
        if phase_only:
            report["status_note"] += (". G1 is a phase gate; only tier G2 can produce an "
                                      "overall PASS.")
    return report


def evaluate(evidence_root, tier="G2", scope="native", profile_name=None, policy=None,
             require_attribution=False, allow_missing_attribution=False, acceptance=None,
             trusted_artifacts=None, allow_foreign_environment=False):
    """Production entry point. ``policy`` may only echo the frozen profile."""
    return _evaluate(evidence_root, tier=tier, scope=scope, profile_name=profile_name,
                     policy=policy, require_attribution=require_attribution,
                     allow_missing_attribution=allow_missing_attribution,
                     acceptance=acceptance, trusted_artifacts=trusted_artifacts,
                     allow_foreign_environment=allow_foreign_environment,
                     strict_policy=True)


def _evaluate_arithmetic_for_tests(evidence_root, policy, tier="G2", scope="native",
                                   profile_name=None, require_attribution=False,
                                   allow_missing_attribution=False, acceptance=None,
                                   trusted_artifacts=None, allow_foreign_environment=False):
    """Test-only arithmetic harness for synthetic small-sample policies.

    Never use this from the CLI or production callers: it bypasses the frozen
    policy check by design so threshold arithmetic can be exercised cheaply.
    """
    return _evaluate(evidence_root, tier=tier, scope=scope, profile_name=profile_name,
                     policy=policy, require_attribution=require_attribution,
                     allow_missing_attribution=allow_missing_attribution,
                     acceptance=acceptance, trusted_artifacts=trusted_artifacts,
                     allow_foreign_environment=allow_foreign_environment,
                     strict_policy=False)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-root", type=Path, required=True)
    parser.add_argument("--tier", choices=["G1", "G2"], default="G2")
    parser.add_argument("--scope", choices=["native", "full"], default="native")
    parser.add_argument("--profile", choices=["quick", "formal"])
    attribution = parser.add_mutually_exclusive_group()
    attribution.add_argument("--require-attribution", action="store_true",
                             help="explicitly require attribution (the default for quick/formal)")
    attribution.add_argument("--allow-missing-attribution", action="store_true",
                             help="diagnostic exception; attribution stays NOT_RUN and the "
                                  "overall status is capped at PHASE_PASS, never PASS")
    parser.add_argument("--trusted-artifacts", type=Path,
                        help="external operator trust root (JSON object arm->sha256); must live "
                             "outside the evidence root and is required for a formal PASS. The "
                             "gate can prove internal consistency and path separation, not that "
                             "the list itself is trustworthy: a wrong binary plus a wrong "
                             "external list remains a residual trust boundary.")
    parser.add_argument("--allow-foreign-environment", action="store_true",
                        help="accept evidence recorded on another os/arch for offline review; "
                             "the overall status is capped at PHASE_PASS, never PASS")
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args(argv)
    try:
        report = evaluate(arguments.evidence_root, tier=arguments.tier, scope=arguments.scope,
                          profile_name=arguments.profile,
                          require_attribution=arguments.require_attribution,
                          allow_missing_attribution=arguments.allow_missing_attribution,
                          trusted_artifacts=arguments.trusted_artifacts,
                          allow_foreign_environment=arguments.allow_foreign_environment)
    except (evidence_module.EvidenceError, perf_contract.ContractError,
            campaign_module.CampaignError, OSError, ValueError) as failure:
        report = {"schema": 1, "status": NOT_RUN, "exit_code": 1,
                  "reasons": [type(failure).__name__ + ": " + str(failure)], "sections": {}}
    if arguments.output:
        try:
            evidence_module.write_json(arguments.output, report)
        except OSError as failure:
            print(json.dumps({"status": NOT_RUN, "error": type(failure).__name__}))
            return 1
    summary = {"status": report["status"], "exit_code": report["exit_code"],
               "formal_pass": report["status"] == PASS,
               "output": str(arguments.output) if arguments.output else None}
    if report["status"] == PHASE_PASS:
        summary["status_note"] = "PHASE_PASS is not a formal PASS; the exit code is nonzero"
        if report.get("phase_only"):
            summary["status_note"] += (". G1 is a phase gate; only tier G2 can produce an "
                                       "overall PASS.")
    if report.get("waivers"):
        summary["waivers"] = report["waivers"]
    print(json.dumps(summary))
    return report["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
