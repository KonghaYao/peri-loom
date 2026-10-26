"""Issue11 gate_checks；按证据职责隔离。"""
from pathlib import Path
from perf_contract import digest
from issue11 import evidence_io
from issue11.attribution import aggregate
from issue11.gate_common import REQUIRED_RUN, OUTCOMES
from issue11.gate_common import GateError, _read_json, _read_jsonl, _is_int, _is_number, _percentile

def _verify_run_hashes(run_directory, problems):
    sums_path = Path(run_directory) / "SHA256SUMS.json"
    if not sums_path.is_file():
        problems.append(("FAIL", "evidence_sha256sums_missing"))
        return
    try:
        sums = _read_json(sums_path)
    except GateError as failure:
        problems.append(("FAIL", "evidence_sha256sums_unreadable: "
                         + str(failure)))
        return
    if not isinstance(sums, dict) or set(sums) != set(REQUIRED_RUN):
        problems.append(("FAIL", "evidence_sha256sums_file_set"))
        return
    for name, expected in sums.items():
        path = Path(run_directory) / name
        if not path.is_file():
            problems.append(("FAIL", "evidence_artifact_missing: " + name))
            continue
        if digest(path) != expected:
            problems.append(("FAIL", "evidence_hash_mismatch: " + name))
    manifest_path = Path(run_directory) / "MANIFEST.json"
    if not manifest_path.is_file():
        problems.append(("FAIL", "evidence_manifest_missing"))
        return
    try:
        manifest = _read_json(manifest_path)
    except GateError as failure:
        problems.append(("FAIL", "evidence_manifest_unreadable: "
                         + str(failure)))
        return
    if manifest.get("schema") != 1 or manifest.get("integrity_only") is not True:
        problems.append(("FAIL", "evidence_manifest_schema"))
    if manifest.get("sha256sums_sha256") != digest(sums_path):
        problems.append(("FAIL", "evidence_manifest_sha256sums_mismatch"))


def _load_run(run_directory, run_id, campaign_record, workloads):
    run_directory = Path(run_directory)
    problems = []
    missing = evidence_io.verify_required(run_directory)
    if missing:
        severity = "FAIL" if "run.json" in missing else "NOT_RUN"
        return None, [(severity, "run_artifact_missing: " + ",".join(missing))]
    _verify_run_hashes(run_directory, problems)
    view = {
        "run_id": run_id,
        "path": str(run_directory),
        "record": campaign_record or {},
        "artifacts": {},
    }
    try:
        view["environment"] = _read_json(run_directory / "environment.json")
        metadata = _read_json(run_directory / "run.json")
        view["metadata"] = metadata
        required = ("scenario", "arm", "pair_index", "state", "profile")
        if any(key not in metadata for key in required):
            problems.append(("FAIL", "run_metadata_missing"))
        if metadata.get("arm") not in {"A", "B"} \
                or metadata.get("state") not in {"fresh", "aged"} \
                or metadata.get("profile") not in {"quick", "formal"} \
                or not _is_int(metadata.get("pair_index")) or metadata.get("pair_index", -1) < 0:
            problems.append(("FAIL", "run_metadata_invalid"))
        if campaign_record:
            if any(metadata.get(key) != campaign_record.get(key) for key in required):
                problems.append(("FAIL", "run_metadata_ledger_mismatch"))
        else:
            view["record"] = metadata
        view["operations"] = _read_jsonl(run_directory / "operations.jsonl")
        view["requests"] = _read_jsonl(run_directory / "requests.jsonl")
        view["attribution"] = _read_jsonl(run_directory / "request-attribution.jsonl")
        view["periodic"] = _read_jsonl(run_directory / "periodic-events.jsonl")
        view["metrics_index"] = _read_json(run_directory / "metrics-snapshot-index.json")
        view["resources"] = _read_json(run_directory / "resources.json")
        view["oracle"] = _read_json(run_directory / "oracle.json")
    except GateError as failure:
        return None, [("FAIL", "run_artifact_unreadable: " + str(failure))]
    return view, problems


def _validate_requests(records, environment, problems):
    by_id = {}
    first_connection = {}
    order = []
    for index, record in enumerate(records):
        where = "request_line:" + str(index + 1)
        request_id = record.get("request_id")
        if not isinstance(request_id, str) or not request_id:
            problems.append(("FAIL", "request_id: " + where))
            continue
        if request_id in by_id:
            problems.append(("FAIL", "duplicate_request_id: " + request_id))
            continue
        if record.get("schema_version") != 1 \
                or record.get("record_type") != "http_request":
            problems.append(("FAIL", "request_schema: " + request_id))
        if not isinstance(record.get("pipeline_correlation_id"), str) \
                or not record["pipeline_correlation_id"]:
            problems.append(("FAIL", "missing_correlation: " + request_id))
        step_ids = record.get("step_ids")
        if not isinstance(step_ids, list) or not step_ids \
                or any(not isinstance(item, str) or not item for item in step_ids):
            problems.append(("FAIL", "request_step_ids: " + request_id))
        if not _is_int(record.get("connection_id")) or record["connection_id"] < 1:
            problems.append(("FAIL", "request_connection_id: " + request_id))
        if not _is_int(record.get("connections_created")) \
                or record["connections_created"] < 1:
            problems.append(("FAIL", "request_connections_created: " + request_id))
        if not isinstance(record.get("reused_connection"), bool):
            problems.append(("FAIL", "request_reused: " + request_id))
        if not _is_int(record.get("request_bytes")) or record["request_bytes"] <= 0:
            problems.append(("FAIL", "request_bytes: " + request_id))
        if not _is_int(record.get("response_bytes")) or record["response_bytes"] < 0:
            problems.append(("FAIL", "response_bytes: " + request_id))
        for key in ("response_complete", "response_connection_close",
                    "connection_reusable_after", "close_acknowledged", "tls",
                    "close_step"):
            if not isinstance(record.get(key), bool):
                problems.append(("FAIL", "request_flag_" + key + ": " + request_id))
        if not isinstance(record.get("baton_in"), str):
            problems.append(("FAIL", "request_baton_in: " + request_id))
        if record.get("baton_out") is not None \
                and not isinstance(record.get("baton_out"), str):
            problems.append(("FAIL", "request_baton_out: " + request_id))
        if record.get("outcome") not in OUTCOMES:
            problems.append(("FAIL", "request_outcome: " + request_id))
        started = record.get("started_ns")
        completed = record.get("completed_ns")
        duration = record.get("duration_ns")
        if not all(_is_int(value) for value in (started, completed, duration)) \
                or started < 0 or completed < started or duration != completed - started:
            problems.append(("FAIL", "request_duration: " + request_id))
        status = record.get("status")
        if status is not None and not _is_int(status):
            problems.append(("FAIL", "request_status: " + request_id))
        if record.get("outcome") in {"success", "error"}:
            if status != 200 or record.get("response_complete") is not True:
                problems.append(("FAIL", "request_success_not_complete: " + request_id))
        if record.get("outcome") == "rejected":
            if not _is_int(status) or not 400 <= status < 500:
                problems.append(("FAIL", "request_rejected_status: " + request_id))
            if record.get("response_complete") is not True:
                problems.append(("FAIL", "request_rejected_body_incomplete: " + request_id))
        if record.get("outcome") == "outcome_unknown":
            if record.get("response_complete") is True and status == 200:
                # A 200 with an undecodable body is still an unknown outcome;
                # it must carry an error code rather than look successful.
                if not record.get("error_code"):
                    problems.append(("FAIL", "request_unknown_without_code: " + request_id))
        if record.get("close_acknowledged"):
            if record.get("close_step") is not True or status != 200:
                problems.append(("FAIL", "close_ack_without_close_step: " + request_id))
        if record.get("response_connection_close") \
                and record.get("connection_reusable_after"):
            problems.append(("FAIL", "connection_close_and_reusable: " + request_id))
        if isinstance(record.get("tls"), bool) and isinstance(environment, dict):
            tls = environment.get("tls", {})
            if isinstance(tls, dict) and record["tls"] != bool(tls.get("enabled")):
                problems.append(("FAIL", "request_tls_mismatch: " + request_id))
        headers = record.get("request_headers_redacted")
        if not isinstance(headers, dict) \
                or headers.get("Authorization") != "Bearer <REDACTED>":
            problems.append(("FAIL", "authorization_not_redacted: " + request_id))
        connection_id = record.get("connection_id")
        if _is_int(connection_id) and connection_id >= 1:
            if connection_id not in first_connection:
                first_connection[connection_id] = len(order)
                if record.get("reused_connection") is not False:
                    problems.append(("FAIL", "first_request_claims_reuse: "
                                     + request_id))
                if _is_int(record.get("connections_created")) \
                        and record["connections_created"] < connection_id:
                    problems.append(("FAIL", "connection_count_order: " + request_id))
            else:
                if record.get("reused_connection") is not True:
                    problems.append(("FAIL", "later_request_claims_new_connection: "
                                     + request_id))
        by_id[request_id] = record
        order.append(request_id)
    return by_id, order


def _validate_operations(records, requests, workloads, run_scenario, problems):
    by_id = {}
    referenced = {}
    warmup_indices = set()
    timed_indices = set()
    warmup_issued = []
    timed_issued = []
    for index, record in enumerate(records):
        where = "operation_line:" + str(index + 1)
        if record.get("schema_version") != 1 \
                or record.get("record_type") != "operation":
            problems.append(("FAIL", "operation_schema: " + where))
        operation_id = record.get("op_id")
        if not isinstance(operation_id, str) or not operation_id:
            problems.append(("FAIL", "operation_id: " + where))
            continue
        if operation_id in by_id:
            problems.append(("FAIL", "duplicate_operation_id: " + operation_id))
            continue
        by_id[operation_id] = record
        scenario = record.get("scenario")
        if scenario != run_scenario or scenario not in workloads["scenarios"]:
            problems.append(("FAIL", "operation_scenario: " + operation_id))
        phase = record.get("phase")
        if phase not in {"warmup", "timed"}:
            problems.append(("FAIL", "operation_phase: " + operation_id))
        sample_index = record.get("sample_index")
        if not _is_int(sample_index) or sample_index < 0:
            problems.append(("FAIL", "operation_sample_index: " + operation_id))
        elif phase == "warmup":
            warmup_indices.add(sample_index)
            warmup_issued.append(record.get("issued_ns"))
        elif phase == "timed":
            timed_indices.add(sample_index)
            timed_issued.append(record.get("issued_ns"))
        started = record.get("issued_ns")
        completed = record.get("completed_ns")
        duration = record.get("duration_ns")
        if not all(_is_int(value) for value in (started, completed, duration)) \
                or started < 0 or completed < started or duration != completed - started:
            problems.append(("FAIL", "operation_duration: " + operation_id))
        if record.get("retry_count") != 0:
            problems.append(("FAIL", "operation_retry: " + operation_id))
        outcome = record.get("outcome")
        if outcome not in OUTCOMES:
            problems.append(("FAIL", "operation_outcome: " + operation_id))
        verification = record.get("verification")
        if not isinstance(verification, dict) \
                or not isinstance(verification.get("ok"), bool):
            problems.append(("FAIL", "operation_verification: " + operation_id))
            verification = {}
        request_ids = record.get("http_request_ids")
        correlations = record.get("pipeline_correlation_ids")
        step_ids = record.get("step_ids")
        if not isinstance(request_ids, list) or not request_ids \
                or any(not isinstance(item, str) or not item for item in request_ids):
            problems.append(("FAIL", "operation_request_ids: " + operation_id))
            request_ids = []
        if len(set(request_ids)) != len(request_ids):
            problems.append(("FAIL", "operation_duplicate_request_ref: " + operation_id))
        if not isinstance(correlations, list) or len(correlations) != len(request_ids):
            problems.append(("FAIL", "operation_correlations: " + operation_id))
            correlations = []
        if not isinstance(step_ids, list) or not step_ids:
            problems.append(("FAIL", "operation_step_ids: " + operation_id))
            step_ids = []
        expected_correlations = []
        expected_steps = []
        request_outcomes = []
        for request_id in request_ids:
            request = requests.get(request_id)
            if request is None:
                problems.append(("FAIL", "unknown_request_id: " + request_id))
                continue
            if request.get("scenario") != scenario or request.get("phase") != phase \
                    or request.get("sample_index") != sample_index:
                problems.append(("FAIL", "operation_request_metadata_mismatch: " + operation_id))
            if request_id in referenced:
                problems.append(("FAIL", "request_reused_by_operations: " + request_id))
            referenced[request_id] = operation_id
            expected_correlations.append(request.get("pipeline_correlation_id"))
            expected_steps.extend(request.get("step_ids", []))
            request_outcomes.append(request.get("outcome"))
        if correlations != expected_correlations:
            problems.append(("FAIL", "cross_request_stitch: " + operation_id))
        if step_ids != expected_steps:
            problems.append(("FAIL", "operation_step_ids_mismatch: " + operation_id))
        expected_rejections = record.get("expected_rejections", 0)
        if not _is_int(expected_rejections) or expected_rejections < 0:
            problems.append(("FAIL", "operation_expected_rejections: " + operation_id))
            expected_rejections = 0
        rejected_ids = [request_id for request_id in request_ids
                        if requests.get(request_id, {}).get("outcome") == "rejected"]
        if len(rejected_ids) != expected_rejections:
            problems.append(("FAIL", "operation_rejection_count: " + operation_id))
        for request_id in rejected_ids:
            if requests[request_id].get("status") != 403:
                problems.append(("FAIL", "expected_rejection_status: " + request_id))
        if outcome == "success":
            if not verification.get("ok"):
                problems.append(("FAIL", "success_with_failed_verification: "
                                 + operation_id))
            non_success = [value for value in request_outcomes if value != "success"]
            if len(non_success) != expected_rejections \
                    or any(value != "rejected" for value in non_success):
                problems.append(("FAIL", "success_with_non_success_request: "
                                 + operation_id))
            if record.get("may_have_committed") is not False:
                problems.append(("FAIL", "success_with_commit_risk: " + operation_id))
        elif outcome == "rejected":
            if record.get("dispatched") is not False \
                    or record.get("may_have_committed") is not False:
                problems.append(("FAIL", "rejected_not_pre_dispatch: " + operation_id))
            if "rejected" not in request_outcomes:
                problems.append(("FAIL", "rejected_without_rejected_request: "
                                 + operation_id))
        elif outcome == "outcome_unknown":
            terminal = record.get("terminal")
            if not isinstance(terminal, dict) or terminal.get("recorded") is not True \
                    or terminal.get("kind") != "outcome_unknown" \
                    or terminal.get("no_retry") is not True:
                problems.append(("FAIL", "unknown_without_terminal_record: "
                                 + operation_id))
            if not isinstance(record.get("may_have_committed"), bool):
                problems.append(("FAIL", "unknown_without_commit_risk: "
                                 + operation_id))
            if "outcome_unknown" not in request_outcomes:
                problems.append(("FAIL", "unknown_without_unknown_request: "
                                 + operation_id))
        if outcome == "success":
            for request_id in request_ids:
                request = requests.get(request_id)
                if request is None:
                    continue
                if request.get("close_step") \
                        and not request.get("close_acknowledged"):
                    problems.append(("FAIL", "close_unconfirmed_counted_success: "
                                     + operation_id))
            workflow = workloads["scenarios"].get(scenario, {}).get("workflow")
            if workflow in {"churn_read_close", "churn_insert_read_close"} and phase == "timed":
                closed = [requests[request_id] for request_id in request_ids
                          if requests.get(request_id, {}).get("close_step")]
                if not closed or not all(item.get("close_acknowledged")
                                         for item in closed):
                    problems.append(("FAIL", "close_not_acknowledged: "
                                     + operation_id))
    if run_scenario != "H4":
        for phase in ("warmup", "timed"):
            indices = [record.get("sample_index") for record in records
                       if record.get("phase") == phase]
            if len(indices) != len(set(indices)):
                problems.append(("FAIL", "duplicate_sample_index: " + phase))
    if run_scenario == "H4":
        groups = {}
        for operation in records:
            groups.setdefault((operation.get("phase"), operation.get("sample_index")), []).append(operation)
        for key, members in groups.items():
            if all(member.get("outcome") == "success" for member in members) and (
                    len(members) != 2 or {member.get("action") for member in members} != {"insert", "select"}
                    or any(len(member.get("http_request_ids", [])) != 1 for member in members)):
                problems.append(("FAIL", "h4_actions_not_independent: " + str(key)))
    if warmup_indices and warmup_indices != set(range(len(warmup_indices))):
        problems.append(("FAIL", "warmup_indices_not_contiguous"))
    if timed_indices and timed_indices != set(range(len(timed_indices))):
        problems.append(("FAIL", "timed_indices_not_contiguous"))
    if warmup_issued and timed_issued:
        if max(warmup_issued) >= min(timed_issued):
            problems.append(("FAIL", "warmup_after_timed"))
    if any(not _is_int(value) for value in timed_issued):
        problems.append(("FAIL", "timed_issued_not_integer"))
    elif timed_issued != sorted(timed_issued):
        problems.append(("FAIL", "timed_issued_decreasing"))
    for request_id, request in requests.items():
        if request.get("phase") in {"setup", "cleanup"}:
            if request.get("outcome") != "success" or not request.get("close_acknowledged"):
                problems.append(("FAIL", "lifecycle_close_not_acknowledged: " + request_id))
            continue
        if request_id not in referenced:
            problems.append(("FAIL", "orphan_request: " + request_id))
    return by_id, referenced


def _validate_attribution(records, requests, problems):
    try:
        documents = aggregate.load_records(records)
    except aggregate.AttributionError as failure:
        problems.append(("FAIL", "attribution_invalid: " + str(failure)))
        return None
    by_request = {}
    for document in documents:
        request_id = document["request_id"]
        request = requests.get(request_id)
        if request is None:
            problems.append(("FAIL", "attribution_unknown_request: " + request_id))
            continue
        if document["pipeline_correlation_id"] != request.get("pipeline_correlation_id"):
            problems.append(("FAIL", "attribution_correlation_mismatch: " + request_id))
        if document["phase"] != request.get("phase"):
            problems.append(("FAIL", "attribution_phase_mismatch: " + request_id))
        if document["scenario"] != request.get("scenario"):
            problems.append(("FAIL", "attribution_scenario_mismatch: " + request_id))
        if document.get("step_ids") != request.get("step_ids"):
            problems.append(("FAIL", "attribution_step_mismatch: " + request_id))
        if document.get("outcome") != request.get("outcome"):
            problems.append(("FAIL", "attribution_outcome_mismatch: " + request_id))
        if document["total_ns"] < request.get("duration_ns", 0):
            problems.append(("FAIL", "attribution_total_below_transport: " + request_id))
        request["client_total_ns"] = document["total_ns"]
        by_request[request_id] = document
    for request_id in requests:
        if request_id not in by_request:
            problems.append(("FAIL", "missing_attribution: " + request_id))
    try:
        result = aggregate.aggregate(documents)
    except aggregate.AttributionError as failure:
        problems.append(("FAIL", "attribution_invalid: " + str(failure)))
        return None
    if not result["unattributed"]["threshold_met"]:
        problems.append(("FAIL", "unattributed_over_10_percent"))
    result["server_evidence"] = any(
        document["server_evidence"] for document in documents)
    return result


def _scenario_operations(operations):
    if not operations or operations[0].get("scenario") != "H4":
        return operations
    grouped = {}
    for record in operations:
        grouped.setdefault((record.get("phase"), record.get("sample_index")), []).append(record)
    result = []
    for members in grouped.values():
        combined = dict(members[0])
        combined["issued_ns"] = min(item["issued_ns"] for item in members)
        combined["completed_ns"] = max(item["completed_ns"] for item in members)
        combined["duration_ns"] = combined["completed_ns"] - combined["issued_ns"]
        combined["http_request_ids"] = [key for item in members for key in item["http_request_ids"]]
        for outcome in ("outcome_unknown", "error", "rejected", "success"):
            if any(item["outcome"] == outcome for item in members):
                combined["outcome"] = outcome
                break
        result.append(combined)
    return result


def _latency_stats(operations, requests):
    raw_operations = operations
    operations = _scenario_operations(operations)
    timed = [record for record in operations if record.get("phase") == "timed"]
    success = [record for record in timed if record.get("outcome") == "success"]
    durations = [record["duration_ns"] for record in timed]
    success_durations = [record["duration_ns"] for record in success]
    unknown = sum(1 for record in timed if record.get("outcome") == "outcome_unknown")
    failure = sum(1 for record in timed if record.get("outcome") == "error")
    rejected = sum(1 for record in timed if record.get("outcome") == "rejected")
    referenced = []
    for record in timed:
        for request_id in record.get("http_request_ids", []):
            request = requests.get(request_id)
            if request is not None:
                referenced.append(request)
    connections = max([request.get("connections_created", 0)
                       for request in referenced] or [0])
    request_count = len(referenced)
    started = min((record["issued_ns"] for record in timed), default=0)
    completed = max((record["completed_ns"] for record in timed), default=0)
    seconds = max(0.001, (completed - started) / 1e9)
    result = {
        "action_latency": {action: {"p%d_ms" % q: _percentile(
            [record["duration_ns"] for record in raw_operations
             if record.get("action") == action and record.get("phase") == "timed"], q / 100) / 1e6
            for q in (50, 95, 99)} for action in ("insert", "select")}
        if raw_operations and raw_operations[0].get("scenario") == "H4" else {},
        "timed_count": len(timed),
        "window_ns": max(0, completed - started),
        "success": len(success),
        "success_latency": {"p%d_ms" % q: _percentile(success_durations, q / 100) / 1e6
                            for q in (50, 95, 99)},
        "all_request_latency": {"p%d_ms" % q: _percentile(
            [request.get("client_total_ns", request["duration_ns"]) for request in requests.values()], q / 100) / 1e6
                                for q in (50, 95, 99)},
        "successful_request_latency": {"p%d_ms" % q: _percentile(
            [request.get("client_total_ns", request["duration_ns"]) for request in requests.values()
             if request.get("outcome") == "success"], q / 100) / 1e6 for q in (50, 95, 99)},
        "recorded_request_counts": {outcome: sum(request.get("outcome") == outcome
                                                 for request in requests.values())
                                    for outcome in OUTCOMES},
        "rejection_ratio": rejected / len(timed) if timed else 0,
        "failure": failure,
        "rejected": rejected,
        "outcome_unknown": unknown,
        "p50_ms": _percentile(durations, 0.50) / 1e6,
        "p95_ms": _percentile(durations, 0.95) / 1e6,
        "p99_ms": _percentile(durations, 0.99) / 1e6,
        "max_ms": (max(durations) / 1e6) if durations else 0.0,
        "over_100ms_ratio": (sum(1 for value in durations if value > 100_000_000)
                             / len(durations)) if durations else 0.0,
        "goodput_per_second": len(success) / seconds,
        "http_requests": request_count,
        "connections_created": connections,
        "connection_reuse_ratio": (
            (request_count - connections) / request_count if request_count else 0.0),
    }
    return result


def _target_problems(scenario_id, stats, targets, problems):
    target = targets.get(scenario_id)
    if not isinstance(target, dict) or not target.get("gate_enforced"):
        return
    if _is_number(target.get("p99_ms_exclusive_max")) \
            and stats["p99_ms"] >= target["p99_ms_exclusive_max"]:
        problems.append(("FAIL", "target_p99_ms_exclusive: " + scenario_id))
    for key in ("p50_ms", "p95_ms", "p99_ms"):
        maximum = target.get(key + "_max")
        if _is_number(maximum) and stats[key] > maximum:
            problems.append(("FAIL", "target_" + key + ": " + scenario_id))
    maximum = target.get("over_100ms_ratio_max")
    if _is_number(maximum) and stats["over_100ms_ratio"] > maximum:
        problems.append(("FAIL", "target_over_100ms: " + scenario_id))
    maximum = target.get("outcome_unknown_max")
    if _is_int(maximum) and stats["outcome_unknown"] > maximum:
        problems.append(("FAIL", "target_unknown: " + scenario_id))
    maximum = target.get("failure_max")
    if _is_int(maximum) and stats["failure"] > maximum:
        problems.append(("FAIL", "target_failure: " + scenario_id))
