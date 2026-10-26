"""Issue11 gate_observability；按证据职责隔离。"""
from pathlib import Path
from perf_contract import digest
from issue11 import contract, environment as environment_module
from issue11.gate_pairing import synthetic
from issue11.gate_common import PASS, FAIL, PERIODIC_KINDS, METRIC_COUNTERS, PROCESS_NAMES
from issue11.gate_common import GateError, _read_json, _is_int, _is_number

def _validate_periodic(records, problems):
    events = [record for record in records
              if record.get("record_type") == "periodic_event"]
    availability = [record for record in records
                    if record.get("record_type") == "availability"]
    if not events:
        return False, ["periodic_events_unavailable"]
    pair_ids = {}
    kinds = set()
    for event in events:
        if event.get("schema_version") != 1:
            problems.append(("FAIL", "periodic_schema"))
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or not event_id:
            problems.append(("FAIL", "periodic_event_id"))
        if event.get("process") not in PROCESS_NAMES - {"client"}:
            problems.append(("FAIL", "periodic_process"))
        if not isinstance(event.get("clock_id"), str) or not event["clock_id"]:
            problems.append(("FAIL", "periodic_clock"))
        kind = event.get("kind")
        if not isinstance(kind, str) or not kind:
            problems.append(("FAIL", "periodic_kind"))
            continue
        kinds.add(kind)
        phase = event.get("event_phase")
        if phase not in {"start", "end", "instant"}:
            problems.append(("FAIL", "periodic_phase"))
        duration = event.get("duration_ns")
        if not _is_int(duration) or duration < 0:
            problems.append(("FAIL", "periodic_duration"))
        if phase in {"start", "end"}:
            pair_id = event.get("pair_id")
            if not isinstance(pair_id, str) or not pair_id:
                problems.append(("FAIL", "periodic_pair_id"))
                continue
            pair_ids.setdefault(pair_id, []).append(event)
    for pair_id, members in pair_ids.items():
        phases = {member.get("event_phase") for member in members}
        if phases != {"start", "end"}:
            problems.append(("FAIL", "periodic_pair_incomplete: " + pair_id))
            continue
        if len({member.get("kind") for member in members}) != 1:
            problems.append(("FAIL", "periodic_pair_kind: " + pair_id))
        if len({member.get("clock_id") for member in members}) != 1:
            problems.append(("FAIL", "periodic_pair_clock: " + pair_id))
    if "periodic_events_unavailable" in {record.get("kind") for record in availability}:
        return False, ["periodic_events_unavailable"]
    missing = sorted(PERIODIC_KINDS - kinds)
    if missing:
        return False, ["periodic_kind_missing: " + ",".join(missing)]
    return True, []


def _validate_metrics(run_directory, index, problems):
    if not isinstance(index, dict) or index.get("schema") != 1:
        return False, ["metrics_index_schema"]
    if index.get("available") is not True:
        return False, ["metrics_snapshot_unavailable"]
    snapshots = index.get("snapshots")
    if not isinstance(snapshots, list) or not snapshots:
        return False, ["metrics_snapshot_missing"]
    for entry in snapshots:
        if not isinstance(entry, dict):
            problems.append(("FAIL", "metrics_entry_not_object"))
            continue
        relative = entry.get("path")
        expected = entry.get("sha256")
        if not isinstance(relative, str) or not relative:
            problems.append(("FAIL", "metrics_path"))
            continue
        path = (Path(run_directory) / relative).resolve()
        if Path(run_directory).resolve() not in path.parents or not path.is_file():
            problems.append(("FAIL", "metrics_path_invalid: " + relative))
            continue
        if digest(path) != expected:
            problems.append(("FAIL", "metrics_hash_mismatch: " + relative))
        try:
            document = _read_json(path)
        except GateError as failure:
            problems.append(("FAIL", "metrics_unreadable: " + str(failure)))
            continue
        if synthetic(document):
            problems.append(("INCOMPLETE", "synthetic_metrics_forbidden: " + relative))
        if document.get("schema_version") != 1 \
                or document.get("work_metrics_enabled") is not True:
            problems.append(("FAIL", "metrics_schema: " + relative))
        process = document.get("process")
        if not isinstance(process, dict) or not _is_int(process.get("pid")) \
                or process["pid"] <= 0:
            problems.append(("FAIL", "metrics_process: " + relative))
        if not _is_int(document.get("generated_at_unix_ms")):
            problems.append(("FAIL", "metrics_generated_at: " + relative))
        counters = document.get("counters")
        if not isinstance(counters, dict):
            problems.append(("FAIL", "metrics_counters: " + relative))
            continue
        for name in METRIC_COUNTERS:
            if not _is_number(counters.get(name)) or counters[name] < 0:
                problems.append(("FAIL", "metrics_counter_" + name))
    return True, []


def _validate_resources(resources, problems):
    if not isinstance(resources, dict) or resources.get("schema") != 1:
        problems.append(("FAIL", "resources_schema"))
        return False
    client = resources.get("client")
    if not isinstance(client, dict) or not _is_int(client.get("pid")) \
            or client["pid"] <= 0 or not _is_int(client.get("rss_bytes")) \
            or client["rss_bytes"] < 0:
        problems.append(("FAIL", "resources_client"))
    if resources.get("product_processes_accounted") is not True:
        return False
    processes = resources.get("product_processes")
    if not isinstance(processes, list) or not processes:
        return False
    pids = set()
    for process in processes:
        if not isinstance(process, dict):
            problems.append(("FAIL", "resources_process_not_object"))
            continue
        pid = process.get("pid")
        if not _is_int(pid) or pid <= 0 or pid in pids:
            problems.append(("FAIL", "resources_process_pid"))
            continue
        pids.add(pid)
        if not _is_int(process.get("rss_bytes")) or process["rss_bytes"] < 0:
            problems.append(("FAIL", "resources_process_rss"))
        if process.get("group_joined") is not True or process.get("exit_code") != 0:
            problems.append(("FAIL", "resources_process_group"))
    return bool(pids)


def _validate_oracle(oracle, workloads, stats, problems):
    if not isinstance(oracle, dict) or oracle.get("schema") != 1:
        problems.append(("FAIL", "oracle_schema"))
        return
    scale = oracle.get("fixture_scale")
    if scale not in workloads["fixture_scales"]:
        problems.append(("FAIL", "oracle_fixture_scale"))
        return
    if oracle.get("fixture_rows") != scale:
        problems.append(("FAIL", "oracle_fixture_rows"))
    if oracle.get("fixture_sha256") != contract.fixture_digest(scale):
        problems.append(("FAIL", "oracle_fixture_hash"))
    mismatches = oracle.get("mismatches")
    if not isinstance(mismatches, list):
        problems.append(("FAIL", "oracle_mismatches_shape"))
    elif mismatches:
        problems.append(("FAIL", "oracle_mismatches"))
    verified = oracle.get("verified_operations")
    if not _is_int(verified) or verified < stats["success"]:
        problems.append(("FAIL", "oracle_verified_operations"))


def _environment_section(view, workloads, problems):
    document = view.get("environment")
    if not isinstance(document, dict):
        problems.append(("FAIL", "environment_missing"))
        return {"status": FAIL, "formal_eligible": False}
    validation = environment_module.validate(document, workloads)
    for problem in validation["problems"]:
        problems.append(("FAIL", "environment: " + problem))
    if document.get("evidence_class") == "formal" \
            and any(problem.startswith("formal_environment_claim_rejected")
                    for problem in validation["problems"]):
        problems.append(("FAIL", "environment_formal_claim_rejected"))
    return {
        "status": PASS if not validation["problems"] else FAIL,
        "formal_eligible": validation["formal_eligible"],
        "diagnostic": validation["diagnostic"],
        "evidence_class": validation.get("evidence_class"),
        "markers": validation.get("markers", []),
        "problems": validation["problems"],
    }
