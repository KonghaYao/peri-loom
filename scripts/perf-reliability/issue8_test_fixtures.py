import copy
import hashlib
import json
import math
import platform
from pathlib import Path

import campaign as campaign_module
import evidence as evidence_module
import issue8_workload as workload
import perf_contract

ROWS = 64
PAYLOAD_BYTES = 256
CONTRACT_SHA = perf_contract.contract_identity()["sha256sums_sha256"]
WORKLOAD_SHA = perf_contract.digest(perf_contract.CONTRACT_ROOT / "workloads.json")
OFFICIAL_DENOMINATOR = perf_contract.official_denominator()
FIXTURE_SOURCE_SHA = hashlib.sha256(b"fixture-source-inventory").hexdigest()
FIXTURE_STORE_ROOT = "/synthetic/perf-store"
FIXTURE_CACHE_ROOT = "/synthetic/perf-cache"
FIXTURE_FILESYSTEM = {"Darwin": "apfs", "Linux": "ext4"}.get(platform.system(), "fixturefs")
HOST_ENVIRONMENT = {"os": platform.system(), "arch": platform.machine(),
                    "kernel": platform.release(), "machine": platform.machine(),
                    "python": platform.python_version()}
# Mirrors prototypes/v1/docs/metrics-exit-snapshot.md (schema_version 1).
METRICS_PHASES = ("snapshot", "clone", "mutate", "prepare", "receipt", "validate",
                  "index", "seal", "encode", "group", "recover")
RESOURCE_INTERVAL_SECONDS = 0.1
RUN_STARTED_UNIX_MS = 1_760_000_000_000
TIMED_STARTED_UNIX_MS = RUN_STARTED_UNIX_MS + 10_000


def metrics_document(writes=0, pid=4242, generated_at=TIMED_STARTED_UNIX_MS + 1_000):
    return {
        "schema_version": 1,
        "generated_at_unix_ms": generated_at,
        "process": {"pid": pid, "version": "1.1.11"},
        "work_metrics_enabled": True,
        "phases": [{"name": name, "count": writes if name == "snapshot" else 0,
                    "total_ns": 0, "max_ns": 0} for name in METRICS_PHASES],
        "counters": [
            {"name": "work.wal_workload.append_calls", "value": writes},
            {"name": "work.metadata.root_publish_attempts", "value": writes + 1},
            {"name": "object_store.put.total", "value": writes + 2},
            {"name": "object_store.get.total", "value": writes + 3},
            {"name": "object_store.wal_durability.total", "value": writes + 4},
        ],
        "group_phases": {"available": False, "reason": "fixture: no process aggregate"},
    }


def artifact_sha(arm):
    return hashlib.sha256(b"fixture-binary-" + arm.encode()).hexdigest()


def campaign_document(tier="G1", pairs=1, order=None):
    if order is None:
        order = ["AB", "BA", "AB"][:pairs]
    cases = [{"id": "S1", "concurrency": [2]}]
    if tier == "G2":
        cases = [{"id": "S1", "concurrency": [2, 1]}, {"id": "S2", "concurrency": [2]}]
    return {"schema": 1, "id": "synthetic-" + tier.lower(), "tier": tier, "pairs": pairs,
            "order": order, "cases": cases,
            "expected_denominator": dict(OFFICIAL_DENOMINATOR),
            "arms": {"A": {"role": "candidate", "binary": "candidate.bin",
                           "version": "v1.1.12", "commit": "candidate-commit"},
                     "B": {"role": "denominator", "binary": "baseline.bin",
                           "version": OFFICIAL_DENOMINATOR["version"],
                           "commit": OFFICIAL_DENOMINATOR["commit"]}}}


def policy(name="quick", pairs=1, minimum_samples=8, minimum_seconds=1,
           max_seconds=10, warmup=4, total_pass=None):
    if total_pass is None:
        total_pass = name == "formal"
    return {"name": name, "min_samples_per_lane": minimum_samples,
            "min_seconds": minimum_seconds, "max_seconds_per_arm": max_seconds,
            "pairs": pairs, "order": ["AB", "BA", "AB"][:pairs],
            "warmup_operations": warmup, "total_pass": total_pass,
            "case_set": ["S1", "S2"]}


def _result_sha256(payload):
    return hashlib.sha256(json.dumps([[payload]], separators=(",", ":"),
                                     sort_keys=True).encode()).hexdigest()


def _durations_for(count, values):
    if isinstance(values, (int, float)):
        return [int(values)] * count
    p50, p95, p99 = values
    result = []
    for seq in range(count):
        if seq < count * 0.5:
            result.append(int(p50))
        elif seq < count * 0.95:
            result.append(int(p95))
        else:
            result.append(int(p99))
    return result


def _simulate(case, lane, warmup_count, timed_count):
    model = workload.PayloadModel(ROWS, PAYLOAD_BYTES)
    operations = []
    for seq in range(warmup_count):
        index = seq
        operation = workload.operation(case, None, index)
        observed = None
        oracle = "not_applicable"
        if operation["kind"] == "write":
            model.write(operation["row"], operation["payload"])
        else:
            observed = _result_sha256(model.expected(operation["row"]))
            oracle = "match"
        operations.append({"phase": "warmup", "seq": seq, "index": index,
                           "kind": operation["kind"], "row": operation["row"],
                           "observed_sha256": observed, "oracle": oracle})
    post_warmup = [[index, model.expected(index)] for index in range(ROWS)]
    for seq in range(timed_count):
        index = warmup_count + seq
        operation = workload.operation(case, None, index)
        observed = None
        oracle = "not_applicable"
        if operation["kind"] == "write":
            model.write(operation["row"], operation["payload"])
        else:
            observed = _result_sha256(model.expected(operation["row"]))
            oracle = "match"
        operations.append({"phase": "timed", "seq": seq, "index": index,
                           "kind": operation["kind"], "row": operation["row"],
                           "observed_sha256": observed, "oracle": oracle})
    final = [[index, model.expected(index)] for index in range(ROWS)]
    initial = [[index, "x" * PAYLOAD_BYTES] for index in range(ROWS)]
    return {"operations": operations, "initial": initial, "post_warmup": post_warmup,
            "final": final}


def _records(run_id, pair, arm, case, lane, simulation, durations, span_ns, base_ns):
    records = []
    warmup = [operation for operation in simulation["operations"] if operation["phase"] == "warmup"]
    timed = [operation for operation in simulation["operations"] if operation["phase"] == "timed"]
    warmup_step = max(10_000_000, max(durations, default=0) + 1_000_000)
    for operation in warmup:
        # Warmup must finish before timed_started_at_ns, not merely appear
        # earlier in the file. Reserve a full slot for each warmup duration.
        issued = base_ns - (len(warmup) - operation["seq"]) * warmup_step
        duration = int(durations[operation["seq"]])
        records.append(_record(run_id, pair, arm, case, lane, operation, issued, duration, "warmup"))
    step = max(1, (span_ns - max(durations) - 1) // max(1, len(timed)))
    for operation in timed:
        issued = base_ns + operation["seq"] * step
        duration = int(durations[operation["seq"]])
        records.append(_record(run_id, pair, arm, case, lane, operation, issued, duration, "timed"))
    return records


def _record(run_id, pair, arm, case, lane, operation, issued, duration, phase):
    if operation["kind"] == "write":
        ack_type, oracle = "statement_autocommit", "not_applicable"
    else:
        ack_type, oracle = "read_result", operation["oracle"]
    return {"run": run_id, "pair": pair, "arm": arm, "case": case, "lane": lane,
            "seq": operation["seq"], "phase": phase, "kind": operation["kind"],
            "row": operation["row"], "schedule_index": operation["index"],
            "planned_at": issued - 1, "issued_at": issued,
            "completed_at": issued + duration, "duration_ns": duration,
            "outcome": "ACK", "ack_type": ack_type, "durable_ack": False,
            "oracle": oracle, "observed_sha256": operation["observed_sha256"],
            "error_category": None, "session_generation": 0}


def run_pid_base(run_id):
    digest = hashlib.sha256(run_id.encode()).hexdigest()
    return 1000 + int(digest[:8], 16) % 50000


def resource_document(concurrency, timed_started_ns=10_000_000_000,
                      timed_completed_ns=11_000_000_000, pid_base=100):
    interval_ns = int(RESOURCE_INTERVAL_SECONDS * 1_000_000_000)
    # Start before and end after the timed window, as the real sampler does.
    first = timed_started_ns - 5 * interval_ns
    last = timed_completed_ns + 5 * interval_ns
    count = max(20, math.ceil((last - first) / interval_ns) + 1)
    samples = [{"monotonic_ns": first + index * interval_ns,
                "roots": [pid_base + lane for lane in range(concurrency)],
                "pids": [pid_base + lane for lane in range(concurrency)],
                "rss_bytes": (1024 + index) * 1024, "cpu_seconds": 0.1 * index,
                "disk_bytes": {}} for index in range(count)]
    window_size = max(1, math.ceil(len(samples) * 0.1))
    window = [sample["rss_bytes"] for sample in samples[-window_size:]]
    return {"schema": 1, "status": "MEASURED",
            "sampler": {"status": "MEASURED", "sample_count": len(samples),
                        "samples": samples,
                        "peak_rss_bytes": max(sample["rss_bytes"] for sample in samples),
                        "last_window_rss_bytes": window, "cpu_seconds": 1.9,
                        "failures": [], "interval_seconds": RESOURCE_INTERVAL_SECONDS},
            "processes": [{"pid": pid_base + lane, "lane": lane, "group_joined": True,
                           "exit_code": 0} for lane in range(concurrency)],
            "process_group_join_proof": True, "all_processes_accounted": True,
            "disk": {"store": {"start_bytes": 0, "end_bytes": 1024, "growth_bytes": 1024},
                     "cache": {"start_bytes": 0, "end_bytes": 512, "growth_bytes": 512}}}


def add_run(root, campaign, policy_document, case, concurrency, pair, arm,
            span_seconds=1.0, count=None, duration_values=(1_000_000, 2_000_000, 3_000_000),
            planned_operations=None, metrics_requested=True, mutate=None):
    root = Path(root)
    campaign_sha = campaign_module.campaign_identity(campaign)
    run_id = campaign_module.run_id(pair, arm, case, concurrency)
    run_directory = root / "runs" / run_id
    run_directory.mkdir(parents=True)
    count = count if count is not None else max(8, policy_document["min_samples_per_lane"] * (2 if case == "S2" else 1))
    planned_operations = planned_operations or max(count, policy_document["min_samples_per_lane"] * (2 if case == "S2" else 1))
    span_ns = int(span_seconds * 1_000_000_000)
    base_ns = 10_000_000_000
    simulations = {lane: _simulate(case, lane, policy_document["warmup_operations"], count)
                   for lane in range(concurrency)}
    all_records = []
    for lane in range(concurrency):
        durations = _durations_for(count, duration_values)
        all_records.extend(_records(run_id, pair, arm, case, lane, simulations[lane],
                                    durations, span_ns, base_ns))
    if mutate:
        mutate(all_records)
    writer = evidence_module.RawRecordWriter(run_directory / "operations.jsonl")
    for record in all_records:
        writer.write(record)
    writer.close()
    initial = [simulations[lane]["initial"] for lane in range(concurrency)]
    post = [simulations[lane]["post_warmup"] for lane in range(concurrency)]
    final = [simulations[lane]["final"] for lane in range(concurrency)]
    oracle = {"schema": 1, "pass": True,
              "initial": {"pass": True, "lanes": [
                  {"lane": lane, "pass": True, "actual": initial[lane]} for lane in range(concurrency)]},
              "post_warmup": {"pass": True, "lanes": [
                  {"lane": lane, "pass": True, "actual": post[lane]} for lane in range(concurrency)]},
              "final": {"pass": True, "lanes": [
                  {"lane": lane, "pass": True, "actual": final[lane]} for lane in range(concurrency)]}}
    metadata = {"schema": 1, "run_id": run_id, "campaign_id": campaign["id"],
                "campaign_sha256": campaign_sha, "contract_sha256": CONTRACT_SHA,
                "workload_sha256": WORKLOAD_SHA,
                "source_inventory_sha256": FIXTURE_SOURCE_SHA,
                "host_environment": dict(HOST_ENVIRONMENT),
                "roots_validation": {
                    "roots": {"store": FIXTURE_STORE_ROOT, "cache": FIXTURE_CACHE_ROOT,
                              "evidence": str(root)},
                    "filesystems": {"store": FIXTURE_FILESYSTEM,
                                    "cache": FIXTURE_FILESYSTEM,
                                    "evidence": FIXTURE_FILESYSTEM},
                    "persistent_mounts_verified": True},
                "profile": policy_document["name"], "case": case, "concurrency": concurrency,
                "pair": pair, "pair_order": campaign["order"][pair], "arm": arm,
                "arm_role": campaign["arms"][arm]["role"],
                "arm_version": campaign["arms"][arm]["version"],
                "arm_commit": campaign["arms"][arm]["commit"],
                "binary_sha256": artifact_sha(arm),
                "rows": ROWS, "payload_bytes": PAYLOAD_BYTES,
                "required_operations_per_lane": planned_operations,
                "warmup_operations": policy_document["warmup_operations"],
                "minimum_seconds": policy_document["min_seconds"],
                "max_seconds_per_arm": policy_document["max_seconds_per_arm"],
                "metrics_snapshot_requested": metrics_requested,
                "timed_started_at_ns": base_ns, "timed_completed_at_ns": base_ns + span_ns,
                "run_started_at_unix_ms": RUN_STARTED_UNIX_MS,
                "timed_started_at_unix_ms": TIMED_STARTED_UNIX_MS,
                "timed_completed_at_unix_ms": TIMED_STARTED_UNIX_MS + max(1000, span_ns // 1_000_000),
                "run_completed_at_unix_ms": TIMED_STARTED_UNIX_MS
                + max(1000, span_ns // 1_000_000) + 10_000,
                "initial_state_sha256": hashlib.sha256(
                    json.dumps(initial, separators=(",", ":"), sort_keys=True).encode()).hexdigest(),
                "post_warmup_state_sha256": hashlib.sha256(
                    json.dumps(post, separators=(",", ":"), sort_keys=True).encode()).hexdigest(),
                "budget_exhausted": False}
    pid_base = run_pid_base(run_id)
    resources = resource_document(concurrency, base_ns, base_ns + span_ns, pid_base=pid_base)
    summary = {"schema": 1, "run_id": run_id, "attempts": len(all_records),
               "percentiles": "synthetic values are not trusted by gate",
               "ack_latency_ms": {"p50": 0.001, "p95": 0.002, "p99": 0.003}}
    if metrics_requested:
        snapshots = []
        for lane in range(concurrency):
            name = "metrics-snapshot-lane" + str(lane) + ".json"
            writes = sum(1 for operation in simulations[lane]["operations"]
                         if operation["kind"] == "write")
            payload = (json.dumps(metrics_document(
                writes, pid=pid_base + lane,
                generated_at=TIMED_STARTED_UNIX_MS + max(1000, span_ns // 1_000_000) + 1_000))
                + "\n").encode()
            (run_directory / name).write_bytes(payload)
            snapshots.append({"lane": lane, "copied": True, "file": name,
                              "sha256": hashlib.sha256(payload).hexdigest(),
                              "bytes": len(payload)})
        evidence_module.write_json(run_directory / "metrics-snapshot-index.json",
                                   {"schema": 1, "requested": True, "snapshots": snapshots})
    evidence_module.write_json(run_directory / "environment.json", metadata)
    evidence_module.write_json(run_directory / "oracle.json", oracle)
    evidence_module.write_json(run_directory / "resources.json", resources)
    evidence_module.write_json(run_directory / "summary.json", summary)
    evidence_module.write_manifest(run_directory, required=("environment.json", "operations.jsonl",
                                                            "oracle.json", "resources.json",
                                                            "summary.json"))
    return {"run_id": run_id, "directory": run_directory, "oracle": oracle,
            "resources": resources, "metadata": metadata}


def build_evidence(root, tier="G1", pairs=1, policy_document=None, arms=("A", "B"),
                   span_seconds=1.0, count=None, duration_values=(1_000_000, 2_000_000, 3_000_000),
                   metrics_requested=True, denominator_span_seconds=None,
                   counts=None, arm_spans=None, duration_by_arm=None, duration_by_pair=None):
    root = Path(root)
    policy_document = policy_document or policy(pairs=pairs)
    campaign = campaign_document(tier=tier, pairs=pairs, order=policy_document["order"])
    evidence_module.write_json(root / "campaign.json", campaign)
    frozen = {}
    for label in ("A", "B"):
        directory = root / "artifacts" / label
        directory.mkdir(parents=True)
        target = directory / ("frozen-" + label + ".bin")
        target.write_bytes(b"fixture-binary-" + label.encode())
        frozen[label] = {"path": str(target), "sha256": artifact_sha(label), "origin": "fixture"}
    evidence_module.write_json(root / "campaign-identity.json", {
        "schema": 1, "campaign_sha256": campaign_module.campaign_identity(campaign),
        "contract_sha256": CONTRACT_SHA, "workload_sha256": WORKLOAD_SHA,
        "profile": policy_document["name"], "tier": tier,
        "source_before_sha256": FIXTURE_SOURCE_SHA, "source_after_sha256": FIXTURE_SOURCE_SHA,
        "source_unchanged": True, "arms": frozen,
        "metrics_snapshot_requested": metrics_requested,
        "paired_order": policy_document["order"][:pairs]})
    runs = {}
    counts = counts or {}
    arm_spans = arm_spans or {}
    duration_by_arm = duration_by_arm or {}
    duration_by_pair = duration_by_pair or {}
    for case in campaign["cases"]:
        for concurrency in case["concurrency"]:
            for pair in range(pairs):
                for arm in campaign["order"][pair]:
                    if arm not in arms:
                        continue
                    run_count = counts.get(case["id"], count)
                    if case["id"] == "S2" and run_count is None:
                        run_count = max(8, policy_document["min_samples_per_lane"] * 2)
                    default_span = span_seconds if arm == "A" else (
                        denominator_span_seconds or span_seconds)
                    span = arm_spans.get((case["id"], arm), arm_spans.get(arm, default_span))
                    values = duration_by_pair.get((pair, arm),
                                                  duration_by_arm.get(arm, duration_values))
                    runs[(case["id"], concurrency, pair, arm)] = add_run(
                        root, campaign, policy_document, case["id"], concurrency, pair, arm,
                        span_seconds=span, count=run_count, duration_values=values,
                        metrics_requested=metrics_requested)
    return {"root": root, "campaign": campaign, "policy": policy_document, "runs": runs}


def reseal(run_directory):
    evidence_module.write_manifest(run_directory, required=("environment.json", "operations.jsonl",
                                                            "oracle.json", "resources.json",
                                                            "summary.json"), reseal=True)
