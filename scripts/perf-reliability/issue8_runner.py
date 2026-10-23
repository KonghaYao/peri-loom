#!/usr/bin/env python3
"""Issue 8 W0-A1 paired S1/S2 runner on the existing stdio worker harness."""
import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import shutil
import signal
import sys
import threading
import time

import campaign as campaign_module
import evidence as evidence_module
import issue8_workload as workload
import perf_contract
import runner


class RunError(ValueError):
    pass


def _canonical(value):
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def _sha256(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _result_sha256(response):
    return _sha256(response.get("result") if isinstance(response, dict) else None)


def _root(roots, label):
    if "roots" in roots:
        return Path(roots["roots"][label])
    return Path(roots[label])


def database_identifier(campaign_id, case, concurrency, lane):
    tenant = "issue8_" + campaign_id + "_" + case + "_c" + str(concurrency) + "_lane" + str(lane)
    return {"namespace": {"tenant": tenant, "database": "db"}, "branch": "main"}


def metrics_paths(base, concurrency):
    if base is None:
        return [None] * concurrency
    base = Path(base)
    paths = []
    for lane in range(concurrency):
        if concurrency == 1:
            paths.append(base)
        else:
            paths.append(base.with_name(base.stem + "-lane" + str(lane) + base.suffix))
    return paths


def prepare_metrics_outputs(base, concurrency):
    paths = metrics_paths(base, concurrency)
    if base is None:
        return paths
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            raise perf_contract.UnsafePath("metrics_snapshot_exists: " + str(path))
    return paths


def _settings(campaign, label):
    arm = campaign["arms"][label]
    return {"version": arm.get("version") or "", "commit": arm.get("commit") or ""}


def _binary(campaign_path, label, campaign):
    value = Path(campaign["arms"][label]["binary"])
    if not value.is_absolute():
        value = Path(campaign_path).resolve().parent / value
    if not value.is_file():
        raise perf_contract.ContractError("arm_binary_missing: " + str(value))
    return value


def _oracle_section(lanes, model_by_lane, actual_by_lane):
    entries, passed = [], True
    for lane in lanes:
        actual = actual_by_lane.get(lane)
        model = model_by_lane[lane]
        expected = {index: model.expected(index) for index in range(model.rows)}
        valid = runner.payload_matches(actual, expected, model.uncertain)
        passed = passed and valid
        entries.append({"lane": lane, "pass": valid, "actual": actual,
                        "expected_sha256": model.digest(),
                        "actual_sha256": _sha256(actual),
                        "unknown_rows": sorted(model.uncertain)})
    return {"pass": passed, "lanes": entries}


def _execute_operation(worker, session, lane, index, seq, phase, plan, model):
    operation = workload.operation(plan["case"], None, index)
    planned_at = time.monotonic_ns()
    response, rpc = worker.request(operation["operation"], session=session, sql=operation["sql"])
    issued_at, completed_at = rpc.get("started_ns"), rpc.get("completed_ns")
    if type(issued_at) is not int or type(completed_at) is not int:
        raise RunError("worker_timing_missing")
    duration_ns = rpc.get("duration_ns")
    if duration_ns != completed_at - issued_at:
        raise RunError("worker_duration_mismatch")
    outcome = rpc["outcome"]
    ack_type, oracle, observed_sha256 = "", "not_applicable", None
    if outcome == "ACK":
        if operation["kind"] == "write":
            ack_type = "statement_autocommit"
            model.write(operation["row"], operation["payload"])
        else:
            ack_type = "read_result"
            oracle = "match" if workload.verify_read(response, model, operation["row"]) else "mismatch"
            observed_sha256 = _result_sha256(response)
    elif outcome == "Unknown" and operation["kind"] == "write":
        model.uncertain_write(operation["row"], operation["payload"])
    record = {"run": None, "pair": None, "arm": None, "case": plan["case"], "lane": lane,
              "seq": seq, "phase": phase, "kind": operation["kind"], "operation": operation["operation"],
              "row": operation["row"], "schedule_index": index,
              "planned_at": planned_at, "issued_at": issued_at, "completed_at": completed_at,
              "duration_ns": duration_ns, "outcome": outcome, "ack_type": ack_type,
              "durable_ack": False, "oracle": oracle, "observed_sha256": observed_sha256,
              "error_category": rpc.get("error_category"), "session_generation": 0}
    return record, outcome, oracle


def _write_record(writer, lock, record, run_identity):
    record.update(run_identity)
    with lock:
        writer.write(record)


def warmup_lane(worker, session, lane, plan, model, writer, lock, identity):
    failures = 0
    for seq in range(plan["warmup_operations"]):
        record, outcome, oracle = _execute_operation(worker, session, lane, seq, seq, "warmup", plan, model)
        _write_record(writer, lock, record, identity)
        if oracle == "mismatch":
            failures += 1
        if outcome != "ACK":
            return {"attempts": seq + 1, "failures": failures, "terminated": True, "outcome": outcome}
    return {"attempts": plan["warmup_operations"], "failures": failures, "terminated": False}


def timed_lane(worker, session, lane, plan, model, writer, lock, identity, timed_started,
               deadline):
    started = time.monotonic()
    seq, index = 0, plan["warmup_operations"]
    failures, unknown, errors = 0, 0, 0
    while seq < plan["required_operations_per_lane"] or (time.monotonic() - started) < plan["minimum_seconds"]:
        if time.monotonic() >= deadline:
            return {"attempts": seq, "failures": failures, "unknown": unknown, "errors": errors,
                    "terminated": True, "budget_exhausted": True}
        record, outcome, oracle = _execute_operation(worker, session, lane, index, seq, "timed", plan, model)
        _write_record(writer, lock, record, identity)
        seq += 1
        index += 1
        if oracle == "mismatch":
            failures += 1
        if outcome == "Unknown":
            unknown += 1
            return {"attempts": seq, "failures": failures, "unknown": unknown, "errors": errors,
                    "terminated": True, "budget_exhausted": False}
        if outcome == "Error":
            errors += 1
            return {"attempts": seq, "failures": failures, "unknown": unknown, "errors": errors,
                    "terminated": True, "budget_exhausted": False}
    return {"attempts": seq, "failures": failures, "unknown": unknown, "errors": errors,
            "terminated": False, "budget_exhausted": False}


def setup_lane(worker, session, plan):
    started = time.monotonic()
    worker.require("exec", session=session, sql="CREATE TABLE bench(id INTEGER PRIMARY KEY, payload TEXT)")
    for chunk in plan["setup_insert_chunks"]:
        worker.require("exec", session=session, sql=chunk["sql"])
    worker.require("checkpoint", session=session)
    response = worker.require("query", session=session, sql="SELECT id,payload FROM bench ORDER BY id")
    actual = response.get("result")
    expected = {index: "x" * plan["payload_bytes"] for index in range(plan["rows"])}
    return {"pass": runner.payload_matches(actual, expected), "actual": actual,
            "seconds": time.monotonic() - started, "chunks": len(plan["setup_insert_chunks"])}


def verify_initial(worker, session, plan):
    """Validate a prepared snapshot without re-running setup DDL."""
    started = time.monotonic()
    response = worker.require("query", session=session, sql="SELECT id,payload FROM bench ORDER BY id")
    actual = response.get("result")
    expected = {index: "x" * plan["payload_bytes"] for index in range(plan["rows"])}
    return {"pass": runner.payload_matches(actual, expected), "actual": actual,
            "seconds": time.monotonic() - started, "chunks": 0}


def query_full(worker, session):
    response, record = worker.request("query", session=session, sql="SELECT id,payload FROM bench ORDER BY id")
    if record["outcome"] != "ACK":
        return None, record
    return response.get("result"), record


def _library_metrics(paths, evidence_run, requested):
    records = []
    for lane, path in enumerate(paths):
        if requested and path is not None:
            records.append(evidence_run.copy_metrics_snapshot(path, lane))
    return records


def execute_run(campaign, campaign_path, arm, case, concurrency, pair, order, profile, plan,
                frozen, roots, contract_sha, workload_sha, campaign_sha, metrics_base,
                timeout, source_sha, host_environment=None):
    run_id = campaign_module.run_id(pair, arm, case, concurrency)
    run_directory = _root(roots, "evidence") / "runs" / run_id
    run = evidence_module.EvidenceRun(run_directory, {"schema": 1, "run_id": run_id})
    store_base = _root(roots, "store") / campaign["id"] / run_id / "objects"
    cache_base = _root(roots, "cache") / campaign["id"] / run_id
    store = perf_contract.ensure_empty_directory(store_base)
    snapshot = campaign.get("snapshots", {}).get(case)
    snapshot_meta = campaign_module.prepare_store_directory(store, snapshot) if snapshot else {
        "path": str(store), "snapshot_sha256": None, "store_sha256": campaign_module.tree_digest(store)}
    paths = {lane: cache_base / ("lane" + str(lane)) for lane in range(concurrency)}
    for path in paths.values():
        perf_contract.ensure_empty_directory(path)
    snapshot_paths = prepare_metrics_outputs(
        Path(metrics_base) / run_id if metrics_base else None, concurrency)
    settings = _settings(campaign, arm)
    identity = {"run": run_id, "pair": pair, "arm": arm, "case": case}
    run_started_unix_ms = time.time_ns() // 1_000_000
    metadata = {"schema": 1, "run_id": run_id, "campaign_id": campaign["id"],
                "campaign_sha256": campaign_sha, "contract_sha256": contract_sha,
                "workload_sha256": workload_sha, "source_inventory_sha256": source_sha,
                "host_environment": host_environment,
                "profile": profile["name"], "case": case, "concurrency": concurrency,
                "pair": pair, "pair_order": order, "arm": arm,
                "arm_role": campaign["arms"][arm]["role"], "arm_version": settings["version"],
                "arm_commit": settings["commit"], "binary_sha256": frozen["sha256"],
                "binary_frozen": frozen["path"], "rows": plan["rows"],
                "payload_bytes": plan["payload_bytes"],
                "required_operations_per_lane": plan["required_operations_per_lane"],
                "warmup_operations": plan["warmup_operations"],
                "minimum_seconds": plan["minimum_seconds"],
                "max_seconds_per_arm": plan["max_seconds_per_arm"],
                "metrics_snapshot_requested": metrics_base is not None,
                "store": str(store), "cache": str(cache_base), "store_snapshot": snapshot_meta,
                "roots_validation": {key: roots[key] for key in
                                     ("roots", "filesystems", "persistent_mounts_verified")
                                     if key in roots},
                "timed_started_at_ns": None, "timed_completed_at_ns": None,
                "run_started_at_unix_ms": run_started_unix_ms,
                "timed_started_at_unix_ms": None, "timed_completed_at_unix_ms": None,
                "run_completed_at_unix_ms": None,
                "initial_state_sha256": _sha256(None), "post_warmup_state_sha256": _sha256(None),
                "budget_exhausted": True}
    writer_lock = threading.Lock()
    workers, sessions, models = [], [], {}
    sampler = None
    sampler_report = None
    oracle = {"schema": 1, "pass": False, "initial": None, "post_warmup": None, "final": None}
    failure = None
    lane_results = {}
    timed_started_ns = timed_completed_ns = time.monotonic_ns()
    timed_started_unix_ms = timed_completed_unix_ms = run_started_unix_ms
    budget_exhausted = False
    disk_start = {}
    started = time.monotonic()
    deadline = started + profile["max_seconds_per_arm"] + 600
    try:
        for lane in range(concurrency):
            # The exit snapshot only carries counters when PERI_WORK_METRICS=1;
            # the path alone writes a disabled, empty document.
            metrics_env = ({"PERI_WORK_METRICS": "1",
                            "PERI_METRICS_SNAPSHOT_PATH": str(snapshot_paths[lane])}
                           if metrics_base else None)
            worker = runner.Worker(frozen["path"], store, paths[lane], timeout, deadline, env=metrics_env)
            workers.append(worker)
            sessions.append(runner.opened(worker, database_identifier(campaign["id"], case, concurrency, lane)))
            models[lane] = workload.PayloadModel(plan["rows"], plan["payload_bytes"])
        disk_start = {"store": evidence_module.directory_size(store),
                      "cache": evidence_module.directory_size(cache_base)}
        sampler = evidence_module.ResourceSampler([worker.process.pid for worker in workers],
                                                  {"store": store, "cache": cache_base})
        sampler.start()
        initial_lanes = {}
        setup_seconds = 0.0
        for lane, worker in enumerate(workers):
            if snapshot:
                setup = verify_initial(worker, sessions[lane], plan)
            else:
                setup = setup_lane(worker, sessions[lane], plan)
            setup_seconds += setup["seconds"]
            initial_lanes[lane] = setup["actual"]
            if not setup["pass"]:
                oracle["initial"] = _oracle_section(range(concurrency), models, initial_lanes)
                raise RunError("initial_oracle_mismatch")
        oracle["initial"] = _oracle_section(range(concurrency), models, initial_lanes)
        metadata["initial_state_sha256"] = _sha256([initial_lanes[lane] for lane in range(concurrency)])
        barrier = threading.Barrier(concurrency)

        def warm_lane(lane):
            barrier.wait()
            return warmup_lane(workers[lane], sessions[lane], lane, plan, models[lane],
                               run.operations, writer_lock, identity)

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            warm_results = list(pool.map(warm_lane, range(concurrency)))
        if any(result["terminated"] for result in warm_results):
            raise RunError("warmup_non_ack")
        post_lanes = {}
        for lane, worker in enumerate(workers):
            actual, observation = query_full(worker, sessions[lane])
            if observation["outcome"] != "ACK":
                raise RunError("post_warmup_query_failed")
            post_lanes[lane] = actual
        oracle["post_warmup"] = _oracle_section(range(concurrency), models, post_lanes)
        metadata["post_warmup_state_sha256"] = _sha256(
            [post_lanes[lane] for lane in range(concurrency)])
        if not oracle["post_warmup"]["pass"]:
            raise RunError("post_warmup_oracle_mismatch")
        timed_started_ns = time.monotonic_ns()
        timed_started_unix_ms = time.time_ns() // 1_000_000
        timed_deadline = time.monotonic() + profile["max_seconds_per_arm"]

        def time_lane(lane):
            barrier.wait()
            return timed_lane(workers[lane], sessions[lane], lane, plan, models[lane],
                              run.operations, writer_lock, identity, timed_started_ns, timed_deadline)

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            timed_results = list(pool.map(time_lane, range(concurrency)))
        timed_completed_ns = time.monotonic_ns()
        timed_completed_unix_ms = time.time_ns() // 1_000_000
        lane_results = {lane: timed_results[lane] for lane in range(concurrency)}
        budget_exhausted = any(result["budget_exhausted"] for result in timed_results)
        sampler_report = sampler.stop()
        sampler = None
        final_lanes = {}
        for lane in range(concurrency):
            worker, session = workers[lane], sessions[lane]
            if lane_results[lane]["terminated"]:
                worker.stop(crash=True)
                worker = runner.Worker(frozen["path"], store,
                                       paths[lane].parent / ("oracle-lane" + str(lane)),
                                       timeout, deadline)
                workers.append(worker)
                session = runner.opened(worker, database_identifier(campaign["id"], case, concurrency, lane))
            actual, observation = query_full(worker, session)
            final_lanes[lane] = actual if observation["outcome"] == "ACK" else None
        oracle["final"] = _oracle_section(range(concurrency), models, final_lanes)
        oracle["pass"] = bool(oracle["initial"]["pass"] and oracle["post_warmup"]["pass"]
                              and oracle["final"]["pass"])
        metadata["timed_started_at_ns"] = timed_started_ns
        metadata["timed_completed_at_ns"] = timed_completed_ns
        metadata["timed_started_at_unix_ms"] = timed_started_unix_ms
        metadata["timed_completed_at_unix_ms"] = timed_completed_unix_ms
        metadata["budget_exhausted"] = budget_exhausted
        metadata["setup_seconds"] = setup_seconds
        usage = runner.stop_workers(workers)
        metadata["run_completed_at_unix_ms"] = time.time_ns() // 1_000_000
        # The sampler only observes the initial workers; tag them with their
        # lane so the gate can bind each metrics snapshot to its worker pid.
        for lane, process in enumerate(usage[:concurrency]):
            process["lane"] = lane
        disk_end = {"store": evidence_module.directory_size(store),
                    "cache": evidence_module.directory_size(cache_base)}
        resources = evidence_module.build_resources_report(
            sampler_report, usage, {"store": store}, {"cache": cache_base}, disk_start, disk_end)
        _library_metrics(snapshot_paths, run, metrics_base is not None)
        run.write_json("oracle.json", oracle)
        run.write_json("resources.json", resources)
        summary = _summary(run_id, plan, lane_results, budget_exhausted, oracle, failure,
                           setup_seconds, timed_started_ns, timed_completed_ns)
        run.write_json("summary.json", summary)
        run.write_json("environment.json", metadata, reseal=True)
        run.finish()
        core_failed = (not oracle["pass"] or budget_exhausted
                       or resources["status"] != "MEASURED"
                       or any(result["unknown"] or result["errors"] or result["failures"]
                               for result in lane_results.values()))
        return {"run_id": run_id, "directory": str(run_directory), "core_failed": core_failed,
                "summary": summary, "environment": metadata}
    except (RunError, RuntimeError, OSError, ValueError, TimeoutError, EOFError) as caught:
        failure = type(caught).__name__ + ": " + str(caught)
        metadata["failure"] = failure
        metadata["timed_started_at_ns"] = timed_started_ns
        metadata["timed_completed_at_ns"] = timed_completed_ns
        metadata["timed_started_at_unix_ms"] = timed_started_unix_ms
        metadata["timed_completed_at_unix_ms"] = timed_completed_unix_ms
        metadata["budget_exhausted"] = budget_exhausted
        if sampler is not None:
            try:
                sampler_report = sampler.stop()
            except evidence_module.EvidenceError as sampler_failure:
                sampler_report = {"status": "UNMEASURED", "failure": str(sampler_failure),
                                  "peak_rss_bytes": None, "last_window_rss_bytes": None,
                                  "cpu_seconds": None}
            sampler = None
        if sampler_report is None:
            sampler_report = {"status": "UNMEASURED", "peak_rss_bytes": None,
                              "last_window_rss_bytes": None, "cpu_seconds": None}
        usage = runner.stop_workers(workers)
        metadata["run_completed_at_unix_ms"] = time.time_ns() // 1_000_000
        for lane, process in enumerate(usage[:concurrency]):
            process["lane"] = lane
        disk_end = {"store": evidence_module.directory_size(store),
                    "cache": evidence_module.directory_size(cache_base)}
        resources = evidence_module.build_resources_report(
            sampler_report, usage, {"store": store}, {"cache": cache_base}, disk_start, disk_end)
        _library_metrics(snapshot_paths, run, metrics_base is not None)
        run.write_json("oracle.json", oracle, reseal=True)
        run.write_json("resources.json", resources, reseal=True)
        summary = _summary(run_id, plan, lane_results, budget_exhausted, oracle, failure,
                           0.0, timed_started_ns, timed_completed_ns)
        run.write_json("summary.json", summary, reseal=True)
        run.write_json("environment.json", metadata, reseal=True)
        try:
            run.finish()
        except (OSError, evidence_module.EvidenceError) as finalize_failure:
            failure += "; finalize=" + str(finalize_failure)
        return {"run_id": run_id, "directory": str(run_directory), "core_failed": True,
                "summary": summary, "environment": metadata, "failure": failure}
    finally:
        if sampler is not None:
            try:
                sampler.stop()
            except Exception as cleanup_failure:
                sys.stderr.write("issue8_runner cleanup: " + type(cleanup_failure).__name__ + "\n")
        try:
            runner.stop_workers(workers)
        except Exception as cleanup_failure:
            sys.stderr.write("issue8_runner cleanup: " + type(cleanup_failure).__name__ + "\n")


def _summary(run_id, plan, lane_results, budget_exhausted, oracle, failure, setup_seconds,
             timed_started_ns, timed_completed_ns):
    per_lane = {}
    for lane, result in lane_results.items():
        per_lane[str(lane)] = {key: result.get(key) for key in
                               ("attempts", "unknown", "errors", "failures", "terminated")}
    return {"schema": 1, "run_id": run_id, "case": plan["case"],
            "attempts": sum(result.get("attempts", 0) for result in lane_results.values()),
            "unknown": sum(result.get("unknown", 0) for result in lane_results.values()),
            "errors": sum(result.get("errors", 0) for result in lane_results.values()),
            "read_oracle_mismatches": sum(result.get("failures", 0) for result in lane_results.values()),
            "per_lane": per_lane, "budget_exhausted": budget_exhausted,
            "oracle_pass": oracle.get("pass"), "failure": failure,
            "setup_seconds": setup_seconds,
            "timed_span_seconds": (timed_completed_ns - timed_started_ns) / 1_000_000_000,
            "percentiles": "NOT_IN_SUMMARY; gate recomputes exact ns percentiles from operations.jsonl"}


def main(argv=None):
    signal.signal(signal.SIGTERM, lambda signum, frame: (_ for _ in ()).throw(KeyboardInterrupt()))
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--profile", choices=["quick", "formal", "steady"], required=True)
    parser.add_argument("--tier", choices=["G1", "G2"], required=True)
    parser.add_argument("--store-root", type=Path, required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--evidence-root", type=Path, required=True)
    parser.add_argument("--metrics-snapshot", type=Path,
                        help="directory receiving raw PERI_METRICS_SNAPSHOT_PATH files")
    parser.add_argument("--minimum-samples", type=int)
    parser.add_argument("--minimum-seconds", type=int)
    parser.add_argument("--max-seconds-per-arm", type=int)
    parser.add_argument("--pairs", type=int)
    parser.add_argument("--timeout", type=float, default=10)
    arguments = parser.parse_args(argv)
    overrides = {key: value for key, value in {
        "min_samples_per_lane": arguments.minimum_samples,
        "min_seconds": arguments.minimum_seconds,
        "max_seconds_per_arm": arguments.max_seconds_per_arm,
        "pairs": arguments.pairs}.items() if value is not None}
    try:
        if not .1 <= arguments.timeout <= 20:
            raise perf_contract.ContractError("timeout_out_of_bounds")
        profile = perf_contract.profile(arguments.profile, overrides=overrides)
        if arguments.profile == "steady":
            raise perf_contract.ContractError("steady_profile_not_implemented_in_w0a1")
        campaign = campaign_module.load_campaign(arguments.campaign)
        campaign_module.validate_profile(campaign, profile)
        campaign_module.validate_coverage(campaign, arguments.tier)
        if campaign.get("tier") and campaign["tier"] != arguments.tier:
            raise campaign_module.CampaignError("campaign_tier_mismatch")
        contract = perf_contract.verify_contract()
        contract_sha = contract["sha256sums_sha256"]
        workload_sha = perf_contract.digest(perf_contract.CONTRACT_ROOT / "workloads.json")
        campaign_sha = campaign_module.campaign_identity(campaign)
        roots = perf_contract.validate_roots(arguments.store_root, arguments.cache_root,
                                             arguments.evidence_root, profile["name"], create=True)
        evidence_root = Path(roots["roots"]["evidence"])
        if any(evidence_root.iterdir()):
            raise perf_contract.UnsafePath("evidence_root_must_be_empty")
        target_campaign = evidence_root / "campaign.json"
        shutil.copyfile(arguments.campaign, target_campaign)
        (evidence_root / "runs").mkdir()
        (evidence_root / "artifacts").mkdir()
        frozen = {label: evidence_module.freeze_artifact(
            _binary(arguments.campaign, label, campaign), evidence_root / "artifacts" / label)
            for label in ("A", "B")}
        source_before = runner.source_inventory()
        source_sha = _sha256(source_before)
        host_environment = perf_contract.environment_identity()
        metrics_base = arguments.metrics_snapshot
        if metrics_base is not None:
            metrics_base.mkdir(parents=True, exist_ok=True)
            if any(metrics_base.iterdir()):
                raise perf_contract.UnsafePath("metrics_snapshot_directory_must_be_empty")
        reports = []
        for planned in campaign_module.planned_runs(campaign, profile):
            plan = workload.case_plan(profile, planned["case"], planned["concurrency"])
            report = execute_run(campaign, arguments.campaign, planned["arm"], planned["case"],
                                 planned["concurrency"], planned["pair"], planned["order"], profile,
                                 plan, frozen[planned["arm"]], roots, contract_sha, workload_sha,
                                 campaign_sha, metrics_base, arguments.timeout, source_sha,
                                 host_environment)
            reports.append(report)
            print(json.dumps({"run": report["run_id"], "core_failed": report["core_failed"],
                              "failure": report.get("failure")}))
        source_after = runner.source_inventory()
        source_unchanged = source_after == source_before
        evidence_module.write_json(evidence_root / "campaign-identity.json", {
            "schema": 1, "campaign_sha256": campaign_sha, "contract_sha256": contract_sha,
            "workload_sha256": workload_sha, "profile": profile["name"], "tier": arguments.tier,
            "source_before_sha256": source_sha, "source_after_sha256": _sha256(source_after),
            "source_unchanged": source_unchanged, "arms": frozen,
            "metrics_snapshot_requested": metrics_base is not None,
            "environment": host_environment,
            "paired_order": profile["order"][:profile["pairs"]]})
        failed = not source_unchanged or any(report["core_failed"] for report in reports)
        print(json.dumps({"runs": len(reports), "source_unchanged": source_unchanged,
                          "core_failed": failed, "evidence_root": str(evidence_root)}))
        return 1 if failed else 0
    except (perf_contract.ContractError, perf_contract.UnsafePath, campaign_module.CampaignError,
            evidence_module.EvidenceError, runner.ProtocolError, OSError, ValueError) as failure:
        print(json.dumps({"status": "NOT_RUN", "error": type(failure).__name__ + ": " + str(failure)}))
        return 2


if __name__ == "__main__":
    sys.exit(main())
