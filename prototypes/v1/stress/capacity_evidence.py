import json
from pathlib import Path

from capacity_metrics import knee, phase_cell, timestamp
from capacity_envelope import rpc_measurement, safe_envelope
from capacity_plan import demand_fraction, digest, expected_phases, minimum_sessions
from capacity_plan import TOPOLOGIES
from capacity_coverage import omitted_cells, phase_slots
from capacity_fifo import database_key, effective_fifo
from capacity_competition import cleanup_contention, io_overlap
import run_guarded as guarded


MAX_EVIDENCE = 64 << 20


def records(path):
    guarded.private(path, 0o600)
    guarded.require(path.stat().st_size <= MAX_EVIDENCE, "oversized_evidence")
    with path.open() as stream:
        for line in stream:
            guarded.require(len(line) <= 8 << 20, "oversized_evidence_record")
            yield json.loads(line)


def supplemental(phase, report, observation, workload, acceptance):
    result = {"effective_fifo": "NOT MEASURED", "owner_reserved_cleanup": "NOT MEASURED",
              "physical_io_competition": "NOT MEASURED", "issues": [], "missing": []}
    relevant = next((entry for entry in observation.get("phases", [])
                     if entry.get("phase") == phase["phase"] and entry.get("concurrency") == phase["concurrency"]), {})
    phase = {**phase, "native_process_identities": relevant.get("native_process_identities", [])}
    if observation and observation.get("run_id") != report["run_id"]:
        result["issues"].append("supplemental evidence run identity mismatch")
        return result
    combined = phase["phase"].startswith("combined")
    if workload == "held_writer" and combined:
        if phase["concurrency"] == 8:
            status, reason = effective_fifo(relevant.get("tickets", []), phase.get("observed_databases", []))
            if status == "PASS":
                start, finish = timestamp(phase["started_at_observed"]), timestamp(phase["finished_at_observed"])
                if not all(start <= timestamp(ticket["enqueued_at"]) <= timestamp(ticket["admitted_at"]) <= finish
                           for ticket in relevant["tickets"] if ticket["valid"] and not ticket["invalidated"]):
                    status, reason = "FAIL", "native FIFO trace outside actual phase window"
            result["effective_fifo"] = status
            if reason:
                result["issues" if status == "FAIL" else "missing"].append(reason)
        else:
            result["effective_fifo"] = "NOT MEASURED"
            result["fifo_scope"] = "safe tier does not substitute for C8 interference DB-queue FIFO proof"
        cleanup = relevant.get("owner_cleanup", [])
        if not cleanup:
            result["missing"].append("native owner reserved cleanup evidence unavailable")
        else:
            valid = all(entry.get("reserved_slots", 0) >= 1 and entry.get("terminal") in ("COMMIT", "ROLLBACK")
                        and entry.get("terminal_acked") is True and 0 <= entry.get("elapsed_seconds", -1) <= acceptance["reserved_cleanup_seconds"]
                        for entry in cleanup) and {entry.get("terminal") for entry in cleanup} == {"COMMIT", "ROLLBACK"}
            terminals = {database: {entry.get("terminal") for entry in cleanup
                                   if database_key(entry.get("project"), entry.get("database")) == database}
                         for database in phase.get("observed_databases", [])}
            valid = valid and bool(terminals) and all(values == {"COMMIT", "ROLLBACK"} for values in terminals.values())
            result["owner_reserved_cleanup"] = "PASS" if valid else "FAIL"
            if not valid:
                result["issues"].append("native owner cleanup bound failed")
            elif phase["concurrency"] == 8 and not cleanup_contention(cleanup, relevant.get("tickets", []), phase.get("observed_databases", [])):
                result["owner_reserved_cleanup"] = "NOT MEASURED"
                result["missing"].append("native reserved cleanup while actual waiter pending not proven")
    if workload == "io_checkpoint":
        io = relevant.get("io_samples", [])
        if not io:
            result["missing"].append("physical IO counter delta unavailable; checkpoint ACK alone is insufficient")
        else:
            start, finish = timestamp(phase["started_at_observed"]), timestamp(phase["finished_at_observed"])
            valid = all(entry.get("source") in ("cgroup.io.stat", "proc.io", "native.io_counter")
                        and entry.get("sample_count", 0) >= 2 and type(entry.get("before_bytes")) is int
                        and type(entry.get("after_bytes")) is int and entry["after_bytes"] > entry["before_bytes"] >= 0
                        and start <= timestamp(entry["started_at"]) < timestamp(entry["finished_at"]) <= finish
                        for entry in io)
            valid = valid and bool(phase.get("observed_databases")) and {database_key(entry.get("project"), entry.get("database")) for entry in io} == set(phase["observed_databases"])
            result["physical_io_competition"] = "PASS" if valid else "FAIL"
            if not valid:
                result["issues"].append("physical IO competition not proven within phase")
            else:
                status, reason = io_overlap(io, phase)
                result["physical_io_competition"] = status
                if reason:
                    result["issues" if status == "FAIL" else "missing"].append(reason)
    return result


def collect_job(job, plan):
    root = Path(job["output"])
    manifests = {}
    paths = list(root.glob("run_*/run.json"))
    if len(paths) != 1:
        return {}, manifests, "NOT MEASURED: exactly one original raw run required"
    summary_path = paths[0]
    guarded.private(summary_path, 0o600)
    guarded.require(summary_path.stat().st_size <= 4 << 20, "oversized_run_summary")
    report = json.loads(summary_path.read_text())
    expected_settings = {"rows": 1000, "concurrency": [job["concurrency"]], "seed": 20261007,
                         "offered_per_second": job["rate"], "warmup_ns": 2_000_000_000,
                         "smoke_ns": 10_000_000_000, "measure_ns": 30_000_000_000,
                         "cases": ["capacity_" + job["workload"]]}
    guarded.require(all(report.get("settings", {}).get(key) == value for key, value in expected_settings.items()), "run_does_not_match_frozen_settings")
    scenarios = [scenario for scenario in report.get("scenarios", []) if scenario["scenario"] == "capacity_" + job["workload"]]
    if len(scenarios) != 1:
        return {}, manifests, "NOT MEASURED: scenario not completed"
    scenario = scenarios[0]
    raw_path = summary_path.parent / "operations.jsonl"
    operations, drops, samples, lifecycle, raw_rpc = {}, {}, [], [], []
    for record in records(raw_path):
        kind = record.get("type")
        if kind in ("operation", "client_admission_drop"):
            key = (record.get("phase"), record.get("concurrency"))
            selected = operations if kind == "operation" else drops
            selected.setdefault(key, []).append(record)
        elif kind == "resource_sample":
            samples.append(record)
        elif kind == "lifecycle":
            lifecycle.append(record)
        elif kind == "gateway_rpc":
            raw_rpc.append(record)
    journal_path = root / "guarded-events.jsonl"
    journal = list(records(journal_path))
    finals = [record for record in journal if record.get("event") == "final"]
    spec = guarded.read_spec(Path(job["spec"]))
    before = [record for record in journal if record.get("event") == "readonly_artifacts_before"]
    after = [record for record in journal if record.get("event") == "readonly_artifacts_after"]
    valid_guard = (len(finals) == 1 and finals[0].get("status") == "CHILD_COMPLETE_PENDING_VERIFIER"
                   and finals[0].get("clean_join") is True and finals[0].get("child_exit") == 0
                   and not finals[0].get("may_have_unknown") and not finals[0].get("confounded")
                   and len(before) == 1 and before[0].get("spec_sha256") == job["spec_sha256"]
                   and len(after) == 1 and after[0].get("sha256") == {**spec["sources"], **spec["artifacts"]}
                   and not any(record.get("event") == "STOP" for record in journal))
    external_busy = plan["config"]["external_busy"] or not valid_guard
    observation_path = root / "capacity-observations.json"
    observation = guarded.read_spec(observation_path) if observation_path.exists() else {}
    collector_status = root / "collector-status.json"
    launch = guarded.read_spec(Path(plan["config"]["launcher_report"]))
    linux_image = launch.get("image", {}).get("Os") == "linux"
    if linux_image:
        guarded.require(collector_status.exists() and (root / "capacity-trace.jsonl").exists() and bool(observation),
                        "Linux_original_collector_status_trace_and_observations_required_not_host_ps")
    if linux_image or collector_status.exists():
        from capacity_collect import normalize
        guarded.require(bool(observation) and normalize(plan, job) == observation, "collector_observations_do_not_reproduce_from_original_trace")
    if observation.get("resource_samples"):
        guarded.require(observation.get("run_id") == report["run_id"] and observation.get("container_name") == launch.get("container_name")
                        and bool(launch.get("container_name")), "container_resource_sample_ownership_mismatch")
        for sample in observation["resource_samples"]:
            guarded.require(sample.get("source") in ("container.ps", "container.proc.status"), "unqualified_container_resource_sample")
        samples.extend(observation["resource_samples"])
    cells = {}
    acceptance = plan["config"]["acceptance"]
    for phase in scenario.get("phases", []):
        if not phase["phase"].endswith("measure"):
            continue
        key = (phase["phase"], phase["concurrency"])
        cell = phase_cell(phase, scenario, operations.get(key, []), drops.get(key, []), samples,
                          acceptance, job["demand"], external_busy)
        cell.update({"rate": job["rate"], "concurrency": phase["concurrency"], "phase": phase["phase"],
                     "purpose": job.get("purpose"), "extension_round": job.get("extension_round"),
                     "effective_slots": phase.get("per_project_effective_slots", {}),
                     "expected_progress": plan["coverage"]["expected_progress"],
                     "admission_drop_classification": {key: cell.get("counters", {}).get(key) for key in ("client_drop", "client_expiry", "typed429", "other_determinate", "Unknown", "pending")},
                     "run_id": report["run_id"], "actual_host_capacity": report.get("actual_host_capacity"),
                     "source": report.get("source", {}),
                     "frozen_resource_limits": plan["config"]["resources"], "guard_valid": valid_guard,
                     "short_gate_not_benchmark": phase.get("short_gate_not_benchmark", True)})
        if phase.get("started_at_observed"):
            cell["rpc_measurement"] = rpc_measurement(samples, raw_rpc, phase, acceptance["rpc_metric_raw_count_tolerance"])
            if job["workload"] == "mixed" and cell["rpc_measurement"]["status"] != "MEASURED" and cell["status"] != "FAIL":
                cell["status"] = "NOT MEASURED"
                cell.setdefault("limitations", []).append("quiet mixed C1 requires actual actions delta / elapsed and raw RPC cross-check")
            identity = plan["identities"][job["topology"]]
            expected_projects = {identity["primary_project"]}
            expected_databases = len(identity["primary_databases"])
            expected_database_keys = {database_key(identity["primary_project"], database) for database in identity["primary_databases"]}
            if phase["phase"].startswith("isolated_noise"):
                expected_projects, expected_databases = {identity["noise_project"]}, 1
                expected_database_keys = {database_key(identity["noise_project"], identity["noise_database"])}
            elif phase["phase"].startswith("combined"):
                expected_projects.add(identity["noise_project"])
                expected_databases += 1
                expected_database_keys.add(database_key(identity["noise_project"], identity["noise_database"]))
            numerator, denominator = demand_fraction(job["topology"], phase["phase"])
            slots = phase_slots(job["topology"], phase["phase"], job["concurrency"])
            expected_slots = {project: slots // len(expected_projects) for project in expected_projects}
            observed = {database_key(operation["project"], operation["database"]) for operation in operations.get(key, [])}
            observed_slots = {project: len({operation["session_slot"] for operation in operations.get(key, []) if operation["project"] == project})
                              for project in expected_projects}
            total_offers = job["rate"] * 30
            primary_offers = total_offers // (10 if job["demand"] == "victim" else 2)
            expected_offers = ({identity["primary_project"]: primary_offers, identity["noise_project"]: total_offers - primary_offers}
                               if phase["phase"].startswith("combined") else {project: total_offers * numerator // denominator for project in expected_projects})
            proof_databases = {database_key(proof.get("project"), proof.get("database")) for proof in scenario.get("correctness", [])
                               if proof.get("phase") == phase["phase"] and proof.get("concurrency") == phase["concurrency"] and proof.get("status") == "PASS"}
            if (set(phase["per_project_offered"]) != expected_projects or phase["independently_active_databases"] != expected_databases
                    or phase.get("per_project_effective_slots") != expected_slots or observed_slots != expected_slots
                    or phase["per_project_offered"] != expected_offers or observed != expected_database_keys
                    or not observed.issubset(proof_databases)
                    or phase["concurrency"] != job["concurrency"] or phase.get("configured_offered_per_second") != job["rate"]
                    or phase["planned_offers"] != job["rate"] * 30 * numerator // denominator or phase["requested_duration_ns"] != 30_000_000_000):
                cell["status"] = "FAIL"
                cell.setdefault("issues", []).append("phase does not match frozen topology/concurrency/offered schedule")
        if cell["status"] != "NOT MEASURED" and phase.get("started_at_observed"):
            extra = supplemental({**phase, "observed_databases": sorted(observed), "observed_operations": operations.get(key, []),
                                  "observed_processes": cell.get("resources", {}).get("processes", {})}, report, observation, job["workload"], acceptance)
            if job["workload"] == "held_writer":
                terminal_counts = {terminal: sum(operation.get("transaction_terminal") == terminal and operation.get("acked")
                                                 for operation in operations.get(key, [])) for terminal in ("COMMIT", "ROLLBACK")}
                extra["acknowledged_terminals"] = terminal_counts
                if not all(terminal_counts.values()) or any(not all(sum(operation.get("project") == project and operation.get("transaction_terminal") == terminal and operation.get("acked")
                    for operation in operations.get(key, [])) for terminal in ("COMMIT", "ROLLBACK")) for project in phase.get("per_project_offered", {})):
                    extra["issues"].append("both COMMIT and ROLLBACK terminal ACKs required")
            if job["workload"] == "io_checkpoint":
                checkpoints = [record for record in lifecycle if record.get("scenario") == "checkpoint_reopen"
                               and timestamp(phase["started_at_observed"]) <= timestamp(record["started_at_observed"]) <= timestamp(phase["finished_at_observed"])
                               and record.get("failure") is None]
                extra["actual_checkpoint_ACKs"] = len(checkpoints)
                if len({json.dumps(record.get("database"), sort_keys=True) for record in checkpoints}) < phase["independently_active_databases"]:
                    extra["issues"].append("actual checkpoint completion ACK missing")
                io_bytes = sum(entry.get("after_bytes", 0) - entry.get("before_bytes", 0)
                               for entry in next((entry for entry in observation.get("phases", [])
                                                  if entry.get("phase") == phase["phase"] and entry.get("concurrency") == phase["concurrency"]), {}).get("io_samples", []))
                if io_bytes > plan["config"]["resources"]["io_budget_bytes"]:
                    extra["issues"].append("physical IO exceeded frozen per-phase observation budget")
            cell["competition_evidence"] = extra
            if extra["issues"]:
                cell["status"] = "FAIL"
            elif extra["missing"] and cell["status"] != "FAIL":
                cell["status"] = "NOT MEASURED"
        if key in cells:
            guarded.require(False, "duplicate_phase_measurement")
        cells[key] = cell
    for path in (summary_path, raw_path, journal_path, observation_path, root / "guard-spec.json", collector_status,
                 root / "capacity-trace.jsonl", root / "collector-ready.json"):
        if path.exists():
            manifests[str(path)] = {"sha256": digest(path), "bytes": path.stat().st_size}
    resource_path = root / "container-resources.jsonl"
    if resource_path.exists():
        manifests[str(resource_path)] = {"sha256": digest(resource_path), "bytes": resource_path.stat().st_size}
    return cells, manifests, None if report["status"] == "COMPLETE" and scenario["status"] == "COMPLETE" else "FAIL: run/scenario incomplete"


def analyze(plan):
    cells, evidence, errors = [], {}, []
    stop_path = Path(plan["config"]["output"]) / "matrix-stop.json"
    if stop_path.exists():
        stop = guarded.read_spec(stop_path)
        errors.append({"job": stop.get("job", "matrix"), "reason": "FAIL: first sticky matrix stop retained; no acceptance or replay"})
        evidence[str(stop_path)] = {"sha256": digest(stop_path), "bytes": stop_path.stat().st_size}
    for job in plan["jobs"]:
        try:
            measured, manifest, error = ({}, {}, None) if job["structural_unmeasured"] else collect_job(job, plan)
            evidence.update(manifest)
        except (ValueError, OSError, KeyError, TypeError) as failure:
            measured, error = {}, "NOT MEASURED: malformed/missing evidence (" + type(failure).__name__ + ")"
        if error:
            errors.append({"job": job["id"], "reason": error})
        for phase in expected_phases(job["topology"]):
            for concurrency in [job["concurrency"]]:
                reason = "too few sessions for independently active databases" if job["structural_unmeasured"] else "missing expected phase"
                cell = measured.get((phase, concurrency), {"status": "NOT MEASURED", "reason": error or reason})
                cells.append({"job": job["id"], "topology": job["topology"], "workload": job["workload"],
                              "purpose": job.get("purpose"), "extension_round": job.get("extension_round"),
                              "phase": phase, "concurrency": concurrency, "rate": job["rate"], **cell})
    groups = {}
    for cell in cells:
        if cell.get("purpose") == "hot_knee":
            key = (cell["topology"], cell["workload"], cell["phase"], cell["concurrency"])
            if cell.get("extension_round") is None or cell.get("status") != "NOT MEASURED" or cell.get("run_id"):
                groups.setdefault(key, []).append(cell)
    knees = [{"topology": key[0], "workload": key[1], "phase": key[2], "concurrency": key[3],
              **knee(points, plan["config"]["acceptance"])} for key, points in groups.items()]
    counts = {status: sum(cell["status"] == status for cell in cells) for status in ("MEASURED", "FAIL", "NOT MEASURED")}
    c1 = [cell for cell in cells if cell["phase"] == "combined_measure" and cell["concurrency"] == 1]
    required = [cell for cell in cells if cell.get("extension_round") is None or cell.get("run_id")]
    required_jobs = {job["id"] for job in plan["jobs"] if job.get("extension_round") is None}
    errors_required = [error for error in errors if error["job"] in required_jobs or error["reason"].startswith("FAIL")]
    knee_complete = len(knees) == 8 and all(entry["status"] == "OBSERVED FUNCTIONAL KNEE" for entry in knees)
    complete = bool(required) and all(cell["status"] == "MEASURED" for cell in required) and not errors_required and not stop_path.exists() and knee_complete
    comparisons = []
    lookup = {(cell["topology"], cell["workload"], cell["rate"], cell["concurrency"], cell["phase"]): cell for cell in cells}
    for cell in cells:
        if cell["phase"] != "combined_measure":
            continue
        identity = plan["identities"][cell["topology"]]
        tenants = {}
        for project, phase in ((identity["primary_project"], "isolated_primary_measure"), (identity["noise_project"], "isolated_noise_measure")):
            baseline = lookup.get((cell["topology"], cell["workload"], cell["rate"], cell["concurrency"], phase), {})
            isolated = baseline.get("projects", {}).get(project, {})
            combined = cell.get("projects", {}).get(project, {})
            before, after = isolated.get("ack_fraction"), combined.get("ack_fraction")
            tenants[project] = {"isolated_ACK_fraction": before, "combined_ACK_fraction": after,
                                "ACK_fraction_delta": after - before if before is not None and after is not None else None,
                                "isolated_latency": isolated.get("ACK_latency"), "combined_latency": combined.get("ACK_latency"),
                                "isolated_progress": isolated.get("progress"), "combined_progress": combined.get("progress")}
        comparisons.append({"topology": cell["topology"], "workload": cell["workload"], "rate": cell["rate"], "concurrency": cell["concurrency"],
                            "status": "MEASURED" if cell["status"] == "MEASURED" and all(entry["isolated_ACK_fraction"] is not None for entry in tenants.values()) else "NOT MEASURED",
                            "tenants": tenants, "scope": "matched per-project effective slots and offered demand; distinct baseline oracle per point; not isolated performance causality"})
    status = "READY FOR INDEPENDENT ACCEPTANCE" if complete else "FAIL" if counts["FAIL"] or any(error["reason"].startswith("FAIL") for error in errors) else "NOT MEASURED"
    return {"version": 1, "task": "D05/Q02", "status": status,
            "scope": plan["scope"], "performance_pass": False, "G02": "NOT PASS", "no_starvation_guarantee": False,
            "acceptance": plan["config"]["acceptance"], "counts": counts, "cells": cells, "knees": knees,
            "coverage": {**plan["coverage"], "selected_status": status,
                         "knee_acceptance": "MEASURED" if knee_complete else "NOT MEASURED",
                         "observed_concurrency": sorted({cell["concurrency"] for cell in cells if cell["status"] == "MEASURED"}),
                         "omitted_combinations": omitted_cells(plan["config"], plan["jobs"], TOPOLOGIES)},
            "interference_comparisons": comparisons,
            "safe_envelope": safe_envelope(cells, plan, status),
            "frozen_plan": {"path": plan["plan_path"], "sha256": digest(plan["plan_path"])},
            "approval_capsule_contract": {"issuer": "Main independent verification only; tool never writes PASS capsule",
                                          "required_fields": ["image_id", "writer_admission", "rpc_rate_safe", "source", "result_sha256", "verified_by", "status"],
                                          "rate_rule": "approved rpc_rate_safe <= measured safe_envelope.safe_rpc_per_second; Soak needs >=1.9 RPC/s plus frozen 20% extra margin"},
            "C1_boundary": {"status": "NOT MEASURED", "reason": "one total session cannot activate independent projects; never noisy PASS", "cells": len(c1)},
            "errors": errors, "evidence_manifest": evidence,
            "independent_review_required": True,
            "limitations": ["PS CPU/RSS are observations, not isolated G02 performance or native queue/IO attribution",
                            "effective FIFO/owner reservation/physical IO require fixture-scoped instrumented raw evidence",
                            "NOT FOUND knee means no knee in frozen range, never capacity PASS"]}
