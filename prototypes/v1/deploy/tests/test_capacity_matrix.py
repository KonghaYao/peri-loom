import copy
import datetime
import argparse
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch


DEPLOY = Path(__file__).resolve().parents[1]
STRESS = DEPLOY.parent / "stress"
sys.path.insert(0, str(STRESS))
from capacity_evidence import analyze, collect_job, supplemental
from capacity_metrics import DEFAULT_ACCEPTANCE, counters, knee, phase_cell, progress, quantiles, writer_profiles
from capacity_plan import TOPOLOGIES, digest, freeze, read_plan, template, write_private
from capacity_envelope import RPC_DEFINITION, rpc_measurement, safe_envelope, validate_capacity_capsule
from capacity_coverage import phase_slots
from capacity_fifo import database_key, effective_fifo
from capacity_competition import io_overlap
from capacity_collect import capture, normalize
from guard_profiles import parameters
import run_guarded as guarded

MODULE_SPEC = importlib.util.spec_from_file_location("capacity_matrix", DEPLOY / "capacity-matrix.py")
matrix = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(matrix)


def operation(second=1, project="primary", ack=True, failure=None):
    return {"type": "operation", "scenario": "capacity_read", "phase": "isolated_primary_measure", "concurrency": 1,
            "business_operation_id": "UNIT-" + project + "-" + str(second),
            "project": project, "acked": ack, "correct": ack, "failure": failure,
            "database": {"namespace": {"tenant": project, "database": "db1"}, "branch": "main"}, "session_slot": 0,
            "http_workflow_ns": 10, "client_queue_ns": 5,
            "finished_at_observed": f"2026-10-08T00:00:{second:02d}Z"}


def sample(second=5):
    return {"type": "resource_sample", "observed_at": f"2026-10-08T00:00:{second:02d}Z", "processes_status": "available",
            "processes": [{"role": "worker", "pid": 42, "status": "available", "rss_bytes": 100,
                           "cpu_percent_ps": 5, "cpu_seconds_ps": second, "process_start_identity": "frozen"}],
            "management_metrics_status": "available", "management_metrics": {
                "projects": {project: {"writer_admission": True} for project in ("primary", "noise")},
                "native": {"workers": {"worker": {"writer_profile": True}}}}}


def phase():
    return {"phase": "isolated_primary_measure", "concurrency": 1, "status": "COMPLETE",
            "started_at_observed": "2026-10-08T00:00:00Z", "finished_at_observed": "2026-10-08T00:00:30Z",
            "offered": 30, "client_admission_dropped": 0, "client_queue_expired": 0,
            "acked_workflows": 30, "attempted": 30, "cleanup_unknown": 0, "correctness_failures": 0,
            "completed_workflows": 30, "unknown": 0, "configured_offered_per_second": 1,
            "per_project_offered": {"primary": 30}, "requested_duration_ns": 30_000_000_000,
            "per_project_effective_slots": {"primary": 1},
            "independently_active_databases": 1, "scheduled_offers_not_emitted": 0, "operation_cap_reached": False,
            "observed_offered_per_second": 1, "acked_per_second_including_drain": 1,
            "client_queue_latency": quantiles([5] * 30), "all_workflow_latency": quantiles([10] * 30),
            "acked_workflow_latency": quantiles([10] * 30), "elapsed_ns": 30_000_000_000,
            "offering_elapsed_ns": 30_000_000_000, "planned_offers": 30, "short_gate_not_benchmark": False}


def scenario():
    return {"scenario": "capacity_read", "status": "COMPLETE", "phases": [phase()],
            "correctness": [{"project": "primary", "database": operation()["database"], "phase": "isolated_primary_measure", "concurrency": 1,
                             "status": "PASS", "expected_ledger_hash": "ledger", "actual_ledger_hash": "ledger"}]}


class MetricsTests(unittest.TestCase):
    def test_fifo_is_by_actual_database_queue_enqueue_order_not_global_ticket_id(self):
        tickets, databases = [], []
        for project in ("primary", "noise"):
            database = operation(project=project)["database"]
            databases.append(database_key(project, database))
            for order in (1, 2):
                tickets.append({"ticket": 99 - order, "project": project, "database": database,
                                "queue_id": "same-local-id", "queue_epoch": "start", "session": str(order),
                                "holder_session": "1" if order == 2 else None,
                                "holder_release_at": "2026-10-08T00:00:02Z" if order == 2 else None,
                                "enqueued_at": "2026-10-08T00:00:01Z" if order == 2 else "2026-10-08T00:00:00Z",
                                "admitted_at": "2026-10-08T00:00:02Z" if order == 2 else "2026-10-08T00:00:00Z",
                                "enqueue_order": order, "valid": True, "invalidated": False, "admitted_order": order})
        self.assertEqual(effective_fifo(tickets, databases)[0], "PASS")
        serial = copy.deepcopy(tickets)
        for ticket in serial:
            ticket["holder_session"] = None
        self.assertEqual(effective_fifo(serial, databases)[0], "NOT MEASURED")
        self.assertEqual(effective_fifo(tickets[:2], databases)[0], "NOT MEASURED")
        tickets[0]["admitted_order"], tickets[1]["admitted_order"] = 2, 1
        self.assertEqual(effective_fifo(tickets, databases)[0], "FAIL")
        tickets[0]["invalidated"] = True
        self.assertEqual(effective_fifo(tickets, databases)[0], "NOT MEASURED")

    def test_physical_io_requires_attributed_cross_tenant_actual_overlap(self):
        entries, operations = [], []
        for index, project in enumerate(("primary", "noise")):
            workflow = {**operation(4, project), "business_operation_id": project,
                        "started_at_observed": "2026-10-08T00:00:00Z"}
            operations.append(workflow)
            kinds = ("write", "checkpoint") if index == 0 else ("checkpoint", "write")
            entries.append({"project": project, "database": workflow["database"], "counter_scope": project + "-native-db-io", "source": "native.io_counter",
                            "pid": 42 + index, "process_start_identity": "fixture-start", "before_bytes": 0, "after_bytes": 20,
                            "started_at": "2026-10-08T00:00:00Z", "finished_at": "2026-10-08T00:00:04Z",
                            "activities": [{"business_operation_id": project, "kind": kind, "before_bytes": offset * 10,
                                            "after_bytes": offset * 10 + 10, "started_at": f"2026-10-08T00:00:0{offset + 1}Z",
                                            "finished_at": f"2026-10-08T00:00:0{offset + 2}Z"} for offset, kind in enumerate(kinds)]})
        selected = {"phase": "combined_measure", "observed_operations": operations,
                    "observed_processes": {"worker:42": {"start_identity": "fixture-start"}, "worker:43": {"start_identity": "fixture-start"}}}
        self.assertEqual(io_overlap(entries, selected)[0], "PASS")
        shared = copy.deepcopy(entries)
        for key in ("counter_scope", "pid", "process_start_identity"):
            shared[1][key] = shared[0][key]
        self.assertEqual(io_overlap(shared, selected)[0], "NOT MEASURED")
        for activity in entries[1]["activities"]:
            activity["started_at"] = "2026-10-08T00:00:03Z"
            activity["finished_at"] = "2026-10-08T00:00:04Z"
        self.assertEqual(io_overlap(entries, selected)[0], "NOT MEASURED")

    def test_actual_go_and_native_writer_profile_flags_are_required(self):
        self.assertEqual(writer_profiles([sample()], phase(), ["primary"])["status"], "PASS")
        disabled = sample()
        disabled["management_metrics"]["native"]["workers"]["worker"]["writer_profile"] = False
        self.assertEqual(writer_profiles([disabled], phase(), ["primary"])["status"], "FAIL")
        disabled["management_metrics"]["native"]["workers"] = {}
        self.assertEqual(writer_profiles([disabled], phase(), ["primary"])["status"], "NOT MEASURED")

    def test_counter_conservation_separates_typed429_unknown_and_other(self):
        operations = [operation(), operation(ack=False, failure={"class": "admission", "http_status": 429}),
                      operation(ack=False, failure={"class": "unknown", "unknown": True, "http_status": 429}),
                      operation(ack=False, failure={"class": "sql_error", "http_status": 400})]
        result = counters(7, 2, operations, 1)
        self.assertTrue(result["conserved"])
        self.assertEqual([result[key] for key in ("ACK", "typed429", "Unknown", "other_determinate", "pending")], [1, 1, 1, 1, 0])
        self.assertFalse(counters(6, 2, operations, 1)["conserved"])
        pending = counters(1, 0, [operation(ack=False)])
        self.assertEqual(pending["pending"], 1)
        self.assertTrue(pending["conserved"])

    def test_empty_quantiles_are_unmeasured_not_zero(self):
        self.assertIsNone(quantiles([])["p99_ns"])
        self.assertEqual(quantiles([1, 2, 3])["p95_ns"], 3)

    def test_single_ack_does_not_prove_finite_progress(self):
        result = progress([operation(1)], phase()["started_at_observed"], 30, DEFAULT_ACCEPTANCE)
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["fixed_window_ACK"], [1, 0, 0, 0, 0, 0])

    def measured(self, **changes):
        selected = phase()
        selected.update(changes)
        return phase_cell(selected, scenario(), [operation(second) for second in range(1, 31)], [], [sample(5), sample(25)], DEFAULT_ACCEPTANCE, "equal")

    def test_full_phase_requires_raw_counts_ledger_progress_resources(self):
        self.assertEqual(self.measured()["status"], "MEASURED")
        self.assertEqual(self.measured(acked_workflows=29)["status"], "FAIL")
        self.assertEqual(self.measured(independently_active_databases=2)["status"], "FAIL")
        self.assertEqual(self.measured(cleanup_unknown=1)["status"], "FAIL")
        self.assertEqual(self.measured(scheduled_offers_not_emitted=1)["status"], "NOT MEASURED")

    def test_external_busy_missing_resources_and_skip_never_pass(self):
        for samples, busy in (([], False), ([sample()], True)):
            result = phase_cell(phase(), scenario(), [operation(second) for second in range(1, 31)], [], samples, DEFAULT_ACCEPTANCE, "equal", busy)
            self.assertEqual(result["status"], "NOT MEASURED")
        self.assertEqual(phase_cell({"status": "SKIP", "reason": "C1"}, {}, [], [], [], DEFAULT_ACCEPTANCE, "equal")["status"], "NOT MEASURED")

    def test_unknown_and_missing_ledger_fail(self):
        operations = [operation(second) for second in range(1, 30)] + [operation(30, ack=False, failure={"unknown": True})]
        result = phase_cell(phase(), {}, operations, [], [sample()], DEFAULT_ACCEPTANCE, "equal")
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["counters"]["Unknown"], 1)

    def test_noisy_lowrate_victim_bound_and_equal_jain(self):
        primary_ops = [operation(second) for second in range(1, 31)]
        noise_ops = [operation(second, "noise") for second in range(1, 31)]
        selected = phase()
        selected.update(phase="combined_measure", concurrency=2, offered=60, attempted=60, acked_workflows=60,
                        completed_workflows=60,
                        per_project_offered={"primary": 30, "noise": 30}, independently_active_databases=2)
        proof = scenario()
        proof["correctness"] = [{**proof["correctness"][0], "phase": "combined_measure", "concurrency": 2, "project": project} for project in ("primary", "noise")]
        result = phase_cell(selected, proof, primary_ops + noise_ops, [], [sample(5), sample(25)], DEFAULT_ACCEPTANCE, "equal")
        self.assertEqual(result["fairness"]["demand_normalized_jain"], 1)
        self.assertEqual(result["status"], "MEASURED")
        selected.update(offered=300, client_admission_dropped=240, per_project_offered={"primary": 60, "noise": 240})
        drops = [{"project": "primary"}] * 30 + [{"project": "noise"}] * 210
        result = phase_cell(selected, proof, primary_ops + noise_ops, drops, [sample(5), sample(25)], DEFAULT_ACCEPTANCE, "victim")
        self.assertEqual(result["status"], "FAIL")
        self.assertIn("victim ACK/offered bound failed", result["issues"])

    def test_knee_requires_two_consecutive_plateaus_and_pressure_growth(self):
        def point(rate, ack, queue, typed=0, status="MEASURED"):
            return {"rate": rate, "status": status, "ACK_rate_including_drain": ack, "observed_offered_rate": rate,
                    "queue_p95_ns": queue, "counters": {"typed429": typed, "client_drop": 1, "client_expiry": 0}, "resources": {"status": "MEASURED"}}
        points = [point(25, 20, 1), point(50, 21, 2), point(100, 22, 3)]
        self.assertEqual(knee(points[:2], DEFAULT_ACCEPTANCE)["status"], "NOT FOUND")
        self.assertEqual(knee(points, DEFAULT_ACCEPTANCE)["status"], "OBSERVED FUNCTIONAL KNEE")
        self.assertIn("service limiter NOT PROVEN", knee(points, DEFAULT_ACCEPTANCE)["mechanism"])
        points[1]["status"] = "NOT MEASURED"
        self.assertEqual(knee(points, DEFAULT_ACCEPTANCE)["status"], "NOT MEASURED")
        constant = [point(25, 20, 1), point(50, 21, 1), point(100, 22, 1)]
        self.assertEqual(knee(constant, DEFAULT_ACCEPTANCE)["status"], "NOT FOUND")

    def test_fifo_cleanup_and_physical_io_are_not_assumed(self):
        selected = {**phase(), "phase": "combined_measure", "concurrency": 8,
                    "observed_databases": [database_key("primary", operation()["database"])]}
        result = supplemental(selected, {"run_id": "run"}, {}, "held_writer", DEFAULT_ACCEPTANCE)
        self.assertEqual(result["effective_fifo"], "NOT MEASURED")
        self.assertEqual(len(result["missing"]), 2)
        observation = {"run_id": "run", "phases": [{"phase": "combined_measure", "concurrency": 8,
                      "tickets": [{"ticket": 99 - order, "project": "primary", "database": operation()["database"], "queue_id": "db-queue",
                                   "queue_epoch": "native-start", "session": str(order), "enqueue_order": order,
                                   "holder_session": "1" if order == 2 else None,
                                   "holder_release_at": "2026-10-08T00:00:02Z" if order == 2 else None,
                                   "enqueued_at": "2026-10-08T00:00:01Z" if order == 2 else "2026-10-08T00:00:00Z",
                                   "admitted_at": "2026-10-08T00:00:02Z" if order == 2 else "2026-10-08T00:00:00Z",
                                   "valid": True, "invalidated": False, "admitted_order": 3 - order} for order in (1, 2)],
                      "owner_cleanup": [{"project": "primary", "database": operation()["database"], "reserved_slots": 1,
                                         "session": "1", "started_at": "2026-10-08T00:00:01Z", "finished_at": "2026-10-08T00:00:02Z",
                                         "terminal": terminal, "terminal_acked": True, "elapsed_seconds": 1} for terminal in ("COMMIT", "ROLLBACK")]}]}
        result = supplemental(selected, {"run_id": "run"}, observation, "held_writer", DEFAULT_ACCEPTANCE)
        self.assertEqual(result["effective_fifo"], "FAIL")
        self.assertEqual(result["owner_reserved_cleanup"], "PASS")
        self.assertEqual(supplemental(selected, {"run_id": "run"}, {}, "io_checkpoint", DEFAULT_ACCEPTANCE)["physical_io_competition"], "NOT MEASURED")


class PlanTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.root.chmod(0o700)
        self.config = template()
        binary = self.root / "peri-stress"
        binary.write_bytes(b"mock binary never executed")
        binary.chmod(0o500)
        source = self.root / "deployment.json"
        write_private(source, {"cpu": 2, "memory": 536870912})
        joint = self.root / "joint-pass.json"
        write_private(joint, {"mock": "not a real gate"})
        output = self.root / "output"
        output.mkdir(mode=0o700)
        self.config.update(binary=str(binary), joint_pass=str(joint), output=str(output), rates=[5, 10, 25])
        launch = self.root / "launcher-report.json"
        write_private(launch, {"writer_admission": True, "image": {"Id": "sha256:" + "a" * 64, "Os": "linux"},
                              "container_id": "mock-container-id", "source_hash": "mock-source",
                              "required_native_roles": ["authority", "worker-a", "worker-b", "platform", "launcher"],
                              "container_name": "owned-mock", "resources": {"Memory": 2147483648, "NanoCpus": 2000000000}})
        self.config["launcher_report"] = str(launch)
        self.config["guard"].update(artifacts={str(binary): digest(binary)}, sources={str(source): digest(source)})
        self.config["connections"] = {}
        for name in TOPOLOGIES:
            target = {"endpoint": "http://127.0.0.1:1", "project": "primary", "api_key": "do-not-serialize-primary-key",
                      "database": {"namespace": {"tenant": "primary", "database": "db1"}, "branch": "main"}, "fixture_only": True,
                      "worker_ids": ["worker-a", "worker-b"], "source_root": str(self.root)}
            if name.endswith("many"):
                target["databases"] = [{"namespace": {"tenant": "primary", "database": "db2"}, "branch": "main"}]
            if not name.startswith("single"):
                target["noisy_tenant"] = {"endpoint": target["endpoint"], "project": "noise", "api_key": "do-not-serialize-noise-key",
                                         "database": {"namespace": {"tenant": "noise", "database": "db3"}, "branch": "main"}}
            path = self.root / (name + ".json")
            write_private(path, target)
            self.config["connections"][name] = str(path)
        self.config_path = self.root / "matrix-config.json"
        write_private(self.config_path, self.config)
        self.plan_path = self.root / "matrix-plan.json"

    def frozen(self):
        return freeze(self.config_path, self.plan_path)

    def test_freeze_is_offline_complete_private_and_guard_compatible(self):
        with patch.object(guarded.OwnedProcess, "__init__", side_effect=AssertionError("no spawn")):
            plan = self.frozen()
            self.assertEqual(len(plan["jobs"]), 79)
            self.assertEqual(sum(job["extension_round"] is None for job in plan["jobs"]), 63)
            self.assertEqual(plan["coverage"]["selected_base_measurement_cells"], 127)
            self.assertEqual(plan["concurrency"], [1, 2, 4, 8])
            self.assertEqual(read_plan(self.plan_path)["jobs"], plan["jobs"])
        self.assertNotIn("do-not-serialize", self.plan_path.read_text())
        self.assertEqual(self.plan_path.stat().st_mode & 0o777, 0o600)
        profile = parameters("capacity-held_writer-victim-25-c1")
        self.assertEqual(profile["victim-every"], "10")
        self.assertEqual(parameters("mixed")["concurrency"], "1,4")

    def test_approved_coverage_has_all_pairs_concurrency_and_both_fairness_tiers(self):
        plan = self.frozen()
        base = [job for job in plan["jobs"] if job["extension_round"] is None]
        self.assertEqual(sum(1 if job["topology"].startswith("single") else 3 for job in base), 127)
        self.assertEqual({job["concurrency"] for job in base}, {1, 2, 4, 8})
        for topology in TOPOLOGIES:
            for workload in self.config["workloads"]:
                selected = [job for job in base if job["topology"] == topology and job["workload"] == workload]
                self.assertTrue(selected)
                if not topology.startswith("single"):
                    self.assertEqual({job["purpose"] for job in selected}, {"fairness_safe", "fairness_interference"})
                    self.assertEqual({job["rate"] for job in selected}, {5, 25})
                    for job in selected:
                        self.assertEqual(phase_slots(topology, "isolated_primary_measure", job["concurrency"]), job["concurrency"] // 2)
                        self.assertEqual(phase_slots(topology, "combined_measure", job["concurrency"]), job["concurrency"])
        self.assertEqual({job["extension_round"] for job in plan["jobs"] if job["extension_round"]}, {1, 2})
        self.assertEqual(plan["coverage"]["stage_seconds"], 127 * 42)

    def test_optional_knee_extensions_are_not_retries_and_stop_after_found(self):
        plan = self.frozen()
        job = next(job for job in plan["jobs"] if job["extension_round"] == 2)
        invoked = []
        def measured(selected, frozen):
            rate = selected["rate"]
            point = {"status": "MEASURED", "rate": rate, "observed_offered_rate": rate,
                     "ACK_rate_including_drain": 10, "queue_p95_ns": rate,
                     "counters": {"typed429": 0, "client_drop": 0, "client_expiry": 0}, "resources": {}}
            return {("isolated_primary_measure", selected["concurrency"]): point}, {}, None
        with patch.object(matrix, "collect_job", side_effect=measured):
            self.assertEqual(matrix.run(plan, selected=job["id"], invoke=lambda selected: invoked.append(selected)), 0)
        self.assertFalse(invoked)
        with patch.object(matrix, "collect_job", return_value=({}, {}, "FAIL: Unknown")):
            with self.assertRaisesRegex(ValueError, "no_FAIL_Unknown_retry"):
                matrix.run(plan, selected=job["id"], invoke=lambda selected: invoked.append(selected))
        self.assertFalse(invoked)

    def test_changed_fairness_rate_cannot_wash_an_overload_failure(self):
        self.config["fairness_rates"]["interference"] = 5
        self.config_path.unlink()
        write_private(self.config_path, self.config)
        with self.assertRaisesRegex(ValueError, "Main_approved"):
            self.frozen()

    def test_owned_launcher_factory_explicitly_enables_writer_admission(self):
        factory = Mock()
        arguments = argparse.Namespace(writer_admission=False, image="frozen", backend="local")
        matrix.new_verification(arguments, factory)
        self.assertIs(factory.call_args.args[0].writer_admission, True)
        self.assertIs(arguments.writer_admission, False)
        factory.return_value.prepare.assert_not_called()

    def test_mutated_threshold_or_artifact_is_rejected(self):
        altered = copy.deepcopy(self.config)
        altered["acceptance"]["victim_ack_fraction_min"] = 0.1
        self.config_path.unlink()
        write_private(self.config_path, altered)
        with self.assertRaisesRegex(ValueError, "acceptance"):
            self.frozen()

    def test_changed_private_connection_blocks_even_analysis(self):
        self.frozen()
        path = Path(self.config["connections"]["single_hot"])
        path.write_text(path.read_text() + "\n")
        with self.assertRaisesRegex(ValueError, "private_input_changed"):
            read_plan(self.plan_path)

    def test_all_unrun_points_explicit_not_measured(self):
        result = analyze(self.frozen())
        self.assertEqual(result["status"], "NOT MEASURED")
        self.assertEqual(result["counts"]["NOT MEASURED"], 143)
        self.assertEqual(result["coverage"]["full_cartesian_status"], "NOT MEASURED")
        self.assertTrue(result["coverage"]["omitted_combinations"])
        self.assertEqual(result["C1_boundary"]["status"], "NOT MEASURED")
        self.assertFalse(result["performance_pass"])
        self.assertEqual(result["G02"], "NOT PASS")
        self.assertEqual(result["safe_envelope"]["status"], "NOT MEASURED")
        self.assertIsNone(result["safe_envelope"]["safe_rpc_per_second"])
        schema = json.loads((STRESS / "capacity-result.schema.json").read_text())
        self.assertEqual(set(result), set(schema["required"]))
        self.assertEqual(set(result["safe_envelope"]), set(schema["$defs"]["safe_envelope"]["required"]))

    def test_serial_stop_retains_first_exit_no_retry(self):
        plan = self.frozen()
        called = []
        def fail(job):
            called.append(job["id"])
            return 7
        self.assertEqual(matrix.run(plan, invoke=fail), 1)
        self.assertEqual(len(called), 1)
        stop = json.loads((Path(plan["config"]["output"]) / "matrix-stop.json").read_text())
        self.assertEqual(stop["original_exit"], 7)
        with self.assertRaisesRegex(ValueError, "first_matrix_stop"):
            matrix.run(plan, invoke=fail)
        self.assertEqual(len(called), 1)
        result = analyze(plan)
        self.assertEqual(result["status"], "FAIL")
        self.assertIn(str(Path(plan["config"]["output"]) / "matrix-stop.json"), result["evidence_manifest"])

    def test_explicit_sidecar_job_selection_and_verification_fail_closed(self):
        plan = self.frozen()
        selected = matrix.next_job(plan)
        self.assertEqual(selected["job"], plan["jobs"][0]["id"])
        output = Path(selected["output"])
        write_private(output / "collector-status.json", {"status": "NOT MEASURED"})
        with self.assertRaisesRegex(ValueError, "pending_verification"):
            matrix.next_job(plan)
        with patch.object(matrix, "collect_job", side_effect=ValueError("invalid source")):
            self.assertEqual(matrix.verify_job(plan, selected["job"])["status"], "NOT MEASURED")
        with self.assertRaisesRegex(ValueError, "first_matrix_stop"):
            matrix.next_job(plan)

    def test_sealed_plan_cannot_change_after_freeze(self):
        self.frozen()
        self.plan_path.write_text(self.plan_path.read_text() + "\n")
        with self.assertRaisesRegex(ValueError, "frozen_plan_changed"):
            read_plan(self.plan_path)

    def test_original_raw_and_guard_are_required(self):
        plan = self.frozen()
        job = next(job for job in plan["jobs"] if job["id"] == "single_hot-read-5-c1")
        output = Path(job["output"])
        raw = output / "run_mock"
        raw.mkdir(mode=0o700)
        report = {"run_id": "mock", "status": "COMPLETE", "settings": {
            "rows": 1000, "concurrency": [1], "seed": 20261007,
            "offered_per_second": 5, "warmup_ns": 2_000_000_000, "smoke_ns": 10_000_000_000,
            "measure_ns": 30_000_000_000, "cases": ["capacity_read"]}, "scenarios": [scenario()]}
        report["scenarios"][0]["phases"][0].update(offered=150, attempted=150, acked_workflows=150, completed_workflows=150,
                                                  per_project_offered={"primary": 150}, planned_offers=150,
                                                  observed_offered_per_second=5, acked_per_second_including_drain=5,
                                                  configured_offered_per_second=5)
        write_private(raw / "run.json", report)
        with os.fdopen(os.open(raw / "operations.jsonl", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as stream:
            for index, record in enumerate([*[operation(second) for second in range(1, 31) for repeat in range(5)], sample(5), sample(25)]):
                if record["type"] == "operation":
                    record["business_operation_id"] = "UNIT-" + str(index)
                stream.write(json.dumps(record) + "\n")
        spec = json.loads(Path(job["spec"]).read_text())
        events = [{"event": "readonly_artifacts_before", "spec_sha256": job["spec_sha256"]},
                  {"event": "readonly_artifacts_after", "sha256": {**spec["sources"], **spec["artifacts"]}},
                  {"event": "final", "status": "CHILD_COMPLETE_PENDING_VERIFIER",
                  "clean_join": True, "child_exit": 0, "may_have_unknown": False, "confounded": False}]
        with os.fdopen(os.open(output / "guarded-events.jsonl", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as stream:
            for event in events:
                stream.write(json.dumps(event) + "\n")
        with self.assertRaisesRegex(ValueError, "Linux_original_collector"):
            collect_job(job, plan)
        self.write_mock_collector(plan, job)
        cells, manifest, error = collect_job(job, plan)
        self.assertIsNone(error)
        self.assertEqual(cells[("isolated_primary_measure", 1)]["status"], "MEASURED")
        self.assertGreaterEqual(len(manifest), 8)
        events[-1]["confounded"] = True
        (output / "guarded-events.jsonl").write_text("\n".join(json.dumps(event) for event in events) + "\n")
        cells, _, _ = collect_job(job, plan)
        self.assertEqual(cells[("isolated_primary_measure", 1)]["status"], "NOT MEASURED")

    def test_rpc_metrics_are_actual_deltas_not_business_conversion(self):
        first, last = sample(2), sample(28)
        first["management_metrics"].update(actions_total={"query": 10}, errors_total={})
        last["management_metrics"].update(actions_total={"query": 140}, errors_total={})
        raw = [{"method": "POST", "path": "/v1/sql/query", "envelope_ok": True, "failure": None,
                "finished_at_observed": operation(second)["finished_at_observed"]}
               for second in range(1, 31) for repeat in range(5)]
        result = rpc_measurement([first, last], raw, phase(), 2)
        self.assertEqual(result["status"], "MEASURED")
        self.assertEqual(result["observed_rpc_per_second"], 5)
        self.assertEqual(result["actions_delta"]["query"], 130)
        self.assertEqual(rpc_measurement([sample(2), sample(28)], raw, phase(), 2)["status"], "NOT MEASURED")
        last["management_metrics"]["actions_total"]["query"] = 2
        self.assertEqual(rpc_measurement([first, last], raw, phase(), 2)["status"], "NOT MEASURED")

    def write_mock_collector(self, plan, job):
        output = Path(job["output"])
        launch = json.loads(Path(plan["config"]["launcher_report"]).read_text())
        base = int(datetime.datetime(2026, 10, 8, tzinfo=datetime.timezone.utc).timestamp() * 1000000)
        database = plan["identities"][job["topology"]]["primary_databases"][0]
        values, resources, times = [], [], []
        for second in [-1, 5, 25, 31]:
            observed = datetime.datetime.fromtimestamp(base / 1000000 + second, datetime.timezone.utc).isoformat()
            source = {"pid": 42, "startUnixMicros": base - 10000000, "generation": "UNIT ONLY", "wallUnixMicros": base + second * 1000000,
                      "monotonicMicros": (second + 2) * 1000000}
            ring = {"queueInstance": "unit-queue", "databaseId": database, "epoch": 1, "scope": "actualWriterAdmission",
                    "status": "available", "lost": 0, "dropped": 0, "sequence": 0, "pendingMessages": 0, "events": []}
            trace = {"schemaVersion": 1, "enabled": True, "capacity": 1024, "source": source,
                     "status": "available", "databases": [ring], "namespaces": []}
            other = copy.deepcopy(trace)
            other["source"].update(pid=43, generation="UNIT-worker-b")
            other["databases"] = []
            authority = copy.deepcopy(other)
            authority["source"].update(pid=40, generation="UNIT-authority")
            values.append({"native": {"workers": {"worker-a": {"capacity_trace": trace}, "worker-b": {"capacity_trace": other}},
                                     "authority": {"capacity_trace": authority}}})
            resources.append({"schema_version": 1, "source": "container.proc", "observed_at": observed,
                "container_id": launch["container_id"], "container_name": launch["container_name"], "image_id": launch["image"]["Id"],
                "source_hash": launch["source_hash"], "running": True, "oom_killed": False, "exit_code": None,
                "boot_id": "UNIT ONLY", "page_size_bytes": 4096, "clock_ticks_per_second": 100,
                "processes": [{"role": role, "pid": pid, "start_ticks": 1000, "rss_pages": 10, "cpu_ticks": 100 + second + 1,
                               "state": "S", "uids": [10001] * 4}
                              for role, pid in zip(launch["required_native_roles"], [40, 42, 43, 44, 45])]})
            times.extend([observed] * 3)
        stop = output / "collector-stop.json"
        reader = Mock()
        reader.read.side_effect = values
        def advance(duration):
            if reader.read.call_count == 3:
                write_private(stop, {})
        with patch("capacity_collect.now", side_effect=times):
            result = capture(plan, job, stop, reader=reader, sleep=advance, resource_reader=Mock(side_effect=resources))
        self.assertEqual(result["status"], "COMPLETE", result)
        write_private(output / "capacity-observations.json", normalize(plan, job))

    def write_quiet_raw(self, plan):
        job = next(job for job in plan["jobs"] if job["id"] == "single_hot-mixed-5-c1")
        output = Path(job["output"])
        raw = output / "run_mock"
        raw.mkdir(mode=0o700)
        selected = scenario()
        selected["scenario"] = "capacity_mixed"
        selected["phases"][0].update(offered=150, attempted=150, acked_workflows=150, completed_workflows=150,
                                     per_project_offered={"primary": 150}, planned_offers=150, configured_offered_per_second=5,
                                     observed_offered_per_second=5, acked_per_second_including_drain=5)
        report = {"run_id": "mock", "status": "COMPLETE", "source": {"git_hash": "b" * 40}, "settings": {
            "rows": 1000, "concurrency": [1], "seed": 20261007, "offered_per_second": 5,
            "warmup_ns": 2_000_000_000, "smoke_ns": 10_000_000_000, "measure_ns": 30_000_000_000,
            "cases": ["capacity_mixed"]}, "scenarios": [selected]}
        write_private(raw / "run.json", report)
        first, last = sample(2), sample(28)
        first["management_metrics"].update(actions_total={"query": 7, "execute": 3}, errors_total={})
        last["management_metrics"].update(actions_total={"query": 98, "execute": 42}, errors_total={})
        with os.fdopen(os.open(raw / "operations.jsonl", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as stream:
            for index in range(150):
                second = index // 5 + 1
                entry = operation(second)
                entry["business_operation_id"] = "UNIT-" + str(index)
                entry["scenario"] = "capacity_mixed"
                stream.write(json.dumps(entry) + "\n")
                stream.write(json.dumps({"type": "gateway_rpc", "method": "POST", "path": "/v1/sql/" + ("query" if index % 10 < 7 else "execute"),
                                        "envelope_ok": True, "failure": None, "finished_at_observed": entry["finished_at_observed"]}) + "\n")
            for entry in (first, last):
                stream.write(json.dumps(entry) + "\n")
        spec = json.loads(Path(job["spec"]).read_text())
        events = [{"event": "readonly_artifacts_before", "spec_sha256": job["spec_sha256"]},
                  {"event": "readonly_artifacts_after", "sha256": {**spec["sources"], **spec["artifacts"]}},
                  {"event": "final", "status": "CHILD_COMPLETE_PENDING_VERIFIER", "clean_join": True, "child_exit": 0,
                   "may_have_unknown": False, "confounded": False}]
        with os.fdopen(os.open(output / "guarded-events.jsonl", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as stream:
            for event in events:
                stream.write(json.dumps(event) + "\n")

        self.write_mock_collector(plan, job)

    def test_Linux_deleting_collector_evidence_blocks_safe_envelope(self):
        plan = self.frozen()
        self.write_quiet_raw(plan)
        job = next(job for job in plan["jobs"] if job["id"] == "single_hot-mixed-5-c1")
        output = Path(job["output"])
        self.assertEqual(analyze(plan)["safe_envelope"]["status"], "CANDIDATE FOR MAIN APPROVAL")
        for name in ["collector-status.json", "capacity-trace.jsonl", "capacity-observations.json"]:
            path = output / name
            content = path.read_bytes()
            path.unlink()
            with self.assertRaises((ValueError, OSError)):
                collect_job(job, plan)
            self.assertEqual(analyze(plan)["safe_envelope"]["status"], "NOT MEASURED")
            with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as stream:
                stream.write(content)
        journal = output / "capacity-trace.jsonl"
        rows = [json.loads(line) for line in journal.read_text().splitlines()]
        journal.write_text("".join(json.dumps(row) + "\n" for row in rows if row["type"] != "container_proc"))
        status_path = output / "collector-status.json"
        status = json.loads(status_path.read_text())
        status["journal_sha256"] = digest(journal)
        status_path.write_text(json.dumps(status))
        with self.assertRaisesRegex(ValueError, "requires_actual_container_proc"):
            collect_job(job, plan)
        self.assertEqual(analyze(plan)["safe_envelope"]["status"], "NOT MEASURED")

    def test_capsule_requires_independent_pass_and_reproduces_original_quiet_raw(self):
        plan = self.frozen()
        self.write_quiet_raw(plan)
        result = analyze(plan)
        self.assertEqual(result["safe_envelope"]["status"], "CANDIDATE FOR MAIN APPROVAL")
        self.assertEqual(result["safe_envelope"]["safe_rpc_per_second"], 2.5)
        self.assertEqual(result["safe_envelope"]["qualified_business_rate_points"], [5])
        result_path = self.root / "capacity-result.json"
        write_private(result_path, result)
        capsule_path = self.root / "approval-mock.json"
        capsule = {"image_id": "sha256:" + "a" * 64, "writer_admission": True, "rpc_rate_safe": 2.5,
                   "source": "b" * 40, "result_sha256": digest(result_path), "verified_by": "Main mock only", "status": "PASS",
                   "rpc_definition": RPC_DEFINITION, "source_sha256": "c" * 64, "independent_review": True}
        write_private(capsule_path, capsule)
        options = {"image_id": capsule["image_id"], "source": capsule["source"], "writer_admission": True,
                   "source_sha256": capsule["source_sha256"]}
        approved = validate_capacity_capsule(result_path, capsule_path, **options)
        self.assertEqual(approved["rpc_rate_safe"], 2.5)
        self.assertAlmostEqual(approved["required_with_margin_rpc_per_second"], 2.28)
        for rate, expected in ((2.28, True), (2.279, False)):
            capsule_path.unlink()
            write_private(capsule_path, {**capsule, "rpc_rate_safe": rate})
            if expected:
                self.assertEqual(validate_capacity_capsule(result_path, capsule_path, **options)["rpc_rate_safe"], rate)
            else:
                with self.assertRaisesRegex(ValueError, "soak_RPC_demand"):
                    validate_capacity_capsule(result_path, capsule_path, **options)
        for key, value in (("status", "CANDIDATE FOR MAIN APPROVAL"), ("rpc_rate_safe", 3), ("result_sha256", "0" * 64),
                           ("writer_admission", False), ("independent_review", False)):
            changed = dict(capsule)
            changed[key] = value
            capsule_path.unlink()
            write_private(capsule_path, changed)
            with self.assertRaises(ValueError):
                validate_capacity_capsule(result_path, capsule_path, **options)
        capsule_path.unlink()
        write_private(capsule_path, capsule)
        with self.assertRaisesRegex(ValueError, "soak_RPC_demand"):
            validate_capacity_capsule(result_path, capsule_path, **options, tx_per_second=0.5)
        raw = Path(plan["jobs"][0]["output"]) / "run_mock" / "operations.jsonl"
        raw.write_text("")
        with self.assertRaisesRegex(ValueError, "reproduce"):
            validate_capacity_capsule(result_path, capsule_path, **options)


if __name__ == "__main__":
    unittest.main()
