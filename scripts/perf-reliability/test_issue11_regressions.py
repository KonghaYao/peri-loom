"""验证报告反例与 raw fail-closed 回归；所有数据仅为离线契约夹具。"""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from test_issue11_support import contract, evidence_io, write_run, write_campaign
from test_issue11_gate import build_quick, _jsonl, _write_jsonl, _rebuild, _ledger
from test_issue11_attribution import request, span
from issue11 import gate, campaign, environment
from issue11.attribution import aggregate
from issue11.gate_pairing import synthetic, reliability_and_extended


class CounterexampleTests(unittest.TestCase):
    def test_synthetic_flags_independently_block(self):
        for document in ({"synthetic_test_fixture": True},
                         {"image": {"tag": "release-synthetic-test"}},
                         {"ledger": [{"synthetic": True}]}):
            self.assertTrue(synthetic(document))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            build_quick(root)
            result = gate.evaluate(root)
            self.assertNotEqual(result["status"], gate.PASS)
            self.assertTrue(any("synthetic" in reason for reason in result["reasons"]))
            print("反例 synthetic campaign: %s exit=%d" % (result["status"], result["exit_code"]))

    def test_h1_only_non_synthetic_formal_declaration_is_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            order = contract.load_acceptance()["sampling"]["formal"]["pair_order"]
            records = []
            for pair, label in enumerate(order):
                for arm in label:
                    run_id = "%d-%s" % (pair, arm)
                    (root / "runs" / run_id).mkdir(parents=True)
                    record = _ledger(run_id, profile="formal", arm=arm, pair_index=pair,
                                     state="fresh" if pair % 2 == 0 else "aged",
                                     order_label=label, duration_seconds=3600)
                    record["synthetic_test_fixture"] = False
                    records.append(record)
            write_campaign(root, records, scenarios=["H1"], contract_fixture=True)
            problems, _ = campaign.validate_campaign(root)
            self.assertIn(("INCOMPLETE", "campaign_formal_coverage_missing: H2,H3,H4,H5,H6,H7"), problems)
            self.assertNotEqual(gate._combine({}, problems, "formal", False), gate.PASS)
            print("反例 H1-only 非合成 formal: INCOMPLETE（缺 H2-H7）")

    def test_client_only_self_reported_server_is_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            run = build_quick(root)
            records = _jsonl(run / "request-attribution.jsonl")
            for record in records:
                record["spans"] = [s for s in record["spans"] if s["process"] == "client"]
                record["process_coverage"] = ["client"]
                record["server_evidence"] = True
            _write_jsonl(run / "request-attribution.jsonl", records)
            _rebuild(run)
            result = gate.evaluate(root)
            self.assertEqual(result["status"], gate.INCOMPLETE, result["reasons"])
            self.assertFalse(result["runs"][0]["attribution"]["server_evidence"])
            self.assertEqual(result["runs"][0]["attribution"]["server_evidence_coverage"], 0)

    def test_declared_process_requires_span(self):
        record = request("r", 100, [span("c", None, "client", "client.wait", 100)],
                         coverage=["client", "worker"])
        with self.assertRaisesRegex(aggregate.AttributionError, "process_coverage_without_span"):
            aggregate.load_records([record])

    def test_cross_process_two_roots_not_added_to_client(self):
        record = request("r1", 100, [
            span("client", None, "client", "client.wait", 100),
            span("gateway", None, "gateway", "gateway.worker", 60, clock="gateway:1")],
            coverage=["client", "gateway"])
        result = aggregate.aggregate(aggregate.load_records([record]))
        slow = result["slowest"]["requests"][0]
        self.assertEqual(slow["unattributed_ns"], 0)
        self.assertEqual(slow["process_roots"][0]["total_ns"], 60)
        self.assertTrue(result["unattributed"]["threshold_met"])
        print("反例 client.wait=100 + gateway.root=60: client unattributed=0，无负值")

    def test_server_root_coverage_checked_separately(self):
        record = request("r", 100, [
            span("c", None, "client", "client.wait", 100),
            span("g", None, "gateway", "gateway.root", 60, clock="gateway:1"),
            span("a", "g", "gateway", "gateway.auth", 20, clock="gateway:1")],
            coverage=["client", "gateway"])
        result = aggregate.aggregate(aggregate.load_records([record]))
        self.assertEqual(result["unattributed"]["coverage"], 1.0)
        self.assertEqual(result["unattributed"]["per_process_coverage"]["gateway"], 0.0)
        self.assertFalse(result["unattributed"]["threshold_met"])

    def test_retry_and_single_run_metadata_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            run = write_run(Path(directory) / "run", timed=10)
            records = _jsonl(run / "operations.jsonl")
            records[0]["retry_count"] = 1
            _write_jsonl(run / "operations.jsonl", records)
            _rebuild(run)
            result = gate.evaluate(run)
            self.assertEqual(result["status"], gate.FAIL)
            self.assertTrue(any("operation_retry" in reason for reason in result["reasons"]))
            (run / "run.json").unlink()
            result = gate.evaluate(run)
            self.assertEqual(result["status"], gate.FAIL)
            self.assertTrue(any("run.json" in reason for reason in result["reasons"]))

    def test_all_latency_denominator_keeps_slow_failures(self):
        operations, requests = [], {}
        for index, outcome in enumerate(("success", "error", "outcome_unknown", "rejected")):
            duration = 1_000_000 if outcome == "success" else 200_000_000
            request_id = str(index)
            operations.append({"phase": "timed", "issued_ns": index * 300_000_000,
                               "completed_ns": index * 300_000_000 + duration,
                               "duration_ns": duration, "outcome": outcome,
                               "http_request_ids": [request_id]})
            requests[request_id] = {"duration_ns": duration, "outcome": outcome}
        stats = gate._latency_stats(operations, requests)
        self.assertEqual(stats["p99_ms"], 200)
        self.assertEqual(stats["success_latency"]["p99_ms"], 1)
        self.assertEqual(stats["all_request_latency"]["p99_ms"], 200)
        self.assertEqual((stats["failure"], stats["outcome_unknown"], stats["rejected"]), (1, 1, 1))

    def test_extended_recovery_permit_evidence_missing_is_incomplete(self):
        view = {"run_id": "r", "record": {}, "oracle": {}, "resources": {}}
        problems = []
        reliability_and_extended(view, contract.load_acceptance(), problems)
        for name in contract.load_acceptance()["extended_evidence"]:
            self.assertIn(("INCOMPLETE", "extended_evidence_missing: " + name), problems)
        self.assertIn(("INCOMPLETE", "permit_residual_unavailable"), problems)
        self.assertIn(("INCOMPLETE", "acked_write_recovery_unproven: r"), problems)

    def test_each_raw_synthetic_source_blocks_gate(self):
        for filename in ("run.json", "environment.json", "resources.json", "oracle.json",
                         "requests.jsonl", "request-attribution.jsonl", "periodic-events.jsonl",
                         "metrics/worker.json"):
            with self.subTest(filename=filename), tempfile.TemporaryDirectory() as directory:
                run = write_run(Path(directory) / "run", timed=10, contract_fixture=True)
                path = run / filename
                if filename.endswith(".jsonl"):
                    records = _jsonl(path)
                    records[0]["synthetic_test_fixture"] = True
                    _write_jsonl(path, records)
                else:
                    document = json.loads(path.read_text())
                    document["synthetic_test_fixture"] = True
                    evidence_io.write_json(path, document, reseal=True)
                if filename == "metrics/worker.json":
                    index = json.loads((run / "metrics-snapshot-index.json").read_text())
                    index["snapshots"][0]["sha256"] = contract.digest(path)
                    evidence_io.write_json(run / "metrics-snapshot-index.json", index, reseal=True)
                _rebuild(run)
                result = gate.evaluate(run)
                self.assertNotEqual(result["status"], gate.PASS)
                self.assertTrue(any("synthetic" in reason for reason in result["reasons"]),
                                result["reasons"])

    def test_h4_warmup_counts_scenarios_not_two_actions(self):
        with tempfile.TemporaryDirectory() as directory:
            run = write_run(Path(directory) / "run", timed=10, warmup=100, scenario="H4")
            result = gate.evaluate(run)
            self.assertTrue(any("warmup_below_minimum" in reason for reason in result["reasons"]))

    def test_diagnostic_parser_rejects_swapped_threshold_and_permille(self):
        from types import SimpleNamespace
        from issue11.http.runtime import _diagnostics, RunnerError
        for switch in ("PERI_SQL_OPEN_OBSERVER_PHASE_THRESHOLD_MS=1000",
                       "PERI_SQL_OPEN_OBSERVER_PHASE_SAMPLE_PERMILLE=20"):
            with self.subTest(switch=switch), self.assertRaises(RunnerError):
                _diagnostics(SimpleNamespace(diagnostic_switch=[switch]))
        values = _diagnostics(SimpleNamespace(diagnostic_switch=["PERI_SQL_OPEN_OBSERVER=1"]))
        self.assertTrue(values["PERI_SQL_OPEN_OBSERVER"])
        self.assertEqual(values["PERI_SQL_OPEN_OBSERVER_PHASE_THRESHOLD_MS"], 20)
        self.assertEqual(values["PERI_SQL_OPEN_OBSERVER_PHASE_SAMPLE_PERMILLE"], 1000)

    def test_diagnostic_process_pid_must_be_positive_integer(self):
        from test_issue11_support import base_environment
        for pid in (True, 0, -1, "123"):
            env = base_environment("/contract-only")
            env["diagnostic_processes"] = {"worker": {
                "pid": pid, "observed_switches": env["diagnostics"]}}
            checked = environment.validate(env, contract.load_workloads())
            self.assertIn("environment_diagnostic_process: worker", checked["problems"])

    def test_duplicate_sample_and_request_metadata_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            run = write_run(Path(directory) / "run", timed=2, warmup=0)
            records = _jsonl(run / "operations.jsonl")
            records[1]["sample_index"] = 0
            _write_jsonl(run / "operations.jsonl", records)
            _rebuild(run)
            result = gate.evaluate(run)
            self.assertEqual(result["status"], gate.FAIL)
            for reason in ("duplicate_sample_index", "operation_request_metadata_mismatch"):
                self.assertTrue(any(reason in item for item in result["reasons"]), result["reasons"])

    def test_runner_template_preserves_synthetic_and_process_proof(self):
        from types import SimpleNamespace
        from test_issue11_support import base_environment
        from issue11.http.runtime import build_environment
        with tempfile.TemporaryDirectory() as directory:
            template = base_environment(directory, synthetic=True)
            diagnostics = template["diagnostics"]
            template["diagnostic_processes"] = {name: {
                "pid": index + 1, "observed_switches": diagnostics}
                for index, name in enumerate(("worker", "authority"))}
            path = Path(directory) / "template.json"
            evidence_io.write_json(path, template)
            args = SimpleNamespace(environment_template=str(path), ca_file=None,
                                   endpoint="http://127.0.0.1:9", synthetic_test_fixture=False)
            observed = build_environment(args, SimpleNamespace(tls=False), "r",
                                         contract.package_digest(), template["source"], diagnostics)
            self.assertTrue(observed["synthetic_test_fixture"])
            self.assertEqual(observed["diagnostic_processes"], template["diagnostic_processes"])

    def test_diagnostic_schema_values_and_process_coverage(self):
        from test_issue11_support import base_environment
        with tempfile.TemporaryDirectory() as directory:
            env = base_environment(directory)
            env["diagnostics"]["PERI_SQL_OPEN_OBSERVER"] = True
            env["diagnostics"]["PERI_SQL_OPEN_OBSERVER_PHASE_THRESHOLD_MS"] = 99
            validation = environment.validate(env, contract.load_workloads())
            self.assertIn("environment_diagnostic_observer_values", validation["problems"])
            run = write_run(Path(directory) / "diagnostic", timed=10,
                            diagnostics={**env["diagnostics"],
                                         "PERI_SQL_OPEN_OBSERVER_PHASE_THRESHOLD_MS": 20})
            checked = gate.evaluate(run)
            self.assertEqual(checked["status"], gate.INCOMPLETE, checked["reasons"])
            self.assertTrue(any("diagnostic_process_unproven: worker" in reason
                                for reason in checked["reasons"]))
            self.assertTrue(any("diagnostic_process_unproven: authority" in reason
                                for reason in checked["reasons"]))
