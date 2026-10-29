"""Fabricated collector records test validation only; no database benchmark."""
import argparse
import copy
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("comparison", Path(__file__).with_name("analyze-comparison.py"))
comparison = importlib.util.module_from_spec(spec)
spec.loader.exec_module(comparison)


class AnalyzerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        manifest = {"schema": comparison.SCHEMA, "dataset_origin": "synthetic-proxy",
                    "dataset_ref": "mock-only", "dataset_sha256": "d" * 64,
                    "scenarios": [{"name": "proxy_empty", "sql": "SELECT 1 WHERE 0", "args": [],
                                   "expected_rows": 0, "expected_result_sha256": "e" * 64}]}
        self.manifest = self.root / "manifest.json"
        self.manifest.write_text(json.dumps(manifest))
        self.runs = []
        for leg, arm in enumerate(comparison.ORDER):
            directory = self.root / str(leg)
            directory.mkdir()
            self.runs.append(directory)
            run_id = "run-" + str(leg)
            run = {"schema": comparison.SCHEMA, "run_id": run_id, "engine": "sqld" if arm == "S" else "peri",
                   "arm": arm, "source_sha": "source-" + arm, "formal_pass": False, "recovery_proven": False,
                   "sdk": "@tursodatabase/serverless@1.4.1", "sdk_lock_sha256": "c" * 64,
                   "manifest_sha256": comparison.sha(self.manifest.read_bytes()),
                   "dataset_sha256": "d" * 64, "dataset_origin": "synthetic-proxy", "scenario": "proxy_empty",
                   "samples_requested": 2, "warmup_requested": 1, "started_at_unix_ms": leg * 10000,
                   "sql_sha256": comparison.sha(b"SELECT 1 WHERE 0"), "expected_rows": 0,
                   "node": "v-test", "arch": "mock", "platform": "mock", "pace_ms": 0, "timeout_ms": 30000}
            run["server_identity"] = {"arm": arm, "source_commit": "source-" + arm,
                                      "binary_sha256": "a" * 64, "shared_native_sha256": "b" * 64}
            operations, requests = [], []
            for ordinal in range(3):
                phase = "warmup" if ordinal == 0 else "timed"
                index = 0 if ordinal == 0 else ordinal - 1
                issued = ordinal * 1000
                duration = {"A": 20, "B": 15, "S": 10}[arm] + ordinal
                operation_id = run_id + "-" + str(ordinal)
                ids = [operation_id + "-read", operation_id + "-close"]
                actions = []
                for action_index, action in enumerate(("read", "close")):
                    is_close = action == "close"
                    start = issued + duration - 2 if is_close else issued + 1
                    resolved = issued + duration if is_close else issued + duration - 2
                    actions.append({"action": action, "started_ms": start, "sdk_resolved_ms": resolved,
                                    "sdk_duration_ms": resolved - start, "requests": [ids[action_index]],
                                    "blocked_retries": 0, "sdk_error": False})
                    requests.append({"request_id": ids[action_index], "run_id": run_id,
                                     "operation_id": operation_id, "phase": phase, "sample_index": index,
                                     "action": action, "outcome": "success", "status": 200,
                                     "request_body_sha256": "a" * 64, "started_ms": start,
                                     "headers_received_ms": start + .5, "body_observed_ms": start + 1,
                                     "response_bytes": 100, "baton_in_sha256": "b" * 64 if is_close else None,
                                     "baton_out_sha256": None if is_close else "b" * 64,
                                     "close_requested": is_close, "close_acknowledged": is_close,
                                     "primary_step_complete": True if not is_close else None,
                                     "path": "/v3/pipeline" if is_close else "/v3/cursor"})
                operations.append({"run_id": run_id, "operation_id": operation_id, "phase": phase,
                                   "sample_index": index, "scenario": "proxy_empty", "retry_count": 0,
                                   "blocked_retry_count": 0, "evidence_failures": [], "outcome": "success",
                                   "close_acknowledged": True, "result_rows": 0, "result_sha256": "e" * 64,
                                   "issued_ms": issued, "connect_returned_ms": issued + .5, "actions": actions,
                                   "sdk_completed_ms": issued + duration, "duration_ms": duration,
                                   "evidence_settled_ms": issued + duration + 1, "observer_wait_ms": 1,
                                   "validation_completed_ms": issued + duration + 2, "request_ids": ids})
            statistics = comparison.quantiles([op["duration_ms"] for op in operations[1:]])
            summary = {"schema": comparison.SCHEMA, "run_id": run_id, "formal_pass": False,
                       "recovery_proven": False, "dataset_origin": "synthetic-proxy", "scenario": "proxy_empty",
                       "status": "RUN_COMPLETED", "all_timed": statistics, "successful_timed": statistics,
                       "failures_all_phases": 0, "unknown_all_phases": 0, "blocked_retries_all_phases": 0,
                       "all_operations": 3, "timed_requests": 4, "timed_close_acknowledgements": 2}
            summary["completed_at_unix_ms"] = leg * 10000 + 5000
            for name, value in (("run.json", run), ("summary.json", summary)):
                (directory / name).write_text(json.dumps(value))
            for name, value in (("operations.jsonl", operations), ("requests.jsonl", requests)):
                (directory / name).write_text("".join(json.dumps(item) + "\n" for item in value))
            summary["published_raw"] = {
                name: {"records": len(value), "bytes": len((directory / name).read_bytes()),
                       "sha256": comparison.sha((directory / name).read_bytes())}
                for name, value in (("operations.jsonl", operations), ("requests.jsonl", requests))}
            (directory / "summary.json").write_text(json.dumps(summary))
            self.rehash(directory)
        self.args = argparse.Namespace(manifest=str(self.manifest), runs=[str(path) for path in self.runs],
                                       samples=2, warmup=1, scenarios=["proxy_empty"])

    def tearDown(self):
        self.temp.cleanup()

    def rehash(self, directory):
        (directory / "SHA256SUMS.json").write_text(json.dumps({name: comparison.sha((directory / name).read_bytes())
                                                              for name in comparison.FILES}))

    def update_ledger(self, directory):
        summary_path = directory / "summary.json"
        summary = json.loads(summary_path.read_bytes())
        for name in ("operations.jsonl", "requests.jsonl"):
            data = (directory / name).read_bytes()
            summary["published_raw"][name] = {"records": len(data.splitlines()), "bytes": len(data), "sha256": comparison.sha(data)}
        summary_path.write_text(json.dumps(summary))

    def change_operation(self, mutation):
        path = self.runs[0] / "operations.jsonl"
        ops = [json.loads(line) for line in path.read_text().splitlines()]
        mutation(ops[0])
        path.write_text("".join(json.dumps(item) + "\n" for item in ops))
        self.update_ledger(self.runs[0])
        self.rehash(self.runs[0])

    def test_valid_all_legs_pooled_raw_and_ratios(self):
        report = comparison.analyze(self.args)
        self.assertTrue(report["evidence_valid"])
        self.assertFalse(report["formal_pass"])
        self.assertFalse(report["recovery_proven"])
        self.assertFalse(report["bootstrap_performed"])
        self.assertEqual(len(report["legs"]), 6)
        scenario = report["scenarios"]["proxy_empty"]
        self.assertEqual(scenario["pooled"]["B"]["business"]["count"], 4)
        self.assertEqual(scenario["pooled"]["B"]["business"]["p99_ms"], 17)
        self.assertEqual(scenario["B_over_A"]["business"]["p99_ms"], 17 / 22)
        self.assertEqual(scenario["B_over_S"]["business"]["p99_ms"], 17 / 12)

    def test_tampered_file_fails_before_statistics(self):
        with (self.runs[0] / "operations.jsonl").open("a") as target:
            target.write("{}\n")
        with self.assertRaisesRegex(comparison.InvalidEvidence, "SHA mismatch"):
            comparison.analyze(self.args)

    def test_warmup_failure_cannot_be_dropped(self):
        self.change_operation(lambda op: op.update(outcome="unknown"))
        with self.assertRaisesRegex(comparison.InvalidEvidence, "failed/Unknown"):
            comparison.analyze(self.args)

    def test_close_ack_required(self):
        self.change_operation(lambda op: op.update(close_acknowledged=False))
        with self.assertRaisesRegex(comparison.InvalidEvidence, "no close ACK"):
            comparison.analyze(self.args)

    def test_whole_result_hash_required(self):
        self.change_operation(lambda op: op.update(result_sha256="f" * 64))
        with self.assertRaisesRegex(comparison.InvalidEvidence, "full result mismatch"):
            comparison.analyze(self.args)

    def test_timing_inversion_rejected(self):
        self.change_operation(lambda op: op.update(evidence_settled_ms=0))
        with self.assertRaisesRegex(comparison.InvalidEvidence, "clock order"):
            comparison.analyze(self.args)

    def test_incomplete_cursor_rejected(self):
        path = self.runs[0] / "requests.jsonl"
        requests = [json.loads(line) for line in path.read_text().splitlines()]
        requests[0]["primary_step_complete"] = False
        path.write_text("".join(json.dumps(item) + "\n" for item in requests))
        self.update_ledger(self.runs[0])
        self.rehash(self.runs[0])
        with self.assertRaisesRegex(comparison.InvalidEvidence, "incomplete cursor"):
            comparison.analyze(self.args)

    def test_declared_sample_count_required(self):
        changed = copy.copy(self.args)
        changed.samples = 200
        with self.assertRaisesRegex(comparison.InvalidEvidence, "declared N"):
            comparison.analyze(changed)

    def test_serial_leg_order_required(self):
        path = self.runs[0] / "run.json"
        run = json.loads(path.read_bytes())
        run["started_at_unix_ms"] = 999999
        path.write_text(json.dumps(run))
        summary_path = self.runs[0] / "summary.json"
        summary = json.loads(summary_path.read_bytes())
        summary["completed_at_unix_ms"] = 1005000
        summary_path.write_text(json.dumps(summary))
        self.rehash(self.runs[0])
        with self.assertRaisesRegex(comparison.InvalidEvidence, "expected serial"):
            comparison.analyze(self.args)

    def test_parallel_run_intervals_rejected(self):
        path = self.runs[0] / "summary.json"
        summary = json.loads(path.read_bytes())
        summary["completed_at_unix_ms"] = 15000
        path.write_text(json.dumps(summary))
        self.rehash(self.runs[0])
        with self.assertRaisesRegex(comparison.InvalidEvidence, "overlapping run"):
            comparison.analyze(self.args)

    def test_changed_binary_identity_rejected(self):
        path = self.runs[0] / "run.json"
        run = json.loads(path.read_bytes())
        run["server_identity"]["binary_sha256"] = "f" * 64
        path.write_text(json.dumps(run))
        self.rehash(self.runs[0])
        with self.assertRaisesRegex(comparison.InvalidEvidence, "changed server_identity"):
            comparison.analyze(self.args)

    def test_only_explicit_setup_category_is_excluded_from_measured_reads(self):
        setup = self.runs[0] / "setup"
        setup.mkdir()
        (setup / "run.json").write_text(json.dumps({"scenario": "isolated_fixture_setup"}))
        # Passing the parent discovers every measured run and the explicit setup.
        self.args.runs = [str(self.root)]
        report = comparison.analyze(self.args)
        self.assertEqual(report["excluded_setup_runs"], [str(setup)])
        self.assertEqual(len(report["legs"]), 6)

    def test_distinct_stable_native_binaries_across_A_B_rejected(self):
        for directory in (self.runs[0], self.runs[5]):
            path = directory / "run.json"
            run = json.loads(path.read_bytes())
            run["server_identity"]["shared_native_sha256"] = "f" * 64
            path.write_text(json.dumps(run))
            self.rehash(directory)
        with self.assertRaisesRegex(comparison.InvalidEvidence, "shared native binary differs"):
            comparison.analyze(self.args)

    def test_original_publication_record_count_required(self):
        path = self.runs[0] / "summary.json"
        summary = json.loads(path.read_bytes())
        summary["published_raw"]["operations.jsonl"]["records"] -= 1
        path.write_text(json.dumps(summary))
        self.rehash(self.runs[0])
        with self.assertRaisesRegex(comparison.InvalidEvidence, "publication ledger mismatch"):
            comparison.analyze(self.args)


if __name__ == "__main__":
    unittest.main()
