"""Synthetic-only tests: never contacts a database or starts a load generator."""

import contextlib
import hashlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import evidence


IDENTIFIER = "0123456789abcdef"


def sample(short=False):
    records = [
        {"type": "operation", "scenario": "example", "phase": "measure", "concurrency": 1,
         "offered_sequence": 0, "business_operation_id": f"stress_{IDENTIFIER}_000000000001",
         "acked": True, "correct": True, "failure": None, "http_workflow_ns": 10, "client_queue_ns": 2,
         "sql": "NEVER PRINT THIS", "token": "NEVER PRINT THIS"},
        {"type": "operation", "scenario": "example", "phase": "measure", "concurrency": 1,
         "offered_sequence": 1, "business_operation_id": f"stress_{IDENTIFIER}_000000000002",
         "acked": False, "correct": False, "http_workflow_ns": 100, "client_queue_ns": 8,
         "failure": {"class": "admission", "unknown": False, "timed_out": False}},
        {"type": "client_admission_drop", "scenario": "example", "phase": "measure", "concurrency": 1,
         "offered_sequence": 2, "business_operation_id": f"stress_{IDENTIFIER}_000000000003"},
    ]
    phase = {"scenario": "example", "phase": "measure", "concurrency": 1, "status": "COMPLETE",
             "started_at_observed": "observed", "finished_at_observed": "observed",
             "requested_duration_ns": 5_000_000_000 if short else 30_000_000_000,
             "elapsed_ns": 1_000_000_000, "offering_elapsed_ns": 500_000_000,
             "planned_offers": 4, "scheduled_offers_not_emitted": 1,
             "offered": 3, "client_admission_dropped": 1, "client_queue_expired": 0,
             "attempted": 2, "completed_workflows": 2, "acked_workflows": 1,
             "correctness_failures": 0, "errors_by_class": {"admission": 1},
             "timeouts": 0, "unknown": 0, "cleanup_unknown": 0,
             "observed_offered_per_second": 6.0, "acked_per_second_including_drain": 1.0,
             "all_workflow_latency": evidence.quantiles([10, 100]),
             "acked_workflow_latency": evidence.quantiles([10]),
             "client_queue_latency": evidence.quantiles([2, 8]),
             "actual_admission_rejection_observed": True, "short_gate_not_benchmark": short}
    report = {"version": 1, "run_id": IDENTIFIER, "status": "COMPLETE",
              "source": {"git_hash": "a" * 40, "dirty": False,
                         "untracked_content_provenance": "status hash only; untracked content not independently frozen"},
              "joint_gate": {"version": 1, "joint_pass": True, "confirmed_by": "main",
                             "source_git": "a" * 40, "stages": [f"P{index}" for index in range(7)],
                             "observed_at": "2026-10-14T12:00:00Z"},
              "preflight_correctness": [{"status": "PASS"}],
              "settings": {"smoke_ns": 10_000_000_000, "measure_ns": 5_000_000_000 if short else 30_000_000_000},
              "redacted_connection": {"token": "NEVER PRINT THIS"},
              "scenarios": [{"scenario": "example", "status": "COMPLETE", "phases": [phase]}]}
    return report, records


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path(tempfile.gettempdir()).resolve())
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name) / ("run_" + IDENTIFIER)
        self.directory.mkdir(mode=0o700)
        self.report, self.records = sample()

    def write(self):
        run_path = self.directory / "run.json"
        ops_path = self.directory / "operations.jsonl"
        run_path.write_text(json.dumps(self.report), encoding="utf-8")
        ops_path.write_text("".join(json.dumps(record) + "\n" for record in self.records), encoding="utf-8")
        run_path.chmod(0o600)
        ops_path.chmod(0o600)

    def assert_rejected_quietly(self, directory=None):
        output = io.StringIO()
        errors = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            code = evidence.main([str(directory or self.directory)])
        self.assertEqual(code, 1)
        self.assertEqual(output.getvalue(), "")
        self.assertNotIn("NEVER PRINT THIS", errors.getvalue())
        self.assertNotIn("NEVER PRINT THIS", output.getvalue())

    def test_bounded_input_and_duplicate_fields(self):
        self.write()
        run_path = self.directory / "run.json"
        ops_path = self.directory / "operations.jsonl"
        for path, limit in ((run_path, evidence.MAX_RUN_BYTES), (ops_path, evidence.MAX_OPERATIONS_BYTES)):
            with self.subTest(path=path):
                with path.open("r+b") as stream:
                    stream.truncate(limit + 1)
                self.assert_rejected_quietly()
                self.write()
        ops_path.write_bytes(json.dumps({"type": "ignored", "secret": "NEVER PRINT THIS" * (evidence.MAX_LINE_BYTES // 17)}).encode() + b"\n")
        self.assert_rejected_quietly()
        self.write()
        run_path.write_text('{"run_id":"' + IDENTIFIER + '","run_id":"NEVER PRINT THIS"}', encoding="utf-8")
        self.assert_rejected_quietly()
        self.write()
        ops_path.write_text('{"type":"operation","type":"NEVER PRINT THIS"}\n', encoding="utf-8")
        self.assert_rejected_quietly()

    def test_untrusted_permissions_and_parent_symlink(self):
        self.write()
        run_path = self.directory / "run.json"
        ops_path = self.directory / "operations.jsonl"
        for path in (run_path, ops_path):
            with self.subTest(path=path):
                path.chmod(0o644)
                self.assert_rejected_quietly()
                path.chmod(0o600)
        with mock.patch.object(evidence.os, "getuid", return_value=os.getuid() + 1):
            self.assert_rejected_quietly()
        self.directory.chmod(0o750)
        self.assert_rejected_quietly()
        self.directory.chmod(0o700)
        parent = self.directory.parent
        parent.chmod(0o770)
        try:
            self.assert_rejected_quietly()
        finally:
            parent.chmod(0o700)
        alias = parent / "linked"
        alias.symlink_to(parent, target_is_directory=True)
        self.assert_rejected_quietly(alias / self.directory.name)
        alias.unlink()
        ops_path.rename(parent / "original-ops")
        ops_path.symlink_to(parent / "original-ops")
        self.assert_rejected_quietly()

    def test_changed_file_and_inode_during_read(self):
        self.write()
        run_path = self.directory / "run.json"
        original = evidence.parse_json

        def rewrite_run(data):
            result = original(data)
            if result.get("run_id") == IDENTIFIER:
                run_path.write_text(json.dumps({"secret": "NEVER PRINT THIS"}), encoding="utf-8")
            return result

        with mock.patch.object(evidence, "parse_json", side_effect=rewrite_run):
            self.assert_rejected_quietly()
        self.write()

        def replace_run(data):
            result = original(data)
            if result.get("run_id") == IDENTIFIER:
                replacement = self.directory / "replacement"
                replacement.write_text(json.dumps(self.report), encoding="utf-8")
                replacement.chmod(0o600)
                os.replace(replacement, run_path)
            return result

        with mock.patch.object(evidence, "parse_json", side_effect=replace_run):
            self.assert_rejected_quietly()

    def test_valid_benchmark_recalculated_and_redacted(self):
        self.write()
        result = evidence.verify(self.directory)
        metrics = result["phases"][0]["benchmark"]
        self.assertEqual(metrics["acked_per_second"], 1)
        self.assertEqual(metrics["all_workflow_latency"]["p99_ns"], 100)
        self.assertEqual(result["phases"][0]["client_admission_dropped"], 1)
        self.assertEqual(result["durability_oracle"], "NOT MEASURED")
        self.assertEqual(result["sha256"]["run.json"], hashlib.sha256((self.directory / "run.json").read_bytes()).hexdigest())
        self.assertNotIn("NEVER PRINT THIS", json.dumps(result))
        self.assertEqual(result["embedded_prerequisites"], "PRESENT_UNVERIFIED")
        self.assertEqual(result["independent_gate_verification"], "NOT MEASURED")
        self.assertEqual(result["untracked_content_verification"], "NOT MEASURED")
        self.assertIn("not certified performance", result["limitation"])

    def test_incomplete_embedded_prerequisites_accounting_only(self):
        changes = [
            lambda report: report.pop("joint_gate"),
            lambda report: report["joint_gate"].update(source_git="b" * 40),
            lambda report: report["joint_gate"].update(joint_pass=False),
            lambda report: report["joint_gate"].update(confirmed_by="not-main"),
            lambda report: report["joint_gate"].update(stages=["P0", "P1"]),
            lambda report: report["joint_gate"].update(version=2),
            lambda report: report["joint_gate"].update(observed_at="not-a-timestamp"),
            lambda report: report.pop("source"),
            lambda report: report["source"].update(dirty=True),
            lambda report: report["source"].update(untracked_content_provenance=""),
            lambda report: report.pop("preflight_correctness"),
            lambda report: report.update(preflight_correctness=[]),
            lambda report: report.update(preflight_correctness=[{"status": "FAIL"}]),
            lambda report: report.update(preflight_correctness=[{"status": "PASS"}, {"status": "FAIL"}]),
        ]
        for change in changes:
            with self.subTest(change=change):
                self.report, self.records = sample()
                change(self.report)
                self.write()
                result = evidence.verify(self.directory)
                self.assertEqual(result["validation"], "PARTIAL")
                self.assertEqual(result["embedded_prerequisites"], "INCOMPLETE")
                self.assertEqual(result["phases"][0]["acked"], 1)
                self.assertEqual(result["phases"][0]["benchmark"], "NOT MEASURED")

    def test_queue_expired_reconciles_but_does_not_certify_benchmark(self):
        phase = self.report["scenarios"][0]["phases"][0]
        phase.update(offered=4, client_queue_expired=1, scheduled_offers_not_emitted=0,
                     observed_offered_per_second=8.0)
        self.write()
        result = evidence.verify(self.directory)
        self.assertEqual(result["validation"], "PARTIAL")
        summary = result["phases"][0]
        self.assertEqual(summary["reported_offered"], 4)
        self.assertEqual(summary["recorded_offers"], 3)
        self.assertEqual(summary["reported_queue_expired"], 1)
        self.assertEqual(summary["expired_offer_identity"], "NOT MEASURED")
        self.assertEqual(summary["offered_evidence"], "RECONCILED_ONLY")
        self.assertEqual(summary["benchmark"], "NOT MEASURED")
        phase["observed_offered_per_second"] = 6.0
        self.write()
        with self.assertRaises(evidence.InvalidEvidence):
            evidence.verify(self.directory)

    def test_short_gate_cannot_be_benchmark(self):
        self.report, self.records = sample(short=True)
        self.write()
        self.assertEqual(evidence.verify(self.directory)["phases"][0]["benchmark"], "NOT MEASURED")
        self.report["scenarios"][0]["phases"][0]["short_gate_not_benchmark"] = False
        self.write()
        with self.assertRaises(evidence.InvalidEvidence):
            evidence.verify(self.directory)

    def test_unknown_and_failed_phase_never_claim_benchmark(self):
        phase = self.report["scenarios"][0]["phases"][0]
        self.report["status"] = self.report["scenarios"][0]["status"] = phase["status"] = "UNRESOLVED"
        self.records[1]["failure"]["unknown"] = True
        phase["unknown"] = 1
        self.write()
        self.assertEqual(evidence.verify(self.directory)["phases"][0]["benchmark"], "NOT MEASURED")

    def test_cleanup_failure_count_and_duplicate_phase_name(self):
        self.records.append({"type": "session_cleanup", "scenario": "example", "phase": "measure", "slot": 0,
                             "failure": {"class": "timeout", "unknown": True}})
        phase = self.report["scenarios"][0]["phases"][0]
        phase["cleanup_unknown"] = 1
        phase["errors_by_class"]["cleanup_timeout"] = 1
        phase["status"] = self.report["status"] = self.report["scenarios"][0]["status"] = "UNRESOLVED"
        self.write()
        self.assertEqual(evidence.verify(self.directory)["phases"][0]["cleanup_unknown"], 1)
        duplicate = dict(phase, concurrency=2, status="NOT MEASURED", phase="measure", started_at_observed="",
                         measurement_status="unavailable_not_started", unknown=0, cleanup_unknown=0)
        self.report["scenarios"][0]["phases"].append(duplicate)
        self.write()
        self.assertEqual(len(evidence.verify(self.directory)["phases"]), 2)

    def test_reject_tampering(self):
        mutations = [
            lambda: self.report.update(run_id="0000000000000000"),
            lambda: self.report["scenarios"][0]["phases"][0].update(offered=4),
            lambda: self.report["scenarios"][0]["phases"][0].update(acked_workflows=2),
            lambda: self.report["scenarios"][0]["phases"][0].update(unknown=1),
            lambda: self.report["scenarios"][0]["phases"][0]["all_workflow_latency"].update(p99_ns=10),
            lambda: self.report["scenarios"][0]["phases"][0].update(status="SKIP"),
            lambda: self.records[2].update(offered_sequence=1),
            lambda: self.report["scenarios"][0]["phases"][0].update(client_queue_expired=1),
        ]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                self.report, self.records = sample()
                mutate()
                self.write()
                with self.assertRaises(evidence.InvalidEvidence):
                    evidence.verify(self.directory)

    def test_unstarted_phase_has_no_benchmark(self):
        self.records = []
        self.report["status"] = "FAIL"
        self.report["scenarios"][0]["status"] = "FAIL"
        self.report["scenarios"][0]["phases"] = [{"scenario": "example", "phase": "measure", "concurrency": 1,
            "status": "FAIL", "measurement_status": "unavailable_not_started", "unknown": 1,
            "cleanup_unknown": 0, "errors_by_class": {"timeout": 1}}]
        self.write()
        self.assertEqual(evidence.verify(self.directory)["phases"][0]["benchmark"], "NOT MEASURED")

    def test_cli_does_not_echo_secrets_on_invalid_json(self):
        self.write()
        (self.directory / "operations.jsonl").write_text('{"sql":"NEVER PRINT THIS"}\n', encoding="utf-8")
        self.assert_rejected_quietly()


if __name__ == "__main__":
    unittest.main()
