import json
from pathlib import Path
import subprocess
import sys
import unittest

PERF_ROOT = Path(__file__).resolve().parent
if str(PERF_ROOT) not in sys.path:
    sys.path.insert(0, str(PERF_ROOT))

import perf_contract  # noqa: E402
from issue11 import contract  # noqa: E402


class ContractTests(unittest.TestCase):
    def test_contract_manifest_and_issue8_freeze(self):
        identity = contract.verify_contract()
        self.assertTrue(identity["verified"], identity["problems"])
        self.assertEqual(contract.issue8_manifest_sha256(),
                         perf_contract.FROZEN_SHA256SUMS_SHA256)
        try:
            issue8 = perf_contract.verify_contract()
        except perf_contract.ContractError as failure:
            self.fail("issue8 contract changed: " + str(failure))
        self.assertTrue(issue8["verified"])

    def test_contract_files_are_declared_and_hashed(self):
        acceptance = contract.load_acceptance()
        files = acceptance["contract"]["files"]
        self.assertEqual(set(files), set(contract.verify_contract()["files"]))
        for required in ("README.md", "acceptance.json", "workloads.json",
                         "environment.schema.json", "attribution/schema.json"):
            self.assertIn(required, files)
        self.assertIn("sql/00-schema.sql", files)
        self.assertIn("sql/01-fixture-1000.sql", files)
        self.assertIn("sql/02-fixture-100000.sql", files)
        manifest = json.loads((contract.CONTRACT_ROOT / "SHA256SUMS.json").read_text())
        self.assertEqual(set(manifest), set(files))

    def test_cross_file_contract_is_consistent(self):
        self.assertEqual(contract.validate_contract(), [])
        acceptance = contract.load_acceptance()
        workloads = contract.load_workloads()
        self.assertEqual(acceptance["baseline_commit"], workloads["baseline_commit"])
        self.assertEqual(acceptance["turso_commit"], workloads["turso_commit"])
        self.assertFalse(acceptance["measured"])
        self.assertEqual(acceptance["target_status"], "TARGET_NOT_MEASURED")
        self.assertIn("NOT_MEASURED", acceptance["status"])
        self.assertGreaterEqual(acceptance["sampling"]["warmup_min"], 200)
        self.assertGreaterEqual(acceptance["sampling"]["quick"]["samples_min"], 2000)
        self.assertGreaterEqual(acceptance["sampling"]["formal"]["samples_min"], 10000)
        self.assertGreaterEqual(acceptance["sampling"]["formal"]["pairs_min"], 5)
        self.assertEqual(acceptance["sampling"]["formal"]["pair_order"],
                         ["AB", "BA", "AB", "BA", "AB"])
        self.assertEqual(set(workloads["scenarios"]), set(contract.SCENARIOS))
        self.assertEqual(workloads["payload_bytes"], 256)
        self.assertEqual(workloads["fixture_scales"], [1000, 100000])
        self.assertFalse(workloads["driver_rules"]["sql_auto_retry"])

    def test_status_machine_and_formal_requirements(self):
        rules = contract.load_acceptance()["status_rules"]
        for key in ("missing_evidence", "contract_hash_mismatch",
                    "insufficient_samples", "unpaired_diagnostic_switch",
                    "unknown_without_terminal_record",
                    "close_unconfirmed_counted_success",
                    "duplicate_span", "unknown_request_id",
                    "cross_request_stitch", "missing_correlation"):
            self.assertIn(key, rules)
        self.assertEqual(rules["missing_evidence"], "NOT_RUN")
        self.assertEqual(rules["insufficient_samples"], "INCOMPLETE")
        self.assertEqual(rules["contract_hash_mismatch"], "FAIL")
        self.assertTrue(rules["any_not_pass_exit_nonzero"])
        self.assertTrue(rules["summary_never_read"])
        self.assertIn("server_attribution",
                      rules["formal_pass_requires"])
        self.assertIn("periodic_events", rules["formal_pass_requires"])
        self.assertIn("pairs", rules["formal_pass_requires"])

    def test_w0_b_dependencies_are_declared_not_claimed(self):
        acceptance = contract.load_acceptance()
        self.assertTrue(acceptance["w0_b_dependencies"]["declared_not_implemented"])
        items = {item["id"] for item in acceptance["w0_b_dependencies"]["items"]}
        self.assertIn("go-intent-span", items)
        self.assertIn("rust-admission-success-wait", items)
        self.assertIn("hrana-dispatch-correlation", items)
        self.assertIn("periodic-events", items)
        attribution = contract.load_attribution_schema()
        self.assertEqual(attribution["available_in_w0_a"], ["client"])
        self.assertTrue(attribution["design_requirement"])
        for process in ("gateway", "worker", "authority", "object_store"):
            self.assertTrue(attribution["process_phases"][process])
        self.assertIn("gateway.intent", attribution["process_phases"]["gateway"])
        self.assertIn("worker.admission_wait",
                      attribution["process_phases"]["worker"])
        self.assertIn("authority.cas", attribution["process_phases"]["authority"])
        self.assertIn("object_store.body_fsync",
                      attribution["process_phases"]["object_store"])

    def test_deterministic_fixture_helpers_and_sql(self):
        for row_id in (1, 999, 1000, 100000):
            payload = contract.fixture_payload(row_id)
            self.assertEqual(len(payload.encode()), 256)
            self.assertEqual(len(contract.insert_payload(row_id).encode()), 256)
            self.assertEqual(payload, contract.fixture_payload(row_id))
        for scale, expected in ((1000, "i < 1000"), (100000, "i < 100000")):
            path = contract.fixture_relative(scale)
            text = contract.read_sql(path)
            self.assertIn(expected, text)
            self.assertIn("printf('r%08d:', i)", text)
            self.assertIn("zeroblob", text)
        self.assertEqual(len(contract.split_sql_file("sql/00-schema.sql")), 2)
        self.assertGreaterEqual(
            len(contract.split_sql_file("sql/01-fixture-1000.sql")), 3)
        self.assertEqual(len(contract.split_sql_file(
            "sql/15-explicit-transfer.sql")), 2)

    def test_authoritative_scenarios_sql_and_placeholders(self):
        workloads = contract.load_workloads()
        acceptance = contract.load_acceptance()
        expected = ["persistent_select_one", "persistent_read", "persistent_write_verify",
                    "persistent_insert_read", "churn_read_close", "churn_insert_read_close",
                    "pipeline_write_read"]
        for index, workflow in enumerate(expected, 1):
            scenario = workloads["scenarios"]["H%d" % index]
            self.assertEqual(scenario["workflow"], workflow)
            self.assertTrue(scenario["available_in_w0_a"])
            self.assertTrue(scenario["formal_eligible"])
        self.assertEqual(acceptance["sampling"]["formal"]["required_scenarios"],
                         ["H%d" % i for i in range(1, 8)])
        for scenario in ("H8", "H9", "H10", "N1"):
            for document in (workloads["scenarios"], acceptance["targets"]):
                self.assertFalse(document[scenario]["available_in_w0_a"])
                self.assertFalse(document[scenario]["formal_eligible"])
        self.assertEqual(contract.read_sql("sql/10-select-one.sql").strip(), "SELECT 1;")
        self.assertIn("LIMIT 100", contract.read_sql("sql/14-range-read.sql"))
        self.assertEqual(workloads["fixture"]["tables"], ["perf_kv", "perf_events"])
        self.assertEqual(acceptance["sampling"]["latency_denominator"], "all_recorded_requests")
        for rule in acceptance["extended_evidence"].values():
            self.assertFalse(rule["available_in_w0_a"])
        self.assertEqual(acceptance["extended_evidence"]["capacity"]["arrival_fractions"],
                         [0.25, 0.50, 0.75, 0.90])
        quotas = acceptance["extended_evidence"]["quotas"]
        self.assertEqual((quotas["max_concurrent"], quotas["overload_concurrency"]), (4, 8))

    def test_entry_points_have_help(self):
        commands = [
            [sys.executable, str(PERF_ROOT / "issue11" / "gate.py"), "--help"],
            [sys.executable, str(PERF_ROOT / "issue11" / "http" / "runner.py"),
             "--help"],
            [sys.executable, str(PERF_ROOT / "issue11" / "attribution" /
                                 "recompute.py"), "--help"],
        ]
        for command in commands:
            completed = subprocess.run(command, capture_output=True, text=True,
                                       timeout=30)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn("usage:", completed.stdout.lower())


if __name__ == "__main__":
    unittest.main()
