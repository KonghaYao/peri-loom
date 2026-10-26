import json
from pathlib import Path
import sys
import tempfile
import unittest

PERF_ROOT = Path(__file__).resolve().parent
if str(PERF_ROOT) not in sys.path:
    sys.path.insert(0, str(PERF_ROOT))

from issue11 import evidence_io
from issue11.http import client as http_client
from test_issue11_support import (
    MockHranaServer,
    TEST_CREDENTIAL,
    run_runner,
    runner_arguments,
)


def _read_jsonl(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines()
            if line.strip()]


class RunnerTests(unittest.TestCase):
    def test_real_http_count_reuse_baton_and_redaction(self):
        with tempfile.TemporaryDirectory() as directory, \
                MockHranaServer(rows=1000) as server:
            evidence_root = Path(directory) / "evidence"
            with run_runner(runner_arguments(server, evidence_root,
                                             samples=5, warmup=2)) as result:
                code, stdout, stderr = result
            self.assertEqual(code, 0, stderr)
            self.assertEqual(len(server.state.requests), 8)
            self.assertEqual(len(server.state.connection_ports), 1)
            self.assertNotIn(TEST_CREDENTIAL, stdout)
            self.assertNotIn(TEST_CREDENTIAL, stderr)
            run_directory = next((evidence_root / "runs").iterdir())
            requests = _read_jsonl(run_directory / "requests.jsonl")
            operations = _read_jsonl(run_directory / "operations.jsonl")
            attribution = _read_jsonl(run_directory / "request-attribution.jsonl")
            self.assertEqual(len(requests), 8)
            self.assertEqual(len(operations), 7)
            self.assertEqual(len(attribution), 8)
            self.assertEqual(max(record["connections_created"]
                                 for record in requests), 1)
            self.assertFalse(requests[0]["reused_connection"])
            self.assertTrue(all(record["reused_connection"]
                                for record in requests[1:]))
            self.assertTrue(all(record["response_complete"]
                                for record in requests))
            self.assertTrue(all(record["response_bytes"] > 0 for record in requests))
            self.assertEqual(requests[0]["baton_in"], "")
            self.assertTrue(all(record["baton_in"] == "baton-1"
                                for record in requests[1:]))
            self.assertTrue(all(not record["close_step"] for record in requests[:-1]))
            self.assertTrue(requests[-1]["close_step"])
            self.assertTrue(requests[-1]["close_acknowledged"])
            self.assertEqual(requests[-1]["phase"], "cleanup")
            self.assertTrue(all(record["request_headers_redacted"]["Authorization"]
                                == "Bearer <REDACTED>" for record in requests))
            for request in server.state.requests:
                self.assertEqual(request["content_type"], "application/json")
                self.assertEqual(request["authorization"],
                                 "Bearer " + TEST_CREDENTIAL)
                self.assertTrue(request["correlation"])
                self.assertTrue(request["step_ids"])
                self.assertTrue(request["request_id"])
            first = json.loads(server.state.requests[0]["body"])
            sql = first["requests"][0]["stmt"]["sql"]
            self.assertEqual(sql, "SELECT 1")
            self.assertEqual(first["requests"][0]["stmt"]["args"], [])
            self.assertEqual(operations[0]["verification"]["ok"], True)
            self.assertEqual(operations[0]["retry_count"], 0)
            self.assertTrue(all(record["process_coverage"] == ["client"]
                                for record in attribution))
            self.assertEqual(evidence_io.verify_required(run_directory), [])
            sums = evidence_io.read_hashes(run_directory)
            for name, expected in sums.items():
                self.assertEqual(contract_digest(run_directory / name), expected)
            for name in evidence_io.REQUIRED_RUN_ARTIFACTS:
                self.assertNotIn(TEST_CREDENTIAL.encode(),
                                 (run_directory / name).read_bytes(), name)
            summary = json.loads((run_directory / "summary.json").read_text())
            self.assertEqual(summary["status"], "RUN_COMPLETED")
            self.assertEqual(summary["counts"]["timed_success"], 5)

    def test_churn_close_acknowledged(self):
        with tempfile.TemporaryDirectory() as directory, \
                MockHranaServer(rows=1000) as server:
            evidence_root = Path(directory) / "evidence"
            with run_runner(runner_arguments(server, evidence_root,
                                             scenario="H5", samples=2,
                                             warmup=1)) as result:
                code, _, stderr = result
            self.assertEqual(code, 0, stderr)
            self.assertEqual(len(server.state.requests), 3)
            run_directory = next((evidence_root / "runs").iterdir())
            requests = _read_jsonl(run_directory / "requests.jsonl")
            operations = _read_jsonl(run_directory / "operations.jsonl")
            closed = [record for record in requests if record["close_step"]]
            self.assertEqual(len(closed), 3)
            self.assertTrue(all(record["close_acknowledged"] for record in closed))
            self.assertTrue(all(record["outcome"] == "success" for record in operations))

    def test_unknown_is_terminal_and_never_retried(self):
        with tempfile.TemporaryDirectory() as directory, \
                MockHranaServer(rows=1000,
                                mode="unknown_after_execute") as server:
            evidence_root = Path(directory) / "evidence"
            with run_runner(runner_arguments(server, evidence_root,
                                             samples=5, warmup=2)) as result:
                code, stdout, stderr = result
            self.assertEqual(code, 1, stderr)
            self.assertEqual(len(server.state.requests), 1)
            self.assertEqual(server.state.unknown_responses, 1)
            run_directory = next((evidence_root / "runs").iterdir())
            operations = _read_jsonl(run_directory / "operations.jsonl")
            self.assertEqual(len(operations), 1)
            self.assertEqual(operations[0]["outcome"], "outcome_unknown")
            self.assertEqual(operations[0]["terminal"]["recorded"], True)
            self.assertEqual(operations[0]["terminal"]["no_retry"], True)
            requests = _read_jsonl(run_directory / "requests.jsonl")
            self.assertEqual(requests[0]["outcome"], "outcome_unknown")
            summary = json.loads((run_directory / "summary.json").read_text())
            self.assertEqual(summary["status"], "RUN_STOPPED")

    def test_truncated_response_is_unknown(self):
        with tempfile.TemporaryDirectory() as directory, \
                MockHranaServer(rows=1000,
                                mode="truncate_after_execute") as server:
            evidence_root = Path(directory) / "evidence"
            with run_runner(runner_arguments(server, evidence_root,
                                             samples=5, warmup=2)) as result:
                code, _, stderr = result
            self.assertEqual(code, 1, stderr)
            self.assertEqual(len(server.state.requests), 1)
            self.assertEqual(server.state.truncated_responses, 1)
            run_directory = next((evidence_root / "runs").iterdir())
            requests = _read_jsonl(run_directory / "requests.jsonl")
            self.assertEqual(requests[0]["outcome"], "outcome_unknown")
            self.assertFalse(requests[0]["response_complete"])
            operations = _read_jsonl(run_directory / "operations.jsonl")
            self.assertEqual(operations[0]["outcome"], "outcome_unknown")

    def test_scenario_workflows_verify_client_results(self):
        scenarios = ("H2", "H3", "H4", "H5", "H6", "H7")
        for scenario in scenarios:
            with self.subTest(scenario=scenario), \
                    tempfile.TemporaryDirectory() as directory, \
                    MockHranaServer(rows=1000) as server:
                evidence_root = Path(directory) / "evidence"
                with run_runner(runner_arguments(server, evidence_root,
                                                 scenario=scenario, samples=2,
                                                 warmup=1)) as result:
                    code, _, stderr = result
                self.assertEqual(code, 0, scenario + ": " + stderr)
                run_directory = next((evidence_root / "runs").iterdir())
                operations = _read_jsonl(run_directory / "operations.jsonl")
                self.assertEqual(len(operations), 6 if scenario == "H4" else 3)
                self.assertTrue(all(record["verification"]["ok"]
                                    for record in operations), scenario)
                self.assertTrue(all(record["outcome"] == "success"
                                    for record in operations), scenario)
                oracle = json.loads((run_directory / "oracle.json").read_text())
                from issue11 import gate
                checked = gate.evaluate(evidence_root)
                self.assertNotEqual(checked["status"], gate.FAIL, checked["reasons"])
                self.assertEqual(oracle["mismatches"], [])
                if scenario in {"H4", "H6", "H7"}:
                    self.assertEqual(server.state.connection.execute(
                        "SELECT count(*) FROM perf_events").fetchone()[0], 3)
                if scenario == "H3":
                    self.assertEqual(server.state.connection.execute(
                        "SELECT sum(value) FROM perf_kv").fetchone()[0], 3)

    def test_setup_records_setup_phase_and_loads_fixture(self):
        with tempfile.TemporaryDirectory() as directory, \
                MockHranaServer(rows=0) as server:
            evidence_root = Path(directory) / "evidence"
            with run_runner(runner_arguments(server, evidence_root,
                                             samples=2, warmup=1,
                                             extra=["--setup"])) as result:
                code, _, stderr = result
            self.assertEqual(code, 0, stderr)
            self.assertEqual(server.state.connection.execute(
                "SELECT count(*) FROM perf_kv").fetchone()[0], 1000)
            run_directory = next((evidence_root / "runs").iterdir())
            requests = _read_jsonl(run_directory / "requests.jsonl")
            setup = [record for record in requests if record["phase"] == "setup"]
            self.assertEqual(len(setup), 1)
            self.assertTrue(setup[0]["close_step"])
            self.assertTrue(setup[0]["close_acknowledged"])
            operations = _read_jsonl(run_directory / "operations.jsonl")
            self.assertEqual(len(operations), 3)
            self.assertTrue(all(record["phase"] != "setup"
                                for record in operations))
            oracle = json.loads((run_directory / "oracle.json").read_text())
            self.assertTrue(oracle["setup_executed"])

    def test_quick_2000_real_runner_coverage_and_single_run(self):
        from issue11 import gate
        from issue11.attribution import aggregate
        with tempfile.TemporaryDirectory() as directory, MockHranaServer() as server:
            root = Path(directory) / "quick"
            args = runner_arguments(server, root, samples=2000, warmup=200)
            args.remove("--synthetic-test-fixture")
            with run_runner(args) as result:
                self.assertEqual(result[0], 0, result[2])
            run = next((root / "runs").iterdir())
            attribution = aggregate.aggregate(aggregate.load_requests(run / "request-attribution.jsonl"))
            self.assertGreaterEqual(attribution["unattributed"]["coverage"], 0.95)
            import subprocess
            recomputed = subprocess.run(
                [sys.executable, str(PERF_ROOT / "issue11/attribution/recompute.py"),
                 "--request-attribution", str(run / "request-attribution.jsonl"),
                 "--operations", str(run / "operations.jsonl")],
                capture_output=True, text=True, timeout=30)
            self.assertEqual(recomputed.returncode, 0, recomputed.stderr)
            self.assertEqual(json.loads(recomputed.stdout)["request_count"], 2201)
            campaign_result = gate.evaluate(root)
            single_result = gate.evaluate(run)
            for checked in (campaign_result, single_result):
                self.assertIn(checked["status"], (gate.INCOMPLETE, gate.PHASE_PASS), checked["reasons"])
                self.assertFalse(any("run_scenario" in reason for reason in checked["reasons"]))
            print("反例 quick: runner=0 timed=2000 warmup=200 coverage=%.4f campaign=%s single=%s" % (
                attribution["unattributed"]["coverage"], campaign_result["status"], single_result["status"]))

    def test_h3_write_only_and_h4_separate_select(self):
        for scenario in ("H3", "H4", "H6", "H7"):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as directory, MockHranaServer() as server:
                root = Path(directory) / "run"
                with run_runner(runner_arguments(server, root, scenario=scenario, samples=2, warmup=1)) as result:
                    self.assertEqual(result[0], 0, result[2])
                run = next((root / "runs").iterdir())
                operations = _read_jsonl(run / "operations.jsonl")
                requests = {r["request_id"]: r for r in _read_jsonl(run / "requests.jsonl")}
                for operation in operations:
                    count = {"H3": 2, "H4": 1, "H6": 3, "H7": 1}[scenario]
                    self.assertEqual(len(operation["http_request_ids"]), count)
                    if scenario == "H3":
                        read = requests[operation["http_request_ids"][1]]
                        self.assertLessEqual(operation["completed_ns"], read["started_ns"])
                if scenario == "H4":
                    self.assertTrue(all(len(json.loads(r["body"])["requests"]) == 1
                                        for r in server.state.requests))

    def test_reserved_scenarios_are_not_executable(self):
        with tempfile.TemporaryDirectory() as directory, MockHranaServer() as server:
            for scenario in ("H8", "H9", "H10", "N1"):
                with run_runner(runner_arguments(server, Path(directory) / scenario,
                                                scenario=scenario)) as result:
                    self.assertEqual(result[0], 2)
                    self.assertIn("reserved", result[2])
            self.assertEqual(server.state.requests, [])

    def test_tls_is_required_outside_loopback_diagnostic(self):
        with self.assertRaises(http_client.ClientError):
            http_client.HranaClient("http://performance.example:9443",
                                    TEST_CREDENTIAL)
        with self.assertRaises(http_client.ClientError):
            http_client.HranaClient("http://127.0.0.1:9", TEST_CREDENTIAL)
        client = http_client.HranaClient("http://127.0.0.1:9", TEST_CREDENTIAL,
                                         allow_plaintext_loopback=True)
        self.assertFalse(client.tls)
        client.close()


def contract_digest(path):
    from perf_contract import digest
    return digest(path)


if __name__ == "__main__":
    unittest.main()
