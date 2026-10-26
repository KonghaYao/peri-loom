"""Recovery reader integration with an in-memory mock, never durability evidence."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from issue11 import evidence_io
from issue11.gate_checks import _verify_run_hashes
from issue11.http.client import HranaClient
from issue11.verify_recovery import collect
from test_issue11_extension_v2 import load_view
from test_issue11_support import MockHranaServer, TEST_CREDENTIAL, run_runner, runner_arguments


class RecoveryReaderTests(unittest.TestCase):
    def test_actual_mock_reads_match_ack_set_and_attach_reseals_integrity(self):
        with tempfile.TemporaryDirectory() as directory, MockHranaServer() as server:
            root = Path(directory) / "campaign"
            with run_runner(runner_arguments(server, root, scenario="H4", samples=2, warmup=1)) as result:
                self.assertEqual(result[0], 0, result[2])
            run = next((root / "runs").iterdir())
            view = load_view(run)
            restart = Path(directory) / "synthetic-restart.json"
            evidence_io.write_json(restart, {
                "before_instance": "synthetic-unit-fixture-before",
                "after_instance": "synthetic-unit-fixture-after", "method": "process_crash",
                "stopped_at_unix_ms": view["metadata"]["completed_at_unix_ms"],
            })
            original_hashes = (run / "SHA256SUMS.json").read_bytes()
            old_count = len(server.state.requests)
            # A fresh interpreter catches script-directory stdlib shadowing;
            # importing main() in this test process already loads stdlib http.
            script = Path(__file__).parent / "issue11/verify_recovery.py"
            result = subprocess.run([sys.executable, str(script), "--run-directory", str(run),
                "--endpoint", server.endpoint, "--restart-evidence", str(restart),
                "--diagnostic-plaintext", "--attach"], cwd=script.parent,
                env={**os.environ, "PERI_SQL_TOKEN": TEST_CREDENTIAL},
                capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertNotIn(TEST_CREDENTIAL, result.stdout + result.stderr)
            proof = evidence_io.read_json(run / "oracle.json")["recovery"]
            self.assertEqual(proof["acked_writes"], 3)
            self.assertEqual(len(proof["rows"]), 3)
            self.assertEqual(len(server.state.requests) - old_count, 4)
            self.assertNotEqual((run / "SHA256SUMS.json").read_bytes(), original_hashes)
            problems = []
            _verify_run_hashes(run, problems)
            self.assertEqual(problems, [])
            observed = [json.loads(item["body"])["requests"][0] for item in server.state.requests[old_count:]]
            self.assertTrue(all(item["stmt"]["sql"].startswith("SELECT") for item in observed[:-1]))
            self.assertEqual(observed[-1]["type"], "close")

    def test_wrong_recovered_value_cannot_create_a_proof(self):
        with tempfile.TemporaryDirectory() as directory, MockHranaServer() as server:
            root = Path(directory) / "campaign"
            with run_runner(runner_arguments(server, root, scenario="H3", samples=2, warmup=1)) as result:
                self.assertEqual(result[0], 0, result[2])
            run = next((root / "runs").iterdir())
            view = load_view(run)
            server.state.connection.execute("UPDATE perf_kv SET value = 999 WHERE id = 1")
            client = HranaClient(server.endpoint, TEST_CREDENTIAL, allow_plaintext_loopback=True)
            try:
                with self.assertRaisesRegex(ValueError, "recovery_final_values_mismatch"):
                    collect(view, client, {"before_instance": "synthetic-before",
                        "after_instance": "synthetic-after", "method": "graceful",
                        "stopped_at_unix_ms": view["metadata"]["completed_at_unix_ms"]})
            finally:
                client.close()


if __name__ == "__main__":
    unittest.main()
