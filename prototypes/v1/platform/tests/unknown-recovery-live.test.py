import importlib.util
import os
import pathlib
import tempfile
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location("unknown_recovery_live", pathlib.Path(__file__).with_name("unknown-recovery-live.py"))
HARNESS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HARNESS)


class UnknownRecoveryHarnessTests(unittest.TestCase):
    def test_commit_case_requires_independent_native_ledger_witness(self):
        self.assertIn("commit", HARNESS.CASES)
        marker = "TRUE_FAULT case=commit native_ack=1 dropped_response=1 worker=worker-a native_request_id=fresh_commit durable_reload=isolated safe_reads=no_native_open decision=new_database_required historical_cause=not_attributed owned_worker_stop=proven local_resources_retired=1 namespace_isolated=1"
        witness = "COMMIT_WITNESS native_ack=1 ledger_rows=1 retained_session_read=1 commit_replays=0 blind_close=0 new_sessions=0"
        self.assertFalse(HARNESS.report(marker, ("commit",), 0)["passed"])
        self.assertTrue(HARNESS.report(marker + "\n" + witness, ("commit",), 0)["passed"])
        self.assertFalse(HARNESS.report(marker + "\n" + witness.replace("ledger_rows=1", "ledger_rows=0"), ("commit",), 0)["passed"])

    def test_environment_has_no_inherited_secrets_and_no_shell(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = pathlib.Path(directory) / "native"
            binary.write_text("fixture executable")
            binary.chmod(0o700)
            with mock.patch.dict(os.environ, {"AWS_SECRET_ACCESS_KEY": "forbidden", "PERI_PRIVATE_CONFIG": "old-config", "S3_ENDPOINT": "forbidden"}):
                arguments, environment, selected = HARNESS.command(binary, "all", True)
            self.assertEqual(arguments[:3], ["go", "test", "."])
            self.assertIn("-race", arguments)
            self.assertEqual(selected, HARNESS.CASES)
            self.assertEqual(environment["PERI_RUST_BINARY"], str(binary.resolve()))
            self.assertNotIn("AWS_SECRET_ACCESS_KEY", environment)
            self.assertNotIn("PERI_PRIVATE_CONFIG", environment)
            self.assertNotIn("S3_ENDPOINT", environment)

    def test_missing_or_non_executable_binary_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = pathlib.Path(directory) / "native"
            with self.assertRaises(FileNotFoundError):
                HARNESS.command(binary, "all")
            binary.write_text("not executable")
            binary.chmod(0o600)
            with self.assertRaises(ValueError):
                HARNESS.command(binary, "all")

    def test_marker_requires_real_ack_and_all_selected_cases(self):
        marker = "TRUE_FAULT case=checkpoint native_ack=1 dropped_response=1 worker=worker-a native_request_id=fresh_checkpoint durable_reload=isolated safe_reads=no_native_open decision=new_database_required historical_cause=not_attributed owned_worker_stop=proven local_resources_retired=1 namespace_isolated=1"
        self.assertTrue(HARNESS.report(marker, ("checkpoint",), 0)["passed"])
        for output, selected, returncode in (
            (marker, HARNESS.CASES, 0),
            (marker, ("checkpoint",), 1),
            (marker.replace("native_ack=1", "native_ack=0"), ("checkpoint",), 0),
            (marker.replace("owned_worker_stop=proven", "owned_worker_stop=unproven"), ("checkpoint",), 0),
            (marker + "\n" + marker, ("checkpoint",), 0),
            (marker + "\n--- SKIP: fixture", ("checkpoint",), 0),
            ("PASS", ("checkpoint",), 0),
        ):
            self.assertFalse(HARNESS.report(output, selected, returncode)["passed"])


if __name__ == "__main__":
    unittest.main()
