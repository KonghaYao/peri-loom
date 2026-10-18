import argparse
import hashlib
import hmac
import json
import threading
import importlib.util
import pathlib
import os
import sys
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

source = pathlib.Path(__file__).resolve().parents[1] / "dev.py"
spec = importlib.util.spec_from_file_location("peri_local_dev", source)
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)


class RuntimeTests(unittest.TestCase):
    def test_bootstrap_credential_can_be_reused_only_when_explicitly_injected(self):
        from unittest.mock import patch
        with patch.dict(os.environ, {"PERI_BOOTSTRAP_KEY": "x" * 64}):
            self.assertEqual(dev.bootstrap_credential(), "x" * 64)
        with patch.dict(os.environ, {"PERI_BOOTSTRAP_KEY": "short"}):
            with self.assertRaisesRegex(RuntimeError, "invalid injected bootstrap credential"):
                dev.bootstrap_credential()

    def project(self, directory):
        project = dev.Project(argparse.Namespace(run_dir=directory, no_build=True, backend="local", listen="127.0.0.1:19443", tls=False, writer_admission=False))
        return project

    def prepared_project(self, directory):
        project = self.project(directory)
        project.prepare()
        return project

    def test_already_exited_uncertain_platform_is_not_clean(self):
        with tempfile.TemporaryDirectory(prefix="peri-test-launcher-") as directory:
            project = self.prepared_project(directory)
            process = Mock(returncode=1)
            process.poll.return_value = 1
            project.processes["platform"] = process
            with self.assertRaisesRegex(RuntimeError, "clean proof"):
                project.cleanup()
            process.terminate.assert_not_called()

    def test_joined_uncertain_platform_is_not_clean(self):
        with tempfile.TemporaryDirectory(prefix="peri-test-launcher-") as directory:
            project = self.prepared_project(directory)
            process = Mock()
            process.poll.return_value = None
            process.wait.return_value = 1
            project.processes["platform"] = process
            with self.assertRaisesRegex(RuntimeError, "clean proof"):
                project.cleanup()
            process.terminate.assert_called_once()

    def test_exited_authority_or_worker_is_not_clean(self):
        for identity in ("authority", "worker-a", "worker-b"):
            with self.subTest(identity=identity), tempfile.TemporaryDirectory(prefix="peri-test-launcher-") as directory:
                project = self.prepared_project(directory)
                process = Mock(returncode=1)
                process.poll.return_value = 1
                project.processes[identity] = process
                with self.assertRaisesRegex(RuntimeError, "clean proof"):
                    project.cleanup()

    def test_clean_platform_has_joined_exit_proof(self):
        with tempfile.TemporaryDirectory(prefix="peri-test-launcher-") as directory:
            project = self.prepared_project(directory)
            process = Mock()
            process.poll.return_value = None
            process.wait.return_value = 0
            project.processes["platform"] = process
            with patch.object(dev.time, "monotonic", return_value=100):
                project.cleanup()
            process.wait.assert_called_once()
            timeout = process.wait.call_args.kwargs["timeout"]
            self.assertEqual(timeout, min(dev.SHUTDOWN_STAGE_CAPS["platform"], project.shutdown_deadline - 100))
            self.assertLessEqual(timeout, project.shutdown_budget_seconds)

    def test_launcher_lock_excludes_concurrent_start_and_releases_after_cleanup(self):
        with tempfile.TemporaryDirectory(prefix="peri-test-launcher-") as directory:
            first, second = self.project(directory), self.project(directory)
            first.acquire_run_lock()
            try:
                with self.assertRaisesRegex(RuntimeError, "active launcher"):
                    second.acquire_run_lock()
                first.prepare()
                with self.assertRaisesRegex(RuntimeError, "active launcher"):
                    second.acquire_run_lock()
            finally:
                first.cleanup()
                first.release_run_lock()
            second.acquire_run_lock()
            second.release_run_lock()

    def test_launcher_lock_rejects_symlink_and_unsafe_file(self):
        with tempfile.TemporaryDirectory(prefix="peri-test-launcher-") as directory:
            target = pathlib.Path(directory) / "target"
            target.mkdir(mode=0o700)
            shortcut = pathlib.Path(directory) / "shortcut"
            shortcut.symlink_to(target, target_is_directory=True)
            with self.assertRaisesRegex(RuntimeError, "symlink"):
                self.project(str(shortcut)).acquire_run_lock()
            lock = target / "launcher.lock"
            lock.symlink_to(pathlib.Path(directory) / "missing")
            with self.assertRaises(OSError):
                self.project(str(target)).acquire_run_lock()
            lock.unlink()
            lock.write_text("unsafe")
            lock.chmod(0o644)
            with self.assertRaisesRegex(RuntimeError, "private regular file"):
                self.project(str(target)).acquire_run_lock()

    def test_s3_storage_identity_binds_endpoint_bucket_region_not_secrets(self):
        synthetic = {"S3_ENDPOINT": "http://127.0.0.1:9000", "S3_BUCKET": "peri-test-synthetic-original", "S3_REGION": "us-east-1", "S3_SECRET_ACCESS_KEY": "synthetic-not-real"}
        with tempfile.TemporaryDirectory(prefix="peri-test-launcher-") as directory, patch.dict(dev.os.environ, synthetic):
            project = self.project(directory)
            project.arguments.backend = "s3"
            project.prepare()
            self.assertNotIn("synthetic-not-real", (pathlib.Path(directory) / "launch.json").read_text())
            restarted = self.project(directory)
            restarted.arguments.backend = "s3"
            with self.assertRaisesRegex(RuntimeError, "proof set incomplete"):
                restarted.prepare()
            for name in ("S3_BUCKET", "S3_ENDPOINT", "S3_REGION"):
                with patch.dict(dev.os.environ, {name: "different"}):
                    with self.assertRaisesRegex(RuntimeError, "storage identity"):
                        restarted.prepare()

    def test_restart_terminal_requires_no_unresolved_platform_operations(self):
        with tempfile.TemporaryDirectory(prefix="peri-test-launcher-") as directory:
            project = self.project(directory)
            project.prepare()
            configuration = project.config["owned_stop_proof"]
            path = pathlib.Path(directory) / "platform/state.json"
            state = {"owned_run_generation": configuration["generation"], "operations": {}}
            dev.private_json(path, {"version": 1, "payload": state})
            dev.verify_no_unresolved_operations(project.directory, configuration)
            known = {"state": "terminal", "business_outcome": "completed", "execution_proven": True,
                     "cleanup_outcome": "completed"}
            state["operations"] = {"known": known}
            dev.atomic_private_json(path, {"version": 1, "payload": state})
            dev.verify_no_unresolved_operations(project.directory, configuration)
            state["operations"]["unknown"] = {**known, "state": "quarantined", "ambiguous": True}
            dev.atomic_private_json(path, {"version": 1, "payload": state})
            with self.assertRaisesRegex(RuntimeError, "unresolved platform operation"):
                dev.verify_no_unresolved_operations(project.directory, configuration)

    def test_redirects_cannot_forward_credentials(self):
        self.assertIsNone(dev.NoRedirect().redirect_request(None, None, 302, "redirect", {}, "https://outside.invalid"))

    def test_child_service_has_isolated_signal_session(self):
        with tempfile.TemporaryDirectory(prefix="peri-test-launcher-") as directory:
            project = self.project(directory)
            project.prepare()
            event = project.start("worker-a", [sys.executable, "-c", "import json,os; print(json.dumps({'ready':True,'pid':os.getpid(),'sid':os.getsid(0)}),flush=True)"], dev.safe_environment())
            self.assertEqual(event["pid"], event["sid"])
            self.assertNotEqual(event["sid"], os.getsid(0))
            project.processes["worker-a"].wait(timeout=5)
            project.cleanup()

    def test_launcher_declares_stage_cap_and_orchestration_grace_to_the_platform(self):
        # The platform refuses a stage cap that reaches the declared grace, so the
        # declaration is what turns the cap into a checked statement instead of a
        # self-reported number. Removing the injection has to fail here, not
        # silently drop the check.
        with tempfile.TemporaryDirectory(prefix="peri-test-launcher-") as directory:
            project = self.project(directory)
            children = {}

            def record_start(name, command, environment):
                children[name] = dict(environment)
                return {}

            with patch.object(project, "start", side_effect=record_start), patch.object(dev, "request", side_effect=RuntimeError("offline stop before API")):
                with self.assertRaisesRegex(RuntimeError, "offline stop before API"):
                    project.run()
            self.assertEqual(children["platform"]["PERI_SHUTDOWN_STAGE_CAP_MS"], str(dev.SHUTDOWN_STAGE_CAPS["platform"] * 1000))
            self.assertEqual(children["platform"]["PERI_SHUTDOWN_OUTER_GRACE_MS"], str(dev.ORCHESTRATION_GRACE_SECONDS * 1000))
            self.assertLess(int(children["platform"]["PERI_SHUTDOWN_STAGE_CAP_MS"]), int(children["platform"]["PERI_SHUTDOWN_OUTER_GRACE_MS"]))
            for name, environment in children.items():
                if name != "platform":
                    self.assertNotIn("PERI_SHUTDOWN_STAGE_CAP_MS", environment)
                    self.assertNotIn("PERI_SHUTDOWN_OUTER_GRACE_MS", environment)

    def test_generated_tls_has_separate_ca_and_verified_server_leaf(self):
        with tempfile.TemporaryDirectory(prefix="peri-test-launcher-") as directory, patch.dict(dev.os.environ, dev.safe_environment(), clear=True):
            project = self.project(directory)
            project.arguments.tls = True
            project.prepare()
            certificate = project.config["tls_certificate"]
            ca = project.launch["ca_file"]
            self.assertNotEqual(certificate, ca)
            leaf = subprocess.check_output(["openssl", "x509", "-in", certificate, "-noout", "-text"], text=True, timeout=5)
            self.assertIn("CA:FALSE", leaf)
            self.assertIn("TLS Web Server Authentication", leaf)
            self.assertIn("IP Address:127.0.0.1", leaf)
            verified = subprocess.run(["openssl", "verify", "-CAfile", ca, certificate], capture_output=True, text=True, timeout=5)
            self.assertEqual(verified.returncode, 0)
            self.assertEqual((pathlib.Path(directory)/"keys/dev-ca.key").stat().st_mode & 0o777, 0o600)
            self.assertEqual(pathlib.Path(project.config["tls_key"]).stat().st_mode & 0o777, 0o600)


TERMINAL_OPERATION = {"state": "terminal", "business_outcome": "completed", "execution_proven": True, "cleanup_outcome": "completed"}


class OperationViewMaterializationTests(unittest.TestCase):
    def project(self, directory):
        project = dev.Project(argparse.Namespace(run_dir=directory, no_build=True, backend="local", listen="127.0.0.1:19443", tls=False, writer_admission=False))
        project.prepare()
        return project

    def publish(self, project, **fields):
        state = {"owned_run_generation": project.config["owned_stop_proof"]["generation"], "operations": {}}
        state.update(fields)
        dev.atomic_private_json(project.directory / "platform/state.json", {"version": 1, "payload": state})
        return project.config["owned_stop_proof"]

    def test_materialized_final_view_remains_reusable(self):
        with tempfile.TemporaryDirectory(prefix="peri-test-launcher-") as directory:
            project = self.project(directory)
            configuration = self.publish(project, operation_storage_version=1, revision=7, operations_checkpoint=7)
            dev.verify_no_unresolved_operations(project.directory, configuration)
            self.publish(project, operation_storage_version=1, revision=7, operations_checkpoint=7, operations={"known": TERMINAL_OPERATION})
            dev.verify_no_unresolved_operations(project.directory, configuration)

    def test_operation_view_that_trails_the_sidecar_is_refused_as_unmaterialized(self):
        cases = (
            ("missing checkpoint", {"operation_storage_version": 1, "revision": 7}, "was not materialized at shutdown"),
            ("stale checkpoint", {"operation_storage_version": 1, "revision": 8, "operations_checkpoint": 7}, "was not materialized at shutdown"),
            ("zero checkpoint", {"operation_storage_version": 1, "revision": 7, "operations_checkpoint": 0}, "was not materialized at shutdown"),
            ("encoded checkpoint", {"operation_storage_version": 1, "revision": 7, "operations_checkpoint": "7"}, "was not materialized at shutdown"),
            ("boolean checkpoint", {"operation_storage_version": 1, "revision": 7, "operations_checkpoint": True}, "was not materialized at shutdown"),
            ("missing revision", {"operation_storage_version": 1, "operations_checkpoint": 7}, "was not materialized at shutdown"),
            ("encoded revision", {"operation_storage_version": 1, "revision": "7", "operations_checkpoint": 7}, "was not materialized at shutdown"),
            ("unreviewed storage identity", {"operation_storage_version": 2, "revision": 7, "operations_checkpoint": 7}, "storage identity unrecognized"),
            ("encoded storage identity", {"operation_storage_version": "1", "revision": 7, "operations_checkpoint": 7}, "storage identity unrecognized"),
        )
        for name, fields, message in cases:
            with self.subTest(case=name), tempfile.TemporaryDirectory(prefix="peri-test-launcher-") as directory:
                project = self.project(directory)
                configuration = self.publish(project, **fields)
                with self.assertRaises(dev.OperationViewNotMaterialized) as raised:
                    dev.verify_no_unresolved_operations(project.directory, configuration)
                self.assertIn(message, str(raised.exception))
                self.assertNotIn("unresolved platform operation", str(raised.exception))

    def test_legacy_payload_without_storage_version_keeps_previous_criteria(self):
        with tempfile.TemporaryDirectory(prefix="peri-test-launcher-") as directory:
            project = self.project(directory)
            configuration = self.publish(project, revision=7)
            dev.verify_no_unresolved_operations(project.directory, configuration)
            self.publish(project, operation_storage_version=0, revision=7, operations={"known": TERMINAL_OPERATION})
            dev.verify_no_unresolved_operations(project.directory, configuration)
            self.publish(project, operation_storage_version=0, revision=7, operations={"known": TERMINAL_OPERATION, "unknown": {"state": "quarantined", "ambiguous": True}})
            with self.assertRaisesRegex(RuntimeError, "unresolved platform operation"):
                dev.verify_no_unresolved_operations(project.directory, configuration)

    def test_materialized_view_still_refuses_unresolved_records(self):
        with tempfile.TemporaryDirectory(prefix="peri-test-launcher-") as directory:
            project = self.project(directory)
            configuration = self.publish(project, operation_storage_version=1, revision=7, operations_checkpoint=7,
                                         operations={"known": TERMINAL_OPERATION, "unknown": {**TERMINAL_OPERATION, "state": "quarantined", "ambiguous": True}})
            with self.assertRaisesRegex(RuntimeError, "unresolved platform operation"):
                dev.verify_no_unresolved_operations(project.directory, configuration)


class ShutdownFixture(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="peri-stop-")
        self.addCleanup(self.temporary.cleanup)
        self.project = dev.Project(argparse.Namespace(run_dir=self.temporary.name, no_build=True, backend="local", listen="127.0.0.1:19443", tls=False, writer_admission=False))
        self.project.prepare()
        self.now = 100.0
        self.clock = patch.object(dev.time, "monotonic", side_effect=lambda: self.now)
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.order = []
        self.waits = []
        self.lock = self.project.directory / "authority/authority.lock"
        self.lock.write_bytes(b"dirty\n")

    def evidence(self):
        envelope = json.loads((self.project.directory / "launcher-stop.json").read_text())
        key = pathlib.Path(self.project.config["owned_stop_proof"]["key_file"]).read_bytes()
        self.assertEqual(envelope["mac"], hmac.new(key, envelope["payload"].encode(), hashlib.sha256).hexdigest())
        return json.loads(envelope["payload"])

    def complete_proof_chain(self, **state):
        configuration = self.project.config["owned_stop_proof"]
        for worker in self.project.launch["workers"]:
            identity = {"version": 1, "generation": configuration["generation"], "worker_id": worker,
                        "nonce": configuration["workers"][worker], "pid": 10001, "started": "incarnation",
                        "waited": False, "exit_code": 0}
            self.project.owned_identities[worker] = identity
            dev.atomic_private_json(self.project.directory / "owned-proof" / (worker + ".identity.json"), dev.signed_owned_identity(configuration, identity))
        payload = {"owned_run_generation": configuration["generation"], "operations": {}}
        payload.update(state)
        dev.private_json(self.project.directory / "platform/state.json", {"payload": payload})
        dev.private_json(self.project.directory / "platform/state.json.floor", {"version": 1})
        return configuration

    def child(self, name, delay=0):
        process = Mock(stdout=None)
        process.poll.return_value = None
        def wait(*, timeout):
            self.assertLessEqual(timeout, self.project.remaining())
            self.waits.append(timeout)
            self.now += min(delay, timeout)
            if delay > timeout:
                raise subprocess.TimeoutExpired(name, timeout)
            process.poll.return_value = 0
            self.order.append(name)
            return 0
        def kill():
            self.assertEqual(self.evidence()["outcome"], "unclean")
            self.assertFalse((self.project.directory / "launcher-terminal.json").exists())
            self.order.append("kill-" + name)
            process.poll.return_value = -9
        process.wait.side_effect = wait
        process.kill.side_effect = kill
        self.project.processes[name] = process
        return process


class TestLauncherBudgetSingleDeadline(ShutdownFixture):
    def test_first_signal_sets_deadline_only_once(self):
        self.project.begin_shutdown()
        self.assertEqual(self.project.shutdown_deadline, 140)
        self.now += 9
        self.project.begin_shutdown()
        self.assertEqual(self.project.shutdown_deadline, 140)
        self.assertEqual(self.project.remaining(), 31)

    def test_slow_platform_then_parallel_workers_then_authority(self):
        for name, delay in (("platform", 14), ("worker-a", 5), ("worker-b", 0), ("authority", 3)):
            self.child(name, delay)
        barrier = threading.Barrier(2)
        def rpc(endpoint, token, path, body, timeout):
            self.assertLessEqual(timeout, self.project.remaining())
            if path == "/control":
                self.assertIn("platform", self.order)
                barrier.wait(timeout=2)
            else:
                self.assertEqual(self.order[0], "platform")
                self.assertNotIn("authority", self.order[1:])
        with patch.object(dev, "request", side_effect=rpc):
            self.project.cleanup()
        self.assertEqual(self.order[0], "platform")
        self.assertEqual(self.order[-1], "authority")
        self.assertEqual(sorted(self.order[1:3]), ["worker-a", "worker-b"])
        self.assertLessEqual(self.now - 100, dev.SHUTDOWN_BUDGET_SECONDS)
        self.assertEqual(self.evidence()["outcome"], "in_progress")
        self.assertEqual(len(self.evidence()["stages"]), 4)
        for stage in self.evidence()["stages"]:
            self.assertLessEqual(stage["waited_ms"], stage["cap_ms"])

    def test_expired_deadline_persists_unclean_before_any_kill(self):
        processes = [self.child(name, 20) for name in ("platform", "worker-a", "worker-b", "authority")]
        self.project.begin_shutdown()
        self.now = 139
        with self.assertRaisesRegex(RuntimeError, "shutdown incomplete"):
            self.project.cleanup()
        self.assertEqual(self.waits, [1])
        self.assertEqual(self.now, 140)
        for process in processes:
            process.kill.assert_called_once()
        self.assertEqual(self.evidence()["deadline_ms"], 140000)
        self.assertEqual(self.lock.read_bytes(), b"dirty\n")
        self.assertEqual(self.evidence()["stages"][0]["signal"], 9)

    def test_stop_evidence_failure_prevents_kill(self):
        process = self.child("platform", 20)
        original = dev.atomic_private_json
        def write(path, value):
            if path.name == "launcher-stop.json" and json.loads(value["payload"])["outcome"] == "unclean":
                raise OSError("disk full")
            original(path, value)
        with patch.object(dev, "atomic_private_json", side_effect=write), self.assertRaisesRegex(OSError, "disk full"):
            self.project.cleanup()
        process.kill.assert_not_called()
        self.assertFalse(self.project.cleanup_complete)

    def test_prepare_rejects_invalid_budget_before_startup(self):
        for budget in (39, 41, float("nan"), float("inf"), -1):
            self.project.shutdown_budget_seconds = budget
            with self.subTest(budget=budget), self.assertRaisesRegex(RuntimeError, "shutdown budget"):
                self.project.prepare()


class TestBudgetExceededNeverClaimsClean(ShutdownFixture):
    def test_stage_timeout_never_reaches_terminal_or_clean_authority(self):
        for name in ("platform", "worker-a", "worker-b", "authority"):
            self.child(name, 16)
        with self.assertRaisesRegex(RuntimeError, "platform shutdown incomplete"):
            self.project.cleanup()
        with self.assertRaisesRegex(RuntimeError, "no clean proof"):
            self.project.mark_clean_terminal()
        self.assertEqual(self.evidence()["outcome"], "unclean")
        self.assertEqual(self.lock.read_bytes(), b"dirty\n")
        self.assertFalse((self.project.directory / "launcher-terminal.json").exists())
        self.assertLessEqual(self.now - 100, dev.SHUTDOWN_BUDGET_SECONDS)

    def test_signed_clean_stop_record_is_never_a_terminal_proof(self):
        self.project.begin_shutdown()
        self.project.write_stop_record("clean", "diagnostic_only")
        self.lock.write_bytes(b"clean\n")
        with self.assertRaisesRegex(RuntimeError, "terminal proof missing"):
            dev.verify_clean_terminal(self.project.directory, self.project.config["owned_stop_proof"])

    def test_proof_validation_failure_is_durably_unclean(self):
        for name in ("platform", "worker-a", "worker-b", "authority"):
            self.child(name)
        with patch.object(dev, "request", return_value={}):
            self.project.cleanup()
        with self.assertRaisesRegex(RuntimeError, "proof set incomplete"):
            self.project.mark_clean_terminal()
        self.assertEqual(self.evidence()["outcome"], "unclean")

    def shutdown_with_complete_chain(self):
        for name in ("platform", "worker-a", "worker-b", "authority"):
            self.child(name)
        with patch.object(dev, "request", return_value={}):
            self.project.cleanup()
        self.lock.write_bytes(b"clean\n")

    def test_unmaterialized_operation_view_is_durably_unclean(self):
        self.complete_proof_chain(operation_storage_version=1, revision=7)
        self.shutdown_with_complete_chain()
        with self.assertRaises(dev.OperationViewNotMaterialized):
            self.project.mark_clean_terminal()
        self.assertEqual(self.evidence()["outcome"], "unclean")
        self.assertIn("was not materialized at shutdown", self.evidence()["reason"])
        self.assertNotIn("unresolved", self.evidence()["reason"])
        self.assertFalse((self.project.directory / "launcher-terminal.json").exists())

    def test_materialized_unresolved_record_keeps_its_own_reason(self):
        self.complete_proof_chain(operation_storage_version=1, revision=7, operations_checkpoint=7,
                                  operations={"unknown": {**TERMINAL_OPERATION, "state": "quarantined", "ambiguous": True}})
        self.shutdown_with_complete_chain()
        with self.assertRaisesRegex(RuntimeError, "unresolved platform operation"):
            self.project.mark_clean_terminal()
        self.assertEqual(self.evidence()["outcome"], "unclean")
        self.assertIn("unresolved platform operation", self.evidence()["reason"])
        self.assertFalse((self.project.directory / "launcher-terminal.json").exists())


class TestSlowStopWithinBudgetIsClean(ShutdownFixture):
    def test_terminal_requires_complete_existing_proof_chain(self):
        configuration = self.complete_proof_chain()
        for name, delay in (("platform", 14), ("worker-a", 4), ("worker-b", 0), ("authority", 3)):
            self.child(name, delay)
        with patch.object(dev, "request", return_value={}):
            self.project.cleanup()
        self.lock.write_bytes(b"clean\n")
        self.project.mark_clean_terminal()
        dev.verify_clean_terminal(self.project.directory, configuration)
        self.assertEqual(self.evidence()["outcome"], "clean")
        self.assertFalse(self.evidence()["unresolved_operations"])


class TestCleanReuseRequiresMaterializedView(ShutdownFixture):
    def clean_terminal(self, **state):
        configuration = self.complete_proof_chain(**state)
        for worker in self.project.launch["workers"]:
            record = {**self.project.owned_identities[worker], "waited": True, "exit_code": 0}
            dev.atomic_private_json(self.project.directory / "owned-proof" / (worker + ".stop.json"), dev.signed_owned_identity(configuration, record))
        self.lock.write_bytes(b"clean\n")
        dev.atomic_private_json(self.project.directory / "launcher-terminal.json", dev.signed_owned_identity(configuration, dev.terminal_record(self.project.directory, configuration)))
        dev.atomic_private_json(self.project.directory / "launcher-stop.json", dev.signed_owned_identity(configuration, {
            "version": 1, "generation": configuration["generation"], "outcome": "clean",
            "reason": "terminal_proof_written", "deadline_ms": 1, "stages": [],
            "unresolved_operations": False}))
        return configuration

    def test_materialized_payload_authorizes_reuse(self):
        configuration = self.clean_terminal(operation_storage_version=1, revision=7, operations_checkpoint=7)
        restarted = dev.Project(self.project.arguments)
        restarted.prepare()
        self.assertEqual(restarted.config["owned_stop_proof"]["generation"], configuration["generation"])

    def test_stale_payload_never_authorizes_reuse(self):
        self.clean_terminal(operation_storage_version=1, revision=7)
        with self.assertRaises(dev.OperationViewNotMaterialized):
            dev.Project(self.project.arguments).prepare()
        self.assertTrue((self.project.directory / "launcher-terminal.json").is_file())


if __name__ == "__main__":
    unittest.main()
