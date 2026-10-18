import argparse
import contextlib
import hashlib
import hmac
import importlib.util
import io
import json
import os
import pathlib
import queue
import secrets
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from unittest.mock import Mock, patch


source = pathlib.Path(__file__).resolve().parents[1] / "dev.py"
spec = importlib.util.spec_from_file_location("owned_launcher", source)
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)


class LiveSQLClient:
    def __init__(self, run_directory):
        connection = json.loads((pathlib.Path(run_directory) / "connection.json").read_text())
        self.endpoint = connection["endpoint"]
        self.database = connection["database"]
        self.project_key = connection["project_api_key"]
        context = ssl.create_default_context(cafile=connection["ca_file"])
        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}), dev.NoRedirect(),
            urllib.request.HTTPSHandler(context=context))

    def post(self, path, body):
        request_id = secrets.token_hex(16)
        payload = dict(body, request_id=request_id)
        request = urllib.request.Request(
            self.endpoint + path, data=json.dumps(payload).encode(), method="POST",
            headers={"Authorization": "Bearer " + self.project_key,
                     "Content-Type": "application/json", "X-Request-ID": request_id})
        with self.opener.open(request, timeout=30) as response:
            reply = json.loads(response.read())
        if reply.get("request_id") != request_id or reply.get("ok") is not True:
            raise RuntimeError("SQL request failed: " + str(reply))
        return reply["result"]

    def statement(self, sql, kind):
        session = self.post("/v1/sql/open", {"database": self.database})["session"]
        try:
            return self.post("/v1/sql/" + kind, {"session": session, "sql": sql})
        finally:
            self.post("/v1/sql/close", {"session": session})


class OwnedStopProofTests(unittest.TestCase):
    def test_capacity_trace_is_strict_native_opt_in_without_secret_inheritance(self):
        for value, expected in ((None, "0"), ("1", "1"), ("0", "0"), ("true", "0"), (" 1", "0"), ("1\n", "0")):
            environment = {"PATH": "/synthetic/bin", "PERI_UNTRUSTED_KEY": "forbidden", "AWS_SECRET_ACCESS_KEY": "forbidden", "S3_SECRET_ACCESS_KEY": "forbidden"}
            if value is not None:
                environment["PERI_CAPACITY_TRACE"] = value
            with patch.dict(dev.os.environ, environment, clear=True):
                self.assertEqual(dev.safe_environment(capacity_trace=True), {"PATH": "/synthetic/bin", "PERI_CAPACITY_TRACE": expected})
                self.assertEqual(dev.safe_environment(), {"PATH": "/synthetic/bin"})

    def project(self, directory, backend="local"):
        arguments = argparse.Namespace(run_dir=directory, backend=backend, listen="127.0.0.1:19443", tls=False, no_build=True, writer_admission=False, sql_open_observer=False)
        project = dev.Project(arguments)
        project.prepare()
        self.addCleanup(project.stop.set)
        return project

    def start_worker(self, project):
        project.start("worker-a", [sys.executable, "-u", "-c", "import json,time; print(json.dumps({'ready':True}),flush=True); time.sleep(120)"], dev.safe_environment())
        self.addCleanup(self.finish_worker, project)

    def finish_worker(self, project):
        process = project.processes["worker-a"]
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        process.stdout.close()
        for log in project.logs:
            log.close()
        if project.maintenance is not None:
            project.maintenance.close()

    def test_owned_wait_signs_atomic_private_matching_incarnation(self):
        with tempfile.TemporaryDirectory(prefix="owned-proof-", dir="/tmp") as directory:
            project = self.project(directory)
            self.start_worker(project)
            configuration = project.config["owned_stop_proof"]
            destination = pathlib.Path(configuration["directory"]) / "worker-a.stop.json"
            self.assertFalse(destination.exists())
            project.start_maintenance()
            dev.stop_owned_worker(pathlib.Path(directory), "worker-a")
            signed = json.loads(destination.read_text())
            key = pathlib.Path(configuration["key_file"]).read_bytes()
            self.assertEqual(signed["mac"], hmac.new(key, signed["payload"].encode(), hashlib.sha256).hexdigest())
            proof = json.loads(signed["payload"])
            self.assertTrue(proof["waited"])
            self.assertEqual(proof["exit_code"], project.processes["worker-a"].returncode)
            for field in ("generation", "worker_id", "nonce", "pid", "started"):
                self.assertEqual(proof[field], project.owned_identities["worker-a"][field])
            self.assertEqual(destination.stat().st_mode & 0o777, 0o600)
            self.assertEqual(list(pathlib.Path(configuration["directory"]).glob("*.stop.json.*")), [])

    def test_graceful_shutdown_records_joined_worker_proof(self):
        with tempfile.TemporaryDirectory(prefix="owned-proof-", dir="/tmp") as directory:
            project = self.project(directory)
            process = Mock()
            process.poll.return_value = None
            process.wait.return_value = 0
            project.processes["worker-a"] = process
            configuration = project.config["owned_stop_proof"]
            identity = {"version": 1, "generation": configuration["generation"], "worker_id": "worker-a", "nonce": configuration["workers"]["worker-a"], "pid": 45678, "started": "test", "waited": False, "exit_code": 0}
            project.owned_identities["worker-a"] = identity
            with patch.object(dev, "request", return_value={}):
                project.cleanup()
            signed = json.loads((pathlib.Path(configuration["directory"]) / "worker-a.stop.json").read_text())
            key = pathlib.Path(configuration["key_file"]).read_bytes()
            self.assertEqual(signed["mac"], hmac.new(key, signed["payload"].encode(), hashlib.sha256).hexdigest())
            self.assertEqual(json.loads(signed["payload"]), {**identity, "waited": True, "exit_code": 0})

    def test_uncertain_shutdown_does_not_record_clean_worker_proof(self):
        for failure in ("rpc", "exit"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory(prefix="owned-proof-", dir="/tmp") as directory:
                project = self.project(directory)
                process = Mock()
                process.poll.return_value = None
                process.wait.return_value = 1 if failure == "exit" else 0
                project.processes["worker-a"] = process
                project.owned_identities["worker-a"] = {"worker_id": "worker-a"}
                with patch.object(dev, "request", side_effect=RuntimeError("uncertain") if failure == "rpc" else None):
                    with self.assertRaises(RuntimeError):
                        project.cleanup()
                self.assertFalse((pathlib.Path(project.config["owned_stop_proof"]["directory"]) / "worker-a.stop.json").exists())

    def test_failed_proof_persistence_makes_shutdown_unsuccessful(self):
        with tempfile.TemporaryDirectory(prefix="owned-proof-", dir="/tmp") as directory:
            project = self.project(directory)
            process = Mock()
            process.poll.return_value = None
            process.wait.return_value = 0
            project.processes["worker-a"] = process
            project.owned_identities["worker-a"] = {"worker_id": "worker-a"}
            with patch.object(dev, "request", return_value={}), patch.object(project, "record_joined_worker_stop", side_effect=OSError("no durable proof")):
                with self.assertRaisesRegex(RuntimeError, "owned stop proof unavailable"):
                    project.cleanup()

    def test_incarnation_cannot_be_relaunched_or_rebound(self):
        with tempfile.TemporaryDirectory(prefix="owned-proof-", dir="/tmp") as directory:
            project = self.project(directory)
            self.start_worker(project)
            project.stop_worker("worker-a")
            with self.assertRaisesRegex(RuntimeError, "cannot be reused"):
                project.start("worker-a", [], {})
            with self.assertRaisesRegex(RuntimeError, "proof set incomplete"):
                dev.Project(project.arguments).prepare()

    def test_maintenance_request_cannot_supply_pid_or_proof(self):
        with tempfile.TemporaryDirectory(prefix="owned-proof-", dir="/tmp") as directory:
            project = self.project(directory)
            self.start_worker(project)
            project.start_maintenance()
            with socket.socket(socket.AF_UNIX) as client:
                client.settimeout(5)
                client.connect(str(pathlib.Path(directory) / "owned-stop.sock"))
                client.sendall(json.dumps({"worker": "worker-a", "pid": os.getpid(), "proof": "/tmp/arbitrary"}).encode() + b"\n")
                self.assertEqual(json.loads(client.makefile("rb").readline(1024)), {"stopped": False})
            self.assertIsNone(project.processes["worker-a"].poll())
            self.assertFalse((pathlib.Path(project.config["owned_stop_proof"]["directory"]) / "worker-a.stop.json").exists())

    def test_unowned_worker_cannot_produce_proof(self):
        with tempfile.TemporaryDirectory(prefix="owned-proof-", dir="/tmp") as directory:
            project = self.project(directory)
            with self.assertRaisesRegex(RuntimeError, "not owned"):
                project.stop_worker("worker-a")
            self.assertEqual(list(pathlib.Path(project.config["owned_stop_proof"]["directory"]).iterdir()), [])

    def test_complete_proofs_without_terminal_marker_do_not_authorize_restart(self):
        for backend in ("local", "s3"):
            environment = {"S3_ENDPOINT": "http://127.0.0.1:9000", "S3_BUCKET": "synthetic", "S3_REGION": "us-east-1"}
            with self.subTest(backend=backend), tempfile.TemporaryDirectory(prefix="owned-proof-", dir="/tmp") as directory, patch.dict(dev.os.environ, environment):
                project = self.project(directory, backend)
                configuration = project.config["owned_stop_proof"]
                for worker in project.launch["workers"]:
                    identity = {"version": 1, "generation": configuration["generation"], "worker_id": worker,
                                "nonce": configuration["workers"][worker], "pid": 10001, "started": "incarnation",
                                "waited": False, "exit_code": 0}
                    for kind, record in (("identity", identity), ("stop", {**identity, "waited": True})):
                        dev.atomic_private_json(pathlib.Path(configuration["directory"]) / f"{worker}.{kind}.json", dev.signed_owned_identity(configuration, record))
                dev.verify_owned_stop_proofs(project.directory, configuration, project.launch["workers"])
                with self.assertRaisesRegex(RuntimeError, "terminal proof missing"):
                    dev.Project(project.arguments).prepare()
                stop_path = pathlib.Path(configuration["directory"]) / "worker-b.stop.json"
                original = stop_path.read_bytes()
                for altered in (original.replace(b'"mac":', b'"bad":'), b'{}'):
                    stop_path.write_bytes(altered)
                    with self.assertRaises(RuntimeError):
                        dev.verify_owned_stop_proofs(project.directory, configuration, project.launch["workers"])
                for changes in ({"exit_code": 1}, {"pid": 99999}, {"waited": False}):
                    record = json.loads(json.loads(original)["payload"])
                    record.update(changes)
                    dev.atomic_private_json(stop_path, dev.signed_owned_identity(configuration, record))
                    with self.assertRaisesRegex(RuntimeError, "incarnation or joined exit"):
                        dev.verify_owned_stop_proofs(project.directory, configuration, project.launch["workers"])
                stop_path.write_bytes(original)
                extra = pathlib.Path(configuration["directory"]) / "untracked.stop.json"
                extra.write_bytes(original)
                with self.assertRaisesRegex(RuntimeError, "unexpected"):
                    dev.Project(project.arguments).prepare()
                extra.unlink()
                (pathlib.Path(configuration["directory"]) / "worker-a.identity.json").unlink()
                with self.assertRaisesRegex(RuntimeError, "incomplete"):
                    dev.Project(project.arguments).prepare()

    def test_clean_terminal_is_single_use_and_rotates_worker_proof_files(self):
        for backend in ("local", "s3"):
            environment = {"S3_ENDPOINT": "http://127.0.0.1:9000", "S3_BUCKET": "synthetic", "S3_REGION": "us-east-1"}
            with self.subTest(backend=backend), tempfile.TemporaryDirectory(prefix="owned-proof-", dir="/tmp") as directory, patch.dict(dev.os.environ, environment):
                project = self.project(directory, backend)
                configuration = project.config["owned_stop_proof"]
                old_generation = configuration["generation"]
                for worker in project.launch["workers"]:
                    identity = {"version": 1, "generation": old_generation, "worker_id": worker,
                                "nonce": configuration["workers"][worker], "pid": 10001, "started": "incarnation",
                                "waited": False, "exit_code": 0}
                    for kind, record in (("identity", identity), ("stop", {**identity, "waited": True})):
                        dev.atomic_private_json(project.directory / "owned-proof" / f"{worker}.{kind}.json", dev.signed_owned_identity(configuration, record))
                authority = project.directory / "authority/authority.lock"
                authority.write_bytes(b"clean\n")
                dev.private_json(project.directory / "platform/state.json", {"payload": {"owned_run_generation": old_generation, "operations": {}}})
                dev.private_json(project.directory / "platform/state.json.floor", {"version": 1})
                self.write_clean_stop(project, configuration)
                dev.atomic_private_json(project.directory / "launcher-terminal.json", dev.signed_owned_identity(configuration, dev.terminal_record(project.directory, configuration)))
                restarted = dev.Project(project.arguments)
                restarted.prepare()
                self.assertEqual(restarted.config["owned_stop_proof"]["generation"], old_generation)
                self.assertFalse((project.directory / "launcher-terminal.json").exists())
                self.assertEqual(list((project.directory / "owned-proof").iterdir()), [])
                archives = list((project.directory / "owned-proof-history").glob(old_generation + "-*"))
                self.assertEqual(len(archives), 1)
                self.assertEqual(len(list(archives[0].iterdir())), 4)
                with self.assertRaisesRegex(RuntimeError, "proof set incomplete"):
                    dev.Project(project.arguments).prepare()

    def write_clean_stop(self, project, configuration):
        record = {"version": 1, "generation": configuration["generation"], "outcome": "clean",
                  "reason": "terminal_proof_written", "deadline_ms": 1, "stages": [],
                  "unresolved_operations": False}
        dev.atomic_private_json(project.directory / "launcher-stop.json", dev.signed_owned_identity(configuration, record))

    def clean_run_directory(self, directory):
        project = self.project(directory)
        configuration = project.config["owned_stop_proof"]
        for worker in project.launch["workers"]:
            identity = {"version": 1, "generation": configuration["generation"], "worker_id": worker,
                        "nonce": configuration["workers"][worker], "pid": 10001, "started": "incarnation",
                        "waited": False, "exit_code": 0}
            for kind, record in (("identity", identity), ("stop", {**identity, "waited": True})):
                dev.atomic_private_json(project.directory / "owned-proof" / f"{worker}.{kind}.json", dev.signed_owned_identity(configuration, record))
        dev.private_json(project.directory / "platform/state.json", {"payload": {"owned_run_generation": configuration["generation"], "operations": {}}})
        dev.private_json(project.directory / "platform/state.json.floor", {"version": 1})
        (project.directory / "authority/authority.lock").write_bytes(b"clean\n")
        self.write_clean_stop(project, configuration)
        dev.atomic_private_json(project.directory / "launcher-terminal.json", dev.signed_owned_identity(configuration, dev.terminal_record(project.directory, configuration)))
        return project

    def launcher_arguments(self, directory):
        return ["dev.py", "--run-dir", str(directory), "--no-build", "--listen", "127.0.0.1:19443"]

    def test_startup_failure_restores_clean_stop_and_terminal(self):
        with tempfile.TemporaryDirectory(prefix="restart-failure-", dir="/tmp") as directory:
            fixture = self.clean_run_directory(directory)
            root = pathlib.Path(directory).resolve()
            stop_path, terminal_path = root / "launcher-stop.json", root / "launcher-terminal.json"
            stop_bytes, terminal_bytes = stop_path.read_bytes(), terminal_path.read_bytes()

            def failing_run(project):
                project.acquire_run_lock()
                project.prepare()
                # The platform commits by rename; simulate a startup write without
                # touching the snapshot inode.
                replacement = project.directory / "platform/state.json.attempt"
                replacement.write_bytes(b'{"payload":{"owned_run_generation":"attempt","operations":{}}}')
                os.replace(replacement, project.directory / "platform/state.json")
                worker = next(iter(project.launch["workers"]))
                (project.directory / "owned-proof" / (worker + ".identity.json")).write_bytes(b"attempt incarnation")
                raise RuntimeError("request rejected: operation_incomplete")

            with patch.object(dev.Project, "run", failing_run), patch.object(sys, "argv", self.launcher_arguments(root)):
                self.assertEqual(dev.main(), 1)
            self.assertEqual(stop_path.read_bytes(), stop_bytes)
            self.assertEqual(terminal_path.read_bytes(), terminal_bytes)
            configuration = fixture.config["owned_stop_proof"]
            dev.verify_owned_stop_proofs(root, configuration, fixture.launch["workers"])
            dev.verify_clean_terminal(root, configuration)
            self.assertFalse((root / "launcher-restart-staging").exists())
            envelope = json.loads((root / "launcher-restart-failure.json").read_text())
            key = pathlib.Path(configuration["key_file"]).read_bytes()
            self.assertEqual(envelope["mac"], hmac.new(key, envelope["payload"].encode(), hashlib.sha256).hexdigest())
            record = json.loads(envelope["payload"])
            self.assertEqual(record["kind"], "launcher_restart_failure")
            self.assertEqual(record["phase"], "startup")
            self.assertEqual(record["run_dir"], str(root))
            self.assertIn("operation_incomplete", record["error"])
            self.assertTrue(record["teardown_completed"])
            self.assertTrue(record["clean_evidence"]["restored"])
            self.assertFalse(record["requires_operator"])
            self.assertTrue(record["teardown_completed"])
            self.assertTrue(record["all_processes_stopped"])
            self.assertEqual(record["not_rolled_back"]["canonical_clean_evidence"], "restored")
            self.assertEqual(record["not_rolled_back"]["authority_lock"], "matches_snapshot")
            self.assertIn(record["not_rolled_back"]["platform_state"], ("matches_snapshot", "absent_at_snapshot"))
            self.assertIsNotNone(record["clean_evidence"]["authority_lock_sha256"])
            self.assertEqual(record["clean_evidence"]["terminal_sha256"], hashlib.sha256(terminal_bytes).hexdigest())
            self.assertEqual(record["clean_evidence"]["stop_sha256"], hashlib.sha256(stop_bytes).hexdigest())
            self.assertEqual(json.loads(json.loads(stop_bytes)["payload"])["outcome"], "clean")
            self.assertEqual(len(list((root / "owned-proof").iterdir())), 4)
            failed = list((root / "owned-proof-history").glob(configuration["generation"] + "-failed-*"))
            self.assertEqual(len(failed), 1)
            self.assertEqual([entry.name for entry in failed[0].iterdir()], ["worker-a.identity.json"])
            # A restored startup failure keeps the run dir reusable.
            dev.Project(fixture.arguments).prepare()

    def test_prepare_refusal_does_not_rewrite_previous_clean_evidence(self):
        with tempfile.TemporaryDirectory(prefix="restart-refusal-", dir="/tmp") as directory:
            self.clean_run_directory(directory)
            root = pathlib.Path(directory).resolve()
            stop_path, terminal_path = root / "launcher-stop.json", root / "launcher-terminal.json"
            stop_bytes, terminal_bytes = stop_path.read_bytes(), terminal_path.read_bytes()
            with (root / "platform.json").open("a") as output:
                output.write(" ")

            def failing_run(project):
                project.acquire_run_lock()
                project.prepare()

            with patch.object(dev.Project, "run", failing_run), patch.object(sys, "argv", self.launcher_arguments(root)):
                self.assertEqual(dev.main(), 1)
            self.assertEqual(stop_path.read_bytes(), stop_bytes)
            self.assertEqual(terminal_path.read_bytes(), terminal_bytes)
            record = json.loads(json.loads((root / "launcher-restart-failure.json").read_text())["payload"])
            self.assertFalse(record["clean_evidence"]["consumed"])
            self.assertTrue(record["clean_evidence"]["preserved"])
            self.assertFalse(record["clean_evidence"]["existing_clean_evidence_verified"])

    def test_failure_after_readiness_keeps_unclean_semantics(self):
        with tempfile.TemporaryDirectory(prefix="restart-running-", dir="/tmp") as directory:
            self.clean_run_directory(directory)
            root = pathlib.Path(directory).resolve()

            def failing_run(project):
                project.acquire_run_lock()
                project.prepare()
                project.phase = "running"
                project.discard_clean_evidence()
                raise RuntimeError("a control-plane service stopped; inspect private logs")

            with patch.object(dev.Project, "run", failing_run), patch.object(sys, "argv", self.launcher_arguments(root)):
                self.assertEqual(dev.main(), 1)
            record = json.loads(json.loads((root / "launcher-stop.json").read_text())["payload"])
            self.assertEqual(record["outcome"], "unclean")
            self.assertEqual(record["reason"], "launcher_failed_before_cleanup")
            self.assertFalse((root / "launcher-terminal.json").exists())
            self.assertFalse((root / "launcher-restart-failure.json").exists())

    def test_cleanup_failure_still_restores_previous_clean_evidence(self):
        # A platform that never became ready cannot write a clean stop proof; the
        # teardown failure must not prevent restoring the last clean snapshot.
        with tempfile.TemporaryDirectory(prefix="restart-teardown-", dir="/tmp") as directory:
            fixture = self.clean_run_directory(directory)
            root = pathlib.Path(directory).resolve()
            stop_path, terminal_path = root / "launcher-stop.json", root / "launcher-terminal.json"
            stop_bytes, terminal_bytes = stop_path.read_bytes(), terminal_path.read_bytes()
            observed = {}

            def failing_run(project):
                project.acquire_run_lock()
                project.prepare()
                process = Mock(stdout=None)
                exited = {"value": False}
                process.poll.side_effect = lambda: 1 if exited["value"] else None
                def wait(timeout=None):
                    exited["value"] = True
                    return 1
                process.wait.side_effect = wait
                project.processes["platform"] = process
                observed["process"] = process
                raise RuntimeError("platform did not become ready; inspect private logs")

            with patch.object(dev.Project, "run", failing_run), patch.object(sys, "argv", self.launcher_arguments(root)):
                self.assertEqual(dev.main(), 1)
            observed["process"].terminate.assert_called_once()
            observed["process"].wait.assert_called()
            self.assertEqual(stop_path.read_bytes(), stop_bytes)
            self.assertEqual(terminal_path.read_bytes(), terminal_bytes)
            record = json.loads(json.loads((root / "launcher-restart-failure.json").read_text())["payload"])
            self.assertTrue(record["teardown_completed"])
            self.assertTrue(record["all_processes_stopped"])
            self.assertFalse(record["teardown_clean"])
            self.assertIn("platform exited without a clean proof", record["teardown_error"])
            self.assertTrue(record["clean_evidence"]["restored"])
            self.assertFalse(record["requires_operator"])
            dev.verify_clean_terminal(root, fixture.config["owned_stop_proof"])
            dev.Project(fixture.arguments).prepare()

    def test_restore_verification_failure_quarantines_terminal_and_blocks_reuse(self):
        with tempfile.TemporaryDirectory(prefix="restart-quarantine-", dir="/tmp") as directory:
            fixture = self.clean_run_directory(directory)
            root = pathlib.Path(directory).resolve()
            stop_path, terminal_path = root / "launcher-stop.json", root / "launcher-terminal.json"
            stop_bytes, terminal_bytes = stop_path.read_bytes(), terminal_path.read_bytes()

            def failing_run(project):
                project.acquire_run_lock()
                project.prepare()
                # launch.json is part of the terminal hash and is not restored.
                with (project.directory / "launch.json").open("a") as output:
                    output.write(" ")
                raise RuntimeError("request rejected: operation_incomplete")

            with patch.object(dev.Project, "run", failing_run), patch.object(sys, "argv", self.launcher_arguments(root)):
                self.assertEqual(dev.main(), 1)
            self.assertEqual(stop_path.read_bytes(), stop_bytes)
            self.assertFalse(terminal_path.exists())
            quarantined = list((root / "launcher-clean-evidence-quarantine").iterdir())
            self.assertEqual(len(quarantined), 1)
            self.assertEqual((quarantined[0] / "launcher-terminal.json").read_bytes(), terminal_bytes)
            record = json.loads(json.loads((root / "launcher-restart-failure.json").read_text())["payload"])
            self.assertTrue(record["requires_operator"])
            self.assertFalse(record["recovery"]["restored"])
            self.assertEqual(record["teardown"]["outcome"], "unclean")
            self.assertEqual(record["clean_evidence_quarantine"]["directory"], str(quarantined[0]))
            self.assertEqual(record["not_rolled_back"]["canonical_clean_evidence"], "quarantined")
            self.assertEqual(record["not_rolled_back"]["authority_lock"], "matches_snapshot")
            self.assertEqual(record["not_rolled_back"]["authority_seal"], "absent_at_snapshot")
            self.assertEqual(record["not_rolled_back"]["platform_state"], "matches_snapshot")
            self.assertTrue(record["teardown_completed"])
            self.assertTrue(record["requires_operator"])
            with self.assertRaisesRegex(RuntimeError, "operator"):
                dev.Project(fixture.arguments).prepare()

    def test_reconciling_failure_preserves_clean_evidence_for_operator(self):
        with tempfile.TemporaryDirectory(prefix="restart-reconciling-", dir="/tmp") as directory:
            fixture = self.clean_run_directory(directory)
            root = pathlib.Path(directory).resolve()
            stop_path, terminal_path = root / "launcher-stop.json", root / "launcher-terminal.json"
            stop_bytes, terminal_bytes = stop_path.read_bytes(), terminal_path.read_bytes()

            def failing_run(project):
                project.acquire_run_lock()
                project.prepare()
                project.phase = "reconciling"
                replacement = project.directory / "platform/state.json.attempt"
                replacement.write_bytes(b'{"payload":{"owned_run_generation":"attempt","operations":{}}}')
                os.replace(replacement, project.directory / "platform/state.json")
                worker = next(iter(project.launch["workers"]))
                (project.directory / "owned-proof" / (worker + ".identity.json")).write_bytes(b"attempt incarnation")
                raise RuntimeError("request rejected: operation_incomplete")

            with patch.object(dev.Project, "run", failing_run), patch.object(sys, "argv", self.launcher_arguments(root)):
                self.assertEqual(dev.main(), 1)
            self.assertEqual(stop_path.read_bytes(), stop_bytes)
            self.assertFalse(terminal_path.exists())
            preserved = list((root / "launcher-clean-evidence-preserved").iterdir())
            self.assertEqual(len(preserved), 1)
            self.assertEqual((preserved[0] / "launcher-terminal.json").read_bytes(), terminal_bytes)
            self.assertTrue((preserved[0] / "owned-proof" / "worker-a.identity.json").is_file())
            self.assertTrue((preserved[0] / "platform/state.json").is_file())
            self.assertTrue((preserved[0] / "authority/authority.lock").is_file())
            record = json.loads(json.loads((root / "launcher-restart-failure.json").read_text())["payload"])
            self.assertTrue(record["requires_operator"])
            self.assertFalse(record["clean_evidence"]["restored"])
            self.assertTrue(record["clean_evidence"]["preserved"])
            self.assertEqual(record["clean_evidence"]["preserved_at"], str(preserved[0]))
            self.assertEqual(record["not_rolled_back"]["canonical_clean_evidence"], "preserved")
            self.assertEqual(record["not_rolled_back"]["authority_backend_leases"], "not_rolled_back")
            self.assertEqual(record["not_rolled_back"]["platform_operations_sidecar"], "not_rolled_back")
            self.assertEqual(record["not_rolled_back"]["platform_state"], "changed")
            self.assertEqual(record["not_rolled_back"]["authority_lock"], "matches_snapshot")
            self.assertEqual(record["not_rolled_back"]["authority_seal"], "absent_at_snapshot")
            self.assertIn("authority_lock", record["not_rolled_back"])
            with self.assertRaisesRegex(RuntimeError, "operator"):
                dev.Project(fixture.arguments).prepare()

    def test_failure_record_write_error_does_not_block_cleanup(self):
        with tempfile.TemporaryDirectory(prefix="restart-record-error-", dir="/tmp") as directory:
            fixture = self.clean_run_directory(directory)
            root = pathlib.Path(directory).resolve()
            stop_path, terminal_path = root / "launcher-stop.json", root / "launcher-terminal.json"
            stop_bytes, terminal_bytes = stop_path.read_bytes(), terminal_path.read_bytes()
            observed = {}

            def failing_run(project):
                project.acquire_run_lock()
                project.prepare()
                process = Mock(stdout=None)
                exited = {"value": False}
                process.poll.side_effect = lambda: 0 if exited["value"] else None
                def wait(timeout=None):
                    exited["value"] = True
                    return 0
                process.wait.side_effect = wait
                project.processes["platform"] = process
                observed["process"] = process
                raise RuntimeError("request rejected: operation_incomplete")

            stderr = io.StringIO()
            with patch.object(dev.Project, "persist_restart_failure", side_effect=OSError("disk full")), \
                    patch.object(dev.Project, "run", failing_run), \
                    patch.object(sys, "argv", self.launcher_arguments(root)), \
                    contextlib.redirect_stderr(stderr):
                self.assertEqual(dev.main(), 1)
            observed["process"].terminate.assert_called_once()
            observed["process"].wait.assert_called()
            # Recovery still uses the in-memory evidence even though the audit
            # record could not be written.
            self.assertEqual(stop_path.read_bytes(), stop_bytes)
            self.assertEqual(terminal_path.read_bytes(), terminal_bytes)
            self.assertFalse((root / "launcher-restart-failure.json").exists())
            unavailable = json.loads((root / "launcher-restart-failure-unavailable.json").read_text())
            self.assertEqual(unavailable["kind"], "launcher_restart_failure_unavailable")
            self.assertIn("operation_incomplete", unavailable["error"])
            self.assertIn("disk full", unavailable["record_error"])
            self.assertTrue(unavailable["recovery"]["restored"])
            self.assertIn("request rejected: operation_incomplete", stderr.getvalue())
            self.assertIn("restart failure record unavailable", stderr.getvalue())
            self.assertIn("disk full", stderr.getvalue())
            with self.assertRaisesRegex(RuntimeError, "record unavailable"):
                dev.Project(fixture.arguments).prepare()

    def test_prepare_requires_clean_stop_outcome(self):
        for outcome in ("unclean", "missing"):
            with self.subTest(outcome=outcome), tempfile.TemporaryDirectory(prefix="restart-stop-gate-", dir="/tmp") as directory:
                project = self.clean_run_directory(directory)
                root = pathlib.Path(directory).resolve()
                configuration = project.config["owned_stop_proof"]
                expected = "operator"
                if outcome == "missing":
                    (root / "launcher-stop.json").unlink()
                    expected = "legacy clean proof without launcher-stop.json"
                else:
                    dev.atomic_private_json(root / "launcher-stop.json", dev.signed_owned_identity(configuration, {
                        "version": 1, "generation": configuration["generation"], "outcome": "unclean",
                        "reason": "synthetic", "deadline_ms": 1, "stages": [],
                        "unresolved_operations": "not_verified"}))
                with self.assertRaisesRegex(RuntimeError, expected):
                    dev.Project(project.arguments).prepare()
                self.assertTrue((root / "launcher-terminal.json").is_file())
                self.assertTrue((root / "owned-proof").is_dir())

    def live_port(self):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            return probe.getsockname()[1]

    def live_environment(self):
        environment = dev.safe_environment()
        environment["PERI_RUST_BINARY"] = os.environ["PERI_RUST_BINARY"]
        environment["PERI_PLATFORM_BINARY"] = os.environ["PERI_PLATFORM_BINARY"]
        return environment

    def launch_live(self, root, port):
        process = subprocess.Popen(
            [sys.executable, str(source), "--run-dir", str(root),
             "--listen", f"127.0.0.1:{port}", "--tls", "--no-build"],
            env=self.live_environment(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        lines = queue.Queue()
        threading.Thread(target=lambda: [lines.put(line) for line in process.stdout], daemon=True).start()
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            try:
                line = lines.get(timeout=0.2)
            except queue.Empty:
                if process.poll() is not None:
                    break
                continue
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("ready") is True:
                return process
        error_text = ""
        if process.poll() is not None and process.stderr is not None:
            error_text = process.stderr.read()
        self.stop_live(process, expect=None, force=True)
        raise RuntimeError("live launcher did not become ready: " + error_text)

    def stop_live(self, process, expect=0, force=False):
        if process.poll() is None:
            process.kill() if force else process.send_signal(signal.SIGTERM)
        try:
            code = process.wait(timeout=120)
        except subprocess.TimeoutExpired:
            process.kill()
            code = process.wait(timeout=10)
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                stream.close()
        if expect is not None:
            self.assertEqual(code, expect)
        return code

    @unittest.skipUnless(
        os.environ.get("PERI_RESTART_LIVE") == "1"
        and pathlib.Path(os.environ.get("PERI_RUST_BINARY", "")).is_file()
        and pathlib.Path(os.environ.get("PERI_PLATFORM_BINARY", "")).is_file(),
        "set PERI_RESTART_LIVE=1 and PERI_RUST_BINARY/PERI_PLATFORM_BINARY to run the live restart")
    def test_live_port_conflict_restores_clean_evidence_and_data(self):
        with tempfile.TemporaryDirectory(prefix="restart-live-", dir="/tmp") as directory:
            root = pathlib.Path(directory).resolve()
            port = self.live_port()
            first = self.launch_live(root, port)
            try:
                client = LiveSQLClient(root)
                client.statement("CREATE TABLE restart_live (id INTEGER PRIMARY KEY, note TEXT)", "execute")
                client.statement("INSERT INTO restart_live (id, note) VALUES (1, 'keep-me')", "execute")
                self.assertEqual(client.statement("SELECT id, note FROM restart_live ORDER BY id", "query"), [[1, "keep-me"]])
            finally:
                self.stop_live(first)
            stop_bytes = (root / "launcher-stop.json").read_bytes()
            terminal_bytes = (root / "launcher-terminal.json").read_bytes()
            holder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            holder.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            holder.bind(("127.0.0.1", port))
            holder.listen(8)
            try:
                failed = subprocess.Popen(
                    [sys.executable, str(source), "--run-dir", str(root),
                     "--listen", f"127.0.0.1:{port}", "--tls", "--no-build"],
                    env=self.live_environment(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                self.assertEqual(failed.wait(timeout=180), 1)
            finally:
                holder.close()
            self.assertEqual((root / "launcher-stop.json").read_bytes(), stop_bytes)
            self.assertEqual((root / "launcher-terminal.json").read_bytes(), terminal_bytes)
            configuration = json.loads((root / "platform.json").read_text())["owned_stop_proof"]
            launch = json.loads((root / "launch.json").read_text())
            dev.verify_owned_stop_proofs(root, configuration, launch["workers"])
            dev.verify_clean_terminal(root, configuration)
            record = json.loads(json.loads((root / "launcher-restart-failure.json").read_text())["payload"])
            self.assertEqual(record["phase"], "startup")
            self.assertFalse(record["requires_operator"])
            self.assertTrue(record["clean_evidence"]["restored"])
            second = self.launch_live(root, port)
            try:
                client = LiveSQLClient(root)
                self.assertEqual(client.statement("SELECT id, note FROM restart_live ORDER BY id", "query"), [[1, "keep-me"]])
            finally:
                self.stop_live(second)
            dev.verify_clean_terminal(root, configuration)

    def test_unconfirmed_process_stops_preserve_evidence_for_operator(self):
        with tempfile.TemporaryDirectory(prefix="restart-unconfirmed-", dir="/tmp") as directory:
            self.clean_run_directory(directory)
            root = pathlib.Path(directory).resolve()
            stop_path, terminal_path = root / "launcher-stop.json", root / "launcher-terminal.json"
            stop_bytes, terminal_bytes = stop_path.read_bytes(), terminal_path.read_bytes()
            observed = {}

            class Unconfirmed:
                stdout = None

                def __init__(self, process):
                    self.process = process

                def poll(self):
                    return None

                def wait(self, timeout=None):
                    raise subprocess.TimeoutExpired("platform", timeout)

                def terminate(self):
                    pass

                def kill(self):
                    pass

            def failing_run(project):
                project.acquire_run_lock()
                project.prepare()
                child = subprocess.Popen(
                    [sys.executable, "-c", "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(120)"])
                observed["child"] = child
                project.processes["platform"] = Unconfirmed(child)
                raise RuntimeError("platform did not become ready; inspect private logs")

            try:
                with patch.object(dev, "SHUTDOWN_STAGE_CAPS", {"platform": 1, "workers": 1, "authority": 1}), \
                        patch.object(dev, "SHUTDOWN_FINALIZE_SECONDS", 0.2), \
                        patch.object(dev.Project, "run", failing_run), \
                        patch.object(sys, "argv", self.launcher_arguments(root)):
                    self.assertEqual(dev.main(), 1)
                self.assertEqual(stop_path.read_bytes(), stop_bytes)
                self.assertFalse(terminal_path.exists())
                preserved = list((root / "launcher-clean-evidence-preserved").iterdir())
                self.assertEqual(len(preserved), 1)
                self.assertEqual((preserved[0] / "launcher-terminal.json").read_bytes(), terminal_bytes)
                record = json.loads(json.loads((root / "launcher-restart-failure.json").read_text())["payload"])
                self.assertTrue(record["requires_operator"])
                self.assertFalse(record["teardown_completed"])
                self.assertFalse(record["all_processes_stopped"])
                self.assertEqual(record["clean_evidence"]["preserved_at"], str(preserved[0]))
                self.assertIn("processes not confirmed stopped", record["clean_evidence"]["preservation_reason"])
                self.assertEqual(record["not_rolled_back"]["canonical_clean_evidence"], "preserved")
                self.assertEqual(record["not_rolled_back"]["authority_lock"], "matches_snapshot")
                self.assertEqual(record["not_rolled_back"]["platform_state"], "matches_snapshot")
                with self.assertRaisesRegex(RuntimeError, "operator"):
                    dev.Project(argparse.Namespace(run_dir=str(root), backend="local", listen="127.0.0.1:19443",
                                                   tls=False, no_build=True, writer_admission=False)).prepare()
            finally:
                child = observed.get("child")
                if child is not None and child.poll() is None:
                    child.kill()
                    child.wait(timeout=10)

    def test_legacy_failure_records_require_explicit_operator_diagnosis(self):
        cases = (
            ("lacks requires_operator", {"teardown_completed": True}),
            ("lacks teardown_completed", {"requires_operator": False}),
            ("teardown not confirmed complete", {"requires_operator": False, "teardown_completed": False}),
        )
        for expected, extra in cases:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory(prefix="restart-legacy-", dir="/tmp") as directory:
                project = self.clean_run_directory(directory)
                root = pathlib.Path(directory).resolve()
                configuration = project.config["owned_stop_proof"]
                record = {"version": 1, "kind": "launcher_restart_failure", "run_dir": str(root),
                          "phase": "startup", "error": "legacy record", "failed_at": "2026-01-01T00:00:00+00:00"}
                record.update(extra)
                dev.atomic_private_json(root / "launcher-restart-failure.json",
                                        dev.signed_owned_identity(configuration, record))
                with self.assertRaisesRegex(RuntimeError, expected):
                    dev.Project(project.arguments).prepare()
                self.assertTrue((root / "launcher-terminal.json").is_file())
                self.assertTrue((root / "launcher-stop.json").is_file())

    def test_crash_before_recovery_never_restores_staged_evidence(self):
        with tempfile.TemporaryDirectory(prefix="restart-crash-", dir="/tmp") as directory:
            project = self.clean_run_directory(directory)
            root = pathlib.Path(directory).resolve()
            terminal_bytes = (root / "launcher-terminal.json").read_bytes()
            dev.Project(project.arguments).prepare()
            self.assertFalse((root / "launcher-terminal.json").exists())
            staging = list((root / "launcher-restart-staging").iterdir())
            self.assertEqual(len(staging), 1)
            self.assertEqual((staging[0] / "launcher-terminal.json").read_bytes(), terminal_bytes)
            # No same-process recovery ran (hard kill/crash). The next launcher
            # must refuse rather than promote the staging copy automatically.
            with self.assertRaises(RuntimeError):
                dev.Project(project.arguments).prepare()
            self.assertFalse((root / "launcher-terminal.json").exists())

    def test_local_object_lookup_validates_json_envelope(self):
        with tempfile.TemporaryDirectory(prefix="seal-envelope-", dir="/tmp") as directory:
            backend = pathlib.Path(directory) / "backend"
            backend.mkdir(mode=0o700)
            key = dev.LOCAL_SEAL_KEY
            (backend / "malformed.obj").write_text("prefix " + key)
            with patch.dict(sys.modules, {"blake3": None}):
                path, reliable = dev.local_object_path(backend, key)
            self.assertIsNone(path)
            self.assertFalse(reliable)
            (backend / "malformed.obj").unlink()
            (backend / "other.obj").write_text(json.dumps({"key": "other", "bytes": "e30="}))
            with patch.dict(sys.modules, {"blake3": None}):
                path, reliable = dev.local_object_path(backend, key)
            self.assertIsNone(path)
            self.assertTrue(reliable)
            (backend / "seal.obj").write_text(json.dumps({"key": key, "bytes": "e30="}))
            with patch.dict(sys.modules, {"blake3": None}):
                path, reliable = dev.local_object_path(backend, key)
            self.assertEqual(path, backend / "seal.obj")
            self.assertTrue(reliable)

    def test_clean_terminal_rejects_dirty_authority_and_modified_configuration(self):
        with tempfile.TemporaryDirectory(prefix="owned-proof-", dir="/tmp") as directory:
            project = self.project(directory)
            configuration = project.config["owned_stop_proof"]
            for worker in project.launch["workers"]:
                identity = {"version": 1, "generation": configuration["generation"], "worker_id": worker,
                            "nonce": configuration["workers"][worker], "pid": 10001, "started": "incarnation",
                            "waited": False, "exit_code": 0}
                for kind, record in (("identity", identity), ("stop", {**identity, "waited": True})):
                    dev.atomic_private_json(project.directory / "owned-proof" / f"{worker}.{kind}.json", dev.signed_owned_identity(configuration, record))
            dev.private_json(project.directory / "platform/state.json", {"payload": {"owned_run_generation": configuration["generation"], "operations": {}}})
            dev.private_json(project.directory / "platform/state.json.floor", {"version": 1})
            dev.atomic_private_json(project.directory / "launcher-terminal.json", dev.signed_owned_identity(configuration, dev.terminal_record(project.directory, configuration)))
            authority = project.directory / "authority/authority.lock"
            authority.write_bytes(b"dirty\n")
            with self.assertRaisesRegex(RuntimeError, "authority clean"):
                dev.Project(project.arguments).prepare()
            authority.write_bytes(b"clean\n")
            with (project.directory / "platform.json").open("a") as output:
                output.write(" ")
            with self.assertRaisesRegex(RuntimeError, "terminal proof invalid"):
                dev.Project(project.arguments).prepare()


if __name__ == "__main__":
    unittest.main()
