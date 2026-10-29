import argparse
import base64
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
                self.assertEqual(dev.safe_environment(capacity_trace=True), {"PATH": "/synthetic/bin", "PERI_CAPACITY_TRACE": expected, "PERI_NATIVE_GATE_TRACE": "0"})
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

    def test_uncertain_shutdown_records_the_observed_join_without_a_clean_proof(self):
        for failure in ("rpc", "exit"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory(prefix="owned-proof-", dir="/tmp") as directory:
                project = self.project(directory)
                configuration = project.config["owned_stop_proof"]
                process = Mock()
                process.poll.return_value = None
                process.wait.return_value = 1 if failure == "exit" else 0
                project.processes["worker-a"] = process
                project.owned_identities["worker-a"] = {
                    "version": 1, "generation": configuration["generation"], "worker_id": "worker-a",
                    "nonce": configuration["workers"]["worker-a"], "pid": 10001, "started": "incarnation",
                    "waited": False, "exit_code": 0,
                }
                with patch.object(dev, "request", side_effect=RuntimeError("uncertain") if failure == "rpc" else None):
                    with self.assertRaises(RuntimeError):
                        project.cleanup()
                stop_path = pathlib.Path(configuration["directory"]) / "worker-a.stop.json"
                if failure == "exit":
                    joined = json.loads(json.loads(stop_path.read_bytes())["payload"])
                    self.assertEqual((joined["waited"], joined["exit_code"]), (True, 1))
                else:
                    self.assertFalse(stop_path.exists())

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
                dev.verify_owned_stop_proofs(project.directory, project.config["owned_stop_proof"], project.launch["workers"])

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
                    dev.verify_owned_stop_proofs(project.directory, configuration, project.launch["workers"])
                extra.unlink()
                (pathlib.Path(configuration["directory"]) / "worker-a.identity.json").unlink()
                with self.assertRaisesRegex(RuntimeError, "incomplete"):
                    dev.verify_owned_stop_proofs(project.directory, configuration, project.launch["workers"])

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
                self.assertTrue((project.directory / "launcher-terminal.json").exists())
                self.assertEqual(len(list((project.directory / "owned-proof").iterdir())), 4)
                archives = list((project.directory / "owned-proof-history").glob(old_generation + "-*"))
                self.assertEqual(len(archives), 1)
                self.assertEqual(len(list(archives[0].iterdir())), 4)
                dev.Project(project.arguments).prepare()
                restarted.write_startup_intent(configuration, services_started=["authority"])
                with self.assertRaisesRegex(RuntimeError, "interrupted startup"):
                    dev.Project(project.arguments).prepare()

    def test_missing_terminal_after_verified_clean_shutdown_is_rebuilt(self):
        with tempfile.TemporaryDirectory(prefix="owned-proof-", dir="/tmp") as directory:
            project = self.clean_run_directory(directory)
            configuration = project.config["owned_stop_proof"]
            terminal = project.directory / "launcher-terminal.json"
            attempt = {"version": 1, "kind": "launcher_startup_attempt",
                       "generation": configuration["generation"], "ready": True,
                       "services_started": ["authority", "platform", "worker-a", "worker-b"],
                       "pids": {}, "commands": {},
                       "terminal_sha256": dev.file_sha256(terminal), "state_hashes": "snapshot"}
            dev.atomic_private_json(project.directory / "launcher-startup-attempt.json",
                                    dev.signed_owned_identity(configuration, attempt))
            terminal.unlink()
            restarted = dev.Project(project.arguments)
            restarted.prepare()
            self.assertTrue(terminal.exists())
            dev.verify_clean_terminal(project.directory, restarted.config["owned_stop_proof"])
            self.assertEqual(len(list(project.directory.glob("launcher-startup-attempt.completed-*"))), 1)
            record = json.loads(json.loads((project.directory / "launcher-stop.json").read_text())["payload"])
            self.assertEqual(record["outcome"], "clean")

    def test_missing_terminal_with_in_progress_joined_stages_is_rebuilt(self):
        with tempfile.TemporaryDirectory(prefix="owned-proof-", dir="/tmp") as directory:
            project = self.clean_run_directory(directory)
            configuration = project.config["owned_stop_proof"]
            record = {"version": 1, "generation": configuration["generation"], "outcome": "in_progress",
                      "reason": "shutdown_started", "deadline_ms": 1,
                      "stages": [{"name": name, "ack": True, "exit_code": 0, "signal": None}
                                 for name in ("platform", "worker-a", "worker-b", "authority")],
                      "unresolved_operations": "not_verified"}
            dev.atomic_private_json(project.directory / "launcher-stop.json",
                                    dev.signed_owned_identity(configuration, record))
            (project.directory / "launcher-terminal.json").unlink()
            restarted = dev.Project(project.arguments)
            restarted.prepare()
            self.assertTrue((project.directory / "launcher-terminal.json").exists())
            saved = json.loads(json.loads((project.directory / "launcher-stop.json").read_text())["payload"])
            self.assertEqual(saved["outcome"], "clean")
            self.assertEqual(saved["reason"], "terminal_proof_recovered")

    def test_missing_terminal_without_clean_shutdown_evidence_is_refused(self):
        cases = (("dirty marker", b"dirty\n", "clean"),
                 ("unclean stop", b"clean\n", "unclean"),
                 ("partial stop", b"clean\n", "in_progress"))
        for label, marker, outcome in cases:
            with self.subTest(label), tempfile.TemporaryDirectory(prefix="owned-proof-", dir="/tmp") as directory:
                project = self.clean_run_directory(directory)
                configuration = project.config["owned_stop_proof"]
                (project.directory / "authority/authority.lock").write_bytes(marker)
                if outcome != "clean":
                    record = json.loads(json.loads((project.directory / "launcher-stop.json").read_text())["payload"])
                    record["outcome"] = outcome
                    record["stages"] = [{"name": "platform", "ack": True, "exit_code": 1, "signal": None}]
                    dev.atomic_private_json(project.directory / "launcher-stop.json",
                                            dev.signed_owned_identity(configuration, record))
                (project.directory / "launcher-terminal.json").unlink()
                with self.assertRaisesRegex(RuntimeError, "clean launcher terminal proof|authority clean startup marker"):
                    dev.Project(project.arguments).prepare()

    def test_missing_terminal_with_pre_ready_attempt_is_refused(self):
        with tempfile.TemporaryDirectory(prefix="owned-proof-", dir="/tmp") as directory:
            project = self.clean_run_directory(directory)
            configuration = project.config["owned_stop_proof"]
            attempt = {"version": 1, "kind": "launcher_startup_attempt",
                       "generation": configuration["generation"], "ready": False,
                       "services_started": [], "pids": {}, "commands": {},
                       "terminal_sha256": None, "state_hashes": "snapshot"}
            dev.atomic_private_json(project.directory / "launcher-startup-attempt.json",
                                    dev.signed_owned_identity(configuration, attempt))
            (project.directory / "launcher-terminal.json").unlink()
            with self.assertRaises((RuntimeError, FileNotFoundError)):
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

    def recovery_arguments(self, project):
        arguments = argparse.Namespace(**vars(project.arguments))
        arguments.recover = True
        return arguments

    def test_recovery_admits_only_an_unclean_run_and_preserves_its_stop_record(self):
        with tempfile.TemporaryDirectory(prefix="recovery-gate-", dir="/tmp") as directory:
            project = self.clean_run_directory(directory)
            configuration = project.config["owned_stop_proof"]
            # A clean run is not a recovery target: its normal gate already passes.
            with self.assertRaisesRegex(RuntimeError, "already passes the clean restart gate"):
                dev.Project(self.recovery_arguments(project)).prepare()
            terminal = project.directory / "launcher-terminal.json"
            terminal.unlink()
            unclean = dict(json.loads(json.loads((project.directory / "launcher-stop.json").read_bytes())["payload"]), outcome="unclean", reason="platform stopped")
            dev.atomic_private_json(project.directory / "launcher-stop.json", dev.signed_owned_identity(configuration, unclean))
            dev.Project(self.recovery_arguments(project)).prepare()
            archive = json.loads(json.loads((project.directory / "launcher-stop.pre-recovery.json").read_bytes())["payload"])
            self.assertEqual(archive["outcome"], "unclean")
            self.assertEqual(archive["reason"], "platform stopped")
            # The authority's own marker is still a hard precondition.
            (project.directory / "authority/authority.lock").write_bytes(b"dirty\n")
            with self.assertRaisesRegex(RuntimeError, "clean authority startup marker"):
                dev.Project(self.recovery_arguments(project)).prepare()

    def test_recovery_admits_a_signed_nonzero_worker_exit(self):
        with tempfile.TemporaryDirectory(prefix="recovery-dead-", dir="/tmp") as directory:
            project = self.clean_run_directory(directory)
            configuration = project.config["owned_stop_proof"]
            (project.directory / "launcher-terminal.json").unlink()
            worker = next(iter(project.launch["workers"]))
            proofs = pathlib.Path(configuration["directory"])
            identity = json.loads(json.loads((proofs / (worker + ".identity.json")).read_bytes())["payload"])
            dev.atomic_private_json(proofs / (worker + ".stop.json"),
                                    dev.signed_owned_identity(configuration, {**identity, "waited": True, "exit_code": 70}))
            dev.atomic_private_json(project.directory / "launcher-stop.json", dev.signed_owned_identity(configuration, {
                "version": 1, "generation": configuration["generation"], "outcome": "unclean",
                "reason": worker + " exited without a clean proof", "deadline_ms": 1,
                "stages": [{"name": worker, "exit_code": 70, "ack": True, "signal": None}],
                "unresolved_operations": False,
            }))
            recovery = dev.Project(self.recovery_arguments(project))
            recovery.prepare()
            self.assertEqual(set(recovery.recovered_dead_workers), {worker})
            self.assertEqual(recovery.recovered_dead_workers[worker]["exit_code"], 70)
            # The recovery terminal carries the signed evidence so a later normal
            # start can verify the dead incarnation without a clean stop proof.
            dev.atomic_private_json(project.directory / "launcher-terminal.json", dev.signed_owned_identity(
                configuration, dev.terminal_record(project.directory, configuration,
                                                   dead_workers=recovery.recovered_dead_workers)))
            self.assertEqual(dev.verify_clean_terminal(project.directory, configuration),
                             recovery.recovered_dead_workers)
            dev.verify_owned_stop_proofs(project.directory, configuration, project.launch["workers"],
                                         allow_dead=recovery.recovered_dead_workers)
            # The raw proof set alone still refuses the nonzero join.
            with self.assertRaisesRegex(RuntimeError, "incarnation or joined exit"):
                dev.verify_owned_stop_proofs(project.directory, configuration, project.launch["workers"])

    def test_recovery_refuses_an_unproven_worker_exit(self):
        with tempfile.TemporaryDirectory(prefix="recovery-unproven-", dir="/tmp") as directory:
            project = self.clean_run_directory(directory)
            configuration = project.config["owned_stop_proof"]
            (project.directory / "launcher-terminal.json").unlink()
            worker = next(iter(project.launch["workers"]))
            (pathlib.Path(configuration["directory"]) / (worker + ".stop.json")).unlink()
            dev.atomic_private_json(project.directory / "launcher-stop.json", dev.signed_owned_identity(configuration, {
                "version": 1, "generation": configuration["generation"], "outcome": "unclean",
                "reason": "shutdown incomplete", "deadline_ms": 1, "stages": [],
                "unresolved_operations": False,
            }))
            with self.assertRaisesRegex(RuntimeError, "no joined exit proof"):
                dev.Project(self.recovery_arguments(project)).prepare()
            # A stage that contradicts the signed stop proof is not evidence.
            identity = json.loads(json.loads((pathlib.Path(configuration["directory"]) / (worker + ".identity.json")).read_bytes())["payload"])
            dev.atomic_private_json(pathlib.Path(configuration["directory"]) / (worker + ".stop.json"),
                                    dev.signed_owned_identity(configuration, {**identity, "waited": True, "exit_code": 7}))
            dev.atomic_private_json(project.directory / "launcher-stop.json", dev.signed_owned_identity(configuration, {
                "version": 1, "generation": configuration["generation"], "outcome": "unclean",
                "reason": "shutdown incomplete", "deadline_ms": 1,
                "stages": [{"name": worker, "exit_code": 1, "ack": True, "signal": None}],
                "unresolved_operations": False,
            }))
            with self.assertRaisesRegex(RuntimeError, "incarnation or joined exit"):
                dev.Project(self.recovery_arguments(project)).prepare()

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
            self.assertEqual(len(list(failed[0].iterdir())), 4)
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
            self.assertFalse((root / "launcher-restart-failure.json").exists())

    def test_operator_recovery_refusal_keeps_existing_failure_record(self):
        with tempfile.TemporaryDirectory(prefix="restart-refusal-record-", dir="/tmp") as directory:
            fixture = self.clean_run_directory(directory)
            root = pathlib.Path(directory).resolve()
            failure_path = root / "launcher-restart-failure.json"
            dev.atomic_private_json(failure_path, dev.signed_owned_identity(fixture.config["owned_stop_proof"], {
                "version": 1, "kind": "launcher_restart_failure", "phase": "startup",
                "requires_operator": True, "teardown_completed": True, "all_processes_stopped": True,
            }))
            previous = failure_path.read_bytes()

            def refusing_run(project):
                project.acquire_run_lock()
                project.prepare()

            with patch.object(dev.Project, "run", refusing_run), patch.object(sys, "argv", self.launcher_arguments(root)):
                self.assertEqual(dev.main(), 1)
            self.assertEqual(failure_path.read_bytes(), previous)

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

    def test_cleanup_failure_preserves_clean_evidence_when_service_started(self):
        # A started platform may already have run the controller, written the
        # authority dirty marker or mutated the backend before readiness. Even
        # when teardown fails but every process is confirmed stopped, the old
        # clean evidence must be preserved for an operator instead of rolled back.
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
            preserved = list((root / "launcher-clean-evidence-preserved").iterdir())
            self.assertEqual(len(preserved), 1)
            self.assertEqual((preserved[0] / "launcher-terminal.json").read_bytes(), terminal_bytes)
            record = json.loads(json.loads((root / "launcher-restart-failure.json").read_text())["payload"])
            self.assertTrue(record["teardown_completed"])
            self.assertTrue(record["all_processes_stopped"])
            self.assertFalse(record["teardown_clean"])
            self.assertIn("platform exited without a clean proof", record["teardown_error"])
            self.assertFalse(record["clean_evidence"]["restored"])
            self.assertTrue(record["clean_evidence"]["preserved"])
            self.assertTrue(record["requires_operator"])
            self.assertIn("services were started", record["clean_evidence"]["preservation_reason"])
            with self.assertRaisesRegex(RuntimeError, "requires operator recovery"):
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
            self.assertEqual(terminal_path.read_bytes(), terminal_bytes)
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
            # Recovery uses the in-memory evidence even though the audit record
            # could not be written. Because a service was started, the evidence is
            # preserved for an operator instead of being rolled back.
            self.assertEqual(stop_path.read_bytes(), stop_bytes)
            self.assertEqual(terminal_path.read_bytes(), terminal_bytes)
            preserved = list((root / "launcher-clean-evidence-preserved").iterdir())
            self.assertEqual(len(preserved), 1)
            self.assertEqual((preserved[0] / "launcher-terminal.json").read_bytes(), terminal_bytes)
            self.assertFalse((root / "launcher-restart-failure.json").exists())
            unavailable = json.loads((root / "launcher-restart-failure-unavailable.json").read_text())
            self.assertEqual(unavailable["kind"], "launcher_restart_failure_unavailable")
            self.assertIn("operation_incomplete", unavailable["error"])
            self.assertIn("disk full", unavailable["record_error"])
            self.assertFalse(unavailable["recovery"]["restored"])
            self.assertTrue(unavailable["recovery"]["preserved"])
            self.assertTrue(unavailable["recovery"]["requires_operator"])
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

    def test_refused_prepare_records_operator_requirement_for_bad_stop(self):
        for outcome in ("unclean", "missing"):
            with self.subTest(outcome=outcome), tempfile.TemporaryDirectory(prefix="restart-stop-record-", dir="/tmp") as directory:
                fixture = self.clean_run_directory(directory)
                root = pathlib.Path(directory).resolve()
                configuration = fixture.config["owned_stop_proof"]
                terminal_bytes = (root / "launcher-terminal.json").read_bytes()
                if outcome == "missing":
                    (root / "launcher-stop.json").unlink()
                else:
                    dev.atomic_private_json(root / "launcher-stop.json", dev.signed_owned_identity(configuration, {
                        "version": 1, "generation": configuration["generation"], "outcome": "unclean",
                        "reason": "synthetic", "deadline_ms": 1, "stages": [],
                        "unresolved_operations": "not_verified"}))

                def refusing_run(project):
                    project.acquire_run_lock()
                    project.prepare()

                with patch.object(dev.Project, "run", refusing_run), patch.object(sys, "argv", self.launcher_arguments(root)):
                    self.assertEqual(dev.main(), 1)
                self.assertFalse((root / "launcher-restart-failure.json").exists())
                with self.assertRaisesRegex(RuntimeError, "stop"):
                    dev.Project(fixture.arguments).prepare()
                self.assertEqual((root / "launcher-terminal.json").read_bytes(), terminal_bytes)

    def test_stale_recovered_failure_record_is_replaced_by_new_refusal(self):
        with tempfile.TemporaryDirectory(prefix="restart-stale-record-", dir="/tmp") as directory:
            fixture = self.clean_run_directory(directory)
            root = pathlib.Path(directory).resolve()
            configuration = fixture.config["owned_stop_proof"]
            failure_path = root / "launcher-restart-failure.json"
            dev.atomic_private_json(failure_path, dev.signed_owned_identity(configuration, {
                "version": 1, "kind": "launcher_restart_failure", "phase": "startup",
                "requires_operator": False, "teardown_completed": True, "all_processes_stopped": True,
                "error": "previous recovered failure",
            }))
            previous = failure_path.read_bytes()
            (root / "launcher-stop.json").unlink()

            def refusing_run(project):
                project.acquire_run_lock()
                project.prepare()

            with patch.object(dev.Project, "run", refusing_run), patch.object(sys, "argv", self.launcher_arguments(root)):
                self.assertEqual(dev.main(), 1)
            self.assertEqual(failure_path.read_bytes(), previous)
            with self.assertRaisesRegex(RuntimeError, "legacy clean proof"):
                dev.Project(fixture.arguments).prepare()

    def test_failure_gate_rejection_is_never_replaced_by_reuse(self):
        cases = {
            "teardown incomplete": {"version": 1, "kind": "launcher_restart_failure",
                                    "requires_operator": False, "teardown_completed": False},
            "processes not stopped": {"version": 1, "kind": "launcher_restart_failure",
                                      "requires_operator": False, "teardown_completed": True,
                                      "all_processes_stopped": False},
            "legacy missing teardown": {"version": 1, "kind": "launcher_restart_failure",
                                        "requires_operator": False},
            "missing requires_operator": {"version": 1, "kind": "launcher_restart_failure",
                                          "teardown_completed": True},
            "unknown kind": {"version": 1, "kind": "other",
                             "requires_operator": False, "teardown_completed": True},
        }
        for label, record in cases.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory(prefix="restart-gate-", dir="/tmp") as directory:
                fixture = self.clean_run_directory(directory)
                root = pathlib.Path(directory).resolve()
                failure_path = root / "launcher-restart-failure.json"
                dev.atomic_private_json(failure_path, dev.signed_owned_identity(
                    fixture.config["owned_stop_proof"], record))
                previous = failure_path.read_bytes()

                def refusing_run(project):
                    project.acquire_run_lock()
                    project.prepare()

                with patch.object(dev.Project, "run", refusing_run), patch.object(sys, "argv", self.launcher_arguments(root)):
                    self.assertEqual(dev.main(), 1)
                self.assertEqual(failure_path.read_bytes(), previous)
                with self.assertRaisesRegex(RuntimeError, "operator recovery"):
                    dev.Project(fixture.arguments).prepare()
        with tempfile.TemporaryDirectory(prefix="restart-gate-mac-", dir="/tmp") as directory:
            fixture = self.clean_run_directory(directory)
            root = pathlib.Path(directory).resolve()
            failure_path = root / "launcher-restart-failure.json"
            dev.atomic_private_json(failure_path, dev.signed_owned_identity(
                fixture.config["owned_stop_proof"], {
                    "version": 1, "kind": "launcher_restart_failure",
                    "requires_operator": True, "teardown_completed": True}))
            tampered = json.loads(failure_path.read_text())
            tampered["mac"] = "0" * 64
            failure_path.write_text(json.dumps(tampered))
            previous = failure_path.read_bytes()

            def refusing_run(project):
                project.acquire_run_lock()
                project.prepare()

            with patch.object(dev.Project, "run", refusing_run), patch.object(sys, "argv", self.launcher_arguments(root)):
                self.assertEqual(dev.main(), 1)
            self.assertEqual(failure_path.read_bytes(), previous)
            with self.assertRaisesRegex(RuntimeError, "operator recovery"):
                dev.Project(fixture.arguments).prepare()

    def live_port(self):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            return probe.getsockname()[1]

    def live_environment(self):
        environment = dev.safe_environment()
        environment["PERI_RUST_BINARY"] = os.environ["PERI_RUST_BINARY"]
        environment["PERI_PLATFORM_BINARY"] = os.environ["PERI_PLATFORM_BINARY"]
        return environment

    def launch_live(self, root, port, launcher=source, extra=()):
        process = subprocess.Popen(
            [sys.executable, str(launcher), "--run-dir", str(root),
             "--listen", f"127.0.0.1:{port}", "--tls", "--no-build", *extra],
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

    @unittest.skipUnless(os.environ.get("PERI_RESTART_LIVE") == "1", "set PERI_RESTART_LIVE=1")
    def test_live_new_volume_rebind_and_second_restart(self):
        with tempfile.TemporaryDirectory(prefix="release-volume-", dir="/tmp") as directory:
            root = pathlib.Path(directory).resolve()
            port = self.live_port()
            first = self.launch_live(root, port)
            try:
                client = LiveSQLClient(root)
                client.statement("CREATE TABLE release_volume (id INTEGER PRIMARY KEY, value TEXT)", "execute")
                client.statement("INSERT INTO release_volume VALUES (1, 'before-upgrade')", "execute")
            finally:
                self.stop_live(first)
            launch_bytes = (root / "launch.json").read_bytes()
            self.assertEqual(json.loads(launch_bytes)["wal_replay_bytes"], 0)
            rejected = subprocess.run(
                [sys.executable, str(source), "--run-dir", str(root), "--listen",
                 f"127.0.0.1:{self.live_port()}", "--tls", "--no-build"],
                env=self.live_environment(), capture_output=True, text=True, timeout=30)
            self.assertEqual(rejected.returncode, 1)
            self.assertIn("cannot be silently rebound", rejected.stderr)
            self.assertFalse((root / "launcher-restart-failure.json").exists())
            for cycle in range(2):
                upgraded = self.launch_live(root, port)
                try:
                    client = LiveSQLClient(root)
                    self.assertEqual(client.statement("SELECT value FROM release_volume WHERE id = 1", "query"), [["before-upgrade"]])
                    if cycle == 0:
                        client.statement("INSERT INTO release_volume VALUES (2, 'after-upgrade')", "execute")
                    self.assertEqual(client.statement("SELECT value FROM release_volume WHERE id = 2", "query"), [["after-upgrade"]])
                finally:
                    self.stop_live(upgraded)
                self.assertEqual((root / "launch.json").read_bytes(), launch_bytes)

    @unittest.skipUnless(os.environ.get("PERI_RESTART_LIVE") == "1", "set PERI_RESTART_LIVE=1")
    def test_live_failed_restart_recovery_supersedes_failure(self):
        with tempfile.TemporaryDirectory(prefix="restart-recover-", dir="/tmp") as directory:
            root = pathlib.Path(directory).resolve()
            port = self.live_port()
            first = self.launch_live(root, port)
            try:
                client = LiveSQLClient(root)
                client.statement("CREATE TABLE recovery_volume (value TEXT)", "execute")
                client.statement("INSERT INTO recovery_volume VALUES ('retained')", "execute")
            finally:
                self.stop_live(first)
            script = """
import importlib.util, sys
spec = importlib.util.spec_from_file_location('launcher', sys.argv[1])
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)
original_request = dev.request
def request(endpoint, credential, path, body, certificate=None, timeout=30):
    result = original_request(endpoint, credential, path, body, certificate, timeout)
    if path == '/v1/databases/status': raise RuntimeError('injected pre-ready failure')
    return result
dev.request = request
sys.argv = sys.argv[1:]
sys.exit(dev.main())
"""
            failed = subprocess.run([sys.executable, "-c", script, str(source), "--run-dir", str(root),
                                     "--listen", f"127.0.0.1:{port}", "--tls", "--no-build"],
                                    env=self.live_environment(), capture_output=True, text=True, timeout=90)
            self.assertEqual(failed.returncode, 1)
            self.assertIn("injected pre-ready failure", failed.stderr)
            failure_path = root / "launcher-restart-failure.json"
            original = failure_path.read_bytes()
            self.assertTrue(json.loads(json.loads(original)["payload"])["requires_operator"])
            recovery = self.launch_live(root, port, extra=("--recover",))
            self.stop_live(recovery)
            record = json.loads(json.loads(failure_path.read_bytes())["payload"])
            self.assertFalse(record["requires_operator"])
            self.assertEqual(record["superseded_by_terminal_sha256"], dev.file_sha256(root / "launcher-terminal.json"))
            self.assertEqual((root / "launcher-restart-failure-history" / (hashlib.sha256(original).hexdigest() + ".json")).read_bytes(), original)
            restarted = self.launch_live(root, port)
            try:
                self.assertEqual(LiveSQLClient(root).statement("SELECT value FROM recovery_volume", "query"), [["retained"]])
            finally:
                self.stop_live(restarted)

    @unittest.skipUnless(os.environ.get("PERI_RESTART_LIVE") == "1", "set PERI_RESTART_LIVE=1")
    def test_live_sigkill_startup_boundaries(self):
        for boundary, label in (("staging", "E1-staging"), ("authority", "authority-ready"),
                                ("platform", "E2a-authority+platform"), ("before-ready", "E2b-all-services")):
            with self.subTest(boundary=label), tempfile.TemporaryDirectory(prefix="kill-startup-", dir="/tmp") as directory:
                root = pathlib.Path(directory).resolve()
                port = self.live_port()
                first = self.launch_live(root, port)
                try:
                    client = LiveSQLClient(root)
                    client.statement("CREATE TABLE startup_kill (value TEXT)", "execute")
                    client.statement("INSERT INTO startup_kill VALUES ('preserved')", "execute")
                finally:
                    self.stop_live(first)
                terminal_bytes = (root / "launcher-terminal.json").read_bytes()
                script = """
import argparse, importlib.util, json, os, pathlib, signal, sys
spec = importlib.util.spec_from_file_location('launcher', sys.argv[1])
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)
p = dev.Project(argparse.Namespace(run_dir=sys.argv[2], backend='local', listen=sys.argv[3], tls=True, no_build=True, writer_admission=False))
boundary = sys.argv[4]
def kill():
    dev.atomic_private_json(p.directory / 'test-child-pids.json', {name: process.pid for name, process in p.processes.items()})
    os.kill(os.getpid(), signal.SIGKILL)
original_prepare, original_start = p.prepare, p.start
def prepare():
    original_prepare()
    if boundary == 'staging': kill()
def start(name, command, environment):
    result = original_start(name, command, environment)
    if boundary == name: kill()
    return result
original_request = dev.request
def request(endpoint, credential, path, body, certificate=None, timeout=30):
    result = original_request(endpoint, credential, path, body, certificate, timeout)
    if boundary == 'before-ready' and path == '/v1/databases/status': kill()
    return result
p.prepare, p.start, dev.request = prepare, start, request
p.run()
"""
                killed = subprocess.run([sys.executable, "-c", script, str(source), str(root),
                                         f"127.0.0.1:{port}", boundary], env=self.live_environment(),
                                        capture_output=True, text=True, timeout=90)
                self.assertEqual(killed.returncode, -signal.SIGKILL)
                pids = json.loads((root / "test-child-pids.json").read_text())
                try:
                    # launcher 已死，测试只终止自身创建的进程，不制造 joined stop proof。
                    for name in ("platform", "worker-a", "worker-b", "authority"):
                        if name in pids:
                            try:
                                os.kill(pids[name], signal.SIGTERM)
                            except ProcessLookupError:
                                pass
                    deadline = time.monotonic() + 20
                    alive = set(pids.values())
                    while alive and time.monotonic() < deadline:
                        for pid in list(alive):
                            try:
                                os.kill(pid, 0)
                            except ProcessLookupError:
                                alive.remove(pid)
                        time.sleep(0.1)
                    self.assertFalse(alive, "test child processes did not exit")
                    if boundary == "staging":
                        restarted = self.launch_live(root, port)
                        try:
                            self.assertEqual(LiveSQLClient(root).statement("SELECT value FROM startup_kill", "query"), [["preserved"]])
                        finally:
                            self.stop_live(restarted)
                    else:
                        refused = subprocess.run(
                            [sys.executable, str(source), "--run-dir", str(root), "--listen",
                             f"127.0.0.1:{port}", "--tls", "--no-build"],
                            env=self.live_environment(), capture_output=True, text=True, timeout=30)
                        self.assertEqual(refused.returncode, 1)
                        self.assertIn("interrupted startup", refused.stderr)
                        self.assertIn("operator recovery", refused.stderr)
                        self.assertEqual((root / "launcher-terminal.json").read_bytes(), terminal_bytes)
                        recovery = self.launch_live(root, port, extra=("--recover",))
                        self.stop_live(recovery)
                        self.assertEqual((root / "authority/authority.lock").read_bytes(), b"clean\n")
                        restarted = self.launch_live(root, port)
                        try:
                            self.assertEqual(LiveSQLClient(root).statement("SELECT value FROM startup_kill", "query"), [["preserved"]])
                        finally:
                            self.stop_live(restarted)
                finally:
                    for pid in pids.values():
                        try:
                            os.kill(pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass

    @unittest.skipUnless(os.environ.get("PERI_RESTART_LIVE") == "1", "set PERI_RESTART_LIVE=1")
    def test_live_new_volume_occupied_port_is_retryable(self):
        with tempfile.TemporaryDirectory(prefix="new-volume-port-", dir="/tmp") as directory:
            root = pathlib.Path(directory).resolve()
            port = self.live_port()
            holder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            holder.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            holder.bind(("127.0.0.1", port))
            holder.listen(8)
            try:
                failed = subprocess.run(
                    [sys.executable, str(source), "--run-dir", str(root),
                     "--listen", f"127.0.0.1:{port}", "--tls", "--no-build"],
                    env=self.live_environment(), capture_output=True, text=True, timeout=60)
            finally:
                holder.close()
            self.assertEqual(failed.returncode, 1)
            self.assertIn("listener unavailable before startup", failed.stderr)
            self.assertFalse((root / "launch.json").exists())
            self.assertFalse((root / "platform.json").exists())
            self.assertFalse((root / "authority/authority.lock").exists())
            self.assertFalse((root / "launcher-restart-failure.json").exists())
            first = self.launch_live(root, port)
            try:
                client = LiveSQLClient(root)
                client.statement("CREATE TABLE new_volume (value TEXT)", "execute")
                client.statement("INSERT INTO new_volume VALUES ('retry')", "execute")
            finally:
                self.stop_live(first)
            second = self.launch_live(root, port)
            try:
                self.assertEqual(LiveSQLClient(root).statement("SELECT value FROM new_volume", "query"), [["retry"]])
            finally:
                self.stop_live(second)

    @unittest.skipUnless(os.environ.get("PERI_RESTART_LIVE") == "1", "set PERI_RESTART_LIVE=1")
    def test_live_spawn_window_authority_recovery(self):
        with tempfile.TemporaryDirectory(prefix="spawn-window-", dir="/tmp") as directory:
            root = pathlib.Path(directory).resolve()
            port = self.live_port()
            first = self.launch_live(root, port)
            try:
                client = LiveSQLClient(root)
                client.statement("CREATE TABLE spawn_window (value TEXT)", "execute")
                client.statement("INSERT INTO spawn_window VALUES ('preserved')", "execute")
            finally:
                self.stop_live(first)
            script = """
import importlib.util, os, signal, sys
spec = importlib.util.spec_from_file_location('launcher', sys.argv[1])
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)
original_popen = dev.subprocess.Popen
def popen(*args, **kwargs):
    process = original_popen(*args, **kwargs)
    command = args[0] if args else kwargs.get('args')
    if command and len(command) > 1 and command[1] == 'authority':
        run_dir = sys.argv[sys.argv.index('--run-dir') + 1]
        dev.private_json(dev.pathlib.Path(run_dir) / 'test-spawn-pid.json', {'pid': process.pid})
        os.kill(os.getpid(), signal.SIGKILL)
    return process
dev.subprocess.Popen = popen
sys.argv = sys.argv[1:]
sys.exit(dev.main())
"""
            killed = subprocess.run(
                [sys.executable, "-c", script, str(source), "--run-dir", str(root),
                 "--listen", f"127.0.0.1:{port}", "--tls", "--no-build"],
                env=self.live_environment(), capture_output=True, text=True, timeout=60)
            self.assertEqual(killed.returncode, -signal.SIGKILL)
            child = json.loads((root / "test-spawn-pid.json").read_text())["pid"]
            decoys = []
            try:
                deadline = time.monotonic() + 10
                marker = root / "authority/authority.lock"
                while time.monotonic() < deadline:
                    if marker.is_file() and marker.read_bytes() == b"dirty\n":
                        break
                    time.sleep(0.05)
                self.assertEqual(marker.read_bytes(), b"dirty\n")
                configuration = json.loads((root / "platform.json").read_text())["owned_stop_proof"]
                intent = dev.read_signed_private_at(root / "launcher-startup-attempt.json", configuration, "intent")
                self.assertEqual(intent["starting"], "authority")
                self.assertEqual(intent["services_started"], [])
                self.assertEqual(intent["pids"], {})
                self.assertTrue(intent["start_nonce"])
                self.assertTrue(intent["seal_before_spawn_sha256"])
                refused = subprocess.run(
                    [sys.executable, str(source), "--run-dir", str(root), "--listen",
                     f"127.0.0.1:{port}", "--tls", "--no-build"],
                    env=self.live_environment(), capture_output=True, text=True, timeout=30)
                self.assertEqual(refused.returncode, 1)
                self.assertIn("interrupted startup", refused.stderr)
                decoys = [
                    subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)", str(root / "authority")],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL),
                    subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)", "authority", str(root / "authority")],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL),
                ]
                time.sleep(0.2)
                recovery = self.launch_live(root, port, extra=("--recover",))
                self.stop_live(recovery)
                self.assertTrue(all(decoy.poll() is None for decoy in decoys))
                self.assertEqual((root / "authority/authority.lock").read_bytes(), b"clean\n")
                recovered = dev.read_signed_private_at(root / "launcher-startup-attempt.json", configuration, "intent")
                self.assertIn("recovered_from_sha256", recovered)
                self.assertTrue(list((root / "launcher-startup-recovery").iterdir()))
                restarted = self.launch_live(root, port)
                try:
                    self.assertEqual(LiveSQLClient(root).statement("SELECT value FROM spawn_window", "query"), [["preserved"]])
                finally:
                    self.stop_live(restarted)
            finally:
                if dev.process_is_running(child):
                    try:
                        os.kill(child, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                for decoy in decoys:
                    if decoy.poll() is None:
                        decoy.kill()
                    decoy.wait(timeout=5)

    @unittest.skipUnless(os.environ.get("PERI_RESTART_LIVE") == "1", "set PERI_RESTART_LIVE=1")
    def test_live_platform_spawn_window_graceful_stop_then_recover(self):
        with tempfile.TemporaryDirectory(prefix="spawn-platform-", dir="/tmp") as directory:
            root = pathlib.Path(directory).resolve()
            port = self.live_port()
            first = self.launch_live(root, port)
            try:
                client = LiveSQLClient(root)
                client.statement("CREATE TABLE spawn_platform (value TEXT)", "execute")
                client.statement("INSERT INTO spawn_platform VALUES ('preserved')", "execute")
            finally:
                self.stop_live(first)
            project = dev.Project(argparse.Namespace(run_dir=str(root), backend="local",
                                                     listen=f"127.0.0.1:{port}", tls=True,
                                                     no_build=True, writer_admission=False))
            project.acquire_run_lock()
            try:
                project.prepare()
                configuration = project.config["owned_stop_proof"]
                seal, _, reliable = dev.local_object_any(root / "backend", dev.LOCAL_SEAL_KEY)
                seal_before = dev.file_sha256(seal) if reliable and seal else None
            finally:
                project.release_run_lock()
            authority_argv = [str(project.rust), "authority", str(root / "backend"),
                              project.launch["authority"], str(root / "authority")]
            platform_argv = [str(project.go), "-config", str(root / "platform.json")]
            authority_env = dev.safe_environment(capacity_trace=True)
            dev.experimental_environment(authority_env)
            authority_env.update({"PERI_AUTHORITY_CONTROL_TOKEN": project.config["controller"]["authority_token"],
                                  "PERI_AUTHORITY_WORKERS_FILE": str(root / "authority-workers.json")})
            platform_env = dev.safe_environment()
            platform_env["PERI_SQL_OPEN_OBSERVER"] = "0"
            platform_env["PERI_HRANA_EXPERIMENTAL"] = "0"
            platform_env["PERI_SHUTDOWN_STAGE_CAP_MS"] = str(dev.SHUTDOWN_STAGE_CAPS["platform"] * 1000)
            platform_env["PERI_SHUTDOWN_OUTER_GRACE_MS"] = str(dev.ORCHESTRATION_GRACE_SECONDS * 1000)
            authority = platform = None
            try:
                authority = subprocess.Popen(authority_argv, env=authority_env, stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE, text=True)
                lines = queue.Queue()
                threading.Thread(target=lambda: [lines.put(line) for line in authority.stdout], daemon=True).start()
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    try:
                        event = json.loads(lines.get(timeout=0.2))
                    except (queue.Empty, ValueError):
                        if authority.poll() is not None:
                            break
                        continue
                    if event.get("ready") is True:
                        break
                else:
                    raise RuntimeError("manual authority did not become ready")
                platform = subprocess.Popen(platform_argv, env=platform_env, stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE, text=True)
                project.write_startup_intent(configuration, starting="platform", start_nonce="platform-spawn",
                                             commands={"authority": authority_argv, "platform": platform_argv},
                                             services_started=["authority"], pids={"authority": authority.pid},
                                             seal_before_spawn_sha256=seal_before)
                refused = subprocess.run(
                    [sys.executable, str(source), "--run-dir", str(root), "--listen",
                     f"127.0.0.1:{port}", "--tls", "--no-build", "--recover"],
                    env=self.live_environment(), capture_output=True, text=True, timeout=30)
                self.assertEqual(refused.returncode, 1)
                self.assertIn("send SIGTERM", refused.stderr)
                self.assertIn("materializes the operation view", refused.stderr)
                self.assertLess(refused.stderr.index("SIGTERM"), refused.stderr.index("rebuild"))
                self.assertTrue(dev.process_is_running(authority.pid))
                self.assertTrue(dev.process_is_running(platform.pid))
                for process in (platform, authority):
                    process.send_signal(signal.SIGTERM)
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    if authority.poll() is not None and platform.poll() is not None:
                        break
                    time.sleep(0.1)
                self.assertIsNotNone(authority.poll())
                self.assertIsNotNone(platform.poll())
                recovery = self.launch_live(root, port, extra=("--recover",))
                self.stop_live(recovery)
                restarted = self.launch_live(root, port)
                try:
                    self.assertEqual(LiveSQLClient(root).statement("SELECT value FROM spawn_platform", "query"), [["preserved"]])
                finally:
                    self.stop_live(restarted)
            finally:
                for process in (platform, authority):
                    if process is None:
                        continue
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=10)
                    for stream in (process.stdout, process.stderr):
                        if stream is not None:
                            stream.close()

    @unittest.skipUnless(os.environ.get("PERI_RESTART_LIVE") == "1", "set PERI_RESTART_LIVE=1")
    def test_live_spawn_window_authority_not_started_is_directly_restartable(self):
        with tempfile.TemporaryDirectory(prefix="spawn-empty-", dir="/tmp") as directory:
            root = pathlib.Path(directory).resolve()
            port = self.live_port()
            first = self.launch_live(root, port)
            try:
                client = LiveSQLClient(root)
                client.statement("CREATE TABLE spawn_empty (value TEXT)", "execute")
                client.statement("INSERT INTO spawn_empty VALUES ('preserved')", "execute")
            finally:
                self.stop_live(first)
            arguments = argparse.Namespace(run_dir=str(root), backend="local", listen=f"127.0.0.1:{port}",
                                           tls=True, no_build=True, writer_admission=False)
            project = dev.Project(arguments)
            project.acquire_run_lock()
            try:
                project.prepare()
                seal, _, reliable = dev.local_object_any(project.directory / "backend", dev.LOCAL_SEAL_KEY)
                project.write_startup_intent(project.config["owned_stop_proof"], starting="authority",
                                             start_nonce="spawn-empty",
                                             seal_before_spawn_sha256=dev.file_sha256(seal) if reliable and seal else None)
            finally:
                project.release_run_lock()
            self.assertFalse((root / "authority/authority.lock").read_bytes() == b"dirty\n")
            restarted = self.launch_live(root, port)
            try:
                self.assertEqual(LiveSQLClient(root).statement("SELECT value FROM spawn_empty", "query"), [["preserved"]])
            finally:
                self.stop_live(restarted)

    @unittest.skipUnless(
        os.environ.get("PERI_RESTART_LIVE") == "1"
        and pathlib.Path(os.environ.get("PERI_RUST_BINARY", "")).is_file()
        and pathlib.Path(os.environ.get("PERI_PLATFORM_BINARY", "")).is_file(),
        "set PERI_RESTART_LIVE=1 and PERI_RUST_BINARY/PERI_PLATFORM_BINARY to run the live restart")
    def test_live_port_conflict_is_retryable_without_consuming_evidence(self):
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
            self.assertEqual((root / "authority/authority.lock").read_bytes(), b"clean\n")
            self.assertFalse((root / "launcher-restart-failure.json").exists())
            restarted = self.launch_live(root, port)
            try:
                self.assertEqual(LiveSQLClient(root).statement("SELECT id, note FROM restart_live", "query"), [[1, "keep-me"]])
            finally:
                self.stop_live(restarted)

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
                self.assertEqual(terminal_path.read_bytes(), terminal_bytes)
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

    def test_restart_failure_gate_requires_confirmed_process_exit(self):
        cases = (
            ("all_processes_stopped false",
             {"requires_operator": False, "teardown_completed": True, "all_processes_stopped": False},
             "all processes stopped"),
            ("all_processes_stopped true",
             {"requires_operator": False, "teardown_completed": True, "all_processes_stopped": True},
             None),
            ("legacy record with completed teardown",
             {"requires_operator": False, "teardown_completed": True},
             None),
            ("legacy record with incomplete teardown",
             {"requires_operator": False, "teardown_completed": False},
             "teardown not confirmed complete"),
        )
        for label, extra, expected in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory(prefix="restart-gate-", dir="/tmp") as directory:
                project = self.clean_run_directory(directory)
                root = pathlib.Path(directory).resolve()
                configuration = project.config["owned_stop_proof"]
                record = {"version": 1, "kind": "launcher_restart_failure", "run_dir": str(root),
                          "phase": "startup", "error": "synthetic", "failed_at": "2026-01-01T00:00:00+00:00"}
                record.update(extra)
                dev.atomic_private_json(root / "launcher-restart-failure.json",
                                        dev.signed_owned_identity(configuration, record))
                if expected is None:
                    dev.Project(project.arguments).prepare()
                else:
                    with self.assertRaisesRegex(RuntimeError, expected):
                        dev.Project(project.arguments).prepare()

    def test_sigkill_after_staging_preserves_canonical_evidence(self):
        with tempfile.TemporaryDirectory(prefix="restart-crash-", dir="/tmp") as directory:
            project = self.clean_run_directory(directory)
            root = pathlib.Path(directory).resolve()
            terminal_bytes = (root / "launcher-terminal.json").read_bytes()
            script = """
import importlib.util, argparse, os, signal, sys
spec = importlib.util.spec_from_file_location('launcher', sys.argv[1])
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)
p = dev.Project(argparse.Namespace(run_dir=sys.argv[2], backend='local', listen='127.0.0.1:19443', tls=False, no_build=True, writer_admission=False))
p.acquire_run_lock()
p.prepare()
os.kill(os.getpid(), signal.SIGKILL)
"""
            result = subprocess.run([sys.executable, "-c", script, str(source), str(root)], timeout=30)
            self.assertEqual(result.returncode, -signal.SIGKILL)
            self.assertEqual((root / "launcher-terminal.json").read_bytes(), terminal_bytes)
            restarted = dev.Project(project.arguments)
            restarted.acquire_run_lock()
            try:
                restarted.prepare()
            finally:
                restarted.release_run_lock()
            self.assertEqual((root / "launcher-terminal.json").read_bytes(), terminal_bytes)

    def test_interrupted_startup_with_changed_state_is_operator_only(self):
        for stage in ("authority", "platform", "ready-before-announcement"):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory(prefix="restart-intent-", dir="/tmp") as directory:
                fixture = self.clean_run_directory(directory)
                attempt = dev.Project(fixture.arguments)
                attempt.prepare()
                attempt.write_startup_intent(attempt.config["owned_stop_proof"], services_started=[stage])
                with self.assertRaisesRegex(RuntimeError, "operator recovery.*stop and join"):
                    dev.Project(fixture.arguments).prepare()
                self.assertTrue((fixture.directory / "launcher-terminal.json").is_file())

    def test_startup_intent_rejects_changed_sidecar_and_bad_signature(self):
        for change in ("sidecar", "signature", "dirty"):
            with self.subTest(change=change), tempfile.TemporaryDirectory(prefix="intent-tamper-", dir="/tmp") as directory:
                fixture = self.clean_run_directory(directory)
                restarted = dev.Project(fixture.arguments)
                restarted.prepare()
                intent = fixture.directory / "launcher-startup-attempt.json"
                if change == "sidecar":
                    dev.private_json(fixture.directory / "platform/state.json.operations", {"pending": True})
                elif change == "signature":
                    envelope = json.loads(intent.read_bytes())
                    envelope["mac"] = "0" * 64
                    dev.atomic_private_json(intent, envelope)
                else:
                    (fixture.directory / "authority/authority.lock").write_bytes(b"dirty\n")
                with self.assertRaises(RuntimeError):
                    dev.Project(fixture.arguments).prepare()
                if change == "dirty":
                    self.assertEqual((fixture.directory / "authority/authority.lock").read_bytes(), b"dirty\n")

    def test_supersede_is_idempotent_and_history_is_bounded(self):
        with tempfile.TemporaryDirectory(prefix="restart-supersede-idem-", dir="/tmp") as directory:
            fixture = self.clean_run_directory(directory)
            configuration = fixture.config["owned_stop_proof"]
            path = fixture.directory / "launcher-restart-failure.json"
            terminal_hash = dev.file_sha256(fixture.directory / "launcher-terminal.json")
            for index in range(40):
                dev.atomic_private_json(path, dev.signed_owned_identity(configuration, {
                    "version": 1, "kind": "launcher_restart_failure", "requires_operator": True,
                    "teardown_completed": True, "all_processes_stopped": True, "error": "failure-" + str(index)}))
                fixture.supersede_restart_failure(terminal_hash)
                self.assertFalse(fixture.read_signed_private(path, configuration, "failure")["requires_operator"])
            history = fixture.directory / "launcher-restart-failure-history"
            self.assertLessEqual(len(list(history.iterdir())), 32)
            previous = path.read_bytes()
            fixture.supersede_restart_failure(terminal_hash)
            self.assertEqual(path.read_bytes(), previous)


        with tempfile.TemporaryDirectory(prefix="restart-rebind-", dir="/tmp") as directory:
            fixture = self.clean_run_directory(directory)
            args = self.launcher_arguments(fixture.directory)
            args[-1] = "127.0.0.1:19444"
            with patch.object(sys, "argv", args):
                self.assertEqual(dev.main(), 1)
            self.assertFalse((fixture.directory / "launcher-restart-failure.json").exists())
            dev.Project(fixture.arguments).prepare()

    def test_v115_shaped_launch_normalizes_before_worker_start(self):
        with tempfile.TemporaryDirectory(prefix="launch-defaults-", dir="/tmp") as directory:
            fixture = self.clean_run_directory(directory)
            launch = dict(fixture.launch)
            launch.pop("wal_replay_bytes")
            dev.atomic_private_json(fixture.directory / "launch.json", launch)
            configuration = fixture.config["owned_stop_proof"]
            dev.atomic_private_json(fixture.directory / "launcher-terminal.json",
                                    dev.signed_owned_identity(configuration, dev.terminal_record(fixture.directory, configuration)))
            restarted = dev.Project(fixture.arguments)
            observed = []
            def start(name, command, environment):
                observed.append(name)
                if name.startswith("worker"):
                    self.assertNotIn("PERI_WORKER_WAL_REPLAY_BYTES", environment)
                restarted.processes[name] = Mock(pid=10001)
            restarted.stop.set()
            try:
                with patch.object(restarted, "start", start), patch.object(restarted, "start_maintenance"), \
                        patch.object(dev, "request", return_value={"credential": "fixture"}), \
                        contextlib.redirect_stdout(io.StringIO()):
                    restarted.run()
                self.assertEqual(observed, ["authority", "platform", "worker-a", "worker-b"])
                self.assertNotIn("wal_replay_bytes", json.loads((fixture.directory / "launch.json").read_bytes()))
            finally:
                restarted.release_run_lock()

    def test_superseding_failure_preserves_original_signed_record(self):
        with tempfile.TemporaryDirectory(prefix="restart-supersede-", dir="/tmp") as directory:
            fixture = self.clean_run_directory(directory)
            configuration = fixture.config["owned_stop_proof"]
            path = fixture.directory / "launcher-restart-failure.json"
            dev.atomic_private_json(path, dev.signed_owned_identity(configuration, {
                "version": 1, "kind": "launcher_restart_failure", "requires_operator": True,
                "teardown_completed": True, "all_processes_stopped": True}))
            original = path.read_bytes()
            fixture.verify_clean_evidence()
            fixture.verify_clean_stop_record(configuration)
            terminal_hash = dev.file_sha256(fixture.directory / "launcher-terminal.json")
            fixture.supersede_restart_failure(terminal_hash)
            fixture.verify_clean_reuse_gate()
            record = fixture.read_signed_private(path, configuration, "failure")
            self.assertFalse(record["requires_operator"])
            self.assertEqual(record["superseded_by_terminal_sha256"], terminal_hash)
            archive = fixture.directory / "launcher-restart-failure-history" / (hashlib.sha256(original).hexdigest() + ".json")
            self.assertEqual(archive.read_bytes(), original)

    def blake3_module(self):
        try:
            import blake3
        except ImportError:
            self.skipTest("blake3 is required for hash-addressed seal lookup")
        return blake3

    def seal_envelope(self, key, checksum=None, **changes):
        payload = json.dumps({"version": 1, "owner": "test", "clean": True}).encode()
        envelope = {"key": key, "bytes": base64.b64encode(payload).decode(),
                    "etag": "etag-1", "checksum": checksum or "0" * 64}
        envelope.update(changes)
        return envelope

    def test_local_object_lookup_validates_hash_addressed_envelope(self):
        blake3 = self.blake3_module()
        with tempfile.TemporaryDirectory(prefix="seal-envelope-", dir="/tmp") as directory:
            backend = pathlib.Path(directory) / "backend"
            backend.mkdir(mode=0o700)
            key = dev.LOCAL_SEAL_KEY
            payload = json.dumps({"version": 1, "owner": "test", "clean": True}).encode()
            candidate = backend / (blake3.blake3(key.encode()).hexdigest() + ".obj")
            valid = {"key": key, "bytes": base64.b64encode(payload).decode(),
                     "etag": "etag-1", "checksum": blake3.blake3(payload).hexdigest()}
            candidate.write_text(json.dumps(valid))
            self.assertEqual(dev.local_object_path(backend, key), (candidate, True))
            malformed = {
                "wrong key": {**valid, "key": "other"},
                "bytes not a string": {**valid, "bytes": [123]},
                "bytes not base64": {**valid, "bytes": "e30"},
                "checksum not 64 hex": {**valid, "checksum": "0" * 63},
                "checksum mismatch": {**valid, "checksum": "0" * 64},
                "empty etag": {**valid, "etag": ""},
                "non-string etag": {**valid, "etag": 1},
            }
            for label, envelope in malformed.items():
                with self.subTest(label=label):
                    candidate.write_text(json.dumps(envelope))
                    self.assertEqual(dev.local_object_path(backend, key), (None, False))
            semantic = {
                "version 2": {"version": 2, "owner": "test", "clean": True},
                "version true": {"version": True, "owner": "test", "clean": True},
                "version float": {"version": 1.0, "owner": "test", "clean": True},
                "clean false": {"version": 1, "owner": "test", "clean": False},
                "clean integer": {"version": 1, "owner": "test", "clean": 1},
                "unknown field": {"version": 1, "owner": "test", "clean": True, "extra": 1},
                "missing owner": {"version": 1, "clean": True},
                "empty owner": {"version": 1, "owner": "", "clean": True},
            }
            for label, record in semantic.items():
                payload = json.dumps(record).encode()
                envelope = {"key": key, "bytes": base64.b64encode(payload).decode(),
                            "etag": "etag-1", "checksum": blake3.blake3(payload).hexdigest()}
                with self.subTest(label=label):
                    candidate.write_text(json.dumps(envelope))
                    self.assertEqual(dev.local_object_path(backend, key), (None, False))
            candidate.write_text("not json")
            self.assertEqual(dev.local_object_path(backend, key), (None, False))
            candidate.unlink()
            self.assertEqual(dev.local_object_path(backend, key), (None, True))

    def test_local_object_lookup_without_blake3_never_confirms_absence(self):
        with tempfile.TemporaryDirectory(prefix="seal-envelope-", dir="/tmp") as directory:
            backend = pathlib.Path(directory) / "backend"
            backend.mkdir(mode=0o700)
            key = dev.LOCAL_SEAL_KEY
            candidate = backend / "seal.obj"
            valid = self.seal_envelope(key)
            with patch.dict(sys.modules, {"blake3": None}):
                candidate.write_text(json.dumps(valid))
                self.assertEqual(dev.local_object_path(backend, key), (candidate, False))
                candidate.write_text(json.dumps({**valid, "key": "other"}))
                self.assertEqual(dev.local_object_path(backend, key), (None, True))
                malformed = {
                    "bytes not a string": {**valid, "bytes": [123]},
                    "bytes not base64": {**valid, "bytes": "e30"},
                    "checksum not 64 hex": {**valid, "checksum": "0" * 63},
                    "empty etag": {**valid, "etag": ""},
                    "non-string etag": {**valid, "etag": 1},
                }
                for label, envelope in malformed.items():
                    with self.subTest(label=label):
                        candidate.write_text(json.dumps(envelope))
                        self.assertEqual(dev.local_object_path(backend, key), (None, False))
                candidate.write_text("not json")
                self.assertEqual(dev.local_object_path(backend, key), (None, False))
                candidate.write_text('["not", "an", "envelope"]')
                self.assertEqual(dev.local_object_path(backend, key), (None, False))
                candidate.write_text(json.dumps(valid))
                with patch.object(dev, "LOCAL_ENVELOPE_LIMIT_BYTES", 16):
                    self.assertEqual(dev.local_object_path(backend, key), (None, False))
                candidate.unlink()
                self.assertEqual(dev.local_object_path(backend, key), (None, True))

    def test_unconfirmed_local_seal_preserves_clean_evidence_for_operator(self):
        blake3 = self.blake3_module()
        key = dev.LOCAL_SEAL_KEY
        seal_name = blake3.blake3(key.encode()).hexdigest() + ".obj"
        scenarios = (
            ("checksum mismatch", self.seal_envelope(key), False, False),
            ("structurally valid without blake3", self.seal_envelope(key), True, True),
        )
        for label, envelope, without_blake3, seal_staged in scenarios:
            with self.subTest(label=label), tempfile.TemporaryDirectory(prefix="restart-seal-", dir="/tmp") as directory:
                fixture = self.clean_run_directory(directory)
                root = pathlib.Path(directory).resolve()

                def failing_run(project):
                    project.acquire_run_lock()
                    backend = project.directory / "backend"
                    backend.mkdir(mode=0o700, exist_ok=True)
                    (backend / seal_name).write_text(json.dumps(envelope))
                    project.prepare()
                    raise RuntimeError("authority did not become ready")

                with contextlib.ExitStack() as stack:
                    if without_blake3:
                        stack.enter_context(patch.dict(sys.modules, {"blake3": None}))
                    stack.enter_context(patch.object(dev.Project, "run", failing_run))
                    stack.enter_context(patch.object(sys, "argv", self.launcher_arguments(root)))
                    self.assertEqual(dev.main(), 1)
                self.assertTrue((root / "launcher-terminal.json").exists())
                preserved = list((root / "launcher-clean-evidence-preserved").iterdir())
                self.assertEqual(len(preserved), 1)
                self.assertEqual((preserved[0] / "launcher-terminal.json").exists(), True)
                if seal_staged:
                    self.assertTrue((preserved[0] / "backend" / seal_name).is_file())
                else:
                    self.assertFalse((preserved[0] / "backend").exists())
                record = json.loads(json.loads((root / "launcher-restart-failure.json").read_text())["payload"])
                self.assertTrue(record["requires_operator"])
                self.assertFalse(record["clean_evidence"]["restored"])
                self.assertTrue(record["clean_evidence"]["preserved"])
                self.assertIn("could not be confirmed during snapshot", record["clean_evidence"]["preservation_reason"])
                self.assertEqual(record["not_rolled_back"]["canonical_clean_evidence"], "preserved")
                self.assertEqual(record["not_rolled_back"]["authority_seal"], "unconfirmed")
                with self.assertRaisesRegex(RuntimeError, "operator"):
                    dev.Project(fixture.arguments).prepare()

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
