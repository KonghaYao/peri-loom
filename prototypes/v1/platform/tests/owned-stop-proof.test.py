import argparse
import hashlib
import hmac
import importlib.util
import json
import os
import pathlib
import socket
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch


source = pathlib.Path(__file__).resolve().parents[1] / "dev.py"
spec = importlib.util.spec_from_file_location("owned_launcher", source)
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)


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
