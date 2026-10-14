import argparse
import ast
import copy
import importlib.util
import json
import pathlib
import shutil
import subprocess
import unittest
from unittest.mock import Mock, patch


source = pathlib.Path(__file__).resolve().parents[1] / "verify-image.py"
spec = importlib.util.spec_from_file_location("verify_image", source)
verify = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify)


class VerificationTests(unittest.TestCase):
    def test_verifier_requires_every_whole_smoke_check(self):
        source = ast.parse(verify.SMOKE.read_text())
        check_lists = [value for node in ast.walk(source) if isinstance(node, ast.Dict)
                       for key, value in zip(node.keys, node.values)
                       if isinstance(key, ast.Constant) and key.value == "checks" and isinstance(value, ast.List)]
        self.assertEqual(len(check_lists), 1)
        self.assertEqual(ast.literal_eval(check_lists[0]), verify.CHECKS)

    def setUp(self):
        self.arguments = argparse.Namespace(image="peri-loom:test", backend="local", ready_timeout=1, smoke_timeout=1, stop_timeout=1)
        self.verification = verify.Verification(self.arguments)
        self.addCleanup(shutil.rmtree, self.verification.directory, True)
        self.verification.docker = Mock()
        self.verification.command = Mock()

    def value(self, running=False):
        return {"Id": "container-id", "Name": "/" + self.verification.name, "Image": "sha256:image",
                "Config": {"User": "10001:10001", "Labels": {verify.LABEL: self.verification.owner}},
                "HostConfig": {}, "NetworkSettings": {"Ports": {"9443/tcp": [{"HostIp": "127.0.0.1", "HostPort": "49123"}]}},
                "State": {"Status": "running" if running else "exited", "Running": running, "ExitCode": 0, "OOMKilled": False, "Pid": 123 if running else 0, "Dead": False}}

    def result(self, stdout="", returncode=0, stderr=""):
        return subprocess.CompletedProcess([], returncode, stdout, stderr)

    def fixture(self):
        verify.private_json(self.verification.fixture / "connection.json", {
            "project_api_key": "project-secret", "global_api_key": "global-secret",
            "endpoint": "https://127.0.0.1:9443", "ca_file": "/data/peri/keys/dev-ca.crt",
            "database": {"namespace": {"tenant": "local_default", "database": "demo"}, "branch": "main"}})

    def smoke_result(self):
        return {"pass": True, "backend": "external_s3_fixture", "tls": "certificate_verified", "checks": verify.CHECKS[:],
                "database": {"namespace": {"tenant": "local_default", "database": "demo"}, "branch": "main"},
                "metrics": {"native": {"authority": {"put": {"total": 1}}, "workers": {"worker-a": {}, "worker-b": {}}}, "errors_total": {}}}

    def acknowledgement(self):
        return json.dumps({"pass": True, "checks": verify.CHECKS, "credentials_logged": False})

    def evidence(self):
        verify.private_json(self.verification.directory / "smoke.json", self.smoke_result())
        verify.private_json(self.verification.directory / "smoke-process.json", {"returncode": 0, "stdout": self.acknowledgement(), "stderr": "", "timed_out": False})
        marker = self.verification.directory / "authority.lock"
        marker.write_bytes(b"clean\n")
        marker.chmod(0o600)

    def eligible_run(self):
        self.evidence()
        self.verification.prepare = Mock()
        self.verification.ready = Mock()
        self.verification.smoke = Mock()
        def cleanup():
            self.verification.report["cleanup"] = "removed_owned_container"
        self.verification.cleanup = Mock(side_effect=cleanup)

    def stopped(self, state=None, marker=b"clean\n"):
        self.verification.attempted_create = True
        self.verification.attempted_volume = True
        value = self.value()
        if state:
            value["State"].update(state)
        self.verification.inspect = Mock(return_value=value)
        self.verification.container = value["Id"]
        def copy(source, destination):
            destination.write_bytes(marker)
            destination.chmod(0o600)
        self.verification.copy = Mock(side_effect=copy)
        self.verification.remove_volume = Mock()

    def test_failed_preparation_reports_stage_without_private_error(self):
        self.verification.prepare = Mock(side_effect=RuntimeError("private details"))
        self.assertFalse(self.verification.run())
        self.assertEqual(self.verification.report["stage"], "prepare")
        self.assertEqual(self.verification.report["failure_type"], "RuntimeError")
        self.assertNotIn("private details", json.dumps(self.verification.report))

    def test_random_private_directories_and_names(self):
        other = verify.Verification(self.arguments)
        self.addCleanup(shutil.rmtree, other.directory, True)
        self.assertNotEqual(self.verification.name, other.name)
        self.assertNotEqual(self.verification.directory, other.directory)
        self.assertEqual(self.verification.directory.stat().st_mode & 0o777, 0o700)
        self.assertEqual(self.verification.fixture.stat().st_mode & 0o777, 0o700)

    def test_private_json_exclusive(self):
        self.fixture()
        connection = self.verification.fixture / "connection.json"
        self.assertEqual(connection.stat().st_mode & 0o777, 0o600)
        with self.assertRaises(FileExistsError):
            verify.private_json(connection, {})

    def test_symlink_temp_path_canonicalizes_directory_fixture_and_bootstrap(self):
        target = self.verification.directory / "temp-root"
        target.mkdir(mode=0o700)
        alias = self.verification.directory / "temp-alias"
        alias.symlink_to(target, target_is_directory=True)
        allocated = target / "peri-image-verify-mock"
        allocated.mkdir(mode=0o700)
        with patch.object(verify.tempfile, "mkdtemp", return_value=str(alias / allocated.name)):
            self.verification = verify.Verification(self.arguments)
        self.assertEqual(self.verification.directory, allocated.resolve())
        self.assertEqual(self.verification.fixture, allocated.resolve() / "private")
        self.assertEqual(self.verification.fixture, self.verification.fixture.resolve())
        self.assertEqual(self.verification.directory.stat().st_mode & 0o777, 0o700)
        self.assertEqual(self.verification.fixture.stat().st_mode & 0o777, 0o700)
        self.verification.container = "container-id"
        self.verification.inspect = Mock(return_value=self.value(running=True))
        self.verification.docker = Mock(side_effect=[self.result('{"ready":true,"service":"local-project"}'), self.result("10001\n")])
        self.verification.command = Mock()
        def copy(source, destination):
            if source.endswith("connection.json"):
                verify.private_json(destination, {"ca_file": "/data/peri/keys/dev-ca.crt", "endpoint": "https://127.0.0.1:9443"})
            elif source.endswith("dev.py"):
                destination.write_bytes((verify.SMOKE.parents[1] / "dev.py").read_bytes())
            else:
                destination.write_text(source)
        self.verification.copy = Mock(side_effect=copy)
        self.verification.ready()
        connection_path = self.verification.fixture / "connection.json"
        self.assertEqual(connection_path, connection_path.resolve())
        connection = json.loads(connection_path.read_text())
        certificate_path = pathlib.Path(connection["ca_file"])
        self.assertEqual(certificate_path, self.verification.fixture / "ca.crt")
        self.assertEqual(certificate_path, certificate_path.resolve())
        for call in self.verification.copy.call_args_list:
            destination = call.args[1]
            self.assertEqual(destination, destination.resolve())
        self.assertIn(str(certificate_path), self.verification.command.call_args.args[0])

    def test_inspect_rejects_foreign_container(self):
        value = self.value()
        value["Config"]["Labels"] = {}
        self.verification.docker.return_value = self.result(json.dumps([value]))
        with self.assertRaisesRegex(RuntimeError, "ownership"):
            self.verification.inspect()

    def test_inspect_rejects_changed_identity(self):
        self.verification.container = "other-id"
        self.verification.docker.return_value = self.result(json.dumps([self.value()]))
        with self.assertRaisesRegex(RuntimeError, "identity"):
            self.verification.inspect()

    def test_prepare_uses_pinned_image_random_port_constraints_and_volume(self):
        image = {"Id": "sha256:image", "Os": "linux", "Architecture": "amd64", "Config": {"User": "10001:10001"}}
        self.verification.docker.side_effect = [self.result(json.dumps([image])), self.result(), self.result(), self.result(json.dumps([self.value()])), self.result()]
        self.verification.prepare()
        command = self.verification.docker.call_args_list[2].args
        for expected in ("sha256:image", "127.0.0.1::9443", "2g", "128", "no-new-privileges", "--no-build"):
            self.assertIn(expected, command)
        self.assertNotIn("--rm", command)
        self.assertNotIn("build", command)
        self.assertIn("type=volume,source=" + self.verification.volume + ",target=/data/peri", command)
        self.assertNotIn("PERI_CAPACITY_TRACE=1", command)
        self.assertFalse(self.verification.report["capacity_trace"])

    def test_capacity_trace_is_explicit_container_environment_opt_in(self):
        image = {"Id": "sha256:image", "Os": "linux", "Architecture": "amd64", "Config": {"User": "10001:10001"}}
        self.verification.arguments.capacity_trace = True
        self.verification.arguments.writer_admission = True
        self.verification.docker.side_effect = [self.result(json.dumps([image])), self.result(), self.result(), self.result(json.dumps([self.value()])), self.result()]
        self.verification.prepare()
        command = self.verification.docker.call_args_list[2].args
        location = command.index("PERI_CAPACITY_TRACE=1")
        self.assertEqual(command[location - 1], "--env")
        self.assertLess(location, command.index("sha256:image"))
        self.assertIn("--writer-admission", command)
        self.assertTrue(self.verification.report["capacity_trace"])
        self.assertTrue(self.verification.report["writer_admission"])

    def test_invalid_images_fail_before_volume_creation(self):
        for operating_system, user in (("windows", "10001:10001"), ("linux", "root")):
            with self.subTest(operating_system=operating_system, user=user):
                self.verification.docker.return_value = self.result(json.dumps([{"Os": operating_system, "Config": {"User": user}}]))
                with self.assertRaises(RuntimeError):
                    self.verification.prepare()
                self.assertFalse(self.verification.attempted_volume)

    def test_ready_rewrites_fixture_and_verifies_issuer(self):
        self.verification.container = "container-id"
        self.verification.inspect = Mock(return_value=self.value(running=True))
        self.verification.docker.side_effect = [self.result('{"ready":true,"service":"local-project"}'), self.result("10001\n")]
        def copy(source, destination):
            if source.endswith("connection.json"):
                verify.private_json(destination, {"ca_file": "/data/peri/keys/dev-ca.crt", "endpoint": "https://127.0.0.1:9443"})
            elif source.endswith("dev.py"):
                destination.write_bytes((verify.SMOKE.parents[1] / "dev.py").read_bytes())
            else:
                destination.write_text(source)
        self.verification.copy = Mock(side_effect=copy)
        self.verification.ready()
        connection = json.loads((self.verification.fixture / "connection.json").read_text())
        self.assertEqual(connection["endpoint"], "https://127.0.0.1:49123")
        self.assertEqual(connection["ca_file"], str(self.verification.fixture / "ca.crt"))
        command = self.verification.command.call_args.args[0]
        self.assertIn("-verify_ip", command)
        self.assertIn("-CAfile", command)
        self.assertIn("sslserver", command)

    def test_uid_mismatch_fails(self):
        self.verification.inspect = Mock(return_value=self.value(running=True))
        self.verification.docker.side_effect = [self.result('{"ready":true,"service":"local-project"}'), self.result("0\n")]
        with self.assertRaisesRegex(RuntimeError, "UID"):
            self.verification.ready()

    def test_smoke_unknown_is_raw_once_and_redacted(self):
        self.fixture()
        self.verification.command.return_value = self.result("Unknown project-secret", 1, "original global-secret")
        with self.assertRaisesRegex(RuntimeError, "Unknown"):
            self.verification.smoke()
        self.verification.command.assert_called_once()
        result = json.loads((self.verification.directory / "smoke-process.json").read_text())
        self.assertEqual(result["stdout"], "Unknown [REDACTED]")
        self.assertEqual(result["stderr"], "original [REDACTED]")
        self.assertEqual(result["returncode"], 1)

    def test_smoke_timeout_retains_partial_raw_output(self):
        self.fixture()
        self.verification.command.side_effect = subprocess.TimeoutExpired("smoke", 1, output=b"Unknown", stderr=b"partial")
        with self.assertRaises(subprocess.TimeoutExpired):
            self.verification.smoke()
        result = json.loads((self.verification.directory / "smoke-process.json").read_text())
        self.assertTrue(result["timed_out"])
        self.assertEqual(result["stdout"], "Unknown")
        self.verification.command.assert_called_once()

    def test_complete_helper_schema_and_acknowledgement_accepted(self):
        self.fixture()
        verify.private_json(self.verification.directory / "smoke.json", self.smoke_result())
        self.verification.command.return_value = self.result(self.acknowledgement())
        self.verification.smoke()
        self.assertTrue(self.verification.smoke_proven)

    def test_false_pass_or_missing_helper_fields_rejected(self):
        self.fixture()
        original = self.smoke_result()
        cases = [{"pass": True}]
        for field in original:
            incomplete = copy.deepcopy(original)
            del incomplete[field]
            cases.append(incomplete)
        for result in cases:
            with self.subTest(result=result):
                with self.assertRaises(RuntimeError):
                    self.verification.validate_smoke(result, self.acknowledgement())

    def test_every_required_check_including_native_ack_is_required(self):
        self.fixture()
        for check in verify.CHECKS:
            result = self.smoke_result()
            result["checks"].remove(check)
            with self.subTest(check=check), self.assertRaises(RuntimeError):
                self.verification.validate_smoke(result, self.acknowledgement())

    def test_invalid_tls_metrics_database_or_ack_rejected(self):
        self.fixture()
        cases = []
        for key, value in (("pass", 1), ("tls", "unverified"), ("database", {}), ("checks", verify.CHECKS + [verify.CHECKS[0]]), ("metrics", {})):
            result = self.smoke_result()
            result[key] = value
            cases.append(result)
        for total in (0, True, "1"):
            result = self.smoke_result()
            result["metrics"]["native"]["authority"]["put"]["total"] = total
            cases.append(result)
        result = self.smoke_result()
        result["metrics"]["errors_total"]["audit_unavailable"] = 1
        cases.append(result)
        result = self.smoke_result()
        result["metrics"]["native"]["workers"]["worker-b"] = {"status": "unavailable"}
        cases.append(result)
        for result in cases:
            with self.subTest(result=result), self.assertRaises(RuntimeError):
                self.verification.validate_smoke(result, self.acknowledgement())
        for acknowledgement in ("", '{"pass":true}', '{"pass":true,"checks":[],"credentials_logged":false}'):
            with self.subTest(acknowledgement=acknowledgement), self.assertRaises((RuntimeError, ValueError)):
                self.verification.validate_smoke(self.smoke_result(), acknowledgement)

    def test_manifest_and_all_required_hashes_gate_pass(self):
        self.eligible_run()
        self.assertTrue(self.verification.run())
        manifest = json.loads((self.verification.directory / "manifest.json").read_text())
        self.assertEqual(set(manifest["sha256"]), set(verify.REQUIRED_EVIDENCE))
        self.assertEqual(len(manifest["source_sha256"]), 4)
        for name, expected in manifest["sha256"].items():
            self.assertEqual(verify.digest(self.verification.directory / name), expected)
        self.assertTrue(json.loads((self.verification.directory / "result.json").read_text())["pass"])

    def test_missing_required_evidence_prevents_pass(self):
        self.eligible_run()
        (self.verification.directory / "smoke.json").unlink()
        self.assertFalse(self.verification.run())
        self.assertFalse(json.loads((self.verification.directory / "result.json").read_text())["pass"])

    def test_artifact_hash_failure_never_publishes_pass(self):
        self.eligible_run()
        with patch.object(verify, "digest", side_effect=OSError("hash failed")):
            self.assertFalse(self.verification.run())
        self.assertFalse(json.loads((self.verification.directory / "result.json").read_text())["pass"])

    def test_source_hash_failure_never_publishes_pass(self):
        self.eligible_run()
        original_digest = verify.digest
        def hashing(path):
            if path == verify.SMOKE:
                raise OSError("source hash failed")
            return original_digest(path)
        with patch.object(verify, "digest", side_effect=hashing):
            self.assertFalse(self.verification.run())
        self.assertFalse(json.loads((self.verification.directory / "result.json").read_text())["pass"])

    def test_manifest_write_failure_never_publishes_pass(self):
        self.eligible_run()
        original_write = verify.private_json
        def writing(path, value):
            if path.name == "manifest.json":
                self.assertFalse(json.loads((self.verification.directory / "result.json").read_text())["pass"])
                raise OSError("manifest write failed")
            return original_write(path, value)
        with patch.object(verify, "private_json", side_effect=writing):
            self.assertFalse(self.verification.run())
        self.assertFalse(json.loads((self.verification.directory / "result.json").read_text())["pass"])
        self.assertFalse((self.verification.directory / ".result.pending.json").exists())

    def test_final_result_publication_failure_leaves_false_result(self):
        self.eligible_run()
        with patch.object(verify.os, "replace", side_effect=OSError("write failed")):
            self.assertFalse(self.verification.run())
        self.assertFalse(json.loads((self.verification.directory / "result.json").read_text())["pass"])

    def test_evidence_hash_change_after_manifest_write_prevents_pass(self):
        self.eligible_run()
        original_digest = verify.digest
        def hashing(path):
            if path.name == "smoke.json" and (self.verification.directory / "manifest.json").exists():
                return "0" * 64
            return original_digest(path)
        with patch.object(verify, "digest", side_effect=hashing):
            self.assertFalse(self.verification.run())
        self.assertFalse(json.loads((self.verification.directory / "result.json").read_text())["pass"])

    def test_source_hash_change_after_manifest_write_prevents_pass(self):
        self.eligible_run()
        original_digest = verify.digest
        def hashing(path):
            if path == verify.SMOKE and (self.verification.directory / "manifest.json").exists():
                return "0" * 64
            return original_digest(path)
        with patch.object(verify, "digest", side_effect=hashing):
            self.assertFalse(self.verification.run())
        self.assertFalse(json.loads((self.verification.directory / "result.json").read_text())["pass"])

    def test_corrupt_manifest_readback_prevents_pass(self):
        self.eligible_run()
        original_write = verify.private_json
        def writing(path, value):
            return original_write(path, {} if path.name == "manifest.json" else value)
        with patch.object(verify, "private_json", side_effect=writing):
            self.assertFalse(self.verification.run())
        self.assertFalse(json.loads((self.verification.directory / "result.json").read_text())["pass"])

    def test_result_write_failure_cannot_publish_pass(self):
        self.eligible_run()
        with patch.object(verify, "private_json", side_effect=OSError("result write failed")):
            self.assertFalse(self.verification.run())
        self.assertFalse((self.verification.directory / "result.json").exists())

    def test_clean_stop_removes_only_owned_resources(self):
        self.stopped()
        self.verification.cleanup()
        self.verification.docker.assert_called_once_with("rm", "container-id")
        self.verification.remove_volume.assert_called_once()
        self.assertEqual(self.verification.report["cleanup"], "removed_owned_container")
        self.assertFalse(self.verification.fixture.exists())

    def test_uncertain_stop_retains_container_volume_and_fixture(self):
        for state in ({"ExitCode": 1}, {"ExitCode": 137}, {"OOMKilled": True}, {"Pid": 99}, {"Status": "created"}, {"Dead": True}):
            with self.subTest(state=state):
                self.stopped(state)
                self.verification.docker.reset_mock()
                self.verification.cleanup()
                self.verification.docker.assert_not_called()
                self.verification.remove_volume.assert_not_called()
                self.assertTrue(self.verification.fixture.exists())
                self.assertEqual(self.verification.report["cleanup"], "retained_unknown")

    def test_dirty_marker_retains_resources(self):
        self.stopped(marker=b"dirty\n")
        self.verification.cleanup()
        self.verification.docker.assert_not_called()
        self.verification.remove_volume.assert_not_called()

    def test_failed_smoke_retains_namespace_even_after_clean_stop(self):
        self.fixture()
        self.stopped()
        self.verification.prepare = Mock()
        self.verification.ready = Mock()
        self.verification.command.return_value = self.result("Unknown", 1, "original failure")
        self.assertFalse(self.verification.run())
        self.verification.command.assert_called_once()
        self.verification.docker.assert_not_called()
        self.verification.remove_volume.assert_not_called()
        self.assertEqual(self.verification.report["cleanup"], "retained_unknown")
        self.assertEqual(self.verification.report["sql_outcome"], "Unknown")
        self.assertTrue(self.verification.report["authority_clean"])
        self.assertTrue((self.verification.fixture / "connection.json").exists())

    def test_timed_out_smoke_sends_only_term_and_retains_namespace(self):
        self.fixture()
        self.stopped()
        self.verification.inspect.side_effect = [self.value(running=True), self.value()]
        self.verification.prepare = Mock()
        self.verification.ready = Mock()
        self.verification.command.side_effect = subprocess.TimeoutExpired("smoke", 1, output=b"Unknown dispatched")
        self.assertFalse(self.verification.run())
        self.verification.command.assert_called_once()
        self.assertEqual([call.args for call in self.verification.docker.call_args_list], [
            ("kill", "--signal", "TERM", "container-id"), ("wait", "container-id")])
        self.verification.remove_volume.assert_not_called()
        self.assertTrue(self.verification.fixture.exists())
        self.assertEqual(self.verification.report["sql_outcome"], "Unknown")
        self.assertEqual(self.verification.report["cleanup"], "retained_unknown")

    def test_stop_timeout_does_not_force_kill_or_remove(self):
        self.stopped({"Running": True, "Pid": 123, "Status": "running"})
        self.verification.docker.side_effect = [self.result(), subprocess.TimeoutExpired("wait", 1)]
        self.verification.cleanup()
        self.assertEqual(self.verification.docker.call_args_list[0].args, ("kill", "--signal", "TERM", "container-id"))
        self.assertEqual(self.verification.docker.call_count, 2)
        self.verification.remove_volume.assert_not_called()

    def test_foreign_volume_not_deleted(self):
        self.verification.docker.return_value = self.result(json.dumps([{"Name": self.verification.volume, "Labels": {}}]))
        with self.assertRaisesRegex(RuntimeError, "ownership"):
            self.verification.remove_volume()
        self.verification.docker.assert_called_once_with("volume", "inspect", self.verification.volume)

    def test_exception_still_cleans_and_manifest_excludes_fixture(self):
        self.fixture()
        self.stopped()
        self.verification.prepare = Mock(side_effect=KeyboardInterrupt())
        self.assertFalse(self.verification.run())
        self.assertEqual(self.verification.report["failure_type"], "KeyboardInterrupt")
        manifest = json.loads((self.verification.directory / "manifest.json").read_text())
        self.assertEqual(manifest["sha256"]["result.json"], verify.digest(self.verification.directory / "result.json"))
        self.assertNotIn("connection.json", manifest["sha256"])
        self.assertNotIn("project-secret", (self.verification.directory / "result.json").read_text())

    def test_s3_secrets_only_passed_by_environment_names(self):
        environment = {"S3_ENDPOINT": "https://s3.test", "S3_BUCKET": "dedicated", "S3_ACCESS_KEY_ID": "access-secret", "S3_SECRET_ACCESS_KEY": "s3-secret"}
        with patch.dict(verify.os.environ, environment, clear=True):
            self.arguments.backend = "s3"
            verification = verify.Verification(self.arguments)
        self.addCleanup(shutil.rmtree, verification.directory, True)
        image = {"Id": "sha256:image", "Os": "linux", "Config": {"User": "10001:10001"}}
        verification.docker = Mock(return_value=self.result())
        verification.docker.side_effect = [self.result(json.dumps([image])), self.result(), self.result(), self.result()]
        verification.inspect = Mock(return_value={"Image": "sha256:image", "Config": {"User": "10001:10001"}, "HostConfig": {}})
        verification.prepare()
        command = verification.docker.call_args_list[2].args
        self.assertIn("S3_SECRET_ACCESS_KEY", command)
        self.assertNotIn("s3-secret", command)
        self.assertEqual(verification.environment["S3_SECRET_ACCESS_KEY"], "s3-secret")
        self.assertNotIn("bucket", " ".join(command))


if __name__ == "__main__":
    unittest.main()
