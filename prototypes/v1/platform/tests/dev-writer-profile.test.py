import argparse
import hashlib
import importlib.util
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch


source = pathlib.Path(__file__).resolve().parents[1] / "dev.py"
spec = importlib.util.spec_from_file_location("peri_dev_writer_profile", source)
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)


class BeforeAPI(RuntimeError):
    pass


class WriterProfileTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="peri-test-writer-profile-")
        self.addCleanup(self.directory.cleanup)
        self.root = pathlib.Path(self.directory.name)
        self.assertEqual(self.root.stat().st_mode & 0o777, 0o700)
        self.environment = patch.dict(dev.os.environ, dev.safe_environment(), clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.builders = patch.object(dev.subprocess, "run", side_effect=AssertionError("builder forbidden"))
        self.builders.start()
        self.addCleanup(self.builders.stop)
        self.processes = patch.object(dev.subprocess, "Popen", side_effect=AssertionError("actual service forbidden"))
        self.processes.start()
        self.addCleanup(self.processes.stop)

    def project(self, enabled, backend="local", worker_max_request_bytes=512 * 1024, sql_max_duration_ms=5000):
        return dev.Project(argparse.Namespace(
            run_dir=str(self.root), no_build=True, backend=backend,
            listen="127.0.0.1:19443", tls=False, writer_admission=enabled,
            worker_max_request_bytes=worker_max_request_bytes,
            sql_max_duration_ms=sql_max_duration_ms,
        ))

    def parse_and_prepare(self, enabled):
        projects = []

        def prepare_only(project):
            project.prepare()
            projects.append(project)

        arguments = ["dev.py", "--no-build", "--run-dir", str(self.root), "--listen", "127.0.0.1:19443"]
        if enabled:
            arguments.append("--writer-admission")
        with patch.object(sys, "argv", arguments), patch.object(dev.Project, "run", prepare_only):
            self.assertEqual(dev.main(), 0)
        self.assertEqual(len(projects), 1)
        project = projects[0]
        self.assertIs(project.arguments.writer_admission, enabled)
        self.assertIs(project.launch["writer_admission"], enabled)
        self.assertIs(project.config["writer_admission"], enabled)
        for name in ("launch.json", "platform.json"):
            persisted = json.loads((self.root / name).read_text())
            self.assertIs(persisted["writer_admission"], enabled)
            self.assertEqual((self.root / name).stat().st_mode & 0o777, 0o600)
        reopened = self.project(enabled)
        with self.assertRaisesRegex(RuntimeError, "proof set incomplete"):
            reopened.prepare()
        self.assertIs(reopened.launch["writer_admission"], enabled)
        self.assertIs(reopened.config["writer_admission"], enabled)

    def test_cli_default_false_persists_and_reopens(self):
        self.parse_and_prepare(False)

    def test_cli_explicit_true_persists_and_reopens(self):
        self.parse_and_prepare(True)

    def test_worker_request_limit_persists_and_cannot_rebind(self):
        original = self.project(False, worker_max_request_bytes=16 * 1024 * 1024)
        original.prepare()
        self.assertEqual(original.launch["worker_max_request_bytes"], 16 * 1024 * 1024)
        with self.assertRaisesRegex(RuntimeError, "worker request limit cannot be silently rebound"):
            self.project(False).prepare()

    def test_sql_duration_limit_persists_and_cannot_rebind(self):
        original = self.project(False, sql_max_duration_ms=20000)
        original.prepare()
        self.assertEqual(original.launch["sql_max_duration_ms"], 20000)
        with self.assertRaisesRegex(RuntimeError, "SQL duration limit cannot be silently rebound"):
            self.project(False).prepare()

    def test_existing_run_directory_rejects_profile_rebind(self):
        original = self.project(False)
        original.prepare()
        before = {name: hashlib.sha256((self.root / name).read_bytes()).digest()
                  for name in ("launch.json", "platform.json")}
        with self.assertRaisesRegex(RuntimeError, "profile cannot be silently rebound"):
            self.project(True).prepare()
        after = {name: hashlib.sha256((self.root / name).read_bytes()).digest()
                 for name in before}
        self.assertEqual(before, after)

    def test_persisted_platform_profile_mismatch_is_rejected(self):
        project = self.project(True)
        project.prepare()
        project.config["writer_admission"] = False
        with (self.root / "platform.json").open("w") as output:
            json.dump(project.config, output)
        with self.assertRaisesRegex(RuntimeError, "configuration mismatch"):
            self.project(True).prepare()

    def test_worker_profile_matches_flag_and_s3_credentials_stay_authority_only(self):
        synthetic = {
            "S3_ENDPOINT": "http://127.0.0.1:19000", "S3_BUCKET": "synthetic-profile-only",
            "S3_ACCESS_KEY_ID": "synthetic-not-live-access", "S3_SECRET_ACCESS_KEY": "synthetic-not-live-secret",
            "S3_REGION": "us-east-1",
        }
        for enabled in (False, True):
            with self.subTest(enabled=enabled), tempfile.TemporaryDirectory(prefix="peri-test-profile-start-") as directory:
                self.assertEqual(pathlib.Path(directory).stat().st_mode & 0o777, 0o700)
                project = self.project(enabled, "s3")
                project.directory = pathlib.Path(directory)
                captured = {}

                def record_start(name, command, environment):
                    captured[name] = {
                        "profile": environment.get("PERI_WORKER_WRITER_PROFILE"),
                        "s3_names": {key for key in environment if key.startswith("S3_")},
                        "command": command[1],
                    }
                    return {"ready": True}

                with patch.dict(dev.os.environ, {**synthetic, "PERI_WORKER_WRITER_PROFILE": "0" if enabled else "1"}), \
                        patch.object(project, "start", side_effect=record_start) as starts, \
                        patch.object(dev, "request", side_effect=BeforeAPI("stop before network dispatch")) as network:
                    with self.assertRaises(BeforeAPI):
                        project.run()
                self.assertEqual(starts.call_count, 4)
                network.assert_called_once()
                self.assertEqual(set(captured), {"authority", "platform", "worker-a", "worker-b"})
                self.assertEqual(captured["authority"]["s3_names"], set(synthetic))
                self.assertEqual(captured["platform"]["s3_names"], set())
                self.assertIsNone(captured["platform"]["profile"])
                for worker in ("worker-a", "worker-b"):
                    self.assertEqual(captured[worker]["command"], "owned-worker")
                    self.assertEqual(captured[worker]["profile"], "1" if enabled else "0")
                    self.assertEqual(captured[worker]["s3_names"], set())
                self.assertIs(project.config["writer_admission"], enabled)
                self.assertIs(project.launch["writer_admission"], enabled)


if __name__ == "__main__":
    unittest.main()
