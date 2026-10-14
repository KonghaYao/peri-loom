import argparse
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
    def project(self, directory):
        return dev.Project(argparse.Namespace(run_dir=directory, no_build=True, backend="local", listen="127.0.0.1:19443", tls=False, writer_admission=False))

    def test_already_exited_uncertain_platform_is_not_clean(self):
        with tempfile.TemporaryDirectory(prefix="peri-test-launcher-") as directory:
            project = self.project(directory)
            process = Mock(returncode=1)
            process.poll.return_value = 1
            project.processes["platform"] = process
            with self.assertRaisesRegex(RuntimeError, "clean proof"):
                project.cleanup()
            process.terminate.assert_not_called()

    def test_joined_uncertain_platform_is_not_clean(self):
        with tempfile.TemporaryDirectory(prefix="peri-test-launcher-") as directory:
            project = self.project(directory)
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
                project = self.project(directory)
                process = Mock(returncode=1)
                process.poll.return_value = 1
                project.processes[identity] = process
                with self.assertRaisesRegex(RuntimeError, "clean proof"):
                    project.cleanup()

    def test_clean_platform_has_joined_exit_proof(self):
        with tempfile.TemporaryDirectory(prefix="peri-test-launcher-") as directory:
            project = self.project(directory)
            process = Mock()
            process.poll.return_value = None
            process.wait.return_value = 0
            project.processes["platform"] = process
            project.cleanup()
            process.wait.assert_called_once_with(timeout=15)

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


if __name__ == "__main__":
    unittest.main()
