import contextlib
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import build_native
import finalize_evidence


class BuildNativeTests(unittest.TestCase):
    def test_default_command_is_release_and_locked(self):
        command = build_native.build_command(build_native.ROOT)
        self.assertIn("--release", command)
        self.assertNotIn("--offline", command)
        self.assertIn("--locked", command)
        self.assertIn(str(build_native.ROOT / "prototypes/v1/Cargo.toml"), command)

    def test_offline_and_debug_are_explicit_opt_ins(self):
        command = build_native.build_command(build_native.ROOT, release=False, offline=True)
        self.assertNotIn("--release", command)
        self.assertIn("--offline", command)

    def test_build_environment_isolates_target_directory(self):
        environment = build_native.build_environment(Path("/var/tmp/perf-target"))
        self.assertEqual(environment["CARGO_TARGET_DIR"], "/var/tmp/perf-target")

    def test_missing_toolchain_fails_closed_before_build(self):
        with patch.object(build_native.shutil, "which", return_value=None), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            code = build_native.main([])
        self.assertEqual(code, 1)
        payload = json.loads(output.getvalue())
        self.assertFalse(payload["pass"])
        self.assertIn("release_toolchain_unavailable", payload["failure"])

    def test_no_implicit_180_second_cache_window(self):
        source = Path(build_native.__file__).read_text()
        self.assertNotIn("180", source)
        self.assertNotIn("offline cached debug build", source)


class FinalizeEvidenceTests(unittest.TestCase):
    def test_arguments_are_explicit_and_required(self):
        with contextlib.redirect_stderr(io.StringIO()) as error:
            with self.assertRaises(SystemExit):
                finalize_evidence.parse_arguments([])
        text = error.getvalue()
        for flag in ("--container", "--date", "--evidence-root", "--output-directory"):
            self.assertIn(flag, text)

    def test_date_is_validated(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                finalize_evidence.parse_arguments(
                    ["--container", "c", "--date", "2026-10-09",
                     "--evidence-root", "/e", "--output-directory", "/o"])

    def test_paths_are_parameterized_and_not_hardcoded(self):
        self.assertEqual(finalize_evidence.result_path("/e", "name", "20260101"),
                         Path("/e/peri-perf-name-20260101.json"))
        source = Path(finalize_evidence.__file__).read_text()
        self.assertNotIn("20261009", source)
        self.assertNotIn("1Panel-rustfs-TcP9", source)
        self.assertNotIn("/private/tmp", source)

    def test_secret_scan_uses_explicit_container(self):
        calls = []

        def runner(command, **kwargs):
            calls.append(command)
            return json.dumps([{"Config": {"Env": [
                "RUSTFS_ACCESS_KEY=alpha", "RUSTFS_SECRET_KEY=beta"]}}])

        secrets = finalize_evidence.container_secret_bytes("explicit-container", runner=runner)
        self.assertEqual(calls, [["docker", "inspect", "explicit-container"]])
        self.assertEqual(secrets, [b"alpha", b"beta"])

    def test_missing_credentials_error_does_not_echo_values(self):
        def runner(command, **kwargs):
            return json.dumps([{"Config": {"Env": ["UNRELATED=value"]}}])

        with self.assertRaises(finalize_evidence.FinalizeError) as failure:
            finalize_evidence.container_secret_bytes("c", runner=runner)
        self.assertIn("rustfs_credentials_missing", str(failure.exception))
        self.assertNotIn("value", str(failure.exception))

    def test_output_inside_scripts_is_rejected_without_docker(self):
        arguments = finalize_evidence.parse_arguments(
            ["--container", "c", "--date", "20260101", "--evidence-root", "/e",
             "--output-directory",
             str(finalize_evidence.ROOT / "scripts/perf-reliability/forbidden-output")])
        with self.assertRaises(finalize_evidence.FinalizeError) as failure:
            finalize_evidence.run(arguments)
        self.assertIn("output_directory_inside_scripts", str(failure.exception))


if __name__ == "__main__":
    unittest.main()
