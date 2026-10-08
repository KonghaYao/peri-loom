import argparse
import importlib.util
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location("peri_dev_sql_observer", pathlib.Path(__file__).with_name("dev.py"))
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)


class ObserverTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="peri-test-open-observer-")
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)
        self.builders = patch.object(dev.subprocess, "run", side_effect=AssertionError("builder forbidden"))
        self.builders.start()
        self.addCleanup(self.builders.stop)
        self.processes = patch.object(dev.subprocess, "Popen", side_effect=AssertionError("actual service forbidden"))
        self.processes.start()
        self.addCleanup(self.processes.stop)

    def project(self, enabled, no_build=True):
        return dev.Project(argparse.Namespace(
            run_dir=str(self.root / "runtime"), no_build=no_build, backend="local",
            listen="127.0.0.1:19443", tls=False, writer_admission=False,
            sql_open_observer=enabled,
        ))

    def test_existing_marker_rejected_before_build_or_directory_creation(self):
        project = self.project(True, no_build=False)
        project.directory.mkdir(mode=0o700)
        marker = project.directory / "launch.json"
        marker.write_text("preserved")
        with self.assertRaisesRegex(RuntimeError, "fresh empty runtime"):
            project.prepare()
        self.assertEqual(marker.read_text(), "preserved")
        self.assertEqual(list(project.directory.iterdir()), [marker])

    def test_default_off_can_reopen_without_observation_file(self):
        project = self.project(False)
        project.prepare()
        self.assertIs(project.launch["sql_open_observer"], False)
        project.prepare()
        self.assertFalse((project.directory / "platform" / "sql-open-observation.jsonl").exists())

    def test_fresh_enabled_pins_and_cannot_reopen(self):
        project = self.project(True)
        project.prepare()
        persisted = json.loads((project.directory / "launch.json").read_text())
        self.assertIs(persisted["sql_open_observer"], True)
        with self.assertRaisesRegex(RuntimeError, "fresh empty runtime"):
            project.prepare()
        with self.assertRaisesRegex(RuntimeError, "observer profile cannot be silently rebound"):
            self.project(False).prepare()

    def test_inherited_observer_environment_not_forwarded(self):
        with patch.dict(dev.os.environ, {"PERI_SQL_OPEN_OBSERVER": "1"}):
            self.assertNotIn("PERI_SQL_OPEN_OBSERVER", dev.safe_environment())

    def test_only_platform_child_receives_explicit_literal(self):
        for enabled in (False, True):
            with self.subTest(enabled=enabled):
                project = self.project(enabled)
                project.directory = self.root / ("enabled" if enabled else "disabled")
                project.arguments.run_dir = str(project.directory)
                children = {}

                def record_start(name, command, environment):
                    children[name] = dict(environment)
                    return {}

                with patch.dict(dev.os.environ, {"PERI_SQL_OPEN_OBSERVER": "inherited-invalid"}), patch.object(project, "start", side_effect=record_start), patch.object(dev, "request", side_effect=RuntimeError("offline stop before API")):
                    with self.assertRaisesRegex(RuntimeError, "offline stop before API"):
                        project.run()
                self.assertEqual(children["platform"]["PERI_SQL_OPEN_OBSERVER"], "1" if enabled else "0")
                for name, environment in children.items():
                    if name != "platform":
                        self.assertNotIn("PERI_SQL_OPEN_OBSERVER", environment)

    def test_fresh_symlink_is_rejected_before_build(self):
        target = self.root / "empty-target"
        target.mkdir(mode=0o700)
        project = self.project(True, no_build=False)
        project.directory.symlink_to(target, target_is_directory=True)
        project = self.project(True, no_build=False)
        with self.assertRaisesRegex(RuntimeError, "fresh empty runtime"):
            project.prepare()
        self.assertEqual(list(target.iterdir()), [])

    def test_cli_flag_pins_fresh_launch(self):
        directory = self.root / "cli-runtime"
        arguments = ["dev.py", "--no-build", "--run-dir", str(directory), "--sql-open-observer"]
        with patch.object(sys, "argv", arguments), patch.object(dev.Project, "run", lambda project: project.prepare()):
            self.assertEqual(dev.main(), 0)
        self.assertIs(json.loads((directory / "launch.json").read_text())["sql_open_observer"], True)


if __name__ == "__main__":
    unittest.main()
