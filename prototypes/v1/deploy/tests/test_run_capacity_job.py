import argparse
import datetime
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import unittest
from unittest.mock import patch


DEPLOY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DEPLOY.parent / "stress"))
from capacity_plan import digest, write_private


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


runner = load("run_capacity_job", DEPLOY / "run-capacity-job.py")
fixtures = load("capacity_matrix_fixtures", DEPLOY / "tests" / "test_capacity_matrix.py")


class OwnedProcess:
    def __init__(self, harness, role, pid):
        self.harness, self.role, self.pid = harness, role, pid
        self.code = None
        self.started = harness.clock

    def poll(self):
        harness = self.harness
        if self.code is not None:
            return self.code
        if self.role in ("monitor", "collector"):
            if self.role == harness.early_role and ("guard" in harness.processes or harness.before_load):
                self.code = harness.early_code
            elif (harness.output / (self.role + "-stop.json")).exists() and self.role not in harness.stuck:
                if self.role == "collector":
                    harness.final_capture()
                else:
                    if harness.on_monitor_stop is not None:
                        harness.on_monitor_stop()
                    harness.events.append("monitor_stopped")
                self.code = harness.codes.get(self.role, 0)
        elif self.role == "guard":
            if harness.clock - self.started >= 0.1 and "guard" not in harness.stuck:
                self.code = harness.codes.get("guard", 0)
        elif self.role not in harness.stuck:
            self.code = harness.codes.get(self.role, 0)
        return self.code

    def wait(self, timeout):
        self.harness.waits.append((self.role, timeout))
        code = self.poll()
        if code is None:
            raise subprocess.TimeoutExpired(self.role, timeout)
        return code


class OrchestratorTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.PlanTests("test_freeze_is_offline_complete_private_and_guard_compatible")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.private = self.fixture.root
        launch_path = Path(self.fixture.config["launcher_report"])
        launch = json.loads(launch_path.read_text())
        launch.update(owner="f" * 32, container_id="b" * 64, source_hash="c" * 64,
                      container_name="peri-verify-" + "f" * 32)
        launch_path.write_text(json.dumps(launch))
        self.config = self.private / "monitor.json"
        write_private(self.config, launch)
        self.script = self.private / "monitor.py"
        self.script.write_text("raise AssertionError('mock must never execute')\n")
        self.script.chmod(0o600)
        self.fixture.config["guard"]["sources"].update({
            str(path): digest(path) for path in (self.config, self.script, DEPLOY / "run-capacity-job.py")
        })
        missing = {
            "test_missing_frozen_monitor_config_blocks_load": self.config,
            "test_missing_frozen_monitor_script_blocks_load": self.script,
            "test_missing_frozen_orchestrator_blocks_load": DEPLOY / "run-capacity-job.py",
        }.get(self._testMethodName)
        if missing is not None:
            self.fixture.config["guard"]["sources"].pop(str(missing))
        self.fixture.config_path.unlink()
        write_private(self.fixture.config_path, self.fixture.config)
        self.plan = self.fixture.frozen()
        self.job = self.plan["jobs"][0]
        self.output = Path(self.job["output"])
        self.root = Path(self.plan["config"]["output"])
        self.arguments = argparse.Namespace(plan=self.fixture.plan_path, job=self.job["id"],
                                            monitor_config=self.config, monitor_script=self.script)
        self.clock = 0
        self.wall = datetime.datetime(2026, 10, 8, tzinfo=datetime.timezone.utc).timestamp()
        self.processes, self.events, self.waits, self.commands = {}, [], [], []
        self.codes, self.stuck, self.unkillable = {}, set(), set()
        self.group_survives_term, self.group_killed = set(), set()
        self.early_role, self.early_code, self.before_load = None, 0, False
        self.sample_tick, self.ready_tick = 1, 3
        self.ticks = 0
        self.on_sleep = None
        self.on_monitor_stop = None
        self.on_final_capture = None
        self.monitor_updates = self.collector_updates = True
        self.addCleanup(patch.stopall)
        patch.object(runner.subprocess, "Popen", side_effect=self.spawn).start()
        patch.object(runner.subprocess, "run", side_effect=AssertionError("no Docker, SQL or real load")).start()
        patch.object(runner.os, "killpg", side_effect=self.killpg).start()
        patch.object(runner.time, "sleep", side_effect=self.sleep).start()
        patch.object(runner.time, "monotonic", side_effect=lambda: self.clock).start()
        patch.object(runner.time, "time", side_effect=lambda: self.wall + self.clock).start()

    def sample(self, **changes):
        value = {"schema_version": 1, "source": "container.proc",
                 "observed_at": datetime.datetime.fromtimestamp(self.wall + self.clock, datetime.timezone.utc).isoformat(),
                 "container_id": "b" * 64, "container_name": "peri-verify-" + "f" * 32,
                 "image_id": "sha256:" + "a" * 64, "source_hash": "c" * 64,
                 "running": True, "oom_killed": False, "exit_code": None, "boot_id": "mock-boot",
                 "page_size_bytes": 4096, "clock_ticks_per_second": 100,
                 "processes": [{"role": role, "pid": index + 10, "start_ticks": 1, "rss_pages": 1,
                                "cpu_ticks": 1, "state": "S", "uids": [10001] * 4}
                               for index, role in enumerate(["authority", "worker-a", "worker-b", "platform", "launcher"])]}
        value.update(changes)
        path = self.output / "container-resources.jsonl"
        first = not path.exists()
        with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600), "w") as stream:
            stream.write(json.dumps(value) + "\n")
        if first:
            self.events.append("fresh_sample")

    def ready(self, **changes):
        value = {"status": "READY READ ONLY NOT MEASURED", "job": self.job["id"],
                 "plan_sha256": digest(self.arguments.plan)}
        value.update(changes)
        write_private(self.output / "collector-ready.json", value)
        self.poll_record()
        self.events.append("collector_ready")

    def poll_record(self, finishing=False):
        with os.fdopen(os.open(self.output / "capacity-trace.jsonl", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600), "w") as stream:
            stream.write(json.dumps({"type": "collector_poll", "job": self.job["id"],
                                    "plan_sha256": digest(self.arguments.plan), "finishing": finishing,
                                    "observed_at": datetime.datetime.fromtimestamp(self.wall + self.clock, datetime.timezone.utc).isoformat()}) + "\n")

    def sleep(self, interval):
        self.clock += interval
        self.ticks += 1
        if self.ticks == self.sample_tick:
            self.sample()
        if self.ticks == self.ready_tick:
            self.ready()
        elif self.collector_updates and (self.output / "collector-ready.json").exists() and "collector" in self.processes and self.processes["collector"].code is None:
            self.poll_record()
        if self.ticks != self.sample_tick and self.monitor_updates and self.ticks % 20 == 0 and (self.output / "container-resources.jsonl").exists() and self.processes["monitor"].code is None:
            self.sample()
        if self.on_sleep is not None:
            self.on_sleep()

    def final_capture(self):
        if (self.output / "collector-status.json").exists():
            return
        if self.on_final_capture is not None:
            self.on_final_capture()
        trace = self.output / "capacity-trace.jsonl"
        self.poll_record(finishing=True)
        write_private(self.output / "collector-status.json", {"status": "COMPLETE", "job": self.job["id"],
                      "plan_sha256": digest(self.arguments.plan), "journal_sha256": digest(trace)})
        self.events.append("collector_final_capture")

    def spawn(self, command, **options):
        self.assertTrue(options["start_new_session"])
        self.assertEqual(options["stdout"], subprocess.DEVNULL)
        self.assertEqual(options["stderr"], subprocess.DEVNULL)
        self.assertNotIn("api_key", options["env"])
        self.commands.append(command)
        if Path(command[1]) == self.script:
            role = "monitor"
            self.assertEqual(command[2:], ["--config", str(self.config), "--output",
                             str(self.output / "container-resources.jsonl"), "--stop-file", str(self.output / "monitor-stop.json")])
        elif "--capture" in command:
            role = "collector"
            self.assertIn("fresh_sample", self.events)
        elif "--spec" in command:
            role = "guard"
            self.assertEqual(command[1:], [str(runner.STRESS / "run_guarded.py"), "--spec", self.job["spec"]])
            self.assertIn("fresh_sample", self.events)
            self.assertIn("collector_ready", self.events)
        elif "--finalize" in command:
            role = "finalize"
            self.assertEqual(self.processes["monitor"].poll(), 0)
            self.assertEqual(self.processes["collector"].poll(), 0)
            self.assertIn("monitor_stopped", self.events)
            self.assertIn("collector_final_capture", self.events)
            self.frozen_raw = (self.output / "container-resources.jsonl").read_bytes()
            write_private(self.output / "capacity-observations.json", {"mock": True,
                          "resource_sha256": digest(self.output / "container-resources.jsonl")})
        elif "--verify-job" in command:
            role = "verify"
            self.assertEqual(self.processes["monitor"].poll(), 0)
            self.assertEqual(self.processes["collector"].poll(), 0)
            self.assertEqual((self.output / "container-resources.jsonl").read_bytes(), self.frozen_raw)
            self.assertEqual(json.loads((self.output / "capacity-observations.json").read_text())["resource_sha256"],
                             digest(self.output / "container-resources.jsonl"))
            manifest = {str(path): {"sha256": digest(path), "bytes": path.stat().st_size}
                        for path in self.output.iterdir() if path.name in
                        {"guard-spec.json", "capacity-trace.jsonl", "collector-status.json", "collector-ready.json",
                         "capacity-observations.json", "container-resources.jsonl"}}
            write_private(self.output / "capacity-verification.json", {"status": "MEASURED", "job": self.job["id"],
                          "spec_sha256": self.job["spec_sha256"], "evidence_manifest": manifest})
        else:
            self.fail("unexpected process")
        self.events.append("spawn_" + role)
        child = OwnedProcess(self, role, 1000 + len(self.processes))
        self.processes[role] = child
        return child

    def killpg(self, pid, selected):
        matches = [child for child in self.processes.values() if child.pid == pid]
        self.assertEqual(len(matches), 1)
        child = matches[0]
        if selected == 0:
            if child.poll() is not None and (child.role not in self.group_survives_term or child.role in self.group_killed):
                raise ProcessLookupError()
            return
        self.events.append((child.role, selected))
        if selected == signal.SIGKILL and child.role not in self.unkillable:
            self.group_killed.add(child.role)
        if child.code is None and child.role not in self.unkillable and (selected == signal.SIGKILL or child.role not in self.stuck):
            child.code = -selected

    def run_job(self):
        self.orchestrator = runner.Orchestrator(self.arguments)
        return self.orchestrator.run()

    def assert_hold(self, code, result):
        self.assertEqual(code, 1)
        self.assertEqual(result["status"], "HOLD")
        self.assertTrue(result["may_have_unknown"])
        self.assertFalse(result["performance_pass"])
        self.assertTrue((self.root / "matrix-stop.json").exists())

    def test_ready_gating_original_guard_and_stop_order(self):
        code, result = self.run_job()
        self.assertEqual(code, 0)
        self.assertEqual(result["status"], "MEASURED NOT ACCEPTED")
        self.assertEqual([event for event in self.events if isinstance(event, str)],
                         ["spawn_monitor", "fresh_sample", "spawn_collector", "collector_ready", "spawn_guard",
                          "collector_final_capture", "monitor_stopped", "spawn_finalize", "spawn_verify"])
        self.assertEqual(result["owned_exits"], dict.fromkeys(["guard", "collector", "monitor", "finalize", "verify"], 0))
        self.assertTrue(all(timeout == self.orchestrator.grace for _, timeout in self.waits))
        self.assertFalse((self.root / ".matrix.lock").exists())
        self.assertTrue((self.output / ".capacity-job.lock").exists())
        for path in self.output.iterdir():
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def assert_missing_frozen_source_blocks_load(self):
        code, result = self.run_job()
        self.assert_hold(code, result)
        self.assertEqual(self.commands, [])
        self.assertEqual(result["reason"], "frozen_execution_and_sidecar_sources_required")

    def test_missing_frozen_monitor_config_blocks_load(self):
        self.assert_missing_frozen_source_blocks_load()

    def test_missing_frozen_monitor_script_blocks_load(self):
        self.assert_missing_frozen_source_blocks_load()

    def test_missing_frozen_orchestrator_blocks_load(self):
        self.assert_missing_frozen_source_blocks_load()

    def test_repeated_same_fresh_sample_is_valid(self):
        self.ready_tick = 8
        self.assertEqual(self.run_job()[0], 0)

    def test_monitor_before_sample_early_zero_exit_never_loads(self):
        self.early_role, self.before_load = "monitor", True
        self.assert_hold(*self.run_job())
        self.assertEqual(list(self.processes), ["monitor"])

    def test_collector_before_ready_early_zero_exit_never_loads(self):
        self.early_role, self.before_load = "collector", True
        self.assert_hold(*self.run_job())
        self.assertNotIn("guard", self.processes)

    def test_monitor_early_exit_stops_owned_guard_group(self):
        self.early_role = "monitor"
        self.stuck.add("guard")
        self.assert_hold(*self.run_job())
        self.assertIn(("guard", signal.SIGTERM), self.events)
        self.assertIn(("guard", signal.SIGKILL), self.events)
        self.assertTrue((self.output / "collector-stop.json").exists())

    def test_collector_early_exit_stops_guard_before_monitor(self):
        self.early_role, self.early_code = "collector", 7
        self.stuck.add("guard")
        self.assert_hold(*self.run_job())
        self.assertLess(self.events.index(("guard", signal.SIGTERM)), self.events.index("monitor_stopped"))
        self.assertNotIn("spawn_finalize", self.events)

    def test_guard_nonzero_still_drains_collector_before_monitor(self):
        self.codes["guard"] = 9
        self.assert_hold(*self.run_job())
        self.assertLess(self.events.index("collector_final_capture"), self.events.index("monitor_stopped"))
        self.assertNotIn("spawn_finalize", self.events)

    def test_collector_nonzero_blocks_finalize(self):
        self.codes["collector"] = 4
        self.assert_hold(*self.run_job())
        self.assertNotIn("spawn_finalize", self.events)

    def test_monitor_nonzero_blocks_finalize_and_verify(self):
        self.codes["monitor"] = 6
        self.assert_hold(*self.run_job())
        self.assertNotIn("spawn_finalize", self.events)
        self.assertNotIn("spawn_verify", self.events)

    def test_finalize_nonzero_blocks_verify(self):
        self.codes["finalize"] = 8
        self.assert_hold(*self.run_job())
        self.assertNotIn("spawn_verify", self.events)

    def test_verify_nonzero_stops_matrix(self):
        self.codes["verify"] = 5
        code, result = self.run_job()
        self.assertEqual(code, 1)
        self.assertEqual(result["status"], "HOLD")
        self.assertTrue((self.root / "matrix-stop.json").exists())

    def test_bounded_collector_join_term_kill_before_monitor_stop(self):
        self.stuck.add("collector")
        self.assert_hold(*self.run_job())
        self.assertIn(("collector", signal.SIGTERM), self.events)
        self.assertIn(("collector", signal.SIGKILL), self.events)
        self.assertLess(self.events.index(("collector", signal.SIGKILL)), self.events.index("monitor_stopped"))
        self.assertLess(self.clock, 2 * self.orchestrator.grace + 1)

    def test_bounded_monitor_join_blocks_finalize_and_verify(self):
        self.stuck.add("monitor")
        self.assert_hold(*self.run_job())
        self.assertIn(("monitor", signal.SIGKILL), self.events)
        self.assertNotIn("spawn_finalize", self.events)
        self.assertNotIn("spawn_verify", self.events)

    def test_unkillable_owned_guard_is_retained_unknown_without_retry(self):
        self.early_role = "collector"
        self.stuck.add("guard")
        self.unkillable.add("guard")
        code, result = self.run_job()
        self.assert_hold(code, result)
        self.assertEqual(result["retained_owned_processes"], [{"role": "guard", "pid": self.processes["guard"].pid}])
        self.assertEqual(sum("--spec" in command for command in self.commands), 1)

    def test_existing_raw_is_preserved_and_never_runs(self):
        raw = self.output / "run_original"
        raw.mkdir(mode=0o700)
        write_private(raw / "run.json", {"unknown": 1})
        original = (raw / "run.json").read_bytes()
        self.assert_hold(*self.run_job())
        self.assertEqual(self.commands, [])
        self.assertEqual((raw / "run.json").read_bytes(), original)

    def test_second_run_rejected_and_status_never_overwritten(self):
        self.assertEqual(self.run_job()[0], 0)
        status = (self.output / "orchestrator-status.json").read_bytes()
        original = {path: path.read_bytes() for path in self.output.iterdir()}
        commands = len(self.commands)
        self.assertEqual(self.run_job()[0], 1)
        self.assertEqual(len(self.commands), commands)
        self.assertEqual((self.output / "orchestrator-status.json").read_bytes(), status)
        for path, body in original.items():
            self.assertEqual(path.read_bytes(), body)

    def test_first_matrix_stop_never_overwritten(self):
        write_private(self.root / "matrix-stop.json", {"reason": "first"})
        original = (self.root / "matrix-stop.json").read_bytes()
        self.assert_hold(*self.run_job())
        self.assertEqual((self.root / "matrix-stop.json").read_bytes(), original)
        self.assertEqual(self.commands, [])

    def test_matrix_lock_exclusive_and_not_removed_if_unowned(self):
        write_private(self.root / ".matrix.lock", {"owner": "other"})
        original = (self.root / ".matrix.lock").read_bytes()
        self.assert_hold(*self.run_job())
        self.assertEqual((self.root / ".matrix.lock").read_bytes(), original)
        self.assertEqual(self.commands, [])

    def test_monitor_config_binding_required(self):
        config = json.loads(self.config.read_text())
        config["container_id"] = "d" * 64
        self.config.write_text(json.dumps(config))
        code, result = self.run_job()
        self.assertEqual(code, 1)
        self.assertEqual(result["status"], "HOLD")
        self.assertEqual(result["reason"], "private_orchestration_error_details_withheld")
        self.assertEqual(self.commands, [])

    def test_wrong_private_script_mode_rejected(self):
        self.script.chmod(0o644)
        code, result = self.run_job()
        self.assertEqual(code, 1)
        self.assertEqual(result["status"], "HOLD")
        self.assertEqual(self.commands, [])

    def test_symlink_private_script_rejected(self):
        alias = self.private / "alias.py"
        alias.symlink_to(self.script)
        self.arguments.monitor_script = alias
        self.assertEqual(self.run_job()[0], 1)
        self.assertEqual(self.commands, [])

    def test_wrong_owner_rejected(self):
        with patch.object(runner.os, "getuid", return_value=os.getuid() + 1):
            self.assertEqual(self.run_job()[0], 1)
        self.assertEqual(self.commands, [])

    def test_changed_frozen_monitor_source_blocks_load(self):
        def mutate():
            if self.ticks == 2:
                self.script.write_text("changed source\n")
        self.on_sleep = mutate
        self.assert_hold(*self.run_job())
        self.assertNotIn("guard", self.processes)

    def test_changed_plan_source_is_rejected_before_spawn(self):
        path = self.private / "deployment.json"
        path.write_text('{"cpu":999}')
        self.assertEqual(self.run_job()[0], 1)
        self.assertEqual(self.commands, [])

    def test_wrong_ready_job_never_loads(self):
        self.ready_tick = -1
        def invalid_ready():
            if self.ticks == 3:
                self.ready(job="other-job")
        self.on_sleep = invalid_ready
        self.assert_hold(*self.run_job())
        self.assertNotIn("guard", self.processes)

    def test_stale_sample_never_starts_collector(self):
        self.sample_tick = -1
        def stale():
            if self.ticks == 1:
                self.sample(observed_at="2020-01-01T00:00:00Z")
        self.on_sleep = stale
        self.assert_hold(*self.run_job())
        self.assertNotIn("collector", self.processes)

    def test_sample_wrong_container_never_loads(self):
        self.sample_tick = -1
        def invalid():
            if self.ticks == 1:
                self.sample(container_id="d" * 64)
        self.on_sleep = invalid
        self.assert_hold(*self.run_job())
        self.assertNotIn("guard", self.processes)

    def test_operator_stop_terminates_only_owned_processes(self):
        self.stuck.add("guard")
        def interrupt():
            if "guard" in self.processes:
                self.orchestrator.interrupted = True
        self.on_sleep = interrupt
        self.assert_hold(*self.run_job())
        self.assertIn(("guard", signal.SIGTERM), self.events)
        self.assertIn(("guard", signal.SIGKILL), self.events)
        self.assertLess(self.events.index("collector_final_capture"), self.events.index("monitor_stopped"))

    def test_private_exception_details_are_not_emitted(self):
        with patch.object(runner, "read_plan", side_effect=ValueError("secret-credential-value")):
            code, result = self.run_job()
        self.assertEqual(code, 1)
        self.assertNotIn("secret-credential-value", json.dumps(result))

    def test_alive_monitor_stalled_samples_stop_owned_guard(self):
        self.monitor_updates = False
        self.stuck.add("guard")
        self.assert_hold(*self.run_job())
        self.assertIn(("guard", signal.SIGTERM), self.events)
        self.assertIn(("guard", signal.SIGKILL), self.events)
        self.assertEqual(self.orchestrator.exits["monitor"], 0)

    def test_alive_collector_stalled_polls_stop_owned_guard(self):
        self.collector_updates = False
        self.stuck.add("guard")
        code, result = self.run_job()
        self.assert_hold(code, result)
        self.assertEqual(result["reason"], "collector_poll_not_fresh")
        self.assertIn(("guard", signal.SIGTERM), self.events)
        self.assertIn(("guard", signal.SIGKILL), self.events)

    def test_monitor_midstream_invalid_sample_cannot_hide_behind_good_last_row(self):
        self.stuck.add("guard")
        def change():
            if "guard" in self.processes and not hasattr(self, "mutated"):
                self.mutated = True
                self.sample(source_hash="d" * 64)
                self.clock += 0.001
                self.sample()
        self.on_sleep = change
        self.assert_hold(*self.run_job())
        self.assertIn(("guard", signal.SIGTERM), self.events)

    def test_monitor_original_prefix_rewrite_stops_guard(self):
        self.stuck.add("guard")
        def change():
            if "guard" in self.processes:
                path = self.output / "container-resources.jsonl"
                path.write_bytes(path.read_bytes().replace(b"mock-boot", b"otherboot"))
        self.on_sleep = change
        code, result = self.run_job()
        self.assert_hold(code, result)
        self.assertEqual(result["reason"], "original_journal_prefix_changed")

    def test_collector_wrong_job_midstream_stops_guard(self):
        self.stuck.add("guard")
        def change():
            if "guard" in self.processes:
                with (self.output / "capacity-trace.jsonl").open("a") as stream:
                    stream.write(json.dumps({"type": "collector_poll", "job": "wrong"}) + "\n")
        self.on_sleep = change
        code, result = self.run_job()
        self.assert_hold(code, result)
        self.assertEqual(result["reason"], "collector_poll_binding_invalid")

    def test_existing_startup_budget_without_monitor_sample_fails_closed(self):
        self.sample_tick = self.ready_tick = -1
        self.on_sleep = lambda: setattr(self, "clock", self.clock + self.plan["config"]["guard"]["hard_max_seconds"])
        code, result = self.run_job()
        self.assert_hold(code, result)
        self.assertEqual(result["reason"], "original_startup_budget_exhausted")
        self.assertNotIn("guard", self.processes)

    def test_existing_startup_budget_without_collector_ready_fails_closed(self):
        self.ready_tick = -1
        def exhaust():
            if "collector" in self.processes:
                self.clock += self.plan["config"]["guard"]["hard_max_seconds"]
        self.on_sleep = exhaust
        code, result = self.run_job()
        self.assert_hold(code, result)
        self.assertEqual(result["reason"], "original_startup_budget_exhausted")
        self.assertNotIn("guard", self.processes)

    def test_finalize_bounded_join_after_sidecars_stop(self):
        self.stuck.add("finalize")
        code, result = self.run_job()
        self.assert_hold(code, result)
        self.assertEqual(result["reason"], "finalize_join_not_complete")
        self.assertIn(("finalize", signal.SIGTERM), self.events)
        self.assertNotIn("spawn_verify", self.events)
        self.assertEqual((self.output / "container-resources.jsonl").read_bytes(), self.frozen_raw)

    def test_verify_bounded_join_after_sidecars_stop(self):
        self.stuck.add("verify")
        code, result = self.run_job()
        self.assert_hold(code, result)
        self.assertEqual(result["reason"], "verify_join_not_complete")
        self.assertIn(("verify", signal.SIGTERM), self.events)
        self.assertEqual((self.output / "container-resources.jsonl").read_bytes(), self.frozen_raw)

    def test_matrix_stop_write_failure_cannot_skip_cleanup(self):
        self.early_role = "collector"
        self.stuck.add("guard")
        original = runner.write_private
        def denied(path, value):
            if Path(path).name == "matrix-stop.json":
                raise OSError("secret-write-error")
            return original(path, value)
        with patch.object(runner, "write_private", side_effect=denied):
            code, result = self.run_job()
        self.assertEqual(code, 1)
        self.assertTrue(result["evidence_write_failed"])
        self.assertNotIn("secret-write-error", json.dumps(result))
        self.assertIn(("guard", signal.SIGTERM), self.events)
        self.assertIn(("guard", signal.SIGKILL), self.events)
        self.assertIn("monitor_stopped", self.events)

    def test_success_status_write_failure_stops_matrix(self):
        original = runner.write_private
        def denied(path, value):
            if Path(path).name == "orchestrator-status.json":
                raise OSError("secret-status-error")
            return original(path, value)
        with patch.object(runner, "write_private", side_effect=denied):
            code, result = self.run_job()
        self.assertEqual(code, 1)
        self.assertEqual(result["status"], "HOLD")
        self.assertTrue((self.root / "matrix-stop.json").exists())
        self.assertNotIn("secret-status-error", json.dumps(result))

    def test_unjoinable_collector_does_not_stop_monitor_before_final_capture(self):
        self.stuck.add("collector")
        self.unkillable.add("collector")
        code, result = self.run_job()
        self.assert_hold(code, result)
        self.assertEqual({entry["role"] for entry in result["retained_owned_processes"]}, {"collector", "monitor"})
        self.assertFalse((self.output / "monitor-stop.json").exists())
        self.assertNotIn("monitor_stopped", self.events)

    def test_config_changed_during_load_stops_guard(self):
        self.stuck.add("guard")
        def mutate():
            if "guard" in self.processes:
                self.config.write_text(self.config.read_text() + "\n")
        self.on_sleep = mutate
        self.assert_hold(*self.run_job())
        self.assertIn(("guard", signal.SIGTERM), self.events)

    def test_monitor_final_append_is_frozen_before_single_finalize_and_verify(self):
        def append():
            self.clock += 0.001
            self.sample()
        self.on_monitor_stop = append
        code, result = self.run_job()
        self.assertEqual(code, 0)
        self.assertEqual(result["status"], "MEASURED NOT ACCEPTED")
        self.assertFalse((self.root / "matrix-stop.json").exists())
        self.assertEqual(self.events.count("spawn_finalize"), 1)
        self.assertEqual(self.events.count("spawn_verify"), 1)
        self.assertTrue((self.output / "capacity-verification.json").exists())
        resource = self.output / "container-resources.jsonl"
        self.assertEqual(len(resource.read_text().splitlines()), 2)
        self.assertEqual(resource.read_bytes(), self.frozen_raw)
        manifest = json.loads((self.output / "capacity-verification.json").read_text())["evidence_manifest"]
        self.assertEqual(manifest[str(resource)], {"sha256": digest(resource), "bytes": resource.stat().st_size})
        self.assertLess(self.events.index("collector_final_capture"), self.events.index("monitor_stopped"))
        self.assertLess(self.events.index("monitor_stopped"), self.events.index("spawn_finalize"))

    def test_monitor_covers_collectors_last_capture_before_any_readonly_processing(self):
        def capturing():
            self.assertIsNone(self.processes["monitor"].poll())
            self.assertFalse((self.output / "monitor-stop.json").exists())
            self.assertNotIn("spawn_finalize", self.events)
            self.assertNotIn("spawn_verify", self.events)
            self.clock += 0.001
            self.sample()
        self.on_final_capture = capturing
        self.assertEqual(self.run_job()[0], 0)
        self.assertEqual(self.events.count("collector_final_capture"), 1)
        self.assertLess(self.events.index("collector_final_capture"), self.events.index("monitor_stopped"))

    def test_readonly_processing_after_stop_does_not_require_live_sample_freshness(self):
        self.stuck.update(("finalize", "verify"))
        def complete():
            for role in ("finalize", "verify"):
                if role in self.processes and self.clock - self.processes[role].started >= 2.5:
                    self.stuck.discard(role)
        self.on_sleep = complete
        self.assertEqual(self.run_job()[0], 0)
        self.assertGreater(self.clock, 5)
        self.assertEqual((self.output / "container-resources.jsonl").read_bytes(), self.frozen_raw)
        self.assertEqual(self.events.count("spawn_finalize"), 1)
        self.assertEqual(self.events.count("spawn_verify"), 1)

    def test_guard_group_descendants_are_killed_even_if_guard_leader_joins(self):
        self.early_role = "collector"
        self.group_survives_term.add("guard")
        self.assert_hold(*self.run_job())
        self.assertIn(("guard", signal.SIGTERM), self.events)
        self.assertIn(("guard", signal.SIGKILL), self.events)
        self.assertEqual(self.processes["guard"].code, -signal.SIGTERM)
        self.assertIn("guard", self.group_killed)


if __name__ == "__main__":
    unittest.main()
