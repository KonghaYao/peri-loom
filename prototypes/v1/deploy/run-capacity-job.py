"""Main's explicit, single frozen capacity job; no retries or implicit load."""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "stress"))
import run_guarded as guarded
from capacity_collect import select_job
from capacity_metrics import timestamp
from capacity_plan import digest, read_plan, write_private
from capacity_resources import ContainerResources


DEPLOY = Path(__file__).resolve().parent
STRESS = DEPLOY.parent / "stress"


class Hold(Exception):
    pass


def require(condition, reason):
    if not condition:
        raise Hold(reason)


def absent(path):
    return not os.path.lexists(path)


class Orchestrator:
    def __init__(self, arguments):
        self.arguments = arguments
        self.children = {}
        self.root = self.output = None
        self.lock = None
        self.interrupted = False
        self.grace = None
        self.hashes = {}
        self.exits = {}
        self.retained = []
        self.last_sample = None
        self.resource_prefix = b""
        self.collector_prefix = b""
        self.last_poll = None
        self.evidence_write_failed = False

    def prepare(self):
        for path in (self.arguments.plan, self.arguments.monitor_config, self.arguments.monitor_script):
            guarded.private(path.parent, 0o700, True)
            guarded.private(path, 0o600)
        self.plan = read_plan(self.arguments.plan)
        require(self.plan["plan_path"] == str(self.arguments.plan), "original_plan_path_required")
        root = Path(self.plan["config"]["output"])
        guarded.private(root, 0o700, True)
        self.root = root
        self.job = select_job(self.plan, self.arguments.job)
        output = Path(self.job["output"])
        require(output.parent == root and output.name == self.job["id"]
                and Path(self.job["spec"]) == output / "guard-spec.json", "owned_job_paths_required")
        guarded.private(output, 0o700, True)
        self.output = output
        require(absent(root / "matrix-stop.json"), "matrix_already_stopped")
        require(self.plan["config"]["external_busy"] is False, "external_busy")
        self.lock_path = root / ".matrix.lock"
        self.lock = os.open(self.lock_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        require({path.name for path in output.iterdir()} == {"guard-spec.json"}, "original_evidence_no_rerun")
        descriptor = os.open(output / ".capacity-job.lock", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(descriptor)
        self.spec = guarded.read_spec(Path(self.job["spec"]))
        require(self.spec["output"] == str(output), "guard_output_binding_required")
        self.grace = self.spec.get("hard_grace_seconds", 5)
        require(type(self.grace) in (int, float) and 0.1 <= self.grace <= 30, "original_join_grace_required")
        launcher = guarded.read_spec(Path(self.plan["config"]["launcher_report"]))
        config = guarded.read_spec(self.arguments.monitor_config)
        for field in ("container_id", "container_name", "image", "source_hash", "owner", "resources", "required_native_roles"):
            require(field in config and config[field] == launcher[field], "monitor_frozen_launcher_binding_required")
        require(config["image"]["Os"] == "linux", "Linux_proc_monitor_required")
        self.resources = ContainerResources(launcher, self.plan["config"]["guard"]["max_process_rss_bytes"])
        filenames = {str(self.arguments.plan), str(self.arguments.plan) + ".sha256.json",
                     str(self.arguments.monitor_config), str(self.arguments.monitor_script),
                     str(Path(__file__).resolve()), *self.plan["private_input_sha256"]}
        for job in self.plan["jobs"]:
            filenames.add(job["spec"])
            spec = guarded.read_spec(Path(job["spec"]))
            filenames.update(spec["sources"])
            filenames.update(spec["artifacts"])
        self.hashes = {filename: digest(filename) for filename in sorted(filenames)}
        read_plan(self.arguments.plan)
        require(all(digest(filename) == expected for filename, expected in self.hashes.items()), "freeze_inputs_changed")
        require(all(str(path) in self.spec["sources"] for path in (
                    STRESS / "run_guarded.py", DEPLOY / "collect-capacity.py",
                    DEPLOY / "capacity-matrix.py", DEPLOY / "run-capacity-job.py",
                    self.arguments.monitor_config, self.arguments.monitor_script)),
                "frozen_execution_and_sidecar_sources_required")
        write_private(output / "orchestrator-start.json", {"job": self.job["id"], "source_sha256": self.hashes,
                      "join_grace_seconds": self.grace, "performance_pass": False})

    def unchanged(self):
        require(not self.interrupted, "operator_stop")
        require(absent(self.root / "matrix-stop.json"), "matrix_stopped")
        for path in (self.arguments.monitor_config, self.arguments.monitor_script):
            guarded.private(path.parent, 0o700, True)
            guarded.private(path, 0o600)
        require(all(digest(filename) == expected for filename, expected in self.hashes.items()), "frozen_source_or_input_changed")

    def spawn(self, role, script, *arguments):
        self.unchanged()
        if role == "guard":
            self.alive("monitor", "collector")
            require(self.fresh() and self.collector_progress() and self.ready(), "sidecar_samples_missing")
        child = subprocess.Popen([sys.executable, str(script), *map(str, arguments)],
                                 stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL, env=guarded.safe_environment(), start_new_session=True)
        self.children[role] = child
        return child

    def alive(self, *roles):
        for role in roles:
            require(self.children[role].poll() is None, role + "_early_exit")

    def journal(self, path, prefix, atomic=False):
        guarded.private(path, 0o600)
        require(path.stat().st_size <= self.spec["max_output_bytes"], "original_output_budget_exceeded")
        body = path.read_bytes()
        require(body.startswith(prefix), "original_journal_prefix_changed")
        complete = body[:body.rfind(b"\n") + 1]
        require(not atomic or complete == body, "monitor_atomic_journal_required")
        return complete, [json.loads(line) for line in complete[len(prefix):].splitlines()]

    def fresh(self):
        path = self.output / "container-resources.jsonl"
        if absent(path):
            return False
        prefix, samples = self.journal(path, self.resource_prefix, atomic=True)
        for sample in samples:
            self.resources.ingest(sample)
            self.last_sample = sample
        self.resource_prefix = prefix
        if self.last_sample is None:
            return False
        require(abs(time.time() - timestamp(self.last_sample["observed_at"])) <= 2, "monitor_sample_not_fresh")
        return True

    def collector_progress(self):
        path = self.output / "capacity-trace.jsonl"
        if absent(path):
            return False
        prefix, records = self.journal(path, self.collector_prefix)
        for record in records:
            if record.get("type") != "collector_poll":
                continue
            require(record.get("job") == self.job["id"]
                    and record.get("plan_sha256") == self.hashes[str(self.arguments.plan)]
                    and record.get("finishing") is False, "collector_poll_binding_invalid")
            observed = timestamp(record["observed_at"])
            require(self.last_poll is None or observed > self.last_poll, "collector_poll_time_not_advancing")
            self.last_poll = observed
        self.collector_prefix = prefix
        if self.last_poll is None:
            return False
        profile = self.plan["collector_profile"]
        require(abs(time.time() - self.last_poll) <= profile["poll_budget_seconds"] + profile["poll_seconds"],
                "collector_poll_not_fresh")
        return True

    def ready(self):
        path = self.output / "collector-ready.json"
        if absent(path):
            return False
        marker = guarded.read_spec(path)
        require(marker == {"status": "READY READ ONLY NOT MEASURED", "job": self.job["id"],
                           "plan_sha256": self.hashes[str(self.arguments.plan)]}, "collector_ready_binding_invalid")
        require(absent(self.output / "collector-status.json"), "collector_already_finished")
        return True

    def gate(self, roles, predicate):
        while True:
            self.unchanged()
            self.alive(*roles)
            require(time.monotonic() - self.started < self.spec["hard_max_seconds"], "original_startup_budget_exhausted")
            if predicate():
                self.alive(*roles)
                return
            time.sleep(0.05)

    def stop_file(self, role):
        write_private(self.output / (role + "-stop.json"), {"job": self.job["id"], "stop": True})

    def terminate(self, role):
        child = self.children[role]
        began = time.monotonic()
        def group_alive():
            try:
                os.killpg(child.pid, 0)
                return True
            except ProcessLookupError:
                return False
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        timed_out = False
        try:
            child.wait(timeout=self.grace)
        except subprocess.TimeoutExpired:
            timed_out = True
        while not timed_out and group_alive() and time.monotonic() - began < self.grace:
            time.sleep(0.05)
        if timed_out or group_alive():
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            began = time.monotonic()
            try:
                child.wait(timeout=self.grace)
            except subprocess.TimeoutExpired:
                pass
            while group_alive() and time.monotonic() - began < self.grace:
                time.sleep(0.05)
        if child.poll() is None or group_alive():
            self.retained.append({"role": role, "pid": child.pid})
        self.exits[role] = child.poll()

    def join(self, role, watch=(), checking=True):
        began = time.monotonic()
        child = self.children[role]
        while True:
            if checking:
                self.unchanged()
            self.alive(*watch)
            if "monitor" in watch:
                require(self.fresh(), "monitor_samples_missing")
            code = child.poll()
            if code is not None:
                self.exits[role] = child.wait(timeout=self.grace)
                require(code == 0, role + "_nonzero_exit")
                return
            require(time.monotonic() - began < self.grace, role + "_join_not_complete")
            time.sleep(0.05)

    def execute(self):
        self.prepare()
        self.started = time.monotonic()
        self.spawn("monitor", self.arguments.monitor_script, "--config", self.arguments.monitor_config,
                   "--output", self.output / "container-resources.jsonl", "--stop-file", self.output / "monitor-stop.json")
        self.gate(("monitor",), self.fresh)
        self.spawn("collector", DEPLOY / "collect-capacity.py", "--capture", "--plan", self.arguments.plan,
                   "--job", self.job["id"], "--stop-file", self.output / "collector-stop.json")
        self.gate(("monitor", "collector"), lambda: self.fresh() and self.ready() and self.collector_progress())
        self.unchanged()
        self.alive("monitor", "collector")
        read_plan(self.arguments.plan)
        self.spawn("guard", STRESS / "run_guarded.py", "--spec", self.job["spec"])
        while True:
            self.alive("monitor", "collector")
            self.unchanged()
            require(self.fresh() and self.collector_progress(), "sidecar_samples_missing")
            code = self.children["guard"].poll()
            if code is not None:
                self.exits["guard"] = self.children["guard"].wait(timeout=self.grace)
                require(code == 0, "guard_nonzero_exit")
                break
            time.sleep(0.05)
        self.stop_file("collector")
        self.join("collector", ("monitor",))
        status = guarded.read_spec(self.output / "collector-status.json")
        require(status.get("status") == "COMPLETE" and status.get("job") == self.job["id"]
                and status.get("plan_sha256") == self.hashes[str(self.arguments.plan)]
                and status.get("journal_sha256") == digest(self.output / "capacity-trace.jsonl"), "collector_final_capture_incomplete")
        self.alive("monitor")
        require(self.fresh(), "monitor_samples_missing")
        self.stop_file("monitor")
        self.join("monitor")
        read_plan(self.arguments.plan)
        self.spawn("finalize", DEPLOY / "collect-capacity.py", "--finalize", "--plan", self.arguments.plan, "--job", self.job["id"])
        self.join("finalize")
        self.spawn("verify", DEPLOY / "capacity-matrix.py", "--verify-job", "--plan", self.arguments.plan, "--job", self.job["id"])
        self.join("verify")
        marker = guarded.read_spec(self.output / "capacity-verification.json")
        require(marker.get("status") == "MEASURED" and marker.get("job") == self.job["id"]
                and marker.get("spec_sha256") == self.job["spec_sha256"], "original_verification_required")
        self.unchanged()
        manifest = marker.get("evidence_manifest")
        require(isinstance(manifest, dict) and str(self.output / "container-resources.jsonl") in manifest,
                "final_original_evidence_manifest_required")
        for filename, expected in manifest.items():
            path = Path(filename)
            require(path.is_relative_to(self.output), "final_evidence_owned_job_required")
            guarded.private(path.parent, 0o700, True)
            guarded.private(path, 0o600)
            require(digest(path) == expected["sha256"] and path.stat().st_size == expected["bytes"],
                    "final_original_evidence_changed")

    def cleanup(self):
        for role in ("guard", "finalize", "verify"):
            if role in self.children and self.children[role].poll() is None:
                try:
                    self.terminate(role)
                except Exception:
                    self.retained.append({"role": role, "pid": self.children[role].pid})
        for role in ("collector", "monitor"):
            if role == "monitor" and "collector" in self.children and self.children["collector"].poll() is None:
                self.retained.append({"role": "monitor", "pid": self.children[role].pid})
                continue
            if role not in self.children or self.children[role].poll() is not None:
                continue
            try:
                if absent(self.output / (role + "-stop.json")):
                    self.stop_file(role)
                    self.join(role, checking=False)
                else:
                    self.terminate(role)
            except Exception:
                try:
                    self.terminate(role)
                except Exception:
                    self.retained.append({"role": role, "pid": self.children[role].pid})

    def record(self, reason):
        result = {"status": "HOLD" if reason else "MEASURED NOT ACCEPTED", "job": self.arguments.job,
                  "reason": reason, "owned_exits": self.exits, "retained_owned_processes": self.retained,
                  "may_have_unknown": bool(reason), "performance_pass": False}
        paths = []
        if reason and self.root is not None:
            paths.append(self.root / "matrix-stop.json")
        if self.output is not None:
            paths.append(self.output / "orchestrator-status.json")
        for path in paths:
            try:
                if absent(path):
                    write_private(path, result)
            except Exception:
                self.evidence_write_failed = True
        if self.evidence_write_failed:
            result.update(status="HOLD", may_have_unknown=True, evidence_write_failed=True)
            try:
                if self.root is not None and absent(self.root / "matrix-stop.json"):
                    write_private(self.root / "matrix-stop.json", result)
            except Exception:
                pass
        return result

    def run(self):
        reason = None
        try:
            self.execute()
        except Exception as error:
            reason = str(error) if isinstance(error, Hold) else "private_orchestration_error_details_withheld"
            try:
                if self.root is not None and absent(self.root / "matrix-stop.json"):
                    write_private(self.root / "matrix-stop.json", {"status": "HOLD", "job": self.arguments.job,
                                  "reason": reason, "may_have_unknown": True, "performance_pass": False})
            except Exception:
                self.evidence_write_failed = True
            try:
                self.cleanup()
            except Exception:
                reason = reason or "owned_cleanup_incomplete"
        try:
            result = self.record(reason)
        finally:
            if self.lock is not None:
                if not absent(self.lock_path) and guarded.metadata(self.lock_path)[:2] == [os.fstat(self.lock).st_dev, os.fstat(self.lock).st_ino]:
                    self.lock_path.unlink()
                os.close(self.lock)
        return (1 if reason or self.evidence_write_failed else 0), result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--job", required=True)
    parser.add_argument("--monitor-config", required=True, type=Path)
    parser.add_argument("--monitor-script", required=True, type=Path)
    arguments = parser.parse_args()
    os.umask(0o077)
    orchestrator = Orchestrator(arguments)
    previous = {selected: signal.getsignal(selected) for selected in (signal.SIGINT, signal.SIGTERM)}
    def stop(signum, frame):
        orchestrator.interrupted = True
    for selected in previous:
        signal.signal(selected, stop)
    try:
        try:
            code, result = orchestrator.run()
        except Exception:
            code, result = 1, {"status": "HOLD", "reason": "private_orchestration_error_details_withheld",
                              "may_have_unknown": True, "performance_pass": False}
        print(json.dumps(result))
        return code
    finally:
        for selected, handler in previous.items():
            signal.signal(selected, handler)


if __name__ == "__main__":
    sys.exit(main())
