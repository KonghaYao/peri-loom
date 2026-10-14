#!/usr/bin/env python3
"""Real verified-TLS HTTP reliability using the existing image verifier fixture."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import sys
import time
import urllib.error
import uuid

from runner import ROOT, digest, source_inventory
from provenance import historical_image_evidence

spec = importlib.util.spec_from_file_location("image_verifier", ROOT / "prototypes/v1/deploy/verify-image.py")
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)
spec = importlib.util.spec_from_file_location("platform_dev", ROOT / "prototypes/v1/platform/dev.py")
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)


class Reliability(verifier.Verification):
    expected_check_count = 4
    def __init__(self, args):
        super().__init__(args)
        self.report["http_records"] = []
        self.report["checks"] = []
        self.report["protocol"] = "actual HTTPS gateway -> controller -> Rust native authority/worker"
        self.report["image_source_identity"] = "unattested existing image; never asserted equal to current HEAD"
        self.report["source_inventory"] = source_inventory()
        self.report["round"] = args.round
        self.report["functional_only"] = True
        self.configuration = None
        self.deadline = time.monotonic() + 300
        self.cleaning = False

    def command(self, command, timeout=30, check=True):
        remaining = self.deadline - time.monotonic()
        if not self.cleaning:
            if remaining <= 0:
                raise RuntimeError("bounded_whole_stack_deadline")
            timeout = min(timeout, remaining)
        return super().command(command, timeout=timeout, check=check)

    def ready(self):
        deadline = time.monotonic() + self.arguments.ready_timeout
        while time.monotonic() < deadline:
            value = self.inspect()
            if not value["State"]["Running"]:
                raise RuntimeError("container_exited_before_fresh_readiness")
            logs = self.docker("logs", "--since", value["State"]["StartedAt"], self.container)
            if any(self.ready_event(line) for line in logs.stdout.splitlines()):
                break
            time.sleep(.25)
        else:
            raise RuntimeError("fresh_readiness_timeout")
        for filename in ["image-dev.py", "connection.json", "ca.crt", "tls.crt"]:
            (self.fixture / filename).unlink(missing_ok=True)
        super().ready()
        self.configuration = json.loads((self.fixture / "connection.json").read_text())

    def call(self, stage, action, body):
        if time.monotonic() >= self.deadline:
            raise RuntimeError("bounded_whole_stack_deadline")
        started = time.monotonic()
        record = {"stage": stage, "action": action, "outcome": "Unknown"}
        try:
            response = dev.request(self.configuration["endpoint"], self.configuration["project_api_key"],
                                   action, body, self.configuration["ca_file"])
            record["outcome"] = "ACK"
            return response
        except urllib.error.HTTPError as failure:
            record["http_status"] = failure.code
            record["outcome"] = "Error"
            raise RuntimeError("HTTP_error_" + str(failure.code)) from None
        except RuntimeError as failure:
            matched = re.fullmatch(r"request rejected: ([a-z0-9_]+)", str(failure))
            if matched:
                record["error_code"] = matched.group(1)
                record["outcome"] = "Unknown" if action == "/v1/sql/execute" or "unknown" in matched.group(1) else "Error"
            else:
                record["error_type"] = "RuntimeError"
            raise RuntimeError("HTTP_" + record["outcome"] + "_" + record.get("error_code", "protocol_or_transport")) from None
        except Exception as failure:
            record["error_type"] = type(failure).__name__
            raise RuntimeError("HTTP_Unknown_" + type(failure).__name__) from None
        finally:
            record["latency_ms"] = (time.monotonic() - started) * 1000
            self.report["http_records"].append(record)

    def lifecycle(self):
        database = self.configuration["database"]
        table = "reliability_" + uuid.uuid4().hex[:12]
        self.report["table"] = table

        def opened(stage):
            return self.call(stage, "/v1/sql/open", {"database": database})

        def query(stage, session):
            return self.call(stage, "/v1/sql/query", {"session": session["session"],
                            "sql": f"SELECT id,payload FROM {table} ORDER BY id"})

        session = opened("initial_open")
        self.call("create_and_commit_ACK_no_explicit_checkpoint", "/v1/sql/execute",
                  {"session": session["session"], "sql": f"CREATE TABLE {table}(id INTEGER PRIMARY KEY,payload TEXT);INSERT INTO {table} VALUES(1,'http_ACK');"})
        actual = query("read_after_ACK", session)
        self.report["checks"].append({"stage": "read_after_ACK", "actual": actual, "pass": actual == [[1, "http_ACK"]]})
        self.call("session_close", "/v1/sql/close", {"session": session["session"]})
        warm = opened("warm_open")
        self.call("warm_close", "/v1/sql/close", {"session": warm["session"]})
        self.call("suspend", "/v1/databases/suspend", {"database": database})
        self.call("explicit_resume", "/v1/databases/resume", {"database": database})
        resumed = opened("resume_open")
        actual = query("read_after_resume", resumed)
        self.report["checks"].append({"stage": "read_after_resume", "actual": actual, "pass": actual == [[1, "http_ACK"]]})
        self.call("resume_close", "/v1/sql/close", {"session": resumed["session"]})
        self.report["stage"] = "clean_restart"
        started = time.monotonic()
        self.inspect()
        self.docker("kill", "--signal", "TERM", self.container)
        self.docker("wait", self.container, timeout=self.arguments.stop_timeout)
        stopped = self.inspect()["State"]
        self.report["clean_stop"] = {key: stopped[key] for key in ["ExitCode", "Running", "OOMKilled", "Pid"]}
        if stopped["ExitCode"] != 0 or stopped["Running"] or stopped["OOMKilled"]:
            raise RuntimeError("clean_stop_not_proven")
        self.docker("start", self.container)
        self.ready()
        self.report["clean_restart_to_ready_ms"] = (time.monotonic() - started) * 1000
        session = opened("clean_restart_open")
        actual = query("clean_restart_read", session)
        self.report["checks"].append({"stage": "clean_restart_read", "actual": actual, "pass": actual == [[1, "http_ACK"]]})
        self.call("ACK_before_dirty_restart", "/v1/sql/execute",
                  {"session": session["session"], "sql": f"INSERT INTO {table} VALUES(2,'dirty_restart_ACK')"})
        self.report["stage"] = "dirty_restart"
        self.inspect()
        self.docker("kill", "--signal", "KILL", self.container)
        self.docker("wait", self.container, timeout=10)
        started = time.monotonic()
        self.docker("start", self.container)
        self.ready()
        self.report["dirty_restart_to_ready_ms"] = (time.monotonic() - started) * 1000
        session = opened("dirty_restart_open")
        actual = query("dirty_restart_read", session)
        self.report["checks"].append({"stage": "dirty_restart_read", "actual": actual,
                                     "pass": actual == [[1, "http_ACK"], [2, "dirty_restart_ACK"]]})
        self.call("final_close", "/v1/sql/close", {"session": session["session"]})

    def experiment(self):
        self.report["stage"] = "prepare"
        started = time.monotonic()
        try:
            self.prepare()
            self.report["historical_build_before"] = historical_image_evidence(self.report["image"]["Id"])
            self.report["image_source_identity"] = self.report["historical_build_before"]["status"]
            self.ready()
            self.report["create_to_ready_ms"] = (time.monotonic() - started) * 1000
            self.copy("/usr/local/bin/peri-loom-v1", self.fixture / "native")
            self.copy("/usr/local/bin/peri-platform", self.fixture / "platform")
            self.report["image_binary_sha256"] = {name: digest(self.fixture / name) for name in ["native", "platform"]}
            self.report["stage"] = "existing_whole_smoke"
            self.smoke()
            self.report["stage"] = "HTTP_lifecycle"
            self.lifecycle()
            self.report["experiment_pass"] = all(check["pass"] for check in self.report["checks"])
        except BaseException as failure:
            self.report["experiment_pass"] = False
            self.report["failure_type"] = type(failure).__name__
            self.report["failure_category"] = str(failure) if isinstance(failure, RuntimeError) else "withheld"
            try:
                state = self.inspect()["State"]
                self.report["failure_container_state"] = {key: state[key] for key in ["Running", "Pid", "ExitCode", "OOMKilled"]}
                logs = self.docker("logs", self.container)
                text = logs.stdout + logs.stderr
                self.report["failure_log_sha256"] = hashlib.sha256(text.encode()).hexdigest()
                self.report["failure_log_categories"] = [category for category in ["dirty", "unclean", "quarantine", "recovery", "authority", "timeout", "panic", "error"] if category in text.lower()]
                self.report["owned_incarnation_relaunch_guard"] = "owned worker incarnation cannot be relaunched; use a fresh fixture" in text
            except Exception as diagnostic_failure:
                self.report["diagnostic_failure_type"] = type(diagnostic_failure).__name__
        finally:
            self.cleaning = True
            self.report["wall_seconds"] = time.monotonic() - started
            try:
                self.report["source_unchanged"] = source_inventory() == self.report["source_inventory"]
                if "image" in self.report:
                    self.report["historical_build_after"] = historical_image_evidence(self.report["image"]["Id"])
                    self.report["historical_build_unchanged"] = self.report.get("historical_build_before") == self.report["historical_build_after"]
            except Exception as failure:
                self.report["freeze_failure_type"] = type(failure).__name__
            self.report["cleanup"] = "retained_for_reliability_inspection"
            try:
                if self.attempted_create:
                    value = self.inspect()
                    if value["State"]["Running"]:
                        self.docker("kill", "--signal", "TERM", self.container)
                        try:
                            self.docker("wait", self.container, timeout=self.arguments.stop_timeout)
                        except Exception:
                            self.inspect()
                            self.docker("kill", "--signal", "KILL", self.container)
                            self.docker("wait", self.container, timeout=10)
                    value = self.inspect()
                    self.report["final_state"] = {key: value["State"][key] for key in ["Running", "Pid", "ExitCode", "OOMKilled"]}
                    self.report["owned_container_joined"] = not value["State"]["Running"] and value["State"]["Pid"] == 0
                    self.report["image_id_unchanged"] = value["Image"] == self.report["image"]["Id"]
                    if "image_binary_sha256" in self.report:
                        for name, source in [("native", "/usr/local/bin/peri-loom-v1"), ("platform", "/usr/local/bin/peri-platform")]:
                            self.copy(source, self.fixture / (name + "-after"))
                        self.report["image_binaries_unchanged"] = all(digest(self.fixture / (name + "-after")) == expected for name, expected in self.report["image_binary_sha256"].items())
            except Exception as failure:
                self.report["cleanup_failure_type"] = type(failure).__name__
            requirements = ["experiment_pass", "source_unchanged", "historical_build_unchanged",
                            "owned_container_joined", "image_id_unchanged", "image_binaries_unchanged"]
            self.report["failure_reasons"] = [name for name in requirements if self.report.get(name) is not True]
            self.report["expected_check_count"] = self.expected_check_count
            if len(self.report["checks"]) != self.expected_check_count:
                self.report["failure_reasons"].append("check_count")
            if not self.smoke_proven:
                self.report["failure_reasons"].append("whole_smoke_unproven")
            if self.report.get("freeze_failure_type") or self.report.get("cleanup_failure_type"):
                self.report["failure_reasons"].append("freeze_or_cleanup_exception")
            self.report["pass"] = not self.report["failure_reasons"]
            self.report["status"] = "PASS" if self.report["pass"] else "FAIL_OR_NOTRUN"
            verifier.private_json(self.directory / "reliability.json", self.report)
        return self.report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", default="peri-loom:goal90-r26-20261009")
    parser.add_argument("--backend", choices=["local", "s3"], default="local")
    parser.add_argument("--round", choices=["smoke", "baseline", "repeat"], default="smoke")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("evidence output already exists")
    args.ready_timeout, args.smoke_timeout, args.stop_timeout = 45, 120, 30
    args.writer_admission, args.capacity_trace = False, False
    os.umask(0o077)
    signal.signal(signal.SIGTERM, lambda signum, frame: (_ for _ in ()).throw(KeyboardInterrupt()))
    experiment = Reliability(args)
    result = experiment.experiment()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    verifier.private_json(args.output, result)
    print(json.dumps({"output": str(args.output), "evidence": str(experiment.directory),
                      "pass": result["pass"], "stage": result["stage"],
                      "owned_container_joined": result.get("owned_container_joined")}))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
