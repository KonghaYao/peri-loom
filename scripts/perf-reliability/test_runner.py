import os
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from runner import ProtocolError, Worker, collect_rss, final_status, payload_matches, percentile, process_group_members, summary, validate_response


class RunnerTests(unittest.TestCase):
    def test_strict_reply_schema(self):
        invalid = [None, [], "reply", {"id": 1, "ok": 1},
                   {"id": True, "ok": True, "result": []},
                   {"id": 1, "ok": True, "error": "busy", "result": []},
                   {"id": 1, "ok": True, "error": None, "result": []},
                   {"id": 1, "ok": True, "result": {}},
                   {"id": 1, "ok": True, "result": [1]},
                   {"id": 1, "ok": True, "result": [[{}]]}]
        for response in invalid:
            with self.subTest(response=response), self.assertRaises(ProtocolError):
                validate_response(response, 1, "query")
        validate_response({"id": 1, "ok": True, "error": "", "result": [[1, "x", None, [0, 255]]]}, 1, "query")
        validate_response({"id": 1, "ok": False, "error": "busy"}, 1, "query")

    def test_fake_rpc_contradiction_and_nonobject_stop_unknown(self):
        for response in [{"id": 1, "ok": True, "error": "busy", "result": []}, None, [],
                         {"id": 1, "ok": True, "error": None, "result": []}]:
            with self.subTest(response=response), tempfile.TemporaryDirectory() as directory:
                binary = Path(directory) / "fake-worker"
                ready = Path(directory) / "ready"
                encoded = json.dumps(response)
                binary.write_text(f"#!{sys.executable}\nimport sys\nopen({str(ready)!r},'w').write('ready')\nsys.stdin.readline()\nprint({encoded!r},flush=True)\nsys.stdin.read()\n")
                binary.chmod(0o700)
                worker = Worker(binary, directory, directory, 2, time.monotonic() + 10)
                try:
                    for _ in range(500):
                        if ready.exists():
                            break
                        time.sleep(.01)
                    self.assertTrue(ready.exists(), "bounded fake-worker readiness failed before RPC")
                    observed, record = worker.request("query", sql="SELECT 1")
                    self.assertIsNone(observed)
                    self.assertEqual(record["outcome"], "Unknown")
                    self.assertEqual(record["error_category"], "ProtocolError")
                finally:
                    usage = worker.stop()
                self.assertTrue(usage["group_joined"])
    def test_worker_env_passthrough_and_raw_ns_timestamps(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "fake-worker"
            ready = Path(directory) / "ready"
            observed = Path(directory) / "observed-env"
            binary.write_text(
                f"#!{sys.executable}\nimport os,sys\n"
                f"open({str(observed)!r},'w').write(os.environ.get('PERI_METRICS_SNAPSHOT_PATH',''))\n"
                f"open({str(ready)!r},'w').write('ready')\n"
                "sys.stdin.readline()\n"
                "print('{\"id\":1,\"ok\":true,\"error\":\"\",\"result\":[]}',flush=True)\n"
                "sys.stdin.read()\n")
            binary.chmod(0o700)
            worker = Worker(binary, directory, directory, 2, time.monotonic() + 10,
                            env={"PERI_METRICS_SNAPSHOT_PATH": str(Path(directory) / "metrics.json")})
            try:
                for _ in range(500):
                    if ready.exists():
                        break
                    time.sleep(.01)
                self.assertTrue(ready.exists())
                self.assertEqual(observed.read_text(), str(Path(directory) / "metrics.json"))
                response, record = worker.request("query", sql="SELECT 1")
                self.assertTrue(response["ok"])
                self.assertEqual(record["duration_ns"], record["completed_ns"] - record["started_ns"])
                self.assertGreaterEqual(record["duration_ns"], 0)
            finally:
                usage = worker.stop()
            self.assertTrue(usage["group_joined"])

    def test_full_payload_oracle_rejects_same_length_wrong_value(self):
        expected = {0: "yyy", 1: "xxx"}
        self.assertTrue(payload_matches([[0, "yyy"], [1, "xxx"]], expected))
        self.assertFalse(payload_matches([[0, "xxx"], [1, "xxx"]], expected))
        self.assertFalse(payload_matches([[0, "yyy"]], expected))
        self.assertFalse(payload_matches([[], [1, "xxx"]], expected))
        self.assertFalse(payload_matches([[0, "yyy"], [2, "xxx"]], expected))
        self.assertFalse(payload_matches([[False, "yyy"], [True, "xxx"]], expected))
        self.assertFalse(payload_matches([[0.0, "yyy"], [1.0, "xxx"]], expected))
        self.assertTrue(payload_matches([[0, "xxx"], [1, "xxx"]], expected, {0: ["xxx", "yyy"]}))
        self.assertFalse(payload_matches([[0, "zzz"], [1, "xxx"]], expected, {0: ["xxx", "yyy"]}))

    def test_missing_modes_and_reliability_fail_closed(self):
        report = {"benchmarks": [], "source_unchanged": True, "frozen_binary_unchanged": True,
                  "load": {"concurrency": 1, "operations": 1}}
        result = final_status(report)
        self.assertFalse(result["pass"])
        self.assertIn("mode_count_or_order", result["reasons"])
        self.assertIn("reliability_missing_or_failed", result["reasons"])
    def test_rss_probe_failure_never_records_zero_memory(self):
        class OneTick:
            def __init__(self):
                self.calls = 0

            def wait(self, timeout):
                self.calls += 1
                return self.calls > 1

        workers = [SimpleNamespace(process=SimpleNamespace(pid=pid)) for pid in [123, 456]]
        for returncode, stdout, expected in [(1, "", None), (0, "", None),
                                             (0, "16\n", None), (0, "16\n32\n", 49152)]:
            with self.subTest(returncode=returncode, stdout=stdout):
                samples = []
                with patch("runner.subprocess.run", return_value=SimpleNamespace(returncode=returncode, stdout=stdout)):
                    collect_rss(workers, samples, OneTick())
                self.assertEqual(samples[0]["combined_rss_bytes"], expected)

    def test_no_rss_samples_cannot_pass_performance_status(self):
        report = {"load": {"operations": 1, "concurrency": 1},
                  "source_unchanged": True, "frozen_binary_unchanged": True,
                  "benchmarks": [{"mode": mode, "completed_budget": True, "ACK": 1,
                                  "Unknown": 0, "errors": 0, "oracles": [{"pass": True}],
                                  "resources": [{"group_joined": True}],
                                  "rss_samples": [{"combined_rss_bytes": None}]}
                                 for mode in ["read", "write", "mixed"]],
                  "reliability": {"checks": [{"pass": True}] * 3,
                                  "resources": [{"group_joined": True}] * 4}}
        result = final_status(report)
        self.assertFalse(result["pass"])
        self.assertIn("read_rss_unmeasured", result["reasons"])
        report["benchmarks"][0]["rss_samples"] = [{"combined_rss_bytes": 1024}]
        self.assertIn("write_rss_unmeasured", final_status(report)["reasons"])
        for mode in report["benchmarks"]:
            mode["rss_samples"] = [{"combined_rss_bytes": 1024}]
        self.assertTrue(final_status(report)["pass"])

    def test_nearest_rank_and_outcomes(self):
        self.assertEqual(percentile(list(range(1, 101)), .99), 99)
        self.assertIsNone(percentile([], .99))
        report = summary([{"outcome": "ACK", "latency_ms": 1},
                          {"outcome": "Unknown", "latency_ms": 20},
                          {"outcome": "Error", "latency_ms": 10}], 2)
        self.assertEqual(report["ACK"], 1)
        self.assertEqual(report["Unknown"], 1)
        self.assertEqual(report["errors"], 1)
        self.assertEqual(report["attempts_per_second"], 1.5)
        self.assertEqual(report["ack_per_second"], .5)
        self.assertEqual(report["ack_latency_ms"]["p99"], 1)

    def test_owned_timeout_unknown_and_join(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "fake-worker"
            binary.write_text("#!/bin/sh\nexec sleep 10\n")
            binary.chmod(0o700)
            worker = Worker(binary, directory, directory, .05, time.monotonic() + 1)
            pid = worker.process.pid
            try:
                response, record = worker.request("exec", sql="test")
                self.assertIsNone(response)
                self.assertEqual(record["outcome"], "Unknown")
                self.assertEqual(record["error_category"], "TimeoutError")
            finally:
                usage = worker.stop(crash=True)
            self.assertEqual(usage["exit_code"], -9)
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)
            self.assertEqual(worker.stop(), usage)

    def test_blocked_stdin_is_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "fake-worker"
            binary.write_text("#!/bin/sh\nexec sleep 10\n")
            binary.chmod(0o700)
            worker = Worker(binary, directory, directory, .05, time.monotonic() + .2)
            started = time.monotonic()
            try:
                _, record = worker.request("exec", sql="x" * 200000)
                self.assertEqual(record["outcome"], "Unknown")
                self.assertEqual(record["error_category"], "TimeoutError")
                self.assertLess(time.monotonic() - started, 1)
            finally:
                usage = worker.stop(crash=True)
            self.assertTrue(usage["group_joined"])

    def test_owned_fork_child_group_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "fake-worker"
            marker = Path(directory) / "child-pid"
            binary.write_text(f"#!{sys.executable}\nimport os,time\nchild=os.fork()\nif child:\n open({str(marker)!r},'w').write(str(child))\ntime.sleep(10)\n")
            binary.chmod(0o700)
            worker = Worker(binary, directory, directory, .1, time.monotonic() + 5)
            try:
                for _ in range(400):
                    if marker.exists():
                        break
                    time.sleep(.01)
                self.assertTrue(marker.exists())
            finally:
                usage = worker.stop(crash=True)
            self.assertTrue(usage["group_joined"])
            self.assertFalse([member for member in process_group_members(worker.group) if not member["state"].startswith("Z")])

    def test_runner_signal_cleanup_and_nonzero_exit(self):
        for interrupt in [signal.SIGTERM, signal.SIGINT]:
            with self.subTest(signal=interrupt), tempfile.TemporaryDirectory() as directory:
                binary = Path(directory) / "fake-worker"
                marker = Path(directory) / "worker-pid"
                binary.write_text(f"#!{sys.executable}\nimport os,time\nopen({str(marker)!r},'w').write(str(os.getpid()))\ntime.sleep(10)\n")
                binary.chmod(0o700)
                process = subprocess.Popen([sys.executable, str(Path(__file__).with_name("runner.py")),
                                            "--binary", str(binary), "--operations", "1", "--rows", "1",
                                            "--concurrency", "1", "--output", str(Path(directory) / "result.json")],
                                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                try:
                    for _ in range(500):
                        if marker.exists():
                            break
                        time.sleep(.01)
                    self.assertTrue(marker.exists())
                    os.kill(process.pid, interrupt)
                    self.assertNotEqual(process.wait(timeout=10), 0)
                    pid = int(marker.read_text())
                    with self.assertRaises(ProcessLookupError):
                        os.kill(pid, 0)
                finally:
                    if process.poll() is None:
                        process.terminate()
                        process.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
