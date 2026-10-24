import json
import math
from pathlib import Path
import platform
import re
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import campaign as campaign_module
import evidence
import gate
import issue8_runner
import issue8_test_fixtures as fixtures
import issue8_workload as workload
import perf_contract


class FakeSampler:
    def __init__(self, roots, paths, interval=.1):
        # Production samples disk usage through these paths; passing byte
        # counts instead of paths used to be masked by this fake.
        for name, path in paths.items():
            if not isinstance(path, (str, Path)):
                raise AssertionError("sampler_path_not_pathlike: " + name)
        self.roots = roots
        self.paths = paths
        self.interval = interval
        self.started_ns = None

    def start(self):
        self.started_ns = time.monotonic_ns()
        return None

    def stop(self):
        started = self.started_ns or time.monotonic_ns()
        now = time.monotonic_ns()
        # The gate only counts samples inside the timed window. The fake run is
        # short, so add a microsecond-resolution tail ending at stop time; the
        # timed window always ends within a few microseconds of this call.
        interval_ns = int(self.interval * 1_000_000_000)
        timestamps = set(range(started, now + 1, interval_ns))
        tail = max(started, now - 2_000_000)
        timestamps.update(range(tail, now + 1, 1_000))
        timestamps = sorted(timestamps)
        samples = [{"monotonic_ns": when,
                    "roots": list(self.roots), "pids": list(self.roots),
                    "rss_bytes": 1024 * 1024, "cpu_seconds": 0.1 * index,
                    "disk_bytes": {}} for index, when in enumerate(timestamps)]
        return {"status": "MEASURED", "sample_count": len(samples), "samples": samples,
                "peak_rss_bytes": 1024 * 1024,
                "last_window_rss_bytes": [1024 * 1024] * max(1, math.ceil(len(samples) * 0.1)),
                "cpu_seconds": 1.0, "failures": [],
                "interval_seconds": self.interval}


class FakeProcessWorker:
    next_pid = 5000

    def __init__(self, binary, store, cache, timeout, deadline, env=None):
        FakeProcessWorker.next_pid += 1
        self.process = type("Process", (), {"pid": FakeProcessWorker.next_pid})()
        self.rows = None
        self.usage = None
        self.metrics_path = Path(env["PERI_METRICS_SNAPSHOT_PATH"]) \
            if env and env.get("PERI_METRICS_SNAPSHOT_PATH") else None
        if self.metrics_path is not None:
            document = fixtures.metrics_document(
                writes=1000, pid=self.process.pid,
                generated_at=time.time_ns() // 1_000_000)
            self.metrics_path.write_text(json.dumps(document) + "\n")

    def request(self, operation, session=None, sql=None, **arguments):
        started = time.monotonic_ns()
        result = None
        if operation == "open":
            response = {"id": 1, "ok": True, "session": "fake-session"}
        elif operation == "checkpoint":
            response = {"id": 1, "ok": True, "result": {"ok": True}}
        elif operation == "exec":
            if sql.startswith("CREATE TABLE"):
                self.rows = [None] * 64
            elif sql.startswith("INSERT INTO bench"):
                self.rows = ["x" * 256 for _ in range(64)]
            elif sql.startswith("UPDATE bench"):
                identifier = int(re.search(r"WHERE id=(\d+)", sql).group(1))
                payload = re.search(r"payload='([yz]+)'", sql).group(1)
                self.rows[identifier] = payload
            response = {"id": 1, "ok": True, "result": {"ok": True}}
        elif operation == "query":
            if "SELECT id,payload" in sql:
                result = [[index, payload] for index, payload in enumerate(self.rows)]
            else:
                identifier = int(re.search(r"WHERE id=(\d+)", sql).group(1))
                result = [[self.rows[identifier]]]
            response = {"id": 1, "ok": True, "result": result}
        else:
            response = {"id": 1, "ok": False, "error": "unsupported"}
        completed = time.monotonic_ns()
        outcome = "ACK" if response.get("ok") else "Error"
        return response, {"operation": operation, "outcome": outcome,
                          "started_ns": started, "completed_ns": completed,
                          "duration_ns": completed - started}

    def require(self, operation, **arguments):
        response, record = self.request(operation, **arguments)
        if record["outcome"] != "ACK":
            raise RuntimeError("fake_require_failed")
        return response

    def stop(self, crash=False):
        if self.metrics_path is not None and self.metrics_path.is_file():
            # The real worker writes its exit snapshot while shutting down,
            # after the timed window; mirror that for the generated_at check.
            document = json.loads(self.metrics_path.read_text())
            document["generated_at_unix_ms"] = time.time_ns() // 1_000_000
            self.metrics_path.write_text(json.dumps(document) + "\n")
        if self.usage is None:
            self.usage = {"pid": self.process.pid, "exit_code": 0, "group_joined": True,
                          "group_live_remaining": [], "group_members_at_cleanup": []}
        return self.usage


class FakeWorker:
    def __init__(self, response=None, fail_after=None):
        self.response = response
        self.fail_after = fail_after
        self.calls = 0

    def request(self, operation, session=None, sql=None):
        self.calls += 1
        started = time.monotonic_ns()
        completed = started + 1_000_000
        if self.fail_after is not None and self.calls > self.fail_after:
            return None, {"operation": operation, "outcome": "Unknown",
                          "error_category": "synthetic", "started_ns": started,
                          "completed_ns": completed, "duration_ns": completed - started}
        return (self.response or {"ok": True, "result": {"ok": True}}), {
            "operation": operation, "outcome": "ACK", "started_ns": started,
            "completed_ns": completed, "duration_ns": completed - started}


def plan_for(case="S1", warmup=4, required=8):
    return {"case": case, "rows": 64, "payload_bytes": 256, "concurrency": 1,
            "required_operations_per_lane": required, "warmup_operations": warmup,
            "minimum_seconds": 0, "max_seconds_per_arm": 10,
            "buckets": workload.BUCKETS[case], "minimum_samples_per_bucket": 8}


class RunnerHelperTests(unittest.TestCase):
    def test_s1_lane_streams_raw_ack_records(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "operations.jsonl"
            writer = evidence.RawRecordWriter(path)
            model = workload.PayloadModel()
            plan = plan_for()
            identity = {"run": "r", "pair": 0, "arm": "A", "case": "S1"}
            lock = threading.Lock()
            worker = FakeWorker()
            warm = issue8_runner.warmup_lane(worker, "session", 0, plan, model, writer, lock, identity)
            self.assertFalse(warm["terminated"])
            self.assertEqual(warm["attempts"], 4)
            timed = issue8_runner.timed_lane(worker, "session", 0, plan, model, writer, lock,
                                             identity, time.monotonic_ns(), time.monotonic() + 10)
            writer.close()
            self.assertEqual(timed["attempts"], 8)
            records = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual(len(records), 12)
            timed_records = [record for record in records if record["phase"] == "timed"]
            self.assertEqual([record["seq"] for record in timed_records], list(range(8)))
            for record in timed_records:
                self.assertEqual(record["kind"], "write")
                self.assertEqual(record["ack_type"], "statement_autocommit")
                self.assertFalse(record["durable_ack"])
                self.assertLessEqual(record["planned_at"], record["issued_at"])
                self.assertEqual(record["duration_ns"], record["completed_at"] - record["issued_at"])
                self.assertEqual(record["run"], "r")

    def test_unknown_stops_lane_without_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            writer = evidence.RawRecordWriter(Path(directory) / "operations.jsonl")
            model = workload.PayloadModel()
            plan = plan_for()
            lock = threading.Lock()
            worker = FakeWorker(fail_after=1)
            result = issue8_runner.timed_lane(worker, "session", 0, plan, model, writer, lock,
                                              {"run": "r", "pair": 0, "arm": "A", "case": "S1"},
                                              time.monotonic_ns(), time.monotonic() + 10)
            writer.close()
            self.assertTrue(result["terminated"])
            self.assertEqual(result["attempts"], 2)
            self.assertEqual(result["unknown"], 1)
            self.assertEqual(worker.calls, 2)

    def test_read_oracle_mismatch_is_recorded(self):
        model = workload.PayloadModel()
        plan = plan_for("S2", warmup=0, required=1)
        worker = FakeWorker(response={"ok": True, "result": [["wrong"]]})
        record, outcome, oracle = issue8_runner._execute_operation(
            worker, "session", 0, 0, 0, "timed", plan, model)
        self.assertEqual(outcome, "ACK")
        self.assertEqual(oracle, "mismatch")
        self.assertEqual(record["ack_type"], "read_result")
        self.assertTrue(record["observed_sha256"])
        self.assertEqual(record["row"], 0)

    def test_metrics_outputs_are_per_lane_and_never_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory) / "metrics.json"
            paths = issue8_runner.metrics_paths(base, 2)
            self.assertEqual(len(set(paths)), 2)
            self.assertNotEqual(paths[0], paths[1])
            prepared = issue8_runner.prepare_metrics_outputs(base, 2)
            self.assertEqual(prepared, paths)
            paths[0].write_text("{}")
            with self.assertRaises(perf_contract.UnsafePath):
                issue8_runner.prepare_metrics_outputs(base, 2)

    def test_metrics_snapshot_with_group_aggregation_is_accepted(self):
        document = fixtures.metrics_document(writes=7)
        document["group_phases"] = {
            "available": True,
            "calls": 8,
            "writes": {"1": 7, "2_4": 1, "5_16": 0, "17_plus": 0},
            "class": {"wal": 1, "receipt": 0, "authority": 7, "other": 0},
            "barrier_ns": 1,
            "body_fsync_ns": 1,
            "dir_flush_ns": 1,
            "total_ns": 3,
            "total_max_ns": 2,
        }
        document["counters"].extend([
            {"name": "work.metadata.publish_origin.append.calls", "value": 7},
            {"name": "work.metadata.publish_origin.pin_extend.calls", "value": 1},
            {"name": "work.metadata.publish_outside_transact.migration.calls", "value": 0},
            {"name": "work.group.calls", "value": 8},
            {"name": "work.group.writes.5_16", "value": 0},
        ])
        reasons, counters = gate._validate_metrics_document(document)
        self.assertEqual(reasons, [])
        self.assertEqual(counters["work.group.calls"], 8)
        self.assertEqual(counters["work.metadata.publish_origin.append.calls"], 7)

    def test_execute_run_produces_gate_passing_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            official = perf_contract.official_denominator()
            campaign = {"schema": 1, "id": "integration", "tier": "G1", "pairs": 1,
                        "order": ["AB"], "cases": [{"id": "S1", "concurrency": [2]}],
                        "expected_denominator": dict(official),
                        "arms": {"A": {"role": "candidate", "binary": "a",
                                       "version": "v1.1.12", "commit": "cand"},
                                 "B": {"role": "denominator", "binary": "b",
                                       "version": official["version"],
                                       "commit": official["commit"]}}}
            evidence_root = root / "evidence"
            evidence_root.mkdir()
            campaign_path = evidence_root / "campaign.json"
            evidence.write_json(campaign_path, campaign)
            roots = {"roots": {"store": str(root / "store"), "cache": str(root / "cache"),
                               "evidence": str(evidence_root)},
                     "filesystems": {"store": "fixturefs", "cache": "fixturefs",
                                     "evidence": "fixturefs"},
                     "persistent_mounts_verified": True}
            host_environment = {"os": platform.system(), "arch": platform.machine(),
                                "kernel": platform.release(), "machine": platform.machine()}
            profile = {"name": "quick", "min_samples_per_lane": 4, "min_seconds": 0,
                       "max_seconds_per_arm": 10, "pairs": 1, "order": ["AB"],
                       "warmup_operations": 4, "total_pass": False, "case_set": ["S1"]}
            plan = {"case": "S1", "rows": 64, "payload_bytes": 256, "concurrency": 2,
                    "required_operations_per_lane": 8, "warmup_operations": 4,
                    "minimum_seconds": 0, "max_seconds_per_arm": 10,
                    "buckets": ("write", "all"), "minimum_samples_per_bucket": 4,
                    "setup_insert_chunks": [{"sql": "INSERT INTO bench VALUES (0,'x')",
                                             "row_count": 1, "request_bytes": 100}]}
            frozen = {}
            for arm in ("A", "B"):
                binary = root / (arm + ".bin")
                binary.write_bytes(b"binary-" + arm.encode())
                frozen[arm] = {"path": str(binary), "sha256": perf_contract.digest(binary)}
            source_sha = "0" * 64
            campaign_sha = campaign_module.campaign_identity(campaign)
            contract_sha = perf_contract.contract_identity()["sha256sums_sha256"]
            workload_sha = perf_contract.digest(perf_contract.CONTRACT_ROOT / "workloads.json")
            with patch.object(issue8_runner.runner, "Worker", FakeProcessWorker), \
                    patch.object(issue8_runner.evidence_module, "ResourceSampler", FakeSampler):
                for arm in ("A", "B"):
                    report = issue8_runner.execute_run(
                        campaign, campaign_path, arm, "S1", 2, 0, "AB", profile, plan,
                        frozen[arm], roots, contract_sha, workload_sha, campaign_sha,
                        root / "metrics", 5, source_sha, host_environment)
                    self.assertFalse(report["core_failed"], report.get("failure"))
            artifacts = {}
            for arm in ("A", "B"):
                directory = evidence_root / "artifacts" / arm
                directory.mkdir(parents=True)
                target = directory / ("frozen-" + arm + ".bin")
                target.write_bytes(Path(frozen[arm]["path"]).read_bytes())
                artifacts[arm] = {"path": str(target), "sha256": frozen[arm]["sha256"]}
            evidence.write_json(evidence_root / "campaign-identity.json", {
                "schema": 1, "campaign_sha256": campaign_sha, "contract_sha256": contract_sha,
                "workload_sha256": workload_sha, "profile": "quick", "tier": "G1",
                "source_before_sha256": source_sha, "source_after_sha256": source_sha,
                "source_unchanged": True, "arms": artifacts,
                "metrics_snapshot_requested": True, "paired_order": ["AB"]})
            gate_report = gate._evaluate_arithmetic_for_tests(
                evidence_root, tier="G1", profile_name="quick", policy=profile)
            self.assertEqual(gate_report["status"], "PHASE_PASS")
            self.assertEqual(gate_report["sections"]["resources"]["status"], "PASS")
            self.assertEqual(gate_report["sections"]["attribution"]["status"], "PASS")
            for run in sorted((evidence_root / "runs").iterdir()):
                resources = json.loads((run / "resources.json").read_text())
                self.assertEqual([process["lane"] for process in resources["processes"]],
                                 [0, 1])
                for process in resources["processes"]:
                    self.assertIn(process["pid"],
                                  {pid for sample in resources["sampler"]["samples"]
                                   for pid in sample["pids"]})
                environment = json.loads((run / "environment.json").read_text())
                self.assertLessEqual(environment["run_started_at_unix_ms"],
                                     environment["timed_started_at_unix_ms"])
                self.assertLessEqual(environment["timed_started_at_unix_ms"],
                                     environment["timed_completed_at_unix_ms"])
                self.assertLessEqual(environment["timed_completed_at_unix_ms"],
                                     environment["run_completed_at_unix_ms"])

    def test_root_helper_accepts_validation_result(self):
        nested = {"roots": {"store": "/s", "cache": "/c", "evidence": "/e"},
                  "filesystems": {"store": "ext4"}, "persistent_mounts_verified": True}
        self.assertEqual(issue8_runner._root(nested, "store"), Path("/s"))
        self.assertEqual(issue8_runner._root(nested, "evidence"), Path("/e"))
        self.assertEqual(issue8_runner._root({"store": "/s"}, "store"), Path("/s"))

    def test_database_identifiers_are_stable_and_lane_scoped(self):
        first = issue8_runner.database_identifier("camp", "S1", 2, 0)
        second = issue8_runner.database_identifier("camp", "S1", 2, 0)
        other = issue8_runner.database_identifier("camp", "S1", 2, 1)
        self.assertEqual(first, second)
        self.assertNotEqual(first, other)
        self.assertEqual(first["namespace"]["database"], "db")


if __name__ == "__main__":
    unittest.main()
