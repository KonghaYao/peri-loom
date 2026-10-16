import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
SKILL = SCRIPTS.parent
sys.path.insert(0, str(SCRIPTS))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import jsonio
import stats
from compare import compare
from contract import template, validate
from mock_gateway import MockGateway
from model import make_rows, oracle
from oracle_manifest import build_manifest, verify_manifest
from run_platform_queries import DEFAULT_CONCURRENCY, MAX_CONCURRENCY
from run_platform_import import MAX_STATEMENT_BYTES, run as run_import
from run_platform_queries import MAX_CONCURRENCY, MAX_SECONDS, execute
from sql import APPROVED_DDL

CREDENTIAL = "q" * 40
DATABASE = "tenant/e01queries/branch"


class ManifestTests(unittest.TestCase):
    def test_manifest_matches_brute_force_oracle_on_small_model(self):
        for per_family in (2, 4, 8):
            config = template()
            manifest = build_manifest(config, per_family, config["queries"]["page_size"],
                                      300000, 50000)
            rows = make_rows(config)
            self.assertEqual(len(manifest["operations"]), per_family * 4)
            for entry in manifest["operations"]:
                self.assertEqual(entry["expected"], oracle(rows, entry), entry["op_id"])
            self.assertTrue(verify_manifest(config, manifest))
            self.assertEqual(manifest["scan"]["rows_scanned"], config["distribution"]["rows"])

    def test_manifest_is_deterministic_and_tamper_evident(self):
        config = template()
        first = build_manifest(config, 2, config["queries"]["page_size"], 300000, 50000)
        second = build_manifest(config, 2, config["queries"]["page_size"], 300000, 50000)
        self.assertEqual(first["manifest_sha256"], second["manifest_sha256"])
        tampered = json.loads(json.dumps(first))
        tampered["operations"][0]["expected"] = []
        with self.assertRaises(ValueError):
            verify_manifest(config, tampered)
        other = template()
        other["seed"] += 1
        with self.assertRaises(ValueError):
            verify_manifest(other, first)

    def test_manifest_refuses_when_depth_budget_is_insufficient(self):
        config = template()
        with self.assertRaisesRegex(ValueError, "manifest_depth_budget_exceeded"):
            build_manifest(config, 2, config["queries"]["page_size"], 900000, 3)


class NoDeletionTests(unittest.TestCase):
    def test_skill_never_issues_drop_and_ddl_fails_closed(self):
        for path in sorted(SCRIPTS.glob("*.py")):
            text = path.read_text()
            self.assertNotIn("DROP TABLE", text.upper(), path.name)
            self.assertNotIn("reset-fixture", text, path.name)
        self.assertEqual(len(APPROVED_DDL), 2)
        for statement in APPROVED_DDL:
            self.assertTrue(statement.startswith("CREATE TABLE e01_records")
                            or statement.startswith("CREATE INDEX e01_records_group_time_id"))
            self.assertNotIn("IF NOT EXISTS", statement.upper())


class StatsTests(unittest.TestCase):
    def test_quantiles_require_samples_and_nulls_are_explicit(self):
        small = stats.quantiles([1.0, 2.0, 3.0])
        self.assertEqual(small["samples"], 3)
        self.assertEqual(small["p50"], 2.0)
        self.assertIsNone(small["p95"])
        self.assertIsNone(small["p99"])
        self.assertFalse(small["sufficient"]["p95"])
        large = stats.quantiles([float(value) for value in range(1000)])
        self.assertIsNotNone(large["p95"])
        self.assertIsNotNone(large["p99"])
        self.assertTrue(all(large["sufficient"].values()))

    def test_summary_separates_outcomes_and_missing_ops(self):
        operations = [{"op_id": f"point:{index}", "family": "point"} for index in range(3)]
        events = [{"op_id": "point:0", "outcome": "success", "planned_ns": 0,
                   "dispatched_ns": 1000000, "completed_ns": 3000000},
                  {"op_id": "point:1", "outcome": "unknown", "planned_ns": 0,
                   "dispatched_ns": 2000000, "completed_ns": 5000000}]
        summary = stats.summarize(events, operations, ("point",))["point"]
        self.assertEqual(summary["planned_ops"], 3)
        self.assertEqual(summary["reported_ops"], 2)
        self.assertEqual(summary["missing_ops"], 1)
        self.assertEqual(summary["outcomes"], {"success": 1, "unknown": 1})
        self.assertEqual(summary["service_ms"]["p50"], 2.0)
        self.assertEqual(summary["end_to_end_ms"]["max"], 5.0)
        # 最近秩分位：两个样本的 p50 取下秩。
        self.assertEqual(summary["dispatch_lag_ms"]["p50"], 1.0)
        self.assertFalse(stats.slo_state(False)["approved"])
        self.assertIsNone(stats.slo_state(False)["comparison"])


class ImportGuardTests(unittest.TestCase):
    def args(self, **overrides):
        values = {"config": "/nonexistent", "out_dir": "/nonexistent", "database": DATABASE,
                  "endpoint": None, "batch_size": 250, "commit_rows": 10000, "max_seconds": 60.0,
                  "min_free_disk_bytes": 1 << 30, "storage_dir": None, "timeout_seconds": 1.0}
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_statement_byte_ceiling_is_pinned_to_measurement(self):
        self.assertEqual(MAX_STATEMENT_BYTES, 61_257)

    def test_budgets_refuse_before_any_network_use(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "config.json"
            jsonio.write_new(config, template())
            for overrides in ({"batch_size": 0}, {"batch_size": 1001}, {"commit_rows": 10},
                              {"max_seconds": 0.0}, {"max_seconds": 86_401.0},
                              {"min_free_disk_bytes": -1}):
                with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                    run_import(self.args(config=str(config), **overrides))


class QueryRunnerTests(unittest.TestCase):
    def build(self, root, per_family="2"):
        config = root / "config.json"
        manifest = root / "manifest.json"
        jsonio.write_new(config, template())
        completed = subprocess.run(
            [sys.executable, "-B", str(SCRIPTS / "e01.py"), "manifest", "--config", str(config),
             "--per-family", per_family, "--output", str(manifest)], capture_output=True, text=True)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        return config, manifest

    def run_runner(self, gateway, extra=()):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, manifest = self.build(root)
            completed = subprocess.run(
                [sys.executable, "-B", str(SCRIPTS / "run_platform_queries.py"),
                 "--config", str(config), "--manifest", str(manifest),
                 "--out-dir", str(root / "run"), "--database", DATABASE, *extra],
                capture_output=True, text=True,
                env={**os.environ, "PERI_ENDPOINT": gateway.endpoint, "PERI_API_KEY": CREDENTIAL})
            report_path = root / "run/queries.json"
            return completed, (jsonio.load(report_path) if report_path.exists() else None)

    def test_wrong_rows_fail_closed_without_fabricating_success(self):
        gateway = MockGateway(CREDENTIAL)
        try:
            completed, report = self.run_runner(gateway)
            self.assertEqual(completed.returncode, 2, completed.stderr)
            self.assertEqual(report["correctness"], "FAIL_STOP")
            self.assertGreater(report["oracle"]["mismatched"], 0)
            self.assertEqual(report["oracle"]["source"], "offline_manifest")
            self.assertEqual(report["performance"], "NOT MEASURED")
            self.assertEqual(report["slo"]["approved"], False)
            self.assertIsNone(report["slo"]["comparison"])
            self.assertEqual(report["sessions_closed"], report["sessions_opened"])
            self.assertEqual(report["close_failures"], [])
            families = report["families"]
            self.assertEqual(sum(entry["reported_ops"] for entry in families.values()), report["ops"])
            self.assertNotIn(CREDENTIAL, json.dumps(report))
        finally:
            gateway.close()

    def test_unknown_stops_dispatch_and_never_replays(self):
        gateway = MockGateway(CREDENTIAL)
        gateway.override("/v1/sql/query", lambda body: gateway.default("/v1/sql/query", body)
                         if body["sql"].startswith("SELECT count(*)")
                         else (_ for _ in ()).throw(RuntimeError("drop")))
        try:
            # 单并发下 Unknown 语义最清晰：恰好一条未获回执，其余不再派发。
            completed, report = self.run_runner(gateway, ("--concurrency", "1"))
            self.assertEqual(completed.returncode, 2)
            self.assertEqual(report["outcomes"].get("unknown"), 1)
            self.assertGreater(report["oracle"]["missing_ops"], 0)
            self.assertEqual(report["correctness"], "INCOMPLETE")
        finally:
            gateway.close()

    def test_runner_budgets_are_refused(self):
        gateway = MockGateway(CREDENTIAL)
        try:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                config, manifest = self.build(root)
                for extra, code in ((("--concurrency", str(MAX_CONCURRENCY + 1)), "concurrency_budget_exceeded"),
                                    (("--max-seconds", str(MAX_SECONDS + 1)), "time_budget_exceeded")):
                    completed = subprocess.run(
                        [sys.executable, "-B", str(SCRIPTS / "run_platform_queries.py"),
                         "--config", str(config), "--manifest", str(manifest),
                         "--out-dir", str(root / ("run-" + code)), "--database", DATABASE, *extra],
                        capture_output=True, text=True,
                        env={**os.environ, "PERI_ENDPOINT": gateway.endpoint,
                             "PERI_API_KEY": CREDENTIAL})
                    self.assertEqual(completed.returncode, 2)
                    self.assertIn(code, completed.stderr)
        finally:
            gateway.close()


if __name__ == "__main__":
    unittest.main()


class ConcurrencyTests(unittest.TestCase):
    def build(self, root, per_family="2"):
        config = root / "config.json"
        manifest = root / "manifest.json"
        jsonio.write_new(config, template())
        completed = subprocess.run(
            [sys.executable, "-B", str(SCRIPTS / "e01.py"), "manifest", "--config", str(config),
             "--per-family", per_family, "--output", str(manifest)], capture_output=True, text=True)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        return config, manifest

    def run_runner(self, gateway, root, extra=()):
        config, manifest = self.build(root)
        completed = subprocess.run(
            [sys.executable, "-B", str(SCRIPTS / "run_platform_queries.py"),
             "--config", str(config), "--manifest", str(manifest),
             "--out-dir", str(root / ("run-" + str(abs(hash(extra)) % 100000))), "--database", DATABASE,
             *extra],
            capture_output=True, text=True,
            env={**os.environ, "PERI_ENDPOINT": gateway.endpoint, "PERI_API_KEY": CREDENTIAL})
        reports = sorted((root).glob("run-*/queries.json"))
        return completed, (jsonio.load(reports[-1]) if reports else None)

    def test_default_concurrency_is_one_hundred_and_configurable(self):
        from run_platform_queries import DEFAULT_CONCURRENCY, MAX_CONCURRENCY
        self.assertEqual(DEFAULT_CONCURRENCY, 100)
        self.assertEqual(MAX_CONCURRENCY, 256)
        gateway = MockGateway(CREDENTIAL)
        try:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                # 请求数(8) 少于并发(100)：只开与请求数相同的会话。
                completed, report = self.run_runner(gateway, root)
                self.assertEqual(report["concurrency"], 100)
                self.assertEqual(report["sessions_requested"], report["ops"])
                self.assertEqual(report["sessions_opened"], report["ops"])
                self.assertFalse(report["partial_admission"])
                self.assertEqual(report["sessions_closed"], report["sessions_opened"])
                # 除池内会话外还有一个只读探针会话（回读行数），同样被显式关闭。
                self.assertEqual(len(set(gateway.issued_sessions)), report["sessions_opened"] + 1)
                self.assertEqual(len(gateway.closed_sessions), report["sessions_opened"] + 1)
                self.assertGreater(report["peak_in_flight"], 0)
                self.assertLessEqual(report["peak_in_flight"], report["sessions_opened"])
        finally:
            gateway.close()

    def test_single_concurrency_remains_available(self):
        gateway = MockGateway(CREDENTIAL)
        try:
            with tempfile.TemporaryDirectory() as directory:
                completed, report = self.run_runner(gateway, Path(directory), ("--concurrency", "1"))
                self.assertEqual(report["concurrency"], 1)
                self.assertEqual(report["sessions_opened"], 1)
                self.assertEqual(report["sessions_closed"], 1)
                self.assertEqual(report["peak_in_flight"], 1)
        finally:
            gateway.close()

    def test_partial_admission_is_reported_not_hidden(self):
        gateway = MockGateway(CREDENTIAL, max_open_sessions=2)
        try:
            with tempfile.TemporaryDirectory() as directory:
                completed, report = self.run_runner(gateway, Path(directory), ("--concurrency", "5"))
                self.assertEqual(report["sessions_requested"], 5)
                self.assertEqual(report["sessions_opened"], 2)
                self.assertTrue(report["partial_admission"])
                self.assertEqual(len(report["open_failures"]), 3)
                self.assertEqual(report["open_failures"][0]["code"], "sql_unavailable")
                self.assertNotEqual(report["sessions_opened"], report["concurrency"])
        finally:
            gateway.close()

    def test_timing_containment_and_denominators(self):
        gateway = MockGateway(CREDENTIAL)
        try:
            with tempfile.TemporaryDirectory() as directory:
                completed, report = self.run_runner(gateway, Path(directory), ("--concurrency", "2"))
                timing_block = report["timing"]
                self.assertEqual(timing_block["unit"], "ms")
                self.assertIn("prepare", timing_block["phases"])
                self.assertIn("query", timing_block["phases"])
                self.assertIn("close", timing_block["phases"])
                self.assertGreaterEqual(timing_block["unaccounted_ms"], 0)
                self.assertEqual(report["throughput"]["denominator_ms"],
                                 timing_block["phases"]["query"])
                for entry in report["families"].values():
                    self.assertIn("success_ops_per_second", entry)
                    self.assertIn("service_ms", entry)
                    self.assertIn("dispatch_lag_ms", entry)
                self.assertEqual(report["resources"]["status"], "NOT MEASURED")
                self.assertIsNone(report["cpu_seconds_per_operation"])
                self.assertEqual(report["performance"], "NOT MEASURED")
        finally:
            gateway.close()


class CompareTests(unittest.TestCase):
    def write(self, path, report):
        jsonio.write_new(path, report)
        return str(path)

    def platform_report(self, digest_value, success, phase_ms, wall_ms, concurrency=100,
                        requested=100, opened=100):
        return {"version": "e01-draft-v1", "target": "platform", "adapter": "public_http_v1",
                "binding": "literal", "config_sha256": digest_value, "manifest_sha256": "m" * 64,
                "concurrency": concurrency, "sessions_requested": requested, "sessions_opened": opened,
                "correctness": "ORACLE_VERIFIED", "outcomes": {"success": success},
                "families": {"point": {"outcomes": {"success": success}}},
                "timing": {"unit": "ms", "phases": {"query": phase_ms}, "total_wall_ms": wall_ms}}

    def test_comparable_scaling_ratio_per_session_efficiency(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = self.write(root / "base.json", self.platform_report("c" * 64, 100, 1000.0, 1500.0, 1, 1, 1))
            candidate = self.write(root / "candidate.json",
                                   self.platform_report("c" * 64, 800, 2000.0, 2600.0, 100, 100, 100))
            result = compare(base, candidate)
            self.assertEqual(result["comparability"], "COMPARABLE")
            self.assertFalse(result["cross_boundary"])
            self.assertEqual(result["scaling"]["query_phase_ratio"], 2.0)
            self.assertEqual(result["scaling"]["wall_ratio"], round(2600.0 / 1500.0, 4))
            self.assertEqual(result["scaling"]["per_session_efficiency_ratio"], round((800 / 100) / (100 / 1), 4))
            self.assertGreater(result["scaling"]["success_ops_per_second_ratio"], 0)

    def test_mismatched_or_partial_or_unknown_yields_no_formal_ratio(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            good = self.write(root / "good.json", self.platform_report("c" * 64, 100, 1000.0, 1500.0))
            other = self.write(root / "other.json", self.platform_report("d" * 64, 100, 1000.0, 1500.0))
            mismatch = compare(good, other)
            self.assertEqual(mismatch["comparability"], "NOT_COMPARABLE")
            self.assertIsNone(mismatch["scaling"])
            self.assertIn("config_sha256_mismatch", mismatch["reasons"])
            partial = self.write(root / "partial.json",
                                 self.platform_report("c" * 64, 100, 1000.0, 1500.0, 100, 100, 60))
            self.assertEqual(compare(good, partial)["comparability"], "EXPLORATION_ONLY")
            self.assertIsNone(compare(good, partial)["scaling"])
            unknown = self.platform_report("c" * 64, 100, 1000.0, 1500.0)
            unknown["outcomes"] = {"success": 99, "unknown": 1}
            self.assertEqual(compare(good, self.write(root / "rejected.json",
                             {**unknown, "outcomes": {"success": 99, "known_failure": 1}}))["comparability"],
                             "EXPLORATION_ONLY")
            unknown_path = self.write(root / "unknown.json", unknown)
            self.assertIn("candidate_non_success_outcomes_present", compare(good, unknown_path)["reasons"])
            self.assertIsNone(compare(good, str(root / "missing.json"))["scaling"])
            self.assertEqual(compare(good, str(root / "missing.json"))["comparability"], "NOT_MEASURED")

    def test_cross_target_comparison_is_labelled_as_boundary_difference(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            native = self.write(root / "native.json", {
                "version": "e01-draft-v1", "target": "native", "config_sha256": "c" * 64,
                "correctness": "ORACLE_VERIFIED", "outcomes": {"success": 100},
                "families": {"point": {"outcomes": {"success": 100}}},
                "harness": {"target": "native", "binding": "literal", "adapter": "turso_core-embedded"}})
            platform = self.write(root / "platform.json",
                                  self.platform_report("c" * 64, 100, 1000.0, 1500.0))
            result = compare(native, platform)
            self.assertTrue(result["cross_boundary"])
            self.assertEqual(result["comparability"], "NOT_COMPARABLE")
            self.assertIsNone(result["scaling"])
            self.assertIn("cross_target_boundary", result["reasons"])
            self.assertIn("纯引擎收益", result["note"])
