import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import jsonio
import platform_client as platform
import verify
from contract import template, validate
from mock_gateway import MockGateway
from model import generate

CREDENTIAL = "k" * 40
DATABASE = "tenant/e01/branch"


class ConnectionModeTests(unittest.TestCase):
    """连接复用协议测试：真实统计连接创建/复用，而不是只看配置。"""

    def setUp(self):
        self.gateway = MockGateway(CREDENTIAL)

    def tearDown(self):
        self.gateway.close()

    def run_calls(self, mode, count, threads=1):
        import threading
        client = platform.PlatformClient(self.gateway.endpoint, CREDENTIAL, connection_mode=mode)
        errors = []
        def worker(base):
            try:
                session, _ = platform.open_session(client, DATABASE, f"open-{base}")
                for index in range(count):
                    session.query("SELECT 1", f"q-{base}-{index}")
            except platform.PlatformFailure as failure:
                errors.append(failure.code)
        workers = [threading.Thread(target=worker, args=(index,)) for index in range(threads)]
        for item in workers:
            item.start()
        for item in workers:
            item.join()
        stats = client.connection_stats()
        client.close_connections()
        return stats, errors

    def test_reuse_uses_one_connection_per_worker(self):
        stats, errors = self.run_calls("reuse", 5, threads=1)
        self.assertEqual(errors, [])
        self.assertEqual(stats["connections_created"], 1)
        self.assertEqual(stats["requests_sent"], 6)
        self.assertEqual(self.gateway.accepted_connections, 1)

    def test_reuse_does_not_share_a_stream_between_workers(self):
        stats, errors = self.run_calls("reuse", 3, threads=4)
        self.assertEqual(errors, [])
        self.assertEqual(stats["connections_created"], 4)
        self.assertEqual(self.gateway.accepted_connections, 4)

    def test_per_request_mode_keeps_creating_connections(self):
        stats, errors = self.run_calls("per-request", 3, threads=1)
        self.assertEqual(errors, [])
        self.assertEqual(stats["connections_created"], 0)  # urllib 路径不计数
        self.assertGreaterEqual(self.gateway.accepted_connections, 4)

    def test_server_closed_connection_yields_unknown_without_resend(self):
        client = platform.PlatformClient(self.gateway.endpoint, CREDENTIAL, connection_mode="reuse")
        session, _ = platform.open_session(client, DATABASE, "open-1")
        session.query("SELECT 1", "q-0")
        before = self.gateway.count("/v1/sql/query")
        self.gateway.override("/v1/sql/query", lambda body: (_ for _ in ()).throw(RuntimeError("reset")))
        with self.assertRaises(platform.PlatformFailure) as caught:
            session.query("SELECT 1", "q-1")
        self.assertEqual(caught.exception.code, "outcome_unknown")
        self.assertEqual(self.gateway.count("/v1/sql/query"), before + 1)  # 未重发
        # 会话已按契约停用：后续操作必须显式开新会话（并因此新建连接），不得复用旧会话。
        self.assertTrue(session.unusable)
        self.gateway.behaviours.clear()
        created_before = client.connection_stats()["connections_created"]
        fresh, _ = platform.open_session(client, DATABASE, "open-2")
        fresh.query("SELECT 1", "q-2")
        self.assertEqual(client.connection_stats()["connections_created"], created_before + 1)
        client.close_connections()

    def test_half_response_is_unknown_and_never_replayed(self):
        client = platform.PlatformClient(self.gateway.endpoint, CREDENTIAL, connection_mode="reuse")
        session, _ = platform.open_session(client, DATABASE, "open-1")
        self.gateway.override("/v1/sql/query", lambda body: "half")
        before = self.gateway.count("/v1/sql/query")
        with self.assertRaises(platform.PlatformFailure) as caught:
            session.query("SELECT 1", "q-1")
        self.assertEqual(caught.exception.code, "outcome_unknown")
        self.assertEqual(self.gateway.count("/v1/sql/query"), before + 1)

    def test_close_connections_is_explicit_and_idempotent(self):
        client = platform.PlatformClient(self.gateway.endpoint, CREDENTIAL, connection_mode="reuse")
        platform.open_session(client, DATABASE, "open-1")
        client.close_connections()
        client.close_connections()
        self.assertEqual(client.connection_stats()["connections_created"], 1)


class ClientTests(unittest.TestCase):
    def setUp(self):
        self.gateway = MockGateway(CREDENTIAL)
        self.client = platform.PlatformClient(self.gateway.endpoint, CREDENTIAL)

    def tearDown(self):
        self.gateway.close()

    def open_session(self):
        session, _ = platform.open_session(self.client, DATABASE, "open-1")
        return session

    def test_round_trip_preserves_integers_and_bearer_header(self):
        self.gateway.rows_for_sql["SELECT 1"] = [[9007199254740993, "text", None]]
        session = self.open_session()
        rows, timings = session.query("SELECT 1", "query-1")
        self.assertEqual(rows, [[9007199254740993, "text", None]])
        self.assertLessEqual(timings["planned_ns"], timings["dispatched_ns"])
        self.assertLessEqual(timings["dispatched_ns"], timings["completed_ns"])
        session.execute("BEGIN", "execute-1")
        session.close("close-1")
        opened = self.gateway.requests[0]
        self.assertEqual(opened["authorization"], "Bearer " + CREDENTIAL)
        self.assertEqual(opened["request_id"], "open-1")
        self.assertEqual(opened["body"]["database"],
                         {"namespace": {"tenant": "tenant", "database": "e01"}, "branch": "branch"})
        self.assertTrue(all(request["request_id"] == request["body"]["request_id"]
                            for request in self.gateway.requests))
        self.assertEqual(self.gateway.count("/v1/sql/close"), 1)

    def test_endpoint_rules_match_the_published_sdk(self):
        rejected = {"http://localhost:8080": "https_required_outside_loopback",
                    "http://example.test:80": "https_required_outside_loopback",
                    "http://127.0.0.1.evil.test": "https_required_outside_loopback",
                    "https://token@host.example/": "invalid_endpoint",
                    "https://host.example/?query=1": "invalid_endpoint",
                    "https://host.example/sql": "invalid_endpoint",
                    "ftp://host.example": "invalid_endpoint",
                    "host.example:1": "invalid_endpoint"}
        for endpoint, code in rejected.items():
            with self.subTest(endpoint=endpoint), self.assertRaises(platform.PlatformFailure) as caught:
                platform.PlatformClient(endpoint, CREDENTIAL)
            self.assertEqual(caught.exception.code, code)
        for endpoint in ("http://127.0.0.1:1/", "http://[::1]:1", "https://host.example"):
            with self.subTest(endpoint=endpoint):
                platform.PlatformClient(endpoint, CREDENTIAL)
        self.assertIn("127.0.0.1", repr(platform.PlatformClient("http://127.0.0.1:1/", CREDENTIAL)))

    def test_credentials_load_from_environment_or_private_file_only(self):
        self.assertEqual(platform.load_credential({"PERI_API_KEY": CREDENTIAL}), CREDENTIAL)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "credential"
            path.write_text(CREDENTIAL + "\n")
            self.assertEqual(platform.load_credential({"PERI_CREDENTIAL_FILE": str(path)}), CREDENTIAL)
        with self.assertRaises(platform.PlatformFailure) as caught:
            platform.load_credential({"PERI_CREDENTIAL_FILE": "/nonexistent/credential"})
        self.assertEqual(caught.exception.code, "credential_file_unreadable")
        for environment in ({}, {"PERI_API_KEY": "short"}, {"PERI_API_KEY": "a" * 31},
                            {"PERI_API_KEY": "a" * 32 + "\x00"}):
            with self.subTest(environment=environment), self.assertRaises(platform.PlatformFailure):
                platform.load_credential(environment)

    def test_envelope_violations_become_unknown_without_replay(self):
        cases = {
            "wrong-request-id": lambda body: (200, self.gateway.envelope("other", [])),
            "version": lambda body: (200, {"version": 2, "request_id": body["request_id"],
                                           "ok": True, "result": []}),
            "ok-missing": lambda body: (200, {"version": 1, "request_id": body["request_id"]}),
            "error-with-result": lambda body: (200, self.gateway.envelope(
                body["request_id"], [], ok=False, error={"code": "sql_error", "message": "x"})),
            "not-json": lambda body: (200, "not-an-envelope"),
        }
        for name, behaviour in cases.items():
            with self.subTest(name=name):
                self.gateway.behaviours.clear()
                self.gateway.override("/v1/sql/query", behaviour)
                session = self.open_session()
                before = self.gateway.count("/v1/sql/query")
                with self.assertRaises(platform.PlatformFailure) as caught:
                    session.query("SELECT 1", "query-1")
                self.assertEqual(caught.exception.code, "outcome_unknown")
                self.assertEqual(self.gateway.count("/v1/sql/query"), before + 1)

    def test_transport_failure_is_unknown_and_never_retried(self):
        session = self.open_session()
        self.gateway.override("/v1/sql/query", lambda body: (_ for _ in ()).throw(RuntimeError("boom")))
        with self.assertRaises(platform.PlatformFailure) as caught:
            session.query("SELECT 1", "query-1")
        self.assertEqual(caught.exception.code, "outcome_unknown")
        self.assertIsNotNone(caught.exception.timings)
        self.assertLessEqual(caught.exception.timings["planned_ns"],
                             caught.exception.timings["completed_ns"])
        self.assertEqual(self.gateway.count("/v1/sql/query"), 1)
        self.assertTrue(session.unusable)
        with self.assertRaises(platform.PlatformFailure) as poisoned:
            session.query("SELECT 2", "query-2")
        self.assertEqual(poisoned.exception.code, "session_unusable")
        self.assertEqual(self.gateway.count("/v1/sql/query"), 1)

    def test_predispatch_rejection_has_no_timings(self):
        session = self.open_session()
        with self.assertRaises(platform.PlatformFailure) as caught:
            session.query("SELECT 1", "bad label")
        self.assertEqual(caught.exception.code, "invalid_request_id")
        self.assertIsNone(caught.exception.timings)
        self.assertEqual(self.gateway.count("/v1/sql/query"), 0)

    def test_settled_failure_keeps_flags_and_code(self):
        self.gateway.override("/v1/sql/query", lambda body: (400, self.gateway.envelope(
            body["request_id"], None, ok=False,
            error={"code": "sql_error", "message": "no such table",
                   "execution_completed": True, "may_have_committed": True})))
        session = self.open_session()
        with self.assertRaises(platform.PlatformFailure) as caught:
            session.query("SELECT missing", "query-1")
        self.assertEqual(caught.exception.code, "sql_error")
        self.assertEqual(caught.exception.flags,
                         {"execution_completed": True, "may_have_committed": True})
        self.assertIsNotNone(caught.exception.timings)
        self.assertFalse(session.unusable)

    def test_session_identifier_and_credential_are_never_echoed(self):
        self.gateway.session = "short"
        with self.assertRaises(platform.PlatformFailure) as caught:
            platform.open_session(self.client, DATABASE, "open-1")
        self.assertEqual(caught.exception.code, "outcome_unknown")
        self.assertNotIn(CREDENTIAL, repr(self.client))
        self.assertNotIn(CREDENTIAL, str(caught.exception))
        self.assertNotIn(CREDENTIAL, json.dumps(caught.exception.flags, default=str))


class RunnerTests(unittest.TestCase):
    """平台运行器全链路：使用模拟网关，结论只覆盖适配器机制。"""

    def test_runner_reports_failure_when_the_target_returns_wrong_rows(self):
        gateway = MockGateway(CREDENTIAL)
        gateway.rows_for_sql["SELECT count(*) FROM e01_records"] = [
            [generate(validate(template()))["logical_rows"]]]
        try:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                config = root / "config.json"
                self.assertEqual(subprocess.run(
                    [sys.executable, "-B", str(Path(__file__).resolve().parents[1]
                                               / "scripts/e01.py"), "template",
                     "--output", str(config)], capture_output=True).returncode, 0)
                environment = {**os.environ, "PERI_ENDPOINT": gateway.endpoint,
                               "PERI_API_KEY": CREDENTIAL, "PYTHONPATH": ""}
                completed = subprocess.run(
                    [sys.executable, "-B", str(Path(__file__).resolve().parents[1]
                                               / "scripts/run_platform.py"),
                     "--config", str(config), "--out-dir", str(root / "run"),
                     "--database", DATABASE], capture_output=True, text=True, env=environment)
                self.assertEqual(completed.returncode, 2, completed.stderr)
                report = jsonio.load(root / "run/report.json")
                self.assertEqual(report["harness"]["target"], "platform")
                self.assertEqual(report["correctness"], "FAIL_STOP")
                # 只有空的终止页会与 oracle 相符；其余全部必须判为 mismatch。
                self.assertEqual(report["oracle_mismatches"],
                                 report["ops_total"] - report["oracle_verified_ops"])
                self.assertGreater(report["oracle_verified_ops"], 0)
                self.assertGreater(report["oracle_mismatches"], report["harness"]["import"]["batches"])
                self.assertEqual(report["performance"], "NOT MEASURED")
                self.assertEqual(report["families"]["point"]["outcomes"]["success"], 16)
                raw = (root / "run/evidence.json").read_text()
                scenario = (root / "run/scenario.json").read_text()
                self.assertNotIn(CREDENTIAL, raw + scenario + json.dumps(report))
                executed = [request["body"]["sql"] for request in gateway.requests
                            if request["path"] == "/v1/sql/execute"]
                self.assertTrue(any(sql.startswith("CREATE TABLE e01_records") for sql in executed))
                self.assertTrue(any(sql.startswith("CREATE INDEX e01_records_group_time_id")
                                    for sql in executed))
                self.assertEqual(executed.count("BEGIN"), 1)
                self.assertEqual(executed.count("COMMIT"), 1)
                self.assertEqual(gateway.count("/v1/sql/close"), 1)
                labels = verify.labels(verify.build_operations(generate(validate(template())),
                                                               "literal"))
                self.assertTrue(all(len(label) <= 64 for label in labels.values()))
        finally:
            gateway.close()

    def test_missing_configuration_refuses_before_any_request(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config.json"
            subprocess.run([sys.executable, "-B", str(Path(__file__).resolve().parents[1]
                                                     / "scripts/e01.py"), "template",
                            "--output", str(config)], capture_output=True)
            environment = {key: value for key, value in os.environ.items()
                           if key not in ("PERI_API_KEY", "PERI_CREDENTIAL_FILE", "PERI_ENDPOINT")}
            completed = subprocess.run(
                [sys.executable, "-B", str(Path(__file__).resolve().parents[1]
                                           / "scripts/run_platform.py"),
                 "--config", str(config), "--out-dir", str(root / "run"),
                 "--database", DATABASE], capture_output=True, text=True, env=environment)
            self.assertEqual(completed.returncode, 2)
            self.assertIn("missing_endpoint", completed.stderr)
            self.assertFalse((root / "run").exists())


if __name__ == "__main__":
    unittest.main()
