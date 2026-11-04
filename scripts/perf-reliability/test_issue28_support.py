"""Offline tests for the Issue 28 harness.

These tests only exercise wire encoding/decoding, statistics, and the report
contract. They are not performance evidence and never open a socket.
"""
import json
import pathlib
import struct
import sys
import unittest
from types import SimpleNamespace

PERF_ROOT = pathlib.Path(__file__).resolve().parent
if str(PERF_ROOT) not in sys.path:
    sys.path.insert(0, str(PERF_ROOT))

from issue28 import contract, hrana, report, runner  # noqa: E402
from issue28 import run as issue28_run  # noqa: E402
from issue28.fixture import DatasetCheck, Fixture  # noqa: E402
from issue28.hrana import FatalTransportError, Target  # noqa: E402
from issue28.runner import ConcurrencyMetrics, Measurement, RunResult  # noqa: E402


def pb_bytes(number, value):
    return hrana.field_bytes(number, value)


def pb_varint(number, value):
    return hrana.field_varint(number, value)


def pb_double(number, value):
    return hrana.varint((number << 3) | 1) + struct.pack("<d", value)


def pb_row(text):
    return pb_bytes(1, pb_bytes(4, text.encode()))


def frame(payload):
    return hrana.varint(len(payload)) + payload


class ProtobufEncodingTests(unittest.TestCase):
    def test_integer_arguments_are_zigzag(self):
        encoded = hrana.encode_value(10)
        fields = hrana.read_fields(encoded)
        self.assertEqual([(field.number, field.wire) for field in fields], [(2, 0)])
        self.assertEqual(fields[0].value, 20)

    def test_pipeline_execute_and_close_shape(self):
        body = hrana.encode_pipeline([
            {"type": "execute", "sql": "SELECT ?", "want_rows": True, "args": (10,)},
            {"type": "close"},
        ])
        top = hrana.read_fields(body)
        self.assertEqual([field.number for field in top], [2, 2])

        execute_request = hrana.read_fields(top[0].value)
        self.assertEqual([(field.number, field.wire) for field in execute_request], [(2, 2)])
        execute = hrana.read_fields(execute_request[0].value)
        self.assertEqual([(field.number, field.wire) for field in execute], [(1, 2)])
        statement = hrana.read_fields(execute[0].value)
        self.assertEqual(statement[0].number, 1)
        self.assertEqual(statement[0].value, b"SELECT ?")
        self.assertIn((3, 2), [(field.number, field.wire) for field in statement])
        self.assertIn((5, 0), [(field.number, field.wire) for field in statement])

        close = hrana.read_fields(hrana.read_fields(top[1].value)[0].value)
        self.assertEqual(close, [])

    def test_cursor_request_shape(self):
        body = hrana.encode_cursor("SELECT 1", True, ())
        top = hrana.read_fields(body)
        self.assertEqual([(field.number, field.wire) for field in top], [(2, 2)])
        batch = hrana.read_fields(top[0].value)
        self.assertEqual([field.number for field in batch], [1])
        step = hrana.read_fields(batch[0].value)
        self.assertEqual([field.number for field in step], [2])
        statement = hrana.read_fields(step[0].value)
        self.assertEqual(statement[0].value, b"SELECT 1")


class ProtobufResponseTests(unittest.TestCase):
    def _statement(self, rows=(), affected=0, read=0, written=0, query=1.25):
        payload = pb_bytes(1, b"cols")
        for row in rows:
            payload += pb_bytes(2, row)
        payload += pb_varint(3, affected)
        payload += pb_varint(6, read)
        payload += pb_varint(7, written)
        payload += pb_double(8, query)
        return payload

    def _pipeline(self, *results, baton=b"baton"):
        body = pb_bytes(1, baton) if baton else b""
        for result in results:
            body += pb_bytes(3, result)
        return body

    def test_execute_result(self):
        stmt = self._statement(rows=(pb_row("a"), pb_row("b")), affected=1,
                               read=2, written=0)
        stream = pb_bytes(2, pb_bytes(1, stmt))  # StreamResponse.execute -> StatementResult
        body = self._pipeline(pb_bytes(1, stream))  # StreamResult.ok
        reply = hrana.parse_pipeline_protobuf(body)
        self.assertEqual(reply.baton, "baton")
        result = reply.results[0]
        self.assertTrue(result.ok)
        self.assertEqual(result.response_type, "execute")
        self.assertEqual(result.rows, 2)
        self.assertEqual(result.affected_row_count, 1)
        self.assertEqual(result.rows_read, 2)
        self.assertAlmostEqual(result.query_ms, 1.25)

    def test_error_result(self):
        # Hrana 3 proto: `message = 1`, `code = 2` (HRANA_3_SPEC.md appendix).
        error = pb_bytes(1, b"statement result exceeded budget") + pb_bytes(2, b"result_limit")
        body = self._pipeline(pb_bytes(2, error))  # StreamResult.error
        result = hrana.parse_pipeline_protobuf(body).results[0]
        self.assertFalse(result.ok)
        self.assertEqual(result.error_code, "result_limit")
        self.assertEqual(result.error_message, "statement result exceeded budget")

    def test_close_result(self):
        body = self._pipeline(pb_bytes(1, pb_bytes(1, b"")))
        result = hrana.parse_pipeline_protobuf(body).results[0]
        self.assertTrue(result.ok)
        self.assertEqual(result.response_type, "close")

    def test_cursor_frames(self):
        header = frame(pb_bytes(1, b"cursor-baton"))
        rows = frame(pb_bytes(4, pb_row("a"))) + frame(pb_bytes(4, pb_row("b")))
        end = frame(pb_bytes(2, b""))
        reply = hrana.parse_cursor_protobuf(header + rows + end)
        self.assertEqual(reply.baton, "cursor-baton")
        self.assertEqual(reply.rows, 2)
        self.assertEqual(reply.frames, 4)  # header + two rows + step_end
        self.assertEqual(reply.errors, [])


class JsonParsingTests(unittest.TestCase):
    def test_pipeline_reply(self):
        body = json.dumps({
            "baton": "b",
            "results": [
                {"type": "ok", "response": {"type": "execute", "result": {
                    "cols": [], "rows": [[{"type": "integer", "value": "1"}]],
                    "affected_row_count": 0, "rows_read": 0,
                    "rows_written": 0, "query_duration_ms": 0.5,
                }}},
                {"type": "ok", "response": {"type": "close"}},
            ],
        }).encode()
        reply = hrana.parse_pipeline_json(body)
        self.assertEqual(reply.baton, "b")
        self.assertEqual(reply.results[0].rows, 1)
        self.assertEqual(reply.results[0].query_ms, 0.5)
        self.assertEqual(reply.results[1].response_type, "close")

    def test_cursor_reply(self):
        body = b"\n".join([
            b'{"baton":"b"}',
            b'{"type":"step_begin","step":0,"cols":[]}',
            b'{"type":"row","row":[]}',
            b'{"type":"row","row":[]}',
            b'{"type":"step_end","affected_row_count":0,"last_insert_rowid":null}',
        ])
        reply = hrana.parse_cursor_json(body)
        self.assertEqual(reply.baton, "b")
        self.assertEqual(reply.rows, 2)
        self.assertEqual(reply.frames, 5)  # baton header + four entry lines
        self.assertEqual(reply.errors, [])


class RunnerRequestShapeTests(unittest.TestCase):
    def _spec(self, kind, **kwargs):
        return contract.ScenarioSpec(
            key=kind, title=kind, kind=kind,
            protocols=(contract.JSON_PIPELINE,), **kwargs)

    def test_single_execute_closes_by_default(self):
        requests = runner._pipeline_requests(
            self._spec("read"), "SELECT 1", (), True, True)
        self.assertEqual([item["type"] for item in requests], ["execute", "close"])

    def test_baton_execute_keeps_the_stream_open(self):
        spec = self._spec("baton", sql="SELECT 1", want_rows=True, baton_executes=10)
        requests = runner._pipeline_requests(spec, "SELECT 1", (), True, False)
        self.assertEqual([item["type"] for item in requests], ["execute"])
        self.assertEqual(requests[0]["args"], ())

    def test_batch_executes_then_closes(self):
        spec = self._spec("batch", sql="SELECT 1", want_rows=True, batch_size=20)
        requests = runner._pipeline_requests(spec, "SELECT 1", (), True, True)
        self.assertEqual([item["type"] for item in requests], ["execute"] * 20 + ["close"])


class ReportTests(unittest.TestCase):
    def _fixture(self, complete=True, projection=0):
        checks = [DatasetCheck("threads", 201, 201, complete)]
        return Fixture(
            threads=201, messages=8625, content_chars=20177069,
            content_bytes=23505319, focus_thread="thread", focus_messages=8625,
            focus_content_bytes=23505319, list_cwd="/tmp", list_threads=20,
            empty_cwd="/tmp/missing", projection_non_null=1,
            projection_bytes=projection, quotas={"max_concurrent": 16},
            checks=checks,
        )

    def _measurement(self, scenario, protocol, duration, index=1, **overrides):
        values = dict(
            scenario=scenario, protocol=protocol, kind="read", phase="timed",
            round_index=1, sample_index=index, status=200,
            duration_ms=duration, ttfb_ms=duration / 2, request_bytes=1,
            response_bytes=10, rows=1, rows_read=0, rows_written=0,
            affected_row_count=0, query_ms=0.2, outcome="ok",
        )
        values.update(overrides)
        return Measurement(**values)

    def _scenario(self, key, protocols, target_ms=None, kind="read", **kwargs):
        return contract.ScenarioSpec(
            key=key, title=key, kind=kind, protocols=protocols,
            target_ms=target_ms, expected_rows=1, **kwargs)

    def test_median_and_percentile(self):
        self.assertEqual(report._median([3.0, 1.0, 2.0]), 2.0)
        self.assertEqual(report._median([4.0, 1.0, 2.0, 3.0]), 2.5)
        self.assertEqual(report._percentile([1.0, 2.0, 3.0], 95), 3.0)

    def test_verdict_uses_median_and_target(self):
        scenario = self._scenario(
            "F1", (contract.JSON_PIPELINE,), target_ms=10.0)
        result = RunResult()
        for index in range(1, 16):
            result.measurements.append(self._measurement(
                "F1", "json-pipeline", 5.0, index))
        rows = report.summarize(result, [scenario], self._fixture())
        self.assertEqual(rows[0].verdict, "PASS")

        for index in range(1, 9):
            result.measurements[index - 1] = self._measurement(
                "F1", "json-pipeline", 20.0, index)
        rows = report.summarize(result, [scenario], self._fixture())
        self.assertEqual(rows[0].verdict, "FAIL")

    def test_expected_result_limit_is_a_pass_for_r1(self):
        scenario = contract.ScenarioSpec(
            key="R1", title="R1", kind="limit", protocols=(contract.JSON_PIPELINE,),
            target_ms=120.0, expect_result_limit=True)
        result = RunResult()
        for index in range(1, 16):
            result.measurements.append(self._measurement(
                "R1", "json-pipeline", 100.0, index, outcome="expected_limit",
                error_code="result_limit", status=200, rows=0))
        rows = report.summarize(result, [scenario], self._fixture())
        self.assertEqual(rows[0].verdict, "PASS")

    def test_projection_note_and_quota_note(self):
        fixture = self._fixture(projection=0)
        rows = report.summarize(RunResult(), [], fixture)
        markdown = report.render_markdown(
            rows, [], fixture, {"platform_label": "HEAD abc / rust test",
                                "rounds": 3, "warmup": 1, "samples": 5},
            [], "")
        self.assertIn("HEAD abc / rust test", markdown)
        self.assertIn("projection", markdown)
        self.assertIn("max_concurrent=16", markdown)


class ConcurrencyVerdictTests(unittest.TestCase):
    def _measurement(self, index, status=200, outcome="ok", concurrency=16):
        return Measurement(
            scenario="X1.concurrency", protocol="json-pipeline", kind="concurrency",
            phase="timed", round_index=1, sample_index=index, status=status,
            duration_ms=1.0, ttfb_ms=0.5, request_bytes=1, response_bytes=1,
            rows=1, rows_read=0, rows_written=0, affected_row_count=0,
            query_ms=0.1, outcome=outcome, concurrency=concurrency)

    def _items(self, count=100):
        return [self._measurement(index) for index in range(1, count + 1)]

    def _metrics(self, planned=100, **drained):
        values = {"active_permits": 0, "unknown_held_permits": 0, "session_seats": 0}
        values.update(drained)
        return ConcurrencyMetrics(
            level=16, before={}, after={}, drained=values, drain_ms=1.0,
            requests=planned, status_counts={}, error_codes={}, planned=planned)

    def test_complete_success_passes(self):
        self.assertEqual(report._concurrency_verdict(self._items(), self._metrics()), "PASS")

    def test_http_500_fails(self):
        items = self._items()
        items[42] = self._measurement(43, status=500, outcome="http_error")
        self.assertEqual(report._concurrency_verdict(items, self._metrics()), "FAIL")

    def test_driver_error_outcome_fails(self):
        items = self._items()
        items[7] = self._measurement(8, outcome="driver_error")
        self.assertEqual(report._concurrency_verdict(items, self._metrics()), "FAIL")

    def test_missing_ledger_entry_fails(self):
        items = self._items(99)
        self.assertEqual(report._concurrency_verdict(items, self._metrics()), "FAIL")

    def test_429_fails(self):
        items = self._items()
        items[0] = self._measurement(1, status=429, outcome="http_error")
        self.assertEqual(report._concurrency_verdict(items, self._metrics()), "FAIL")

    def test_undrained_resources_fail(self):
        for drained in ({"active_permits": 1},
                        {"unknown_held_permits": 1},
                        {"session_seats": 1}):
            with self.subTest(drained=drained):
                self.assertEqual(
                    report._concurrency_verdict(self._items(), self._metrics(**drained)),
                    "FAIL")

    def test_rejected_requests_fail(self):
        metrics = self._metrics()
        metrics.before = {"rejected_requests_total": 3}
        metrics.after = {"rejected_requests_total": 4}
        self.assertEqual(report._concurrency_verdict(self._items(), metrics), "FAIL")

    def test_expected_limit_needs_explicit_permission(self):
        items = self._items()
        items[0] = self._measurement(1, outcome="expected_limit")
        metrics = self._metrics()
        self.assertEqual(report._concurrency_verdict(items, metrics), "FAIL")
        metrics.allow_expected_limit = True
        self.assertEqual(report._concurrency_verdict(items, metrics), "PASS")


class _FakeMetrics:
    def snapshot(self):
        return {
            "active_permits": 0,
            "unknown_held_permits": 0,
            "session_seats": 0,
            "rejected_requests_total": 0,
        }


class _FakeClient:
    def close(self):
        pass


class _FakeTarget:
    @staticmethod
    def build():
        return Target(
            endpoint="https://example.invalid", project_api_key="p",
            global_api_key="g",
            database={"branch": "main",
                      "namespace": {"tenant": "t", "database": "d"}})


class _LedgerRunner(runner.Runner):
    def __init__(self, spec, failure_index=0, error_factory=None):
        super().__init__(
            target=_FakeTarget.build(), fixture=None, scenarios=[spec],
            metrics=_FakeMetrics(), concurrency_requests=4)
        self.failure_index = failure_index
        self.error_factory = error_factory or (
            lambda: RuntimeError("simulated driver failure"))

    def _client(self, name):
        return _FakeClient()

    def _request_once(self, *, spec, protocol, client, sql, args, want_rows,
                      phase, round_index, sample_index, operation="",
                      concurrency=0, baton=None, baton_reused=False,
                      close_stream=True):
        return Measurement(
            scenario=spec.key, protocol=protocol.name, kind=spec.kind,
            phase=phase, round_index=round_index, sample_index=sample_index,
            status=200, duration_ms=1.0, ttfb_ms=0.5, request_bytes=1,
            response_bytes=1, rows=1, rows_read=0, rows_written=0,
            affected_row_count=0, query_ms=0.1, outcome="ok",
            concurrency=concurrency)

    def _concurrent_one(self, spec, protocol, local, level, index):
        if index == self.failure_index:
            raise self.error_factory()
        return self._request_once(
            spec=spec, protocol=protocol, client=None, sql=spec.sql,
            args=spec.args, want_rows=spec.want_rows, phase="timed",
            round_index=1, sample_index=index + 1, concurrency=level)


class ConcurrencyLedgerTests(unittest.TestCase):
    def _spec(self):
        return contract.ScenarioSpec(
            key="X1.concurrency", title="X1", kind="concurrency",
            protocols=(contract.JSON_PIPELINE,), sql="SELECT 1",
            expected_rows=1, concurrency_levels=(4,))

    def test_exception_still_becomes_a_measurement(self):
        result = _LedgerRunner(self._spec()).run()
        self.assertEqual(len(result.measurements), 4)
        self.assertEqual(result.concurrency[0].requests, 4)
        self.assertEqual(result.concurrency[0].planned, 4)
        failed = [item for item in result.measurements if item.outcome != "ok"]
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0].outcome, "driver_error")
        self.assertEqual(failed[0].error_code, "RuntimeError")
        self.assertEqual(failed[0].status, 0)
        summary = report.concurrency_summary(result)
        self.assertEqual(summary[0]["requests"], 4)
        self.assertEqual(summary[0]["planned"], 4)
        self.assertEqual(summary[0]["verdict"], "FAIL")

    def test_fatal_transport_is_recorded_before_abort(self):
        result = _LedgerRunner(
            self._spec(), error_factory=lambda: FatalTransportError("authentication rejected"),
        ).run()
        self.assertTrue(result.fatal)
        self.assertEqual(len(result.measurements), 4)
        self.assertEqual(
            [item.outcome for item in result.measurements].count("transport_error"), 1)


class ExitCodeTests(unittest.TestCase):
    def test_concurrency_failure_is_not_waived_by_allow_misses(self):
        concurrency = [{"level": 16, "requests": 99, "planned": 100, "verdict": "FAIL"}]
        self.assertEqual(issue28_run.resolve_exit_code([], concurrency, "", True, True), 1)

    def test_clean_run_exits_zero(self):
        concurrency = [{"level": 16, "requests": 100, "planned": 100, "verdict": "PASS"}]
        self.assertEqual(issue28_run.resolve_exit_code([], concurrency, "", True, False), 0)

    def test_fatal_or_incomplete_fixture_exits_two(self):
        self.assertEqual(issue28_run.resolve_exit_code([], [], "fatal", True, True), 2)
        self.assertEqual(issue28_run.resolve_exit_code([], [], "", False, True), 2)

    def test_regular_failure_honors_allow_misses(self):
        rows = [SimpleNamespace(verdict="FAIL")]
        self.assertEqual(issue28_run.resolve_exit_code(rows, [], "", True, False), 1)
        self.assertEqual(issue28_run.resolve_exit_code(rows, [], "", True, True), 0)


class ConcurrencyArgumentTests(unittest.TestCase):
    def test_requests_default_and_override(self):
        default = issue28_run._parse_args(["--connection-file", "private.json"])
        self.assertEqual(default.concurrency_requests, 100)
        override = issue28_run._parse_args([
            "--connection-file", "private.json", "--concurrency-requests", "7"])
        self.assertEqual(override.concurrency_requests, 7)

    def test_zero_requests_is_rejected(self):
        with self.assertRaises(SystemExit):
            issue28_run._parse_args([
                "--connection-file", "private.json", "--concurrency-requests", "0"])


if __name__ == "__main__":
    unittest.main()
