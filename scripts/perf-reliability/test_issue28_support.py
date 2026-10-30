"""Offline tests for the Issue 28 harness.

These tests only exercise wire encoding/decoding, statistics, and the report
contract. They are not performance evidence and never open a socket.
"""
import json
import pathlib
import struct
import sys
import unittest

PERF_ROOT = pathlib.Path(__file__).resolve().parent
if str(PERF_ROOT) not in sys.path:
    sys.path.insert(0, str(PERF_ROOT))

from issue28 import contract, hrana, report, runner  # noqa: E402
from issue28.fixture import DatasetCheck, Fixture  # noqa: E402
from issue28.runner import Measurement, RunResult  # noqa: E402


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
        error = pb_bytes(1, b"result_limit")
        body = self._pipeline(pb_bytes(2, error))  # StreamResult.error
        result = hrana.parse_pipeline_protobuf(body).results[0]
        self.assertFalse(result.ok)
        self.assertEqual(result.error_code, "result_limit")

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


if __name__ == "__main__":
    unittest.main()
