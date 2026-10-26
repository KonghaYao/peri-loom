import json
from pathlib import Path
import subprocess
import sys
import unittest

PERF_ROOT = Path(__file__).resolve().parent
if str(PERF_ROOT) not in sys.path:
    sys.path.insert(0, str(PERF_ROOT))

from issue11.attribution import aggregate  # noqa: E402


def span(span_id, parent, process, phase, duration, clock="client:1",
         nested=False):
    return {
        "span_id": span_id,
        "parent_span_id": parent,
        "process": process,
        "phase": phase,
        "clock_id": clock,
        "duration_ns": duration,
        "started_monotonic_ns": 0,
        "nested": nested,
        "outcome": "success",
    }


def request(request_id, total, spans, correlation=None, step_ids=None,
            coverage=None, phase="timed"):
    return {
        "schema_version": 1,
        "record_type": "request",
        "request_id": request_id,
        "pipeline_correlation_id": correlation or "corr-" + request_id,
        "scenario": "H1",
        "sample_index": 0,
        "phase": phase,
        "step_ids": step_ids or ["step-" + request_id],
        "total_ns": total,
        "spans": spans,
        "process_coverage": coverage or ["client"],
        "server_evidence": False,
        "outcome": "success",
    }


class AttributionTests(unittest.TestCase):
    def test_nested_child_is_subtracted_once(self):
        records = [request("r1", 100, [
            span("root", None, "client", "client.wait", 100),
            span("child", "root", "client", "client.read", 40),
            span("grandchild", "child", "client", "client.decode", 10),
        ])]
        documents = aggregate.load_records(records)
        result = aggregate.aggregate(documents)
        self.assertEqual(result["unattributed"]["coverage"], 1.0)
        # root 100-40=60, child 40-10=30, grandchild 10 => total 100.
        self.assertEqual(result["phase_percentiles_ns"]["client.wait"]["p99"], 60)
        self.assertEqual(result["phase_percentiles_ns"]["client.read"]["p99"], 30)
        self.assertEqual(result["phase_percentiles_ns"]["client.decode"]["p99"], 10)

    def test_cross_process_parent_is_correlation_not_nesting(self):
        records = [request("r1", 100, [
            span("root", None, "client", "client.wait", 100),
            span("gateway", "root", "gateway", "gateway.root", 60,
                 clock="gateway:1"),
            span("child", "gateway", "gateway", "gateway.worker", 60,
                 clock="gateway:1"),
        ], coverage=["client", "gateway"])]
        result = aggregate.aggregate(aggregate.load_records(records))
        self.assertEqual(result["unattributed"]["coverage"], 1.0)
        self.assertTrue(result["unattributed"]["threshold_met"])
        self.assertEqual(result["slowest"]["requests"][0]["unattributed_ns"], 0)
        self.assertEqual(result["phase_percentiles_ns"]["client.wait"]["p99"], 100)

    def test_root_containers_do_not_self_attribute(self):
        records = [request("r", 100, [
            span("c", None, "client", "client.root", 100),
            span("g", None, "gateway", "gateway.root", 60, clock="gateway:1"),
            span("nested", "g", "gateway", "gateway.root", 60, clock="gateway:1"),
        ], coverage=["client", "gateway"])]
        result = aggregate.aggregate(aggregate.load_records(records))
        self.assertFalse(result["unattributed"]["threshold_met"])
        self.assertEqual(result["unattributed"]["per_process_coverage"],
                         {"client": 0, "gateway": 0})

    def test_many_good_roots_cannot_hide_one_uncovered_root(self):
        spans = [span("c", None, "client", "client.wait", 100),
                 span("bad", None, "gateway", "gateway.root", 60, clock="gateway:1")]
        spans.extend(span("g%d" % i, None, "gateway", "gateway.work", 60,
                          clock="gateway:1") for i in range(20))
        result = aggregate.aggregate(aggregate.load_records([
            request("r", 100, spans, coverage=["client", "gateway"])]))
        self.assertFalse(result["unattributed"]["threshold_met"])
        self.assertEqual(result["unattributed"]["per_process_coverage"]["gateway"], 0)

    def test_span_refs_cannot_stitch_another_step_or_pipeline(self):
        for refs in ({"step_id": "other-step"},
                     {"pipeline_correlation_id": "other-pipeline"}):
            child = span("c", None, "client", "client.wait", 100)
            child["refs"] = refs
            with self.subTest(refs=refs), self.assertRaisesRegex(
                    aggregate.AttributionError, "cross_request_stitch"):
                aggregate.load_records([request("r", 100, [child])])

    def test_duplicate_span_is_rejected(self):
        records = [request("r1", 100, [
            span("dup", None, "client", "client.wait", 50),
            span("dup", None, "client", "client.read", 50),
        ])]
        with self.assertRaises(aggregate.AttributionError) as failure:
            aggregate.load_records(records)
        self.assertIn("duplicate_span", str(failure.exception))

    def test_unknown_request_id_is_rejected(self):
        records = [request("r1", 100, [
            span("s1", None, "client", "client.wait", 100),
        ]), {
            "schema_version": 1,
            "record_type": "span",
            "request_id": "missing",
            "span_id": "orphan",
            "process": "gateway",
            "phase": "gateway.worker",
            "clock_id": "gateway:1",
            "duration_ns": 10,
            "outcome": "success",
        }]
        with self.assertRaises(aggregate.AttributionError) as failure:
            aggregate.load_records(records)
        self.assertIn("unknown_request_id", str(failure.exception))

    def test_cross_request_stitch_is_rejected(self):
        records = [request("r1", 100, [
            span("s1", None, "client", "client.wait", 100),
        ]), {
            "schema_version": 1,
            "record_type": "span",
            "request_id": "r1",
            "pipeline_correlation_id": "other-correlation",
            "span_id": "s2",
            "process": "gateway",
            "phase": "gateway.worker",
            "clock_id": "gateway:1",
            "duration_ns": 10,
            "outcome": "success",
        }]
        with self.assertRaises(aggregate.AttributionError) as failure:
            aggregate.load_records(records)
        self.assertIn("cross_request_stitch", str(failure.exception))

    def test_missing_correlation_is_rejected(self):
        records = [request("r1", 100, [
            span("s1", None, "client", "client.wait", 100),
        ])]
        records[0]["pipeline_correlation_id"] = ""
        with self.assertRaises(aggregate.AttributionError) as failure:
            aggregate.load_records(records)
        self.assertIn("missing_correlation", str(failure.exception))

    def test_nested_duration_overflow_is_rejected(self):
        records = [request("r1", 100, [
            span("root", None, "client", "client.wait", 50),
            span("child", "root", "client", "client.read", 60),
        ])]
        with self.assertRaises(aggregate.AttributionError) as failure:
            aggregate.load_records(records)
        self.assertIn("nested_duration_overflow", str(failure.exception))

    def test_slowest_one_percent_and_95_percent_rule(self):
        records = []
        for index in range(200):
            unattributed = 20 if index < 11 else 0
            records.append(request("r%03d" % index, 100, [
                span("s%03d" % index, None, "client", "client.wait",
                     100 - unattributed),
            ]))
        documents = aggregate.load_records(records)
        result = aggregate.aggregate(documents)
        self.assertEqual(result["slowest"]["count"], 2)
        self.assertEqual(len(result["slowest"]["requests"]), 2)
        self.assertEqual(result["request_count"], 200)
        self.assertFalse(result["unattributed"]["threshold_met"])
        self.assertEqual(len(result["unattributed"]["violations"]), 11)
        for index in range(200):
            records[index] = request("r%03d" % index, 100, [
                span("s%03d" % index, None, "client", "client.wait",
                     89 if index < 10 else 100),
            ])
        documents = aggregate.load_records(records)
        result = aggregate.aggregate(documents)
        self.assertTrue(result["unattributed"]["threshold_met"])
        self.assertEqual(len(result["unattributed"]["violations"]), 10)

    def test_recompute_rejects_operation_with_missing_request(self):
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            document = request("r", 100, [span("c", None, "client", "client.wait", 100)])
            attribution = root / "request-attribution.jsonl"
            operations = root / "operations.jsonl"
            attribution.write_text(json.dumps(document) + "\n")
            operations.write_text(json.dumps({"record_type": "operation", "op_id": "op",
                                              "http_request_ids": ["r", "missing"]}) + "\n")
            completed = subprocess.run(
                [sys.executable, str(PERF_ROOT / "issue11/attribution/recompute.py"),
                 "--request-attribution", str(attribution), "--operations", str(operations)],
                capture_output=True, text=True, timeout=30)
            self.assertEqual(completed.returncode, 2)
            self.assertIn("operation_request_set_mismatch", completed.stderr)

    def test_recompute_help_runs(self):
        completed = subprocess.run(
            [sys.executable, str(PERF_ROOT / "issue11" / "attribution" / "recompute.py"),
             "--help"], capture_output=True, text=True, timeout=30)
        self.assertEqual(completed.returncode, 0)
        self.assertIn("Recompute Issue 11 attribution", completed.stdout)


if __name__ == "__main__":
    unittest.main()
