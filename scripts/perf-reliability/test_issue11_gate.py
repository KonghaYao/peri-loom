import json
from pathlib import Path
import sys
import tempfile
import unittest

PERF_ROOT = Path(__file__).resolve().parent
if str(PERF_ROOT) not in sys.path:
    sys.path.insert(0, str(PERF_ROOT))

from issue11 import contract, evidence_io, gate  # noqa: E402
from test_issue11_support import write_campaign, write_run  # noqa: E402


def _jsonl(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines()
            if line.strip()]


def _write_jsonl(path, records):
    with Path(path).open("w") as output:
        for record in records:
            output.write(json.dumps(record, separators=(",", ":"),
                                    sort_keys=True) + "\n")
        output.flush()


def _rebuild(run_directory):
    evidence_io.write_manifests(run_directory, Path(run_directory).name,
                                reseal=True)


def _ledger(run_id, scenario="H1", profile="quick", arm="A", pair_index=0,
            state="fresh", order_label="AB", diagnostics=None,
            duration_seconds=0):
    return {
        "schema": 1,
        "run_id": run_id,
        "path": "runs/" + run_id,
        "scenario": scenario,
        "profile": profile,
        "arm": arm,
        "pair_index": pair_index,
        "pair_order_label": order_label,
        "state": state,
        "concurrency": 1,
        "duration_seconds": duration_seconds,
        "diagnostics": diagnostics or {
            "PERI_WORK_METRICS": False,
            "PERI_DIAG_GROUP_PHASE": False,
            "PERI_ADMISSION_TRACE": False,
            "PERI_MAINTENANCE_SCHEDULE": False,
            "sql_open_observer": False,
            "PERI_SQL_OPEN_OBSERVER": False,
            "PERI_SQL_OPEN_OBSERVER_PHASE_THRESHOLD_MS": 20,
            "PERI_SQL_OPEN_OBSERVER_PHASE_SAMPLE_PERMILLE": 1000,
        },
        "synthetic_test_fixture": True,
    }


def build_quick(root, timed=2000, warmup=200, diagnostics=None):
    run_id = "quick-run"
    run_directory = write_run(Path(root) / "runs" / run_id, timed=timed,
                              warmup=warmup, diagnostics=diagnostics)
    environment = json.loads((run_directory / "environment.json").read_text())
    record = _ledger(run_id, diagnostics=environment["diagnostics"])
    write_campaign(root, [record],
                   profile="quick", pair_order=[])
    return run_directory


def build_formal(root, pairs=5, timed=10000, aged_spacing_ns=360_000_000):
    order = contract.load_acceptance()["sampling"]["formal"]["pair_order"]
    records = []
    for pair_index in range(pairs):
        state = "fresh" if pair_index % 2 == 0 else "aged"
        for arm in order[pair_index]:
            run_id = "run-p%d-%s" % (pair_index, arm)
            write_run(Path(root) / "runs" / run_id, timed=timed, warmup=200,
                      formal=True, arm=arm, pair_index=pair_index, state=state,
                      spacing_ns=aged_spacing_ns if state == "aged" else 10_000)
            records.append(_ledger(run_id, profile="formal", arm=arm,
                                   pair_index=pair_index, state=state,
                                   order_label=order[pair_index],
                                   duration_seconds=3600 if state == "aged" else 0))
    write_campaign(root, records, profile="formal", pair_order=order,
                   scenarios=["H1"])
    return root


class GateTests(unittest.TestCase):
    def test_no_evidence_is_not_run(self):
        with tempfile.TemporaryDirectory() as directory:
            result = gate.evaluate(Path(directory) / "missing")
            self.assertEqual(result["status"], gate.NOT_RUN)
            self.assertEqual(result["exit_code"], 1)
            self.assertFalse(result["formal_pass"])

    def test_quick_complete_is_phase_pass_and_summary_is_ignored(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            run_directory = build_quick(root)
            first = gate.evaluate(root)
            self.assertEqual(first["status"], gate.INCOMPLETE, first["reasons"])
            self.assertEqual(first["exit_code"], 1)
            self.assertFalse(first["formal_pass"])
            self.assertFalse(first["summary_used"])
            (run_directory / "summary.json").write_text(json.dumps({
                "status": "PASS",
                "formal_pass": True,
                "counts": {"timed_success": 999999999},
            }))
            second = gate.evaluate(root)
            self.assertEqual(second["status"], first["status"])
            self.assertEqual(second["runs"][0]["stats"], first["runs"][0]["stats"])
            (run_directory / "summary.json").unlink()
            third = gate.evaluate(root)
            self.assertEqual(third["status"], first["status"])
            self.assertEqual(third["reasons"], second["reasons"])
            self.assertEqual(third["runs"][0]["stats"], first["runs"][0]["stats"])
            self.assertFalse(second["formal_pass"])

    def test_synthetic_h1_only_campaign_cannot_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            build_formal(root)
            result = gate.evaluate(root)
            self.assertEqual(result["status"], gate.INCOMPLETE, result["reasons"][:10])
            self.assertEqual(result["exit_code"], 1)
            self.assertFalse(result["formal_pass"])
            self.assertTrue(any("synthetic" in reason for reason in result["reasons"]))
            self.assertTrue(any("coverage_missing" in reason for reason in result["reasons"]))
            self.assertEqual(len(result["runs"]), 10)
            self.assertTrue(all(run["stats"]["timed_count"] == 10000
                                for run in result["runs"]))

    def test_formal_missing_pairs_is_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            build_formal(root, pairs=1, timed=10000)
            result = gate.evaluate(root)
            self.assertNotEqual(result["status"], gate.PASS)
            self.assertEqual(result["status"], gate.INCOMPLETE)
            self.assertTrue(any("campaign_pair_incomplete" in reason
                                for reason in result["reasons"]))

    def test_aged_window_too_short_is_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            build_formal(root, pairs=2, timed=10000, aged_spacing_ns=10_000)
            result = gate.evaluate(root)
            self.assertEqual(result["status"], gate.INCOMPLETE)
            self.assertTrue(any("aged_window_too_short" in reason
                                for reason in result["reasons"]))

    def test_insufficient_samples_is_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            build_quick(root, timed=100)
            result = gate.evaluate(root)
            self.assertEqual(result["status"], gate.INCOMPLETE)
            self.assertTrue(any("samples_below_minimum" in reason
                                for reason in result["reasons"]))

    def test_hash_mismatch_is_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            run_directory = build_quick(root)
            records = _jsonl(run_directory / "operations.jsonl")
            records[0]["duration_ns"] = 1
            _write_jsonl(run_directory / "operations.jsonl", records)
            result = gate.evaluate(root)
            self.assertEqual(result["status"], gate.FAIL)
            self.assertTrue(any("evidence_hash_mismatch" in reason
                                for reason in result["reasons"]))

    def test_invalid_formal_environment_is_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            run_directory = build_quick(root)
            environment = json.loads(
                (run_directory / "environment.json").read_text())
            environment["evidence_class"] = "formal"
            environment["host"]["virtualization"] = "orbstack"
            environment["storage"]["root_type"] = "overlay2"
            (run_directory / "environment.json").write_text(
                json.dumps(environment, indent=2, sort_keys=True) + "\n")
            _rebuild(run_directory)
            result = gate.evaluate(root)
            self.assertEqual(result["status"], gate.FAIL)
            self.assertTrue(any("environment" in reason
                                for reason in result["reasons"]))

    def test_unpaired_diagnostic_switch_is_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            diagnostics = {
                "PERI_WORK_METRICS": True,
                "PERI_DIAG_GROUP_PHASE": False,
                "PERI_ADMISSION_TRACE": False,
                "PERI_MAINTENANCE_SCHEDULE": False,
                "sql_open_observer": False,
            "PERI_SQL_OPEN_OBSERVER": False,
            "PERI_SQL_OPEN_OBSERVER_PHASE_THRESHOLD_MS": 20,
            "PERI_SQL_OPEN_OBSERVER_PHASE_SAMPLE_PERMILLE": 1000,
            }
            build_quick(root, diagnostics=diagnostics)
            result = gate.evaluate(root)
            self.assertNotEqual(result["status"], gate.PASS)
            self.assertTrue(any("diagnostic_switch_unpaired" in reason
                                for reason in result["reasons"]))

    def test_warmup_failure_is_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            run_directory = build_quick(root)
            operations = _jsonl(run_directory / "operations.jsonl")
            for record in operations:
                if record["phase"] == "warmup":
                    record["outcome"] = "error"
                    record["verification"] = {"ok": False, "kind": "test"}
                    break
            _write_jsonl(run_directory / "operations.jsonl", operations)
            _rebuild(run_directory)
            result = gate.evaluate(root)
            self.assertEqual(result["status"], gate.FAIL)
            self.assertTrue(any("warmup_failure" in reason
                                for reason in result["reasons"]))

    def test_unknown_without_terminal_record_is_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            run_directory = build_quick(root)
            records = _jsonl(run_directory / "operations.jsonl")
            records[0]["outcome"] = "outcome_unknown"
            records[0]["may_have_committed"] = True
            records[0].pop("terminal", None)
            _write_jsonl(run_directory / "operations.jsonl", records)
            _rebuild(run_directory)
            result = gate.evaluate(root)
            self.assertEqual(result["status"], gate.FAIL)
            self.assertTrue(any("unknown_without_terminal_record" in reason
                                for reason in result["reasons"]))

    def test_close_unconfirmed_counted_success_is_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            run_directory = build_quick(root)
            requests = _jsonl(run_directory / "requests.jsonl")
            requests[0]["close_step"] = True
            requests[0]["close_acknowledged"] = False
            _write_jsonl(run_directory / "requests.jsonl", requests)
            _rebuild(run_directory)
            result = gate.evaluate(root)
            self.assertEqual(result["status"], gate.FAIL)
            self.assertTrue(any("close_unconfirmed_counted_success" in reason
                                for reason in result["reasons"]))

    def test_duplicate_span_is_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            run_directory = build_quick(root)
            attribution = _jsonl(run_directory / "request-attribution.jsonl")
            attribution[1]["spans"][0]["span_id"] = \
                attribution[0]["spans"][0]["span_id"]
            _write_jsonl(run_directory / "request-attribution.jsonl", attribution)
            _rebuild(run_directory)
            result = gate.evaluate(root)
            self.assertEqual(result["status"], gate.FAIL)
            self.assertTrue(any("duplicate_span" in reason
                                for reason in result["reasons"]))

    def test_unknown_request_id_and_cross_stitch_are_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            run_directory = build_quick(root)
            operations = _jsonl(run_directory / "operations.jsonl")
            operations[0]["http_request_ids"] = ["missing-request"]
            operations[0]["pipeline_correlation_ids"] = ["missing-correlation"]
            operations[0]["step_ids"] = ["missing-step"]
            _write_jsonl(run_directory / "operations.jsonl", operations)
            _rebuild(run_directory)
            result = gate.evaluate(root)
            self.assertEqual(result["status"], gate.FAIL)
            self.assertTrue(any("unknown_request_id" in reason
                                for reason in result["reasons"]))

    def test_cross_request_stitch_is_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            run_directory = build_quick(root)
            operations = _jsonl(run_directory / "operations.jsonl")
            operations[0]["pipeline_correlation_ids"] = ["wrong-correlation"]
            _write_jsonl(run_directory / "operations.jsonl", operations)
            _rebuild(run_directory)
            result = gate.evaluate(root)
            self.assertEqual(result["status"], gate.FAIL)
            self.assertTrue(any("cross_request_stitch" in reason
                                for reason in result["reasons"]))

    def test_missing_correlation_is_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            run_directory = build_quick(root)
            requests = _jsonl(run_directory / "requests.jsonl")
            requests[0]["pipeline_correlation_id"] = ""
            _write_jsonl(run_directory / "requests.jsonl", requests)
            _rebuild(run_directory)
            result = gate.evaluate(root)
            self.assertEqual(result["status"], gate.FAIL)
            self.assertTrue(any("missing_correlation" in reason
                                for reason in result["reasons"]))

    def test_attribution_coverage_below_95_percent_is_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            run_directory = build_quick(root)
            attribution = _jsonl(run_directory / "request-attribution.jsonl")
            threshold = len(attribution) * 11 // 100
            for record in attribution[:threshold]:
                record["spans"][0]["duration_ns"] = 3_000_000
            _write_jsonl(run_directory / "request-attribution.jsonl", attribution)
            _rebuild(run_directory)
            result = gate.evaluate(root)
            self.assertEqual(result["status"], gate.FAIL)
            self.assertTrue(any("unattributed_over_10_percent" in reason
                                for reason in result["reasons"]))

    def test_combine_only_passes_complete_formal(self):
        self.assertEqual(gate._combine({}, [], "formal", False), gate.PASS)
        self.assertEqual(gate._combine({}, [], "quick", False), gate.PHASE_PASS)
        self.assertEqual(gate._combine({}, [("INCOMPLETE", "x")], "formal",
                                       False), gate.INCOMPLETE)
        self.assertEqual(gate._combine({}, [("FAIL", "x")], "formal", False),
                         gate.FAIL)
        self.assertEqual(gate._combine({}, [], "formal", True), gate.PHASE_PASS)


if __name__ == "__main__":
    unittest.main()
