import copy
import json
import tempfile
import unittest
from pathlib import Path

import gate

PERI_SHA = "913a20b" + "0" * 33
TURSO_SHA = "24a3a46" + "0" * 33
SQLD_DIGEST = "sha256:" + "b" * 64
DATASET_SHA = "c" * 64
SCRIPT_SHA = "d" * 64

PERI_BASE = {
    "single_read": 1_100_000,
    "single_write": 1_200_000,
    "sequential_read_50rps": 1_300_000,
    "mixed_read": 1_400_000,
    "mixed_write": 1_450_000,
}
SQLD_BASE = 1_000_000


def samples(base_ns, count=40):
    return [base_ns] * count


def evidence():
    runs = []
    for scenario, peri_ns in PERI_BASE.items():
        for arm, value in (("peri", peri_ns), ("sqld", SQLD_BASE)):
            for round_no in range(1, 4):
                runs.append(
                    {
                        "run_id": f"{arm}-{scenario}-r{round_no}",
                        "arm": arm,
                        "scenario": scenario,
                        "round": round_no,
                        "dimensions": {
                            "backend": "native",
                            "cache": "warm",
                            "maintenance": "off",
                        },
                        "planned_requests": 40,
                        "samples_ns": samples(value),
                        "errors": {
                            "rejected": 0,
                            "failed": 0,
                            "unknown": 0,
                            "excluded": 0,
                        },
                    }
                )
    return {
        "schema_version": gate.SCHEMA_VERSION,
        "campaign": {
            "id": "unit-test-native-warm-off",
            "backend": "native",
            "cache": "warm",
            "maintenance": "off",
            "tls": {"enabled": True, "profile": "unit-test-tls"},
            "durability": "strict",
            "dataset": {"ref": "unit-fixture", "sha256": DATASET_SHA},
            "request_script": {"ref": "unit-script", "sha256": SCRIPT_SHA},
            "rounds": 3,
        },
        "metadata": {
            "peri_sha": PERI_SHA,
            "turso_submodule_sha": TURSO_SHA,
            "sqld_image_digest": SQLD_DIGEST,
            "arch": "aarch64",
            "hardware": {
                "cpu": "unit-test-cpu",
                "memory_bytes": 32 << 30,
                "disk": "unit-test-disk",
                "network": "loopback",
            },
        },
        "correctness": {
            "g0_passed": True,
            "oracle": "SP01",
            "evidence": "unit-test-oracle.json",
            "unknown_results": 0,
        },
        "runs": runs,
    }


def scenario(result, name):
    return next(entry for entry in result["scenarios"] if entry["scenario"] == name)


def replace_samples(data, scenario_name, arm, values):
    for run in data["runs"]:
        if run["scenario"] == scenario_name and run["arm"] == arm:
            run["samples_ns"] = list(values)


class GatePassTests(unittest.TestCase):
    def test_qualified_evidence_passes(self):
        result = gate.evaluate(evidence())
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(len(result["scenarios"]), 5)
        self.assertTrue(all(entry["passed"] for entry in result["scenarios"]))
        self.assertEqual(gate.EXIT_CODES[result["status"]], 0)

    def test_exact_1_5x_passes(self):
        data = evidence()
        replace_samples(data, "mixed_write", "peri", samples(1_500_000))
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "PASS")
        self.assertTrue(scenario(result, "mixed_write")["passed"])

    def test_markdown_contains_five_scenario_table(self):
        result = gate.evaluate(evidence())
        markdown = gate.render_markdown(result)
        self.assertIn("# sqld relative gate: PASS", markdown)
        for name in gate.SCENARIOS:
            self.assertIn(f"| {name} |", markdown)
        self.assertIn("| scenario |", markdown)


class GateRatioTests(unittest.TestCase):
    def test_single_scenario_over_limit_fails_without_averaging(self):
        data = evidence()
        replace_samples(data, "mixed_write", "peri", samples(1_600_000))
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "FAIL")
        self.assertFalse(scenario(result, "mixed_write")["passed"])
        for name in gate.SCENARIOS:
            if name != "mixed_write":
                self.assertTrue(scenario(result, name)["passed"])
        self.assertEqual(result["failures"][0]["scenario"], "mixed_write")
        self.assertAlmostEqual(result["failures"][0]["ratio"], 1.6)

    def test_each_scenario_is_judged_separately(self):
        data = evidence()
        replace_samples(data, "single_read", "peri", samples(1_600_000))
        replace_samples(data, "single_write", "peri", samples(1_700_000))
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "FAIL")
        failed = {entry["scenario"] for entry in result["failures"]}
        self.assertEqual(failed, {"single_read", "single_write"})


class GateMetadataTests(unittest.TestCase):
    def test_missing_metadata_is_invalid(self):
        data = evidence()
        del data["metadata"]["sqld_image_digest"]
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "INVALID")
        self.assertEqual(result["scenarios"], [])
        self.assertTrue(any("sqld_image_digest" in error for error in result["errors"]))

    def test_expected_sha_mismatch_is_invalid(self):
        result = gate.evaluate(evidence(), expect_peri_sha="f" * 40)
        self.assertEqual(result["status"], "INVALID")
        self.assertTrue(any("peri_sha" in error for error in result["errors"]))

    def test_dimension_mixing_is_invalid(self):
        data = evidence()
        data["runs"][0]["dimensions"]["backend"] = "s3"
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "INVALID")
        self.assertTrue(any("pools must not be mixed" in error for error in result["errors"]))

    def test_unknown_scenario_is_invalid(self):
        data = evidence()
        data["runs"][0]["scenario"] = "single_scan"
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "INVALID")

    def test_duplicate_run_id_is_invalid(self):
        data = evidence()
        data["runs"][1]["run_id"] = data["runs"][0]["run_id"]
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "INVALID")
        self.assertTrue(any("duplicate" in error for error in result["errors"]))


class GateErrorTests(unittest.TestCase):
    def test_missing_error_counter_is_invalid(self):
        data = evidence()
        del data["runs"][0]["errors"]["unknown"]
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "INVALID")
        self.assertTrue(any("missing counters" in error for error in result["errors"]))

    def test_unknown_error_is_invalid(self):
        data = evidence()
        data["runs"][0]["samples_ns"].pop()
        data["runs"][0]["errors"]["unknown"] = 1
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "INVALID")
        self.assertTrue(any("unsettled Unknown" in error for error in result["errors"]))

    def test_excluded_sample_is_invalid(self):
        data = evidence()
        data["runs"][0]["samples_ns"].pop()
        data["runs"][0]["errors"]["excluded"] = 1
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "INVALID")
        self.assertTrue(any("must not be excluded" in error for error in result["errors"]))

    def test_failed_or_rejected_request_is_invalid(self):
        for key in ("failed", "rejected"):
            data = evidence()
            data["runs"][0]["samples_ns"].pop()
            data["runs"][0]["errors"][key] = 1
            result = gate.evaluate(data)
            self.assertEqual(result["status"], "INVALID")
            self.assertTrue(any(key in error for error in result["errors"]))

    def test_unsettled_request_accounting_is_invalid(self):
        data = evidence()
        data["runs"][0]["samples_ns"].pop()
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "INVALID")
        self.assertTrue(any("does not settle" in error for error in result["errors"]))

    def test_unknown_error_counter_is_invalid(self):
        data = evidence()
        data["runs"][0]["errors"]["failed"] = "0"
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "INVALID")
        self.assertTrue(any("non-negative integer" in error for error in result["errors"]))

    def test_unclassified_error_counter_is_invalid(self):
        data = evidence()
        data["runs"][0]["errors"]["timeout"] = 0
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "INVALID")
        self.assertTrue(any("unclassified" in error for error in result["errors"]))


class GateRoundTests(unittest.TestCase):
    def test_declared_rounds_below_minimum_is_invalid(self):
        data = evidence()
        data["campaign"]["rounds"] = 2
        data["runs"] = [run for run in data["runs"] if run["round"] != 3]
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "INVALID")
        self.assertTrue(any("campaign.rounds" in error for error in result["errors"]))

    def test_missing_round_is_invalid(self):
        data = evidence()
        data["runs"] = [
            run
            for run in data["runs"]
            if not (run["scenario"] == "single_read" and run["arm"] == "peri" and run["round"] == 3)
        ]
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "INVALID")
        self.assertTrue(any("rounds do not match" in error for error in result["errors"]))

    def test_small_sample_set_is_invalid(self):
        data = evidence()
        data["runs"][0]["planned_requests"] = 2
        data["runs"][0]["samples_ns"] = [1_000_000, 1_000_000]
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "INVALID")
        self.assertTrue(any("planned_requests" in error for error in result["errors"]))


class GateCorrectnessTests(unittest.TestCase):
    def test_g0_not_passed_holds(self):
        data = evidence()
        data["correctness"]["g0_passed"] = False
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "HOLD")
        self.assertEqual(result["failures"], [{"reason": "g0_not_passed"}])
        self.assertTrue(all(not entry["accepted"] for entry in result["scenarios"]))
        self.assertEqual(gate.EXIT_CODES[result["status"]], 3)

    def test_g0_unknown_results_is_invalid(self):
        data = evidence()
        data["correctness"]["unknown_results"] = 1
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "INVALID")
        self.assertTrue(any("unknown_results" in error for error in result["errors"]))

    def test_missing_g0_is_invalid(self):
        data = evidence()
        del data["correctness"]
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "INVALID")
        self.assertTrue(any("correctness" in error for error in result["errors"]))


class GateSummaryTests(unittest.TestCase):
    def test_recomputes_and_ignores_untrusted_summary(self):
        data = evidence()
        replace_samples(data, "single_read", "peri", samples(2_000_000))
        for run in data["runs"]:
            if run["scenario"] == "single_read" and run["arm"] == "peri":
                run["summary"] = {"p50_ns": 1, "p95_ns": 1, "p99_ns": 1}
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "FAIL")
        self.assertFalse(scenario(result, "single_read")["passed"])
        self.assertTrue(any("summary ignored" in warning for warning in result["warnings"]))

    def test_summary_cannot_force_failure_when_raw_passes(self):
        data = evidence()
        for run in data["runs"]:
            if run["scenario"] == "mixed_read" and run["arm"] == "peri":
                run["summary"] = {"p50_ns": 99_000_000}
        result = gate.evaluate(data)
        self.assertEqual(result["status"], "PASS")
        self.assertTrue(scenario(result, "mixed_read")["passed"])
        self.assertTrue(any("summary ignored" in warning for warning in result["warnings"]))


class GatePercentileTests(unittest.TestCase):
    def test_nearest_rank_percentiles(self):
        values = list(range(1, 101))
        self.assertEqual(gate.percentile_ns(values, 50), 50)
        self.assertEqual(gate.percentile_ns(values, 95), 95)
        self.assertEqual(gate.percentile_ns(values, 99), 99)

    def test_percentiles_are_order_independent(self):
        values = [5, 1, 4, 2, 3]
        self.assertEqual(gate.percentile_ns(values, 50), 3)
        self.assertEqual(gate.percentile_ns(list(reversed(values)), 50), 3)


class GateCliTests(unittest.TestCase):
    def test_no_input_holds(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "verdict.json"
            code = gate.main(["--json-out", str(output)])
            self.assertEqual(code, 3)
            verdict = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(verdict["status"], "HOLD")
            self.assertTrue(verdict["evidence_gaps"])

    def test_valid_input_writes_json_and_markdown(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence_path = root / "evidence.json"
            json_path = root / "verdict.json"
            markdown_path = root / "verdict.md"
            evidence_path.write_text(json.dumps(evidence()), encoding="utf-8")
            code = gate.main(
                [
                    "--input",
                    str(evidence_path),
                    "--json-out",
                    str(json_path),
                    "--markdown-out",
                    str(markdown_path),
                ]
            )
            self.assertEqual(code, 0)
            verdict = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual(verdict["status"], "PASS")
            markdown = markdown_path.read_text(encoding="utf-8")
            self.assertIn("| mixed_write |", markdown)

    def test_invalid_input_exits_invalid(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = evidence()
            del data["metadata"]["arch"]
            evidence_path = root / "evidence.json"
            json_path = root / "verdict.json"
            evidence_path.write_text(json.dumps(data), encoding="utf-8")
            code = gate.main(
                ["--input", str(evidence_path), "--json-out", str(json_path)]
            )
            self.assertEqual(code, 2)
            verdict = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual(verdict["status"], "INVALID")

    def test_missing_file_holds(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "verdict.json"
            code = gate.main(
                ["--input", str(Path(directory) / "absent.json"), "--json-out", str(output)]
            )
            self.assertEqual(code, 3)
            verdict = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(verdict["status"], "HOLD")
            self.assertIn("not found", verdict["evidence_gaps"][0])


if __name__ == "__main__":
    unittest.main()
