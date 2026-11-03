"""The scorer is fail-closed: every contract negative must lose its PASS."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import support
from pl_retail.contract import load_contract
from pl_retail.errors import EXIT_CODES, Status
from pl_retail.evidence import EvidenceWriter
from pl_retail.score import score_run


class ScorerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.contract = load_contract(None)

    def _score(self, **kwargs):
        root = Path(tempfile.mkdtemp()) / "bundle"
        support.build_bundle(root, **kwargs)
        return root, score_run(root, self.contract)

    def test_synthetic_full_bundle_passes_every_dimension(self) -> None:
        _, scorecard = self._score()
        self.assertEqual(scorecard["status"], "PASS", scorecard["failures"])
        self.assertEqual(
            {name: item["status"] for name, item in scorecard["dimensions"].items()},
            {
                "performance": "PASS",
                "correctness": "PASS",
                "recovery": "PASS",
                "load": "PASS",
                "idle": "PASS",
                "cleanup_ab": "PASS",
                "evidence": "PASS",
            },
        )
        self.assertTrue(all(item["status"] == "PASS" for item in scorecard["levels"].values()))

    def test_a_new_driver_is_measured_but_never_certified(self) -> None:
        _, scorecard = self._score()
        self.assertIsNone(scorecard["run"]["certified_driver_sha256"])
        self.assertFalse(scorecard["run"]["driver_certified"])
        self.assertFalse(scorecard["run"]["certifiable"])
        self.assertTrue(
            any("not certified" in warning for warning in scorecard["warnings"]),
            scorecard["warnings"],
        )

    def test_empty_success_trace_fails(self) -> None:
        _, scorecard = self._score(defects={"empty_success"})
        # No successful sample means the round carries no evidence at all, so
        # the trace is INVALID rather than merely slow.
        self.assertEqual(scorecard["status"], "INVALID")
        self.assertTrue(
            any("no successful" in failure for failure in scorecard["failures"]),
            scorecard["failures"][:5],
        )

    def test_missing_window_invalidates(self) -> None:
        _, scorecard = self._score(defects={"missing_window"})
        self.assertEqual(scorecard["status"], "INVALID")
        self.assertTrue(
            any("timeline" in failure for failure in scorecard["failures"]),
            scorecard["failures"][:5],
        )

    def test_missing_repetition_invalidates(self) -> None:
        _, scorecard = self._score(defects={"missing_repetition"})
        self.assertEqual(scorecard["status"], "INVALID")
        self.assertTrue(
            any("r03" in failure for failure in scorecard["failures"]),
            scorecard["failures"][:5],
        )

    def test_tampered_evidence_invalidates(self) -> None:
        root, _ = self._score()
        target = root / "rounds/r01-U1/round.json"
        original = target.read_bytes()
        # Same length, different bytes: the digest must be what fails.
        tampered = original.replace(b'"mode": "full"', b'"mode": "FUll"')
        self.assertEqual(len(tampered), len(original))
        self.assertNotEqual(tampered, original)
        target.write_bytes(tampered)
        scorecard = score_run(root, self.contract)
        self.assertEqual(scorecard["status"], "INVALID")
        self.assertTrue(
            any("sha256 mismatch" in failure for failure in scorecard["failures"]),
            scorecard["failures"][:5],
        )

    def test_cleanup_round_must_be_listed_in_run_json(self) -> None:
        root, _ = self._score()
        run_path = root / "run.json"
        run = json.loads(run_path.read_text(encoding="utf-8"))
        run["rounds"] = [item for item in run["rounds"] if item != "r01-CLEANUP"]
        run_path.write_text(json.dumps(run, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        EvidenceWriter(root).finalize()
        scorecard = score_run(root, self.contract)
        self.assertEqual(scorecard["status"], "INVALID")
        self.assertTrue(
            any("run.json does not list it" in failure for failure in scorecard["failures"]),
            scorecard["failures"][:5],
        )

    def test_missing_manifest_entry_invalidates(self) -> None:
        root, _ = self._score()
        (root / "unlisted.json").write_text("{}\n", encoding="utf-8")
        scorecard = score_run(root, self.contract)
        self.assertEqual(scorecard["status"], "INVALID")
        self.assertTrue(
            any("manifest does not cover" in failure for failure in scorecard["failures"]),
            scorecard["failures"][:5],
        )

    def test_unknown_terminal_fails(self) -> None:
        _, scorecard = self._score(defects={"unknown_terminal"})
        self.assertEqual(scorecard["status"], "FAIL")
        self.assertTrue(
            any("service_unknown" in failure or "unknown" in failure
                for failure in scorecard["failures"]),
            scorecard["failures"][:5],
        )

    def test_wrong_checkout_statement_count_fails(self) -> None:
        _, scorecard = self._score(defects={"bad_http_calls"})
        # 12 statements instead of the fixed 13 is a contract violation, so
        # the round is INVALID rather than a slow FAIL.
        self.assertEqual(scorecard["status"], "INVALID")
        self.assertTrue(
            any("HTTP calls" in failure for failure in scorecard["failures"]),
            scorecard["failures"][:5],
        )

    def test_slo_breach_fails(self) -> None:
        _, scorecard = self._score(defects={"slo_breach"})
        self.assertEqual(scorecard["status"], "FAIL")
        self.assertTrue(
            any("p95" in failure or "p99" in failure for failure in scorecard["failures"]),
            scorecard["failures"][:5],
        )

    def test_smoke_mode_cannot_be_certified(self) -> None:
        root = Path(tempfile.mkdtemp()) / "bundle"
        support.build_bundle(root, mode="smoke")
        scorecard = score_run(root, self.contract)
        self.assertNotEqual(scorecard["status"], "PASS")
        self.assertEqual(scorecard["status"], "INCONCLUSIVE")
        self.assertFalse(scorecard["run"]["certifiable"])

    def test_exit_codes_are_fail_closed(self) -> None:
        self.assertEqual(EXIT_CODES[Status.PASS], 0)
        for status in (Status.FAIL, Status.INVALID, Status.INCONCLUSIVE,
                       Status.BLOCKED, Status.NOT_RUN):
            self.assertNotEqual(EXIT_CODES[status], 0, status)
        self.assertEqual(len(set(EXIT_CODES.values())), len(Status))


if __name__ == "__main__":
    unittest.main()
