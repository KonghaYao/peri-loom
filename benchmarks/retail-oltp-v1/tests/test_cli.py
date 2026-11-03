"""Command-line contract: exit codes, summary semantics and scorecard output."""
from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

import support
from pl_retail.cli import main
from pl_retail.errors import EXIT_CODES, Status


def _quiet(argv: list[str]) -> int:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
        return main(argv)


class CommandLineTests(unittest.TestCase):
    def test_self_check_exits_zero(self) -> None:
        self.assertEqual(_quiet(["self-check"]), 0)

    def test_score_writes_scorecards_and_per_round_files(self) -> None:
        root = Path(tempfile.mkdtemp()) / "bundle"
        support.build_bundle(root)
        self.assertEqual(_quiet(["score", "--evidence", str(root)]), EXIT_CODES[Status.PASS])
        scorecard = json.loads((root / "scorecard.json").read_text(encoding="utf-8"))
        self.assertEqual(scorecard["status"], "PASS")
        self.assertFalse(scorecard["run"]["certifiable"])
        per_round = json.loads(
            (root / "rounds/r01-U1/scorecard.json").read_text(encoding="utf-8")
        )
        self.assertEqual(per_round["round_id"], "r01-U1")
        manifest = json.loads(
            (root / "evidence-manifest.json").read_text(encoding="utf-8")
        )
        paths = {entry["path"] for entry in manifest["files"]}
        self.assertIn("scorecard.json", paths)
        self.assertIn("rounds/r01-U1/scorecard.json", paths)
        # Re-finalizing must not drop the evidence the scorecards were computed
        # from; otherwise a second score run sees an uncovered bundle.
        self.assertIn("rounds/r01-U1/attempts.jsonl", paths)
        self.assertIn("run.json", paths)
        self.assertNotIn("evidence-manifest.json", paths)

    def test_scoring_twice_is_idempotent(self) -> None:
        root = Path(tempfile.mkdtemp()) / "bundle"
        support.build_bundle(root)
        self.assertEqual(_quiet(["score", "--evidence", str(root)]), EXIT_CODES[Status.PASS])
        self.assertEqual(_quiet(["score", "--evidence", str(root)]), EXIT_CODES[Status.PASS])
        manifest = json.loads((root / "evidence-manifest.json").read_text(encoding="utf-8"))
        paths = {entry["path"] for entry in manifest["files"]}
        self.assertIn("rounds/r01-U1/attempts.jsonl", paths)

    def test_score_exit_code_follows_status(self) -> None:
        root = Path(tempfile.mkdtemp()) / "bundle"
        support.build_bundle(root, defects={"empty_success"})
        code = _quiet(["score", "--evidence", str(root)])
        self.assertEqual(code, EXIT_CODES[Status.INVALID])
        self.assertNotEqual(code, 0)

    def test_summary_never_signals_acceptance(self) -> None:
        root = Path(tempfile.mkdtemp()) / "bundle"
        support.build_bundle(root, defects={"empty_success"})
        # A summary of a failing bundle still exits 0: it carries no authority.
        self.assertEqual(_quiet(["summary", "--evidence", str(root)]), 0)

    def test_run_requires_a_fixture_file(self) -> None:
        with self.assertRaises(SystemExit) as caught:
            _quiet(["run", "--mode", "smoke", "--output", tempfile.mkdtemp()])
        self.assertEqual(caught.exception.code, 2)

    def test_full_matrix_rejects_a_level_filter(self) -> None:
        fixture = Path(tempfile.mkdtemp()) / "fixture.json"
        fixture.write_text(
            json.dumps(
                {
                    "endpoint": "https://127.0.0.1:1",
                    "tenant": "t",
                    "project": "p",
                    "global_api_key": "g",
                    "project_api_key": "k",
                }
            ),
            encoding="utf-8",
        )
        code = _quiet(
            [
                "run",
                "--fixture-file",
                str(fixture),
                "--output",
                tempfile.mkdtemp(),
                "--mode",
                "full",
                "--level",
                "U1",
            ]
        )
        self.assertEqual(code, EXIT_CODES[Status.BLOCKED])

    def test_digest_command_prints_the_driver_digest(self) -> None:
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            self.assertEqual(main(["digest"]), 0)
        document = json.loads(buffer.getvalue())
        self.assertEqual(document["contract"], "PL-RETAIL-OLTP")
        self.assertRegex(document["driver_sha256"], r"^[0-9a-f]{64}$")
        self.assertIsNone(document["certified_driver_sha256"])


if __name__ == "__main__":
    unittest.main()
