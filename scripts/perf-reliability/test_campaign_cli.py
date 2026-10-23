import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import campaign_cli
import issue8_test_fixtures as fixtures
import perf_contract


class CampaignCliTests(unittest.TestCase):
    def _environment(self, root, **overrides):
        environment = {"tier": "G1", "store_root": str(root / "store"),
                       "cache_root": str(root / "cache"),
                       "evidence_root": str(root / "evidence"), "timeout": 5}
        environment.update(overrides)
        path = root / "environment.json"
        path.write_text(json.dumps(environment))
        return path

    def _campaign(self, root, tier="G1"):
        path = root / "campaign.json"
        path.write_text(json.dumps(fixtures.campaign_document(tier)))
        return path

    def test_round_and_environment_map_to_runner_arguments(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            campaign = self._campaign(root)
            environment = self._environment(root, metrics_snapshot=str(root / "metrics"))
            captured = {}

            def fake_main(argv):
                captured["argv"] = argv
                document = json.loads(Path(argv[argv.index("--campaign") + 1]).read_text())
                captured["campaign"] = document
                return 0

            with patch.object(campaign_cli.issue8_runner, "main", side_effect=fake_main):
                code = campaign_cli.main([
                    "--campaign", str(campaign), "--environment", str(environment),
                    "--round", "quick", "--case", "S1", "--output", str(root / "result")])
            self.assertEqual(code, 0)
            arguments = captured["argv"]
            self.assertEqual(arguments[arguments.index("--profile") + 1], "quick")
            self.assertEqual(arguments[arguments.index("--tier") + 1], "G1")
            self.assertEqual(arguments[arguments.index("--store-root") + 1],
                             str(root / "store"))
            self.assertEqual(arguments[arguments.index("--evidence-root") + 1],
                             str(root / "result"))
            self.assertEqual(arguments[arguments.index("--metrics-snapshot") + 1],
                             str(root / "metrics"))
            self.assertEqual([case["id"] for case in captured["campaign"]["cases"]], ["S1"])
            for arm in captured["campaign"]["arms"].values():
                self.assertTrue(Path(arm["binary"]).is_absolute())

    def test_contract_and_acceptance_copies_must_match_frozen_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            contract = root / "workloads.json"
            contract.write_bytes((perf_contract.CONTRACT_ROOT / "workloads.json").read_bytes())
            acceptance = root / "acceptance.json"
            acceptance.write_bytes((perf_contract.CONTRACT_ROOT / "acceptance.json").read_bytes())
            campaign_cli.verify_anchored_inputs(contract, acceptance)
            acceptance.write_text(acceptance.read_text() + " ")
            with self.assertRaises(campaign_cli.CliError) as failure:
                campaign_cli.verify_anchored_inputs(contract, acceptance)
            self.assertIn("acceptance_bytes_not_frozen", str(failure.exception))
            acceptance.write_bytes((perf_contract.CONTRACT_ROOT / "acceptance.json").read_bytes())
            contract.write_text("{}\n")
            with self.assertRaises(campaign_cli.CliError) as failure:
                campaign_cli.verify_anchored_inputs(contract, acceptance)
            self.assertIn("contract_bytes_not_frozen", str(failure.exception))

    def test_case_filter_rejects_unsupported_and_missing_cases(self):
        campaign = fixtures.campaign_document("G1")
        filtered = campaign_cli.filter_campaign(campaign, "S1")
        self.assertEqual([case["id"] for case in filtered["cases"]], ["S1"])
        with self.assertRaises(campaign_cli.CliError) as failure:
            campaign_cli.filter_campaign(campaign, "S3")
        self.assertIn("case_not_supported_in_w0a1", str(failure.exception))
        with self.assertRaises(campaign_cli.CliError) as failure:
            campaign_cli.filter_campaign(campaign, "S2")
        self.assertIn("case_missing_from_campaign", str(failure.exception))

    def test_steady_round_is_not_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = io.StringIO()
            with patch.object(campaign_cli.issue8_runner, "main",
                              side_effect=AssertionError("runner must not start")), \
                    contextlib.redirect_stdout(output):
                code = campaign_cli.main([
                    "--campaign", str(self._campaign(root)),
                    "--environment", str(self._environment(root)),
                    "--round", "steady"])
            self.assertEqual(code, 1)
            self.assertIn("steady_not_implemented_in_w0a1", output.getvalue())

    def test_mutated_contract_stops_before_runner(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            contract = root / "workloads.json"
            contract.write_text("{}\n")
            output = io.StringIO()
            with patch.object(campaign_cli.issue8_runner, "main",
                              side_effect=AssertionError("runner must not start")), \
                    contextlib.redirect_stdout(output):
                code = campaign_cli.main([
                    "--campaign", str(self._campaign(root)),
                    "--environment", str(self._environment(root)),
                    "--contract", str(contract), "--round", "quick"])
            self.assertEqual(code, 1)
            self.assertIn("contract_bytes_not_frozen", output.getvalue())

    def test_environment_requires_bounded_timeout_and_roots(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = self._environment(root, timeout=600)
            with self.assertRaises(campaign_cli.CliError):
                campaign_cli.load_environment(path)
            path = self._environment(root, tier="G3")
            with self.assertRaises(campaign_cli.CliError):
                campaign_cli.load_environment(path)
            path = self._environment(root, profile_overrides={"min_samples_per_lane": 1})
            environment = campaign_cli.load_environment(path)
            self.assertEqual(environment["profile_overrides"],
                             {"min_samples_per_lane": 1})


if __name__ == "__main__":
    unittest.main()
