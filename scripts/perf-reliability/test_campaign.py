import copy
import json
from pathlib import Path
import tempfile
import unittest

import campaign as campaign_module
import perf_contract


def valid_document(tier="G2", pairs=3):
    return {"schema": 1, "id": "issue8-test", "tier": tier, "pairs": pairs,
            "order": ["AB", "BA", "AB"][:pairs],
            "cases": ([{"id": "S1", "concurrency": [2, 1]}, {"id": "S2", "concurrency": [2]}]
                      if tier == "G2" else [{"id": "S1", "concurrency": [2]}]),
            "expected_denominator": {"version": "v1.1.11", "commit": "baseline"},
            "arms": {"A": {"role": "candidate", "binary": "a.bin",
                           "version": "v1.1.12", "commit": "candidate"},
                     "B": {"role": "denominator", "binary": "b.bin",
                           "version": "v1.1.11", "commit": "baseline"}}}


class CampaignTests(unittest.TestCase):
    def _load(self, document):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "campaign.json"
            path.write_text(json.dumps(document))
            return campaign_module.load_campaign(path)

    def test_formal_campaign_pairs_and_coverage(self):
        loaded = self._load(valid_document())
        profile = perf_contract.profile("formal")
        self.assertEqual(campaign_module.validate_profile(loaded, profile),
                         {"pairs": 3, "order": ["AB", "BA", "AB"]})
        self.assertEqual(campaign_module.validate_coverage(loaded, "G2"),
                         [("S1", 2), ("S1", 1), ("S2", 2)])
        planned = list(campaign_module.planned_runs(loaded, profile))
        self.assertEqual(len(planned), 3 * 2 * 3)
        self.assertEqual([item["order"] for item in planned if item["pair"] == 0
                          and item["case"] == "S1" and item["concurrency"] == 2],
                         ["AB", "AB"])
        self.assertEqual(campaign_module.ordered_arms(loaded, 1), ["B", "A"])
        self.assertEqual(campaign_module.arm_role(loaded, "B"), "denominator")

    def test_quick_campaign_single_pair(self):
        document = valid_document(tier="G1", pairs=1)
        loaded = self._load(document)
        profile = perf_contract.profile("quick")
        self.assertEqual(campaign_module.validate_profile(loaded, profile)["order"], ["AB"])
        self.assertEqual(campaign_module.validate_coverage(loaded, "G1"), [("S1", 2)])

    def test_invalid_campaigns_are_rejected(self):
        cases = []
        invalid = []
        document = valid_document()
        document["id"] = "../escape"
        invalid.append(document)
        document = valid_document()
        document["arms"]["A"]["role"] = "denominator"
        invalid.append(document)
        document = valid_document()
        document["arms"]["A"].pop("binary")
        invalid.append(document)
        document = valid_document()
        document["order"] = ["AA", "BA", "AB"]
        invalid.append(document)
        document = valid_document()
        document["cases"][0]["id"] = "S3"
        invalid.append(document)
        document = valid_document()
        document["cases"].append({"id": "S1", "concurrency": [2]})
        invalid.append(document)
        for document in invalid:
            with self.subTest(document=document.get("id")), self.assertRaises(campaign_module.CampaignError):
                self._load(document)
        loaded = self._load(valid_document(tier="G1", pairs=1))
        with self.assertRaises(campaign_module.CampaignError):
            campaign_module.validate_coverage(loaded, "G2")
        with self.assertRaises(campaign_module.CampaignError):
            campaign_module.validate_profile(loaded, perf_contract.profile("formal"))

    def test_denominator_metadata_is_checked(self):
        document = valid_document()
        loaded = self._load(document)
        self.assertEqual(campaign_module.arm_metadata_problems(loaded, "B"), [])
        metadata = {"arm": "B", "arm_version": "v1.1.11", "arm_commit": "baseline"}
        self.assertTrue(campaign_module.check_arm_evidence(loaded, "B", metadata))
        broken = dict(metadata, arm_version="v1.1.10")
        with self.assertRaises(campaign_module.CampaignError):
            campaign_module.check_arm_evidence(loaded, "B", broken)
        document = valid_document()
        document["arms"]["B"].pop("version")
        document["expected_denominator"]["version"] = "v1.1.11"
        loaded = self._load(document)
        self.assertIn("version", campaign_module.arm_metadata_problems(loaded, "B"))

    def test_snapshot_copy_and_store_reuse_refusal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot = root / "snapshot"
            snapshot.mkdir()
            (snapshot / "object").write_bytes(b"payload")
            target = root / "run" / "objects"
            result = campaign_module.prepare_store_directory(target, snapshot)
            self.assertEqual(result["snapshot_sha256"], result["store_sha256"])
            self.assertEqual((target / "object").read_bytes(), b"payload")
            with self.assertRaises(perf_contract.UnsafePath):
                campaign_module.prepare_store_directory(target, snapshot)
            with self.assertRaises(campaign_module.CampaignError):
                campaign_module.prepare_store_directory(root / "other", root / "missing")
            fresh = campaign_module.prepare_store_directory(root / "fresh")
            self.assertIsNone(fresh["snapshot_sha256"])
            before = campaign_module.tree_digest(root / "fresh")
            (root / "fresh" / "new").write_bytes(b"x")
            self.assertNotEqual(campaign_module.tree_digest(root / "fresh"), before)

    def test_run_ids_and_identity(self):
        document = valid_document()
        self.assertEqual(campaign_module.run_id(2, "B", "S2", 1), "pair2-B-S2-c1")
        first = campaign_module.campaign_identity(document)
        reordered = dict(reversed(list(document.items())))
        self.assertEqual(first, campaign_module.campaign_identity(reordered))


if __name__ == "__main__":
    unittest.main()
