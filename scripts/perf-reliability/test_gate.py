import contextlib
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import campaign as campaign_module
import evidence
import gate
import issue8_test_fixtures as fixtures
import perf_contract


class GateTests(unittest.TestCase):
    def _mutate_snapshot(self, run, lane, mutate):
        index_path = run["directory"] / "metrics-snapshot-index.json"
        index = json.loads(index_path.read_text())
        item = next(entry for entry in index["snapshots"] if entry["lane"] == lane)
        path = run["directory"] / item["file"]
        document = json.loads(path.read_text())
        mutate(document)
        path.write_text(json.dumps(document) + "\n")
        item["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        item["bytes"] = path.stat().st_size
        index_path.write_text(json.dumps(index))
        fixtures.reseal(run["directory"])

    def _replace_snapshot(self, target_run, target_lane, source_run, source_lane):
        source_index = json.loads(
            (source_run["directory"] / "metrics-snapshot-index.json").read_text())
        source_item = next(entry for entry in source_index["snapshots"]
                           if entry["lane"] == source_lane)
        payload = (source_run["directory"] / source_item["file"]).read_bytes()
        index_path = target_run["directory"] / "metrics-snapshot-index.json"
        index = json.loads(index_path.read_text())
        item = next(entry for entry in index["snapshots"] if entry["lane"] == target_lane)
        path = target_run["directory"] / item["file"]
        path.write_bytes(payload)
        item["sha256"] = hashlib.sha256(payload).hexdigest()
        item["bytes"] = len(payload)
        index_path.write_text(json.dumps(index))
        fixtures.reseal(target_run["directory"])

    def _passing(self, directory, tier="G1", pairs=1, **overrides):
        # Synthetic policy for gate arithmetic; contract minima are covered by _real tests.
        policy = overrides.pop("policy_document", None) or fixtures.policy(pairs=pairs)
        count = overrides.pop("count", 600)
        return fixtures.build_evidence(directory, tier=tier, pairs=pairs,
                                       policy_document=policy, count=count, **overrides)

    def _real(self, directory, profile="quick", tier="G1", **overrides):
        policy = perf_contract.profile(profile)
        # G1 needs 100 ACK/s over the contract 30 s window; 512 samples/lane is
        # below that rate, so use 3000 synthetic samples while keeping 30 s.
        count = overrides.pop("count", 3000 if profile == "quick" else None)
        return fixtures.build_evidence(directory, tier=tier, pairs=policy["pairs"],
                                       policy_document=policy, span_seconds=policy["min_seconds"],
                                       count=count, **overrides)

    def _rehash_campaign(self, snapshot, document, mutate_run=None):
        root = snapshot["root"]
        campaign_sha = campaign_module.campaign_identity(document)
        evidence.write_json(root / "campaign.json", document, reseal=True)
        for run in snapshot["runs"].values():
            path = run["directory"] / "environment.json"
            metadata = json.loads(path.read_text())
            metadata["campaign_sha256"] = campaign_sha
            if mutate_run is not None:
                mutate_run(metadata)
            path.write_text(json.dumps(metadata))
            fixtures.reseal(run["directory"])
        identity_path = root / "campaign-identity.json"
        identity = json.loads(identity_path.read_text())
        identity["campaign_sha256"] = campaign_sha
        identity_path.write_text(json.dumps(identity))
        return campaign_sha

    def _trusted(self, root, arms=("A", "B")):
        root = Path(root)
        path = root.parent / (root.name + "-trusted-artifacts.json")
        path.write_text(json.dumps({label: fixtures.artifact_sha(label) for label in arms}))
        return path

    def _mutate_environments(self, evidence, mutate):
        for run in evidence["runs"].values():
            path = run["directory"] / "environment.json"
            metadata = json.loads(path.read_text())
            mutate(metadata)
            path.write_text(json.dumps(metadata))
            fixtures.reseal(run["directory"])

    def _rewrite_records(self, run, transform):
        path = run["directory"] / "operations.jsonl"
        records = [json.loads(line) for line in path.read_text().splitlines()]
        records = transform(records)
        path.write_text("".join(json.dumps(record) + "\n" for record in records))
        fixtures.reseal(run["directory"])

    def _mutate_resources(self, run, mutate):
        path = run["directory"] / "resources.json"
        resources = json.loads(path.read_text())
        mutate(resources)
        path.write_text(json.dumps(resources))
        fixtures.reseal(run["directory"])

    def _stack_samples_outside_window(self, resources, run):
        environment = json.loads((run["directory"] / "environment.json").read_text())
        started = environment["timed_started_at_ns"]
        completed = environment["timed_completed_at_ns"]
        samples = resources["sampler"]["samples"]
        half = len(samples) // 2
        for index, sample in enumerate(samples):
            if index < half:
                sample["monotonic_ns"] = started - 1_000_000_000 + index * 1_000
            else:
                sample["monotonic_ns"] = completed + 1_000_000 + (index - half) * 1_000

    def _mutate_metrics(self, evidence, mutate=None, raw=None):
        for run in evidence["runs"].values():
            index_path = run["directory"] / "metrics-snapshot-index.json"
            index = json.loads(index_path.read_text())
            for item in index["snapshots"]:
                path = run["directory"] / item["file"]
                if raw is not None:
                    path.write_bytes(raw)
                else:
                    document = json.loads(path.read_text())
                    mutate(document)
                    path.write_text(json.dumps(document) + "\n")
                item["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
                item["bytes"] = path.stat().st_size
            index_path.write_text(json.dumps(index))
            fixtures.reseal(run["directory"])

    def test_synthetic_policy_quick_g1_passes_phase_only(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(directory)
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                   policy=evidence["policy"])
            self.assertEqual(report["status"], "PHASE_PASS")
            self.assertFalse(report["formal_pass"])
            self.assertIn("not a formal PASS", report["status_note"])
            self.assertEqual(report["exit_code"], 1)
            self.assertEqual(report["sections"]["native"]["status"], "PASS")
            self.assertEqual(report["sections"]["correctness"]["status"], "PASS")
            self.assertEqual(report["sections"]["resources"]["status"], "PASS")
            self.assertEqual(report["sections"]["attribution"]["status"], "PASS")
            self.assertEqual(report["waivers"], [])
            self.assertIn("paired_groups", report["sections"]["native"])
            json.dumps(report)

    def test_formal_g1_is_phase_only_even_with_total_pass_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = fixtures.policy(name="formal", pairs=3, total_pass=True)
            evidence = self._passing(directory, pairs=3, policy_document=policy)
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="formal",
                                   policy=policy,
                                   trusted_artifacts=self._trusted(evidence["root"]))
            self.assertEqual(report["status"], "PHASE_PASS")
            self.assertEqual(report["exit_code"], 1)
            self.assertFalse(report["formal_pass"])
            self.assertTrue(report["phase_only"])
            self.assertEqual(report["total_pass_tier"], "G2")
            self.assertIn("only tier G2", report["status_note"])
            self.assertEqual(report["sections"]["native"]["status"], "PASS")
            self.assertEqual(report["sections"]["native"]["paired_groups"][0]["pair_count"], 3)

    def test_g2_absolute_and_paired_median_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = fixtures.policy(name="formal", pairs=3, total_pass=True)
            evidence = fixtures.build_evidence(
                directory, tier="G2", pairs=3, policy_document=policy,
                counts={"S1": 200, "S2": 50},
                arm_spans={("S1", "A"): 1.0, ("S1", "B"): 8.0,
                           ("S2", "A"): 1.0, ("S2", "B"): 6.0},
                duration_by_arm={"A": (1_000_000, 2_000_000, 3_000_000),
                                 "B": (12_000_000, 12_000_000, 12_000_000)})
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G2", profile_name="formal",
                                   policy=policy,
                                   trusted_artifacts=self._trusted(evidence["root"]))
            self.assertEqual(report["status"], "PASS")
            groups = {(group["case"], group["concurrency"]): group
                      for group in report["sections"]["native"]["paired_groups"]}
            self.assertGreaterEqual(groups[("S1", 2)]["median_p50_speedup"], 10)
            self.assertGreaterEqual(groups[("S1", 2)]["median_ack_throughput_speedup"], 8)
            self.assertGreaterEqual(groups[("S2", 2)]["median_ack_throughput_speedup"], 5)
            self.assertEqual(set(groups[("S2", 2)]["ratios"][0]["buckets"]),
                             {"read", "write", "all"})

    def test_s2_contract_schedule_passes_quick_and_formal(self):
        # One contract round is 32 reads + 32 writes covering all 64 rows. The
        # gate must accept that schedule, not demand 64 rows per kind.
        for name, policy, expected in (
                ("quick", fixtures.policy(pairs=1), "PHASE_PASS"),
                ("formal", fixtures.policy(name="formal", pairs=1, total_pass=True),
                 "PASS")):
            with self.subTest(profile=name), tempfile.TemporaryDirectory() as directory:
                evidence = fixtures.build_evidence(
                    directory, tier="G2", pairs=1, policy_document=policy,
                    counts={"S1": 200, "S2": 128},
                    arm_spans={("S1", "A"): 1.0, ("S1", "B"): 8.0,
                               ("S2", "A"): 1.0, ("S2", "B"): 6.0},
                    duration_by_arm={"A": (1_000_000, 2_000_000, 3_000_000),
                                     "B": (12_000_000, 12_000_000, 12_000_000)})
                trusted = self._trusted(evidence["root"]) if name == "formal" else None
                report = gate._evaluate_arithmetic_for_tests(
                    evidence["root"], tier="G2", profile_name=name, policy=policy,
                    trusted_artifacts=trusted)
                self.assertEqual(report["status"], expected)
                self.assertEqual(report["exit_code"], 0 if name == "formal" else 1)
                self.assertEqual(report["sections"]["native"]["status"], "PASS")
                for key, run in report["runs"].items():
                    if key.startswith("S2/"):
                        self.assertEqual(run["integrity"], [])

    def test_g2_c1_absolute_threshold_is_enforced(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = fixtures.policy(pairs=1)
            evidence = fixtures.build_evidence(
                directory, tier="G2", pairs=1, policy_document=policy,
                counts={"S1": 200, "S2": 50},
                arm_spans={("S1", "A"): 1.0, ("S1", "B"): 8.0,
                           ("S2", "A"): 1.0, ("S2", "B"): 6.0},
                duration_by_arm={"A": (1_000_000, 2_000_000, 3_000_000),
                                 "B": (12_000_000, 12_000_000, 12_000_000)})
            run = evidence["runs"][("S1", 1, 0, "A")]
            import shutil
            shutil.rmtree(run["directory"])
            fixtures.add_run(evidence["root"], evidence["campaign"], policy, "S1", 1, 0, "A",
                             span_seconds=1.0, count=200,
                             duration_values=(6_000_000, 6_000_000, 6_000_000))
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G2", profile_name="quick", policy=policy)
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["sections"]["native"]["status"], "FAIL")
            self.assertTrue(any("candidate_absolute_failure" in reason
                                for reason in report["sections"]["native"]["reasons"]))

    def test_g2_s2_relative_threshold_is_enforced(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = fixtures.policy(pairs=1)
            evidence = fixtures.build_evidence(
                directory, tier="G2", pairs=1, policy_document=policy,
                counts={"S1": 200, "S2": 50},
                arm_spans={("S1", "A"): 1.0, ("S1", "B"): 8.0,
                           ("S2", "A"): 1.0, ("S2", "B"): 1.0},
                duration_by_arm={"A": (1_000_000, 2_000_000, 3_000_000),
                                 "B": (12_000_000, 12_000_000, 12_000_000)})
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G2", profile_name="quick", policy=policy)
            self.assertEqual(report["status"], "FAIL")
            groups = {(group["case"], group["concurrency"]): group
                      for group in report["sections"]["native"]["paired_groups"]}
            self.assertLess(groups[("S2", 2)]["median_ack_throughput_speedup"], 5)

    def test_paired_median_not_a_single_pair(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = fixtures.policy(name="formal", pairs=3, total_pass=True)
            evidence = fixtures.build_evidence(
                directory, tier="G2", pairs=3, policy_document=policy,
                counts={"S1": 200, "S2": 50},
                arm_spans={("S1", "A"): 1.0, ("S1", "B"): 8.0,
                           ("S2", "A"): 1.0, ("S2", "B"): 6.0},
                duration_by_arm={"A": 1_000_000},
                duration_by_pair={(0, "B"): 5_000_000, (1, "B"): 12_000_000,
                                  (2, "B"): 12_000_000})
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G2", profile_name="formal", policy=policy,
                                   trusted_artifacts=self._trusted(evidence["root"]))
            self.assertEqual(report["status"], "PASS")
            self.assertTrue(report["formal_pass"])
            self.assertEqual(report["exit_code"], 0)
            self.assertNotIn("status_note", report)
            group = next(group for group in report["sections"]["native"]["paired_groups"]
                         if group["case"] == "S1" and group["concurrency"] == 2)
            self.assertEqual(group["median_p50_speedup"], 12.0)
            self.assertEqual([ratio["p50_speedup"] for ratio in group["ratios"]],
                             [5.0, 12.0, 12.0])

    def test_g1_request_cannot_downgrade_g2_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = fixtures.policy(pairs=1)
            evidence = fixtures.build_evidence(
                directory, tier="G2", pairs=1, policy_document=policy,
                counts={"S1": 200, "S2": 50},
                arm_spans={("S1", "A"): 1.0, ("S1", "B"): 2.0,
                           ("S2", "A"): 1.0, ("S2", "B"): 2.0},
                duration_by_arm={"A": 6_000_000, "B": 12_000_000})
            run = evidence["runs"][("S2", 2, 0, "A")]
            self._rewrite_records(
                run, lambda records: [
                    dict(record, kind="write") if record["phase"] == "timed"
                    and record["kind"] == "read" else record for record in records])
            g1 = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G1", profile_name="quick", policy=policy)
            g2 = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G2", profile_name="quick", policy=policy)
            self.assertEqual(g1["status"], "NOT_RUN")
            self.assertEqual(g1["exit_code"], 1)
            self.assertFalse(g1["formal_pass"])
            self.assertTrue(g1["tier_downgrade_rejected"])
            self.assertEqual(g1["requested_tier"], "G1")
            self.assertEqual(g1["declared_tier"], "G2")
            self.assertEqual(g1["tier"], "G2")
            self.assertIn("tier_downgrade_rejected", " ".join(
                g1["sections"]["native"]["reasons"]))
            # The lower CLI tier must not hide corrupted S2 evidence.
            self.assertTrue(g1["runs"]["S2/C2/pair0/A"]["integrity"])
            self.assertEqual(g2["status"], "NOT_RUN")
            self.assertEqual(g2["exit_code"], 1)

    def test_tier_declarations_must_agree(self):
        for name in ("campaign-lower", "identity-lower"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                policy = fixtures.policy(pairs=1)
                evidence = fixtures.build_evidence(
                    directory, tier="G2", pairs=1, policy_document=policy,
                    counts={"S1": 200, "S2": 50},
                    arm_spans={("S1", "A"): 1.0, ("S1", "B"): 8.0,
                               ("S2", "A"): 1.0, ("S2", "B"): 6.0},
                    duration_by_arm={"A": (1_000_000, 2_000_000, 3_000_000),
                                     "B": (12_000_000, 12_000_000, 12_000_000)})
                if name == "campaign-lower":
                    document = copy.deepcopy(evidence["campaign"])
                    document["tier"] = "G1"
                    self._rehash_campaign(evidence, document)
                else:
                    path = Path(evidence["root"]) / "campaign-identity.json"
                    identity = json.loads(path.read_text())
                    identity["tier"] = "G1"
                    path.write_text(json.dumps(identity))
                report = gate._evaluate_arithmetic_for_tests(
                    evidence["root"], tier="G2", profile_name="quick", policy=policy)
                self.assertEqual(report["status"], "NOT_RUN")
                self.assertEqual(report["exit_code"], 1)
                self.assertTrue(report["tier_mismatch"])
                self.assertEqual(report["declared_tier"], "G2")
                self.assertIn("tier_declaration_mismatch", " ".join(
                    report["sections"]["native"]["reasons"]))

    def test_g1_evidence_passes_at_g1(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(directory)
            report = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G1", profile_name="quick",
                policy=evidence["policy"])
            self.assertEqual(report["status"], "PHASE_PASS")
            self.assertEqual(report["exit_code"], 1)
            self.assertFalse(report["formal_pass"])
            self.assertTrue(report["phase_only"])
            self.assertEqual(report["tier"], "G1")
            self.assertFalse(report["tier_downgrade_rejected"])
            self.assertFalse(report["tier_mismatch"])
            self.assertEqual(report["sections"]["native"]["status"], "PASS")

    def test_g1_omitted_s2_is_phase_only_and_g2_request_is_not_run(self):
        # A producer can declare G1 and never run S2; that is a legitimate
        # phase check at best and must never become an overall PASS.
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(directory)
            phase = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G1", profile_name="quick",
                policy=evidence["policy"])
            self.assertEqual(phase["status"], "PHASE_PASS")
            self.assertEqual(phase["exit_code"], 1)
            self.assertFalse(phase["formal_pass"])
            self.assertTrue(phase["phase_only"])
            self.assertFalse(any(key.startswith("S2/") for key in phase["runs"]))
            total = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G2", profile_name="quick",
                policy=evidence["policy"])
            self.assertEqual(total["status"], "NOT_RUN")
            self.assertEqual(total["exit_code"], 1)
            self.assertFalse(total["formal_pass"])
            self.assertIn("campaign_missing_required_runs: S1/C1,S2/C2",
                          " ".join(total["reasons"]))

    def test_g2_complete_campaign_is_the_only_total_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = fixtures.policy(name="formal", pairs=1, total_pass=True)
            evidence = fixtures.build_evidence(
                directory, tier="G2", pairs=1, policy_document=policy,
                counts={"S1": 200, "S2": 128},
                arm_spans={("S1", "A"): 1.0, ("S1", "B"): 8.0,
                           ("S2", "A"): 1.0, ("S2", "B"): 6.0},
                duration_by_arm={"A": (1_000_000, 2_000_000, 3_000_000),
                                 "B": (12_000_000, 12_000_000, 12_000_000)})
            report = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G2", profile_name="formal", policy=policy,
                trusted_artifacts=self._trusted(evidence["root"]))
            self.assertEqual(report["status"], "PASS")
            self.assertTrue(report["formal_pass"])
            self.assertFalse(report["phase_only"])
            self.assertEqual(report["exit_code"], 0)
            self.assertEqual(report["tier"], "G2")

    def test_one_candidate_round_absolute_failure_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = fixtures.policy(name="formal", pairs=3, total_pass=True)
            evidence = self._passing(directory, pairs=3, policy_document=policy)
            run = evidence["runs"][("S1", 2, 1, "A")]
            directory_path = run["directory"]
            import shutil
            shutil.rmtree(directory_path)
            fixtures.add_run(evidence["root"], evidence["campaign"], policy, "S1", 2, 1, "A",
                             span_seconds=2.0, count=600,
                             duration_values=(30_000_000, 30_000_000, 30_000_000))
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="formal",
                                   policy=policy,
                                   trusted_artifacts=self._trusted(evidence["root"]))
            self.assertEqual(report["status"], "FAIL")
            self.assertIn("paired", report["sections"]["native"])

    def test_sample_and_duration_shortfalls_are_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = fixtures.policy(minimum_samples=100, pairs=1)
            evidence = fixtures.build_evidence(directory, tier="G1", pairs=1,
                                               policy_document=policy, count=8)
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick", policy=policy)
            self.assertEqual(report["status"], "INCOMPLETE")
        with tempfile.TemporaryDirectory() as directory:
            policy = fixtures.policy(minimum_seconds=30, max_seconds=60, pairs=1)
            evidence = fixtures.build_evidence(directory, tier="G1", pairs=1,
                                               policy_document=policy, count=600, span_seconds=1.0)
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick", policy=policy)
            self.assertEqual(report["status"], "INCOMPLETE")

    def test_unknown_and_error_are_failures(self):
        for outcome in ("Unknown", "Error"):
            with self.subTest(outcome=outcome), tempfile.TemporaryDirectory() as directory:
                evidence = self._passing(directory)
                run = evidence["runs"][("S1", 2, 0, "A")]
                path = run["directory"] / "operations.jsonl"
                records = [json.loads(line) for line in path.read_text().splitlines()]
                target = next(record for record in records
                              if record["phase"] == "timed" and record["lane"] == 0)
                target.update(outcome=outcome, ack_type="", observed_sha256=None,
                              error_category="synthetic")
                path.write_text("".join(json.dumps(record) + "\n" for record in records))
                fixtures.reseal(run["directory"])
                report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                       policy=evidence["policy"])
                self.assertEqual(report["status"], "FAIL")
                self.assertIn(outcome.lower(), " ".join(report["sections"]["correctness"]["reasons"]).lower())

    def test_missing_and_tampered_evidence_are_not_run(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(directory)
            run = evidence["runs"][("S1", 2, 0, "A")]
            (run["directory"] / "operations.jsonl").unlink()
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                   policy=evidence["policy"])
            self.assertEqual(report["status"], "NOT_RUN")
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(directory)
            run = evidence["runs"][("S1", 2, 0, "A")]
            with (run["directory"] / "operations.jsonl").open("a") as output:
                output.write("{}\n")
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                   policy=evidence["policy"])
            self.assertEqual(report["status"], "NOT_RUN")

    def test_rss_missing_and_processes_not_joined_are_not_run(self):
        for mutate in ("rss", "join"):
            with self.subTest(mutate=mutate), tempfile.TemporaryDirectory() as directory:
                evidence = self._passing(directory)
                run = evidence["runs"][("S1", 2, 0, "A")]
                resources = json.loads((run["directory"] / "resources.json").read_text())
                if mutate == "rss":
                    resources.update(status="UNMEASURED")
                    resources["sampler"].update(status="UNMEASURED", peak_rss_bytes=None,
                                                last_window_rss_bytes=None)
                else:
                    resources.update(status="UNMEASURED", process_group_join_proof=False,
                                     all_processes_accounted=False)
                    for process in resources["processes"]:
                        process["group_joined"] = False
                (run["directory"] / "resources.json").write_text(json.dumps(resources))
                fixtures.reseal(run["directory"])
                report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                       policy=evidence["policy"])
                self.assertEqual(report["status"], "NOT_RUN")
                self.assertEqual(report["sections"]["resources"]["status"], "NOT_RUN")

    def test_relative_gate_cannot_pass_without_denominator(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = fixtures.policy(pairs=1)
            evidence = fixtures.build_evidence(directory, tier="G2", pairs=1,
                                               policy_document=policy, arms=("A",),
                                               counts={"S1": 200, "S2": 50})
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G2", profile_name="quick", policy=policy)
            self.assertEqual(report["status"], "NOT_RUN")

    def test_arm_plan_asymmetry_is_not_run(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(directory)
            run = evidence["runs"][("S1", 2, 0, "B")]
            metadata = json.loads((run["directory"] / "environment.json").read_text())
            metadata["max_seconds_per_arm"] = 20
            (run["directory"] / "environment.json").write_text(json.dumps(metadata))
            fixtures.reseal(run["directory"])
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                   policy=evidence["policy"])
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertIn("arm_plan_mismatch", " ".join(report["sections"]["native"]["reasons"]))

    def test_wrong_denominator_version_is_not_run(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(directory)
            run = evidence["runs"][("S1", 2, 0, "B")]
            metadata = json.loads((run["directory"] / "environment.json").read_text())
            metadata["arm_version"] = "v1.1.10"
            (run["directory"] / "environment.json").write_text(json.dumps(metadata))
            fixtures.reseal(run["directory"])
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                   policy=evidence["policy"])
            self.assertEqual(report["status"], "NOT_RUN")

    def test_metrics_snapshot_missing_blocks_overall_by_default(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory, metrics_requested=True)
            for run in evidence["runs"].values():
                for path in run["directory"].glob("metrics-snapshot*"):
                    path.unlink()
                fixtures.reseal(run["directory"])
            policy = evidence["policy"]
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick", policy=policy)
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertEqual(report["exit_code"], 1)
            self.assertEqual(report["sections"]["native"]["status"], "PASS")
            self.assertEqual(report["sections"]["attribution"]["status"], "NOT_RUN")
            self.assertTrue(report["sections"]["attribution"]["required"])
            for run in evidence["runs"].values():
                path = run["directory"] / "environment.json"
                metadata = json.loads(path.read_text())
                metadata["metrics_snapshot_requested"] = False
                path.write_text(json.dumps(metadata))
                fixtures.reseal(run["directory"])
            flipped = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                    policy=policy)
            self.assertEqual(flipped["status"], "NOT_RUN")
            self.assertTrue(flipped["sections"]["attribution"]["required"])
            identity_path = evidence["root"] / "campaign-identity.json"
            identity = json.loads(identity_path.read_text())
            identity["metrics_snapshot_requested"] = False
            identity_path.write_text(json.dumps(identity))
            unpinned = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                     policy=policy)
            self.assertEqual(unpinned["status"], "NOT_RUN")
            self.assertEqual(unpinned["exit_code"], 1)
            self.assertEqual(unpinned["sections"]["attribution"]["status"], "NOT_RUN")
            waived = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick", policy=policy,
                                   allow_missing_attribution=True)
            self.assertEqual(waived["status"], "PHASE_PASS")
            self.assertEqual(waived["sections"]["attribution"]["status"], "NOT_RUN")
            self.assertFalse(waived["sections"]["attribution"]["required"])
            self.assertEqual(waived["waivers"][0]["section"], "attribution")
            self.assertEqual(waived["waivers"][0]["status"], "NOT_RUN")
            required = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                     policy=policy, require_attribution=True)
            self.assertEqual(required["status"], "NOT_RUN")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                returncode = gate.main(["--evidence-root", str(evidence["root"]), "--tier", "G1"])
            self.assertEqual(returncode, 1)
            self.assertIn('"status": "NOT_RUN"', output.getvalue())

    def test_metrics_snapshot_content_is_validated(self):
        def opaque(document):
            document.clear()
            document["opaque"] = True

        def drop_counter(document):
            document["counters"] = [counter for counter in document["counters"]
                                    if counter["name"] != "object_store.put.total"]

        cases = (
            ("opaque", opaque, "metrics_snapshot_schema_version"),
            ("schema", lambda document: document.update(schema_version=2),
             "metrics_snapshot_schema_version"),
            ("phases", lambda document: document.pop("phases"), "metrics_snapshot_phases"),
            ("counters", lambda document: document.update(counters=[]), "metrics_snapshot_counters"),
            ("missing-counter", drop_counter,
             "metrics_snapshot_counter_missing:object_store.put.total"),
            ("negative-phase", lambda document: document["phases"][0].update(count=-1),
             "metrics_snapshot_phase_negative:snapshot:count"),
            ("negative-counter",
             lambda document: document["counters"][0].update(value=-1),
             "metrics_snapshot_counter_negative:work.wal_workload.append_calls"),
            ("disabled", lambda document: document.update(work_metrics_enabled=False),
             "metrics_snapshot_work_metrics_disabled"),
            ("generated-at", lambda document: document.update(generated_at_unix_ms=0),
             "metrics_snapshot_generated_at"),
        )
        for name, mutate, reason in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                evidence = self._real(directory)
                self._mutate_metrics(evidence, mutate)
                report = gate._evaluate_arithmetic_for_tests(
                    evidence["root"], tier="G1", profile_name="quick", policy=evidence["policy"])
                self.assertEqual(report["status"], "NOT_RUN")
                self.assertEqual(report["exit_code"], 1)
                self.assertEqual(report["sections"]["attribution"]["status"], "NOT_RUN")
                self.assertIn(reason, " ".join(report["sections"]["attribution"]["reasons"]))

    def test_metrics_snapshot_must_be_parseable_json(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory)
            self._mutate_metrics(evidence, raw=b"{not json\n")
            report = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G1", profile_name="quick", policy=evidence["policy"])
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertIn("metrics_snapshot_unreadable",
                          " ".join(report["sections"]["attribution"]["reasons"]))

    def test_metrics_snapshot_valid_content_and_unavailable_groups_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory)
            report = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G1", profile_name="quick", policy=evidence["policy"])
            self.assertEqual(report["sections"]["attribution"]["status"], "PASS")
            self.assertEqual(report["status"], "PHASE_PASS")
            for run in evidence["runs"].values():
                for item in json.loads(
                        (run["directory"] / "metrics-snapshot-index.json").read_text())["snapshots"]:
                    document = json.loads((run["directory"] / item["file"]).read_text())
                    self.assertFalse(document["group_phases"]["available"])

    def test_metrics_snapshot_is_bound_to_run_process_and_window(self):
        def pid_mismatch(document):
            document["process"]["pid"] += 99_999

        def before_timed(document):
            document["generated_at_unix_ms"] = 1_760_000_000_000

        def after_window(document):
            document["generated_at_unix_ms"] = 4_000_000_000_000

        def missing_pid(document):
            document.pop("process")

        cases = (
            ("pid-mismatch", pid_mismatch, "metrics_snapshot_worker_pid_mismatch:lane0"),
            ("before-timed-start", before_timed, "metrics_snapshot_before_timed_start:lane0"),
            ("after-run-window", after_window, "metrics_snapshot_after_run_window:lane0"),
            ("missing-pid", missing_pid, "metrics_snapshot_process_missing:lane0"),
        )
        for name, mutate, reason in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                evidence = self._passing(directory)
                run = evidence["runs"][("S1", 2, 0, "A")]
                self._mutate_snapshot(run, 0, mutate)
                report = gate._evaluate_arithmetic_for_tests(
                    evidence["root"], tier="G1", profile_name="quick",
                    policy=evidence["policy"])
                self.assertEqual(report["status"], "NOT_RUN")
                self.assertEqual(report["sections"]["attribution"]["status"], "NOT_RUN")
                self.assertIn(reason, " ".join(report["sections"]["attribution"]["reasons"]))
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(directory)
            target = evidence["runs"][("S1", 2, 0, "A")]
            source = evidence["runs"][("S1", 2, 0, "B")]
            self._replace_snapshot(target, 0, source, 0)
            report = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G1", profile_name="quick",
                policy=evidence["policy"])
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertEqual(report["sections"]["attribution"]["status"], "NOT_RUN")
            self.assertIn("metrics_snapshot_worker_pid_mismatch:lane0",
                          " ".join(report["sections"]["attribution"]["reasons"]))

    def test_metrics_wal_appends_must_cover_timed_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory)

            def zero_appends(document):
                for counter in document["counters"]:
                    if counter["name"] == "work.wal_workload.append_calls":
                        counter["value"] = 0
            self._mutate_metrics(evidence, zero_appends)
            report = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G1", profile_name="quick", policy=evidence["policy"])
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertIn("metrics_snapshot_wal_append_below_writes",
                          " ".join(report["sections"]["attribution"]["reasons"]))

    def test_summary_values_are_ignored(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(
                directory, duration_values=(30_000_000, 30_000_000, 30_000_000))
            run = evidence["runs"][("S1", 2, 0, "A")]
            (run["directory"] / "summary.json").write_text(json.dumps(
                {"p50_ms": 0.001, "p99_ms": 0.002, "ack_per_second": 99999}))
            fixtures.reseal(run["directory"])
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                   policy=evidence["policy"])
            self.assertEqual(report["status"], "FAIL")

    def test_oracle_content_is_recomputed(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(directory)
            run = evidence["runs"][("S1", 2, 0, "A")]
            oracle = json.loads((run["directory"] / "oracle.json").read_text())
            oracle["final"]["lanes"][0]["actual"][0][1] = "q" * 256
            (run["directory"] / "oracle.json").write_text(json.dumps(oracle))
            fixtures.reseal(run["directory"])
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                   policy=evidence["policy"])
            self.assertEqual(report["status"], "FAIL")
            self.assertIn("oracle_final_content_mismatch",
                          " ".join(report["sections"]["correctness"]["reasons"]))

    def test_full_scope_remains_not_run(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(directory)
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                   policy=evidence["policy"], scope="full")
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertEqual(report["sections"]["product"]["status"], "NOT_RUN")
            self.assertEqual(report["sections"]["s3_nonregression"]["status"], "NOT_RUN")

    def test_cli_never_prints_pass_for_missing_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            completed = subprocess.run(
                [sys.executable, str(Path(__file__).with_name("gate.py")),
                 "--evidence-root", directory, "--tier", "G1"],
                capture_output=True, text=True, timeout=20)
            self.assertNotEqual(completed.returncode, 0)
            self.assertNotIn('"status": "PASS"', completed.stdout)

    def test_real_quick_contract_minimum_is_phase_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory)
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                   policy=evidence["policy"])
            self.assertEqual(report["status"], "PHASE_PASS")
            self.assertEqual(report["exit_code"], 1)
            self.assertFalse(report["formal_pass"])
            self.assertEqual(report["sections"]["native"]["status"], "PASS")
            self.assertEqual(report["sections"]["resources"]["status"], "PASS")
            for run in report["runs"].values():
                self.assertGreaterEqual(run["span_seconds"], 30)
                for lane in run["lanes"].values():
                    self.assertGreaterEqual(lane["attempts"], 512)
                    self.assertEqual(lane["warmup_attempts"], 512)

    def test_status_rules_map_only_pass_to_zero_exit(self):
        rules = perf_contract.load_acceptance()["status_rules"]
        self.assertIs(rules["any_not_pass_exit_nonzero"], True)
        self.assertEqual(gate.status_exit_code(gate.PASS, rules), 0)
        for status in (gate.PHASE_PASS, gate.INCOMPLETE, gate.NOT_RUN, gate.FAIL):
            self.assertEqual(gate.status_exit_code(status, rules), 1)
        # Missing or contradicted rules fail closed.
        for broken in ({}, {"any_not_pass_exit_nonzero": False}):
            self.assertEqual(gate.status_exit_code(gate.PHASE_PASS, broken), 1)

    def test_cli_exits_nonzero_for_quick_waiver_and_foreign_phase_pass(self):
        gate_path = str(Path(__file__).with_name("gate.py"))

        def invoke(root, *extra):
            completed = subprocess.run(
                [sys.executable, gate_path, "--evidence-root", str(root), "--tier", "G1", *extra],
                capture_output=True, text=True, timeout=60)
            return completed.returncode, json.loads(completed.stdout)

        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory)
            code, summary = invoke(evidence["root"])
            self.assertEqual((code, summary["status"], summary["formal_pass"]), (1, "PHASE_PASS", False))
            self.assertIn("not a formal PASS", summary["status_note"])
            self.assertEqual(summary["exit_code"], 1)

        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory)
            for run in evidence["runs"].values():
                for path in run["directory"].glob("metrics-snapshot*"):
                    path.unlink()
                fixtures.reseal(run["directory"])
            code, summary = invoke(evidence["root"], "--allow-missing-attribution")
            self.assertEqual((code, summary["status"]), (1, "PHASE_PASS"))
            self.assertFalse(summary["formal_pass"])
            self.assertEqual(summary["waivers"][0]["section"], "attribution")

        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory)

            def foreign(metadata):
                metadata["host_environment"]["os"] = "NotThisOS"
                metadata["host_environment"]["arch"] = "not-this-arch"
            self._mutate_environments(evidence, foreign)
            code, summary = invoke(evidence["root"], "--allow-foreign-environment")
            self.assertEqual((code, summary["status"]), (1, "PHASE_PASS"))
            self.assertFalse(summary["formal_pass"])
            self.assertEqual(summary["waivers"][0]["section"], "environment")

    def test_quick_contract_sample_floor_accepts_512_over_30s(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory, count=512)
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                   policy=evidence["policy"])
            self.assertEqual(report["sections"]["native"]["status"], "FAIL")
            for run in report["runs"].values():
                self.assertEqual(run["shortage"], [])
                self.assertGreaterEqual(run["span_seconds"], 30)
            self.assertIn("candidate_absolute_failure:write_ack_per_second",
                          " ".join(report["sections"]["native"]["reasons"]))

    def test_real_quick_denominator_must_match_frozen_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory)
            document = json.loads((evidence["root"] / "campaign.json").read_text())
            denominator = next(label for label, arm in document["arms"].items()
                               if arm["role"] == "denominator")
            fallback = {"version": "v1.1.10",
                        "commit": "913a20b63203fe42078655d5e43dcc519e5108dc"}
            document["expected_denominator"] = dict(fallback)
            document["arms"][denominator].update(fallback)

            def relabel(metadata):
                if metadata["arm"] == denominator:
                    metadata.update(arm_version=fallback["version"],
                                    arm_commit=fallback["commit"])

            self._rehash_campaign(evidence, document, mutate_run=relabel)
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                   policy=evidence["policy"])
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertEqual(report["exit_code"], 1)
            self.assertIn("official_denominator_mismatch", " ".join(report["reasons"]))

    def test_real_quick_binary_source_and_artifacts_are_anchored(self):
        cases = ("binary-hash", "source-hash", "identity-hash", "artifact-content",
                 "artifacts-missing", "identity-missing")
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                evidence = self._real(directory)
                root = evidence["root"]
                if case == "artifacts-missing":
                    shutil.rmtree(root / "artifacts")
                elif case == "identity-missing":
                    (root / "campaign-identity.json").unlink()
                elif case == "identity-hash":
                    path = root / "campaign-identity.json"
                    identity = json.loads(path.read_text())
                    for arm in identity["arms"].values():
                        arm["sha256"] = "f" * 64
                    path.write_text(json.dumps(identity))
                elif case == "artifact-content":
                    for path in (root / "artifacts").rglob("*"):
                        if path.is_file():
                            path.write_bytes(b"replaced")
                else:
                    for run in evidence["runs"].values():
                        path = run["directory"] / "environment.json"
                        metadata = json.loads(path.read_text())
                        key = "binary_sha256" if case == "binary-hash" else "source_inventory_sha256"
                        metadata[key] = "f" * 64
                        path.write_text(json.dumps(metadata))
                        fixtures.reseal(run["directory"])
                report = gate._evaluate_arithmetic_for_tests(root, tier="G1", profile_name="quick",
                                       policy=evidence["policy"])
                self.assertEqual(report["status"], "NOT_RUN")
                self.assertEqual(report["exit_code"], 1)

    def test_real_quick_resources_are_recomputed_from_raw_samples(self):
        cases = ("samples-empty", "summary-tampered")
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                evidence = self._real(directory)
                for run in evidence["runs"].values():
                    path = run["directory"] / "resources.json"
                    resources = json.loads(path.read_text())
                    if case == "samples-empty":
                        resources["sampler"]["samples"] = []
                        resources["sampler"]["sample_count"] = 0
                    else:
                        resources["sampler"]["peak_rss_bytes"] += 1
                        resources["sampler"]["last_window_rss_bytes"] = [1]
                    path.write_text(json.dumps(resources))
                    fixtures.reseal(run["directory"])
                report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                       policy=evidence["policy"])
                self.assertEqual(report["status"], "NOT_RUN")
                self.assertEqual(report["sections"]["resources"]["status"], "NOT_RUN")
                reasons = " ".join(report["sections"]["resources"]["reasons"])
                expected = "raw_samples_missing" if case == "samples-empty" else "rss_peak_summary_mismatch"
                self.assertIn(expected, reasons)

    def test_real_quick_pair_order_is_validated(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory)
            for run in evidence["runs"].values():
                path = run["directory"] / "environment.json"
                metadata = json.loads(path.read_text())
                metadata["pair_order"] = "BA"
                path.write_text(json.dumps(metadata))
                fixtures.reseal(run["directory"])
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                   policy=evidence["policy"])
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertIn("pair_order_mismatch", " ".join(report["sections"]["native"]["reasons"]))

    def test_metrics_flags_cannot_exempt_attribution_by_default(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory, metrics_requested=True)
            self._mutate_environments(
                evidence, lambda metadata: metadata.update(metrics_snapshot_requested=False))
            identity_path = evidence["root"] / "campaign-identity.json"
            identity = json.loads(identity_path.read_text())
            identity["metrics_snapshot_requested"] = False
            identity_path.write_text(json.dumps(identity))
            policy = evidence["policy"]
            # The flag is informational: complete snapshots still count.
            present = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                    policy=policy)
            self.assertEqual(present["status"], "PHASE_PASS")
            self.assertEqual(present["sections"]["attribution"]["status"], "PASS")
            self.assertFalse(present["sections"]["attribution"]["requested"])
            waived_present = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                           policy=policy, allow_missing_attribution=True)
            self.assertEqual(waived_present["status"], "PHASE_PASS")
            self.assertEqual(waived_present["sections"]["attribution"]["status"], "NOT_RUN")
            for run in evidence["runs"].values():
                for path in run["directory"].glob("metrics-snapshot*"):
                    path.unlink()
                fixtures.reseal(run["directory"])
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick", policy=policy)
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertEqual(report["exit_code"], 1)
            self.assertTrue(report["sections"]["attribution"]["required"])
            self.assertEqual(report["sections"]["attribution"]["status"], "NOT_RUN")
            waived = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick", policy=policy,
                                   allow_missing_attribution=True)
            self.assertEqual(waived["status"], "PHASE_PASS")
            self.assertNotEqual(waived["status"], "PASS")
            self.assertEqual(waived["sections"]["attribution"]["status"], "NOT_RUN")
            self.assertEqual(waived["waivers"][0]["reasons"],
                             sorted(set(waived["sections"]["attribution"]["reasons"])))
            output = evidence["root"] / "gate-output.json"
            completed = subprocess.run(
                [sys.executable, str(Path(__file__).with_name("gate.py")),
                 "--evidence-root", str(evidence["root"]), "--tier", "G1",
                 "--output", str(output)],
                capture_output=True, text=True, timeout=30)
            self.assertEqual(completed.returncode, 1)
            self.assertIn('"status": "NOT_RUN"', completed.stdout)
            waived_output = evidence["root"] / "gate-waived.json"
            completed = subprocess.run(
                [sys.executable, str(Path(__file__).with_name("gate.py")),
                 "--evidence-root", str(evidence["root"]), "--tier", "G1",
                 "--allow-missing-attribution", "--output", str(waived_output)],
                capture_output=True, text=True, timeout=30)
            self.assertEqual(completed.returncode, 1)
            summary = json.loads(completed.stdout)
            self.assertEqual(summary["status"], "PHASE_PASS")
            self.assertFalse(summary["formal_pass"])
            self.assertIn("not a formal PASS", summary["status_note"])
            self.assertEqual(summary["waivers"][0]["section"], "attribution")
            self.assertEqual(summary["waivers"][0]["status"], "NOT_RUN")
            full = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick", policy=policy,
                                 scope="full")
            self.assertEqual(full["status"], "NOT_RUN")

    def test_formal_attribution_waiver_never_reports_total_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory, profile="formal", metrics_requested=True)
            trusted = self._trusted(evidence["root"])
            for run in evidence["runs"].values():
                for path in run["directory"].glob("metrics-snapshot*"):
                    path.unlink()
                fixtures.reseal(run["directory"])
            policy = evidence["policy"]
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="formal",
                                   policy=policy, trusted_artifacts=trusted)
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertEqual(report["sections"]["attribution"]["status"], "NOT_RUN")
            waived = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="formal",
                                   policy=policy, trusted_artifacts=trusted,
                                   allow_missing_attribution=True)
            self.assertEqual(waived["status"], "PHASE_PASS")
            self.assertNotEqual(waived["status"], "PASS")
            self.assertEqual(waived["exit_code"], 1)
            self.assertFalse(waived["formal_pass"])
            self.assertEqual(waived["sections"]["attribution"]["status"], "NOT_RUN")
            self.assertTrue(waived["waivers"])
            output = evidence["root"] / "formal-waived.json"
            completed = subprocess.run(
                [sys.executable, str(Path(__file__).with_name("gate.py")),
                 "--evidence-root", str(evidence["root"]), "--tier", "G1",
                 "--trusted-artifacts", str(trusted), "--allow-missing-attribution",
                 "--output", str(output)],
                capture_output=True, text=True, timeout=60)
            self.assertEqual(completed.returncode, 1)
            summary = json.loads(completed.stdout)
            self.assertEqual(summary["status"], "PHASE_PASS")
            self.assertNotEqual(summary["status"], "PASS")
            self.assertFalse(summary["formal_pass"])

    def test_schedule_index_and_row_coverage_are_enforced(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory)
            run = evidence["runs"][("S1", 2, 0, "A")]
            path = run["directory"] / "operations.jsonl"
            records = [json.loads(line) for line in path.read_text().splitlines()]
            for record in records:
                record.update(schedule_index=0, row=0)
            path.write_text("".join(json.dumps(record) + "\n" for record in records))
            fixtures.reseal(run["directory"])
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                   policy=evidence["policy"])
            self.assertEqual(report["sections"]["native"]["status"], "NOT_RUN")
            self.assertIn("record_schedule_index_mismatch",
                          report["runs"]["S1/C2/pair0/A"]["integrity"])
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory)
            run = evidence["runs"][("S1", 2, 0, "B")]
            path = run["directory"] / "operations.jsonl"
            records = [json.loads(line) for line in path.read_text().splitlines()]
            target = next(record for record in records if record["phase"] == "timed")
            target["schedule_index"] += 1
            target["row"] = target["schedule_index"] % 64
            path.write_text("".join(json.dumps(record) + "\n" for record in records))
            fixtures.reseal(run["directory"])
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                   policy=evidence["policy"])
            self.assertEqual(report["sections"]["native"]["status"], "NOT_RUN")
            self.assertIn("schedule_index_gap",
                          " ".join(report["runs"]["S1/C2/pair0/B"]["integrity"]))
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory, tier="G2")
            run = evidence["runs"][("S2", 2, 0, "A")]
            path = run["directory"] / "operations.jsonl"
            records = [json.loads(line) for line in path.read_text().splitlines()]
            target = next(record for record in records
                          if record["phase"] == "timed" and record["kind"] == "read")
            target["kind"] = "write"
            path.write_text("".join(json.dumps(record) + "\n" for record in records))
            fixtures.reseal(run["directory"])
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G2", profile_name="quick",
                                   policy=evidence["policy"])
            self.assertNotEqual(report["status"], "PASS")
            self.assertIn("record_kind_schedule_mismatch",
                          " ".join(report["runs"]["S2/C2/pair0/A"]["integrity"]))
            self.assertIn("s2_read_write_row_coverage:lane0:timed",
                          report["runs"]["S2/C2/pair0/A"]["integrity"])

    def test_declared_plan_above_actual_is_incomplete(self):
        cases = (("duration", {"minimum_seconds": 60}, "duration_below_declared_plan"),
                 ("operations", {"required_operations_per_lane": 10000},
                  "samples_below_declared_plan"))
        for name, update, reason in cases:
            with self.subTest(case=name), tempfile.TemporaryDirectory() as directory:
                evidence = self._real(directory)
                self._mutate_environments(
                    evidence, lambda metadata: metadata.update(update))
                report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                       policy=evidence["policy"])
                self.assertEqual(report["status"], "INCOMPLETE")
                self.assertEqual(report["exit_code"], 1)
                self.assertIn(reason,
                              " ".join(report["runs"]["S1/C2/pair0/A"]["shortage"]))
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory)
            self._mutate_environments(
                evidence, lambda metadata: metadata.update(required_operations_per_lane=100,
                                                           minimum_seconds=1))
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                   policy=evidence["policy"])
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertIn("operation_plan_below_contract",
                          " ".join(error["error"] for error in report["evidence_errors"]))
            self.assertIn("duration_plan_below_contract",
                          " ".join(error["error"] for error in report["evidence_errors"]))

    def test_formal_requires_external_trusted_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory, profile="formal")
            trusted = self._trusted(evidence["root"])
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="formal",
                                   policy=evidence["policy"])
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertIn("trusted_artifacts_required", " ".join(report["reasons"]))
            accepted = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="formal",
                                     policy=evidence["policy"], trusted_artifacts=trusted)
            self.assertEqual(accepted["status"], "PHASE_PASS")
        with tempfile.TemporaryDirectory() as directory:
            policy = fixtures.policy(name="formal", pairs=3, total_pass=True)
            evidence = self._passing(directory, pairs=3, policy_document=policy)
            trusted = self._trusted(evidence["root"])
            wrong = Path(evidence["root"]).parent / (Path(evidence["root"]).name + "-wrong-binary.json")
            wrong.write_text(json.dumps({"A": fixtures.artifact_sha("A"),
                                         "B": "0" * 64}))
            rejected = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="formal",
                                     policy=policy, trusted_artifacts=wrong)
            self.assertEqual(rejected["status"], "NOT_RUN")
            self.assertIn("trusted_artifact_mismatch:B", " ".join(rejected["reasons"]))
            self._sync_wrong_binary(evidence, "B")
            synced = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="formal",
                                   policy=policy, trusted_artifacts=trusted)
            self.assertEqual(synced["status"], "NOT_RUN")
            self.assertIn("trusted_artifact_mismatch:B", " ".join(synced["reasons"]))

    def _sync_wrong_binary(self, evidence, label):
        root = evidence["root"]
        artifact = sorted((root / "artifacts" / label).iterdir())[0]
        artifact.write_bytes(b"wrong-binary-" + label.encode())
        digest = perf_contract.digest(artifact)
        identity_path = root / "campaign-identity.json"
        identity = json.loads(identity_path.read_text())
        identity["arms"][label]["sha256"] = digest
        identity_path.write_text(json.dumps(identity))
        for run in evidence["runs"].values():
            path = run["directory"] / "environment.json"
            metadata = json.loads(path.read_text())
            if metadata["arm"] == label:
                metadata["binary_sha256"] = digest
            path.write_text(json.dumps(metadata))
            fixtures.reseal(run["directory"])

    def test_denominator_version_is_frozen_by_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory)
            document = json.loads((evidence["root"] / "campaign.json").read_text())
            document["expected_denominator"]["version"] = "v1.1.10"
            self._rehash_campaign(evidence, document)
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="quick",
                                   policy=evidence["policy"])
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertIn("expected_denominator_version", " ".join(report["reasons"]))

    def test_real_formal_contract_minimum_and_pair_order(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory, profile="formal")
            trusted = self._trusted(evidence["root"])
            report = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="formal",
                                   policy=evidence["policy"], trusted_artifacts=trusted)
            self.assertEqual(report["status"], "PHASE_PASS")
            self.assertEqual(report["sections"]["native"]["status"], "PASS")
            group = report["sections"]["native"]["paired_groups"][0]
            self.assertEqual(group["pair_count"], 3)
            self.assertEqual([ratio["pair"] for ratio in group["ratios"]], [0, 1, 2])
            for run in report["runs"].values():
                self.assertGreaterEqual(run["span_seconds"], 180)
            for run in evidence["runs"].values():
                path = run["directory"] / "environment.json"
                metadata = json.loads(path.read_text())
                if metadata["pair"] == 1:
                    metadata["pair_order"] = "AB"
                path.write_text(json.dumps(metadata))
                fixtures.reseal(run["directory"])
            rejected = gate._evaluate_arithmetic_for_tests(evidence["root"], tier="G1", profile_name="formal",
                                     policy=evidence["policy"], trusted_artifacts=trusted)
            self.assertEqual(rejected["status"], "NOT_RUN")
            self.assertIn("pair_order_mismatch:pair1",
                          " ".join(rejected["sections"]["native"]["reasons"]))


    def test_schedule_file_order_permutations_are_rejected(self):
        def swap_seq_and_index(records):
            targets = [record for record in records
                       if record["phase"] == "timed" and record["lane"] == 0][:2]
            for key in ("seq", "schedule_index"):
                targets[0][key], targets[1][key] = targets[1][key], targets[0][key]
            for record in targets:
                record["row"] = record["schedule_index"] % 64
            return records

        def reverse_timed(records):
            positions = [index for index, record in enumerate(records)
                         if record["phase"] == "timed" and record["lane"] == 0]
            ordered = [records[index] for index in reversed(positions)]
            for position, record in zip(positions, ordered):
                records[position] = record
            return records

        def group_by_row(records):
            positions = [index for index, record in enumerate(records)
                         if record["phase"] == "timed" and record["lane"] == 0]
            ordered = sorted((records[index] for index in positions),
                             key=lambda record: record["row"])
            for position, record in zip(positions, ordered):
                records[position] = record
            return records

        def shuffle_all(records):
            return records[1::2] + records[0::2]

        for name, transform in (("swap", swap_seq_and_index),
                                ("reverse", reverse_timed),
                                ("group-by-row", group_by_row),
                                ("whole-file-shuffle", shuffle_all)):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                evidence = self._passing(directory)
                run = evidence["runs"][("S1", 2, 0, "A")]
                self._rewrite_records(run, transform)
                report = gate._evaluate_arithmetic_for_tests(
                    evidence["root"], tier="G1", profile_name="quick",
                    policy=evidence["policy"])
                self.assertEqual(report["status"], "NOT_RUN")
                integrity = " ".join(report["runs"]["S1/C2/pair0/A"]["integrity"])
                self.assertTrue("timed_sequence_gap:lane0" in integrity
                                or "schedule_index_gap:lane0:timed" in integrity, integrity)

    def test_warmup_and_timed_interleaving_is_rejected(self):
        def interleave(records):
            timed = next(index for index, record in enumerate(records)
                         if record["phase"] == "timed" and record["lane"] == 0)
            last_warmup = max(index for index, record in enumerate(records)
                              if record["phase"] == "warmup" and record["lane"] == 0)
            record = records.pop(timed)
            records.insert(last_warmup, record)
            return records

        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(directory)
            run = evidence["runs"][("S1", 2, 0, "A")]
            self._rewrite_records(run, interleave)
            report = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G1", profile_name="quick",
                policy=evidence["policy"])
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertIn("warmup_after_timed", report["runs"]["S1/C2/pair0/A"]["integrity"])

    def test_warmup_and_timed_timestamps_must_agree(self):
        def late_warmup(records):
            timed = [record for record in records
                     if record["phase"] == "timed" and record["lane"] == 0]
            first_timed = min(record["issued_at"] for record in timed)
            target = next(record for record in records
                          if record["phase"] == "warmup" and record["lane"] == 0)
            target["completed_at"] = first_timed + 1_000_000
            target["issued_at"] = target["completed_at"] - target["duration_ns"]
            target["planned_at"] = target["issued_at"] - 1
            return records

        def regressed_timed(records):
            target = next(record for record in records
                          if record["phase"] == "timed" and record["lane"] == 0)
            following = next(record for record in records
                             if record["phase"] == "timed" and record["lane"] == 0
                             and record is not target)
            target["issued_at"] = following["issued_at"] + 1_000_000
            target["completed_at"] = target["issued_at"] + target["duration_ns"]
            target["planned_at"] = target["issued_at"] - 1
            return records

        for name, transform, reason in (
                ("late-warmup", late_warmup, "warmup_completed_after_timed_start:lane0"),
                ("regressed-timed", regressed_timed, "timed_record_time_regressed:lane0")):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                evidence = self._passing(directory)
                run = evidence["runs"][("S1", 2, 0, "A")]
                self._rewrite_records(run, transform)
                report = gate._evaluate_arithmetic_for_tests(
                    evidence["root"], tier="G1", profile_name="quick",
                    policy=evidence["policy"])
                self.assertEqual(report["status"], "NOT_RUN")
                self.assertIn(reason, report["runs"]["S1/C2/pair0/A"]["integrity"])

    def test_s2_schedule_permutation_is_rejected(self):
        def swap_seq_and_index(records):
            targets = [record for record in records
                       if record["phase"] == "timed" and record["lane"] == 0][:2]
            for key in ("seq", "schedule_index"):
                targets[0][key], targets[1][key] = targets[1][key], targets[0][key]
            for record in targets:
                record["row"] = record["schedule_index"] % 64
            return records

        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(directory, tier="G2", counts={"S2": 128})
            run = evidence["runs"][("S2", 2, 0, "A")]
            self._rewrite_records(run, swap_seq_and_index)
            report = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G2", profile_name="quick",
                policy=evidence["policy"])
            self.assertEqual(report["status"], "NOT_RUN")
            integrity = " ".join(report["runs"]["S2/C2/pair0/A"]["integrity"])
            self.assertIn("timed_sequence_gap:lane0", integrity)

    def test_environment_contract_is_required_and_validated(self):
        cases = (
            ("tmpfs-store", lambda metadata: metadata["roots_validation"]["filesystems"]
             .__setitem__("store", "tmpfs"), "forbidden_filesystem"),
            ("overlay-cache", lambda metadata: metadata["roots_validation"]["filesystems"]
             .__setitem__("cache", "overlay"), "forbidden_filesystem"),
            ("persistent-false", lambda metadata: metadata["roots_validation"]
             .update(persistent_mounts_verified=False), "persistent_mounts_not_verified"),
            ("missing-host", lambda metadata: metadata.pop("host_environment"),
             "missing_host_environment"),
            ("missing-roots", lambda metadata: metadata.pop("roots_validation"),
             "missing_roots_validation"),
            ("missing-kernel", lambda metadata: metadata["host_environment"].pop("kernel"),
             "host_environment_missing:kernel"),
            ("missing-filesystems", lambda metadata: metadata["roots_validation"]
             .pop("filesystems"), "root_filesystem_missing"),
            ("tmp-store-root", lambda metadata: metadata["roots_validation"]["roots"]
             .__setitem__("store", "/tmp/perf-store"), "root_on_ephemeral_path"),
            ("shm-cache-root", lambda metadata: metadata["roots_validation"]["roots"]
             .__setitem__("cache", "/dev/shm/perf-cache"), "root_on_ephemeral_path"),
        )
        for name, mutate, reason in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                evidence = self._passing(directory)
                self._mutate_environments(evidence, mutate)
                report = gate._evaluate_arithmetic_for_tests(
                    evidence["root"], tier="G1", profile_name="quick",
                    policy=evidence["policy"])
                self.assertEqual(report["status"], "NOT_RUN")
                self.assertIn(reason, " ".join(error["error"]
                                               for error in report["evidence_errors"]))
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(directory)

            def embed_store(metadata, evidence=evidence):
                metadata["roots_validation"]["roots"]["store"] = str(
                    Path(evidence["root"]) / "store")

            self._mutate_environments(evidence, embed_store)
            report = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G1", profile_name="quick",
                policy=evidence["policy"])
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertIn("root_inside_evidence:store",
                          " ".join(error["error"] for error in report["evidence_errors"]))

    def test_foreign_environment_requires_flag_and_caps_at_phase_pass(self):
        def foreign(metadata):
            metadata["host_environment"]["os"] = "NotThisOS"
            metadata["host_environment"]["arch"] = "not-this-arch"

        policy = fixtures.policy(name="formal", pairs=3, total_pass=True)
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(directory, pairs=3, policy_document=policy)
            trusted = self._trusted(evidence["root"])
            self._mutate_environments(evidence, foreign)
            rejected = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G1", profile_name="formal",
                policy=policy, trusted_artifacts=trusted)
            self.assertEqual(rejected["status"], "NOT_RUN")
            self.assertIn("foreign_environment_not_allowed",
                          " ".join(error["error"] for error in rejected["evidence_errors"]))
            allowed = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G1", profile_name="formal",
                policy=policy, trusted_artifacts=trusted,
                allow_foreign_environment=True)
            self.assertEqual(allowed["status"], "PHASE_PASS")
            self.assertTrue(allowed["foreign_environment"])
            self.assertEqual(allowed["waivers"][0]["section"], "environment")
            self.assertNotEqual(allowed["status"], "PASS")

    def test_run_and_campaign_failure_markers_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = self._passing(directory)
            self._mutate_environments(
                candidate, lambda metadata: metadata.update(
                    failure="RunError: worker_timing_missing"))
            report = gate._evaluate_arithmetic_for_tests(
                candidate["root"], tier="G1", profile_name="quick",
                policy=candidate["policy"])
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertEqual(report["exit_code"], 1)
            self.assertIn("environment_failure", " ".join(
                error["error"] for error in report["evidence_errors"]))
        with tempfile.TemporaryDirectory() as directory:
            candidate = self._passing(directory)
            for run in candidate["runs"].values():
                path = run["directory"] / "summary.json"
                summary = json.loads(path.read_text())
                summary["failure"] = "RunError: worker_timing_missing"
                path.write_text(json.dumps(summary))
                fixtures.reseal(run["directory"])
            report = gate._evaluate_arithmetic_for_tests(
                candidate["root"], tier="G1", profile_name="quick",
                policy=candidate["policy"])
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertIn("run_summary_failure", " ".join(
                error["error"] for error in report["evidence_errors"]))
        for target, reason in (("campaign", "campaign_failure"),
                               ("arm", "campaign_arm_failure"),
                               ("identity", "identity_failure"),
                               ("identity-arm", "identity_arm_failure")):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as directory:
                candidate = self._passing(directory)
                document = copy.deepcopy(candidate["campaign"])
                if target == "campaign":
                    document["failure"] = "RunError: campaign_aborted"
                    (Path(candidate["root"]) / "campaign.json").write_text(
                        json.dumps(document))
                elif target == "arm":
                    document["arms"]["A"]["failure"] = "RunError: arm_aborted"
                    (Path(candidate["root"]) / "campaign.json").write_text(
                        json.dumps(document))
                else:
                    path = Path(candidate["root"]) / "campaign-identity.json"
                    identity = json.loads(path.read_text())
                    if target == "identity":
                        identity["failure"] = "RunError: campaign_aborted"
                    else:
                        identity["arms"]["A"]["failure"] = "RunError: arm_aborted"
                    path.write_text(json.dumps(identity))
                report = gate._evaluate_arithmetic_for_tests(
                    candidate["root"], tier="G1", profile_name="quick",
                    policy=candidate["policy"])
                self.assertEqual(report["status"], "NOT_RUN")
                self.assertEqual(report["exit_code"], 1)
                self.assertIn(reason, " ".join(report["reasons"]))

    def test_process_accounting_is_enforced(self):
        cases = (
            ("one-process",
             lambda resources: resources["processes"].__setitem__(
                 slice(None), resources["processes"][:1]),
             "process_count_below_concurrency"),
            ("no-pid",
             lambda resources: resources["processes"].__setitem__(
                 slice(None), [{"group_joined": True, "exit_code": 0}]),
             "process_pid_invalid"),
            ("duplicate-pid",
             lambda resources: resources["processes"][1].__setitem__(
                 "pid", resources["processes"][0]["pid"]),
             "process_pid_duplicate"),
            ("cleanup-failure",
             lambda resources: resources["processes"][0].__setitem__(
                 "cleanup_failure", "SIGKILL"),
             "process_cleanup_failure"),
            ("group-proof-failure",
             lambda resources: resources["processes"][0].__setitem__(
                 "group_proof_failure", "ps_unavailable"),
             "process_group_proof_failure"),
            ("group-live-remaining",
             lambda resources: resources["processes"][0].__setitem__(
                 "group_live_remaining", [{"pid": 1, "state": "R"}]),
             "process_group_live_remaining"),
        )
        for name, mutate, reason in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                candidate = self._passing(directory)
                run = candidate["runs"][("S1", 2, 0, "A")]
                self._mutate_resources(run, mutate)
                report = gate._evaluate_arithmetic_for_tests(
                    candidate["root"], tier="G1", profile_name="quick",
                    policy=candidate["policy"])
                self.assertEqual(report["status"], "NOT_RUN")
                self.assertEqual(report["sections"]["resources"]["status"], "NOT_RUN")
                self.assertIn(reason, report["sections"]["resources"]["reasons"])

    def test_raw_sample_failures_are_rejected(self):
        cases = (
            ("failure",
             lambda sample: sample.update(failure="ps_unavailable", rss_bytes=None,
                                          missing_roots=[100, 101]),
             ("raw_sample_failure:0", "raw_sample_missing_roots:0",
              "raw_sample_rss:0")),
            ("missing-roots",
             lambda sample: sample.update(missing_roots=[100]),
             ("raw_sample_missing_roots:0",)),
            ("null-rss",
             lambda sample: sample.update(rss_bytes=None),
             ("raw_sample_rss:0",)),
            ("negative-rss",
             lambda sample: sample.update(rss_bytes=-1),
             ("raw_sample_rss:0",)),
        )
        for name, mutate, reasons in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                candidate = self._passing(directory)
                run = candidate["runs"][("S1", 2, 0, "A")]
                self._mutate_resources(
                    run, lambda resources: mutate(resources["sampler"]["samples"][0]))
                report = gate._evaluate_arithmetic_for_tests(
                    candidate["root"], tier="G1", profile_name="quick",
                    policy=candidate["policy"])
                self.assertEqual(report["status"], "NOT_RUN")
                self.assertEqual(report["sections"]["resources"]["status"], "NOT_RUN")
                for reason in reasons:
                    self.assertIn(reason, report["sections"]["resources"]["reasons"])
                self.assertNotIn("sampler_failures",
                                 report["sections"]["resources"]["reasons"])

    def test_resource_sampling_density_and_coverage_are_enforced(self):
        def one_sample(resources):
            sampler = resources["sampler"]
            sample = sampler["samples"][0]
            sampler["samples"] = [sample]
            sampler["sample_count"] = 1
            sampler["peak_rss_bytes"] = sample["rss_bytes"]
            sampler["last_window_rss_bytes"] = [sample["rss_bytes"]]

        def short_coverage(resources):
            samples = resources["sampler"]["samples"]
            start = samples[0]["monotonic_ns"]
            step = 100_000_000 // max(1, len(samples))
            for index, sample in enumerate(samples):
                sample["monotonic_ns"] = start + index * step

        def before_window(resources):
            environment = json.loads(
                (resources["_run"] / "environment.json").read_text())
            span = environment["timed_completed_at_ns"] - environment["timed_started_at_ns"]
            for sample in resources["sampler"]["samples"]:
                sample["monotonic_ns"] -= span + 1_000_000_000

        cases = (
            ("one-sample", one_sample,
             ("resource_sample_density_low", "resource_sample_coverage_short")),
            ("short-coverage", short_coverage, ("resource_sample_coverage_short",)),
            ("before-window", before_window, ("resource_sample_window_end_early",)),
        )
        for name, mutate, reasons in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                evidence = self._passing(directory)
                run = evidence["runs"][("S1", 2, 0, "A")]

                def resource_mutation(resources, run=run, mutate=mutate):
                    resources["_run"] = run["directory"]
                    mutate(resources)
                    resources.pop("_run", None)

                self._mutate_resources(run, resource_mutation)
                report = gate._evaluate_arithmetic_for_tests(
                    evidence["root"], tier="G1", profile_name="quick",
                    policy=evidence["policy"])
                self.assertEqual(report["status"], "NOT_RUN")
                self.assertEqual(report["sections"]["resources"]["status"], "NOT_RUN")
                for reason in reasons:
                    self.assertIn(reason, report["sections"]["resources"]["reasons"])
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(directory)
            report = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G1", profile_name="quick",
                policy=evidence["policy"])
            self.assertEqual(report["sections"]["resources"]["status"], "PASS")

    def test_process_pids_must_appear_in_raw_samples(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(directory)
            run = evidence["runs"][("S1", 2, 0, "A")]

            def drop_sampled_pid(resources):
                pid = resources["processes"][0]["pid"]
                for sample in resources["sampler"]["samples"]:
                    sample["pids"] = [value for value in sample["pids"] if value != pid]

            self._mutate_resources(run, drop_sampled_pid)
            report = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G1", profile_name="quick",
                policy=evidence["policy"])
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertTrue(any(reason.startswith("process_pid_not_sampled:")
                                for reason in report["sections"]["resources"]["reasons"]))

    def test_resource_samples_must_cover_timed_window(self):
        def window_bounds(run):
            environment = json.loads((run["directory"] / "environment.json").read_text())
            return environment["timed_started_at_ns"], environment["timed_completed_at_ns"]

        def too_few_inside(resources, run):
            started, completed = window_bounds(run)
            sampler = resources["sampler"]
            template = sampler["samples"][0]
            kept = []
            for when in (started, (started + completed) // 2, completed):
                sample = dict(template)
                sample["monotonic_ns"] = when
                kept.append(sample)
            sampler["samples"] = kept
            sampler["sample_count"] = len(kept)
            sampler["peak_rss_bytes"] = max(sample["rss_bytes"] for sample in kept)
            sampler["last_window_rss_bytes"] = [kept[-1]["rss_bytes"]]

        def long_gap_inside(resources, run):
            started, completed = window_bounds(run)
            samples = resources["sampler"]["samples"]
            inside = [index for index, sample in enumerate(samples)
                      if started <= sample["monotonic_ns"] <= completed]
            for offset, index in enumerate(inside[:-1]):
                samples[index]["monotonic_ns"] = started + offset * 1_000_000
            samples[inside[-1]]["monotonic_ns"] = completed

        cases = (
            ("stacked-outside", self._stack_samples_outside_window,
             ("resource_sample_density_low", "resource_sample_coverage_short")),
            ("too-few-inside", too_few_inside, ("resource_sample_density_low",)),
            ("long-gap-inside", long_gap_inside, ("resource_sample_window_gap",)),
        )
        for name, mutate, reasons in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                evidence = self._passing(directory)
                run = evidence["runs"][("S1", 2, 0, "A")]

                def resource_mutation(resources, run=run, mutate=mutate):
                    mutate(resources, run)

                self._mutate_resources(run, resource_mutation)
                report = gate._evaluate_arithmetic_for_tests(
                    evidence["root"], tier="G1", profile_name="quick",
                    policy=evidence["policy"])
                self.assertEqual(report["status"], "NOT_RUN")
                self.assertEqual(report["sections"]["resources"]["status"], "NOT_RUN")
                for reason in reasons:
                    self.assertIn(reason, report["sections"]["resources"]["reasons"])
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(directory)
            report = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G1", profile_name="quick",
                policy=evidence["policy"])
            self.assertEqual(report["sections"]["resources"]["status"], "PASS")

    def test_stacked_outside_samples_cannot_produce_formal_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = fixtures.policy(name="formal", pairs=1, total_pass=True)
            evidence = fixtures.build_evidence(
                directory, tier="G2", pairs=1, policy_document=policy,
                counts={"S1": 200, "S2": 128},
                arm_spans={("S1", "A"): 1.0, ("S1", "B"): 8.0,
                           ("S2", "A"): 1.0, ("S2", "B"): 6.0},
                duration_by_arm={"A": (1_000_000, 2_000_000, 3_000_000),
                                 "B": (12_000_000, 12_000_000, 12_000_000)})
            trusted = self._trusted(evidence["root"])
            baseline = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G2", profile_name="formal", policy=policy,
                trusted_artifacts=trusted)
            self.assertEqual(baseline["status"], "PASS")
            run = evidence["runs"][("S1", 2, 0, "A")]
            self._mutate_resources(
                run, lambda resources: self._stack_samples_outside_window(resources, run))
            report = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G2", profile_name="formal", policy=policy,
                trusted_artifacts=trusted)
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertEqual(report["exit_code"], 1)
            self.assertEqual(report["sections"]["resources"]["status"], "NOT_RUN")
            self.assertIn("resource_sample_density_low",
                          report["sections"]["resources"]["reasons"])
            self.assertIn("resource_sample_coverage_short",
                          report["sections"]["resources"]["reasons"])

    def test_resource_window_samples_require_pids_and_roots(self):
        def window_indices(resources, run):
            environment = json.loads((run["directory"] / "environment.json").read_text())
            started = environment["timed_started_at_ns"]
            completed = environment["timed_completed_at_ns"]
            return [index for index, sample in enumerate(resources["sampler"]["samples"])
                    if started <= sample["monotonic_ns"] <= completed]

        def empty_pids(resources, run):
            index = window_indices(resources, run)[0]
            resources["sampler"]["samples"][index]["pids"] = []

        def empty_roots(resources, run):
            index = window_indices(resources, run)[0]
            resources["sampler"]["samples"][index]["roots"] = []

        def missing_roots(resources, run):
            index = window_indices(resources, run)[0]
            resources["sampler"]["samples"][index].pop("roots")

        def mismatched_roots(resources, run):
            index = window_indices(resources, run)[0]
            pid = resources["sampler"]["samples"][index]["roots"][0]
            resources["sampler"]["samples"][index]["roots"] = [pid + 1_000_000]

        def alternate_pids(resources, run):
            workers = sorted(process["pid"] for process in resources["processes"]
                             if "lane" in process)
            for offset, index in enumerate(window_indices(resources, run)):
                resources["sampler"]["samples"][index]["pids"] = [
                    workers[offset % len(workers)]]

        def pid_only_before_window(resources, run):
            workers = sorted(process["pid"] for process in resources["processes"]
                             if "lane" in process)
            for index in window_indices(resources, run):
                resources["sampler"]["samples"][index]["pids"] = [workers[0]]

        cases = (
            ("empty-pids", empty_pids, "raw_sample_pids_empty:"),
            ("empty-roots", empty_roots, "raw_sample_roots_empty:"),
            ("missing-roots", missing_roots, "raw_sample_roots:"),
            ("mismatched-roots", mismatched_roots, "raw_sample_roots_mismatch:"),
            ("alternate-pids", alternate_pids, "resource_window_pids_not_together"),
            ("pid-only-before-window", pid_only_before_window,
             "resource_window_pid_unsampled:"),
        )
        for name, mutate, reason in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                evidence = self._passing(directory)
                run = evidence["runs"][("S1", 2, 0, "A")]

                def resource_mutation(resources, run=run, mutate=mutate):
                    mutate(resources, run)

                self._mutate_resources(run, resource_mutation)
                report = gate._evaluate_arithmetic_for_tests(
                    evidence["root"], tier="G1", profile_name="quick",
                    policy=evidence["policy"])
                self.assertEqual(report["status"], "NOT_RUN")
                self.assertEqual(report["sections"]["resources"]["status"], "NOT_RUN")
                self.assertTrue(any(item.startswith(reason)
                                    for item in report["sections"]["resources"]["reasons"]),
                                report["sections"]["resources"]["reasons"])
        with tempfile.TemporaryDirectory() as directory:
            # Positive case: every in-window sample contains all worker pids
            # and the matching process-root list.
            evidence = self._passing(directory)
            report = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G1", profile_name="quick",
                policy=evidence["policy"])
            self.assertEqual(report["sections"]["resources"]["status"], "PASS")

    def test_resource_failure_signals_are_rejected(self):
        cases = (
            ("sampler-failures", lambda resources: resources["sampler"]
             .__setitem__("failures", ["ps_unavailable"]), "sampler_failures"),
            ("process-exit", lambda resources: resources["processes"][0]
             .__setitem__("exit_code", 1), "process_exit_nonzero"),
            ("missing-exit", lambda resources: resources["processes"][0]
             .pop("exit_code"), "process_exit_nonzero"),
            ("negative-disk", lambda resources: resources["disk"]["store"]
             .update(start_bytes=0, end_bytes=-1, growth_bytes=-1),
             "disk_growth_negative:store"),
            ("missing-cache", lambda resources: resources["disk"].pop("cache"),
             "disk_missing:cache"),
            ("inconsistent-growth", lambda resources: resources["disk"]["store"]
             .update(start_bytes=0, end_bytes=1024, growth_bytes=10),
             "disk_growth_inconsistent:store"),
        )
        for name, mutate, reason in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                evidence = self._passing(directory)
                run = evidence["runs"][("S1", 2, 0, "A")]
                self._mutate_resources(run, mutate)
                report = gate._evaluate_arithmetic_for_tests(
                    evidence["root"], tier="G1", profile_name="quick",
                    policy=evidence["policy"])
                self.assertEqual(report["status"], "NOT_RUN")
                self.assertEqual(report["sections"]["resources"]["status"], "NOT_RUN")
                self.assertIn(reason, report["sections"]["resources"]["reasons"])

    def test_production_evaluate_rejects_injected_policy_and_acceptance(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = fixtures.policy(pairs=1)
            evidence = self._passing(directory, policy_document=policy)
            rejected = gate.evaluate(evidence["root"], tier="G1", profile_name="quick",
                                     policy=policy)
            self.assertEqual(rejected["status"], "NOT_RUN")
            self.assertIn("policy_injection_rejected", " ".join(rejected["reasons"]))
            arithmetic = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G1", profile_name="quick", policy=policy)
            self.assertEqual(arithmetic["status"], "PHASE_PASS")
            forged = copy.deepcopy(perf_contract.load_acceptance())
            forged["sampling"]["quick"]["minimum_operations_per_lane"] = 8
            rejected = gate.evaluate(evidence["root"], tier="G1", profile_name="quick",
                                     acceptance=forged)
            self.assertEqual(rejected["status"], "NOT_RUN")
            self.assertIn("acceptance_injection_rejected", " ".join(rejected["reasons"]))

    def test_production_evaluate_accepts_frozen_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._real(directory)
            report = gate.evaluate(evidence["root"], tier="G1", profile_name="quick")
            self.assertEqual(report["status"], "PHASE_PASS")

    def test_cli_exposes_no_policy_or_acceptance_injection(self):
        completed = subprocess.run(
            [sys.executable, str(Path(__file__).with_name("gate.py")), "--help"],
            capture_output=True, text=True, timeout=30)
        self.assertEqual(completed.returncode, 0)
        self.assertNotIn("--policy", completed.stdout)
        self.assertNotIn("--acceptance", completed.stdout)
        self.assertIn("--allow-foreign-environment", completed.stdout)

    def test_trusted_artifacts_must_live_outside_evidence(self):
        policy = fixtures.policy(name="formal", pairs=3, total_pass=True)
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._passing(directory, pairs=3, policy_document=policy)
            inside = Path(evidence["root"]) / "trusted-artifacts.json"
            inside.write_text(json.dumps({label: fixtures.artifact_sha(label)
                                          for label in ("A", "B")}))
            report = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G1", profile_name="formal",
                policy=policy, trusted_artifacts=inside)
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertIn("trusted_artifacts_inside_evidence", " ".join(report["reasons"]))
            outside = self._trusted(evidence["root"])
            outside.write_text(json.dumps({"A": fixtures.artifact_sha("A"),
                                           "B": "0" * 64}))
            report = gate._evaluate_arithmetic_for_tests(
                evidence["root"], tier="G1", profile_name="formal",
                policy=policy, trusted_artifacts=outside)
            self.assertEqual(report["status"], "NOT_RUN")
            self.assertIn("trusted_artifact_mismatch:B", " ".join(report["reasons"]))


if __name__ == "__main__":
    unittest.main()
