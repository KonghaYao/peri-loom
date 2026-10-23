import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest

import perf_contract


class ContractTests(unittest.TestCase):
    def test_frozen_files_match_original_manifest(self):
        identity = perf_contract.verify_contract()
        self.assertTrue(identity["verified"])
        manifest = json.loads((perf_contract.CONTRACT_ROOT / "SHA256SUMS.json").read_text())
        for name, digest in identity["files"].items():
            self.assertEqual(manifest[name], digest, name)
        self.assertEqual(identity["sha256sums_sha256"],
                         "0e4eb35e279e452d7b984dcf13b82fc580ea7a7803ea3385aa7533579f524692")
        derived = perf_contract.load_derived()
        self.assertEqual(set(derived["required_files"]), set(identity["files"]))
        self.assertEqual(derived["request"]["max_request_bytes"], perf_contract.MAX_REQUEST_BYTES)
        acceptance = perf_contract.load_acceptance()
        workloads = perf_contract.load_workloads()
        self.assertEqual(acceptance["baseline_commit"], workloads["baseline_commit"])
        self.assertEqual(workloads["turso_commit"], "24a3a4609683b73935888f8bc30b47727d100aa8")

    def test_official_denominator_is_anchored_to_frozen_commit(self):
        official = perf_contract.official_denominator()
        self.assertEqual(official["version"], "v1.1.11")
        self.assertEqual(official["commit"],
                         perf_contract.load_acceptance()["baseline_commit"])
        self.assertEqual(official["commit"],
                         "f3c75622924d84189fa74f3069c209761ceeec31")
        self.assertEqual(perf_contract.FROZEN_DENOMINATOR[official["commit"]],
                         official["version"])

    def test_denominator_version_cannot_come_from_mutable_policy(self):
        acceptance = perf_contract.load_acceptance()
        official = perf_contract.official_denominator(
            acceptance=acceptance,
            derived={"official_denominator": {"version": "v1.1.10", "commit": "evil"}})
        self.assertEqual(official["version"], "v1.1.11")
        self.assertEqual(official["commit"], acceptance["baseline_commit"])

    def test_unknown_denominator_commit_is_rejected(self):
        acceptance = dict(perf_contract.load_acceptance(),
                          baseline_commit="0" * 40)
        with self.assertRaises(perf_contract.ContractError) as failure:
            perf_contract.official_denominator(acceptance=acceptance)
        self.assertIn("denominator_commit_unfrozen", str(failure.exception))

    def test_missing_or_tampered_copied_contract_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "issue8"
            shutil.copytree(perf_contract.CONTRACT_ROOT, root)
            (root / "acceptance.json").unlink()
            with self.assertRaises(perf_contract.ContractError):
                perf_contract.verify_contract(root=root)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "issue8"
            shutil.copytree(perf_contract.CONTRACT_ROOT, root)
            (root / "sql/10-smoke-read.sql").write_text("-- changed\n")
            with self.assertRaises(perf_contract.ContractError):
                perf_contract.verify_contract(root=root)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "issue8"
            shutil.copytree(perf_contract.CONTRACT_ROOT, root)
            (root / "SHA256SUMS.json").write_text("{}\n")
            with self.assertRaises(perf_contract.ContractError):
                perf_contract.verify_contract(root=root)

    def test_reference_materials_are_read_only_kit_copies(self):
        identity = perf_contract.verify_reference()
        self.assertTrue(identity["verified"])
        kit = json.loads((perf_contract.CONTRACT_ROOT / "SHA256SUMS.json").read_text())
        self.assertEqual(set(identity["files"]),
                         {"generate_fixture.py", "validate_sql.py", "sql-validation.json"})
        for name, digest in identity["files"].items():
            self.assertEqual(kit[name], digest, name)

    def test_reference_tamper_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "issue8"
            shutil.copytree(perf_contract.CONTRACT_ROOT, root)
            path = root / "reference" / "validate_sql.py"
            path.write_bytes(path.read_bytes() + b"\n")
            with self.assertRaises(perf_contract.ContractError) as failure:
                perf_contract.verify_reference(root=root)
            self.assertIn("reference_mismatch", str(failure.exception))

    def test_derived_required_files_cannot_prune_extend_or_duplicate(self):
        frozen = perf_contract.frozen_required_files()
        self.assertIn("acceptance.json", frozen)
        self.assertNotIn("generate_fixture.py", frozen)
        self.assertFalse(set(perf_contract.REFERENCE_FILES) & set(frozen))
        base = perf_contract.load_derived()
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            root = directory / "issue8"
            shutil.copytree(perf_contract.CONTRACT_ROOT, root)
            declarations = {
                "pruned": base["required_files"][1:],
                "extended": base["required_files"] + ["sql/99-extra.sql"],
                "duplicated": base["required_files"] + [base["required_files"][0]],
            }
            for name, declared in declarations.items():
                with self.subTest(name=name):
                    derived = dict(base, required_files=declared)
                    path = directory / (name + ".json")
                    path.write_text(json.dumps(derived))
                    with self.assertRaises(perf_contract.ContractError) as failure:
                        perf_contract.verify_contract(root=root, derived_path=path)
                    self.assertIn("derived_required_files", str(failure.exception))
            reordered = dict(base, required_files=list(reversed(base["required_files"])))
            reordered_path = directory / "reordered.json"
            reordered_path.write_text(json.dumps(reordered))
            identity = perf_contract.verify_contract(root=root, derived_path=reordered_path)
            # Reordering is not an escape hatch: the frozen set is still verified.
            self.assertEqual(set(identity["files"]), set(frozen))

    def test_manifest_swap_cannot_shrink_the_required_set(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            root = directory / "issue8"
            shutil.copytree(perf_contract.CONTRACT_ROOT, root)
            manifest_path = root / "SHA256SUMS.json"
            manifest = json.loads(manifest_path.read_text())
            manifest.pop("acceptance.json")
            manifest_path.write_text(json.dumps(manifest))
            derived = dict(perf_contract.load_derived(),
                           sha256sums_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                           required_files=[name for name
                                           in perf_contract.load_derived()["required_files"]
                                           if name != "acceptance.json"])
            derived_path = directory / "derived.json"
            derived_path.write_text(json.dumps(derived))
            with self.assertRaises(perf_contract.ContractError) as failure:
                perf_contract.verify_contract(root=root, derived_path=derived_path)
            self.assertIn("sha256sums_manifest_changed", str(failure.exception))

    def test_unlisted_contract_file_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "issue8"
            shutil.copytree(perf_contract.CONTRACT_ROOT, root)
            (root / "extra.txt").write_text("not in the frozen set\n")
            with self.assertRaises(perf_contract.ContractError) as failure:
                perf_contract.verify_contract(root=root)
            self.assertIn("unexpected=extra.txt", str(failure.exception))

    def test_profiles_keep_contract_minima(self):
        quick = perf_contract.profile("quick")
        self.assertEqual(quick["min_samples_per_lane"], 512)
        self.assertEqual(quick["min_seconds"], 30)
        self.assertEqual(quick["warmup_operations"], 512)
        self.assertEqual(quick["pairs"], 1)
        self.assertEqual(quick["order"], ["AB"])
        formal = perf_contract.profile("formal")
        self.assertEqual(formal["min_samples_per_lane"], 10000)
        self.assertEqual(formal["min_seconds"], 180)
        self.assertEqual(formal["max_seconds_per_arm"], 900)
        self.assertEqual(formal["pairs"], 3)
        self.assertEqual(formal["order"], ["AB", "BA", "AB"])
        self.assertTrue(formal["total_pass"])
        smoke = perf_contract.profile("smoke")
        self.assertEqual(smoke["min_samples_per_lane"], 100)
        self.assertEqual(smoke["warmup_operations"], 0)
        steady = perf_contract.profile("steady")
        self.assertEqual(steady["min_seconds"], 1800)
        self.assertEqual(steady["maintenance_cycles_min"], 3)
        self.assertIsNone(steady["min_samples_per_lane"])

    def test_profile_overrides_cannot_lower_or_disable_bounds(self):
        for name, overrides in [
                ("quick", {"min_samples_per_lane": 1}),
                ("quick", {"min_seconds": 1}),
                ("quick", {"max_seconds_per_arm": 1}),
                ("quick", {"pairs": 4}),
                ("formal", {"min_samples_per_lane": 100}),
                ("formal", {"pairs": 1}),
                ("formal", {"max_seconds_per_arm": 100}),
                ("formal", {"min_samples_per_lane": 2_000_000}),
                ("formal", {"min_seconds": True})]:
            with self.subTest(name=name, overrides=overrides), self.assertRaises(perf_contract.ContractError):
                perf_contract.profile(name, overrides)
        quick = perf_contract.profile("quick", {"min_samples_per_lane": 600, "min_seconds": 31,
                                               "max_seconds_per_arm": 600, "pairs": 2})
        self.assertEqual(quick["min_samples_per_lane"], 600)
        self.assertEqual(quick["pairs"], 2)
        self.assertEqual(quick["order"], ["AB", "BA"])
        formal = perf_contract.profile("formal", {"max_seconds_per_arm": 1800})
        self.assertEqual(formal["max_seconds_per_arm"], 1800)
        with self.assertRaises(perf_contract.ContractError):
            perf_contract.profile("formal", {"max_seconds_per_arm": 7200})

    def test_case_parameters_and_large_payload_chunking(self):
        plan = perf_contract.validate_case_parameters("S1", concurrency=2)
        self.assertEqual(plan["rows"], 64)
        self.assertEqual(plan["payload_bytes"], 256)
        self.assertEqual(len(plan["setup_insert_chunks"]), 1)
        with self.assertRaises(perf_contract.ContractError):
            perf_contract.validate_case_parameters("S1", concurrency=3)
        with self.assertRaises(perf_contract.ContractError):
            perf_contract.validate_case_parameters("S1", payload_bytes=65536)
        with self.assertRaises(perf_contract.RequestTooLarge):
            perf_contract.validate_case_parameters("S1", payload_bytes=2 * perf_contract.MAX_REQUEST_BYTES)
        for payload in (51200, 65536):
            chunks = perf_contract.chunk_insert_statements(64, payload)
            self.assertEqual(sum(chunk["row_count"] for chunk in chunks), 64)
            self.assertTrue(all(chunk["request_bytes"] <= perf_contract.MAX_REQUEST_BYTES
                                for chunk in chunks))
            self.assertGreater(len(chunks), 1)
        with self.assertRaises(perf_contract.RequestTooLarge):
            perf_contract.chunk_insert_statements(1, 2 * perf_contract.MAX_REQUEST_BYTES)
        with self.assertRaises(perf_contract.RequestTooLarge):
            perf_contract.chunk_insert_statements(4, perf_contract.MAX_REQUEST_BYTES)
        with self.assertRaises(perf_contract.RequestTooLarge):
            perf_contract.assert_request_size("x" * perf_contract.MAX_REQUEST_BYTES)

    def test_store_roots_reject_temp_tmpfs_overlap_and_unverified_formal(self):
        with tempfile.TemporaryDirectory(dir=perf_contract.ROOT) as directory:
            base = Path(directory)
            store, cache, evidence = base / "store", base / "cache", base / "evidence"
            mount = "device " + str(perf_contract.ROOT) + " ext4 rw 0 0\n"
            result = perf_contract.validate_roots(store, cache, evidence, "quick",
                                                  mount_table=mount, create=True)
            self.assertTrue(result["persistent_mounts_verified"])
            self.assertEqual(result["filesystems"]["store"], "ext4")
            with self.assertRaises(perf_contract.UnsafePath):
                perf_contract.validate_roots(store, cache, evidence, "quick",
                                             mount_table="device " + str(perf_contract.ROOT)
                                             + " tmpfs rw 0 0\n")
            with self.assertRaises(perf_contract.UnsafePath):
                perf_contract.validate_roots(store, cache, evidence, "formal",
                                             mount_table="")
            with self.assertRaises(perf_contract.UnsafePath):
                perf_contract.validate_roots(store, store / "nested", evidence, "quick",
                                             mount_table=mount)
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            with self.assertRaises(perf_contract.UnsafePath):
                perf_contract.validate_roots(base / "store", base / "cache", base / "evidence",
                                             "quick", mount_table="")
        with tempfile.TemporaryDirectory(dir=perf_contract.ROOT) as directory:
            base = Path(directory)
            with self.assertRaises(perf_contract.UnsafePath):
                perf_contract.validate_roots(perf_contract.ROOT / "scripts" / "x",
                                             base / "cache", base / "evidence", "quick")

    def test_derived_policy_is_separate_from_originals(self):
        derived = perf_contract.load_derived()
        self.assertEqual(derived["contract_root"], "issue8")
        self.assertFalse((perf_contract.CONTRACT_ROOT / "derived-contract.json").exists())
        self.assertTrue(perf_contract.DERIVED_PATH.is_file())
        self.assertEqual(derived["request"]["truncation_allowed"], False)


if __name__ == "__main__":
    unittest.main()
