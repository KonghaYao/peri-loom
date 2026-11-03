"""Evidence bundles must be self-describing and tamper-evident."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pl_retail.evidence import EvidenceWriter, sha256_file


class EvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp()) / "bundle"

    def test_manifest_records_digest_bytes_and_record_count(self) -> None:
        writer = EvidenceWriter(self.root)
        writer.write_json("rounds/r01-U1/round.json", {"a": 1})
        writer.append("rounds/r01-U1/attempts.jsonl", {"b": 2})
        manifest = writer.finalize()
        entries = {entry["path"]: entry for entry in manifest["files"]}
        self.assertIn("rounds/r01-U1/round.json", entries)
        self.assertEqual(entries["rounds/r01-U1/attempts.jsonl"]["records"], 1)
        for relative, entry in entries.items():
            self.assertEqual(entry["sha256"], sha256_file(self.root / relative))

    def test_finalize_twice_is_stable(self) -> None:
        writer = EvidenceWriter(self.root)
        writer.write_json("a.json", {"a": 1})
        first = writer.finalize()
        second = writer.finalize()
        self.assertEqual(first["files"], second["files"])

    def test_append_after_finalize_is_rehashed(self) -> None:
        writer = EvidenceWriter(self.root)
        writer.append("a.jsonl", {"a": 1})
        writer.finalize()
        writer.append("a.jsonl", {"a": 2})
        manifest = writer.finalize()
        entry = next(item for item in manifest["files"] if item["path"] == "a.jsonl")
        self.assertEqual(entry["records"], 2)
        self.assertEqual(entry["sha256"], sha256_file(self.root / "a.jsonl"))

    def test_manifest_lists_itself_out(self) -> None:
        writer = EvidenceWriter(self.root)
        writer.write_json("a.json", {"a": 1})
        manifest = writer.finalize()
        paths = [entry["path"] for entry in manifest["files"]]
        self.assertNotIn("evidence-manifest.json", paths)

    def test_tampering_is_visible(self) -> None:
        writer = EvidenceWriter(self.root)
        writer.write_json("a.json", {"a": 1})
        manifest = writer.finalize()
        recorded = {entry["path"]: entry["sha256"] for entry in manifest["files"]}
        (self.root / "a.json").write_text('{"a": 2}', encoding="utf-8")
        self.assertNotEqual(recorded["a.json"], sha256_file(self.root / "a.json"))

    def test_json_is_written_deterministically(self) -> None:
        writer = EvidenceWriter(self.root)
        writer.write_json("a.json", {"b": 1, "a": [2, 3]})
        writer.finalize()
        text = (self.root / "a.json").read_text(encoding="utf-8")
        self.assertEqual(json.loads(text), {"b": 1, "a": [2, 3]})
        self.assertTrue(text.endswith("\n"))


if __name__ == "__main__":
    unittest.main()
