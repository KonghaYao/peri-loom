import json
from pathlib import Path
import tempfile
import time
import unittest

import evidence
from perf_contract import digest


class EvidenceTests(unittest.TestCase):
    def test_streaming_writer_and_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "operations.jsonl"
            writer = evidence.RawRecordWriter(path, flush_every=3)
            for index in range(1000):
                writer.write({"seq": index, "duration_ns": index + 1})
            writer.close()
            self.assertEqual(writer.count, 1000)
            self.assertEqual(writer.sha256, digest(path))
            self.assertFalse(hasattr(writer, "records"))
            lines = path.read_text().splitlines()
            self.assertEqual(len(lines), 1000)
            self.assertEqual(json.loads(lines[0]), {"duration_ns": 1, "seq": 0})
            self.assertEqual(json.loads(lines[-1]), {"duration_ns": 1000, "seq": 999})

    def test_manifest_detects_missing_tampered_and_unlisted_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.json").write_text("{\"a\":1}\n")
            (root / "b.txt").write_text("b\n")
            evidence.write_manifest(root, required=("a.json", "b.txt"))
            self.assertEqual(evidence.verify_manifest(root)["required"], ["a.json", "b.txt"])
            (root / "b.txt").write_text("changed\n")
            with self.assertRaises(evidence.EvidenceError):
                evidence.verify_manifest(root)
            evidence.write_manifest(root, required=("a.json", "b.txt"), reseal=True)
            (root / "c.txt").write_text("unlisted\n")
            with self.assertRaises(evidence.EvidenceError):
                evidence.verify_manifest(root)
            (root / "c.txt").unlink()
            (root / "a.json").unlink()
            with self.assertRaises(evidence.EvidenceError):
                evidence.verify_manifest(root)

    def test_process_table_tree_and_ps_time(self):
        rows = evidence.parse_process_table(
            "  1   0  100 0:01.50\n  2   1  200 01:02:03\n  3   1  300 2-01:02:03\n")
        self.assertEqual(rows[0]["rss_bytes"], 100 * 1024)
        self.assertEqual(rows[0]["cpu_seconds"], 1.5)
        self.assertEqual(rows[1]["cpu_seconds"], 3723)
        self.assertEqual(rows[2]["cpu_seconds"], 2 * 86400 + 3723)
        members, missing = evidence.descendants(rows, [1])
        self.assertEqual([member["pid"] for member in members], [1, 2, 3])
        self.assertEqual(missing, [])
        _, missing = evidence.descendants(rows, [9])
        self.assertEqual(missing, [9])
        with self.assertRaises(evidence.EvidenceError):
            evidence.parse_ps_time("not-a-time")

    def test_resource_sampler_measures_tree_rss_cpu_and_disk(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "store").mkdir()
            (root / "store" / "file").write_bytes(b"x" * 128)
            rows = evidence.parse_process_table(
                "  1   0  100 0:00.10\n  2   1  200 0:00.20\n")
            sampler = evidence.ResourceSampler([1], {"store": root / "store"},
                                                reader=lambda: rows)
            for _ in range(5):
                sampler.sample_once()
            report = sampler.report()
            self.assertEqual(report["status"], "MEASURED")
            self.assertEqual(report["peak_rss_bytes"], 300 * 1024)
            self.assertTrue(report["last_window_rss_bytes"])
            self.assertAlmostEqual(report["cpu_seconds"], 0.3)
            self.assertEqual(report["samples"][-1]["disk_bytes"]["store"], 128)
            missing = evidence.ResourceSampler([99], {"store": root / "store"},
                                                reader=lambda: rows)
            missing.sample_once()
            self.assertEqual(missing.report()["status"], "UNMEASURED")

    def test_resource_report_requires_join_and_disk_growth(self):
        sampler = {"status": "MEASURED", "peak_rss_bytes": 1024,
                   "last_window_rss_bytes": [1000], "cpu_seconds": 1.0}
        processes = [{"pid": 1, "group_joined": True}]
        report = evidence.build_resources_report(
            sampler, processes, {"store": "s"}, {"cache": "c"},
            {"store": 0, "cache": 0}, {"store": 10, "cache": 20})
        self.assertEqual(report["status"], "MEASURED")
        self.assertTrue(report["process_group_join_proof"])
        self.assertEqual(report["disk"]["store"]["growth_bytes"], 10)
        report = evidence.build_resources_report(
            sampler, [{"pid": 1, "group_joined": False}], {"store": "s"}, {"cache": "c"},
            {"store": 0, "cache": 0}, {"store": 10, "cache": 20})
        self.assertEqual(report["status"], "UNMEASURED")
        self.assertFalse(report["process_group_join_proof"])

    def test_metrics_snapshot_is_copied_byte_for_byte_and_missing_is_recorded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = evidence.EvidenceRun(root / "run", {"schema": 1, "run_id": "r"})
            run.write_json("oracle.json", {"pass": True})
            run.write_json("resources.json", {"status": "MEASURED"})
            run.write_json("summary.json", {"percentiles": "ignored"})
            raw = b'{"opaque": [1, 2, 3]}\n'
            source = root / "raw-metrics.json"
            source.write_bytes(raw)
            record = run.copy_metrics_snapshot(source, 0)
            self.assertTrue(record["copied"])
            destination = run.directory / record["file"]
            self.assertEqual(destination.read_bytes(), raw)
            missing = run.copy_metrics_snapshot(root / "missing.json", 1)
            self.assertFalse(missing["copied"])
            run.finish()
            evidence.verify_manifest(run.directory)
            index = json.loads((run.directory / "metrics-snapshot-index.json").read_text())
            self.assertEqual(index["snapshots"][0]["sha256"], digest(source))

    def test_freeze_artifact_detects_tampering(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.write_bytes(b"binary")
            frozen = evidence.freeze_artifact(source, root / "artifacts")
            self.assertEqual(frozen["sha256"], digest(source))
            self.assertEqual(Path(frozen["path"]).read_bytes(), b"binary")
            again = evidence.freeze_artifact(source, root / "artifacts")
            self.assertEqual(again["path"], frozen["path"])
            Path(frozen["path"]).chmod(0o600)
            Path(frozen["path"]).write_bytes(b"tampered")
            with self.assertRaises(evidence.EvidenceError):
                evidence.freeze_artifact(source, root / "artifacts")


if __name__ == "__main__":
    unittest.main()
