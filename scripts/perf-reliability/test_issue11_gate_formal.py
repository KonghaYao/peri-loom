"""完整 H1–H7 的 formal gate 契约测试；不是真实环境或性能证据。

环境元数据故意满足 formal 合同且不带 synthetic 标记，仅验证 gate 逻辑。
所有文件在临时目录，未连接平台，未运行 formal 基准。
"""
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest

from test_issue11_support import contract, evidence_io, write_campaign, write_run
from test_issue11_gate import _ledger
from issue11 import gate
from issue11.gate_pairing import paired_problems


def complete_contract_campaign(root):
    root = Path(root)
    records = []
    order = contract.load_acceptance()["sampling"]["formal"]["pair_order"]
    for scenario in ("H1", "H2", "H3", "H4", "H5", "H6", "H7"):
        templates = {}
        for pair in range(5):
            state = "fresh" if pair % 2 == 0 else "aged"
            for arm in order[pair]:
                run_id = "%s-p%d-%s" % (scenario, pair, arm)
                path = root / "runs" / run_id
                if arm not in templates:
                    write_run(path, timed=10000, formal=True, scenario=scenario,
                              arm=arm, pair_index=pair, state=state,
                              spacing_ns=360_000_000, close=scenario in {"H5", "H6"},
                              contract_fixture=True)
                    templates[arm] = path
                else:
                    path.mkdir(parents=True)
                    for source in templates[arm].rglob("*"):
                        relative = source.relative_to(templates[arm])
                        if source.is_dir():
                            (path / relative).mkdir()
                        elif source.name not in {"run.json", "SHA256SUMS.json", "MANIFEST.json"}:
                            os.link(source, path / relative)
                    metadata = json.loads((templates[arm] / "run.json").read_text())
                    start = 1_000_000_000 + pair * 10_000_000 + order[pair].index(arm) * 4_000_000
                    metadata.update(pair_index=pair, state=state,
                                    started_at_unix_ms=start, completed_at_unix_ms=start + 3_900_000)
                    evidence_io.write_json(path / "run.json", metadata)
                    evidence_io.write_manifests(path, run_id)
                metadata = json.loads((path / "run.json").read_text())
                record = _ledger(run_id, scenario=scenario, profile="formal", arm=arm,
                                 pair_index=pair, state=state, order_label=order[pair],
                                 duration_seconds=3600 if state == "aged" else 0,
                                 diagnostics=metadata["diagnostics"])
                record.update(synthetic_test_fixture=False,
                              matched_state=metadata["matched_state"])
                records.append(record)
    write_campaign(root, records, scenarios=["H%d" % i for i in range(1, 8)],
                   contract_fixture=True)
    return root


class FormalContractTests(unittest.TestCase):
    def test_complete_non_synthetic_contract_fixture_passes_and_summary_optional(self):
        with tempfile.TemporaryDirectory() as directory:
            root = complete_contract_campaign(Path(directory) / "contract-only")
            result = gate.evaluate(root)
            self.assertEqual(result["status"], gate.PASS, result["reasons"][:20])
            self.assertEqual(result["exit_code"], 0)
            self.assertEqual(len(result["runs"]), 70)
            # summary 不在 hash 集合；无需重封 manifest 即可删除或篡改。
            for index, run in enumerate((root / "runs").iterdir()):
                summary = run / "summary.json"
                if index % 2:
                    summary.unlink()
                else:
                    summary.write_text("not even JSON\n")
            for run in (root / "runs").iterdir():
                problems = []
                gate._verify_run_hashes(run, problems)
                self.assertEqual(problems, [])
            after = gate.evaluate(root)
            self.assertEqual(after["status"], result["status"])
            self.assertEqual(after["reasons"], result["reasons"])
            self.assertEqual([run["stats"] for run in after["runs"]],
                             [run["stats"] for run in result["runs"]])
            print("契约测试（非性能）: H1-H7 70 runs × 10000 timed = PASS；summary 删除/篡改不改变判定")

    def test_round_stability_uses_candidate_p50_not_p95(self):
        acceptance = contract.load_acceptance()
        runs = [{"scenario": "H4", "state": "fresh", "arm": "B",
                 "stats": {"p50_ms": value, "p95_ms": 10}}
                for value in (1, 4)]
        self.assertTrue(gate._stability_problems(runs, acceptance))
        runs[1]["stats"]["p50_ms"] = 1
        self.assertEqual(gate._stability_problems(runs, acceptance), [])
        print("反例 stability: p50 ratio=4.0 FAIL；ratio=1.0 通过")

    def test_pairwise_targets_state_and_baseline_fail_closed(self):
        acceptance = contract.load_acceptance()
        runs = [{"run_id": arm, "scenario": "H4", "state": "fresh", "pair_index": 0,
                 "arm": arm, "stats": {"p50_ms": 10, "p95_ms": 20, "p99_ms": 50}}
                for arm in ("A", "B")]
        views = [{"run_id": arm, "record": {"diagnostics": {}, "matched_state": {
                    "data_sha256": "a" * 64, "metadata_sha256": "b" * 64}},
                  "metadata": {"started_at_unix_ms": 1 + index * 10,
                               "completed_at_unix_ms": 10 + index * 10},
                  "environment": {}, "oracle": {"fixture_sha256": "c" * 64}}
                 for index, arm in enumerate(("A", "B"))]
        problems = paired_problems(runs, views, acceptance)
        self.assertIn(("FAIL", "paired_p50_reduction: H4"), problems)
        runs[1]["stats"]["p50_ms"] = 8
        self.assertNotIn(("FAIL", "paired_p50_reduction: H4"), paired_problems(runs, views, acceptance))
        runs[1]["stats"]["p99_ms"] = 51
        self.assertIn(("FAIL", "paired_p99_regression: H4"), paired_problems(runs, views, acceptance))
        self.assertTrue(any("baseline_missing" in reason for _, reason in
                            paired_problems(runs[1:], views, acceptance)))
        views[1]["record"]["matched_state"]["metadata_sha256"] = "d" * 64
        self.assertIn(("FAIL", "paired_data_metadata_state_mismatch"), paired_problems(runs, views, acceptance))
        views[1]["metadata"]["started_at_unix_ms"] = 5
        self.assertIn(("FAIL", "paired_execution_order_or_overlap: H4"), paired_problems(runs, views, acceptance))
        views[1]["metadata"] = {}
        self.assertIn(("INCOMPLETE", "paired_execution_window_missing: H4"), paired_problems(runs, views, acceptance))

    def test_read_regression_and_strict_absolute_p99(self):
        acceptance = contract.load_acceptance()
        problems = []
        gate._target_problems("H6", {"p50_ms": 35, "p95_ms": 99, "p99_ms": 100},
                              acceptance["targets"], problems)
        self.assertIn(("FAIL", "target_p99_ms_exclusive: H6"), problems)
        for scenario in ("H1", "H2", "H5"):
            runs = [{"run_id": arm, "scenario": scenario, "state": "fresh", "pair_index": 0,
                     "arm": arm, "stats": {"p50_ms": 10, "p95_ms": 10, "p99_ms": 10}}
                    for arm in ("A", "B")]
            views = [{"run_id": arm, "record": {}, "environment": {}, "oracle": {}}
                     for arm in ("A", "B")]
            runs[1]["stats"].update(p50_ms=10.6, p95_ms=10.6)
            problems = paired_problems(runs, views, acceptance)
            self.assertIn(("FAIL", "paired_p50_regression: " + scenario), problems)
            self.assertIn(("FAIL", "paired_p95_regression: " + scenario), problems)
