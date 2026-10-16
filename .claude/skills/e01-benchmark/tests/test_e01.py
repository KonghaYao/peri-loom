import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
SKILL = SCRIPTS.parent
sys.path.insert(0, str(SCRIPTS))

import jsonio
from contract import FAMILIES, digest, plan, template, validate
from evidence import quantiles, summarize, template as raw_template
from model import (check_complete_pages, check_result, generate, make_rows, oracle, row_view,
                   traversal_operations)
from sql import APPROVED_DDL, import_statements, literal, render, schema_sha256

BINARY = SKILL / "native-harness/target/debug/e01-native-harness"
APPROVED_TABLE = ("CREATE TABLE e01_records (id INTEGER PRIMARY KEY, group_id INTEGER NOT NULL, "
                  "created_at INTEGER NOT NULL, payload TEXT NOT NULL)")
APPROVED_INDEX = "CREATE INDEX e01_records_group_time_id ON e01_records(group_id, created_at, id)"


def reference(kind="synthetic"):
    return {"kind": kind, "captured_at": "test-only", "window": "test-window",
            "source": "unit-test-not-business-evidence", "method": "synthetic",
            "snapshot_sha256": "0" * 64, "rows": 10, "logical_bytes": 1000,
            "working_set_bytes": {q: 100 for q in FAMILIES}, "concurrency": 3, "rate": 5}


def records(config, bundle=None):
    bundle = bundle or generate(config)
    raw = raw_template(config)
    for index, query in enumerate(bundle["queries"]):
        raw["records"].append({"op_id": query["op_id"], "outcome": "success",
                               "planned_ns": index * 10000000,
                               "dispatched_ns": index * 10000000 + 2000000,
                               "completed_ns": index * 10000000 + 5000000,
                               "result_sha256": query["expected_sha256"], "oracle": "match",
                               "error_class": None, "error_message": None})
    return raw


class ContractTests(unittest.TestCase):
    def test_missing_reference_and_no_default_approval(self):
        result = plan(template())
        self.assertEqual(result["reference_status"], "REFERENCE_MISSING")
        self.assertEqual(result["stages"], [])
        self.assertEqual(result["performance"], "NOT MEASURED")
        self.assertTrue(result["execution"].startswith("BLOCKED"))

    def test_separate_scale_axes_and_synthetic_label(self):
        config = template()
        config["reference"] = reference()
        result = plan(config)
        self.assertEqual(result["reference_status"], "SYNTHETIC_REFERENCE")
        for stage, multiplier in zip(result["stages"], (2, 4, 8)):
            self.assertEqual(stage["minimum_rows"], 10 * multiplier)
            self.assertEqual(stage["minimum_logical_bytes"], 1000 * multiplier)
            self.assertEqual(stage["minimum_working_set_bytes"], {q: 100 * multiplier for q in FAMILIES})
            self.assertEqual(stage["data_axis_concurrency"], 3)
            self.assertEqual(stage["separate_concurrency_axis"], 3 * multiplier)

    def test_reject_invalid_contracts(self):
        mutations = [("seed", True), ("version", "future"), ("unexpected", 1),
                     ("sync_mode", "eventual")]
        for key, value in mutations:
            config = template()
            config[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate(config)
        config = template()
        config["reference"] = reference()
        config["reference"]["working_set_bytes"]["point"] = 0
        with self.assertRaises(ValueError):
            validate(config)
        for value in (float("nan"), float("inf"), -1, True):
            config = template()
            config["budget"]["cpu_cores"] = value
            with self.assertRaises(ValueError):
                validate(config)


class SqlTests(unittest.TestCase):
    def test_frozen_ddl_matches_the_approved_structure(self):
        self.assertEqual(APPROVED_DDL, (APPROVED_TABLE, APPROVED_INDEX))
        self.assertEqual(schema_sha256(), digest_text("\n".join(APPROVED_DDL)))
        self.assertEqual(len(schema_sha256()), 64)

    def test_renderers_are_independent_and_ordered(self):
        point, point_params = render({"family": "point", "args": {"id": 7}}, "parameter")
        self.assertTrue(point.startswith("SELECT id, group_id, created_at, payload FROM e01_records"))
        self.assertTrue(point.endswith("id = ?"))
        self.assertEqual(point_params, [7])
        listed, listed_params = render({"family": "index_list",
                                        "args": {"group_id": 1, "since": 4, "limit": 3}}, "parameter")
        self.assertIn("group_id = ? AND created_at >= ?", listed)
        self.assertTrue(listed.endswith(" ORDER BY created_at, id LIMIT ?"))
        self.assertEqual(listed_params, [1, 4, 3])
        page, page_params = render({"family": "offset",
                                    "args": {"group_id": 1, "limit": 3, "offset": 9}}, "parameter")
        self.assertTrue(page.endswith(" ORDER BY created_at, id LIMIT ? OFFSET ?"))
        self.assertEqual(page_params, [1, 3, 9])
        keyset, keyset_params = render({"family": "keyset",
                                       "args": {"group_id": 1, "limit": 3, "after": [4, 2]}}, "parameter")
        self.assertIn("created_at > ? OR (created_at = ? AND id > ?)", keyset)
        self.assertEqual(keyset_params, [1, 4, 4, 2, 3])
        for operation in ({"family": "offset", "args": {"group_id": 1, "limit": 3.5, "offset": 0}},
                          {"family": "keyset", "args": {"group_id": 1, "limit": 3, "after": [4]}},
                          {"family": "unknown", "args": {}}):
            with self.assertRaises(ValueError):
                render(operation)
        for binding in ("literal", "parameter"):
            for operation in ({"family": "point", "args": {"id": 7}},
                              {"family": "index_list", "args": {"group_id": 1, "since": 4, "limit": 3}},
                              {"family": "offset", "args": {"group_id": 1, "limit": 3, "offset": 9}},
                              {"family": "keyset", "args": {"group_id": 1, "limit": 3, "after": [4, 2]}}):
                sql, params = render(operation, binding)
                self.assertNotIn("None", sql)
                self.assertEqual(len(params), sql.count("?"))
        literal_sql, literal_params = render({"family": "keyset",
                                             "args": {"group_id": 1, "limit": 3, "after": [4, 2]}})
        self.assertIn("(created_at > 4 OR (created_at = 4 AND id > 2))", literal_sql)
        self.assertIn("LIMIT 3", literal_sql)
        self.assertEqual((literal_sql, literal_params), (literal_sql, []))
        with self.assertRaises(ValueError):
            render({"family": "point", "args": {"id": 7}}, "named")
        with self.assertRaises(ValueError):
            render({"family": "point", "args": {"id": 7.5}})

    def test_import_batches_escape_and_boundaries(self):
        self.assertEqual(literal("ab'c"), "'ab''c'")
        for bad in ("a\x00b", "line\nbreak", 5):
            with self.assertRaises(ValueError):
                literal(bad)
        rows = make_rows(template())
        batches = list(import_statements(rows[:5], 2))
        self.assertEqual(len(batches), 3)
        self.assertTrue(all(statement.endswith(")") for statement in batches))
        self.assertEqual(batches[0].count("),("), 1)
        for size in (0, 1001):
            with self.assertRaises(ValueError):
                list(import_statements(rows, size))


class ModelTests(unittest.TestCase):
    def test_deterministic_seed_and_digests(self):
        config = template()
        first = generate(config)
        self.assertEqual(first, generate(copy.deepcopy(config)))
        config["seed"] += 1
        self.assertNotEqual(first["dataset_sha256"], generate(config)["dataset_sha256"])
        self.assertEqual(first["logical_rows"], len({r["id"] for r in first["rows"]}))
        self.assertEqual(first["dataset_sha256"], digest(first["rows"]))
        self.assertEqual(first["queries_sha256"], digest(first["queries"]))
        self.assertEqual({q["family"] for q in first["queries"]}, set(FAMILIES))
        for size in first["returned_working_set_bytes"].values():
            self.assertLessEqual(size, first["logical_bytes"])

    def test_traversal_operations_cover_every_group_row_once(self):
        config = template()
        rows = make_rows(config)
        plans = traversal_operations(rows, config["queries"]["page_size"])
        self.assertTrue(plans)
        for traversal in {plan_entry["traversal"] for plan_entry in plans}:
            family, group = traversal.split(":")
            entries = [entry for entry in plans if entry["traversal"] == traversal]
            pages = [oracle(rows, entry) for entry in entries]
            self.assertEqual(pages[-1], [])
            self.assertTrue(all(page for page in pages[:-1]))
            check_complete_pages(rows, int(group), pages)
            identities = [row["id"] for page in pages for row in page]
            self.assertEqual(len(identities), len(set(identities)))
            self.assertEqual({entry["family"] for entry in entries}, {family})
            if family == "keyset":
                self.assertEqual(entries[0]["args"]["after"], [-1, -1])
                self.assertNotEqual(entries[1]["args"]["after"], [-1, -1])

    def test_hand_written_oracle_with_ties_and_empty_boundaries(self):
        rows = [{"id": i, "group_id": g, "created_at": t, "payload": str(i)}
                for i, g, t in ((1, 0, 2), (2, 1, 0), (3, 0, 1), (4, 0, 2))]
        cases = [("point", {"id": 3}, [3]), ("point", {"id": 99}, []),
                 ("index_list", {"group_id": 0, "since": 2, "limit": 9}, [1, 4]),
                 ("offset", {"group_id": 0, "offset": 1, "limit": 9}, [1, 4]),
                 ("keyset", {"group_id": 0, "after": [2, 1], "limit": 9}, [4]),
                 ("keyset", {"group_id": 0, "after": [2, 4], "limit": 9}, [])]
        for family, args, ids in cases:
            op = {"family": family, "args": args}
            self.assertEqual([r["id"] for r in oracle(rows, op)], ids)
        with self.assertRaisesRegex(ValueError, "oracle_mismatch"):
            check_result(rows, {"family": "point", "args": {"id": 1}}, [])

    def test_complete_keyset_and_offset_pagination(self):
        rows = make_rows(template())
        for group in (0, 1):
            offset_pages, keyset_pages = [], []
            cursor, offset = [-1, -1], 0
            while True:
                args = {"group_id": group, "limit": 3}
                page = oracle(rows, {"family": "offset", "args": {**args, "offset": offset}})
                key_page = oracle(rows, {"family": "keyset", "args": {**args, "after": cursor}})
                self.assertEqual(page, key_page)
                if not page:
                    break
                offset_pages.append(page)
                keyset_pages.append(key_page)
                offset += len(page)
                cursor = [page[-1]["created_at"], page[-1]["id"]]
            check_complete_pages(rows, group, offset_pages)
            check_complete_pages(rows, group, keyset_pages)
            with self.assertRaises(ValueError):
                check_complete_pages(rows, group, keyset_pages + [keyset_pages[0]])
            with self.assertRaises(ValueError):
                check_complete_pages(rows, group, keyset_pages[1:])

    def test_row_view_rejects_non_integer_and_non_text(self):
        self.assertEqual(row_view([1, 0, 5, "ab"]),
                         {"id": 1, "group_id": 0, "created_at": 5, "payload": "ab"})
        for bad in ([1, 0, 5], [1, 0, 5, "ab", "extra"], [True, 0, 5, "ab"], [1, 0, 5.5, "ab"],
                    [1, 0, 5, 7]):
            with self.assertRaises(ValueError):
                row_view(bad)

    def test_resource_caps_and_no_formal_generation(self):
        for field, value in (("rows", 2049), ("payload_bytes", [1048576])):
            config = template()
            config["distribution"][field] = value
            with self.assertRaises(ValueError):
                generate(config)
        config = template()
        config["mode"] = "formal"
        with self.assertRaisesRegex(ValueError, "not_implemented"):
            generate(config)
        self.assertEqual(plan(config)["performance"], "NOT MEASURED")


class EvidenceTests(unittest.TestCase):
    def test_empty_input_never_passes(self):
        config = template()
        result = summarize(config, raw_template(config))
        self.assertEqual(result["correctness"], "NOT MEASURED")
        self.assertEqual(result["missing_records"], len(generate(config)["queries"]))

    def test_digest_matches_are_not_performance_evidence(self):
        config = template()
        bundle = generate(config)
        result = summarize(config, records(config, bundle))
        self.assertEqual(result["correctness"], "ORACLE_VERIFIED")
        self.assertEqual(result["oracle_verified_ops"], len(bundle["queries"]))
        self.assertEqual(result["performance"], "NOT MEASURED")
        self.assertEqual(result["scale_coverage"], "SMOKE_ONLY")
        family = result["families"]["point"]
        self.assertEqual(family["all_dispatched_latency"]["p50_ms"], 5)
        self.assertEqual(family["dispatch_lag"]["p50_ms"], 2)
        self.assertIsNone(family["all_dispatched_latency"]["p99_ms"])

    def test_unknown_error_missing_and_mismatch_not_hidden(self):
        config = template()
        for outcome in ("unknown", "timeout", "rejected", "known_failure", "not_dispatched"):
            raw = records(config)
            raw["records"][0].update(outcome=outcome, result_sha256=None, oracle="not_checked",
                                     error_class="Error", error_message="synthetic failure")
            if outcome == "not_dispatched":
                raw["records"][0].update(dispatched_ns=None, completed_ns=None)
            result = summarize(config, raw)
            self.assertEqual(result["correctness"], "INCOMPLETE")
            self.assertEqual(result["families"]["point"]["outcomes"][outcome], 1)
            self.assertLess(result["oracle_verified_ops"], result["ops_total"])
            self.assertEqual(result["families"]["point"]["verified_success_latency"]["samples"], 15)
        raw = records(config)
        raw["records"].pop()
        self.assertEqual(summarize(config, raw)["missing_records"], 1)
        raw = records(config)
        raw["records"][0].update(oracle="mismatch", result_sha256="0" * 64)
        result = summarize(config, raw)
        self.assertEqual(result["correctness"], "FAIL_STOP")
        self.assertEqual(result["oracle_mismatches"], 1)
        self.assertEqual(result["digest_mismatches"], 1)

    def test_duplicate_identity_and_bad_clock_rejected(self):
        config = template()
        raw = records(config)
        raw["records"].append(raw["records"][0])
        with self.assertRaises(ValueError):
            summarize(config, raw)
        raw = records(config)
        raw["records"][0]["completed_ns"] = 0
        with self.assertRaises(ValueError):
            summarize(config, raw)
        raw = records(config)
        raw["records"][0]["oracle"] = "not_checked"
        with self.assertRaises(ValueError):
            summarize(config, raw)
        raw = raw_template(config)
        raw["dataset_sha256"] = "0" * 64
        with self.assertRaises(ValueError):
            summarize(config, raw)
        raw = records(config)
        raw["sync_mode"] = "off"
        with self.assertRaises(ValueError):
            summarize(config, raw)
        self.assertEqual(quantiles(list(range(1000)))["p99_ms"], 989)


class CLITests(unittest.TestCase):
    def test_documented_pipeline_and_exclusive_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config.json"

            def run(command, output, extra=()):
                return subprocess.run([sys.executable, "-B", str(SCRIPTS / "e01.py"), command,
                                       "--output", str(root / output), *extra],
                                      capture_output=True, text=True)

            self.assertEqual(run("template", "config.json").returncode, 0)
            frozen = config.read_bytes()
            self.assertEqual(run("template", "config.json").returncode, 2)
            self.assertEqual(config.read_bytes(), frozen)
            self.assertEqual(config.stat().st_mode & 0o777, 0o600)
            for command in ("plan", "generate", "raw-template"):
                self.assertEqual(run(command, command + ".json", ["--config", str(config)]).returncode, 0)
            environment = jsonio.load(root / "generate.json")
            self.assertEqual(environment["schema_status"], "APPROVED_ISOLATED_FIXTURE")
            result = run("report", "report.json", ["--config", str(config), "--raw", str(root / "raw-template.json")])
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads((root / "report.json").read_text())["correctness"], "NOT MEASURED")
            config.write_text('{"duplicate":1,"duplicate":2}')
            result = run("plan", "invalid.json", ["--config", str(config)])
            self.assertEqual(result.returncode, 2)
            self.assertNotIn("duplicate", result.stderr)
            self.assertFalse((root / "invalid.json").exists())


@unittest.skipUnless(BINARY.is_file(), "native harness 未构建")
class NativeHarnessTests(unittest.TestCase):
    def test_native_smoke_verifies_every_family_against_the_oracle(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config.json"
            self.assertEqual(subprocess.run(
                [sys.executable, "-B", str(SCRIPTS / "e01.py"), "template",
                 "--output", str(config)], capture_output=True).returncode, 0)
            completed = subprocess.run(
                [sys.executable, "-B", str(SCRIPTS / "run_native.py"), "--config", str(config),
                 "--out-dir", str(root / "run")], capture_output=True, text=True)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            report = jsonio.load(root / "run/report.json")
            scenario = jsonio.load(root / "run/scenario.json")
            self.assertEqual(report["correctness"], "ORACLE_VERIFIED")
            self.assertEqual(report["oracle_verified_ops"], report["ops_total"])
            self.assertEqual(report["harness"]["import"]["rows"], scenario["logical_rows"])
            self.assertEqual(report["harness"]["sync_mode"], "full")
            self.assertEqual({item["verdict"] for item in report["traversal"]}, {"complete"})
            self.assertEqual(report["harness"]["schema_sha256"], schema_sha256())
            for family in FAMILIES:
                outcomes = report["families"][family]["outcomes"]
                self.assertEqual(outcomes["success"] > 0, True)
                self.assertEqual(sum(outcomes.values()), outcomes["success"])
            self.assertEqual(report["performance"], "NOT MEASURED")
            parameterised = subprocess.run(
                [sys.executable, "-B", str(SCRIPTS / "run_native.py"), "--config", str(config),
                 "--out-dir", str(root / "run-parameter"), "--binding", "parameter"],
                capture_output=True, text=True)
            self.assertEqual(parameterised.returncode, 0, parameterised.stderr)
            bound = jsonio.load(root / "run-parameter/report.json")
            self.assertEqual(bound["correctness"], "ORACLE_VERIFIED")
            self.assertEqual(bound["harness"]["binding"], "parameter")
            with self.assertRaises(FileExistsError):
                jsonio.write_new(root / "run/report.json", {})
            # 第二次运行必须拒绝复用旧 fixture 目录，而不是静默重放。
            repeated = subprocess.run(
                [sys.executable, "-B", str(SCRIPTS / "run_native.py"), "--config", str(config),
                 "--out-dir", str(root / "run")], capture_output=True, text=True)
            self.assertEqual(repeated.returncode, 2)
            self.assertTrue((root / "run/query.ndjson").read_text().strip())


def digest_text(text):
    import hashlib
    return hashlib.sha256(text.encode()).hexdigest()


if __name__ == "__main__":
    unittest.main()
