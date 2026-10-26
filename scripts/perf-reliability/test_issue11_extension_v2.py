"""Recovery/semantic gate regressions; all inputs are synthetic unit fixtures."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from test_issue11_support import contract, write_run
from issue11.gate_pairing import reliability_and_extended
from issue11.extended_v2 import validate_extended
from test_issue11_v2_support import extended_fixture


def load_view(path):
    documents = {name: json.loads((path / (name + ".json")).read_text())
                 for name in ("oracle", "resources")}
    documents.update(run_id=path.name, metadata=json.loads((path / "run.json").read_text()))
    documents["record"] = documents["metadata"]
    for name in ("operations", "requests"):
        documents[name] = [json.loads(line) for line in (path / (name + ".jsonl")).read_text().splitlines()]
    return documents


class RecoveryExtensionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        path = write_run(Path(self.directory.name) / "h4", timed=3, warmup=2,
                         scenario="H4", contract_fixture=True)
        self.view = load_view(path)

    def tearDown(self):
        self.directory.cleanup()

    def problems(self, view=None):
        problems = []
        reliability_and_extended(view or self.view, contract.load_acceptance(), problems)
        return problems

    def test_exact_raw_ack_set_and_values_are_accepted(self):
        self.assertEqual(self.problems(), [])
        self.assertEqual(self.view["oracle"]["recovery"]["acked_writes"], 5)

    def test_zero_recovery_for_nonempty_writes_fails(self):
        self.view["oracle"]["recovery"].update(acked_writes=0, recovered_writes=0, rows=[])
        self.assertIn(("FAIL", "recovery_acknowledged_count_mismatch"), self.problems())
        self.assertIn(("FAIL", "recovery_final_values_mismatch"), self.problems())

    def test_same_count_wrong_row_or_value_fails(self):
        for field in ("id", "value", "payload_sha256"):
            with self.subTest(field=field):
                view = copy.deepcopy(self.view)
                view["oracle"]["recovery"]["rows"][0][field] = 999 if field != "payload_sha256" else "a" * 64
                self.assertIn(("FAIL", "recovery_final_values_mismatch"), self.problems(view))

    def test_identity_digest_and_request_binding_are_required(self):
        self.view["operations"][0]["op_id"] = "a-different-acknowledged-operation"
        self.assertIn(("FAIL", "recovery_acknowledged_identity_mismatch"), self.problems())
        self.view["operations"][0]["write_effect"]["request_id"] = self.view["operations"][1]["http_request_ids"][0]
        self.assertIn(("FAIL", "acknowledged_write_request_mismatch"), self.problems())

    def test_successful_write_missing_identity_is_incomplete(self):
        del self.view["operations"][0]["write_effect"]
        self.assertIn(("INCOMPLETE", "acknowledged_write_identity_missing"), self.problems())

    def test_old_count_only_proof_cannot_pass(self):
        self.view["oracle"]["recovery"] = {"acked_writes": 0, "recovered_writes": 0, "mismatches": 0}
        self.assertIn(("INCOMPLETE", "recovery_identity_proof_missing"), self.problems())

    def test_restart_proof_must_match_run_namespace_and_later_instance(self):
        for field in ("run_id", "database"):
            view = copy.deepcopy(self.view)
            view["oracle"]["recovery"][field] = "other"
            self.assertIn(("FAIL", "recovery_run_or_database_mismatch"), self.problems(view))
        restart = self.view["oracle"]["recovery"]["restart"]
        restart["after_instance"] = restart["before_instance"]
        self.assertIn(("FAIL", "restart_identity_or_window_invalid"), self.problems())

    def test_update_recovery_uses_final_delta_not_row_count(self):
        path = write_run(Path(self.directory.name) / "h3", timed=3, warmup=2,
                         scenario="H3", contract_fixture=True)
        view = load_view(path)
        self.assertEqual(self.problems(view), [])
        self.assertEqual(view["oracle"]["recovery"]["rows"][0]["value"], 2)
        view["operations"][2]["write_effect"]["value"] = 1
        self.assertIn(("FAIL", "acknowledged_write_value_mismatch"), self.problems(view))

    def test_readback_request_is_not_the_write_acknowledgement(self):
        view = copy.deepcopy(self.view)
        operation = view["operations"][0]
        operation["http_request_ids"].append(view["operations"][1]["http_request_ids"][0])
        operation["write_effect"]["request_id"] = operation["http_request_ids"][1]
        self.assertIn(("FAIL", "acknowledged_write_request_mismatch"), self.problems(view))


class ExtendedSemanticTests(unittest.TestCase):
    def test_valid_fixture_and_contradictory_results(self):
        evidence = extended_fixture()
        problems = []
        validate_extended(evidence, problems)
        self.assertEqual(problems, [])
        edits = [
            ("quotas", "violations", 7, "quota_violations"),
            ("quotas", "observed", 5, "quota_observed_exceeds_configured"),
            ("overload", "outcome_unknown", 5, "overload_outcome_unknown"),
            ("overload", "failure", 1, "overload_failure"),
            ("overload", "completed", 5, "overload_counts_do_not_close"),
            ("overload", "rejection_ratio", 0, "overload_rejection_ratio"),
            ("queueing", "wait_p50_ms", 2, "queue_quantile_order"),
            ("capacity", "successful_rps", 11, "successful_rps_exceeds_offered"),
            ("capacity", "concurrency", 0.5, "capacity_concurrency_not_integer"),
            ("quotas", "configured", {"max_concurrent": 5}, "quota_configured_differs_from_contract"),
            ("confidence_intervals", "p50_lower", 1, "confidence_bounds_reversed: p50"),
        ]
        for section, field, value, reason in edits:
            with self.subTest(reason=reason):
                invalid = copy.deepcopy(evidence)
                invalid[section][field] = value
                problems = []
                validate_extended(invalid, problems)
                self.assertIn(("FAIL", "extended_semantics: " + reason), problems)


if __name__ == "__main__":
    unittest.main()
