import json
import unittest

import issue8_workload as workload
import perf_contract


class WorkloadTests(unittest.TestCase):
    def test_s0_preserves_original_literal_schedule(self):
        self.assertEqual(workload.operation("S0", "read", 0),
                         {"kind": "read", "operation": "query", "row": 0,
                          "sql": "SELECT payload FROM bench WHERE id=0", "payload": None})
        write = workload.operation("S0", "write", 1)
        self.assertEqual(write["kind"], "write")
        self.assertEqual(write["operation"], "exec")
        self.assertEqual(write["row"], 1)
        self.assertEqual(write["sql"], "UPDATE bench SET payload='" + "y" * 256 + "' WHERE id=1")
        mixed = [workload.operation("S0", "mixed", index)["kind"] for index in range(4)]
        self.assertEqual(mixed, ["read", "write", "read", "write"])

    def test_s1_single_row_autocommit_schedule(self):
        for index in (0, 63, 64, 65, 127, 128):
            operation = workload.operation("S1", None, index)
            self.assertEqual(operation["kind"], "write")
            self.assertEqual(operation["row"], index % 64)
            expected = ("y" if (index // 64) % 2 == 0 else "z") * 256
            self.assertEqual(operation["sql"],
                             "UPDATE bench SET payload='" + expected + "' WHERE id=" + str(index % 64))

    def test_s2_odd_writes_even_reads(self):
        kinds = [workload.operation("S2", None, index)["kind"] for index in range(6)]
        self.assertEqual(kinds, ["read", "write", "read", "write", "read", "write"])
        self.assertEqual([workload.operation_kind("S2", index) for index in range(4)],
                         ["read", "write", "read", "write"])
        self.assertEqual(workload.operation_kind("S1", 7), "write")
        read = workload.operation("S2", None, 2)
        self.assertEqual(read["sql"], "SELECT payload FROM bench WHERE id=2")
        with self.assertRaises(workload.WorkloadError):
            workload.operation("S3", None, 0)

    def test_case_plans_enforce_operation_and_bucket_minima(self):
        quick = perf_contract.profile("quick")
        s1 = workload.case_plan(quick, "S1", 2)
        self.assertEqual(s1["required_operations_per_lane"], 512)
        self.assertEqual(s1["warmup_operations"], 512)
        self.assertEqual(s1["minimum_samples_per_bucket"], 512)
        self.assertEqual(s1["buckets"], ("write", "all"))
        s2 = workload.case_plan(quick, "S2", 2)
        self.assertEqual(s2["required_operations_per_lane"], 1024)
        self.assertEqual(s2["buckets"], ("read", "write", "all"))
        smoke = perf_contract.profile("smoke")
        s0 = workload.case_plan(smoke, "S0", 2)
        self.assertEqual(s0["required_operations_per_lane"], 100)
        self.assertEqual(s0["warmup_operations"], 0)
        with self.assertRaises(workload.WorkloadError):
            workload.case_plan(quick, "S0", 2)
        with self.assertRaises(perf_contract.ContractError):
            workload.case_plan(quick, "S1", 3)

    def test_payload_model_and_read_verification(self):
        model = workload.PayloadModel()
        self.assertEqual(model.expected(0), "x" * 256)
        before = model.digest()
        model.write(0, "y" * 256)
        self.assertNotEqual(model.digest(), before)
        self.assertTrue(workload.verify_read({"ok": True, "result": [["y" * 256]]}, model, 0))
        self.assertFalse(workload.verify_read({"ok": True, "result": [["x" * 256]]}, model, 0))
        model.uncertain_write(1, "z" * 256)
        self.assertIn("z" * 256, model.allowed(1))
        model.write(1, "y" * 256)
        self.assertNotIn("z" * 256, model.allowed(1))

    def test_required_record_fields_match_evidence_contract(self):
        required = set(workload.REQUIRED_RECORD_FIELDS)
        for field in ("run", "pair", "arm", "case", "lane", "seq", "kind", "planned_at",
                      "issued_at", "completed_at", "duration_ns", "outcome"):
            self.assertIn(field, required)


if __name__ == "__main__":
    unittest.main()
