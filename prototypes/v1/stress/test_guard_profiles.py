import unittest

from guard_profiles import parameters


class GuardProfileTests(unittest.TestCase):
    def test_exact_profiles_and_shared_limits(self):
        shared = {
            "rows": "1000", "seed": "20261007", "request-timeout": "10s",
            "max-process-rss-bytes": "536870912", "max-generated-bytes": "268435456",
            "max-artifact-bytes": "67108864",
        }
        expected = {
            "mixed": {
                "cases": "mixed_oltp", "concurrency": "1,4", "warmup": "2s",
                "smoke": "10s", "measure": "30s", "offered-rate": "50",
            },
            "architecture": {
                "cases": "session_cycles,many_db,noisy_tenant,saturation_admission,idle_suspend_resume,graceful_migrate",
                "concurrency": "1,2", "warmup": "2s", "smoke": "10s",
                "measure": "10s", "offered-rate": "50",
            },
            "common": {
                "cases": "point_read,index_range,pagination,mixed_oltp,hot_keys,bulk_tx,sort_aggregate_join,index_plan,rollback,constraint_error_reuse,large_values,sustained_wal,checkpoint_reopen",
                "concurrency": "1,2", "warmup": "1s", "smoke": "2s",
                "measure": "5s", "offered-rate": "25",
            },
            "checkpoint": {
                "cases": "checkpoint_reopen", "concurrency": "1,2", "warmup": "1s",
                "smoke": "2s", "measure": "5s", "offered-rate": "25",
            },
        }
        for name, profile in expected.items():
            with self.subTest(name=name):
                self.assertEqual(parameters(name), {**shared, **profile})

    def test_default_is_mixed(self):
        self.assertEqual(parameters(), parameters("mixed"))
        self.assertIsNot(parameters(), parameters("mixed"))

    def test_checkpoint_changes_only_common_cases(self):
        common = parameters("common")
        checkpoint = parameters("checkpoint")
        self.assertEqual(checkpoint, {**common, "cases": "checkpoint_reopen"})
        self.assertIsNot(checkpoint, parameters("checkpoint"))

    def test_case_names_and_concurrency_are_unique(self):
        for name in ("mixed", "architecture", "common", "checkpoint"):
            with self.subTest(name=name):
                profile = parameters(name)
                for field in ("cases", "concurrency"):
                    values = profile[field].split(",")
                    self.assertNotIn("", values)
                    self.assertEqual(len(values), len(set(values)))
        case_lists = [parameters(name)["cases"] for name in ("mixed", "architecture", "common", "checkpoint")]
        self.assertEqual(len(case_lists), len(set(case_lists)))

    def test_fresh_copy_isolates_all_profiles(self):
        originals = {name: parameters(name) for name in ("mixed", "architecture", "common", "checkpoint")}
        for name in originals:
            changed = parameters(name)
            changed["rows"] = "1"
            changed["cases"] = "unapproved_case"
            changed["max-artifact-bytes"] = "0"
            changed["extra"] = "not_a_command_parameter"
            del changed["seed"]
            for other, expected in originals.items():
                self.assertEqual(parameters(other), expected)

    def test_unknown_names_and_invalid_types_are_rejected(self):
        for name in ("", "Mixed", "all", "unknown", "mixed ", "Checkpoint", "checkpoint ",
                     "checkpoint_reopen", "checkpoint,mixed", None, 1, True, [], {}):
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    parameters(name)

    def test_values_are_nonempty_cli_strings(self):
        for name in ("mixed", "architecture", "common", "checkpoint"):
            for key, value in parameters(name).items():
                with self.subTest(name=name, key=key):
                    self.assertIsInstance(key, str)
                    self.assertFalse(key.startswith("-"))
                    self.assertIsInstance(value, str)
                    self.assertTrue(value)


if __name__ == "__main__":
    unittest.main()
