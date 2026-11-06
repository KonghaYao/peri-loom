"""Call-shape profiles are a closed table: names, modes and call counts."""
from __future__ import annotations

import unittest

from pl_retail.call_profiles import (
    DEFAULT_PROFILE_NAME,
    PROFILES,
    CallProfile,
    expected_http_calls,
    resolve_profile,
    success_tail_label,
)
from pl_retail.errors import ContractError

BUSINESS = ("product_stock", "order_detail", "customer_orders", "stock_report", "checkout")


def _profile(**overrides) -> CallProfile:
    document = {
        "name": "test-profile",
        "read_mode": "transactional",
        "stock_mode": "per_item",
        "expected_http_calls": {name: 3 for name in BUSINESS},
    }
    document.update(overrides)
    return CallProfile(**document)


class CallProfileTests(unittest.TestCase):
    def test_baseline_is_the_default_and_the_frozen_v1_shape(self) -> None:
        self.assertEqual(DEFAULT_PROFILE_NAME, "baseline-v1")
        self.assertEqual(resolve_profile(None).name, "baseline-v1")
        self.assertEqual(
            expected_http_calls(PROFILES["baseline-v1"]),
            {
                "product_stock": 3,
                "order_detail": 3,
                "customer_orders": 3,
                "stock_report": 3,
                "checkout": 13,
            },
        )

    def test_every_profile_has_the_frozen_call_table(self) -> None:
        table = {
            "baseline-v1": (3, 13),
            "app-read-v1": (1, 13),
            "app-stock-v1": (3, 9),
            "app-read-stock-v1": (1, 9),
        }
        self.assertEqual(set(PROFILES), set(table))
        for name, (read_calls, checkout_calls) in table.items():
            profile = PROFILES[name]
            self.assertEqual(resolve_profile(name), profile)
            counts = expected_http_calls(profile)
            self.assertEqual(set(counts), set(BUSINESS), name)
            self.assertEqual(counts["checkout"], checkout_calls, name)
            for read in BUSINESS[:-1]:
                self.assertEqual(counts[read], read_calls, name)

    def test_expected_http_calls_is_a_copy(self) -> None:
        profile = PROFILES["baseline-v1"]
        counts = expected_http_calls(profile)
        counts["checkout"] = 1
        self.assertEqual(profile.expected_http_calls["checkout"], 13)

    def test_unknown_profile_is_rejected(self) -> None:
        with self.assertRaises(ContractError):
            resolve_profile("no-such-profile")
        with self.assertRaises(ContractError):
            resolve_profile("")

    def test_unknown_read_mode_is_rejected(self) -> None:
        with self.assertRaises(ContractError):
            expected_http_calls(_profile(read_mode="eventual"))

    def test_unknown_stock_mode_is_rejected(self) -> None:
        with self.assertRaises(ContractError):
            expected_http_calls(_profile(stock_mode="batched"))

    def test_a_missing_business_is_rejected(self) -> None:
        counts = {name: 3 for name in BUSINESS}
        counts.pop("stock_report")
        with self.assertRaises(ContractError):
            expected_http_calls(_profile(expected_http_calls=counts))

    def test_an_unknown_business_is_rejected(self) -> None:
        counts = {name: 3 for name in BUSINESS}
        counts["refund"] = 1
        with self.assertRaises(ContractError):
            expected_http_calls(_profile(expected_http_calls=counts))

    def test_non_positive_and_non_integer_call_counts_are_rejected(self) -> None:
        for value in (0, -1, 2.5, True, "3", None):
            counts = {name: 3 for name in BUSINESS}
            counts["checkout"] = value
            with self.assertRaises(ContractError, msg=repr(value)):
                expected_http_calls(_profile(expected_http_calls=counts))

    def test_success_tail_label_follows_the_read_mode(self) -> None:
        transactional = PROFILES["baseline-v1"]
        autocommit = PROFILES["app-read-v1"]
        for read in BUSINESS[:-1]:
            self.assertEqual(success_tail_label(transactional, read), "commit")
            self.assertEqual(success_tail_label(autocommit, read), "query")
        # Checkout always commits, whatever the read mode is.
        self.assertEqual(success_tail_label(transactional, "checkout"), "commit")
        self.assertEqual(success_tail_label(autocommit, "checkout"), "commit")
        self.assertEqual(
            success_tail_label(PROFILES["app-read-stock-v1"], "product_stock"), "query"
        )

    def test_success_tail_label_rejects_an_unknown_business(self) -> None:
        with self.assertRaises(ContractError):
            success_tail_label(PROFILES["baseline-v1"], "refund")

    def test_a_profile_records_only_the_shape_not_the_counts(self) -> None:
        self.assertEqual(
            PROFILES["app-read-stock-v1"].as_dict(),
            {"name": "app-read-stock-v1", "read_mode": "autocommit", "stock_mode": "merged"},
        )


if __name__ == "__main__":
    unittest.main()
