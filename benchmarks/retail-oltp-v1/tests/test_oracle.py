"""The contract's independent oracle must fall out of the generation rules."""
from __future__ import annotations

import unittest

from pl_retail.contract import DDL_PATH, load_contract
from pl_retail.generate import load_dataset


class OracleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.contract = load_contract(None)
        cls.dataset = load_dataset(1.0)

    def test_generator_reproduces_every_contract_total(self) -> None:
        totals = self.dataset.totals()
        for key, expected in self.contract.document["oracle"].items():
            self.assertEqual(int(totals[key]), int(expected), key)

    def test_row_counts_match_the_contract(self) -> None:
        shape = self.dataset.shape
        tables = self.contract.document["dataset"]["tables"]
        self.assertEqual(shape.warehouses, tables["warehouses"])
        self.assertEqual(shape.products, tables["products"])
        self.assertEqual(shape.customers, tables["customers"])
        self.assertEqual(shape.orders, tables["orders"])
        self.assertEqual(shape.stock_keys, tables["stock"])
        self.assertEqual(shape.order_lines, tables["order_lines"])
        self.assertEqual(shape.orders, tables["payments"])

    def test_initial_row_total(self) -> None:
        shape = self.dataset.shape
        total = (
            shape.warehouses
            + shape.products
            + shape.stock_keys
            + shape.customers
            + shape.orders
            + shape.order_lines
            + shape.orders  # one payment per order
        )
        self.assertEqual(total, int(self.contract.document["dataset"]["initial_rows"]))

    def test_stock_carrying_capacity(self) -> None:
        # 40,000 keys x 1,000,000 units, of which the oracle says 1,000,000 sold.
        self.assertEqual(self.dataset.totals()["sold_units"], 1_000_000)

    def test_manifest_is_stable_and_carries_the_oracle(self) -> None:
        first = self.dataset.manifest()
        second = self.dataset.manifest()
        self.assertEqual(first, second)
        self.assertEqual(first["oracle"], self.contract.document["oracle"])

    def test_ddl_matches_the_generated_shape(self) -> None:
        ddl = DDL_PATH.read_text(encoding="utf-8")
        for table in self.contract.document["dataset"]["tables"]:
            self.assertIn("CREATE TABLE %s (" % table, ddl)

    def test_small_scale_is_not_the_contract_dataset(self) -> None:
        # The runner may shrink a smoke import, but a shrunk dataset can never
        # satisfy the oracle, so the shape exposes that difference explicitly.
        small = load_dataset(8)
        self.assertFalse(small.shape.full)
        self.assertNotEqual(small.totals()["payments_cents"], 47_551_602_679)


if __name__ == "__main__":
    unittest.main()
