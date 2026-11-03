"""The plan stream is part of the contract: deterministic, fixed mix, hotspot shape."""
from __future__ import annotations

import unittest
from collections import Counter

from pl_retail.contract import load_contract
from pl_retail.plan import PlanStream


def _stream(mode: str = "uniform", seed: int | None = None) -> PlanStream:
    contract = load_contract(None)
    dataset = contract.document["dataset"]["tables"]
    return PlanStream(
        mode,
        seed=contract.seed if seed is None else seed,
        products=dataset["products"],
        warehouses=dataset["warehouses"],
        customers=dataset["customers"],
        orders=dataset["orders"],
    )


class PlanTests(unittest.TestCase):
    def test_two_streams_with_the_same_seed_are_identical(self) -> None:
        first, second = _stream(), _stream()
        for _ in range(5000):
            left, right = first.next(), second.next()
            self.assertEqual(left.kind, right.kind)
            self.assertEqual(dict(left.params), dict(right.params))

    def test_a_different_seed_changes_the_parameters(self) -> None:
        first, second = _stream(seed=1), _stream(seed=2)
        self.assertNotEqual(
            [dict(first.next().params) for _ in range(20)],
            [dict(second.next().params) for _ in range(20)],
        )

    def test_mix_matches_the_contract_over_200_blocks(self) -> None:
        stream = _stream()
        counts = Counter(stream.next().kind for _ in range(10_000))
        for kind, percent in (("product_stock", 20), ("order_detail", 25),
                              ("customer_orders", 15), ("stock_report", 10),
                              ("checkout", 30)):
            self.assertEqual(counts[kind], percent * 100, kind)

    def test_reads_are_uniform_across_the_full_domain(self) -> None:
        stream = _stream()
        warehouses: set[int] = set()
        products: set[int] = set()
        for _ in range(4000):
            op = stream.next()
            if op.kind == "product_stock":
                warehouses.add(int(op.params["warehouse_id"]))
                products.add(int(op.params["product_id"]))
        self.assertEqual(warehouses, {1, 2, 3, 4})
        # 800 draws over 10,000 products: expect ~770 distinct.
        self.assertGreater(len(products), 700)

    def test_hotspot_checkouts_are_confined(self) -> None:
        stream = _stream("hotspot")
        hotspot = 0
        for _ in range(5000):
            op = stream.next()
            if op.kind != "checkout":
                continue
            if not op.params.get("hotspot"):
                continue
            hotspot += 1
            self.assertEqual(int(op.params["warehouse_id"]), 1)
            self.assertEqual(len(op.params["items"]), 5)
            for product_id, quantity in op.params["items"]:
                self.assertGreaterEqual(int(product_id), 1)
                self.assertLessEqual(int(product_id), 100)
                self.assertGreaterEqual(int(quantity), 1)
                self.assertLessEqual(int(quantity), 3)
            self.assertIn(int(op.params["customer_id"]), {1 + 4 * index for index in range(100)})
        self.assertGreater(hotspot, 900)

    def test_hotspot_mode_keeps_reads_uniform(self) -> None:
        stream = _stream("hotspot")
        warehouses: set[int] = set()
        for _ in range(3000):
            op = stream.next()
            if op.kind == "product_stock":
                warehouses.add(int(op.params["warehouse_id"]))
                self.assertGreater(int(op.params["product_id"]), 0)
        self.assertEqual(warehouses, {1, 2, 3, 4})

    def test_every_block_holds_the_full_mix(self) -> None:
        stream = _stream()
        first = Counter(stream.next().kind for _ in range(100))
        self.assertEqual(first["checkout"], 30)
        self.assertEqual(len(first), 5)


if __name__ == "__main__":
    unittest.main()
