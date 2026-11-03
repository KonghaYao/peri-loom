"""Deterministic closed-loop plan stream.

Every 100 planned businesses contain the fixed weights from the contract and
are shuffled with seed 20261103.  Warmups consume the same stream; a different
concurrency level therefore does not promise the same measured prefix.

Parameter draw order (documented so a replay is unambiguous):

* product_stock: warehouse_id, product_id
* order_detail: order_id
* customer_orders: customer_id
* stock_report: warehouse_id, first_product_id
* checkout: hotspot branch (H8 only), warehouse_id, customer_id, 5 product ids,
  5 quantities

All ids are 1-based.  The historical order domain is [1, 100000]; checkout
customers and products come from the same full domain as the historical data.
"""
from __future__ import annotations

import random
import threading
from dataclasses import dataclass
from typing import Any

from .errors import BUSINESS_CLASSES
from .oracle import CUSTOMERS, ORDERS, PRODUCTS, SEED, WAREHOUSES

BLOCK = 100
MIX = {
    "product_stock": 20,
    "order_detail": 25,
    "customer_orders": 15,
    "stock_report": 10,
    "checkout": 30,
}
HOTSPOT_CUSTOMERS = tuple(1 + 4 * index for index in range(100))
HOTSPOT_PRODUCTS = tuple(range(1, 101))


@dataclass(frozen=True)
class PlannedOp:
    index: int
    kind: str
    params: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {"index": self.index, "kind": self.kind, "params": self.params}


class PlanStream:
    def __init__(self, mode: str, seed: int = SEED, products: int = PRODUCTS,
                 customers: int = CUSTOMERS, orders: int = ORDERS,
                 warehouses: int = WAREHOUSES):
        if mode not in {"uniform", "hotspot"}:
            raise ValueError("unknown plan mode %r" % mode)
        self.mode = mode
        self.products = products
        self.customers = customers
        self.orders = orders
        self.warehouses = warehouses
        self._kind_rng = random.Random(seed)
        self._param_rng = random.Random(seed ^ 0x5EED5EED)
        self._hotspot_products = tuple(range(1, min(100, products) + 1))
        self._hotspot_customers = tuple(
            customer for customer in HOTSPOT_CUSTOMERS if customer <= customers
        ) or (1,)
        self._block: list[str] = []
        self._block_offset = 0
        self._index = 0
        self._lock = threading.Lock()

    def next(self) -> PlannedOp:
        with self._lock:
            kind = self._next_kind_locked()
            index = self._index
            self._index += 1
            params = self._draw_params(kind)
        return PlannedOp(index=index, kind=kind, params=params)

    def _next_kind_locked(self) -> str:
        if self._block_offset >= len(self._block):
            block: list[str] = []
            for kind in BUSINESS_CLASSES:
                block.extend([kind] * MIX[kind])
            self._kind_rng.shuffle(block)
            self._block = block
            self._block_offset = 0
        kind = self._block[self._block_offset]
        self._block_offset += 1
        return kind

    def _draw_params(self, kind: str) -> dict[str, Any]:
        rng = self._param_rng
        if kind == "product_stock":
            return {
                "warehouse_id": rng.randint(1, self.warehouses),
                "product_id": rng.randint(1, self.products),
            }
        if kind == "order_detail":
            return {"order_id": rng.randint(1, self.orders)}
        if kind == "customer_orders":
            return {"customer_id": rng.randint(1, self.customers)}
        if kind == "stock_report":
            first = rng.randint(1, self.products - 99)
            return {
                "warehouse_id": rng.randint(1, self.warehouses),
                "first_product_id": first,
            }
        if kind == "checkout":
            hotspot = self.mode == "hotspot" and rng.random() < 0.8
            if hotspot:
                warehouse_id = 1
                customer_id = self._hotspot_customers[rng.randrange(len(self._hotspot_customers))]
                product_ids = rng.sample(self._hotspot_products, 5)
            else:
                warehouse_id = rng.randint(1, self.warehouses)
                customer_id = rng.randint(1, self.customers)
                product_ids = rng.sample(range(1, self.products + 1), 5)
            quantities = [rng.randint(1, 3) for _ in range(5)]
            return {
                "warehouse_id": warehouse_id,
                "customer_id": customer_id,
                "items": tuple(zip(product_ids, quantities)),
                "hotspot": hotspot,
            }
        raise ValueError("unknown business kind %r" % kind)
