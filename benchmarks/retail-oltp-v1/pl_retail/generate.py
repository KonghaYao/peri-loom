"""Deterministic dataset generation for PL-RETAIL-OLTP 1.0.0.

The full-scale generator is exact: it asserts the independent oracle before any
row is emitted.  ``scale`` > 1 produces a smaller self-consistent dataset for
smoke runs; smoke data never enters the full leaderboard and makes no oracle
claim.

Generation is intentionally written as a fixed, documented rule rather than as
a stream of random draws.  Reproducing byte-identical data matters more than
matching an unavailable historical generator draw-for-draw; the divergence is
recorded in RUNBOOK.md and keeps ``certified_driver_sha256`` null.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

from . import oracle
from .oracle import OracleMismatch

_FILL = "0123456789abcdefghijklmnopqrstuvwxyz"


def fixed_text(prefix: str, length: int) -> str:
    if length <= 0:
        return ""
    body = prefix + "|" + _FILL * (length // len(_FILL) + 1)
    value = body[:length]
    if len(value.encode("ascii")) != length:
        raise ValueError("fixed text is not single-byte ASCII")
    return value


def product_description(product_id: int) -> str:
    return fixed_text("P%05d" % product_id, oracle.PRODUCT_DESCRIPTION_BYTES)


def customer_note(customer_id: int) -> str:
    return fixed_text("C%05d" % customer_id, oracle.CUSTOMER_NOTE_BYTES)


def order_note(order_id: int) -> str:
    return fixed_text("O%06d" % order_id, oracle.ORDER_NOTE_BYTES)


def order_line_note(line_id: int) -> str:
    return fixed_text("L%07d" % line_id, oracle.ORDER_LINE_NOTE_BYTES)


def payment_note(payment_id: int) -> str:
    return fixed_text("Y%06d" % payment_id, oracle.PAYMENT_NOTE_BYTES)


@dataclass(frozen=True)
class Shape:
    warehouses: int
    products: int
    customers: int
    orders: int
    stock_keys: int
    order_lines: int

    @property
    def full(self) -> bool:
        return (
            self.warehouses == oracle.WAREHOUSES
            and self.products == oracle.PRODUCTS
            and self.customers == oracle.CUSTOMERS
            and self.orders == oracle.ORDERS
        )

    @classmethod
    def for_scale(cls, scale: int) -> "Shape":
        if scale < 1:
            raise ValueError("scale must be >= 1")
        if scale == 1:
            return cls(
                warehouses=oracle.WAREHOUSES,
                products=oracle.PRODUCTS,
                customers=oracle.CUSTOMERS,
                orders=oracle.ORDERS,
                stock_keys=oracle.STOCK_KEYS,
                order_lines=oracle.ORDER_LINES,
            )
        products = max(10, oracle.PRODUCTS // scale)
        customers = max(10, oracle.CUSTOMERS // scale)
        orders = max(100, oracle.ORDERS // scale)
        warehouses = oracle.WAREHOUSES
        return cls(
            warehouses=warehouses,
            products=products,
            customers=customers,
            orders=orders,
            stock_keys=warehouses * products,
            order_lines=orders * oracle.LINES_PER_ORDER,
        )


def base_line(shape: Shape, order_id: int, offset: int) -> tuple[int, int]:
    """Fixed pre-adjustment (product_id, quantity) for one historical line."""
    index = (order_id - 1) * oracle.LINES_PER_ORDER + offset
    product_id = index % shape.products + 1
    if order_id == shape.orders and offset == oracle.LINES_PER_ORDER - 1:
        # The repeating 1/2/3 pattern totals 999,999 over 500,000 lines; the
        # final line carries one extra unit so the full dataset sells exactly
        # 1,000,000.  The override is part of the fixed rule.
        quantity = 3
    else:
        quantity = 1 + (index % 3)
    return product_id, quantity


class Dataset:
    """A fully materialised description of the deterministic rows.

    Only small aggregate arrays are retained (order totals, per-customer spend,
    per-warehouse cash, per-stock sold units); rows are produced on demand so
    the 760,004-row full dataset never has to sit in memory as objects.
    """

    def __init__(self, scale: int = 1):
        self.scale = scale
        self.shape = Shape.for_scale(scale)
        if self.shape.full:
            oracle.assert_initial_oracle()
        self._order_totals: list[int] | None = None
        self._customer_spend: list[int] | None = None
        self._warehouse_cash: list[int] | None = None
        self._stock_sold: list[int] | None = None
        self._patches: dict[int, int] | None = None
        self._materialise()

    # -- fixed rules -------------------------------------------------------

    def customer_id(self, order_id: int) -> int:
        return (order_id - 1) % self.shape.customers + 1

    def warehouse_id(self, order_id: int) -> int:
        return (order_id - 1) % self.shape.warehouses + 1

    def request_key(self, order_id: int) -> str:
        return "hist-%d-%06d" % (oracle.SEED, order_id)

    def price(self, product_id: int) -> int:
        if self.shape.full:
            return oracle.price_cents(product_id)
        return oracle.PRICE_BASE_CENTS + product_id

    def line_pid(self, order_id: int, offset: int) -> int:
        patch = self._patches.get(self._line_index(order_id, offset))
        if patch is not None:
            return patch
        return base_line(self.shape, order_id, offset)[0]

    def line_qty(self, order_id: int, offset: int) -> int:
        return base_line(self.shape, order_id, offset)[1]

    def _line_index(self, order_id: int, offset: int) -> int:
        return (order_id - 1) * oracle.LINES_PER_ORDER + offset

    # -- adjustment --------------------------------------------------------

    def _base_total(self) -> int:
        total = 0
        for order_id in range(1, self.shape.orders + 1):
            for offset in range(oracle.LINES_PER_ORDER):
                product_id, quantity = base_line(self.shape, order_id, offset)
                total += quantity * self.price(product_id)
        return total

    def _adjust(self) -> dict[int, int]:
        if not self.shape.full:
            return {}
        target = oracle.PAYMENTS_TOTAL_CENTS
        residual = target - self._base_total()
        if residual == 0:
            return {}
        patches: dict[int, int] = {}
        current: dict[int, list[int]] = {}
        for order_id in range(1, self.shape.orders + 1):
            current[order_id] = [
                base_line(self.shape, order_id, offset)[0]
                for offset in range(oracle.LINES_PER_ORDER)
            ]
        for order_id in range(1, self.shape.orders + 1):
            if residual == 0:
                break
            for offset in range(oracle.LINES_PER_ORDER):
                if residual == 0:
                    break
                product_id = current[order_id][offset]
                quantity = base_line(self.shape, order_id, offset)[1]
                others = {
                    current[order_id][index]
                    for index in range(oracle.LINES_PER_ORDER)
                    if index != offset
                }
                chosen = _choose_shift(
                    self.shape.products, product_id, others, quantity, residual
                )
                if chosen is None:
                    continue
                new_product_id, delta = chosen
                patches[self._line_index(order_id, offset)] = new_product_id
                current[order_id][offset] = new_product_id
                residual -= delta
        if residual != 0:
            raise OracleMismatch(
                "deterministic adjustment left a residual of %d cents" % residual
            )
        return patches

    def _materialise(self) -> None:
        self._patches = self._adjust()
        order_totals = [0] * (self.shape.orders + 1)
        customer_spend = [0] * (self.shape.customers + 1)
        warehouse_cash = [0] * (self.shape.warehouses + 1)
        stock_sold = [0] * (self.shape.stock_keys + 1)
        for order_id in range(1, self.shape.orders + 1):
            customer = self.customer_id(order_id)
            warehouse = self.warehouse_id(order_id)
            total = 0
            for offset in range(oracle.LINES_PER_ORDER):
                product_id = self.line_pid(order_id, offset)
                quantity = self.line_qty(order_id, offset)
                total += quantity * self.price(product_id)
                stock_sold[(warehouse - 1) * self.shape.products + product_id] += quantity
            order_totals[order_id] = total
            customer_spend[customer] += total
            warehouse_cash[warehouse] += total
        self._order_totals = order_totals
        self._customer_spend = customer_spend
        self._warehouse_cash = warehouse_cash
        self._stock_sold = stock_sold
        if self.shape.full:
            self._assert_oracle()

    def _assert_oracle(self) -> None:
        total = sum(self._order_totals or [])
        if total != oracle.PAYMENTS_TOTAL_CENTS:
            raise OracleMismatch("payments total %d != %d" % (total, oracle.PAYMENTS_TOTAL_CENTS))
        sold = sum(self._stock_sold or [])
        if sold != oracle.SOLD_UNITS:
            raise OracleMismatch("sold units %d != %d" % (sold, oracle.SOLD_UNITS))
        if sum(self._customer_spend or []) != oracle.PAYMENTS_TOTAL_CENTS:
            raise OracleMismatch("customer spend total does not close")
        text_bytes = (
            self.shape.products * oracle.PRODUCT_DESCRIPTION_BYTES
            + self.shape.customers * oracle.CUSTOMER_NOTE_BYTES
            + self.shape.orders * oracle.ORDER_NOTE_BYTES
            + self.shape.order_lines * oracle.ORDER_LINE_NOTE_BYTES
            + self.shape.orders * oracle.PAYMENT_NOTE_BYTES
        )
        if text_bytes != oracle.INITIAL_TEXT_BYTES:
            raise OracleMismatch("text bytes %d != %d" % (text_bytes, oracle.INITIAL_TEXT_BYTES))

    # -- aggregates --------------------------------------------------------

    def order_total(self, order_id: int) -> int:
        assert self._order_totals is not None
        return self._order_totals[order_id]

    def customer_balance(self, customer_id: int) -> int:
        assert self._customer_spend is not None
        return oracle.CUSTOMER_INITIAL_BALANCE_CENTS - self._customer_spend[customer_id]

    def warehouse_cash(self, warehouse_id: int) -> int:
        assert self._warehouse_cash is not None
        return self._warehouse_cash[warehouse_id]

    def stock_available(self, warehouse_id: int, product_id: int) -> int:
        assert self._stock_sold is not None
        return (
            oracle.STOCK_UNITS_PER_KEY
            - self._stock_sold[(warehouse_id - 1) * self.shape.products + product_id]
        )

    def stock_sold(self, warehouse_id: int, product_id: int) -> int:
        assert self._stock_sold is not None
        return self._stock_sold[(warehouse_id - 1) * self.shape.products + product_id]

    def totals(self) -> dict[str, int]:
        """Totals keyed exactly like ``benchmark.json.oracle``.

        The values are recomputed from the generated rows (not copied from the
        constants), which is what makes this an independent check: a change to
        the generation rule must move these numbers.
        """
        sold = sum(self._stock_sold or [])
        payments = sum(self._order_totals or [])
        balance = sum(
            oracle.CUSTOMER_INITIAL_BALANCE_CENTS - spend
            for spend in (self._customer_spend or [])[1:]
        )
        text_bytes = (
            oracle.PRODUCT_DESCRIPTION_BYTES * self.shape.products
            + oracle.CUSTOMER_NOTE_BYTES * self.shape.customers
            + oracle.ORDER_NOTE_BYTES * self.shape.orders
            + oracle.ORDER_LINE_NOTE_BYTES * self.shape.order_lines
            + oracle.PAYMENT_NOTE_BYTES * self.shape.orders
        )
        return {
            "payments_total_cents": payments,
            "sold_units": sold,
            "available_stock_units": oracle.STOCK_UNITS_PER_KEY * self.shape.stock_keys - sold,
            "customer_balance_total_cents": balance,
            "initial_text_bytes": text_bytes,
            "payments_cents": payments,
            "customer_balance_cents": balance,
            "warehouse_cash_cents": sum((self._warehouse_cash or [])[1:]),
        }

    def manifest(self) -> dict:
        return {
            "scale": self.scale,
            "shape": {
                "warehouses": self.shape.warehouses,
                "products": self.shape.products,
                "customers": self.shape.customers,
                "orders": self.shape.orders,
                "stock": self.shape.stock_keys,
                "order_lines": self.shape.order_lines,
                "payments": self.shape.orders,
            },
            "full_scale_oracle": self.shape.full,
            "oracle": {
                "payments_total_cents": oracle.PAYMENTS_TOTAL_CENTS if self.shape.full else None,
                "sold_units": oracle.SOLD_UNITS if self.shape.full else None,
                "available_stock_units": oracle.AVAILABLE_STOCK_UNITS if self.shape.full else None,
                "customer_balance_total_cents": (
                    oracle.CUSTOMER_BALANCE_TOTAL_CENTS if self.shape.full else None
                ),
                "initial_text_bytes": oracle.INITIAL_TEXT_BYTES if self.shape.full else None,
            },
            "generation_rule": "benchmark.json dataset.generation",
            "adjusted_lines": len(self._patches or {}),
        }

    # -- row iterators -----------------------------------------------------

    def warehouses_rows(self) -> Iterator[tuple]:
        for warehouse_id in range(1, self.shape.warehouses + 1):
            yield (
                warehouse_id,
                "warehouse-%d" % warehouse_id,
                self.warehouse_cash(warehouse_id),
            )

    def products_rows(self) -> Iterator[tuple]:
        for product_id in range(1, self.shape.products + 1):
            yield (
                product_id,
                "product-%05d" % product_id,
                self.price(product_id),
                product_description(product_id),
            )

    def stock_rows(self) -> Iterator[tuple]:
        for warehouse_id in range(1, self.shape.warehouses + 1):
            for product_id in range(1, self.shape.products + 1):
                yield (
                    warehouse_id,
                    product_id,
                    oracle.STOCK_UNITS_PER_KEY,
                    self.stock_available(warehouse_id, product_id),
                )

    def customers_rows(self) -> Iterator[tuple]:
        for customer_id in range(1, self.shape.customers + 1):
            yield (
                customer_id,
                "customer-%05d" % customer_id,
                oracle.CUSTOMER_INITIAL_BALANCE_CENTS,
                self.customer_balance(customer_id),
                customer_note(customer_id),
            )

    def orders_rows(self) -> Iterator[tuple]:
        for order_id in range(1, self.shape.orders + 1):
            yield (
                order_id,
                self.customer_id(order_id),
                self.warehouse_id(order_id),
                self.request_key(order_id),
                self.order_total(order_id),
                order_note(order_id),
            )

    def order_lines_rows(self) -> Iterator[tuple]:
        line_id = 0
        for order_id in range(1, self.shape.orders + 1):
            for offset in range(oracle.LINES_PER_ORDER):
                line_id += 1
                product_id = self.line_pid(order_id, offset)
                yield (
                    line_id,
                    order_id,
                    product_id,
                    self.line_qty(order_id, offset),
                    self.price(product_id),
                    order_line_note(line_id),
                )

    def payments_rows(self) -> Iterator[tuple]:
        for order_id in range(1, self.shape.orders + 1):
            yield (
                order_id,
                order_id,
                self.customer_id(order_id),
                self.order_total(order_id),
                payment_note(order_id),
            )

    def rows(self, table: str) -> Iterator[tuple]:
        if table == "warehouses":
            return self.warehouses_rows()
        if table == "products":
            return self.products_rows()
        if table == "stock":
            return self.stock_rows()
        if table == "customers":
            return self.customers_rows()
        if table == "orders":
            return self.orders_rows()
        if table == "order_lines":
            return self.order_lines_rows()
        if table == "payments":
            return self.payments_rows()
        raise KeyError(table)

    def row_count(self, table: str) -> int:
        return {
            "warehouses": self.shape.warehouses,
            "products": self.shape.products,
            "stock": self.shape.stock_keys,
            "customers": self.shape.customers,
            "orders": self.shape.orders,
            "order_lines": self.shape.order_lines,
            "payments": self.shape.orders,
        }[table]


def _choose_shift(
    products: int,
    product_id: int,
    others: set[int],
    quantity: int,
    residual: int,
) -> tuple[int, int] | None:
    """Pick one deterministic product shift whose value moves toward zero.

    Returns ``(new_product_id, delta_cents)``.  The delta never overshoots the
    residual, so a later line can always finish the exact adjustment.
    """
    if residual > 0:
        limit = min(products - product_id, residual // quantity)
        if limit <= 0:
            return None
        for magnitude in _candidate_magnitudes(limit):
            candidate = product_id + magnitude
            if candidate not in others:
                return candidate, magnitude * quantity
        return None
    need = -residual
    limit = min(product_id - 1, need // quantity)
    if limit <= 0:
        return None
    for magnitude in _candidate_magnitudes(limit):
        candidate = product_id - magnitude
        if candidate not in others:
            return candidate, -magnitude * quantity
    return None


def _candidate_magnitudes(limit: int) -> Iterator[int]:
    """Try the largest shift first, then walk down to one."""
    for magnitude in range(limit, 0, -1):
        yield magnitude


def load_dataset(scale: int = 1) -> Dataset:
    return Dataset(scale=scale)
