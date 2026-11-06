"""Full stream verification for one retail fixture.

Verification is one consistent read transaction, keyset paginated at 500 rows
per page, over all seven tables to a final empty page.  Every historical row
is compared with the deterministic generator, every confirmed increment is
compared with the ledger, and the full table is scanned for the per-stock,
per-customer, per-warehouse and global conservation rules.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any, Iterator

from .business import BEGIN, COMMIT, ROLLBACK, Session
from .client import HranaClient, Target
from .generate import Dataset

PAGE_SIZE = 500
MAX_PAGES = 4_000_000
MAX_VIOLATIONS = 64


class VerifyError(RuntimeError):
    """Verification could not be established; the round cannot be trusted."""


@dataclass
class Ledger:
    entries: dict[str, dict[str, Any]] = field(default_factory=dict)
    stock_delta: dict[tuple[int, int], int] = field(default_factory=dict)
    customer_delta: dict[int, int] = field(default_factory=dict)
    warehouse_delta: dict[int, int] = field(default_factory=dict)
    units: int = 0
    payments_total: int = 0
    rejected_keys: set[str] = field(default_factory=set)
    unknown_keys: set[str] = field(default_factory=set)

    @classmethod
    def from_records(
        cls,
        confirmed: list[dict[str, Any]],
        rejected_keys: list[str] | None = None,
        unknown_keys: list[str] | None = None,
    ) -> "Ledger":
        ledger = cls(rejected_keys=set(rejected_keys or []), unknown_keys=set(unknown_keys or []))
        for entry in confirmed:
            key = str(entry["request_key"])
            if key in ledger.entries:
                raise VerifyError("duplicate confirmed request key %s" % key)
            ledger.entries[key] = entry
            warehouse = int(entry["warehouse_id"])
            customer = int(entry["customer_id"])
            total = int(entry["total_cents"])
            computed = 0
            for item in entry["items"]:
                product = int(item["product_id"])
                quantity = int(item["qty"])
                price = int(item["unit_price_cents"])
                if quantity < 1 or quantity > 3 or price <= 0:
                    raise VerifyError("ledger item out of range for %s" % key)
                ledger.stock_delta[(warehouse, product)] = (
                    ledger.stock_delta.get((warehouse, product), 0) + quantity
                )
                ledger.units += quantity
                computed += quantity * price
            if computed != total:
                raise VerifyError("ledger total does not match its items for %s" % key)
            ledger.customer_delta[customer] = ledger.customer_delta.get(customer, 0) + total
            ledger.warehouse_delta[warehouse] = (
                ledger.warehouse_delta.get(warehouse, 0) + total
            )
            ledger.payments_total += total
        return ledger


@dataclass
class TableScan:
    rows: int = 0
    bytes: int = 0
    pages: int = 0
    digest: hashlib._Hash = field(default_factory=hashlib.sha256)

    def add(self, row: list[Any]) -> None:
        encoded = json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.digest.update(encoded)
        self.digest.update(b"\n")
        self.bytes += len(encoded) + 1
        self.rows += 1

    def report(self) -> dict[str, Any]:
        return {
            "rows": self.rows,
            "bytes": self.bytes,
            "pages": self.pages,
            "sha256": self.digest.hexdigest(),
        }


class _Violations:
    def __init__(self) -> None:
        self.checked = 0
        self.items: list[dict[str, Any]] = []
        self.count = 0

    def check(self, condition: bool, table: str, rule: str, detail: Any = None) -> bool:
        self.checked += 1
        if not condition:
            self.count += 1
            if len(self.items) < MAX_VIOLATIONS:
                self.items.append({"table": table, "rule": rule, "detail": detail})
        return condition

    def fail(self, table: str, rule: str, detail: Any = None) -> None:
        self.check(False, table, rule, detail)


def verify_stream(
    target: Target,
    dataset: Dataset,
    ledger: Ledger,
    phase: str,
    recorder: Any = None,
) -> dict[str, Any]:
    started = time.monotonic_ns()
    client = HranaClient(target, timeout=60.0, recorder=recorder)
    session = Session(client)
    violations = _Violations()
    tables: dict[str, TableScan] = {
        name: TableScan()
        for name in ("warehouses", "products", "stock", "customers", "orders", "order_lines", "payments")
    }
    aggregates: dict[str, int] = {
        "payments_total_cents": 0,
        "sold_units": 0,
        "available_stock_units": 0,
        "customer_balance_cents": 0,
        "warehouse_cash_cents": 0,
        "text_bytes": 0,
    }
    order_state = _OrderState(dataset, ledger, violations)
    line_state = _LineState(dataset, ledger, order_state, violations)
    payment_state = _PaymentState(dataset, ledger, order_state, violations)
    stock_state = _StockState(dataset, ledger, violations, aggregates)
    customer_state = _CustomerState(dataset, ledger, violations, aggregates)
    warehouse_state = _WarehouseState(dataset, ledger, violations, aggregates)
    product_state = _ProductState(dataset, violations, aggregates)

    error: str | None = None
    try:
        session.open()
        reply = session.execute(BEGIN)
        if reply.transport_error or reply.error or _step_failed(reply):
            raise VerifyError("verification BEGIN failed: %s" % _reply_reason(reply))
        _scan(session, "warehouses", lambda row: warehouse_state.accept(row, tables["warehouses"]))
        _scan(session, "products", lambda row: product_state.accept(row, tables["products"]))
        _scan(session, "stock", lambda row: stock_state.accept(row, tables["stock"]))
        _scan(session, "customers", lambda row: customer_state.accept(row, tables["customers"]))
        _scan(session, "orders", lambda row: order_state.accept(row, tables["orders"]))
        _scan(session, "order_lines", lambda row: line_state.accept(row, tables["order_lines"]))
        _scan(session, "payments", lambda row: payment_state.accept(row, tables["payments"]))
        reply = session.execute(COMMIT)
        if reply.transport_error or reply.error or _step_failed(reply):
            raise VerifyError("verification COMMIT failed: %s" % _reply_reason(reply))
    except Exception as failure:  # noqa: BLE001 - the report preserves the reason
        error = "%s: %s" % (type(failure).__name__, failure)
        try:
            session.execute(ROLLBACK)
        except Exception:
            pass
    finally:
        try:
            session.close()
        except Exception:
            pass

    # Cross-table conservation is checked even when a scan failed: the report
    # must say what was observed, and the status decision belongs to the scorer.
    order_state.finish(aggregates, violations)
    line_state.finish(aggregates, violations)
    payment_state.finish(aggregates, violations)
    stock_state.finish(aggregates, violations)
    customer_state.finish(aggregates, violations)
    warehouse_state.finish(aggregates, violations)
    product_state.finish(aggregates, violations)
    _global_conservation(dataset, ledger, aggregates, violations, tables)

    finished = time.monotonic_ns()
    return {
        "phase": phase,
        "ok": error is None and violations.count == 0,
        "error": error,
        "started_ns": started,
        "finished_ns": finished,
        "duration_ms": round((finished - started) / 1_000_000, 3),
        "tables": {name: scan.report() for name, scan in tables.items()},
        "aggregates": aggregates,
        "oracle": dataset.manifest()["oracle"],
        "invariants": {
            "checked": violations.checked,
            "violation_count": violations.count,
            "violations": violations.items,
        },
        "ledger": order_state.ledger_report(),
        "immutable": {"products_sha256": tables["products"].report()["sha256"]},
    }


# -- pagination -------------------------------------------------------------


def _scan(session: Session, table: str, accept) -> None:
    pages = 0
    cursor: tuple | int | None = None
    seen_rows = 0
    while True:
        pages += 1
        if pages > MAX_PAGES:
            raise VerifyError("pagination did not terminate for %s" % table)
        if table == "stock":
            if cursor is None:
                sql = (
                    "SELECT warehouse_id, product_id, initial_qty, available_qty FROM stock "
                    "ORDER BY warehouse_id, product_id LIMIT %d" % PAGE_SIZE
                )
                args: tuple = ()
            else:
                sql = (
                    "SELECT warehouse_id, product_id, initial_qty, available_qty FROM stock "
                    "WHERE warehouse_id > ? OR (warehouse_id = ? AND product_id > ?) "
                    "ORDER BY warehouse_id, product_id LIMIT %d" % PAGE_SIZE
                )
                args = (cursor[0], cursor[0], cursor[1])
        else:
            if cursor is None:
                sql = "SELECT * FROM %s ORDER BY id LIMIT %d" % (table, PAGE_SIZE)
                args = ()
            else:
                sql = "SELECT * FROM %s WHERE id > ? ORDER BY id LIMIT %d" % (table, PAGE_SIZE)
                args = (cursor,)
        reply = session.execute(sql, args=args, want_rows=True)
        if reply.transport_error or reply.error or _step_failed(reply):
            raise VerifyError("page query failed for %s: %s" % (table, _reply_reason(reply)))
        if not reply.results or reply.results[0].rows is None:
            raise VerifyError("page query returned no row set for %s" % table)
        rows = reply.results[0].rows
        if not rows:
            break
        for row in rows:
            accept(row)
            seen_rows += 1
        if len(rows) < PAGE_SIZE:
            break
        if table == "stock":
            new_cursor = (int(rows[-1][0]), int(rows[-1][1]))
            if cursor is not None and new_cursor <= cursor:
                raise VerifyError("stock cursor did not advance")
        else:
            new_cursor = int(rows[-1][0])
            if cursor is not None and new_cursor <= cursor:
                raise VerifyError("cursor did not advance for %s" % table)
        cursor = new_cursor
    if seen_rows == 0:
        raise VerifyError("table %s was empty" % table)


def _step_failed(reply) -> bool:
    return bool(reply.results and not reply.results[0].ok)


def _reply_reason(reply) -> str:
    if reply.transport_error:
        return "transport_error=%s" % reply.transport_error
    if reply.error:
        return "error=%s" % (reply.error.get("code") or reply.error)
    step = reply.first_error_step()
    if step is not None:
        return "step_error=%s:%s" % (step.error_code, step.error_message)
    return "unknown_reply"


# -- per-table state --------------------------------------------------------


class _ProductState:
    def __init__(self, dataset: Dataset, violations: _Violations, aggregates: dict[str, int]):
        self.dataset = dataset
        self.violations = violations
        self.aggregates = aggregates
        self.expected: Iterator[tuple] | None = dataset.products_rows()

    def accept(self, row: list[Any], scan: TableScan) -> None:
        scan.add(row)
        expected = next(self.expected, None) if self.expected is not None else None
        if expected is None:
            self.violations.fail("products", "unexpected_extra_row", row[:2])
            return
        product_id, name, price, description = expected
        ok = (
            int(row[0]) == product_id
            and str(row[1]) == name
            and int(row[2]) == price
            and str(row[3]) == description
        )
        self.violations.check(ok, "products", "row_mismatch", row[:3])
        self.aggregates["text_bytes"] += len(str(row[3]).encode("utf-8"))

    def finish(self, aggregates: dict[str, int], violations: _Violations) -> None:
        if self.expected is not None and next(self.expected, None) is not None:
            violations.fail("products", "missing_rows")


class _WarehouseState:
    def __init__(self, dataset: Dataset, ledger: Ledger, violations: _Violations,
                 aggregates: dict[str, int]):
        self.dataset = dataset
        self.ledger = ledger
        self.violations = violations
        self.aggregates = aggregates
        self.expected: Iterator[tuple] | None = dataset.warehouses_rows()

    def accept(self, row: list[Any], scan: TableScan) -> None:
        scan.add(row)
        expected = next(self.expected, None) if self.expected is not None else None
        if expected is None:
            self.violations.fail("warehouses", "unexpected_extra_row", row[:2])
            return
        warehouse_id, name, cash = expected
        cash += self.ledger.warehouse_delta.get(warehouse_id, 0)
        ok = int(row[0]) == warehouse_id and str(row[1]) == name and int(row[2]) == cash
        self.violations.check(ok, "warehouses", "row_mismatch", row[:3])
        self.violations.check(int(row[2]) >= 0, "warehouses", "negative_cash", row[0])
        self.aggregates["warehouse_cash_cents"] += int(row[2])

    def finish(self, aggregates: dict[str, int], violations: _Violations) -> None:
        if self.expected is not None and next(self.expected, None) is not None:
            violations.fail("warehouses", "missing_rows")


class _StockState:
    def __init__(self, dataset: Dataset, ledger: Ledger, violations: _Violations,
                 aggregates: dict[str, int]):
        self.dataset = dataset
        self.ledger = ledger
        self.violations = violations
        self.aggregates = aggregates
        self.expected: Iterator[tuple] | None = dataset.stock_rows()

    def accept(self, row: list[Any], scan: TableScan) -> None:
        scan.add(row)
        expected = next(self.expected, None) if self.expected is not None else None
        if expected is None:
            self.violations.fail("stock", "unexpected_extra_row", row[:2])
            return
        warehouse_id, product_id, initial, available = expected
        available -= self.ledger.stock_delta.get((warehouse_id, product_id), 0)
        ok = (
            int(row[0]) == warehouse_id
            and int(row[1]) == product_id
            and int(row[2]) == initial
            and int(row[3]) == available
        )
        self.violations.check(ok, "stock", "row_mismatch", row)
        self.violations.check(0 <= int(row[3]) <= int(row[2]), "stock", "available_out_of_range", row)
        self.aggregates["sold_units"] += int(row[2]) - int(row[3])
        self.aggregates["available_stock_units"] += int(row[3])

    def finish(self, aggregates: dict[str, int], violations: _Violations) -> None:
        if self.expected is not None and next(self.expected, None) is not None:
            violations.fail("stock", "missing_rows")


class _CustomerState:
    def __init__(self, dataset: Dataset, ledger: Ledger, violations: _Violations,
                 aggregates: dict[str, int]):
        self.dataset = dataset
        self.ledger = ledger
        self.violations = violations
        self.aggregates = aggregates
        self.expected: Iterator[tuple] | None = dataset.customers_rows()

    def accept(self, row: list[Any], scan: TableScan) -> None:
        scan.add(row)
        expected = next(self.expected, None) if self.expected is not None else None
        if expected is None:
            self.violations.fail("customers", "unexpected_extra_row", row[:2])
            return
        customer_id, name, initial, balance, note = expected
        balance -= self.ledger.customer_delta.get(customer_id, 0)
        ok = (
            int(row[0]) == customer_id
            and str(row[1]) == name
            and int(row[2]) == initial
            and int(row[3]) == balance
            and str(row[4]) == note
        )
        self.violations.check(ok, "customers", "row_mismatch", row[:4])
        self.violations.check(int(row[3]) >= 0, "customers", "negative_balance", row[0])
        self.aggregates["customer_balance_cents"] += int(row[3])
        self.aggregates["text_bytes"] += len(str(row[4]).encode("utf-8"))

    def finish(self, aggregates: dict[str, int], violations: _Violations) -> None:
        if self.expected is not None and next(self.expected, None) is not None:
            violations.fail("customers", "missing_rows")


class _OrderState:
    def __init__(self, dataset: Dataset, ledger: Ledger, violations: _Violations):
        self.dataset = dataset
        self.ledger = ledger
        self.violations = violations
        self.expected: Iterator[tuple] | None = dataset.orders_rows()
        self.seen_keys: set[str] = set()
        self.seen_ledger: set[str] = set()
        self.unexpected_keys: list[str] = []
        self.unknown_present: list[str] = []
        self.extra_orders: dict[int, dict[str, Any]] = {}
        self.count = 0
        self.total_cents = 0
        self.note_bytes = 0
        self.rejected_present = False

    def accept(self, row: list[Any], scan: TableScan) -> None:
        scan.add(row)
        self.count += 1
        order_id = int(row[0])
        key = str(row[3])
        self.total_cents += int(row[4])
        self.note_bytes += len(str(row[5]).encode("utf-8"))
        if not self.violations.check(key not in self.seen_keys, "orders", "duplicate_request_key", key):
            return
        self.seen_keys.add(key)
        if key in self.ledger.rejected_keys:
            self.rejected_present = True
            self.violations.fail("orders", "rejected_key_present", key)
        if key in self.ledger.unknown_keys:
            self.unknown_present.append(key)
        if order_id <= self.dataset.shape.orders:
            expected = next(self.expected, None) if self.expected is not None else None
            if expected is None:
                self.violations.fail("orders", "unexpected_historical_row", order_id)
                return
            ok = tuple(row[:6]) == expected
            self.violations.check(ok, "orders", "historical_row_mismatch", order_id)
            return
        entry = self.ledger.entries.get(key)
        if entry is None:
            self.unexpected_keys.append(key)
            self.violations.fail("orders", "unconfirmed_order", key)
            return
        self.seen_ledger.add(key)
        ok = (
            int(row[1]) == int(entry["customer_id"])
            and int(row[2]) == int(entry["warehouse_id"])
            and int(row[4]) == int(entry["total_cents"])
        )
        self.violations.check(ok, "orders", "confirmed_row_mismatch", key)
        self.violations.check(len(str(row[5]).encode("utf-8")) == 128, "orders", "note_length", key)
        self.extra_orders[order_id] = entry

    def finish(self, aggregates: dict[str, int], violations: _Violations) -> None:
        if self.expected is not None and next(self.expected, None) is not None:
            violations.fail("orders", "missing_historical_rows")
        missing = sorted(set(self.ledger.entries) - self.seen_ledger)
        if missing:
            violations.fail("orders", "missing_confirmed_orders", missing[:8])
        aggregates["order_total_cents"] += self.total_cents
        aggregates["text_bytes"] += self.note_bytes
        aggregates["order_count"] = self.count

    def ledger_report(self) -> dict[str, Any]:
        return {
            "confirmed": len(self.ledger.entries),
            "seen": len(self.seen_ledger),
            "missing": sorted(set(self.ledger.entries) - self.seen_ledger)[:16],
            "unexpected": self.unexpected_keys[:16],
            "unknown_present": self.unknown_present[:16],
            "rejected_absent": not self.rejected_present,
        }


class _LineState:
    def __init__(self, dataset: Dataset, ledger: Ledger, orders: _OrderState,
                 violations: _Violations):
        self.dataset = dataset
        self.ledger = ledger
        self.orders = orders
        self.violations = violations
        self.expected: Iterator[tuple] | None = dataset.order_lines_rows()
        self.count = 0
        self.note_bytes = 0
        self.per_order: dict[int, list[tuple[int, int, int]]] = {}
        self.historical_lines = 0

    def accept(self, row: list[Any], scan: TableScan) -> None:
        scan.add(row)
        self.count += 1
        order_id = int(row[1])
        self.note_bytes += len(str(row[5]).encode("utf-8"))
        if order_id <= self.dataset.shape.orders:
            expected = next(self.expected, None) if self.expected is not None else None
            if expected is None:
                self.violations.fail("order_lines", "unexpected_historical_row", row[0])
                return
            ok = tuple(row[:6]) == expected
            self.violations.check(ok, "order_lines", "historical_row_mismatch", row[0])
            self.historical_lines += 1
            return
        entry = self.orders.extra_orders.get(order_id)
        if entry is None:
            self.violations.fail("order_lines", "line_for_unconfirmed_order", row[1])
            return
        product = int(row[2])
        quantity = int(row[3])
        price = int(row[4])
        ok = (
            price == self.dataset.price(product)
            and 1 <= quantity <= 3
            and len(str(row[5]).encode("utf-8")) == 64
        )
        self.violations.check(ok, "order_lines", "line_field_invalid", row[:5])
        self.per_order.setdefault(order_id, []).append((product, quantity, price))

    def finish(self, aggregates: dict[str, int], violations: _Violations) -> None:
        if self.expected is not None and next(self.expected, None) is not None:
            violations.fail("order_lines", "missing_historical_rows")
        for order_id, entry in self.orders.extra_orders.items():
            lines = self.per_order.get(order_id, [])
            if not self.violations.check(
                len(lines) == 5, "order_lines", "line_count", order_id
            ):
                continue
            products = {item[0] for item in lines}
            self.violations.check(len(products) == 5, "order_lines", "distinct_products", order_id)
            expected = sorted(
                (int(item["product_id"]), int(item["qty"]), int(item["unit_price_cents"]))
                for item in entry["items"]
            )
            self.violations.check(sorted(lines) == expected, "order_lines", "ledger_lines", order_id)
        aggregates["text_bytes"] += self.note_bytes
        aggregates["line_count"] = self.count


class _PaymentState:
    def __init__(self, dataset: Dataset, ledger: Ledger, orders: _OrderState,
                 violations: _Violations):
        self.dataset = dataset
        self.ledger = ledger
        self.orders = orders
        self.violations = violations
        self.expected: Iterator[tuple] | None = dataset.payments_rows()
        self.count = 0
        self.note_bytes = 0
        self.extra_payments: dict[int, int] = {}
        self.total_cents = 0

    def accept(self, row: list[Any], scan: TableScan) -> None:
        scan.add(row)
        self.count += 1
        payment_id = int(row[0])
        order_id = int(row[1])
        self.note_bytes += len(str(row[4]).encode("utf-8"))
        self.total_cents += int(row[3])
        if payment_id <= self.dataset.shape.orders:
            expected = next(self.expected, None) if self.expected is not None else None
            if expected is None:
                self.violations.fail("payments", "unexpected_historical_row", payment_id)
                return
            ok = tuple(row[:5]) == expected
            self.violations.check(ok, "payments", "historical_row_mismatch", payment_id)
            return
        entry = self.orders.extra_orders.get(order_id)
        if entry is None:
            self.violations.fail("payments", "payment_for_unconfirmed_order", order_id)
            return
        ok = (
            int(row[2]) == int(entry["customer_id"])
            and int(row[3]) == int(entry["total_cents"])
            and len(str(row[4]).encode("utf-8")) == 64
        )
        self.violations.check(ok, "payments", "payment_field_invalid", row[:4])
        self.extra_payments[order_id] = self.extra_payments.get(order_id, 0) + 1

    def finish(self, aggregates: dict[str, int], violations: _Violations) -> None:
        if self.expected is not None and next(self.expected, None) is not None:
            violations.fail("payments", "missing_historical_rows")
        for order_id in self.orders.extra_orders:
            if self.extra_payments.get(order_id, 0) != 1:
                violations.fail("payments", "payment_count", order_id)
        aggregates["text_bytes"] += self.note_bytes
        aggregates["payment_count"] = self.count
        aggregates["payments_total_cents"] = self.total_cents


def _global_conservation(
    dataset: Dataset,
    ledger: Ledger,
    aggregates: dict[str, int],
    violations: _Violations,
    tables: dict[str, TableScan],
) -> None:
    shape = dataset.shape
    expected_payments = dataset.totals()["payments_cents"] + ledger.payments_total
    expected_units = dataset.totals()["sold_units"] + ledger.units
    expected_balance = dataset.totals()["customer_balance_cents"] - ledger.payments_total
    violations.check(
        aggregates["payments_total_cents"] == expected_payments,
        "global",
        "payments_total",
        aggregates["payments_total_cents"],
    )
    violations.check(
        aggregates["order_total_cents"] == expected_payments,
        "global",
        "order_total",
        aggregates["order_total_cents"],
    )
    violations.check(
        aggregates["payments_total_cents"] == aggregates["warehouse_cash_cents"],
        "global",
        "warehouse_cash_mismatch",
        aggregates["warehouse_cash_cents"],
    )
    violations.check(
        aggregates["sold_units"] == expected_units,
        "global",
        "sold_units",
        aggregates["sold_units"],
    )
    violations.check(
        aggregates["available_stock_units"] == shape.stock_keys * 1_000_000 - expected_units,
        "global",
        "available_stock_units",
        aggregates["available_stock_units"],
    )
    violations.check(
        aggregates["customer_balance_cents"] == expected_balance,
        "global",
        "customer_balance_total",
        aggregates["customer_balance_cents"],
    )
    expected_text = (
        shape.products * 256
        + shape.customers * 128
        + (shape.orders + len(ledger.entries)) * 128
        + (shape.order_lines + 5 * len(ledger.entries)) * 64
        + (shape.orders + len(ledger.entries)) * 64
    )
    violations.check(
        aggregates["text_bytes"] == expected_text,
        "global",
        "text_bytes",
        aggregates["text_bytes"],
    )
    expected_orders = shape.orders + len(ledger.entries)
    violations.check(
        tables["orders"].rows == expected_orders,
        "global",
        "order_count",
        tables["orders"].rows,
    )
    violations.check(
        tables["order_lines"].rows == shape.order_lines + 5 * len(ledger.entries),
        "global",
        "line_count",
        tables["order_lines"].rows,
    )
    violations.check(
        tables["payments"].rows == expected_orders,
        "global",
        "payment_count",
        tables["payments"].rows,
    )
    violations.check(tables["stock"].rows == shape.stock_keys, "global", "stock_count")
    violations.check(tables["products"].rows == shape.products, "global", "product_count")
    violations.check(tables["customers"].rows == shape.customers, "global", "customer_count")
    violations.check(tables["warehouses"].rows == shape.warehouses, "global", "warehouse_count")
    if shape.full:
        violations.check(
            aggregates["payments_total_cents"] == 47_551_602_679 + ledger.payments_total,
            "global",
            "full_scale_payment_total",
        )
