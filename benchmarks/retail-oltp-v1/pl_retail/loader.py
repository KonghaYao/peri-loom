"""Bulk loader for the fixed dataset.

The loader uses one execute per HTTP request, exactly like the business driver.
A statement carries many rows but never exceeds the fixed 65,536-byte SQL
budget; the byte budget is measured on the encoded statement, not guessed.
The whole load is one transaction, so a partial load is never left behind.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Iterator

from .business import Session
from .ddl import statements as ddl_statements
from .generate import Dataset

DEFAULT_BATCH_BYTES = 48 * 1024
DEFAULT_BATCH_ROWS = 500

TABLE_INSERT = {
    "warehouses": "INSERT INTO warehouses (id, name, cash_cents) VALUES ",
    "products": "INSERT INTO products (id, name, price_cents, description) VALUES ",
    "stock": "INSERT INTO stock (warehouse_id, product_id, initial_qty, available_qty) VALUES ",
    "customers": (
        "INSERT INTO customers (id, name, initial_balance_cents, balance_cents, note) VALUES "
    ),
    "orders": (
        "INSERT INTO orders (id, customer_id, warehouse_id, request_key, total_cents, note) VALUES "
    ),
    "order_lines": (
        "INSERT INTO order_lines (id, order_id, product_id, qty, unit_price_cents, note) VALUES "
    ),
    "payments": "INSERT INTO payments (id, order_id, customer_id, amount_cents, note) VALUES ",
}

TABLE_ORDER = ("warehouses", "products", "stock", "customers", "orders", "order_lines", "payments")


@dataclass
class LoadReport:
    ok: bool = False
    rows: dict[str, int] = field(default_factory=dict)
    statements: int = 0
    sql_bytes: int = 0
    elapsed_ms: float = 0.0
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "rows": self.rows,
            "statements": self.statements,
            "sql_bytes": self.sql_bytes,
            "elapsed_ms": self.elapsed_ms,
            "error": self.error,
        }


def _quoted(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    text = str(value)
    if "'" in text:
        raise ValueError("fixed text must not contain a single quote")
    return "'%s'" % text


def _statement(prefix: str, rows: list[tuple]) -> tuple[str, int]:
    parts: list[str] = []
    size = len(prefix.encode("utf-8"))
    for row in rows:
        encoded = "(" + ",".join(_quoted(value) for value in row) + ")"
        parts.append(encoded)
        size += len(encoded.encode("utf-8")) + 1  # comma/newline budget
    return prefix + ",".join(parts), size


def _chunks(rows: Iterator[tuple], prefix: str, batch_bytes: int, batch_rows: int) -> Iterator[tuple[str, int, int]]:
    current: list[tuple] = []
    current_bytes = 0
    for row in rows:
        row_size = len((",".join(_quoted(value) for value in row)).encode("utf-8")) + 3
        if current and (len(current) >= batch_rows or current_bytes + row_size > batch_bytes):
            statement, size = _statement(prefix, current)
            yield statement, len(current), size
            current = []
            current_bytes = 0
        current.append(row)
        current_bytes += row_size
    if current:
        statement, size = _statement(prefix, current)
        yield statement, len(current), size


def apply_schema(session: Session) -> None:
    for statement in ddl_statements():
        reply = session.execute(statement)
        if reply.transport_error or reply.error or reply.first_error_step() is not None:
            raise RuntimeError("DDL failed: %s :: %s" % (statement.splitlines()[0], reply.evidence()))


def load_dataset(
    session: Session,
    dataset: Dataset,
    *,
    batch_bytes: int = DEFAULT_BATCH_BYTES,
    batch_rows: int = DEFAULT_BATCH_ROWS,
    progress=None,
) -> LoadReport:
    report = LoadReport()
    started = time.monotonic()
    apply_schema(session)
    reply = session.execute("BEGIN")
    if reply.transport_error or reply.error or reply.first_error_step() is not None:
        report.error = "load BEGIN failed: %s" % (reply.evidence(),)
        return report
    try:
        for table in TABLE_ORDER:
            prefix = TABLE_INSERT[table]
            count = 0
            for statement, rows, size in _chunks(
                dataset.rows(table), prefix, batch_bytes, batch_rows
            ):
                if size > 65_536:
                    raise RuntimeError(
                        "generated %s statement is %d bytes, above the fixed SQL budget"
                        % (table, size)
                    )
                reply = session.execute(statement)
                if reply.transport_error or reply.error or reply.first_error_step() is not None:
                    raise RuntimeError(
                        "load of %s failed after %d rows: %s"
                        % (table, count, reply.evidence())
                    )
                count += rows
                report.statements += 1
                report.sql_bytes += size
                if progress is not None and report.statements % 200 == 0:
                    progress(table, count)
            report.rows[table] = count
        reply = session.execute("COMMIT")
        if reply.transport_error or reply.error or reply.first_error_step() is not None:
            raise RuntimeError("load COMMIT failed: %s" % (reply.evidence(),))
        report.ok = True
    except Exception as failure:  # noqa: BLE001 - the report is the evidence
        report.error = "%s: %s" % (type(failure).__name__, failure)
        try:
            session.execute("ROLLBACK")
        except Exception:
            pass
    finally:
        report.elapsed_ms = round((time.monotonic() - started) * 1000, 3)
    return report
