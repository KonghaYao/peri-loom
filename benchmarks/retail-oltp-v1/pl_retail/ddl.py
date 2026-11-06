"""Fixed DDL and the schema audit.

The DDL is shipped as a file so an auditor can diff it independently.  The
runner sends it to a fresh fixture and the config audit compares the resulting
``sqlite_schema`` rows with the same file, so a silently added or dropped index
is visible in the evidence.
"""
from __future__ import annotations

import re
from pathlib import Path

from .contract import DDL_PATH

TABLES = (
    "warehouses",
    "products",
    "stock",
    "customers",
    "orders",
    "order_lines",
    "payments",
)

TABLE_COLUMNS = {
    "warehouses": ("id", "name", "cash_cents"),
    "products": ("id", "name", "price_cents", "description"),
    "stock": ("warehouse_id", "product_id", "initial_qty", "available_qty"),
    "customers": ("id", "name", "initial_balance_cents", "balance_cents", "note"),
    "orders": ("id", "customer_id", "warehouse_id", "request_key", "total_cents", "note"),
    "order_lines": ("id", "order_id", "product_id", "qty", "unit_price_cents", "note"),
    "payments": ("id", "order_id", "customer_id", "amount_cents", "note"),
}

INDEX_NAMES = (
    "orders_customer_id_id",
    "order_lines_order_id",
    "stock_product_id",
    "payments_customer_id",
)


def read_ddl(path: str | Path | None = None) -> str:
    with Path(path or DDL_PATH).open("r", encoding="utf-8") as handle:
        return handle.read()


def statements(path: str | Path | None = None) -> list[str]:
    """Split the fixed DDL into executable statements (comments stripped)."""
    text = read_ddl(path)
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        lines.append(line)
    body = "\n".join(lines)
    return [statement.strip() for statement in body.split(";") if statement.strip()]


def normalize_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql.strip().rstrip(";")).strip().lower()


def expected_schema() -> dict[tuple[str, str], str]:
    """Return ``{(type, name): normalized_sql}`` for tables and indexes."""
    expected: dict[tuple[str, str], str] = {}
    for statement in statements():
        lowered = normalize_sql(statement)
        match = re.match(r"create\s+(table|index)\s+([a-z0-9_]+)", lowered)
        if not match:
            raise ValueError("unexpected DDL statement: %s" % statement[:60])
        kind, name = match.group(1), match.group(2)
        expected[(kind, name)] = lowered
    for table in TABLES:
        if ("table", table) not in expected:
            raise ValueError("DDL is missing table %s" % table)
    for index in INDEX_NAMES:
        if ("index", index) not in expected:
            raise ValueError("DDL is missing index %s" % index)
    return expected


def schema_audit(schema_rows: list[dict]) -> list[str]:
    """Compare ``sqlite_schema`` rows with the fixed DDL; return violations."""
    violations: list[str] = []
    actual: dict[tuple[str, str], str] = {}
    for row in schema_rows:
        kind = str(row.get("type") or "")
        name = str(row.get("name") or "")
        if kind not in {"table", "index"}:
            continue
        if name.startswith("sqlite_"):
            continue
        actual[(kind, name)] = normalize_sql(str(row.get("sql") or ""))
    expected = expected_schema()
    for key, sql in expected.items():
        if key not in actual:
            violations.append("missing %s %s" % key)
        elif actual[key] != sql:
            violations.append("schema drift on %s %s" % key)
    for key in sorted(actual):
        if key not in expected:
            violations.append("unexpected %s %s" % key)
    return violations
