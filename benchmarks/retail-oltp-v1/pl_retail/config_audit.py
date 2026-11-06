"""Effective-configuration and initial-data audit.

The contract fixes the deployment profile.  This module records what the
service actually reports, compares it with the contract, and checks the loaded
schema and the independent oracle.  A value that cannot be observed is a gap,
never an assumed pass.
"""
from __future__ import annotations

import json
from typing import Any

from . import ddl
from .business import Session
from .client import HranaClient, Target
from .contract import Contract
from .errors import ContractError
from .fixture import FixtureConfig, ManagementClient
from .generate import Dataset

CAPABILITIES_PATH = "/v1/platform/capabilities"
METRICS_PATH = "/v1/metrics"


def load_descriptor(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        document = json.load(handle)
    required = (
        "endpoint",
        "durability",
        "object_pack",
        "hrana_experimental",
        "tls_verify",
        "quotas",
        "limits",
    )
    missing = [key for key in required if key not in document]
    if missing:
        raise ContractError("platform descriptor is missing %s" % ", ".join(missing))
    return document


def _compare(
    values: dict[str, Any], expected: dict[str, Any], label: str, violations: list[str]
) -> None:
    for key, value in expected.items():
        actual = values.get(key)
        if actual != value:
            violations.append("%s.%s: expected %r, observed %r" % (label, key, value, actual))


def audit_environment(
    target: Target,
    fixture_config: FixtureConfig,
    descriptor: dict[str, Any],
    contract: Contract,
) -> dict[str, Any]:
    violations: list[str] = []
    gaps: list[str] = []
    values: dict[str, Any] = {}
    environment = contract.environment()

    endpoint = str(descriptor.get("endpoint") or "").rstrip("/")
    values["endpoint"] = endpoint
    if endpoint != fixture_config.endpoint.rstrip("/"):
        violations.append("descriptor endpoint does not match the fixture endpoint")
    if environment.get("tls_verify") is True:
        if not endpoint.startswith("https://"):
            violations.append("the fixed profile requires https")
        if not fixture_config.ca_file:
            violations.append("TLS verification is on but no CA file was supplied")

    _compare(
        {
            "durability": descriptor.get("durability"),
            "object_pack": descriptor.get("object_pack"),
            "hrana_experimental": descriptor.get("hrana_experimental"),
            "tls_verify": descriptor.get("tls_verify"),
        },
        {
            "durability": environment.get("durability"),
            "object_pack": environment.get("object_pack"),
            "hrana_experimental": environment.get("hrana_experimental"),
            "tls_verify": environment.get("tls_verify"),
        },
        "environment",
        violations,
    )
    _compare(
        descriptor.get("quotas") or {},
        environment.get("project_quotas") or {},
        "quotas",
        violations,
    )
    _compare(
        descriptor.get("limits") or {},
        environment.get("limits") or {},
        "limits",
        violations,
    )

    management = ManagementClient(fixture_config)
    try:
        status, capabilities = management.get(CAPABILITIES_PATH)
        values["capabilities_status"] = status
        values["capabilities"] = capabilities
        if status != 200:
            violations.append("capabilities returned HTTP %d" % status)
        else:
            if environment.get("hrana_experimental") == "on" and not capabilities.get(
                "hrana_http"
            ):
                violations.append("capabilities reports the experimental Hrana entry off")
            if not capabilities.get("sql_enabled", True):
                violations.append("capabilities reports SQL disabled")
    except Exception as error:  # noqa: BLE001 - an unreadable capability is a gap
        gaps.append("capabilities: %s: %s" % (type(error).__name__, error))

    try:
        status, metrics = management.get(METRICS_PATH)
        values["metrics_status"] = status
        audit = metrics.get("audit") or {}
        values["durability_source"] = audit.get("durability_source")
        values["session_seats"] = metrics.get("session_seats")
        values["active_permits"] = metrics.get("active_permits")
        values["rejection_reason_values"] = metrics.get("rejection_reason_values")
        if status != 200:
            violations.append("metrics returned HTTP %d" % status)
        elif environment.get("durability") == "legacy_completion":
            source = audit.get("durability_source")
            if source != "audit_file_v1":
                violations.append(
                    "metrics durability_source is %r, the profile fixes audit_file_v1"
                    % (source,)
                )
    except Exception as error:  # noqa: BLE001 - an unreadable metric is a gap
        gaps.append("metrics: %s: %s" % (type(error).__name__, error))

    for key in ("version", "commit", "image_digest", "binary_sha256", "server_pid", "cgroup_path"):
        if key in descriptor:
            values[key] = descriptor[key]
        else:
            gaps.append("descriptor.%s is not recorded" % key)

    return {"violations": violations, "gaps": gaps, "values": values}


def audit_schema(session: Session) -> dict[str, Any]:
    reply = session.execute(
        "SELECT type, name, sql FROM sqlite_schema WHERE type IN ('table','index') "
        "ORDER BY type, name",
        want_rows=True,
    )
    if reply.transport_error or reply.error or reply.first_error_step() is not None:
        raise ContractError("schema query failed: %s" % (reply.evidence(),))
    rows = reply.results[0].rows or []
    schema_rows = [
        {"type": row[0], "name": row[1], "sql": row[2]} for row in rows
    ]
    violations = ddl.schema_audit(schema_rows)
    return {"rows": len(schema_rows), "violations": violations, "schema": schema_rows}


def expected_oracle(dataset: Dataset) -> dict[str, int]:
    totals = dataset.totals()
    shape = dataset.shape
    text_bytes = (
        shape.products * 256
        + shape.customers * 128
        + shape.orders * 128
        + shape.order_lines * 64
        + shape.orders * 64
    )
    return {
        "warehouses": shape.warehouses,
        "products": shape.products,
        "stock": shape.stock_keys,
        "customers": shape.customers,
        "orders": shape.orders,
        "order_lines": shape.order_lines,
        "payments": shape.orders,
        "warehouse_cash_cents": totals["warehouse_cash_cents"],
        "stock_initial_units": shape.stock_keys * 1_000_000,
        "stock_available_units": shape.stock_keys * 1_000_000 - totals["sold_units"],
        "stock_sold_units": totals["sold_units"],
        "customer_initial_cents": shape.customers * 1_000_000_000,
        "customer_balance_cents": totals["customer_balance_cents"],
        "payments_cents": totals["payments_cents"],
        "text_bytes": text_bytes,
    }


ORACLE_SQL = (
    "SELECT (SELECT COUNT(*) FROM warehouses),"
    " (SELECT COUNT(*) FROM products),"
    " (SELECT COUNT(*) FROM stock),"
    " (SELECT COUNT(*) FROM customers),"
    " (SELECT COUNT(*) FROM orders),"
    " (SELECT COUNT(*) FROM order_lines),"
    " (SELECT COUNT(*) FROM payments),"
    " (SELECT COALESCE(SUM(cash_cents),0) FROM warehouses),"
    " (SELECT COALESCE(SUM(initial_qty),0) FROM stock),"
    " (SELECT COALESCE(SUM(available_qty),0) FROM stock),"
    " (SELECT COALESCE(SUM(initial_qty - available_qty),0) FROM stock),"
    " (SELECT COALESCE(SUM(initial_balance_cents),0) FROM customers),"
    " (SELECT COALESCE(SUM(balance_cents),0) FROM customers),"
    " (SELECT COALESCE(SUM(amount_cents),0) FROM payments),"
    " (SELECT COALESCE(SUM(LENGTH(CAST(description AS BLOB))),0) FROM products),"
    " (SELECT COALESCE(SUM(LENGTH(CAST(note AS BLOB))),0) FROM customers),"
    " (SELECT COALESCE(SUM(LENGTH(CAST(note AS BLOB))),0) FROM orders),"
    " (SELECT COALESCE(SUM(LENGTH(CAST(note AS BLOB))),0) FROM order_lines),"
    " (SELECT COALESCE(SUM(LENGTH(CAST(note AS BLOB))),0) FROM payments)"
)

ORACLE_KEYS = (
    "warehouses",
    "products",
    "stock",
    "customers",
    "orders",
    "order_lines",
    "payments",
    "warehouse_cash_cents",
    "stock_initial_units",
    "stock_available_units",
    "stock_sold_units",
    "customer_initial_cents",
    "customer_balance_cents",
    "payments_cents",
    "product_description_bytes",
    "customer_note_bytes",
    "order_note_bytes",
    "order_line_note_bytes",
    "payment_note_bytes",
)


def audit_oracle(session: Session, dataset: Dataset) -> dict[str, Any]:
    reply = session.execute(ORACLE_SQL, want_rows=True)
    if reply.transport_error or reply.error or reply.first_error_step() is not None:
        raise ContractError("oracle query failed: %s" % (reply.evidence(),))
    rows = reply.results[0].rows or []
    if not rows:
        raise ContractError("oracle query returned no row")
    actual = {key: int(value) for key, value in zip(ORACLE_KEYS, rows[0])}
    expected = expected_oracle(dataset)
    expected["product_description_bytes"] = dataset.shape.products * 256
    expected["customer_note_bytes"] = dataset.shape.customers * 128
    expected["order_note_bytes"] = dataset.shape.orders * 128
    expected["order_line_note_bytes"] = dataset.shape.order_lines * 64
    expected["payment_note_bytes"] = dataset.shape.orders * 64
    violations = [
        "%s: expected %d, observed %d" % (key, expected[key], actual.get(key, -1))
        for key in ORACLE_KEYS
        if expected.get(key) != actual.get(key)
    ]
    return {"actual": actual, "expected": expected, "violations": violations}


def audit_config(
    target: Target,
    fixture_config: FixtureConfig,
    descriptor: dict[str, Any],
    contract: Contract,
    dataset: Dataset,
    *,
    phase: str,
) -> dict[str, Any]:
    environment = audit_environment(target, fixture_config, descriptor, contract)
    client = HranaClient(target, timeout=60.0)
    session = Session(client, budget_seconds=60.0)
    try:
        session.open()
        schema = audit_schema(session)
    finally:
        session.close()
        client.close_quietly()
    report: dict[str, Any] = {
        "phase": phase,
        "ok": not environment["violations"] and not schema["violations"],
        "environment": environment,
        "schema": {"rows": schema["rows"], "violations": schema["violations"]},
        "gaps": list(environment["gaps"]),
        "violations": list(environment["violations"]) + list(schema["violations"]),
    }
    if phase == "before":
        client = HranaClient(target, timeout=60.0)
        session = Session(client, budget_seconds=60.0)
        try:
            session.open()
            report["oracle"] = audit_oracle(session, dataset)
            report["violations"].extend(report["oracle"]["violations"])
            report["ok"] = not report["violations"]
        finally:
            session.close()
            client.close_quietly()
    return report
