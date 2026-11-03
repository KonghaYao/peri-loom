"""Preflight: positive traffic, one committed checkout, four known negatives.

The four negatives must be rejected as known-not-executed/known-failed and must
leave the database byte-for-byte unchanged apart from the preflight checkout.
Any unknown reply aborts the fixture instead of being retried.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .business import Session, run_business
from .client import Reply, classify_transport
from .generate import Dataset, fixed_text
from .plan import PlannedOp


@dataclass
class PreflightReport:
    ok: bool = False
    checks: list[dict[str, Any]] = field(default_factory=list)
    violations: list[str] = field(default_factory=list)
    unknown: bool = False
    ledger: dict[str, Any] | None = None
    rows_after_import: int = 0
    rows_after_preflight: int = 0
    counts_before: dict[str, int] = field(default_factory=dict)
    counts_after: dict[str, int] = field(default_factory=dict)
    sums_before: dict[str, int] = field(default_factory=dict)
    sums_after: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "unknown": self.unknown,
            "checks": self.checks,
            "violations": self.violations,
            "ledger": self.ledger,
            "rows_after_import": self.rows_after_import,
            "rows_after_preflight": self.rows_after_preflight,
            "counts_before": self.counts_before,
            "counts_after": self.counts_after,
            "sums_before": self.sums_before,
            "sums_after": self.sums_after,
        }


COUNTS_SQL = (
    "SELECT (SELECT COUNT(*) FROM warehouses),"
    " (SELECT COUNT(*) FROM products),"
    " (SELECT COUNT(*) FROM stock),"
    " (SELECT COUNT(*) FROM customers),"
    " (SELECT COUNT(*) FROM orders),"
    " (SELECT COUNT(*) FROM order_lines),"
    " (SELECT COUNT(*) FROM payments)"
)
SUMS_SQL = (
    "SELECT (SELECT COALESCE(SUM(amount_cents),0) FROM payments),"
    " (SELECT COALESCE(SUM(initial_qty - available_qty),0) FROM stock),"
    " (SELECT COALESCE(SUM(balance_cents),0) FROM customers),"
    " (SELECT COALESCE(SUM(cash_cents),0) FROM warehouses)"
)
COUNT_KEYS = ("warehouses", "products", "stock", "customers", "orders", "order_lines", "payments")
SUM_KEYS = ("payments_cents", "sold_units", "customer_balance_cents", "warehouse_cash_cents")


def _row(reply: Reply, context: str) -> list[Any]:
    if reply.transport_error or reply.error or reply.first_error_step() is not None:
        raise RuntimeError("%s failed: %s" % (context, reply.evidence()))
    steps = reply.results
    if not steps or not steps[0].rows:
        raise RuntimeError("%s returned no row" % context)
    return list(steps[0].rows[0])


def snapshot(session: Session) -> dict[str, Any]:
    counts = dict(zip(COUNT_KEYS, (int(value) for value in _row(session.execute(COUNTS_SQL, want_rows=True), "counts"))))
    sums = dict(zip(SUM_KEYS, (int(value) for value in _row(session.execute(SUMS_SQL, want_rows=True), "sums"))))
    target = {
        "stock_1_1": _row(
            session.execute(
                "SELECT initial_qty, available_qty FROM stock WHERE warehouse_id=1 AND product_id=1",
                want_rows=True,
            ),
            "stock 1/1",
        ),
        "customer_1": _row(
            session.execute("SELECT balance_cents FROM customers WHERE id=1", want_rows=True),
            "customer 1",
        ),
        "warehouse_1": _row(
            session.execute("SELECT cash_cents FROM warehouses WHERE id=1", want_rows=True),
            "warehouse 1",
        ),
        "payment_count_1": _row(
            session.execute("SELECT COUNT(*) FROM payments WHERE order_id=1", want_rows=True),
            "payment of order 1",
        ),
        "order_1": _row(
            session.execute(
                "SELECT request_key, total_cents FROM orders WHERE id=1", want_rows=True
            ),
            "order 1",
        ),
    }
    return {"counts": counts, "sums": sums, "target": target}


def _step_error(reply: Reply) -> tuple[str, str]:
    verdict, reason = classify_transport(reply)
    return verdict, reason


def _begin(session: Session) -> None:
    reply = session.execute("BEGIN")
    if _step_error(reply)[0] != "ok":
        raise RuntimeError("preflight BEGIN failed: %s" % (reply.evidence(),))


def _rollback(session: Session) -> None:
    reply = session.execute("ROLLBACK")
    if _step_error(reply)[0] != "ok":
        raise RuntimeError("preflight ROLLBACK failed: %s" % (reply.evidence(),))


def _negative_duplicate_payment(session: Session, report: PreflightReport) -> None:
    _begin(session)
    reply = session.execute(
        "INSERT INTO payments (order_id, customer_id, amount_cents, note) "
        "SELECT order_id, customer_id, amount_cents, note FROM payments WHERE id = 1"
    )
    verdict, reason = _step_error(reply)
    duplicate = {
        "name": "duplicate_payment",
        "verdict": verdict,
        "reason": reason,
        "message": (reply.message or "")[:200],
        "known": verdict == "known_failure" and "unique constraint" in (reply.message or "").lower(),
    }
    report.checks.append(duplicate)
    if verdict == "unknown":
        report.unknown = True
        report.violations.append("duplicate payment was not a proven rejection: %s" % reason)
    elif not duplicate["known"]:
        report.violations.append(
            "duplicate payment classification drifted: verdict=%s message=%s"
            % (verdict, duplicate["message"])
        )
    _rollback(session)


def _negative_insufficient_stock(session: Session, report: PreflightReport) -> None:
    _begin(session)
    reply = session.execute(
        "UPDATE stock SET available_qty = available_qty - 1000000000 "
        "WHERE warehouse_id = 1 AND product_id = 1 AND available_qty >= 1000000000"
    )
    verdict, reason = _step_error(reply)
    affected = reply.results[0].affected_row_count if reply.results else None
    check = {
        "name": "insufficient_stock",
        "verdict": verdict,
        "reason": reason,
        "affected_row_count": affected,
        "known": verdict == "ok" and affected == 0,
    }
    report.checks.append(check)
    if verdict == "unknown":
        report.unknown = True
        report.violations.append("insufficient stock was not a proven rejection: %s" % reason)
    elif not check["known"]:
        report.violations.append(
            "insufficient stock did not match zero rows (verdict=%s affected=%r)"
            % (verdict, affected)
        )
    _rollback(session)


def _negative_duplicate_request_key(session: Session, report: PreflightReport) -> None:
    _begin(session)
    reply = session.execute(
        "INSERT INTO orders (customer_id, warehouse_id, request_key, total_cents, note) "
        "SELECT customer_id, warehouse_id, request_key, total_cents, note FROM orders WHERE id = 1"
    )
    verdict, reason = _step_error(reply)
    duplicate = {
        "name": "duplicate_request_key",
        "verdict": verdict,
        "reason": reason,
        "message": (reply.message or "")[:200],
        "known": verdict == "known_failure" and "unique constraint" in (reply.message or "").lower(),
    }
    report.checks.append(duplicate)
    if verdict == "unknown":
        report.unknown = True
        report.violations.append("duplicate request_key was not a proven rejection: %s" % reason)
    elif not duplicate["known"]:
        report.violations.append(
            "duplicate request_key classification drifted: verdict=%s message=%s"
            % (verdict, duplicate["message"])
        )
    _rollback(session)


def _negative_rollback(session: Session, report: PreflightReport, round_id: str) -> None:
    key = "preflight-rollback-%s" % round_id
    _begin(session)
    writes = [
        "UPDATE stock SET available_qty = available_qty - 1 WHERE warehouse_id=1 AND product_id=1",
        "UPDATE customers SET balance_cents = balance_cents - 1 WHERE id=1",
        "UPDATE warehouses SET cash_cents = cash_cents + 1 WHERE id=1",
        "INSERT INTO orders (customer_id, warehouse_id, request_key, total_cents, note) "
        "VALUES (1, 1, '%s', 1, '%s')" % (key, fixed_text("R" + key, 128)),
    ]
    for statement in writes:
        verdict, reason = _step_error(session.execute(statement))
        if verdict != "ok":
            report.unknown = verdict == "unknown"
            report.violations.append("rollback case write failed: %s %s" % (verdict, reason))
            _rollback(session)
            return
    _rollback(session)
    row = session.execute(
        "SELECT COUNT(*) FROM orders WHERE request_key = ?", args=(key,), want_rows=True
    )
    present = int(_row(row, "rollback residue")[0])
    check = {"name": "rollback_discards_all_writes", "request_key": key, "residue_rows": present}
    report.checks.append(check)
    if present != 0:
        report.violations.append("ROLLBACK left the preflight order behind")


def _positive_reads(session: Session, report: PreflightReport, round_id: str) -> None:
    cases = [
        PlannedOp(0, "product_stock", {"warehouse_id": 1, "product_id": 1}),
        PlannedOp(1, "order_detail", {"order_id": 1}),
        PlannedOp(2, "customer_orders", {"customer_id": 1}),
        PlannedOp(3, "stock_report", {"warehouse_id": 1, "first_product_id": 1}),
    ]
    for op in cases:
        outcome = run_business(session, op, "preflight-read-%s-%d" % (round_id, op.index), round_id)
        report.checks.append(
            {
                "name": "read:%s" % op.kind,
                "terminal": outcome.terminal,
                "success": outcome.success,
                "http_calls": outcome.trace.calls,
            }
        )
        if not outcome.success or outcome.trace.calls != 3:
            report.violations.append(
                "positive read %s failed: %s (%d calls)"
                % (op.kind, outcome.terminal, outcome.trace.calls)
            )
        if outcome.stop_fixture or outcome.terminal not in {"read_ok"}:
            report.unknown = report.unknown or outcome.terminal in {
                "service_unknown",
                "transport_unknown",
                "cleanup_unconfirmed",
            }


def run_preflight(target, dataset: Dataset, round_id: str) -> PreflightReport:
    from .client import HranaClient

    report = PreflightReport()
    report.rows_after_import = (
        dataset.shape.warehouses
        + dataset.shape.products
        + dataset.shape.stock_keys
        + dataset.shape.customers
        + dataset.shape.orders
        + dataset.shape.order_lines
        + dataset.shape.orders
    )
    client = HranaClient(target, timeout=60.0)
    session = Session(client, budget_seconds=60.0)
    try:
        session.open()
        before = snapshot(session)
        report.counts_before = before["counts"]
        report.sums_before = before["sums"]

        _positive_reads(session, report, round_id)
        _negative_duplicate_payment(session, report)
        _negative_insufficient_stock(session, report)
        _negative_duplicate_request_key(session, report)
        _negative_rollback(session, report, round_id)

        checkout = PlannedOp(
            index=0,
            kind="checkout",
            params={
                "warehouse_id": 1,
                "customer_id": 1,
                "items": ((1, 1), (2, 2), (3, 3), (4, 1), (5, 2)),
                "hotspot": True,
            },
        )
        request_key = "preflight-checkout-%s" % round_id
        outcome = run_business(session, checkout, request_key, round_id)
        report.checks.append(
            {
                "name": "checkout:preflight",
                "terminal": outcome.terminal,
                "success": outcome.success,
                "http_calls": outcome.trace.calls,
            }
        )
        if not outcome.success or outcome.terminal != "committed_ack" or outcome.ledger is None:
            report.violations.append(
                "preflight checkout did not commit: %s (%d calls)"
                % (outcome.terminal, outcome.trace.calls)
            )
            if outcome.terminal in {"service_unknown", "transport_unknown", "cleanup_unconfirmed"}:
                report.unknown = True
        else:
            report.ledger = dict(outcome.ledger)
            report.ledger["phase"] = "preflight"
            if outcome.trace.calls != 13:
                report.violations.append(
                    "preflight checkout used %d HTTP calls, expected 13" % outcome.trace.calls
                )
            row = session.execute(
                "SELECT customer_id, warehouse_id, total_cents FROM orders WHERE request_key = ?",
                args=(request_key,),
                want_rows=True,
            )
            actual = _row(row, "preflight checkout row")
            expected = [1, 1, int(outcome.ledger["total_cents"])]
            if actual != expected:
                report.violations.append(
                    "preflight checkout row mismatch: %r != %r" % (actual, expected)
                )

        after = snapshot(session)
        report.counts_after = after["counts"]
        report.sums_after = after["sums"]

        expected_counts = dict(before["counts"])
        if report.ledger is not None:
            expected_counts["orders"] += 1
            expected_counts["order_lines"] += 5
            expected_counts["payments"] += 1
        if after["counts"] != expected_counts:
            report.violations.append(
                "preflight row-count delta is wrong: %r != %r" % (after["counts"], expected_counts)
            )
        expected_sums = dict(before["sums"])
        if report.ledger is not None:
            total = int(report.ledger["total_cents"])
            units = sum(qty for _, qty in report.ledger["items"])
            expected_sums["payments_cents"] += total
            expected_sums["sold_units"] += units
            expected_sums["customer_balance_cents"] -= total
            expected_sums["warehouse_cash_cents"] += total
        if after["sums"] != expected_sums:
            report.violations.append(
                "preflight sum delta is wrong: %r != %r" % (after["sums"], expected_sums)
            )
        if after["target"]["order_1"] != before["target"]["order_1"]:
            report.violations.append("historical order 1 changed during preflight")
        if (
            after["target"]["stock_1_1"] == before["target"]["stock_1_1"]
            and report.ledger is not None
            and any(pid == 1 for pid, _ in report.ledger["items"])
        ):
            report.violations.append("preflight stock 1/1 did not move")
        if after["target"]["payment_count_1"] != before["target"]["payment_count_1"]:
            report.violations.append("payment uniqueness for order 1 changed")

        report.rows_after_preflight = sum(report.counts_after.values())
        report.ok = not report.violations and not report.unknown
    finally:
        session.close()
        client.close_quietly()
    return report
