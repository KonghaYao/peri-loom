"""The five fixed businesses and their failure handling.

A business attempt is a closed group of HTTP requests.  The attempt's latency
covers every request, response decode and business check up to the transaction
terminal.  SQL is never retried and a failed step never continues to COMMIT.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable

from .call_profiles import CallProfile, resolve_profile
from .client import HranaClient, Reply, classify_transport
from .errors import Terminal
from .generate import fixed_text
from .plan import PlannedOp

BEGIN = "BEGIN"
BEGIN_IMMEDIATE = "BEGIN IMMEDIATE"
COMMIT = "COMMIT"
ROLLBACK = "ROLLBACK"

PRODUCT_STOCK_SQL = """
SELECT p.id, p.price_cents, length(p.description), s.initial_qty, s.available_qty
FROM products p JOIN stock s ON s.product_id = p.id
WHERE s.warehouse_id = ? AND p.id = ?
""".strip()

ORDER_DETAIL_SQL = """
SELECT o.id, o.customer_id, o.total_cents, o.request_key,
       l.id, l.product_id, l.qty, l.unit_price_cents,
       pay.amount_cents, pay.customer_id
FROM orders o
JOIN order_lines l ON l.order_id = o.id
JOIN payments pay ON pay.order_id = o.id
WHERE o.id = ?
ORDER BY l.id
""".strip()

CUSTOMER_ORDERS_SQL = """
SELECT id, request_key, total_cents, warehouse_id
FROM orders WHERE customer_id = ?
ORDER BY id DESC LIMIT 10
""".strip()

STOCK_REPORT_SQL = """
SELECT COUNT(*), COALESCE(SUM(available_qty), 0), MIN(available_qty), MAX(available_qty)
FROM stock
WHERE warehouse_id = ? AND product_id >= ? AND product_id < ?
""".strip()

PRICE_SQL = "SELECT id, price_cents FROM products WHERE id IN (?, ?, ?, ?, ?) ORDER BY id"

STOCK_UPDATE_SQL = (
    "UPDATE stock SET available_qty = available_qty - ? "
    "WHERE warehouse_id = ? AND product_id = ? AND available_qty >= ?"
)
WALLET_UPDATE_SQL = (
    "UPDATE customers SET balance_cents = balance_cents - ? "
    "WHERE id = ? AND balance_cents >= ?"
)
WAREHOUSE_UPDATE_SQL = "UPDATE warehouses SET cash_cents = cash_cents + ? WHERE id = ?"
ORDER_INSERT_SQL = (
    "INSERT INTO orders (customer_id, warehouse_id, request_key, total_cents, note) "
    "VALUES (?, ?, ?, ?, ?)"
)
ORDER_LINE_INSERT_SQL = (
    "INSERT INTO order_lines (order_id, product_id, qty, unit_price_cents, note) "
    "VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?), (?, ?, ?, ?, ?), (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)"
)
PAYMENT_INSERT_SQL = (
    "INSERT INTO payments (order_id, customer_id, amount_cents, note) VALUES (?, ?, ?, ?)"
)

#: The ``merged`` stock profile collapses the five checkout decrements into one
#: statement.  The parameter order is fixed and must stay positional: this
#: client only sends positional arguments, so ``:p1`` would arrive as NULL.
#: Five (product_id, qty) pairs feed the decrement CASE, then the warehouse id,
#: then the five product ids for IN, then the same five pairs feed the guard
#: CASE.  The guard is evaluated per row, so a short product only skips its own
#: row: the other qualified rows are still decremented and the caller must roll
#: the transaction back when fewer than five rows changed.
MERGED_STOCK_UPDATE_SQL = (
    "UPDATE stock SET available_qty = available_qty - CASE product_id "
    "WHEN ? THEN ? WHEN ? THEN ? WHEN ? THEN ? WHEN ? THEN ? WHEN ? THEN ? END "
    "WHERE warehouse_id = ? AND product_id IN (?, ?, ?, ?, ?) "
    "AND available_qty >= CASE product_id "
    "WHEN ? THEN ? WHEN ? THEN ? WHEN ? THEN ? WHEN ? THEN ? WHEN ? THEN ? END"
)

DEFAULT_BUDGET_SECONDS = 5.0


@dataclass
class AttemptTrace:
    request_ids: list[str] = field(default_factory=list)
    calls: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    started_ns: int = 0
    finished_ns: int = 0
    phases: list[dict[str, Any]] = field(default_factory=list)

    def add(self, label: str, reply: Reply) -> None:
        self.calls += 1
        self.request_ids.append(reply.request_id)
        self.bytes_sent += reply.request_bytes
        self.bytes_received += reply.response_bytes
        self.phases.append(
            {
                "label": label,
                "status": reply.status,
                "code": reply.code,
                "latency_ms": round(reply.latency_ms, 6),
                "request_id": reply.request_id,
            }
        )


@dataclass
class BusinessOutcome:
    terminal: str
    detail: dict[str, Any]
    trace: AttemptTrace
    ledger: dict[str, Any] | None = None
    stop_fixture: bool = False
    isolation_required: bool = False
    unexpected: bool = False
    # True only when the peer proved the statement never started.  Policy B
    # (one extra independent close) is confined to this state.
    execution_proven_not_started: bool = False

    @property
    def success(self) -> bool:
        return self.terminal in {Terminal.READ_OK.value, Terminal.COMMITTED_ACK.value}


class Session:
    """One worker's independent session and reused HTTP connection."""

    def __init__(
        self,
        client: HranaClient,
        budget_seconds: float = DEFAULT_BUDGET_SECONDS,
        profile: CallProfile | None = None,
    ):
        self.client = client
        self.budget_seconds = budget_seconds
        self.profile = resolve_profile(None) if profile is None else profile
        self.baton: str | None = None
        # Tracks an explicitly opened transaction.  An autocommit read is only
        # legal while this is False; an unknown COMMIT/ROLLBACK keeps it True
        # because the server state is unproven and the fixture stops anyway.
        self.in_transaction = False

    def open(self) -> None:
        reply = self.client.open()
        if reply.baton:
            self.baton = reply.baton
        verdict, reason = classify_transport(reply)
        if verdict != "ok" or not self.baton:
            raise RuntimeError("session open failed: %s %s" % (verdict, reason))

    def execute(self, sql: str, args: tuple | list = (), want_rows: bool = False) -> Reply:
        return self.client.execute(sql, args=args, want_rows=want_rows, baton=self.baton)

    def close(self) -> tuple[bool, Reply | None]:
        if not self.baton:
            return True, None
        reply = self.client.close(self.baton)
        verdict, _ = classify_transport(reply)
        self.baton = None
        return verdict == "ok", reply

    def retire(self) -> dict[str, Any]:
        """One extra, independent Hrana close (policy B).

        Used only after a *known not executed* rejection.  The close travels
        on a fresh client with its own connection, so a broken/stale baton on
        the worker's main connection cannot mask the retirement, and the call
        is bounded by ``close_budget_seconds`` from the contract.
        """
        baton = self.baton
        if not baton:
            return {"attempted": True, "ok": True, "verdict": "no_baton", "reason": ""}
        started = time.monotonic_ns()
        try:
            fresh = HranaClient(self.client.target)
            try:
                reply = fresh.close(baton)
            finally:
                fresh.close_quietly()
            verdict, reason = classify_transport(reply)
            return {
                "attempted": True,
                "ok": verdict == "ok",
                "verdict": verdict,
                "reason": reason,
                "status": reply.status,
                "elapsed_ms": round((time.monotonic_ns() - started) / 1_000_000, 3),
            }
        except FatalTransportError as error:
            return {
                "attempted": True,
                "ok": False,
                "verdict": "fatal",
                "reason": str(error),
                "elapsed_ms": round((time.monotonic_ns() - started) / 1_000_000, 3),
            }
        finally:
            self.baton = None


#: Terminals for which the worker session may still hold seats although the
#: statement provably did not execute.  Only these may be retired by policy B;
#: ``service_unknown`` / ``transport_unknown`` keep the original isolation.
def should_retire_session(policy: str, terminal: str, not_executed: bool) -> bool:
    """Policy B adds exactly one independent close, and only for known states.

    ``known_failure/rolled_back`` covers both "the peer proved it did not
    start" and "it ran and we rolled it back".  Only the former may be
    retired; a true unknown is never touched by either policy.
    """
    return (
        policy == "B"
        and not_executed
        and terminal == Terminal.KNOWN_FAILURE_ROLLED_BACK.value
    )


def _verdict(reply: Reply) -> tuple[str, str]:
    verdict, reason = classify_transport(reply)
    if verdict == "unknown" and reply.first_error_step() is not None:
        message = reply.first_error_step().error_message.lower()
        if "database is locked" in message or "database table is locked" in message:
            return "known_failure", "locked"
    return verdict, reason


def _known_not_executed(reply: Reply) -> bool:
    return _verdict(reply)[0] == "known_not_executed"


def run_business(
    session: Session,
    op: PlannedOp,
    request_key: str,
    round_id: str,
) -> BusinessOutcome:
    started = time.monotonic_ns()
    outcome = _run_business(session, op, request_key, round_id)
    outcome.trace.started_ns = started
    outcome.trace.finished_ns = time.monotonic_ns()
    return outcome


def _run_business(
    session: Session,
    op: PlannedOp,
    request_key: str,
    round_id: str,
) -> BusinessOutcome:
    trace = AttemptTrace()
    if op.kind == "product_stock":
        return _read(session, trace, op, "read_ok", PRODUCT_STOCK_SQL,
                     (op.params["warehouse_id"], op.params["product_id"]),
                     _check_product_stock)
    if op.kind == "order_detail":
        return _read(session, trace, op, "read_ok", ORDER_DETAIL_SQL,
                     (op.params["order_id"],), _check_order_detail)
    if op.kind == "customer_orders":
        return _read(session, trace, op, "read_ok", CUSTOMER_ORDERS_SQL,
                     (op.params["customer_id"],), _check_customer_orders)
    if op.kind == "stock_report":
        first = op.params["first_product_id"]
        return _read(session, trace, op, "read_ok", STOCK_REPORT_SQL,
                     (op.params["warehouse_id"], first, first + 100),
                     _check_stock_report)
    if op.kind == "checkout":
        return _checkout(session, trace, op, request_key, round_id)
    raise ValueError("unknown business kind %r" % op.kind)


# -- reads -----------------------------------------------------------------


def _read(
    session: Session,
    trace: AttemptTrace,
    op: PlannedOp,
    success_terminal: str,
    sql: str,
    args: tuple,
    check: Callable[[list[list[Any]], PlannedOp], str | None],
) -> BusinessOutcome:
    if session.profile.read_mode == "autocommit" and not session.in_transaction:
        return _read_autocommit(session, trace, op, success_terminal, sql, args, check)

    reply = session.execute(BEGIN)
    trace.add("begin", reply)
    verdict, reason = _verdict(reply)
    if verdict == "unknown":
        return BusinessOutcome(
            terminal=Terminal.SERVICE_UNKNOWN.value,
            detail={"stage": "begin", "reason": reason, "kind": op.kind},
            trace=trace,
            stop_fixture=True,
            isolation_required=True,
        )
    if verdict != "ok":
        return BusinessOutcome(
            terminal=Terminal.KNOWN_FAILURE_ROLLED_BACK.value,
            detail={
                "stage": "begin",
                "reason": reason,
                "kind": op.kind,
                "rollback_sent": False,
                "rollback_reason": "transaction_not_started",
            },
            trace=trace,
        )
    session.in_transaction = True

    reply = session.execute(sql, args=args, want_rows=True)
    trace.add("query", reply)
    verdict, reason = _verdict(reply)
    if verdict == "unknown":
        return _abort_unknown(session, trace, "query", reason, op.kind)
    if verdict != "ok":
        return _abort_known(
            session,
            trace,
            "query",
            reason,
            op.kind,
            not_executed=verdict == "known_not_executed",
        )

    rows = reply.results[0].rows if reply.results else None
    if rows is None:
        return _abort_unknown(session, trace, "query", "missing_rows", op.kind)
    problem = check(rows, op)
    if problem is not None:
        rollback = _rollback(session, trace)
        return BusinessOutcome(
            terminal=Terminal.UNEXPECTED_ERROR.value,
            detail={
                "stage": "business_check",
                "reason": problem,
                "kind": op.kind,
                "rollback_confirmed": rollback,
            },
            trace=trace,
            stop_fixture=True,
            unexpected=True,
        )

    reply = session.execute(COMMIT)
    trace.add("commit", reply)
    verdict, reason = _verdict(reply)
    if verdict == "unknown":
        return BusinessOutcome(
            terminal=Terminal.SERVICE_UNKNOWN.value,
            detail={"stage": "commit", "reason": reason, "kind": op.kind},
            trace=trace,
            stop_fixture=True,
            isolation_required=True,
        )
    if verdict != "ok":
        rollback = _rollback(session, trace)
        return BusinessOutcome(
            terminal=(
                Terminal.KNOWN_FAILURE_ROLLED_BACK.value
                if rollback
                else Terminal.CLEANUP_UNCONFIRMED.value
            ),
            detail={
                "stage": "commit",
                "reason": reason,
                "kind": op.kind,
                "rollback_confirmed": rollback,
            },
            trace=trace,
            stop_fixture=not rollback,
            isolation_required=not rollback,
        )
    session.in_transaction = False
    return BusinessOutcome(
        terminal=success_terminal,
        detail={"kind": op.kind, "rows": len(rows)},
        trace=trace,
    )


def _read_autocommit(
    session: Session,
    trace: AttemptTrace,
    op: PlannedOp,
    success_terminal: str,
    sql: str,
    args: tuple,
    check: Callable[[list[list[Any]], PlannedOp], str | None],
) -> BusinessOutcome:
    """One SELECT, no BEGIN/COMMIT and therefore no ROLLBACK.

    The statement is its own transaction, so a failed or ambiguous reply has
    no transaction to clean up.  Sending ROLLBACK would be a second attempt
    against a session whose state is not proven; the outcome records that no
    rollback was sent instead of pretending the read is transactional.
    """
    reply = session.execute(sql, args=args, want_rows=True)
    trace.add("query", reply)
    verdict, reason = _verdict(reply)
    if verdict == "unknown":
        return BusinessOutcome(
            terminal=Terminal.SERVICE_UNKNOWN.value,
            detail={
                "stage": "query",
                "reason": reason,
                "kind": op.kind,
                "rollback_sent": False,
                "rollback_reason": "autocommit_no_transaction",
            },
            trace=trace,
            stop_fixture=True,
            isolation_required=True,
        )
    if verdict == "known_not_executed":
        return BusinessOutcome(
            terminal=Terminal.KNOWN_FAILURE_ROLLED_BACK.value,
            detail={
                "stage": "query",
                "reason": reason,
                "kind": op.kind,
                "rollback_sent": False,
                "rollback_reason": "autocommit_no_transaction",
            },
            trace=trace,
            execution_proven_not_started=True,
        )
    if verdict != "ok":
        return BusinessOutcome(
            terminal=Terminal.KNOWN_FAILURE_ROLLED_BACK.value,
            detail={
                "stage": "query",
                "reason": reason,
                "kind": op.kind,
                "rollback_sent": False,
                "rollback_reason": "autocommit_statement_failed",
            },
            trace=trace,
        )

    rows = reply.results[0].rows if reply.results else None
    if rows is None:
        return BusinessOutcome(
            terminal=Terminal.SERVICE_UNKNOWN.value,
            detail={
                "stage": "query",
                "reason": "missing_rows",
                "kind": op.kind,
                "rollback_sent": False,
                "rollback_reason": "autocommit_no_transaction",
            },
            trace=trace,
            stop_fixture=True,
            isolation_required=True,
        )
    problem = check(rows, op)
    if problem is not None:
        return BusinessOutcome(
            terminal=Terminal.UNEXPECTED_ERROR.value,
            detail={
                "stage": "business_check",
                "reason": problem,
                "kind": op.kind,
                "rollback_sent": False,
                "rollback_reason": "autocommit_no_transaction",
            },
            trace=trace,
            stop_fixture=True,
            unexpected=True,
        )
    return BusinessOutcome(
        terminal=success_terminal,
        detail={"kind": op.kind, "rows": len(rows)},
        trace=trace,
    )


def _check_product_stock(rows: list[list[Any]], op: PlannedOp) -> str | None:
    if len(rows) != 1:
        return "product_stock expected 1 row, got %d" % len(rows)
    row = rows[0]
    if int(row[0]) != int(op.params["product_id"]):
        return "product_stock returned a different product"
    if int(row[1]) <= 0 or int(row[2]) != 256:
        return "product_stock price or description length is wrong"
    initial, available = int(row[3]), int(row[4])
    if not 0 <= available <= initial:
        return "product_stock available quantity out of range"
    return None


def _check_order_detail(rows: list[list[Any]], op: PlannedOp) -> str | None:
    if len(rows) != 5:
        return "order_detail expected 5 lines, got %d" % len(rows)
    products = {int(row[5]) for row in rows}
    if len(products) != 5:
        return "order_detail products are not distinct"
    total = int(rows[0][2])
    payment = int(rows[0][8])
    computed = 0
    for row in rows:
        if int(row[0]) != int(op.params["order_id"]):
            return "order_detail returned a different order"
        if int(row[1]) != int(rows[0][1]) or int(row[9]) != int(rows[0][9]):
            return "order_detail customer mismatch"
        quantity = int(row[6])
        unit_price = int(row[7])
        if not 1 <= quantity <= 3 or unit_price <= 0:
            return "order_detail quantity or price out of range"
        computed += quantity * unit_price
    if computed != total or total != payment:
        return "order_detail total does not match lines and payment"
    return None


def _check_customer_orders(rows: list[list[Any]], op: PlannedOp) -> str | None:
    if len(rows) != 10:
        return "customer_orders expected 10 orders, got %d" % len(rows)
    previous = None
    keys = set()
    for row in rows:
        order_id = int(row[0])
        key = str(row[1])
        if previous is not None and order_id >= previous:
            return "customer_orders is not in descending id order"
        if key in keys:
            return "customer_orders repeated a request key"
        previous = order_id
        keys.add(key)
        if int(row[2]) <= 0:
            return "customer_orders total is not positive"
    return None


def _check_stock_report(rows: list[list[Any]], op: PlannedOp) -> str | None:
    if len(rows) != 1:
        return "stock_report expected 1 aggregate row"
    count, total, minimum, maximum = (int(value) for value in rows[0])
    if count != 100:
        return "stock_report expected 100 products, got %d" % count
    if minimum < 0 or maximum > 1_000_000 or total < 0:
        return "stock_report aggregate out of range"
    return None


# -- checkout --------------------------------------------------------------


def _checkout(
    session: Session,
    trace: AttemptTrace,
    op: PlannedOp,
    request_key: str,
    round_id: str,
) -> BusinessOutcome:
    items = list(op.params["items"])
    merged_items: tuple[tuple[int, int], ...] | None = None
    if session.profile.stock_mode == "merged":
        # Validate the plan before the first statement: a malformed batch must
        # not open a transaction or touch stock at all.
        merged_items, problem = _ordered_merged_items(items)
        if problem is not None:
            return _invalid_merged_items(trace, problem)
    if len(items) != 5:
        raise ValueError("checkout requires exactly five items")
    warehouse_id = int(op.params["warehouse_id"])
    customer_id = int(op.params["customer_id"])

    reply = session.execute(BEGIN_IMMEDIATE)
    trace.add("begin_immediate", reply)
    verdict, reason = _verdict(reply)
    if verdict == "unknown":
        return _unknown(trace, "begin_immediate", reason, "checkout")
    if verdict != "ok":
        return _known(
            trace,
            "begin_immediate",
            reason,
            "checkout",
            rollback=False,
            not_executed=verdict == "known_not_executed",
        )
    session.in_transaction = True

    reply = session.execute(PRICE_SQL, args=tuple(pid for pid, _ in items), want_rows=True)
    trace.add("prices", reply)
    verdict, reason = _verdict(reply)
    if verdict == "unknown":
        return _unknown(trace, "prices", reason, "checkout")
    if verdict != "ok":
        return _known_with_rollback(
            session,
            trace,
            "prices",
            reason,
            "checkout",
            not_executed=verdict == "known_not_executed",
        )
    rows = reply.results[0].rows if reply.results else None
    if rows is None or len(rows) != 5:
        return _abort_unexpected(session, trace, "prices", "price query did not return five rows")
    prices = {int(row[0]): int(row[1]) for row in rows}
    if set(prices) != {int(pid) for pid, _ in items}:
        return _abort_unexpected(session, trace, "prices", "price query returned other products")
    total = sum(prices[int(pid)] * int(qty) for pid, qty in items)
    if total <= 0:
        return _abort_unexpected(session, trace, "prices", "checkout total is not positive")

    if merged_items is not None:
        blocked = _stock_merged(session, trace, merged_items, warehouse_id)
        if blocked is not None:
            return blocked
    else:
        for index, (pid, qty) in enumerate(items):
            reply = session.execute(
                STOCK_UPDATE_SQL, args=(int(qty), warehouse_id, int(pid), int(qty))
            )
            trace.add("stock_%d" % index, reply)
            verdict, reason = _verdict(reply)
            if verdict == "unknown":
                return _unknown(trace, "stock_%d" % index, reason, "checkout")
            if verdict != "ok":
                return _known_with_rollback(
                    session,
                    trace,
                    "stock_%d" % index,
                    reason,
                    "checkout",
                    not_executed=verdict == "known_not_executed",
                )
            affected = _affected(reply)
            if affected is None:
                return _unknown(trace, "stock_%d" % index, "missing_affected_rows", "checkout")
            if affected != 1:
                return _known_with_rollback(
                    session, trace, "stock_%d" % index, "stock_unavailable", "checkout"
                )

    reply = session.execute(WALLET_UPDATE_SQL, args=(total, customer_id, total))
    trace.add("wallet", reply)
    verdict, reason = _verdict(reply)
    if verdict == "unknown":
        return _unknown(trace, "wallet", reason, "checkout")
    if verdict != "ok":
        return _known_with_rollback(
            session, trace, "wallet", reason, "checkout",
            not_executed=verdict == "known_not_executed",
        )
    if _affected(reply) != 1:
        return _known_with_rollback(session, trace, "wallet", "insufficient_balance", "checkout")

    reply = session.execute(WAREHOUSE_UPDATE_SQL, args=(total, warehouse_id))
    trace.add("warehouse", reply)
    verdict, reason = _verdict(reply)
    if verdict == "unknown":
        return _unknown(trace, "warehouse", reason, "checkout")
    if verdict != "ok":
        return _known_with_rollback(
            session, trace, "warehouse", reason, "checkout",
            not_executed=verdict == "known_not_executed",
        )
    if _affected(reply) != 1:
        return _known_with_rollback(session, trace, "warehouse", "warehouse_missing", "checkout")

    reply = session.execute(
        ORDER_INSERT_SQL,
        args=(customer_id, warehouse_id, request_key, total, fixed_text("N" + request_key, 128)),
    )
    trace.add("order_insert", reply)
    verdict, reason = _verdict(reply)
    if verdict == "unknown":
        return _unknown(trace, "order_insert", reason, "checkout")
    if verdict != "ok":
        return _known_with_rollback(
            session, trace, "order_insert", reason, "checkout",
            not_executed=verdict == "known_not_executed",
        )
    order_id = reply.results[0].last_insert_rowid if reply.results else None
    if order_id is None:
        return _unknown(trace, "order_insert", "missing_last_insert_rowid", "checkout")

    line_args: list[Any] = []
    for index, (pid, qty) in enumerate(items):
        line_args.extend(
            [
                order_id,
                int(pid),
                int(qty),
                prices[int(pid)],
                fixed_text("L%s-%d" % (request_key, index), 64),
            ]
        )
    reply = session.execute(ORDER_LINE_INSERT_SQL, args=tuple(line_args))
    trace.add("order_lines", reply)
    verdict, reason = _verdict(reply)
    if verdict == "unknown":
        return _unknown(trace, "order_lines", reason, "checkout")
    if verdict != "ok":
        return _known_with_rollback(
            session, trace, "order_lines", reason, "checkout",
            not_executed=verdict == "known_not_executed",
        )
    if _affected(reply) != 5:
        return _known_with_rollback(session, trace, "order_lines", "line_count_mismatch", "checkout")

    reply = session.execute(
        PAYMENT_INSERT_SQL,
        args=(order_id, customer_id, total, fixed_text("Y" + request_key, 64)),
    )
    trace.add("payment_insert", reply)
    verdict, reason = _verdict(reply)
    if verdict == "unknown":
        return _unknown(trace, "payment_insert", reason, "checkout")
    if verdict != "ok":
        return _known_with_rollback(
            session, trace, "payment_insert", reason, "checkout",
            not_executed=verdict == "known_not_executed",
        )
    if _affected(reply) != 1:
        return _known_with_rollback(session, trace, "payment_insert", "payment_not_inserted", "checkout")

    reply = session.execute(COMMIT)
    trace.add("commit", reply)
    verdict, reason = _verdict(reply)
    if verdict == "unknown":
        return _unknown(trace, "commit", reason, "checkout")
    if verdict != "ok":
        return _known_with_rollback(
            session, trace, "commit", reason, "checkout",
            not_executed=verdict == "known_not_executed",
        )
    session.in_transaction = False

    ledger = {
        "request_key": request_key,
        "round_id": round_id,
        "order_id": int(order_id),
        "customer_id": customer_id,
        "warehouse_id": warehouse_id,
        "items": [
            {"product_id": int(pid), "qty": int(qty), "unit_price_cents": prices[int(pid)]}
            for pid, qty in items
        ],
        "total_cents": total,
    }
    return BusinessOutcome(
        terminal=Terminal.COMMITTED_ACK.value,
        detail={"kind": "checkout", "order_id": int(order_id), "total_cents": total},
        trace=trace,
        ledger=ledger,
    )


def _ordered_merged_items(
    items: list[Any],
) -> tuple[tuple[tuple[int, int], ...] | None, str | None]:
    """Normalize checkout items for the merged update, or name the defect.

    The merged statement matches rows by product id, so duplicates would make
    one CASE branch unreachable and the affected-row count meaningless.  The
    plan draws five distinct products with quantities 1..3; anything else is a
    plan defect, not a database condition.
    """
    if len(items) != 5:
        return None, "checkout requires exactly five items"
    normalized: list[tuple[int, int]] = []
    for item in items:
        try:
            product_id, qty = item
        except (TypeError, ValueError):
            return None, "checkout item is not a product id and quantity pair"
        if qty is True or not isinstance(qty, int) or not 1 <= qty <= 3:
            return None, "checkout quantity is outside 1..3: %r" % (qty,)
        try:
            product_id_value = int(product_id)
        except (TypeError, ValueError):
            return None, "checkout product id is not an integer: %r" % (product_id,)
        normalized.append((product_id_value, qty))
    if len({product_id for product_id, _ in normalized}) != 5:
        return None, "checkout product ids are not distinct"
    return tuple(sorted(normalized, key=lambda pair: pair[0])), None


def _invalid_merged_items(trace: AttemptTrace, reason: str) -> BusinessOutcome:
    # Nothing was dispatched, so there is nothing to roll back; the fixture
    # stops because a later attempt would validate against unknown data.
    return BusinessOutcome(
        terminal=Terminal.UNEXPECTED_ERROR.value,
        detail={
            "stage": "validate",
            "reason": reason,
            "kind": "checkout",
            "rollback_sent": False,
            "rollback_reason": "invalid_plan",
        },
        trace=trace,
        stop_fixture=True,
        unexpected=True,
    )


def _stock_merged(
    session: Session,
    trace: AttemptTrace,
    ordered_items: tuple[tuple[int, int], ...],
    warehouse_id: int,
) -> BusinessOutcome | None:
    """Run the single merged decrement; ``None`` means all five rows moved.

    The guard CASE is per row, so a shortage changes only the other rows and
    returns fewer than five affected rows; that is the same
    ``stock_unavailable`` condition the per-item path detects, and the caller
    rolls the whole transaction back.
    """
    args: list[Any] = []
    for product_id, qty in ordered_items:
        args.extend((product_id, qty))
    args.append(warehouse_id)
    args.extend(product_id for product_id, _ in ordered_items)
    for product_id, qty in ordered_items:
        args.extend((product_id, qty))

    reply = session.execute(MERGED_STOCK_UPDATE_SQL, args=tuple(args))
    trace.add("stock_merged", reply)
    verdict, reason = _verdict(reply)
    if verdict == "unknown":
        return _unknown(trace, "stock_merged", reason, "checkout")
    if verdict != "ok":
        return _known_with_rollback(
            session,
            trace,
            "stock_merged",
            reason,
            "checkout",
            not_executed=verdict == "known_not_executed",
        )
    affected = _affected(reply)
    if affected is None:
        return _unknown(trace, "stock_merged", "missing_affected_rows", "checkout")
    if affected < 5:
        return _known_with_rollback(
            session, trace, "stock_merged", "stock_unavailable", "checkout"
        )
    if affected > 5:
        # One row per product is the only legal outcome; more means the
        # statement matched something the plan never asked for.
        confirmed = _rollback(session, trace)
        return BusinessOutcome(
            terminal=Terminal.UNEXPECTED_ERROR.value,
            detail={
                "stage": "stock_merged",
                "reason": "merged_stock_affected_more_than_five",
                "kind": "checkout",
                "rollback_sent": True,
                "rollback_confirmed": confirmed,
            },
            trace=trace,
            stop_fixture=True,
            isolation_required=not confirmed,
            unexpected=True,
        )
    return None


def _affected(reply: Reply) -> int | None:
    if not reply.results:
        return None
    return reply.results[0].affected_row_count


def _unknown(trace: AttemptTrace, stage: str, reason: str, kind: str) -> BusinessOutcome:
    terminal = (
        Terminal.TRANSPORT_UNKNOWN.value
        if reason.startswith("transport_failure")
        else Terminal.SERVICE_UNKNOWN.value
    )
    return BusinessOutcome(
        terminal=terminal,
        detail={"stage": stage, "reason": reason, "kind": kind},
        trace=trace,
        stop_fixture=True,
        isolation_required=True,
    )


def _known(
    trace: AttemptTrace,
    stage: str,
    reason: str,
    kind: str,
    rollback: bool,
    not_executed: bool = False,
) -> BusinessOutcome:
    return BusinessOutcome(
        terminal=Terminal.KNOWN_FAILURE_ROLLED_BACK.value,
        detail={
            "stage": stage,
            "reason": reason,
            "kind": kind,
            "rollback_sent": rollback,
            "rollback_reason": "transaction_not_started" if not rollback else "",
        },
        trace=trace,
        execution_proven_not_started=not_executed,
    )


def _known_with_rollback(
    session: Session,
    trace: AttemptTrace,
    stage: str,
    reason: str,
    kind: str,
    not_executed: bool = False,
) -> BusinessOutcome:
    confirmed = _rollback(session, trace)
    return BusinessOutcome(
        terminal=(
            Terminal.KNOWN_FAILURE_ROLLED_BACK.value
            if confirmed
            else Terminal.CLEANUP_UNCONFIRMED.value
        ),
        detail={
            "stage": stage,
            "reason": reason,
            "kind": kind,
            "rollback_sent": True,
            "rollback_confirmed": confirmed,
        },
        trace=trace,
        stop_fixture=not confirmed,
        isolation_required=not confirmed,
        execution_proven_not_started=not_executed,
    )


def _abort_unknown(
    session: Session, trace: AttemptTrace, stage: str, reason: str, kind: str
) -> BusinessOutcome:
    # Unknown outcomes must not be followed by a blind rollback or close: the
    # original transaction may still be open, and the fixture preserves it.
    terminal = (
        Terminal.TRANSPORT_UNKNOWN.value
        if reason.startswith("transport_failure")
        else Terminal.SERVICE_UNKNOWN.value
    )
    return BusinessOutcome(
        terminal=terminal,
        detail={"stage": stage, "reason": reason, "kind": kind, "attempted_cleanup": False},
        trace=trace,
        stop_fixture=True,
        isolation_required=True,
    )


def _abort_known(
    session: Session,
    trace: AttemptTrace,
    stage: str,
    reason: str,
    kind: str,
    not_executed: bool = False,
) -> BusinessOutcome:
    confirmed = _rollback(session, trace)
    return BusinessOutcome(
        terminal=(
            Terminal.KNOWN_FAILURE_ROLLED_BACK.value
            if confirmed
            else Terminal.CLEANUP_UNCONFIRMED.value
        ),
        detail={
            "stage": stage,
            "reason": reason,
            "kind": kind,
            "rollback_sent": True,
            "rollback_confirmed": confirmed,
        },
        trace=trace,
        stop_fixture=not confirmed,
        isolation_required=not confirmed,
        execution_proven_not_started=not_executed,
    )


def _abort_unexpected(
    session: Session, trace: AttemptTrace, stage: str, reason: str
) -> BusinessOutcome:
    confirmed = _rollback(session, trace)
    return BusinessOutcome(
        terminal=Terminal.UNEXPECTED_ERROR.value,
        detail={
            "stage": stage,
            "reason": reason,
            "kind": "checkout",
            "rollback_sent": True,
            "rollback_confirmed": confirmed,
        },
        trace=trace,
        stop_fixture=True,
        isolation_required=not confirmed,
        unexpected=True,
    )


def _rollback(session: Session, trace: AttemptTrace) -> bool:
    reply = session.execute(ROLLBACK)
    trace.add("rollback", reply)
    verdict, reason = _verdict(reply)
    if verdict == "ok":
        session.in_transaction = False
        return True
    if verdict == "known_not_executed":
        # The server proved the rollback was not dispatched.  That is only
        # acceptable when it also proves there is no transaction to roll back.
        if "no transaction" in (reply.message or "").lower():
            session.in_transaction = False
            return True
    # Unknown or refused: the transaction may still be open, so the flag stays
    # set and the session is not reused.
    return False
