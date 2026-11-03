"""Business attempt shape: statement counts, no retry, and known/unknown handling."""
from __future__ import annotations

import unittest

from pl_retail.business import (
    Session,
    run_business,
    should_retire_session,
)
from pl_retail.client import Reply, StepResult
from pl_retail.plan import PlannedOp


def _reply(results=(), status=200, error=None, transport_error=None) -> Reply:
    return Reply(
        status=status,
        baton="baton-1",
        results=tuple(results),
        error=error,
        cleanup_error=None,
        transport_error=transport_error,
        started_ns=1,
        completed_ns=2,
        ttfb_ns=2,
        request_id="request-1",
        connection_id=1,
        connection_reused=True,
        request_bytes=1,
        response_bytes=1,
    )


class FakeClient:
    """Answers exactly the statements the business layer issues."""

    def __init__(self, fail_on: str | None = None, fail_reply: Reply | None = None) -> None:
        self.calls: list[tuple[str, tuple]] = []
        self.fail_on = fail_on
        self.fail_reply = fail_reply
        self.target = "http://127.0.0.1:1"

    def open(self) -> Reply:
        return _reply()[0:0] if False else _reply()

    def close(self, baton: str) -> Reply:
        return _reply()

    def close_quietly(self) -> None:
        return None

    def execute(self, sql: str, args=(), want_rows=False, baton=None) -> Reply:
        label = self._label(sql)
        self.calls.append((label, tuple(args)))
        if self.fail_on and label == self.fail_on:
            assert self.fail_reply is not None
            return self.fail_reply
        if label == "begin" or label == "commit" or label == "rollback":
            return _reply([StepResult(ok=True, affected_row_count=0)])
        if label == "prices":
            ids = [int(value) for value in args]
            return _reply([StepResult(ok=True, rows=[[pid, 5000 + pid] for pid in ids])])
        if label == "order_insert":
            return _reply([StepResult(ok=True, last_insert_rowid=100001)])
        if label == "lines":
            return _reply([StepResult(ok=True, affected_row_count=5)])
        if label == "payment":
            return _reply([StepResult(ok=True, affected_row_count=1)])
        if label == "read":
            return _reply(
                [StepResult(ok=True, rows=[[1, 5000, 256, 1_000_000, 1_000_000]])]
            )
        return _reply([StepResult(ok=True, affected_row_count=1)])

    @staticmethod
    def _label(sql: str) -> str:
        collapsed = " ".join(sql.split())
        if collapsed in {"BEGIN", "BEGIN IMMEDIATE"}:
            return "begin"
        if collapsed == "COMMIT":
            return "commit"
        if collapsed == "ROLLBACK":
            return "rollback"
        if collapsed.startswith("SELECT id, price_cents FROM products"):
            return "prices"
        if collapsed.startswith("INSERT INTO orders "):
            return "order_insert"
        if collapsed.startswith("INSERT INTO order_lines "):
            return "lines"
        if collapsed.startswith("INSERT INTO payments "):
            return "payment"
        if collapsed.startswith("SELECT p.id"):
            return "read"
        if collapsed.startswith("UPDATE stock"):
            return "stock"
        if collapsed.startswith("UPDATE customers"):
            return "wallet"
        if collapsed.startswith("UPDATE warehouses"):
            return "warehouse"
        return "other"


def _read_op() -> PlannedOp:
    return PlannedOp(index=0, kind="product_stock", params={"warehouse_id": 1, "product_id": 1})


def _checkout_op() -> PlannedOp:
    return PlannedOp(
        index=0,
        kind="checkout",
        params={
            "warehouse_id": 1,
            "customer_id": 1,
            "items": [[1, 1], [2, 1], [3, 1], [4, 1], [5, 1]],
            "hotspot": True,
        },
    )


class BusinessShapeTests(unittest.TestCase):
    def test_read_uses_three_statements_and_never_retries(self) -> None:
        client = FakeClient()
        session = Session(client)
        session.open()
        outcome = run_business(session, _read_op(), "key-1", "r01-U1")
        self.assertTrue(outcome.success)
        self.assertEqual([label for label, _ in client.calls], ["begin", "read", "commit"])

    def test_checkout_uses_thirteen_statements(self) -> None:
        client = FakeClient()
        session = Session(client)
        session.open()
        outcome = run_business(session, _checkout_op(), "key-2", "r01-U1")
        self.assertTrue(outcome.success, outcome.terminal)
        labels = [label for label, _ in client.calls]
        self.assertEqual(len(labels), 13, labels)
        self.assertEqual(labels[0], "begin")
        self.assertEqual(labels[-1], "commit")
        self.assertEqual(labels.count("stock"), 5)
        self.assertEqual(labels[1], "prices")
        self.assertEqual(labels[-2], "payment")

    def test_known_not_executed_is_flagged_and_not_retried(self) -> None:
        refusal = _reply(
            status=429,
            error={"code": "quota_exceeded", "partial_execution_possible": False},
        )
        client = FakeClient(fail_on="prices", fail_reply=refusal)
        session = Session(client)
        session.open()
        outcome = run_business(session, _checkout_op(), "key-3", "r01-U1")
        self.assertEqual(outcome.terminal, "known_failure/rolled_back")
        self.assertTrue(outcome.execution_proven_not_started)
        self.assertTrue(outcome.success is False)
        # begin, prices, rollback: exactly one attempt per statement.
        self.assertEqual([label for label, _ in client.calls], ["begin", "prices", "rollback"])

    def test_unknown_is_never_flagged_as_not_executed(self) -> None:
        ambiguity = _reply(status=502, error={"code": "outcome_unknown", "outcome_unknown": True})
        client = FakeClient(fail_on="prices", fail_reply=ambiguity)
        session = Session(client)
        session.open()
        outcome = run_business(session, _checkout_op(), "key-4", "r01-U1")
        self.assertEqual(outcome.terminal, "service_unknown")
        self.assertFalse(outcome.execution_proven_not_started)
        self.assertTrue(outcome.stop_fixture)
        self.assertTrue(outcome.isolation_required)

    def test_retirement_policy_is_confined_to_known_not_executed(self) -> None:
        self.assertTrue(should_retire_session("B", "known_failure/rolled_back", True))
        self.assertFalse(should_retire_session("B", "known_failure/rolled_back", False))
        self.assertFalse(should_retire_session("B", "service_unknown", True))
        self.assertFalse(should_retire_session("B", "transport_unknown", True))
        self.assertFalse(should_retire_session("B", "cleanup_unconfirmed", True))
        self.assertFalse(should_retire_session("A", "known_failure/rolled_back", True))

    def test_read_failure_after_begin_rolls_back_once(self) -> None:
        refusal = _reply(status=400, error={"code": "invalid_arguments", "partial_execution_possible": False})
        client = FakeClient(fail_on="read", fail_reply=refusal)
        session = Session(client)
        session.open()
        outcome = run_business(session, _read_op(), "key-5", "r01-U1")
        self.assertEqual(outcome.terminal, "known_failure/rolled_back")
        self.assertEqual([label for label, _ in client.calls], ["begin", "read", "rollback"])


if __name__ == "__main__":
    unittest.main()
