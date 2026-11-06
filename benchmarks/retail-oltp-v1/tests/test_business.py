"""Business attempt shape: statement counts, no retry, and known/unknown handling."""
from __future__ import annotations

import unittest

from pl_retail.business import (
    MERGED_STOCK_UPDATE_SQL,
    Session,
    run_business,
    should_retire_session,
)
from pl_retail.call_profiles import PROFILES
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

    def __init__(
        self,
        fail_on: str | None = None,
        fail_reply: Reply | None = None,
        merged_affected: int = 5,
    ) -> None:
        self.calls: list[tuple[str, tuple]] = []
        self.fail_on = fail_on
        self.fail_reply = fail_reply
        self.merged_affected = merged_affected
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
        if label == "stock_merged":
            return _reply([StepResult(ok=True, affected_row_count=self.merged_affected)])
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
        if collapsed.startswith("UPDATE stock") and "CASE product_id" in collapsed:
            return "stock_merged"
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
        self.assertFalse(session.in_transaction)

    def test_a_session_defaults_to_the_baseline_profile(self) -> None:
        session = Session(FakeClient())
        self.assertEqual(session.profile.name, "baseline-v1")
        self.assertFalse(session.in_transaction)

    def test_an_unknown_commit_keeps_the_transaction_open(self) -> None:
        ambiguity = _reply(status=502, error={"code": "outcome_unknown", "outcome_unknown": True})
        client = FakeClient(fail_on="commit", fail_reply=ambiguity)
        session = Session(client)
        session.open()
        outcome = run_business(session, _read_op(), "key-6", "r01-U1")
        self.assertEqual(outcome.terminal, "service_unknown")
        # The server may still hold the transaction; the session is not reused.
        self.assertTrue(session.in_transaction)


AUTOCOMMIT = PROFILES["app-read-v1"]
MERGED = PROFILES["app-stock-v1"]
READ_AND_MERGED = PROFILES["app-read-stock-v1"]


class AutocommitReadTests(unittest.TestCase):
    def _session(self, client: FakeClient, profile=AUTOCOMMIT) -> Session:
        session = Session(client, profile=profile)
        session.open()
        return session

    def test_autocommit_read_sends_one_query_and_no_transaction(self) -> None:
        client = FakeClient()
        session = self._session(client)
        outcome = run_business(session, _read_op(), "key-a1", "r01-U1")
        self.assertTrue(outcome.success, outcome.terminal)
        self.assertEqual(outcome.terminal, "read_ok")
        self.assertEqual([label for label, _ in client.calls], ["read"])
        self.assertEqual([phase["label"] for phase in outcome.trace.phases], ["query"])
        self.assertEqual(outcome.detail, {"kind": "product_stock", "rows": 1})
        self.assertEqual(outcome.trace.calls, 1)
        self.assertFalse(session.in_transaction)

    def test_autocommit_unknown_does_not_send_rollback(self) -> None:
        ambiguity = _reply(status=502, error={"code": "outcome_unknown", "outcome_unknown": True})
        client = FakeClient(fail_on="read", fail_reply=ambiguity)
        session = self._session(client)
        outcome = run_business(session, _read_op(), "key-a2", "r01-U1")
        self.assertEqual(outcome.terminal, "service_unknown")
        self.assertTrue(outcome.stop_fixture)
        self.assertTrue(outcome.isolation_required)
        self.assertEqual([label for label, _ in client.calls], ["read"])
        self.assertIs(outcome.detail["rollback_sent"], False)
        self.assertEqual(outcome.detail["rollback_reason"], "autocommit_no_transaction")

    def test_autocommit_known_not_executed_does_not_send_rollback(self) -> None:
        refusal = _reply(
            status=429,
            error={"code": "quota_exceeded", "partial_execution_possible": False},
        )
        client = FakeClient(fail_on="read", fail_reply=refusal)
        session = self._session(client)
        outcome = run_business(session, _read_op(), "key-a3", "r01-U1")
        self.assertEqual(outcome.terminal, "known_failure/rolled_back")
        self.assertTrue(outcome.execution_proven_not_started)
        self.assertEqual([label for label, _ in client.calls], ["read"])
        self.assertIs(outcome.detail["rollback_sent"], False)
        self.assertEqual(outcome.detail["rollback_reason"], "autocommit_no_transaction")

    def test_autocommit_statement_failure_is_known_without_rollback(self) -> None:
        failed = _reply(
            [
                StepResult(
                    ok=False,
                    error_code="SQL_PARSE_ERROR",
                    error_message="syntax error near FROM",
                )
            ]
        )
        client = FakeClient(fail_on="read", fail_reply=failed)
        session = self._session(client)
        outcome = run_business(session, _read_op(), "key-a4", "r01-U1")
        self.assertEqual(outcome.terminal, "known_failure/rolled_back")
        self.assertFalse(outcome.execution_proven_not_started)
        self.assertEqual([label for label, _ in client.calls], ["read"])
        self.assertIs(outcome.detail["rollback_sent"], False)
        self.assertEqual(outcome.detail["rollback_reason"], "autocommit_statement_failed")

    def test_autocommit_business_check_failure_stops_without_rollback(self) -> None:
        wrong = _reply([StepResult(ok=True, rows=[[1, 5000, 256, 10, 999]])])
        client = FakeClient(fail_on="read", fail_reply=wrong)
        session = self._session(client)
        outcome = run_business(session, _read_op(), "key-a5", "r01-U1")
        self.assertEqual(outcome.terminal, "unexpected_error")
        self.assertTrue(outcome.stop_fixture)
        self.assertTrue(outcome.unexpected)
        self.assertEqual([label for label, _ in client.calls], ["read"])
        self.assertEqual(outcome.detail["stage"], "business_check")
        self.assertIs(outcome.detail["rollback_sent"], False)
        self.assertEqual(outcome.detail["rollback_reason"], "autocommit_no_transaction")

    def test_an_open_transaction_falls_back_to_the_transactional_path(self) -> None:
        client = FakeClient()
        session = self._session(client)
        session.in_transaction = True
        outcome = run_business(session, _read_op(), "key-a6", "r01-U1")
        self.assertTrue(outcome.success, outcome.terminal)
        self.assertEqual([label for label, _ in client.calls], ["begin", "read", "commit"])
        self.assertFalse(session.in_transaction)


class MergedCheckoutTests(unittest.TestCase):
    def _session(self, client: FakeClient, profile=MERGED) -> Session:
        session = Session(client, profile=profile)
        session.open()
        return session

    def test_merged_checkout_uses_nine_statements_and_26_parameters(self) -> None:
        client = FakeClient()
        session = self._session(client)
        outcome = run_business(session, _checkout_op(), "key-m1", "r01-U1")
        self.assertTrue(outcome.success, outcome.terminal)
        labels = [label for label, _ in client.calls]
        self.assertEqual(len(labels), 9, labels)
        self.assertEqual(labels[0], "begin")
        self.assertEqual(labels[-1], "commit")
        self.assertEqual(labels.count("stock_merged"), 1)
        self.assertEqual(MERGED_STOCK_UPDATE_SQL.count("?"), 26)
        merged = [args for label, args in client.calls if label == "stock_merged"]
        self.assertEqual(len(merged[0]), 26)
        # Five decrement pairs, the warehouse id, the IN ids, then the guards.
        self.assertEqual(list(merged[0][:10]), [1, 1, 2, 1, 3, 1, 4, 1, 5, 1])
        self.assertEqual(merged[0][10], 1)
        self.assertEqual(list(merged[0][11:16]), [1, 2, 3, 4, 5])
        self.assertEqual(list(merged[0][16:]), [1, 1, 2, 1, 3, 1, 4, 1, 5, 1])

    def test_merged_parameters_follow_the_sorted_product_order(self) -> None:
        client = FakeClient()
        session = self._session(client)
        items = [[3, 2], [1, 1], [5, 3], [2, 1], [4, 2]]
        op = PlannedOp(
            index=0,
            kind="checkout",
            params={"warehouse_id": 7, "customer_id": 1, "items": items, "hotspot": True},
        )
        outcome = run_business(session, op, "key-m2", "r01-U1")
        self.assertTrue(outcome.success, outcome.terminal)
        args = [args for label, args in client.calls if label == "stock_merged"][0]
        self.assertEqual(list(args[:10]), [1, 1, 2, 1, 3, 2, 4, 2, 5, 3])
        self.assertEqual(args[10], 7)
        self.assertEqual(list(args[11:16]), [1, 2, 3, 4, 5])
        self.assertEqual(list(args[16:]), [1, 1, 2, 1, 3, 2, 4, 2, 5, 3])

    def test_merged_checkout_rolls_back_when_fewer_rows_are_updated(self) -> None:
        client = FakeClient(merged_affected=4)
        session = self._session(client)
        outcome = run_business(session, _checkout_op(), "key-m3", "r01-U1")
        self.assertEqual(outcome.terminal, "known_failure/rolled_back")
        self.assertEqual(
            [label for label, _ in client.calls],
            ["begin", "prices", "stock_merged", "rollback"],
        )
        self.assertEqual(outcome.detail["stage"], "stock_merged")
        self.assertEqual(outcome.detail["reason"], "stock_unavailable")
        self.assertTrue(outcome.detail["rollback_confirmed"])
        self.assertFalse(session.in_transaction)

    def test_merged_checkout_rolls_back_on_more_than_five_rows(self) -> None:
        client = FakeClient(merged_affected=6)
        session = self._session(client)
        outcome = run_business(session, _checkout_op(), "key-m4", "r01-U1")
        self.assertEqual(outcome.terminal, "unexpected_error")
        self.assertTrue(outcome.stop_fixture)
        self.assertTrue(outcome.unexpected)
        self.assertEqual(
            [label for label, _ in client.calls],
            ["begin", "prices", "stock_merged", "rollback"],
        )
        self.assertEqual(outcome.detail["stage"], "stock_merged")
        self.assertTrue(outcome.detail["rollback_confirmed"])

    def _assert_rejected_before_dispatch(self, items: list[list[int]]) -> None:
        client = FakeClient()
        session = self._session(client)
        op = PlannedOp(
            index=0,
            kind="checkout",
            params={"warehouse_id": 1, "customer_id": 1, "items": items, "hotspot": True},
        )
        outcome = run_business(session, op, "key-m5", "r01-U1")
        self.assertEqual(client.calls, [])
        self.assertEqual(outcome.trace.calls, 0)
        self.assertEqual(outcome.terminal, "unexpected_error")
        self.assertTrue(outcome.stop_fixture)
        self.assertTrue(outcome.unexpected)
        self.assertEqual(outcome.detail["stage"], "validate")
        self.assertIs(outcome.detail["rollback_sent"], False)

    def test_merged_checkout_rejects_duplicate_products(self) -> None:
        self._assert_rejected_before_dispatch([[1, 1], [1, 1], [2, 1], [3, 1], [4, 1]])

    def test_merged_checkout_rejects_a_zero_quantity(self) -> None:
        self._assert_rejected_before_dispatch([[1, 0], [2, 1], [3, 1], [4, 1], [5, 1]])

    def test_merged_checkout_rejects_a_quantity_above_three(self) -> None:
        self._assert_rejected_before_dispatch([[1, 4], [2, 1], [3, 1], [4, 1], [5, 1]])

    def test_merged_checkout_rejects_four_items(self) -> None:
        self._assert_rejected_before_dispatch([[1, 1], [2, 1], [3, 1], [4, 1]])

    def test_the_read_and_stock_profile_combines_both_shapes(self) -> None:
        client = FakeClient()
        session = self._session(client, profile=READ_AND_MERGED)
        read = run_business(session, _read_op(), "key-m6", "r01-U1")
        self.assertEqual([label for label, _ in client.calls], ["read"])
        self.assertTrue(read.success)
        client.calls.clear()
        checkout = run_business(session, _checkout_op(), "key-m7", "r01-U1")
        self.assertTrue(checkout.success, checkout.terminal)
        self.assertEqual(len(client.calls), 9)

    def test_app_read_v1_keeps_the_transactional_checkout(self) -> None:
        client = FakeClient()
        session = self._session(client, profile=PROFILES["app-read-v1"])
        read = run_business(session, _read_op(), "key-p1", "r01-U1")
        self.assertEqual([label for label, _ in client.calls], ["read"])
        self.assertTrue(read.success)
        client.calls.clear()
        checkout = run_business(session, _checkout_op(), "key-p2", "r01-U1")
        self.assertTrue(checkout.success, checkout.terminal)
        self.assertEqual(len(client.calls), 13)
        self.assertEqual(client.calls[2][0], "stock")

    def test_app_stock_v1_keeps_transactional_reads(self) -> None:
        client = FakeClient()
        session = self._session(client, profile=MERGED)
        outcome = run_business(session, _read_op(), "key-p3", "r01-U1")
        self.assertTrue(outcome.success, outcome.terminal)
        self.assertEqual([label for label, _ in client.calls], ["begin", "read", "commit"])


if __name__ == "__main__":
    unittest.main()
