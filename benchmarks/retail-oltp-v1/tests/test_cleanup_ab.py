"""§九(b): the close-retirement policy and its directed A/B comparison."""
from __future__ import annotations

import unittest

from pl_retail.cleanup_ab import compare_policies, probe_policy
from pl_retail.client import Reply, StepResult


def _reply(status=200, error=None) -> Reply:
    return Reply(
        status=status,
        baton="baton",
        results=(StepResult(ok=status == 200),),
        error=error,
        cleanup_error=None,
        transport_error=None,
        started_ns=1,
        completed_ns=2,
        ttfb_ns=2,
        request_id="r",
        connection_id=1,
        connection_reused=True,
        request_bytes=1,
        response_bytes=1,
    )


class FakeSession:
    def __init__(self, verdict: str = "known_not_executed", opens: int = 2) -> None:
        self.verdict = verdict
        self.retired = 0
        self.reopened = 0
        self.opens = opens

    def execute(self, sql: str, args=(), want_rows=False, baton=None) -> Reply:
        if self.verdict == "known_not_executed":
            return _reply(
                status=429,
                error={"code": "quota_exceeded", "partial_execution_possible": False},
            )
        return _reply(status=502, error={"code": "outcome_unknown", "outcome_unknown": True})

    def retire(self) -> dict:
        self.retired += 1
        return {"ok": True, "status": 200}

    def open(self) -> Reply:
        self.reopened += 1
        return _reply()


class PolicyTests(unittest.TestCase):
    def test_policy_a_never_retires(self) -> None:
        session = FakeSession()
        result = probe_policy(session, "A", 10)
        self.assertEqual(result["retired"], 0)
        self.assertEqual(result["known_not_executed"], 10)
        self.assertEqual(session.reopened, 0)

    def test_policy_b_retires_every_known_not_executed_attempt(self) -> None:
        session = FakeSession()
        result = probe_policy(session, "B", 10)
        self.assertEqual(result["retired"], 10)
        self.assertEqual(result["retirement_failures"], 0)
        self.assertEqual(session.reopened, 10)

    def test_policy_b_never_touches_an_unknown_outcome(self) -> None:
        session = FakeSession(verdict="unknown")
        result = probe_policy(session, "B", 5)
        self.assertEqual(result["retired"], 0)
        self.assertEqual(result["unknown_attempts"], 5)
        self.assertEqual(result["retirements_in_unknown"], 0)
        self.assertEqual(session.reopened, 0)

    def test_ab_requires_both_sides_and_a_bounded_latency_cost(self) -> None:
        base = {"policy": "A", "known_not_executed": 400, "retired": 0, "p95_ms": 1.0,
                "capacity": {"requested": 16, "opened": 16}}
        candidate = {"policy": "B", "known_not_executed": 400, "retired": 400, "p95_ms": 1.4,
                     "capacity": {"requested": 16, "opened": 16}}
        self.assertEqual(compare_policies(base, candidate)["verdict"], "PASS")

        slow = dict(candidate, p95_ms=4.0)
        verdict = compare_policies(base, slow)
        self.assertEqual(verdict["verdict"], "FAIL")
        self.assertTrue(any("p95" in item for item in verdict["violations"]))

        leaky = dict(candidate, capacity={"requested": 16, "opened": 15})
        self.assertEqual(compare_policies(base, leaky)["verdict"], "FAIL")

        wrong_side = dict(candidate, policy="A")
        self.assertEqual(compare_policies(base, wrong_side)["verdict"], "FAIL")

    def test_scope_is_recorded_for_the_reader(self) -> None:
        base = {"policy": "A", "known_not_executed": 10, "retired": 0, "p95_ms": 1.0,
                "capacity": {"requested": 16, "opened": 16}}
        candidate = {"policy": "B", "known_not_executed": 10, "retired": 10, "p95_ms": 1.1,
                     "capacity": {"requested": 16, "opened": 16}}
        result = compare_policies(base, candidate)
        self.assertIn("known_failure/rolled_back", result["scope"]["applies_to"])
        self.assertTrue(result["scope"]["unknown_isolation_unchanged"])


if __name__ == "__main__":
    unittest.main()
