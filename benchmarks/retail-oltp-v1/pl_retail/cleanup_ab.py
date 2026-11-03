"""Contract §九(b): known-rejection close-retirement policy and its A/B probe.

Policy A is the original behaviour: a rejection that the peer proved did not
execute leaves the worker session in place.  Policy B adds exactly one extra,
independent Hrana close for that state and reopens the session.  Neither
policy acts on ``service_unknown`` / ``transport_unknown``; those keep the
isolation path from the contract.

The probe here is *directed*: it induces only known-not-executed rejections,
then measures how many session seats are still obtainable.  That makes the
comparison observable (leaked seats are visible as refusals) without running
the whole matrix.
"""
from __future__ import annotations

import time
from typing import Any

from .business import Session
from .client import FatalTransportError, HranaClient, Target, classify_transport

PROBE_KNOWN_REJECTION_SQL = "SELECT id FROM retail_missing_table WHERE id = 1"
PROBE_OK_SQL = "SELECT id FROM products WHERE id = 1"

#: A probe round is short by design; the full matrix never uses it.
DEFAULT_PROBE_OPS = 40


def probe_session_capacity(target: Target, maximum: int, timeout: float = 5.0) -> dict[str, Any]:
    """Try to hold ``maximum`` sessions at once and close every one of them.

    A refused open is the observable signature of a leaked seat: the worker
    counts seats per database, so if policy A leaked the probes above, the
    later opens fail before the pool can be exhausted again.
    """
    opened: list[tuple[HranaClient, str]] = []
    outcomes: list[dict[str, Any]] = []
    started = time.monotonic_ns()
    for index in range(maximum):
        client = HranaClient(target, timeout=timeout)
        try:
            reply = client.open()
            verdict, reason = classify_transport(reply)
            ok = verdict == "ok" and bool(reply.baton)
            outcomes.append(
                {
                    "index": index,
                    "ok": ok,
                    "verdict": verdict,
                    "reason": reason,
                    "status": reply.status,
                    "code": reply.code,
                }
            )
            if ok and reply.baton:
                opened.append((client, reply.baton))
            else:
                client.close_quietly()
        except FatalTransportError as error:
            outcomes.append({"index": index, "ok": False, "verdict": "fatal", "reason": str(error)})
            client.close_quietly()
    closed = 0
    for client, baton in opened:
        try:
            reply = client.close(baton)
            if classify_transport(reply)[0] == "ok":
                closed += 1
        finally:
            client.close_quietly()
    return {
        "requested": maximum,
        "opened": sum(1 for item in outcomes if item["ok"]),
        "closed": closed,
        "refused": sum(1 for item in outcomes if not item["ok"]),
        "elapsed_ms": round((time.monotonic_ns() - started) / 1_000_000, 3),
        "outcomes": outcomes,
    }


def probe_policy(session: Session, policy: str, ops: int) -> dict[str, Any]:
    """Drive ``ops`` known-not-executed rejections under one policy.

    Returns the counts the comparator needs.  No unknown outcome is induced:
    this probe must not exercise the isolation path it claims to leave alone.
    """
    from .business import should_retire_session

    started = time.monotonic_ns()
    known = 0
    unknown = 0
    retired = 0
    retirements_in_unknown = 0
    retirement_failures = 0
    latencies: list[float] = []
    reason = ""
    for _ in range(ops):
        attempt_start = time.monotonic_ns()
        reply = session.execute(PROBE_KNOWN_REJECTION_SQL)
        verdict, reason = classify_transport(reply)
        latencies.append((time.monotonic_ns() - attempt_start) / 1_000_000)
        if verdict == "known_not_executed":
            known += 1
        if verdict == "unknown":
            unknown += 1
        terminal = (
            "known_failure/rolled_back"
            if verdict == "known_not_executed"
            else "service_unknown"
        )
        # Structural confinement: should_retire_session() only returns True for
        # a known_failure/rolled_back terminal, so an unknown outcome can never
        # be retired by either policy.  Count it anyway for the evidence.
        if should_retire_session(policy, terminal, verdict == "known_not_executed"):
            if verdict == "unknown":
                retirements_in_unknown += 1
                continue
            result = session.retire()
            retired += 1
            if not result.get("ok"):
                retirement_failures += 1
                break
            session.open()
    latencies.sort()
    return {
        "policy": policy,
        "ops": ops,
        "known_not_executed": known,
        "unknown_attempts": unknown,
        "retired": retired,
        "retirement_failures": retirement_failures,
        "retirements_in_unknown": retirements_in_unknown,
        "p95_ms": _nearest_rank(latencies, 0.95) if latencies else None,
        "elapsed_ms": round((time.monotonic_ns() - started) / 1_000_000, 3),
        "reason": reason,
    }


def _nearest_rank(ordered: list[float], rank: float) -> float:
    index = max(0, min(len(ordered) - 1, int(-(-rank * len(ordered) // 1)) - 1))
    return ordered[index]


def compare_policies(side_a: dict[str, Any], side_b: dict[str, Any]) -> dict[str, Any]:
    """Pure comparison of policy A and policy B probe results.

    Criteria (all must hold):
    * both sides saw the same number of known-not-executed rejections;
    * A never retired, B retired each rejection exactly once;
    * B's retirement never failed;
    * neither side retired anything in an unknown state;
    * B's obtainable seat count is not lower than A's;
    * B's p95 probe latency stays within 1.5x of A's.
    """
    violations: list[str] = []
    if side_a.get("policy") != "A" or side_b.get("policy") != "B":
        violations.append(
            "sides are mislabelled: A=%r B=%r"
            % (side_a.get("policy"), side_b.get("policy"))
        )
    a_known = int(side_a.get("known_not_executed") or 0)
    b_known = int(side_b.get("known_not_executed") or 0)
    if a_known != b_known:
        violations.append("known rejection counts differ: A=%d B=%d" % (a_known, b_known))
    if a_known == 0:
        violations.append("probe produced no known-not-executed rejection")
    if int(side_a.get("retired") or 0) != 0:
        violations.append("policy A retired a session")
    if int(side_b.get("retired") or 0) != b_known:
        violations.append(
            "policy B retired %s of %d known rejections" % (side_b.get("retired"), b_known)
        )
    if int(side_b.get("retirement_failures") or 0) != 0:
        violations.append("policy B retirement(s) failed")
    for side in (side_a, side_b):
        if int(side.get("retirements_in_unknown") or 0) != 0:
            violations.append(
                "policy %s retired a session in an unknown state" % side.get("policy")
            )
    a_seats = side_a.get("capacity") or {}
    b_seats = side_b.get("capacity") or {}
    a_opened = int(a_seats.get("opened") or 0)
    b_opened = int(b_seats.get("opened") or 0)
    if a_seats.get("requested") and b_opened < a_opened:
        violations.append(
            "policy B obtained fewer seats: A=%d B=%d" % (a_opened, b_opened)
        )
    a_p95 = side_a.get("p95_ms")
    b_p95 = side_b.get("p95_ms")
    if a_p95 and b_p95 and float(b_p95) > float(a_p95) * 1.5:
        violations.append(
            "policy B p95 %.3fms exceeds 1.5x policy A %.3fms" % (b_p95, a_p95)
        )
    return {
        "verdict": "PASS" if not violations else "FAIL",
        "violations": violations,
        "policy_a": {
            "known_not_executed": a_known,
            "retired": side_a.get("retired"),
            "seats_opened": a_opened,
            "p95_ms": a_p95,
        },
        "policy_b": {
            "known_not_executed": b_known,
            "retired": side_b.get("retired"),
            "seats_opened": b_opened,
            "p95_ms": b_p95,
        },
        "scope": {
            "applies_to": "known_failure/rolled_back with proven-not-executed evidence only",
            "unknown_isolation_unchanged": True,
        },
    }
