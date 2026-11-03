"""Formal matrix runner.

One round is: fresh fixture, full import, preflight, pre-stream verification,
barrier-started warmups, a 120-second timed window, drain, session retirement,
post-stream verification, audit, clean shutdown, same-volume restart and a
post-restart verification.  A round that observes an unknown terminal stops
dispatching immediately and never retries, rolls back or closes the fixture.
"""
from __future__ import annotations

import random
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from .business import Session, run_business, should_retire_session
from .call_profiles import DEFAULT_PROFILE_NAME, CallProfile, resolve_profile
from .cleanup_ab import (
    DEFAULT_PROBE_OPS,
    compare_policies,
    probe_policy,
    probe_session_capacity,
)
from .client import HranaClient, Target
from .config_audit import audit_config
from .contract import driver_digest, load_contract
from .errors import PrerequisiteError, Terminal
from .evidence import EvidenceWriter
from .fixture import Fixture, FixtureConfig
from .generate import Dataset, Shape
from .lifecycle import LifecycleConfig, restart, shutdown
from .loader import load_dataset
from .metrics import ResourceSampler
from .plan import PlanStream
from .preflight import run_preflight
from .verify import Ledger, verify_stream

LEVELS = (
    ("U1", 1, "uniform"),
    ("U4", 4, "uniform"),
    ("U8", 8, "uniform"),
    ("U16", 16, "uniform"),
    ("H8", 8, "hotspot"),
)

DEFAULT_WINDOW_SECONDS = 120.0
DEFAULT_WARMUPS = 20
DRAIN_BUDGET_SECONDS = 30.0
SESSION_BUDGET_SECONDS = 30.0


@dataclass
class RunnerConfig:
    fixture: FixtureConfig
    output_dir: str
    mode: str = "full"
    scale: int = 1
    repetitions: int = 3
    window_seconds: float = DEFAULT_WINDOW_SECONDS
    warmups: int = DEFAULT_WARMUPS
    levels: tuple[tuple[str, int, str], ...] = LEVELS
    platform_descriptor: dict[str, Any] | None = None
    lifecycle: LifecycleConfig | None = None
    keep_fixtures: bool = False
    idle: bool = False
    close_policy: str = "A"
    idle_cycles: int = 10
    run_id: str = ""
    call_profile: str = DEFAULT_PROFILE_NAME
    contract: dict[str, Any] = field(default_factory=dict)


def _token() -> str:
    return "%d-%06x" % (int(time.time()), random.SystemRandom().randrange(1 << 24))


def _safe_name(value: str) -> str:
    return re.sub(r"[^a-z0-9-]", "-", value.lower())[:120]


def _iso(wall_ns: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(wall_ns / 1_000_000_000)) + (
        ".%03dZ" % ((wall_ns // 1_000_000) % 1000)
    )


def _redact(value: Any) -> Any:
    """Keep evidence free of credentials even if a descriptor carries them."""
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            lowered = str(key).lower()
            if any(token in lowered for token in ("key", "token", "secret", "password")):
                redacted[key] = "<redacted>"
            else:
                redacted[key] = _redact(item)
        return redacted
    if isinstance(value, list):
        return [_redact(item) for item in value]
    return value


def _open_session(
    target: Target,
    timeout: float = 5.0,
    profile: CallProfile | None = None,
) -> Session:
    client = HranaClient(target, timeout=timeout)
    session = Session(client, profile=profile)
    session.open()
    return session


def _close_session(session: Session) -> dict[str, Any]:
    started = time.monotonic_ns()
    try:
        reply = session.close()
        return {
            "ok": reply.status == 200 and reply.error is None and not reply.transport_error,
            "status": reply.status,
            "code": reply.code,
            "transport_error": reply.transport_error,
            "duration_ms": round((time.monotonic_ns() - started) / 1_000_000, 3),
        }
    except Exception as error:  # noqa: BLE001 - cleanup evidence
        return {
            "ok": False,
            "status": None,
            "code": "cleanup_exception",
            "reason": "%s: %s" % (type(error).__name__, error),
            "duration_ms": round((time.monotonic_ns() - started) / 1_000_000, 3),
        }
    finally:
        session.client.close_quietly()


class _WindowState:
    def __init__(self) -> None:
        self.t0_ns = 0
        self.t1_ns = 0
        self.stop = threading.Event()


class _WorkerStats:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.warmups = 0
        self.measure = 0
        # Kept apart: goodput counts only attempts started inside [t0, t1), so a
        # reader must never have to subtract warmups from a combined total.
        self.warmup_successes = 0
        self.measure_successes = 0
        self.terminals: dict[str, int] = {}
        self.open_error: str | None = None
        self.close: dict[str, Any] = {}
        self.warmup_checkout_acks = 0

    def add(self, outcome, window: str) -> None:
        with self.lock:
            self.terminals[outcome.terminal] = self.terminals.get(outcome.terminal, 0) + 1
            if window == "warmup":
                self.warmups += 1
                if outcome.success:
                    self.warmup_successes += 1
                if outcome.terminal == Terminal.COMMITTED_ACK.value:
                    self.warmup_checkout_acks += 1
            else:
                self.measure += 1
                if outcome.success:
                    self.measure_successes += 1


class RoundRunner:
    def __init__(self, config: RunnerConfig):
        self.config = config
        # Resolved once per run: attempts and run.json must record the same
        # profile the sessions were driven with.
        self.call_profile = resolve_profile(config.call_profile)
        self.contract = config.contract or load_contract()
        self.dataset = Dataset(scale=config.scale)
        self.shape = Shape.for_scale(config.scale)
        self._cleanup_lock = threading.Lock()
        self.evidence = EvidenceWriter(config.output_dir)

    # -- one round ---------------------------------------------------------

    def run(self, repetition: int, level_name: str, concurrency: int, mode: str) -> dict[str, Any]:
        round_id = "r%02d-%s" % (repetition, level_name)
        relative = "rounds/%s" % round_id
        record: dict[str, Any] = {
            "round_id": round_id,
            "repetition": repetition,
            "level": level_name,
            "concurrency": concurrency,
            "mode": mode,
            "scale": self.config.scale,
            "window_seconds": self.config.window_seconds,
            "warmups_per_worker": self.config.warmups,
            "status": "running",
            "abort_reason": None,
            "started_ns": time.monotonic_ns(),
        }
        fixture_name = _safe_name(
            "%s-%s-%s" % (self.config.fixture.database_prefix, self.config.run_id, round_id)
        )
        fixture = Fixture(self.config.fixture, fixture_name, round_id)
        record["database"] = fixture.database
        ledger_confirmed: list[dict[str, Any]] = []
        ledger_rejected: set[str] = set()
        ledger_unknown: set[str] = set()
        sampler: ResourceSampler | None = None
        try:
            target = fixture.create()
            record["fixture"] = {
                "create": {"status": "ok"},
                "wait_ready": fixture.wait_ready(30.0),
            }
            load_session = _open_session(target, timeout=60.0, profile=self.call_profile)
            try:
                load_report = load_dataset(load_session, self.dataset)
            finally:
                record["load_session"] = _close_session(load_session)
            record["load"] = load_report.as_dict()
            if not load_report.ok:
                record["status"] = "aborted"
                record["abort_reason"] = "load_failed"
                return self._finish(record, relative, fixture, ledger_confirmed, ledger_rejected, ledger_unknown)

            record["config"] = {"before": self._audit(target, "before")}
            record["preflight"] = run_preflight(target, self.dataset)
            if not record["preflight"].get("ok"):
                record["status"] = "aborted"
                record["abort_reason"] = "preflight_failed"
                return self._finish(record, relative, fixture, ledger_confirmed, ledger_rejected, ledger_unknown)

            pre = verify_stream(target, self.dataset, Ledger(), "pre")
            record["verify"] = {"pre": pre}
            if not pre["ok"]:
                record["status"] = "aborted"
                record["abort_reason"] = "pre_verification_failed"
                return self._finish(record, relative, fixture, ledger_confirmed, ledger_rejected, ledger_unknown)

            sampler = ResourceSampler(
                self.config.platform_descriptor or {},
                lambda sample: self.evidence.append("%s/resource.jsonl" % relative, sample),
                phase="round",
            )
            sampler.start()
            window = self._run_window(target, round_id, concurrency, mode, relative, ledger_confirmed,
                                      ledger_rejected, ledger_unknown, record)
            sampler.stop()
            record["timeline"] = window["timeline"]
            record["counts"] = window["counts"]
            record["sessions"] = window["sessions"]

            ledger = Ledger.from_records(
                ledger_confirmed, sorted(ledger_rejected), sorted(ledger_unknown)
            )
            record["ledger"] = {
                "confirmed": len(ledger.entries),
                "rejected": len(ledger.rejected_keys),
                "unknown": len(ledger.unknown_keys),
                "units": ledger.units,
                "payments_total_cents": ledger.payments_total,
            }
            post = verify_stream(target, self.dataset, ledger, "post")
            record["verify"]["post"] = post
            record["config"]["after"] = self._audit(target, "after")

            if window["aborted"]:
                record["status"] = "aborted"
                record["abort_reason"] = window["abort_reason"]
                return self._finish(record, relative, fixture, ledger_confirmed, ledger_rejected, ledger_unknown)

            if self.config.lifecycle is not None:
                stop = shutdown(self.config.lifecycle)
                record["shutdown"] = stop.as_dict()
                if stop.configured and stop.ok:
                    started_again = restart(self.config.lifecycle)
                    record["restart"] = started_again.as_dict()
                    if started_again.configured and started_again.ok:
                        record["verify"]["restart"] = verify_stream(
                            target, self.dataset, ledger, "restart"
                        )
                record["recovery_seconds"] = {
                    "shutdown": stop.duration_ms / 1000.0,
                    "restart": record.get("restart", {}).get("duration_ms", 0) / 1000.0
                    if record.get("restart")
                    else None,
                    "ready": record.get("restart", {}).get("ready", {}).get("seconds")
                    if record.get("restart")
                    else None,
                }
            else:
                record["shutdown"] = {"configured": False, "status": "not_configured"}
                record["restart"] = {"configured": False, "status": "not_configured"}

            record["status"] = "completed"
            return self._finish(record, relative, fixture, ledger_confirmed, ledger_rejected, ledger_unknown)
        except PrerequisiteError as failure:
            record["status"] = "blocked"
            record["abort_reason"] = str(failure)
            return self._finish(record, relative, fixture, ledger_confirmed, ledger_rejected, ledger_unknown)
        except Exception as failure:  # noqa: BLE001 - the round report preserves the reason
            record["status"] = "aborted"
            record["abort_reason"] = "%s: %s" % (type(failure).__name__, failure)
            return self._finish(record, relative, fixture, ledger_confirmed, ledger_rejected, ledger_unknown)
        finally:
            if sampler is not None:
                sampler.stop()

    def _audit(self, target: Target, phase: str) -> dict[str, Any]:
        return audit_config(
            target,
            self.config.fixture,
            self.config.platform_descriptor or {},
            self.contract,
            self.dataset,
            phase=phase,
        )

    # -- timed window ------------------------------------------------------

    def _run_window(
        self,
        target: Target,
        round_id: str,
        concurrency: int,
        mode: str,
        relative: str,
        ledger_confirmed: list[dict[str, Any]],
        ledger_rejected: set[str],
        ledger_unknown: set[str],
        record: dict[str, Any],
    ) -> dict[str, Any]:
        state = _WindowState()
        plan = PlanStream(
            "hotspot" if mode == "hotspot" else "uniform",
            seed=int(self.contract.document.get("seed", 20261103)),
            products=self.shape.products,
            warehouses=self.shape.warehouses,
            customers=self.shape.customers,
            orders=self.shape.orders,
        )
        stats = [_WorkerStats() for _ in range(concurrency)]
        ready_lock = threading.Lock()
        ready_count = 0
        release = threading.Event()
        ledger_lock = threading.Lock()
        attempts_path = "%s/attempts.jsonl" % relative
        http_path = "%s/http.jsonl" % relative
        record["cleanup_policy"] = {
            "policy": self.config.close_policy,
            "retirements": [],
            "failures": 0,
        }
        sessions_path = "%s/sessions.jsonl" % relative
        workers: list[threading.Thread] = []

        def http_recorder(entry: dict[str, Any]) -> None:
            self.evidence.append(http_path, entry)

        def worker(index: int) -> None:
            nonlocal ready_count
            client = HranaClient(target, timeout=5.0, recorder=http_recorder)
            session = Session(client, profile=self.call_profile)
            try:
                try:
                    session.open()
                except Exception as error:  # noqa: BLE001 - session open evidence
                    stats[index].open_error = "%s: %s" % (type(error).__name__, error)
                    state.stop.set()
                    with ready_lock:
                        ready_count += 1
                    return
                for _ in range(self.config.warmups):
                    if state.stop.is_set():
                        break
                    op = plan.next()
                    request_key = self._request_key(op, round_id)
                    outcome = run_business(session, op, request_key, round_id)
                    self._maybe_retire(session, index, outcome, round_id, record, stats[index], state)
                    self._record_attempt(
                        attempts_path, record, round_id, index, op, outcome, "warmup", state
                    )
                    stats[index].add(outcome, "warmup")
                    if outcome.success:
                        self._ledger_add(outcome, request_key, ledger_confirmed,
                                         ledger_rejected, ledger_unknown, ledger_lock)
                    if outcome.stop_fixture:
                        state.stop.set()
                        break
                with ready_lock:
                    ready_count += 1
                release.wait(timeout=120.0)
                if state.stop.is_set():
                    return
                while not state.stop.is_set():
                    if time.monotonic_ns() >= state.t1_ns:
                        break
                    op = plan.next()
                    request_key = self._request_key(op, round_id)
                    outcome = run_business(session, op, request_key, round_id)
                    self._maybe_retire(session, index, outcome, round_id, record, stats[index], state)
                    self._record_attempt(
                        attempts_path, record, round_id, index, op, outcome, "measure", state
                    )
                    stats[index].add(outcome, "measure")
                    if outcome.success:
                        self._ledger_add(outcome, request_key, ledger_confirmed,
                                         ledger_rejected, ledger_unknown, ledger_lock)
                    if outcome.stop_fixture:
                        state.stop.set()
                        break
            finally:
                close = _close_session(session)
                stats[index].close = close
                self.evidence.append(
                    sessions_path,
                    {"worker": index, "action": "close", **close},
                )

        for index in range(concurrency):
            thread = threading.Thread(target=worker, args=(index,), name="retail-%d" % index)
            thread.start()
            workers.append(thread)

        deadline = time.monotonic() + 300.0
        while True:
            with ready_lock:
                if ready_count >= concurrency:
                    break
            if time.monotonic() > deadline:
                state.stop.set()
                break
            time.sleep(0.01)
        state.t0_ns = time.monotonic_ns()
        state.t1_ns = state.t0_ns + int(self.config.window_seconds * 1_000_000_000)
        record["measure_started_at"] = _iso(time.time_ns())
        release.set()
        for thread in workers:
            thread.join(timeout=self.config.window_seconds + DRAIN_BUDGET_SECONDS + 60.0)
        drain_finished = time.monotonic_ns()
        measure_finished = state.t1_ns

        aborted = state.stop.is_set()
        abort_reason = None
        if aborted:
            abort_reason = "unknown_or_unexpected_terminal"
        open_errors = [item.open_error for item in stats if item.open_error]
        if open_errors:
            aborted = True
            abort_reason = "session_open_failed"
        close_failures = [item.close for item in stats if item.close and not item.close.get("ok")]
        if close_failures:
            aborted = True
            abort_reason = "session_close_failed"

        counts = {
            "workers": concurrency,
            "warmups_per_worker": self.config.warmups,
            "warmups": sum(item.warmups for item in stats),
            "warmup_checkout_acks": sum(item.warmup_checkout_acks for item in stats),
            "measure_attempts": sum(item.measure for item in stats),
            "measure_successes": sum(item.measure_successes for item in stats),
            "warmup_successes": sum(item.warmup_successes for item in stats),
            "terminals": self._merge_terminals(stats),
            "open_errors": open_errors,
        }
        # The three row counts are distinct: after import, after the one
        # preflight checkout, and at the timing window (which also holds the
        # warmup checkout commits, seven rows each).
        table = self.contract.document["matrix"]["row_counts_recorded_separately"]
        observed = record.get("preflight") or {}
        observed_counts = observed.get("counts_before") or {}
        counts["rows_after_import"] = sum(int(value) for value in observed_counts.values())
        counts["rows_expected_after_import"] = int(table["after_import"])
        counts["rows_after_preflight"] = counts["rows_after_import"] + int(
            table["preflight_checkout_rows"]
        )
        counts["rows_at_window_start"] = counts["rows_after_preflight"] + (
            int(table["preflight_checkout_rows"]) * counts["warmup_checkout_acks"]
        )
        sessions = {
            "opened": concurrency - len(open_errors),
            "closed_ok": concurrency - len(close_failures) - len(open_errors),
            "close_failed": len(close_failures),
            "close": [item.close for item in stats],
        }
        return {
            "timeline": {
                "t0_ns": state.t0_ns,
                "t1_ns": state.t1_ns,
                "measure_finished_ns": measure_finished,
                "drain_finished_ns": drain_finished,
                "window_seconds": (state.t1_ns - state.t0_ns) / 1_000_000_000,
                "drain_ms": round((drain_finished - measure_finished) / 1_000_000, 3),
                "measure_started_at": record.get("measure_started_at"),
                "measure_finished_at": _iso(time.time_ns()),
            },
            "counts": counts,
            "sessions": sessions,
            "aborted": aborted,
            "abort_reason": abort_reason,
        }

    @staticmethod
    def _merge_terminals(stats: list[_WorkerStats]) -> dict[str, int]:
        merged: dict[str, int] = {}
        for item in stats:
            for terminal, count in item.terminals.items():
                merged[terminal] = merged.get(terminal, 0) + count
        return merged

    def _request_key(self, op, round_id: str) -> str:
        if op.kind == "checkout":
            return "chk-%s-%010d" % (round_id, op.index)
        return "read-%s-%010d" % (round_id, op.index)

    def _record_attempt(
        self,
        path: str,
        record: dict[str, Any],
        round_id: str,
        worker: int,
        op,
        outcome,
        window: str,
        state: _WindowState,
    ) -> None:
        finished_ns = outcome.trace.finished_ns or time.monotonic_ns()
        finished_wall = time.time_ns()
        started_ns = outcome.trace.started_ns or finished_ns
        entry = {
            "attempt_id": "%s-w%02d-%06d" % (round_id, worker, op.index),
            "business_id": "%s-w%02d-%06d" % (round_id, worker, op.index),
            "round_id": round_id,
            "mode": record.get("mode"),
            "level": record.get("level"),
            "repetition": record.get("repetition"),
            "worker": worker,
            "plan_index": op.index,
            "business": op.kind,
            "params": op.params,
            "window": window,
            "started_ns": started_ns,
            "finished_ns": finished_ns,
            "started_wall_ns": finished_wall - (finished_ns - started_ns),
            "finished_wall_ns": finished_wall,
            "started_at": _iso(finished_wall - (finished_ns - started_ns)),
            "finished_at": _iso(finished_wall),
            "latency_ms": round(max(0, finished_ns - started_ns) / 1_000_000, 6),
            "completed_after_t1": bool(state.t1_ns and finished_ns > state.t1_ns),
            "terminal": outcome.terminal,
            "success": outcome.success,
            "stop_fixture": outcome.stop_fixture,
            "unexpected": outcome.unexpected,
            "http_calls": outcome.trace.calls,
            "call_profile": self.call_profile.name,
            "http_request_ids": outcome.trace.request_ids,
            "bytes_sent": outcome.trace.bytes_sent,
            "bytes_received": outcome.trace.bytes_received,
            "detail": outcome.detail,
            "phases": outcome.trace.phases,
        }
        self.evidence.append(path, entry)

    @staticmethod
    def _ledger_add(
        outcome,
        request_key: str,
        confirmed: list[dict[str, Any]],
        rejected: set[str],
        unknown: set[str],
        lock: threading.Lock,
    ) -> None:
        with lock:
            if outcome.ledger is not None:
                confirmed.append(outcome.ledger)
            if outcome.terminal == Terminal.KNOWN_FAILURE_ROLLED_BACK.value:
                rejected.add(request_key)
            if outcome.terminal in {
                Terminal.SERVICE_UNKNOWN.value,
                Terminal.TRANSPORT_UNKNOWN.value,
                Terminal.CLEANUP_UNCONFIRMED.value,
            }:
                unknown.add(request_key)

    def _maybe_retire(
        self,
        session: Session,
        worker: int,
        outcome,
        round_id: str,
        record: dict[str, Any],
        stats: _WorkerStats,
        state: _WindowState,
    ) -> None:
        """Policy B: one extra independent close after a proven-not-executed rejection.

        The policy never runs for an unknown outcome (``execution_proven_not_started``
        is False there), so the isolation path is unchanged.
        """
        if not should_retire_session(
            self.config.close_policy,
            outcome.terminal,
            bool(getattr(outcome, "execution_proven_not_started", False)),
        ):
            return
        result = session.retire()
        result["worker"] = worker
        result["round_id"] = round_id
        result["stage"] = (outcome.detail or {}).get("stage")
        outcome.detail = dict(outcome.detail or {})
        outcome.detail["retirement"] = result
        with self._cleanup_lock:
            policy = record.setdefault(
                "cleanup_policy",
                {"policy": self.config.close_policy, "retirements": [], "failures": 0},
            )
            policy["retirements"].append(result)
            if not result.get("ok"):
                policy["failures"] += 1
        if not result.get("ok"):
            return
        try:
            session.open()
        except Exception as error:  # noqa: BLE001 - reopening is part of the policy
            stats.open_error = "retire_reopen: %s: %s" % (type(error).__name__, error)
            state.stop.set()

    def run_cleanup_ab(self, repetition: int) -> dict[str, Any]:
        """Contract §九(b): directed A/B of the close-retirement policy."""
        round_id = "r%02d-CLEANUP" % repetition
        relative = "rounds/%s" % round_id
        maximum = int(
            self.contract.document["environment"]["project_quotas"]["max_sessions"]
        )
        started = time.monotonic_ns()
        sides: list[dict[str, Any]] = []
        for policy in ("A", "B"):
            fixture = self._fresh_fixture("%s-%s" % (round_id, policy))
            side: dict[str, Any] = {
                "policy": policy,
                "ops": DEFAULT_PROBE_OPS,
                "known_not_executed": 0,
                "unknown_attempts": 0,
                "retired": 0,
                "retirement_failures": 0,
                "retirements_in_unknown": 0,
                "p95_ms": None,
                "elapsed_ms": 0.0,
                "reason": "",
                "capacity": {"requested": maximum, "opened": 0, "closed": 0, "refused": 0},
                "error": None,
            }
            client: HranaClient | None = None
            session: Session | None = None
            try:
                target = fixture.create()
                fixture.wait_ready(30.0)
                client = HranaClient(target, timeout=5.0)
                session = Session(client)
                session.open()
                side.update(probe_policy(session, policy, DEFAULT_PROBE_OPS))
                side["capacity"] = probe_session_capacity(target, maximum)
            except Exception as error:  # noqa: BLE001 - the A/B record carries the failure
                side["error"] = "%s: %s" % (type(error).__name__, error)
            finally:
                if session is not None:
                    try:
                        session.close()
                    except Exception as close_error:  # noqa: BLE001
                        side["close_error"] = "%s: %s" % (
                            type(close_error).__name__,
                            close_error,
                        )
                if client is not None:
                    client.close_quietly()
                side["fixture"] = {"database": fixture.database, "delete": fixture.delete()}
            sides.append(side)
        comparison = compare_policies(sides[0], sides[1])
        record = {
            "round_id": round_id,
            "round": round_id,
            "kind": "cleanup-ab",
            "repetition": repetition,
            "started_ns": started,
            "duration_ms": round((time.monotonic_ns() - started) / 1_000_000, 3),
            "sides": sides,
            "comparison": comparison,
            "status": "ok" if comparison["verdict"] == "PASS" else "failed",
        }
        self.evidence.write_json("%s/cleanup-ab.json" % relative, record)
        self.evidence.write_json("%s/round.json" % relative, record)
        return record


    def _finish(self, record, relative, fixture, confirmed, rejected, unknown) -> dict[str, Any]:
        if record.get("duration_ms") is None and record.get("started_ns"):
            record["duration_ms"] = round(
                (time.monotonic_ns() - int(record["started_ns"])) / 1_000_000, 3
            )
        record["ledger_snapshot"] = {
            "confirmed": len(confirmed),
            "rejected": len(rejected),
            "unknown": len(unknown),
        }
        fixture_record = record.setdefault("fixture", {})
        if not self.config.keep_fixtures:
            fixture_record["delete"] = fixture.delete()
        else:
            fixture_record["delete"] = {"status": "kept"}
        self._write_round_files(relative, record)
        return record

    def _write_round_files(self, relative: str, record: dict[str, Any]) -> None:
        """Split the round into the per-round files the contract requires.

        ``round.json`` stays the human-readable rollup; the individual files
        below are the machine-checked evidence entries from
        ``benchmark.json.evidence.required_per_round``.
        """
        config = record.get("config") or {}
        verify = record.get("verify") or {}
        self.evidence.write_json("%s/config-before.json" % relative, config.get("before") or {"present": False})
        self.evidence.write_json("%s/config-after.json" % relative, config.get("after") or {"present": False})
        self.evidence.write_json(
            "%s/initial-data-manifest.json" % relative,
            {"dataset": self.dataset.manifest(), "generated_rows": self.dataset.totals()},
        )
        self.evidence.write_json(
            "%s/ledger.json" % relative,
            {
                "ledger": record.get("ledger"),
                "snapshot": record.get("ledger_snapshot"),
            },
        )
        for name in ("pre", "post", "restart"):
            self.evidence.write_json(
                "%s/verify-%s.json" % (relative, name),
                verify.get(name) or {"present": False, "ok": False},
            )
        self.evidence.write_json(
            "%s/shutdown.json" % relative, record.get("shutdown") or {"configured": False}
        )
        self.evidence.write_json(
            "%s/restart.json" % relative, record.get("restart") or {"configured": False}
        )
        self.evidence.write_json("%s/round.json" % relative, record)

    # -- load and recovery rounds -----------------------------------------

    def _fresh_fixture(self, round_id: str) -> Fixture:
        name = _safe_name(
            "%s-%s-%s" % (self.config.fixture.database_prefix, self.config.run_id, round_id)
        )
        return Fixture(self.config.fixture, name, round_id)

    def _load_only(self, record: dict[str, Any], target: Target) -> dict[str, Any]:
        session = _open_session(target, timeout=60.0, profile=self.call_profile)
        try:
            report = load_dataset(session, self.dataset)
        finally:
            record["load_session"] = _close_session(session)
        record["load"] = report.as_dict()
        return report


    def run_load(self, repetition: int) -> dict[str, Any]:
        """L1: one complete 760,004-row load plus the initial stream verify."""
        round_id = "r%02d-L1" % repetition
        relative = "rounds/%s" % round_id
        record: dict[str, Any] = {
            "round_id": round_id,
            "level": "L1",
            "mode": self.config.mode,
            "status": "running",
            "started_ns": time.monotonic_ns(),
        }
        fixture = self._fresh_fixture(round_id)
        try:
            target = fixture.create()
            record["fixture"] = {"database": fixture.database}
            session = _open_session(target, timeout=60.0, profile=self.call_profile)
            try:
                report = load_dataset(session, self.dataset)
            finally:
                record["load_session_close"] = _close_session(session)
            record["load"] = report.as_dict()
            if not report.ok:
                record["status"] = "aborted"
                record["abort_reason"] = "load_failed"
                return self._finish(record, relative, fixture, {}, set(), set())
            # The contract audits the data and the effective configuration in
            # every round, not only in the timed ones.
            record["config"] = {"before": self._audit(target, "before")}
            pre = verify_stream(target, self.dataset, Ledger(), "pre")
            record["verify"] = {"pre": pre}
            record["config"]["after"] = self._audit(target, "after")
            record["status"] = "completed" if pre["ok"] else "aborted"
            if not pre["ok"]:
                record["abort_reason"] = "pre_verification_failed"
            return self._finish(record, relative, fixture, {}, set(), set())
        except PrerequisiteError as error:
            record["status"] = "blocked"
            record["abort_reason"] = str(error)
            return self._finish(record, relative, fixture, {}, set(), set())
        except Exception as error:  # noqa: BLE001 - evidence of a broken fixture
            record["status"] = "aborted"
            record["abort_reason"] = "%s: %s" % (type(error).__name__, error)
            return self._finish(record, relative, fixture, {}, set(), set())

    def run_restart(self, repetition: int) -> dict[str, Any]:
        """H1: clean stop, restart on the same volume, post-restart verify."""
        round_id = "r%02d-H1" % repetition
        relative = "rounds/%s" % round_id
        record: dict[str, Any] = {
            "round_id": round_id,
            "level": "H1",
            "mode": self.config.mode,
            "status": "running",
            "started_ns": time.monotonic_ns(),
        }
        fixture = self._fresh_fixture(round_id)
        try:
            target = fixture.create()
            record["fixture"] = {"database": fixture.database}
            session = _open_session(target, timeout=60.0, profile=self.call_profile)
            try:
                report = load_dataset(session, self.dataset)
            finally:
                record["load_session_close"] = _close_session(session)
            record["load"] = report.as_dict()
            pre = verify_stream(target, self.dataset, Ledger(), "pre")
            record["verify"] = {"pre": pre}
            if not report.ok or not pre["ok"]:
                record["status"] = "aborted"
                record["abort_reason"] = (
                    "load_failed" if not report.ok else "pre_verification_failed"
                )
                return self._finish(record, relative, fixture, {}, set(), set())
            if self.config.lifecycle is None or not self.config.lifecycle.stop:
                record["status"] = "blocked"
                record["abort_reason"] = "lifecycle stop/start commands are not configured"
                return self._finish(record, relative, fixture, {}, set(), set())
            record["config"] = {"before": self._audit(target, "before")}
            record["shutdown"] = shutdown(self.config.lifecycle).as_dict()
            record["restart"] = restart(self.config.lifecycle).as_dict()
            post = verify_stream(target, self.dataset, Ledger(), "restart")
            record["verify"]["restart"] = post
            record["config"]["after"] = self._audit(target, "after")
            clean = record["shutdown"].get("clean_proof", {}).get("matched") is True
            record["status"] = (
                "completed"
                if record["shutdown"].get("ok")
                and clean
                and record["restart"].get("ok")
                and post["ok"]
                else "aborted"
            )
            if record["status"] != "completed":
                record["abort_reason"] = "restart_or_post_restart_verification_failed"
            return self._finish(record, relative, fixture, {}, set(), set())
        except PrerequisiteError as error:
            record["status"] = "blocked"
            record["abort_reason"] = str(error)
            return self._finish(record, relative, fixture, {}, set(), set())
        except Exception as error:  # noqa: BLE001 - evidence of a broken fixture
            record["status"] = "aborted"
            record["abort_reason"] = "%s: %s" % (type(error).__name__, error)
            return self._finish(record, relative, fixture, {}, set(), set())

    # -- idle --------------------------------------------------------------

    def run_idle(self, repetition: int) -> dict[str, Any]:
        round_id = "r%02d-IDLE" % repetition
        relative = "rounds/%s" % round_id
        record: dict[str, Any] = {
            "round_id": round_id,
            "level": "IDLE",
            "status": "running",
            "started_ns": time.monotonic_ns(),
        }
        fixture = Fixture(
            self.config.fixture,
            _safe_name("%s-%s-%s" % (self.config.fixture.database_prefix, self.config.run_id, round_id)),
            round_id,
        )
        try:
            target = fixture.create()
            fixture.wait_ready(30.0)
            session = _open_session(target, timeout=60.0, profile=self.call_profile)
            try:
                load = load_dataset(session, self.dataset)
            finally:
                _close_session(session)
            record["load"] = load.as_dict()
            if not load.ok:
                record["status"] = "aborted"
                record["abort_reason"] = "load_failed"
                return self._finish(record, relative, fixture, [], set(), set())
            pre = verify_stream(target, self.dataset, Ledger(), "pre")
            record["verify"] = {"pre": pre}
            record["config"] = {"before": self._audit(target, "before")}
            deadline_ms = float(
                self.contract.document["environment"]["protocol"]["business_deadline_seconds"]
            ) * 1000.0
            cycles: list[dict[str, Any]] = []
            all_ok = True
            unproven = False
            for cycle in range(1, self.config.idle_cycles + 1):
                quiet_started = time.monotonic_ns()
                time.sleep(self.config.window_seconds)
                quiet_finished = time.monotonic_ns()
                unload = self._unload_probe()
                if unload.get("status") != "unloaded":
                    unproven = True
                session = _open_session(target, profile=self.call_profile)
                started = time.monotonic_ns()
                try:
                    plan = PlanStream(
                        "uniform",
                        seed=int(self.contract.document.get("seed", 20261103)) + 1,
                        products=self.shape.products,
                        warehouses=self.shape.warehouses,
                        customers=self.shape.customers,
                        orders=self.shape.orders,
                    )
                    op = plan.next()
                    outcome = run_business(
                        session, op, "idle-%s-%02d" % (round_id, cycle), round_id
                    )
                    finished = time.monotonic_ns()
                    trigger = {
                        "terminal": outcome.terminal,
                        "success": outcome.success,
                        "latency_ms": round((finished - started) / 1_000_000, 6),
                    }
                finally:
                    _close_session(session)
                cycle_ok = (
                    unload.get("status") == "unloaded"
                    and trigger["success"]
                    and trigger["latency_ms"] <= deadline_ms
                )
                cycles.append(
                    {
                        "cycle": cycle,
                        "quiet_seconds": (quiet_finished - quiet_started) / 1_000_000_000,
                        "unload": unload,
                        "trigger": trigger,
                        "ok": cycle_ok,
                    }
                )
                if not cycle_ok:
                    all_ok = False
                    break
            record["idle"] = {
                "cycles": cycles,
                "cycles_required": self.config.idle_cycles,
                "cycles_completed": len(cycles),
                "ok": all_ok,
                # Rollup of the last observed cycle, kept alongside the list so
                # a reader does not have to index into it.
                "seconds": cycles[-1]["quiet_seconds"] if cycles else 0.0,
                "unload": cycles[-1]["unload"] if cycles else {},
            }
            record["trigger"] = cycles[-1]["trigger"] if cycles else {}
            record["verify"]["post"] = verify_stream(target, self.dataset, Ledger(), "post")
            record["config"]["after"] = self._audit(target, "after")
            record["status"] = "completed" if all_ok else ("invalid" if unproven else "failed")
            if not all_ok:
                record["abort_reason"] = (
                    "worker unload was not proven from service state"
                    if unproven
                    else "idle trigger failed or exceeded the business deadline"
                )
            return self._finish(record, relative, fixture, [], set(), set())
        except Exception as error:  # noqa: BLE001
            record["status"] = "aborted"
            record["abort_reason"] = "%s: %s" % (type(error).__name__, error)
            return self._finish(record, relative, fixture, [], set(), set())

    def _unload_probe(self) -> dict[str, Any]:
        descriptor = self.config.platform_descriptor or {}
        probe = descriptor.get("unload_probe")
        if not probe:
            return {
                "status": "unproven",
                "reason": "descriptor has no unload_probe; worker cold load is not exposed by the SQL protocol",
            }
        command = probe.get("command")
        if not command:
            return {"status": "unproven", "reason": "unload_probe has no command"}
        import subprocess

        try:
            completed = subprocess.run(
                list(command), capture_output=True, text=True, timeout=30, check=False
            )
        except Exception as error:  # noqa: BLE001
            return {"status": "unavailable", "reason": "%s: %s" % (type(error).__name__, error)}
        expect = probe.get("expect")
        expect_matched = expect is None or str(expect) in completed.stdout
        if completed.returncode != 0:
            status = "failed"
        elif not expect_matched:
            status = "unproven"
        else:
            # A zero exit plus the expected token is the only state the scorer
            # accepts as proof that the worker dropped the database.
            status = "unloaded"
        return {
            "status": status,
            "exit_code": completed.returncode,
            "expect": expect,
            "expect_matched": expect_matched,
            "stdout_tail": completed.stdout[-2000:],
            "stderr_tail": completed.stderr[-2000:],
        }


def run_matrix(config: RunnerConfig) -> dict[str, Any]:
    runner = RoundRunner(config)
    contract = runner.contract
    levels = config.levels
    rounds: list[dict[str, Any]] = []
    cleanup_ab: list[dict[str, Any]] = []
    aborted = False
    for repetition in range(1, config.repetitions + 1):
        for level_name, concurrency, mode in levels:
            record = runner.run(repetition, level_name, concurrency, mode)
            rounds.append(record)
            if record["status"] in {"aborted", "blocked"}:
                aborted = True
                break
        if aborted and config.mode == "full":
            break
        if config.mode == "full":
            rounds.append(runner.run_load(repetition))
            rounds.append(runner.run_restart(repetition))
            cleanup_ab.append(runner.run_cleanup_ab(repetition))
        if config.idle or config.mode == "full":
            rounds.append(runner.run_idle(repetition))
    runner.evidence.write_json(
        "run.json",
        {
            "run_id": config.run_id,
            "mode": config.mode,
            "scale": config.scale,
            "repetitions": config.repetitions,
            "window_seconds": config.window_seconds,
            "warmups_per_worker": config.warmups,
            "levels": [{"name": name, "concurrency": c, "mode": m} for name, c, m in levels],
            "call_profile": runner.call_profile.as_dict(),
            "driver_sha256": driver_digest(),
            "contract": {
                "path": str(contract.path),
                "sha256": contract.sha256,
                "version": contract.version,
                "profile": contract.profile,
                "document": contract.document,
            },
            "fixture": {
                "tenant": config.fixture.tenant,
                "project": config.fixture.project,
                "endpoint": config.fixture.endpoint,
            },
            "platform_descriptor": _redact(config.platform_descriptor),
            # Cleanup A/B rounds live under rounds/ too; listing them keeps the
            # scorer's stray-evidence check exact instead of exempting a prefix.
            "rounds": [item["round_id"] for item in rounds]
            + [item["round_id"] for item in cleanup_ab],
            "cleanup_ab": [
                {
                    "round_id": item["round_id"],
                    "verdict": item["comparison"]["verdict"],
                    "violations": item["comparison"]["violations"],
                }
                for item in cleanup_ab
            ],
        },
    )
    runner.evidence.finalize()
    return {"run_id": config.run_id, "rounds": rounds}
