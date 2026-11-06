"""Synthetic evidence bundles used by the scorer tests.

The bundles are produced through :class:`pl_retail.evidence.EvidenceWriter`, so
the manifest, digests and record counts are the real ones.  Only the payloads
are synthetic: the point is to make the scorer's pass and negative paths
reproducible without a 120-second live window.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from pl_retail.call_profiles import (  # noqa: E402
    DEFAULT_PROFILE_NAME,
    PROFILES,
    CallProfile,
    resolve_profile,
)
from pl_retail.contract import (  # noqa: E402
    driver_digest,
    load_contract,
)
from pl_retail.evidence import EvidenceWriter  # noqa: E402

WINDOW_NS = 120_000_000_000
T0 = 1_000_000_000_000

READ_PHASES = ("begin", "query", "commit")
AUTOCOMMIT_READ_PHASES = ("query",)
CHECKOUT_PHASES = (
    "begin_immediate",
    "prices",
    "stock_1",
    "stock_2",
    "stock_3",
    "stock_4",
    "stock_5",
    "wallet",
    "warehouse",
    "order_insert",
    "order_lines",
    "payment_insert",
    "commit",
)
MERGED_CHECKOUT_PHASES = (
    "begin_immediate",
    "prices",
    "stock_merged",
    "wallet",
    "warehouse",
    "order_insert",
    "order_lines",
    "payment_insert",
    "commit",
)
HTTP_CALLS = dict(PROFILES[DEFAULT_PROFILE_NAME].expected_http_calls)
BUSINESS = tuple(HTTP_CALLS)


def _phases(
    business: str,
    attempt_id: str,
    profile: CallProfile,
    defects: set[str],
) -> list[dict[str, Any]]:
    if business == "checkout":
        labels = (
            MERGED_CHECKOUT_PHASES if profile.stock_mode == "merged" else CHECKOUT_PHASES
        )
    elif profile.read_mode == "autocommit":
        # ``autocommit_commit_tail`` is the negative twin of the normal shape:
        # it keeps the one-call count but ends the attempt on a commit label.
        labels = (
            ("commit",)
            if "autocommit_commit_tail" in defects
            else AUTOCOMMIT_READ_PHASES
        )
    else:
        labels = READ_PHASES
    return [
        {
            "label": label,
            "status": 200,
            "code": "",
            "latency_ms": 0.5,
            "request_id": "%s-%02d" % (attempt_id, index),
        }
        for index, label in enumerate(labels)
    ]


def _attempt(
    round_id: str,
    worker: int,
    index: int,
    business: str,
    window: str,
    started_ns: int,
    latency_ms: float,
    profile: CallProfile,
    defects: set[str],
    terminal: str = "read_ok",
) -> dict[str, Any]:
    attempt_id = "%s-w%02d-%06d" % (round_id, worker, index)
    success = terminal in {"read_ok", "committed_ack"}
    phases = _phases(business, attempt_id, profile, defects)
    if not success:
        phases = phases[:-1] + [
            {"label": "commit", "status": 400, "code": "known_failure", "latency_ms": 1.0,
             "request_id": "%s-bad" % attempt_id}
        ]
    return {
        "attempt_id": attempt_id,
        "business_id": attempt_id,
        "round_id": round_id,
        "level": round_id.split("-", 1)[1],
        "repetition": 1,
        "worker": worker,
        "plan_index": index,
        "business": business,
        "params": {},
        "window": window,
        "started_ns": started_ns,
        "finished_ns": started_ns + int(latency_ms * 1_000_000),
        "latency_ms": latency_ms,
        "terminal": terminal,
        "success": success,
        "stop_fixture": False,
        "unexpected": False,
        "http_calls": len(phases),
        "call_profile": profile.name,
        "http_request_ids": [phase["request_id"] for phase in phases],
        "bytes_sent": 10,
        "bytes_received": 10,
        "detail": {},
        "phases": phases,
    }


def _config(contract) -> dict[str, Any]:
    audit = {
        "ok": True,
        "violations": [],
        "gaps": [],
        "checks": 12,
        "schema": {"ok": True},
        "oracle": {"ok": True},
    }
    return {"before": audit, "after": dict(audit)}


def _verify() -> dict[str, Any]:
    return {
        "ok": True,
        "invariants": {"violations": []},
        "ledger": {"missing": [], "unexpected": [], "rejected_absent": True},
        "aggregates": {"payments_total_cents": 47551602679},
    }


def _round_record(round_id: str, level: str, contract, *, repetitions: int) -> dict[str, Any]:
    record = {
        "round_id": round_id,
        "level": level,
        "status": "completed",
        "repetition": int(round_id[1:3]),
        "mode": "full",
        "timeline": {"t0_ns": T0, "t1_ns": T0 + WINDOW_NS},
        "config": _config(contract),
        "verify": {
            "pre": _verify(),
            "post": _verify(),
            "restart": _verify(),
        },
        "ledger": {"confirmed": 5, "seen": 5, "missing": [], "unexpected": [], "rejected_absent": True},
    }
    if level in {"U1", "U4", "U8", "U16", "H8"}:
        record["shutdown"] = {
            "configured": True,
            "ok": True,
            "status": "stopped",
            "duration_ms": 1200,
            "clean_proof": {"matched": True},
        }
        record["restart"] = {
            "configured": True,
            "ok": True,
            "status": "ready",
            "duration_ms": 1500,
            "ready": {"status": 200},
        }
    elif level == "H1":
        record["shutdown"] = {
            "configured": True,
            "ok": True,
            "status": "stopped",
            "duration_ms": 1200,
            "clean_proof": {"matched": True},
        }
        record["restart"] = {
            "configured": True,
            "ok": True,
            "status": "ready",
            "duration_ms": 1500,
            "ready": {"status": 200},
        }
        record["verify"]["post"] = {"present": False}
    elif level == "L1":
        record["load"] = {
            "ok": True,
            "rows": dict(contract.document["dataset"]["tables"]),
            "elapsed_ms": 1000.0,
        }
        record["verify"]["restart"] = {"present": False}
    elif level == "IDLE":
        cycles = [
            {
                "cycle": index + 1,
                "quiet_seconds": 120.5,
                "unload": {"status": "unloaded"},
                "trigger": {"success": True, "latency_ms": 40.0, "terminal": "read_ok"},
                "ok": True,
            }
            for index in range(10)
        ]
        record["idle"] = {
            "cycles": cycles,
            "cycles_required": 10,
            "cycles_completed": 10,
            "ok": True,
            "seconds": 120.5,
            "unload": {"status": "unloaded"},
        }
        record["trigger"] = {"success": True, "latency_ms": 40.0, "terminal": "read_ok"}
        record["verify"]["restart"] = {"present": False}
    return record


def _write_round(
    writer: EvidenceWriter,
    round_id: str,
    level: str,
    contract,
    profile: CallProfile,
    *,
    successes_per_class: int = 400,
    defects: set[str] | None = None,
) -> None:
    defects = defects or set()
    directory = "rounds/%s" % round_id
    record = _round_record(round_id, level, contract, repetitions=0)
    if "missing_window" in defects:
        record.pop("timeline", None)
    writer.write_json("%s/round.json" % directory, record)
    writer.write_json("%s/config-before.json" % directory, record["config"]["before"])
    writer.write_json("%s/config-after.json" % directory, record["config"]["after"])
    writer.write_json(
        "%s/initial-data-manifest.json" % directory,
        {"dataset": contract.document["dataset"], "generated_rows": contract.document["dataset"]["tables"]},
    )
    writer.write_json("%s/ledger.json" % directory, {"ledger": record["ledger"]})
    for name in ("pre", "post", "restart"):
        writer.write_json("%s/verify-%s.json" % (directory, name), record["verify"][name])
    writer.write_json("%s/shutdown.json" % directory, record.get("shutdown") or {"configured": False})
    writer.write_json("%s/restart.json" % directory, record.get("restart") or {"configured": False})
    if level not in {"U1", "U4", "U8", "U16", "H8"}:
        writer.append("%s/attempts.jsonl" % directory, {"attempt_id": round_id + "-none"})
        writer.append("%s/resource.jsonl" % directory, {"skip": True})
        writer.append("%s/http.jsonl" % directory, {"request_id": round_id + "-none"})
        writer.append("%s/sessions.jsonl" % directory, {"worker": 0, "action": "close", "ok": True})
        return

    concurrency = int(contract.document["matrix"]["levels"][level]["concurrency"])
    warmups = int(contract.document["matrix"]["warmups_per_worker"]) * concurrency
    attempts: list[dict[str, Any]] = []
    for index in range(warmups):
        business = BUSINESS[index % len(BUSINESS)]
        attempts.append(
            _attempt(
                round_id,
                0,
                index,
                business,
                "warmup",
                T0 - 10_000_000_000 + index,
                5.0,
                profile,
                defects,
                "committed_ack" if business == "checkout" else "read_ok",
            )
        )
    measure_start = T0 + 1_000_000
    step = 1_000_000
    index = warmups
    for business in BUSINESS:
        for offset in range(successes_per_class):
            if "empty_success" in defects:
                continue
            latency = 2.0 + (offset % 10) * 1.5
            if "slo_breach" in defects and offset % 50 == 0:
                # A single outlier cannot move a p99 over 1,200 samples; the
                # breach has to look like a real tail.
                latency = 2500.0
            attempts.append(
                _attempt(
                    round_id,
                    0,
                    index,
                    business,
                    "measure",
                    measure_start + (index - warmups) * step,
                    latency,
                    profile,
                    defects,
                    "committed_ack" if business == "checkout" else "read_ok",
                )
            )
            index += 1
    if "unknown_terminal" in defects:
        attempts.append(
            _attempt(
                round_id,
                0,
                index,
                "checkout",
                "measure",
                measure_start + (index - warmups) * step,
                3.0,
                profile,
                defects,
                "service_unknown",
            )
        )
    if "bad_http_calls" in defects:
        attempts.append(
            {
                **_attempt(
                    round_id,
                    0,
                    index + 2,
                    "checkout",
                    "measure",
                    measure_start + 10,
                    4.0,
                    profile,
                    defects,
                    "committed_ack",
                ),
                "http_calls": 12,
            }
        )
    for attempt in attempts:
        writer.append("%s/attempts.jsonl" % directory, attempt)
        for phase in attempt.get("phases", []):
            writer.append(
                "%s/http.jsonl" % directory,
                {"request_id": phase["request_id"], "round_id": round_id, "status": phase["status"]},
            )
    writer.append("%s/resource.jsonl" % directory, {"at_ms": 0, "cpu": 1.0})
    writer.append("%s/sessions.jsonl" % directory, {"worker": 0, "action": "close", "ok": True})


def build_bundle(
    root: Path,
    *,
    mode: str = "full",
    repetitions: int | None = None,
    defects: set[str] | None = None,
    include_cleanup_ab: bool = True,
    call_profile: str = DEFAULT_PROFILE_NAME,
) -> Path:
    """Write a complete synthetic evidence bundle and return its root."""
    defects = defects or set()
    contract = load_contract(None)
    profile = resolve_profile(call_profile)
    # ``baseline_calls`` keeps the attempts at the frozen 3/13 shape while
    # run.json still declares the requested profile: the contradiction the
    # scorer must reject.
    attempt_profile = (
        resolve_profile(DEFAULT_PROFILE_NAME)
        if "baseline_calls" in defects
        else profile
    )
    repetitions = repetitions if repetitions is not None else int(
        contract.document["matrix"]["repetitions"]
    )
    writer = EvidenceWriter(root)
    rounds: list[str] = []
    levels = list(contract.document["matrix"]["levels"])
    for repetition in range(1, repetitions + 1):
        if "missing_repetition" in defects and repetition == repetitions:
            # A missing repetition must be missing on disk, not just absent
            # from run.json: the scorer derives the matrix from the contract.
            continue
        for level in levels:
            round_id = "r%02d-%s" % (repetition, level)
            rounds.append(round_id)
            _write_round(writer, round_id, level, contract, attempt_profile, defects=defects)
        for level in ("L1", "H1", "IDLE"):
            round_id = "r%02d-%s" % (repetition, level)
            rounds.append(round_id)
            _write_round(writer, round_id, level, contract, attempt_profile, defects=defects)
    if include_cleanup_ab and mode == "full":
        for repetition in range(1, repetitions + 1):
            round_id = "r%02d-CLEANUP" % repetition
            document = {
                "round_id": round_id,
                "kind": "cleanup-ab",
                "status": "ok",
                "comparison": {
                    "verdict": "PASS",
                    "violations": [],
                    "scope": {"unknown_isolation_unchanged": True},
                },
            }
            # The runner writes both documents for a cleanup round, so the
            # synthetic bundle must too: run.json has to list the directory or
            # the stray-evidence check would reject a faithful run.
            writer.write_json("rounds/%s/cleanup-ab.json" % round_id, document)
            writer.write_json("rounds/%s/round.json" % round_id, document)
            rounds.append(round_id)
    run_rounds = list(rounds)
    if "missing_repetition" in defects and repetitions > 1:
        run_rounds = [item for item in run_rounds if not item.startswith("r%02d-" % repetitions)]
    run = {
        "run_id": "synthetic",
        "mode": mode,
        "scale": 1.0,
        "repetitions": repetitions,
        "window_seconds": 120.0,
        "warmups_per_worker": int(contract.document["matrix"]["warmups_per_worker"]),
        "levels": [{"name": name, "concurrency": int(contract.document["matrix"]["levels"][name]["concurrency"]), "mode": contract.document["matrix"]["levels"][name]["mode"]} for name in levels],
        "call_profile": profile.as_dict(),
        "driver_sha256": driver_digest(),
        "contract": {
            "path": str(contract.path),
            "sha256": contract.sha256,
            "version": contract.version,
            "profile": contract.profile,
            "document": contract.document,
        },
        "rounds": run_rounds,
        "cleanup_ab": [],
    }
    writer.write_json("run.json", run)
    writer.finalize()
    return root


def rewrite_manifest_without(root: Path, relative: str) -> None:
    """Drop one file from the manifest while leaving the bytes on disk."""
    manifest_path = root / "evidence-manifest.json"
    document = json.loads(manifest_path.read_text(encoding="utf-8"))
    document["files"] = [entry for entry in document["files"] if entry.get("path") != relative]
    manifest_path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def corrupt_file(root: Path, relative: str) -> None:
    path = root / relative
    path.write_bytes(path.read_bytes() + b"\n")
