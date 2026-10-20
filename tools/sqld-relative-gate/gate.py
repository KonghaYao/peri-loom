"""Fail-closed reconciliation gate for the five-scenario sqld comparison.

The gate consumes raw per-request latency records and recomputes percentiles.
It never measures, and it never trusts a collector-provided summary. A verdict
is only usable when the evidence is structurally complete and the G0
correctness gate is explicitly passed.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

TOOL = "sqld-relative-gate"
SCHEMA_VERSION = "sqld-relative-gate/1"

SCENARIOS = (
    "single_read",
    "single_write",
    "sequential_read_50rps",
    "mixed_read",
    "mixed_write",
)
ARMS = ("peri", "sqld")
BACKENDS = ("native", "s3")
CACHES = ("cold", "warm")
MAINTENANCE_STATES = ("on", "off")
ERROR_KEYS = ("rejected", "failed", "unknown", "excluded")

# Three repetitions per arm/scenario is the smallest set that can expose a
# single lucky or unlucky round. The sample floor is a conservative guard
# against "three rounds" built from a handful of requests.
MIN_ROUNDS = 3
MIN_SAMPLES_PER_ROUND = 30
RATIO_NUMERATOR = 3
RATIO_DENOMINATOR = 2

EXIT_CODES = {"PASS": 0, "FAIL": 1, "INVALID": 2, "HOLD": 3}

_SHA1 = re.compile(r"[0-9a-f]{40}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_IMAGE_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


@dataclass(frozen=True)
class Run:
    run_id: str
    arm: str
    scenario: str
    round: int
    planned_requests: int
    samples_ns: tuple[int, ...]
    errors: dict[str, int]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_sha1(value: Any) -> bool:
    return isinstance(value, str) and _SHA1.fullmatch(value) is not None


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and _SHA256.fullmatch(value) is not None


def percentile_ns(samples: list[int] | tuple[int, ...], percentile: int) -> int:
    """Nearest-rank percentile over raw nanosecond samples."""
    if not samples:
        raise ValueError("percentile_ns requires at least one sample")
    ordered = sorted(samples)
    rank = (percentile * len(ordered) + 99) // 100
    rank = min(max(rank, 1), len(ordered))
    return ordered[rank - 1]


def _stats(samples: list[int] | tuple[int, ...]) -> dict[str, int]:
    return {
        "samples": len(samples),
        "p50_ns": percentile_ns(samples, 50),
        "p95_ns": percentile_ns(samples, 95),
        "p99_ns": percentile_ns(samples, 99),
    }


def _validate_identity(value: Any, path: str, errors: list[str]) -> None:
    if not isinstance(value, dict):
        errors.append(f"{path}: expected object with ref and sha256")
        return
    if not _non_empty_str(value.get("ref")):
        errors.append(f"{path}.ref: expected non-empty string")
    if not _is_sha256(value.get("sha256")):
        errors.append(f"{path}.sha256: expected 64 lowercase hex characters")


def _validate_metadata(
    metadata: Any,
    errors: list[str],
    *,
    expect_peri_sha: str | None,
    expect_turso_sha: str | None,
) -> None:
    if not isinstance(metadata, dict):
        errors.append("metadata: expected object")
        return

    for key in ("peri_sha", "turso_submodule_sha"):
        value = metadata.get(key)
        if not _is_sha1(value):
            errors.append(f"metadata.{key}: expected 40 lowercase hex characters")
            continue
        expected = expect_peri_sha if key == "peri_sha" else expect_turso_sha
        if expected is not None and value != expected:
            errors.append(f"metadata.{key}: expected {expected}, got {value}")

    digest = metadata.get("sqld_image_digest")
    if not isinstance(digest, str) or _IMAGE_DIGEST.fullmatch(digest) is None:
        errors.append("metadata.sqld_image_digest: expected sha256:<64 lowercase hex>")

    if not _non_empty_str(metadata.get("arch")):
        errors.append("metadata.arch: expected non-empty string")

    hardware = metadata.get("hardware")
    if not isinstance(hardware, dict):
        errors.append("metadata.hardware: expected object")
        return
    for key in ("cpu", "disk", "network"):
        if not _non_empty_str(hardware.get(key)):
            errors.append(f"metadata.hardware.{key}: expected non-empty string")
    memory = hardware.get("memory_bytes")
    if not _is_int(memory) or memory <= 0:
        errors.append("metadata.hardware.memory_bytes: expected positive integer")


def _validate_campaign(campaign: Any, errors: list[str]) -> int | None:
    if not isinstance(campaign, dict):
        errors.append("campaign: expected object")
        return None

    if not _non_empty_str(campaign.get("id")):
        errors.append("campaign.id: expected non-empty string")
    for key, allowed in (
        ("backend", BACKENDS),
        ("cache", CACHES),
        ("maintenance", MAINTENANCE_STATES),
    ):
        if campaign.get(key) not in allowed:
            errors.append(f"campaign.{key}: expected one of {list(allowed)}")

    tls = campaign.get("tls")
    if not isinstance(tls, dict):
        errors.append("campaign.tls: expected object")
    else:
        if tls.get("enabled") is not True:
            errors.append("campaign.tls.enabled: performance acceptance requires TLS")
        if not _non_empty_str(tls.get("profile")):
            errors.append("campaign.tls.profile: expected non-empty string")

    if not _non_empty_str(campaign.get("durability")):
        errors.append("campaign.durability: expected non-empty string")
    _validate_identity(campaign.get("dataset"), "campaign.dataset", errors)
    _validate_identity(campaign.get("request_script"), "campaign.request_script", errors)

    rounds = campaign.get("rounds")
    if not _is_int(rounds) or rounds < MIN_ROUNDS:
        errors.append(f"campaign.rounds: expected integer >= {MIN_ROUNDS}")
        return None
    return rounds


def _validate_correctness(correctness: Any, errors: list[str]) -> bool | None:
    if not isinstance(correctness, dict):
        errors.append("correctness: expected object (G0 evidence is mandatory)")
        return None

    g0_passed = correctness.get("g0_passed")
    if not isinstance(g0_passed, bool):
        errors.append("correctness.g0_passed: expected boolean")
        g0_passed = None

    unknown = correctness.get("unknown_results")
    if not _is_int(unknown) or unknown < 0:
        errors.append("correctness.unknown_results: expected non-negative integer")
    elif unknown != 0:
        errors.append(
            "correctness.unknown_results: must be 0; unknown outcomes cannot enter "
            "performance acceptance"
        )

    for key in ("oracle", "evidence"):
        if not _non_empty_str(correctness.get(key)):
            errors.append(f"correctness.{key}: expected non-empty string")
    return g0_passed


def _check_summary(
    raw: dict[str, Any],
    label: str,
    samples: tuple[int, ...],
    warnings: list[str],
) -> None:
    summary = raw.get("summary")
    if summary is None:
        return
    if not isinstance(summary, dict):
        warnings.append(f"{label}: summary ignored (not an object); percentiles recomputed")
        return

    recomputed = _stats(samples)
    mismatches = []
    for key in ("p50_ns", "p95_ns", "p99_ns"):
        if key in summary and summary[key] != recomputed[key]:
            mismatches.append(f"{key} provided={summary[key]!r} recomputed={recomputed[key]}")
    if mismatches:
        warnings.append(f"{label}: summary ignored; " + "; ".join(mismatches))


def _validate_runs(
    runs_raw: Any,
    campaign: Any,
    rounds: int | None,
    errors: list[str],
    warnings: list[str],
) -> list[Run]:
    if not isinstance(runs_raw, list) or not runs_raw:
        errors.append("runs: expected a non-empty array")
        return []

    expected_dims = {}
    if isinstance(campaign, dict):
        expected_dims = {key: campaign.get(key) for key in ("backend", "cache", "maintenance")}

    runs: list[Run] = []
    seen_ids: set[str] = set()
    for index, raw in enumerate(runs_raw):
        path = f"runs[{index}]"
        before = len(errors)

        if not isinstance(raw, dict):
            errors.append(f"{path}: expected object")
            continue

        run_id = raw.get("run_id")
        if not _non_empty_str(run_id):
            errors.append(f"{path}.run_id: expected non-empty string")
        elif run_id in seen_ids:
            errors.append(f"{path}.run_id: duplicate {run_id!r}")
        else:
            seen_ids.add(run_id)

        arm = raw.get("arm")
        if arm not in ARMS:
            errors.append(f"{path}.arm: expected one of {list(ARMS)}")

        scenario = raw.get("scenario")
        if scenario not in SCENARIOS:
            errors.append(f"{path}.scenario: expected one of {list(SCENARIOS)}")

        round_no = raw.get("round")
        if not _is_int(round_no) or round_no < 1:
            errors.append(f"{path}.round: expected integer >= 1")
        elif rounds is not None and round_no > rounds:
            errors.append(f"{path}.round: {round_no} exceeds campaign.rounds {rounds}")

        planned = raw.get("planned_requests")
        if not _is_int(planned) or planned < MIN_SAMPLES_PER_ROUND:
            errors.append(
                f"{path}.planned_requests: expected integer >= {MIN_SAMPLES_PER_ROUND}"
            )

        dimensions = raw.get("dimensions")
        if not isinstance(dimensions, dict):
            errors.append(f"{path}.dimensions: expected object")
        else:
            for key, expected in expected_dims.items():
                if dimensions.get(key) != expected:
                    errors.append(
                        f"{path}.dimensions.{key}: {dimensions.get(key)!r} does not match "
                        f"campaign {expected!r}; pools must not be mixed"
                    )

        samples = raw.get("samples_ns")
        samples_valid = None
        if not isinstance(samples, list):
            errors.append(f"{path}.samples_ns: expected array of raw nanosecond integers")
        elif any(not _is_int(sample) or sample <= 0 for sample in samples):
            errors.append(f"{path}.samples_ns: every sample must be a positive integer")
        else:
            samples_valid = tuple(samples)
            if len(samples_valid) < MIN_SAMPLES_PER_ROUND:
                errors.append(
                    f"{path}.samples_ns: expected at least {MIN_SAMPLES_PER_ROUND} samples, "
                    f"got {len(samples_valid)}"
                )

        errors_raw = raw.get("errors")
        counts_valid = None
        if not isinstance(errors_raw, dict):
            errors.append(f"{path}.errors: expected object")
        else:
            missing = [key for key in ERROR_KEYS if key not in errors_raw]
            extra = [key for key in errors_raw if key not in ERROR_KEYS]
            if missing:
                errors.append(f"{path}.errors: missing counters {missing}")
            if extra:
                errors.append(
                    f"{path}.errors: unclassified counters {extra}; map them to "
                    f"{list(ERROR_KEYS)}"
                )
            counts = {}
            for key in ERROR_KEYS:
                value = errors_raw.get(key)
                if not _is_int(value) or value < 0:
                    errors.append(f"{path}.errors.{key}: expected non-negative integer")
                else:
                    counts[key] = value
            if len(counts) == len(ERROR_KEYS):
                counts_valid = counts
                if counts["excluded"] > 0:
                    errors.append(f"{path}.errors.excluded: samples must not be excluded")
                if counts["unknown"] > 0:
                    errors.append(
                        f"{path}.errors.unknown: unsettled Unknown results cannot be accepted"
                    )
                if counts["rejected"] > 0:
                    errors.append(f"{path}.errors.rejected: rejected requests invalidate the run")
                if counts["failed"] > 0:
                    errors.append(f"{path}.errors.failed: failed requests invalidate the run")
                if samples_valid is not None and _is_int(planned):
                    total = len(samples_valid) + sum(counts.values())
                    if total != planned:
                        errors.append(
                            f"{path}: request accounting does not settle "
                            f"(samples={len(samples_valid)}, errors={counts}, planned={planned})"
                        )

        if samples_valid is not None:
            _check_summary(raw, run_id if _non_empty_str(run_id) else path, samples_valid, warnings)

        if len(errors) == before:
            runs.append(
                Run(
                    run_id=run_id,
                    arm=arm,
                    scenario=scenario,
                    round=round_no,
                    planned_requests=planned,
                    samples_ns=samples_valid,
                    errors=counts_valid,
                )
            )
    return runs


def _group_runs(
    runs: list[Run], rounds: int | None, errors: list[str]
) -> dict[tuple[str, str], dict[int, Run]]:
    groups: dict[tuple[str, str], dict[int, Run]] = {}
    for run in runs:
        bucket = groups.setdefault((run.scenario, run.arm), {})
        if run.round in bucket:
            errors.append(f"{run.scenario}/{run.arm}: duplicate round {run.round}")
            continue
        bucket[run.round] = run

    for scenario in SCENARIOS:
        planned_values = set()
        for arm in ARMS:
            bucket = groups.get((scenario, arm), {})
            if rounds is not None:
                expected = set(range(1, rounds + 1))
                actual = set(bucket)
                if actual != expected:
                    missing = sorted(expected - actual)
                    extra = sorted(actual - expected)
                    errors.append(
                        f"{scenario}/{arm}: rounds do not match campaign.rounds={rounds} "
                        f"(missing={missing}, extra={extra})"
                    )
            elif not bucket:
                errors.append(f"{scenario}/{arm}: no runs")
            for run in bucket.values():
                planned_values.add(run.planned_requests)
        if len(planned_values) > 1:
            errors.append(
                f"{scenario}: planned_requests differs between runs {sorted(planned_values)}"
            )
    return groups


def _arm_summary(runs: list[Run]) -> dict[str, Any]:
    ordered_runs = sorted(runs, key=lambda run: run.round)
    samples = tuple(sample for run in ordered_runs for sample in run.samples_ns)
    result = {
        "rounds": len(ordered_runs),
        **_stats(samples),
        "per_round": [
            {"round": run.round, **_stats(run.samples_ns)} for run in ordered_runs
        ],
    }
    return result


def _scenario_results(groups: dict[tuple[str, str], dict[int, Run]]) -> list[dict[str, Any]]:
    results = []
    for scenario in SCENARIOS:
        peri_runs = list(groups.get((scenario, "peri"), {}).values())
        sqld_runs = list(groups.get((scenario, "sqld"), {}).values())
        if not peri_runs or not sqld_runs:
            continue

        peri = _arm_summary(peri_runs)
        sqld = _arm_summary(sqld_runs)
        peri_p50 = peri["p50_ns"]
        sqld_p50 = sqld["p50_ns"]
        passed = peri_p50 * RATIO_DENOMINATOR <= sqld_p50 * RATIO_NUMERATOR
        planned = sorted({run.planned_requests for run in peri_runs + sqld_runs})
        results.append(
            {
                "scenario": scenario,
                "planned_requests_per_round": planned[0] if len(planned) == 1 else None,
                "peri": peri,
                "sqld": sqld,
                "ratio": peri_p50 / sqld_p50,
                "limit_p50_ns": (sqld_p50 * RATIO_NUMERATOR) // RATIO_DENOMINATOR,
                "passed": passed,
                "accepted": False,
            }
        )
    return results


def _base_verdict(
    status: str,
    *,
    input_path: str | None,
    campaign: Any = None,
    metadata: Any = None,
    correctness: Any = None,
    checks: dict[str, Any] | None = None,
    scenarios: list[dict[str, Any]] | None = None,
    failures: list[dict[str, Any]] | None = None,
    errors: list[str] | None = None,
    warnings: list[str] | None = None,
    evidence_gaps: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "tool": TOOL,
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "generated_at": _now(),
        "input": input_path,
        "campaign": campaign,
        "metadata": metadata,
        "correctness": correctness,
        "checks": checks or {"rounds_required": MIN_ROUNDS},
        "scenarios": scenarios or [],
        "failures": failures or [],
        "errors": errors or [],
        "warnings": warnings or [],
        "evidence_gaps": evidence_gaps or [],
    }


def _invalid(reason: str, *, input_path: str | None = None) -> dict[str, Any]:
    return _base_verdict("INVALID", input_path=input_path, errors=[reason])


def hold_no_evidence(
    input_path: str | None = None, reason: str = "no evidence supplied"
) -> dict[str, Any]:
    return _base_verdict(
        "HOLD",
        input_path=input_path,
        evidence_gaps=[
            reason,
            "raw per-request nanosecond records for all five scenarios are required",
            "G0 correctness evidence must be passed before performance acceptance",
        ],
    )


def evaluate(
    data: Any,
    *,
    input_path: str | None = None,
    expect_peri_sha: str | None = None,
    expect_turso_sha: str | None = None,
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []

    if not isinstance(data, dict):
        return _invalid("root: expected JSON object", input_path=input_path)

    if data.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version: expected {SCHEMA_VERSION!r}")

    campaign = data.get("campaign")
    rounds = _validate_campaign(campaign, errors)

    metadata = data.get("metadata")
    _validate_metadata(
        metadata,
        errors,
        expect_peri_sha=expect_peri_sha,
        expect_turso_sha=expect_turso_sha,
    )

    correctness = data.get("correctness")
    g0_passed = _validate_correctness(correctness, errors)

    runs_raw = data.get("runs")
    runs = _validate_runs(runs_raw, campaign, rounds, errors, warnings)

    checks = {
        "rounds_required": MIN_ROUNDS,
        "rounds_declared": rounds,
        "runs_received": len(runs_raw) if isinstance(runs_raw, list) else 0,
        "runs_valid": len(runs),
    }

    if errors:
        return _base_verdict(
            "INVALID",
            input_path=input_path,
            campaign=campaign,
            metadata=metadata,
            correctness=correctness,
            checks=checks,
            errors=errors,
            warnings=warnings,
        )

    groups = _group_runs(runs, rounds, errors)
    if errors:
        return _base_verdict(
            "INVALID",
            input_path=input_path,
            campaign=campaign,
            metadata=metadata,
            correctness=correctness,
            checks=checks,
            errors=errors,
            warnings=warnings,
        )

    scenarios = _scenario_results(groups)
    if len(scenarios) != len(SCENARIOS):
        return _base_verdict(
            "INVALID",
            input_path=input_path,
            campaign=campaign,
            metadata=metadata,
            correctness=correctness,
            checks=checks,
            errors=["scenarios: not all five scenarios produced comparable arm records"],
            warnings=warnings,
        )

    if g0_passed is False:
        return _base_verdict(
            "HOLD",
            input_path=input_path,
            campaign=campaign,
            metadata=metadata,
            correctness=correctness,
            checks=checks,
            scenarios=scenarios,
            failures=[{"reason": "g0_not_passed"}],
            warnings=warnings,
            evidence_gaps=[
                "G0 correctness gate is not passed; latency ratios are informational only",
                "rerun the independent oracle and set correctness.g0_passed=true with evidence",
            ],
        )

    failed = [scenario for scenario in scenarios if not scenario["passed"]]
    for scenario in scenarios:
        scenario["accepted"] = True
    status = "FAIL" if failed else "PASS"
    failures = [
        {
            "scenario": scenario["scenario"],
            "reason": "p50_ratio_exceeds_1.5x",
            "ratio": scenario["ratio"],
            "limit_p50_ns": scenario["limit_p50_ns"],
        }
        for scenario in failed
    ]
    return _base_verdict(
        status,
        input_path=input_path,
        campaign=campaign,
        metadata=metadata,
        correctness=correctness,
        checks=checks,
        scenarios=scenarios,
        failures=failures,
        warnings=warnings,
    )


def _ms(ns: int) -> str:
    return f"{ns / 1_000_000:.3f}"


def _markdown_verdict_label(scenario: dict[str, Any]) -> str:
    if not scenario["accepted"]:
        return "BLOCKED"
    return "PASS" if scenario["passed"] else "FAIL"


def render_markdown(verdict: dict[str, Any]) -> str:
    lines = [f"# sqld relative gate: {verdict['status']}", ""]
    lines.append(f"- Generated: {verdict['generated_at']}")
    if verdict.get("input"):
        lines.append(f"- Input: `{verdict['input']}`")

    campaign = verdict.get("campaign")
    if isinstance(campaign, dict):
        tls = campaign.get("tls")
        tls_label = tls.get("profile") if isinstance(tls, dict) else None
        lines.append(
            "- Campaign: "
            f"id={campaign.get('id')}, backend={campaign.get('backend')}, "
            f"cache={campaign.get('cache')}, maintenance={campaign.get('maintenance')}, "
            f"tls={tls_label}, durability={campaign.get('durability')}, "
            f"rounds={campaign.get('rounds')}"
        )

    metadata = verdict.get("metadata")
    if isinstance(metadata, dict):
        lines.append(
            "- Build: "
            f"peri={metadata.get('peri_sha')}, "
            f"turso={metadata.get('turso_submodule_sha')}, "
            f"sqld={metadata.get('sqld_image_digest')}, arch={metadata.get('arch')}"
        )

    correctness = verdict.get("correctness")
    if isinstance(correctness, dict):
        lines.append(
            "- G0: "
            f"passed={correctness.get('g0_passed')}, "
            f"unknown_results={correctness.get('unknown_results')}, "
            f"oracle={correctness.get('oracle')}"
        )

    lines.append("")
    if verdict["status"] == "PASS":
        lines.append(
            "All five scenarios satisfy peri p50 <= 1.5 x sqld p50 under the same "
            "campaign. G0 passed and no errors or excluded samples remain."
        )
    elif verdict["status"] == "FAIL":
        lines.append(
            "At least one scenario exceeds peri p50 <= 1.5 x sqld p50. Scenarios are "
            "judged independently; no average can hide a failure."
        )
    elif verdict["status"] == "HOLD":
        lines.append(
            "Evidence is incomplete or G0 is not passed. No performance acceptance is "
            "possible; any latency values below are informational only."
        )
    else:
        lines.append(
            "The evidence was rejected. No performance acceptance is possible."
        )

    errors = verdict.get("errors") or []
    if errors:
        lines.extend(["", "## Rejected evidence", ""])
        lines.extend(f"- {error}" for error in errors)

    warnings = verdict.get("warnings") or []
    if warnings:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {warning}" for warning in warnings)

    scenarios = verdict.get("scenarios") or []
    if scenarios:
        lines.extend(
            [
                "",
                "## Per-scenario p50 comparison",
                "",
                "| scenario | sqld p50 (ms) | peri p50 (ms) | ratio | limit 1.5x (ms) "
                "| sqld p95 / p99 (ms) | peri p95 / p99 (ms) | samples sqld/peri | verdict |",
                "|---|---:|---:|---:|---:|---:|---:|---:|---|",
            ]
        )
        for scenario in scenarios:
            sqld = scenario["sqld"]
            peri = scenario["peri"]
            lines.append(
                f"| {scenario['scenario']} | {_ms(sqld['p50_ns'])} | "
                f"{_ms(peri['p50_ns'])} | {scenario['ratio']:.3f} | "
                f"{_ms(scenario['limit_p50_ns'])} | "
                f"{_ms(sqld['p95_ns'])} / {_ms(sqld['p99_ns'])} | "
                f"{_ms(peri['p95_ns'])} / {_ms(peri['p99_ns'])} | "
                f"{sqld['samples']} / {peri['samples']} | "
                f"{_markdown_verdict_label(scenario)} |"
            )

    gaps = verdict.get("evidence_gaps") or []
    if gaps:
        lines.extend(["", "## Evidence gaps", ""])
        lines.extend(f"- {gap}" for gap in gaps)

    lines.extend(
        [
            "",
            "The gate recomputes p50/p95/p99 from raw nanosecond samples. It does not "
            "measure, and it does not replace the SP01 G1-G5 tail-latency or stability "
            "conditions.",
            "",
        ]
    )
    return "\n".join(lines)


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Recompute the five-scenario sqld relative p50 gate from raw evidence. "
            "Missing evidence holds; malformed or incomplete evidence is invalid."
        )
    )
    parser.add_argument("--input", help="raw evidence JSON; omit to emit HOLD")
    parser.add_argument("--json-out", help="write the JSON verdict to this path")
    parser.add_argument("--markdown-out", help="write the Markdown verdict to this path")
    parser.add_argument("--expect-peri-sha", help="require metadata.peri_sha to match")
    parser.add_argument(
        "--expect-turso-sha", help="require metadata.turso_submodule_sha to match"
    )
    args = parser.parse_args(argv)

    if args.input is None:
        verdict = hold_no_evidence()
    else:
        path = Path(args.input)
        if not path.exists():
            verdict = hold_no_evidence(str(path), f"evidence file not found: {path}")
        else:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except OSError as exc:
                verdict = _invalid(f"cannot read input: {exc}", input_path=str(path))
            except json.JSONDecodeError as exc:
                verdict = _invalid(f"invalid JSON: {exc}", input_path=str(path))
            else:
                verdict = evaluate(
                    data,
                    input_path=str(path),
                    expect_peri_sha=args.expect_peri_sha,
                    expect_turso_sha=args.expect_turso_sha,
                )

    encoded = json.dumps(verdict, indent=2) + "\n"
    try:
        if args.json_out:
            _write(Path(args.json_out), encoded)
        else:
            sys.stdout.write(encoded)
        if args.markdown_out:
            _write(Path(args.markdown_out), render_markdown(verdict))
    except OSError as exc:
        sys.stderr.write(f"{TOOL}: cannot write verdict: {exc}\n")
        return EXIT_CODES["INVALID"]
    return EXIT_CODES[verdict["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
