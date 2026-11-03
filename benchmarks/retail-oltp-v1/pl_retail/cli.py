"""Command line entry point for the PL-RETAIL-OLTP 1.0.0 benchmark.

Only ``score`` and ``self-check`` carry acceptance authority for the artefacts
they inspect; ``summary`` is a convenience view and always exits zero.  Every
other non-PASS outcome maps to the exit code table in :mod:`pl_retail.errors`,
so a CI step cannot mistake a FAIL/INVALID/INCONCLUSIVE bundle for a pass.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

from .cleanup_ab import DEFAULT_PROBE_OPS, compare_policies, probe_policy, probe_session_capacity
from .business import Session
from .client import HranaClient
from .contract import (
    DDL_PATH,
    PACKAGE_ROOT,
    SCORECARD_TEMPLATE_PATH,
    driver_digest,
    load_contract,
    reference_driver_digest,
    verify_package,
    write_sha256sums,
)
from .errors import (
    EXIT_CODES,
    ContractError,
    PrerequisiteError,
    Status,
)
from .fixture import Fixture, FixtureConfig
from .generate import load_dataset
from .lifecycle import LifecycleConfig
from .plan import PlanStream
from .runner import LEVELS, RunnerConfig, run_matrix
from .score import emit_scorecards, print_summary, score_run


def _add_run_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--fixture-file", required=True, help="JSON with endpoint/tenant/project/keys")
    parser.add_argument("--output", required=True, help="evidence directory to create")
    parser.add_argument("--contract", default=None)
    parser.add_argument("--mode", choices=("full", "smoke"), default="full")
    parser.add_argument("--scale", type=int, default=None, help="1 = full dataset, N = 1/N")
    parser.add_argument("--repetitions", type=int, default=None)
    parser.add_argument("--window-seconds", type=float, default=None)
    parser.add_argument("--warmups", type=int, default=None)
    parser.add_argument("--idle-cycles", type=int, default=None)
    parser.add_argument("--close-policy", choices=("A", "B"), default="A")
    parser.add_argument("--keep-fixtures", action="store_true")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--lifecycle-file", default=None, help="JSON with stop/start/ready proof commands")
    parser.add_argument("--platform-descriptor", default=None, help="JSON platform facts recorded into run.json")
    parser.add_argument(
        "--level",
        action="append",
        default=None,
        help="restrict to a level (repeatable), e.g. --level U1 --level H8",
    )


def _load_json_file(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    with open(path, encoding="utf-8") as handle:
        document = json.load(handle)
    if not isinstance(document, dict):
        raise PrerequisiteError("%s must contain a JSON object" % path)
    return document


def _runner_config(args: argparse.Namespace) -> RunnerConfig:
    contract = load_contract(args.contract)
    fixture = FixtureConfig.from_file(args.fixture_file)
    levels = LEVELS
    if args.level:
        if args.mode == "full":
            # A filtered matrix is not the fixed contract matrix; refusing is
            # clearer than emitting a bundle the scorer must mark INVALID.
            raise PrerequisiteError(
                "--level is a smoke/debug filter; the full matrix cannot be filtered"
            )
        wanted = set(args.level)
        levels = tuple(item for item in LEVELS if item[0] in wanted)
        if not levels:
            raise PrerequisiteError("no known level selected: %s" % sorted(wanted))
    matrix = contract.document["matrix"]
    repetition_count = args.repetitions or int(
        1 if args.mode == "smoke" else matrix.get("repetitions", 3)
    )
    window = args.window_seconds or float(
        10.0 if args.mode == "smoke" else matrix.get("window_seconds", 120)
    )
    warmups = args.warmups if args.warmups is not None else int(
        1 if args.mode == "smoke" else matrix.get("warmups_per_worker", 20)
    )
    idle_cycles = args.idle_cycles if args.idle_cycles is not None else int(
        1 if args.mode == "smoke" else 10
    )
    lifecycle = (
        LifecycleConfig.load(
            args.lifecycle_file,
            endpoint=fixture.endpoint,
            api_key=fixture.global_api_key,
            ca_file=fixture.ca_file,
        )
        if args.lifecycle_file
        else None
    )
    return RunnerConfig(
        fixture=fixture,
        output_dir=args.output,
        mode=args.mode,
        scale=int(args.scale if args.scale is not None else (8 if args.mode == "smoke" else 1)),
        repetitions=repetition_count,
        window_seconds=window,
        warmups=warmups,
        levels=levels,
        platform_descriptor=_load_json_file(args.platform_descriptor) or None,
        lifecycle=lifecycle,
        keep_fixtures=bool(args.keep_fixtures),
        close_policy=args.close_policy,
        idle_cycles=idle_cycles,
        run_id=args.run_id or time.strftime("retail-%Y%m%dT%H%M%SZ", time.gmtime()),
        contract=contract.document,
    )


def _command_run(args: argparse.Namespace) -> int:
    try:
        config = _runner_config(args)
    except PrerequisiteError as error:
        print("run blocked: %s" % error, file=sys.stderr)
        return EXIT_CODES[Status.BLOCKED]
    contract = load_contract(args.contract)
    try:
        result = run_matrix(config)
    except PrerequisiteError as error:
        print("run blocked: %s" % error, file=sys.stderr)
        return EXIT_CODES[Status.BLOCKED]
    details: dict[str, Any] = {}
    scorecard = score_run(Path(config.output_dir), contract, details=details)
    emit_scorecards(Path(config.output_dir), scorecard, details)
    print_summary(scorecard)
    print("rounds: %d" % len(result["rounds"]))
    return EXIT_CODES[Status(scorecard["status"])]


def _command_score(args: argparse.Namespace) -> int:
    contract = load_contract(args.contract)
    root = Path(args.evidence)
    details: dict[str, Any] = {}
    scorecard = score_run(root, contract, details=details)
    if not args.no_emit:
        emit_scorecards(root, scorecard, details)
    if args.output:
        Path(args.output).write_text(
            json.dumps(scorecard, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    if not args.quiet:
        print_summary(scorecard)
    return EXIT_CODES[Status(scorecard["status"])]


def _command_summary(args: argparse.Namespace) -> int:
    """Non-authoritative view of a bundle (contract §五: summary has no authority)."""
    contract = load_contract(args.contract)
    scorecard = score_run(Path(args.evidence), contract)
    if args.json:
        print(json.dumps(scorecard, indent=2, sort_keys=True))
    else:
        print_summary(scorecard)
    return 0


def _command_ab_cleanup(args: argparse.Namespace) -> int:
    contract = load_contract(args.contract)
    fixture_config = FixtureConfig.from_file(args.fixture_file)
    maximum = int(contract.document["environment"]["project_quotas"]["max_sessions"])
    sides: list[dict[str, Any]] = []
    try:
        for policy in ("A", "B"):
            fixture = Fixture(fixture_config, "cleanup-ab-%s-%d" % (policy.lower(), int(time.time())), "cleanup")
            target = fixture.create()
            fixture.wait_ready(60.0)
            client = HranaClient(target, timeout=5.0)
            session = Session(client)
            try:
                session.open()
                side = probe_policy(session, policy, args.ops or DEFAULT_PROBE_OPS)
            finally:
                session.close()
                client.close_quietly()
            side["capacity"] = probe_session_capacity(target, maximum)
            side["fixture"] = {"database": fixture.database, "delete": fixture.delete()}
            sides.append(side)
    except PrerequisiteError as error:
        print("cleanup A/B blocked: %s" % error, file=sys.stderr)
        return EXIT_CODES[Status.BLOCKED]
    comparison = compare_policies(sides[0], sides[1])
    document = {
        "schema": "pl-retail-oltp-cleanup-ab/1.0.0",
        "contract": contract.version,
        "profile": contract.profile,
        "sides": sides,
        "comparison": comparison,
    }
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(
            json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    print(json.dumps(comparison, indent=2, sort_keys=True))
    return 0 if comparison["verdict"] == "PASS" else EXIT_CODES[Status.FAIL]


def _command_digest(_: argparse.Namespace) -> int:
    contract = load_contract(None)
    document = {
        "contract": contract.document["contract"],
        "contract_version": contract.version,
        "contract_sha256": contract.sha256,
        "driver_sha256": driver_digest(),
        # Never hardcode this: it is the contract's acceptance gate and must
        # stay null until the four P0 deliverables are accepted.
        "certified_driver_sha256": contract.document.get("certified_driver_sha256"),
        "driver_digest_scope": "sha256 over sorted pl_retail/*.py name+content",
        "reference_driver": reference_driver_digest(),
        "package_root": str(PACKAGE_ROOT),
    }
    print(json.dumps(document, indent=2, sort_keys=True))
    return 0


def _self_check() -> tuple[Status, list[str], list[str]]:
    violations: list[str] = []
    notes: list[str] = []
    contract = load_contract(None)
    notes.append("contract %s / %s" % (contract.version, contract.profile))
    try:
        package = verify_package()
        notes.append("package files: %d" % package["files"])
    except ContractError as error:
        violations.append(str(error))
    if not SCORECARD_TEMPLATE_PATH.is_file():
        violations.append("scorecard-template.json is missing")
    if not DDL_PATH.is_file():
        violations.append("reference/ddl.sql is missing")

    dataset = load_dataset(1)
    totals = dataset.totals()
    for key, expected in contract.document["oracle"].items():
        if key not in totals:
            violations.append("oracle %s is not produced by the generator" % key)
            continue
        if int(totals[key]) != int(expected):
            violations.append(
                "oracle %s: generated %s expected %s" % (key, totals[key], expected)
            )
    notes.append("oracle totals: %s" % json.dumps(totals, sort_keys=True))

    first = PlanStream("uniform", seed=contract.seed, products=10000, warehouses=4, customers=10000, orders=100000)
    second = PlanStream("uniform", seed=contract.seed, products=10000, warehouses=4, customers=10000, orders=100000)
    counts: dict[str, int] = {}
    for index in range(10000):
        op = first.next()
        twin = second.next()
        if op.kind != twin.kind or dict(op.params) != dict(twin.params):
            violations.append("plan stream is not deterministic at index %d" % index)
            break
        counts[op.kind] = counts.get(op.kind, 0) + 1
    classes = contract.document["business"]["classes"]
    for name, document in classes.items():
        expected = float(document["weight_per_100"])
        observed = counts.get(name, 0) / 10000.0 * 100.0
        if abs(observed - expected) > 1.0:
            violations.append(
                "business mix %s: observed %.2f%% expected %.2f%%" % (name, observed, expected)
            )
    notes.append("plan mix over 10k ops: %s" % json.dumps(counts, sort_keys=True))

    hotspot = PlanStream("hotspot", seed=contract.seed, products=10000, warehouses=4, customers=10000, orders=100000)
    checkouts = 0
    hotspot_checkouts = 0
    for _ in range(4000):
        op = hotspot.next()
        if op.kind != "checkout":
            continue
        checkouts += 1
        if not op.params.get("hotspot"):
            continue
        hotspot_checkouts += 1
        if int(op.params["warehouse_id"]) != 1:
            violations.append("hotspot checkout did not target warehouse 1")
            break
        if any(int(pid) > 100 for pid, _ in op.params["items"]):
            violations.append("hotspot checkout uses a product outside 1..100")
            break
        if int(op.params["customer_id"]) % 4 != 1:
            violations.append("hotspot checkout uses a customer outside the hotspot set")
            break
    if checkouts and hotspot_checkouts / checkouts < 0.7:
        violations.append(
            "hotspot checkout share %.2f%% is below the 80%% target"
            % (hotspot_checkouts / checkouts * 100.0)
        )
    notes.append(
        "checkouts inspected: %d (hotspot %d)" % (checkouts, hotspot_checkouts)
    )

    if driver_digest() != driver_digest():
        violations.append("driver digest is not stable across calls")
    notes.append("driver_sha256 %s" % driver_digest())
    notes.append("reference driver: %s" % json.dumps(reference_driver_digest(), sort_keys=True))
    if SCORECARD_TEMPLATE_PATH.is_file():
        template = json.loads(SCORECARD_TEMPLATE_PATH.read_text(encoding="utf-8"))
        if template.get("status") is not None:
            violations.append("scorecard template must not carry a status")
    return (Status.PASS if not violations else Status.INVALID), violations, notes


def _command_self_check(args: argparse.Namespace) -> int:
    # A stale digest file makes the check fail; regenerating after the fact
    # would leave the caller with an INVALID that --write-sums cannot fix.
    written = None
    if args.write_sums:
        written = write_sha256sums()
    status, violations, notes = _self_check()
    if written is not None:
        notes.append("wrote %s" % written)
    if args.json:
        print(json.dumps({"status": status.value, "violations": violations, "notes": notes}, indent=2))
    else:
        print("self-check: %s" % status.value)
        for note in notes:
            print("  note: %s" % note)
        for violation in violations:
            print("  violation: %s" % violation)
    return EXIT_CODES[status]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pl_retail", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="execute the fixed matrix and score the evidence")
    _add_run_arguments(run)
    run.set_defaults(handler=_command_run)

    score = sub.add_parser("score", help="strictly score an existing evidence bundle")
    score.add_argument("--evidence", required=True)
    score.add_argument("--contract", default=None)
    score.add_argument("--output", default=None)
    score.add_argument("--quiet", action="store_true")
    score.add_argument("--no-emit", action="store_true", help="do not write scorecard.json into the bundle")
    score.set_defaults(handler=_command_score)

    summary = sub.add_parser("summary", help="non-authoritative summary (always exits zero)")
    summary.add_argument("--evidence", required=True)
    summary.add_argument("--contract", default=None)
    summary.add_argument("--json", action="store_true")
    summary.set_defaults(handler=_command_summary)

    ab = sub.add_parser("ab-cleanup", help="directed A/B of the known-rejection close policy")
    ab.add_argument("--fixture-file", required=True)
    ab.add_argument("--contract", default=None)
    ab.add_argument("--ops", type=int, default=DEFAULT_PROBE_OPS)
    ab.add_argument("--output", default=None)
    ab.set_defaults(handler=_command_ab_cleanup)

    digest = sub.add_parser("digest", help="print the driver digest and reference gap")
    digest.set_defaults(handler=_command_digest)

    check = sub.add_parser("self-check", help="verify the package, oracle and plan determinism")
    check.add_argument("--json", action="store_true")
    check.add_argument("--write-sums", action="store_true", help="regenerate SHA256SUMS first")
    check.set_defaults(handler=_command_self_check)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.handler(args))
    except (ContractError, PrerequisiteError) as error:
        print("blocked: %s" % error, file=sys.stderr)
        return EXIT_CODES[error.status]
    except FileNotFoundError as error:
        print("blocked: %s" % error, file=sys.stderr)
        return EXIT_CODES[Status.BLOCKED]


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
