"""CLI 参数、环境及证据落盘。"""
import argparse
import json
import os
from pathlib import Path
import platform
import sys
from issue11 import contract, evidence_io, environment as environment_module
from issue11.http import client as http_client, protocol
from issue11.http.runtime import RunnerError, RUNNER_VERSION, utc_now, unix_ms, rss_bytes, cpu_seconds, build_environment, _diagnostics, _digest_or_zero, _read_credential, _database_argument, _run_id
from issue11.http.runner_engine import ScenarioRunner
def _resource_document(arguments, started_cpu, started_rss):
    return {
        "schema": 1,
        "client": {
            "pid": os.getpid(),
            "rss_bytes": max(started_rss, rss_bytes()),
            "cpu_seconds": max(0.0, cpu_seconds() - started_cpu),
            "python": platform.python_version(),
        },
        "product_processes": [],
        "product_processes_accounted": False,
        "failure": None,
        "note": "W0-A records the client process only; product resources are a W0-B requirement.",
    }


def _campaign_document(arguments, acceptance, run_id):
    order = (acceptance["sampling"]["formal"]["pair_order"]
             if arguments.profile == "formal" else [])
    return {
        "schema": 1,
        "id": arguments.campaign_id or ("issue11-" + run_id),
        "profile": arguments.profile,
        "pair_order": order,
        "pairs_min": acceptance["sampling"]["formal"]["pairs_min"]
        if arguments.profile == "formal" else 0,
        "synthetic_test_fixture": bool(arguments.synthetic_test_fixture),
        "created_at": utc_now(),
        "note": "The campaign ledger is campaign-runs.jsonl; the gate ignores summary.json.",
    }


def _campaign_identity(arguments, run_id):
    return {
        "schema": 1,
        "run_id": run_id,
        "runner_version": RUNNER_VERSION,
        "runner_sha256": _digest_or_zero(Path(__file__)),
        "contract_sha256": contract.package_digest(),
        "acceptance_sha256": contract.digest(contract.CONTRACT_ROOT / "acceptance.json"),
        "workloads_sha256": contract.digest(contract.CONTRACT_ROOT / "workloads.json"),
        "issue8_manifest_sha256": contract.issue8_manifest_sha256(),
        "source": contract.source_identity(),
        "created_at": utc_now(),
        "synthetic_test_fixture": bool(arguments.synthetic_test_fixture),
    }


def _ensure_campaign_headers(root, arguments, acceptance, run_id):
    campaign_path = Path(root) / "campaign.json"
    if campaign_path.exists():
        existing = evidence_io.read_json(campaign_path)
        if existing.get("profile") != arguments.profile:
            raise RunnerError("campaign_profile_mismatch")
        if arguments.profile == "formal" \
                and existing.get("pair_order") != acceptance["sampling"]["formal"]["pair_order"]:
            raise RunnerError("campaign_pair_order_mismatch")
        return
    evidence_io.write_json(campaign_path, _campaign_document(arguments,
                                                             acceptance, run_id))
    evidence_io.write_json(Path(root) / "campaign-identity.json",
                           _campaign_identity(arguments, run_id))


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(
        description="Issue 11 real Hrana /v2/pipeline runner (W0-A).")
    parser.add_argument("--scenario", required=True,
                        help="H1-H7; H8/H9/H10/N1 are unavailable placeholders")
    parser.add_argument("--profile", choices=("quick", "formal"), default="quick")
    parser.add_argument("--endpoint", required=True,
                        help="https://host:port (http only for loopback diagnostic)")
    parser.add_argument("--database", required=True,
                        help="tenant/database/branch")
    credentials = parser.add_mutually_exclusive_group()
    credentials.add_argument("--credential-env", default=None,
                             help="凭据环境变量；未指定来源时使用 PERI_SQL_TOKEN")
    credentials.add_argument("--credential-file", default=None,
                             help="只读凭据文件")
    parser.add_argument("--ca-file", default=None,
                        help="CA bundle for verified TLS; required for https")
    parser.add_argument("--diagnostic-plaintext", action="store_true",
                        help="allow http on loopback only; never a formal environment")
    parser.add_argument("--evidence-root", required=True)
    parser.add_argument("--environment-template", default=None,
                        help="operator environment document to merge")
    parser.add_argument("--samples", type=int, default=None)
    parser.add_argument("--warmup", type=int, default=200)
    parser.add_argument("--fixture-scale", type=int, choices=(1000, 100000), default=1000)
    parser.add_argument("--setup", action="store_true",
                        help="execute schema and fixture before warmup")
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--arm-label", choices=("A", "B"), default="A")
    parser.add_argument("--pair-index", type=int, default=0)
    parser.add_argument("--pair-order-label", choices=("AB", "BA"), default="AB")
    parser.add_argument("--state", choices=("fresh", "aged"), default="fresh")
    parser.add_argument("--duration-seconds", type=int, default=0,
                        help="reserved sustained duration; must be >=3600 for aged formal")
    parser.add_argument("--campaign-id", default=None)
    parser.add_argument("--diagnostic-switch", action="append", default=[],
                        metavar="NAME=0|1")
    parser.add_argument("--cpu-cores", type=float, default=None)
    parser.add_argument("--memory-bytes", type=int, default=8 * 1024 * 1024 * 1024)
    parser.add_argument("--store-root", default=None)
    parser.add_argument("--cache-root", default=None)
    parser.add_argument("--synthetic-test-fixture", action="store_true",
                        help="mark evidence as a unit-test fixture")
    return parser.parse_args(argv)


def main(argv=None):
    arguments = parse_arguments(argv)
    try:
        acceptance = contract.load_acceptance()
        workloads = contract.load_workloads()
        problems = contract.validate_contract()
        if problems:
            raise RunnerError("contract_invalid: " + ",".join(problems))
        scenario = contract.scenario(workloads, arguments.scenario)
        if scenario.get("reserved"):
            raise RunnerError("scenario_reserved: " + arguments.scenario
                              + " (independent native/concurrency/sustained driver)")
        if arguments.scenario == "N1":
            raise RunnerError("native_scenario_reserved")
        if arguments.profile == "quick":
            default_samples = acceptance["sampling"]["quick"]["samples_min"]
            minimum = acceptance["sampling"]["quick"]["samples_min"]
        else:
            default_samples = acceptance["sampling"]["formal"]["samples_min"]
            minimum = acceptance["sampling"]["formal"]["samples_min"]
        samples = arguments.samples if arguments.samples is not None else default_samples
        if samples <= 0 or arguments.warmup < 0:
            raise RunnerError("sample_count_invalid")
        if arguments.state == "aged" and arguments.duration_seconds < 3600:
            raise RunnerError("aged_state_requires_3600_seconds")
        credential = _read_credential(arguments)
        database = _database_argument(arguments.database)
        diagnostics = _diagnostics(arguments)
        client = http_client.HranaClient(
            arguments.endpoint, credential, ca_file=arguments.ca_file,
            timeout_seconds=arguments.timeout_seconds,
            allow_plaintext_loopback=arguments.diagnostic_plaintext)
        root = evidence_io.create_root(arguments.evidence_root)
        run_id = _run_id(arguments)
        run_directory = evidence_io.create_run_directory(root, run_id)
        contract_sha = contract.package_digest()
        environment = build_environment(arguments, client, run_id, contract_sha,
                                        contract.source_identity(), diagnostics)
        validation = environment_module.validate(environment, workloads)
        if validation["problems"]:
            raise RunnerError("environment_invalid: "
                              + ",".join(validation["problems"]))
        evidence_io.write_json(run_directory / "environment.json", environment)
        evidence_io.write_json(run_directory / "run.json", {
            "scenario": arguments.scenario, "arm": arguments.arm_label,
            "pair_index": arguments.pair_index, "state": arguments.state,
            "profile": arguments.profile, "diagnostics": diagnostics, "concurrency": 1,
        })
        _ensure_campaign_headers(root, arguments, acceptance, run_id)
        writers = {
            "operations": evidence_io.raw_writer(run_directory / "operations.jsonl"),
            "requests": evidence_io.raw_writer(run_directory / "requests.jsonl"),
            "attribution": evidence_io.raw_writer(run_directory / "request-attribution.jsonl"),
            "periodic": evidence_io.raw_writer(run_directory / "periodic-events.jsonl"),
        }
        writers["periodic"].write({
            "schema_version": 1,
            "record_type": "availability",
            "kind": "periodic_events_unavailable",
            "available": False,
            "process": "product",
            "reason": "W0-B must emit audit head, journal compact, receipt roll, checkpoint and index compact events",
            "design_requirement": True,
        })
        started_cpu = cpu_seconds()
        started_rss = rss_bytes()
        started_at = unix_ms()
        runner = ScenarioRunner(arguments, workloads, scenario, client,
                                run_directory, writers, arguments.fixture_scale,
                                database)
        try:
            if arguments.setup:
                runner.setup_fixture()
            result = runner.run()
        finally:
            client.close()
            for writer in writers.values():
                writer.close()
        completed_at = unix_ms()
        metadata_path = run_directory / "run.json"
        metadata = evidence_io.read_json(metadata_path)
        metadata.update(started_at_unix_ms=started_at, completed_at_unix_ms=completed_at)
        evidence_io.write_json(metadata_path, metadata, reseal=True)
        oracle = {
            "schema": 1,
            "fixture_scale": arguments.fixture_scale,
            "fixture_rows": arguments.fixture_scale,
            "fixture_sha256": contract.fixture_digest(arguments.fixture_scale),
            "setup_executed": bool(arguments.setup),
            "verified_operations": result["verified_operations"],
            "mismatches": result["mismatches"],
            "scale_used": "1000" if arguments.fixture_scale == 1000 else "100000",
        }
        evidence_io.write_json(run_directory / "oracle.json", oracle)
        evidence_io.write_json(run_directory / "resources.json",
                               _resource_document(arguments, started_cpu,
                                                  started_rss))
        evidence_io.write_json(run_directory / "metrics-snapshot-index.json", {
            "schema": 1,
            "available": False,
            "reason": "W0-B must export worker and authority metrics snapshots",
            "snapshots": [],
        })
        status = "RUN_COMPLETED" if not result["fatal"] else "RUN_STOPPED"
        summary = {
            "schema": 1,
            "status": status,
            "display_only": True,
            "gate_never_reads_this_file": True,
            "run_id": run_id,
            "scenario": arguments.scenario,
            "profile": arguments.profile,
            "arm": arguments.arm_label,
            "pair_index": arguments.pair_index,
            "pair_order_label": arguments.pair_order_label,
            "state": arguments.state,
            "samples_requested": samples,
            "samples_minimum": minimum,
            "warmup_requested": arguments.warmup,
            "counts": {key: result[key] for key in (
                "warmup_records", "timed_records", "timed_success",
                "timed_failure", "timed_rejected", "timed_unknown",
                "verified_operations")},
            "mismatches": result["mismatches"],
            "http_counters": client.counters(),
            "started_at_unix_ms": started_at,
            "completed_at_unix_ms": completed_at,
            "timed_started_ns": result["timed_started_ns"],
            "timed_completed_ns": result["timed_completed_ns"],
            "error": result["error"],
            "synthetic_test_fixture": bool(arguments.synthetic_test_fixture),
        }
        evidence_io.write_json(run_directory / "summary.json", summary)
        hashes = evidence_io.write_manifests(run_directory, run_id)
        secret_matches = evidence_io.scan_for_bytes(
            [run_directory / name for name in evidence_io.REQUIRED_RUN_ARTIFACTS]
            + [run_directory / "SHA256SUMS.json",
               run_directory / "MANIFEST.json",
               root / "campaign.json", root / "campaign-identity.json"],
            credential.encode())
        if secret_matches:
            raise RunnerError("credential_leaked_into_evidence")
        evidence_io.append_ledger(root, {
            "schema": 1,
            "run_id": run_id,
            "path": "runs/" + run_id,
            "scenario": arguments.scenario,
            "profile": arguments.profile,
            "arm": arguments.arm_label,
            "pair_index": arguments.pair_index,
            "pair_order_label": arguments.pair_order_label,
            "state": arguments.state,
            "concurrency": 1,
            "duration_seconds": arguments.duration_seconds,
            "diagnostics": diagnostics,
            "synthetic_test_fixture": bool(arguments.synthetic_test_fixture),
        })
        output = {
            "run_id": run_id,
            "run_directory": str(run_directory),
            "status": status,
            "counts": summary["counts"],
            "http_counters": summary["http_counters"],
            "mismatches": summary["mismatches"],
            "evidence_sha256": hashes,
        }
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0 if not result["fatal"] else 1
    except (RunnerError, http_client.ClientError, contract.ContractError,
            evidence_io.EvidenceError, protocol.ProtocolError,
            environment_module.EnvironmentError) as failure:
        print("issue11 runner error: " + str(failure), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
