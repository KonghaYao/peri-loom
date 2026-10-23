#!/usr/bin/env python3
"""Roadmap-style Issue 8 CLI for W0-A1 S1/S2 campaigns.

Maps the planned interface (--round/--contract/--acceptance/--environment/
--case/--output) onto issue8_runner.py. The legacy S0 entry stays
``runner.py --round smoke``; steady state is not implemented in W0-A1.

--contract and --acceptance are byte anchors, not relocation knobs: they may
point at copies of issue8/workloads.json and issue8/acceptance.json, but any
byte difference from the frozen contract is rejected.
"""
import argparse
import json
from pathlib import Path
import sys
import tempfile

import campaign as campaign_module
import evidence as evidence_module
import issue8_runner
import perf_contract

SUPPORTED_CASES = ("S1", "S2")
OVERRIDE_FLAGS = {"min_samples_per_lane": "--minimum-samples",
                  "min_seconds": "--minimum-seconds",
                  "max_seconds_per_arm": "--max-seconds-per-arm",
                  "pairs": "--pairs"}


class CliError(ValueError):
    pass


def verify_anchored_inputs(contract_path=None, acceptance_path=None):
    if contract_path is not None:
        actual = perf_contract.digest(Path(contract_path))
        if actual != perf_contract.digest(perf_contract.CONTRACT_ROOT / "workloads.json"):
            raise CliError("contract_bytes_not_frozen: " + str(contract_path))
    if acceptance_path is not None:
        actual = perf_contract.digest(Path(acceptance_path))
        if actual != perf_contract.digest(perf_contract.CONTRACT_ROOT / "acceptance.json"):
            raise CliError("acceptance_bytes_not_frozen: " + str(acceptance_path))


def load_environment(path):
    environment = evidence_module.read_json(path)
    if not isinstance(environment, dict):
        raise CliError("environment_schema")
    for key in ("tier", "store_root", "cache_root", "evidence_root"):
        if not isinstance(environment.get(key), str) or not environment[key]:
            raise CliError("environment_missing_" + key)
    if environment["tier"] not in {"G1", "G2"}:
        raise CliError("environment_tier")
    timeout = environment.get("timeout", 10)
    if type(timeout) not in (int, float) or not 0.1 <= timeout <= 20:
        raise CliError("environment_timeout")
    metrics = environment.get("metrics_snapshot")
    if metrics is not None and (not isinstance(metrics, str) or not metrics):
        raise CliError("environment_metrics_snapshot")
    overrides = environment.get("profile_overrides") or {}
    if not isinstance(overrides, dict):
        raise CliError("environment_profile_overrides")
    unknown = sorted(set(overrides) - set(OVERRIDE_FLAGS))
    if unknown:
        raise CliError("environment_override_unknown: " + ",".join(unknown))
    for key, value in overrides.items():
        if type(value) is not int or value <= 0:
            raise CliError("environment_override_invalid: " + key)
    return environment


def filter_campaign(campaign, specification):
    if not specification:
        return campaign
    requested = [item.strip() for item in specification.split(",") if item.strip()]
    if not requested:
        raise CliError("case_empty")
    unsupported = sorted(set(requested) - set(SUPPORTED_CASES))
    if unsupported:
        raise CliError("case_not_supported_in_w0a1: " + ",".join(unsupported))
    available = {case["id"] for case in campaign["cases"]}
    missing = sorted(set(requested) - available)
    if missing:
        raise CliError("case_missing_from_campaign: " + ",".join(missing))
    return dict(campaign, cases=[case for case in campaign["cases"]
                                 if case["id"] in requested])


def _absolute_binaries(campaign, campaign_path):
    """The filtered campaign is copied to a temp file; keep relative arm paths
    anchored to the operator's original campaign directory."""
    campaign = dict(campaign)
    campaign["arms"] = {label: dict(arm) for label, arm in campaign["arms"].items()}
    base = Path(campaign_path).resolve().parent
    for arm in campaign["arms"].values():
        binary = Path(arm["binary"])
        if not binary.is_absolute():
            arm["binary"] = str((base / binary).resolve())
    return campaign


def build_runner_arguments(round_name, environment, campaign_path, output=None,
                           metrics_snapshot=None):
    arguments = ["--campaign", str(campaign_path), "--profile", round_name,
                 "--tier", environment["tier"],
                 "--store-root", environment["store_root"],
                 "--cache-root", environment["cache_root"],
                 "--evidence-root", str(output or environment["evidence_root"]),
                 "--timeout", str(environment.get("timeout", 10))]
    metrics = metrics_snapshot or environment.get("metrics_snapshot")
    if metrics:
        arguments.extend(["--metrics-snapshot", str(metrics)])
    overrides = environment.get("profile_overrides", {})
    for key, flag in OVERRIDE_FLAGS.items():
        if key in overrides:
            arguments.extend([flag, str(overrides[key])])
    return arguments


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--contract", type=Path,
                        help="workloads.json copy; bytes must match issue8/workloads.json")
    parser.add_argument("--acceptance", type=Path,
                        help="acceptance.json copy; bytes must match issue8/acceptance.json")
    parser.add_argument("--environment", type=Path, required=True,
                        help="JSON with tier, store_root, cache_root, evidence_root")
    parser.add_argument("--round", choices=["quick", "formal", "steady"], required=True)
    parser.add_argument("--case", help="comma separated subset of S1,S2")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--metrics-snapshot", type=Path)
    arguments = parser.parse_args(argv)
    try:
        if arguments.round == "steady":
            raise CliError("steady_not_implemented_in_w0a1")
        verify_anchored_inputs(arguments.contract, arguments.acceptance)
        environment = load_environment(arguments.environment)
        campaign = campaign_module.load_campaign(arguments.campaign)
        filtered = filter_campaign(campaign, arguments.case)
        filtered = _absolute_binaries(filtered, arguments.campaign)
        with tempfile.TemporaryDirectory(prefix="peri-issue8-cli-") as directory:
            campaign_path = Path(directory) / "campaign.json"
            evidence_module.write_json(campaign_path, filtered)
            return issue8_runner.main(build_runner_arguments(
                arguments.round, environment, campaign_path, arguments.output,
                arguments.metrics_snapshot))
    except (CliError, perf_contract.ContractError, campaign_module.CampaignError,
            evidence_module.EvidenceError, OSError, ValueError) as failure:
        print(json.dumps({"status": "NOT_RUN",
                          "error": type(failure).__name__ + ": " + str(failure)}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
