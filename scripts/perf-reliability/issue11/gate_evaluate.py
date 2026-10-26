"""Issue11 gate_evaluate；按证据职责隔离。"""
import argparse
import json
from pathlib import Path
from perf_contract import FROZEN_SHA256SUMS_SHA256
from issue11 import campaign as campaign_module
from issue11 import contract
from issue11.gate_pairing import synthetic, paired_problems, reliability_and_extended
from issue11.gate_common import PASS, INCOMPLETE, NOT_RUN, FAIL
from issue11.gate_common import GateError, _read_json, _section
from issue11.gate_checks import _load_run, _validate_requests, _validate_operations, _validate_attribution, _latency_stats, _target_problems, _scenario_operations
from issue11.gate_observability import (_validate_periodic, _validate_metrics, _validate_resources, _validate_oracle, _environment_section)
from issue11.gate_status import (_stability_problems, _combine, _section_problems, _result)

def _evaluate_runs(views, acceptance, workloads, profile, problems):
    runs = []
    formal_eligible = bool(views)
    diagnostic = False
    timed_total = 0
    paired_views = []
    for pending in views:
        view, load_problems = _load_run(pending["path"], pending["run_id"],
                                        pending["record"], workloads)
        problems.extend(load_problems)
        if view is None:
            continue
        paired_views.append({key: view[key] for key in
                             ("run_id", "record", "metadata", "environment", "oracle")})
        run_id = view["run_id"]
        record = view["record"]
        if synthetic(view):
            problems.append(("INCOMPLETE", "synthetic_evidence_forbidden: " + run_id))
        scenario_id = record.get("scenario")
        if scenario_id not in workloads["scenarios"]:
            problems.append(("FAIL", "run_scenario: " + run_id))
            continue
        environment = _environment_section(view, workloads, problems)
        diagnostics = view["environment"].get("diagnostics", {})
        enabled = any(value is True for value in diagnostics.values())
        if record.get("diagnostics") != diagnostics:
            problems.append(("FAIL", "diagnostic_ledger_environment_mismatch"))
        if enabled:
            coverage = view["environment"].get("diagnostic_processes", {})
            for process in ("worker", "authority"):
                observed = coverage.get(process, {})
                if not observed.get("pid") or observed.get("observed_switches") != diagnostics:
                    problems.append(("INCOMPLETE", "diagnostic_process_unproven: " + process))
        if not environment["formal_eligible"]:
            formal_eligible = False
        if environment.get("diagnostic"):
            diagnostic = True
        requests, request_order = _validate_requests(view["requests"],
                                                     view["environment"], problems)
        operations, referenced = _validate_operations(
            view["operations"], requests, workloads, scenario_id, problems)
        attribution = _validate_attribution(view["attribution"], requests, problems)
        stats = _latency_stats(view["operations"], requests)
        timed_total += stats["timed_count"]
        minimum = (acceptance["sampling"]["quick"]["samples_min"]
                   if profile == "quick"
                   else acceptance["sampling"]["formal"]["samples_min"])
        warmup_min = (acceptance["sampling"]["quick"]["warmup_min"]
                      if profile == "quick"
                      else acceptance["sampling"]["formal"]["warmup_min"])
        warmup_records = [operation for operation in view["operations"]
                          if operation.get("phase") == "warmup"]
        warmup_count = len(_scenario_operations(warmup_records))
        if warmup_count < warmup_min:
            problems.append(("INCOMPLETE", "warmup_below_minimum: " + run_id))
        if any(operation.get("outcome") != "success"
               or not operation.get("verification", {}).get("ok")
               for operation in warmup_records):
            problems.append(("FAIL", "warmup_failure: " + run_id))
        if stats["timed_count"] < minimum:
            problems.append(("INCOMPLETE", "samples_below_minimum: " + run_id))
        if record.get("state") == "aged" \
                and stats["window_ns"] < 3600 * 1_000_000_000:
            problems.append(("INCOMPLETE", "aged_window_too_short: " + run_id))
        if stats["outcome_unknown"] > 0:
            problems.append(("FAIL", "timed_outcome_unknown: " + run_id))
        if stats["failure"] > 0:
            problems.append(("FAIL", "timed_failure: " + run_id))
        if stats["rejected"] > 0:
            problems.append(("FAIL", "timed_rejected: " + run_id))
        if profile != "formal" or record.get("arm") == "B":
            _target_problems(scenario_id, stats, acceptance["targets"], problems)
        reliability_and_extended(view, acceptance, problems)
        periodic_ok, periodic_reasons = _validate_periodic(view["periodic"], problems)
        if record.get("state") == "aged" and periodic_ok:
            cycles = sum(event.get("kind") == "checkpoint" and event.get("event_phase") == "end"
                         for event in view["periodic"])
            if cycles < acceptance["sampling"]["formal"]["maintenance_cycles_min"]:
                problems.append(("INCOMPLETE", "maintenance_cycles_below_minimum: " + run_id))
        if not periodic_ok:
            problems.extend(("INCOMPLETE", reason + ": " + run_id)
                            for reason in periodic_reasons)
        metrics_ok, metrics_reasons = _validate_metrics(view["path"],
                                                        view["metrics_index"],
                                                        problems)
        if not metrics_ok:
            problems.extend(("INCOMPLETE", reason + ": " + run_id)
                            for reason in metrics_reasons)
        resources_ok = _validate_resources(view["resources"], problems)
        if not resources_ok:
            problems.append(("INCOMPLETE", "product_resources_unavailable: " + run_id))
        _validate_oracle(view["oracle"], workloads, stats, problems)
        if attribution is None:
            problems.append(("INCOMPLETE", "attribution_unavailable: " + run_id))
        else:
            if attribution["server_evidence_coverage"] < 0.95:
                problems.append(("INCOMPLETE", "server_attribution_below_95_percent: "
                                 + run_id))
            if not attribution["server_evidence"]:
                problems.append(("INCOMPLETE", "server_attribution_missing: "
                                 + run_id))
        runs.append({
            "run_id": run_id,
            "path": view["path"],
            "scenario": scenario_id,
            "arm": record.get("arm"),
            "pair_index": record.get("pair_index"),
            "state": record.get("state"),
            "profile": record.get("profile"),
            "environment": environment,
            "stats": stats,
            "attribution": attribution,
            "periodic_events": periodic_ok,
            "metrics": metrics_ok,
            "product_resources": resources_ok,
            "requests": len(request_order),
            "operations": len(operations),
        })
    if profile == "formal" and not views:
        problems.append(("INCOMPLETE", "pairs_missing"))
    if profile == "formal":
        problems.extend(paired_problems(runs, paired_views, acceptance))
    stability_problems = _stability_problems(runs, acceptance)
    problems.extend(stability_problems)
    return {
        "runs": runs,
        "formal_eligible": formal_eligible,
        "diagnostic": diagnostic,
        "timed_total": timed_total,
    }


def evaluate(evidence_root, profile=None):
    evidence_root = Path(evidence_root)
    problems = []
    sections = {}
    try:
        identity = contract.verify_contract()
    except contract.ContractError as failure:
        return _result(NOT_RUN, [str(failure)], {}, profile or "quick", False,
                       [])
    if not identity["verified"]:
        problems.extend(("FAIL", problem) for problem in identity["problems"])
    contract_problems = contract.validate_contract()
    problems.extend(("FAIL", problem) for problem in contract_problems)
    if contract.issue8_manifest_sha256() != FROZEN_SHA256SUMS_SHA256:
        problems.append(("FAIL", "issue8_contract_manifest_changed"))
    sections["contract"] = _section(
        PASS if not problems else FAIL,
        [reason for _, reason in problems],
        files=identity["files"],
    )
    if problems:
        return _result(FAIL, [reason for _, reason in problems], sections,
                       profile or "quick", False, [])
    if not evidence_root.exists() or not evidence_root.is_dir():
        return _result(NOT_RUN, ["evidence_root_missing"], sections,
                       profile or "quick", False, [])
    try:
        acceptance = contract.load_acceptance()
        workloads = contract.load_workloads()
    except contract.ContractError as failure:
        return _result(FAIL, [str(failure)], sections, profile or "quick",
                       False, [])
    views = []
    if (evidence_root / "campaign.json").is_file():
        campaign_problems, identities = campaign_module.validate_campaign(
            evidence_root, acceptance, workloads)
        problems.extend(campaign_problems)
        identity_path = evidence_root / "campaign-identity.json"
        identity_document = {}
        if not identity_path.is_file():
            problems.append(("FAIL", "campaign_identity_missing"))
        else:
            try:
                identity_document = _read_json(identity_path)
            except GateError as failure:
                problems.append(("FAIL", "campaign_identity_unreadable: "
                                 + str(failure)))
            else:
                if identity_document.get("schema") != 1:
                    problems.append(("FAIL", "campaign_identity_schema"))
        try:
            campaign_document = campaign_module.load_campaign(evidence_root)
        except campaign_module.CampaignError as failure:
            return _result(FAIL, [str(failure)], sections, profile or "quick", False, [])
        if synthetic(campaign_document) or synthetic(identity_document if identity_path.is_file() else {}):
            problems.append(("INCOMPLETE", "synthetic_campaign_forbidden"))
        effective_profile = campaign_document["profile"]
        if profile is None:
            profile = effective_profile
        elif profile == "quick" and effective_profile == "formal":
            problems.append(("INCOMPLETE", "profile_downgrade_rejected"))
            profile = "formal"
        sections["campaign"] = _section(
            FAIL if any(severity == FAIL for severity, _ in campaign_problems)
            else INCOMPLETE if campaign_problems else PASS,
            [reason for _, reason in campaign_problems])
        for record, run_path in identities:
            views.append({"run_id": record.get("run_id", ""),
                          "path": str(run_path), "record": record})
        sections["evidence"] = _section(
            FAIL if any(severity == FAIL for severity, _ in problems)
            else INCOMPLETE if problems else PASS,
            [reason for _, reason in problems])
    elif (evidence_root / "environment.json").is_file():
        profile = profile or "quick"
        views.append({"run_id": evidence_root.name,
                      "path": str(evidence_root), "record": {}})
        run_problems = []
        sections["evidence"] = _section(
            FAIL if any(severity == FAIL for severity, _ in run_problems)
            else INCOMPLETE if run_problems else PASS,
            [reason for _, reason in run_problems])
        if profile == "formal":
            problems.append(("INCOMPLETE", "campaign_missing"))
    else:
        return _result(NOT_RUN, ["campaign_or_environment_missing"], sections,
                       profile or "quick", False, [])
    if len(views) == 1 and not views[0]["record"]:
        metadata_path = Path(views[0]["path"]) / "run.json"
        try:
            metadata = _read_json(metadata_path) if metadata_path.is_file() else {}
        except GateError as failure:
            return _result(FAIL, [str(failure)], sections, profile or "quick", False, [])
        if metadata.get("profile") == "formal":
            profile = "formal"
            problems.append(("INCOMPLETE", "campaign_missing"))
    sections["environment"] = _section(PASS)
    evaluations = _evaluate_runs(views, acceptance, workloads,
                                  profile or "quick", problems)
    for run in evaluations["runs"]:
        if run["environment"]["status"] == FAIL:
            sections["environment"] = _section(
                FAIL, run["environment"]["problems"])
            break
    sections["attribution"] = _section(PASS)
    sections["periodic_events"] = _section(PASS)
    sections["metrics"] = _section(PASS)
    sections["resources"] = _section(PASS)
    sections["samples"] = _section(PASS)
    sections["correctness"] = _section(PASS)
    sections["targets"] = _section(PASS)
    sections["stability"] = _section(PASS)
    status = _combine(sections, problems,
                      profile or "quick", evaluations["diagnostic"])
    formal_pass = status == PASS
    return _result(status, [reason for _, reason in problems] + _section_problems(sections),
                   sections, profile or "quick", formal_pass,
                   evaluations["runs"])


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(
        description="Issue 11 gate; recomputes from raw JSONL and manifests.")
    parser.add_argument("--evidence-root", required=True,
                        help="campaign root containing campaign.json/runs or a single run directory")
    parser.add_argument("--profile", choices=("quick", "formal"), default=None,
                        help="optional override; a campaign's formal profile is never downgraded")
    return parser.parse_args(argv)


def main(argv=None):
    arguments = parse_arguments(argv)
    try:
        result = evaluate(arguments.evidence_root, arguments.profile)
    except (GateError, contract.ContractError) as failure:
        result = _result(NOT_RUN, [str(failure)], {}, arguments.profile or "quick",
                         False, [])
    print(json.dumps(result, indent=2, sort_keys=True))
    return result["exit_code"]
