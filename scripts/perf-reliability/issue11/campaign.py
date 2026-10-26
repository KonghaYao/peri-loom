#!/usr/bin/env python3
"""Issue 11 campaign root layout, preregistered pairing, and run ledger checks."""
import json
import os
from pathlib import Path
import sys

PERF_ROOT = Path(__file__).resolve().parent.parent
if str(PERF_ROOT) not in sys.path:
    sys.path.insert(0, str(PERF_ROOT))

from issue11 import contract  # noqa: E402


class CampaignError(ValueError):
    pass


def load_campaign(root):
    path = Path(root) / "campaign.json"
    try:
        document = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as failure:
        raise CampaignError("campaign_unreadable: " + type(failure).__name__) from None
    if not isinstance(document, dict) or document.get("schema") != 1:
        raise CampaignError("campaign_schema")
    if not isinstance(document.get("id"), str) or not document["id"]:
        raise CampaignError("campaign_id")
    if document.get("profile") not in {"quick", "formal"}:
        raise CampaignError("campaign_profile")
    order = document.get("pair_order")
    if not isinstance(order, list) or any(item not in {"AB", "BA"} for item in order):
        raise CampaignError("campaign_pair_order")
    scenarios = document.get("scenarios")
    if scenarios is not None:
        if not isinstance(scenarios, list) or not scenarios \
                or any(not isinstance(item, str) or not item for item in scenarios):
            raise CampaignError("campaign_scenarios")
    return document


def read_ledger(root):
    path = Path(root) / "campaign-runs.jsonl"
    records = []
    if not path.exists():
        return records
    try:
        lines = path.read_text().splitlines()
    except OSError as failure:
        raise CampaignError("campaign_ledger_unreadable: "
                            + type(failure).__name__) from None
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            raise CampaignError("campaign_ledger_json: " + str(line_number)) from None
        if not isinstance(record, dict) or record.get("schema") != 1:
            raise CampaignError("campaign_ledger_record: " + str(line_number))
        records.append(record)
    return records


def write_ledger_record(root, record):
    path = Path(root) / "campaign-runs.jsonl"
    with path.open("a") as output:
        output.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
        output.flush()
        os.fsync(output.fileno())


def _run_path(root, record):
    relative = record.get("path")
    if not isinstance(relative, str) or not relative:
        return None
    candidate = (Path(root) / relative).resolve()
    if Path(root).resolve() not in candidate.parents:
        return None
    return candidate


def validate_campaign(root, acceptance=None, workloads=None):
    """Return (problems, runs). problems are (severity, code) tuples."""
    root = Path(root)
    acceptance = acceptance or contract.load_acceptance()
    workloads = workloads or contract.load_workloads()
    problems = []
    try:
        campaign = load_campaign(root)
        records = read_ledger(root)
    except CampaignError as failure:
        return [("FAIL", str(failure))], []
    profile = campaign["profile"]
    declared = campaign.get("scenarios") or sorted(
        {record.get("scenario") for record in records})
    if profile == "formal":
        required_scenarios = {key for key, value in workloads["scenarios"].items()
                              if value.get("formal_eligible")}
        if not required_scenarios.issubset(declared):
            problems.append(("INCOMPLETE", "campaign_formal_coverage_missing: "
                             + ",".join(sorted(required_scenarios - set(declared)))))
        for scenario_id in declared:
            if scenario_id not in workloads["scenarios"]:
                problems.append(("FAIL", "campaign_scenario_unknown: "
                                 + str(scenario_id)))
            elif not workloads["scenarios"][scenario_id].get("formal_eligible"):
                problems.append(("FAIL", "campaign_scenario_not_formal: "
                                 + str(scenario_id)))
        expected = tuple(acceptance["sampling"]["formal"]["pair_order"])
        if tuple(campaign["pair_order"]) != expected:
            problems.append(("FAIL", "campaign_pair_order_not_preregistered"))
        if len(campaign["pair_order"]) < acceptance["sampling"]["formal"]["pairs_min"]:
            problems.append(("INCOMPLETE", "campaign_pairs_below_minimum"))
    identities = []
    seen_ids = set()
    listed_dirs = set()
    for record in records:
        run_id = record.get("run_id")
        if not isinstance(run_id, str) or not run_id or run_id in seen_ids:
            problems.append(("FAIL", "campaign_run_id_duplicate_or_invalid"))
            continue
        seen_ids.add(run_id)
        run_path = _run_path(root, record)
        if run_path is None or not run_path.is_dir():
            problems.append(("FAIL", "campaign_run_path_invalid: " + run_id))
            continue
        listed_dirs.add(run_path)
        scenario_id = record.get("scenario")
        if scenario_id not in workloads["scenarios"]:
            problems.append(("FAIL", "campaign_run_scenario: " + run_id))
        if record.get("arm") not in {"A", "B"}:
            problems.append(("FAIL", "campaign_run_arm: " + run_id))
        if not isinstance(record.get("pair_index"), int) or record["pair_index"] < 0:
            problems.append(("FAIL", "campaign_run_pair_index: " + run_id))
        if record.get("state") not in {"fresh", "aged"}:
            problems.append(("FAIL", "campaign_run_state: " + run_id))
        if record.get("profile") != profile:
            problems.append(("FAIL", "campaign_run_profile: " + run_id))
        if record.get("pair_order_label") not in {"AB", "BA"}:
            problems.append(("FAIL", "campaign_run_order_label: " + run_id))
        identities.append((record, run_path))
    runs_directory = root / "runs"
    if runs_directory.is_dir():
        for candidate in runs_directory.iterdir():
            if candidate.is_dir() and candidate.resolve() not in listed_dirs:
                problems.append(("FAIL", "campaign_unlisted_run: " + candidate.name))
    if profile == "formal":
        groups = {}
        for record, _ in identities:
            groups.setdefault((record.get("scenario"), record["pair_index"]),
                              []).append(record)
        required = acceptance["sampling"]["formal"]["pairs_min"]
        for scenario_id in declared:
            for pair_index in range(required):
                members = groups.get((scenario_id, pair_index), [])
                if {member.get("arm") for member in members} != {"A", "B"}:
                    problems.append(("INCOMPLETE", "campaign_pair_incomplete: "
                                     + str(scenario_id) + ":" + str(pair_index)))
                if len(members) == 2:
                    expected_order = acceptance["sampling"]["formal"]["pair_order"][pair_index]
                    if "".join(member.get("arm", "") for member in members) != expected_order \
                            or any(member.get("pair_order_label") != expected_order for member in members):
                        problems.append(("FAIL", "campaign_pair_order_mismatch: " + str(scenario_id)))
                    if len({member.get("state") for member in members}) != 1:
                        problems.append(("FAIL", "campaign_pair_state_mismatch: " + str(scenario_id)))
                if len(members) != 2:
                    problems.append(("INCOMPLETE", "campaign_pair_member_count: "
                                     + str(scenario_id) + ":" + str(pair_index)))
    problems.extend(_state_and_switch_problems(identities, workloads, profile))
    for scenario_id in declared:
        if not any(record.get("scenario") == scenario_id for record, _ in identities):
            problems.append(("INCOMPLETE", "campaign_scenario_missing: "
                             + str(scenario_id)))
    return problems, identities


def _state_and_switch_problems(identities, workloads, profile):
    problems = []
    if not identities:
        return [("INCOMPLETE", "campaign_runs_missing")]
    by_scenario = {}
    for record, run_path in identities:
        by_scenario.setdefault(record["scenario"], []).append((record, run_path))
    if profile == "formal":
        for scenario_id, members in by_scenario.items():
            scenario = workloads["scenarios"].get(scenario_id, {})
            if not scenario.get("formal_eligible"):
                continue
            states = {record.get("state") for record, _ in members}
            if states != {"fresh", "aged"}:
                problems.append(("INCOMPLETE", "campaign_state_missing: "
                                 + scenario_id))
            for record, _ in members:
                if record.get("state") == "aged" \
                        and int(record.get("duration_seconds") or 0) < 3600:
                    problems.append(("INCOMPLETE", "campaign_aged_too_short: "
                                     + scenario_id))
    switch_names = (
        "PERI_WORK_METRICS", "PERI_DIAG_GROUP_PHASE", "PERI_ADMISSION_TRACE",
        "PERI_MAINTENANCE_SCHEDULE", "sql_open_observer", "PERI_SQL_OPEN_OBSERVER",
    )
    for scenario_id, members in by_scenario.items():
        for switch in switch_names:
            values = set()
            for record, _ in members:
                diagnostics = record.get("diagnostics")
                if isinstance(diagnostics, dict) and switch in diagnostics:
                    values.add(diagnostics[switch] is True)
            if True in values and False not in values:
                problems.append(("FAIL", "diagnostic_switch_unpaired: "
                                 + scenario_id + ":" + switch))
    return problems
