#!/usr/bin/env python3
"""Campaign/arm configuration and AB/BA/AB pairing orchestration."""
import hashlib
import json
from pathlib import Path
import re
import shutil

from perf_contract import ensure_empty_directory


class CampaignError(ValueError):
    pass


DEFAULT_ORDER = ["AB", "BA", "AB"]
REQUIRED_RUNS = {"G1": [("S1", 2)], "G2": [("S1", 2), ("S1", 1), ("S2", 2)]}
CAMPAIGN_ID = re.compile(r"^[A-Za-z0-9._-]+$")


def _failure_marker(value):
    if value is None or value is False:
        return False
    if isinstance(value, str):
        return value != ""
    if isinstance(value, (dict, list, tuple, set)):
        return bool(value)
    return True


def load_campaign(path):
    try:
        campaign = json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as failure:
        raise CampaignError("campaign_unreadable: " + type(failure).__name__) from None
    if campaign.get("schema") != 1:
        raise CampaignError("campaign_schema")
    if not isinstance(campaign.get("id"), str) or not campaign["id"]:
        raise CampaignError("campaign_id")
    if not CAMPAIGN_ID.match(campaign["id"]):
        raise CampaignError("campaign_id_invalid")
    arms = campaign.get("arms")
    if not isinstance(arms, dict) or set(arms) != {"A", "B"} \
            or any(not isinstance(arm, dict) for arm in arms.values()):
        raise CampaignError("campaign_arms_must_be_A_and_B")
    roles = [arms[label].get("role") for label in ("A", "B")]
    if any(not isinstance(role, str) for role in roles) \
            or sorted(roles) != ["candidate", "denominator"]:
        raise CampaignError("campaign_roles_must_be_candidate_and_denominator")
    for label, arm in arms.items():
        if not isinstance(arm.get("binary"), str) or not arm["binary"]:
            raise CampaignError("campaign_arm_binary: " + label)
        if _failure_marker(arm.get("failure")):
            raise CampaignError("campaign_arm_failure: " + label)
        for key in ("version", "commit"):
            if key in arm and (not isinstance(arm[key], str) or not arm[key]):
                raise CampaignError("campaign_arm_" + key + ": " + label)
    # A campaign or arm marked failed must not be reviewable as evidence.
    if _failure_marker(campaign.get("failure")):
        raise CampaignError("campaign_failure")
    order = campaign.get("order", DEFAULT_ORDER)
    if not isinstance(order, list) or not order \
            or any(not isinstance(item, str) or item not in {"AB", "BA"} for item in order):
        raise CampaignError("campaign_order")
    pairs = campaign.get("pairs", len(order))
    if type(pairs) is not int or pairs <= 0 or len(order) != pairs:
        raise CampaignError("campaign_pair_count")
    cases = campaign.get("cases")
    if not isinstance(cases, list) or not cases:
        raise CampaignError("campaign_cases")
    seen = set()
    for case in cases:
        identifier = case.get("id") if isinstance(case, dict) else None
        if not isinstance(case, dict) or not isinstance(identifier, str) \
                or identifier not in {"S1", "S2"}:
            raise CampaignError("campaign_case_not_implemented: " + str(identifier))
        concurrency = case.get("concurrency")
        if not isinstance(concurrency, list) or not concurrency \
                or any(type(value) is not int or value not in {1, 2}
                       for value in concurrency):
            raise CampaignError("campaign_case_concurrency")
        for value in concurrency:
            key = (case["id"], value)
            if key in seen:
                raise CampaignError("campaign_case_duplicate")
            seen.add(key)
    if "tier" in campaign and (not isinstance(campaign["tier"], str)
                               or campaign["tier"] not in REQUIRED_RUNS):
        raise CampaignError("campaign_tier")
    snapshots = campaign.get("snapshots")
    if snapshots is not None and (not isinstance(snapshots, dict)
                                  or any(not isinstance(value, str) or not value
                                         for value in snapshots.values())):
        raise CampaignError("campaign_snapshots")
    expected = campaign.get("expected_denominator")
    if expected is not None:
        if not isinstance(expected, dict) or not isinstance(expected.get("version"), str) \
                or not expected.get("version") or not isinstance(expected.get("commit"), str) \
                or not expected.get("commit"):
            raise CampaignError("campaign_expected_denominator")
    return campaign


def validate_coverage(campaign, tier):
    if tier not in REQUIRED_RUNS:
        raise CampaignError("tier_unknown: " + str(tier))
    available = {(case["id"], value) for case in campaign["cases"] for value in case["concurrency"]}
    missing = [case + "/C" + str(value) for case, value in REQUIRED_RUNS[tier] if (case, value) not in available]
    if missing:
        raise CampaignError("campaign_missing_required_runs: " + ",".join(missing))
    return REQUIRED_RUNS[tier]


def validate_profile(campaign, profile):
    order = campaign.get("order", DEFAULT_ORDER)
    pairs = campaign.get("pairs", len(order))
    if pairs != profile["pairs"] or order != profile["order"]:
        raise CampaignError("campaign_pairing_must_match_profile")
    if profile["name"] not in {"quick", "formal"}:
        raise CampaignError("campaign_profile_not_supported: " + profile["name"])
    return {"pairs": pairs, "order": order}


def arm_role(campaign, label):
    return campaign["arms"][label]["role"]


def ordered_arms(campaign, pair_index):
    order = campaign.get("order", DEFAULT_ORDER)
    if not 0 <= pair_index < len(order):
        raise CampaignError("pair_index_out_of_range")
    return list(order[pair_index])


def run_id(pair, arm, case, concurrency):
    return "pair" + str(pair) + "-" + arm + "-" + case + "-c" + str(concurrency)


def planned_runs(campaign, profile):
    for case in campaign["cases"]:
        for concurrency in case["concurrency"]:
            for pair in range(profile["pairs"]):
                for arm in ordered_arms(campaign, pair):
                    yield {"pair": pair, "arm": arm, "case": case["id"], "concurrency": concurrency,
                           "order": campaign.get("order", DEFAULT_ORDER)[pair],
                           "run_id": run_id(pair, arm, case["id"], concurrency)}


def arm_metadata_problems(campaign, label):
    if not isinstance(label, str) or label not in campaign["arms"]:
        return ["arm_unknown"]
    arm = campaign["arms"][label]
    problems = [key for key in ("version", "commit") if not arm.get(key)]
    expected = campaign.get("expected_denominator")
    if arm.get("role") == "denominator" and expected is not None:
        for key in ("version", "commit"):
            if arm.get(key) != expected.get(key):
                problems.append("expected_denominator_" + key)
    return problems


def check_arm_evidence(campaign, label, metadata):
    if not isinstance(label, str) or label not in campaign["arms"]:
        raise CampaignError("arm_unknown: " + str(label))
    if metadata.get("arm") != label:
        raise CampaignError("arm_label_mismatch")
    arm = campaign["arms"][label]
    for key in ("version", "commit"):
        if metadata.get("arm_" + key) != arm.get(key):
            raise CampaignError("arm_" + key + "_mismatch: " + label)
    expected = campaign.get("expected_denominator")
    if arm.get("role") == "denominator" and expected is not None:
        for key in ("version", "commit"):
            if metadata.get("arm_" + key) != expected[key]:
                raise CampaignError("denominator_" + key + "_mismatch")
    return True


def tree_digest(path):
    path = Path(path).resolve()
    hasher = hashlib.sha256()
    for candidate in sorted(path.rglob("*")):
        if candidate.is_file():
            hasher.update(candidate.relative_to(path).as_posix().encode())
            hasher.update(b"\0")
            with candidate.open("rb") as source:
                while chunk := source.read(1024 * 1024):
                    hasher.update(chunk)
            hasher.update(b"\0")
    return hasher.hexdigest()


def prepare_store_directory(path, snapshot=None):
    path = ensure_empty_directory(path)
    if snapshot is None:
        return {"path": str(path), "snapshot_sha256": None, "store_sha256": tree_digest(path)}
    snapshot = Path(snapshot)
    if not snapshot.is_dir():
        raise CampaignError("snapshot_missing: " + str(snapshot))
    before = tree_digest(snapshot)
    shutil.copytree(snapshot, path, dirs_exist_ok=True)
    after = tree_digest(path)
    if after != before:
        raise CampaignError("snapshot_copy_mismatch")
    return {"path": str(path), "snapshot_sha256": before, "store_sha256": after}


def campaign_identity(campaign):
    canonical = json.dumps(campaign, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()
