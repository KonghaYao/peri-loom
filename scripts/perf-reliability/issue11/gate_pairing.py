"""配对目标、冻结状态与补充证据的 fail-closed 检查。"""
from issue11 import contract
from issue11.gate_common import _is_int, _is_number
from issue11.recovery_v2 import validate_recovery
from issue11.extended_v2 import validate_extended


def synthetic(value):
    if isinstance(value, dict):
        return any(("synthetic" in str(key).lower() and bool(item))
                   or (key == "tag" and "synthetic" in str(item).lower())
                   or synthetic(item) for key, item in value.items())
    if isinstance(value, list):
        return any(synthetic(item) for item in value)
    return False


def reliability_and_extended(view, acceptance, problems):
    if view["record"].get("concurrency") != 1:
        problems.append(("INCOMPLETE", "concurrency_one_unproven"))
    oracle = view["oracle"]
    initial = oracle.get("initial_content_sha256")
    if initial is None:
        problems.append(("INCOMPLETE", "initial_content_verification_missing"))
    elif initial != contract.fixture_digest(oracle.get("fixture_scale", 1000)):
        problems.append(("FAIL", "initial_content_verification_mismatch"))
    proof = oracle.get("recovery")
    if not isinstance(proof, dict) or not _is_int(proof.get("acked_writes")) \
            or proof["acked_writes"] < 0 or not _is_int(proof.get("recovered_writes")) \
            or proof.get("recovered_writes") != proof.get("acked_writes"):
        problems.append(("INCOMPLETE", "acked_write_recovery_unproven: " + view["run_id"]))
    elif proof.get("mismatches") != 0:
        problems.append(("FAIL", "acked_write_recovery_mismatch"))
    residual = view["resources"].get("permit_residual")
    if residual is None:
        problems.append(("INCOMPLETE", "permit_residual_unavailable"))
    elif residual != 0:
        problems.append(("FAIL", "permit_residual_nonzero"))
    evidence = view["resources"].get("extended_evidence", {})
    for name, rule in acceptance["extended_evidence"].items():
        record = evidence.get(name, {})
        if not isinstance(record, dict) or any(field not in record
                                               for field in rule["required_fields"]):
            problems.append(("INCOMPLETE", "extended_evidence_missing: " + name))
        if isinstance(record, dict) and record:
            for field, value in record.items():
                if field not in rule["required_fields"]:
                    continue
                if field in {"method"}:
                    valid = isinstance(value, str) and bool(value)
                elif field == "configured":
                    valid = isinstance(value, dict) and bool(value)
                elif field == "confidence":
                    valid = _is_number(value) and 0 < value < 1
                elif "difference" in field:
                    valid = _is_number(value)
                else:
                    valid = _is_number(value) and value >= 0
                if not valid:
                    problems.append(("FAIL", "extended_evidence_invalid: " + name + "." + field))
    validate_recovery(view, problems)
    validate_extended(evidence, problems, acceptance)


def paired_problems(runs, views, acceptance):
    problems = []
    grouped = {}
    sources = {view["run_id"]: view for view in views}
    for run in runs:
        grouped.setdefault((run["scenario"], run["state"], run["pair_index"]), []).append(run)
    complete = {}
    for (scenario, state, pair), members in grouped.items():
        arms = {member["arm"]: member for member in members}
        if len(members) != 2 or set(arms) != {"A", "B"}:
            problems.append(("INCOMPLETE", "paired_baseline_missing: " + scenario))
            continue
        left, right = (sources[arms[arm]["run_id"]] for arm in ("A", "B"))
        expected = acceptance["sampling"]["formal"]["pair_order"]
        if not _is_int(pair) or not 0 <= pair < len(expected):
            problems.append(("FAIL", "paired_order_index_invalid: " + scenario))
        else:
            ordered = [sources[arms[arm]["run_id"]].get("metadata", {}) for arm in expected[pair]]
            windows = [(item.get("started_at_unix_ms"), item.get("completed_at_unix_ms"))
                       for item in ordered]
            if any(not _is_int(start) or not _is_int(end) or start <= 0 or end <= start
                   for start, end in windows):
                problems.append(("INCOMPLETE", "paired_execution_window_missing: " + scenario))
            elif windows[0][1] > windows[1][0]:
                problems.append(("FAIL", "paired_execution_order_or_overlap: " + scenario))
        if left["record"].get("diagnostics") != right["record"].get("diagnostics"):
            problems.append(("FAIL", "paired_diagnostics_mismatch: " + scenario))
        for field in ("host", "limits", "backend", "encryption", "quotas", "mounts", "tls"):
            if left["environment"].get(field) != right["environment"].get(field):
                problems.append(("FAIL", "paired_environment_mismatch: " + field))
        for field in ("root_type", "persistent", "durable", "retention", "store_root", "cache_root"):
            if left["environment"].get("storage", {}).get(field) != right["environment"].get("storage", {}).get(field):
                problems.append(("FAIL", "paired_storage_mismatch: " + field))
        matched = left["record"].get("matched_state")
        if not isinstance(matched, dict) or any(not matched.get(key) for key in
                                                ("data_sha256", "metadata_sha256")):
            problems.append(("INCOMPLETE", "matched_data_metadata_state_missing"))
        elif matched != right["record"].get("matched_state"):
            problems.append(("FAIL", "paired_data_metadata_state_mismatch"))
        if left["oracle"].get("fixture_sha256") != right["oracle"].get("fixture_sha256"):
            problems.append(("FAIL", "paired_fixture_mismatch"))
        complete.setdefault(scenario, set()).add((state, pair))
        baseline, candidate = arms["A"]["stats"], arms["B"]["stats"]
        target = acceptance["targets"][scenario]
        reduction = target.get("p50_reduction_min")
        if reduction is not None and candidate["p50_ms"] > baseline["p50_ms"] * (1 - reduction):
            problems.append(("FAIL", "paired_p50_reduction: " + scenario))
        for quantile in ("p50", "p95", "p99"):
            maximum = target.get(quantile + "_regression_max")
            if maximum is not None and candidate[quantile + "_ms"] > baseline[quantile + "_ms"] * (1 + maximum):
                problems.append(("FAIL", "paired_" + quantile + "_regression: " + scenario))
    for scenario in acceptance["sampling"]["formal"]["required_scenarios"]:
        if len(complete.get(scenario, ())) < acceptance["sampling"]["formal"]["pairs_min"]:
            problems.append(("INCOMPLETE", "paired_groups_below_five: " + scenario))
    return problems
