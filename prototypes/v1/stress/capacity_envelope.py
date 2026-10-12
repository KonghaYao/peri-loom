import hashlib
import json
import math
from decimal import Decimal
from pathlib import Path

from capacity_metrics import timestamp
import run_guarded as guarded


RPC_ACTIONS = ("open", "execute", "query", "close")
RPC_DEFINITION = "gateway SQL business API RPC: actions_total(open+execute+query+close) delta / observed elapsed; excludes management API and native internal RPC"


def rpc_measurement(samples, raw_rpc, phase, tolerance):
    start, finish = timestamp(phase["started_at_observed"]), timestamp(phase["finished_at_observed"])
    selected = [sample for sample in samples if start <= timestamp(sample["observed_at"]) <= finish
                and sample.get("management_metrics_status") == "available"]
    if len(selected) < 2:
        return {"status": "NOT MEASURED", "reason": "two actual actions_total snapshots required"}
    previous = None
    for sample in selected:
        metrics = sample.get("management_metrics") or {}
        actions = metrics.get("actions_total")
        errors = metrics.get("errors_total")
        if not isinstance(actions, dict) or not isinstance(errors, dict):
            return {"status": "NOT MEASURED", "reason": "actual action/error metrics unavailable"}
        if any(type(value) is not int or value < 0 for value in [*actions.values(), *errors.values()]):
            return {"status": "NOT MEASURED", "reason": "invalid cumulative metrics"}
        if previous is not None and any(actions.get(key, 0) < value for key, value in previous.items()):
            return {"status": "NOT MEASURED", "reason": "action counters reset or changed epoch"}
        previous = actions
    first, last = selected[0], selected[-1]
    lower, upper = timestamp(first["observed_at"]), timestamp(last["observed_at"])
    elapsed = upper - lower
    if elapsed <= 0:
        return {"status": "NOT MEASURED", "reason": "nonpositive metric observation interval"}
    before, after = first["management_metrics"], last["management_metrics"]
    deltas = {action: after["actions_total"].get(action, 0) - before["actions_total"].get(action, 0) for action in RPC_ACTIONS}
    error_delta = sum(after["errors_total"].values()) - sum(before["errors_total"].values())
    observed = [record for record in raw_rpc if lower < timestamp(record["finished_at_observed"]) <= upper
                and record.get("path", "").startswith("/v1/sql/") and record.get("method") == "POST"]
    good = sum(record.get("envelope_ok") is True and record.get("failure") is None for record in observed)
    total = sum(deltas.values())
    valid = total > 0 and error_delta >= 0 and abs(total - len(observed)) <= tolerance and good == len(observed)
    return {"status": "MEASURED" if valid else "NOT MEASURED", "definition": RPC_DEFINITION,
            "first_observed_at": first["observed_at"], "last_observed_at": last["observed_at"],
            "elapsed_seconds": elapsed, "actions_delta": deltas, "errors_delta": error_delta,
            "raw_SQL_RPC": len(observed), "raw_SQL_RPC_ACK": good, "observed_rpc_per_second": total / elapsed,
            "boundary_count_tolerance": tolerance,
            "reason": None if valid else "metric/raw boundary or completion mismatch"}


def safe_envelope(cells, plan, matrix_status):
    acceptance = plan["config"]["acceptance"]
    launch = guarded.read_spec(Path(plan["config"]["launcher_report"]))
    quiet = sorted((cell for cell in cells if cell["topology"] == "single_hot" and cell["workload"] == "mixed"
                    and cell["concurrency"] == 1 and cell["phase"] == "isolated_primary_measure"), key=lambda cell: cell["rate"])
    points = []
    eligible = []
    contiguous = True
    for cell in quiet:
        count = cell.get("counters", {})
        rpc = cell.get("rpc_measurement", {"status": "NOT MEASURED", "reason": "raw actions delta unavailable"})
        source = cell.get("source", {})
        good = (cell["status"] == "MEASURED" and cell.get("guard_valid") is True
                and cell.get("durable_ledger") == "PASS" and cell.get("writer_profiles", {}).get("status") == "PASS"
                and cell.get("resources", {}).get("status") == "MEASURED" and count.get("conserved") is True
                and count.get("offered", 0) > 0 and count.get("ACK", 0) / count["offered"] >= acceptance["quiet_mixed_ack_fraction_min"]
                and all(count.get(key) == 0 for key in ("client_drop", "client_expiry", "typed429", "other_determinate", "Unknown", "pending"))
                and rpc["status"] == "MEASURED" and rpc.get("errors_delta") == 0 and bool(source.get("git_hash")))
        points.append({"business_offered_rate": cell["rate"], "phase_status": cell["status"], "eligible": good,
                       "run_id": cell.get("run_id"), "rpc_measurement": rpc, "source": source})
        if not good:
            contiguous = False
        if contiguous:
            eligible.append(cell)
    sources = {cell["source"]["git_hash"] for cell in eligible}
    if len(sources) > 1:
        eligible = []
    safe = max((cell["rpc_measurement"]["observed_rpc_per_second"] for cell in eligible), default=0) * acceptance["rpc_safety_factor"] if eligible else None
    required = acceptance["soak_default_required_rpc_per_second"]
    with_margin = required * (1 + acceptance["soak_extra_margin_fraction"])
    source_hash = eligible[0]["source"]["git_hash"] if eligible else None
    return {"schema_version": 1, "status": "CANDIDATE FOR MAIN APPROVAL" if safe is not None else "NOT MEASURED",
            "image_id": launch["image"]["Id"], "writer_admission": True, "source_hash": source_hash,
            "source": source_hash, "rpc_definition": RPC_DEFINITION, "safe_rpc_per_second": safe, "rpc_rate_safe": safe,
            "business_load_definition": "quiet single-hot-DB, single project, C1 mixed_oltp; seeded 70% reads / 30% transactional ledger writes, no retries; measured RPC counts, no assumed RPC/business multiplier",
            "qualified_business_rate_points": [cell["rate"] for cell in eligible], "points": points,
            "rpc_safety_factor": acceptance["rpc_safety_factor"],
            "soak_default": {"tx_per_second": 0.2, "RPC_per_tx": 8, "observation_maintenance_RPC_per_second": 0.3,
                             "required_rpc_per_second": required, "extra_margin_fraction": acceptance["soak_extra_margin_fraction"],
                             "required_with_margin_rpc_per_second": with_margin,
                             "status": "NOT MEASURED" if safe is None else "MEETS FROZEN RATE MARGIN" if safe >= with_margin else "INSUFFICIENT"},
            "d05_full_matrix_status": "NOT MEASURED", "d05_selected_coverage_status": matrix_status,
            "approval_required": True, "performance_pass": False,
            "limitations": ["Not an approval capsule or PASS; Main must independently review image/source provenance, raw gate and workload/RPC comparability",
                            "Only a contiguous prefix of frozen measured quiet C1 points; no interpolation, production capacity, starvation or G02 guarantee",
                            "RPC definition excludes management/native-internal calls; Main must reconcile Soak's 8 RPC/tx + observation budget before approval"]}


def unique_object(pairs):
    result = {}
    for name, value in pairs:
        guarded.require(name not in result, "duplicate_capacity_document_field")
        result[name] = value
    return result


def read_capacity_document(path, maximum=64 << 20):
    path = Path(path)
    guarded.private(path, 0o600)
    guarded.private(path.parent, 0o700, True)
    guarded.require(path.stat().st_size <= maximum, "oversized_capacity_document")
    body = path.read_bytes()
    guarded.require(len(body) <= maximum, "oversized_capacity_document")
    return json.loads(body, object_pairs_hook=unique_object), hashlib.sha256(body).hexdigest()


def validate_capacity_capsule(result_path, capsule_path, *, image_id, source, writer_admission, tx_per_second=0.2, source_sha256=None):
    from capacity_evidence import collect_job
    from capacity_plan import digest, read_plan

    result, result_sha256 = read_capacity_document(result_path)
    capsule, _ = read_capacity_document(capsule_path, 64 << 10)
    fields = {"image_id", "writer_admission", "rpc_rate_safe", "source", "result_sha256", "verified_by", "status"}
    extra = {"rpc_definition", "source_sha256", "independent_review"}
    guarded.require(fields <= set(capsule) and not set(capsule) - fields - extra, "unexpected_approval_capsule_fields")
    guarded.require("independent_review" not in capsule or capsule["independent_review"] is True, "independent_review_required")
    guarded.require("rpc_definition" not in capsule or capsule["rpc_definition"] == RPC_DEFINITION, "approval_RPC_definition_mismatch")
    if source_sha256 is not None:
        guarded.require(capsule.get("source_sha256") == source_sha256, "approval_soak_source_changed")
    guarded.require(capsule["status"] == "PASS" and isinstance(capsule["verified_by"], str) and bool(capsule["verified_by"].strip()),
                    "independent_Main_PASS_approval_required")
    guarded.require(capsule["result_sha256"] == result_sha256, "approved_capacity_result_changed")
    guarded.require(writer_admission is True and capsule["writer_admission"] is True, "writer_admission_must_remain_enabled")
    guarded.require(capsule["image_id"] == image_id and capsule["source"] == source, "approval_image_or_source_mismatch")
    guarded.require(result.get("version") == 1 and result.get("task") == "D05/Q02" and result.get("status") != "FAIL"
                    and result.get("performance_pass") is False, "actual_D05_result_required_no_failed_matrix")
    reference = result["frozen_plan"]
    guarded.require(digest(reference["path"]) == reference["sha256"], "approved_frozen_plan_changed")
    plan = read_plan(reference["path"])
    original = result["safe_envelope"]
    guarded.require(original.get("status") == "CANDIDATE FOR MAIN APPROVAL" and original.get("image_id") == image_id
                    and original.get("source") == source and original.get("writer_admission") is True,
                    "measured_quiet_mixed_C1_envelope_required")
    reproduced = []
    for job in plan["jobs"]:
        if job["topology"] != "single_hot" or job["workload"] != "mixed" or job["concurrency"] != 1:
            continue
        cells, _, error = collect_job(job, plan)
        selected = cells.get(("isolated_primary_measure", 1), {"status": "NOT MEASURED"})
        if error:
            selected = {"status": "NOT MEASURED"}
        reproduced.append({"topology": "single_hot", "workload": "mixed", "concurrency": 1,
                           "phase": "isolated_primary_measure", "rate": job["rate"], **selected})
    actual = safe_envelope(reproduced, plan, result["status"])
    for key in ("status", "image_id", "writer_admission", "source", "rpc_definition", "safe_rpc_per_second", "qualified_business_rate_points"):
        guarded.require(actual[key] == original[key], "envelope_does_not_reproduce_from_original_raw")
    rate = capsule["rpc_rate_safe"]
    guarded.require(type(rate) in (int, float) and math.isfinite(rate) and rate > 0
                    and actual["safe_rpc_per_second"] is not None and rate <= actual["safe_rpc_per_second"], "approved_RPC_rate_exceeds_measured_safe_envelope")
    guarded.require(type(tx_per_second) in (int, float) and math.isfinite(tx_per_second) and tx_per_second > 0, "invalid_soak_tx_rate")
    required_decimal = Decimal(str(tx_per_second)) * 8 + Decimal("0.3")
    margin = Decimal(str(plan["config"]["acceptance"]["soak_extra_margin_fraction"]))
    required = float(required_decimal)
    with_margin = float(required_decimal * (1 + margin))
    guarded.require(with_margin <= rate, "soak_RPC_demand_with_margin_exceeds_approved_capacity")
    return {"status": "APPROVED BY INDEPENDENT CAPSULE", "image_id": image_id, "writer_admission": True,
            "rpc_rate_safe": rate, "source": source, "result_sha256": result_sha256,
            "verified_by": capsule["verified_by"], "required_rpc_per_second": required,
            "required_with_margin_rpc_per_second": with_margin, "rpc_definition": actual["rpc_definition"]}


def approve_capacity(result_path, capsule_path, **options):
    return validate_capacity_capsule(result_path, capsule_path, **options)
