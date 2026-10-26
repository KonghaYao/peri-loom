"""Semantic constraints for issue11 supplemental evidence (extension v2)."""
import math
from issue11 import contract
from issue11.gate_common import _is_int, _is_number


def validate_extended(evidence, problems, acceptance=None):
    if not isinstance(evidence, dict):
        return  # The required-field validator reports absent evidence.

    def section(name):
        item = evidence.get(name)
        return item if isinstance(item, dict) else {}

    def fail(reason):
        problems.append(("FAIL", "extended_semantics: " + reason))

    rules = (acceptance or contract.load_acceptance())["extended_evidence"]

    capacity = section("capacity")
    offered, successful = capacity.get("offered_rps"), capacity.get("successful_rps")
    if _is_number(offered) and _is_number(successful) and successful > offered:
        fail("successful_rps_exceeds_offered")
    if capacity:
        if not _is_int(capacity.get("concurrency")):
            fail("capacity_concurrency_not_integer")
        for name in ("concurrency", "duration_seconds"):
            if _is_number(capacity.get(name)) and capacity[name] <= 0:
                fail("capacity_" + name + "_not_positive")
        fraction = capacity.get("arrival_fraction_of_capacity")
        if _is_number(fraction) and fraction not in {0.25, 0.5, 0.75, 0.9}:
            fail("capacity_arrival_fraction")
        if _is_number(offered) and offered <= 0:
            fail("capacity_no_offers")

    queue = section("queueing")
    waits = [queue.get("wait_" + quantile + "_ms") for quantile in ("p50", "p95", "p99")]
    if all(_is_number(value) for value in waits) and waits != sorted(waits):
        fail("queue_quantile_order")
    if queue and not _is_int(queue.get("queue_depth_max")):
        fail("queue_depth_not_integer")

    overload = section("overload")
    if overload and (not _is_int(overload.get("offered_concurrency"))
                     or overload["offered_concurrency"] != rules["overload"]["execution_concurrency"]):
        problems.append(("INCOMPLETE", "overload_offered_concurrency_unproven"))
    for name in ("failure", "outcome_unknown"):
        if _is_number(overload.get(name)) and overload[name] != 0:
            fail("overload_" + name)
    fields = ("offered", "completed", "rejected_before_execution", "failure", "outcome_unknown")
    if overload and all(name in overload for name in fields):
        if not all(_is_int(overload[name]) and overload[name] >= 0 for name in fields):
            fail("overload_counts_not_integers")
        elif overload["offered"] <= 0:
            fail("overload_no_offers")
        else:
            if sum(overload[name] for name in fields[1:]) != overload["offered"]:
                fail("overload_counts_do_not_close")
            ratio = overload.get("rejection_ratio")
            expected = overload["rejected_before_execution"] / overload["offered"]
            if _is_number(ratio) and not math.isclose(ratio, expected, rel_tol=1e-9, abs_tol=1e-12):
                fail("overload_rejection_ratio")
    values = [overload.get("all_request_" + quantile + "_ms") for quantile in ("p50", "p95", "p99")]
    if all(_is_number(value) for value in values) and values != sorted(values):
        fail("overload_quantile_order")

    quotas = section("quotas")
    if _is_number(quotas.get("violations")) and quotas["violations"] != 0:
        fail("quota_violations")
    configured = quotas.get("configured", {})
    maximum = configured.get("max_concurrent") if isinstance(configured, dict) else None
    if quotas:
        if not _is_int(maximum) or maximum <= 0 or not _is_int(quotas.get("observed")):
            fail("quota_concurrency_evidence")
        elif maximum != rules["quotas"]["max_concurrent"]:
            fail("quota_configured_differs_from_contract")
        elif quotas["observed"] > maximum:
            fail("quota_observed_exceeds_configured")

    confidence = section("confidence_intervals")
    for stem in ("p50", "p99", "paired_p50_difference", "paired_p99_difference"):
        suffix = "_ms" if "difference" in stem else ""
        lower, upper = (confidence.get(stem + side + suffix) for side in ("_lower", "_upper"))
        if _is_number(lower) and _is_number(upper) and lower > upper:
            fail("confidence_bounds_reversed: " + stem)
