import datetime
import math


DEFAULT_ACCEPTANCE = {
    "ack_growth_below": 0.10,
    "consecutive_plateaus": 2,
    "victim_ack_fraction_min": 0.80,
    "equal_normalized_jain_min": 0.90,
    "progress_window_seconds": 5,
    "acks_per_window_min": 1,
    "max_progress_gap_seconds": 5,
    "reserved_cleanup_seconds": 5,
    "require_effective_fifo_tickets": True,
    "require_actual_io_counters": True,
    "rpc_safety_factor": 0.50,
    "quiet_mixed_ack_fraction_min": 0.99,
    "rpc_metric_raw_count_tolerance": 2,
    "soak_default_required_rpc_per_second": 1.9,
    "soak_extra_margin_fraction": 0.20,
}


def timestamp(value):
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def quantiles(values):
    ordered = sorted(values)
    if not ordered:
        return {"count": 0, "p50_ns": None, "p95_ns": None, "p99_ns": None, "max_ns": None}
    return {"count": len(ordered), **{
        f"p{percent}_ns": ordered[math.ceil(len(ordered) * percent / 100) - 1]
        for percent in (50, 95, 99)
    }, "max_ns": ordered[-1]}


def counters(offered, dropped, operations, expired=None, pending=0, sent=None):
    ack = typed = other = unknown = 0
    for operation in operations:
        failure = operation.get("failure") or {}
        if operation.get("acked") is True:
            ack += 1
        elif failure.get("unknown") is True:
            unknown += 1
        elif failure.get("http_status") == 429 and failure.get("class") not in (None, "unknown"):
            typed += 1
        elif failure.get("class") not in (None, "unknown"):
            other += 1
        else:
            pending += 1
    if sent is None:
        sent = len(operations)
    else:
        pending += sent - len(operations)
    if expired is None:
        expired = offered - dropped - sent
    result = {"offered": offered, "client_drop": dropped, "client_expiry": expired,
              "sent": sent, "ACK": ack, "typed429": typed, "other_determinate": other,
              "Unknown": unknown, "pending": pending}
    result["conserved"] = (all(type(value) is int and value >= 0 for value in result.values())
                           and offered == dropped + expired + sent
                           and sent == ack + typed + other + unknown + pending)
    return result


def progress(operations, started, duration, acceptance):
    start = timestamp(started)
    end = start + duration
    completed = sorted(timestamp(operation["finished_at_observed"]) for operation in operations
                       if operation.get("acked") and start <= timestamp(operation["finished_at_observed"]) <= end)
    boundaries = [start, *completed, end]
    gap = max(right - left for left, right in zip(boundaries, boundaries[1:]))
    window = acceptance["progress_window_seconds"]
    windows = [sum(start + offset <= value < start + offset + window for value in completed)
               for offset in range(0, int(duration - window) + 1, window)]
    return {"offering_window_ACK": len(completed), "max_observed_gap_seconds": gap,
            "fixed_window_ACK": windows,
            "status": "PASS" if windows and min(windows) >= acceptance["acks_per_window_min"]
            and gap <= acceptance["max_progress_gap_seconds"] else "FAIL",
            "scope": "finite observed wall-time windows, guard clock-drift gate required; not starvation freedom"}


def resource_window(samples, phase):
    start, end = timestamp(phase["started_at_observed"]), timestamp(phase["finished_at_observed"])
    selected = [sample for sample in samples if start <= timestamp(sample["observed_at"]) <= end]
    usable = [sample for sample in selected if sample.get("processes_status") != "unavailable_no_pids"]
    processes = {}
    for sample in selected:
        for process in sample.get("processes", []):
            if process.get("status") != "available":
                continue
            scope = sample.get("source", "host.ps") + ":" + str(sample.get("container_id", "host"))
            key = f'{scope}:{process["role"]}:{process["pid"]}'
            processes.setdefault(key, []).append(process)
    measured = len(usable) >= 2 and bool(processes) and all(sample.get("processes_status") == "available" for sample in usable)
    return {"status": "MEASURED" if measured else "NOT MEASURED", "sample_count": len(selected),
            "processes": {key: {
            "peak_rss_bytes": max(item["rss_bytes"] for item in values),
            "peak_ps_cpu_percent": max((item["cpu_percent_ps"] for item in values if item.get("cpu_percent_ps") is not None), default=None),
            "peak_proc_interval_cpu_percent": max((item["cpu_percent_proc_interval"] for item in values if item.get("cpu_percent_proc_interval") is not None), default=None),
                "cpu_seconds_delta": (values[-1]["cpu_seconds_proc"] - values[0]["cpu_seconds_proc"] if "cpu_seconds_proc" in values[0]
                                      else values[-1]["cpu_seconds_ps"] - values[0]["cpu_seconds_ps"]),
                "start_identity": values[0].get("process_start_identity"),
            } for key, values in processes.items()},
            "service_queue_delay": None,
            "service_queue_status": "NOT MEASURED",
            "sources": sorted({sample.get("source", "host.ps") for sample in usable}),
            "scope": "host/container process samples, not native IO/cache/queue counters"}


def writer_profiles(samples, phase, projects):
    start, finish = timestamp(phase["started_at_observed"]), timestamp(phase["finished_at_observed"])
    observed = []
    missing = False
    disabled = False
    for sample in samples:
        if not start <= timestamp(sample["observed_at"]) <= finish:
            continue
        if sample.get("source") == "container.proc.status":
            continue
        metrics = sample.get("management_metrics") or {}
        if not isinstance(metrics, dict):
            missing = True
            continue
        project_metrics = metrics.get("projects", {})
        native = metrics.get("native", {})
        if not isinstance(project_metrics, dict) or not isinstance(native, dict):
            missing = True
            continue
        native_workers = native.get("workers", {})
        if not isinstance(native_workers, dict):
            missing = True
            continue
        project_flags = {project: project_metrics.get(project, {}).get("writer_admission") for project in projects}
        worker_flags = {worker: entry.get("writer_profile") for worker, entry in native_workers.items() if isinstance(entry, dict)}
        flags = [*project_flags.values(), *worker_flags.values()]
        missing = missing or not worker_flags or any(type(flag) is not bool for flag in flags) or sample.get("management_metrics_status") != "available"
        disabled = disabled or any(flag is False for flag in flags)
        observed.append({"observed_at": sample["observed_at"], "project.writer_admission": project_flags,
                         "worker.writer_profile": worker_flags})
    return {"status": "FAIL" if disabled else "NOT MEASURED" if missing or not observed else "PASS",
            "requested_writer_admission": True, "observations": observed,
            "scope": "actual Go project and native worker profile flags; enabling alone does not prove reserved cleanup/FIFO"}


def phase_cell(phase, scenario, operations, drops, samples, acceptance, demand, external_busy=False):
    if not phase.get("started_at_observed"):
        return {"status": "NOT MEASURED", "reason": phase.get("reason", "phase not complete")}
    counts = counters(phase["offered"], phase["client_admission_dropped"], operations, phase["client_queue_expired"], sent=phase["attempted"])
    projects = {}
    for project, offered in phase["per_project_offered"].items():
        selected = [operation for operation in operations if operation["project"] == project]
        project_counts = counters(offered, sum(drop["project"] == project for drop in drops), selected)
        projects[project] = {
            "counters": project_counts,
            "ack_fraction": project_counts["ACK"] / offered if offered else None,
            "all_latency": quantiles([operation["http_workflow_ns"] for operation in selected]),
            "ACK_latency": quantiles([operation["http_workflow_ns"] for operation in selected if operation["acked"]]),
            "queue_latency": quantiles([operation["client_queue_ns"] for operation in selected]),
            "progress": progress(selected, phase["started_at_observed"], phase["requested_duration_ns"] / 1e9, acceptance),
        }
    fractions = [entry["ack_fraction"] for entry in projects.values() if entry["ack_fraction"] is not None]
    jain = sum(fractions) ** 2 / (len(fractions) * sum(value ** 2 for value in fractions)) if fractions and any(fractions) else None
    proofs = [proof for proof in scenario.get("correctness", [])
              if proof.get("phase") == phase["phase"] and proof.get("concurrency") == phase["concurrency"]]
    ledger = bool(projects) and len(proofs) >= phase["independently_active_databases"] and all(proof.get("status") == "PASS" for proof in proofs) and all(any(proof.get("project") == project and proof.get("status") == "PASS"
                                      and proof.get("expected_ledger_hash") == proof.get("actual_ledger_hash")
                                      and bool(proof.get("actual_ledger_hash")) for proof in proofs)
                                  for project in projects)
    issues = []
    if phase.get("status") != "COMPLETE":
        issues.append("phase incomplete: " + phase.get("status", "missing status"))
    if not counts["conserved"] or not all(entry["counters"]["conserved"] for entry in projects.values()):
        issues.append("counter conservation failed")
    if len(drops) != counts["client_drop"] or counts["ACK"] != phase["acked_workflows"] or len(operations) != phase["attempted"]:
        issues.append("raw/summary counts disagree")
    if phase.get("completed_workflows") != len(operations) or phase.get("unknown") != counts["Unknown"]:
        issues.append("completed/Unknown summary mismatch")
    if any(operation.get("acked") and operation.get("correct") is not True for operation in operations):
        issues.append("acknowledged raw correctness not proven")
    if counts["Unknown"] or counts["pending"] or phase.get("cleanup_unknown") or phase.get("correctness_failures"):
        issues.append("Unknown/pending/cleanup/correctness unresolved")
    if not ledger:
        issues.append("per-project per-phase durable ledger missing")
    if any(entry["progress"]["status"] != "PASS" for entry in projects.values()):
        issues.append("finite progress bound failed")
    if demand == "equal" and len(projects) > 1 and (jain is None or jain < acceptance["equal_normalized_jain_min"]):
        issues.append("demand-normalized fairness bound failed")
    if demand == "victim" and phase["phase"].startswith("combined"):
        victim = min(projects.values(), key=lambda entry: entry["counters"]["offered"])
        if victim["ack_fraction"] is None or victim["ack_fraction"] < acceptance["victim_ack_fraction_min"]:
            issues.append("victim ACK/offered bound failed")
    limited = []
    if external_busy:
        limited.append("external_busy: interference cannot be attributed")
    if phase.get("scheduled_offers_not_emitted") or phase.get("operation_cap_reached"):
        limited.append("offered schedule incomplete/capped")
    resources = resource_window(samples, phase)
    if resources["status"] != "MEASURED":
        limited.append("resource samples unavailable")
    profiles = writer_profiles(samples, phase, projects)
    if profiles["status"] == "FAIL":
        issues.append("Go/native writer profile disabled")
    elif profiles["status"] == "NOT MEASURED":
        limited.append("actual Go/native writer profile metrics unavailable")
    return {"status": "FAIL" if issues else "NOT MEASURED" if limited else "MEASURED",
            "issues": issues, "limitations": limited, "counters": counts, "projects": projects,
            "durable_ledger": "PASS" if ledger else "NOT MEASURED", "ledger_evidence": proofs,
            "fairness": {"demand_normalized_jain": jain, "no_starvation_guarantee": False},
            "resources": resources, "observed_offered_rate": phase["observed_offered_per_second"],
            "effective_configured_rate": phase.get("effective_configured_offered_per_second"),
            "writer_profiles": profiles,
            "ACK_rate_including_drain": phase["acked_per_second_including_drain"],
            "queue_p95_ns": phase["client_queue_latency"]["p95_ns"],
            "elapsed_ns": phase["elapsed_ns"], "offering_elapsed_ns": phase["offering_elapsed_ns"],
            "planned_offers": phase["planned_offers"], "missed_offers": phase["scheduled_offers_not_emitted"],
            "all_latency": phase["all_workflow_latency"], "ACK_latency": phase["acked_workflow_latency"],
            "queue_latency": phase["client_queue_latency"]}


def knee(points, acceptance):
    ordered = sorted(points, key=lambda point: point["rate"])
    streak = 0
    transitions = []
    for previous, current in zip(ordered, ordered[1:]):
        if previous["status"] != "MEASURED" or current["status"] != "MEASURED":
            streak = 0
            continue
        base = previous["ACK_rate_including_drain"]
        if base <= 0 or current["observed_offered_rate"] <= previous["observed_offered_rate"]:
            streak = 0
            continue
        growth = current["ACK_rate_including_drain"] / base - 1
        rejection_rise = current["counters"]["typed429"] > previous["counters"]["typed429"]
        queue_rise = (current["queue_p95_ns"] is not None and previous["queue_p95_ns"] is not None
                      and current["queue_p95_ns"] > previous["queue_p95_ns"])
        overload = current["counters"]["client_drop"] + current["counters"]["client_expiry"]
        transitions.append({"from_rate": previous["rate"], "to_rate": current["rate"], "ACK_growth": growth,
                            "queue_rise": queue_rise, "typed429_rise": rejection_rise,
                            "client_overload": overload,
                            "resource_comparison": {"before": previous["resources"], "after": current["resources"]}})
        streak = streak + 1 if growth < acceptance["ack_growth_below"] and (queue_rise or rejection_rise) else 0
        if streak >= acceptance["consecutive_plateaus"]:
            return {"status": "OBSERVED FUNCTIONAL KNEE", "bracket_rates": [transitions[-2]["from_rate"], current["rate"]],
                    "transitions": transitions[-2:],
                    "mechanism": "typed service rejection observed" if rejection_rise else "client queue/offered overload; service limiter NOT PROVEN",
                    "resource_causality": "samples compared, correlation is not quota/IO bottleneck attribution",
                    "performance_pass": False}
    return {"status": "NOT FOUND" if ordered and all(point["status"] == "MEASURED" for point in ordered) else "NOT MEASURED",
            "reason": "no two consecutive measured ACK plateaus with rising queue/rejections",
            "transitions": transitions, "performance_pass": False}
