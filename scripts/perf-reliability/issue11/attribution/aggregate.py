#!/usr/bin/env python3
"""Independent request/step/phase attribution aggregation.

Rules enforced here:
  * joins use identifiers only; timestamps are never compared across clocks
  * nested child durations are subtracted from the parent exactly once
  * phase percentiles are computed per request and then across requests; phase
    p99 values are never summed
  * every request must carry a pipeline correlation and unique request/span IDs
"""
import json
import math
from pathlib import Path

SCHEMA_PATH = Path(__file__).resolve().parent / "schema.json"
OUTCOMES = {"success", "error", "rejected", "outcome_unknown"}
PHASES = {"setup", "warmup", "timed", "cleanup"}


class AttributionError(ValueError):
    pass


def _load_schema():
    try:
        return json.loads(SCHEMA_PATH.read_text())
    except (OSError, json.JSONDecodeError) as failure:
        raise AttributionError("attribution_schema_unreadable: "
                               + type(failure).__name__) from None


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _require_string(record, key, problems, where):
    value = record.get(key)
    if not isinstance(value, str) or not value:
        problems.append("missing_" + key + ": " + where)
        return None
    return value


def _validate_span(span, request_id, problems, where):
    if not isinstance(span, dict):
        problems.append("span_not_object: " + where)
        return None
    if span.get("request_id") not in (None, request_id):
        problems.append("cross_request_span_ref: " + where)
    span_id = _require_string(span, "span_id", problems, where)
    process = span.get("process")
    phase = span.get("phase")
    clock_id = _require_string(span, "clock_id", problems, where)
    outcome = span.get("outcome")
    if process not in {"client", "gateway", "worker", "authority", "object_store"}:
        problems.append("span_process: " + where)
    if not isinstance(phase, str) or not phase:
        problems.append("span_phase: " + where)
    if outcome not in OUTCOMES:
        problems.append("span_outcome: " + where)
    duration = span.get("duration_ns")
    if not _is_int(duration) or duration < 0:
        problems.append("span_duration: " + where)
    started = span.get("started_monotonic_ns")
    if started is not None and (not _is_int(started) or started < 0):
        problems.append("span_started: " + where)
    parent = span.get("parent_span_id")
    if parent is not None and (not isinstance(parent, str) or not parent):
        problems.append("span_parent: " + where)
    refs = span.get("refs")
    if refs is not None and not isinstance(refs, dict):
        problems.append("span_refs: " + where)
    if isinstance(refs, dict):
        for key, value in refs.items():
            if not isinstance(key, str) or not isinstance(value, str):
                problems.append("span_ref_type: " + where)
        if refs.get("request_id") not in (None, request_id):
            problems.append("cross_request_span_ref: " + where)
    return {
        "span_id": span_id,
        "parent_span_id": parent,
        "process": process,
        "phase": phase,
        "clock_id": clock_id,
        "duration_ns": duration,
        "started_monotonic_ns": started,
        "nested": bool(span.get("nested", False)),
        "outcome": outcome,
        "refs": refs or {},
    }


def load_requests(path):
    """Load a JSONL file and validate it."""
    try:
        lines = Path(path).read_text().splitlines()
    except OSError as failure:
        raise AttributionError("attribution_unreadable: "
                               + type(failure).__name__) from None
    records = []
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            raise AttributionError("attribution_json: " + str(line_number)) from None
        if not isinstance(record, dict):
            raise AttributionError("attribution_record_not_object: " + str(line_number))
        records.append(record)
    return load_records(records)


def load_records(records):
    """Validate inline or sidecar span records; fail closed on inconsistency."""
    requests = {}
    sidecars = []
    order = []
    for line_number, record in enumerate(records, 1):
        kind = record.get("record_type")
        if kind == "span":
            sidecars.append((line_number, record))
            continue
        if kind != "request":
            raise AttributionError("attribution_record_type: " + str(line_number))
        request_id = record.get("request_id")
        if not isinstance(request_id, str) or not request_id:
            raise AttributionError("attribution_request_id: " + str(line_number))
        if request_id in requests:
            raise AttributionError("duplicate_request_id: " + request_id)
        requests[request_id] = record
        order.append(request_id)
    problems = []
    spans_by_request = {request_id: [] for request_id in requests}
    span_ids = set()
    for request_id in order:
        record = requests[request_id]
        where = "request:" + request_id
        if record.get("schema_version") != 1:
            problems.append("request_schema_version: " + where)
        correlation = _require_string(record, "pipeline_correlation_id", problems, where)
        _require_string(record, "scenario", problems, where)
        if record.get("phase") not in PHASES:
            problems.append("request_phase: " + where)
        total = record.get("total_ns")
        if not _is_int(total) or total < 0:
            problems.append("request_total: " + where)
        coverage = record.get("process_coverage")
        if not isinstance(coverage, list) or not coverage \
                or any(not isinstance(item, str) for item in coverage):
            problems.append("request_process_coverage: " + where)
        step_ids = record.get("step_ids")
        if not isinstance(step_ids, list) or not step_ids \
                or any(not isinstance(item, str) or not item for item in step_ids):
            problems.append("request_step_ids: " + where)
        if not correlation:
            problems.append("missing_correlation: " + where)
        inline = record.get("spans")
        if not isinstance(inline, list):
            problems.append("request_spans: " + where)
            inline = []
        for span in inline:
            if isinstance(span, dict) and span.get("pipeline_correlation_id", correlation) != correlation:
                problems.append("cross_request_stitch: " + request_id)
            parsed = _validate_span(span, request_id, problems, where)
            if parsed is None:
                continue
            if parsed["span_id"] in span_ids:
                problems.append("duplicate_span: " + str(parsed["span_id"]))
                continue
            span_ids.add(parsed["span_id"])
            spans_by_request[request_id].append(parsed)
    for line_number, span in sidecars:
        request_id = span.get("request_id")
        where = "span_line:" + str(line_number)
        if not isinstance(request_id, str) or request_id not in requests:
            problems.append("unknown_request_id: " + str(request_id))
            continue
        parsed = _validate_span(span, request_id, problems, where)
        if parsed is None:
            continue
        if parsed["span_id"] in span_ids:
            problems.append("duplicate_span: " + str(parsed["span_id"]))
            continue
        correlation = span.get("pipeline_correlation_id")
        if not correlation:
            problems.append("missing_correlation: " + where)
        elif correlation != requests[request_id].get("pipeline_correlation_id"):
            problems.append("cross_request_stitch: " + request_id)
        span_ids.add(parsed["span_id"])
        spans_by_request[request_id].append(parsed)
    if problems:
        raise AttributionError("; ".join(sorted(set(problems))))
    documents = []
    for request_id in order:
        record = requests[request_id]
        spans = spans_by_request[request_id]
        _validate_tree(request_id, spans)
        for span in spans:
            refs = span["refs"]
            if refs.get("pipeline_correlation_id", record["pipeline_correlation_id"]) \
                    != record["pipeline_correlation_id"] \
                    or refs.get("step_id", record["step_ids"][0]) not in record["step_ids"]:
                raise AttributionError("cross_request_stitch: " + request_id)
        actual_processes = {span["process"] for span in spans}
        missing = set(record["process_coverage"]) - actual_processes
        if missing:
            raise AttributionError("process_coverage_without_span: " + request_id
                                   + ":" + ",".join(sorted(missing)))
        documents.append({
            "request_id": request_id,
            "pipeline_correlation_id": record["pipeline_correlation_id"],
            "scenario": record.get("scenario", ""),
            "sample_index": record.get("sample_index", 0),
            "phase": record["phase"],
            "total_ns": record["total_ns"],
            "step_ids": list(record.get("step_ids", [])),
            "process_coverage": sorted(actual_processes),
            "server_evidence": {"gateway", "worker", "authority", "object_store"}.issubset(actual_processes),
            "outcome": record.get("outcome", ""),
            "spans": spans,
        })
    return documents


def _validate_tree(request_id, spans):
    by_id = {}
    for span in spans:
        if span["span_id"] in by_id:
            raise AttributionError("duplicate_span: " + span["span_id"])
        by_id[span["span_id"]] = span
    children = {span_id: [] for span_id in by_id}
    roots = []
    for span in spans:
        parent_id = span["parent_span_id"]
        if parent_id is None:
            roots.append(span)
            continue
        parent = by_id.get(parent_id)
        if parent is None:
            raise AttributionError("unknown_parent_span: " + span["span_id"])
        if (parent["process"], parent["clock_id"]) != (span["process"], span["clock_id"]):
            roots.append(span)
            continue
        children[parent_id].append(span)
    # Cycle detection: every node must reach a root.
    for span in spans:
        seen = set()
        current = span
        while current["parent_span_id"] is not None:
            if current["span_id"] in seen:
                raise AttributionError("span_cycle: " + request_id)
            seen.add(current["span_id"])
            current = by_id[current["parent_span_id"]]
    for span in spans:
        direct = sum(child["duration_ns"] for child in children[span["span_id"]])
        if direct > span["duration_ns"]:
            raise AttributionError("nested_duration_overflow: " + request_id
                                   + ":" + span["span_id"])
    return roots, children, by_id


def _percentile(values, fraction):
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[min(index, len(ordered) - 1)]


def _effective(span, children):
    return max(0, span["duration_ns"]
               - sum(child["duration_ns"] for child in children[span["span_id"]]))


def _root_holes(root, children):
    pending = [root]
    missing = 0
    while pending:
        span = pending.pop()
        if span["phase"].endswith(".root"):
            missing += _effective(span, children)
        pending.extend(children[span["span_id"]])
    return missing


def aggregate(documents):
    if not documents:
        raise AttributionError("no_requests")
    totals = []
    phase_totals = {}
    slowest = []
    violations = []
    unattributed_ok = 0
    process_checks = {}
    for document in documents:
        roots, children, by_id = _validate_tree(document["request_id"],
                                                document["spans"])
        client_roots = [root for root in roots if root["process"] == "client"]
        attributed = sum(root["duration_ns"] for root in client_roots)
        # root 容器自身不是阶段；未被子阶段解释的时间不能算已归因。
        client_holes = sum(_effective(span, children) for span in document["spans"]
                           if span["process"] == "client" and span["phase"].endswith(".root"))
        unattributed = document["total_ns"] - attributed + client_holes
        if attributed > document["total_ns"]:
            raise AttributionError("unattributed_negative: " + document["request_id"])
        process_roots = []
        for root in roots:
            if root["process"] == "client":
                continue
            # 每个服务端 root 独立，不把其他进程或同进程的另一个 root 补进来。
            missing_ns = _root_holes(root, children)
            process_roots.append({
                "process": root["process"], "clock_id": root["clock_id"],
                "root_span_id": root["span_id"], "total_ns": root["duration_ns"],
                "unattributed_ns": missing_ns,
                "ratio": missing_ns / root["duration_ns"] if root["duration_ns"] else 0,
            })
        totals.append(document["total_ns"])
        per_phase = {}
        for span in document["spans"]:
            effective = _effective(span, children)
            per_phase[span["phase"]] = per_phase.get(span["phase"], 0) + effective
        for phase, value in per_phase.items():
            phase_totals.setdefault(phase, []).append(value)
        ratio = unattributed / document["total_ns"] if document["total_ns"] else 0.0
        if ratio <= 0.10 or document["total_ns"] == 0:
            unattributed_ok += 1
        else:
            violations.append({
                "request_id": document["request_id"],
                "scenario": document["scenario"],
                "phase": document["phase"],
                "total_ns": document["total_ns"],
                "unattributed_ns": unattributed,
                "ratio": ratio,
            })
        process_checks.setdefault("client", []).append(ratio <= 0.10)
        request_checks = {}
        for check in process_roots:
            request_checks.setdefault(check["process"], []).append(check["ratio"] <= 0.10)
            if check["ratio"] > 0.10:
                violations.append({"request_id": document["request_id"], **check})
        for process, checks in request_checks.items():
            # 一个请求有多个 root 时，任何 root 不合格都不能被其他 root 稀释。
            process_checks.setdefault(process, []).append(all(checks))
        slowest.append({
            "process_roots": process_roots,
            "request_id": document["request_id"],
            "pipeline_correlation_id": document["pipeline_correlation_id"],
            "scenario": document["scenario"],
            "sample_index": document["sample_index"],
            "phase": document["phase"],
            "total_ns": document["total_ns"],
            "unattributed_ns": unattributed,
            "phase_breakdown_ns": per_phase,
        })
    slowest.sort(key=lambda item: (-item["total_ns"], item["request_id"]))
    slowest_count = max(1, math.ceil(len(documents) * 0.01))
    coverage = unattributed_ok / len(documents)
    process_counts = {}
    server_requests = 0
    required_server = {"gateway", "worker", "authority", "object_store"}
    for document in documents:
        for process in document["process_coverage"]:
            process_counts[process] = process_counts.get(process, 0) + 1
        if required_server.issubset(document["process_coverage"]):
            server_requests += 1
    return {
        "request_count": len(documents),
        "latency_ns": {
            "p50": _percentile(totals, 0.50),
            "p95": _percentile(totals, 0.95),
            "p99": _percentile(totals, 0.99),
            "max": max(totals),
        },
        "phase_percentiles_ns": {
            phase: {
                "p50": _percentile(values, 0.50),
                "p95": _percentile(values, 0.95),
                "p99": _percentile(values, 0.99),
                "max": max(values),
            }
            for phase, values in sorted(phase_totals.items())
        },
        "slowest": {
            "fraction": 0.01,
            "count": slowest_count,
            "requests": slowest[:slowest_count],
        },
        "unattributed": {
            "ratio_max": 0.10,
            "coverage_min": 0.95,
            "coverage": coverage,
            "threshold_met": all(sum(checks) / len(checks) >= 0.95
                                 for checks in process_checks.values()),
            "per_process_coverage": {process: sum(checks) / len(checks)
                                     for process, checks in process_checks.items()},
            "violations": violations,
        },
        "process_coverage": process_counts,
        "server_evidence_requests": server_requests,
        "server_evidence_coverage": server_requests / len(documents),
    }
