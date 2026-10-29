#!/usr/bin/env python3
"""Validate every diagnostic record before reporting exploratory A/B/S latency.

Inputs are explicit run directories (or leg directories containing run.json).
Never scans an ambient workspace, drops failed samples or changes acceptance.
"""
import argparse
import hashlib
import json
import math
from collections import defaultdict
from pathlib import Path


SCHEMA = "sqld-read-diagnostic/1"
FILES = {"run.json", "operations.jsonl", "requests.jsonl", "summary.json"}
ORDER = ["A", "S", "B", "B", "S", "A"]


class InvalidEvidence(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise InvalidEvidence(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def sha_valid(value):
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def same_number(a, b):
    return number(a) and number(b) and math.isclose(a, b, rel_tol=1e-10, abs_tol=1e-8)


def ordered(*values):
    return all(number(x) for x in values) and all(a <= b for a, b in zip(values, values[1:]))


def quantiles(values):
    values = sorted(values)
    def q(p):
        return values[math.ceil(p * len(values)) - 1] if values else None
    return {"count": len(values), "mean_ms": sum(values) / len(values) if values else None,
            "p50_ms": q(.5), "p95_ms": q(.95), "p99_ms": q(.99),
            "max_ms": values[-1] if values else None}


def distribution(values):
    result = quantiles(values)
    count = sum(x > 100 for x in values)
    return {**result, "over_100ms_count": count,
            "over_100ms_fraction": count / len(values) if values else None}


def validate_summary_stats(actual, expected, label):
    require(isinstance(actual, dict), label + ": missing stats")
    for key, value in expected.items():
        require(actual.get(key) == value if value is None or key == "count"
                else same_number(actual.get(key), value), label + ": inconsistent " + key)


def json_lines(data, label):
    lines = data.decode().splitlines()
    require(lines and all(line.strip() for line in lines), label + ": empty/interior blank records")
    return [json.loads(line) for line in lines]


def validate_run(directory, manifest, manifest_sha, samples, warmup):
    """Raises on any bad raw record, including warmups; returns validated records."""
    sums = json.loads((directory / "SHA256SUMS.json").read_bytes())
    require(set(sums) == FILES, "SHA inventory mismatch")
    data = {name: (directory / name).read_bytes() for name in FILES}
    for name, content in data.items():
        require(sha_valid(sums[name]) and sha(content) == sums[name], name + ": SHA mismatch")
    run = json.loads(data["run.json"])
    summary = json.loads(data["summary.json"])
    ops = json_lines(data["operations.jsonl"], "operations")
    requests = json_lines(data["requests.jsonl"], "requests")
    published = summary.get("published_raw")
    require(isinstance(published, dict) and set(published) == {"operations.jsonl", "requests.jsonl"}, "missing original publication ledger")
    for name, records in (("operations.jsonl", ops), ("requests.jsonl", requests)):
        ledger = published[name]
        require(isinstance(ledger, dict) and ledger.get("records") == len(records)
                and ledger.get("bytes") == len(data[name]) and ledger.get("sha256") == sha(data[name]),
                name + ": original publication ledger mismatch")
    require(run.get("schema") == summary.get("schema") == SCHEMA, "schema mismatch")
    require(run.get("run_id") and run["run_id"] == summary.get("run_id"), "run ID mismatch")
    require(run.get("formal_pass") is False and summary.get("formal_pass") is False, "formal gate must remain false")
    require(run.get("recovery_proven") is False and summary.get("recovery_proven") is False, "recovery must remain false")
    require(summary.get("status") == "RUN_COMPLETED", "run stopped or incomplete")
    require(run.get("arm") in {"A", "B", "S"}, "arm must be A, B or S")
    require(run.get("engine") == ("sqld" if run["arm"] == "S" else "peri"), "arm/engine mismatch")
    require(run.get("sdk") == "@tursodatabase/serverless@1.4.1", "SDK mismatch")
    identity = run.get("server_identity")
    require(isinstance(identity, dict) and identity, "missing server identity")
    require(identity.get("arm") == run["arm"] and identity.get("source_commit") == run.get("source_sha"), "source identity mismatch")
    require(sha_valid(identity.get("binary_sha256")), "missing binary checksum")
    if run["arm"] != "S":
        require(sha_valid(identity.get("shared_native_sha256")), "missing shared native checksum")
    require(sha_valid(run.get("sdk_lock_sha256")), "missing SDK lock digest")
    require(run.get("manifest_sha256") == manifest_sha, "manifest SHA mismatch")
    require(run.get("dataset_sha256") == manifest["dataset_sha256"], "dataset digest mismatch")
    require(run.get("dataset_origin") == summary.get("dataset_origin") == manifest["dataset_origin"], "dataset origin mismatch")
    require(run.get("samples_requested") == samples and run.get("warmup_requested") == warmup, "declared N mismatch")
    require(number(run.get("started_at_unix_ms")), "missing wall-clock start")
    require(ordered(run["started_at_unix_ms"], summary.get("completed_at_unix_ms")), "wall-clock run interval")
    scenarios = {item["name"]: item for item in manifest["scenarios"]}
    scenario = scenarios.get(run.get("scenario"))
    require(scenario is not None and summary.get("scenario") == scenario["name"], "unknown scenario")
    require(run.get("sql_sha256") == sha(scenario["sql"].encode()), "SQL digest mismatch")
    require(run.get("expected_rows") == scenario["expected_rows"], "expected row count mismatch")
    require(len(ops) == samples + warmup, "operation N mismatch")
    require(len(requests) == 2 * len(ops), "request count mismatch")
    req_by_id = {item.get("request_id"): item for item in requests}
    require(None not in req_by_id and len(req_by_id) == len(requests), "duplicate/missing request ID")
    require(len({item.get("operation_id") for item in ops}) == len(ops), "duplicate operation ID")
    seen = set()
    previous_completed = None
    for ordinal, op in enumerate(ops):
        where = "operation " + str(ordinal)
        phase = "warmup" if ordinal < warmup else "timed"
        index = ordinal if phase == "warmup" else ordinal - warmup
        require(op.get("run_id") == run["run_id"] and op.get("scenario") == scenario["name"], where + ": identity mismatch")
        require(op.get("phase") == phase and op.get("sample_index") == index, where + ": phase/index mismatch")
        require(op.get("outcome") == "success", where + ": failed/Unknown sample")
        require(op.get("retry_count") == op.get("blocked_retry_count") == 0, where + ": retry")
        require(op.get("evidence_failures") == [], where + ": evidence failures")
        require(op.get("close_acknowledged") is True, where + ": no close ACK")
        require(op.get("result_rows") == scenario["expected_rows"] and
                op.get("result_sha256") == scenario["expected_result_sha256"], where + ": full result mismatch")
        actions = op.get("actions", [])
        require([item.get("action") for item in actions] == ["read", "close"], where + ": action shape mismatch")
        read, close = actions
        require(ordered(op.get("issued_ms"), op.get("connect_returned_ms"), read.get("started_ms"),
                        read.get("sdk_resolved_ms"), close.get("started_ms"), close.get("sdk_resolved_ms"),
                        op.get("sdk_completed_ms"), op.get("evidence_settled_ms"), op.get("validation_completed_ms")),
                where + ": clock order")
        require(same_number(op["sdk_completed_ms"], close["sdk_resolved_ms"]), where + ": business clock end")
        require(same_number(op.get("duration_ms"), op["sdk_completed_ms"] - op["issued_ms"]), where + ": business duration")
        require(same_number(op.get("observer_wait_ms"), op["evidence_settled_ms"] - op["sdk_completed_ms"]), where + ": observer duration")
        require(previous_completed is None or previous_completed <= op["issued_ms"], where + ": overlapping operations")
        previous_completed = op["validation_completed_ms"]
        ids = []
        baton = None
        for action in actions:
            require(action.get("sdk_error") is False and action.get("blocked_retries") == 0, where + ": SDK error/retry")
            require(same_number(action.get("sdk_duration_ms"), action["sdk_resolved_ms"] - action["started_ms"]), where + ": action duration")
            require(len(action.get("requests", [])) == 1, where + ": action request count")
            req_id = action["requests"][0]
            require(req_id in req_by_id and req_id not in seen, where + ": missing/reused request")
            ids.append(req_id)
            seen.add(req_id)
            request = req_by_id[req_id]
            require(request.get("run_id") == run["run_id"] and request.get("operation_id") == op["operation_id"], where + ": request identity")
            require(request.get("phase") == phase and request.get("sample_index") == index, where + ": request phase/index")
            require(request.get("action") == action["action"], where + ": request action")
            require(request.get("outcome") == "success" and not request.get("fetch_error") and not request.get("observation_error"), where + ": HTTP unconfirmed")
            status = request.get("status")
            require(isinstance(status, int) and 200 <= status < 300, where + ": HTTP status")
            require(sha_valid(request.get("request_body_sha256")), where + ": request body digest")
            require(ordered(action["started_ms"], request.get("started_ms"), request.get("headers_received_ms"), action["sdk_resolved_ms"]), where + ": request/header clock")
            require(ordered(request["headers_received_ms"], request.get("body_observed_ms"), op["evidence_settled_ms"]), where + ": body/proof clock")
            require(number(request.get("response_bytes")) and request["response_bytes"] > 0, where + ": response bytes")
            require(request.get("baton_in_sha256") == baton, where + ": baton chain")
            is_close = action["action"] == "close"
            require(request.get("close_requested") is is_close and request.get("close_acknowledged") is is_close, where + ": close proof")
            require(request.get("path", "").endswith("/v3/pipeline" if is_close else "/v3/cursor"), where + ": endpoint shape")
            if is_close:
                require(request.get("baton_out_sha256") is None, where + ": close retained baton")
            else:
                require(request.get("primary_step_complete") is True, where + ": incomplete cursor step")
                require(sha_valid(request.get("baton_out_sha256")), where + ": missing baton")
            baton = request.get("baton_out_sha256")
        require(op.get("request_ids") == ids, where + ": operation/request correlation")
    require(seen == set(req_by_id), "orphan requests")
    timed = ops[warmup:]
    expected_stats = quantiles([item["duration_ms"] for item in timed])
    validate_summary_stats(summary.get("all_timed"), expected_stats, "all_timed")
    validate_summary_stats(summary.get("successful_timed"), expected_stats, "successful_timed")
    for key in ("failures_all_phases", "unknown_all_phases", "blocked_retries_all_phases"):
        require(summary.get(key) == 0, "nonzero " + key)
    require(summary.get("all_operations") == samples + warmup, "summary operation count")
    require(summary.get("timed_requests") == 2 * samples and summary.get("timed_close_acknowledgements") == samples, "summary request/ACK count")
    values = {"business": [item["duration_ms"] for item in timed],
              "sdk_read": [item["actions"][0]["sdk_duration_ms"] for item in timed],
              "sdk_close": [item["actions"][1]["sdk_duration_ms"] for item in timed]}
    return {"path": str(directory), "run": run, "values": values, "file_sha256": sums,
            "completed_at_unix_ms": summary["completed_at_unix_ms"],
            "timed_requests": 2 * samples, "close_acknowledgements": samples,
            "response_bytes_timed": sum(item["response_bytes"] for item in requests if item["phase"] == "timed")}


def compare_ratios(numerator, denominator):
    return {stage: {key: numerator[stage][key] / denominator[stage][key]
                   if denominator[stage][key] else None
                   for key in ("p50_ms", "p95_ms", "p99_ms", "max_ms")}
            for stage in ("business", "sdk_read", "sdk_close")}


def analyze(args):
    manifest_bytes = Path(args.manifest).read_bytes()
    manifest = json.loads(manifest_bytes)
    require(manifest.get("schema") == SCHEMA, "manifest schema")
    require(sha_valid(manifest.get("dataset_sha256")), "dataset SHA missing")
    directories = []
    excluded_setup_runs = []
    for source in args.runs:
        path = Path(source).resolve()
        require(path.is_dir(), "input is not a directory: " + str(path))
        found = [path] if (path / "run.json").is_file() else sorted(item.parent for item in path.rglob("run.json"))
        require(found, "no run in explicit directory: " + str(path))
        for directory in found:
            header = json.loads((directory / "run.json").read_bytes())
            if directory.name == "setup" and header.get("scenario") == "isolated_fixture_setup":
                excluded_setup_runs.append(str(directory))
            else:
                directories.append(directory)
    require(len(set(directories)) == len(directories), "duplicate input run")
    require(len(set(args.scenarios)) == len(args.scenarios), "duplicate scenario selection")
    require(len(directories) == 6 * len(args.scenarios), "expected exactly six legs per selected scenario")
    validated = []
    for path in directories:
        try:
            validated.append(validate_run(path, manifest, sha(manifest_bytes), args.samples, args.warmup))
        except (InvalidEvidence, KeyError, TypeError, ValueError, OSError) as error:
            raise InvalidEvidence(str(path) + ": " + str(error)) from error
    require(len({item["run"]["run_id"] for item in validated}) == len(validated), "reused run ID")
    serial = sorted(validated, key=lambda item: item["run"]["started_at_unix_ms"])
    require(all(previous["completed_at_unix_ms"] <= following["run"]["started_at_unix_ms"]
                for previous, following in zip(serial, serial[1:])), "overlapping run intervals; expected serial clients")
    require({item["run"]["scenario"] for item in validated} == set(args.scenarios), "unexpected/missing scenario")
    for key in ("sdk_lock_sha256", "manifest_sha256", "dataset_sha256", "node", "arch", "platform", "pace_ms", "timeout_ms"):
        require(len({json.dumps(item["run"].get(key), sort_keys=True) for item in validated}) == 1, "cross-arm mismatch: " + key)
    for arm in ("A", "B", "S"):
        members = [item for item in validated if item["run"]["arm"] == arm]
        for key in ("source_sha", "server_identity", "auth", "tls_verified"):
            require(len({json.dumps(item["run"].get(key), sort_keys=True) for item in members}) == 1,
                    "arm " + arm + " changed " + key + " across scenarios/legs")
    require(len({item["run"]["server_identity"]["shared_native_sha256"]
                 for item in validated if item["run"]["arm"] in {"A", "B"}}) == 1,
            "A/B shared native binary differs; Go delta cannot be isolated")
    legs = []
    grouped = defaultdict(list)
    for item in validated:
        run = item["run"]
        grouped[run["scenario"]].append(item)
        legs.append({"path": item["path"], "run_id": run["run_id"], "arm": run["arm"],
                     "scenario": run["scenario"], "started_at_unix_ms": run["started_at_unix_ms"],
                     "completed_at_unix_ms": item["completed_at_unix_ms"],
                     "source_sha": run.get("source_sha"), "server_identity": run.get("server_identity"),
                     "auth": run.get("auth"), "tls_verified": run.get("tls_verified"),
                     "latency": {stage: distribution(values) for stage, values in item["values"].items()},
                     "timed_requests": item["timed_requests"], "confirmed_closes": item["close_acknowledgements"],
                     "response_bytes_timed": item["response_bytes_timed"], "file_sha256": item["file_sha256"]})
    scenarios = {}
    for scenario, items in grouped.items():
        ordered_items = sorted(items, key=lambda item: item["run"]["started_at_unix_ms"])
        actual_order = [item["run"]["arm"] for item in ordered_items]
        require(actual_order == ORDER, scenario + ": expected serial A/S/B/B/S/A order, got " + str(actual_order))
        arms = {}
        for arm in ("A", "B", "S"):
            members = [item for item in items if item["run"]["arm"] == arm]
            require(len(members) == 2, scenario + ": arm leg count")
            require(len({item["run"].get("source_sha") for item in members}) == 1, scenario + ": arm source changed")
            arms[arm] = {stage: distribution([value for member in members for value in member["values"][stage]])
                         for stage in ("business", "sdk_read", "sdk_close")}
        scenarios[scenario] = {"order": actual_order, "pooled": arms,
                               "B_over_A": compare_ratios(arms["B"], arms["A"]),
                               "B_over_S": compare_ratios(arms["B"], arms["S"])}
    return {"schema": "sqld-read-comparison/1", "evidence_valid": True,
            "formal_pass": False, "recovery_proven": False, "capacity_proven": False,
            "dataset_origin": manifest["dataset_origin"], "dataset_ref": manifest.get("dataset_ref"),
            "dataset_sha256": manifest["dataset_sha256"], "manifest_sha256": sha(manifest_bytes),
            "samples_per_leg": args.samples, "warmup_per_leg": args.warmup, "legs_per_arm": 2,
            "quantile_method": "nearest rank; pooled raw observations, never averaged quantiles",
            "clock": "logical connect through all() full materialization and SDK close resolution",
            "limitations": ["Synthetic proxy results do not reproduce issue 15's six real business queries.",
                            f"At N={args.samples} each leg p99 is rank {math.ceil(.99 * args.samples)}; "
                            f"only {args.samples - math.ceil(.99 * args.samples)} observations are above that rank.",
                            "Per-stage quantiles are separate; never add read p99 and close p99.",
                            "Observer clone/tee collection overlaps SDK work; later proof/digest/writes are excluded.",
                            "Two legs and sequential dependent observations do not justify a formal confidence interval.",
                            "Authentication/TLS configuration is disclosed per leg; service-contract differences remain.",
                            "Concurrency 1 and validation gaps do not establish sustainable throughput."],
            "bootstrap_performed": False, "excluded_setup_runs": excluded_setup_runs,
            "setup_exclusion_reason": "Explicit isolated_fixture_setup runs are writes, not measured read samples; no business sample is excluded.",
            "legs": legs, "scenarios": scenarios}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--runs", nargs="+", required=True)
    parser.add_argument("--scenarios", nargs="+", default=["proxy_empty", "proxy_one", "proxy_twenty", "proxy_narrow"])
    parser.add_argument("--samples", type=int, default=200)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = Path(args.out)
    require(not out.exists(), "refusing to overwrite an analysis")
    require(args.samples > 0 and args.warmup >= 0, "invalid sample counts")
    try:
        report = analyze(args)
    except (InvalidEvidence, KeyError, TypeError, ValueError, OSError) as error:
        report = {"schema": "sqld-read-comparison/1", "evidence_valid": False,
                  "formal_pass": False, "recovery_proven": False, "capacity_proven": False,
                  "error": str(error), "statistics_withheld": True}
    with out.open("x") as target:
        json.dump(report, target, ensure_ascii=False, indent=2, allow_nan=False)
        target.write("\n")
    print(json.dumps({"output": str(out), "evidence_valid": report["evidence_valid"], "formal_pass": False}))
    return 0 if report["evidence_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
