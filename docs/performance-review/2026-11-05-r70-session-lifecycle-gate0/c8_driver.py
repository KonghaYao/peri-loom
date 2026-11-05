#!/usr/bin/env python3
"""Run the Issue 28 F1 scenario from N concurrent harness clients.

This is a thin driver over the Issue 28 Runner so the c8 window measures the
same request shape and the same timing code as the stock c1 window. Each
worker owns one HranaClient and one Runner; the fixture is discovered once
before the metrics window opens, so the metric delta covers only benchmark
requests.

Writes metadata.json, raw.jsonl, summary.json and the metrics snapshots into
--out. Never prints the connection file or an Authorization value.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import json
import pathlib
import statistics
import sys


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection-file", required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--samples", type=int, default=5)
    arguments = parser.parse_args()

    sys.path.insert(0, str(pathlib.Path(arguments.repo) / "scripts/perf-reliability"))
    from issue28.contract import build_scenarios
    from issue28.fixture import discover
    from issue28.hrana import Target
    from issue28.runner import Runner

    def full_metrics() -> dict:
        # MetricsApi.snapshot() returns only the admission subset; this window
        # needs the action buckets, so fetch the whole result document.
        import urllib.request
        request = urllib.request.Request(
            target.endpoint + "/v1/metrics",
            headers={"Authorization": "Bearer " + target.global_api_key},
        )
        with urllib.request.urlopen(request, timeout=15) as response:
            document = json.load(response)
        result = document.get("result")
        if not isinstance(result, dict):
            raise SystemExit("metrics response has no result")
        return result

    output = pathlib.Path(arguments.out)
    output.mkdir(parents=True, exist_ok=True)

    target = Target.from_file(arguments.connection_file)
    # Discovery runs before the metrics window so its open/query/close actions
    # do not enter the delta.
    fixture = discover(target)
    before = full_metrics()

    scenario = [item for item in build_scenarios(fixture)
                if item.key == "F1.select-1"][0]
    protocol = [item for item in scenario.protocols if item.name == "json-pipeline"][0]
    scenario = dataclasses.replace(scenario, protocols=(protocol,))

    def leg(worker: int):
        runner = Runner(
            target=target,
            fixture=fixture,
            scenarios=[scenario],
            rounds=arguments.rounds,
            warmup=arguments.warmup,
            samples=arguments.samples,
        )
        return worker, runner.run()

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=arguments.concurrency) as pool:
        futures = [pool.submit(leg, index) for index in range(arguments.concurrency)]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    results.sort(key=lambda item: item[0])

    after = full_metrics()

    timed = []
    per_worker = []
    warnings = []
    fatal = ""
    with open(output / "raw.jsonl", "w", encoding="utf-8") as handle:
        for worker, result in results:
            durations = [item.duration_ms for item in result.measurements
                         if item.phase == "timed"]
            per_worker.append({
                "worker": worker,
                "timed_samples": len(durations),
                "median_ms": statistics.median(durations) if durations else 0.0,
                "mean_ms": statistics.fmean(durations) if durations else 0.0,
            })
            timed.extend(durations)
            for measurement in result.measurements:
                record = measurement.as_record()
                record["worker"] = worker
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
            warnings.extend(result.warnings)
            if result.fatal:
                fatal = result.fatal

    timed.sort()

    def percentile(values, fraction):
        if not values:
            return 0.0
        index = min(int(fraction * (len(values) - 1)), len(values) - 1)
        return values[index]

    outcomes = {}
    with open(output / "raw.jsonl", encoding="utf-8") as handle:
        for line in handle:
            record = json.loads(line)
            if record.get("phase") == "timed":
                outcomes[record.get("outcome", "")] = outcomes.get(record.get("outcome", ""), 0) + 1

    document = {
        "metadata": {
            "concurrency": arguments.concurrency,
            "rounds": arguments.rounds,
            "warmup": arguments.warmup,
            "samples": arguments.samples,
            "timed_samples": len(timed),
            "scenario": scenario.key,
            "protocol": protocol.name,
            "timed_outcomes": outcomes,
        },
        "fixture": fixture.as_record(),
        "rows": [{
            "scenario": scenario.key,
            "protocol": protocol.name,
            "concurrency": arguments.concurrency,
            "timed_samples": len(timed),
            "median_ms": statistics.median(timed) if timed else 0.0,
            "mean_ms": statistics.fmean(timed) if timed else 0.0,
            "p95_ms": percentile(timed, 0.95),
            "min_ms": timed[0] if timed else 0.0,
            "max_ms": timed[-1] if timed else 0.0,
            "per_worker": per_worker,
        }],
        "warnings": warnings,
        "fatal": fatal,
    }
    (output / "summary.json").write_text(
        json.dumps(document, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "metrics-before.json").write_text(
        json.dumps(before, indent=2, sort_keys=True), encoding="utf-8")
    (output / "metrics-after.json").write_text(
        json.dumps(after, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(document["rows"][0], ensure_ascii=False))
    return 0 if not fatal else 2


if __name__ == "__main__":
    raise SystemExit(main())
