#!/usr/bin/env python3
"""Run previously built identical release harnesses serially; never builds or stops services."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def workload(sample):
    """Compare every non-timing field, including all child-process content oracles."""
    result = {key: value for key, value in sample.items()
              if key not in {"run", "source", "fixture_ns", "total_request_ns", "phases"}}
    result["phases"] = [{key: value for key, value in phase.items() if key != "wall_ns"}
                        for phase in sample["phases"]]
    return result


def summarize(samples):
    pairs = []
    for round_id in sorted({sample["round"] for sample in samples}):
        pair = {sample["arm"]: sample["result"] for sample in samples if sample["round"] == round_id}
        if len(pair) != 2:
            continue
        pairs.append({"round": round_id,
                      "total_speedup": pair["baseline"]["total_request_ns"] / pair["candidate"]["total_request_ns"],
                      "phases": {before["name"]: before["wall_ns"] / after["wall_ns"]
                                 for before, after in zip(pair["baseline"]["phases"], pair["candidate"]["phases"])}})
    if not pairs:
        return {"status": "INCOMPLETE"}
    median_speedup = statistics.median(pair["total_speedup"] for pair in pairs)
    phase_medians = {name: statistics.median(pair["phases"][name] for pair in pairs)
                     for name in pairs[0]["phases"]}
    regressions = {name: speedup for name, speedup in phase_medians.items() if speedup < 1 / 1.10}
    passed = (len(pairs) >= 2 and median_speedup >= 1.5
              and all(pair["total_speedup"] >= 1.5 for pair in pairs) and not regressions)
    return {"status": "PASS" if passed else "NOT_YET_1_5X", "paired_rounds": pairs,
            "median_total_speedup": median_speedup, "median_phase_speedup": phase_medians,
            "phase_regressions_over_10_percent": regressions}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-bin", required=True, type=Path)
    parser.add_argument("--candidate-bin", required=True, type=Path)
    parser.add_argument("--baseline-source", required=True)
    parser.add_argument("--candidate-source", required=True)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--identity", type=Path, default=Path(__file__).with_name("identity.json"))
    args = parser.parse_args()
    if args.rounds < 2:
        parser.error("at least two paired rounds are required")
    args.output.mkdir(parents=True, exist_ok=False)
    scratch = args.output.resolve() / "scratch"
    scratch.mkdir()
    binaries = {"baseline": args.baseline_bin.resolve(), "candidate": args.candidate_bin.resolve()}
    sources = {"baseline": args.baseline_source, "candidate": args.candidate_source}
    report = {"schema": 1, "started_unix": time.time(), "host": platform.platform(),
              "processor": platform.processor(), "load_before": os.getloadavg(),
              "identity": json.loads(args.identity.read_text()),
              "harness_sha256": sha256(Path(__file__).parents[2] / "prototypes/v1/tests/local_performance.rs"),
              "binaries": {arm: {"path": str(path), "sha256": sha256(path), "source": sources[arm]}
                           for arm, path in binaries.items()}, "samples": []}
    if report["binaries"]["baseline"]["sha256"] == report["binaries"]["candidate"]["sha256"]:
        raise RuntimeError("baseline and candidate binaries are identical; check stale build artifacts")
    if report["harness_sha256"] != report["identity"]["harness_sha256"]:
        raise RuntimeError("current harness differs from the frozen identity hash")
    expected = None
    for round_id in range(1, args.rounds + 1):
        order = ["baseline", "candidate"] if round_id % 2 else ["candidate", "baseline"]
        for arm in order:
            # Cargo is absent from timing; executable startup is excluded by the harness timer.
            run_id = f"round-{round_id:02}-{arm}"
            environment = dict(os.environ, PERI_LOCAL_PERF_RUN_ID=run_id,
                               PERI_LOCAL_PERF_SOURCE_ID=sources[arm], TMPDIR=str(scratch))
            environment.pop("PERI_LOCAL_PERF_COLD_ROOT", None)
            environment.pop("PERI_NATIVE_GATE_TRACE", None)
            command = [str(binaries[arm]), "--ignored", "--exact", "local_read_write_workload",
                       "--nocapture", "--test-threads=1"]
            print(f"{run_id}: start", flush=True)
            completed = subprocess.run(command, env=environment, capture_output=True, text=True)
            (args.output / f"{run_id}.stdout.log").write_text(completed.stdout)
            (args.output / f"{run_id}.stderr.log").write_text(completed.stderr)
            if completed.returncode:
                raise RuntimeError(f"{run_id} failed ({completed.returncode}); inspect preserved logs")
            records = [json.loads(line.split("LOCAL_PERF ", 1)[1])
                       for line in completed.stderr.splitlines() if "LOCAL_PERF {" in line]
            if len(records) != 1:
                raise RuntimeError(f"{run_id}: expected exactly one complete workload record")
            result = records[0]
            durations = [phase["wall_ns"] for phase in result["phases"]]
            if not durations or any(type(duration) is not int or duration <= 0 for duration in durations):
                raise RuntimeError(f"{run_id}: expected positive integer phase timings")
            if sum(durations) != result["total_request_ns"]:
                raise RuntimeError(f"{run_id}: total_request_ns differs from the sum of phase timings")
            if result["run"] != run_id or result["source"] != sources[arm]:
                raise RuntimeError(f"{run_id}: executable identity mismatch")
            if expected is None:
                expected = workload(result)
            if workload(result) != expected:
                raise RuntimeError(f"{run_id}: workload/content oracle differs from first arm")
            report["samples"].append({"round": round_id, "arm": arm, "result": result,
                                      "load_after": os.getloadavg()})
            report["summary"] = summarize(report["samples"])
            (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
            print(f"{run_id}: {result['total_request_ns'] / 1e9:.3f}s", flush=True)
    report["finished_unix"] = time.time()
    report["load_after"] = os.getloadavg()
    (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["summary"], indent=2))
    return 0 if report["summary"]["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
