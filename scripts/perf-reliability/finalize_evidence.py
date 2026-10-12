#!/usr/bin/env python3
"""Freeze safe result identities without rerunning any workload."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time

from runner import ROOT, digest

NAMES = ["local-native-baseline", "s3-native-baseline", "local-http-baseline", "s3-http-baseline",
         "local-native-short-baseline", "s3-native-short-baseline", "local-native-short-repeat", "s3-native-short-repeat",
         "local-http-fixed-baseline", "s3-http-fixed-baseline", "local-http-fixed-repeat", "s3-http-fixed-repeat",
         "local-http-dirty-independent", "s3-http-dirty-independent"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-directory", type=Path)
    arguments = parser.parse_args()
    os.umask(0o077)
    output_directory = arguments.output_directory or Path(tempfile.mkdtemp(prefix="peri-perf-evidence-final-"))
    if output_directory.resolve().is_relative_to(ROOT / "scripts"):
        parser.error("dynamic evidence must be outside the scripts directory")
    output_directory.mkdir(parents=True, exist_ok=True)
    service = json.loads(subprocess.check_output(["docker", "inspect", "1Panel-rustfs-TcP9"], text=True, timeout=10))[0]
    private = dict(item.split("=", 1) for item in service["Config"]["Env"] if "=" in item)
    secret_bytes = [private[name].encode() for name in ["RUSTFS_ACCESS_KEY", "RUSTFS_SECRET_KEY"]]
    entries = []
    for name in NAMES:
        path = Path("/private/tmp/peri-perf-" + name + "-20261009.json")
        data = path.read_bytes()
        if any(secret in data for secret in secret_bytes):
            raise RuntimeError("secret_in_result: " + name)
        result = json.loads(data)
        entry = {"name": name, "path": str(path), "sha256": digest(path), "status": result["status"],
                 "wall_seconds": result["wall_seconds"], "git_head": result.get("git_head"),
                 "actual_tool_sha256": {key: value for key, value in result["source_inventory"].items()
                                        if key.startswith("scripts/perf-reliability/")},
                 "source_unchanged": result["source_unchanged"]}
        if "benchmarks" in result:
            entry["native_binary_sha256"] = result["binary_sha256"]
            entry["operations_per_lane"] = result["load"]["operations"]
            entry["mode_outcomes"] = [{key: mode.get(key) for key in ["mode", "attempts", "ACK", "Unknown", "errors", "completed_budget"]} for mode in result["benchmarks"]]
            entry["group_cleanup_recorded"] = all(resource.get("group_joined") for mode in result["benchmarks"] for resource in mode.get("resources", []))
        else:
            value = json.loads(subprocess.check_output(["docker", "inspect", result["container_name"]], text=True, timeout=10))[0]
            expected_owner = result["container_name"].removeprefix("peri-verify-")
            if value["Config"]["Labels"].get("peri.verify-image.owner") != expected_owner or value["State"]["Running"] or value["State"]["Pid"] != 0:
                raise RuntimeError("owned_container_stop_not_proven: " + name)
            entry.update(stage=result["stage"], image_id=result["image"]["Id"],
                         stopped_container=value["Id"], owned_container_stopped_verified=True,
                         volume=result["volume_name"], fixture_directory=result.get("runtime"),
                         historical_inputs_verified=result.get("historical_build_after", {}).get("status"))
        entries.append(entry)
    destination = output_directory / "EVIDENCE-INDEX.json"
    index = {"utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "entries": entries,
             "active_workload_wall_seconds_sum": sum(entry["wall_seconds"] for entry in entries),
             "secret_scan": "RustFS service credential bytes absent in all indexed results; privateenv intentionally excluded",
             "limits": "active sum excludes build, tools, coordination and review; not total elapsed or a product PASS"}
    with destination.open("x") as output:
        json.dump(index, output, indent=2)
        output.write("\n")
    files = sorted((ROOT / "scripts/perf-reliability").glob("*.py"))
    files.append(destination)
    frozen = {str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path): digest(path) for path in files}
    with (output_directory / "FINAL-FREEZE.json").open("x") as output:
        json.dump({"utc": index["utc"], "sha256": frozen, "workloads_ended": True}, output, indent=2)
        output.write("\n")
    print(json.dumps({"indexed_results": len(entries), "active_seconds_sum": index["active_workload_wall_seconds_sum"],
                      "evidence_index": str(destination), "final_freeze": str(output_directory / "FINAL-FREEZE.json")}))


if __name__ == "__main__":
    main()
