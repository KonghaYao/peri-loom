#!/usr/bin/env python3
"""Freeze safe result identities without rerunning any workload.

Every deployment-specific input is explicit: --container names the RustFS
service used only for the credential-bytes leak scan, --date and
--evidence-root locate the raw results, and --output-directory is the final
evidence root. Service environment values are never printed or indexed.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import time

from runner import ROOT, digest

NAMES = ["local-native-baseline", "s3-native-baseline", "local-http-baseline", "s3-http-baseline",
         "local-native-short-baseline", "s3-native-short-baseline", "local-native-short-repeat", "s3-native-short-repeat",
         "local-http-fixed-baseline", "s3-http-fixed-baseline", "local-http-fixed-repeat", "s3-http-fixed-repeat",
         "local-http-dirty-independent", "s3-http-dirty-independent"]
DATE_PATTERN = re.compile(r"^\d{8}$")
SECRET_NAMES = ("RUSTFS_ACCESS_KEY", "RUSTFS_SECRET_KEY")


class FinalizeError(RuntimeError):
    pass


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--container", required=True,
                        help="RustFS container inspected only for credential-byte leak scanning")
    parser.add_argument("--date", required=True, help="raw result date suffix, YYYYMMDD")
    parser.add_argument("--evidence-root", type=Path, required=True,
                        help="directory containing the raw peri-perf-<name>-<date>.json files")
    parser.add_argument("--output-directory", type=Path, required=True,
                        help="evidence output directory; must be outside scripts/")
    arguments = parser.parse_args(argv)
    if not DATE_PATTERN.match(arguments.date):
        parser.error("--date must be YYYYMMDD")
    return arguments


def inspect_container(container, runner=subprocess.check_output):
    try:
        return json.loads(runner(["docker", "inspect", container], text=True, timeout=10))[0]
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError, IndexError, TypeError):
        raise FinalizeError("container_inspect_failed: " + container) from None


def container_secret_bytes(container, runner=subprocess.check_output):
    service = inspect_container(container, runner)
    config = service.get("Config") if isinstance(service, dict) else None
    entries = config.get("Env") if isinstance(config, dict) else None
    private = dict(item.split("=", 1) for item in entries or [] if "=" in item)
    missing = [name for name in SECRET_NAMES if not private.get(name)]
    if missing:
        raise FinalizeError("rustfs_credentials_missing: " + ",".join(missing))
    return [private[name].encode() for name in SECRET_NAMES]


def result_path(evidence_root, name, date):
    return Path(evidence_root) / ("peri-perf-" + name + "-" + date + ".json")


def read_result(path, name):
    try:
        data = Path(path).read_bytes()
    except OSError:
        raise FinalizeError("result_unreadable: " + name) from None
    try:
        return data, json.loads(data)
    except json.JSONDecodeError:
        raise FinalizeError("result_json_invalid: " + name) from None


def run(arguments):
    os.umask(0o077)
    output_directory = arguments.output_directory
    if output_directory.resolve().is_relative_to(ROOT / "scripts"):
        raise FinalizeError("output_directory_inside_scripts")
    output_directory.mkdir(parents=True, exist_ok=True)
    secret_bytes = container_secret_bytes(arguments.container)
    entries = []
    for name in NAMES:
        path = result_path(arguments.evidence_root, name, arguments.date)
        data, result = read_result(path, name)
        if any(secret in data for secret in secret_bytes):
            raise FinalizeError("secret_in_result: " + name)
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
            value = inspect_container(result["container_name"])
            expected_owner = result["container_name"].removeprefix("peri-verify-")
            if value["Config"]["Labels"].get("peri.verify-image.owner") != expected_owner or value["State"]["Running"] or value["State"]["Pid"] != 0:
                raise FinalizeError("owned_container_stop_not_proven: " + name)
            entry.update(stage=result["stage"], image_id=result["image"]["Id"],
                         stopped_container=value["Id"], owned_container_stopped_verified=True,
                         volume=result["volume_name"], fixture_directory=result.get("runtime"),
                         historical_inputs_verified=result.get("historical_build_after", {}).get("status"))
        entries.append(entry)
    destination = output_directory / "EVIDENCE-INDEX.json"
    index = {"utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "entries": entries,
             "active_workload_wall_seconds_sum": sum(entry["wall_seconds"] for entry in entries),
             "secret_scan": "credential bytes from the explicitly named service absent in all "
                            "indexed results; private environment intentionally excluded",
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


def main(argv=None):
    arguments = parse_arguments(argv)
    try:
        run(arguments)
    except (FinalizeError, OSError, KeyError, TypeError, ValueError) as failure:
        print(json.dumps({"status": "NOT_RUN", "error": str(failure)}))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
