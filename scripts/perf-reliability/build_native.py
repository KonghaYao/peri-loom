#!/usr/bin/env python3
"""Offline cached build selection with frozen source/artifact evidence."""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time

from runner import ROOT, digest, process_group_members, source_inventory


def main():
    signal.signal(signal.SIGTERM, lambda signum, frame: (_ for _ in ()).throw(KeyboardInterrupt()))
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-seconds", type=int, default=120)
    args = parser.parse_args()
    if not 1 <= args.max_seconds <= 180:
        parser.error("build budget must be 1..180 seconds")
    os.umask(0o077)
    directory = Path(tempfile.mkdtemp(prefix="peri-perf-build-"))
    environment = {name: os.environ[name] for name in ["PATH", "HOME", "TMPDIR", "CARGO_HOME", "RUSTUP_HOME"] if name in os.environ}
    before = source_inventory()
    command = ["cargo", "build", "--manifest-path", str(ROOT / "prototypes/v1/Cargo.toml"),
               "--offline", "--locked", "--jobs", "2", "--bin", "peri-loom-v1", "--message-format=json"]
    report = {"source_before": before, "command": command, "build_directory": str(directory),
              "build_kind": "offline cached debug build; not a clean build",
              "registry_binding": "Cargo.lock checksums; arbitrary cache tampering not independently excluded"}
    started = time.monotonic()
    try:
        report["cargo_version"] = subprocess.check_output(["cargo", "--version"], env=environment, text=True, timeout=5).strip()
        report["rustc_version"] = subprocess.check_output(["rustc", "-Vv"], env=environment, text=True, timeout=5).strip()
        with (directory / "build.stdout").open("x") as stdout, (directory / "build.stderr").open("x") as stderr:
            child = subprocess.Popen(command, cwd=ROOT, env=environment, stdout=stdout, stderr=stderr, start_new_session=True)
            try:
                returncode = child.wait(timeout=args.max_seconds)
            finally:
                if child.poll() is None:
                    os.killpg(child.pid, signal.SIGTERM)
                    try:
                        child.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        os.killpg(child.pid, signal.SIGKILL)
                        child.wait()
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                members = process_group_members(child.pid)
                report["build_group_live_remaining"] = [member for member in members if not member["state"].startswith("Z")]
        report["returncode"] = returncode
        artifacts = [json.loads(line) for line in (directory / "build.stdout").read_text().splitlines() if line.startswith("{")]
        selected = [artifact for artifact in artifacts if artifact.get("reason") == "compiler-artifact"
                    and artifact.get("executable") and artifact.get("target", {}).get("name") == "peri-loom-v1"]
        if returncode != 0 or len(selected) != 1:
            raise RuntimeError("offline_build_not_proven")
        artifact = selected[0]
        executable = Path(artifact["executable"])
        report["cargo_artifact"] = artifact
        report["built_binary_sha256"] = digest(executable)
        shutil.copy2(executable, directory / "native")
        (directory / "native").chmod(0o500)
        report["frozen_binary_sha256"] = digest(directory / "native")
        report["source_after"] = source_inventory()
        report["pass"] = report["source_after"] == before and report["frozen_binary_sha256"] == report["built_binary_sha256"] and not report["build_group_live_remaining"]
    except Exception as failure:
        report["pass"] = False
        report["failure_type"] = type(failure).__name__
    report["wall_seconds"] = time.monotonic() - started
    (directory / "provenance.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"pass": report["pass"], "provenance": str(directory / "provenance.json"),
                      "binary": str(directory / "native"), "wall_seconds": report["wall_seconds"]}))
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
