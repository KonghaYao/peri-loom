#!/usr/bin/env python3
"""Release build selection with frozen source/compiler/artifact evidence.

The default is a release build in an isolated CARGO_TARGET_DIR. Offline mode
and a wall-clock budget are explicit opt-ins; there is no implicit short cache
window. A missing cargo/rustc toolchain fails closed instead of silently
falling back to a debug or cached artifact.
"""
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

DEFAULT_JOBS = 2


class BuildError(RuntimeError):
    pass


def build_command(root, release=True, offline=False, jobs=DEFAULT_JOBS,
                  binary="peri-loom-v1"):
    command = ["cargo", "build",
               "--manifest-path", str(Path(root) / "prototypes/v1/Cargo.toml"),
               "--locked", "--jobs", str(jobs), "--bin", binary,
               "--message-format=json"]
    if release:
        command.append("--release")
    if offline:
        command.append("--offline")
    return command


def build_environment(target_directory=None):
    environment = {name: os.environ[name]
                   for name in ("PATH", "HOME", "TMPDIR", "CARGO_HOME", "RUSTUP_HOME")
                   if name in os.environ}
    if target_directory is not None:
        environment["CARGO_TARGET_DIR"] = str(target_directory)
    return environment


def require_toolchain(environment, runner=subprocess.check_output):
    missing = [name for name in ("cargo", "rustc")
               if shutil.which(name, path=environment.get("PATH")) is None]
    if missing:
        raise BuildError("release_toolchain_unavailable: " + ",".join(missing))
    try:
        cargo = runner(["cargo", "--version"], env=environment, text=True, timeout=5).strip()
        rustc = runner(["rustc", "-Vv"], env=environment, text=True, timeout=5).strip()
    except (OSError, subprocess.SubprocessError) as failure:
        raise BuildError("release_toolchain_unavailable: " + type(failure).__name__) from None
    return {"cargo": cargo, "rustc": rustc}


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true",
                        help="diagnostic debug build; never eligible as evidence")
    parser.add_argument("--offline", action="store_true",
                        help="explicit opt-in to cargo --offline (cache reuse)")
    parser.add_argument("--max-seconds", type=int,
                        help="optional wall-clock build budget; no implicit cache window")
    parser.add_argument("--jobs", type=int, default=DEFAULT_JOBS)
    arguments = parser.parse_args(argv)
    if arguments.max_seconds is not None and arguments.max_seconds < 1:
        parser.error("--max-seconds must be positive")
    if arguments.jobs < 1:
        parser.error("--jobs must be positive")
    return arguments


def main(argv=None):
    signal.signal(signal.SIGTERM, lambda signum, frame: (_ for _ in ()).throw(KeyboardInterrupt()))
    arguments = parse_arguments(argv)
    os.umask(0o077)
    try:
        environment = build_environment()
        toolchain = require_toolchain(environment)
    except BuildError as failure:
        print(json.dumps({"pass": False, "failure": str(failure)}))
        return 1
    directory = Path(tempfile.mkdtemp(prefix="peri-perf-build-"))
    target_directory = directory / "target"
    environment = build_environment(target_directory)
    before = source_inventory()
    release = not arguments.debug
    command = build_command(ROOT, release=release, offline=arguments.offline,
                            jobs=arguments.jobs)
    report = {"source_before": before, "command": command, "build_directory": str(directory),
              "target_directory": str(target_directory), "isolated_target_directory": True,
              "offline": arguments.offline, "compiler": toolchain,
              "build_kind": "release build" if release
              else "debug build (diagnostic only; not evidence)",
              "registry_binding": "Cargo.lock checksums; arbitrary cache tampering not "
                                  "independently excluded"}
    started = time.monotonic()
    try:
        report["cargo_version"] = toolchain["cargo"]
        report["rustc_version"] = toolchain["rustc"]
        with (directory / "build.stdout").open("x") as stdout, \
                (directory / "build.stderr").open("x") as stderr:
            child = subprocess.Popen(command, cwd=ROOT, env=environment, stdout=stdout,
                                     stderr=stderr, start_new_session=True)
            try:
                returncode = child.wait(timeout=arguments.max_seconds) \
                    if arguments.max_seconds else child.wait()
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
                report["build_group_live_remaining"] = [
                    member for member in members if not member["state"].startswith("Z")]
        report["returncode"] = returncode
        artifacts = [json.loads(line) for line in
                     (directory / "build.stdout").read_text().splitlines() if line.startswith("{")]
        selected = [artifact for artifact in artifacts
                    if artifact.get("reason") == "compiler-artifact"
                    and artifact.get("executable")
                    and artifact.get("target", {}).get("name") == "peri-loom-v1"]
        if returncode != 0 or len(selected) != 1:
            raise BuildError("build_not_proven")
        artifact = selected[0]
        executable = Path(artifact["executable"])
        report["compiler_artifact"] = artifact
        report["cargo_artifact"] = artifact
        report["built_binary_sha256"] = digest(executable)
        shutil.copy2(executable, directory / "native")
        (directory / "native").chmod(0o500)
        report["frozen_binary_sha256"] = digest(directory / "native")
        report["source_after"] = source_inventory()
        report["evidence_eligible"] = release and not arguments.offline
        report["pass"] = bool(report["source_after"] == before
                              and report["frozen_binary_sha256"] == report["built_binary_sha256"]
                              and not report["build_group_live_remaining"]
                              and report["evidence_eligible"])
    except Exception as failure:
        report["pass"] = False
        report["evidence_eligible"] = False
        report["failure_type"] = type(failure).__name__
        report["failure"] = str(failure)
    report["wall_seconds"] = time.monotonic() - started
    (directory / "provenance.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"pass": report["pass"], "evidence_eligible": report.get("evidence_eligible", False),
                      "provenance": str(directory / "provenance.json"),
                      "binary": str(directory / "native"), "wall_seconds": report["wall_seconds"]}))
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
