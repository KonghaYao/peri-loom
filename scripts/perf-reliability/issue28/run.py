#!/usr/bin/env python3
"""CLI for the Issue 28 pure peri-loom performance plan.

Example:
  python3 scripts/perf-reliability/issue28/run.py \
    --connection-file /tmp/peri-issue28/target-connection.json \
    --container peri-loom-issue28 \
    --out /tmp/peri-issue28/results
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import pathlib
import platform
import subprocess
import sys
import time

if __package__ in (None, ""):
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
    __package__ = "issue28"

from .contract import build_scenarios
from .fixture import FixtureError, MetricsApi, discover
from .hrana import FatalTransportError, Target
from .report import concurrency_summary, render_markdown, summarize
from .runner import Runner


def main(argv: list[str] | None = None) -> int:
    arguments = _parse_args(argv)
    try:
        target = Target.from_file(arguments.connection_file)
    except Exception as error:  # noqa: BLE001 - fail closed before touching SQL
        print("cannot load connection file: %s" % type(error).__name__, file=sys.stderr)
        return 2
    try:
        fixture = discover(target)
    except (FixtureError, FatalTransportError) as error:
        print("fixture discovery failed: %s" % type(error).__name__, file=sys.stderr)
        return 2
    print("fixture: threads=%d messages=%d content_chars=%d content_bytes=%d complete=%s" % (
        fixture.threads, fixture.messages, fixture.content_chars,
        fixture.content_bytes, fixture.complete), file=sys.stderr)
    for check in fixture.checks:
        print("  %-16s expected=%r actual=%r %s" % (
            check.name, check.expected, check.actual, "OK" if check.ok else "MISMATCH"),
            file=sys.stderr)

    scenarios = build_scenarios(fixture)
    if arguments.scenario:
        wanted = set(arguments.scenario)
        scenarios = [item for item in scenarios
                     if item.key in wanted or any(item.key.startswith(value + ".") for value in wanted)]
    if arguments.protocol:
        wanted_protocols = set(arguments.protocol)
        filtered = []
        for scenario in scenarios:
            protocols = tuple(item for item in scenario.protocols if item.name in wanted_protocols)
            if protocols:
                filtered.append(_with_protocols(scenario, protocols))
        scenarios = filtered
    if not scenarios:
        print("no scenarios selected", file=sys.stderr)
        return 2

    print("plan: %d scenarios" % len(scenarios), file=sys.stderr)
    for scenario in scenarios:
        print("  %-24s %-10s %s" % (
            scenario.key, scenario.kind,
            ",".join(item.name for item in scenario.protocols)), file=sys.stderr)
    if arguments.dry_run:
        return 0

    output = pathlib.Path(arguments.out or _default_output())
    output.mkdir(parents=True, exist_ok=True)
    metadata = _metadata(target, arguments)
    metadata["platform_label"] = "HEAD %s / rust %s" % (
        metadata["git_revision"][:7],
        metadata.get("peri_image_version", "unknown"))
    for key in ("max_sessions", "max_concurrent", "requests_per_second", "max_result_bytes"):
        if key in fixture.quotas:
            metadata["quota_" + key] = fixture.quotas[key]
    metrics = MetricsApi(target)
    runner = Runner(
        target=target,
        fixture=fixture,
        scenarios=scenarios,
        rounds=arguments.rounds,
        warmup=arguments.warmup,
        samples=arguments.samples,
        metrics=metrics,
    )
    stats_before = _docker_stats(arguments.container)
    started = datetime.datetime.now(datetime.timezone.utc)
    result = runner.run()
    ended = datetime.datetime.now(datetime.timezone.utc)
    stats_after = _docker_stats(arguments.container)
    stats_idle = None
    if arguments.post_idle_seconds > 0:
        time.sleep(arguments.post_idle_seconds)
        stats_idle = _docker_stats(arguments.container)
    metadata["started_utc"] = started.isoformat()
    metadata["ended_utc"] = ended.isoformat()
    metadata["duration_seconds"] = round((ended - started).total_seconds(), 3)
    metadata["rounds"] = arguments.rounds
    metadata["warmup"] = arguments.warmup
    metadata["samples"] = arguments.samples
    metadata["resource_before"] = stats_before or {}
    metadata["resource_after"] = stats_after or {}
    metadata["resource_after_idle"] = stats_idle or {}

    rows = summarize(result, scenarios, fixture)
    concurrency = concurrency_summary(result)
    _write_raw(result, output / "raw.jsonl")
    document = {
        "metadata": metadata,
        "fixture": fixture.as_record(),
        "rows": [row.as_record() for row in rows],
        "concurrency": concurrency,
        "warnings": result.warnings,
        "fatal": result.fatal,
    }
    with open(output / "summary.json", "w", encoding="utf-8") as handle:
        json.dump(document, handle, ensure_ascii=False, indent=2)
    markdown = render_markdown(rows, concurrency, fixture, metadata,
                               result.warnings, result.fatal)
    with open(output / "summary.md", "w", encoding="utf-8") as handle:
        handle.write(markdown)
    print(markdown if arguments.print_table else "wrote %s and %s" % (
        output / "summary.md", output / "summary.json"), file=sys.stderr)

    failed = [row for row in rows if row.verdict == "FAIL"]
    if result.fatal:
        return 2
    if not fixture.complete:
        return 2
    if failed and not arguments.allow_misses:
        return 1
    return 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connection-file", required=True,
                        help="private connection.json; never printed")
    parser.add_argument("--out", help="output directory")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--scenario", action="append",
                        help="scenario key or prefix such as F1 or H1")
    parser.add_argument("--protocol", action="append",
                        help="protocol name such as json-pipeline or json-cursor")
    parser.add_argument("--container", help="docker container for optional resource evidence")
    parser.add_argument("--post-idle-seconds", type=float, default=0.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--print-table", action="store_true")
    parser.add_argument("--allow-misses", action="store_true",
                        help="return 0 even when acceptance targets are missed")
    arguments = parser.parse_args(argv)
    if arguments.rounds < 1 or arguments.warmup < 0 or arguments.samples < 1:
        parser.error("rounds >= 1, warmup >= 0 and samples >= 1 are required")
    if arguments.post_idle_seconds < 0:
        parser.error("post-idle-seconds must not be negative")
    return arguments


def _with_protocols(scenario, protocols):
    from dataclasses import replace
    return replace(scenario, protocols=protocols)


def _default_output() -> str:
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return str(pathlib.Path("artifacts") / ("issue28-" + stamp))


def _metadata(target: Target, arguments: argparse.Namespace) -> dict:
    metadata = {
        "target": target.public_identity(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "git_revision": _git_revision(),
        "connection_file": "provided (redacted)",
    }
    if arguments.container:
        metadata["container"] = arguments.container
        image = _docker_value(arguments.container, "{{.Config.Image}}")
        if image:
            metadata["image"] = image
        for key in ("PERI_IMAGE_VERSION", "PERI_IMAGE_REVISION"):
            value = _docker_env(arguments.container, key)
            if value:
                metadata[key.lower()] = value
    return metadata


def _docker_env(container: str, key: str) -> str:
    try:
        output = subprocess.check_output(
            ["docker", "inspect", "--format", "{{json .Config.Env}}", container],
            text=True, stderr=subprocess.DEVNULL).strip()
        for item in json.loads(output):
            if item.startswith(key + "="):
                return item.split("=", 1)[1]
    except (OSError, subprocess.CalledProcessError, ValueError):
        return ""
    return ""


def _git_revision() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _docker_value(container: str, template: str) -> str:
    try:
        return subprocess.check_output(
            ["docker", "inspect", "--format", template, container],
            text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return ""


def _docker_stats(container: str | None) -> dict | None:
    if not container:
        return None
    try:
        output = subprocess.check_output(
            ["docker", "stats", "--no-stream", "--format",
             "{{.MemUsage}}\t{{.CPUPerc}}", container],
            text=True, stderr=subprocess.DEVNULL, timeout=20).strip()
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    if not output:
        return None
    memory, _, cpu = output.partition("\t")
    return {
        "mem_usage": memory.strip(),
        "cpu_percent": cpu.strip(),
        "memory_stat": _memory_stat(container),
    }


def _memory_stat(container: str) -> dict:
    try:
        output = subprocess.check_output(
            ["docker", "exec", container, "cat", "/sys/fs/cgroup/memory.stat"],
            text=True, stderr=subprocess.DEVNULL, timeout=20)
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return {}
    wanted = {
        "anon", "file", "active_file", "inactive_file", "slab",
        "pgfault", "pgmajfault", "file_dirty", "file_writeback",
    }
    values = {}
    for line in output.splitlines():
        key, _, raw = line.partition(" ")
        if key in wanted:
            try:
                values[key] = int(raw)
            except ValueError:
                continue
    return values


def _write_raw(result, path: pathlib.Path) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        for measurement in result.measurements:
            handle.write(json.dumps(measurement.as_record(), ensure_ascii=False) + "\n")
        for metrics in result.concurrency:
            handle.write(json.dumps({
                "record_type": "concurrency_metrics",
                "level": metrics.level,
                "before": metrics.before,
                "after": metrics.after,
                "drained": metrics.drained,
                "drain_ms": metrics.drain_ms,
                "requests": metrics.requests,
                "status_counts": metrics.status_counts,
                "error_codes": metrics.error_codes,
            }, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
