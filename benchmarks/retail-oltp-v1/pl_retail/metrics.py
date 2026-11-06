"""Explainable resource and refusal metrics.

The platform publishes a closed rejection-reason vocabulary.  This module
keeps that vocabulary intact, records deltas across a measured window, and
correlates every attempt with its business id, outer request ids and internal
operation id.  It never relabels a reason and never reports a missing source
as zero.
"""
from __future__ import annotations

import json
import os
import shutil
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

MAX_SAMPLE_BYTES = 8 * 1024 * 1024


def _unavailable(reason: str) -> dict[str, Any]:
    return {"status": "unavailable", "reason": reason}


def read_text(path: str | Path, limit: int = 1 << 20) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        return handle.read(limit)


def parse_kv(text: str) -> dict[str, int]:
    values: dict[str, int] = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[1].isdigit():
            values[parts[0]] = int(parts[1])
    return values


def proc_status(pid: int) -> dict[str, Any]:
    try:
        text = read_text("/proc/%d/status" % pid)
    except OSError as error:
        return _unavailable(str(error))
    fields: dict[str, Any] = {}
    for line in text.splitlines():
        key, _, value = line.partition(":")
        if key in {"VmRSS", "VmHWM", "Threads"}:
            parts = value.split()
            if parts and parts[0].isdigit():
                fields[key] = int(parts[0])
    return {"status": "ok", "fields": fields}


def proc_io(pid: int) -> dict[str, Any]:
    try:
        text = read_text("/proc/%d/io" % pid)
    except OSError as error:
        return _unavailable(str(error))
    values = {}
    for line in text.splitlines():
        key, _, value = line.partition(":")
        if value.strip().isdigit():
            values[key] = int(value.strip())
    return {"status": "ok", "fields": values}


def proc_cpu(pid: int) -> dict[str, Any]:
    try:
        text = read_text("/proc/%d/stat" % pid)
    except OSError as error:
        return _unavailable(str(error))
    fields = text.rsplit(")", 1)[-1].split()
    if len(fields) < 15:
        return _unavailable("short_proc_stat")
    return {
        "status": "ok",
        "utime_ticks": int(fields[11]),
        "stime_ticks": int(fields[12]),
        "rss_pages": int(fields[21]) if len(fields) > 21 else None,
        "clock_ticks": os.sysconf("SC_CLK_TCK"),
    }


def cgroup_snapshot(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return _unavailable("no_cgroup_path_in_descriptor")
    root = Path(path)
    if not root.exists():
        return _unavailable("cgroup_path_missing")
    result: dict[str, Any] = {"status": "ok", "path": str(root)}
    files = {
        "memory_current": "memory.current",
        "memory_max": "memory.max",
        "memory_events": "memory.events",
        "cpu_stat": "cpu.stat",
        "io_stat": "io.stat",
    }
    for key, name in files.items():
        target = root / name
        try:
            text = read_text(target)
        except OSError as error:
            result[key] = _unavailable(str(error))
            continue
        if name in {"memory_events", "cpu_stat"}:
            result[key] = parse_kv(text)
        elif name == "io_stat":
            result[key] = text.strip()
        else:
            stripped = text.strip()
            result[key] = int(stripped) if stripped.isdigit() else stripped
    return result


def disk_snapshot(path: str | Path) -> dict[str, Any]:
    try:
        usage = shutil.disk_usage(str(path))
    except OSError as error:
        return _unavailable(str(error))
    return {
        "status": "ok",
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
    }


def client_snapshot() -> dict[str, Any]:
    status = proc_status(os.getpid())
    io = proc_io(os.getpid())
    cpu = proc_cpu(os.getpid())
    return {"pid": os.getpid(), "status": status, "io": io, "cpu": cpu}


@dataclass
class ResourceSampler:
    """One-second samples of the client, server process, cgroup and disk."""

    descriptor: dict[str, Any]
    sink: Callable[[dict[str, Any]], None]
    interval_seconds: float = 1.0
    phase: str = "setup"
    _thread: threading.Thread | None = None
    _stop: threading.Event = field(default_factory=threading.Event)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="resource-sampler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.sink(self.sample())
            except Exception as error:  # sampling must never take the run down
                self.sink(
                    {
                        "monotonic_ns": time.monotonic_ns(),
                        "phase": self.phase,
                        "status": "sampler_error",
                        "reason": "%s: %s" % (type(error).__name__, error),
                    }
                )
            self._stop.wait(self.interval_seconds)

    def sample(self) -> dict[str, Any]:
        server_pid = self.descriptor.get("server_pid")
        server = (
            {
                "status": "unavailable",
                "reason": "no_server_pid_in_descriptor",
            }
            if not server_pid
            else {
                "pid": int(server_pid),
                "status": proc_status(int(server_pid)),
                "io": proc_io(int(server_pid)),
                "cpu": proc_cpu(int(server_pid)),
            }
        )
        return {
            "monotonic_ns": time.monotonic_ns(),
            "wall_ns": time.time_ns(),
            "phase": self.phase,
            "client": client_snapshot(),
            "server": server,
            "cgroup": cgroup_snapshot(self.descriptor.get("cgroup_path")),
            "disk": disk_snapshot(self.descriptor.get("work_path") or "."),
        }


# -- platform metric snapshots ---------------------------------------------


def fetch_metrics(base_url: str, api_key: str, ca_file: str | None, timeout: float = 5.0) -> dict[str, Any]:
    url = base_url.rstrip("/") + "/v1/metrics"
    request = urllib.request.Request(url, method="GET")
    request.add_header("Authorization", "Bearer %s" % api_key)
    context = None
    if ca_file:
        import ssl

        context = ssl.create_default_context(cafile=ca_file)
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
            payload = response.read(MAX_SAMPLE_BYTES)
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        return _unavailable("%s: %s" % (type(error).__name__, error))
    try:
        document = json.loads(payload)
    except json.JSONDecodeError as error:
        return _unavailable("invalid_json: %s" % error)
    if not isinstance(document, dict):
        return _unavailable("metrics_not_an_object")
    return {"status": "ok", "document": document}


COUNTER_SECTIONS = (
    "actions_total",
    "errors_total",
    "rejection_reasons_total",
    "admitted_requests_total",
    "successful_requests_total",
    "rejected_requests_total",
    "action_latency_ns_total",
    "successful_latency_ns_total",
)


def _numeric_leaves(value: Any, prefix: str = "") -> dict[str, int]:
    leaves: dict[str, int] = {}
    if isinstance(value, dict):
        for key, child in value.items():
            child_prefix = "%s.%s" % (prefix, key) if prefix else str(key)
            leaves.update(_numeric_leaves(child, child_prefix))
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        leaves[prefix] = int(value)
    return leaves


def metric_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    if before.get("status") != "ok" or after.get("status") != "ok":
        return _unavailable("metric_snapshot_missing")
    left = before.get("document", {})
    right = after.get("document", {})
    delta: dict[str, Any] = {"status": "ok"}
    for section in COUNTER_SECTIONS:
        left_values = _numeric_leaves(left.get(section, {}))
        right_values = _numeric_leaves(right.get(section, {}))
        keys = set(left_values) | set(right_values)
        values = {
            key: int(right_values.get(key, 0)) - int(left_values.get(key, 0)) for key in keys
        }
        delta[section] = values
    delta["session_seats_before"] = left.get("session_seats")
    delta["session_seats_after"] = right.get("session_seats")
    delta["active_permits_before"] = left.get("active_permits")
    delta["active_permits_after"] = right.get("active_permits")
    delta["unknown_held_permits_after"] = right.get("unknown_held_permits")
    delta["observed_workers_after"] = right.get("observed_workers")
    delta["audit_after"] = right.get("audit")
    delta["cleanup_ledger_after"] = right.get("cleanup_ledger")
    delta["state_operation_cost_after"] = right.get("state_operation_cost")
    delta["rejection_reason_values"] = right.get("rejection_reason_values")
    return delta


def rejection_breakdown(delta: dict[str, Any]) -> dict[str, Any]:
    """Split named refusals into explainable buckets without renaming them."""
    if delta.get("status") != "ok":
        return _unavailable("metric_delta_missing")
    reasons = delta.get("rejection_reasons_total", {})
    groups: dict[str, int] = {}
    for reason, count in reasons.items():
        if count == 0:
            continue
        if "rate" in reason:
            group = "rate_limit"
        elif "concurrent" in reason or "capacity" in reason:
            group = "concurrency"
        elif "session" in reason:
            group = "session"
        elif "queue" in reason:
            group = "writer_queue"
        elif "audit" in reason:
            group = "audit"
        else:
            group = "other"
        groups[group] = groups.get(group, 0) + count
    return {"status": "ok", "by_reason": reasons, "by_group": groups}


def metric_gaps(document: dict[str, Any]) -> list[str]:
    gaps: list[str] = []
    if document.get("cold_load_instrumentation") == "not_exposed_by_worker_sql_protocol":
        gaps.append("cold_load_not_exposed_by_worker_sql_protocol")
    if "cleanup_ledger" not in document:
        gaps.append("cleanup_ledger_missing")
    return gaps
