#!/usr/bin/env python3
"""Shared metrics helpers for the R71 Gate 2 drivers.

The platform publishes /v1/metrics as a flat result document. The drivers
snapshot the whole document immediately before and after the timed window, so
every counter below is a delta of two snapshots taken on the same process. The
connection file is read for the endpoint and the global key and is never
printed.
"""
from __future__ import annotations

import json
import pathlib
import urllib.error
import urllib.request


def metrics_snapshot(target, timeout: float = 15.0) -> dict:
    request = urllib.request.Request(
        target.endpoint + "/v1/metrics",
        headers={"Authorization": "Bearer " + target.global_api_key},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    result = document.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("metrics response has no result")
    return result


def counter(document: dict, section: str, key: str) -> int:
    """Read one counter as an int; a missing section or key is zero."""
    if section:
        document = document.get(section) or {}
    return int(document.get(key) or 0)


def counter_delta(before: dict, after: dict, section: str, key: str) -> int:
    return counter(after, section, key) - counter(before, section, key)


def map_delta(before: dict, after: dict, section: str) -> dict:
    """Delta of a map of counters, keeping only keys that moved."""
    left = before.get(section) or {}
    right = after.get(section) or {}
    keys = set(left) | set(right)
    return {
        key: int(right.get(key) or 0) - int(left.get(key) or 0)
        for key in sorted(keys)
        if int(right.get(key) or 0) - int(left.get(key) or 0) != 0
    }


def deferred_status_delta(before: dict, after: dict) -> dict | None:
    """The deferred close account's own counters over the window.

    The account is absent while the kill switch is off, so the caller can use
    None to prove the flag produced no new counter key.
    """
    if "deferred_session_close" not in after:
        return None
    left = before.get("deferred_session_close") or {}
    right = after.get("deferred_session_close") or {}
    fields = (
        "enqueued_total",
        "inline_total",
        "failed_total",
        "retried_total",
        "unresolved_total",
        "flushed_total",
    )
    delta = {field: int(right.get(field) or 0) - int(left.get(field) or 0)
             for field in fields}
    delta["pending"] = int(right.get("pending") or 0)
    delta["pending_max"] = int(right.get("pending_max") or 0)
    delta["maximum"] = int(right.get("maximum") or 0)
    delta["per_database_maximum"] = int(right.get("per_database_maximum") or 0)
    delta["semantics"] = right.get("semantics")
    return delta


def window_summary(before: dict, after: dict) -> dict:
    """The counters the Gate 2 decision is read from, as window deltas."""
    close_count = counter_delta(before, after, "actions_total", "close")
    close_ns = counter_delta(before, after, "action_latency_ns_total", "close")
    deferred = deferred_status_delta(before, after)
    ratio = None
    if deferred is not None and close_count > 0:
        ratio = round(deferred["enqueued_total"] / close_count, 6)
    return {
        "close_actions": close_count,
        "close_latency_ns_total": close_ns,
        "close_latency_mean_us": round(close_ns / close_count / 1000.0, 3)
        if close_count else None,
        "actions_total": map_delta(before, after, "actions_total"),
        "action_latency_ns_total": map_delta(before, after, "action_latency_ns_total"),
        "errors_total": map_delta(before, after, "errors_total"),
        "rejection_reasons_total": map_delta(before, after, "rejection_reasons_total"),
        "admitted_requests_total": counter_delta(
            before, after, "", "admitted_requests_total"),
        "successful_requests_total": counter_delta(
            before, after, "", "successful_requests_total"),
        "rejected_requests_total": counter_delta(
            before, after, "", "rejected_requests_total"),
        "deferred_session_close": deferred,
        "eligibility_ratio": ratio,
        "session_seats_after": int(after.get("session_seats") or 0),
        "active_permits_after": int(after.get("active_permits") or 0),
        "unknown_held_permits_after": int(after.get("unknown_held_permits") or 0),
        "cleanup_ledger_after": after.get("cleanup_ledger") or {},
    }


def percentiles(values: list[float]) -> dict:
    if not values:
        return {}
    ordered = sorted(values)
    return {
        "min": ordered[0],
        "p50": ordered[len(ordered) // 2],
        "p95": ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))],
        "max": ordered[-1],
        "mean": sum(ordered) / len(ordered),
    }


def write_json(path: pathlib.Path, document) -> None:
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8")


def redact(text: str, target) -> str:
    """Remove the private keys from a failure string before it is recorded."""
    for secret in (target.project_api_key, target.global_api_key):
        if secret:
            text = text.replace(secret, "<REDACTED>")
    return text
