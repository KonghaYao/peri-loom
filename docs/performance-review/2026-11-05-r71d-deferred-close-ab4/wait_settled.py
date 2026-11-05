#!/usr/bin/env python3
"""Wait for one isolated stack to settle, then snapshot `/v1/metrics`.

A deferred worker close happens after the reply, so the counters and the
worker's session gauge can move for a short time after the driver returns.
This script polls until:

  * deferred_session_close.pending == 0 (absent when the flag is off),
  * cleanup_ledger.pending_close.count == 0,
  * native.workers.*.lifecycle.pending_close.count == 0,
  * native.workers.*.sessions == 0,
  * session_seats == 0,
  * active_permits == 0,
  * unknown_held_permits == 0,

or until the deadline. It writes the final metrics document and a JSON trace of
every poll, so a timeout is evidence rather than a silent pass. The connection
file is read for the endpoint and key and is never printed.

Usage: wait_settled.py <connection.json> <metrics-out.json> <trace-out.json>
                       [--timeout 30] [--interval 0.2]
"""
from __future__ import annotations

import json
import sys
import time
import urllib.request


def snapshot(document: dict) -> dict:
    request = urllib.request.Request(
        document["endpoint"] + "/v1/metrics",
        headers={"Authorization": "Bearer " + document["global_api_key"]},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        result = json.load(response).get("result")
    if not isinstance(result, dict):
        raise RuntimeError("metrics response has no result")
    return result


def state(metrics: dict) -> dict:
    deferred = metrics.get("deferred_session_close") or {}
    pending_close = (metrics.get("cleanup_ledger") or {}).get("pending_close") or {}
    workers = (metrics.get("native") or {}).get("workers") or {}
    worker_sessions = {}
    worker_pending = {}
    for name, worker in workers.items():
        worker_sessions[name] = int(worker.get("sessions") or 0)
        worker_pending[name] = int((worker.get("lifecycle") or {})
                                   .get("pending_close", {}).get("count") or 0)
    return {
        "deferred_pending": int(deferred.get("pending") or 0),
        "deferred_present": "deferred_session_close" in metrics,
        "cleanup_pending_close": int(pending_close.get("count") or 0),
        "worker_sessions": worker_sessions,
        "worker_pending_close": worker_pending,
        "session_seats": int(metrics.get("session_seats") or 0),
        "active_permits": int(metrics.get("active_permits") or 0),
        "unknown_held_permits": int(metrics.get("unknown_held_permits") or 0),
    }


def settled(value: dict) -> bool:
    # The flag-off side publishes no deferred key; that is a settled state.
    if value["deferred_present"] and value["deferred_pending"] != 0:
        return False
    if value["cleanup_pending_close"] != 0:
        return False
    if any(count != 0 for count in value["worker_pending_close"].values()):
        return False
    if any(count != 0 for count in value["worker_sessions"].values()):
        return False
    # P3 also requires the admission and session accounts to be empty. A
    # "settled" verdict that leaves them non-zero hides exactly the leak P3
    # exists to catch, so they are part of the same condition here.
    if value["session_seats"] != 0:
        return False
    if value["active_permits"] != 0:
        return False
    if value["unknown_held_permits"] != 0:
        return False
    return True


def main() -> int:
    arguments = sys.argv[1:]
    timeout = 30.0
    interval = 0.2
    positional = []
    index = 0
    while index < len(arguments):
        if arguments[index] == "--timeout":
            timeout = float(arguments[index + 1])
            index += 2
        elif arguments[index] == "--interval":
            interval = float(arguments[index + 1])
            index += 2
        else:
            positional.append(arguments[index])
            index += 1
    if len(positional) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    connection_path, metrics_path, trace_path = positional
    document = json.load(open(connection_path, encoding="utf-8"))
    trace = []
    started = time.monotonic()
    final = None
    while True:
        try:
            final = snapshot(document)
            observed = state(final)
        except Exception as error:  # noqa: BLE001 - record and retry inside the deadline
            trace.append({"elapsed_s": round(time.monotonic() - started, 4),
                          "error": type(error).__name__})
        else:
            trace.append({"elapsed_s": round(time.monotonic() - started, 4),
                          **observed})
            if settled(observed):
                break
        if time.monotonic() - started >= timeout:
            break
        time.sleep(interval)
    if final is None:
        raise SystemExit("metrics never answered")
    verdict = "settled" if settled(state(final)) else "timeout"
    with open(metrics_path, "w", encoding="utf-8") as handle:
        json.dump(final, handle, indent=2, sort_keys=True)
        handle.write("\n")
    with open(trace_path, "w", encoding="utf-8") as handle:
        json.dump({"verdict": verdict, "timeout_s": timeout, "polls": trace}, handle,
                  indent=2, sort_keys=True)
        handle.write("\n")
    print("settle: %s after %d polls (%.3fs)"
          % (verdict, len(trace), trace[-1].get("elapsed_s", 0)))
    return 0 if verdict == "settled" else 1


if __name__ == "__main__":
    sys.exit(main())
