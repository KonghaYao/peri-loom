#!/usr/bin/env python3
"""Timeline probe for the Issue 28 X1 tail.

The harness records one duration per request, which cannot distinguish a stall
inside the server from a stall in the client process or on the host. This probe
records the two facts the harness discards: when each request started and ended,
and whether the host kept scheduling at all.

Two modes, both writing one JSON object per line to stdout. Every timestamp is
`time.monotonic()` from the platform clock, so a `canary` line and a `client`
line can be joined directly.

  client  --threads 4 --total 100
  canary  --seconds 5 --step-ms 5

Run one of each concurrently against the same stack: a client stall that lines
up with a canary lag is a host or scheduler event, not a peri-loom one.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "issue28"))

from hrana import HranaClient, Target  # noqa: E402

# The X1 request shape: an implicit session open fused with one read-only
# statement, then the close in the same pipeline.
REQUESTS = [
    {"type": "execute", "sql": "SELECT 1", "want_rows": True, "args": ()},
    {"type": "close"},
]


def _emit(record: dict) -> None:
    sys.stdout.write(json.dumps(record, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def run_client(arguments: argparse.Namespace) -> int:
    target = Target.from_file(arguments.connection_file)
    local = threading.local()
    lock = threading.Lock()
    in_flight: dict[tuple[int, int], int] = {}
    started = time.monotonic_ns()
    failures: list[str] = []

    def client() -> HranaClient:
        instance = getattr(local, "client", None)
        if instance is None:
            instance = HranaClient(target)
            local.client = instance
        return instance

    def watch() -> None:
        """Dump the server's goroutines the moment a request is stalled.

        A random SIGQUIT mostly lands outside a stall; this fires while the
        evidence exists. The dump ends the server, so one shot is enough.
        """
        threshold = int(arguments.watch_ms * 1_000_000)
        while True:
            time.sleep(0.001)
            with lock:
                oldest = min(in_flight.values(), default=0)
            if oldest and time.monotonic_ns() - oldest >= threshold:
                print(json.dumps({
                    "kind": "watch",
                    "stalled_ms": round((time.monotonic_ns() - oldest) / 1_000_000, 3),
                }, separators=(",", ":")), file=sys.stderr)
                os.kill(arguments.watch_pid, signal.SIGQUIT)
                return

    def worker(index: int, planned: int) -> None:
        try:
            connection = client()
            connection.pipeline_json(REQUESTS)  # warm the session path
            for sequence in range(planned):
                slot = (index, sequence)
                with lock:
                    in_flight[slot] = time.monotonic_ns()
                reply = connection.pipeline_json(REQUESTS)
                with lock:
                    in_flight.pop(slot, None)
                _emit({
                    "kind": "client",
                    "thread": index,
                    "seq": sequence,
                    "start_ns": reply.started_ns,
                    "header_ns": reply.headers_ns,
                    "end_ns": reply.completed_ns,
                    "status": reply.status,
                    "bytes": len(reply.body),
                    "reused": reply.connection_reused,
                })
        except Exception as error:  # noqa: BLE001 - reported, never swallowed
            with lock:
                failures.append("thread %d: %s" % (index, type(error).__name__))

    threads = []
    base, extra = divmod(arguments.total, arguments.threads)
    for index in range(arguments.threads):
        planned = base + (1 if index < extra else 0)
        thread = threading.Thread(target=worker, args=(index, planned), daemon=False)
        threads.append(thread)
    if arguments.watch_pid:
        threading.Thread(target=watch, daemon=True).start()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed_ms = (time.monotonic_ns() - started) / 1_000_000
    print(json.dumps({
        "kind": "client-summary",
        "threads": arguments.threads,
        "total": arguments.total,
        "elapsed_ms": round(elapsed_ms, 3),
        "failures": failures,
    }, separators=(",", ":")), file=sys.stderr)
    return 1 if failures else 0


def run_canary(arguments: argparse.Namespace) -> int:
    step_ns = int(arguments.step_ms * 1_000_000)
    deadline = time.monotonic_ns()
    stop = deadline + int(arguments.seconds * 1_000_000_000)
    threshold_ns = int(arguments.threshold_ms * 1_000_000)
    worst_ns = 0
    while deadline < stop:
        deadline += step_ns
        remaining = deadline - time.monotonic_ns()
        if remaining > 0:
            time.sleep(remaining / 1_000_000_000)
        now = time.monotonic_ns()
        lag = now - deadline
        worst_ns = max(worst_ns, lag)
        if lag >= threshold_ns:
            _emit({"kind": "canary", "at_ns": now, "lag_ms": round(lag / 1_000_000, 3)})
    print(json.dumps({
        "kind": "canary-summary",
        "step_ms": arguments.step_ms,
        "worst_ms": round(worst_ns / 1_000_000, 3),
    }, separators=(",", ":")), file=sys.stderr)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)

    client = sub.add_parser("client")
    client.add_argument("--connection-file", required=True)
    client.add_argument("--threads", type=int, default=4)
    client.add_argument("--total", type=int, default=100)
    client.add_argument("--watch-pid", type=int,
                        help="server pid to SIGQUIT while a request is stalled")
    client.add_argument("--watch-ms", type=float, default=30.0)
    client.set_defaults(handler=run_client)

    canary = sub.add_parser("canary")
    canary.add_argument("--seconds", type=float, default=5.0)
    canary.add_argument("--step-ms", type=float, default=5.0)
    canary.add_argument("--threshold-ms", type=float, default=20.0)
    canary.set_defaults(handler=run_canary)

    arguments = parser.parse_args()
    return arguments.handler(arguments)


if __name__ == "__main__":
    raise SystemExit(main())
