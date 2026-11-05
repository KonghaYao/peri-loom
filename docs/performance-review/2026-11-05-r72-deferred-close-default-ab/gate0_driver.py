#!/usr/bin/env python3
"""Gate 0 short driver: a handful of P1-shaped requests on both R72 stacks.

One request is the same Hrana JSON pipeline used by the P1 blocks:

    {"requests":[{"type":"execute","stmt":{"sql":"SELECT 1",
     "want_rows":true}},{"type":"close"}]}

The Gate 0 legs are mechanism checks, not performance measurements: five
requests per stack are enough to open and close five sessions and to publish
the deferred_session_close account on the default-on side.

The connection files are read for their endpoints and keys and are never
printed.

Usage: gate0_driver.py --a-connection FILE --b-connection FILE --repo REPO --out DIR
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time


REQUESTS = [
    {"type": "execute", "sql": "SELECT 1", "want_rows": True},
    {"type": "close"},
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--a-connection", required=True)
    parser.add_argument("--b-connection", required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--requests", type=int, default=5)
    arguments = parser.parse_args()

    repo = pathlib.Path(arguments.repo)
    sys.path.insert(0, str(repo / "scripts" / "perf-reliability"))
    from issue28.hrana import (  # noqa: E402 - path is set above
        FatalTransportError,
        HranaClient,
        HranaError,
        Target,
        parse_pipeline_json,
    )

    out = pathlib.Path(arguments.out)
    out.mkdir(parents=True, exist_ok=True)
    records = []
    status = 0
    for side, path in (("a", arguments.a_connection), ("b", arguments.b_connection)):
        client = HranaClient(Target.from_file(path), timeout=120.0)
        started = time.perf_counter_ns()
        failures = []
        for _ in range(arguments.requests):
            try:
                raw = client.pipeline_json(REQUESTS)
            except (FatalTransportError, HranaError, OSError) as error:
                failures.append({"kind": "transport", "detail": type(error).__name__})
                break
            if raw.status != 200:
                failures.append({"kind": "http", "status": raw.status})
                break
            try:
                reply = parse_pipeline_json(raw.body)
            except HranaError as error:
                failures.append({"kind": "decode", "detail": type(error).__name__})
                break
            rejected = [item for item in reply.results if not item.ok]
            if rejected:
                failures.append({"kind": "rejected", "code": rejected[0].error_code})
                break
        elapsed = time.perf_counter_ns() - started
        record = {
            "side": side,
            "requests": arguments.requests,
            "failures": failures,
            "elapsed_ms": round(elapsed / 1e6, 3),
            "per_request_ms": round(elapsed / 1e6 / arguments.requests, 3),
        }
        records.append(record)
        if failures:
            status = 1
        print(json.dumps(record, ensure_ascii=False), flush=True)
        client.close()
    (out / "gate0-driver.json").write_text(
        json.dumps({"records": records}, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8")
    return status


if __name__ == "__main__":
    sys.exit(main())
