#!/usr/bin/env python3
"""One discarded warm-up request per stack before the P1 interleaved blocks.

The frozen block statistics never include this request. It exists so the first
measured block does not pay a connection/session cold-start that the A1/A2/B
order would then hand to whichever block happened to be drawn first. The
connection files are read for their endpoints and keys and are never printed.

Usage: warmup_stacks.py --a-connection FILE --b-connection FILE --repo REPO
"""
from __future__ import annotations

import argparse
import pathlib
import sys

REQUESTS = [
    {"type": "execute", "sql": "SELECT 1", "want_rows": True},
    {"type": "close"},
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--a-connection", required=True)
    parser.add_argument("--b-connection", required=True)
    parser.add_argument("--repo", required=True)
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

    status = 0
    for side, path in (("a", arguments.a_connection),
                       ("b", arguments.b_connection)):
        try:
            client = HranaClient(Target.from_file(path), timeout=120.0)
            raw = client.pipeline_json(REQUESTS)
            if raw.status != 200:
                print("warmup %s failed: http_%d" % (side, raw.status))
                status = 1
                continue
            reply = parse_pipeline_json(raw.body)
            rejected = [item for item in reply.results if not item.ok]
            if rejected:
                print("warmup %s rejected: %s" % (side,
                                                  rejected[0].error_code or "rejected"))
                status = 1
                continue
            print("warmup %s ok" % side)
        except (FatalTransportError, HranaError, OSError) as error:
            print("warmup %s error: %s" % (side, type(error).__name__))
            status = 1
    return status


if __name__ == "__main__":
    sys.exit(main())
