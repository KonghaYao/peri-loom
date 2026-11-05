#!/usr/bin/env python3
"""Capture and structurally compare one Hrana JSON pipeline response.

The harness records response byte counts, but a response embeds
`query_duration_ms`, whose float fraction width changes the byte count. This
probe sends the same F1-shaped pipeline a few times, stores the raw bodies and
records a normalized form where duration values and opaque identifiers are
replaced. A and B are compared on the normalized form, so a structural wire
change would be visible while duration-digit noise is not.

Usage: wire_probe.py --connection-file FILE --repo REPO --out DIR --label LABEL
The connection file is read for the endpoint/key and is never printed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys

REQUESTS = [
    {"type": "execute", "sql": "SELECT 1", "want_rows": True},
    {"type": "close"},
]
DURATION_KEYS = {"query_duration_ms", "duration_ms", "elapsed_ms", "query_ms"}
OPAQUE_KEYS = {"baton", "session_id", "request_id", "operation_id"}


def normalize(value, key=""):
    if isinstance(value, dict):
        return {name: normalize(item, name) for name, item in value.items()}
    if isinstance(value, list):
        return [normalize(item, key) for item in value]
    if key in DURATION_KEYS and isinstance(value, (int, float)):
        return "<DURATION>"
    if key in OPAQUE_KEYS and isinstance(value, str):
        return "<OPAQUE:%d>" % len(value)
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection-file", required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--requests", type=int, default=3)
    arguments = parser.parse_args()

    sys.path.insert(0, str(pathlib.Path(arguments.repo) / "scripts" / "perf-reliability"))
    from issue28.hrana import HranaClient, Target  # noqa: E402

    out = pathlib.Path(arguments.out)
    out.mkdir(parents=True, exist_ok=True)
    target = Target.from_file(arguments.connection_file)
    client = HranaClient(target, timeout=30.0)
    records = []
    for index in range(arguments.requests):
        raw = client.pipeline_json(REQUESTS)
        body = raw.body.decode("utf-8")
        parsed = json.loads(body)
        normalized = normalize(parsed)
        encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
        records.append({
            "index": index,
            "status": raw.status,
            "bytes": len(raw.body),
            "raw_sha256": hashlib.sha256(raw.body).hexdigest(),
            "normalized_sha256": hashlib.sha256(encoded.encode()).hexdigest(),
            "normalized": normalized,
        })
    client.close()
    document = {"label": arguments.label, "requests": records}
    (out / "wire-bodies.json").write_text(
        json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    sizes = [record["bytes"] for record in records]
    normalized = {record["normalized_sha256"] for record in records}
    print("%s bytes=%s normalized=%s"
          % (arguments.label, sizes, sorted(normalized)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
