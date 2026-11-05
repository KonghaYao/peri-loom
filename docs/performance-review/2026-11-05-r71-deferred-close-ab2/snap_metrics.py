#!/usr/bin/env python3
"""Snapshot the platform's full /v1/metrics result document.

Usage: snap_metrics.py <connection.json> <out.json>
The connection file carries private keys; it is read, never printed.
"""
import json
import sys
import urllib.request

document = json.load(open(sys.argv[1], encoding="utf-8"))
request = urllib.request.Request(
    document["endpoint"] + "/v1/metrics",
    headers={"Authorization": "Bearer " + document["global_api_key"]},
)
with urllib.request.urlopen(request, timeout=15) as response:
    result = json.load(response)["result"]
open(sys.argv[2], "w", encoding="utf-8").write(
    json.dumps(result, indent=2, sort_keys=True) + "\n")
