#!/usr/bin/env python3
"""Verify one copied audit directory from the R71 frozen A/B legs.

The platform writes a hash-chained JSONL audit: every record carries
payload.sequence (previous + 1) and payload.previous (the previous record's
hash). Archived rotation segments are named `events.json.segment.<20 digits>`
in chain order, and the active `events.json` continues the chain. A clean stop
also commits the head; when the head is present this script compares its
sequence/hash with the last record.

This is a structural continuity check (links, order, count), not a
reimplementation of the platform's checksum. It reads only copied evidence; the
connection file is never involved.

Usage: verify_audit.py <leg-directory> [more-leg-directories...]
Writes <leg-directory>/audit-verify.json and prints one summary line per leg.
"""
from __future__ import annotations

import json
import pathlib
import sys


def records_in(path: pathlib.Path) -> list[dict]:
    records = []
    with open(path, "r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise ValueError("%s:%d invalid JSON: %s" % (path, line_number, error))
    return records


def verify(directory: pathlib.Path) -> dict:
    audit_dir = directory / "audit"
    base = audit_dir / "events.json"
    report = {
        "leg": directory.name,
        "audit_dir": str(audit_dir),
        "present": audit_dir.is_dir(),
        "records": 0,
        "segments": [],
        "first_sequence": None,
        "last_sequence": None,
        "last_hash": None,
        "chain_ok": False,
        "head_present": False,
        "head_sequence": None,
        "head_hash": None,
        "head_matches_tail": None,
        "errors": [],
    }
    if not audit_dir.is_dir():
        report["errors"].append("audit directory missing")
        return report

    ordered: list[pathlib.Path] = []
    for entry in sorted(audit_dir.iterdir()):
        name = entry.name
        if not entry.is_file():
            continue
        if name.startswith("events.json.segment."):
            suffix = name[len("events.json.segment."):]
            if len(suffix) != 20 or not suffix.isdigit():
                report["errors"].append("unexpected segment name: " + name)
                continue
            ordered.append(entry)
        elif name == "events.json":
            ordered.append(entry)
        elif (name in ("events.json.head", "events.json.head.floor",
                       "events.json.rotate")
              or name.endswith(".lock")):
            continue
        else:
            report["errors"].append("unexpected audit file: " + name)
    ordered.sort(key=lambda item: (item.name != "events.json", item.name))

    previous_sequence = None
    previous_hash = None
    for path in ordered:
        try:
            records = records_in(path)
        except ValueError as error:
            report["errors"].append(str(error))
            continue
        report["segments"].append({"file": path.name, "records": len(records)})
        for record in records:
            payload = record.get("payload") or {}
            sequence = payload.get("sequence")
            if not isinstance(sequence, int):
                report["errors"].append("record without integer sequence in " + path.name)
                continue
            if previous_sequence is None:
                pass
            elif sequence != previous_sequence + 1:
                report["errors"].append(
                    "sequence gap: %r -> %r in %s" % (previous_sequence, sequence, path.name))
            if previous_hash is not None and payload.get("previous") != previous_hash:
                report["errors"].append(
                    "previous link mismatch at sequence %d in %s" % (sequence, path.name))
            if report["first_sequence"] is None:
                report["first_sequence"] = sequence
            previous_sequence = sequence
            previous_hash = record.get("hash")
            report["records"] += 1
            report["last_sequence"] = sequence
            report["last_hash"] = previous_hash

    report["chain_ok"] = not report["errors"] and report["records"] > 0

    head_path = audit_dir / "events.json.head"
    if head_path.is_file():
        report["head_present"] = True
        try:
            envelope = json.loads(head_path.read_text(encoding="utf-8"))
            head = envelope.get("payload") or {}
            report["head_sequence"] = head.get("sequence")
            report["head_hash"] = head.get("hash")
            report["head_matches_tail"] = (
                head.get("sequence") == report["last_sequence"]
                and head.get("hash") == report["last_hash"])
        except (OSError, json.JSONDecodeError) as error:
            report["errors"].append("head unreadable: %s" % type(error).__name__)
    return report


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    failures = 0
    for argument in sys.argv[1:]:
        directory = pathlib.Path(argument)
        report = verify(directory)
        (directory / "audit-verify.json").write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        status = "OK" if report["chain_ok"] else "BROKEN"
        if not report["chain_ok"]:
            failures += 1
        print("%s %-28s records=%d seq=%s..%s head=%s errors=%d"
              % (status, directory.name, report["records"],
                 report["first_sequence"], report["last_sequence"],
                 report["head_matches_tail"], len(report["errors"])))
        for error in report["errors"][:5]:
            print("  error: " + error)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
