#!/usr/bin/env python3
"""Append-only issue 11 evidence layout and manifests.

The raw JSONL writer and JSON helper are the existing perf-reliability ones.
This module only adds the issue 11 run layout, exclusive creation, and an
integrity manifest; the trust root for the run remains external.
"""
import json
import os
from pathlib import Path
import sys

PERF_ROOT = Path(__file__).resolve().parent.parent
if str(PERF_ROOT) not in sys.path:
    sys.path.insert(0, str(PERF_ROOT))

import evidence as base_evidence  # noqa: E402
from perf_contract import digest, ensure_empty_directory  # noqa: E402

REQUIRED_RUN_ARTIFACTS = (
    "environment.json",
    "operations.jsonl",
    "requests.jsonl",
    "request-attribution.jsonl",
    "periodic-events.jsonl",
    "metrics-snapshot-index.json",
    "resources.json",
    "oracle.json",
    "run.json",
)


class EvidenceError(ValueError):
    pass


def write_json(path, payload, reseal=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w" if reseal else "x") as output:
        json.dump(payload, output, indent=2, sort_keys=True)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    return path


def read_json(path):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as failure:
        raise EvidenceError("evidence_json_unreadable: " + str(path) + ": "
                            + type(failure).__name__) from None


def raw_writer(path, flush_every=256):
    return base_evidence.RawRecordWriter(path, flush_every=flush_every)


def create_root(path):
    try:
        return ensure_empty_directory(path)
    except ValueError as failure:
        raise EvidenceError("evidence_root_reuse_refused: " + str(failure)) from None


def create_run_directory(root, run_id):
    if not run_id or "/" in run_id or run_id in {".", ".."}:
        raise EvidenceError("run_id_invalid")
    run_directory = Path(root) / "runs" / run_id
    run_directory.parent.mkdir(parents=True, exist_ok=True)
    return ensure_empty_directory(run_directory)


def append_ledger(root, record):
    path = Path(root) / "campaign-runs.jsonl"
    with path.open("a") as output:
        output.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
        output.flush()
        os.fsync(output.fileno())


def artifact_paths(run_directory):
    return [Path(run_directory) / name for name in REQUIRED_RUN_ARTIFACTS]


def verify_required(run_directory):
    missing = [name for name in REQUIRED_RUN_ARTIFACTS
               if not (Path(run_directory) / name).is_file()]
    return missing


def file_hashes(run_directory, names=None):
    names = names or REQUIRED_RUN_ARTIFACTS
    hashes = {}
    for name in names:
        path = Path(run_directory) / name
        if not path.is_file():
            raise EvidenceError("evidence_artifact_missing: " + str(name))
        hashes[str(name)] = digest(path)
    return hashes


def write_manifests(run_directory, run_id, reseal=False):
    hashes = file_hashes(run_directory)
    sums_path = Path(run_directory) / "SHA256SUMS.json"
    write_json(sums_path, hashes, reseal=reseal)
    manifest = {
        "schema": 1,
        "run_id": run_id,
        "integrity_only": True,
        "trust_root": "operator or CI; this file proves internal consistency only",
        "artifacts": [
            {
                "path": name,
                "bytes": (Path(run_directory) / name).stat().st_size,
                "sha256": hashes[name],
            }
            for name in REQUIRED_RUN_ARTIFACTS
        ],
        "sha256sums_sha256": digest(sums_path),
    }
    write_json(Path(run_directory) / "MANIFEST.json", manifest, reseal=reseal)
    return hashes


def scan_for_bytes(paths, needle):
    """Return paths that contain the forbidden byte sequence."""
    if not needle:
        raise EvidenceError("secret_scan_empty_needle")
    matches = []
    for path in paths:
        try:
            if needle in Path(path).read_bytes():
                matches.append(str(path))
        except OSError as failure:
            raise EvidenceError("secret_scan_unreadable: " + str(path) + ": "
                                + type(failure).__name__) from None
    return matches


def read_hashes(run_directory):
    document = read_json(Path(run_directory) / "SHA256SUMS.json")
    if not isinstance(document, dict):
        raise EvidenceError("sha256sums_not_object")
    return document
