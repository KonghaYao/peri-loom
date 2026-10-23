#!/usr/bin/env python3
"""Streaming raw evidence, process-tree resource sampling, and manifests."""
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import threading
import time

from perf_contract import digest, ensure_empty_directory

PS_TIME = re.compile(r"^(?:(?P<days>\d+)-)?(?:(?P<hours>\d+):)?(?P<minutes>\d+):(?P<seconds>\d+(?:\.\d+)?)$")


class EvidenceError(ValueError):
    pass


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def write_json(path, payload, reseal=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if reseal else "x"
    with path.open(mode) as output:
        json.dump(payload, output, indent=2, sort_keys=True)
        output.write("\n")
    return path


def read_json(path):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as failure:
        raise EvidenceError("evidence_json_unreadable: " + str(path) + ": " + type(failure).__name__) from None


class RawRecordWriter:
    """Append-only JSONL writer with bounded buffering; no per-record fsync."""

    def __init__(self, path, flush_every=256):
        self.path = Path(path)
        self.flush_every = flush_every
        self.count = 0
        self.hasher = hashlib.sha256()
        self.output = self.path.open("xb", buffering=1024 * 1024)
        self.closed = False

    def write(self, record):
        line = (json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n").encode()
        self.output.write(line)
        self.hasher.update(line)
        self.count += 1
        if self.count % self.flush_every == 0:
            self.output.flush()

    def close(self):
        if self.closed:
            return
        self.output.flush()
        os.fsync(self.output.fileno())
        self.output.close()
        self.closed = True

    @property
    def sha256(self):
        return self.hasher.hexdigest()


def directory_size(path):
    path = Path(path)
    if not path.exists():
        return None
    total = 0
    for root, directories, files in os.walk(path):
        for name in files:
            candidate = Path(root) / name
            try:
                total += candidate.stat().st_size
            except OSError:
                return None
    return total


def parse_ps_time(value):
    match = PS_TIME.match(value.strip())
    if not match:
        raise EvidenceError("ps_time_unparseable: " + str(value))
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes"))
    seconds = float(match.group("seconds"))
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def parse_process_table(text):
    processes = []
    for line in text.splitlines():
        fields = line.split()
        if len(fields) != 4:
            continue
        try:
            pid, ppid, rss_kb = int(fields[0]), int(fields[1]), int(fields[2])
        except ValueError:
            raise EvidenceError("ps_row_unparseable: " + line) from None
        processes.append({"pid": pid, "ppid": ppid, "rss_bytes": rss_kb * 1024,
                          "cpu_seconds": parse_ps_time(fields[3])})
    return processes


def descendants(processes, roots):
    children, known = {}, {}
    for process in processes:
        known[process["pid"]] = process
        children.setdefault(process["ppid"], []).append(process["pid"])
    seen, pending = set(), [root for root in roots]
    missing = []
    while pending:
        pid = pending.pop()
        if pid in seen:
            continue
        if pid not in known:
            missing.append(pid)
            continue
        seen.add(pid)
        pending.extend(children.get(pid, []))
    return [known[pid] for pid in sorted(seen)], missing


def read_ps_table():
    try:
        result = subprocess.run(["ps", "-A", "-o", "pid=,ppid=,rss=,time="], capture_output=True,
                                text=True, timeout=2, check=False)
    except (OSError, subprocess.SubprocessError) as failure:
        raise EvidenceError("ps_unavailable: " + type(failure).__name__) from None
    if result.returncode != 0:
        raise EvidenceError("ps_failed: " + str(result.returncode))
    return parse_process_table(result.stdout)


class ResourceSampler:
    def __init__(self, roots, paths, interval=0.1, reader=None, clock=time.monotonic,
                 sleeper=time.sleep):
        self.roots = [int(root) for root in roots]
        self.paths = {name: Path(path) for name, path in paths.items()}
        self.interval = interval
        self.reader = reader or read_ps_table
        self.clock = clock
        self.sleeper = sleeper
        self.samples = []
        self.failures = []
        self.done = threading.Event()
        self.thread = None

    def sample_once(self):
        sample = {"monotonic_ns": time.monotonic_ns(), "roots": list(self.roots), "pids": [],
                  "rss_bytes": None, "cpu_seconds": None, "disk_bytes": {}}
        try:
            processes, missing = descendants(self.reader(), self.roots)
            sample["pids"] = [process["pid"] for process in processes]
            sample["missing_roots"] = missing
            if not missing and processes:
                sample["rss_bytes"] = sum(process["rss_bytes"] for process in processes)
                sample["cpu_seconds"] = sum(process["cpu_seconds"] for process in processes)
        except EvidenceError as failure:
            sample["failure"] = str(failure)
            self.failures.append(str(failure))
        for name, path in self.paths.items():
            sample["disk_bytes"][name] = directory_size(path)
        self.samples.append(sample)
        return sample

    def _run(self):
        while not self.done.wait(self.interval):
            self.sample_once()

    def start(self):
        self.sample_once()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self, timeout=5):
        self.done.set()
        if self.thread is not None:
            self.thread.join(timeout=timeout)
            if self.thread.is_alive():
                raise EvidenceError("resource_sampler_not_joined")
        if not self.samples:
            self.sample_once()
        return self.report()

    def report(self):
        measured = [sample for sample in self.samples if type(sample.get("rss_bytes")) is int]
        peak = max((sample["rss_bytes"] for sample in measured), default=None)
        window_size = max(1, math.ceil(len(self.samples) * 0.1))
        window = self.samples[-window_size:] if self.samples else []
        window_measured = all(type(sample.get("rss_bytes")) is int for sample in window)
        status = "MEASURED" if measured and window and window_measured and peak and peak > 0 else "UNMEASURED"
        return {"status": status, "sample_count": len(self.samples), "samples": self.samples,
                "peak_rss_bytes": peak,
                "last_window_rss_bytes": [sample["rss_bytes"] for sample in window] if window_measured else None,
                "last_window_samples": len(window),
                "cpu_seconds": max((sample["cpu_seconds"] for sample in measured), default=None),
                "failures": self.failures,
                "interval_seconds": self.interval}


def sanitize_process(usage):
    keys = ("pid", "lane", "exit_code", "group_joined", "group_live_remaining",
            "group_members_at_cleanup", "user_cpu_seconds", "system_cpu_seconds",
            "peak_rss_bytes", "cleanup_failure", "group_proof_failure")
    return {key: usage.get(key) for key in keys if key in usage}


def build_resources_report(sampler_report, processes, store_paths, cache_paths, disk_start, disk_end):
    sanitized = [sanitize_process(process) for process in processes]
    disk = {}
    for name in set(store_paths) | set(cache_paths):
        start, end = disk_start.get(name), disk_end.get(name)
        disk[name] = {"start_bytes": start, "end_bytes": end,
                      "growth_bytes": end - start if type(start) is int and type(end) is int else None}
    joined = bool(sanitized) and all(process.get("group_joined") is True for process in sanitized)
    measured = (sampler_report.get("status") == "MEASURED" and joined
                and all(value["growth_bytes"] is not None for value in disk.values()))
    return {"schema": 1, "status": "MEASURED" if measured else "UNMEASURED",
            "sampler": sampler_report, "processes": sanitized,
            "process_group_join_proof": joined,
            "all_processes_accounted": joined and sampler_report.get("status") == "MEASURED",
            "disk": disk}


class EvidenceRun:
    def __init__(self, directory, metadata, flush_every=256):
        self.directory = ensure_empty_directory(directory)
        self.metadata = metadata
        write_json(self.directory / "environment.json", metadata)
        self.operations = RawRecordWriter(self.directory / "operations.jsonl", flush_every)
        self.metrics = []

    def write_json(self, name, payload, reseal=False):
        return write_json(self.directory / name, payload, reseal=reseal)

    def copy_metrics_snapshot(self, source, lane):
        source = Path(source)
        record = {"lane": lane, "source": str(source), "copied": False}
        if source.is_file():
            destination = self.directory / ("metrics-snapshot-lane" + str(lane) + ".json")
            shutil.copyfile(source, destination)
            record.update(copied=True, file=destination.name, sha256=digest(destination),
                          bytes=destination.stat().st_size)
        self.metrics.append(record)
        return record

    def finish(self, required=("environment.json", "operations.jsonl", "oracle.json",
                               "resources.json", "summary.json")):
        self.operations.close()
        if self.metrics:
            write_json(self.directory / "metrics-snapshot-index.json",
                       {"schema": 1, "requested": True, "snapshots": self.metrics})
        return write_manifest(self.directory, required=required)


def write_manifest(directory, required=(), reseal=False):
    directory = Path(directory)
    files = {}
    for path in sorted(directory.rglob("*")):
        if not path.is_file() or path.name == "manifest.json":
            continue
        relative = path.relative_to(directory).as_posix()
        files[relative] = {"sha256": digest(path), "bytes": path.stat().st_size}
    manifest = {"schema": 1, "files": files, "required": list(required)}
    write_json(directory / "manifest.json", manifest, reseal=reseal)
    return manifest


def verify_manifest(directory):
    directory = Path(directory).resolve()
    manifest = read_json(directory / "manifest.json")
    if manifest.get("schema") != 1 or not isinstance(manifest.get("files"), dict):
        raise EvidenceError("manifest_schema")
    listed = manifest["files"]
    missing = [name for name in manifest.get("required", []) if name not in listed]
    for path in directory.rglob("*"):
        if not path.is_file() or path.name == "manifest.json":
            continue
        relative = path.relative_to(directory).as_posix()
        if relative not in listed:
            missing.append("unlisted:" + relative)
            continue
        entry = listed[relative]
        if entry.get("bytes") != path.stat().st_size or entry.get("sha256") != digest(path):
            missing.append("tampered:" + relative)
    for name in listed:
        if name.startswith("/") or ".." in Path(name).parts:
            missing.append("unsafe:" + name)
        elif not (directory / name).is_file():
            missing.append("missing:" + name)
    if missing:
        raise EvidenceError("manifest_mismatch: " + ",".join(sorted(missing)))
    return {"directory": str(directory), "files": listed, "required": manifest.get("required", [])}


def freeze_artifact(source, directory, name="native"):
    source = Path(source)
    expected = digest(source)
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / (expected[:16] + "-" + name)
    if target.exists():
        if digest(target) != expected:
            raise EvidenceError("frozen_artifact_mismatch: " + str(target))
    else:
        shutil.copy2(source, target)
        target.chmod(0o500)
        if digest(target) != expected:
            raise EvidenceError("frozen_artifact_copy_mismatch")
    return {"path": str(target), "sha256": expected, "origin": str(source.resolve())}
