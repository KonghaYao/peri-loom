"""Append-only evidence writer with a SHA256 manifest.

Every file produced by a run is registered with its byte count, record count
and digest.  The scorer refuses evidence whose manifest entry does not match
the bytes on disk, so a truncated or hand-edited log cannot pass.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


@dataclass
class RecordedFile:
    relative_path: str
    kind: str
    bytes: int = 0
    records: int = 0
    sha256: str = ""
    complete: bool = True

    def manifest_entry(self) -> dict[str, Any]:
        return {
            "path": self.relative_path,
            "kind": self.kind,
            "bytes": self.bytes,
            "records": self.records,
            "sha256": self.sha256,
            "complete": self.complete,
        }


class EvidenceWriter:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._files: dict[str, RecordedFile] = {}
        self._handles: dict[str, Any] = {}
        self._lock = threading.Lock()
        self._closed = False

    # -- paths -------------------------------------------------------------

    def path(self, relative: str) -> Path:
        target = self.root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        return target

    def _register(self, relative: str, kind: str) -> RecordedFile:
        if relative in self._files:
            return self._files[relative]
        entry = RecordedFile(relative_path=relative, kind=kind)
        self._files[relative] = entry
        return entry

    # -- JSON documents ----------------------------------------------------

    def write_json(self, relative: str, document: Any, kind: str = "json") -> dict[str, Any]:
        raw = self._write_json_document(relative, document)
        with self._lock:
            entry = self._register(relative, kind)
            entry.bytes = len(raw)
            entry.records = _count_records(document)
            entry.sha256 = sha256_bytes(raw)
            entry.complete = True
        return self._files[relative].manifest_entry()

    def _write_json_document(self, relative: str, document: Any) -> bytes:
        """Atomically write a JSON document without registering it in the manifest."""
        data = json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        raw = data.encode("utf-8")
        target = self.path(relative)
        fd, temporary = tempfile.mkstemp(dir=str(target.parent), prefix=".evidence-")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        _fsync_directory(target.parent)
        return raw

    # -- JSONL -------------------------------------------------------------

    def append(self, relative: str, record: Any, kind: str = "jsonl") -> None:
        line = json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
        raw = line.encode("utf-8")
        with self._lock:
            if relative not in self._handles:
                handle = open(self.path(relative), "ab", buffering=0)
                self._handles[relative] = handle
                entry = self._register(relative, kind)
                # A finalized file that is appended to again must be re-hashed
                # on the next finalize; otherwise the manifest would keep the
                # digest of a prefix.
                entry.sha256 = ""
            handle = self._handles[relative]
            handle.write(raw)
            entry = self._files[relative]
            entry.bytes += len(raw)
            entry.records += 1

    # -- raw files ---------------------------------------------------------

    def copy_file(self, source: str | Path, relative: str, kind: str = "text") -> dict[str, Any]:
        raw = Path(source).read_bytes()
        target = self.path(relative)
        target.write_bytes(raw)
        with self._lock:
            entry = self._register(relative, kind)
            entry.bytes = len(raw)
            entry.records = raw.count(b"\n")
            entry.sha256 = sha256_bytes(raw)
        return self._files[relative].manifest_entry()

    def register_external(self, relative: str, kind: str, bytes_count: int, records: int,
                          digest: str) -> None:
        with self._lock:
            entry = self._register(relative, kind)
            entry.bytes = bytes_count
            entry.records = records
            entry.sha256 = digest

    # -- finalisation ------------------------------------------------------

    def flush(self) -> None:
        with self._lock:
            # The manifest cannot contain its own digest, and re-finalizing must
            # not register it as an ordinary evidence file.
            self._files.pop("evidence-manifest.json", None)
            for handle in self._handles.values():
                handle.flush()
                try:
                    os.fsync(handle.fileno())
                except OSError:
                    pass

    def finalize(self) -> dict[str, Any]:
        self.flush()
        with self._lock:
            # The manifest cannot contain its own digest, and re-finalizing must
            # not register it as an ordinary evidence file.
            self._files.pop("evidence-manifest.json", None)
            for handle in self._handles.values():
                handle.close()
            self._handles.clear()
            for relative, entry in self._files.items():
                if entry.kind == "jsonl" and not entry.sha256:
                    data = (self.root / relative).read_bytes()
                    entry.sha256 = sha256_bytes(data)
            manifest = {
                "contract": "PL-RETAIL-OLTP",
                "version": "1.0.0",
                "generated_ns": time.monotonic_ns(),
                "files": [
                    entry.manifest_entry()
                    for entry in sorted(self._files.values(), key=lambda item: item.relative_path)
                ],
            }
            self._closed = True
        self._write_json_document("evidence-manifest.json", manifest)
        return manifest

    def manifest_entries(self) -> list[dict[str, Any]]:
        with self._lock:
            return [entry.manifest_entry() for entry in self._files.values()]


def _count_records(document: Any) -> int:
    if isinstance(document, list):
        return len(document)
    return 1


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


class Clock:
    """Wall and monotonic timestamps recorded together."""

    def __init__(self) -> None:
        self.origin_ns = time.monotonic_ns()
        self.wall_origin_ns = time.time_ns()

    def now(self) -> dict[str, int]:
        return {
            "monotonic_ns": time.monotonic_ns(),
            "wall_ns": time.time_ns(),
        }

    def elapsed_ns(self) -> int:
        return time.monotonic_ns() - self.origin_ns
