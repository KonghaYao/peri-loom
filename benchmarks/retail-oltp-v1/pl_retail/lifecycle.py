"""Normal-shutdown and same-volume restart orchestration.

The benchmark is a client; it cannot invent a clean shutdown.  The operator
supplies the stop/start commands and the path of the clean proof.  Missing
orchestration is reported as not configured, never as passed.
"""
from __future__ import annotations

import json
import ssl
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any

READY_BUDGET_SECONDS = 30.0
STOP_BUDGET_SECONDS = 30.0


@dataclass
class LifecycleConfig:
    stop: list[str] | None = None
    start: list[str] | None = None
    ready_path: str = "/v1/platform/capabilities"
    clean_proof_path: str | None = None
    clean_proof_contains: str | None = None
    endpoint: str = ""
    api_key: str = ""
    ca_file: str | None = None

    @classmethod
    def load(cls, path: str, *, endpoint: str, api_key: str, ca_file: str | None) -> "LifecycleConfig":
        with open(path, encoding="utf-8") as handle:
            document = json.load(handle)
        return cls(
            stop=list(document["stop"]) if document.get("stop") else None,
            start=list(document["start"]) if document.get("start") else None,
            ready_path=str(document.get("ready_path") or "/v1/platform/capabilities"),
            clean_proof_path=document.get("clean_proof_path"),
            clean_proof_contains=document.get("clean_proof_contains"),
            endpoint=endpoint,
            api_key=api_key,
            ca_file=ca_file,
        )


@dataclass
class LifecycleResult:
    configured: bool
    ok: bool = False
    status: str = "not_configured"
    duration_ms: float = 0.0
    exit_code: int | None = None
    stdout_tail: str = ""
    stderr_tail: str = ""
    clean_proof: dict[str, Any] = field(default_factory=dict)
    ready: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "configured": self.configured,
            "ok": self.ok,
            "status": self.status,
            "duration_ms": self.duration_ms,
            "exit_code": self.exit_code,
            "stdout_tail": self.stdout_tail,
            "stderr_tail": self.stderr_tail,
            "clean_proof": self.clean_proof,
            "ready": self.ready,
        }


def _run(command: list[str], budget: float) -> dict[str, Any]:
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=budget,
            check=False,
        )
    except subprocess.TimeoutExpired as timeout:
        return {
            "ok": False,
            "status": "timeout",
            "duration_ms": round((time.monotonic() - started) * 1000, 3),
            "exit_code": None,
            "stdout_tail": (timeout.stdout or "")[-2000:] if isinstance(timeout.stdout, str) else "",
            "stderr_tail": (timeout.stderr or "")[-2000:] if isinstance(timeout.stderr, str) else "",
        }
    except OSError as error:
        return {
            "ok": False,
            "status": "spawn_failed",
            "duration_ms": round((time.monotonic() - started) * 1000, 3),
            "exit_code": None,
            "stdout_tail": "",
            "stderr_tail": "%s: %s" % (type(error).__name__, error),
        }
    return {
        "ok": completed.returncode == 0,
        "status": "ok" if completed.returncode == 0 else "exit_nonzero",
        "duration_ms": round((time.monotonic() - started) * 1000, 3),
        "exit_code": completed.returncode,
        "stdout_tail": completed.stdout[-2000:],
        "stderr_tail": completed.stderr[-2000:],
    }


def probe_ready(config: LifecycleConfig, budget: float = READY_BUDGET_SECONDS) -> dict[str, Any]:
    context = ssl.create_default_context(cafile=config.ca_file) if config.ca_file else None
    started = time.monotonic()
    last: dict[str, Any] = {"status": "never_probed"}
    while time.monotonic() - started < budget:
        request = urllib.request.Request(config.endpoint.rstrip("/") + config.ready_path)
        if config.api_key:
            request.add_header("Authorization", "Bearer %s" % config.api_key)
        try:
            with urllib.request.urlopen(request, timeout=5.0, context=context) as response:
                if 200 <= response.status < 300:
                    return {
                        "status": "ready",
                        "seconds": round(time.monotonic() - started, 3),
                        "http_status": response.status,
                    }
                last = {"status": "http_error", "http_status": response.status}
        except Exception as error:  # noqa: BLE001 - the probe loop records the last reason
            last = {"status": "unavailable", "reason": "%s: %s" % (type(error).__name__, error)}
        time.sleep(0.25)
    return {
        "status": "not_ready",
        "seconds": round(time.monotonic() - started, 3),
        "last": last,
    }


def shutdown(config: LifecycleConfig) -> LifecycleResult:
    if not config.stop:
        return LifecycleResult(configured=False)
    result = LifecycleResult(configured=True)
    outcome = _run(config.stop, STOP_BUDGET_SECONDS)
    result.status = outcome["status"]
    result.duration_ms = outcome["duration_ms"]
    result.exit_code = outcome["exit_code"]
    result.stdout_tail = outcome["stdout_tail"]
    result.stderr_tail = outcome["stderr_tail"]
    result.ok = bool(outcome["ok"])
    if config.clean_proof_path:
        try:
            with open(config.clean_proof_path, encoding="utf-8", errors="replace") as handle:
                content = handle.read(1 << 20)
            result.clean_proof = {
                "path": config.clean_proof_path,
                "present": True,
                "bytes": len(content.encode("utf-8")),
                "contains": config.clean_proof_contains,
                "matched": (
                    config.clean_proof_contains is None
                    or config.clean_proof_contains in content
                ),
                "content_tail": content[-2000:],
            }
        except OSError as error:
            result.clean_proof = {
                "path": config.clean_proof_path,
                "present": False,
                "reason": "%s: %s" % (type(error).__name__, error),
            }
    else:
        result.clean_proof = {"present": False, "reason": "no clean proof path configured"}
    return result


def restart(config: LifecycleConfig) -> LifecycleResult:
    if not config.start:
        return LifecycleResult(configured=False)
    result = LifecycleResult(configured=True)
    outcome = _run(config.start, READY_BUDGET_SECONDS)
    result.status = outcome["status"]
    result.duration_ms = outcome["duration_ms"]
    result.exit_code = outcome["exit_code"]
    result.stdout_tail = outcome["stdout_tail"]
    result.stderr_tail = outcome["stderr_tail"]
    if not outcome["ok"]:
        return result
    probe = probe_ready(config)
    result.ready = probe
    result.ok = probe["status"] == "ready"
    result.status = "ready" if result.ok else "not_ready"
    return result
