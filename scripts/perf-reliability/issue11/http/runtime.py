#!/usr/bin/env python3
"""W0-A 环境记录、凭据边界和执行结果类型。"""
import json
import os
from pathlib import Path
import platform
import resource
import sys
import time
import uuid

PERF_ROOT = Path(__file__).resolve().parents[2]
if str(PERF_ROOT) not in sys.path:
    sys.path.insert(0, str(PERF_ROOT))

from issue11 import contract, environment as environment_module  # noqa: E402
from issue11.http import client as http_client  # noqa: E402
from issue11.http import protocol  # noqa: E402

RUNNER_VERSION = "issue11-w0a-runner-1"
ZERO_DIGEST = "sha256:" + ("0" * 64)
ZERO_SHA256 = "0" * 64


class RunnerError(RuntimeError):
    pass


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def unix_ms():
    return int(time.time() * 1000)


def monotonic_ns():
    return time.monotonic_ns()


def normalize_arch(machine):
    machine = machine.lower()
    if machine in {"x86_64", "amd64"}:
        return "amd64"
    if machine in {"aarch64", "arm64"}:
        return "arm64"
    return machine


def rss_bytes():
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


def cpu_seconds():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime + usage.ru_stime


def _digest_or_zero(path):
    try:
        return contract.digest(path)
    except OSError:
        return ZERO_SHA256


def _read_credential(arguments):
    if arguments.credential_env and arguments.credential_file:
        raise RunnerError("choose_one_credential_source")
    if arguments.credential_env:
        value = os.environ.get(arguments.credential_env, "")
    elif arguments.credential_file:
        try:
            value = Path(arguments.credential_file).read_text().strip()
        except OSError as failure:
            raise RunnerError("credential_file_unreadable: "
                              + type(failure).__name__) from None
    else:
        value = os.environ.get("PERI_SQL_TOKEN", "")
    if not value or "\n" in value or "\r" in value:
        raise RunnerError("credential_missing_or_invalid")
    return value


def _database_argument(value):
    try:
        protocol.pipeline_path(value)
    except protocol.ProtocolError as failure:
        raise RunnerError(str(failure)) from None
    return value


def _run_id(arguments):
    suffix = uuid.uuid4().hex[:8]
    return "%s-%s-p%d-%s-%s-%s" % (
        arguments.scenario, arguments.arm_label, arguments.pair_index,
        arguments.state, arguments.profile, suffix)


def _diagnostics(arguments):
    values = {
        "PERI_WORK_METRICS": False,
        "PERI_DIAG_GROUP_PHASE": False,
        "PERI_ADMISSION_TRACE": False,
        "PERI_MAINTENANCE_SCHEDULE": False,
        "sql_open_observer": False,
        "PERI_SQL_OPEN_OBSERVER": False,
        "PERI_SQL_OPEN_OBSERVER_PHASE_THRESHOLD_MS": 20,
        "PERI_SQL_OPEN_OBSERVER_PHASE_SAMPLE_PERMILLE": 1000,
    }
    for item in arguments.diagnostic_switch:
        if "=" not in item:
            raise RunnerError("diagnostic_switch_must_be_name=0|1")
        name, raw = item.split("=", 1)
        if name not in values:
            raise RunnerError("unknown_diagnostic_switch: " + name)
        if type(values[name]) is int:
            try:
                values[name] = int(raw)
            except ValueError:
                raise RunnerError("diagnostic_switch_integer: " + name) from None
            expected = 20 if name.endswith("THRESHOLD_MS") else 1000
            if values[name] != expected:
                raise RunnerError("diagnostic_switch_value: " + name)
        else:
            if raw not in {"0", "1"}:
                raise RunnerError("diagnostic_switch_value: " + name)
            values[name] = raw == "1"
    return values


def _template(arguments):
    if not arguments.environment_template:
        return {}
    try:
        document = json.loads(Path(arguments.environment_template).read_text())
    except (OSError, json.JSONDecodeError) as failure:
        raise RunnerError("environment_template_unreadable: "
                          + type(failure).__name__) from None
    if not isinstance(document, dict):
        raise RunnerError("environment_template_not_object")
    return document


def build_environment(arguments, client, run_id, contract_sha, source,
                      diagnostics):
    template = _template(arguments)
    head = contract.source_identity()
    if template:
        source_document = template.get("source", source)
        image = template.get("image")
        runtime = template.get("runtime")
        host = template.get("host", environment_module.host_identity())
        limits = template.get("limits")
        storage = template.get("storage")
        backend = template.get("backend")
        encryption = template.get("encryption")
        quotas = template.get("quotas")
        mounts = template.get("mounts")
        evidence_class = template.get("evidence_class", "diagnostic")
        if not all(isinstance(value, dict) for value in (
                source_document, image, runtime, limits, storage, backend,
                encryption, quotas)):
            raise RunnerError("environment_template_incomplete")
        if not isinstance(mounts, list) or not mounts:
            raise RunnerError("environment_template_mounts")
    else:
        arch = normalize_arch(platform.machine())
        evidence_class = "diagnostic"
        source_document = dict(source)
        source_document["sdk"] = {
            "package": "unattested-diagnostic",
            "version": "unattested-diagnostic",
            "lock_sha256": ZERO_SHA256,
        }
        image = {
            "digest": ZERO_DIGEST,
            "tag": "unattested-diagnostic",
            "os": sys.platform,
            "arch": arch,
            "arch_native": True,
            "built_from_commit": source["commit"] or "0" * 40,
        }
        runtime = {
            "python": platform.python_version(),
            "gateway_binary_sha256": ZERO_SHA256,
            "worker_binary_sha256": ZERO_SHA256,
            "worker_arch": arch,
            "gateway_arch": arch,
        }
        host = environment_module.host_identity()
        limits = {
            "cpu_cores": arguments.cpu_cores or float(os.cpu_count() or 1),
            "memory_bytes": arguments.memory_bytes,
            "pids": 512,
        }
        storage = {
            "root_type": "apfs-diagnostic" if sys.platform == "darwin" else "unknown-diagnostic",
            "persistent": False,
            "durable": False,
            "retention": "ephemeral-diagnostic",
            "store_root": arguments.store_root or str(Path(arguments.evidence_root) / "store"),
            "cache_root": arguments.cache_root or str(Path(arguments.evidence_root) / "cache"),
            "evidence_root": str(Path(arguments.evidence_root).resolve()),
            "metrics_snapshot": str(Path(arguments.evidence_root).resolve() / "metrics"),
        }
        backend = {
            "kind": "local",
            "durability": "unattested-diagnostic",
            "root": str(Path(arguments.evidence_root).resolve()),
        }
        encryption = {"at_rest": False, "key_broker": False}
        quotas = {
            "max_result_bytes": 8 * 1024 * 1024,
            "max_streams": 256,
            "max_pipeline_steps": 64,
        }
        mounts = [{
            "path": str(Path(arguments.evidence_root).resolve()),
            "type": "host-directory-diagnostic",
            "readonly": False,
        }]
    if not source_document.get("commit"):
        source_document["commit"] = contract.load_acceptance()["baseline_commit"]
    if not source_document.get("turso_gitlink"):
        source_document["turso_gitlink"] = contract.load_workloads()["turso_commit"]
    if "dirty" not in source_document:
        source_document["dirty"] = False
    if client.tls:
        if not arguments.ca_file:
            raise RunnerError("ca_file_required_for_verified_tls")
        ca_digest = contract.digest(arguments.ca_file)
    else:
        ca_digest = ZERO_SHA256
    tls = {
        "enabled": client.tls,
        "verified": client.tls,
        "ca_sha256": ca_digest,
    }
    document = {
        "schema": "issue11-environment",
        "schema_version": 1,
        "evidence_class": evidence_class,
        "source": source_document,
        "image": image,
        "runtime": runtime,
        "host": host,
        "limits": limits,
        "storage": storage,
        "backend": backend,
        "encryption": encryption,
        "tls": tls,
        "quotas": quotas,
        "mounts": mounts,
        "diagnostics": diagnostics,
        "recording": {
            "started_at_unix_ms": unix_ms(),
            "runner_sha256": _digest_or_zero(Path(__file__)),
            "contract_sha256": contract_sha,
            "endpoint": arguments.endpoint,
            "authorization": http_client.REDACTED_AUTHORIZATION,
        },
        "synthetic_test_fixture": bool(arguments.synthetic_test_fixture
                                       or template.get("synthetic_test_fixture")),
        "diagnostic_processes": template.get("diagnostic_processes", {}),
        "notes": [
            "W0-A emits client spans only; product-side spans and periodic events are W0-B.",
            "Diagnostic environments can never produce a formal PASS.",
        ],
    }
    if head.get("dirty"):
        document["notes"].append("source checkout reported tracked modifications")
    return document


# ---------------------------------------------------------------------------
# Operations


class Attempt:
    def __init__(self, request_id, correlation_id, exchange, reply, failure,
                 encoded, decoded, spans, total_ns, step_ids):
        self.request_id = request_id
        self.correlation_id = correlation_id
        self.exchange = exchange
        self.reply = reply
        self.failure = failure
        self.encoded = encoded
        self.decoded = decoded
        self.spans = spans
        self.total_ns = total_ns
        self.started_ns = spans[0]["started_monotonic_ns"]
        self.completed_ns = self.started_ns + total_ns
        self.step_ids = list(step_ids)
        self.expected = False

    @property
    def ok(self):
        if self.expected and isinstance(self.failure, protocol.Rejected):
            return True
        return self.failure is None and self.reply is not None and self.reply.ok

    @property
    def outcome(self):
        if self.failure is not None:
            return ("rejected" if isinstance(self.failure, protocol.Rejected)
                    else "outcome_unknown")
        if self.reply is None or not self.reply.ok:
            return "error"
        return "success"

    @property
    def error_code(self):
        if self.failure is not None:
            return self.failure.code
        if self.reply is not None:
            for result in self.reply.results:
                if not result.ok:
                    return result.error_code
        return ""


class OperationResult:
    def __init__(self, attempts, verification, may_have_committed=False,
                 error_code=""):
        self.attempts = attempts
        self.verification = verification
        self.may_have_committed = may_have_committed
        self.error_code = error_code

    @property
    def success(self):
        return (all(attempt.ok for attempt in self.attempts)
                and self.verification["ok"])

    @property
    def outcome(self):
        relevant = [attempt for attempt in self.attempts if not attempt.expected]
        if any(attempt.outcome == "outcome_unknown" for attempt in relevant):
            return "outcome_unknown"
        if any(attempt.outcome == "rejected" for attempt in relevant):
            return "rejected"
        if any(attempt.outcome == "error" for attempt in relevant):
            return "error"
        if not self.verification["ok"]:
            return "error"
        return "success"
