#!/usr/bin/env python3
"""Issue 11 environment validation and formal/diagnostic classification."""
import json
import os
from pathlib import Path
import platform
import re
import sys

PERF_ROOT = Path(__file__).resolve().parent.parent
if str(PERF_ROOT) not in sys.path:
    sys.path.insert(0, str(PERF_ROOT))

HEX40 = re.compile(r"^[0-9a-f]{40}$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
ARCHES = {"amd64", "x86_64", "arm64", "aarch64"}
REJECT_ROOTS = {"overlay", "overlay2", "tmpfs", "ramfs", "devtmpfs"}
FORMAL_ROOTS = {"ext4", "xfs"}
DIAGNOSTIC_MARKERS = ("orbstack", "docker-desktop", "colima")
SECRET_PATHS = ("/tmp", "/var/tmp", "/dev/shm")


class EnvironmentError(ValueError):
    pass


def load(path):
    try:
        document = json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as failure:
        raise EnvironmentError("environment_unreadable: " + str(path) + ": "
                               + type(failure).__name__) from None
    if not isinstance(document, dict):
        raise EnvironmentError("environment_not_object")
    return document


def _require(document, key, kind, problems, prefix=""):
    value = document.get(key)
    if not isinstance(value, kind) or isinstance(value, bool) and kind is int:
        problems.append("environment_type: " + prefix + key)
        return None
    return value


def _require_object(document, key, required, problems, prefix=""):
    value = _require(document, key, dict, problems, prefix)
    if value is None:
        return None
    for name in required:
        if name not in value:
            problems.append("environment_missing: " + prefix + key + "." + name)
    return value


def _inside_secret_path(value):
    if not isinstance(value, str):
        return False
    normalized = os.path.normpath(value)
    return any(normalized == root or normalized.startswith(root + "/")
               for root in SECRET_PATHS)


def _formal_markers(document):
    markers = []
    host = document.get("host") if isinstance(document.get("host"), dict) else {}
    virtualization = str(host.get("virtualization", "")).lower()
    if any(marker in virtualization for marker in DIAGNOSTIC_MARKERS):
        markers.append("diagnostic_virtualization: " + virtualization)
    storage = document.get("storage") if isinstance(document.get("storage"), dict) else {}
    root_type = str(storage.get("root_type", "")).lower()
    if root_type in REJECT_ROOTS:
        markers.append("diagnostic_root_type: " + root_type)
    for key in ("store_root", "cache_root", "evidence_root", "metrics_snapshot"):
        if _inside_secret_path(storage.get(key)):
            markers.append("temporary_path: storage." + key)
    image = document.get("image") if isinstance(document.get("image"), dict) else {}
    if image.get("arch_native") is False:
        markers.append("emulated_or_non_native_image")
    if document.get("evidence_class") != "formal":
        markers.append("evidence_class: " + str(document.get("evidence_class")))
    return markers


def validate(document, workloads):
    """Return structural problems and the formal/diagnostic classification."""
    problems = []
    if document.get("schema") != "issue11-environment":
        problems.append("environment_schema")
    if document.get("schema_version") != 1:
        problems.append("environment_schema_version")
    evidence_class = document.get("evidence_class")
    if evidence_class not in {"formal", "diagnostic", "test"}:
        problems.append("environment_evidence_class")

    source = _require_object(document, "source", (
        "commit", "branch", "dirty", "turso_gitlink", "sdk"), problems)
    if source:
        if not HEX40.match(str(source.get("commit", ""))):
            problems.append("environment_source_commit")
        if not isinstance(source.get("dirty"), bool):
            problems.append("environment_source_dirty")
        if not HEX40.match(str(source.get("turso_gitlink", ""))):
            problems.append("environment_turso_gitlink")
        elif source.get("turso_gitlink") != workloads.get("turso_commit"):
            problems.append("environment_turso_mismatch")
        sdk = _require_object(source, "sdk", ("package", "version", "lock_sha256"),
                              problems, "source.")
        if sdk:
            if not isinstance(sdk.get("package"), str) or not sdk["package"]:
                problems.append("environment_sdk_package")
            if not isinstance(sdk.get("version"), str) or not sdk["version"]:
                problems.append("environment_sdk_version")
            if not HEX64.match(str(sdk.get("lock_sha256", ""))):
                problems.append("environment_sdk_lock")
        if source.get("commit") != workloads.get("baseline_commit"):
            # A different source commit is allowed only for an explicitly
            # diagnostic environment, never for a formal claim.
            if evidence_class == "formal":
                problems.append("environment_source_not_baseline")

    image = _require_object(document, "image", (
        "digest", "tag", "os", "arch", "arch_native", "built_from_commit"), problems)
    if image:
        if not DIGEST.match(str(image.get("digest", ""))):
            problems.append("environment_image_digest")
        if image.get("arch") not in ARCHES:
            problems.append("environment_image_arch")
        if not isinstance(image.get("arch_native"), bool):
            problems.append("environment_image_arch_native")
        if not HEX40.match(str(image.get("built_from_commit", ""))):
            problems.append("environment_image_commit")
        if source and image.get("built_from_commit") != source.get("commit"):
            problems.append("environment_image_commit_mismatch")

    runtime = _require_object(document, "runtime", (
        "python", "gateway_binary_sha256", "worker_binary_sha256",
        "worker_arch", "gateway_arch"), problems)
    if runtime:
        for key in ("gateway_binary_sha256", "worker_binary_sha256"):
            if not HEX64.match(str(runtime.get(key, ""))):
                problems.append("environment_runtime_" + key)
        if runtime.get("worker_arch") not in ARCHES \
                or runtime.get("gateway_arch") not in ARCHES:
            problems.append("environment_runtime_arch")
        if image and runtime.get("worker_arch") != image.get("arch"):
            problems.append("environment_worker_image_arch_mismatch")

    host = _require_object(document, "host", (
        "os", "arch", "kernel", "machine", "virtualization"), problems)
    if host and host.get("arch") not in ARCHES:
        problems.append("environment_host_arch")

    limits = _require_object(document, "limits", ("cpu_cores", "memory_bytes", "pids"),
                             problems)
    if limits:
        if not isinstance(limits.get("cpu_cores"), (int, float)) \
                or limits.get("cpu_cores") <= 0:
            problems.append("environment_cpu_cores")
        for key in ("memory_bytes", "pids"):
            if not isinstance(limits.get(key), int) or limits.get(key) <= 0:
                problems.append("environment_" + key)

    storage = _require_object(document, "storage", (
        "root_type", "persistent", "durable", "retention", "store_root",
        "cache_root", "evidence_root", "metrics_snapshot"), problems)
    if storage:
        if not isinstance(storage.get("persistent"), bool) \
                or not isinstance(storage.get("durable"), bool):
            problems.append("environment_storage_booleans")
        if not isinstance(storage.get("retention"), str) or not storage["retention"]:
            problems.append("environment_storage_retention")
        for key in ("store_root", "cache_root", "evidence_root", "metrics_snapshot"):
            if not isinstance(storage.get(key), str) or not storage[key]:
                problems.append("environment_storage_" + key)

    backend = _require_object(document, "backend", ("kind", "durability", "root"),
                              problems)
    if backend:
        if backend.get("kind") not in {"local", "s3"}:
            problems.append("environment_backend_kind")
        if not isinstance(backend.get("durability"), str) or not backend["durability"]:
            problems.append("environment_backend_durability")
        if not isinstance(backend.get("root"), str) or not backend["root"]:
            problems.append("environment_backend_root")

    encryption = _require_object(document, "encryption", ("at_rest", "key_broker"),
                                 problems)
    if encryption:
        for key in ("at_rest", "key_broker"):
            if not isinstance(encryption.get(key), bool):
                problems.append("environment_encryption_" + key)

    tls = _require_object(document, "tls", ("enabled", "verified", "ca_sha256"),
                          problems)
    if tls:
        for key in ("enabled", "verified"):
            if not isinstance(tls.get(key), bool):
                problems.append("environment_tls_" + key)
        if not HEX64.match(str(tls.get("ca_sha256", ""))):
            problems.append("environment_tls_ca")

    quotas = _require_object(document, "quotas", (
        "max_result_bytes", "max_streams", "max_pipeline_steps"), problems)
    if quotas:
        for key in ("max_result_bytes", "max_streams", "max_pipeline_steps"):
            if not isinstance(quotas.get(key), int) or quotas[key] <= 0:
                problems.append("environment_quota_" + key)

    mounts = _require(document, "mounts", list, problems)
    if mounts is not None:
        if not mounts:
            problems.append("environment_mounts_empty")
        for index, mount in enumerate(mounts):
            if not isinstance(mount, dict):
                problems.append("environment_mount_type: " + str(index))
                continue
            if not isinstance(mount.get("path"), str) or not mount["path"]:
                problems.append("environment_mount_path: " + str(index))
            if not isinstance(mount.get("type"), str) or not mount["type"]:
                problems.append("environment_mount_type_name: " + str(index))
            if not isinstance(mount.get("readonly"), bool):
                problems.append("environment_mount_readonly: " + str(index))

    diagnostics = _require_object(document, "diagnostics", (
        "PERI_WORK_METRICS", "PERI_DIAG_GROUP_PHASE", "PERI_ADMISSION_TRACE",
        "PERI_MAINTENANCE_SCHEDULE", "sql_open_observer", "PERI_SQL_OPEN_OBSERVER",
        "PERI_SQL_OPEN_OBSERVER_PHASE_THRESHOLD_MS",
        "PERI_SQL_OPEN_OBSERVER_PHASE_SAMPLE_PERMILLE"), problems)
    if diagnostics:
        for key in diagnostics:
            if key in {"PERI_SQL_OPEN_OBSERVER_PHASE_THRESHOLD_MS",
                       "PERI_SQL_OPEN_OBSERVER_PHASE_SAMPLE_PERMILLE"}:
                value = diagnostics[key]
                if type(value) is not int or value < 0 \
                        or key.endswith("SAMPLE_PERMILLE") and value > 1000:
                    problems.append("environment_diagnostic_integer: " + key)
            elif key not in {"PERI_WORK_METRICS", "PERI_DIAG_GROUP_PHASE",
                             "PERI_ADMISSION_TRACE", "PERI_MAINTENANCE_SCHEDULE",
                             "sql_open_observer", "PERI_SQL_OPEN_OBSERVER"}:
                problems.append("environment_diagnostic_unknown: " + key)
            elif not isinstance(diagnostics[key], bool):
                problems.append("environment_diagnostic_flag: " + key)
        if diagnostics.get("sql_open_observer") or diagnostics.get("PERI_SQL_OPEN_OBSERVER"):
            if diagnostics.get("PERI_SQL_OPEN_OBSERVER_PHASE_THRESHOLD_MS") != 20 \
                    or diagnostics.get("PERI_SQL_OPEN_OBSERVER_PHASE_SAMPLE_PERMILLE") != 1000:
                problems.append("environment_diagnostic_observer_values")

    if "synthetic_test_fixture" in document \
            and not isinstance(document["synthetic_test_fixture"], bool):
        problems.append("environment_synthetic_flag")
    process_coverage = document.get("diagnostic_processes", {})
    if not isinstance(process_coverage, dict):
        problems.append("environment_diagnostic_processes")
    else:
        for name, observed in process_coverage.items():
            if name not in {"worker", "authority"} or not isinstance(observed, dict):
                problems.append("environment_diagnostic_process: " + str(name))
                continue
            pid = observed.get("pid")
            if type(pid) is not int or pid <= 0 \
                    or not isinstance(observed.get("observed_switches"), dict):
                problems.append("environment_diagnostic_process: " + name)

    recording = _require_object(document, "recording", (
        "started_at_unix_ms", "runner_sha256", "contract_sha256", "endpoint",
        "authorization"), problems)
    if recording:
        if not isinstance(recording.get("started_at_unix_ms"), int) \
                or recording["started_at_unix_ms"] <= 0:
            problems.append("environment_started_at")
        for key in ("runner_sha256", "contract_sha256"):
            if not HEX64.match(str(recording.get(key, ""))):
                problems.append("environment_" + key)
        if not isinstance(recording.get("endpoint"), str) or not recording["endpoint"]:
            problems.append("environment_endpoint")
        if recording.get("authorization") != "Bearer <REDACTED>":
            problems.append("environment_authorization_not_redacted")

    markers = _formal_markers(document)
    formal_eligible = not problems and not markers and evidence_class == "formal"
    if evidence_class == "formal":
        if markers:
            problems.append("formal_environment_claim_rejected: " + ",".join(markers))
        if storage and str(storage.get("root_type", "")).lower() not in FORMAL_ROOTS:
            problems.append("formal_root_type: " + str(storage.get("root_type")))
        if storage and (not storage.get("persistent") or not storage.get("durable")):
            problems.append("formal_storage_not_persistent_durable")
        if tls and not (tls.get("enabled") and tls.get("verified")):
            problems.append("formal_tls_not_verified")
        if encryption and not encryption.get("at_rest"):
            problems.append("formal_encryption_not_at_rest")
        if source and source.get("dirty"):
            problems.append("formal_source_dirty")
    return {
        "problems": problems,
        "formal_eligible": formal_eligible and not problems,
        "diagnostic": bool(markers) or evidence_class != "formal",
        "markers": markers,
        "evidence_class": evidence_class,
    }


def host_identity():
    uname = platform.uname()
    virtual = "unknown"
    if sys.platform == "darwin":
        virtual = "macos-native"
    elif os.path.exists("/.dockerenv"):
        virtual = "docker"
    elif os.path.exists("/run/.containerenv"):
        virtual = "podman"
    return {
        "os": uname.system.lower(),
        "arch": uname.machine,
        "kernel": uname.release,
        "machine": uname.machine,
        "virtualization": virtual,
    }


def load_and_validate(path, workloads):
    return validate(load(path), workloads)
