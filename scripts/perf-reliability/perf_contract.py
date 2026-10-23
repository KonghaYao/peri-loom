#!/usr/bin/env python3
"""Frozen Issue 8 contract, profile bounds, request sizing, and mount guards."""
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
CONTRACT_ROOT = HERE / "issue8"
REFERENCE_ROOT = CONTRACT_ROOT / "reference"
REFERENCE_MANIFEST = REFERENCE_ROOT / "reference-manifest.json"
DERIVED_PATH = HERE / "issue8_derived.json"
MAX_REQUEST_BYTES = 1024 * 1024
MOUNT_ESCAPES = {"\\040": " ", "\\011": "\t", "\\012": "\n", "\\134": "\\"}
# The performance kit roadmap pins v1.1.11 to this baseline commit. Keeping the
# map in code (not in the editable derived policy) stops a denominator retarget
# by editing issue8_derived.json.
FROZEN_DENOMINATOR = {"f3c75622924d84189fa74f3069c209761ceeec31": "v1.1.11"}
# The kit manifest digest and its file set are the contract's fixed points.
# issue8_derived.json may restate them, but it cannot redefine them: the
# derived list must equal the frozen set or the contract is NOT_RUN.
FROZEN_SHA256SUMS_SHA256 = "0e4eb35e279e452d7b984dcf13b82fc580ea7a7803ea3385aa7533579f524692"
# Upstream kit documents that were never copied into the harness contract.
KIT_FILES_NOT_COPIED = ("PERFORMANCE-ROADMAP.md", "README.md")
# The three generator/validator artifacts are copied under issue8/reference/
# and verified by the independent reference-manifest.json. They keep their
# kit-manifest hashes but are not root contract files.
REFERENCE_FILES = ("generate_fixture.py", "sql-validation.json", "validate_sql.py")
REFERENCE_MANIFEST_NAME = "reference-manifest.json"


class ContractError(ValueError):
    pass


class RequestTooLarge(ContractError):
    pass


class UnsafePath(ContractError):
    pass


def digest(path):
    with open(path, "rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def _load_json(path):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as failure:
        raise ContractError("contract_json_unreadable: " + str(path) + ": " + type(failure).__name__) from None


def load_derived(path=DERIVED_PATH):
    derived = _load_json(path)
    if derived.get("schema") != 1:
        raise ContractError("derived_contract_schema")
    if not isinstance(derived.get("required_files"), list) or not derived["required_files"]:
        raise ContractError("derived_required_files")
    return derived


def load_acceptance(root=CONTRACT_ROOT):
    acceptance = _load_json(Path(root) / "acceptance.json")
    if acceptance.get("version") != 1:
        raise ContractError("acceptance_version")
    return acceptance


def load_workloads(root=CONTRACT_ROOT):
    workloads = _load_json(Path(root) / "workloads.json")
    if workloads.get("version") != 1:
        raise ContractError("workloads_version")
    return workloads


def official_denominator(acceptance=None, derived=None):
    """Official denominator: commit comes from the frozen acceptance and the
    version from the frozen commit map. ``derived`` is accepted for callers that
    pass a loaded policy but is deliberately ignored: a mutable derived file
    must never retarget the denominator."""
    acceptance = acceptance or load_acceptance()
    commit = acceptance.get("baseline_commit")
    if not isinstance(commit, str) or not commit:
        raise ContractError("acceptance_baseline_commit_missing")
    version = FROZEN_DENOMINATOR.get(commit)
    if version is None:
        raise ContractError("denominator_commit_unfrozen: " + commit)
    return {"version": version, "commit": commit}


def frozen_required_files(root=CONTRACT_ROOT):
    """The contract file set is fixed by the kit manifest, not by derived policy.

    Every manifest entry is required except the upstream documents that were
    never copied and the three reference artifacts verified separately by
    ``verify_reference``. A mutable derived list may only restate this set.
    """
    root = Path(root)
    manifest = _load_json(root / "SHA256SUMS.json")
    reference = _load_json(root / "reference" / REFERENCE_MANIFEST_NAME)
    if not isinstance(reference, dict) or reference.get("schema") != 1:
        raise ContractError("reference_manifest_schema")
    reference_files = reference.get("files")
    if not isinstance(reference_files, dict) or set(reference_files) != set(REFERENCE_FILES):
        raise ContractError("reference_manifest_schema")
    required = sorted(name for name in manifest
                      if name not in KIT_FILES_NOT_COPIED and name not in REFERENCE_FILES)
    if not required:
        raise ContractError("frozen_contract_empty")
    return required


def _copied_contract_files(root):
    return {path.relative_to(root).as_posix() for path in Path(root).rglob("*") if path.is_file()}


def _validate_derived_required_files(declared, required):
    if not isinstance(declared, list) or not all(isinstance(name, str) for name in declared):
        raise ContractError("derived_required_files")
    duplicates = sorted({name for name in declared if declared.count(name) > 1})
    if duplicates:
        raise ContractError("derived_required_files_duplicate: " + ",".join(duplicates))
    missing = sorted(set(required) - set(declared))
    extra = sorted(set(declared) - set(required))
    if missing or extra:
        raise ContractError("derived_required_files_mismatch: missing=" + ",".join(missing)
                            + " extra=" + ",".join(extra))


def verify_contract(root=CONTRACT_ROOT, derived_path=DERIVED_PATH):
    root = Path(root)
    derived = load_derived(derived_path)
    sums = root / "SHA256SUMS.json"
    if not sums.is_file() or digest(sums) != FROZEN_SHA256SUMS_SHA256:
        raise ContractError("sha256sums_manifest_changed")
    if derived.get("sha256sums_sha256") != FROZEN_SHA256SUMS_SHA256:
        raise ContractError("derived_sha256sums_pin_mismatch")
    manifest = _load_json(sums)
    if not isinstance(manifest, dict) or not manifest:
        raise ContractError("sha256sums_manifest_schema")
    required = frozen_required_files(root)
    _validate_derived_required_files(derived["required_files"], required)
    files, missing, mismatched, unlisted = {}, [], [], []
    for name in required:
        path = root / name
        if not path.is_file():
            missing.append(name)
            continue
        actual = digest(path)
        files[name] = actual
        expected = manifest.get(name)
        if expected is None:
            unlisted.append(name)
        elif actual != expected:
            mismatched.append(name)
    expected_files = set(required) | {"SHA256SUMS.json", "reference/" + REFERENCE_MANIFEST_NAME}
    expected_files |= {"reference/" + name for name in REFERENCE_FILES}
    actual_files = _copied_contract_files(root)
    unexpected = sorted(actual_files - expected_files)
    absent = sorted(expected_files - actual_files)
    if missing or mismatched or unlisted or unexpected or absent:
        raise ContractError("frozen_contract_mismatch: missing=" + ",".join(missing + absent)
                            + " mismatched=" + ",".join(mismatched)
                            + " unlisted=" + ",".join(unlisted)
                            + " unexpected=" + ",".join(unexpected))
    reference = verify_reference(root)
    return {"root": str(root), "sha256sums_sha256": FROZEN_SHA256SUMS_SHA256,
            "files": files, "reference_files": reference["files"], "verified": True}


def contract_identity():
    identity = verify_contract()
    return {"sha256sums_sha256": identity["sha256sums_sha256"],
            "verified_files": len(identity["files"]),
            "reference_files": len(identity["reference_files"])}


def verify_reference(root=CONTRACT_ROOT):
    """Verify the read-only generator/validator copies against both manifests."""
    root = Path(root)
    document = _load_json(root / "reference" / "reference-manifest.json")
    if not isinstance(document, dict):
        raise ContractError("reference_manifest_schema")
    files = document.get("files")
    if document.get("schema") != 1 or not isinstance(files, dict) or not files:
        raise ContractError("reference_manifest_schema")
    kit = _load_json(root / "SHA256SUMS.json")
    verified = {}
    for name, expected in sorted(files.items()):
        if Path(name).name != name or name == "reference-manifest.json":
            raise ContractError("reference_name_unsafe: " + str(name))
        path = root / "reference" / name
        if not path.is_file():
            raise ContractError("reference_missing: " + name)
        actual = digest(path)
        if actual != expected:
            raise ContractError("reference_mismatch: " + name)
        if kit.get(name) != expected:
            raise ContractError("reference_not_in_kit_manifest: " + name)
        verified[name] = actual
    return {"root": str(root / "reference"), "files": verified, "verified": True}


def _positive_integer(value, name):
    if type(value) is not int or value <= 0:
        raise ContractError("profile_override_invalid: " + name)
    return value


def profile(name, overrides=None, acceptance=None, derived=None):
    """Resolve a run profile. Overrides may only tighten contract minima."""
    acceptance = acceptance or load_acceptance()
    derived = derived or load_derived()
    if name not in {"smoke", "quick", "formal", "steady"}:
        raise ContractError("profile_unknown: " + str(name))
    sampling = acceptance["sampling"]
    driver = load_workloads()["driver_rules"]
    if name == "smoke":
        base = {"min_samples_per_lane": sampling["smoke"]["operations_per_lane"],
                "min_seconds": 0, "max_seconds_per_arm": 300, "pairs": 0,
                "warmup_operations": sampling["smoke"]["warmup_operations"],
                "order": [], "total_pass": False, "case_set": ["S0"]}
    elif name == "quick":
        base = {"min_samples_per_lane": sampling["quick"]["minimum_operations_per_lane"],
                "min_seconds": sampling["quick"]["minimum_seconds"],
                "max_seconds_per_arm": derived["profiles"]["quick"]["max_seconds_per_arm"],
                "pairs": 1, "warmup_operations": driver["warmup_operations_for_S1_to_S6"],
                "order": ["AB", "BA", "AB"], "total_pass": False, "case_set": ["S1", "S2"]}
    elif name == "formal":
        base = {"min_samples_per_lane": sampling["formal"]["minimum_samples_per_lane"],
                "min_seconds": sampling["formal"]["minimum_seconds"],
                "max_seconds_per_arm": sampling["formal"]["max_seconds_per_arm"],
                "pairs": sampling["formal"]["pairs"], "order": list(sampling["formal"]["order"]),
                "warmup_operations": driver["warmup_operations_for_S1_to_S6"],
                "total_pass": True, "case_set": ["S1", "S2"]}
    else:
        steady = acceptance["steady_state"]
        base = {"min_samples_per_lane": None, "min_seconds": steady["seconds_min"],
                "max_seconds_per_arm": derived["profiles"]["steady"]["max_seconds_per_arm"],
                "pairs": 0, "warmup_operations": driver["warmup_operations_for_S1_to_S6"],
                "order": [], "total_pass": False, "case_set": [],
                "maintenance_cycles_min": steady["maintenance_cycles_min"]}
    result = dict(base, name=name)
    result["order"] = list(base["order"][:result["pairs"]])
    overrides = overrides or {}
    allowed = {"min_samples_per_lane", "min_seconds", "max_seconds_per_arm", "pairs"}
    unknown = sorted(set(overrides) - allowed)
    if unknown:
        raise ContractError("profile_override_unknown: " + ",".join(unknown))
    for key, value in overrides.items():
        _positive_integer(value, key)
        if key == "pairs":
            maximum = derived["profiles"].get(name, {}).get("maximum_pairs", 0)
            if name == "formal" and value != base["pairs"]:
                raise ContractError("formal_pair_count_fixed")
            if name not in {"quick", "formal"} or value > maximum:
                raise ContractError("profile_pairs_out_of_bounds")
            result[key] = value
            result["order"] = base["order"][:value]
            continue
        if base[key] is None:
            result[key] = value
            continue
        if value < base[key]:
            raise ContractError("profile_override_relaxes_contract: " + key)
        maximum = derived["profiles"].get(name, {}).get("maximum_seconds_per_arm")
        if key == "min_samples_per_lane":
            sample_maximum = derived["profiles"].get(name, {}).get("maximum_samples_per_lane")
            if sample_maximum is not None and value > sample_maximum:
                raise ContractError("profile_samples_out_of_bounds")
        if key == "max_seconds_per_arm":
            if maximum is not None and value > maximum:
                raise ContractError("profile_arm_budget_out_of_bounds")
            if value < result["min_seconds"]:
                raise ContractError("profile_arm_budget_below_minimum_duration")
            if name == "formal" and value < acceptance["sampling"]["formal"]["max_seconds_per_arm"]:
                raise ContractError("profile_arm_budget_relaxes_formal_default")
        result[key] = value
    if result["pairs"] and len(result["order"]) != result["pairs"]:
        raise ContractError("profile_pair_order_length")
    if result["max_seconds_per_arm"] < result["min_seconds"]:
        raise ContractError("profile_arm_budget_below_minimum_duration")
    return result


def request_bytes(sql, operation="exec", session="s" * 128):
    """Measure the worst-case stdio JSON request; sessions are capped at 128 bytes."""
    request = {"id": 1, "op": operation, "session": session, "sql": sql}
    return len(json.dumps(request).encode())


def assert_request_size(sql, maximum=MAX_REQUEST_BYTES, operation="exec"):
    measured = request_bytes(sql, operation=operation)
    if measured > maximum:
        raise RequestTooLarge("request_too_large: " + str(measured) + " > " + str(maximum)
                              + "; chunk the statement instead of truncating it")
    return measured


def validate_payload_ceiling(payload_bytes, maximum=MAX_REQUEST_BYTES):
    if type(payload_bytes) is not int or payload_bytes <= 0:
        raise ContractError("payload_bytes_invalid")
    if payload_bytes > maximum:
        raise RequestTooLarge("payload_exceeds_request_limit: " + str(payload_bytes) + " > " + str(maximum))
    return payload_bytes


def chunk_insert_statements(rows, payload_bytes, table="bench", maximum=MAX_REQUEST_BYTES,
                            payload_char="x"):
    """Split a multi-row INSERT by encoded request size; never truncate a row."""
    validate_payload_ceiling(payload_bytes, maximum)
    if type(rows) is not int or rows <= 0:
        raise ContractError("insert_rows_invalid")
    prefix = "INSERT INTO " + table + " VALUES "
    payload = payload_char * payload_bytes
    chunks, current, current_rows = [], [], 0
    for identifier in range(rows):
        item = "(" + str(identifier) + ",'" + payload + "')"
        candidate = prefix + ",".join(current + [item])
        if request_bytes(candidate) <= maximum:
            current.append(item)
            current_rows += 1
            continue
        if not current:
            raise RequestTooLarge("single_insert_row_exceeds_request_limit: row=" + str(identifier)
                                  + " payload_bytes=" + str(payload_bytes) + " maximum=" + str(maximum))
        sql = prefix + ",".join(current)
        chunks.append({"sql": sql, "row_count": current_rows, "request_bytes": request_bytes(sql)})
        current, current_rows = [item], 1
    if current:
        sql = prefix + ",".join(current)
        chunks.append({"sql": sql, "row_count": current_rows, "request_bytes": request_bytes(sql)})
    return chunks


def _workload_case(case_id, workloads=None):
    workloads = workloads or load_workloads()
    for case in workloads["cases"]:
        if case["id"] == case_id:
            return case
    raise ContractError("case_unknown: " + str(case_id))


def validate_case_parameters(case_id, rows=None, payload_bytes=None, concurrency=None):
    case = _workload_case(case_id)
    setup = load_workloads()["setups"][case["setup"]]
    resolved = {"case": case_id, "rows": setup["rows"] if "rows" in setup else 64,
                "payload_bytes": setup.get("payload_bytes", 256), "concurrency": case.get("concurrency")}
    for key, value in [("rows", rows), ("payload_bytes", payload_bytes), ("concurrency", concurrency)]:
        if value is None:
            continue
        if type(value) is not int or value <= 0:
            raise ContractError("case_parameter_invalid: " + key)
        if key == "payload_bytes":
            validate_payload_ceiling(value)
        if key == "concurrency":
            allowed = case.get("concurrency") or [case.get("lanes")]
            if value not in allowed:
                raise ContractError("contract_concurrency_mismatch: " + str(value))
        elif value != resolved[key]:
            raise ContractError("contract_" + key + "_mismatch: " + str(value) + " != " + str(resolved[key]))
    validate_payload_ceiling(resolved["payload_bytes"])
    write_sql = "UPDATE bench SET payload='" + ("x" * resolved["payload_bytes"]) + "' WHERE id=0"
    assert_request_size(write_sql)
    resolved["setup_insert_chunks"] = chunk_insert_statements(resolved["rows"], resolved["payload_bytes"],
                                                              table="bench")
    return resolved


def _unescape_mount(value):
    for source, target in MOUNT_ESCAPES.items():
        value = value.replace(source, target)
    return value


def filesystem_type(path, mount_table=None):
    """Return the mount filesystem type, or None when the host has no /proc/self/mounts."""
    if mount_table is None:
        try:
            mount_table = Path("/proc/self/mounts").read_text()
        except OSError:
            return None
    target = Path(path).resolve()
    selected, selected_length = None, -1
    for line in mount_table.splitlines():
        fields = line.split()
        if len(fields) < 3:
            continue
        mountpoint = Path(_unescape_mount(fields[1]))
        if target == mountpoint or target.is_relative_to(mountpoint):
            if len(str(mountpoint)) > selected_length:
                selected, selected_length = fields[2], len(str(mountpoint))
    return selected


def _temp_roots():
    roots = {Path(tempfile.gettempdir()), Path("/tmp"), Path("/var/tmp"), Path("/dev/shm")}
    return {root.resolve() for root in roots if root.exists() or root in {Path("/tmp"), Path("/var/tmp"), Path("/dev/shm")}}


def _overlap(first, second):
    return first == second or first.is_relative_to(second) or second.is_relative_to(first)


def _check_root(path, label):
    path = Path(path)
    if not path.is_absolute():
        raise UnsafePath(label + "_root_must_be_absolute")
    resolved = path.resolve()
    for temporary in _temp_roots():
        if resolved == temporary or resolved.is_relative_to(temporary):
            raise UnsafePath(label + "_root_is_ephemeral: " + str(resolved))
    scripts = (ROOT / "scripts").resolve()
    if resolved == scripts or resolved.is_relative_to(scripts):
        raise UnsafePath(label + "_root_inside_harness_sources")
    return resolved


def validate_roots(store_root, cache_root, evidence_root, profile_name, mount_table=None, create=False):
    roots = {"store": _check_root(store_root, "store"), "cache": _check_root(cache_root, "cache"),
             "evidence": _check_root(evidence_root, "evidence")}
    labels = list(roots)
    for index, first in enumerate(labels):
        for second in labels[index + 1:]:
            if _overlap(roots[first], roots[second]):
                raise UnsafePath("roots_must_be_distinct: " + first + " overlaps " + second)
    if create:
        for root in roots.values():
            root.mkdir(parents=True, exist_ok=True)
    for label, root in roots.items():
        if not root.is_dir():
            raise UnsafePath(label + "_root_missing: " + str(root))
    filesystems = {label: filesystem_type(root, mount_table) for label, root in roots.items()}
    forbidden = set(load_derived()["paths"]["forbidden_filesystems"])
    for label, fstype in filesystems.items():
        if fstype in forbidden:
            raise UnsafePath(label + "_root_on_forbidden_filesystem: " + str(fstype))
    formal = profile_name in {"formal", "steady"}
    measured = all(value is not None for value in filesystems.values())
    if formal and load_derived()["paths"]["formal_requires_measured_filesystem"] and not measured:
        raise UnsafePath("filesystem_type_unverified_for_formal_evidence")
    return {"roots": {key: str(value) for key, value in roots.items()}, "filesystems": filesystems,
            "persistent_mounts_verified": measured, "profile": profile_name, "distinct": True}


def ensure_empty_directory(path):
    path = Path(path)
    if path.exists():
        if not path.is_dir():
            raise UnsafePath("run_directory_not_a_directory: " + str(path))
        if any(path.iterdir()):
            raise UnsafePath("run_directory_reuse_refused: " + str(path))
    else:
        path.mkdir(parents=True)
    return path


def repository_identity():
    try:
        root_head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, timeout=5).strip()
        turso_head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT / "prototypes/p0/turso",
                                             text=True, timeout=5).strip()
    except (OSError, subprocess.SubprocessError) as failure:
        raise ContractError("repository_identity_unavailable: " + type(failure).__name__) from None
    return {"git_head": root_head, "turso_head": turso_head}


def _read_optional(path):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def environment_identity():
    cgroup = {name: _read_optional(path) for name, path in {
        "memory_max": "/sys/fs/cgroup/memory.max",
        "cpu_max": "/sys/fs/cgroup/cpu.max",
        "cpuset_effective": "/sys/fs/cgroup/cpuset.cpus.effective"}.items()}
    return {"utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "host": platform.platform(), "os": platform.system(),
            "arch": platform.machine(), "kernel": platform.release(),
            "machine": platform.machine(),
            "python": platform.python_version(), "cpu_count": os.cpu_count(),
            "cgroup": cgroup, "repository": repository_identity()}
