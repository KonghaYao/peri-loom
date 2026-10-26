#!/usr/bin/env python3
"""Issue 11 contract loading, hashing, and deterministic fixture helpers."""
import hashlib
import json
from pathlib import Path
import sys

PERF_ROOT = Path(__file__).resolve().parent.parent
if str(PERF_ROOT) not in sys.path:
    sys.path.insert(0, str(PERF_ROOT))

from perf_contract import digest  # noqa: E402

CONTRACT_ROOT = Path(__file__).resolve().parent
SQL_ROOT = CONTRACT_ROOT / "sql"
ATTRIBUTION_ROOT = CONTRACT_ROOT / "attribution"
ISSUE8_ROOT = PERF_ROOT / "issue8"
ISSUE8_MANIFEST = ISSUE8_ROOT / "SHA256SUMS.json"
SCENARIOS = ("H1", "H2", "H3", "H4", "H5", "H6", "H7", "H8", "H9", "H10", "N1")
PAYLOAD_BYTES = 256


class ContractError(ValueError):
    pass


def _read_json(path):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as failure:
        raise ContractError("contract_json_unreadable: " + str(path) + ": "
                            + type(failure).__name__) from None


def load_acceptance(root=CONTRACT_ROOT):
    document = _read_json(Path(root) / "acceptance.json")
    if document.get("version") != 1 or document.get("schema") != "issue11-hrana-acceptance":
        raise ContractError("acceptance_schema")
    return document


def load_workloads(root=CONTRACT_ROOT):
    document = _read_json(Path(root) / "workloads.json")
    if document.get("version") != 1 or document.get("schema") != "issue11-hrana-workloads":
        raise ContractError("workloads_schema")
    return document


def load_environment_schema(root=CONTRACT_ROOT):
    document = _read_json(Path(root) / "environment.schema.json")
    if document.get("$id") != "issue11-environment":
        raise ContractError("environment_schema")
    return document


def load_attribution_schema(root=CONTRACT_ROOT):
    document = _read_json(Path(root) / "attribution" / "schema.json")
    if document.get("schema") != "issue11-attribution" \
            or document.get("schema_version") != 1:
        raise ContractError("attribution_schema")
    return document


def contract_files(acceptance=None):
    acceptance = acceptance or load_acceptance()
    files = acceptance.get("contract", {}).get("files")
    if not isinstance(files, list) or not files:
        raise ContractError("contract_files_missing")
    return tuple(files)


def verify_contract(root=CONTRACT_ROOT):
    """Recompute the contract manifest; never trust a stored verified flag."""
    root = Path(root)
    acceptance = load_acceptance(root)
    files = contract_files(acceptance)
    problems = []
    manifest = _read_json(root / "SHA256SUMS.json")
    if not isinstance(manifest, dict):
        return {"verified": False, "files": {}, "problems": ["manifest_not_object"]}
    if set(manifest) != set(files):
        problems.append("manifest_file_set_mismatch")
    observed = {}
    for name in files:
        path = (root / name).resolve()
        if root.resolve() not in path.parents:
            problems.append("contract_path_escape: " + name)
            continue
        if not path.is_file():
            problems.append("contract_file_missing: " + name)
            continue
        actual = digest(path)
        observed[name] = actual
        if manifest.get(name) != actual:
            problems.append("contract_hash_mismatch: " + name)
    return {"verified": not problems, "files": observed, "problems": problems}


def issue8_manifest_sha256():
    return digest(ISSUE8_MANIFEST)


def validate_contract(root=CONTRACT_ROOT):
    """Cross-file checks that are independent of the SHA manifest."""
    root = Path(root)
    problems = []
    acceptance = load_acceptance(root)
    workloads = load_workloads(root)
    environment = load_environment_schema(root)
    attribution = load_attribution_schema(root)
    if acceptance["baseline_commit"] != workloads["baseline_commit"]:
        problems.append("baseline_commit_mismatch")
    if acceptance["turso_commit"] != workloads["turso_commit"]:
        problems.append("turso_commit_mismatch")
    if acceptance.get("measured") is not False \
            or acceptance.get("target_status") != "TARGET_NOT_MEASURED":
        problems.append("acceptance_must_stay_unmeasured")
    sampling = acceptance.get("sampling", {})
    if sampling.get("warmup_min", 0) < 200:
        problems.append("warmup_below_200")
    if sampling.get("quick", {}).get("samples_min", 0) < 2000:
        problems.append("quick_samples_below_2000")
    formal = sampling.get("formal", {})
    if formal.get("samples_min", 0) < 10000:
        problems.append("formal_samples_below_10000")
    if formal.get("pairs_min", 0) < 5:
        problems.append("formal_pairs_below_5")
    if len(formal.get("pair_order", [])) != formal.get("pairs_min"):
        problems.append("formal_pair_order_length")
    if set(formal.get("states", [])) != {"fresh", "aged"}:
        problems.append("formal_states")
    if formal.get("aged_min_seconds", 0) < 3600:
        problems.append("aged_state_below_3600")
    if set(workloads.get("scenarios", {})) != set(SCENARIOS):
        problems.append("scenario_set_mismatch")
    for scenario_id, scenario in workloads.get("scenarios", {}).items():
        if scenario.get("reserved"):
            continue
        if scenario.get("formal_eligible"):
            if scenario.get("quick_samples_min", 0) < 2000:
                problems.append("scenario_quick_samples: " + scenario_id)
            if scenario.get("formal_samples_min", 0) < 10000:
                problems.append("scenario_formal_samples: " + scenario_id)
            if scenario.get("warmup_min", 0) < 200:
                problems.append("scenario_warmup: " + scenario_id)
    if workloads.get("payload_bytes") != PAYLOAD_BYTES:
        problems.append("payload_bytes")
    if environment.get("properties", {}).get("schema_version", {}).get("const") != 1:
        problems.append("environment_schema_version")
    if attribution.get("design_requirement") is not True:
        problems.append("attribution_design_requirement")
    if attribution.get("available_in_w0_a") != ["client"]:
        problems.append("attribution_available_in_w0_a")
    for process in ("client", "gateway", "worker", "authority", "object_store"):
        if not attribution.get("process_phases", {}).get(process):
            problems.append("attribution_phase_missing: " + process)
    return problems


def scenario(workloads, scenario_id):
    scenarios = workloads.get("scenarios", {})
    if scenario_id not in scenarios:
        raise ContractError("unknown_scenario: " + str(scenario_id))
    return scenarios[scenario_id]


def sql_path(relative):
    path = (CONTRACT_ROOT / relative).resolve()
    if CONTRACT_ROOT.resolve() not in path.parents:
        raise ContractError("sql_path_escape: " + str(relative))
    return path


def read_sql(relative):
    path = sql_path(relative)
    try:
        return path.read_text()
    except OSError as failure:
        raise ContractError("sql_unreadable: " + str(relative) + ": "
                            + type(failure).__name__) from None


def split_sql_file(relative):
    statements = []
    for statement in read_sql(relative).split(";"):
        statement = statement.strip()
        if statement:
            statements.append(statement)
    return statements


def fixture_relative(scale):
    workloads = load_workloads()
    fixtures = workloads["fixture"]["scales"]
    if str(scale) not in fixtures:
        raise ContractError("unknown_fixture_scale: " + str(scale))
    return fixtures[str(scale)]


def fixture_payload(row_id):
    return ("r%08d:" % row_id) + ("0" * (PAYLOAD_BYTES - 10))


def insert_payload(row_id):
    prefix = "i%08d:" % row_id
    return prefix + ("0" * (PAYLOAD_BYTES - len(prefix)))


def payload_shape_ok(value):
    return isinstance(value, str) and len(value.encode()) == PAYLOAD_BYTES


def fixture_digest(scale):
    import hashlib
    hasher = hashlib.sha256()
    for row_id in range(1, int(scale) + 1):
        hasher.update((json.dumps([row_id, 0, fixture_payload(row_id)],
                                 separators=(",", ":")) + "\n").encode())
    return hasher.hexdigest()


def source_identity():
    import subprocess
    def run(*arguments):
        try:
            return subprocess.run(arguments, cwd=PERF_ROOT.parent.parent, capture_output=True,
                                  text=True, timeout=10)
        except (OSError, subprocess.SubprocessError):
            return None
    head = run("git", "rev-parse", "HEAD")
    branch = run("git", "rev-parse", "--abbrev-ref", "HEAD")
    status = run("git", "status", "--porcelain", "--untracked-files=no")
    turso = run("git", "rev-parse", "HEAD:prototypes/p0/turso")
    return {
        "commit": head.stdout.strip() if head and head.returncode == 0 else "",
        "branch": branch.stdout.strip() if branch and branch.returncode == 0 else "",
        "dirty": bool(status.stdout.strip()) if status and status.returncode == 0 else True,
        "turso_gitlink": turso.stdout.strip() if turso and turso.returncode == 0 else "",
    }


def package_digest():
    files = [
        CONTRACT_ROOT / "acceptance.json",
        CONTRACT_ROOT / "workloads.json",
        CONTRACT_ROOT / "environment.schema.json",
        ATTRIBUTION_ROOT / "schema.json",
    ]
    hasher = hashlib.sha256()
    for path in files:
        hasher.update(path.read_bytes())
    return hasher.hexdigest()
