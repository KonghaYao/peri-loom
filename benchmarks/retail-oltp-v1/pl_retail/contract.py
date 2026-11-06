"""Contract loading, package integrity and the effective profile.

The contract file is data.  This module only enforces structural invariants
that the scorer and the runner both rely on; it never invents a value the
contract does not contain.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import ContractError

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
BENCHMARK_PATH = PACKAGE_ROOT / "benchmark.json"
SCORECARD_TEMPLATE_PATH = PACKAGE_ROOT / "scorecard-template.json"
DDL_PATH = PACKAGE_ROOT / "reference" / "ddl.sql"
SHA256SUMS_PATH = PACKAGE_ROOT / "SHA256SUMS"
REFERENCE_DRIVER_PATH = PACKAGE_ROOT / "reference" / "reference_driver.py"

DRIVER_SOURCE_GLOBS = ("pl_retail/*.py",)
EXCLUDED_PARTS = {"__pycache__", ".pytest_cache", ".mypy_cache"}
EXCLUDED_NAMES = {"SHA256SUMS"}


def load_json(path: str | Path) -> Any:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


@dataclass(frozen=True)
class Contract:
    document: dict[str, Any]
    path: Path
    sha256: str

    @property
    def version(self) -> str:
        return str(self.document["contract_version"])

    @property
    def profile(self) -> str:
        return str(self.document["profile"])

    @property
    def seed(self) -> int:
        return int(self.document["seed"])

    @property
    def levels(self) -> dict[str, dict[str, Any]]:
        return self.document["matrix"]["levels"]

    @property
    def repetitions(self) -> int:
        return int(self.document["matrix"]["repetitions"])

    @property
    def window_seconds(self) -> int:
        return int(self.document["matrix"]["window_seconds"])

    @property
    def warmups(self) -> int:
        return int(self.document["matrix"]["warmups_per_worker"])

    def slo(self) -> dict[str, Any]:
        return self.document["slo"]

    def environment(self) -> dict[str, Any]:
        return self.document["environment"]


def load_contract(path: str | Path | None = None) -> Contract:
    target = Path(path or BENCHMARK_PATH)
    if not target.exists():
        raise ContractError("benchmark.json is missing at %s" % target)
    document = load_json(target)
    validate_contract(document)
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    return Contract(document=document, path=target, sha256=digest)


def validate_contract(document: dict[str, Any]) -> None:
    if document.get("contract") != "PL-RETAIL-OLTP":
        raise ContractError("unknown contract id")
    if document.get("contract_version") != "1.0.0":
        raise ContractError("unsupported contract version")
    levels = document.get("matrix", {}).get("levels", {})
    for required in ("U1", "U4", "U8", "U16", "H8"):
        if required not in levels:
            raise ContractError("matrix is missing level %s" % required)
    if document["matrix"].get("repetitions") != 3:
        raise ContractError("the contract requires exactly three repetitions")
    if document["matrix"].get("window_seconds") != 120:
        raise ContractError("the contract requires a 120 second window")
    if document["matrix"].get("warmups_per_worker") != 20:
        raise ContractError("the contract requires 20 warmups per worker")
    weights = document.get("business", {}).get("classes", {})
    if sum(int(entry["weight_per_100"]) for entry in weights.values()) != 100:
        raise ContractError("business weights must sum to 100")
    if document.get("slo", {}).get("business_deadline_ms") != 5000:
        raise ContractError("business deadline must be 5000 ms")
    if document.get("certified_driver_sha256") is not None:
        raise ContractError(
            "certified_driver_sha256 must stay null until the four P0 items are accepted"
        )
    environment = document.get("environment", {})
    quotas = environment.get("project_quotas", {})
    expected = {
        "max_sessions": 16,
        "max_concurrent": 100,
        "requests_per_second": 500,
        "max_databases": 8,
    }
    for key, value in expected.items():
        if int(quotas.get(key, -1)) != value:
            raise ContractError("profile quota %s must be %d" % (key, value))
    limits = environment.get("limits", {})
    expected_limits = {
        "sql_bytes": 65536,
        "result_bytes": 4194304,
        "sql_vm_steps": 10000000,
        "execution_seconds": 5,
        "segment_ram_bytes": 8388608,
        "segment_disk_bytes": 67108864,
        "native_pager_bytes_per_connection": 16777216,
    }
    for key, value in expected_limits.items():
        if int(limits.get(key, -1)) != value:
            raise ContractError("profile limit %s must be %d" % (key, value))
    if environment.get("durability") != "legacy_completion":
        raise ContractError("the profile fixes legacy completion")
    if environment.get("object_pack") != "off":
        raise ContractError("the profile fixes object pack off")
    if environment.get("hrana_experimental") != "on":
        raise ContractError("the profile fixes the experimental Hrana entry on")
    if environment.get("tls_verify") is not True:
        raise ContractError("the profile requires TLS verification")


# -- package integrity ------------------------------------------------------


def package_files(root: Path | None = None) -> list[Path]:
    root = root or PACKAGE_ROOT
    files: list[Path] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        if any(part in EXCLUDED_PARTS for part in relative.parts):
            continue
        if path.name in EXCLUDED_NAMES:
            continue
        files.append(path)
    return files


def package_digests(root: Path | None = None) -> dict[str, str]:
    root = root or PACKAGE_ROOT
    result: dict[str, str] = {}
    for path in package_files(root):
        relative = path.relative_to(root).as_posix()
        result[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def write_sha256sums(root: Path | None = None) -> Path:
    root = root or PACKAGE_ROOT
    lines = ["%s  %s" % (digest, relative) for relative, digest in sorted(package_digests(root).items())]
    target = root / "SHA256SUMS"
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return target


def read_sha256sums(root: Path | None = None) -> dict[str, str]:
    root = root or PACKAGE_ROOT
    target = root / "SHA256SUMS"
    if not target.exists():
        raise ContractError("SHA256SUMS is missing; the package is not self-verifying")
    entries: dict[str, str] = {}
    for line in target.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        digest, _, relative = line.partition("  ")
        if len(digest) != 64 or not relative:
            raise ContractError("malformed SHA256SUMS line")
        entries[relative] = digest
    return entries


def verify_package(root: Path | None = None) -> dict[str, Any]:
    root = root or PACKAGE_ROOT
    recorded = read_sha256sums(root)
    actual = package_digests(root)
    missing = sorted(set(recorded) - set(actual))
    unexpected = sorted(set(actual) - set(recorded))
    mismatched = sorted(
        relative for relative in set(recorded) & set(actual) if recorded[relative] != actual[relative]
    )
    if missing or unexpected or mismatched:
        raise ContractError(
            "package digest failure: missing=%s unexpected=%s mismatched=%s"
            % (missing[:5], unexpected[:5], mismatched[:5])
        )
    return {"files": len(actual), "sha256sums_sha256": hashlib.sha256(
        (root / "SHA256SUMS").read_bytes()
    ).hexdigest()}


def reference_driver_digest() -> dict[str, Any]:
    if not REFERENCE_DRIVER_PATH.exists():
        return {
            "present": False,
            "sha256": None,
            "contract_sha256": "5201f42520464b1a32e42408c3bb87734207802a5f795ad2a9d5c4429add68c1",
            "gap": "the historical reference driver bytes are not in this worktree; restore them from the delivery archive before certification",
        }
    digest = hashlib.sha256(REFERENCE_DRIVER_PATH.read_bytes()).hexdigest()
    return {"present": True, "sha256": digest, "contract_sha256": (
        "5201f42520464b1a32e42408c3bb87734207802a5f795ad2a9d5c4429add68c1"
    )}


def driver_digest() -> str:
    """Digest of the driver package used to produce a run.

    The digest covers every module in ``pl_retail`` so an evidence bundle can
    never be scored against a different driver than the one that produced it.
    """
    root = Path(__file__).resolve().parent
    digest = hashlib.sha256()
    for path in sorted(root.glob("*.py")):
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()
