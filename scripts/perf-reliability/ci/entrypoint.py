#!/usr/bin/env python3
"""Issue 8 performance harness container entrypoint.

The image contains only the pinned toolchain and this file. The repository is
mounted read-only at /workspace; every writable location is an explicit mount.
The entrypoint does not build performance binaries and does not relax gate
semantics: ``quick`` may only be accepted as ``PHASE_PASS`` (gate exit 1), while
``formal`` requires a real ``PASS`` and exit 0.

Subprocesses are executed with argument vectors; no shell is involved. The
environment handed to them is an allowlist so host credentials never leak into
tests, workers or the harness.
"""
import argparse
import hashlib
import json
import os
import platform
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

PASS = "PASS"
PHASE_PASS = "PHASE_PASS"
FAIL = "FAIL"
NOT_RUN = "NOT_RUN"
INCOMPLETE = "INCOMPLETE"

SECTION_NAMES = ("native", "correctness", "resources", "attribution")
TRUST_KEYS = ("A", "B")
MODES = ("test", "quick", "formal")
EXPECTED_PYTHON = (3, 11)
EXPECTED_RUST = "1.98.1"

QUICK_PAIRS = 1
QUICK_ORDER = ["AB"]
QUICK_CASES = {"S1": [2]}
QUICK_RUNS = 2

FORMAL_PAIRS = 3
FORMAL_ORDER = ["AB", "BA", "AB"]
FORMAL_CASES = {"S1": [2, 1], "S2": [2]}
FORMAL_RUNS = 18

EPHEMERAL_FILESYSTEMS = frozenset({
    "tmpfs", "ramfs", "devtmpfs", "overlay", "overlayfs", "aufs", "shm",
})
SHARED_FILESYSTEM_PREFIXES = ("fuse", "virtiofs", "9p", "nfs", "cifs", "smb")
FORMAL_FILESYSTEMS = frozenset({"ext4", "xfs"})

ENV_ALLOWLIST = (
    "PATH", "HOME", "LANG", "LC_ALL", "LANGUAGE", "TZ", "TERM", "USER",
    "LOGNAME", "SHELL", "TMPDIR", "CARGO_HOME", "RUSTUP_HOME",
    "CARGO_TARGET_DIR", "RUST_BACKTRACE", "SSL_CERT_FILE", "SSL_CERT_DIR",
    "CARGO_NET_OFFLINE",
)
DEFAULT_PATH = "/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"


class PreflightError(Exception):
    """The container cannot safely start the requested mode."""


@dataclass(frozen=True)
class Layout:
    workspace: Path = Path("/workspace")
    cargo_cache: Path = Path("/cargo-cache")
    cargo_target: Path = Path("/cargo-target")
    build: Path = Path("/build")
    store: Path = Path("/perf/store")
    cache: Path = Path("/perf/cache")
    evidence: Path = Path("/perf/evidence")
    metrics: Path = Path("/perf/metrics")
    reports: Path = Path("/reports")
    inputs: Path = Path("/inputs")
    artifacts: Path = Path("/artifacts")
    trust: Path = Path("/trust")
    mountinfo: Path = Path("/proc/self/mountinfo")


DEFAULT_LAYOUT = Layout()


@dataclass(frozen=True)
class MountEntry:
    mountpoint: Path
    options: frozenset
    fstype: str
    source: str

    @property
    def read_only(self):
        return "ro" in self.options


@dataclass(frozen=True)
class MountTable:
    entries: tuple

    @classmethod
    def read(cls, path):
        try:
            text = Path(path).read_text()
        except OSError as failure:
            raise PreflightError("mountinfo_unavailable: " + str(path) + ": " + str(failure))
        return parse_mountinfo(text)

    def for_path(self, path):
        target = Path(path).resolve()
        selected = None
        for entry in self.entries:
            mountpoint = entry.mountpoint
            if target == mountpoint or target.is_relative_to(mountpoint):
                if selected is None or len(str(mountpoint)) > len(str(selected.mountpoint)):
                    selected = entry
        return selected


def _unescape_mount(value):
    return re.sub(r"\\([0-7]{3})", lambda match: chr(int(match.group(1), 8)), value)


def parse_mountinfo(text):
    entries = []
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 7 or "-" not in fields:
            continue
        separator = fields.index("-")
        if separator < 6 or len(fields) < separator + 4:
            continue
        try:
            # Resolve both sides of the prefix comparison: /var is a symlink on
            # some development hosts, and mountinfo reports the canonical path.
            mountpoint = Path(_unescape_mount(fields[4])).resolve()
        except (OSError, ValueError):
            continue
        entries.append(MountEntry(
            mountpoint=mountpoint,
            options=frozenset(fields[5].split(",")),
            fstype=fields[separator + 1].lower(),
            source=_unescape_mount(fields[separator + 2])))
    return MountTable(entries=tuple(entries))


def normalize_arch(machine):
    value = (machine or "").lower()
    if value in {"x86_64", "amd64", "x64"}:
        return "amd64"
    if value in {"aarch64", "arm64"}:
        return "arm64"
    return value


def command_environment(layout=DEFAULT_LAYOUT):
    environment = {name: os.environ[name] for name in ENV_ALLOWLIST if name in os.environ}
    environment.setdefault("PATH", DEFAULT_PATH)
    environment.setdefault("HOME", "/home/peri")
    environment.setdefault("LANG", "C.UTF-8")
    environment.setdefault("LC_ALL", "C.UTF-8")
    environment.setdefault("TZ", "UTC")
    environment["TMPDIR"] = str(Path(layout.build) / "tmp")
    environment["CARGO_HOME"] = str(layout.cargo_cache)
    environment["CARGO_TARGET_DIR"] = str(layout.cargo_target)
    # The checkout's rust-toolchain.toml asks for rustfmt/clippy. With a
    # read-only root, rustup must not follow that override and try to sync
    # components into RUSTUP_HOME; the image installs them and pins the
    # toolchain explicitly for every subprocess.
    environment["RUSTUP_TOOLCHAIN"] = EXPECTED_RUST
    # perf_contract resolves the repository root from this explicit path; the
    # harness-unittest step overrides it with the writable snapshot root.
    environment["PERI_REPO_ROOT"] = str(layout.workspace)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PYTHONUNBUFFERED"] = "1"
    environment["CARGO_TERM_COLOR"] = "never"
    environment["GIT_TERMINAL_PROMPT"] = "0"
    environment["GIT_ASKPASS"] = "/bin/false"
    # Optional locks would make read-only commands try to rewrite .git/index on
    # the read-only /workspace mount.
    environment["GIT_OPTIONAL_LOCKS"] = "0"
    # Process-scoped safe.directory entries only; no git config is written.
    environment["GIT_CONFIG_COUNT"] = "2"
    environment["GIT_CONFIG_KEY_0"] = "safe.directory"
    environment["GIT_CONFIG_VALUE_0"] = str(layout.workspace)
    environment["GIT_CONFIG_KEY_1"] = "safe.directory"
    environment["GIT_CONFIG_VALUE_1"] = str(layout.workspace / "prototypes/p0/turso")
    return environment


def _require_mount(table, path, label):
    entry = table.for_path(path)
    if entry is None:
        raise PreflightError(label + "_mount_not_found: " + str(path))
    return entry


def _forbidden_filesystem(fstype):
    lowered = fstype.lower()
    if lowered in EPHEMERAL_FILESYSTEMS:
        return True
    return any(lowered.startswith(prefix) for prefix in SHARED_FILESYSTEM_PREFIXES)


def _check_performance_mount(mode, table, path, label):
    entry = _require_mount(table, path, label)
    if _forbidden_filesystem(entry.fstype):
        raise PreflightError(label + "_root_on_forbidden_filesystem: " + entry.fstype)
    if mode == "formal" and entry.fstype not in FORMAL_FILESYSTEMS:
        raise PreflightError(label + "_root_not_formal_filesystem: " + entry.fstype)
    return entry


def check_mounts(mode, layout=DEFAULT_LAYOUT, table=None):
    table = table or MountTable.read(layout.mountinfo)
    workspace = _require_mount(table, layout.workspace, "workspace")
    if not workspace.read_only:
        raise PreflightError("workspace_mount_not_read_only: " + str(layout.workspace))
    if mode == "test":
        return {"workspace": workspace}
    performance = {
        "store": layout.store,
        "cache": layout.cache,
        "evidence": layout.evidence,
        "metrics": layout.metrics,
        "reports": layout.reports,
    }
    observed = {"workspace": workspace}
    for label, path in performance.items():
        observed[label] = _check_performance_mount(mode, table, path, label)
    for label, path in (("inputs", layout.inputs), ("artifacts", layout.artifacts),
                        ("trust", layout.trust)):
        entry = _require_mount(table, path, label)
        if not entry.read_only:
            raise PreflightError(label + "_mount_not_read_only: " + str(path))
        observed[label] = entry
    if "noexec" in observed["artifacts"].options:
        raise PreflightError("artifacts_mount_noexec: " + str(layout.artifacts))
    return observed


def _overlap(first, second):
    first, second = Path(first).resolve(), Path(second).resolve()
    return first == second or first.is_relative_to(second) or second.is_relative_to(first)


def check_distinct_roots(mode, layout=DEFAULT_LAYOUT):
    roots = {
        "workspace": layout.workspace,
        "build": layout.build,
        "store": layout.store,
        "cache": layout.cache,
        "evidence": layout.evidence,
        "metrics": layout.metrics,
        "reports": layout.reports,
    }
    if mode == "test":
        roots.update(cargo_cache=layout.cargo_cache, cargo_target=layout.cargo_target)
    else:
        roots.update(inputs=layout.inputs, artifacts=layout.artifacts, trust=layout.trust)
    labels = list(roots)
    for index, first in enumerate(labels):
        for second in labels[index + 1:]:
            if _overlap(roots[first], roots[second]):
                raise PreflightError("roots_must_be_distinct: " + first + " overlaps " + second)
    if mode != "test":
        for label in ("inputs", "artifacts", "trust"):
            if _overlap(roots[label], layout.evidence):
                raise PreflightError(label + "_must_live_outside_evidence")


def _probe_writable(path, label):
    path = Path(path)
    if not path.is_dir():
        raise PreflightError(label + "_not_a_directory: " + str(path))
    probe = path / (".peri-write-probe-" + str(os.getpid()))
    try:
        descriptor = os.open(probe, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except OSError as failure:
        raise PreflightError(label + "_not_writable: " + str(path) + ": " + str(failure))
    else:
        os.close(descriptor)
    try:
        probe.unlink()
    except FileNotFoundError:
        pass


def _require_empty(path, label):
    path = Path(path)
    if not path.is_dir():
        raise PreflightError(label + "_not_a_directory: " + str(path))
    if any(path.iterdir()):
        raise PreflightError(label + "_must_be_empty: " + str(path))


def check_writable_roots(mode, layout=DEFAULT_LAYOUT):
    if mode == "test":
        for label, path in (("cargo_cache", layout.cargo_cache),
                            ("cargo_target", layout.cargo_target),
                            ("build", layout.build), ("reports", layout.reports)):
            _probe_writable(path, label)
        return
    _require_empty(layout.evidence, "evidence_root")
    _require_empty(layout.metrics, "metrics_root")
    for label, path in (("build", layout.build), ("store", layout.store),
                        ("cache", layout.cache), ("evidence", layout.evidence),
                        ("metrics", layout.metrics), ("reports", layout.reports)):
        _probe_writable(path, label)


def _run_git(argv, layout):
    try:
        completed = subprocess.run(argv, cwd=str(layout.workspace),
                                   env=command_environment(layout), stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True, timeout=120, check=False)
    except (OSError, subprocess.SubprocessError) as failure:
        raise PreflightError("git_command_unavailable: " + str(failure))
    if completed.returncode != 0:
        detail = completed.stderr.strip().splitlines()
        raise PreflightError("git_command_failed: " + " ".join(argv[1:])
                             + (": " + detail[0][:300] if detail else ""))
    return completed.stdout.strip()


def check_source(layout=DEFAULT_LAYOUT, allow_dirty=False):
    required = (
        layout.workspace / ".git",
        layout.workspace / "prototypes/v1/Cargo.toml",
        layout.workspace / "prototypes/v1/Cargo.lock",
        layout.workspace / "prototypes/v1/src",
        layout.workspace / "prototypes/p0/turso/core/Cargo.toml",
        layout.workspace / "prototypes/p0/turso/.git",
        layout.workspace / "scripts/perf-reliability/campaign_cli.py",
        layout.workspace / "scripts/perf-reliability/gate.py",
        layout.workspace / "scripts/perf-reliability/issue8_runner.py",
    )
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise PreflightError("workspace_incomplete: " + ",".join(missing))
    gitlink = _run_git(["git", "ls-tree", "HEAD", "prototypes/p0/turso"], layout)
    fields = gitlink.split()
    if len(fields) < 3 or fields[1] != "commit":
        raise PreflightError("turso_gitlink_missing_from_head")
    expected = fields[2]
    actual = _run_git(["git", "-C", str(layout.workspace / "prototypes/p0/turso"),
                       "rev-parse", "HEAD"], layout)
    if actual != expected:
        raise PreflightError("turso_gitlink_mismatch: expected " + expected + " actual " + actual)
    status = _run_git(["git", "status", "--porcelain=v1", "--untracked-files=normal"], layout)
    if status and not allow_dirty:
        first = status.splitlines()[:5]
        raise PreflightError("workspace_checkout_dirty: " + " | ".join(first))
    head = _run_git(["git", "rev-parse", "HEAD"], layout)
    return {"head": head, "turso_gitlink": expected, "turso_head": actual,
            "dirty": bool(status), "dirty_allowed": bool(status and allow_dirty)}


def check_runtime(mode, layout=DEFAULT_LAYOUT, system=None, machine=None):
    system = system or platform.system()
    machine = machine or platform.machine()
    if not hasattr(os, "geteuid") or os.geteuid() == 0:
        raise PreflightError("runtime_must_not_run_as_root")
    if system != "Linux":
        raise PreflightError("runtime_not_native_linux: " + system)
    if sys.version_info[:2] < EXPECTED_PYTHON:
        raise PreflightError("runtime_python_too_old: " + platform.python_version())
    identity = {"system": system, "machine": machine, "arch": normalize_arch(machine),
                "uid": os.geteuid(), "python": platform.python_version()}
    if mode == "test":
        environments = command_environment(layout)
        missing = [name for name in ("cargo", "rustc")
                   if shutil.which(name, path=environments.get("PATH")) is None]
        if missing:
            raise PreflightError("runtime_toolchain_missing: " + ",".join(missing))
        try:
            cargo = subprocess.run(["cargo", "--version"], env=environments,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True, timeout=10, check=False)
            rustc = subprocess.run(["rustc", "--version"], env=environments,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True, timeout=10, check=False)
        except (OSError, subprocess.SubprocessError) as failure:
            raise PreflightError("runtime_toolchain_unavailable: " + str(failure))
        if cargo.returncode != 0 or not cargo.stdout.startswith("cargo " + EXPECTED_RUST):
            raise PreflightError("runtime_cargo_version_mismatch: expected "
                                 + EXPECTED_RUST + " actual " + cargo.stdout.strip())
        if rustc.returncode != 0 or not rustc.stdout.startswith("rustc " + EXPECTED_RUST):
            raise PreflightError("runtime_rustc_version_mismatch: expected "
                                 + EXPECTED_RUST + " actual " + rustc.stdout.strip())
        identity["cargo"] = cargo.stdout.strip()
        identity["rustc"] = rustc.stdout.strip()
    return identity


def _read_json(path, label):
    try:
        text = Path(path).read_text()
    except OSError as failure:
        raise PreflightError(label + "_unreadable: " + str(path) + ": " + str(failure))
    try:
        document = json.loads(text)
    except json.JSONDecodeError as failure:
        raise PreflightError(label + "_invalid_json: " + str(path) + ": " + str(failure))
    if not isinstance(document, dict):
        raise PreflightError(label + "_must_be_object: " + str(path))
    return document


def _resolve_inside(value, root, label):
    try:
        resolved = Path(value).resolve(strict=True)
        root = Path(root).resolve(strict=True)
    except OSError as failure:
        raise PreflightError(label + "_missing: " + str(value) + ": " + str(failure))
    if not (resolved == root or resolved.is_relative_to(root)):
        raise PreflightError(label + "_outside_allowed_mount: " + str(resolved))
    return resolved


def _non_empty_string(value):
    return isinstance(value, str) and bool(value.strip())


def _schema_is_one(document):
    # bool is an int subclass; a True schema is not a schema version.
    return type(document.get("schema")) is int and document["schema"] == 1


def check_environment(document, expected_tier, layout=DEFAULT_LAYOUT):
    if document.get("tier") != expected_tier:
        raise PreflightError("environment_tier_mismatch: expected " + expected_tier)
    for key, root in (("store_root", layout.store), ("cache_root", layout.cache),
                      ("evidence_root", layout.evidence)):
        value = document.get(key)
        if not _non_empty_string(value):
            raise PreflightError("environment_missing_" + key)
        if Path(value).resolve() != Path(root).resolve():
            raise PreflightError("environment_" + key + "_mismatch: " + value)
    metrics = document.get("metrics_snapshot")
    if not _non_empty_string(metrics):
        raise PreflightError("environment_missing_metrics_snapshot")
    if Path(metrics).resolve() != Path(layout.metrics).resolve():
        raise PreflightError("environment_metrics_snapshot_mismatch: " + metrics)
    timeout = document.get("timeout", 10)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) \
            or not 0.1 <= timeout <= 20:
        raise PreflightError("environment_timeout_out_of_bounds")
    overrides = document.get("profile_overrides")
    if overrides is not None and overrides != {}:
        raise PreflightError("environment_profile_overrides_not_allowed")
    return document


def _check_case_list(document):
    cases = document.get("cases")
    if not isinstance(cases, list) or not cases:
        raise PreflightError("campaign_cases_missing")
    observed = {}
    for case in cases:
        if not isinstance(case, dict):
            raise PreflightError("campaign_case_not_object")
        identifier = case.get("id")
        concurrency = case.get("concurrency")
        if not _non_empty_string(identifier):
            raise PreflightError("campaign_case_id_missing")
        if not isinstance(concurrency, list) or not concurrency \
                or any(isinstance(value, bool) or not isinstance(value, int) or value <= 0
                       for value in concurrency):
            raise PreflightError("campaign_case_concurrency_invalid: " + identifier)
        if identifier in observed:
            raise PreflightError("campaign_case_duplicate: " + identifier)
        observed[identifier] = concurrency
    return observed


def check_campaign(document, mode):
    if not _schema_is_one(document):
        raise PreflightError("campaign_schema_must_be_1")
    if not _non_empty_string(document.get("id")):
        raise PreflightError("campaign_id_missing")
    tier = "G1" if mode == "quick" else "G2"
    if document.get("tier") != tier:
        raise PreflightError("campaign_tier_mismatch: expected " + tier)
    pairs = QUICK_PAIRS if mode == "quick" else FORMAL_PAIRS
    order = QUICK_ORDER if mode == "quick" else FORMAL_ORDER
    if type(document.get("pairs")) is not int or document.get("pairs") != pairs:
        raise PreflightError("campaign_pairs_mismatch: expected " + str(pairs))
    if document.get("order") != order:
        raise PreflightError("campaign_order_mismatch")
    expected_cases = QUICK_CASES if mode == "quick" else FORMAL_CASES
    observed_cases = _check_case_list(document)
    if observed_cases != expected_cases:
        raise PreflightError("campaign_cases_mismatch: " + json.dumps(observed_cases,
                                                                     sort_keys=True))
    arms = document.get("arms")
    if not isinstance(arms, dict) or sorted(arms) != list(TRUST_KEYS):
        raise PreflightError("campaign_arms_must_be_A_and_B")
    for label, role in (("A", "candidate"), ("B", "denominator")):
        arm = arms.get(label)
        if not isinstance(arm, dict):
            raise PreflightError("campaign_arm_missing: " + label)
        if arm.get("role") != role:
            raise PreflightError("campaign_arm_role_mismatch: " + label)
        for key in ("binary", "version", "commit"):
            if not _non_empty_string(arm.get(key)):
                raise PreflightError("campaign_arm_" + label + "_missing_" + key)
    expected = document.get("expected_denominator")
    if not isinstance(expected, dict):
        raise PreflightError("campaign_expected_denominator_missing")
    denominator = arms["B"]
    for key in ("version", "commit"):
        if not _non_empty_string(expected.get(key)) or expected[key] != denominator[key]:
            raise PreflightError("campaign_expected_denominator_mismatch: " + key)
    return expected_cases


def load_trust(path):
    document = _read_json(path, "trusted_artifacts")
    if not _schema_is_one(document):
        raise PreflightError("trusted_artifacts_schema_must_be_1")
    for label in TRUST_KEYS:
        value = document.get(label)
        if not isinstance(value, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", value):
            raise PreflightError("trusted_artifacts_bad_sha256: " + label)
    return {label: document[label].lower() for label in TRUST_KEYS}


def _sha256(path):
    with open(path, "rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def check_arm_binaries(campaign, campaign_path, trust, artifacts_root):
    artifacts_root = Path(artifacts_root).resolve()
    arms = campaign["arms"]
    observed = {}
    for label in TRUST_KEYS:
        binary = Path(arms[label]["binary"])
        if not binary.is_absolute():
            binary = Path(campaign_path).resolve().parent / binary
        try:
            resolved = binary.resolve(strict=True)
        except OSError as failure:
            raise PreflightError("arm_binary_missing: " + label + ": " + str(failure))
        if not resolved.is_relative_to(artifacts_root):
            raise PreflightError("arm_binary_outside_artifacts: " + label + ": " + str(resolved))
        if not resolved.is_file():
            raise PreflightError("arm_binary_not_a_file: " + label + ": " + str(resolved))
        # Check the permission bits rather than os.access: access also reflects
        # a noexec mount, which would make this test depend on the runner's
        # tmpfs options. The mount check below rejects noexec artifacts anyway.
        try:
            mode = resolved.stat().st_mode
        except OSError as failure:
            raise PreflightError("arm_binary_unreadable: " + label + ": " + str(failure))
        if not mode & 0o111:
            raise PreflightError("arm_binary_not_executable: " + label + ": " + str(resolved))
        try:
            digest = _sha256(resolved)
        except OSError as failure:
            raise PreflightError("arm_binary_unreadable: " + label + ": " + str(failure))
        if digest != trust[label]:
            raise PreflightError("arm_binary_sha256_mismatch: " + label)
        observed[label] = {"binary": str(resolved), "sha256": digest}
    return observed


def check_attestation(document, machine=None):
    machine = machine or platform.machine()
    if not _schema_is_one(document):
        raise PreflightError("host_attestation_schema_must_be_1")
    for key in ("native_linux", "persistent_disk", "exclusive_window"):
        if document.get(key) is not True:
            raise PreflightError("host_attestation_" + key + "_must_be_true")
    arch = document.get("arch")
    if not isinstance(arch, str) or normalize_arch(arch) != normalize_arch(machine):
        raise PreflightError("host_attestation_arch_mismatch: "
                             + str(arch) + " vs " + machine)
    for key in ("provisioner", "device"):
        if not _non_empty_string(document.get(key)):
            raise PreflightError("host_attestation_missing_" + key)
    return document


@dataclass(frozen=True)
class RunInputs:
    campaign_path: Path
    environment_path: Path
    trust_path: Path
    artifacts_root: Path
    attestation_path: Path = None
    campaign: dict = field(default_factory=dict)
    environment: dict = field(default_factory=dict)
    trust: dict = field(default_factory=dict)
    attestation: dict = None


def check_inputs(mode, options, layout=DEFAULT_LAYOUT, machine=None):
    if mode not in {"quick", "formal"}:
        raise PreflightError("inputs_only_for_performance_modes")
    campaign_path = _resolve_inside(options.campaign, layout.inputs, "campaign")
    environment_path = _resolve_inside(options.environment, layout.inputs, "environment")
    trust_path = _resolve_inside(options.trusted_artifacts, layout.trust, "trusted_artifacts")
    artifacts_root = _resolve_inside(options.artifacts, layout.artifacts, "artifacts")
    if not artifacts_root.is_dir():
        raise PreflightError("artifacts_not_a_directory: " + str(artifacts_root))
    environment = check_environment(_read_json(environment_path, "environment"),
                                    "G1" if mode == "quick" else "G2", layout)
    campaign = _read_json(campaign_path, "campaign")
    check_campaign(campaign, mode)
    trust = load_trust(trust_path)
    check_arm_binaries(campaign, campaign_path, trust, artifacts_root)
    attestation_path = None
    attestation = None
    if mode == "formal":
        if not getattr(options, "host_attestation", None):
            raise PreflightError("formal_requires_host_attestation")
        attestation_path = _resolve_inside(options.host_attestation, layout.inputs,
                                           "host_attestation")
        attestation = check_attestation(_read_json(attestation_path, "host_attestation"),
                                        machine)
    report_path = layout.reports / (mode + "-gate.json")
    if report_path.exists():
        raise PreflightError("gate_report_exists_refusing_overwrite: " + str(report_path))
    return RunInputs(campaign_path=campaign_path, environment_path=environment_path,
                     trust_path=trust_path, artifacts_root=artifacts_root,
                     attestation_path=attestation_path, campaign=campaign,
                     environment=environment, trust=trust, attestation=attestation)


class FormalLock:
    def __init__(self, path):
        self.path = Path(path)
        self.handle = None

    def acquire(self):
        try:
            import fcntl
        except ImportError:
            raise PreflightError("formal_exclusive_lock_requires_fcntl")
        try:
            self.handle = open(self.path, "a+")
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as failure:
            if self.handle is not None:
                self.handle.close()
                self.handle = None
            raise PreflightError("formal_exclusive_lock_unavailable: "
                                 + str(self.path) + ": " + str(failure))

    def release(self):
        if self.handle is None:
            return
        try:
            import fcntl
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        finally:
            self.handle.close()
            self.handle = None


@dataclass
class Prepared:
    mode: str
    layout: Layout
    identity: dict
    source: dict
    mounts: dict
    inputs: RunInputs = None
    lock: FormalLock = None


def preflight(mode, options, layout=DEFAULT_LAYOUT, system=None, machine=None):
    identity = check_runtime(mode, layout, system=system, machine=machine)
    table = MountTable.read(layout.mountinfo)
    mounts = check_mounts(mode, layout, table)
    check_distinct_roots(mode, layout)
    check_writable_roots(mode, layout)
    try:
        (Path(layout.build) / "tmp").mkdir(parents=True, exist_ok=True)
    except OSError as failure:
        raise PreflightError("build_tmp_not_writable: " + str(failure))
    source = check_source(layout, allow_dirty=getattr(options, "allow_dirty", False))
    inputs = None
    if mode in {"quick", "formal"}:
        inputs = check_inputs(mode, options, layout, machine=machine)
    lock = None
    if mode == "formal":
        lock = FormalLock(layout.store / ".peri-perf.lock")
        lock.acquire()
    return Prepared(mode=mode, layout=layout, identity=identity, source=source,
                    mounts=mounts, inputs=inputs, lock=lock)


@dataclass(frozen=True)
class Command:
    label: str
    argv: tuple
    capture: bool = False
    cwd: Path = None
    env: tuple = ()


def _python():
    return sys.executable or "python3"


def _tree_digest(root):
    digest = hashlib.sha256()
    root = Path(root)
    for path in sorted(root.rglob("*")):
        if "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        relative = path.relative_to(root).as_posix().encode()
        if path.is_symlink():
            digest.update(b"L" + relative + b"\0" + os.readlink(path).encode())
        elif path.is_file():
            digest.update(b"F" + relative + b"\0")
            with open(path, "rb") as handle:
                for chunk in iter(lambda: handle.read(1 << 20), b""):
                    digest.update(chunk)
        elif path.is_dir():
            digest.update(b"D" + relative + b"\0")
    return digest.hexdigest()


def prepare_harness_snapshot(layout=DEFAULT_LAYOUT):
    """Copy the harness to a writable root and verify byte identity.

    ``test_issue8_contract`` intentionally creates a temporary directory under
    ``perf_contract.ROOT`` to exercise the "inside harness sources" guard. The
    mounted checkout is read-only by contract, so the Python suite runs from a
    hash-checked snapshot under /build instead of weakening the source mount.
    """
    source = Path(layout.workspace) / "scripts/perf-reliability"
    if not source.is_dir():
        raise PreflightError("harness_sources_missing: " + str(source))
    try:
        root = Path(tempfile.mkdtemp(prefix="peri-harness-", dir=str(layout.build)))
        destination = root / "scripts/perf-reliability"
        shutil.copytree(source, destination, symlinks=True,
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        # runner.source_inventory reads the prototype tree and asks Git for the
        # Turso file list. Link those read-only inputs instead of copying the
        # multi-gigabyte checkout; the harness itself stays a real writable copy.
        os.symlink(Path(layout.workspace) / "prototypes", root / "prototypes")
        os.symlink(Path(layout.workspace) / ".git", root / ".git")
    except OSError as failure:
        raise PreflightError("harness_snapshot_copy_failed: " + str(failure))
    source_digest = _tree_digest(source)
    snapshot_digest = _tree_digest(destination)
    if source_digest != snapshot_digest:
        raise PreflightError("harness_snapshot_digest_mismatch")
    return root, source_digest, snapshot_digest


def _snapshot_git_environment(layout, harness_root):
    entries = [str(layout.workspace), str(Path(layout.workspace) / "prototypes/p0/turso"),
               str(harness_root), str(Path(harness_root) / "prototypes/p0/turso")]
    values = {"GIT_CONFIG_COUNT": str(len(entries))}
    for index, entry in enumerate(entries):
        values["GIT_CONFIG_KEY_" + str(index)] = "safe.directory"
        values["GIT_CONFIG_VALUE_" + str(index)] = entry
    return tuple(sorted(values.items()))


def plan_test_commands(layout=DEFAULT_LAYOUT, harness_root=None):
    manifest = "prototypes/v1/Cargo.toml"
    snapshot_environment = ()
    if harness_root is not None:
        snapshot_environment = _snapshot_git_environment(layout, harness_root) + (
            ("PERI_REPO_ROOT", str(harness_root)),)
    return [
        Command("harness-unittest",
                (_python(), "-B", "-m", "unittest", "discover", "-s",
                 "scripts/perf-reliability", "-p", "test_*.py", "-v"), capture=True,
                cwd=harness_root, env=snapshot_environment),
        Command("cargo-check",
                ("cargo", "check", "--manifest-path", manifest, "--locked")),
        # The lease tests use a 20s wall-clock window and expired in 2 of 7
        # full runs even with four threads on an emulated, CPU-capped runner.
        # Run lib tests serially so no test starves another; this changes test
        # concurrency only, never the production lease or timeout checks.
        Command("cargo-test-lib",
                ("cargo", "test", "--manifest-path", manifest, "--locked", "--lib",
                 "--", "--test-threads=1")),
        Command("cargo-test-worker-integration",
                ("cargo", "test", "--manifest-path", manifest, "--locked",
                 "--test", "local_provider_worker", "--test", "statement_worker")),
    ]


def plan_campaign_command(mode, inputs, layout=DEFAULT_LAYOUT):
    round_name = "quick" if mode == "quick" else "formal"
    case = "S1" if mode == "quick" else "S1,S2"
    return Command(mode + "-campaign",
                   (_python(), "-B", str(layout.workspace
                                         / "scripts/perf-reliability/campaign_cli.py"),
                    "--campaign", str(inputs.campaign_path),
                    "--environment", str(inputs.environment_path),
                    "--round", round_name, "--case", case,
                    "--metrics-snapshot", str(layout.metrics)))


def plan_gate_command(mode, inputs, layout=DEFAULT_LAYOUT):
    tier = "G1" if mode == "quick" else "G2"
    return Command(mode + "-gate",
                   (_python(), "-B", str(layout.workspace
                                         / "scripts/perf-reliability/gate.py"),
                    "--evidence-root", str(layout.evidence), "--tier", tier,
                    "--profile", mode, "--scope", "native",
                    "--trusted-artifacts", str(inputs.trust_path),
                    "--output", str(layout.reports / (mode + "-gate.json"))),
                   capture=True)


def plan_commands(mode, options, layout=DEFAULT_LAYOUT):
    if mode == "test":
        return plan_test_commands(layout)
    inputs = RunInputs(campaign_path=Path(options.campaign),
                       environment_path=Path(options.environment),
                       trust_path=Path(options.trusted_artifacts),
                       artifacts_root=Path(options.artifacts))
    return [plan_campaign_command(mode, inputs, layout),
            plan_gate_command(mode, inputs, layout)]


_ACTIVE_PROCESS = None


def _handle_signal(signum, frame):
    process = _ACTIVE_PROCESS
    if process is not None and process.poll() is None:
        try:
            process.terminate()
            process.wait(timeout=30)
        except (OSError, subprocess.TimeoutExpired):
            try:
                process.kill()
            except OSError:
                pass
    raise SystemExit(128 + signum)


def install_signal_handlers():
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, _handle_signal)


def run_command(command, layout=DEFAULT_LAYOUT):
    global _ACTIVE_PROCESS
    print("[entrypoint] " + command.label, flush=True)
    print("[entrypoint] $ " + shlex.join(list(command.argv)), flush=True)
    environment = command_environment(layout)
    environment.update(dict(command.env))
    try:
        process = subprocess.Popen(
            list(command.argv), cwd=str(command.cwd or layout.workspace),
            env=environment,
            stdout=subprocess.PIPE if command.capture else None,
            stderr=subprocess.STDOUT if command.capture else None, text=True)
    except OSError as failure:
        print("[entrypoint] command_unavailable: " + str(failure),
              file=sys.stderr, flush=True)
        return 127, ""
    _ACTIVE_PROCESS = process
    output = ""
    try:
        if command.capture:
            output, _ = process.communicate()
            if output:
                print(output, end="" if output.endswith("\n") else "\n", flush=True)
        else:
            process.wait()
    finally:
        _ACTIVE_PROCESS = None
    return process.returncode, output


_UNITTEST_RAN = re.compile(r"^Ran (\d+) tests? in ", re.MULTILINE)
_UNITTEST_VERDICT = re.compile(r"^(OK|FAILED)(?: \((.*)\))?$", re.MULTILINE)


def parse_unittest_summary(output):
    executed = None
    ran = _UNITTEST_RAN.search(output)
    if ran:
        executed = int(ran.group(1))
    failures = errors = skipped = 0
    verdict = _UNITTEST_VERDICT.search(output)
    if verdict:
        details = verdict.group(2) or ""
        for part in details.split(","):
            if "=" not in part:
                continue
            name, value = (item.strip() for item in part.split("=", 1))
            if not value.isdigit():
                continue
            if name == "failures":
                failures = int(value)
            elif name == "errors":
                errors = int(value)
            elif name == "skipped":
                skipped = int(value)
    passed = None if executed is None else executed - failures - errors - skipped
    return {"executed": executed, "passed": passed, "failed": failures + errors,
            "skipped": skipped}


def _entrypoint_digest():
    try:
        return _sha256(__file__)
    except OSError:
        return None


def write_summary(layout, name, document):
    document = dict(document)
    document.setdefault("schema", 1)
    document["generated_at_unix_ms"] = time.time_ns() // 1_000_000
    document["entrypoint_sha256"] = _entrypoint_digest()
    path = Path(layout.reports) / (name + ".json")
    temporary = path.with_name(path.name + ".tmp")
    try:
        temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
        os.replace(temporary, path)
    except OSError as failure:
        print("[entrypoint] summary_write_failed: " + str(path) + ": " + str(failure),
              file=sys.stderr, flush=True)
        return None
    return path


def _gate_report(path):
    try:
        document = json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return document if isinstance(document, dict) else None


def _gate_structure_problems(report, exit_code):
    if not isinstance(report, dict):
        return ["gate_report_missing_or_invalid"]
    problems = []
    if report.get("exit_code") != exit_code:
        problems.append("gate_report_exit_code_mismatch")
    if report.get("waivers") != []:
        problems.append("gate_waivers_present")
    if report.get("evidence_errors") != []:
        problems.append("gate_evidence_errors_present")
    if report.get("foreign_environment") is not False:
        problems.append("gate_foreign_environment")
    if report.get("tier_mismatch") is not False:
        problems.append("gate_tier_mismatch")
    if report.get("tier_downgrade_rejected") is not False:
        problems.append("gate_tier_downgrade_rejected")
    sections = report.get("sections")
    if not isinstance(sections, dict):
        problems.append("gate_sections_missing")
    else:
        for name in SECTION_NAMES:
            section = sections.get(name)
            if not isinstance(section, dict) or section.get("status") != PASS:
                problems.append("gate_section_not_pass:" + name)
    runs = report.get("runs")
    if not isinstance(runs, dict) or not runs:
        problems.append("gate_runs_missing")
    else:
        for key, run in runs.items():
            if not isinstance(run, dict) or run.get("integrity") != []:
                problems.append("gate_run_integrity_failure:" + key)
    return problems


def evaluate_quick_gate(report, exit_code):
    problems = _gate_structure_problems(report, exit_code)
    if not isinstance(report, dict):
        return problems
    if exit_code != 1:
        problems.append("quick_gate_exit_must_be_1")
    if report.get("status") != PHASE_PASS:
        problems.append("quick_gate_status_not_phase_pass")
    if report.get("formal_pass") is not False:
        problems.append("quick_gate_formal_pass_must_be_false")
    if report.get("phase_only") is not True:
        problems.append("quick_gate_phase_only_must_be_true")
    if report.get("tier") != "G1" or report.get("profile") != "quick" \
            or report.get("scope") != "native":
        problems.append("quick_gate_identity_mismatch")
    runs = report.get("runs") if isinstance(report.get("runs"), dict) else {}
    if len(runs) != QUICK_RUNS:
        problems.append("quick_gate_run_count:" + str(len(runs)))
    groups = {}
    for key in runs:
        parts = key.split("/")
        if len(parts) == 4:
            group = parts[0] + "/" + parts[1]
            groups[group] = groups.get(group, 0) + 1
    if groups != {"S1/C2": QUICK_RUNS}:
        problems.append("quick_gate_run_groups_mismatch")
    return problems


def evaluate_formal_gate(report, exit_code):
    problems = _gate_structure_problems(report, exit_code)
    if not isinstance(report, dict):
        return problems
    if exit_code != 0:
        problems.append("formal_gate_exit_must_be_0")
    if report.get("status") != PASS:
        problems.append("formal_gate_status_not_pass")
    if report.get("formal_pass") is not True:
        problems.append("formal_gate_formal_pass_must_be_true")
    if report.get("phase_only") is not False:
        problems.append("formal_gate_phase_only_must_be_false")
    if report.get("tier") != "G2" or report.get("profile") != "formal" \
            or report.get("scope") != "native":
        problems.append("formal_gate_identity_mismatch")
    if report.get("total_pass_tier") != "G2":
        problems.append("formal_gate_total_pass_tier_mismatch")
    runs = report.get("runs") if isinstance(report.get("runs"), dict) else {}
    if len(runs) != FORMAL_RUNS:
        problems.append("formal_gate_run_count:" + str(len(runs)))
    groups = {}
    for key in runs:
        parts = key.split("/")
        if len(parts) == 4:
            group = parts[0] + "/" + parts[1]
            groups[group] = groups.get(group, 0) + 1
    expected = {"S1/C2": 6, "S1/C1": 6, "S2/C2": 6}
    if groups != expected:
        problems.append("formal_gate_run_groups_mismatch")
    return problems


def _base_summary(prepared, status):
    return {"schema": 1, "mode": prepared.mode, "status": status,
            "identity": prepared.identity, "source": prepared.source,
            "mounts": {label: {"fstype": entry.fstype, "read_only": entry.read_only}
                       for label, entry in prepared.mounts.items()}}


def run_test(prepared):
    layout = prepared.layout
    snapshot_root, source_digest, snapshot_digest = prepare_harness_snapshot(layout)
    steps = []
    passed = True
    for command in plan_test_commands(layout, harness_root=snapshot_root):
        code, output = run_command(command, layout)
        step = {"name": command.label, "exit_code": code}
        if command.label == "harness-unittest":
            step.update(parse_unittest_summary(output))
        steps.append(step)
        if code != 0:
            passed = False
            print("[entrypoint] step failed: " + command.label + " exit=" + str(code),
                  file=sys.stderr, flush=True)
            break
    if passed and prepared.source:
        try:
            after = check_source(
                layout, allow_dirty=bool(prepared.source.get("dirty_allowed")))
        except PreflightError as failure:
            passed = False
            steps.append({"name": "source-integrity", "exit_code": 1,
                          "error": str(failure)})
            print("[entrypoint] source integrity failed: " + str(failure),
                  file=sys.stderr, flush=True)
        else:
            if after != prepared.source:
                passed = False
                steps.append({"name": "source-integrity", "exit_code": 1,
                              "error": "workspace_source_changed"})
            else:
                steps.append({"name": "source-integrity", "exit_code": 0})
    summary = _base_summary(prepared, "PASS" if passed else "FAIL")
    summary["steps"] = steps
    summary["harness_snapshot"] = {"root": str(snapshot_root),
                                   "source_sha256": source_digest,
                                   "snapshot_sha256": snapshot_digest}
    path = write_summary(layout, "test-summary", summary)
    print(json.dumps({"mode": "test", "status": summary["status"],
                      "steps": steps, "summary": str(path) if path else None}), flush=True)
    return 0 if passed and path is not None else 1


def run_performance(prepared, mode):
    layout = prepared.layout
    inputs = prepared.inputs
    campaign_code, _ = run_command(plan_campaign_command(mode, inputs, layout), layout)
    summary = _base_summary(prepared, "CAMPAIGN_FAILED")
    summary.update(campaign_exit=campaign_code,
                   campaign_path=str(inputs.campaign_path),
                   trusted_artifacts=str(inputs.trust_path),
                   artifacts_root=str(inputs.artifacts_root))
    if campaign_code != 0:
        path = write_summary(layout, mode + "-summary", summary)
        print(json.dumps({"mode": mode, "accepted": False,
                          "campaign_exit": campaign_code,
                          "summary": str(path) if path else None,
                          "error": "campaign_failed"}), flush=True)
        return 1
    gate_code, _ = run_command(plan_gate_command(mode, inputs, layout), layout)
    report_path = layout.reports / (mode + "-gate.json")
    report = _gate_report(report_path)
    problems = (evaluate_quick_gate(report, gate_code) if mode == "quick"
                else evaluate_formal_gate(report, gate_code))
    accepted = not problems
    summary["status"] = ("PHASE_PASS_ACCEPTED" if accepted and mode == "quick"
                         else "PASS" if accepted else "REJECTED")
    summary.update(campaign_exit=campaign_code, gate_exit=gate_code,
                   gate_status=(report or {}).get("status"),
                   formal_pass=(report or {}).get("formal_pass"),
                   phase_only=(report or {}).get("phase_only"),
                   gate_report=str(report_path), accepted=accepted, problems=problems)
    path = write_summary(layout, mode + "-summary", summary)
    print(json.dumps({"mode": mode, "accepted": accepted, "campaign_exit": campaign_code,
                      "gate_exit": gate_code, "gate_status": summary["gate_status"],
                      "problems": problems, "summary": str(path) if path else None}),
          flush=True)
    return 0 if accepted and path is not None else 1


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(
        description="Issue 8 native performance harness container entrypoint")
    parser.add_argument("--version", action="version", version="peri-issue8-ci 1")
    parser.add_argument("--dry-run", action="store_true",
                        help="print planned commands without validating mounts or running")
    subparsers = parser.add_subparsers(dest="mode")
    common = argparse.ArgumentParser(add_help=False)
    # SUPPRESS keeps the global --dry-run value when the flag is given before
    # the mode; argparse would otherwise reset it to False from the subparser.
    common.add_argument("--dry-run", action="store_true", default=argparse.SUPPRESS,
                        help=argparse.SUPPRESS)
    test = subparsers.add_parser("test", parents=[common],
                                 help="deterministic harness and Rust tests")
    test.add_argument("--allow-dirty", action="store_true",
                      help="diagnostic only: accept a dirty checkout in test mode")
    for mode in ("quick", "formal"):
        subparser = subparsers.add_parser(mode, parents=[common],
                                          help=mode + " campaign and gate")
        subparser.add_argument("--campaign", required=True,
                               help="/inputs campaign JSON")
        subparser.add_argument("--environment", required=True,
                               help="/inputs environment JSON")
        subparser.add_argument("--trusted-artifacts", required=True,
                               help="/trust arm sha256 manifest")
        subparser.add_argument("--artifacts", required=True,
                               help="/artifacts candidate/denominator binaries")
        if mode == "formal":
            subparser.add_argument("--host-attestation", required=True,
                                   help="/inputs operator host attestation JSON")
    arguments = parser.parse_args(argv)
    if arguments.mode is None:
        arguments.mode = "test"
        arguments.allow_dirty = False
    return arguments


def main(argv=None):
    arguments = parse_arguments(argv)
    layout = DEFAULT_LAYOUT
    if arguments.dry_run:
        print("# dry-run mode=" + arguments.mode, flush=True)
        for command in plan_commands(arguments.mode, arguments, layout):
            print("$ " + shlex.join(list(command.argv)), flush=True)
        return 0
    install_signal_handlers()
    prepared = None
    try:
        prepared = preflight(arguments.mode, arguments, layout)
        if arguments.mode == "test":
            return run_test(prepared)
        return run_performance(prepared, arguments.mode)
    except PreflightError as failure:
        document = {"status": NOT_RUN, "mode": arguments.mode, "error": str(failure)}
        if layout.reports.is_dir():
            write_summary(layout, arguments.mode + "-summary", document)
        print(json.dumps(document), flush=True)
        return 2
    finally:
        if prepared is not None and prepared.lock is not None:
            prepared.lock.release()


if __name__ == "__main__":
    sys.exit(main())
