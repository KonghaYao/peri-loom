import contextlib
import hashlib
import io
import json
import os
import sys
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from ci import entrypoint  # noqa: E402


def make_layout(root):
    root = Path(root)
    return entrypoint.Layout(
        workspace=root / "workspace",
        cargo_cache=root / "cargo-cache",
        cargo_target=root / "cargo-target",
        build=root / "build",
        store=root / "perf/store",
        cache=root / "perf/cache",
        evidence=root / "perf/evidence",
        metrics=root / "perf/metrics",
        reports=root / "reports",
        inputs=root / "inputs",
        artifacts=root / "artifacts",
        trust=root / "trust",
        mountinfo=root / "mountinfo",
    )


LAYOUT_PATHS = ("workspace", "cargo_cache", "cargo_target", "build", "store", "cache",
                "evidence", "metrics", "reports", "inputs", "artifacts", "trust")


def create_layout_dirs(layout):
    for name in LAYOUT_PATHS:
        getattr(layout, name).mkdir(parents=True, exist_ok=True)


def mount_line(index, mountpoint, fstype, options):
    return f"{index} 1 8:1 / {mountpoint} {options} - {fstype} device {options}"


def write_mountinfo(layout, overrides=None):
    overrides = overrides or {}
    specs = {
        "workspace": (layout.workspace, "ext4", "ro"),
        "cargo_cache": (layout.cargo_cache, "ext4", "rw"),
        "cargo_target": (layout.cargo_target, "ext4", "rw"),
        "build": (layout.build, "ext4", "rw"),
        "store": (layout.store, "ext4", "rw"),
        "cache": (layout.cache, "ext4", "rw"),
        "evidence": (layout.evidence, "ext4", "rw"),
        "metrics": (layout.metrics, "ext4", "rw"),
        "reports": (layout.reports, "ext4", "rw"),
        "inputs": (layout.inputs, "ext4", "ro"),
        "artifacts": (layout.artifacts, "ext4", "ro"),
        "trust": (layout.trust, "ext4", "ro"),
    }
    lines = []
    for index, (name, (path, fstype, options)) in enumerate(specs.items(), 1):
        fstype, options = overrides.get(name, (fstype, options))
        lines.append(mount_line(index, path, fstype, options))
    lines.append(mount_line(99, "/", "overlay", "rw"))
    layout.mountinfo.write_text("\n".join(lines) + "\n")
    return entrypoint.parse_mountinfo(layout.mountinfo.read_text())


def sha256(path):
    with open(path, "rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def quick_campaign(binary_a="/artifacts/A/native", binary_b="/artifacts/B/native"):
    return {
        "schema": 1,
        "id": "issue8-quick-test",
        "tier": "G1",
        "pairs": 1,
        "order": ["AB"],
        "expected_denominator": {"version": "v1.1.11",
                                 "commit": "f3c75622924d84189fa74f3069c209761ceeec31"},
        "arms": {
            "A": {"role": "candidate", "binary": binary_a, "version": "candidate",
                  "commit": "candidate-commit"},
            "B": {"role": "denominator", "binary": binary_b, "version": "v1.1.11",
                  "commit": "f3c75622924d84189fa74f3069c209761ceeec31"},
        },
        "cases": [{"id": "S1", "concurrency": [2]}],
    }


def formal_campaign(binary_a="/artifacts/A/native", binary_b="/artifacts/B/native"):
    document = quick_campaign(binary_a, binary_b)
    document.update(tier="G2", pairs=3, order=["AB", "BA", "AB"],
                    cases=[{"id": "S1", "concurrency": [2, 1]},
                           {"id": "S2", "concurrency": [2]}])
    return document


def section(status="PASS"):
    return {"status": status, "reasons": []}


def quick_runs():
    return {"S1/C2/pair0/A": {"integrity": []},
            "S1/C2/pair0/B": {"integrity": []}}


def formal_runs():
    runs = {}
    for group in ("S1/C2", "S1/C1", "S2/C2"):
        for pair in range(3):
            for arm in ("A", "B"):
                runs[group + "/pair" + str(pair) + "/" + arm] = {"integrity": []}
    return runs


def quick_report(**overrides):
    report = {
        "schema": 1, "status": "PHASE_PASS", "exit_code": 1,
        "formal_pass": False, "phase_only": True, "total_pass_tier": "G2",
        "tier": "G1", "profile": "quick", "scope": "native",
        "tier_mismatch": False, "tier_downgrade_rejected": False,
        "foreign_environment": False, "waivers": [], "evidence_errors": [],
        "sections": {name: section() for name in entrypoint.SECTION_NAMES},
        "runs": quick_runs(),
    }
    report.update(overrides)
    return report


def formal_report(**overrides):
    report = {
        "schema": 1, "status": "PASS", "exit_code": 0,
        "formal_pass": True, "phase_only": False, "total_pass_tier": "G2",
        "tier": "G2", "profile": "formal", "scope": "native",
        "tier_mismatch": False, "tier_downgrade_rejected": False,
        "foreign_environment": False, "waivers": [], "evidence_errors": [],
        "sections": {name: section() for name in entrypoint.SECTION_NAMES},
        "runs": formal_runs(),
    }
    report.update(overrides)
    return report


class MountInfoTests(unittest.TestCase):
    def test_unescape_mount_paths(self):
        self.assertEqual(entrypoint._unescape_mount("/perf/a\\040b"), "/perf/a b")

    def test_longest_mount_point_wins(self):
        table = entrypoint.parse_mountinfo("\n".join([
            mount_line(1, "/", "overlay", "rw"),
            mount_line(2, "/perf", "ext4", "rw"),
            mount_line(3, "/perf/store", "tmpfs", "rw"),
        ]))
        self.assertEqual(table.for_path("/perf/store").fstype, "tmpfs")
        self.assertEqual(table.for_path("/perf/cache").fstype, "ext4")
        self.assertEqual(table.for_path("/elsewhere").fstype, "overlay")

    def test_malformed_lines_are_ignored(self):
        table = entrypoint.parse_mountinfo("garbage\n" + mount_line(1, "/", "ext4", "rw"))
        self.assertEqual(len(table.entries), 1)


class MountValidationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.layout = make_layout(self.temporary.name)
        create_layout_dirs(self.layout)

    def tearDown(self):
        self.temporary.cleanup()

    def test_quick_and_formal_accept_persistent_mounts(self):
        table = write_mountinfo(self.layout)
        entrypoint.check_mounts("quick", self.layout, table)
        entrypoint.check_mounts("formal", self.layout, table)

    def test_workspace_must_be_read_only(self):
        table = write_mountinfo(self.layout, {"workspace": ("ext4", "rw")})
        with self.assertRaisesRegex(entrypoint.PreflightError, "workspace_mount_not_read_only"):
            entrypoint.check_mounts("quick", self.layout, table)

    def test_tmpfs_and_overlay_are_rejected(self):
        for name, fstype in (("store", "tmpfs"), ("cache", "ramfs"),
                             ("evidence", "overlay"), ("metrics", "devtmpfs")):
            table = write_mountinfo(self.layout, {name: (fstype, "rw")})
            with self.assertRaisesRegex(entrypoint.PreflightError,
                                        name + "_root_on_forbidden_filesystem"):
                entrypoint.check_mounts("quick", self.layout, table)

    def test_quick_rejects_shared_filesystems(self):
        table = write_mountinfo(self.layout, {"cache": ("virtiofs", "rw")})
        with self.assertRaisesRegex(entrypoint.PreflightError,
                                    "cache_root_on_forbidden_filesystem"):
            entrypoint.check_mounts("quick", self.layout, table)

    def test_formal_requires_ext4_or_xfs(self):
        table = write_mountinfo(self.layout, {"store": ("btrfs", "rw")})
        with self.assertRaisesRegex(entrypoint.PreflightError,
                                    "store_root_not_formal_filesystem"):
            entrypoint.check_mounts("formal", self.layout, table)

    def test_inputs_artifacts_and_trust_must_be_read_only(self):
        for name in ("inputs", "artifacts", "trust"):
            table = write_mountinfo(self.layout, {name: ("ext4", "rw")})
            with self.assertRaisesRegex(entrypoint.PreflightError,
                                        name + "_mount_not_read_only"):
                entrypoint.check_mounts("quick", self.layout, table)

    def test_artifacts_mount_must_not_be_noexec(self):
        table = write_mountinfo(self.layout, {"artifacts": ("ext4", "ro,noexec")})
        with self.assertRaisesRegex(entrypoint.PreflightError, "artifacts_mount_noexec"):
            entrypoint.check_mounts("quick", self.layout, table)

    def test_missing_mount_is_rejected(self):
        self.layout.mountinfo.write_text("")
        table = entrypoint.parse_mountinfo("")
        with self.assertRaisesRegex(entrypoint.PreflightError, "workspace_mount_not_found"):
            entrypoint.check_mounts("quick", self.layout, table)


class WritableRootTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.layout = make_layout(self.temporary.name)
        create_layout_dirs(self.layout)

    def tearDown(self):
        self.temporary.cleanup()

    def test_quick_accepts_empty_evidence_and_metrics(self):
        entrypoint.check_writable_roots("quick", self.layout)
        self.assertEqual(list(self.layout.evidence.iterdir()), [])
        self.assertEqual(list(self.layout.metrics.iterdir()), [])

    def test_non_empty_evidence_is_rejected(self):
        (self.layout.evidence / "run").mkdir()
        with self.assertRaisesRegex(entrypoint.PreflightError, "evidence_root_must_be_empty"):
            entrypoint.check_writable_roots("quick", self.layout)

    def test_non_empty_metrics_is_rejected(self):
        (self.layout.metrics / "snapshot.json").write_text("{}")
        with self.assertRaisesRegex(entrypoint.PreflightError, "metrics_root_must_be_empty"):
            entrypoint.check_writable_roots("quick", self.layout)

    def test_missing_writable_directory_is_rejected(self):
        self.layout.build.rmdir()
        with self.assertRaisesRegex(entrypoint.PreflightError, "build_not_a_directory"):
            entrypoint.check_writable_roots("quick", self.layout)

    def test_test_mode_requires_cargo_directories(self):
        self.layout.cargo_cache.rmdir()
        with self.assertRaisesRegex(entrypoint.PreflightError,
                                    "cargo_cache_not_a_directory"):
            entrypoint.check_writable_roots("test", self.layout)


class EnvironmentTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.layout = make_layout(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def environment(self):
        return {"tier": "G1", "store_root": str(self.layout.store),
                "cache_root": str(self.layout.cache),
                "evidence_root": str(self.layout.evidence),
                "metrics_snapshot": str(self.layout.metrics), "timeout": 10}

    def test_valid_environment(self):
        document = self.environment()
        self.assertIs(entrypoint.check_environment(document, "G1", self.layout), document)

    def test_roots_must_match_the_container_layout(self):
        document = self.environment()
        document["store_root"] = "/tmp/store"
        with self.assertRaisesRegex(entrypoint.PreflightError,
                                    "environment_store_root_mismatch"):
            entrypoint.check_environment(document, "G1", self.layout)

    def test_metrics_snapshot_is_required(self):
        document = self.environment()
        del document["metrics_snapshot"]
        with self.assertRaisesRegex(entrypoint.PreflightError,
                                    "environment_missing_metrics_snapshot"):
            entrypoint.check_environment(document, "G1", self.layout)

    def test_profile_overrides_are_not_allowed(self):
        document = self.environment()
        document["profile_overrides"] = {"pairs": 1}
        with self.assertRaisesRegex(entrypoint.PreflightError,
                                    "environment_profile_overrides_not_allowed"):
            entrypoint.check_environment(document, "G1", self.layout)

    def test_timeout_bounds(self):
        document = self.environment()
        document["timeout"] = 60
        with self.assertRaisesRegex(entrypoint.PreflightError,
                                    "environment_timeout_out_of_bounds"):
            entrypoint.check_environment(document, "G1", self.layout)


class CampaignTests(unittest.TestCase):
    def test_quick_campaign_shape(self):
        entrypoint.check_campaign(quick_campaign(), "quick")
        document = quick_campaign()
        document["pairs"] = 3
        with self.assertRaisesRegex(entrypoint.PreflightError, "campaign_pairs_mismatch"):
            entrypoint.check_campaign(document, "quick")

    def test_formal_campaign_shape(self):
        entrypoint.check_campaign(formal_campaign(), "formal")
        document = formal_campaign()
        document["cases"] = [{"id": "S1", "concurrency": [2]},
                             {"id": "S2", "concurrency": [2]}]
        with self.assertRaisesRegex(entrypoint.PreflightError, "campaign_cases_mismatch"):
            entrypoint.check_campaign(document, "formal")

    def test_arm_roles_and_denominator_are_enforced(self):
        document = quick_campaign()
        document["arms"]["B"]["role"] = "candidate"
        with self.assertRaisesRegex(entrypoint.PreflightError,
                                    "campaign_arm_role_mismatch"):
            entrypoint.check_campaign(document, "quick")
        document = quick_campaign()
        document["expected_denominator"]["commit"] = "other"
        with self.assertRaisesRegex(entrypoint.PreflightError,
                                    "campaign_expected_denominator_mismatch"):
            entrypoint.check_campaign(document, "quick")

    def test_schema_must_be_the_integer_one(self):
        document = quick_campaign()
        document["schema"] = True
        with self.assertRaisesRegex(entrypoint.PreflightError,
                                    "campaign_schema_must_be_1"):
            entrypoint.check_campaign(document, "quick")


class AttestationTests(unittest.TestCase):
    def attestation(self, **overrides):
        document = {"schema": 1, "native_linux": True, "persistent_disk": True,
                    "exclusive_window": True, "arch": "amd64",
                    "provisioner": "perf-runner", "device": "nvme0n1"}
        document.update(overrides)
        return document

    def test_valid_attestation(self):
        entrypoint.check_attestation(self.attestation(), "x86_64")

    def test_false_claims_are_rejected(self):
        for key in ("native_linux", "persistent_disk", "exclusive_window"):
            with self.assertRaisesRegex(entrypoint.PreflightError,
                                        "host_attestation_" + key + "_must_be_true"):
                entrypoint.check_attestation(self.attestation(**{key: False}), "amd64")

    def test_arch_mismatch_is_rejected(self):
        with self.assertRaisesRegex(entrypoint.PreflightError,
                                    "host_attestation_arch_mismatch"):
            entrypoint.check_attestation(self.attestation(), "aarch64")

    def test_provisioner_is_required(self):
        with self.assertRaisesRegex(entrypoint.PreflightError,
                                    "host_attestation_missing_provisioner"):
            entrypoint.check_attestation(self.attestation(provisioner=""), "amd64")


class InputBundleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.layout = make_layout(self.temporary.name)
        create_layout_dirs(self.layout)
        self.binaries = {}
        for label in ("A", "B"):
            directory = self.layout.artifacts / label
            directory.mkdir()
            binary = directory / "native"
            binary.write_bytes(("binary-" + label).encode())
            binary.chmod(0o755)
            self.binaries[label] = binary
        self.trust = self.layout.trust / "trusted-artifacts.json"
        self.trust.write_text(json.dumps({
            "schema": 1,
            "A": sha256(self.binaries["A"]),
            "B": sha256(self.binaries["B"])}))
        self.environment = self.layout.inputs / "quick-environment.json"
        self.environment.write_text(json.dumps({
            "tier": "G1", "store_root": str(self.layout.store),
            "cache_root": str(self.layout.cache),
            "evidence_root": str(self.layout.evidence),
            "metrics_snapshot": str(self.layout.metrics), "timeout": 10}))
        self.campaign = self.layout.inputs / "quick-campaign.json"
        self.campaign.write_text(json.dumps(quick_campaign(
            str(self.binaries["A"]), str(self.binaries["B"]))))
        self.attestation = self.layout.inputs / "host-attestation.json"
        self.attestation.write_text(json.dumps({
            "schema": 1, "native_linux": True, "persistent_disk": True,
            "exclusive_window": True, "arch": "amd64",
            "provisioner": "perf-runner", "device": "nvme0n1"}))

    def tearDown(self):
        self.temporary.cleanup()

    def options(self, mode):
        return Namespace(campaign=str(self.campaign), environment=str(self.environment),
                         trusted_artifacts=str(self.trust),
                         artifacts=str(self.layout.artifacts),
                         host_attestation=str(self.attestation),
                         dry_run=False, allow_dirty=False, mode=mode)

    def test_quick_bundle_is_accepted(self):
        inputs = entrypoint.check_inputs("quick", self.options("quick"), self.layout)
        self.assertEqual(inputs.trust, {"A": sha256(self.binaries["A"]),
                                        "B": sha256(self.binaries["B"])})

    def test_campaign_outside_inputs_is_rejected(self):
        outside = Path(self.temporary.name) / "campaign.json"
        outside.write_text(self.campaign.read_text())
        options = self.options("quick")
        options.campaign = str(outside)
        with self.assertRaisesRegex(entrypoint.PreflightError, "campaign_outside_allowed_mount"):
            entrypoint.check_inputs("quick", options, self.layout)

    def test_artifact_hash_mismatch_is_rejected(self):
        self.trust.write_text(json.dumps({"schema": 1, "A": "0" * 64, "B": "0" * 64}))
        with self.assertRaisesRegex(entrypoint.PreflightError,
                                    "arm_binary_sha256_mismatch"):
            entrypoint.check_inputs("quick", self.options("quick"), self.layout)

    def test_non_executable_artifact_is_rejected(self):
        self.binaries["A"].chmod(0o644)
        with self.assertRaisesRegex(entrypoint.PreflightError,
                                    "arm_binary_not_executable"):
            entrypoint.check_inputs("quick", self.options("quick"), self.layout)

    def test_arm_binary_outside_artifacts_is_rejected(self):
        outside = Path(self.temporary.name) / "outside-native"
        outside.write_bytes(b"outside")
        outside.chmod(0o755)
        self.campaign.write_text(json.dumps(quick_campaign(
            str(outside), str(self.binaries["B"]))))
        self.trust.write_text(json.dumps({
            "schema": 1, "A": sha256(outside), "B": sha256(self.binaries["B"])}))
        with self.assertRaisesRegex(entrypoint.PreflightError,
                                    "arm_binary_outside_artifacts"):
            entrypoint.check_inputs("quick", self.options("quick"), self.layout)

    def test_formal_bundle_requires_attestation(self):
        self.environment.write_text(json.dumps({
            "tier": "G2", "store_root": str(self.layout.store),
            "cache_root": str(self.layout.cache),
            "evidence_root": str(self.layout.evidence),
            "metrics_snapshot": str(self.layout.metrics), "timeout": 10}))
        self.campaign.write_text(json.dumps(formal_campaign(
            str(self.binaries["A"]), str(self.binaries["B"]))))
        options = self.options("formal")
        options.host_attestation = None
        with self.assertRaisesRegex(entrypoint.PreflightError,
                                    "formal_requires_host_attestation"):
            entrypoint.check_inputs("formal", options, self.layout)

    def test_formal_bundle_is_accepted_with_attestation(self):
        self.environment.write_text(json.dumps({
            "tier": "G2", "store_root": str(self.layout.store),
            "cache_root": str(self.layout.cache),
            "evidence_root": str(self.layout.evidence),
            "metrics_snapshot": str(self.layout.metrics), "timeout": 10}))
        self.campaign.write_text(json.dumps(formal_campaign(
            str(self.binaries["A"]), str(self.binaries["B"]))))
        inputs = entrypoint.check_inputs("formal", self.options("formal"), self.layout,
                                         machine="amd64")
        self.assertEqual(inputs.attestation["provisioner"], "perf-runner")


class RuntimeAndSourceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.layout = make_layout(self.temporary.name)
        create_layout_dirs(self.layout)

    def tearDown(self):
        self.temporary.cleanup()

    def test_non_linux_host_is_rejected(self):
        with self.assertRaisesRegex(entrypoint.PreflightError,
                                    "runtime_not_native_linux"):
            entrypoint.check_runtime("quick", self.layout, system="Darwin",
                                     machine="arm64")

    def test_root_is_rejected(self):
        with patch.object(entrypoint.os, "geteuid", return_value=0):
            with self.assertRaisesRegex(entrypoint.PreflightError,
                                        "runtime_must_not_run_as_root"):
                entrypoint.check_runtime("quick", self.layout, system="Linux",
                                         machine="aarch64")

    def test_incomplete_workspace_is_rejected(self):
        with self.assertRaisesRegex(entrypoint.PreflightError, "workspace_incomplete"):
            entrypoint.check_source(self.layout)


class EnvironmentAllowlistTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.layout = make_layout(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def test_credentials_never_reach_subprocesses(self):
        with patch.dict(os.environ, {
                "AWS_SECRET_ACCESS_KEY": "aws-secret",
                "S3_SECRET_ACCESS_KEY": "s3-secret",
                "GITHUB_TOKEN": "github-token"}, clear=False):
            environment = entrypoint.command_environment(self.layout)
        for name in ("AWS_SECRET_ACCESS_KEY", "S3_SECRET_ACCESS_KEY", "GITHUB_TOKEN"):
            self.assertNotIn(name, environment)
        self.assertEqual(environment["GIT_CONFIG_VALUE_0"], str(self.layout.workspace))
        self.assertEqual(environment["GIT_CONFIG_COUNT"], "2")
        self.assertEqual(environment["TMPDIR"], str(self.layout.build / "tmp"))
        self.assertEqual(environment["PERI_REPO_ROOT"], str(self.layout.workspace))
        self.assertEqual(environment["RUSTUP_TOOLCHAIN"], entrypoint.EXPECTED_RUST)

    def test_host_toolchain_override_does_not_reach_subprocesses(self):
        with patch.dict(os.environ, {"RUSTUP_TOOLCHAIN": "nightly"}, clear=False):
            environment = entrypoint.command_environment(self.layout)
        self.assertEqual(environment["RUSTUP_TOOLCHAIN"], entrypoint.EXPECTED_RUST)


class GateVerdictTests(unittest.TestCase):
    def test_quick_phase_pass_is_accepted(self):
        self.assertEqual(entrypoint.evaluate_quick_gate(quick_report(), 1), [])

    def test_quick_formal_pass_is_not_accepted(self):
        problems = entrypoint.evaluate_quick_gate(
            quick_report(status="PASS", exit_code=0, formal_pass=True, phase_only=False), 0)
        self.assertIn("quick_gate_exit_must_be_1", problems)
        self.assertIn("quick_gate_status_not_phase_pass", problems)
        self.assertIn("quick_gate_formal_pass_must_be_false", problems)
        self.assertIn("quick_gate_phase_only_must_be_true", problems)

    def test_quick_fail_and_not_run_are_rejected(self):
        for status in ("FAIL", "NOT_RUN", "INCOMPLETE"):
            problems = entrypoint.evaluate_quick_gate(
                quick_report(status=status), 1)
            self.assertIn("quick_gate_status_not_phase_pass", problems)

    def test_quick_waiver_is_rejected(self):
        problems = entrypoint.evaluate_quick_gate(
            quick_report(waivers=[{"section": "attribution"}]), 1)
        self.assertIn("gate_waivers_present", problems)

    def test_quick_section_failure_is_rejected(self):
        report = quick_report()
        report["sections"]["resources"] = section("NOT_RUN")
        problems = entrypoint.evaluate_quick_gate(report, 1)
        self.assertIn("gate_section_not_pass:resources", problems)

    def test_quick_run_count_and_integrity_are_checked(self):
        report = quick_report(runs={"S1/C2/pair0/A": {"integrity": []}})
        problems = entrypoint.evaluate_quick_gate(report, 1)
        self.assertIn("quick_gate_run_count:1", problems)
        report = quick_report()
        report["runs"]["S1/C2/pair0/A"]["integrity"] = ["broken"]
        problems = entrypoint.evaluate_quick_gate(report, 1)
        self.assertIn("gate_run_integrity_failure:S1/C2/pair0/A", problems)
        report = quick_report()
        del report["runs"]["S1/C2/pair0/B"]["integrity"]
        problems = entrypoint.evaluate_quick_gate(report, 1)
        self.assertIn("gate_run_integrity_failure:S1/C2/pair0/B", problems)

    def test_missing_report_does_not_crash(self):
        self.assertIn("gate_report_missing_or_invalid",
                      entrypoint.evaluate_quick_gate(None, 1))
        self.assertIn("gate_report_missing_or_invalid",
                      entrypoint.evaluate_formal_gate(None, 0))

    def test_formal_pass_is_accepted(self):
        self.assertEqual(entrypoint.evaluate_formal_gate(formal_report(), 0), [])

    def test_formal_phase_pass_is_rejected(self):
        problems = entrypoint.evaluate_formal_gate(
            formal_report(status="PHASE_PASS", exit_code=1, formal_pass=False,
                          phase_only=True), 1)
        self.assertIn("formal_gate_exit_must_be_0", problems)
        self.assertIn("formal_gate_status_not_pass", problems)
        self.assertIn("formal_gate_formal_pass_must_be_true", problems)
        self.assertIn("formal_gate_phase_only_must_be_false", problems)

    def test_formal_requires_all_eighteen_runs(self):
        runs = formal_runs()
        runs.pop("S2/C2/pair2/B")
        problems = entrypoint.evaluate_formal_gate(formal_report(runs=runs), 0)
        self.assertIn("formal_gate_run_count:17", problems)
        self.assertIn("formal_gate_run_groups_mismatch", problems)

    def test_formal_waiver_and_foreign_environment_are_rejected(self):
        problems = entrypoint.evaluate_formal_gate(
            formal_report(waivers=[{"section": "environment"}],
                          foreign_environment=True), 0)
        self.assertIn("gate_waivers_present", problems)
        self.assertIn("gate_foreign_environment", problems)


class PlanTests(unittest.TestCase):
    def setUp(self):
        self.layout = entrypoint.Layout()

    def test_test_plan_is_ordered_and_locked(self):
        commands = entrypoint.plan_test_commands(self.layout)
        self.assertEqual([command.label for command in commands],
                         ["harness-unittest", "cargo-check", "cargo-test-lib",
                          "cargo-test-worker-integration"])
        for command in commands:
            self.assertIsInstance(command.argv, tuple)
        self.assertIsNone(commands[0].cwd)
        self.assertIn("--locked", commands[1].argv)
        self.assertIn("--lib", commands[2].argv)
        self.assertEqual(commands[2].argv[-2:], ("--", "--test-threads=1"))
        self.assertIn("local_provider_worker", commands[3].argv)
        self.assertIn("statement_worker", commands[3].argv)

    def test_test_plan_can_run_from_a_writable_snapshot(self):
        snapshot = Path("/build/peri-harness-test")
        commands = entrypoint.plan_test_commands(self.layout, harness_root=snapshot)
        self.assertEqual(commands[0].cwd, snapshot)
        environment = dict(commands[0].env)
        self.assertEqual(environment["GIT_CONFIG_COUNT"], "4")
        self.assertEqual(environment["GIT_CONFIG_VALUE_2"], str(snapshot))
        self.assertEqual(environment["GIT_CONFIG_VALUE_3"],
                         str(snapshot / "prototypes/p0/turso"))
        self.assertEqual(environment["PERI_REPO_ROOT"], str(snapshot))

    def test_harness_snapshot_is_hash_identical_and_writable(self):
        with tempfile.TemporaryDirectory() as directory:
            layout = make_layout(directory)
            source = layout.workspace / "scripts/perf-reliability"
            source.mkdir(parents=True)
            (source / "gate.py").write_text("print('gate')\n")
            (source / "issue8").mkdir()
            (source / "issue8" / "workloads.json").write_text("{}\n")
            layout.build.mkdir()
            root, source_digest, snapshot_digest = entrypoint.prepare_harness_snapshot(layout)
            self.assertEqual(source_digest, snapshot_digest)
            copied = root / "scripts/perf-reliability" / "gate.py"
            self.assertEqual(copied.read_text(), "print('gate')\n")
            copied.write_text("print('snapshot-only')\n")
            self.assertEqual((source / "gate.py").read_text(), "print('gate')\n")
            self.assertTrue((root / "prototypes").is_symlink())
            self.assertTrue((root / ".git").is_symlink())

    def test_quick_plan_keeps_g1_semantics(self):
        inputs = entrypoint.RunInputs(
            campaign_path=Path("/inputs/quick-campaign.json"),
            environment_path=Path("/inputs/quick-environment.json"),
            trust_path=Path("/trust/trusted-artifacts.json"),
            artifacts_root=Path("/artifacts"))
        campaign = entrypoint.plan_campaign_command("quick", inputs, self.layout)
        gate = entrypoint.plan_gate_command("quick", inputs, self.layout)
        self.assertIn("--round", campaign.argv)
        self.assertEqual(campaign.argv[campaign.argv.index("--round") + 1], "quick")
        self.assertEqual(gate.argv[gate.argv.index("--tier") + 1], "G1")
        self.assertEqual(gate.argv[gate.argv.index("--profile") + 1], "quick")
        self.assertEqual(gate.argv[gate.argv.index("--scope") + 1], "native")
        self.assertNotIn("--allow-missing-attribution", gate.argv)
        self.assertNotIn("--allow-foreign-environment", gate.argv)

    def test_formal_plan_keeps_g2_semantics(self):
        inputs = entrypoint.RunInputs(
            campaign_path=Path("/inputs/formal-campaign.json"),
            environment_path=Path("/inputs/formal-environment.json"),
            trust_path=Path("/trust/trusted-artifacts.json"),
            artifacts_root=Path("/artifacts"))
        campaign = entrypoint.plan_campaign_command("formal", inputs, self.layout)
        gate = entrypoint.plan_gate_command("formal", inputs, self.layout)
        self.assertEqual(campaign.argv[campaign.argv.index("--round") + 1], "formal")
        self.assertEqual(campaign.argv[campaign.argv.index("--case") + 1], "S1,S2")
        self.assertEqual(gate.argv[gate.argv.index("--tier") + 1], "G2")
        self.assertEqual(gate.argv[gate.argv.index("--profile") + 1], "formal")


class CliTests(unittest.TestCase):
    def test_default_mode_is_test(self):
        arguments = entrypoint.parse_arguments([])
        self.assertEqual(arguments.mode, "test")
        self.assertFalse(arguments.dry_run)

    def test_unknown_mode_is_rejected(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as failure:
                entrypoint.parse_arguments(["steady"])
        self.assertEqual(failure.exception.code, 2)

    def test_quick_requires_explicit_inputs(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as failure:
                entrypoint.parse_arguments(["quick"])
        self.assertEqual(failure.exception.code, 2)

    def test_test_mode_rejects_performance_arguments(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                entrypoint.parse_arguments(["test", "--campaign", "/inputs/c.json"])

    def test_quick_rejects_allow_dirty(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                entrypoint.parse_arguments([
                    "quick", "--campaign", "/inputs/c", "--environment", "/inputs/e",
                    "--trusted-artifacts", "/trust/t", "--artifacts", "/artifacts",
                    "--allow-dirty"])

    def test_dry_run_test_prints_four_commands(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = entrypoint.main(["test", "--dry-run"])
        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("unittest discover", text)
        self.assertIn("cargo test", text)
        self.assertIn("local_provider_worker", text)

    def test_dry_run_flag_is_accepted_before_the_mode(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = entrypoint.main(["--dry-run", "test"])
        self.assertEqual(code, 0)
        self.assertIn("cargo check", output.getvalue())

    def test_dry_run_quick_prints_campaign_and_gate(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = entrypoint.main([
                "quick", "--dry-run", "--campaign", "/inputs/quick-campaign.json",
                "--environment", "/inputs/quick-environment.json",
                "--trusted-artifacts", "/trust/trusted-artifacts.json",
                "--artifacts", "/artifacts"])
        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("campaign_cli.py", text)
        self.assertIn("--round quick", text)
        self.assertIn("--tier G1", text)
        self.assertIn("--profile quick", text)
        self.assertIn("--scope native", text)

    def test_dry_run_formal_prints_g2_commands(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = entrypoint.main([
                "formal", "--dry-run", "--campaign", "/inputs/formal-campaign.json",
                "--environment", "/inputs/formal-environment.json",
                "--trusted-artifacts", "/trust/trusted-artifacts.json",
                "--artifacts", "/artifacts",
                "--host-attestation", "/inputs/host-attestation.json"])
        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("--round formal", text)
        self.assertIn("--tier G2", text)
        self.assertIn("--profile formal", text)


class CommandExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.layout = make_layout(self.temporary.name)
        create_layout_dirs(self.layout)

    def tearDown(self):
        self.temporary.cleanup()

    def test_run_command_uses_argv_and_preserves_exit_code(self):
        command = entrypoint.Command(
            "probe", (sys.executable, "-B", "-c", "import sys; sys.exit(3)"))
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code, captured = entrypoint.run_command(command, self.layout)
        self.assertEqual(code, 3)
        self.assertEqual(captured, "")
        self.assertIn("$", output.getvalue())

    def test_run_command_captures_original_output(self):
        command = entrypoint.Command(
            "probe", (sys.executable, "-B", "-c",
                      "print('original-output'); raise SystemExit(4)"), capture=True)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code, captured = entrypoint.run_command(command, self.layout)
        self.assertEqual(code, 4)
        self.assertIn("original-output", captured)
        self.assertIn("original-output", output.getvalue())

    def test_parse_unittest_summary(self):
        summary = entrypoint.parse_unittest_summary(
            "Ran 7 tests in 0.120s\n\nOK (skipped=2)\n")
        self.assertEqual(summary, {"executed": 7, "passed": 5, "failed": 0, "skipped": 2})
        summary = entrypoint.parse_unittest_summary(
            "Ran 4 tests in 0.010s\n\nFAILED (failures=1, errors=1, skipped=1)\n")
        self.assertEqual(summary, {"executed": 4, "passed": 1, "failed": 2, "skipped": 1})

    def test_formal_lock_is_exclusive(self):
        first = entrypoint.FormalLock(self.layout.store / ".peri-perf.lock")
        first.acquire()
        second = entrypoint.FormalLock(self.layout.store / ".peri-perf.lock")
        try:
            with self.assertRaisesRegex(entrypoint.PreflightError,
                                        "formal_exclusive_lock_unavailable"):
                second.acquire()
        finally:
            first.release()
        second.acquire()
        second.release()


class MainFlowTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.layout = make_layout(self.temporary.name)
        create_layout_dirs(self.layout)
        self.prepared = entrypoint.Prepared(
            mode="test", layout=self.layout, identity={"uid": 10001},
            source={"head": "deadbeef"}, mounts={})

    def tearDown(self):
        self.temporary.cleanup()

    def test_test_mode_success_writes_summary(self):
        outputs = [(0, "Ran 7 tests in 0.1s\n\nOK (skipped=2)\n"),
                   (0, ""), (0, ""), (0, "")]
        with patch.object(entrypoint, "preflight", return_value=self.prepared), \
                patch.object(entrypoint, "install_signal_handlers"), \
                patch.object(entrypoint, "run_command", side_effect=outputs), \
                patch.object(entrypoint, "check_source", return_value=self.prepared.source), \
                patch.object(entrypoint, "prepare_harness_snapshot",
                             return_value=(self.layout.workspace, "source", "source")):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = entrypoint.main(["test"])
        self.assertEqual(code, 0)
        summary = json.loads((self.layout.reports / "test-summary.json").read_text())
        self.assertEqual(summary["status"], "PASS")
        self.assertEqual(len(summary["steps"]), 5)
        self.assertEqual(summary["steps"][0]["executed"], 7)
        self.assertEqual(summary["steps"][0]["skipped"], 2)
        self.assertEqual(summary["steps"][4]["name"], "source-integrity")

    def test_changed_workspace_fails_the_test_mode(self):
        outputs = [(0, "Ran 1 test in 0.1s\n\nOK\n"), (0, ""), (0, ""), (0, "")]
        with patch.object(entrypoint, "preflight", return_value=self.prepared), \
                patch.object(entrypoint, "install_signal_handlers"), \
                patch.object(entrypoint, "run_command", side_effect=outputs), \
                patch.object(entrypoint, "check_source",
                             return_value={"head": "different"}), \
                patch.object(entrypoint, "prepare_harness_snapshot",
                             return_value=(self.layout.workspace, "source", "source")):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = entrypoint.main(["test"])
        self.assertEqual(code, 1)
        summary = json.loads((self.layout.reports / "test-summary.json").read_text())
        self.assertEqual(summary["steps"][-1]["error"], "workspace_source_changed")

    def test_test_mode_failure_stops_and_returns_nonzero(self):
        outputs = [(0, "Ran 1 test in 0.1s\n\nOK\n"), (1, "compile error")]
        with patch.object(entrypoint, "preflight", return_value=self.prepared), \
                patch.object(entrypoint, "install_signal_handlers"), \
                patch.object(entrypoint, "run_command", side_effect=outputs), \
                patch.object(entrypoint, "prepare_harness_snapshot",
                             return_value=(self.layout.workspace, "source", "source")):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = entrypoint.main(["test"])
        self.assertEqual(code, 1)
        summary = json.loads((self.layout.reports / "test-summary.json").read_text())
        self.assertEqual(summary["status"], "FAIL")
        self.assertEqual(len(summary["steps"]), 2)
        self.assertEqual(summary["steps"][1]["exit_code"], 1)

    def test_quick_accepts_phase_pass_and_rejects_not_run(self):
        inputs = entrypoint.RunInputs(
            campaign_path=Path("/inputs/quick-campaign.json"),
            environment_path=Path("/inputs/quick-environment.json"),
            trust_path=Path("/trust/trusted-artifacts.json"),
            artifacts_root=Path("/artifacts"))
        prepared = entrypoint.Prepared(
            mode="quick", layout=self.layout, identity={}, source={}, mounts={},
            inputs=inputs)
        arguments = [
            "quick", "--campaign", "/inputs/quick-campaign.json",
            "--environment", "/inputs/quick-environment.json",
            "--trusted-artifacts", "/trust/trusted-artifacts.json",
            "--artifacts", "/artifacts"]

        def accepted(command, layout):
            if command.label == "quick-campaign":
                return 0, ""
            (layout.reports / "quick-gate.json").write_text(json.dumps(quick_report()))
            return 1, "{}\n"

        with patch.object(entrypoint, "preflight", return_value=prepared), \
                patch.object(entrypoint, "install_signal_handlers"), \
                patch.object(entrypoint, "run_command", side_effect=accepted):
            with contextlib.redirect_stdout(io.StringIO()):
                code = entrypoint.main(arguments)
        self.assertEqual(code, 0)
        summary = json.loads((self.layout.reports / "quick-summary.json").read_text())
        self.assertEqual(summary["status"], "PHASE_PASS_ACCEPTED")

    def test_preflight_failure_returns_not_run_and_writes_summary(self):
        with patch.object(entrypoint, "preflight",
                          side_effect=entrypoint.PreflightError("mount_unavailable")), \
                patch.object(entrypoint, "install_signal_handlers"), \
                patch.object(entrypoint, "DEFAULT_LAYOUT", self.layout):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = entrypoint.main(["test"])
        self.assertEqual(code, 2)
        summary = json.loads((self.layout.reports / "test-summary.json").read_text())
        self.assertEqual(summary["status"], "NOT_RUN")
        self.assertEqual(summary["error"], "mount_unavailable")

    def test_quick_rejects_gate_not_run(self):
        inputs = entrypoint.RunInputs(            campaign_path=Path("/inputs/quick-campaign.json"),
            environment_path=Path("/inputs/quick-environment.json"),
            trust_path=Path("/trust/trusted-artifacts.json"),
            artifacts_root=Path("/artifacts"))
        prepared = entrypoint.Prepared(
            mode="quick", layout=self.layout, identity={}, source={}, mounts={},
            inputs=inputs)
        arguments = [
            "quick", "--campaign", "/inputs/quick-campaign.json",
            "--environment", "/inputs/quick-environment.json",
            "--trusted-artifacts", "/trust/trusted-artifacts.json",
            "--artifacts", "/artifacts"]

        def not_run(command, layout):
            if command.label == "quick-campaign":
                return 0, ""
            (layout.reports / "quick-gate.json").write_text(
                json.dumps(quick_report(status="NOT_RUN", exit_code=1)))
            return 1, "{}\n"

        with patch.object(entrypoint, "preflight", return_value=prepared), \
                patch.object(entrypoint, "install_signal_handlers"), \
                patch.object(entrypoint, "run_command", side_effect=not_run):
            with contextlib.redirect_stdout(io.StringIO()):
                code = entrypoint.main(arguments)
        self.assertEqual(code, 1)
        summary = json.loads((self.layout.reports / "quick-summary.json").read_text())
        self.assertEqual(summary["status"], "REJECTED")
        self.assertIn("quick_gate_status_not_phase_pass", summary["problems"])


if __name__ == "__main__":
    unittest.main()
