"""Launcher proof acceptance must agree with the Rust pack-v1 reader."""

import argparse
import base64
import copy
import importlib.util
import json
import os
import pathlib
import struct
import sys
import tempfile
import unittest
from unittest.mock import patch


source = pathlib.Path(__file__).resolve().parents[1] / "dev.py"
spec = importlib.util.spec_from_file_location("peri_dev_pack_format", source)
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)


class PackProofTests(unittest.TestCase):
    def setUp(self):
        try:
            import blake3
        except ImportError:
            self.skipTest("pack-v1 proof tests require the Python blake3 package")
        self.blake3 = blake3
        directory = tempfile.TemporaryDirectory(prefix="peri-pack-proof-")
        self.addCleanup(directory.cleanup)
        self.backend = pathlib.Path(directory.name)
        self.key = dev.LOCAL_SEAL_KEY
        self.payload = json.dumps({"version": 1, "owner": "fixture", "clean": True}).encode()
        self.sibling_key = "authority-roots/fixture/main"
        self.sibling_payload = b'{"revision":7}'
        self.candidate = self.alias(self.key)

    def alias(self, key):
        return self.backend / (self.blake3.blake3(key.encode()).hexdigest() + ".obj")

    def directory(self, items=None):
        if items is None:
            items = [(self.key, self.payload), (self.sibling_key, self.sibling_payload)]
        entries, payload = [], b""
        for key, body in items:
            entries.append({"key": key, "etag": "fixture-etag",
                            "checksum": self.blake3.blake3(body).hexdigest(),
                            "offset": len(payload), "length": len(body)})
            payload += body
        return {"version": 1, "entries": entries}, payload

    def encoded(self, directory=None, payload=None, directory_raw=None):
        if directory is None:
            directory, payload = self.directory()
        if directory_raw is None:
            directory_raw = json.dumps(directory, separators=(",", ":")).encode()
        return (b"PLPACK1\0" + struct.pack("<IQ", len(directory_raw), len(payload))
                + self.blake3.blake3(directory_raw).digest() + directory_raw + payload)

    def write(self, **kwargs):
        self.candidate.write_bytes(self.encoded(**kwargs))
        return self.candidate

    def assert_rejected(self, encoded):
        self.candidate.write_bytes(encoded)
        with self.assertRaises((ValueError, RuntimeError, UnicodeError)):
            dev.read_local_payload(self.candidate, self.key, self.blake3)
        self.assertEqual(dev.local_object_path(self.backend, self.key), (None, False))
        self.assertEqual(dev.local_object_any(self.backend, self.key), (None, None, False))

    def test_valid_pack_reads_each_live_alias_and_dirty_seal_is_not_clean(self):
        self.write()
        sibling = self.alias(self.sibling_key)
        os.link(self.candidate, sibling)
        self.assertEqual(dev.local_object_path(self.backend, self.key), (self.candidate, True))
        self.assertEqual(dev.local_object_any(self.backend, self.sibling_key),
                         (sibling, self.sibling_payload, True))
        dirty = json.dumps({"version": 1, "owner": "fixture", "clean": False}).encode()
        directory, payload = self.directory([(self.key, dirty)])
        self.write(directory=directory, payload=payload)
        self.assertEqual(dev.local_object_path(self.backend, self.key), (None, False))
        self.assertEqual(dev.local_object_any(self.backend, self.key), (self.candidate, dirty, True))

    def test_deleted_alias_is_not_resurrected_from_another_live_pack_alias(self):
        self.write()
        sibling = self.alias(self.sibling_key)
        os.link(self.candidate, sibling)
        self.candidate.unlink()
        self.assertEqual(dev.local_object_path(self.backend, self.key), (None, True))
        self.assertEqual(dev.local_object_any(self.backend, self.key), (None, None, True))
        self.assertEqual(dev.local_object_any(self.backend, self.sibling_key),
                         (sibling, self.sibling_payload, True))
        with self.assertRaisesRegex(ValueError, "live alias"):
            dev.read_local_payload(sibling, self.key, self.blake3)
        with patch.dict(sys.modules, {"blake3": None}):
            self.assertEqual(dev.local_object_path(self.backend, self.key), (None, False))
            self.assertEqual(dev.local_object_any(self.backend, self.key), (None, None, False))

    def test_directory_and_requested_payload_checksums_fail_closed(self):
        original = self.encoded()
        for label, offset in (("directory checksum", 20), ("requested payload", -len(self.sibling_payload) - 1)):
            mutated = bytearray(original)
            mutated[offset] ^= 1
            with self.subTest(label=label):
                self.assert_rejected(bytes(mutated))

    def test_truncated_padded_and_over_budget_headers_fail_closed(self):
        original = self.encoded()
        variants = [original[:20], original[:-1], original + b"x"]
        for directory_length, payload_length in ((0, 0), (65537, 0), (1, 262145)):
            variants.append(original[:8] + struct.pack("<IQ", directory_length, payload_length) + original[20:])
        for index, encoded in enumerate(variants):
            with self.subTest(index=index):
                self.assert_rejected(encoded)

    def test_directory_structure_matches_rust_strict_decoder(self):
        valid, payload = self.directory()
        variants = {}
        changed = copy.deepcopy(valid)
        changed["extra"] = True
        variants["unknown directory field"] = changed
        changed = copy.deepcopy(valid)
        changed["entries"][1]["extra"] = True
        variants["unknown entry field"] = changed
        for label, value in (("boolean version", True), ("unknown version", 2)):
            changed = copy.deepcopy(valid)
            changed["version"] = value
            variants[label] = changed
        for field, value in (("key", self.key), ("offset", True), ("offset", 0),
                             ("length", True), ("length", -1), ("checksum", "x" * 64),
                             ("etag", ""), ("etag", "é" * 129)):
            changed = copy.deepcopy(valid)
            changed["entries"][1][field] = value
            variants[f"invalid {field}: {value!r}"] = changed
        for label, changed in variants.items():
            with self.subTest(label=label):
                self.assert_rejected(self.encoded(directory=changed, payload=payload))
        directory_raw = json.dumps(valid).encode()[:-1] + b',"version":1}'
        self.assert_rejected(self.encoded(directory=valid, payload=payload, directory_raw=directory_raw))

    def test_all_entry_keys_and_small_object_limits_are_validated(self):
        for key in ("", "/root", "root/", "root//child", "root/./child", "root/../child",
                    "root?child", "é", "a" * 1025):
            directory, payload = self.directory([(self.key, self.payload), (key, b"sibling")])
            with self.subTest(key=key):
                self.assert_rejected(self.encoded(directory=directory, payload=payload))
        directory, payload = self.directory([(self.key, self.payload), (self.sibling_key, b"x" * 32769)])
        self.assert_rejected(self.encoded(directory=directory, payload=payload))
        directory, payload = self.directory([(self.key, self.payload)] + [(f"item/{index}", b"") for index in range(32)])
        self.assert_rejected(self.encoded(directory=directory, payload=payload))

    def test_pack_without_blake3_cannot_supply_restart_evidence(self):
        self.write()
        with self.assertRaisesRegex(ValueError, "live alias"):
            dev.read_local_payload(self.candidate, self.key, None)
        with patch.dict(sys.modules, {"blake3": None}):
            self.assertEqual(dev.local_object_path(self.backend, self.key), (None, False))
            self.assertEqual(dev.local_object_any(self.backend, self.key), (None, None, False))

    def test_legacy_envelope_remains_readable_and_unverified_without_blake3(self):
        envelope = {"key": self.key, "bytes": base64.b64encode(self.payload).decode(),
                    "etag": "fixture-etag", "checksum": self.blake3.blake3(self.payload).hexdigest()}
        self.candidate.write_text(json.dumps(envelope))
        self.assertEqual(dev.local_object_path(self.backend, self.key), (self.candidate, True))
        self.assertEqual(dev.local_object_any(self.backend, self.key), (self.candidate, self.payload, True))
        with patch.dict(sys.modules, {"blake3": None}):
            self.assertEqual(dev.local_object_path(self.backend, self.key), (self.candidate, False))
            self.assertEqual(dev.local_object_any(self.backend, self.key), (self.candidate, self.payload, False))
        envelope["checksum"] = "0" * 64
        self.assert_rejected(json.dumps(envelope).encode())


class DurableFormatProfileTests(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(dev.os.environ, dev.safe_environment(), clear=True)
        environment.start()
        self.addCleanup(environment.stop)
        builders = patch.object(dev.subprocess, "run", side_effect=AssertionError("builder forbidden"))
        builders.start()
        self.addCleanup(builders.stop)
        processes = patch.object(dev.subprocess, "Popen", side_effect=AssertionError("actual service forbidden"))
        processes.start()
        self.addCleanup(processes.stop)

    def project(self, directory):
        return dev.Project(argparse.Namespace(
            run_dir=directory, no_build=True, backend="local", listen="127.0.0.1:19443",
            tls=False, writer_admission=False, sql_open_observer=False,
        ))

    def test_persisted_profile_cannot_be_rebound_or_rewritten_on_restart(self):
        for enabled in (False, True):
            with self.subTest(enabled=enabled), tempfile.TemporaryDirectory(prefix="peri-format-profile-") as directory:
                value = "1" if enabled else "0"
                settings = {name: value for name in dev.PERFORMANCE_FORMAT_SWITCHES.values()}
                with patch.dict(dev.os.environ, settings):
                    project = self.project(directory)
                    project.prepare()
                path = pathlib.Path(directory) / "launch.json"
                persisted = path.read_bytes()
                self.assertEqual(dev.performance_format_profile(project.launch),
                                 {name: enabled for name in dev.PERFORMANCE_FORMAT_SWITCHES})
                for field, name in dev.PERFORMANCE_FORMAT_SWITCHES.items():
                    self.assertIs(json.loads(persisted)[field], enabled)
                    with patch.dict(dev.os.environ, {name: "0" if enabled else "1"}):
                        with self.assertRaisesRegex(RuntimeError, "durable format profile cannot be silently rebound"):
                            self.project(directory).prepare()
                    self.assertEqual(path.read_bytes(), persisted)
                with self.assertRaisesRegex(RuntimeError, "proof set incomplete"):
                    self.project(directory).prepare()
                self.assertEqual(path.read_bytes(), persisted)

    def test_legacy_launch_defaults_are_in_memory_and_types_are_strict(self):
        with tempfile.TemporaryDirectory(prefix="peri-format-legacy-") as directory:
            project = self.project(directory)
            project.prepare()
            legacy = {key: value for key, value in project.launch.items()
                      if key not in dev.PERFORMANCE_FORMAT_SWITCHES}
            before = copy.deepcopy(legacy)
            normalized = dev.normalize_launch(legacy)
            self.assertEqual(legacy, before)
            for field in dev.PERFORMANCE_FORMAT_SWITCHES:
                self.assertIs(normalized[field], False)
                for invalid in (0, 1, "1", None):
                    with self.subTest(field=field, invalid=invalid):
                        with self.assertRaisesRegex(RuntimeError, field):
                            dev.normalize_launch({**legacy, field: invalid})
            with patch.dict(dev.os.environ, {"PERI_COMPLETION_JOURNAL": "1"}):
                with self.assertRaisesRegex(RuntimeError, "cannot be silently rebound"):
                    dev.performance_format_profile(normalized)

    def test_invalid_opt_in_and_missing_pack_hash_dependency_fail_before_start(self):
        for name in dev.PERFORMANCE_FORMAT_SWITCHES.values():
            for value in ("", "true", " 1", "1\n", "2"):
                with self.subTest(name=name, value=value), patch.dict(dev.os.environ, {name: value}):
                    with self.assertRaisesRegex(RuntimeError, "must be 0 or 1"):
                        dev.performance_format_profile()
        with patch.dict(sys.modules, {"blake3": None}), patch.dict(dev.os.environ, {"PERI_LOCAL_OBJECT_PACK": "1"}):
            with self.assertRaisesRegex(RuntimeError, "require the Python blake3 package"):
                dev.performance_format_profile()
        with patch.dict(sys.modules, {"blake3": None}):
            with self.assertRaisesRegex(RuntimeError, "require the Python blake3 package"):
                dev.performance_format_profile({"local_object_pack": True, "completion_journal": True})

    def test_normal_and_recovery_children_use_the_persisted_format_profile(self):
        class BeforeAPI(RuntimeError):
            pass

        for enabled in (False, True):
            with self.subTest(enabled=enabled), tempfile.TemporaryDirectory(prefix="peri-format-child-") as directory:
                settings = {name: "1" if enabled else "0" for name in dev.PERFORMANCE_FORMAT_SWITCHES.values()}
                with patch.dict(dev.os.environ, settings):
                    project = self.project(directory)
                    project.prepare()
                # A recovery launch contacts status after starting its two control
                # services. Stop there, without creating a process or a socket.
                dev.private_json(pathlib.Path(directory) / "connection.json",
                                 {"project_api_key": "synthetic-format-fixture", "database": {}})
                for recovery in (False, True):
                    captured = {}

                    def record_start(name, command, environment):
                        captured[name] = environment

                    with self.subTest(recovery=recovery), \
                            patch.object(project, "acquire_run_lock"), \
                            patch.object(project, "prepare"), \
                            patch.object(project, "start", side_effect=record_start), \
                            patch.object(project, "start_maintenance"), \
                            patch.object(dev, "request", side_effect=BeforeAPI("before network")):
                        with self.assertRaises(BeforeAPI):
                            project.run_recovery() if recovery else project.run()
                    self.assertEqual(captured["authority"]["PERI_LOCAL_OBJECT_PACK"], "1" if enabled else "0")
                    self.assertEqual(captured["platform"]["PERI_COMPLETION_JOURNAL"], "1" if enabled else "0")
                    self.assertNotIn("PERI_COMPLETION_JOURNAL", captured["authority"])
                    self.assertNotIn("PERI_LOCAL_OBJECT_PACK", captured["platform"])
                    self.assertEqual(set(captured), {"authority", "platform"} if recovery
                                     else {"authority", "platform", "worker-a", "worker-b"})
                    for worker in ("worker-a", "worker-b"):
                        if worker in captured:
                            for name in dev.PERFORMANCE_FORMAT_SWITCHES.values():
                                self.assertNotIn(name, captured[worker])

    def test_completion_v2_checkpoint_must_name_the_final_revision(self):
        dev.require_materialized_operation_view({"operation_storage_version": 2,
                                                 "revision": 19, "operations_checkpoint": 19})
        for updates in ({"operations_checkpoint": 18}, {"operations_checkpoint": 20},
                        {"operations_checkpoint": None}, {"operations_checkpoint": True},
                        {"revision": True, "operations_checkpoint": True},
                        {"revision": 0, "operations_checkpoint": 0}):
            with self.subTest(updates=updates), self.assertRaises(dev.OperationViewNotMaterialized):
                dev.require_materialized_operation_view({"operation_storage_version": 2,
                                                         "revision": 19, "operations_checkpoint": 19, **updates})
        for version in (-1, 3, True, "2", None):
            with self.subTest(version=version), self.assertRaises(dev.OperationViewNotMaterialized):
                dev.require_materialized_operation_view({"operation_storage_version": version,
                                                         "revision": 19, "operations_checkpoint": 19})
        dev.require_materialized_operation_view({})
        dev.require_materialized_operation_view({"operation_storage_version": 0})
        dev.require_materialized_operation_view({"operation_storage_version": 1,
                                                 "revision": 19, "operations_checkpoint": 19})


if __name__ == "__main__":
    unittest.main()
