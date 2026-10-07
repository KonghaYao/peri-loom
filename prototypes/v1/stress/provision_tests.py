import http.client
import json
import os
import pathlib
import ssl
import stat
import sys

from provision import API, Failure, LABEL, LIMIT, QUOTAS, build_plan, decode, endpoint, envelope, main, read_file, require, reserve, provision


def run():
    import contextlib
    import io
    import tempfile
    import unittest
    from unittest import mock

    class SafetyTests(unittest.TestCase):
        def test_fifo_rejected_before_open(self):
            with tempfile.TemporaryDirectory() as temporary:
                path = pathlib.Path(temporary).resolve() / "input.json"
                os.mkfifo(path, 0o600)
                with mock.patch("os.open", side_effect=AssertionError("FIFO open forbidden")) as opened:
                    with self.assertRaises(Failure):
                        read_file(path)
                self.assertEqual(opened.call_count, 0)

        def test_all_before_metadata_guards(self):
            with tempfile.TemporaryDirectory() as temporary:
                path = pathlib.Path(temporary).resolve() / "input.json"
                path.write_text("{}"); path.chmod(0o600)
                original = path.stat()
                fields = {name: getattr(original, name) for name in ("st_mode", "st_uid", "st_nlink", "st_size", "st_dev", "st_ino")}
                for field, value in (("st_mode", stat.S_IFDIR | 0o700), ("st_uid", os.geteuid() + 1),
                                     ("st_nlink", 2), ("st_size", 65537), ("st_mode", stat.S_IFREG | 0o644)):
                    with self.subTest(field=field, value=value):
                        metadata = mock.Mock(**{**fields, field: value})
                        with mock.patch.object(pathlib.Path, "lstat", return_value=metadata), \
                                mock.patch("os.open", side_effect=AssertionError("unsafe open forbidden")) as opened:
                            with self.assertRaises(Failure):
                                read_file(path)
                        self.assertEqual(opened.call_count, 0)

        def test_open_flags_and_inode_race(self):
            with tempfile.TemporaryDirectory() as temporary:
                directory = pathlib.Path(temporary).resolve()
                source = directory / "input.json"; replacement = directory / "replacement.json"
                for path in (source, replacement):
                    path.write_text("{}"); path.chmod(0o600)
                descriptor = os.open(replacement, os.O_RDONLY | os.O_CLOEXEC)
                with mock.patch("os.open", return_value=descriptor) as opened:
                    with self.assertRaisesRegex(Failure, "input_identity_changed"):
                        read_file(source)
                flags = opened.call_args.args[1]
                for flag in (os.O_NONBLOCK, os.O_NOFOLLOW, os.O_CLOEXEC):
                    self.assertEqual(flags & flag, flag)
                with self.assertRaises(OSError):
                    os.fstat(descriptor)

        def test_all_after_metadata_guards(self):
            with tempfile.TemporaryDirectory() as temporary:
                path = pathlib.Path(temporary).resolve() / "input.json"
                path.write_text("{}"); path.chmod(0o600)
                original = path.stat()
                fields = {name: getattr(original, name) for name in ("st_mode", "st_uid", "st_nlink", "st_size", "st_dev", "st_ino")}
                for field, value in (("st_mode", stat.S_IFIFO | 0o600), ("st_uid", os.geteuid() + 1),
                                     ("st_nlink", 2), ("st_size", 65537), ("st_mode", stat.S_IFREG | 0o644),
                                     ("st_dev", original.st_dev + 1)):
                    with self.subTest(field=field, value=value), mock.patch("os.fstat", return_value=mock.Mock(**{**fields, field: value})):
                        with self.assertRaises(Failure):
                            read_file(path)

        def test_growth_still_bounded(self):
            with tempfile.TemporaryDirectory() as temporary:
                path = pathlib.Path(temporary).resolve() / "input.json"
                path.write_text("{}"); path.chmod(0o600)
                original = os.fstat
                def grow_after_stat(descriptor):
                    metadata = original(descriptor)
                    path.write_bytes(b"x" * 32)
                    return metadata
                with mock.patch("os.fstat", side_effect=grow_after_stat):
                    with self.assertRaisesRegex(Failure, "file_too_large"):
                        read_file(path, limit=16)

        def test_labels(self):
            for label in ("baseline", "fixed", "release_2"):
                self.assertTrue(LABEL.fullmatch(label))
            for label in ("", "../user", "baseline\n", "x" * 25, "USER"):
                self.assertIsNone(LABEL.fullmatch(label))

        def test_plan_exact_routes_fields(self):
            plan = build_plan("baseline")
            self.assertEqual(len(plan["operations"]), 9)
            self.assertEqual([entry["route"] for entry in plan["operations"]].count("/v1/databases/create"), 5)
            self.assertEqual(len(plan["databases"]), 4)
            self.assertEqual(plan["databases"][0]["namespace"]["tenant"], "local_stress_baseline")
            for entry in plan["operations"]:
                self.assertNotIn("quotas", entry["body"])
                if entry["route"] == "/v1/keys/create":
                    self.assertEqual(set(entry["body"]), {"subject", "project", "role", "service_account"})
                    self.assertTrue(entry["body"]["service_account"])

        def test_literal_verified_tls_only(self):
            endpoint("https://127.0.0.1:9443")
            endpoint("https://[::1]:9443")
            for address in ("http://127.0.0.1:9443", "https://localhost:9443", "https://example.com:443", "https://127.0.0.1:9443/x", "https://user@127.0.0.1:9443"):
                with self.assertRaises((Failure, ValueError)):
                    endpoint(address)

        def test_strict_json(self):
            for encoded in (b'{"key":1,"key":2}', b'{"key":NaN}', b'{} {}'):
                with self.assertRaises((Failure, ValueError)):
                    decode(encoded)

        def test_envelope_identity_and_redirect(self):
            value = {"version": 1, "request_id": "safe", "ok": True, "result": {}, "error": None}
            self.assertEqual(envelope(json.dumps(value).encode(), 200, "safe"), {})
            for field, item in (("version", True), ("request_id", "wrong"), ("ok", 1), ("error", {})):
                with self.assertRaises(Failure):
                    envelope(json.dumps({**value, field: item}).encode(), 200, "safe")
            with self.assertRaises(Failure):
                envelope(json.dumps(value).encode(), 302, "safe")

        def test_response_bound(self):
            with self.assertRaises(Failure):
                envelope(b" " * (LIMIT + 1), 200, "safe")

        def test_private_exclusive_output(self):
            with tempfile.TemporaryDirectory() as temporary:
                path = pathlib.Path(temporary).resolve() / "output.json"
                os.close(reserve(path, {"safe": True}))
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
                with self.assertRaises(FileExistsError):
                    reserve(path, {"overwrite": True})
                self.assertEqual(decode(read_file(path)), {"safe": True})

        def test_symlink_and_public_input(self):
            with tempfile.TemporaryDirectory() as temporary:
                path = pathlib.Path(temporary).resolve() / "input.json"
                path.write_text("{}"); path.chmod(0o644)
                with self.assertRaises(Failure):
                    read_file(path)
                path.chmod(0o600)
                link = path.with_name("link.json"); link.symlink_to(path)
                with self.assertRaises(Failure):
                    read_file(link)

        def test_no_arguments_offline(self):
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main([]), 0)

        def test_private_fixture_real_plan_no_network(self):
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT); context.load_default_certs()
            certificate = ssl.DER_cert_to_PEM_cert(context.get_ca_certs(binary_form=True)[0])
            with tempfile.TemporaryDirectory(prefix="peri-stress-unit-fixture-") as temporary:
                directory = pathlib.Path(temporary).resolve(); directory.chmod(0o700)
                ca_file = directory / "ca.crt"; ca_file.write_text(certificate); ca_file.chmod(0o600)
                workers = {"worker-a": {"address": "127.0.0.1:1"}, "worker-b": {"address": "127.0.0.1:2"}}
                launch = {"backend": "s3", "tls": True, "storage_identity": {"backend": "s3"}, "authority": "127.0.0.1:3", "workers": workers}
                connection = {"endpoint": "https://127.0.0.1:9443", "global_api_key": "synthetic_" * 5, "ca_file": str(ca_file),
                              "database": {"namespace": {"tenant": "local_default", "database": "demo"}, "branch": "main"}}
                topology = {"gateway": connection["endpoint"], "authority": "http://127.0.0.1:3",
                            "workers": {worker: "http://" + value["address"] for worker, value in workers.items()},
                            "processes": {role: {"pid": os.getpid() + index} for index, role in enumerate(["authority", "platform", *workers], 1)}}
                for name, value in (("launch", launch), ("topology", topology), ("connection", connection)):
                    os.close(reserve(directory / (name + ".json"), value))
                arguments = ["--plan", "--run-dir", str(directory), "--output", str(directory / "parent.json"), "--label", "unit"]
                with contextlib.redirect_stdout(io.StringIO()) as captured:
                    self.assertEqual(main(arguments), 0)
                self.assertTrue(decode(captured.getvalue())["plan_only"])
                self.assertFalse((directory / "parent.json").exists())
                self.assertFalse((directory / ".stress-provision-unit.json").exists())

        def test_transport_unknown_single_request_redacted(self):
            configuration = {"connection": {"endpoint": "https://127.0.0.1:9443"}, "context": ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)}
            transport = mock.Mock(); transport.getresponse.side_effect = OSError("synthetic_secret_not_to_log")
            with mock.patch("http.client.HTTPSConnection", return_value=transport) as constructor:
                with self.assertRaises(Failure) as failure:
                    API(configuration).call("/v1/projects/create", "synthetic_" * 5, {"request_id": "safe"})
            self.assertNotIn("synthetic_secret", str(failure.exception))
            self.assertEqual(constructor.call_count, 1)
            self.assertEqual(transport.request.call_count, 1)
            self.assertEqual(transport.close.call_count, 1)
            self.assertEqual(transport.auto_open, 0)
            encoded = json.dumps({"version": 1, "request_id": "safe", "ok": True, "result": {}, "error": None}).encode()
            stream = io.BytesIO(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: " + str(len(encoded)).encode() + b"\r\n\r\n" + encoded)
            response = http.client.HTTPResponse(mock.Mock(makefile=lambda mode: stream)); response.begin()
            transport.getresponse.side_effect = None; transport.getresponse.return_value = response
            transport.sock.settimeout.side_effect = lambda timeout: require(not response.isclosed(), "closed_socket_used")
            with mock.patch("http.client.HTTPSConnection", return_value=transport):
                self.assertEqual(API(configuration).call("/v1/projects/create", "synthetic_" * 5, {"request_id": "safe"}), {})
            self.assertTrue(response.isclosed())

        def test_failed_mutation_not_retried(self):
            with tempfile.TemporaryDirectory(prefix="peri-stress-unit-fixture-") as temporary:
                directory = pathlib.Path(temporary).resolve()
                configuration = {"directory": directory, "output": directory / "parent.json", "processes": [],
                                 "connection": {"global_api_key": "synthetic_" * 5}, "workers": ["worker-a", "worker-b"]}
                fake = mock.Mock()
                fake.call.side_effect = [{"projects": {}, "databases": {}}, Failure("unknown")]
                with self.assertRaises(Failure):
                    provision(configuration, build_plan("unit"), "unit", fake)
                self.assertEqual(fake.call.call_count, 2)
                self.assertTrue(decode(read_file(configuration["output"]))["incomplete"])
                self.assertTrue((directory / ".stress-provision-unit.json").exists())
                with self.assertRaises(FileExistsError):
                    provision(configuration, build_plan("unit"), "unit", fake)
                self.assertEqual(fake.call.call_count, 2)

        def test_catalog_existing_label_no_mutation(self):
            with tempfile.TemporaryDirectory(prefix="peri-stress-unit-fixture-") as temporary:
                directory = pathlib.Path(temporary).resolve()
                configuration = {"directory": directory, "output": directory / "parent.json", "processes": [],
                                 "connection": {"global_api_key": "synthetic_" * 5}}
                fake = mock.Mock(); fake.call.return_value = {"projects": {"stress_unit": {}}, "databases": {}}
                with self.assertRaises(Failure):
                    provision(configuration, build_plan("unit"), "unit", fake)
                self.assertEqual(fake.call.call_count, 1)

        def test_full_static_config_confirmations(self):
            with tempfile.TemporaryDirectory(prefix="peri-stress-unit-fixture-") as temporary:
                directory = pathlib.Path(temporary).resolve()
                configuration = {"directory": directory, "output": directory / "parent.json", "processes": [],
                                 "connection": {"endpoint": "https://127.0.0.1:9443", "global_api_key": "bootstrap_" * 5,
                                                "ca_file": "/synthetic/ca.crt"}, "workers": ["worker-a", "worker-b"]}
                def confirmation(route, key, body=None):
                    if body is None:
                        return {"projects": {}, "databases": {}}
                    if route.endswith("projects/create"):
                        return {"id": body["id"], "organization": "local", "tenant": "local_" + body["id"], "region": "local", "quotas": QUOTAS}
                    if route.endswith("keys/create"):
                        identifier = body["project"] + "_key"
                        return {"key": identifier, "project": body["project"], "role": "admin", "subject": body["subject"],
                                "credential": "plk1_" + identifier + "_" + "synthetic_" * 5, "shown_once": True}
                    return {"database": body["database"], "phase": "active", "desired_state": "active", "role": "writer", "desired_worker": "worker-b"}
                fake = mock.Mock(); fake.call.side_effect = confirmation
                provision(configuration, build_plan("unit"), "unit", fake)
                result = decode(read_file(configuration["output"]))
                self.assertEqual(fake.call.call_count, 10)
                self.assertEqual(result["migration_worker"], "worker-a")
                self.assertEqual(len(result["databases"]), 4)
                self.assertNotEqual(result["api_key"], result["noisy_tenant"]["api_key"])
                self.assertTrue(result["fixture_only"])
                self.assertNotIn("request_id", result)

    suite = unittest.defaultTestLoader.loadTestsFromTestCase(SafetyTests)
    with mock.patch("socket.socket", side_effect=AssertionError("network forbidden")) as sockets:
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    passed = result.wasSuccessful() and sockets.call_count == 0
    print(json.dumps({"self_test": "PASS" if passed else "FAIL", "tests": result.testsRun,
                      "failures": len(result.failures), "errors": len(result.errors), "network_calls": sockets.call_count}))
    return 0 if passed else 1



if __name__ == "__main__":
    sys.exit(run())
