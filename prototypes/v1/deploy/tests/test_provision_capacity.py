import copy
import argparse
import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch


source = Path(__file__).resolve().parents[1] / "provision-capacity.py"
spec = importlib.util.spec_from_file_location("provision_capacity", source)
tool = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tool)


class ContainerProvisioningTests(unittest.TestCase):
    def prepared_fixture(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name).resolve()
        root.chmod(0o700)
        output = root / "output"
        output.mkdir(mode=0o700)
        report = {"image": {"Os": "linux", "User": "10001:10001", "Id": "sha256:" + "a" * 64},
                  "writer_admission": True, "capacity_trace": True, "container_id": "b" * 64,
                  "owner": "c" * 32, "container_name": "peri-verify-" + "c" * 32,
                  "source_hash": "d" * 64, "runtime": {"port": "9443"},
                  "required_native_roles": ["authority", "worker-a", "worker-b", "platform", "launcher"]}
        connection = {"endpoint": "https://127.0.0.1:9443", "global_api_key": "plk1_global_" + "e" * 64,
                      "ca_file": str(root / "ca.crt")}
        for name, contents in (("monitor.json", json.dumps(report)), ("connection.json", json.dumps(connection)),
                               ("ca.crt", "test-certificate"), ("monitor.py", "raise RuntimeError('not imported in plan')\n")):
            path = root / name
            path.write_text(contents)
            path.chmod(0o600)
        arguments = argparse.Namespace(output=output, monitor_config=root / "monitor.json", connection=root / "connection.json",
                                       monitor_script=root / "monitor.py", label="capacitytest",
                                       monitor_sha256=hashlib.sha256((root / "monitor.py").read_bytes()).hexdigest())
        return arguments, report

    def connection(self):
        return {"database": {"name": "primary"}, "databases": [{"name": "primary"}, {"name": "second"}],
                "processes": [{"role": "worker-a", "pid": 123}], "noisy_tenant": {"project": "noise"}}

    def test_six_topologies_have_exact_database_and_project_counts(self):
        original = self.connection()
        before = copy.deepcopy(original)
        for topology in tool.TOPOLOGIES:
            with self.subTest(topology=topology):
                result = tool.topology_connection(original, topology)
                self.assertEqual(len(result["databases"]), int(topology.endswith("many")))
                self.assertNotIn(result["database"], result["databases"])
                self.assertEqual("noisy_tenant" in result, not topology.startswith("single"))
                self.assertEqual(result["processes"], [])
        self.assertEqual(original, before)

    def test_unknown_topology_and_missing_second_database_rejected(self):
        with self.assertRaises(ValueError):
            tool.topology_connection(self.connection(), "invented")
        source = self.connection()
        source["databases"] = [source["database"]]
        with self.assertRaises(ValueError):
            tool.topology_connection(source, "single_many")

    def test_actual_container_proof_precedes_and_follows_single_request(self):
        configuration = {"connection": {"endpoint": "https://127.0.0.1:9443"}, "context": Mock()}
        events = []
        probe = Mock(side_effect=lambda: events.append("probe") or {})
        resources = Mock()
        with patch.object(tool.provision.API, "call", side_effect=lambda *args: events.append("request") or {"ack": True}) as request:
            api = tool.ContainerAPI(configuration, probe, resources)
            self.assertEqual(api.call("/v1/projects/create", "secret", {"request_id": "correlation"}), {"ack": True})
        self.assertEqual(events, ["probe", "request", "probe"])
        self.assertEqual(resources.ingest.call_count, 2)
        request.assert_called_once()

    def test_failed_container_proof_never_dispatches_management(self):
        configuration = {"connection": {"endpoint": "https://127.0.0.1:9443"}, "context": Mock()}
        resources = Mock()
        resources.ingest.side_effect = ValueError("wrong incarnation")
        with patch.object(tool.provision.API, "call") as request:
            api = tool.ContainerAPI(configuration, Mock(return_value={}), resources)
            with self.assertRaises(ValueError):
                api.call("/v1/projects/create", "secret", {"request_id": "correlation"})
        request.assert_not_called()

    def test_failed_post_proof_does_not_replay_acknowledged_request(self):
        configuration = {"connection": {"endpoint": "https://127.0.0.1:9443"}, "context": Mock()}
        resources = Mock()
        resources.ingest.side_effect = [None, ValueError("incarnation changed")]
        with patch.object(tool.provision.API, "call", return_value={"ack": True}) as request:
            api = tool.ContainerAPI(configuration, Mock(return_value={}), resources)
            with self.assertRaises(ValueError):
                api.call("/v1/projects/create", "secret", {"request_id": "correlation"})
        request.assert_called_once()

    def test_plan_validates_private_bootstrap_without_import_or_requests(self):
        arguments, report = self.prepared_fixture()
        with patch.object(tool.ssl, "SSLContext") as context, patch.object(tool.provision.API, "call") as request:
            configuration, actual = tool.prepare(arguments)
        self.assertEqual(actual, report)
        self.assertEqual(configuration["processes"], [])
        self.assertEqual(configuration["workers"], ["worker-a", "worker-b"])
        self.assertEqual(list(arguments.output.iterdir()), [])
        context.return_value.load_verify_locations.assert_called_once()
        request.assert_not_called()

    def test_provisioning_rejects_existing_output(self):
        arguments, _ = self.prepared_fixture()
        (arguments.output / "original.json").write_text("retained")
        with self.assertRaisesRegex(ValueError, "fresh_provisioning_output"):
            tool.prepare(arguments)
        self.assertEqual((arguments.output / "original.json").read_text(), "retained")

    def test_connection_cannot_point_to_different_container_port(self):
        arguments, report = self.prepared_fixture()
        report["runtime"]["port"] = "9000"
        arguments.monitor_config.write_text(json.dumps(report))
        with self.assertRaisesRegex(ValueError, "owned_container_publication"):
            tool.prepare(arguments)

    def test_monitor_source_change_rejected_without_loading_code(self):
        arguments, _ = self.prepared_fixture()
        arguments.monitor_script.write_text("raise RuntimeError('changed')\n")
        with patch.object(tool.ssl, "SSLContext"), self.assertRaisesRegex(ValueError, "frozen_monitor_source"):
            tool.prepare(arguments)

    def test_execute_rechecks_monitor_before_running_trusted_source(self):
        arguments, report = self.prepared_fixture()
        with patch.object(tool.ssl, "SSLContext"):
            configuration, report = tool.prepare(arguments)
            arguments.monitor_script.write_text("raise RuntimeError('must not run changed source')\n")
            with self.assertRaisesRegex(ValueError, "frozen_monitor_source"):
                tool.execute(arguments, configuration, report)

    def executable_fixture(self):
        arguments, _ = self.prepared_fixture()
        arguments.monitor_script.write_text("def snapshot(report, environment):\n    return {'actual_test_snapshot': True}\n")
        arguments.monitor_sha256 = hashlib.sha256(arguments.monitor_script.read_bytes()).hexdigest()
        with patch.object(tool.ssl, "SSLContext"):
            configuration, report = tool.prepare(arguments)
        return arguments, configuration, report

    def test_complete_execute_writes_six_connections_without_host_PID_checks(self):
        arguments, configuration, report = self.executable_fixture()
        issued = {}
        def confirmation(route, key, body=None):
            last = json.loads((arguments.output / "provision-phases.jsonl").read_text().splitlines()[-1])
            self.assertEqual((last["stage"], last["edge"], last["route"]), ("API", "before", route))
            self.assertEqual(last["request_id"], body["request_id"] if body else "")
            if body is None:
                self.assertEqual(route, "/v1/catalog")
                return {"projects": {}, "databases": {}}
            if route == "/v1/projects/create":
                return {"id": body["id"], "organization": "local", "tenant": "local_" + body["id"],
                        "region": "local", "quotas": tool.provision.QUOTAS}
            if route == "/v1/keys/create":
                identifier = body["project"] + "_key"
                issued[body["project"]] = "plk1_" + identifier + "_" + "synthetic_" * 5
                return {"key": identifier, "project": body["project"], "role": "admin", "subject": body["subject"],
                        "credential": issued[body["project"]], "shown_once": True}
            self.assertEqual(route, "/v1/databases/create")
            self.assertEqual(key, issued[body["project"]])
            return {"database": body["database"], "phase": "active", "desired_state": "active",
                    "role": "writer", "desired_worker": "worker-b"}
        captured = io.StringIO()
        with patch.object(tool.ssl, "SSLContext"), patch.object(tool, "ContainerResources") as resources, \
                patch.object(tool.provision.API, "call", side_effect=confirmation) as requests, \
                patch.object(tool.provision.os, "kill", side_effect=AssertionError("container PID is not a HostPID")), \
                contextlib.redirect_stdout(captured):
            tool.execute(arguments, configuration, report)
        self.assertEqual(requests.call_count, 10)
        self.assertEqual(resources.return_value.ingest.call_count, 21)
        self.assertEqual(captured.getvalue(), "")
        original = json.loads(configuration["output"].read_text())
        for topology in tool.TOPOLOGIES:
            derived = json.loads((arguments.output / (topology + ".json")).read_text())
            self.assertEqual(derived, tool.topology_connection(original, topology))
        self.assertTrue((arguments.output / ".stress-provision-capacitytest.json").exists())
        provenance = json.loads((arguments.output / "provisioning-source.json").read_text())
        self.assertFalse(provenance["host_native_PIDs_claimed"])
        rows = self.phases(arguments)
        self.assertEqual((rows[-1]["stage"], rows[-1]["edge"], rows[-1]["failure_category"]), ("result", "terminal", "none"))
        for stage in ("API", "confirmation"):
            self.assertEqual(len([row for row in rows if row["stage"] == stage and row["edge"] == "before"]), 10)
            self.assertEqual(len([row for row in rows if row["stage"] == stage and row["edge"] == "terminal"]), 10)
        for secret in [configuration["connection"]["global_api_key"], *issued.values()]:
            self.assertNotIn(secret, (arguments.output / "provision-phases.jsonl").read_text())
        with self.assertRaisesRegex(ValueError, "fresh_provisioning_output"):
            tool.prepare(arguments)

    def test_management_unknown_preserves_reservation_and_stops_without_retry(self):
        arguments, configuration, report = self.executable_fixture()
        outcomes = [{"projects": {}, "databases": {}}, tool.provision.Failure("management_outcome_unknown_request_id=test")]
        captured = io.StringIO()
        with patch.object(tool.ssl, "SSLContext"), patch.object(tool, "ContainerResources"), \
                patch.object(tool.provision.API, "call", side_effect=outcomes) as requests, \
                contextlib.redirect_stdout(captured):
            with self.assertRaises(tool.provision.Failure):
                tool.execute(arguments, configuration, report)
        self.assertEqual(requests.call_count, 2)
        self.assertEqual(captured.getvalue(), "")
        original = configuration["output"].read_bytes()
        self.assertTrue(json.loads(original)["incomplete"])
        self.assertTrue((arguments.output / ".stress-provision-capacitytest.json").exists())
        self.assertFalse(any((arguments.output / (topology + ".json")).exists() for topology in tool.TOPOLOGIES))
        with self.assertRaisesRegex(ValueError, "fresh_provisioning_output"):
            tool.prepare(arguments)
        self.assertEqual(configuration["output"].read_bytes(), original)

    def phases(self, arguments):
        path = arguments.output / "provision-phases.jsonl"
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        self.assertEqual([row["sequence"] for row in rows], list(range(len(rows))))
        self.assertLessEqual(len(rows), 128)
        for row in rows:
            self.assertEqual(set(row), {"sequence", "stage", "edge", "route", "request_id", "failure_category"})
        return rows

    def test_before_resource_failure_has_zero_API_and_safe_durable_terminal(self):
        arguments, configuration, report = self.executable_fixture()
        secret = "DO-NOT-LOG-resource-key-body"
        with patch.object(tool.ssl, "SSLContext"), patch.object(tool, "ContainerResources") as resources, \
                patch.object(tool.provision.API, "call") as request:
            resources.return_value.ingest.side_effect = [None, ValueError(secret)]
            with self.assertRaises(tool.PhaseFailure) as raised:
                tool.execute(arguments, configuration, report)
        self.assertEqual(raised.exception.category, "resource_before")
        request.assert_not_called()
        rows = self.phases(arguments)
        self.assertEqual(rows[-1]["stage"], "before_resource")
        self.assertEqual(rows[-1]["failure_category"], "resource_before")
        self.assertEqual(rows[-1]["edge"], "terminal")
        self.assertTrue(json.loads(configuration["output"].read_text())["incomplete"])
        self.assertNotIn(secret, (arguments.output / "provision-phases.jsonl").read_text())
        self.assertFalse(any(row["stage"] == "API" for row in rows))

    def test_API_exception_only_one_call_and_no_arbitrary_failure_text(self):
        arguments, configuration, report = self.executable_fixture()
        secret = "DO-NOT-LOG-API-credential-response"
        with patch.object(tool.ssl, "SSLContext"), patch.object(tool, "ContainerResources"), \
                patch.object(tool.provision.API, "call", side_effect=RuntimeError(secret)) as request:
            with self.assertRaises(tool.PhaseFailure) as raised:
                tool.execute(arguments, configuration, report)
        self.assertEqual(raised.exception.category, "api_outcome_unknown")
        request.assert_called_once()
        rows = self.phases(arguments)
        self.assertEqual(rows[-1]["failure_category"], "api_outcome_unknown")
        self.assertEqual([(row["stage"], row["edge"]) for row in rows if row["stage"] == "API"], [("API", "before"), ("API", "terminal")])
        self.assertNotIn(secret, (arguments.output / "provision-phases.jsonl").read_text())
        original = configuration["output"].read_bytes()
        journal = (arguments.output / "provision-phases.jsonl").read_bytes()
        with patch.object(tool.ssl, "SSLContext"), self.assertRaisesRegex(ValueError, "fresh_provisioning_output"):
            tool.execute(arguments, configuration, report)
        self.assertEqual(configuration["output"].read_bytes(), original)
        self.assertEqual((arguments.output / "provision-phases.jsonl").read_bytes(), journal)

    def test_after_resource_failure_retains_API_ack_without_replay(self):
        arguments, configuration, report = self.executable_fixture()
        with patch.object(tool.ssl, "SSLContext"), patch.object(tool, "ContainerResources") as resources, \
                patch.object(tool.provision.API, "call", return_value={"projects": {}, "databases": {}}) as request:
            resources.return_value.ingest.side_effect = [None, None, ValueError("DO-NOT-LOG-postprobe")]
            with self.assertRaises(tool.PhaseFailure) as raised:
                tool.execute(arguments, configuration, report)
        self.assertEqual(raised.exception.category, "resource_after")
        request.assert_called_once()
        rows = self.phases(arguments)
        self.assertEqual(rows[-1]["stage"], "after_resource")
        self.assertEqual(rows[-1]["failure_category"], "resource_after")
        self.assertEqual([row["failure_category"] for row in rows if row["stage"] == "API" and row["edge"] == "terminal"], ["none"])
        self.assertFalse(any(row["stage"] == "confirmation" for row in rows))

    def test_bad_confirmation_remains_failed_with_safe_request_ID(self):
        arguments, configuration, report = self.executable_fixture()
        secret = "DO-NOT-LOG-confirmation-body-secret"
        with patch.object(tool.ssl, "SSLContext"), patch.object(tool, "ContainerResources"), \
                patch.object(tool.provision.API, "call", side_effect=[{"projects": {}, "databases": {}}, {"secret": secret}]) as request:
            with self.assertRaises(tool.PhaseFailure) as raised:
                tool.execute(arguments, configuration, report)
        self.assertEqual(raised.exception.category, "confirmation")
        self.assertEqual(request.call_count, 2)
        rows = self.phases(arguments)
        self.assertEqual(rows[-1]["stage"], "confirmation")
        self.assertEqual(rows[-1]["failure_category"], "confirmation")
        self.assertEqual(rows[-1]["route"], "/v1/projects/create")
        reservation = json.loads(configuration["output"].read_text())
        self.assertEqual(rows[-1]["request_id"], reservation["requests"][0]["request_id"])
        self.assertNotIn(secret, (arguments.output / "provision-phases.jsonl").read_text())
        self.assertFalse((arguments.output / "provisioning-source.json").exists())

    def test_plan_only_never_creates_journal_or_API(self):
        arguments, _, _ = self.executable_fixture()
        argv = [str(source), "--connection", str(arguments.connection), "--monitor-config", str(arguments.monitor_config),
                "--monitor-script", str(arguments.monitor_script), "--monitor-sha256", arguments.monitor_sha256,
                "--output", str(arguments.output), "--label", arguments.label]
        captured = io.StringIO()
        with patch.object(tool.sys, "argv", argv), patch.object(tool.ssl, "SSLContext"), \
                patch.object(tool.provision.API, "call") as request, contextlib.redirect_stdout(captured):
            self.assertEqual(tool.main(), 0)
        request.assert_not_called()
        self.assertEqual(list(arguments.output.iterdir()), [])
        self.assertEqual(json.loads(captured.getvalue())["status"], "PLAN ONLY NO REQUESTS")

    def test_result_failure_is_not_mislabeled_as_confirmation_or_retried(self):
        arguments, configuration, report = self.executable_fixture()
        def interrupted(configuration, plan, label, api):
            api.call("/v1/catalog", configuration["connection"]["global_api_key"])
            raise tool.provision.Failure("output_identity_changed")
        with patch.object(tool.ssl, "SSLContext"), patch.object(tool, "ContainerResources"), \
                patch.object(tool.provision, "provision", side_effect=interrupted), \
                patch.object(tool.provision.API, "call", return_value={"projects": {}, "databases": {}}) as request:
            with self.assertRaises(tool.PhaseFailure) as raised:
                tool.execute(arguments, configuration, report)
        self.assertEqual(raised.exception.category, "result")
        request.assert_called_once()
        rows = self.phases(arguments)
        self.assertEqual(rows[-1]["failure_category"], "result")
        self.assertEqual(len([row for row in rows if row["stage"] == "confirmation" and row["edge"] == "terminal"]), 1)

    def test_existing_journal_is_never_overwritten_and_request_fields_are_safe(self):
        arguments, _, _ = self.executable_fixture()
        journal = tool.PhaseJournal(arguments.output)
        self.addCleanup(journal.stream.close)
        journal.request("DO-NOT-LOG-route", {"request_id": "DO-NOT-LOG-id", "key": "DO-NOT-LOG-key"})
        journal.begin("confirmation")
        journal.terminal("confirmation")
        original = (arguments.output / "provision-phases.jsonl").read_bytes()
        with self.assertRaises(FileExistsError):
            tool.PhaseJournal(arguments.output)
        self.assertEqual((arguments.output / "provision-phases.jsonl").read_bytes(), original)
        self.assertNotIn(b"DO-NOT-LOG", original)


if __name__ == "__main__":
    unittest.main()
