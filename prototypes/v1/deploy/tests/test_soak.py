import copy
import importlib.util
import io
import json
import pathlib
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import urllib.error


specification = importlib.util.spec_from_file_location("soak", pathlib.Path(__file__).resolve().parents[1] / "soak.py")
soak = importlib.util.module_from_spec(specification)
specification.loader.exec_module(soak)


def sample(elapsed=0):
    project = dict.fromkeys(("active", "sessions", "unknown_held", "control_active", "owner_active", "classifying"), 0)
    worker = {"sql_inflight": 0, "sessions": 0, "resources": {"active_sessions": 0, "loading_databases": 0}, "retirement": dict.fromkeys(("quarantined_databases", "retiring_databases", "pending_statements", "pending_io"), 0)}
    return {"elapsed_seconds": elapsed, "observation_started_elapsed_seconds": elapsed, "processes": {name: {"pid": index + 1, "start_ticks": 10, "uids": [10001] * 4, "rss_bytes": 10 * 1024 * 1024, "anon_bytes": 1024, "threads": 1, "fds": 8} for index, name in enumerate(("authority", "worker-a", "worker-b", "platform", "launcher"))}, "metrics": {"errors_total": {"sql": 0}, "active_permits": 0, "unknown_held_permits": 0, "projects": {"default": project}, "native": {"authority": {name: {"total": 1, "errors": 0} for name in ("get", "put", "checkpoint", "wal_durability")}, "workers": {"worker-a": copy.deepcopy(worker), "worker-b": copy.deepcopy(worker)}}}, "storage_file_bytes": dict.fromkeys(("backend", "cache-a", "cache-b", "authority", "platform", "audit"), 1024), "backend_object_bytes": 1024}


def arguments(**overrides):
    values = dict(mode="smoke", duration=20, warmup=0, rate=0.2, seed=20261008, writer_admission=True, image="sha256:" + "a" * 64, backend="local", capacity_evidence=None, capacity_approval=None)
    values.update(overrides)
    return SimpleNamespace(**values)


class VirtualClock:
    def __init__(self):
        self.now = 0

    def monotonic(self):
        return self.now

    def sleep(self, duration):
        self.now += duration


class ResourceTests(unittest.TestCase):
    def test_real_gateway_sparse_zero_error_map_is_available(self):
        metrics = sample()["metrics"]
        metrics["errors_total"] = {}
        soak.check_quiescent(metrics)
        self.assertEqual(soak.metrics_evidence(metrics)["errors_total"], {"total": 0})

    def test_missing_gateway_error_map_is_not_zero(self):
        metrics = sample()["metrics"]
        del metrics["errors_total"]
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            soak.check_quiescent(metrics)

    def test_missing_or_failed_authority_and_errors_fail_closed(self):
        for mutation in (lambda metrics: metrics.pop("errors_total"), lambda metrics: metrics.update(errors_total=None), lambda metrics: metrics.update(errors_total={"sql": 1}), lambda metrics: metrics["native"].pop("authority"), lambda metrics: metrics["native"]["authority"]["get"].update(errors=1), lambda metrics: metrics["native"]["authority"]["get"].pop("total")):
            with self.subTest(mutation=mutation):
                metrics = sample()["metrics"]
                mutation(metrics)
                with self.assertRaises(RuntimeError):
                    soak.check_quiescent(metrics)

    def test_every_uid_is_required(self):
        for name in sample()["processes"]:
            for index in range(4):
                samples = [sample(0), sample(10)]
                samples[-1]["processes"][name]["uids"][index] = 0
                with self.subTest(process=name, uid=index), self.assertRaisesRegex(RuntimeError, "identities"):
                    soak.evaluate(samples, 10, soak.LIMITS, False)

    def test_timestamp_schema_and_endpoint_are_strict(self):
        for value in (None, False, float("nan"), float("inf"), -1):
            samples = [sample(0), sample(10)]
            samples[-1]["observation_started_elapsed_seconds"] = value
            with self.subTest(value=value), self.assertRaisesRegex(RuntimeError, "time schema"):
                soak.evaluate(samples, 10, soak.LIMITS, False)
        for samples, duration in (([sample(0), sample(10)], 11), ([sample(0), sample(10)], 9), ([sample(0), sample(0)], 10)):
            with self.assertRaises(RuntimeError):
                soak.evaluate(samples, duration, soak.LIMITS, False)

    def test_full_hour_with_required_observations(self):
        samples = [sample(elapsed) for elapsed in range(0, 3601, 10)]
        result = soak.evaluate(samples, 3600, soak.LIMITS, True)
        self.assertEqual(result["samples"], 361)
        self.assertEqual(result["tail_rss_bytes_per_second"], 0)

    def test_short_run_cannot_qualify(self):
        with self.assertRaisesRegex(RuntimeError, "duration"):
            soak.evaluate([sample(0), sample(30)], 30, soak.LIMITS, True)

    def test_missing_retirement_observation_not_zero(self):
        observed = sample()
        del observed["metrics"]["native"]["workers"]["worker-a"]["retirement"]
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            soak.check_quiescent(observed["metrics"])

    def test_pending_native_io_fails(self):
        observed = sample()
        observed["metrics"]["native"]["workers"]["worker-a"]["retirement"]["pending_io"] = 1
        with self.assertRaisesRegex(RuntimeError, "pending"):
            soak.check_quiescent(observed["metrics"])

    def test_boolean_native_counter_fails(self):
        observed = sample()
        observed["metrics"]["native"]["workers"]["worker-a"]["retirement"]["pending_io"] = False
        with self.assertRaisesRegex(RuntimeError, "pending"):
            soak.check_quiescent(observed["metrics"])

    def test_unknown_permit_fails(self):
        observed = sample()
        observed["metrics"]["unknown_held_permits"] = 1
        with self.assertRaisesRegex(RuntimeError, "permits"):
            soak.check_quiescent(observed["metrics"])

    def test_missing_or_invalid_process_identity_cannot_qualify(self):
        for field in ("pid", "start_ticks"):
            for invalid in (None, False, 0, -1, "123"):
                samples = [sample(0), sample(10)]
                for observed in samples:
                    observed["processes"]["worker-a"][field] = invalid
                with self.subTest(field=field, invalid=invalid), self.assertRaisesRegex(RuntimeError, "identity"):
                    soak.evaluate(samples, 10, soak.LIMITS, False)

    def test_identity_reuse_fails(self):
        samples = [sample(0), sample(10)]
        samples[1]["processes"]["platform"]["start_ticks"] = 11
        with self.assertRaisesRegex(RuntimeError, "identity"):
            soak.evaluate(samples, 10, soak.LIMITS, False)

    def test_large_observation_gap_fails(self):
        with self.assertRaisesRegex(RuntimeError, "gap"):
            soak.evaluate([sample(0), sample(40)], 40, soak.LIMITS, False)

    def test_peak_rss_fails(self):
        samples = [sample(0), sample(10)]
        samples[1]["processes"]["platform"]["rss_bytes"] = 2 * 1024 * 1024 * 1024
        with self.assertRaisesRegex(RuntimeError, "resource limit"):
            soak.evaluate(samples, 10, soak.LIMITS, False)

    def test_growth_and_tail_are_final_not_transient_gate(self):
        samples = [sample(elapsed) for elapsed in range(0, 3601, 10)]
        for observed in samples:
            observed["processes"]["platform"]["rss_bytes"] += int(max(0, observed["elapsed_seconds"] - 2700) * 20000)
        soak.evaluate(samples, 3600, soak.LIMITS, False, trends=False)
        with self.assertRaisesRegex(RuntimeError, "tail memory"):
            soak.evaluate(samples, 3600, soak.LIMITS, True)

    def test_fd_growth_fails(self):
        samples = [sample(elapsed) for elapsed in range(0, 3601, 10)]
        for observed in samples:
            if observed["elapsed_seconds"] >= 3300:
                observed["processes"]["platform"]["fds"] += 13
        with self.assertRaisesRegex(RuntimeError, "growth"):
            soak.evaluate(samples, 3600, soak.LIMITS, True)

    def test_missing_storage_directory_cannot_qualify(self):
        for name in ("backend", "cache-a", "cache-b", "authority", "platform", "audit"):
            samples = [sample(0), sample(10)]
            for observed in samples:
                del observed["storage_file_bytes"][name]
            with self.subTest(name=name), self.assertRaisesRegex(RuntimeError, "storage observations unavailable"):
                soak.evaluate(samples, 10, soak.LIMITS, False)

    def test_storage_cap_fails(self):
        samples = [sample(0), sample(10)]
        samples[1]["backend_object_bytes"] = soak.LIMITS["maximum_backend_object_bytes"] + 1
        with self.assertRaisesRegex(RuntimeError, "storage"):
            soak.evaluate(samples, 10, soak.LIMITS, False)


class ProtocolTests(unittest.TestCase):
    def client(self, result):
        journal = io.StringIO()
        config = {"endpoint": "https://127.0.0.1:9443", "database": "synthetic", "ca_file": "synthetic.crt", "project_api_key": "synthetic-secret", "global_api_key": "synthetic-admin"}
        with patch.object(soak.ssl, "create_default_context"), patch.object(soak.urllib.request, "build_opener") as builder:
            client = soak.Client(config, journal)
        def respond(request, **options):
            payload = json.loads(request.data)
            response = Mock(status=200)
            response.__enter__ = Mock(return_value=response)
            response.__exit__ = Mock(return_value=False)
            response.read.return_value = json.dumps({"version": 1, "request_id": payload["request_id"], "ok": True, "error": None, "result": result}).encode()
            return response
        client.transport = Mock()
        client.transport.open.side_effect = respond
        return client, journal

    def test_commit_ack_correlates_without_sql_or_credentials(self):
        client, journal = self.client({"executed": True})
        client.phase = "measure"
        client.execute("private-session", "COMMIT")
        rows = [json.loads(line) for line in journal.getvalue().splitlines()]
        self.assertEqual(rows[-1]["sql_action"], "COMMIT")
        self.assertEqual(rows[-1]["request_id"], client.last_reply_id)
        self.assertEqual(rows[-1]["result_flags"], {"executed": True})
        self.assertNotIn("synthetic-secret", journal.getvalue())
        self.assertNotIn("private-session", journal.getvalue())

    def test_close_missing_ack_is_not_success(self):
        client, journal = self.client({"closed": False})
        with self.assertRaisesRegex(RuntimeError, "Close"):
            client.closed("private-session")
        self.assertEqual(client.transport.open.call_count, 1)

    def test_transport_unknown_never_retries(self):
        client, journal = self.client(None)
        client.transport.open.side_effect = urllib.error.URLError("synthetic transport failure")
        with self.assertRaises(urllib.error.URLError):
            client.execute("private-session", "COMMIT")
        self.assertEqual(client.transport.open.call_count, 1)
        self.assertEqual(len(journal.getvalue().splitlines()), 1)

    def test_integer_execute_flag_is_not_proof(self):
        client, journal = self.client({"executed": 1})
        with self.assertRaisesRegex(RuntimeError, "Execute"):
            client.execute("private-session", "COMMIT")

    def test_transaction_unknown_has_one_dispatch_no_close(self):
        client, journal = self.client(None)
        client.transport.open.side_effect = urllib.error.URLError("private-session synthetic-secret")
        runner = soak.Soak.__new__(soak.Soak)
        runner.random = Mock()
        runner.random.randint.return_value = 1
        client.opened = Mock(return_value="private-session")
        client.query = Mock(return_value=[[3]])
        client.closed = Mock()
        with self.assertRaises(urllib.error.URLError):
            runner.transaction(client, 1, io.StringIO())
        self.assertEqual(client.transport.open.call_count, 1)
        self.assertEqual(len(journal.getvalue().splitlines()), 1)
        self.assertEqual(json.loads(journal.getvalue())["sql_action"], "BEGIN")
        client.closed.assert_not_called()
        self.assertNotIn("private-session", journal.getvalue())
        self.assertNotIn("synthetic-secret", journal.getvalue())


class WorkloadTests(unittest.TestCase):
    def fixture(self, directory):
        fixture = Mock(directory=pathlib.Path(directory), fixture=pathlib.Path(directory), container="synthetic", report={"cleanup": "removed_owned_container"}, smoke_proven=False)
        (fixture.fixture / "connection.json").write_text("{}")
        fixture.docker.return_value.stdout = json.dumps(sample())
        return fixture

    def workload(self, final_delay=0.01, transaction_delay=4.1, **overrides):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        clock = VirtualClock()
        with patch.object(soak.image, "Verification", return_value=self.fixture(temporary.name)):
            runner = soak.Soak(arguments(mode="soak", duration=3600, warmup=60, **overrides))
        self.latest_runner = runner
        client = Mock(config={"database": "synthetic"})
        client.metrics.return_value = sample()["metrics"]
        client.call.return_value = {"checkpointed": True}
        runner.setup = Mock()
        runner.transaction = Mock(side_effect=lambda *unused: clock.sleep(transaction_delay))
        runner.oracle = Mock(side_effect=lambda current: clock.sleep(final_delay if current.phase == "final_oracle" else 0.01))
        with patch.object(soak, "Client", return_value=client), patch.object(soak.time, "monotonic", clock.monotonic), patch.object(soak.time, "sleep", clock.sleep):
            runner.workload()
        return runner

    def test_default_point_two_rate_samples_full_hour_fixed_axis(self):
        runner = self.workload()
        self.assertGreaterEqual(len(runner.samples), 361)
        self.assertEqual(runner.result["measurement_elapsed_seconds"], 3600)
        self.assertGreater(runner.result["elapsed_seconds"], 3600)
        self.assertTrue(runner.fixture.smoke_proven)
        self.assertGreaterEqual(runner.result["completed_transactions"], 576)
        self.assertTrue(all(finish["elapsed_seconds"] - start["elapsed_seconds"] <= 30 for start, finish in zip(runner.samples, runner.samples[1:])))

    def test_final_oracle_real_120_second_gap_is_not_hidden(self):
        with self.assertRaisesRegex(RuntimeError, "gap"):
            self.workload(final_delay=120)

    def test_progress_failure_retains_measured_counter_conservation(self):
        with self.assertRaisesRegex(RuntimeError, "progress minimum"):
            self.workload(transaction_delay=8)
        result = self.latest_runner.result
        self.assertEqual(result["offered_transactions"], result["completed_transactions"] + result["generator_dropped"])
        self.assertGreater(result["generator_dropped"], 0)
        self.assertTrue(result["durable_prefix_verified"])
        self.assertFalse(self.latest_runner.fixture.smoke_proven)

    def test_sampling_independent_of_sparse_transactions(self):
        runner = self.workload(rate=0.01, transaction_delay=0.1)
        self.assertGreaterEqual(len(runner.samples), 361)

    def test_failure_observation_persisted_and_redacted(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.fixture(directory)
            observed = sample()
            observed["secret"] = "synthetic-secret"
            observed["processes"]["platform"]["command"] = "synthetic-secret"
            fixture.docker.return_value.stdout = json.dumps(observed)
            with patch.object(soak.image, "Verification", return_value=fixture):
                runner = soak.Soak(arguments())
            client = Mock()
            client.metrics.side_effect = RuntimeError("synthetic-secret")
            destination = pathlib.Path(directory) / "observations.jsonl"
            with soak.private_stream(destination) as stream, self.assertRaises(RuntimeError):
                runner.sample(client, soak.time.monotonic(), stream)
            encoded = destination.read_text()
            self.assertNotIn("synthetic-secret", encoded)
            self.assertEqual(json.loads(encoded)["event"], "observation_failure")
            self.assertEqual(destination.stat().st_mode & 0o777, 0o600)


    def test_failed_quiescence_preserves_sanitized_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.fixture(directory)
            with patch.object(soak.image, "Verification", return_value=fixture):
                runner = soak.Soak(arguments())
            client = Mock()
            metrics = sample()["metrics"]
            metrics["unknown_held_permits"] = 1
            metrics["credential"] = "synthetic-secret"
            client.metrics.return_value = metrics
            destination = pathlib.Path(directory) / "observations.jsonl"
            with soak.private_stream(destination) as stream, self.assertRaisesRegex(RuntimeError, "permits"):
                runner.sample(client, soak.time.monotonic(), stream)
            encoded = destination.read_text()
            self.assertNotIn("synthetic-secret", encoded)
            self.assertEqual(json.loads(encoded)["partial"]["metrics"]["unknown_held_permits"], 1)

    def test_malformed_metrics_redaction_does_not_manufacture_zero(self):
        evidence = soak.metrics_evidence({"native": None, "errors_total": {"synthetic-secret": False}, "credential": "synthetic-secret"})
        self.assertIsNone(evidence["errors_total"]["total"])
        self.assertIsNone(evidence["native"]["workers"]["worker-a"]["retirement"]["pending_io"])
        self.assertNotIn("synthetic-secret", json.dumps(evidence))


class ApprovalTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = pathlib.Path(self.temporary.name)
        self.raw_path = self.directory / "capacity-result.json"
        self.approval_path = self.directory / "approval.json"
        self.source_hashes = {"soak.py": "synthetic"}
        self.args = arguments(mode="soak", duration=3600, warmup=60, capacity_evidence=str(self.raw_path), capacity_approval=str(self.approval_path))
        artifact = self.directory / "raw.jsonl"
        artifact.write_text("mock evidence only")
        self.raw = {"task": "D05/Q02", "status": "READY FOR INDEPENDENT ACCEPTANCE", "safe_envelope": {"status": "CANDIDATE FOR MAIN APPROVAL", "d05_full_matrix_status": "READY FOR INDEPENDENT ACCEPTANCE", "performance_pass": False, "approval_required": True, "image_id": self.args.image, "writer_admission": True, "source": "synthetic-git", "source_hash": "synthetic-git", "rpc_definition": "synthetic RPC", "safe_rpc_per_second": 3}, "evidence_manifest": {str(artifact): {"bytes": artifact.stat().st_size, "sha256": soak.image.digest(artifact)}}}
        self.approval = {"status": "PASS", "verified_by": "Main", "independent_review": True, "result_sha256": None, "image_id": self.args.image, "writer_admission": True, "source": "synthetic-git", "source_sha256": self.source_hashes, "rpc_definition": "synthetic RPC", "rpc_rate_safe": 2.28}

    def save(self):
        self.raw_path.write_text(json.dumps(self.raw))
        self.raw_path.chmod(0o600)
        self.approval["result_sha256"] = soak.image.digest(self.raw_path)
        self.approval_path.write_text(json.dumps(self.approval))
        self.approval_path.chmod(0o600)

    def approve(self):
        with patch.object(soak.subprocess, "check_output", return_value="synthetic-git\n"), patch.object(soak, "verify_capacity_raw"):
            return soak.approve_capacity(self.args, self.source_hashes)

    def test_raw_recomputation_rejection_is_not_an_approval(self):
        self.save()
        with patch.object(soak.subprocess, "check_output", return_value="synthetic-git\n"), patch.object(soak, "verify_capacity_raw", side_effect=RuntimeError("raw cannot reproduce")) as verifier:
            with self.assertRaisesRegex(RuntimeError, "reproduce"):
                soak.approve_capacity(self.args, self.source_hashes)
            verifier.assert_called_once_with(self.raw_path.resolve(), self.approval_path.resolve(), image_id=self.args.image, source="synthetic-git", writer_admission=True, tx_per_second=0.2, source_sha256=self.source_hashes)

    def test_real_validator_rejects_unsealed_mock_evidence(self):
        definition = "gateway SQL business API RPC: actions_total(open+execute+query+close) delta / observed elapsed; excludes management API and native internal RPC"
        self.raw.update(version=1, performance_pass=False)
        self.raw["safe_envelope"]["rpc_definition"] = definition
        self.approval["rpc_definition"] = definition
        self.save()
        with patch.object(soak.subprocess, "check_output", return_value="synthetic-git\n"):
            with self.assertRaises((RuntimeError, ValueError)):
                soak.approve_capacity(self.args, self.source_hashes)

    def test_missing_capsule_does_not_accept_candidate(self):
        self.save()
        self.approval_path.unlink()
        with self.assertRaises(RuntimeError):
            self.approve()

    def test_full_matrix_not_ready_cannot_qualify(self):
        for status in ("FAIL", "NOT MEASURED"):
            self.raw["status"] = status
            self.raw["safe_envelope"]["d05_full_matrix_status"] = status
            self.save()
            with self.subTest(status=status), self.assertRaises(RuntimeError):
                self.approve()

    def test_approval_rate_tracks_nondefault_tx_demand(self):
        self.args.rate = 0.3
        self.save()
        with self.assertRaises(RuntimeError):
            self.approve()
        self.raw["safe_envelope"]["safe_rpc_per_second"] = 4
        self.approval["rpc_rate_safe"] = 3.24
        self.save()
        self.assertAlmostEqual(self.approve()["required_rpc_with_margin"], 3.24)

    def test_mock_independent_capsule_exact_margin(self):
        self.save()
        result = self.approve()
        self.assertAlmostEqual(result["required_rpc_with_margin"], 2.28)

    def test_candidate_and_binding_mutations_fail_closed(self):
        for key, value in (("status", "CANDIDATE FOR MAIN APPROVAL"), ("verified_by", "Singer"), ("independent_review", False), ("image_id", "other-image"), ("source", "other-source"), ("source_sha256", {}), ("rpc_definition", "business ops"), ("writer_admission", False), ("rpc_rate_safe", 2.279), ("rpc_rate_safe", 4), ("rpc_rate_safe", float("nan")), ("rpc_rate_safe", True)):
            with self.subTest(key=key, value=value):
                original = copy.deepcopy(self.approval)
                self.approval[key] = value
                self.save()
                with self.assertRaises(RuntimeError):
                    self.approve()
                self.approval = original

    def test_raw_result_and_raw_artifact_hash_binding(self):
        self.save()
        self.raw_path.write_text(self.raw_path.read_text() + " ")
        with self.assertRaises(RuntimeError):
            self.approve()
        self.save()
        (self.directory / "raw.jsonl").write_text("mutated")
        with self.assertRaises(RuntimeError):
            self.approve()

    def test_frozen_qualifying_controls(self):
        self.save()
        for key, value in (("duration", 3599), ("warmup", 59), ("seed", 1), ("writer_admission", False)):
            with self.subTest(key=key):
                original = getattr(self.args, key)
                setattr(self.args, key, value)
                with self.assertRaises(RuntimeError):
                    self.approve()
                setattr(self.args, key, original)


class PublicationTests(unittest.TestCase):
    def runner(self, directory):
        fixture = Mock(directory=pathlib.Path(directory), report={"cleanup": "removed_owned_container"}, smoke_proven=False)
        with patch.object(soak.image, "Verification", return_value=fixture):
            runner = soak.Soak(arguments())
        def workload():
            fixture.smoke_proven = True
            for name in ("requests.jsonl", "observations.jsonl", "commit-acks.jsonl", "oracle.json", "authority.lock"):
                (fixture.directory / name).write_text("clean\n" if name == "authority.lock" else "{}\n")
        runner.workload = workload
        return runner

    def assert_failed(self, runner):
        self.assertFalse(runner.run())
        self.assertFalse(runner.result["pass"])
        destination = runner.fixture.directory / "soak-result.json"
        if destination.exists():
            self.assertFalse(json.loads(destination.read_text())["pass"])
        self.assertFalse((runner.fixture.directory / ".soak-result.pending.json").exists())
        self.assertFalse((runner.fixture.directory / "soak-manifest.json").exists())

    def test_ready_source_mismatch_is_redacted_and_identified(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = self.runner(directory)
            runner.fixture.ready.side_effect = RuntimeError("image launcher differs from current full launcher")
            self.assert_failed(runner)
            self.assertEqual(runner.result["failure_stage"], "ready")
            self.assertEqual(runner.result["failure_code"], "image_launcher_source_mismatch")
            self.assertFalse(runner.fixture.smoke_proven)

    def test_progress_gate_failure_is_not_published_as_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = self.runner(directory)
            runner.workload = Mock(side_effect=RuntimeError("frozen workload progress minimum not met"))
            self.assert_failed(runner)
            self.assertEqual(runner.result["failure_stage"], "workload")
            self.assertEqual(runner.result["failure_code"], "workload_progress_below_frozen_minimum")

    def test_mock_smoke_publication_is_not_long_soak(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = self.runner(directory)
            self.assertTrue(runner.run())
            self.assertFalse(runner.result["qualifies_long_soak"])
            manifest = json.loads((runner.fixture.directory / "soak-manifest.json").read_text())
            self.assertEqual(manifest["result_sha256"], soak.image.digest(runner.fixture.directory / "soak-result.json"))

    def test_result_pending_and_manifest_write_failures(self):
        for name in ("soak-result.json", ".soak-result.pending.json", "soak-manifest.json"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                runner = self.runner(directory)
                writer = soak.image.private_json
                def write(path, value):
                    if path.name == name:
                        raise OSError("synthetic-secret")
                    writer(path, value)
                with patch.object(soak.image, "private_json", side_effect=write):
                    self.assert_failed(runner)

    def test_hash_and_final_replace_failures(self):
        for operation in ("digest", "replace"):
            with self.subTest(operation=operation), tempfile.TemporaryDirectory() as directory:
                runner = self.runner(directory)
                target = soak.image if operation == "digest" else soak.os
                with patch.object(target, operation, side_effect=OSError("synthetic-secret")):
                    self.assert_failed(runner)

    def test_source_hash_mismatch_cannot_publish(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = self.runner(directory)
            runner.source_start[str(runner.sources[0].relative_to(soak.ROOT))] = "mutated"
            self.assert_failed(runner)

    def test_replace_that_publishes_then_raises_revokes_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = self.runner(directory)
            replace = soak.os.replace
            def publish_then_fail(source, destination):
                replace(source, destination)
                raise OSError("synthetic-secret")
            with patch.object(soak.os, "replace", side_effect=publish_then_fail):
                self.assert_failed(runner)

    def test_manifest_readback_or_artifact_tamper(self):
        for artifact in ("soak-manifest.json", "requests.jsonl"):
            with self.subTest(artifact=artifact), tempfile.TemporaryDirectory() as directory:
                runner = self.runner(directory)
                writer = soak.image.private_json
                def write(path, value):
                    writer(path, value)
                    if path.name == "soak-manifest.json":
                        (runner.fixture.directory / artifact).write_text("{}")
                with patch.object(soak.image, "private_json", side_effect=write):
                    self.assert_failed(runner)

    def test_capacity_rejection_happens_before_prepare(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = self.runner(directory)
            runner.arguments = arguments(mode="soak", duration=3600, warmup=60)
            self.assert_failed(runner)
            runner.fixture.prepare.assert_not_called()

    def test_cleanup_failure_cannot_publish_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = self.runner(directory)
            runner.fixture.cleanup.side_effect = OSError("synthetic-secret")
            self.assert_failed(runner)


if __name__ == "__main__":
    unittest.main()
