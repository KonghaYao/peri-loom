"""Explicit Main-owned container provisioning; default mode never sends management requests."""

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import ssl
import sys
import types


sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "stress"))
import provision
import run_guarded as guarded
from capacity_plan import TOPOLOGIES, write_private
from capacity_resources import ContainerResources


class PhaseFailure(provision.Failure):
    def __init__(self, category):
        guarded.require(category in {"resource_before", "api_outcome_unknown", "resource_after", "confirmation", "result"}, "invalid_failure_category")
        self.category = category
        super().__init__(category)


class PhaseJournal:
    def __init__(self, directory):
        self.stream = os.fdopen(os.open(directory / "provision-phases.jsonl", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600), "w")
        self.sequence, self.active, self.route, self.request_id = 0, None, "none", ""
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def record(self, edge, category="none"):
        guarded.require(self.sequence < 128, "provision_phase_journal_bound")
        value = {"sequence": self.sequence, "stage": self.active, "edge": edge, "route": self.route,
                 "request_id": self.request_id, "failure_category": category}
        self.stream.write(json.dumps(value, sort_keys=True) + "\n")
        self.stream.flush()
        os.fsync(self.stream.fileno())
        self.sequence += 1

    def begin(self, stage):
        guarded.require(stage in {"before_resource", "API", "after_resource", "confirmation", "result"}, "invalid_phase_stage")
        guarded.require(self.active is None, "provision_phase_already_active")
        self.active = stage
        self.record("before")

    def terminal(self, category="none"):
        if self.active is not None:
            self.record("terminal", category)
            self.active = None

    def phase(self, stage, operation):
        self.begin(stage)
        try:
            result = operation()
        except Exception:
            category = {"before_resource": "resource_before", "API": "api_outcome_unknown",
                        "after_resource": "resource_after", "result": "result"}[stage]
            self.terminal(category)
            raise PhaseFailure(category) from None
        self.terminal()
        return result

    def request(self, route, body):
        self.terminal()
        self.route = route if route in {"/v1/catalog", "/v1/projects/create", "/v1/keys/create", "/v1/databases/create"} else "unsupported"
        request_id = body.get("request_id") if isinstance(body, dict) else ""
        self.request_id = request_id if isinstance(request_id, str) and (request_id == "" or re.fullmatch(r"[a-f0-9]{32}", request_id)) else "unavailable"


class ContainerAPI(provision.API):
    def __init__(self, configuration, probe, resources, journal=None):
        super().__init__(configuration)
        self.probe, self.resources, self.journal = probe, resources, journal

    def call(self, route, key, body=None):
        if self.journal is not None:
            self.journal.request(route, body)
            self.journal.phase("before_resource", lambda: self.resources.ingest(self.probe()))
            result = self.journal.phase("API", lambda: super(ContainerAPI, self).call(route, key, body))
            self.journal.phase("after_resource", lambda: self.resources.ingest(self.probe()))
            self.journal.begin("confirmation")
            return result
        self.resources.ingest(self.probe())
        result = super().call(route, key, body)
        self.resources.ingest(self.probe())
        return result


def topology_connection(connection, topology):
    guarded.require(topology in TOPOLOGIES, "unknown_frozen_topology")
    result = copy.deepcopy(connection)
    others = [database for database in result["databases"] if database != result["database"]]
    guarded.require(others, "second_actual_database_required")
    result["databases"] = others[:1] if topology.endswith("many") else []
    if topology.startswith("single"):
        result.pop("noisy_tenant", None)
    result["processes"] = []
    return result


def prepare(arguments):
    guarded.require(provision.LABEL.fullmatch(arguments.label) is not None, "invalid_label")
    guarded.private(arguments.output, 0o700, True)
    guarded.require(not any(arguments.output.iterdir()), "fresh_provisioning_output_required")
    report = guarded.read_spec(arguments.monitor_config)
    guarded.require(report.get("image", {}).get("Os") == "linux"
                    and report.get("image", {}).get("User") == "10001:10001"
                    and report.get("writer_admission") is True and report.get("capacity_trace") is True,
                    "actual_nonroot_writer_trace_image_required")
    guarded.require(re.fullmatch(r"[a-f0-9]{64}", report.get("container_id", ""))
                    and re.fullmatch(r"sha256:[a-f0-9]{64}", report["image"].get("Id", ""))
                    and re.fullmatch(r"[a-f0-9]{64}", report.get("source_hash", ""))
                    and re.fullmatch(r"[a-f0-9]{32}", report.get("owner", ""))
                    and report.get("container_name") == "peri-verify-" + report["owner"],
                    "owned_container_identity_required")
    source = guarded.read_spec(arguments.connection)
    address = provision.endpoint(source.get("endpoint"))
    guarded.require(address.hostname == "127.0.0.1"
                    and str(address.port) == str(report.get("runtime", {}).get("port")),
                    "connection_must_match_owned_container_publication")
    provision.credential(source.get("global_api_key"))
    workers = ["worker-a", "worker-b"]
    guarded.require(source.get("worker_ids", workers) == workers, "actual_two_worker_ids_required")
    certificate = Path(source["ca_file"])
    guarded.require(certificate.is_relative_to(arguments.connection.parent), "owned_fixture_CA_required")
    certificate_bytes = provision.read_file(certificate, provision.LIMIT, secret=False)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_verify_locations(cadata=certificate_bytes.decode("ascii"))
    guarded.private(arguments.monitor_script, 0o600)
    monitor_bytes = arguments.monitor_script.read_bytes()
    guarded.require(len(monitor_bytes) <= 65536 and re.fullmatch(r"[a-f0-9]{64}", arguments.monitor_sha256)
                    and hashlib.sha256(monitor_bytes).hexdigest() == arguments.monitor_sha256,
                    "frozen_monitor_source_required")
    configuration = {"directory": arguments.output, "output": arguments.output / "provisioned-private.json",
                     "connection": source, "context": context, "workers": workers, "processes": []}
    return configuration, report


def execute(arguments, configuration, report):
    current_configuration, current_report = prepare(arguments)
    guarded.require(current_report == report and current_configuration["connection"] == configuration["connection"],
                    "prepared_fixture_changed_before_execution")
    identities = {path: hashlib.sha256(path.read_bytes()).hexdigest()
                  for path in (arguments.connection, arguments.monitor_config, arguments.monitor_script)}
    monitor_bytes = arguments.monitor_script.read_bytes()
    guarded.require(hashlib.sha256(monitor_bytes).hexdigest() == arguments.monitor_sha256,
                    "frozen_monitor_changed_before_execution")
    journal = PhaseJournal(arguments.output)
    try:
        journal.begin("result")
        monitor = types.ModuleType("owned_capacity_monitor")
        monitor.__file__ = str(arguments.monitor_script)
        exec(compile(monitor_bytes, str(arguments.monitor_script), "exec"), monitor.__dict__)
        environment = {name: os.environ[name] for name in
                       ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG", "XDG_RUNTIME_DIR", "TMPDIR")
                       if name in os.environ}
        def probe():
            guarded.require(all(hashlib.sha256(path.read_bytes()).hexdigest() == digest
                                for path, digest in identities.items()), "frozen_provisioning_input_changed")
            return monitor.snapshot(report, environment)
        resources = ContainerResources(report, 536870912)
        journal.terminal()
        journal.phase("before_resource", lambda: resources.ingest(probe()))
        provision.provision(configuration, provision.build_plan(arguments.label), arguments.label,
                            api=ContainerAPI(configuration, probe, resources, journal))
        journal.terminal()
        journal.begin("result")
        original = guarded.read_spec(configuration["output"])
        for topology in TOPOLOGIES:
            write_private(arguments.output / (topology + ".json"), topology_connection(original, topology))
        write_private(arguments.output / "provisioning-source.json",
                      {"status": "PROVISIONED NOT CAPACITY ACCEPTED", "inputs": {str(path): digest for path, digest in identities.items()},
                       "container_id": report["container_id"], "image_id": report["image"]["Id"],
                       "source_hash": report["source_hash"], "host_native_PIDs_claimed": False})
        journal.terminal()
    except Exception as failure:
        confirmation_codes = {"invalid_catalog", "label_already_used", "project_confirmation_mismatch", "key_confirmation_mismatch",
                              "key_identity_mismatch", "database_confirmation_mismatch", "invalid_credential"}
        code = failure.args[0] if failure.args and isinstance(failure.args[0], str) else None
        category = failure.category if isinstance(failure, PhaseFailure) else "confirmation" if isinstance(failure, provision.Failure) and journal.active == "confirmation" and code in confirmation_codes else "result"
        journal.terminal(category)
        raise PhaseFailure(category) from None
    finally:
        journal.stream.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connection", type=Path, required=True)
    parser.add_argument("--monitor-config", type=Path, required=True)
    parser.add_argument("--monitor-script", type=Path, required=True)
    parser.add_argument("--monitor-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--execute", action="store_true")
    arguments = parser.parse_args()
    os.umask(0o077)
    try:
        configuration, report = prepare(arguments)
        if arguments.execute:
            execute(arguments, configuration, report)
        print(json.dumps({"status": "PROVISIONED NOT ACCEPTED" if arguments.execute else "PLAN ONLY NO REQUESTS",
                          "topologies": list(TOPOLOGIES), "performance_pass": False}))
        return 0
    except PhaseFailure as failure:
        print(json.dumps({"status": "HOLD", "reason": "retain_private_provisioning_evidence_no_retry", "failure_category": failure.category}))
        return 1
    except (ValueError, OSError, KeyError, TypeError, RuntimeError, provision.Failure):
        print(json.dumps({"status": "HOLD", "reason": "retain_private_provisioning_evidence_no_retry"}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
