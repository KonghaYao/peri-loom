import argparse
import http.client
import ipaddress
import json
import os
import pathlib
import re
import secrets
import ssl
import stat
import sys
import time
import urllib.parse


LIMIT = 1024 * 1024
LABEL = re.compile(r"[a-z][a-z0-9_-]{0,23}\Z")
QUOTAS = {"max_databases": 8, "max_sessions": 16, "max_concurrent": 4,
          "requests_per_second": 100, "max_sql_bytes": 65536, "max_result_bytes": 4194304}


class Failure(Exception):
    pass


def require(condition, code):
    if not condition:
        raise Failure(code)


def unique_object(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, "duplicate_json_field")
        value[key] = item
    return value


def invalid_constant(value):
    raise Failure("nonfinite_json")


def decode(encoded):
    return json.loads(encoded, object_pairs_hook=unique_object, parse_constant=invalid_constant)


def canonical(value):
    path = pathlib.Path(value)
    require(path.is_absolute() and str(path.resolve()) == str(value), "canonical_absolute_path_required")
    return path


def private_directory(path):
    metadata = path.lstat()
    require(stat.S_ISDIR(metadata.st_mode) and metadata.st_uid == os.geteuid()
            and stat.S_IMODE(metadata.st_mode) == 0o700, "private_owned_directory_required")


def validate_file(metadata, limit, secret):
    require(stat.S_ISREG(metadata.st_mode) and metadata.st_uid == os.geteuid()
            and metadata.st_nlink == 1 and 0 <= metadata.st_size <= limit, "bounded_owned_file_required")
    require(stat.S_IMODE(metadata.st_mode) == 0o600 if secret else metadata.st_mode & 0o022 == 0,
            "unsafe_file_permissions")


def read_file(path, limit=65536, secret=True):
    canonical(str(path))
    before = path.lstat()
    validate_file(before, limit, secret)
    descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        after = os.fstat(descriptor)
        validate_file(after, limit, secret)
        require((before.st_dev, before.st_ino) == (after.st_dev, after.st_ino), "input_identity_changed")
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            encoded = source.read(limit + 1)
        require(len(encoded) <= limit, "file_too_large")
        return encoded
    finally:
        os.close(descriptor)


def credential(value):
    require(isinstance(value, str) and 32 <= len(value) <= 4096
            and all(33 <= ord(character) <= 126 for character in value), "invalid_credential")
    return value


def endpoint(value, tls=True):
    require(isinstance(value, str) and all(33 <= ord(character) <= 126 for character in value), "invalid_endpoint")
    parsed = urllib.parse.urlsplit(value)
    require(parsed.scheme == ("https" if tls else "http") and parsed.username is None
            and parsed.password is None and not parsed.path and not parsed.query
            and not parsed.fragment and parsed.port is not None and 1 <= parsed.port <= 65535
            and "%" not in parsed.hostname, "literal_loopback_endpoint_required")
    require(ipaddress.ip_address(parsed.hostname).is_loopback, "literal_loopback_endpoint_required")
    return parsed


def fixture(run_dir, output, label):
    require(LABEL.fullmatch(label) is not None, "invalid_label")
    directory = canonical(run_dir)
    require(directory.name.startswith("peri-stress-") and len(directory.name) >= 24, "isolated_stress_run_dir_required")
    private_directory(directory)
    destination = canonical(output)
    private_directory(destination.parent)
    require(not os.path.lexists(destination), "output_already_exists")
    require(not os.path.lexists(directory / (".stress-provision-" + label + ".json")), "label_already_reserved")
    launch = decode(read_file(directory / "launch.json"))
    topology = decode(read_file(directory / "topology.json"))
    connection = decode(read_file(directory / "connection.json"))
    require(launch.get("backend") == "s3" and launch.get("tls") is True
            and launch.get("storage_identity", {}).get("backend") == "s3", "s3_tls_fixture_required")
    endpoint(connection.get("endpoint"))
    require(topology.get("gateway") == connection["endpoint"], "gateway_topology_mismatch")
    require(connection.get("database") == {"namespace": {"tenant": "local_default", "database": "demo"},
                                           "branch": "main"}, "launcher_bootstrap_identity_required")
    credential(connection.get("global_api_key"))
    certificate = canonical(connection.get("ca_file"))
    require(certificate.is_relative_to(directory), "fixture_ca_required")
    ca_bytes = read_file(certificate, LIMIT, secret=False)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_verify_locations(cadata=ca_bytes.decode("ascii"))
    workers = sorted(launch.get("workers", {}))
    require(len(workers) == 2 and set(workers) == set(topology.get("workers", {})), "two_worker_topology_required")
    endpoint(topology.get("authority"), tls=False)
    require(topology["authority"] == "http://" + launch.get("authority", ""), "authority_topology_mismatch")
    processes = topology.get("processes", {})
    require(set(processes) == {"authority", "platform", *workers}, "unexpected_process_topology")
    pids = [processes[role].get("pid") for role in processes]
    require(all(type(pid) is int and pid > 1 for pid in pids) and len(set(pids)) == 4, "invalid_process_pids")
    for worker in workers:
        require(re.fullmatch(r"[A-Za-z0-9_-]{1,64}", worker) is not None, "invalid_worker_id")
        endpoint(topology["workers"][worker], tls=False)
        require(topology["workers"][worker] == "http://" + launch["workers"][worker]["address"], "worker_topology_mismatch")
    return {"directory": directory, "output": destination, "connection": connection, "context": context,
            "workers": workers, "processes": [{"role": role, "pid": processes[role]["pid"]} for role in sorted(processes)]}


def build_plan(label):
    project = "stress_" + label
    noisy_project = "noisy_" + label
    databases = [{"namespace": {"tenant": "local_" + project, "database": project + suffix}, "branch": "main"}
                 for suffix in ("_main", "_many_1", "_many_2", "_many_3")]
    noisy_database = {"namespace": {"tenant": "local_" + noisy_project, "database": "stress_noisy_" + label}, "branch": "main"}
    operations = []
    for identity, targets in ((project, databases), (noisy_project, [noisy_database])):
        operations.append({"route": "/v1/projects/create", "key": "global",
                           "body": {"id": identity, "organization": "local", "region": "local"}})
        operations.append({"route": "/v1/keys/create", "key": "global",
                           "body": {"subject": identity + "_service", "project": identity, "role": "admin", "service_account": True}})
        operations.extend({"route": "/v1/databases/create", "key": identity,
                           "body": {"project": identity, "database": database, "role": "writer"}} for database in targets)
    return {"project": project, "databases": databases, "noisy_project": noisy_project,
            "noisy_database": noisy_database, "operations": operations}


def envelope(encoded, status, request_id):
    require(len(encoded) <= LIMIT, "response_too_large")
    value = decode(encoded)
    require(isinstance(value, dict) and set(value) == {"version", "request_id", "ok", "result", "error"}, "invalid_envelope")
    require(type(value["version"]) is int and value["version"] == 1
            and value["request_id"] == request_id and type(value["ok"]) is bool, "invalid_envelope_identity")
    require(status == 200 and value["ok"] is True and value["error"] is None
            and isinstance(value["result"], dict), "request_failed_outcome_unknown")
    return value["result"]


class API:
    def __init__(self, configuration):
        self.address = endpoint(configuration["connection"]["endpoint"])
        self.context = configuration["context"]

    def call(self, route, key, body=None):
        require(route in {"/v1/catalog", "/v1/projects/create", "/v1/keys/create", "/v1/databases/create"}, "unsupported_route")
        request_id = "" if body is None else body["request_id"]
        encoded = None if body is None else json.dumps(body, allow_nan=False).encode()
        require(encoded is None or len(encoded) <= LIMIT, "request_too_large")
        credential(key)
        connection = http.client.HTTPSConnection(self.address.hostname, self.address.port, context=self.context, timeout=30)
        deadline = time.monotonic() + 30
        try:
            connection.connect()
            require(time.monotonic() < deadline, "connection_deadline")
            transport = connection.sock
            connection.auto_open = 0
            transport.settimeout(max(0.001, deadline - time.monotonic()))
            connection.request("GET" if body is None else "POST", route, body=encoded,
                               headers={"Authorization": "Bearer " + key, "Content-Type": "application/json", "Connection": "close"})
            transport.settimeout(max(0.001, deadline - time.monotonic()))
            response = connection.getresponse()
            require(response.getheader("Content-Encoding", "identity") == "identity", "encoded_response_denied")
            length = response.getheader("Content-Length")
            require(length is None or length.isascii() and length.isdigit() and int(length) <= LIMIT, "response_too_large")
            chunks = bytearray()
            while len(chunks) <= LIMIT:
                remaining = deadline - time.monotonic()
                require(remaining > 0, "response_deadline")
                transport.settimeout(remaining)
                chunk = response.read1(min(65536, LIMIT + 1 - len(chunks)))
                if not chunk:
                    break
                chunks.extend(chunk)
                if response.isclosed():
                    break
            require(length is None or len(chunks) == int(length), "truncated_response")
            return envelope(bytes(chunks), response.status, request_id)
        except Exception:
            raise Failure("management_outcome_unknown_request_id=" + request_id) from None
        finally:
            connection.close()


def durable_json(descriptor, value):
    encoded = json.dumps(value, allow_nan=False).encode()
    require(len(encoded) <= 65536, "output_too_large")
    os.lseek(descriptor, 0, os.SEEK_SET)
    remaining = memoryview(encoded)
    while remaining:
        written = os.write(descriptor, remaining)
        require(written > 0, "output_write_failed")
        remaining = remaining[written:]
    os.ftruncate(descriptor, len(encoded))
    os.fsync(descriptor)


def reserve(path, value):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        durable_json(descriptor, value)
        parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def provision(configuration, plan, label, api=None):
    for process in configuration["processes"]:
        os.kill(process["pid"], 0)
    for operation in plan["operations"]:
        operation["body"]["request_id"] = secrets.token_hex(16)
    reservation = {"label": label, "output": str(configuration["output"]),
                   "requests": [{"route": operation["route"], "request_id": operation["body"]["request_id"]} for operation in plan["operations"]]}
    marker = configuration["directory"] / (".stress-provision-" + label + ".json")
    os.close(reserve(marker, reservation))
    descriptor = reserve(configuration["output"], {"incomplete": True, **reservation})
    try:
        api = API(configuration) if api is None else api
        keys = {"global": configuration["connection"]["global_api_key"]}
        catalog = api.call("/v1/catalog", keys["global"])
        require(isinstance(catalog.get("projects"), dict) and isinstance(catalog.get("databases"), dict), "invalid_catalog")
        require(plan["project"] not in catalog["projects"] and plan["noisy_project"] not in catalog["projects"], "label_already_used")
        primary_worker = None
        for operation in plan["operations"]:
            body = operation["body"]
            result = api.call(operation["route"], keys[operation["key"]], body)
            if operation["route"] == "/v1/projects/create":
                require(result == {"id": body["id"], "organization": "local", "tenant": "local_" + body["id"],
                                   "region": "local", "quotas": QUOTAS}, "project_confirmation_mismatch")
            elif operation["route"] == "/v1/keys/create":
                require(set(result) == {"key", "subject", "project", "role", "credential", "shown_once"}
                        and result.get("project") == body["project"] and result.get("role") == "admin"
                        and result.get("subject") == body["subject"] and result.get("shown_once") is True, "key_confirmation_mismatch")
                new_key = credential(result["credential"])
                require(isinstance(result["key"], str) and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", result["key"])
                        and new_key.startswith("plk1_" + result["key"] + "_") and new_key not in keys.values(), "key_identity_mismatch")
                keys[body["project"]] = new_key
            else:
                require(result.get("database") == body["database"] and result.get("phase") == "active"
                        and result.get("desired_state") == "active" and result.get("role") == "writer"
                        and result.get("desired_worker") in configuration["workers"], "database_confirmation_mismatch")
                if body["database"] == plan["databases"][0]:
                    primary_worker = result["desired_worker"]
        endpoint_value = configuration["connection"]["endpoint"]
        output = {"endpoint": endpoint_value, "project": plan["project"], "api_key": keys[plan["project"]],
                  "database": plan["databases"][0], "databases": plan["databases"], "fixture_only": True,
                  "ca_file": configuration["connection"]["ca_file"], "source_root": str(pathlib.Path(__file__).resolve().parents[3]),
                  "management_endpoint": endpoint_value, "management_key": keys["global"],
                  "processes": configuration["processes"], "worker_ids": configuration["workers"],
                  "migration_worker": next(worker for worker in configuration["workers"] if worker != primary_worker),
                  "noisy_tenant": {"endpoint": endpoint_value,
                  "project": plan["noisy_project"], "api_key": keys[plan["noisy_project"]], "database": plan["noisy_database"]}}
        metadata = os.fstat(descriptor)
        current = configuration["output"].lstat()
        require(metadata.st_ino == current.st_ino and metadata.st_dev == current.st_dev and current.st_nlink == 1
                and current.st_uid == os.geteuid() and stat.S_IMODE(current.st_mode) == 0o600, "output_identity_changed")
        durable_json(descriptor, output)
    finally:
        os.close(descriptor)


def self_test():
    from provision_tests import run
    return run()


def main(arguments=None):
    parser = argparse.ArgumentParser(description="Private S3 stress fixture provisioner; default plan never contacts services.")
    parser.add_argument("--run-dir"); parser.add_argument("--output"); parser.add_argument("--label")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--execute", action="store_true", help="main authorization required; one attempt per mutation")
    modes.add_argument("--plan", action="store_true")
    modes.add_argument("--self-test", action="store_true")
    supplied = sys.argv[1:] if arguments is None else arguments
    if not supplied:
        parser.print_help(); return 0
    options = parser.parse_args(supplied)
    if options.self_test:
        require(not any((options.run_dir, options.output, options.label)), "self_test_takes_no_fixture")
        return self_test()
    require(all((options.run_dir, options.output, options.label)), "run_dir_output_label_required")
    configuration = fixture(options.run_dir, options.output, options.label)
    plan = build_plan(options.label)
    if options.execute:
        provision(configuration, plan, options.label)
        print(json.dumps({"provisioned": True, "connection_file": options.output}))
    else:
        print(json.dumps({"plan_only": True, "network_calls": 0, "project": plan["project"], "databases": plan["databases"],
                          "noisy_project": plan["noisy_project"], "noisy_database": plan["noisy_database"],
                          "mutations": len(plan["operations"]), "default_quotas": QUOTAS, "output": options.output}))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (Exception, KeyboardInterrupt) as failure:
        print(str(failure) if isinstance(failure, Failure) else "provision_stopped_private_inputs_or_outcome_unknown", file=sys.stderr)
        sys.exit(1)
