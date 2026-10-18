import argparse
import base64
import datetime
import errno
import fcntl
import hashlib
import hmac
import json
import os
import pathlib
import queue
import re
import secrets
import shutil
import signal
import stat
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import ssl


SHUTDOWN_BUDGET_SECONDS = 40
SHUTDOWN_FINALIZE_SECONDS = 5
ORCHESTRATION_GRACE_SECONDS = SHUTDOWN_BUDGET_SECONDS + SHUTDOWN_FINALIZE_SECONDS
# Each stage cap has to stay strictly above the budget the component gives
# itself (10 s for the platform and for both Rust processes): a cap the process
# reaches first is a launcher SIGKILL mid-teardown, which loses the unclean exit
# that component would have reported itself. The caps sum to the launcher budget,
# and the launcher declares the platform's cap and this grace to the platform
# child, which refuses a cap that reaches the grace.
SHUTDOWN_STAGE_CAPS = {"platform": 15, "workers": 13, "authority": 12}
# Once /v1/reconcile starts, the platform may have changed controller placement or
# grants, authority leases/backend state, and its operations sidecar. None of those
# are covered by the canonical terminal hashes, so a failure from here on must not
# restore the old proof as if it still described the current run. This is a
# conservative behavior change: even an immediate 409 or a refused connection can
# hide a request that already mutated state before its reply was lost.
LOCAL_SEAL_KEY = "authority-control/single-authority-seal"
LOCAL_SEAL_FIELDS = {"version", "owner", "clean"}
LOCAL_ENVELOPE_LIMIT_BYTES = 1024 * 1024
LOCAL_CHECKSUM_PATTERN = re.compile(r"[0-9a-fA-F]{64}\Z")


def validate_local_seal_payload(payload):
    """Return the decoded seal record when it is reusable, otherwise None.

    The Rust authority denies unknown fields and requires version 1 with
    clean=true before it will take over a backend. A checksum-valid envelope
    around any other record must not be promoted to canonical clean evidence.
    """
    try:
        record = json.loads(payload, object_pairs_hook=unique_object)
    except (ValueError, TypeError, UnicodeError, RuntimeError):
        return None
    if not isinstance(record, dict) or set(record) != LOCAL_SEAL_FIELDS:
        return None
    if record.get("version") != 1 or type(record.get("version")) is not int or record.get("clean") is not True:
        return None
    if not isinstance(record.get("owner"), str) or not record["owner"]:
        return None
    return record


def decode_local_envelope(envelope, key):
    """Return the decoded payload when a local object envelope is well formed.

    This validates structure only. The checksum is also a 64-hex digest here;
    callers that can recompute it must still compare it with the payload.
    """
    if not isinstance(envelope, dict) or envelope.get("key") != key:
        return None
    encoded = envelope.get("bytes")
    etag = envelope.get("etag")
    checksum = envelope.get("checksum")
    if not isinstance(encoded, str) or not isinstance(etag, str) or not etag:
        return None
    if not isinstance(checksum, str) or LOCAL_CHECKSUM_PATTERN.fullmatch(checksum) is None:
        return None
    try:
        return base64.b64decode(encoded, validate=True)
    except (ValueError, TypeError):
        return None


def local_object_path(backend_directory, key):
    """Return (path, reliable) for a local object envelope.

    reliable is False when a candidate cannot be parsed, does not carry the
    expected key, or does not match the LocalObjectStore envelope schema, so the
    caller must not treat "not found" as "absent". Without blake3 the checksum
    cannot be recomputed: a structurally valid match is returned with
    reliable=False so staging still preserves it, but restore must not promote
    it to canonical evidence.
    """
    if not backend_directory.is_dir():
        return None, True
    try:
        import blake3
    except ImportError:
        blake3 = None
    if blake3 is not None:
        candidate = backend_directory / (blake3.blake3(key.encode()).hexdigest() + ".obj")
        if not candidate.is_file():
            return None, True
        try:
            if candidate.stat().st_size > LOCAL_ENVELOPE_LIMIT_BYTES:
                return None, False
            envelope = json.loads(candidate.read_bytes(), object_pairs_hook=unique_object)
        except (OSError, ValueError, TypeError, UnicodeError, RuntimeError):
            return None, False
        payload = decode_local_envelope(envelope, key)
        if payload is None or envelope["checksum"] != blake3.blake3(payload).hexdigest():
            return None, False
        if validate_local_seal_payload(payload) is None:
            return None, False
        return candidate, True
    reliable = True
    match = None
    for candidate in sorted(backend_directory.glob("*.obj")):
        try:
            if candidate.stat().st_size > LOCAL_ENVELOPE_LIMIT_BYTES:
                reliable = False
                continue
            envelope = json.loads(candidate.read_bytes(), object_pairs_hook=unique_object)
        except (OSError, ValueError, TypeError, UnicodeError, RuntimeError):
            reliable = False
            continue
        if not isinstance(envelope, dict):
            reliable = False
            continue
        if envelope.get("key") != key:
            continue
        payload = decode_local_envelope(envelope, key)
        if payload is None or validate_local_seal_payload(payload) is None:
            reliable = False
            continue
        if match is None:
            match = candidate
    if match is not None:
        return match, False
    return None, reliable


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, destination):
        return None


def storage_identity(backend):
    if backend != "s3":
        return {"backend": "local"}
    return {"backend": "s3", "endpoint": os.environ["S3_ENDPOINT"], "bucket": os.environ["S3_BUCKET"], "region": os.environ.get("S3_REGION", "us-east-1")}


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise RuntimeError("duplicate service reply field; outcome unknown")
        result[key] = value
    return result


def private_json(path, value):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as output:
        json.dump(value, output)
        output.flush()
        os.fsync(output.fileno())


def private_directory(path):
    path.mkdir(parents=True, mode=0o700, exist_ok=True)
    if path.is_symlink() or path.stat().st_mode & 0o077:
        raise RuntimeError("runtime directory must be private and not a symlink")


def atomic_private_json(destination, value):
    temporary = destination.with_name(destination.name + "." + secrets.token_hex(16))
    private_json(temporary, value)
    os.replace(temporary, destination)
    descriptor = os.open(destination.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def fsync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def private_copy(source, destination):
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with source.open("rb") as input_file, os.fdopen(descriptor, "wb") as output:
        shutil.copyfileobj(input_file, output)
        output.flush()
        os.fsync(output.fileno())


def signed_owned_identity(configuration, identity):
    payload = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    key = pathlib.Path(configuration["key_file"]).read_bytes()
    return {"payload": payload, "mac": hmac.new(key, payload.encode(), hashlib.sha256).hexdigest()}


def terminal_record(run_directory, configuration):
    return {"version": 1, "generation": configuration["generation"],
            "launch_sha256": hashlib.sha256((run_directory / "launch.json").read_bytes()).hexdigest(),
            "platform_sha256": hashlib.sha256((run_directory / "platform.json").read_bytes()).hexdigest(),
            "state_sha256": hashlib.sha256((run_directory / "platform/state.json").read_bytes()).hexdigest(),
            "state_floor_sha256": hashlib.sha256((run_directory / "platform/state.json.floor").read_bytes()).hexdigest(),
            "proof_sha256": {f"{worker}.{kind}.json": hashlib.sha256((run_directory / "owned-proof" / f"{worker}.{kind}.json").read_bytes()).hexdigest()
                             for worker in configuration["workers"] for kind in ("identity", "stop")}}


def verify_clean_terminal(run_directory, configuration):
    marker = run_directory / "launcher-terminal.json"
    if marker.is_symlink() or not marker.is_file() or marker.stat().st_mode & 0o077 or marker.stat().st_size > 4096:
        raise RuntimeError("clean launcher terminal proof missing or invalid")
    try:
        signed = json.loads(marker.read_bytes(), object_pairs_hook=unique_object)
        if set(signed) != {"payload", "mac"} or not isinstance(signed["payload"], str) or not isinstance(signed["mac"], str):
            raise ValueError("invalid terminal proof envelope")
        expected = terminal_record(run_directory, configuration)
        payload = json.loads(signed["payload"], object_pairs_hook=unique_object)
        if payload != expected or type(payload["version"]) is not int:
            raise ValueError("terminal proof does not match runtime")
        key = pathlib.Path(configuration["key_file"]).read_bytes()
        if not hmac.compare_digest(signed["mac"], hmac.new(key, signed["payload"].encode(), hashlib.sha256).hexdigest()):
            raise ValueError("terminal proof signature mismatch")
    except (OSError, ValueError, TypeError, UnicodeError, RuntimeError) as error:
        raise RuntimeError("clean launcher terminal proof invalid") from error
    authority = run_directory / "authority/authority.lock"
    if authority.is_symlink() or not authority.is_file() or authority.read_bytes() != b"clean\n":
        raise RuntimeError("authority clean startup marker missing")


class OperationViewNotMaterialized(RuntimeError):
    """The payload is not the final operation view a shutdown checkpoint wrote."""


def require_materialized_operation_view(state):
    """Mirror operationsCheckpointMaterialized: operation records commit to a sidecar that
    dev.py does not read, so a payload whose checkpoint trails its revision may hide
    unresolved records; only the shutdown checkpoint writes the field, and a version-0
    payload predates the sidecar and carries its whole operation view itself."""
    version = state.get("operation_storage_version", 0)
    if type(version) is not int or version not in (0, 1):
        raise OperationViewNotMaterialized("platform operation storage identity unrecognized; clean restart requires the reviewed format")
    if version == 0:
        return
    checkpoint = state.get("operations_checkpoint")
    revision = state.get("revision")
    if type(checkpoint) is not int or checkpoint <= 0 or type(revision) is not int or checkpoint != revision:
        raise OperationViewNotMaterialized("platform operation view was not materialized at shutdown")


def verify_no_unresolved_operations(run_directory, configuration):
    path = run_directory / "platform/state.json"
    if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077 or path.stat().st_size > 32 * 1024 * 1024:
        raise RuntimeError("private platform state unavailable for clean restart")
    try:
        envelope = json.loads(path.read_bytes(), object_pairs_hook=unique_object)
        state = envelope["payload"]
        operations = state.get("operations", {})
        if state.get("owned_run_generation") != configuration["generation"] or not isinstance(operations, dict):
            raise ValueError("platform run binding mismatch")
        require_materialized_operation_view(state)
        for record in operations.values():
            if not isinstance(record, dict) or record.get("state") != "terminal" or record.get("ambiguous", False) or record.get("retirement_pending", False) or record.get("local_resources_retired", False) or record.get("decision", ""):
                raise ValueError("unresolved operation")
            if record.get("cleanup_outcome") not in ("completed", "not_applicable"):
                raise ValueError("unresolved cleanup")
            if not (record.get("business_outcome") == "completed" and record.get("execution_proven") is True
                    or record.get("business_outcome") == "not_executed"
                    or record.get("source") == "gateway" and record.get("action") == "close"
                    and record.get("business_outcome") == "not_applicable" and record.get("cleanup_outcome") == "completed"):
                raise ValueError("unresolved business outcome")
    except OperationViewNotMaterialized:
        raise
    except (KeyError, ValueError, TypeError, UnicodeError, RuntimeError) as error:
        raise RuntimeError("unresolved platform operation prevents clean restart") from error


def verify_owned_stop_proofs(run_directory, configuration, workers):
    """Verify both worker incarnations were joined before a clean restart."""
    proof_directory = run_directory / "owned-proof"
    key_file = run_directory / "keys/owned-proof.key"
    if (configuration.get("directory") != str(proof_directory)
            or configuration.get("key_file") != str(key_file)
            or set(configuration.get("workers", {})) != set(workers)
            or not isinstance(configuration.get("generation"), str)):
        raise RuntimeError("owned stop proof configuration mismatch")
    if not proof_directory.is_dir():
        raise RuntimeError("owned stop proof directory missing")
    private_directory(proof_directory)
    if key_file.is_symlink() or not stat.S_ISREG(key_file.stat().st_mode) or key_file.stat().st_mode & 0o077:
        raise RuntimeError("owned stop proof key is not private")
    key = key_file.read_bytes()
    if len(key) != 32:
        raise RuntimeError("owned stop proof key invalid")
    expected = {f"{worker}.{kind}.json" for worker in workers for kind in ("identity", "stop")}
    if {entry.name for entry in proof_directory.iterdir()} != expected:
        raise RuntimeError("owned stop proof set incomplete or unexpected")
    for worker in workers:
        records = []
        for kind in ("identity", "stop"):
            path = proof_directory / f"{worker}.{kind}.json"
            if path.is_symlink() or not stat.S_ISREG(path.stat().st_mode) or path.stat().st_mode & 0o077 or path.stat().st_size > 4096:
                raise RuntimeError("owned stop proof file invalid")
            try:
                signed = json.loads(path.read_bytes(), object_pairs_hook=unique_object)
                if set(signed) != {"payload", "mac"} or not isinstance(signed["payload"], str) or not isinstance(signed["mac"], str):
                    raise ValueError("invalid envelope")
                payload = signed["payload"].encode()
                if not hmac.compare_digest(signed["mac"], hmac.new(key, payload, hashlib.sha256).hexdigest()):
                    raise ValueError("bad mac")
                record = json.loads(signed["payload"], object_pairs_hook=unique_object)
                if set(record) != {"version", "generation", "worker_id", "nonce", "pid", "started", "waited", "exit_code"}:
                    raise ValueError("invalid fields")
            except (ValueError, TypeError, UnicodeError, RuntimeError) as error:
                raise RuntimeError("owned stop proof validation failed") from error
            records.append(record)
        identity, stopped = records
        if (identity["version"] != 1 or isinstance(identity["version"], bool)
                or identity["generation"] != configuration["generation"]
                or identity["worker_id"] != worker or identity["nonce"] != configuration["workers"][worker]
                or type(identity["pid"]) is not int or identity["pid"] <= 0
                or not isinstance(identity["started"], str) or not identity["started"]
                or identity["waited"] is not False or type(identity["exit_code"]) is not int or identity["exit_code"] != 0
                or stopped != {**identity, "waited": True, "exit_code": 0}
                or stopped["waited"] is not True or type(stopped["exit_code"]) is not int):
            raise RuntimeError("owned stop proof incarnation or joined exit mismatch")


def stop_owned_worker(run_directory, worker):
    private_directory(run_directory)
    deadline = time.monotonic() + SHUTDOWN_STAGE_CAPS["workers"] + SHUTDOWN_FINALIZE_SECONDS
    with socket.socket(socket.AF_UNIX) as client:
        client.settimeout(max(0, deadline - time.monotonic()))
        client.connect(str(run_directory / "owned-stop.sock"))
        client.settimeout(max(0, deadline - time.monotonic()))
        client.sendall(json.dumps({"worker": worker}).encode() + b"\n")
        client.settimeout(max(0, deadline - time.monotonic()))
        reply = json.loads(client.makefile("rb").readline(1024))
    if reply != {"stopped": True}:
        raise RuntimeError("owned worker stop unavailable; retain isolation")


def free_address():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{listener.getsockname()[1]}"


def safe_environment(capacity_trace=False):
    environment = {name: os.environ[name] for name in ("PATH", "HOME", "TMPDIR", "LANG", "TZ") if name in os.environ}
    if capacity_trace:
        environment["PERI_CAPACITY_TRACE"] = "1" if os.environ.get("PERI_CAPACITY_TRACE") == "1" else "0"
        environment["PERI_NATIVE_GATE_TRACE"] = "1" if os.environ.get("PERI_NATIVE_GATE_TRACE") == "1" else "0"
    return environment


def experimental_environment(environment, maintenance=False):
    """Forward the calibration switches by name, never the rest of the caller's environment.

    The maintenance scheduler and the work-metrics snapshot are experiment inputs; they are
    off unless the operator sets them. Only these names cross into the child, so credentials
    and unrelated switches still require their own explicit wiring. The scheduler belongs to
    the data plane, so its switches are forwarded to workers only.
    """
    for name, value in os.environ.items():
        if name == "PERI_WORK_METRICS" or (maintenance and name.startswith("PERI_MAINTENANCE_")):
            environment[name] = value
    return environment


def bootstrap_credential():
    credential = os.environ.get("PERI_BOOTSTRAP_KEY")
    if credential is None:
        return secrets.token_hex(32)
    if not 32 <= len(credential) <= 256 or any(ord(character) < 33 or ord(character) > 126 for character in credential):
        raise RuntimeError("invalid injected bootstrap credential")
    return credential


def request(endpoint, credential, path, body, certificate=None, timeout=30):
    payload = dict(body)
    if "request_id" not in payload:
        payload["request_id"] = secrets.token_hex(16)
    headers = {"Authorization": "Bearer " + credential, "Content-Type": "application/json", "X-Request-ID": payload["request_id"]}
    outgoing = urllib.request.Request(endpoint + path, data=json.dumps(payload).encode(), headers=headers, method="POST")
    context = ssl.create_default_context(cafile=certificate) if endpoint.startswith("https:") else None
    handlers = [urllib.request.ProxyHandler({}), NoRedirect()]
    if context:
        handlers.append(urllib.request.HTTPSHandler(context=context))
    client = urllib.request.build_opener(*handlers)
    http_failure = False
    try:
        try:
            incoming = client.open(outgoing, timeout=timeout)
        except urllib.error.HTTPError as failure:
            if not 400 <= failure.code <= 599:
                raise
            incoming = failure
            http_failure = True
        with incoming:
            encoded = incoming.read(32 * 1024 * 1024 + 1)
            if len(encoded) > 32 * 1024 * 1024:
                raise RuntimeError("oversized service reply; outcome unknown")
            result = json.loads(encoded, object_pairs_hook=unique_object)
    except (urllib.error.URLError, OSError):
        raise RuntimeError("request failed; dispatched outcomes must not be blindly retried") from None
    if not isinstance(result, dict) or result.get("version") != 1 or result.get("request_id") != payload["request_id"]:
        raise RuntimeError("invalid service reply identity; outcome unknown")
    if result.get("ok") is not True or result.get("error") is not None:
        code = result.get("error", {})
        raise RuntimeError("request rejected: " + str(code.get("code", "incomplete") if isinstance(code, dict) else "incomplete"))
    if http_failure:
        raise RuntimeError("invalid HTTP failure success reply; outcome unknown")
    return result.get("result")


class Project:
    def __init__(self, arguments):
        self.arguments = arguments
        self.root = pathlib.Path(os.environ.get("PERI_PROJECT_ROOT") or pathlib.Path(__file__).resolve().parents[3])
        self.directory = pathlib.Path(arguments.run_dir).resolve()
        self.processes = {}
        self.logs = []
        self.stop = threading.Event()
        self.owned_identities = {}
        self.maintenance = None
        self.run_lock = None
        self.shutdown_budget_seconds = getattr(arguments, "shutdown_budget_seconds", SHUTDOWN_BUDGET_SECONDS)
        self.shutdown_deadline = None
        self.stop_record = None
        self.cleanup_complete = False
        self.shutdown_failure = None
        self.stop_record_lock = threading.Lock()
        # Startup failures never announce readiness, so no workload was admitted. The
        # previous clean proof has already been consumed by prepare() by then, and it
        # is the only evidence that the last shutdown was clean; keep it restorable
        # until readiness makes later writes possible.
        self.phase = "startup"
        self.restart_attempt = False
        # Set when prepare() refused because the existing failure record failed
        # the operator gate. The record is authoritative even when it is legacy,
        # unsigned or asks for reuse: it must not be replaced by this attempt.
        self.restart_failure_gate_rejected = False
        self.ready_services = set()
        self.clean_evidence = None
        self.restart_failure = None
        self.restart_failure_error = None
        self.clean_evidence_quarantine = None
        self.last_clean_evidence_outcome = None
        # Set before teardown of a failed restart so diagnostics never overwrite the
        # previous clean launcher-stop.json, even when the failure record is lost.
        self.preserve_clean_evidence = False

    def begin_shutdown(self):
        if self.shutdown_deadline is None:
            self.shutdown_deadline = time.monotonic() + self.shutdown_budget_seconds
        self.stop.set()

    def remaining(self, deadline=None):
        end = self.shutdown_deadline
        if deadline is not None:
            end = deadline if end is None else min(end, deadline)
        return max(0, end - time.monotonic())

    def write_stop_record(self, outcome=None, reason=None):
        with self.stop_record_lock:
            configuration = getattr(self, "config", {}).get("owned_stop_proof")
            if not configuration:
                if self.processes:
                    raise RuntimeError("stop evidence signing configuration unavailable; retain isolation")
                return
            if self.stop_record is None:
                self.stop_record = {"version": 1, "generation": configuration["generation"],
                                    "outcome": "in_progress", "reason": "shutdown_started",
                                    "deadline_ms": int(self.shutdown_deadline * 1000),
                                    "stages": [], "unresolved_operations": "not_verified"}
            if outcome is not None:
                self.stop_record["outcome"] = outcome
            if reason is not None:
                self.stop_record["reason"] = reason
            if self.preserve_clean_evidence:
                # A failed restart's teardown belongs to its own record. Never write
                # launcher-stop.json here: it is the previous clean proof until the
                # failed attempt can prove its own terminal (or an operator decides).
                if self.restart_failure is not None:
                    self.restart_failure["teardown"] = self.stop_record
                    try:
                        self.persist_restart_failure()
                    except Exception as error:
                        # Keep the in-memory record: recovery still has a usable
                        # clean-evidence snapshot, and the final error plus a
                        # separate diagnostic must not silently lose it.
                        self.restart_failure_error = error
                return
            atomic_private_json(self.directory / "launcher-stop.json", signed_owned_identity(configuration, self.stop_record))

    def persist_restart_failure(self):
        configuration = getattr(self, "config", {}).get("owned_stop_proof")
        value = self.restart_failure
        if configuration:
            value = signed_owned_identity(configuration, value)
        atomic_private_json(self.directory / "launcher-restart-failure.json", value)
        # A later successful persist supersedes a transient earlier failure.
        self.restart_failure_error = None

    def read_signed_private(self, path, configuration, label):
        if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077 or path.stat().st_size > 64 * 1024:
            raise RuntimeError(label + " missing or invalid")
        try:
            signed = json.loads(path.read_bytes(), object_pairs_hook=unique_object)
            if set(signed) != {"payload", "mac"} or not isinstance(signed["payload"], str) or not isinstance(signed["mac"], str):
                raise ValueError("invalid envelope")
            key = pathlib.Path(configuration["key_file"]).read_bytes()
            if not hmac.compare_digest(signed["mac"], hmac.new(key, signed["payload"].encode(), hashlib.sha256).hexdigest()):
                raise ValueError("signature mismatch")
            record = json.loads(signed["payload"], object_pairs_hook=unique_object)
            if not isinstance(record, dict):
                raise ValueError("invalid record")
        except (OSError, ValueError, TypeError, UnicodeError, RuntimeError) as error:
            raise RuntimeError(label + " missing or invalid") from error
        return record

    def verify_restart_failure_gate(self, configuration):
        unavailable = self.directory / "launcher-restart-failure-unavailable.json"
        if unavailable.exists():
            raise RuntimeError("launcher restart requires operator recovery: restart failure record unavailable; inspect launcher-restart-failure-unavailable.json")
        path = self.directory / "launcher-restart-failure.json"
        if not path.exists():
            return
        try:
            record = self.read_signed_private(path, configuration, "launcher restart failure record")
        except RuntimeError as error:
            raise RuntimeError("launcher restart requires operator recovery: restart failure record is not verifiable") from error
        if record.get("kind") != "launcher_restart_failure":
            raise RuntimeError("launcher restart requires operator recovery: unrecognized restart failure record")
        if "requires_operator" not in record:
            raise RuntimeError("launcher restart requires operator recovery: legacy restart failure record lacks requires_operator; inspect launcher-restart-failure.json")
        if "teardown_completed" not in record:
            raise RuntimeError("launcher restart requires operator recovery: legacy restart failure record lacks teardown_completed")
        if record.get("requires_operator") is not False:
            raise RuntimeError("launcher restart requires operator recovery: " + str(record.get("recovery") or record.get("phase") or "unspecified"))
        if record.get("teardown_completed") is not True:
            raise RuntimeError("launcher restart requires operator recovery: launcher teardown not confirmed complete")
        if "all_processes_stopped" in record and record.get("all_processes_stopped") is not True:
            raise RuntimeError("launcher restart requires operator recovery: launcher teardown did not confirm all processes stopped")

    def verify_clean_stop_record(self, configuration):
        path = self.directory / "launcher-stop.json"
        if not path.exists():
            raise RuntimeError("legacy clean proof without launcher-stop.json; launcher stop outcome is not clean; operator recovery required")
        try:
            record = self.read_signed_private(path, configuration, "clean launcher stop record")
        except RuntimeError as error:
            raise RuntimeError("launcher stop outcome is not clean; operator recovery required") from error
        if (type(record.get("version")) is not int or record.get("version") != 1
                or record.get("generation") != configuration["generation"]
                or record.get("outcome") != "clean"):
            raise RuntimeError("launcher stop outcome is not clean; operator recovery required")

    def begin_restart_failure(self, error):
        existing = self.directory / "launcher-restart-failure.json"
        if not self.processes and self.clean_evidence is None and existing.exists():
            # prepare() refused before staging anything. Keep an existing
            # operator-required record: it is the only account of the original
            # failure, and a repeated refusal must not overwrite it. The gate
            # rejection flag covers legacy/unsigned records and violations that
            # are not expressed by requires_operator alone. A record that still
            # passes the gate does not describe this refusal, so the new failure
            # is written below.
            if self.restart_failure_gate_rejected:
                return
            configuration = getattr(self, "config", {}).get("owned_stop_proof")
            if configuration:
                try:
                    previous = self.read_signed_private(existing, configuration,
                                                        "launcher restart failure record")
                except RuntimeError:
                    previous = None
                if isinstance(previous, dict) and previous.get("requires_operator") is True:
                    return
        configuration = getattr(self, "config", {}).get("owned_stop_proof")
        reconciling = self.phase != "startup"
        self.restart_failure = {
            "version": 1,
            "kind": "launcher_restart_failure",
            "run_dir": str(self.directory),
            "generation": configuration["generation"] if configuration else None,
            "phase": self.phase,
            "readiness": "announced" if self.phase == "running" else "not_announced",
            "requires_operator": reconciling,
            "not_rolled_back": {},
            "error_type": type(error).__name__,
            "error": str(error)[:4096],
            "failed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "services_started": sorted(self.processes),
            "ready_services": sorted(self.ready_services),
            "clean_evidence": self.clean_evidence_summary(),
            "teardown": None,
            "teardown_completed": False,
        }
        try:
            self.persist_restart_failure()
        except Exception as persist_error:
            # Cleanup must still run: orphan processes are worse than a missing
            # diagnostic, and the run dir stays fail-closed without a terminal.
            self.restart_failure_error = persist_error

    def clean_evidence_summary(self):
        evidence = self.clean_evidence
        if evidence is None:
            return None
        return {name: evidence[name] for name in
                ("generation", "staging", "archive", "stop_sha256", "terminal_sha256",
                 "state_sha256", "state_floor_sha256", "authority_lock_sha256",
                 "seal_name", "seal_sha256", "seal_lookup_reliable")}

    def stage_clean_evidence(self, configuration):
        """Keep the verified clean chain restorable until readiness is announced.

        This is a same-process recovery boundary only. A hard kill or crash never
        restores it automatically: the canonical terminal stays absent and the next
        prepare refuses reuse. Recovery additionally requires every started process
        to be confirmed exited; an unknown state preserves the evidence for an
        operator instead.
        """
        terminal = self.directory / "launcher-terminal.json"
        stop = self.directory / "launcher-stop.json"
        state = self.directory / "platform/state.json"
        floor = self.directory / "platform/state.json.floor"
        authority_lock = self.directory / "authority/authority.lock"
        staging_root = self.directory / "launcher-restart-staging"
        private_directory(staging_root)
        staging = staging_root / (configuration["generation"] + "-" + secrets.token_hex(8))
        private_directory(staging)
        private_directory(staging / "platform")
        private_directory(staging / "authority")
        for source, name in ((state, "platform/state.json"), (floor, "platform/state.json.floor")):
            try:
                # The platform commits by rename, so a hard link is a zero-copy
                # snapshot. Filesystems without links still get an exact private copy.
                os.link(source, staging / name)
            except OSError:
                private_copy(source, staging / name)
        for source, name in ((stop, "launcher-stop.json"), (authority_lock, "authority/authority.lock")):
            if source.is_file():
                private_copy(source, staging / name)
        seal_path, seal_reliable = None, True
        if self.launch["backend"] == "local":
            seal_path, seal_reliable = local_object_path(self.directory / "backend", LOCAL_SEAL_KEY)
            if seal_path is not None:
                private_directory(staging / "backend")
                private_copy(seal_path, staging / "backend" / seal_path.name)
        evidence = {
            "generation": configuration["generation"],
            "staging": str(staging),
            "archive": None,
            "stop_sha256": file_sha256(stop) if stop.is_file() else None,
            "terminal_sha256": file_sha256(terminal),
            "state_sha256": file_sha256(state),
            "state_floor_sha256": file_sha256(floor),
            "authority_lock_sha256": file_sha256(authority_lock) if authority_lock.is_file() else None,
            "seal_name": seal_path.name if seal_path is not None else None,
            "seal_sha256": file_sha256(seal_path) if seal_path is not None else None,
            "seal_lookup_reliable": seal_reliable,
        }
        self.clean_evidence = evidence
        os.replace(terminal, staging / "launcher-terminal.json")
        fsync_directory(staging / "platform")
        fsync_directory(staging / "authority")
        if seal_path is not None:
            fsync_directory(staging / "backend")
        fsync_directory(staging)
        fsync_directory(staging_root)
        fsync_directory(self.directory)
        history = self.directory / "owned-proof-history"
        private_directory(history)
        archive = history / (configuration["generation"] + "-" + secrets.token_hex(8))
        if archive.exists():
            raise RuntimeError("owned stop proof generation already archived")
        (self.directory / "owned-proof").rename(archive)
        private_directory(self.directory / "owned-proof")
        evidence["archive"] = str(archive)
        fsync_directory(history)
        fsync_directory(self.directory)

    def discard_clean_evidence(self):
        evidence = self.clean_evidence
        if evidence is None:
            return
        # Readiness makes the snapshot unusable: later writes are allowed, so a
        # leftover directory is harmless and must not fail a running launcher.
        try:
            shutil.rmtree(evidence["staging"])
            pathlib.Path(evidence["staging"]).parent.rmdir()
        except OSError:
            pass
        self.clean_evidence = None

    def verify_clean_evidence(self):
        configuration = self.config["owned_stop_proof"]
        verify_owned_stop_proofs(self.directory, configuration, self.launch["workers"])
        verify_clean_terminal(self.directory, configuration)
        verify_no_unresolved_operations(self.directory, configuration)

    def verify_clean_reuse_gate(self):
        """Reuse also requires the stop record and the failure-record gate.

        verify_clean_evidence covers terminal/owned proofs/operations only. A
        clean terminal whose signer still says the stop was unclean, or a run
        with an unavailable failure record, must not be reported as reusable
        without an operator.
        """
        configuration = self.config["owned_stop_proof"]
        if self.restart_failure is None:
            self.verify_restart_failure_gate(configuration)
        elif (self.directory / "launcher-restart-failure-unavailable.json").exists():
            raise RuntimeError("restart failure record unavailable; operator recovery required")
        self.verify_clean_stop_record(configuration)
        self.verify_clean_evidence()

    def confirm_processes_stopped(self, timeout=None):
        if timeout is None:
            timeout = SHUTDOWN_FINALIZE_SECONDS
        deadline = time.monotonic() + timeout
        while True:
            if all(process.poll() is not None for process in self.processes.values()):
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.05)

    def evidence_file_status(self, relative, digest):
        if digest is None:
            return "absent_at_snapshot"
        path = self.directory / relative
        if not path.is_file():
            return "missing"
        return "matches_snapshot" if file_sha256(path) == digest else "changed"

    def recovery_status(self, evidence, canonical_status):
        status = {
            "controller_placement_grants": "not_rolled_back",
            "authority_backend_leases": "not_rolled_back",
            "platform_operations_sidecar": "not_rolled_back",
            "platform_state_floor_writes": "not_rolled_back",
            "canonical_clean_evidence": canonical_status,
        }
        if evidence is None:
            for name in ("authority_lock", "authority_seal", "platform_state", "platform_state_floor"):
                status[name] = "no_clean_evidence"
            return status
        status["authority_lock"] = self.evidence_file_status("authority/authority.lock", evidence.get("authority_lock_sha256"))
        seal_name = evidence.get("seal_name")
        if not evidence.get("seal_lookup_reliable", True):
            status["authority_seal"] = "unconfirmed"
        elif seal_name is None:
            status["authority_seal"] = "absent_at_snapshot"
        else:
            status["authority_seal"] = self.evidence_file_status("backend/" + seal_name, evidence.get("seal_sha256"))
        status["platform_state"] = self.evidence_file_status("platform/state.json", evidence.get("state_sha256"))
        status["platform_state_floor"] = self.evidence_file_status("platform/state.json.floor", evidence.get("state_floor_sha256"))
        return status

    def write_failure_record_unavailable(self, original_failure):
        if self.restart_failure_error is None:
            return
        value = {
            "version": 1, "kind": "launcher_restart_failure_unavailable",
            "run_dir": str(self.directory),
            "error": str(original_failure)[:4096],
            "record_error": str(self.restart_failure_error)[:2048],
            "recovery": self.last_clean_evidence_outcome,
            "written_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        try:
            atomic_private_json(self.directory / "launcher-restart-failure-unavailable.json", value)
        except Exception as error:
            self.restart_failure_error = RuntimeError(
                str(self.restart_failure_error) + "; unavailable-record diagnostic write failed: " + str(error))

    def remove_staging(self, staging):
        try:
            shutil.rmtree(staging)
            staging.parent.rmdir()
            return True
        except OSError:
            # A leftover snapshot is not authoritative; verification already passed.
            return False

    def restore_clean_files(self, evidence):
        staging = pathlib.Path(evidence["staging"])
        stop = self.directory / "launcher-stop.json"
        if evidence["stop_sha256"] is not None and (not stop.is_file() or file_sha256(stop) != evidence["stop_sha256"]):
            staged_stop = staging / "launcher-stop.json"
            if not staged_stop.is_file():
                raise RuntimeError("clean evidence snapshot incomplete")
            os.replace(staged_stop, stop)
        attempt_archive = None
        if evidence["archive"] is not None:
            current = self.directory / "owned-proof"
            history = self.directory / "owned-proof-history"
            private_directory(history)
            attempt_archive = history / (evidence["generation"] + "-failed-" + secrets.token_hex(8))
            if current.exists():
                current.rename(attempt_archive)
            pathlib.Path(evidence["archive"]).rename(current)
            fsync_directory(history)
        staged_terminal = staging / "launcher-terminal.json"
        if staged_terminal.exists():
            os.replace(staged_terminal, self.directory / "launcher-terminal.json")
        for name, digest in (("platform/state.json", evidence["state_sha256"]),
                             ("platform/state.json.floor", evidence["state_floor_sha256"]),
                             ("authority/authority.lock", evidence["authority_lock_sha256"])):
            if digest is None:
                continue
            current = self.directory / name
            if current.is_file() and file_sha256(current) == digest:
                continue
            staged = staging / name
            if not staged.is_file():
                raise RuntimeError("clean evidence snapshot incomplete")
            os.replace(staged, current)
        seal_name = evidence.get("seal_name")
        if seal_name is not None:
            current = self.directory / "backend" / seal_name
            staged = staging / "backend" / seal_name
            if current.is_file() and file_sha256(current) != evidence["seal_sha256"]:
                # The failed attempt rewrote the backend seal. Installing the old
                # one would hide its dirty marker and bypass the Rust quiescence
                # check, so leave the backend untouched and ask for an operator.
                raise RuntimeError("authority backend seal changed during failed restart")
            if not current.is_file():
                if not staged.is_file():
                    raise RuntimeError("clean evidence snapshot incomplete")
                if file_sha256(staged) != evidence["seal_sha256"]:
                    raise RuntimeError("clean evidence snapshot seal changed")
                os.replace(staged, current)
            fsync_directory(self.directory / "backend")
        else:
            current, reliable = local_object_path(self.directory / "backend", LOCAL_SEAL_KEY)
            if not reliable:
                raise RuntimeError("authority backend seal could not be confirmed")
            if current is not None:
                # The failed attempt created a seal the previous clean run did not
                # have; we cannot prove the old authority terminal any more.
                raise RuntimeError("authority backend seal appeared during failed restart")
        fsync_directory(self.directory / "platform")
        fsync_directory(self.directory / "authority")
        fsync_directory(self.directory)
        if file_sha256(self.directory / "launcher-terminal.json") != evidence["terminal_sha256"]:
            raise RuntimeError("clean terminal proof changed during restore")
        if evidence["stop_sha256"] is not None and file_sha256(stop) != evidence["stop_sha256"]:
            raise RuntimeError("previous clean stop record changed during restore")
        self.verify_clean_evidence()
        return attempt_archive

    def preserve_clean_evidence_for_operator(self):
        evidence = self.clean_evidence
        if evidence is None:
            return {"consumed": False, "restored": False, "preserved": True,
                    "requires_operator": True, "preserved_at": None}
        staging = pathlib.Path(evidence["staging"])
        created = datetime.datetime.now(datetime.timezone.utc)
        root = self.directory / "launcher-clean-evidence-preserved"
        private_directory(root)
        destination = root / (created.strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(8))
        private_directory(destination)
        private_directory(destination / "platform")
        private_directory(destination / "authority")
        for name in ("launcher-terminal.json", "launcher-stop.json",
                     "platform/state.json", "platform/state.json.floor",
                     "authority/authority.lock"):
            staged = staging / name
            if staged.exists():
                os.replace(staged, destination / name)
        seal_name = evidence.get("seal_name")
        if seal_name is not None:
            private_directory(destination / "backend")
            staged = staging / "backend" / seal_name
            if staged.exists():
                os.replace(staged, destination / "backend" / seal_name)
        owned_proof_preserved = evidence["archive"] is not None and pathlib.Path(evidence["archive"]).exists()
        if owned_proof_preserved:
            pathlib.Path(evidence["archive"]).rename(destination / "owned-proof")
        not_rolled_back = self.recovery_status(evidence, "preserved")
        receipt = {
            "version": 1, "kind": "launcher_clean_evidence_preserved",
            "run_dir": str(self.directory), "generation": evidence["generation"],
            "phase": self.phase, "created_at": created.isoformat(),
            "services_started": sorted(self.processes),
            "ready_services": sorted(self.ready_services),
            "not_rolled_back": not_rolled_back,
            "terminal_sha256": evidence["terminal_sha256"], "stop_sha256": evidence["stop_sha256"],
            "state_sha256": evidence["state_sha256"], "state_floor_sha256": evidence["state_floor_sha256"],
            "authority_lock_sha256": evidence["authority_lock_sha256"],
            "seal_name": evidence.get("seal_name"), "seal_sha256": evidence.get("seal_sha256"),
            "owned_proof_preserved": owned_proof_preserved,
        }
        private_json(destination / "preservation.json", receipt)
        fsync_directory(destination / "platform")
        fsync_directory(destination / "authority")
        if seal_name is not None:
            fsync_directory(destination / "backend")
        fsync_directory(destination)
        fsync_directory(root)
        fsync_directory(self.directory)
        staging_removed = self.remove_staging(staging)
        self.clean_evidence = None
        outcome = {"consumed": True, "restored": False, "preserved": True,
                   "requires_operator": True, "preserved_at": str(destination),
                   "terminal_sha256": evidence["terminal_sha256"], "stop_sha256": evidence["stop_sha256"],
                   "state_sha256": evidence["state_sha256"], "state_floor_sha256": evidence["state_floor_sha256"],
                   "authority_lock_sha256": evidence["authority_lock_sha256"],
                   "seal_name": evidence.get("seal_name"), "seal_sha256": evidence.get("seal_sha256"),
                   "owned_proof_preserved": owned_proof_preserved,
                   "services_started": receipt["services_started"],
                   "not_rolled_back": not_rolled_back,
                   "staging_removed": staging_removed}
        self.last_clean_evidence_outcome = outcome
        return outcome

    def quarantine_clean_terminal(self, error):
        terminal = self.directory / "launcher-terminal.json"
        if not terminal.exists():
            return
        created = datetime.datetime.now(datetime.timezone.utc)
        root = self.directory / "launcher-clean-evidence-quarantine"
        private_directory(root)
        destination = root / (created.strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(8))
        private_directory(destination)
        terminal_sha256 = file_sha256(terminal)
        removed = False
        try:
            os.replace(terminal, destination / "launcher-terminal.json")
        except OSError as move_error:
            try:
                terminal.unlink()
            except OSError as remove_error:
                raise RuntimeError("restored clean terminal could not be quarantined or removed: "
                                   + str(move_error) + "; " + str(remove_error)) from remove_error
            removed = True
        private_json(destination / "quarantine.json", {
            "version": 1, "run_dir": str(self.directory), "reason": str(error)[:2048],
            "quarantined_at": created.isoformat(), "terminal_sha256": terminal_sha256,
            "removed": removed,
        })
        fsync_directory(destination)
        fsync_directory(root)
        fsync_directory(self.directory)
        self.clean_evidence_quarantine = {
            "directory": str(destination), "terminal_sha256": terminal_sha256,
            "removed": removed, "reason": str(error)[:2048],
        }

    def restore_clean_evidence(self, processes_stopped):
        evidence = self.clean_evidence
        if evidence is None:
            # prepare() refused before it staged anything, so no service ran. The
            # previous stop/terminal are untouched; verify them for the record but
            # never rewrite that history with a teardown that never happened.
            # Reuse needs every canonical precondition, not just the terminal:
            # a clean terminal next to an unclean/missing stop record or an
            # unavailable failure record still requires an operator.
            verified, verification_error = False, None
            configuration = getattr(self, "config", {}).get("owned_stop_proof")
            if configuration:
                try:
                    self.verify_clean_reuse_gate()
                    verified = True
                except Exception as error:
                    verification_error = str(error)[:2048]
            else:
                verification_error = "owned stop proof configuration unavailable"
            return {"consumed": False, "preserved": True, "restored": False,
                    "requires_operator": not verified,
                    "existing_clean_evidence_verified": verified,
                    "verification_error": verification_error}
        if not processes_stopped:
            summary = self.preserve_clean_evidence_for_operator()
            summary["preservation_reason"] = "processes not confirmed stopped; authority marker/seal and canonical evidence are not restored"
            return summary
        if self.processes:
            # A started process may have written the authority dirty marker, its
            # backend seal, controller placement/grants or the operations sidecar
            # before readiness. Rolling the old clean evidence back would bypass
            # the authority's quiescence check, so preserve it for an operator
            # instead. Popen failures that never started a process still recover.
            summary = self.preserve_clean_evidence_for_operator()
            summary["preservation_reason"] = "one or more services were started; canonical evidence is not restored"
            return summary
        if self.phase != "startup":
            # Conservative change: any failure from phase "reconciling" onward,
            # including an immediate 409 or a refused connection, may already have
            # mutated controller placement/grants or the operations sidecar, so the
            # old clean terminal is preserved for an operator instead of restored.
            summary = self.preserve_clean_evidence_for_operator()
            summary["preservation_reason"] = "failure after reconciling started; canonical evidence is not restored"
            return summary
        if self.launch.get("backend") != "local":
            summary = self.preserve_clean_evidence_for_operator()
            summary["preservation_reason"] = "authority backend seal cannot be restored for this backend"
            return summary
        if not evidence.get("seal_lookup_reliable", True):
            summary = self.preserve_clean_evidence_for_operator()
            summary["preservation_reason"] = "authority backend seal could not be confirmed during snapshot"
            return summary
        if evidence.get("stop_sha256") is None:
            summary = self.preserve_clean_evidence_for_operator()
            summary["preservation_reason"] = "clean evidence snapshot has no stop record"
            return summary
        staging = pathlib.Path(evidence["staging"])
        try:
            attempt_archive = self.restore_clean_files(evidence)
        except Exception as error:
            try:
                self.quarantine_clean_terminal(error)
            except Exception as quarantine_error:
                raise RuntimeError(
                    "clean evidence restore failed: " + str(error)
                    + "; quarantine failed: " + str(quarantine_error)) from error
            raise
        summary = self.clean_evidence_summary()
        summary.update({"consumed": True, "restored": True, "requires_operator": False,
                        "all_processes_stopped": True,
                        "not_rolled_back": self.recovery_status(evidence, "restored"),
                        "failed_attempt_proof_archive": str(attempt_archive) if attempt_archive else None})
        summary["staging_removed"] = self.remove_staging(staging)
        self.clean_evidence = None
        self.last_clean_evidence_outcome = summary
        return summary

    def complete_restart_failure(self, evidence, teardown_error=None, processes_stopped=True):
        self.restart_failure["clean_evidence"] = evidence
        self.restart_failure["teardown"] = self.stop_record
        # "completed" means every process this launcher started is confirmed gone,
        # not that the stop proof was clean. A platform that never became ready can
        # leave an unclean teardown while the previous clean evidence is still valid.
        self.restart_failure["teardown_completed"] = processes_stopped
        self.restart_failure["all_processes_stopped"] = processes_stopped
        self.restart_failure["teardown_clean"] = teardown_error is None and self.cleanup_complete
        if teardown_error is not None:
            self.restart_failure["teardown_error"] = str(teardown_error)[:2048]
        if evidence.get("requires_operator") is not None:
            self.restart_failure["requires_operator"] = evidence["requires_operator"]
        if evidence.get("not_rolled_back") is not None:
            self.restart_failure["not_rolled_back"] = evidence["not_rolled_back"]
        self.last_clean_evidence_outcome = evidence
        try:
            self.persist_restart_failure()
        except Exception as error:
            self.restart_failure_error = error
        self.restart_failure = None

    def abandon_restart_recovery(self, error, teardown_error=None, processes_stopped=False):
        evidence = self.clean_evidence or (self.restart_failure or {}).get("clean_evidence")
        if self.stop_record is not None:
            self.stop_record["outcome"] = "unclean"
            self.stop_record["reason"] = ("clean evidence not restorable: " + str(error))[:512]
        canonical_status = "quarantined" if self.clean_evidence_quarantine is not None else "not_restored"
        status = self.recovery_status(evidence, canonical_status)
        self.last_clean_evidence_outcome = {
            "restored": False, "requires_operator": True, "not_rolled_back": status,
            "clean_evidence_quarantine": self.clean_evidence_quarantine,
        }
        if self.restart_failure is not None:
            self.restart_failure["requires_operator"] = True
            self.restart_failure["recovery"] = {"restored": False, "error": str(error)[:2048]}
            self.restart_failure["teardown"] = self.stop_record
            self.restart_failure["teardown_completed"] = processes_stopped
            self.restart_failure["all_processes_stopped"] = processes_stopped
            self.restart_failure["teardown_clean"] = False
            self.restart_failure["not_rolled_back"] = status
            if teardown_error is not None:
                self.restart_failure["teardown_error"] = str(teardown_error)[:2048]
            if self.clean_evidence_quarantine is not None:
                self.restart_failure["clean_evidence_quarantine"] = self.clean_evidence_quarantine
            try:
                self.persist_restart_failure()
            except Exception as persist_error:
                self.restart_failure_error = persist_error
            self.restart_failure = None
        # Never write this attempt's teardown over the previous clean stop record.

    def authority_lock_status(self):
        path = self.directory / "authority/authority.lock"
        if path.is_symlink() or not path.is_file():
            return "unavailable"
        with path.open("rb") as source:
            value = source.read(7)
        return "clean" if value == b"clean\n" else "dirty" if value == b"dirty\n" else "invalid"

    def acquire_run_lock(self):
        if self.run_lock is not None:
            raise RuntimeError("launcher run lock already held")
        if pathlib.Path(self.arguments.run_dir).is_symlink():
            raise RuntimeError("runtime directory must not be a symlink")
        private_directory(self.directory)
        path = self.directory / "launcher.lock"
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_nlink != 1:
                raise RuntimeError("launcher run lock must be a private regular file")
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as error:
                if error.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                    raise RuntimeError("runtime directory already has an active launcher") from error
                raise
            self.run_lock = descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def release_run_lock(self):
        if self.run_lock is not None:
            os.close(self.run_lock)
            self.run_lock = None

    def prepare(self):
        if not sum(SHUTDOWN_STAGE_CAPS.values()) <= self.shutdown_budget_seconds <= ORCHESTRATION_GRACE_SECONDS - SHUTDOWN_FINALIZE_SECONDS:
            raise RuntimeError("shutdown budget must cover stage caps and leave five seconds within orchestration grace; use --shutdown-budget-seconds 40 with the reviewed 45-second deployment profile")
        observer = getattr(self.arguments, "sql_open_observer", False)
        worker_request_bytes = getattr(self.arguments, "worker_max_request_bytes", 512 * 1024)
        if not 1 <= worker_request_bytes <= 32 * 1024 * 1024:
            raise RuntimeError("worker request limit must be between 1 and 33554432 bytes")
        request_timeout_seconds = getattr(self.arguments, "dev_sql_timeout_seconds", 30)
        if not 30 <= request_timeout_seconds <= 300 or request_timeout_seconds != 30 and self.arguments.tls:
            raise RuntimeError("extended local SQL timeout requires loopback without TLS")
        sql_duration_ms = getattr(self.arguments, "sql_max_duration_ms", 5000)
        if not 1 <= sql_duration_ms <= (request_timeout_seconds - 10) * 1000:
            raise RuntimeError("SQL duration limit must leave ten seconds for gateway response")
        wal_replay_bytes = getattr(self.arguments, "wal_replay_bytes", 0)
        if not 0 <= wal_replay_bytes <= 1 << 34:
            raise RuntimeError("WAL replay budget must be a positive byte count, or 0 for the worker default")
        if observer and (pathlib.Path(self.arguments.run_dir).is_symlink() or self.directory.is_symlink() or self.directory.exists() and (not self.directory.is_dir() or any(entry.name != "launcher.lock" for entry in self.directory.iterdir()))):
            raise RuntimeError("SQL open observer requires a fresh empty runtime")
        private_directory(self.directory)
        for name in ("logs", "keys", "keys/kms", "authority", "controller", "platform", "cache-a", "cache-b"):
            private_directory(self.directory / name)
        self.rust = pathlib.Path(os.environ.get("PERI_RUST_BINARY", self.root / "prototypes/v1/target/debug/peri-loom-v1"))
        self.go = pathlib.Path(os.environ.get("PERI_PLATFORM_BINARY", self.directory / "peri-platform"))
        if not self.arguments.no_build:
            frontend = self.root / "prototypes/v1/platform/frontend"
            if not (frontend / "node_modules").is_dir():
                subprocess.run(["npm", "ci"], cwd=frontend, check=True, env=safe_environment())
            subprocess.run(["npm", "run", "build"], cwd=frontend, check=True, env=safe_environment())
            subprocess.run(["cargo", "build", "--locked", "--manifest-path", str(self.root / "prototypes/v1/Cargo.toml")], check=True, env=safe_environment())
            subprocess.run(["go", "build", "-o", str(self.go), "./cmd/peri-platform"], cwd=self.root / "prototypes/v1/platform", check=True, env=safe_environment())
        launch_path = self.directory / "launch.json"
        if launch_path.exists():
            self.restart_attempt = True
            self.launch = json.loads(launch_path.read_text())
            if self.launch.get("sql_open_observer", False) != observer:
                raise RuntimeError("SQL open observer profile cannot be silently rebound")
            if self.launch["backend"] != self.arguments.backend or self.launch["listen"] != self.arguments.listen or self.launch["tls"] != self.arguments.tls:
                raise RuntimeError("existing runtime identity/configuration cannot be silently rebound")
            if self.launch.get("writer_admission", False) != self.arguments.writer_admission:
                raise RuntimeError("existing writer admission profile cannot be silently rebound")
            if self.launch.get("worker_max_request_bytes", 512 * 1024) != worker_request_bytes:
                raise RuntimeError("existing worker request limit cannot be silently rebound")
            if self.launch.get("dev_sql_timeout_seconds", 30) != request_timeout_seconds:
                raise RuntimeError("existing local SQL timeout cannot be silently rebound")
            if self.launch.get("sql_max_duration_ms", 5000) != sql_duration_ms:
                raise RuntimeError("existing SQL duration limit cannot be silently rebound")
            if self.launch.get("wal_replay_bytes", 0) != wal_replay_bytes:
                raise RuntimeError("WAL replay budget cannot be silently rebound")
            if self.arguments.backend == "s3" and self.launch.get("storage_identity") != storage_identity("s3"):
                raise RuntimeError("existing S3 storage identity cannot be silently rebound")
            self.config = json.loads((self.directory / "platform.json").read_text())
            if self.config.get("writer_admission", False) != self.arguments.writer_admission:
                raise RuntimeError("writer admission configuration mismatch")
            configuration = self.config.get("owned_stop_proof")
            if not configuration:
                raise RuntimeError("owned stop proof configuration missing")
            try:
                self.verify_restart_failure_gate(configuration)
            except Exception:
                self.restart_failure_gate_rejected = True
                raise
            verify_owned_stop_proofs(self.directory, configuration, self.launch["workers"])
            verify_clean_terminal(self.directory, configuration)
            # 复用的 clean 语义必须与停机时一致：历史 terminal proof 也是在一个可能滞后于侧车的 payload 上做出的判定。
            verify_no_unresolved_operations(self.directory, configuration)
            self.verify_clean_stop_record(configuration)
            self.stage_clean_evidence(configuration)
            return
        if (self.directory / "launcher-terminal.json").exists():
            raise RuntimeError("unexpected clean launcher terminal proof")
        authority = free_address()
        broker = free_address()
        self.launch = {"backend": self.arguments.backend, "storage_identity": storage_identity(self.arguments.backend), "listen": self.arguments.listen, "tls": self.arguments.tls, "authority": authority, "workers": {}}
        self.launch["writer_admission"] = self.arguments.writer_admission
        self.launch["worker_max_request_bytes"] = worker_request_bytes
        self.launch["dev_sql_timeout_seconds"] = request_timeout_seconds
        self.launch["sql_max_duration_ms"] = sql_duration_ms
        self.launch["wal_replay_bytes"] = wal_replay_bytes
        self.launch["sql_open_observer"] = observer
        worker_auth = {}
        worker_config = []
        sql_tokens, key_tokens = {}, {}
        for suffix in ("a", "b"):
            identity = "worker-" + suffix
            credentials = {name: secrets.token_hex(32) for name in ("authority_token", "control_token", "sql_token", "key_token")}
            credentials["address"] = free_address()
            credentials["cache"] = str(self.directory / ("cache-" + suffix))
            self.launch["workers"][identity] = credentials
            worker_auth[identity] = credentials["authority_token"]
            sql_tokens[identity] = credentials["sql_token"]
            key_tokens[identity] = credentials["key_token"]
            worker_config.append({"worker": {"id": identity, "address": "http://" + credentials["address"], "region": "local", "capacity": 32, "load": 0, "healthy": False, "last_heartbeat_ms": 0, "draining": False}, "control_token": credentials["control_token"]})
        signing = self.directory / "keys/signing.key"
        descriptor = os.open(signing, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(secrets.token_bytes(32))
            output.flush()
            os.fsync(output.fileno())
        self.config = {
            "controller": {"authority_url": "http://" + authority, "authority_token": secrets.token_hex(32), "api_token": secrets.token_hex(32), "workers": worker_config},
            "metadata_directory": str(self.directory / "controller"), "state_directory": str(self.directory / "platform"),
            "audit_path": str(self.directory / "audit/events.json"), "kms_keyring": str(self.directory / "keys/kms/keyring.json"), "signing_key_file": str(signing),
            "bootstrap_key": bootstrap_credential(), "gateway_listen": self.arguments.listen, "broker_listen": broker,
            "worker_sql_tokens": sql_tokens, "worker_key_tokens": key_tokens, "worker_authority_tokens": worker_auth,
            "insecure_loopback": not self.arguments.tls,
            "writer_admission": self.arguments.writer_admission,
        }
        proof_directory = self.directory / "owned-proof"
        private_directory(proof_directory)
        proof_key = self.directory / "keys/owned-proof.key"
        descriptor = os.open(proof_key, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(secrets.token_bytes(32))
            output.flush()
            os.fsync(output.fileno())
        self.config["owned_stop_proof"] = {"generation": secrets.token_hex(32), "directory": str(proof_directory), "key_file": str(proof_key), "workers": {identity: secrets.token_hex(32) for identity in self.launch["workers"]}}
        if self.arguments.tls:
            certificate = pathlib.Path(os.environ.get("PERI_TLS_CERT", self.directory / "keys/gateway.crt"))
            private_key = pathlib.Path(os.environ.get("PERI_TLS_KEY", self.directory / "keys/gateway.key"))
            if not certificate.exists() or not private_key.exists():
                if os.environ.get("PERI_TLS_CERT") or os.environ.get("PERI_TLS_KEY"):
                    raise RuntimeError("configured TLS files are missing")
                host = self.arguments.listen.rsplit(":", 1)[0]
                ca = self.directory / "keys/dev-ca.crt"
                ca_key = self.directory / "keys/dev-ca.key"
                signing_request = self.directory / "keys/gateway.csr"
                extensions = self.directory / "keys/gateway.ext"
                extensions.write_text("basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1,IP:" + host + "\n")
                commands = [
                    ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "7", "-subj", "/CN=peri-loom disposable development CA", "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign", "-keyout", str(ca_key), "-out", str(ca)],
                    ["openssl", "req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost", "-keyout", str(private_key), "-out", str(signing_request)],
                    ["openssl", "x509", "-req", "-in", str(signing_request), "-CA", str(ca), "-CAkey", str(ca_key), "-CAcreateserial", "-days", "7", "-sha256", "-extfile", str(extensions), "-out", str(certificate)],
                ]
                for command in commands:
                    subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
                ca_key.chmod(0o600)
                private_key.chmod(0o600)
                self.launch["ca_file"] = str(ca)
            else:
                self.launch["ca_file"] = os.environ.get("PERI_TLS_CA", str(certificate))
            self.config["tls_certificate"], self.config["tls_key"] = str(certificate), str(private_key)
        private_json(self.directory / "authority-workers.json", worker_auth)
        private_json(self.directory / "platform.json", self.config)
        private_json(launch_path, self.launch)

    def start(self, name, command, environment):
        if name in self.owned_identities:
            raise RuntimeError("owned worker incarnation cannot be reused")
        path = self.directory / "logs" / (name + ".log")
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        log = os.fdopen(descriptor, "w")
        self.logs.append(log)
        process = subprocess.Popen(command, env=environment, stdout=subprocess.PIPE, stderr=log, text=True, start_new_session=True)
        self.processes[name] = process
        configuration = self.config.get("owned_stop_proof")
        if configuration and name in configuration["workers"]:
            identity = {"version": 1, "generation": configuration["generation"], "worker_id": name, "nonce": configuration["workers"][name], "pid": process.pid, "started": secrets.token_hex(32), "waited": False, "exit_code": 0}
            self.owned_identities[name] = identity
            atomic_private_json(pathlib.Path(configuration["directory"]) / (name + ".identity.json"), signed_owned_identity(configuration, identity))
        ready = queue.Queue()
        def read_output():
            for line in process.stdout:
                if ready.empty():
                    ready.put(line)
        threading.Thread(target=read_output, daemon=True).start()
        try:
            event = json.loads(ready.get(timeout=15))
        except (queue.Empty, ValueError):
            raise RuntimeError(name + " did not become ready; inspect private logs") from None
        if not event.get("ready"):
            raise RuntimeError(name + " readiness rejected")
        self.ready_services.add(name)
        return event

    def stop_worker(self, worker):
        if worker not in self.owned_identities:
            raise RuntimeError("worker is not owned by this launcher")
        process = self.processes[worker]
        if self.stop.is_set():
            raise RuntimeError("launcher shutdown owns worker stop")
        deadline = time.monotonic() + SHUTDOWN_STAGE_CAPS["workers"]
        if process.poll() is None:
            process.terminate()
        while not self.stop.is_set():
            timeout = min(0.2, self.remaining(deadline))
            try:
                exit_code = process.wait(timeout=timeout)
                if self.stop.is_set():
                    break
                self.record_joined_worker_stop(worker, exit_code)
                return
            except subprocess.TimeoutExpired:
                if self.remaining(deadline) > 0:
                    continue
                self.shutdown_failure = "maintenance worker shutdown incomplete"
                self.begin_shutdown()
                configuration = self.config.get("owned_stop_proof")
                if configuration:
                    self.write_stop_record("unclean", self.shutdown_failure)
                    stage = {"name": worker, "cap_ms": SHUTDOWN_STAGE_CAPS["workers"] * 1000,
                             "waited_ms": SHUTDOWN_STAGE_CAPS["workers"] * 1000,
                             "exit_code": process.poll(), "signal": signal.SIGKILL,
                             "ack": False, "authority_lock": self.authority_lock_status()}
                    self.stop_record["stages"].append(stage)
                    self.write_stop_record()
                process.kill()
                process.wait(timeout=SHUTDOWN_FINALIZE_SECONDS)
                raise RuntimeError("maintenance worker shutdown incomplete; retain isolation")
        raise RuntimeError("launcher shutdown owns worker stop")

    def record_joined_worker_stop(self, worker, exit_code):
        proof = dict(self.owned_identities[worker], waited=True, exit_code=exit_code)
        configuration = self.config["owned_stop_proof"]
        atomic_private_json(pathlib.Path(configuration["directory"]) / (worker + ".stop.json"), signed_owned_identity(configuration, proof))

    def start_maintenance(self):
        listener = socket.socket(socket.AF_UNIX)
        listener.bind(str(self.directory / "owned-stop.sock"))
        os.chmod(self.directory / "owned-stop.sock", 0o600)
        listener.listen(2)
        listener.settimeout(0.2)
        self.maintenance = listener
        def serve():
            while not self.stop.is_set():
                try:
                    connection, _ = listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    return
                with connection:
                    connection.settimeout(2)
                    try:
                        incoming = json.loads(connection.makefile("rb").readline(1024), object_pairs_hook=unique_object)
                        if set(incoming) != {"worker"} or not isinstance(incoming["worker"], str):
                            raise RuntimeError("invalid owned worker request")
                        self.stop_worker(incoming["worker"])
                        connection.sendall(b'{"stopped":true}\n')
                    except Exception:
                        try:
                            connection.sendall(b'{"stopped":false}\n')
                        except OSError:
                            pass
        threading.Thread(target=serve, daemon=True).start()

    def run(self):
        self.acquire_run_lock()
        self.prepare()
        authority_environment = safe_environment(capacity_trace=True)
        experimental_environment(authority_environment)
        authority_environment.update({"PERI_AUTHORITY_CONTROL_TOKEN": self.config["controller"]["authority_token"], "PERI_AUTHORITY_WORKERS_FILE": str(self.directory / "authority-workers.json")})
        backend = str(self.directory / "backend")
        if self.arguments.backend == "s3":
            backend = "s3"
            for name in ("S3_ENDPOINT", "S3_BUCKET", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY"):
                if not os.environ.get(name):
                    raise RuntimeError(name + " is required for the authority only")
            authority_environment.update({name: value for name, value in os.environ.items() if name.startswith("S3_")})
        self.start("authority", [str(self.rust), "authority", backend, self.launch["authority"], str(self.directory / "authority")], authority_environment)
        platform_environment = safe_environment()
        if self.launch["dev_sql_timeout_seconds"] != 30:
            platform_environment["PERI_DEV_SQL_TIMEOUT_SECONDS"] = str(self.launch["dev_sql_timeout_seconds"])
        platform_environment["PERI_SQL_OPEN_OBSERVER"] = "1" if getattr(self.arguments, "sql_open_observer", False) else "0"
        platform_environment["PERI_HRANA_EXPERIMENTAL"] = "1" if os.environ.get("PERI_HRANA_EXPERIMENTAL") == "1" else "0"
        # The launcher owns the outer deadline, so it declares it: the platform
        # refuses to start on a budget its own stage cap would not cover, and on a
        # stage cap the deployment grace would not cover either.
        platform_environment["PERI_SHUTDOWN_STAGE_CAP_MS"] = str(SHUTDOWN_STAGE_CAPS["platform"] * 1000)
        platform_environment["PERI_SHUTDOWN_OUTER_GRACE_MS"] = str(ORCHESTRATION_GRACE_SECONDS * 1000)
        self.start("platform", [str(self.go), "-config", str(self.directory / "platform.json")], platform_environment)
        for identity, worker in self.launch["workers"].items():
            environment = experimental_environment(safe_environment(capacity_trace=True), maintenance=True)
            environment.update({"PERI_WORKER_AUTHORITY_TOKEN": worker["authority_token"], "PERI_WORKER_CONTROL_TOKEN": worker["control_token"], "PERI_WORKER_SQL_TOKEN": worker["sql_token"], "PERI_WORKER_KEY_TOKEN": worker["key_token"], "PERI_KMS_ENDPOINT": "http://" + self.config["broker_listen"]})
            environment["PERI_WORKER_WRITER_PROFILE"] = "1" if self.arguments.writer_admission else "0"
            environment["PERI_WORKER_MAX_REQUEST_BYTES"] = str(self.launch["worker_max_request_bytes"])
            environment["PERI_SQL_MAX_DURATION_MS"] = str(self.launch["sql_max_duration_ms"])
            # Unset means the worker's own default, so the number lives in one place unless an
            # experiment asks for a different window.
            if self.launch["wal_replay_bytes"] > 0:
                environment["PERI_WORKER_WAL_REPLAY_BYTES"] = str(self.launch["wal_replay_bytes"])
            self.start(identity, [str(self.rust), "owned-worker", "http://" + self.launch["authority"], worker["cache"], identity, worker["address"]], environment)
        self.start_maintenance()
        scheme = "https://" if self.arguments.tls else "http://"
        address = self.arguments.listen.replace("0.0.0.0:", "127.0.0.1:")
        endpoint = scheme + address
        certificate = self.launch.get("ca_file") or self.config.get("tls_certificate")
        # From here on the platform may mutate controller placement/grants, the
        # authority backend and its own operations sidecar. Only a failure before
        # this point may restore the previous clean canonical proof.
        self.phase = "reconciling"
        request(endpoint, self.config["bootstrap_key"], "/v1/reconcile", {}, certificate)
        connection_path = self.directory / "connection.json"
        if not connection_path.exists():
            credential = self.config["bootstrap_key"]
            request(endpoint, credential, "/v1/organizations/create", {"id": "local", "name": "Local"}, certificate)
            request(endpoint, credential, "/v1/projects/create", {"id": "default", "organization": "local", "region": "local"}, certificate)
            key = request(endpoint, credential, "/v1/keys/create", {"subject": "local-admin", "project": "default", "role": "admin", "service_account": True}, certificate)
            database = {"namespace": {"tenant": "local_default", "database": "demo"}, "branch": "main"}
            request(endpoint, key["credential"], "/v1/databases/create", {"request_id": "local-bootstrap-db", "project": "default", "database": database, "role": "writer"}, certificate)
            private_json(connection_path, {"endpoint": endpoint, "project_api_key": key["credential"], "global_api_key": credential, "database": database, "ca_file": certificate})
        else:
            connection = json.loads(connection_path.read_text())
            request(endpoint, connection["project_api_key"], "/v1/databases/status", {"database": connection["database"]}, certificate)
        topology = {"launcher_pid": os.getpid(), "processes": {name: {"pid": process.pid} for name, process in self.processes.items()}, "gateway": endpoint, "authority": "http://" + self.launch["authority"], "broker": "http://" + self.config["broker_listen"], "workers": {identity: "http://" + worker["address"] for identity, worker in self.launch["workers"].items()}}
        topology_path = self.directory / "topology.json"
        if topology_path.exists():
            topology_path.unlink()
        private_json(topology_path, topology)
        # Readiness is the point where client work can begin. Until it is announced a
        # failure is a startup failure and the last clean proof stays restorable; after
        # it, any failure must keep ordinary unclean-stop semantics.
        self.phase = "running"
        self.discard_clean_evidence()
        print(json.dumps({"ready": True, "service": "local-project", "address": endpoint, "connection_file": str(connection_path), "topology_file": str(topology_path)}), flush=True)
        reported = set()
        while not self.stop.wait(0.2):
            if any(self.processes[name].poll() is not None for name in ("platform", "authority")):
                raise RuntimeError("a control-plane service stopped; inspect private logs")
            for name in ("worker-a", "worker-b"):
                if self.processes[name].poll() is not None and name not in reported:
                    reported.add(name)
                    print(json.dumps({"degraded": True, "service": name, "reason": "worker_stopped_controller_reconciles_remaining_capacity"}), flush=True)

    def shutdown_request(self, name, deadline, replies):
        try:
            if name == "authority":
                address, token, path = self.launch["authority"], self.config["controller"]["authority_token"], "/"
            else:
                worker = self.launch["workers"][name]
                address, token, path = worker["address"], worker["control_token"], "/control"
            timeout = self.remaining(deadline)
            if timeout <= 0:
                raise RuntimeError("shutdown deadline expired")
            request("http://" + address, token, path, {"version": 1, "command": {"command": "shutdown"}}, timeout=timeout)
            replies.put(True)
        except Exception:
            replies.put(False)

    def kill_unclean(self, name, stage, reason):
        stage["signal"] = signal.SIGKILL
        stage["authority_lock"] = self.authority_lock_status()
        self.write_stop_record("unclean", reason)
        process = self.processes[name]
        if process.poll() is None:
            process.kill()
        stage["exit_code"] = process.poll()
        self.write_stop_record()

    def shutdown_stage(self, names, cap, failures):
        started = time.monotonic()
        deadline = min(self.shutdown_deadline, started + cap)
        pending = []
        for name in names:
            process = self.processes.get(name)
            if process is None:
                continue
            stage = {"name": name, "cap_ms": cap * 1000, "waited_ms": 0,
                     "exit_code": process.poll(), "ack": False,
                     "authority_lock": self.authority_lock_status(), "waits": []}
            self.stop_record["stages"].append(stage)
            self.write_stop_record()
            replies = None
            if stage["exit_code"] is None and not failures and self.remaining(deadline) > 0:
                if name == "platform":
                    process.terminate()
                    stage["ack"] = True
                else:
                    replies = queue.Queue()
                    # urlopen 的 socket timeout 不是总 deadline；后台请求只能交回结果，不能写证明。
                    threading.Thread(target=self.shutdown_request, args=(name, deadline, replies), daemon=True).start()
            pending.append((name, process, stage, replies))

        def finish(item):
            name, process, stage, replies = item
            local_failures = []
            try:
                if stage["exit_code"] is None:
                    if self.remaining(deadline) <= 0:
                        raise subprocess.TimeoutExpired(name, 0)
                    if replies is not None:
                        timeout = self.remaining(deadline)
                        stage["waits"].append({"operation": "rpc", "timeout_ms": timeout * 1000})
                        stage["ack"] = replies.get(timeout=timeout)
                        if not stage["ack"]:
                            local_failures.append(name + " shutdown response incomplete")
                    timeout = self.remaining(deadline)
                    stage["waits"].append({"operation": "join", "timeout_ms": timeout * 1000})
                    stage["exit_code"] = process.wait(timeout=timeout)
                    if self.remaining(deadline) <= 0:
                        raise subprocess.TimeoutExpired(name, timeout)
                    if not local_failures and stage["ack"] and stage["exit_code"] == 0 and name in self.owned_identities:
                        try:
                            self.record_joined_worker_stop(name, 0)
                        except (OSError, RuntimeError, ValueError):
                            local_failures.append(name + " owned stop proof unavailable")
                if stage["exit_code"] != 0:
                    local_failures.append(name + " exited without a clean proof")
            except (subprocess.TimeoutExpired, queue.Empty):
                local_failures.append(name + " shutdown incomplete; no clean proof")
                stage["remaining_ms"] = self.remaining() * 1000
                self.kill_unclean(name, stage, local_failures[-1])
            stage["waited_ms"] = (time.monotonic() - started) * 1000
            stage["authority_lock"] = self.authority_lock_status()
            return local_failures

        if failures:
            for item in pending:
                name, process, stage, _ = item
                if stage["exit_code"] is None:
                    reason = name + " skipped graceful shutdown after prior failure"
                    self.kill_unclean(name, stage, reason)
                    failures.append(reason)
            self.write_stop_record("unclean", "; ".join(failures))
            return

        if len(pending) == 1:
            failures.extend(finish(pending[0]))
        else:
            results = [None] * len(pending)
            errors = [None] * len(pending)
            def run(index, item):
                try:
                    results[index] = finish(item)
                except BaseException as error:
                    errors[index] = error
            threads = [threading.Thread(target=run, args=(index, item), daemon=True)
                       for index, item in enumerate(pending)]
            for thread in threads:
                thread.start()
            for index, thread in enumerate(threads):
                thread.join(timeout=max(0, self.remaining(deadline)))
                if thread.is_alive():
                    name, process, stage, _ = pending[index]
                    reason = name + " shutdown deadline expired; no clean proof"
                    self.kill_unclean(name, stage, reason)
                    failures.append(reason)
                elif errors[index] is not None:
                    raise errors[index]
                else:
                    failures.extend(results[index])
        self.write_stop_record("unclean" if failures else None, "; ".join(failures) if failures else None)

    def cleanup(self):
        self.begin_shutdown()
        self.write_stop_record("unclean" if self.shutdown_failure else None, self.shutdown_failure)
        if self.maintenance is not None:
            self.maintenance.close()
            (self.directory / "owned-stop.sock").unlink(missing_ok=True)
        failures = ["prior unclean stop; no clean proof"] if self.stop_record and self.stop_record["outcome"] == "unclean" else []
        for names, stage in ((["platform"], "platform"), (["worker-a", "worker-b"], "workers"), (["authority"], "authority")):
            self.shutdown_stage(names, SHUTDOWN_STAGE_CAPS[stage], failures)
        for process in self.processes.values():
            if process.stdout is not None and process.poll() is not None:
                process.stdout.close()
        for log in self.logs:
            log.close()
        if self.remaining() <= 0:
            failures.append("shutdown deadline expired; no clean proof")
        if failures:
            self.write_stop_record("unclean", "; ".join(failures))
            raise RuntimeError("; ".join(failures))
        self.cleanup_complete = True

    def mark_clean_terminal(self):
        try:
            if not self.cleanup_complete or self.stop_record is None or self.stop_record["outcome"] == "unclean" or self.remaining() <= 0:
                raise RuntimeError("shutdown incomplete or deadline expired; no clean proof")
            if set(self.processes) != {"authority", "platform", "worker-a", "worker-b"}:
                raise RuntimeError("clean launcher terminal requires all services")
            configuration = self.config["owned_stop_proof"]
            verify_owned_stop_proofs(self.directory, configuration, self.launch["workers"])
            authority = self.directory / "authority/authority.lock"
            if authority.is_symlink() or not authority.is_file() or authority.read_bytes() != b"clean\n":
                raise RuntimeError("authority clean startup marker missing")
            verify_no_unresolved_operations(self.directory, configuration)
            self.stop_record["unresolved_operations"] = False
            terminal = signed_owned_identity(configuration, terminal_record(self.directory, configuration))
            if self.remaining() <= 0:
                raise RuntimeError("shutdown deadline expired; no clean proof")
            atomic_private_json(self.directory / "launcher-terminal.json", terminal)
            self.write_stop_record("clean", "terminal_proof_written")
        except Exception as error:
            # 诊断要能区分“未物化”和“未解析”，否则 launcher-stop.json 只留下同一句话。
            self.write_stop_record("unclean", ("terminal_proof_unavailable: " + str(error))[:512])
            raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", default=".run/v1")
    parser.add_argument("--backend", choices=("local", "s3"), default="local")
    parser.add_argument("--listen", default="127.0.0.1:9443")
    parser.add_argument("--tls", action="store_true")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--writer-admission", action="store_true")
    parser.add_argument("--worker-max-request-bytes", type=int, default=512 * 1024)
    parser.add_argument("--sql-max-duration-ms", type=int, default=5000)
    parser.add_argument("--wal-replay-bytes", type=int, default=0)
    parser.add_argument("--dev-sql-timeout-seconds", type=int, default=30)
    parser.add_argument("--sql-open-observer", action="store_true")
    parser.add_argument("--shutdown-budget-seconds", type=float, default=SHUTDOWN_BUDGET_SECONDS)
    parser.add_argument("--stop-worker", choices=("worker-a", "worker-b"))
    arguments = parser.parse_args()
    if arguments.stop_worker:
        try:
            stop_owned_worker(pathlib.Path(arguments.run_dir).resolve(), arguments.stop_worker)
        except Exception:
            print("owned worker stop unavailable; retain isolation", file=sys.stderr)
            return 1
        print(json.dumps({"stopped": True, "remote_terminal": "not_proven", "recovery": "new_database_or_fixture_required"}))
        return 0
    project = Project(arguments)
    signal.signal(signal.SIGTERM, lambda signum, frame: project.begin_shutdown())
    signal.signal(signal.SIGINT, lambda signum, frame: project.begin_shutdown())
    failure = None
    try:
        project.run()
    except Exception as error:
        failure = error
    finally:
        original_failure = failure
        try:
            # A failed restart before readiness never admitted workload, so its
            # teardown diagnostics go to the restart-failure record and the last
            # clean stop/terminal stay untouched (restored or preserved for audit).
            startup_failure = failure is not None and project.phase != "running" and project.restart_attempt
            teardown_error = None
            processes_stopped = True
            if startup_failure:
                project.preserve_clean_evidence = True
                project.begin_restart_failure(failure)
            try:
                project.cleanup()
            except Exception as error:
                if not startup_failure:
                    raise
                # A teardown failure does not invalidate the snapshot by itself:
                # recovery still runs if every process is confirmed stopped.
                teardown_error = error
            processes_stopped = project.confirm_processes_stopped()
            if failure is None:
                if teardown_error is not None:
                    raise teardown_error
                if not processes_stopped:
                    raise RuntimeError("launcher processes not confirmed stopped; no clean proof")
                project.mark_clean_terminal()
            elif startup_failure:
                try:
                    outcome = project.restore_clean_evidence(processes_stopped)
                except Exception as error:
                    project.abandon_restart_recovery(error, teardown_error, processes_stopped)
                    failure = RuntimeError(f"{failure}; clean evidence recovery failed: {error}")
                else:
                    if project.restart_failure is not None:
                        project.complete_restart_failure(outcome, teardown_error, processes_stopped)
                    else:
                        project.last_clean_evidence_outcome = outcome
                project.write_failure_record_unavailable(original_failure)
            else:
                project.write_stop_record("unclean", "launcher_failed_before_cleanup")
        except Exception as error:
            if original_failure is not None:
                failure = RuntimeError(f"{original_failure}; launcher cleanup failed: {error}")
            else:
                failure = error
        finally:
            if failure is not None and project.restart_failure_error is not None:
                failure = RuntimeError(f"{failure}; restart failure record unavailable: {project.restart_failure_error}")
            project.release_run_lock()
    if failure:
        print("project stopped: " + str(failure), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
