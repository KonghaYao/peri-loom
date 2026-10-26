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
SPAWN_STOP_FINALIZE_SECONDS = 5
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


def decode_seal_record(payload):
    """Return a structurally valid seal record, regardless of its clean flag."""
    try:
        record = json.loads(payload, object_pairs_hook=unique_object)
    except (ValueError, TypeError, UnicodeError, RuntimeError):
        return None
    if not isinstance(record, dict) or set(record) != LOCAL_SEAL_FIELDS:
        return None
    if record.get("version") != 1 or type(record.get("version")) is not int or type(record.get("clean")) is not bool:
        return None
    if not isinstance(record.get("owner"), str) or not record["owner"]:
        return None
    return record


def validate_local_seal_payload(payload):
    """Return the decoded seal record when it is reusable, otherwise None.

    The Rust authority denies unknown fields and requires version 1 with
    clean=true before it will take over a backend. A checksum-valid envelope
    around any other record must not be promoted to canonical clean evidence.
    """
    record = decode_seal_record(payload)
    if record is None or record.get("clean") is not True:
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


def local_object_any(backend_directory, key):
    """Return (path, payload, reliable) without requiring a clean seal record.

    Proof recovery must hash the dirty seal this startup attempt wrote, so it
    cannot use local_object_path, which deliberately rejects dirty records.
    """
    try:
        import blake3
    except ImportError:
        blake3 = None
    if blake3 is None:
        matches = []
        for candidate in sorted(backend_directory.glob("*.obj")):
            try:
                envelope = json.loads(candidate.read_bytes(), object_pairs_hook=unique_object)
            except (OSError, ValueError, TypeError, UnicodeError, RuntimeError):
                continue
            if not isinstance(envelope, dict) or envelope.get("key") != key:
                continue
            payload = decode_local_envelope(envelope, key)
            if payload is not None:
                matches.append((candidate, payload))
        if len(matches) == 1:
            return matches[0][0], matches[0][1], False
        return None, None, False
    candidate = backend_directory / (blake3.blake3(key.encode()).hexdigest() + ".obj")
    if not candidate.is_file():
        return None, None, True
    try:
        if candidate.stat().st_size > LOCAL_ENVELOPE_LIMIT_BYTES:
            return None, None, False
        envelope = json.loads(candidate.read_bytes(), object_pairs_hook=unique_object)
    except (OSError, ValueError, TypeError, UnicodeError, RuntimeError):
        return None, None, False
    payload = decode_local_envelope(envelope, key)
    if payload is None or envelope["checksum"] != blake3.blake3(payload).hexdigest():
        return None, None, False
    return candidate, payload, True


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, destination):
        return None


def normalize_launch(value):
    # 只在内存中补齐历史默认值；改写原文件会使已有 terminal 哈希失效。
    if not isinstance(value, dict):
        raise RuntimeError("launch configuration must be an object")
    defaults = {"writer_admission": False, "worker_max_request_bytes": 512 * 1024,
                "dev_sql_timeout_seconds": 30, "sql_max_duration_ms": 5000,
                "wal_replay_bytes": 0, "sql_open_observer": False}
    normalized = {**defaults, **value}
    for name, default in defaults.items():
        if type(normalized[name]) is not type(default):
            raise RuntimeError("invalid launch configuration field: " + name)
    for name in ("backend", "listen", "tls", "authority", "workers"):
        if name not in normalized:
            raise RuntimeError("required launch identity missing: " + name)
    if not isinstance(normalized["workers"], dict) or set(normalized["workers"]) != {"worker-a", "worker-b"}:
        raise RuntimeError("launch worker identity set invalid")
    for worker in normalized["workers"].values():
        for name in ("address", "cache", "authority_token", "control_token", "sql_token", "key_token"):
            if not isinstance(worker, dict) or not isinstance(worker.get(name), str) or not worker[name]:
                raise RuntimeError("required launch worker field missing or invalid: " + name)
    return normalized


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


def file_sha256_or_none(path):
    return file_sha256(path) if path.is_file() else None


def atomic_private_bytes(destination, value):
    temporary = destination.with_name(destination.name + "." + secrets.token_hex(16))
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as output:
        output.write(value)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, destination)
    fsync_directory(destination.parent)


def backend_manifest(directory, exclude=()):
    """文件清单只用于证明启动尝试未写 backend；seal/root 由单独快照校验。"""
    excluded = {exclude} if isinstance(exclude, str) else set(exclude)
    entries = []
    if directory.is_dir():
        for path in sorted(directory.rglob("*")):
            if path.is_symlink():
                raise RuntimeError("authority backend must not contain symlinks")
            if path.is_file() and path.name not in excluded:
                info = path.stat()
                entries.append([str(path.relative_to(directory)), info.st_size, info.st_mtime_ns])
    return hashlib.sha256(json.dumps(entries, separators=(",", ":")).encode()).hexdigest()


def authority_root_records(state_payload, backend_directory):
    """Snapshot mutable authority root metadata so startup leases can be rolled back."""
    records, seen = [], set()
    databases = state_payload.get("databases", {})
    if not isinstance(databases, dict):
        raise RuntimeError("platform database view is invalid")
    for record in databases.values():
        try:
            namespace = record["id"]["namespace"]
            key = str(namespace["tenant"]) + "/" + str(namespace["database"]) + "/authority/root"
        except (KeyError, TypeError) as error:
            raise RuntimeError("platform database identity is invalid") from error
        if key in seen:
            continue
        seen.add(key)
        path, payload, reliable = local_object_any(backend_directory, key)
        if path is None:
            continue
        try:
            root = json.loads(payload, object_pairs_hook=unique_object)
        except (ValueError, TypeError, UnicodeError, RuntimeError) as error:
            raise RuntimeError("authority root is invalid: " + key) from error
        if not isinstance(root, dict) or not isinstance(root.get("root"), dict):
            raise RuntimeError("authority root is invalid: " + key)
        records.append({"key": key, "name": path.name, "sha256": file_sha256(path), "reliable": reliable})
    return records


def process_is_running(pid):
    if type(pid) is not int or pid <= 0:
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:
        status = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True, check=False).stdout.strip()
    except OSError:
        return True
    return not status.startswith("Z")


def parse_process_table(text):
    entries = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        pid_text, separator, command = stripped.partition(" ")
        if not separator:
            continue
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        command = command.lstrip()
        if command:
            entries.append((pid, command))
    return entries


def recorded_argv(attempt, name):
    commands = attempt.get("commands")
    if not isinstance(commands, dict):
        raise RuntimeError("recorded startup argv is missing; stop residual services with the operator")
    argv = commands.get(name)
    if (not isinstance(argv, list) or not argv
            or not all(isinstance(part, str) and part for part in argv)):
        raise RuntimeError("recorded startup argv is missing or invalid for " + name)
    return argv


def match_recorded_argv(entries, argv):
    if (not isinstance(argv, list) or not argv
            or not all(isinstance(part, str) and part for part in argv)):
        raise RuntimeError("recorded startup argv is missing or invalid")
    target = " ".join(argv)
    return {pid for pid, command in entries if command == target}


def pid_matches_recorded_argv(entries, pid, argv):
    target = " ".join(argv)
    return any(entry_pid == pid and command == target for entry_pid, command in entries)


def verify_addresses_available(addresses):
    probes = []
    try:
        for address in addresses:
            host, port = address.rsplit(":", 1)
            host = host.strip("[]")
            family = socket.AF_INET6 if ":" in host else socket.AF_INET
            probe = socket.socket(family, socket.SOCK_STREAM)
            probes.append(probe)
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind((host, int(port)))
            probe.listen(1)
    except OSError as error:
        raise RuntimeError("listener unavailable before startup; release the occupied port and retry the original configuration") from error
    finally:
        for probe in probes:
            probe.close()


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


def terminal_record(run_directory, configuration, proof_directory=None, state_directory=None):
    proof_directory = proof_directory or run_directory / "owned-proof"
    state_directory = state_directory or run_directory / "platform"
    return {"version": 1, "generation": configuration["generation"],
            "launch_sha256": hashlib.sha256((run_directory / "launch.json").read_bytes()).hexdigest(),
            "platform_sha256": hashlib.sha256((run_directory / "platform.json").read_bytes()).hexdigest(),
            "state_sha256": hashlib.sha256((state_directory / "state.json").read_bytes()).hexdigest(),
            "state_floor_sha256": hashlib.sha256((state_directory / "state.json.floor").read_bytes()).hexdigest(),
            "proof_sha256": {f"{worker}.{kind}.json": hashlib.sha256((proof_directory / f"{worker}.{kind}.json").read_bytes()).hexdigest()
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


def read_signed_private_at(path, configuration, label):
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


def verify_snapshot_terminal(staging, run_directory, configuration):
    marker = staging / "launcher-terminal.json"
    if marker.is_symlink() or not marker.is_file() or marker.stat().st_mode & 0o077 or marker.stat().st_size > 4096:
        raise RuntimeError("preserved terminal proof missing or invalid")
    try:
        signed = json.loads(marker.read_bytes(), object_pairs_hook=unique_object)
        if set(signed) != {"payload", "mac"} or not isinstance(signed["payload"], str) or not isinstance(signed["mac"], str):
            raise ValueError("invalid terminal proof envelope")
        expected = terminal_record(run_directory, configuration, staging / "owned-proof", staging / "platform")
        payload = json.loads(signed["payload"], object_pairs_hook=unique_object)
        if payload != expected or type(payload["version"]) is not int:
            raise ValueError("preserved terminal does not match its snapshot")
        key = pathlib.Path(configuration["key_file"]).read_bytes()
        if not hmac.compare_digest(signed["mac"], hmac.new(key, signed["payload"].encode(), hashlib.sha256).hexdigest()):
            raise ValueError("preserved terminal signature mismatch")
    except (OSError, ValueError, TypeError, UnicodeError, RuntimeError) as error:
        raise RuntimeError("preserved clean launcher terminal proof invalid") from error
    return payload


def verify_snapshot_stop(staging, configuration):
    record = read_signed_private_at(staging / "launcher-stop.json", configuration, "preserved launcher stop record")
    if (type(record.get("version")) is not int or record.get("version") != 1
            or record.get("generation") != configuration["generation"]
            or record.get("outcome") != "clean"):
        raise RuntimeError("preserved launcher stop outcome is not clean")
    return record


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
        raise OperationViewNotMaterialized("platform operation view was not materialized at shutdown; if a surviving platform or authority process remains, send SIGTERM and wait for its joined exit so the shutdown checkpoint materializes the operation view, then rerun the original configuration with --recover; rebuild the volume only if the checkpoint is still absent after the joined stop; never clear markers or proofs to force startup")


def released_isolation(record):
    """A quarantine that the release path ended under its own durable proof.

    The operation stays quarantined and its outcome stays unknown on purpose: the
    release never claims the old data is intact. What ended is the namespace
    isolation, and gateway admission treats such a record as closed for the same
    reason (gateway.go skips released quarantines). The launcher must agree with
    that durable rule or a released run can never restart cleanly.
    """
    return (record.get("state") == "quarantined"
            and bool(record.get("release_request_id"))
            and record.get("decision") == "retain_isolation_new_database_required"
            and record.get("local_resources_retired") is True
            and record.get("retirement_pending", False) is False
            and record.get("release_authority_mode") in ("detached", "revoked")
            and type(record.get("release_epoch")) is int
            and record.get("release_epoch") >= 0)


def verify_no_unresolved_operations(run_directory, configuration, state_path=None):
    path = state_path or run_directory / "platform/state.json"
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
            if not isinstance(record, dict):
                raise ValueError("unresolved operation")
            if released_isolation(record):
                # The release path recorded the stop proof, the retirement and the
                # authority attestation this record ends under; it is history now,
                # not an open isolation.
                continue
            if (record.get("source") == "gateway" and record.get("action") == "open"
                    and record.get("state") == "terminal"
                    and record.get("business_outcome") == "unknown"
                    and record.get("may_have_committed") is False):
                # Opening a session stores nothing and submits no SQL. The reply
                # may have been lost, so the outcome stays unknown, but it cannot
                # have committed: the leaked worker session is reclaimed by the
                # worker's idle sweep. It therefore neither isolates the namespace
                # nor blocks a clean restart. Every other unknown still does.
                continue
            if record.get("state") != "terminal" or record.get("ambiguous", False) or record.get("retirement_pending", False) or record.get("local_resources_retired", False) or record.get("decision", ""):
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


def verify_owned_stop_proofs(run_directory, configuration, workers, proof_directory=None):
    """Verify both worker incarnations were joined before a clean restart."""
    canonical_directory = run_directory / "owned-proof"
    key_file = run_directory / "keys/owned-proof.key"
    if (configuration.get("key_file") != str(key_file)
            or set(configuration.get("workers", {})) != set(workers)
            or not isinstance(configuration.get("generation"), str)):
        raise RuntimeError("owned stop proof configuration mismatch")
    if proof_directory is None:
        proof_directory = canonical_directory
        if configuration.get("directory") != str(canonical_directory):
            raise RuntimeError("owned stop proof configuration mismatch")
    else:
        proof_directory = pathlib.Path(proof_directory)
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
    the data plane, so its switches are forwarded to workers only. The PERI_DIAG_* switches are
    temporary slow/phase diagnostics wiring; only the slow pair needs
    prepare_slow_diagnostic_directory, while the group-phase switch writes to stderr only.
    """
    for name, value in os.environ.items():
        # Temporary diagnostic forwarding: remove these names and their guard once slow
        # diagnostics become a supported flag or are no longer needed.
        if (name in ("PERI_WORK_METRICS", "PERI_WORKER_DB_IDLE_UNLOAD_MS", "PERI_ADMISSION_TRACE",
                     "PERI_DIAG_SLOW", "PERI_DIAG_DIR", "PERI_DIAG_GROUP_PHASE")
                or (maintenance and name.startswith("PERI_MAINTENANCE_"))):
            environment[name] = value
    return environment


def prepare_slow_diagnostic_directory():
    """Create the slow-diagnostic output directory or reject an unrecordable start.

    Temporary experiment wiring: the Rust sink disables itself silently when PERI_DIAG_DIR
    is missing or unusable, so an enabled diagnostic must fail the launch here instead of
    leaving a campaign without evidence. Remove this guard together with the PERI_DIAG_*
    forwarding once slow diagnostics become a supported flag or are no longer needed.
    """
    if os.environ.get("PERI_DIAG_SLOW") != "1":
        return
    directory = os.environ.get("PERI_DIAG_DIR")
    if not directory:
        raise RuntimeError("PERI_DIAG_SLOW=1 requires PERI_DIAG_DIR to name a private directory")
    try:
        private_directory(pathlib.Path(directory))
    except (OSError, RuntimeError) as error:
        raise RuntimeError("PERI_DIAG_DIR must be a private directory without symlinks: " + directory + ": " + str(error)) from error


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
        self.recovery = getattr(arguments, "recover", False)
        # Set when prepare() refused because the existing failure record failed
        # the operator gate. The record is authoritative even when it is legacy,
        # unsigned or asks for reuse: it must not be replaced by this attempt.
        self.restart_failure_gate_rejected = False
        self.recovered_interrupted_startup = False
        self.verified_reuse_terminal = None
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

    def recover_interrupted_startup(self, configuration):
        """证明式回滚一次未 ready 的启动尝试；只用于显式 --recover。

        不信任 live 的残缺 proof/incarnation：以签名 intent 指向的 preserved
        terminal/stop/proof/state/floor/seal 为准，并要求 live state、operation
        sidecar 和本地 backend 清单都证明这次尝试没有持久化写入。
        """
        path = self.directory / "launcher-startup-attempt.json"
        if not path.exists():
            return False
        attempt = self.read_signed_private(path, configuration, "startup attempt intent")
        if (attempt.get("version") != 1 or attempt.get("generation") != configuration["generation"]
                or attempt.get("kind") != "launcher_startup_attempt"):
            raise RuntimeError("startup attempt identity invalid")
        services = attempt.get("services_started")
        starting = attempt.get("starting")
        order = ["authority", "platform", "worker-a", "worker-b"]
        if not isinstance(services, list) or starting not in (None, *order):
            raise RuntimeError("startup intent service identity is invalid")
        if not services and starting is None:
            return False
        if services:
            if services[0] != "authority" or services != order[:len(services)]:
                raise RuntimeError("startup service order is not recognized")
        elif starting != "authority":
            raise RuntimeError("startup intent has no recoverable service")
        failure_path = self.directory / "launcher-restart-failure.json"
        if failure_path.exists():
            try:
                failure = self.read_signed_private(failure_path, configuration, "launcher restart failure record")
                failed_at = datetime.datetime.fromisoformat(failure.get("failed_at", ""))
                created_at = datetime.datetime.fromisoformat(attempt.get("created_at", ""))
            except (RuntimeError, ValueError, TypeError) as error:
                raise RuntimeError("startup intent and failure record cannot be reconciled") from error
            if (failure.get("teardown_completed") is True
                    and failure.get("services_started") == services
                    and failed_at >= created_at):
                # 同一进程已经处理过这次失败：走既有 recovery 路径，不回滚。
                return False
        if attempt.get("ready") is not False:
            raise RuntimeError("readiness was announced; use normal unclean-stop recovery, not startup rollback")
        staging_text = attempt.get("staging")
        staging_root = (self.directory / "launcher-restart-staging").resolve()
        if not isinstance(staging_text, str) or not staging_text:
            raise RuntimeError("preserved startup snapshot is not recorded")
        staging_candidate = self.directory / staging_text
        staging = staging_candidate.resolve()
        if (staging_candidate.is_symlink() or staging == staging_root
                or staging_root not in staging.parents or not staging.is_dir()):
            raise RuntimeError("preserved startup snapshot path is invalid")
        evidence = attempt.get("evidence")
        if not isinstance(evidence, dict):
            raise RuntimeError("preserved startup evidence is missing")
        terminal_hash = attempt.get("terminal_sha256")
        if (not isinstance(terminal_hash, str)
                or file_sha256_or_none(self.directory / "launcher-terminal.json") != terminal_hash
                or file_sha256_or_none(staging / "launcher-terminal.json") != terminal_hash):
            raise RuntimeError("canonical terminal changed after the interrupted startup")
        if file_sha256_or_none(staging / "launcher-stop.json") != evidence.get("stop_sha256"):
            raise RuntimeError("preserved stop record changed")
        if file_sha256_or_none(self.directory / "launcher-stop.json") != evidence.get("stop_sha256"):
            raise RuntimeError("live stop record changed after the interrupted startup")
        payload = verify_snapshot_terminal(staging, self.directory, configuration)
        for name in ("state_sha256", "state_floor_sha256"):
            if payload.get(name) != evidence.get(name):
                raise RuntimeError("preserved terminal does not bind the preserved snapshot")
        verify_snapshot_stop(staging, configuration)
        verify_owned_stop_proofs(self.directory, configuration, self.launch["workers"],
                                 proof_directory=staging / "owned-proof")
        verify_no_unresolved_operations(self.directory, configuration,
                                        state_path=staging / "platform/state.json")
        if starting == "authority":
            # The spawn window has no persisted PID. Stop only exact recorded
            # argv matches before judging live state; a lookalike decoy is not
            # touched and an unreadable process table fails closed.
            self.stop_spawn_window_processes(attempt)
        elif starting is not None:
            # platform/worker shutdown may materialize the operation view. Do not
            # SIGKILL them here; the operator must stop them gracefully first.
            names = sorted(set([*(services or []), starting]))
            table = self.process_table()
            survivors = [name for name in names if match_recorded_argv(table, recorded_argv(attempt, name))]
            if survivors:
                raise RuntimeError("platform/worker spawn window still has surviving "
                                   + ", ".join(survivors)
                                   + "; send SIGTERM to the surviving platform and authority, wait for their joined exit so the shutdown checkpoint materializes the operation view, then rerun the original configuration with --recover; rebuild only if the checkpoint is still absent after the joined stop; never clear markers or proofs")
        state_unchanged = True
        for relative, digest in (("platform/state.json", evidence.get("state_sha256")),
                                 ("platform/state.json.floor", evidence.get("state_floor_sha256"))):
            if not isinstance(digest, str) or file_sha256_or_none(staging / relative) != digest:
                raise RuntimeError("preserved platform snapshot is incomplete: " + relative)
            if file_sha256_or_none(self.directory / relative) != digest:
                state_unchanged = False
        if state_unchanged:
            if file_sha256_or_none(self.directory / "platform/state.json.floor") != evidence.get("state_floor_sha256"):
                raise RuntimeError("platform state floor changed after the interrupted startup")
        else:
            # 未 ready 前平台可能写入 worker 注册等控制面状态；只有 state/floor
            # 一致、checkpoint 已物化且没有任何未决操作时才接受这类前进。
            verify_no_unresolved_operations(self.directory, configuration)
            try:
                state = json.loads((self.directory / "platform/state.json").read_bytes(), object_pairs_hook=unique_object)["payload"]
                floor = json.loads((self.directory / "platform/state.json.floor").read_bytes(), object_pairs_hook=unique_object)
                if floor.get("version") != 1 or floor.get("revision") != state.get("revision"):
                    raise ValueError("state and floor revisions differ")
            except (OSError, ValueError, TypeError, UnicodeError, KeyError) as error:
                raise RuntimeError("advanced platform state is not durable") from error
        journal = self.directory / "platform/operations/journal"
        if file_sha256_or_none(journal) != evidence.get("journal_sha256"):
            raise RuntimeError("platform operation sidecar changed after the interrupted startup")
        if self.launch["backend"] != "local":
            raise RuntimeError("proof-based startup recovery is limited to the local backend; keep the run isolated and inspect the S3 authority seal")
        seal_name = evidence.get("seal_name")
        if not isinstance(seal_name, str) or not seal_name:
            raise RuntimeError("preserved authority seal is missing")
        root_records = evidence.get("authority_roots")
        if not isinstance(root_records, list):
            raise RuntimeError("preserved authority root snapshot is missing")
        root_names = []
        for record in root_records:
            if (not isinstance(record, dict) or not all(isinstance(record.get(name), str) for name in ("key", "name", "sha256"))
                    or record.get("reliable") is not True
                    or not record["key"].endswith("/authority/root")):
                raise RuntimeError("preserved authority root snapshot is invalid")
            root_names.append(record["name"])
        if len(set(root_names)) != len(root_names):
            raise RuntimeError("preserved authority root snapshot is duplicated")
        if backend_manifest(self.directory / "backend", [seal_name, *root_names]) != evidence.get("backend_manifest_sha256"):
            raise RuntimeError("authority backend changed after the interrupted startup")
        for record in root_records:
            staged_root, staged_payload, staged_reliable = local_object_any(staging / "authority-roots", record["key"])
            if (not staged_reliable or staged_root is None or staged_root.name != record["name"]
                    or file_sha256(staged_root) != record["sha256"]):
                raise RuntimeError("preserved authority root changed")
            live_root, live_payload, live_reliable = local_object_any(self.directory / "backend", record["key"])
            if not live_reliable or live_root is None or live_root.name != record["name"]:
                raise RuntimeError("live authority root changed after the interrupted startup")
            try:
                staged_value = json.loads(staged_payload, object_pairs_hook=unique_object)["root"]
                live_value = json.loads(live_payload, object_pairs_hook=unique_object)["root"]
            except (ValueError, TypeError, UnicodeError, KeyError, RuntimeError) as error:
                raise RuntimeError("authority root payload is invalid") from error
            # ownership/revision 和 lease pin/receipt 是未 ready 启动的控制面
            # 写入；数据引用 blobs/objects/branches 等任何未知字段都不允许变化。
            mutable = {"ownership", "revision", "pins", "pin_entries", "receipts"}
            stable = lambda value: {key: item for key, item in value.items() if key not in mutable}
            if stable(staged_value) != stable(live_value):
                raise RuntimeError("authority data changed after the interrupted startup: " + record["key"])
        staged_seal, staged_payload, staged_reliable = local_object_any(staging / "backend", LOCAL_SEAL_KEY)
        if (not staged_reliable or staged_seal is None or validate_local_seal_payload(staged_payload) is None
                or file_sha256(staged_seal) != evidence.get("seal_sha256")):
            raise RuntimeError("preserved authority seal could not be verified")
        live_seal, live_payload, live_reliable = local_object_any(self.directory / "backend", LOCAL_SEAL_KEY)
        if not live_reliable or live_seal is None:
            raise RuntimeError("live authority seal could not be verified")
        live_seal_hash = file_sha256(live_seal)
        preserved_seal_hash = evidence.get("seal_sha256")
        attempt_seal_hash = attempt.get("authority_seal_sha256")
        authority_involved = "authority" in services or starting == "authority"
        allowed_seal = {preserved_seal_hash}
        if attempt_seal_hash is not None:
            allowed_seal.add(attempt_seal_hash)
        if live_seal_hash not in allowed_seal:
            if (not authority_involved or attempt_seal_hash is not None
                    or attempt.get("seal_before_spawn_sha256") != preserved_seal_hash):
                raise RuntimeError("authority seal changed beyond this interrupted startup")
            preserved_seal = decode_seal_record(staged_payload)
            live_seal_record = decode_seal_record(live_payload)
            if (preserved_seal is None or preserved_seal.get("clean") is not True
                    or live_seal_record is None
                    or live_seal_record["owner"] == preserved_seal["owner"]):
                raise RuntimeError("authority seal changed beyond this interrupted startup")
        marker = self.directory / "authority/authority.lock"
        if marker.is_symlink() or not marker.is_file():
            raise RuntimeError("authority startup marker is missing")
        marker_value = marker.read_bytes()
        if marker_value == b"clean\n":
            # A clean marker is accepted only when it is the preserved state
            # untouched, or when the changed seal proves this attempt reached a
            # clean/graceless stop. The proof below still restores the preserved
            # marker together with the seal/root/proof snapshot.
            expected = {f"{worker}.{kind}.json" for worker in self.launch["workers"] for kind in ("identity", "stop")}
            proofs_match = all(
                file_sha256_or_none(self.directory / "owned-proof" / name)
                == file_sha256_or_none(staging / "owned-proof" / name) for name in expected)
            roots_match = all(
                file_sha256_or_none(self.directory / "backend" / record["name"]) == record["sha256"]
                for record in root_records)
            if not authority_involved or not (
                    live_seal_hash != preserved_seal_hash or proofs_match and roots_match):
                raise RuntimeError("authority marker is clean but live evidence does not match the preserved snapshot")
        elif marker_value != b"dirty\n":
            raise RuntimeError("authority startup marker is neither clean nor the expected dirty marker")
        staged_marker = staging / "authority/authority.lock"
        if (file_sha256_or_none(staged_marker) != evidence.get("authority_lock_sha256")
                or staged_marker.read_bytes() != b"clean\n"):
            raise RuntimeError("preserved authority startup marker is not the verified clean marker")
        survivors = self.surviving_recorded_services(attempt)
        if survivors:
            raise RuntimeError("services from the interrupted startup are still running ("
                               + ", ".join(sorted(survivors))
                               + "); stop and join them with the operator path first")
        verify_addresses_available([self.launch["listen"], self.launch["authority"], self.config["broker_listen"]]
                                   + [worker["address"] for worker in self.launch["workers"].values()])
        return self.restore_startup_snapshot(configuration, path, attempt, staging, evidence, live_seal, staged_seal, state_unchanged, root_records)

    def restore_startup_snapshot(self, configuration, intent_path, attempt, staging, evidence, live_seal, staged_seal, state_unchanged, root_records):
        created = datetime.datetime.now(datetime.timezone.utc)
        root = self.directory / "launcher-startup-recovery"
        private_directory(root)
        destination = root / (created.strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(8))
        private_directory(destination)
        intent_bytes = intent_path.read_bytes()
        intent_hash = hashlib.sha256(intent_bytes).hexdigest()
        private_copy(intent_path, destination / "launcher-startup-attempt.json")
        canonical_proof = self.directory / "owned-proof"
        private_directory(canonical_proof)
        expected = {f"{worker}.{kind}.json" for worker in self.launch["workers"] for kind in ("identity", "stop")}
        live_proofs = {entry.name for entry in canonical_proof.iterdir()}
        if live_proofs - expected:
            raise RuntimeError("live owned proof set has unexpected entries; operator review required")
        private_directory(destination / "attempt-owned-proof")
        for name in sorted(live_proofs):
            private_copy(canonical_proof / name, destination / "attempt-owned-proof" / name)
        for name in sorted(expected):
            atomic_private_bytes(canonical_proof / name, (staging / "owned-proof" / name).read_bytes())
        for record in root_records:
            live_root, _, _ = local_object_any(self.directory / "backend", record["key"])
            atomic_private_bytes(live_root, (staging / "authority-roots" / record["name"]).read_bytes())
        atomic_private_bytes(live_seal, staged_seal.read_bytes())
        atomic_private_bytes(self.directory / "authority/authority.lock", b"clean\n")
        receipt = {"version": 1, "kind": "launcher_startup_recovery",
                   "run_dir": str(self.directory), "generation": configuration["generation"],
                   "created_at": created.isoformat(), "intent_sha256": intent_hash,
                   "staging": str(staging), "services_started": attempt["services_started"],
                   "restored": ["owned-proof", "authority backend seal", "authority roots", "authority clean startup marker"],
                   "terminal_sha256": evidence["terminal_sha256"],
                   "state_sha256": evidence["state_sha256"],
                   "state_floor_sha256": evidence["state_floor_sha256"],
                   "seal_sha256": evidence["seal_sha256"],
                   "backend_manifest_sha256": evidence["backend_manifest_sha256"],
                   "journal_sha256": evidence["journal_sha256"],
                   "state_advanced": not state_unchanged,
                   "live_state_sha256": file_sha256(self.directory / "platform/state.json")}
        atomic_private_json(destination / "receipt.json", signed_owned_identity(configuration, receipt))
        fsync_directory(destination / "attempt-owned-proof")
        fsync_directory(destination)
        fsync_directory(root)
        fsync_directory(self.directory)
        self.startup_intent = {"version": 1, "kind": "launcher_startup_attempt",
                               "generation": configuration["generation"], "ready": False,
                               "services_started": [], "pids": {}, "starting": None, "commands": {},
                               "staging": str(staging.relative_to(self.directory)),
                               "terminal_sha256": evidence["terminal_sha256"],
                               "evidence": evidence, "state_hashes": self.startup_state_hashes(),
                               "recovered_from_sha256": intent_hash,
                               "recovery_directory": str(destination),
                               "state_advanced": not state_unchanged,
                               "created_at": created.isoformat()}
        self.write_startup_intent(configuration)
        if state_unchanged:
            self.verify_clean_evidence()
            self.verify_clean_stop_record(configuration)
            self.verify_startup_attempt(configuration)
        self.recovered_interrupted_startup = True
        return True

    def prepare_recovery(self, configuration):
        """Admit an unclean run for isolation release or a proven startup rollback.

        The normal clean-restart gate stays exactly as it is. A signed startup
        intent plus a complete preserved snapshot may be rolled back to the last
        clean point; if that proof is absent or fails, the isolation-release path
        still requires the owned worker joins and the authority's clean marker.
        A dirty authority without such proof needs its remote terminal proven
        first; starting it here would be the takeover the authority refuses.
        """
        try:
            recovered = self.recover_interrupted_startup(configuration)
        except Exception as error:
            raise RuntimeError("interrupted startup proof recovery refused: " + str(error)
                               + "; operator: send SIGTERM to any surviving platform and authority, wait for their joined exit so the shutdown checkpoint materializes the operation view, retain all markers, proofs and launcher-restart-staging, then rerun the original configuration with --recover; do not delete or rewrite markers, and keep the volume isolated if the preserved proof no longer matches") from error
        if not recovered:
            verify_owned_stop_proofs(self.directory, configuration, self.launch["workers"])
            authority = self.directory / "authority/authority.lock"
            if authority.is_symlink() or not authority.is_file() or authority.read_bytes() != b"clean\n":
                raise RuntimeError("recovery requires a clean authority startup marker; if a signed startup intent and complete preserved proof exist, use --recover to run proof recovery; otherwise retain isolation and inspect the authority")
        try:
            verify_clean_terminal(self.directory, configuration)
            verify_no_unresolved_operations(self.directory, configuration)
            self.verify_clean_stop_record(configuration)
            self.verify_startup_attempt(configuration)
        except Exception:
            pass
        else:
            if not self.restart_failure_gate_rejected and not self.recovered_interrupted_startup:
                raise RuntimeError("run already passes the clean restart gate; start without --recover")
        self.preserve_previous_stop_record(configuration)

    def preserve_previous_stop_record(self, configuration):
        """Keep the stop record that made this run unrecoverable by normal means.

        The recovery instance writes its own stop/terminal when it ends, so the
        original account would otherwise be replaced. The archive is written
        once; later recovery attempts keep the first record, which is the one
        that describes the failure an operator is recovering from.
        """
        path = self.directory / "launcher-stop.json"
        if not path.exists():
            return
        self.read_signed_private(path, configuration, "previous launcher stop record")
        archive = self.directory / "launcher-stop.pre-recovery.json"
        if archive.exists():
            return
        atomic_private_json(archive, json.loads(path.read_bytes(), object_pairs_hook=unique_object))

    def begin_restart_failure(self, error):
        if not self.processes and self.clean_evidence is None:
            # 配置拒绝没有运行副作用，不应制造下次启动无法消除的故障门禁。
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
            "requires_operator": reconciling or bool(self.processes),
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

    def startup_state_hashes(self):
        hashes = {}
        for directory in ("authority", "controller", "platform", "owned-proof"):
            root = self.directory / directory
            if root.exists():
                for path in sorted(root.rglob("*")):
                    if path.is_symlink():
                        raise RuntimeError("startup evidence must not contain symlinks")
                    if path.is_file():
                        hashes[str(path.relative_to(self.directory))] = file_sha256(path)
        if self.launch["backend"] == "local":
            seal, reliable = local_object_path(self.directory / "backend", LOCAL_SEAL_KEY)
            hashes["authority_seal"] = file_sha256(seal) if seal else None
            hashes["authority_seal_reliable"] = reliable
        for name in ("launch.json", "platform.json", "launcher-terminal.json", "launcher-stop.json"):
            hashes[name] = file_sha256(self.directory / name)
        return hashlib.sha256(json.dumps(hashes, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def write_startup_intent(self, configuration, **changes):
        self.startup_intent.update(changes)
        atomic_private_json(self.directory / "launcher-startup-attempt.json",
                            signed_owned_identity(configuration, self.startup_intent))

    def recover_completed_clean_shutdown(self, configuration):
        """Rebuild a clean terminal proof when the shutdown completed but the write was lost.

        A container stop can be killed after the authority wrote its clean marker
        and every service joined, but before mark_clean_terminal wrote the proof.
        The terminal is derived evidence, so a missing one must not force an
        operator to delete the volume when the independent clean evidence agrees.
        Any doubt (dirty marker, unclean or partial stop, unresolved operations,
        failed restart record, pre-ready attempt) keeps the existing refusal.
        """
        terminal = self.directory / "launcher-terminal.json"
        if terminal.is_symlink():
            return False
        if (self.directory / "launcher-restart-failure-unavailable.json").exists():
            return False
        authority = self.directory / "authority/authority.lock"
        if authority.is_symlink() or not authority.is_file() or authority.read_bytes() != b"clean\n":
            return False
        attempt_path = self.directory / "launcher-startup-attempt.json"
        if attempt_path.exists():
            try:
                attempt = self.read_signed_private(attempt_path, configuration, "startup attempt intent")
            except RuntimeError:
                return False
            if (attempt.get("version") != 1 or attempt.get("generation") != configuration["generation"]
                    or attempt.get("kind") != "launcher_startup_attempt" or attempt.get("ready") is not True):
                return False
        stop_path = self.directory / "launcher-stop.json"
        if stop_path.is_symlink() or not stop_path.is_file():
            return False
        try:
            record = self.read_signed_private(stop_path, configuration, "clean launcher stop record")
        except RuntimeError:
            return False
        if (type(record.get("version")) is not int or record.get("version") != 1
                or record.get("generation") != configuration["generation"]):
            return False
        stages = record.get("stages")
        completed = record.get("outcome") == "clean" or (
            record.get("outcome") == "in_progress" and isinstance(stages, list) and bool(stages)
            and all(isinstance(stage, dict) and stage.get("ack") is True
                    and stage.get("exit_code") == 0 and stage.get("signal") is None for stage in stages))
        if not completed:
            return False
        verify_owned_stop_proofs(self.directory, configuration, self.launch["workers"])
        verify_no_unresolved_operations(self.directory, configuration)
        if terminal.exists():
            verify_clean_terminal(self.directory, configuration)
        else:
            atomic_private_json(terminal, signed_owned_identity(configuration, terminal_record(self.directory, configuration)))
        if record.get("outcome") != "clean":
            record["outcome"] = "clean"
            record["reason"] = "terminal_proof_recovered"
            record["unresolved_operations"] = False
            atomic_private_json(stop_path, signed_owned_identity(configuration, record))
        if attempt_path.exists():
            os.replace(attempt_path, self.directory / ("launcher-startup-attempt.completed-" + configuration["generation"] + ".json"))
            fsync_directory(self.directory)
        print(json.dumps({"launcher": "clean_terminal_recovered", "generation": configuration["generation"]}),
              file=sys.stderr, flush=True)
        return True

    def verify_startup_attempt(self, configuration):
        path = self.directory / "launcher-startup-attempt.json"
        if not path.exists():
            return
        attempt = self.read_signed_private(path, configuration, "startup attempt intent")
        if (attempt.get("version") != 1 or attempt.get("generation") != configuration["generation"]
                or attempt.get("kind") != "launcher_startup_attempt"):
            raise RuntimeError("startup attempt identity invalid; operator recovery required")
        # 新 terminal 是正常停机或显式 recovery 的证明，不受旧 attempt 的约束。
        if ((self.directory / "launcher-terminal.json").is_file()
                and file_sha256(self.directory / "launcher-terminal.json") != attempt.get("terminal_sha256")):
            self.verify_clean_evidence()
            self.verify_clean_stop_record(configuration)
            return
        if attempt.get("ready") is not False or attempt.get("services_started") != []:
            raise RuntimeError("interrupted startup may have changed state; operator recovery required: stop and join surviving services, retain all markers, then run --recover for evidence-proven startup rollback if launcher-startup-attempt.json and its preserved snapshot exist; see README release-volume compatibility")
        if attempt.get("state_hashes") != self.startup_state_hashes():
            raise RuntimeError("interrupted startup state changed; operator recovery required; retain markers and startup evidence, stop and join every service, and run --recover only after the preserved snapshot matches")
        self.verify_clean_evidence()
        self.verify_clean_stop_record(configuration)

    def supersede_restart_failure(self, terminal_sha256):
        path = self.directory / "launcher-restart-failure.json"
        if not path.exists():
            return
        if (self.directory / "launcher-restart-failure-unavailable.json").exists():
            raise RuntimeError("restart failure record unavailable; operator recovery required")
        configuration = self.config["owned_stop_proof"]
        previous = self.read_signed_private(path, configuration, "launcher restart failure record")
        if previous.get("requires_operator") is False:
            # 已取代的记录保持字节不变；否则每次正常重启都会重写并新增历史。
            return
        if previous.get("requires_operator") is not True:
            raise RuntimeError("unrecognized restart failure record cannot be superseded")
        history = self.directory / "launcher-restart-failure-history"
        private_directory(history)
        digest = file_sha256(path)
        archive = history / (digest + ".json")
        if not archive.exists():
            private_copy(path, archive)
        fsync_directory(history)
        record = {**previous, "version": 1, "kind": "launcher_restart_failure",
                  "generation": configuration["generation"], "requires_operator": False,
                  "teardown_completed": True, "all_processes_stopped": True,
                  "supersedes_sha256": digest, "superseded_by_terminal_sha256": terminal_sha256,
                  "superseded_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
        atomic_private_json(path, signed_owned_identity(configuration, record))
        self.prune_failure_history(history)

    def prune_failure_history(self, history):
        limit = 32
        entries = [entry for entry in history.iterdir() if entry.is_file() and not entry.is_symlink()]
        if len(entries) <= limit:
            return
        for entry in sorted(entries, key=lambda value: (value.stat().st_mtime_ns, value.name))[:-limit]:
            entry.unlink()
        fsync_directory(history)

    def stage_clean_evidence(self, configuration):
        """复制旧证明；ready 前不移动唯一 canonical 证据。"""
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
        private_directory(staging / "owned-proof")
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
        for proof in (self.directory / "owned-proof").iterdir():
            private_copy(proof, staging / "owned-proof" / proof.name)
        seal_path, seal_reliable = None, True
        if self.launch["backend"] == "local":
            seal_path, seal_reliable = local_object_path(self.directory / "backend", LOCAL_SEAL_KEY)
            if seal_path is not None:
                private_directory(staging / "backend")
                private_copy(seal_path, staging / "backend" / seal_path.name)
        root_records = []
        if self.launch["backend"] == "local":
            state_payload = json.loads(state.read_bytes(), object_pairs_hook=unique_object)["payload"]
            root_records = authority_root_records(state_payload, self.directory / "backend")
            if root_records:
                private_directory(staging / "authority-roots")
                for record in root_records:
                    private_copy(self.directory / "backend" / record["name"], staging / "authority-roots" / record["name"])
        journal = self.directory / "platform/operations/journal"
        manifest = backend_manifest(self.directory / "backend",
                                    ([seal_path.name] if seal_path is not None else [])
                                    + [record["name"] for record in root_records]) if self.launch["backend"] == "local" else None
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
            "journal_sha256": file_sha256_or_none(journal),
            "backend_manifest_sha256": manifest,
            "authority_roots": root_records,
        }
        self.clean_evidence = evidence
        private_copy(terminal, staging / "launcher-terminal.json")
        fsync_directory(staging / "platform")
        fsync_directory(staging / "authority")
        fsync_directory(staging / "owned-proof")
        if seal_path is not None:
            fsync_directory(staging / "backend")
        if root_records:
            fsync_directory(staging / "authority-roots")
        fsync_directory(staging)
        fsync_directory(staging_root)
        fsync_directory(self.directory)
        history = self.directory / "owned-proof-history"
        private_directory(history)
        archive = history / (configuration["generation"] + "-" + secrets.token_hex(8))
        if archive.exists():
            raise RuntimeError("owned stop proof generation already archived")
        private_directory(archive)
        for proof in (self.directory / "owned-proof").iterdir():
            private_copy(proof, archive / proof.name)
        fsync_directory(archive)
        evidence["archive"] = str(archive)
        fsync_directory(history)
        fsync_directory(self.directory)
        self.startup_intent = {"version": 1, "kind": "launcher_startup_attempt",
                               "generation": configuration["generation"], "ready": False,
                               "services_started": [], "pids": {}, "commands": {},
                               "staging": str(staging.relative_to(self.directory)),
                               "terminal_sha256": evidence["terminal_sha256"],
                               "evidence": {name: evidence[name] for name in
                                            ("terminal_sha256", "stop_sha256", "state_sha256", "state_floor_sha256",
                                             "authority_lock_sha256", "seal_name", "seal_sha256",
                                             "journal_sha256", "backend_manifest_sha256", "authority_roots")},
                               "state_hashes": self.startup_state_hashes(),
                               "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
        self.write_startup_intent(configuration)

    def discard_clean_evidence(self):
        evidence = self.clean_evidence
        if evidence is None:
            return
        # ready 后只归档，不销毁旧 terminal；它不能再作为当前运行的证明。
        terminal = self.directory / "launcher-terminal.json"
        if terminal.exists():
            os.replace(terminal, pathlib.Path(evidence["staging"]) / "launcher-terminal.json")
        if hasattr(self, "startup_intent"):
            self.write_startup_intent(self.config["owned_stop_proof"], ready=True)
        fsync_directory(self.directory)
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
            if name == "authority/authority.lock":
                raise RuntimeError("authority marker changed; retain isolation without restoring markers")
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
        if self.recovery and not launch_path.exists():
            raise RuntimeError("recovery requires an existing run directory")
        if launch_path.exists():
            self.restart_attempt = True
            self.launch = normalize_launch(json.loads(launch_path.read_text(), object_pairs_hook=unique_object))
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
            if self.recovery:
                # Recovery is never a way to skip the normal gate when the gate
                # already passes. An operator-required restart-failure record is
                # tolerated here because ending the isolation is the operator
                # action it asks for. Only a later verified clean shutdown may supersede it.
                try:
                    self.verify_restart_failure_gate(configuration)
                except Exception:
                    self.restart_failure_gate_rejected = True
                self.prepare_recovery(configuration)
                return
            try:
                self.verify_restart_failure_gate(configuration)
            except Exception:
                self.restart_failure_gate_rejected = True
                raise
            self.recover_completed_clean_shutdown(configuration)
            self.verify_startup_attempt(configuration)
            verify_owned_stop_proofs(self.directory, configuration, self.launch["workers"])
            verify_clean_terminal(self.directory, configuration)
            # 复用的 clean 语义必须与停机时一致：历史 terminal proof 也是在一个可能滞后于侧车的 payload 上做出的判定。
            verify_no_unresolved_operations(self.directory, configuration)
            self.verify_clean_reuse_gate()
            self.verified_reuse_terminal = file_sha256(self.directory / "launcher-terminal.json")
            self.verify_listener_availability()
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
        # 新卷同样要在写密钥/身份文件前探明监听地址；失败后目录仍可按新卷重试。
        verify_addresses_available([self.arguments.listen, authority, broker]
                                   + [worker["address"] for worker in self.launch["workers"].values()])
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

    def verify_listener_availability(self):
        # 常见端口冲突在 Authority 改变持久状态前拒绝；bind 的竞态仍由启动失败门禁兜底。
        verify_addresses_available([self.launch["listen"], self.launch["authority"], self.config["broker_listen"]]
                                   + [worker["address"] for worker in self.launch["workers"].values()])

    def process_table(self):
        try:
            listing = subprocess.run(["ps", "-ax", "-ww", "-o", "pid=,command="],
                                     capture_output=True, text=True, check=False)
        except OSError as error:
            raise RuntimeError("process table unavailable; stop residual services with the operator") from error
        if listing.returncode != 0:
            raise RuntimeError("process table unavailable; stop residual services with the operator")
        return parse_process_table(listing.stdout)

    def stop_spawn_window_processes(self, attempt):
        """Boundedly stop only exact recorded argv matches for the Authority spawn window.

        A PID written before SIGKILL is authoritative only when its current
        command is byte-equal to the argv recorded in the signed intent. PID
        reuse or a lookalike decoy cannot make us kill an unrelated process.
        """
        names = list(attempt.get("services_started") or [])
        starting = attempt.get("starting")
        if starting is not None:
            names.append(starting)
        names = sorted(set(names))
        if not names:
            return
        pids = set()
        for name in names:
            pids |= match_recorded_argv(self.process_table(), recorded_argv(attempt, name))
        for pid in sorted(pids):
            try:
                # These processes belong to an unready attempt whose durable state
                # is not trusted; a graceful shutdown could write new checkpoints.
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + SPAWN_STOP_FINALIZE_SECONDS
        alive = {pid for pid in pids if process_is_running(pid)}
        while alive and time.monotonic() < deadline:
            time.sleep(0.05)
            alive = {pid for pid in alive if process_is_running(pid)}
        if alive:
            raise RuntimeError("residual startup services could not be joined: " + ", ".join(str(pid) for pid in sorted(alive)))
        remaining = self.process_table()
        for name in names:
            if match_recorded_argv(remaining, recorded_argv(attempt, name)):
                raise RuntimeError("residual " + name + " process survived bounded stop")

    def surviving_recorded_services(self, attempt):
        table = self.process_table()
        surviving = set()
        for name, pid in (attempt.get("pids") or {}).items():
            if pid_matches_recorded_argv(table, pid, recorded_argv(attempt, name)):
                surviving.add(name)
        return surviving

    def start(self, name, command, environment):
        if name in self.owned_identities:
            raise RuntimeError("owned worker incarnation cannot be reused")
        path = self.directory / "logs" / (name + ".log")
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        log = os.fdopen(descriptor, "w")
        self.logs.append(log)
        if hasattr(self, "startup_intent"):
            nonce = secrets.token_hex(16)
            commands = {**self.startup_intent.get("commands", {}), name: list(command)}
            changes = {"starting": name, "start_nonce": nonce, "commands": commands}
            if name == "authority" and self.launch["backend"] == "local":
                seal, _, reliable = local_object_any(self.directory / "backend", LOCAL_SEAL_KEY)
                changes["seal_before_spawn_sha256"] = file_sha256(seal) if reliable and seal is not None else None
            self.write_startup_intent(self.config["owned_stop_proof"], **changes)
        process = subprocess.Popen(command, env=environment, stdout=subprocess.PIPE, stderr=log, text=True, start_new_session=True)
        self.processes[name] = process
        if hasattr(self, "startup_intent"):
            pids = {**self.startup_intent.get("pids", {}), name: process.pid}
            self.write_startup_intent(self.config["owned_stop_proof"],
                                      services_started=[*self.startup_intent["services_started"], name],
                                      pids=pids, starting=None)
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
        if hasattr(self, "startup_intent") and name == "authority":
            seal, _, reliable = (local_object_any(self.directory / "backend", LOCAL_SEAL_KEY)
                                 if self.launch["backend"] == "local" else (None, None, False))
            self.write_startup_intent(self.config["owned_stop_proof"],
                                      authority_seal_sha256=file_sha256(seal) if reliable and seal is not None else None,
                                      authority_seal_reliable=reliable)
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
        # The exclusive launcher lock proves no live launcher owns a leftover
        # socket from a killed attempt; never remove a regular file here.
        path = self.directory / "owned-stop.sock"
        if path.is_socket():
            path.unlink()
        elif path.exists():
            raise RuntimeError("owned stop socket path is not a socket")
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

    def run_recovery(self):
        """Start only the services the release needs, with SQL disabled.

        No worker is started and no reconcile request is sent. The workers this
        run owned are stopped and their owned proofs are read from disk; placing
        them back in service before the release would destroy the fencing
        evidence the release has to verify.
        """
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
        platform_environment["PERI_SQL_OPEN_OBSERVER"] = "0"
        platform_environment["PERI_HRANA_EXPERIMENTAL"] = "0"
        platform_environment["PERI_RECOVERY_ONLY"] = "1"
        platform_environment["PERI_SHUTDOWN_STAGE_CAP_MS"] = str(SHUTDOWN_STAGE_CAPS["platform"] * 1000)
        platform_environment["PERI_SHUTDOWN_OUTER_GRACE_MS"] = str(ORCHESTRATION_GRACE_SECONDS * 1000)
        self.start("platform", [str(self.go), "-config", str(self.directory / "platform.json")], platform_environment)
        scheme = "https://" if self.arguments.tls else "http://"
        address = self.arguments.listen.replace("0.0.0.0:", "127.0.0.1:")
        endpoint = scheme + address
        certificate = self.launch.get("ca_file") or self.config.get("tls_certificate")
        connection_path = self.directory / "connection.json"
        if connection_path.exists():
            connection = json.loads(connection_path.read_text())
            # status stays readable while the namespace is isolated and proves the
            # management plane, the credential and the database identity are the
            # ones the operator is about to recover.
            request(endpoint, connection["project_api_key"], "/v1/databases/status", {"database": connection["database"]}, certificate)
        # From here on the operator may retire local resources and release the
        # isolation; those are durable writes, so an exit is no longer a startup
        # failure that could restore earlier evidence.
        self.phase = "recovery"
        print(json.dumps({"ready": True, "service": "recovery", "recovery": True, "sql": "disabled", "hrana": "disabled", "address": endpoint, "connection_file": str(connection_path)}), flush=True)
        while not self.stop.wait(0.2):
            if any(self.processes[name].poll() is not None for name in ("platform", "authority")):
                raise RuntimeError("a control-plane service stopped; inspect private logs")

    def run(self):
        self.acquire_run_lock()
        self.prepare()
        if self.recovery:
            self.run_recovery()
            return
        # Fail closed before any Rust process starts: an unusable diagnostic directory
        # would otherwise disable the sink without failing the launch.
        prepare_slow_diagnostic_directory()
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
        # The phase knobs belong to the profile. Forward only explicit values, and only when
        # the profile is on; the platform keeps its documented defaults for names not set.
        if getattr(self.arguments, "sql_open_observer", False):
            for name in ("PERI_SQL_OPEN_OBSERVER_PHASE_THRESHOLD_MS", "PERI_SQL_OPEN_OBSERVER_PHASE_SAMPLE_PERMILLE",
                         "PERI_SQL_OPEN_OBSERVER_PHASE_WINDOW_MS", "PERI_SQL_OPEN_OBSERVER_PHASE_CAPACITY"):
                if name in os.environ:
                    platform_environment[name] = os.environ[name]
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
        terminal_sha256 = self.verified_reuse_terminal
        self.phase = "running"
        print(json.dumps({"ready": True, "service": "local-project", "address": endpoint, "connection_file": str(connection_path), "topology_file": str(topology_path)}), flush=True)
        if terminal_sha256 is not None:
            self.supersede_restart_failure(terminal_sha256)
        self.discard_clean_evidence()
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
            if set(self.processes) != ({"authority", "platform"} if self.recovery else {"authority", "platform", "worker-a", "worker-b"}):
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
            if self.recovery and self.phase == "recovery":
                self.verify_clean_evidence()
                self.verify_clean_stop_record(configuration)
                self.supersede_restart_failure(file_sha256(self.directory / "launcher-terminal.json"))
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
    parser.add_argument("--recover", action="store_true", help="start an unclean run with SQL disabled, only to release an isolation")
    arguments = parser.parse_args()
    if arguments.recover and arguments.stop_worker:
        print("--recover and --stop-worker are separate operator actions", file=sys.stderr)
        return 2
    if arguments.stop_worker:
        try:
            stop_owned_worker(pathlib.Path(arguments.run_dir).resolve(), arguments.stop_worker)
        except Exception:
            print("owned worker stop unavailable; retain isolation", file=sys.stderr)
            return 1
        print(json.dumps({"stopped": True, "remote_terminal": "not_proven", "recovery": "isolation_release_or_new_database_required"}))
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
