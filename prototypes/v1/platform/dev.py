import argparse
import errno
import fcntl
import hashlib
import hmac
import json
import os
import pathlib
import queue
import secrets
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
            atomic_private_json(self.directory / "launcher-stop.json", signed_owned_identity(configuration, self.stop_record))

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
            verify_owned_stop_proofs(self.directory, configuration, self.launch["workers"])
            verify_clean_terminal(self.directory, configuration)
            # 复用的 clean 语义必须与停机时一致：历史 terminal proof 也是在一个可能滞后于侧车的 payload 上做出的判定。
            verify_no_unresolved_operations(self.directory, configuration)
            marker = self.directory / "launcher-terminal.json"
            marker.unlink()
            descriptor = os.open(self.directory, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            history = self.directory / "owned-proof-history"
            private_directory(history)
            archive = history / (configuration["generation"] + "-" + secrets.token_hex(8))
            if archive.exists():
                raise RuntimeError("owned stop proof generation already archived")
            (self.directory / "owned-proof").rename(archive)
            private_directory(self.directory / "owned-proof")
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
        try:
            project.cleanup()
            if failure is None:
                project.mark_clean_terminal()
            else:
                project.write_stop_record("unclean", "launcher_failed_before_cleanup")
        except Exception as error:
            failure = error
        finally:
            project.release_run_lock()
    if failure:
        print("project stopped: " + str(failure), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
