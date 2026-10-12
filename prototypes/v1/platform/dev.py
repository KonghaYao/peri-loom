import argparse
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


def verify_owned_stop_proofs(run_directory, configuration, workers):
    """Inspect historical evidence only; this does not authorize a restart."""
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
    with socket.socket(socket.AF_UNIX) as client:
        client.settimeout(40)
        client.connect(str(run_directory / "owned-stop.sock"))
        client.sendall(json.dumps({"worker": worker}).encode() + b"\n")
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
    return environment


def request(endpoint, credential, path, body, certificate=None):
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
            incoming = client.open(outgoing, timeout=30)
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

    def prepare(self):
        observer = getattr(self.arguments, "sql_open_observer", False)
        if observer and (pathlib.Path(self.arguments.run_dir).is_symlink() or self.directory.is_symlink() or self.directory.exists() and (not self.directory.is_dir() or any(self.directory.iterdir()))):
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
            if self.arguments.backend == "s3" and self.launch.get("storage_identity") != storage_identity("s3"):
                raise RuntimeError("existing S3 storage identity cannot be silently rebound")
            self.config = json.loads((self.directory / "platform.json").read_text())
            if self.config.get("writer_admission", False) != self.arguments.writer_admission:
                raise RuntimeError("writer admission configuration mismatch")
            configuration = self.config.get("owned_stop_proof")
            if configuration:
                verify_owned_stop_proofs(self.directory, configuration, self.launch["workers"])
            raise RuntimeError("owned worker incarnation cannot be relaunched; use a fresh fixture")
        authority = free_address()
        broker = free_address()
        self.launch = {"backend": self.arguments.backend, "storage_identity": storage_identity(self.arguments.backend), "listen": self.arguments.listen, "tls": self.arguments.tls, "authority": authority, "workers": {}}
        self.launch["writer_admission"] = self.arguments.writer_admission
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
            "bootstrap_key": secrets.token_hex(32), "gateway_listen": self.arguments.listen, "broker_listen": broker,
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
        if process.poll() is None:
            process.terminate()
        try:
            exit_code = process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            exit_code = process.wait(timeout=10)
        self.record_joined_worker_stop(worker, exit_code)

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
        self.prepare()
        authority_environment = safe_environment(capacity_trace=True)
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
        platform_environment["PERI_SQL_OPEN_OBSERVER"] = "1" if getattr(self.arguments, "sql_open_observer", False) else "0"
        platform_environment["PERI_HRANA_EXPERIMENTAL"] = "1" if os.environ.get("PERI_HRANA_EXPERIMENTAL") == "1" else "0"
        self.start("platform", [str(self.go), "-config", str(self.directory / "platform.json")], platform_environment)
        for identity, worker in self.launch["workers"].items():
            environment = safe_environment(capacity_trace=True)
            environment.update({"PERI_WORKER_AUTHORITY_TOKEN": worker["authority_token"], "PERI_WORKER_CONTROL_TOKEN": worker["control_token"], "PERI_WORKER_SQL_TOKEN": worker["sql_token"], "PERI_WORKER_KEY_TOKEN": worker["key_token"], "PERI_KMS_ENDPOINT": "http://" + self.config["broker_listen"]})
            environment["PERI_WORKER_WRITER_PROFILE"] = "1" if self.arguments.writer_admission else "0"
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

    def cleanup(self):
        if self.maintenance is not None:
            self.maintenance.close()
            (self.directory / "owned-stop.sock").unlink(missing_ok=True)
        failures = []
        platform = self.processes.get("platform")
        if platform:
            if platform.poll() is None:
                platform.terminate()
                try:
                    if platform.wait(timeout=15) != 0:
                        failures.append("platform exited without a clean proof")
                except subprocess.TimeoutExpired:
                    failures.append("platform shutdown incomplete")
                    platform.kill()
                    platform.wait()
            elif platform.returncode != 0:
                failures.append("platform exited without a clean proof")
        for name in ("worker-a", "worker-b", "authority"):
            process = self.processes.get(name)
            if not process:
                continue
            if process.poll() is not None:
                if process.returncode != 0:
                    failures.append(name + " exited before a joined clean proof")
                continue
            shutdown_acknowledged = True
            try:
                if name == "authority":
                    address, token, path = self.launch["authority"], self.config["controller"]["authority_token"], "/"
                else:
                    worker = self.launch["workers"][name]
                    address, token, path = worker["address"], worker["control_token"], "/control"
                request("http://" + address, token, path, {"version": 1, "command": {"command": "shutdown"}})
            except RuntimeError:
                shutdown_acknowledged = False
                failures.append(name + " shutdown response incomplete")
            try:
                result = process.wait(timeout=20)
                if result != 0:
                    failures.append(name + " exited without a clean proof")
                elif shutdown_acknowledged and name in self.owned_identities:
                    try:
                        self.record_joined_worker_stop(name, result)
                    except (OSError, RuntimeError, ValueError):
                        failures.append(name + " owned stop proof unavailable")
            except subprocess.TimeoutExpired:
                failures.append(name + " still running; killed and dirty restart must be reconciled")
                process.kill()
                process.wait()
        for process in self.processes.values():
            if process.stdout is not None:
                process.stdout.close()
        for log in self.logs:
            log.close()
        if failures:
            raise RuntimeError("; ".join(failures))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", default=".run/v1")
    parser.add_argument("--backend", choices=("local", "s3"), default="local")
    parser.add_argument("--listen", default="127.0.0.1:9443")
    parser.add_argument("--tls", action="store_true")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--writer-admission", action="store_true")
    parser.add_argument("--sql-open-observer", action="store_true")
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
    signal.signal(signal.SIGTERM, lambda signum, frame: project.stop.set())
    signal.signal(signal.SIGINT, lambda signum, frame: project.stop.set())
    failure = None
    try:
        project.run()
    except Exception as error:
        failure = error
    finally:
        try:
            project.cleanup()
        except Exception as error:
            failure = error
    if failure:
        print("project stopped: " + str(failure), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
