import argparse
import hashlib
import json
import os
import pathlib
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import time


LABEL = "peri.verify-image.owner"
S3_NAMES = ("S3_ENDPOINT", "S3_BUCKET", "S3_REGION", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY", "S3_SESSION_TOKEN")
ROOT = pathlib.Path(__file__).resolve().parents[3]
SMOKE = ROOT / "prototypes/v1/platform/tests/whole-smoke.py"
CHECKS = ["native_encrypted_sql_ack", "exact_int64", "explicit_rollback", "integrity", "checkpoint",
          "snapshot_restore", "stale_token_denial", "native_readonly_clone", "native_readonly_write_denial", "migration_epoch",
          "suspend_status_resume", "actual_native_metrics"]
REQUIRED_EVIDENCE = ("smoke-process.json", "smoke.json", "authority.lock", "result.json")


def private_json(path, value):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as output:
        json.dump(value, output, indent=2)
        output.flush()
        os.fsync(output.fileno())


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class Verification:
    def __init__(self, arguments):
        self.arguments = arguments
        self.directory = pathlib.Path(tempfile.mkdtemp(prefix="peri-image-verify-")).resolve()
        self.fixture = self.directory / "private"
        self.fixture.mkdir(mode=0o700)
        self.owner = secrets.token_hex(16)
        self.name = "peri-verify-" + self.owner
        self.volume = self.name + "-data"
        self.attempted_volume = False
        self.container = None
        self.attempted_create = False
        self.smoke_dispatched = False
        self.smoke_proven = False
        self.report = {"pass": False, "backend": arguments.backend, "container_name": self.name, "volume_name": self.volume, "status": "Unknown", "cleanup": "not_created"}
        self.environment = {name: os.environ[name] for name in ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG", "XDG_RUNTIME_DIR", "TMPDIR") if name in os.environ}
        if arguments.backend == "s3":
            for name in S3_NAMES:
                if name in os.environ:
                    self.environment[name] = os.environ[name]

    def command(self, command, timeout=30, check=True):
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout, env=self.environment)
        if check and result.returncode != 0:
            raise RuntimeError("command failed; private diagnostics withheld")
        return result

    def docker(self, *arguments, **options):
        return self.command(["docker", *arguments], **options)

    def inspect(self):
        value = json.loads(self.docker("container", "inspect", self.container or self.name).stdout)[0]
        if value["Config"].get("Labels", {}).get(LABEL) != self.owner or value["Name"] != "/" + self.name:
            raise RuntimeError("container ownership not proven")
        if self.container and value["Id"] != self.container:
            raise RuntimeError("container identity changed")
        self.container = value["Id"]
        return value

    def copy(self, source, destination):
        self.docker("cp", self.container + ":" + source, str(destination))
        destination.chmod(0o600)

    def prepare(self):
        if self.arguments.backend == "s3":
            for name in ("S3_ENDPOINT", "S3_BUCKET", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY"):
                if not self.environment.get(name):
                    raise RuntimeError("required S3 environment missing")
        image = json.loads(self.docker("image", "inspect", self.arguments.image).stdout)[0]
        if image.get("Os") != "linux" or image["Config"].get("User") != "10001:10001":
            raise RuntimeError("Linux image with expected non-root identity required")
        self.report["image"] = {name: image.get(name) for name in ("Id", "RepoDigests", "Os", "Architecture")}
        self.report["image"]["User"] = image["Config"]["User"]
        self.attempted_volume = True
        self.docker("volume", "create", "--label", LABEL + "=" + self.owner, self.volume)
        command = ["create", "--name", self.name, "--label", LABEL + "=" + self.owner,
                   "--pull", "never", "--restart", "no", "--memory", "2g", "--cpus", "2", "--pids-limit", "128",
                   "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--publish", "127.0.0.1::9443",
                   "--mount", "type=volume,source=" + self.volume + ",target=/data/peri"]
        if self.arguments.backend == "s3":
            for name in S3_NAMES:
                if name in self.environment:
                    command.extend(["--env", name])
        if getattr(self.arguments, "capacity_trace", False):
            command.extend(["--env", "PERI_CAPACITY_TRACE=1"])
        command.extend([image["Id"], "--run-dir", "/data/peri", "--backend", self.arguments.backend,
                        "--listen", "0.0.0.0:9443", "--tls", "--no-build"])
        if getattr(self.arguments, "writer_admission", False):
            command.append("--writer-admission")
        self.report["writer_admission"] = bool(getattr(self.arguments, "writer_admission", False))
        self.report["capacity_trace"] = bool(getattr(self.arguments, "capacity_trace", False))
        self.attempted_create = True
        self.docker(*command)
        value = self.inspect()
        if value["Image"] != image["Id"] or value["Config"]["User"] != "10001:10001":
            raise RuntimeError("created image identity mismatch")
        self.report["resources"] = {name: value["HostConfig"].get(name) for name in ("Memory", "NanoCpus", "PidsLimit", "RestartPolicy", "CapDrop", "SecurityOpt")}
        self.docker("start", self.container)

    def ready(self):
        deadline = time.monotonic() + self.arguments.ready_timeout
        while time.monotonic() < deadline:
            value = self.inspect()
            if not value["State"]["Running"]:
                raise RuntimeError("container exited before readiness")
            logs = self.docker("logs", self.container)
            if any(self.ready_event(line) for line in logs.stdout.splitlines()):
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("readiness timeout")
        uid = self.docker("exec", self.container, "id", "-u").stdout.strip()
        if uid != "10001":
            raise RuntimeError("runtime UID mismatch")
        launcher = self.fixture / "image-dev.py"
        self.copy("/app/prototypes/v1/platform/dev.py", launcher)
        self.report["image_launcher_sha256"] = digest(launcher)
        if digest(launcher) != digest(SMOKE.parents[1] / "dev.py"):
            raise RuntimeError("image launcher differs from current full launcher")
        ports = value["NetworkSettings"]["Ports"]["9443/tcp"]
        if len(ports) != 1 or ports[0]["HostIp"] != "127.0.0.1":
            raise RuntimeError("loopback publication required")
        self.report["runtime"] = {"uid": uid, "pid": value["State"]["Pid"], "port": ports[0]["HostPort"]}
        connection = self.fixture / "connection.json"
        self.copy("/data/peri/connection.json", connection)
        config = json.loads(connection.read_text())
        if config["ca_file"] != "/data/peri/keys/dev-ca.crt":
            raise RuntimeError("generated issuer CA required")
        ca = self.fixture / "ca.crt"
        self.copy(config["ca_file"], ca)
        leaf = self.fixture / "tls.crt"
        self.copy("/data/peri/keys/gateway.crt", leaf)
        self.command(["openssl", "verify", "-CAfile", str(ca), "-purpose", "sslserver", "-verify_ip", "127.0.0.1", str(leaf)])
        if ca.read_bytes() == leaf.read_bytes():
            raise RuntimeError("separate CA and leaf required")
        config["endpoint"] = "https://127.0.0.1:" + ports[0]["HostPort"]
        config["ca_file"] = str(ca)
        connection.unlink()
        private_json(connection, config)
        self.report["tls"] = "issuer_and_loopback_verified"

    @staticmethod
    def ready_event(line):
        try:
            event = json.loads(line)
            return isinstance(event, dict) and event.get("ready") is True and event.get("service") == "local-project"
        except ValueError:
            return False

    def smoke(self):
        output = self.directory / "smoke.json"
        self.smoke_dispatched = True
        self.report["sql_outcome"] = "Unknown"
        try:
            result = self.command([sys.executable, str(SMOKE), "--connection", str(self.fixture / "connection.json"),
                                   "--output", str(output)], timeout=self.arguments.smoke_timeout, check=False)
        except subprocess.TimeoutExpired as error:
            self.save_process(None, error.stdout or "", error.stderr or "", timed_out=True)
            raise
        self.save_process(result.returncode, result.stdout, result.stderr)
        self.report["smoke_returncode"] = result.returncode
        if result.returncode != 0:
            raise RuntimeError("smoke failed; outcome Unknown; no retry or additional Close")
        self.validate_smoke(json.loads(output.read_text()), result.stdout)
        self.smoke_proven = True
        self.report["sql_outcome"] = "smoke_proven"

    def validate_smoke(self, result, stdout):
        if not isinstance(result, dict) or set(result) != {"pass", "backend", "tls", "checks", "metrics", "database"}:
            raise RuntimeError("incomplete smoke result schema")
        if result["pass"] is not True or result["backend"] != "external_s3_fixture" or result["tls"] != "certificate_verified" or result["checks"] != CHECKS:
            raise RuntimeError("required smoke checks or verified TLS missing")
        acknowledgement = json.loads(stdout)
        if acknowledgement != {"pass": True, "checks": CHECKS, "credentials_logged": False} or acknowledgement["pass"] is not True or acknowledgement["credentials_logged"] is not False:
            raise RuntimeError("smoke acknowledgement missing")
        config = json.loads((self.fixture / "connection.json").read_text())
        if result["database"] != config["database"] or result["database"] != {"namespace": {"tenant": "local_default", "database": "demo"}, "branch": "main"}:
            raise RuntimeError("smoke database identity mismatch")
        metrics = result["metrics"]
        if not isinstance(metrics, dict) or not isinstance(metrics.get("native"), dict) or not isinstance(metrics.get("errors_total"), dict):
            raise RuntimeError("native metrics schema missing")
        native = metrics["native"]
        authority = native.get("authority")
        workers = native.get("workers")
        if not isinstance(authority, dict) or not isinstance(authority.get("put"), dict):
            raise RuntimeError("native authority put evidence missing")
        total = authority["put"].get("total")
        if type(total) is not int or total <= 0 or not isinstance(workers, dict) or set(workers) != {"worker-a", "worker-b"}:
            raise RuntimeError("native metrics proof missing")
        if any(not isinstance(worker, dict) or "status" in worker for worker in workers.values()):
            raise RuntimeError("native worker metrics unavailable")
        audit_errors = metrics["errors_total"].get("audit_unavailable", 0)
        if type(audit_errors) is not int or audit_errors != 0:
            raise RuntimeError("audit availability proof missing")

    def save_process(self, returncode, stdout, stderr, timed_out=False):
        config = json.loads((self.fixture / "connection.json").read_text())
        credentials = [config["project_api_key"], config["global_api_key"]]
        credentials.extend(self.environment.get(name, "") for name in ("S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY", "S3_SESSION_TOKEN"))
        streams = []
        for stream in (stdout, stderr):
            if isinstance(stream, bytes):
                stream = stream.decode("utf-8", errors="replace")
            for credential in credentials:
                if credential:
                    stream = stream.replace(credential, "[REDACTED]")
            streams.append(stream)
        private_json(self.directory / "smoke-process.json", {"returncode": returncode, "stdout": streams[0], "stderr": streams[1], "timed_out": timed_out})

    def remove_volume(self):
        value = json.loads(self.docker("volume", "inspect", self.volume).stdout)[0]
        if value["Name"] != self.volume or value.get("Labels", {}).get(LABEL) != self.owner:
            raise RuntimeError("volume ownership not proven")
        self.docker("volume", "rm", self.volume)

    def cleanup(self):
        if not self.attempted_create and not self.attempted_volume:
            shutil.rmtree(self.fixture)
            return
        self.report["cleanup"] = "retained_unknown"
        try:
            if not self.attempted_create:
                self.remove_volume()
                self.report["cleanup"] = "removed_owned_volume"
                shutil.rmtree(self.fixture)
                return
            value = self.inspect()
            if value["State"]["Running"]:
                self.docker("kill", "--signal", "TERM", self.container)
                self.docker("wait", self.container, timeout=self.arguments.stop_timeout)
            value = self.inspect()
            state = value["State"]
            self.report["stopped_state"] = {name: state.get(name) for name in ("Status", "Running", "ExitCode", "OOMKilled", "Pid", "Dead", "StartedAt", "FinishedAt")}
            if state["Running"] or state["Pid"] != 0 or state["ExitCode"] != 0 or state["OOMKilled"] or state.get("Dead") or state["Status"] != "exited":
                raise RuntimeError("joined clean stop not proven")
            marker = self.directory / "authority.lock"
            self.copy("/data/peri/authority/authority.lock", marker)
            self.report["authority_clean"] = marker.read_bytes() == b"clean\n"
            if not self.report["authority_clean"]:
                raise RuntimeError("authority clean marker missing")
            if self.smoke_dispatched and not self.smoke_proven:
                self.report["retention_reason"] = "SQL outcome Unknown; clean process stop is not SQL resolution"
                return
            self.docker("rm", self.container)
            self.remove_volume()
            self.report["cleanup"] = "removed_owned_container"
            shutil.rmtree(self.fixture)
        except Exception as error:
            self.report["cleanup_error_type"] = type(error).__name__

    def run(self):
        try:
            self.report["stage"] = "prepare"
            self.prepare()
            self.report["stage"] = "ready"
            self.ready()
            self.report["stage"] = "smoke"
            self.smoke()
            self.report["status"] = "smoke_passed"
        except BaseException as error:
            self.report["failure_type"] = type(error).__name__
        finally:
            self.cleanup()
            eligible = self.report["status"] == "smoke_passed" and self.report["cleanup"] == "removed_owned_container"
            self.report["pass"] = False
            self.report["status"] = "Unknown"
            try:
                self.finalize(eligible)
            except BaseException as error:
                self.report["evidence_error_type"] = type(error).__name__
                self.report["pass"] = False
                self.report["status"] = "Unknown"
        return self.report["pass"]

    def finalize(self, eligible):
        result_path = self.directory / "result.json"
        private_json(result_path, self.report)
        evidence = [self.directory / name for name in REQUIRED_EVIDENCE] if eligible else [
            path for path in self.directory.iterdir() if path.is_file()]
        for path in evidence:
            if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
                raise RuntimeError("required private evidence missing")
        candidate = dict(self.report, **{"pass": eligible, "status": "verified" if eligible else "Unknown"})
        pending = self.directory / ".result.pending.json"
        try:
            private_json(pending, candidate)
            artifacts = {path.name: digest(pending if path == result_path else path) for path in evidence}
            source_paths = (pathlib.Path(__file__), SMOKE, SMOKE.parents[1] / "dev.py", ROOT / "prototypes/v1/deploy/Dockerfile")
            sources = {str(path.relative_to(ROOT)): digest(path) for path in source_paths}
            manifest = {"sha256": artifacts, "source_sha256": sources, "credentials_in_manifest": False}
            manifest_path = self.directory / "manifest.json"
            private_json(manifest_path, manifest)
            if json.loads(manifest_path.read_text()) != manifest:
                raise RuntimeError("manifest write verification failed")
            for path in evidence:
                if digest(pending if path == result_path else path) != artifacts[path.name]:
                    raise RuntimeError("evidence hash changed")
            for path in source_paths:
                if digest(path) != sources[str(path.relative_to(ROOT))]:
                    raise RuntimeError("source hash changed")
            os.replace(pending, result_path)
            self.report = candidate
        except BaseException:
            pending.unlink(missing_ok=True)
            raise


def interrupted(signum, frame):
    raise KeyboardInterrupt()


def main():
    parser = argparse.ArgumentParser(description="Verify an existing Linux image without building or pulling")
    parser.add_argument("--image", default=os.environ.get("IMAGE"))
    parser.add_argument("--backend", choices=("local", "s3"), default="local")
    parser.add_argument("--writer-admission", action="store_true")
    parser.add_argument("--capacity-trace", action="store_true")
    parser.add_argument("--ready-timeout", type=int, default=120)
    parser.add_argument("--smoke-timeout", type=int, default=300)
    parser.add_argument("--stop-timeout", type=int, default=90)
    arguments = parser.parse_args()
    if not arguments.image or min(arguments.ready_timeout, arguments.smoke_timeout, arguments.stop_timeout) <= 0:
        parser.error("--image (or IMAGE) and positive timeouts required")
    verification = Verification(arguments)
    signal.signal(signal.SIGTERM, interrupted)
    passed = verification.run()
    print(json.dumps({"pass": passed, "results": str(verification.directory), "cleanup": verification.report["cleanup"],
                      "stage": verification.report.get("stage"), "failure_type": verification.report.get("failure_type")}))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
