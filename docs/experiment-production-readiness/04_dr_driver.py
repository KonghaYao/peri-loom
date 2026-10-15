#!/usr/bin/env python3
"""One-shot, isolated S3 disaster-recovery rehearsal. Retains all evidence/resources.

Requires locally cached digest-pinned app and RustFS images. No image is pulled.
The output directory is private and must not be copied into a public report.
"""
import argparse
import datetime as dt
import hashlib
import hmac
import importlib.util
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[2]
DEV = ROOT / "prototypes/v1/platform/dev.py"
LABEL = "peri.dr.owner"
DEFAULT_APP = "ghcr.io/konghayao/peri-loom@sha256:901f37f2cceb62bf265072e5b0159fa86974af3440a79383ba31e351c0895333"
DEFAULT_S3 = "rustfs/rustfs@sha256:1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c"


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def save(path, value):
    with path.open("x", encoding="utf-8") as output:
        json.dump(value, output, indent=2, sort_keys=True)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    path.chmod(0o600)


class Rehearsal:
    def __init__(self, args):
        self.args = args
        self.owner = secrets.token_hex(16)
        self.prefix = "peri-dr-" + self.owner
        self.bucket = self.prefix
        self.directory = Path(tempfile.mkdtemp(prefix=self.prefix + "-", dir=args.private_parent)).resolve()
        self.directory.chmod(0o700)
        self.network = self.prefix
        self.endpoint = "http://rustfs:9000"
        self.access = "dr" + secrets.token_hex(12)
        self.secret = secrets.token_urlsafe(36)
        self.report = {"status": "PARTIAL", "pass": False, "owner": self.owner,
                       "bucket": self.bucket, "stage": "initialized", "events": [],
                       "resources_retained": True, "rpo_seconds": None, "rto_seconds": None}
        self.ids = {}
        self.images = {}
        self.ready_counts = {}
        self.spec = importlib.util.spec_from_file_location("peri_dr_dev", DEV)
        self.dev = importlib.util.module_from_spec(self.spec)
        self.spec.loader.exec_module(self.dev)
        self.env = {k: os.environ[k] for k in ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG", "XDG_RUNTIME_DIR", "TMPDIR") if k in os.environ}

    def event(self, stage, **fields):
        self.report["stage"] = stage
        self.report["events"].append({"stage": stage, "utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                                      "monotonic_ns": time.monotonic_ns(), **fields})
        self.flush()

    def flush(self):
        temporary = self.directory / ("result-" + secrets.token_hex(8) + ".json")
        save(temporary, self.report)
        os.replace(temporary, self.directory / "result.json")

    def command(self, argv, timeout=120):
        try:
            result = subprocess.run(argv, env=self.env, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise RuntimeError("command timed out; outcome unknown") from None
        if result.returncode:
            raise RuntimeError("command failed; diagnostics withheld; outcome unknown")
        return result.stdout.strip()

    def docker(self, *args, timeout=120):
        return self.command(["docker", *args], timeout=timeout)

    def inspect(self, kind, suffix):
        name = self.prefix + "-" + suffix
        value = json.loads(self.docker(kind, "inspect", name))[0]
        if kind == "container":
            owned = value.get("Name", "").lstrip("/") == name and value.get("Config", {}).get("Labels", {}).get(LABEL) == self.owner
        else:
            owned = value.get("Name") == name and value.get("Labels", {}).get(LABEL) == self.owner
        if not owned:
            raise RuntimeError("resource ownership mismatch")
        if suffix in self.ids and value.get("Id") != self.ids[suffix]:
            raise RuntimeError("resource identity changed")
        self.ids[suffix] = value.get("Id")
        return value

    def create_volume(self, suffix):
        name = self.prefix + "-" + suffix
        probe = subprocess.run(["docker", "volume", "inspect", name], env=self.env, capture_output=True, timeout=10)
        if probe.returncode == 0:
            raise RuntimeError("volume name already exists; refusing reuse")
        self.docker("volume", "create", "--label", LABEL + "=" + self.owner, name)
        self.inspect("volume", suffix)

    def image(self, reference):
        if "@sha256:" not in reference:
            raise RuntimeError("digest-pinned image required")
        image = json.loads(self.docker("image", "inspect", reference))[0]
        if image.get("Os") != "linux":
            raise RuntimeError("Linux image required")
        return image["Id"]

    def create_s3(self, suffix, volume):
        name = self.prefix + "-" + suffix
        self.docker("create", "--name", name, "--label", LABEL + "=" + self.owner,
                    "--pull", "never", "--restart", "no", "--network", self.network,
                    "--network-alias", "rustfs", "--publish", "127.0.0.1::9000",
                    "--memory", "512m", "--cpus", "0.5", "--pids-limit", "64",
                    "--mount", "type=volume,source=" + self.prefix + "-" + volume + ",target=/data",
                    "--env", "RUSTFS_ACCESS_KEY=" + self.access,
                    "--env", "RUSTFS_SECRET_KEY=" + self.secret,
                    self.images["s3"], "server", "/data")
        self.inspect("container", suffix)
        self.docker("start", name)
        return self.s3_host_endpoint(suffix)

    def s3_host_endpoint(self, suffix):
        value = self.inspect("container", suffix)
        port = value["NetworkSettings"]["Ports"]["9000/tcp"]
        if len(port) != 1 or port[0]["HostIp"] != "127.0.0.1":
            raise RuntimeError("S3 endpoint not loopback bound")
        return "http://127.0.0.1:" + port[0]["HostPort"]

    def create_app(self, suffix, volume):
        name = self.prefix + "-" + suffix
        self.docker("create", "--name", name, "--label", LABEL + "=" + self.owner,
                    "--pull", "never", "--restart", "no", "--network", self.network,
                    "--publish", "127.0.0.1::9443", "--memory", "2g", "--cpus", "2",
                    "--pids-limit", "128", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                    "--mount", "type=volume,source=" + self.prefix + "-" + volume + ",target=/data/peri",
                    "--env", "S3_ENDPOINT=" + self.endpoint, "--env", "S3_BUCKET=" + self.bucket,
                    "--env", "S3_REGION=us-east-1", "--env", "S3_ACCESS_KEY_ID=" + self.access,
                    "--env", "S3_SECRET_ACCESS_KEY=" + self.secret,
                    self.images["app"], "--run-dir", "/data/peri", "--backend", "s3",
                    "--listen", "0.0.0.0:9443", "--tls", "--no-build")
        self.inspect("container", suffix)
        self.docker("start", name)

    def ready(self, suffix):
        deadline = time.monotonic() + self.args.ready_timeout
        name = self.prefix + "-" + suffix
        prior = self.ready_counts.get(suffix, 0)
        while time.monotonic() < deadline:
            value = self.inspect("container", suffix)
            if not value["State"]["Running"]:
                raise RuntimeError("app exited before ready")
            count = 0
            for line in self.docker("logs", name).splitlines():
                try:
                    event = json.loads(line)
                    count += event.get("service") == "local-project" and event.get("ready") is True
                except ValueError:
                    pass
            if count > prior:
                self.ready_counts[suffix] = count
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("app readiness timeout")
        port = value["NetworkSettings"]["Ports"]["9443/tcp"]
        if len(port) != 1 or port[0]["HostIp"] != "127.0.0.1":
            raise RuntimeError("app endpoint not loopback bound")
        connection = self.directory / (suffix + "-connection.json")
        ca = self.directory / (suffix + "-ca.crt")
        leaf = self.directory / (suffix + "-leaf.crt")
        self.docker("cp", name + ":/data/peri/connection.json", str(connection))
        config = json.loads(connection.read_text())
        if config["ca_file"] != "/data/peri/keys/dev-ca.crt":
            raise RuntimeError("unexpected CA path")
        self.docker("cp", name + ":" + config["ca_file"], str(ca))
        self.docker("cp", name + ":/data/peri/keys/gateway.crt", str(leaf))
        for path in (connection, ca, leaf):
            path.chmod(0o600)
        self.command(["openssl", "verify", "-CAfile", str(ca), "-purpose", "sslserver", "-verify_ip", "127.0.0.1", str(leaf)])
        config["endpoint"] = "https://127.0.0.1:" + port[0]["HostPort"]
        config["ca_file"] = str(ca)
        return config

    def sql(self, config, operation, body):
        return self.dev.request(config["endpoint"], config["project_api_key"], "/v1/sql/" + operation, body, config["ca_file"])

    def query(self, config, table):
        session = self.sql(config, "open", {"database": config["database"]})["session"]
        try:
            return self.sql(config, "query", {"session": session, "sql": "SELECT value FROM " + table + " ORDER BY rowid"})
        finally:
            self.sql(config, "close", {"session": session})

    def write(self, config, table, value, create=False):
        session = self.sql(config, "open", {"database": config["database"]})["session"]
        statement = ("CREATE TABLE " + table + "(value TEXT NOT NULL);" if create else "")
        statement += "INSERT INTO " + table + " VALUES('" + value + "');"
        try:
            answer = self.sql(config, "execute", {"session": session, "sql": statement})
            if answer != {"executed": True}:
                raise RuntimeError("SQL acknowledgement missing")
        finally:
            self.sql(config, "close", {"session": session})

    def stop(self, suffix):
        value = self.inspect("container", suffix)
        if not value["State"]["Running"]:
            raise RuntimeError("expected running owned container")
        self.docker("kill", "--signal", "TERM", value["Id"])
        if self.docker("wait", value["Id"], timeout=self.args.stop_timeout) != "0":
            raise RuntimeError("clean stop not proven")
        state = self.inspect("container", suffix)["State"]
        if state["Running"] or state["ExitCode"] != 0 or state["Pid"] != 0 or state["OOMKilled"]:
            raise RuntimeError("clean stop state not proven")

    def archive(self, volume, file):
        # The app image includes GNU tar. Read source read-only; backup lives on host.
        self.inspect("volume", volume)
        self.docker("run", "--name", self.prefix + "-backup-" + volume,
                    "--label", LABEL + "=" + self.owner, "--pull", "never", "--network", "none",
                    "--entrypoint", "tar", "--user", "0:0",
                    "--mount", "type=volume,source=" + self.prefix + "-" + volume + ",target=/source,readonly",
                    "--mount", "type=bind,source=" + str(self.directory) + ",target=/backup",
                    self.images["app"], "--numeric-owner", "--xattrs", "--acls", "-C", "/source",
                    "-cpf", "/backup/" + file, ".")
        path = self.directory / file
        path.chmod(0o600)
        if not path.is_file() or path.stat().st_size == 0:
            raise RuntimeError("empty backup archive")
        return {"file": file, "bytes": path.stat().st_size, "sha256": sha(path)}

    def extract(self, volume, archive):
        self.inspect("volume", volume)
        if sha(self.directory / archive["file"]) != archive["sha256"]:
            raise RuntimeError("backup checksum mismatch")
        self.docker("run", "--name", self.prefix + "-extract-" + volume,
                    "--label", LABEL + "=" + self.owner, "--pull", "never", "--network", "none",
                    "--entrypoint", "tar", "--user", "0:0",
                    "--mount", "type=volume,source=" + self.prefix + "-" + volume + ",target=/target",
                    "--mount", "type=bind,source=" + str(self.directory) + ",target=/backup,readonly",
                    self.images["app"], "--numeric-owner", "--xattrs", "--acls", "-C", "/target",
                    "-xpf", "/backup/" + archive["file"])

    def retire_source_resources(self):
        # The synthetic source has failed, and both independent backup archives
        # have already been checksummed. Remove only this run's labelled source
        # containers and volumes so restore cannot read the original data.
        removed_containers = []
        for suffix in ("app-source-service", "s3-source-service", "backup-app-source", "backup-s3-source"):
            item = self.inspect("container", suffix)
            if item["State"]["Running"]:
                raise RuntimeError("owned source container still running")
            self.docker("rm", item["Id"])
            removed_containers.append(item["Id"])
        removed_volumes = []
        for suffix in ("app-source", "s3-source"):
            item = self.inspect("volume", suffix)
            self.docker("volume", "rm", item["Name"])
            removed_volumes.append(item["Name"])
        self.report["source_resources_retired"] = True
        self.event("source_resources_retired", container_ids=removed_containers, volumes=removed_volumes)

    def s3(self, endpoint, method, path, body=b""):
        parsed = urllib.parse.urlsplit(endpoint)
        if parsed.hostname != "127.0.0.1" or parsed.scheme != "http":
            raise RuntimeError("S3 request must use private loopback publication")
        now = dt.datetime.now(dt.timezone.utc)
        timestamp, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
        digest = hashlib.sha256(body).hexdigest()
        headers = {"host": parsed.netloc, "x-amz-content-sha256": digest, "x-amz-date": timestamp}
        names = ";".join(sorted(headers))
        split = urllib.parse.urlsplit(path)
        query = urllib.parse.parse_qsl(split.query, keep_blank_values=True)
        canonical_query = "&".join(urllib.parse.quote(k, safe="-_.~") + "=" + urllib.parse.quote(v, safe="-_.~") for k, v in sorted(query))
        canonical = "\n".join((method, split.path, canonical_query,
                               "".join(k + ":" + headers[k] + "\n" for k in sorted(headers)), names, digest))
        scope = day + "/us-east-1/s3/aws4_request"
        signing = "\n".join(("AWS4-HMAC-SHA256", timestamp, scope, hashlib.sha256(canonical.encode()).hexdigest()))
        key = ("AWS4" + self.secret).encode()
        for part in (day, "us-east-1", "s3", "aws4_request"):
            key = hmac.new(key, part.encode(), hashlib.sha256).digest()
        signature = hmac.new(key, signing.encode(), hashlib.sha256).hexdigest()
        headers["authorization"] = "AWS4-HMAC-SHA256 Credential=" + self.access + "/" + scope + ", SignedHeaders=" + names + ", Signature=" + signature
        request = urllib.request.Request(endpoint + path, data=body if method in ("PUT", "POST") else None, headers=headers, method=method)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(request, timeout=15) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, b""

    def objects(self, endpoint):
        # ListObjects v1 with marker avoids signed query-string encoding ambiguity.
        marker = ""
        result = []
        while True:
            query = "?marker=" + urllib.parse.quote(marker, safe="") if marker else ""
            status, data = self.s3(endpoint, "GET", "/" + self.bucket + query)
            if status != 200:
                raise RuntimeError("S3 listing failed")
            root = ET.fromstring(data)
            ns = {"s": root.tag.split("}")[0].lstrip("{")} if "}" in root.tag else {}
            def find(node, name):
                return node.findtext("s:" + name, namespaces=ns) if ns else node.findtext(name)
            page = [find(item, "Key") for item in root.findall("s:Contents", ns) if find(item, "Key") is not None] if ns else [find(item, "Key") for item in root.findall("Contents")]
            for key in page:
                object_path = "/" + self.bucket + "/" + urllib.parse.quote(key, safe="/-_.~")
                code, content = self.s3(endpoint, "GET", object_path)
                if code != 200:
                    raise RuntimeError("S3 object read failed")
                result.append({"key": key, "bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()})
            if find(root, "IsTruncated") != "true":
                break
            if not page or page[-1] == marker:
                raise RuntimeError("S3 listing pagination did not advance")
            marker = page[-1]
        if not result:
            raise RuntimeError("S3 object manifest empty")
        return result

    def wait_s3(self, endpoint, expected_status):
        deadline = time.monotonic() + self.args.ready_timeout
        while time.monotonic() < deadline:
            try:
                status, _ = self.s3(endpoint, "HEAD", "/" + self.bucket)
                if status == expected_status:
                    return
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.5)
        raise RuntimeError("isolated S3 readiness timeout")

    def run(self):
        table = "dr_" + secrets.token_hex(8)
        value_a, value_b = secrets.token_hex(16), secrets.token_hex(16)
        try:
            self.event("preflight")
            self.images = {"app": self.image(self.args.app_image), "s3": self.image(self.args.s3_image)}
            if json.loads(self.docker("image", "inspect", self.args.app_image))[0]["Config"].get("User") != "10001:10001":
                raise RuntimeError("expected non-root app identity missing")
            self.docker("network", "create", "--label", LABEL + "=" + self.owner, self.network)
            for volume in ("app-source", "s3-source", "app-restore", "s3-restore"):
                self.create_volume(volume)
            source_s3 = self.create_s3("s3-source-service", "s3-source")
            self.event("source_s3_started")
            self.wait_s3(source_s3, 404)
            if self.s3(source_s3, "PUT", "/" + self.bucket)[0] not in (200, 204):
                raise RuntimeError("new bucket creation failed")
            self.create_app("app-source-service", "app-source")
            original = self.ready("app-source-service")
            self.write(original, table, value_a, create=True)
            if self.query(original, table) != [[value_a]]:
                raise RuntimeError("ACK A readback failed")
            ack_a_ns = time.monotonic_ns()
            self.event("ack_a", value_sha256=hashlib.sha256(value_a.encode()).hexdigest(),
                       row_count=1)
            self.stop("app-source-service")
            marker = self.directory / "source-authority.lock"
            self.docker("cp", self.prefix + "-app-source-service:/data/peri/authority/authority.lock", str(marker))
            marker.chmod(0o600)
            if marker.read_bytes() != b"clean\n":
                raise RuntimeError("app clean authority marker absent")
            source_objects = self.objects(source_s3)
            save(self.directory / "backup-object-manifest.json", source_objects)
            # S3 is stopped before volume backup, making both archives cold snapshots.
            self.stop("s3-source-service")
            app_archive = self.archive("app-source", "app-state.tar")
            s3_archive = self.archive("s3-source", "s3-state.tar")
            save(self.directory / "backup-manifest.json", {"app": app_archive, "s3": s3_archive,
                    "backup_utc": dt.datetime.now(dt.timezone.utc).isoformat(), "owner": self.owner})
            backup_ns = time.monotonic_ns()
            self.event("backup_complete", archives={"app": app_archive, "s3": s3_archive}, object_count=len(source_objects))
            self.docker("start", self.prefix + "-s3-source-service")
            source_s3 = self.s3_host_endpoint("s3-source-service")
            self.wait_s3(source_s3, 200)
            self.docker("start", self.prefix + "-app-source-service")
            original = self.ready("app-source-service")
            self.write(original, table, value_b)
            if self.query(original, table) != [[value_a], [value_b]]:
                raise RuntimeError("ACK B readback failed")
            ack_b_ns = time.monotonic_ns()
            self.event("ack_b", value_sha256=hashlib.sha256(value_b.encode()).hexdigest(),
                       row_count=2)
            self.docker("kill", "--signal", "KILL", self.ids["app-source-service"])
            self.docker("wait", self.ids["app-source-service"])
            self.docker("kill", "--signal", "KILL", self.ids["s3-source-service"])
            self.docker("wait", self.ids["s3-source-service"])
            fault_ns = time.monotonic_ns()
            if self.inspect("container", "app-source-service")["State"]["Running"] or self.inspect("container", "s3-source-service")["State"]["Running"]:
                raise RuntimeError("source fault not proven")
            self.event("source_fault", original_volumes_unmounted_from_restore=True)
            self.retire_source_resources()
            self.extract("app-restore", app_archive)
            self.extract("s3-restore", s3_archive)
            restored_s3 = self.create_s3("s3-restore-service", "s3-restore")
            self.wait_s3(restored_s3, 200)
            # Both app generations have exactly the same S3 endpoint string.
            restored_objects = self.objects(restored_s3)
            if restored_objects != source_objects:
                raise RuntimeError("restored S3 object manifest differs from backup")
            save(self.directory / "restored-object-manifest.json", restored_objects)
            self.event("object_restore_verified", object_count=len(restored_objects))
            self.create_app("app-restore-service", "app-restore")
            restored = self.ready("app-restore-service")
            if restored["database"] != original["database"] or restored["project_api_key"] != original["project_api_key"]:
                raise RuntimeError("restored database or credential changed")
            rows = self.query(restored, table)
            if rows != [[value_a]]:
                raise RuntimeError("restored ACK A missing or post-backup ACK B unexpectedly present")
            recovered_ns = time.monotonic_ns()
            self.event("restored_sql_read", row_count=len(rows),
                       first_value_sha256=hashlib.sha256(rows[0][0].encode()).hexdigest(),
                       b_value_present=any(hashlib.sha256(row[0].encode()).hexdigest() == hashlib.sha256(value_b.encode()).hexdigest() for row in rows))
            self.report.update(status="RESTORE_PROVEN_WITH_B_LOSS",
                               rpo_seconds=round((fault_ns - ack_a_ns) / 1e9, 3),
                               rto_seconds=round((recovered_ns - fault_ns) / 1e9, 3),
                               rpo_definition="fault to newest restored acknowledged write A",
                               rto_definition="source fault to successful restored TLS SQL readback",
                               backup_to_lost_ack_seconds=round((ack_b_ns - backup_ns) / 1e9, 3),
                               timepoints_monotonic_ns={"ack_a": ack_a_ns, "backup_complete": backup_ns,
                                                        "ack_b": ack_b_ns, "source_fault": fault_ns,
                                                        "restored_sql_read": recovered_ns},
                               lost_acknowledged_writes=1,
                               ack_a_restored=True, ack_b_restored=False,
                               object_count=len(restored_objects), same_endpoint=self.endpoint,
                               same_database_and_credential=True)
            self.report["pass"] = True
            self.event("complete")
        except BaseException as error:
            self.report["pass"] = False
            self.report["status"] = "PARTIAL"
            self.report["failure_type"] = type(error).__name__
            self.report["failure_detail_private"] = str(error)
            self.flush()
        return self.report["pass"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-image", default=DEFAULT_APP)
    parser.add_argument("--s3-image", default=DEFAULT_S3)
    parser.add_argument("--private-parent", default=None)
    parser.add_argument("--ready-timeout", type=int, default=120)
    parser.add_argument("--stop-timeout", type=int, default=90)
    args = parser.parse_args()
    if min(args.ready_timeout, args.stop_timeout) <= 0:
        parser.error("positive timeouts required")
    os.umask(0o077)
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    trial = Rehearsal(args)
    passed = trial.run()
    print(json.dumps({"status": trial.report["status"], "pass": passed,
                      "private_directory": str(trial.directory), "stage": trial.report["stage"],
                      "failure_type": trial.report.get("failure_type")}))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
