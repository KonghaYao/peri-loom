#!/usr/bin/env python3
"""Isolated manual cold-clone fencing diagnostic for the published v1.0.1 image.

Creates one local-backend source, stops it cleanly, then restores one cold tar
into two distinct volumes. Both clones are started together. This is a manual
clone safety test, not an automatic failover or high-availability test.
No resource is automatically removed, including after an uncertain result.
The evidence directory and archive contain credentials and must stay private.
"""

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import tarfile
import tempfile
import time


IMAGE = "ghcr.io/konghayao/peri-loom@sha256:901f37f2cceb62bf265072e5b0159fa86974af3440a79383ba31e351c0895333"
LABEL = "peri.clone.owner"


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


class Diagnostic:
    def __init__(self, args):
        self.args = args
        self.owner = secrets.token_hex(16)
        self.prefix = "peri-clone-" + self.owner
        self.directory = Path(tempfile.mkdtemp(prefix=self.prefix + "-", dir=args.private_parent)).resolve()
        self.directory.chmod(0o700)
        self.events = self.directory / "evidence.jsonl"
        self.ids = {}
        self.image_id = None
        self.env = {k: os.environ[k] for k in ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG", "XDG_RUNTIME_DIR", "TMPDIR") if k in os.environ}
        self.tables = {suffix: "clone_probe_" + secrets.token_hex(8) for suffix in ("source", "a", "b")}
        self.values = {suffix: secrets.token_hex(24) for suffix in ("source", "a", "b")}
        self.private_json("probes.json", {"tables": self.tables, "values": self.values})
        self.record("initialized", image=IMAGE, resources=[self.name(s) for s in ("source", "a", "b")])

    def name(self, suffix):
        return self.prefix + "-" + suffix

    def private_json(self, filename, value):
        path = self.directory / filename
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as output:
            json.dump(value, output, sort_keys=True, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())

    def record(self, event, **fields):
        row = {"event": event, "utc": dt.datetime.now(dt.timezone.utc).isoformat(),
               "monotonic_ns": time.monotonic_ns(), **fields}
        fd = os.open(self.events, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(fd, "w") as output:
            output.write(json.dumps(row, sort_keys=True) + "\n")
            output.flush()
            os.fsync(output.fileno())

    def command(self, argv, timeout=120, check=True):
        try:
            result = subprocess.run(argv, env=self.env, text=True, capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise RuntimeError("command timed out; outcome unknown") from None
        if check and result.returncode:
            raise RuntimeError("command failed; output withheld; outcome unknown")
        return result

    def docker(self, *args, timeout=120, check=True):
        return self.command(["docker", *args], timeout=timeout, check=check)

    def inspect(self, kind, suffix):
        name = self.name(suffix)
        item = json.loads(self.docker(kind, "inspect", name).stdout)[0]
        labels = (item.get("Config", {}).get("Labels", {}) if kind == "container"
                  else item.get("Labels", {})) or {}
        actual_name = item.get("Name", "").lstrip("/")
        if actual_name != name or labels.get(LABEL) != self.owner:
            raise RuntimeError("resource ownership mismatch")
        if kind == "container":
            previous = self.ids.get(suffix)
            if previous is not None and previous != item["Id"]:
                raise RuntimeError("container identity changed")
            self.ids[suffix] = item["Id"]
        return item

    def create_volume(self, suffix):
        name = self.name(suffix)
        if self.docker("volume", "inspect", name, check=False).returncode == 0:
            raise RuntimeError("volume name already exists")
        self.record("volume_create_attempt", suffix=suffix)
        self.docker("volume", "create", "--label", LABEL + "=" + self.owner, name)
        self.inspect("volume", suffix)
        self.record("volume_created", suffix=suffix)

    def create_app(self, suffix):
        name = self.name(suffix)
        if self.docker("container", "inspect", name, check=False).returncode == 0:
            raise RuntimeError("container name already exists")
        self.inspect("volume", suffix)
        self.record("container_create_attempt", suffix=suffix)
        self.docker("create", "--name", name, "--label", LABEL + "=" + self.owner,
                    "--pull", "never", "--restart", "no", "--publish", "127.0.0.1::9443",
                    "--memory", "2g", "--cpus", "2", "--pids-limit", "128",
                    "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                    "--mount", "type=volume,source=" + name + ",target=/data/peri",
                    self.image_id, "--run-dir", "/data/peri", "--backend", "local",
                    "--listen", "0.0.0.0:9443", "--tls", "--no-build")
        item = self.inspect("container", suffix)
        if item["Image"] != self.image_id or item["Config"].get("User") != "10001:10001":
            raise RuntimeError("created container image or UID mismatch")
        bindings = item["HostConfig"]["PortBindings"]["9443/tcp"]
        if len(bindings) != 1 or bindings[0]["HostIp"] != "127.0.0.1":
            raise RuntimeError("published endpoint is not loopback")
        self.record("container_created", suffix=suffix, id=item["Id"])

    def state(self, suffix):
        item = self.inspect("container", suffix)
        state = item["State"]
        return {"running": state["Running"], "exit_code": state["ExitCode"],
                "oom_killed": state["OOMKilled"], "pid": state["Pid"],
                "started_at": state["StartedAt"], "finished_at": state["FinishedAt"]}

    def start(self, suffix):
        self.record("start_dispatch", suffix=suffix)
        outcome = self.docker("start", self.ids[suffix], check=False)
        self.record("start_result", suffix=suffix, returncode=outcome.returncode,
                    state=self.state(suffix))
        return outcome.returncode == 0

    def ready(self, suffix):
        deadline = time.monotonic() + self.args.ready_timeout
        while time.monotonic() < deadline:
            state = self.state(suffix)
            if not state["running"]:
                self.record("exited_before_ready", suffix=suffix, state=state)
                return False
            logs = self.docker("logs", self.ids[suffix]).stdout
            for line in logs.splitlines():
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if isinstance(event, dict) and event.get("service") == "local-project" and event.get("ready") is True:
                    self.record("ready", suffix=suffix, state=state)
                    return True
            time.sleep(0.5)
        self.record("readiness_timeout", suffix=suffix, state=self.state(suffix))
        return False

    def connection(self, suffix):
        name = self.name(suffix)
        connection = self.directory / (suffix + "-connection.json")
        ca = self.directory / (suffix + "-ca.crt")
        leaf = self.directory / (suffix + "-leaf.crt")
        self.docker("cp", name + ":/data/peri/connection.json", str(connection))
        connection.chmod(0o600)
        config = json.loads(connection.read_text())
        if config["ca_file"] != "/data/peri/keys/dev-ca.crt":
            raise RuntimeError("unexpected CA path")
        self.docker("cp", name + ":" + config["ca_file"], str(ca))
        self.docker("cp", name + ":/data/peri/keys/gateway.crt", str(leaf))
        for path in (ca, leaf):
            path.chmod(0o600)
        self.command(["openssl", "verify", "-CAfile", str(ca), "-purpose", "sslserver",
                      "-verify_ip", "127.0.0.1", str(leaf)])
        item = self.inspect("container", suffix)
        ports = item["NetworkSettings"]["Ports"]["9443/tcp"]
        if len(ports) != 1 or ports[0]["HostIp"] != "127.0.0.1":
            raise RuntimeError("runtime endpoint is not loopback")
        config["endpoint"] = "https://127.0.0.1:" + ports[0]["HostPort"]
        config["ca_file"] = str(ca)
        self.record("tls_verified", suffix=suffix, ca_sha256=digest(ca), leaf_sha256=digest(leaf))
        return config

    def sql(self, config, operation, body):
        # Import only after Docker preparation; dev.request validates TLS and response IDs.
        import importlib.util
        path = Path(__file__).resolve().parents[2] / "prototypes/v1/platform/dev.py"
        spec = importlib.util.spec_from_file_location("peri_clone_dev", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.request(config["endpoint"], config["project_api_key"],
                              "/v1/sql/" + operation, body, config["ca_file"])

    def query(self, config, table):
        session = self.sql(config, "open", {"database": config["database"]})["session"]
        try:
            return self.sql(config, "query", {"session": session,
                                               "sql": "SELECT value FROM " + table + " ORDER BY rowid"})
        finally:
            self.sql(config, "close", {"session": session})

    def table_exists(self, config, table):
        session = self.sql(config, "open", {"database": config["database"]})["session"]
        try:
            rows = self.sql(config, "query", {"session": session,
                "sql": "SELECT name FROM sqlite_master WHERE type='table' AND name='" + table + "'"})
            return rows == [[table]]
        finally:
            self.sql(config, "close", {"session": session})

    def write(self, suffix, config):
        table, value = self.tables[suffix], self.values[suffix]
        session = self.sql(config, "open", {"database": config["database"]})["session"]
        self.record("write_dispatch", suffix=suffix, table=table)
        try:
            answer = self.sql(config, "execute", {"session": session,
                "sql": "CREATE TABLE " + table + "(value TEXT NOT NULL);"
                       "INSERT INTO " + table + " VALUES('" + value + "');"})
        finally:
            self.sql(config, "close", {"session": session})
        if answer != {"executed": True}:
            raise RuntimeError("SQL acknowledgement missing")
        rows = self.query(config, table)
        if rows != [[value]]:
            raise RuntimeError("acknowledged write readback mismatch")
        self.record("write_acknowledged_and_read", suffix=suffix, table=table,
                    value_sha256=hashlib.sha256(value.encode()).hexdigest(),
                    rows_sha256=hashlib.sha256(json.dumps(rows).encode()).hexdigest(),
                    state=self.state(suffix))

    def stop_source(self):
        if not self.state("source")["running"]:
            raise RuntimeError("source not running before clean stop")
        self.record("term_dispatch", suffix="source")
        self.docker("kill", "--signal", "TERM", self.ids["source"])
        wait = self.docker("wait", self.ids["source"], timeout=self.args.stop_timeout)
        state = self.state("source")
        self.record("source_stopped", wait_exit=wait.stdout.strip(), state=state)
        if wait.stdout.strip() != "0" or state["running"] or state["exit_code"] != 0 or state["oom_killed"]:
            raise RuntimeError("clean source stop not proven")

    def archive(self):
        self.inspect("volume", "source")
        self.record("cold_archive_dispatch", source_state=self.state("source"))
        self.docker("run", "--name", self.name("archive"), "--label", LABEL + "=" + self.owner,
                    "--pull", "never", "--network", "none", "--entrypoint", "tar", "--user", "0:0",
                    "--mount", "type=volume,source=" + self.name("source") + ",target=/source,readonly",
                    "--mount", "type=bind,source=" + str(self.directory) + ",target=/backup",
                    self.image_id, "--numeric-owner", "--xattrs", "--acls", "-C", "/source",
                    "-cpf", "/backup/cold-state.tar", ".")
        path = self.directory / "cold-state.tar"
        path.chmod(0o600)
        if not path.is_file() or path.stat().st_size == 0:
            raise RuntimeError("cold archive missing or empty")
        with tarfile.open(path, "r:") as archive:
            marker = archive.extractfile("./authority/authority.lock")
            terminal = archive.getmember("./launcher-terminal.json")
            if marker is None or marker.read() != b"clean\n" or not terminal.isfile():
                raise RuntimeError("clean terminal markers missing from cold archive")
        self.archive_sha256 = digest(path)
        self.record("cold_archive_created", bytes=path.stat().st_size, sha256=self.archive_sha256,
                    source_state=self.state("source"))

    def extract(self, suffix):
        self.inspect("volume", suffix)
        if self.state("source")["running"] or digest(self.directory / "cold-state.tar") != self.archive_sha256:
            raise RuntimeError("cold source or archive integrity lost")
        self.record("extract_dispatch", suffix=suffix, archive_sha256=self.archive_sha256)
        self.docker("run", "--name", self.name("extract-" + suffix), "--label", LABEL + "=" + self.owner,
                    "--pull", "never", "--network", "none", "--entrypoint", "tar", "--user", "0:0",
                    "--mount", "type=volume,source=" + self.name(suffix) + ",target=/target",
                    "--mount", "type=bind,source=" + str(self.directory) + ",target=/backup,readonly",
                    self.image_id, "--numeric-owner", "--xattrs", "--acls", "-C", "/target",
                    "-xpf", "/backup/cold-state.tar")
        self.record("extracted", suffix=suffix, archive_sha256=self.archive_sha256)

    def run(self):
        image = json.loads(self.docker("image", "inspect", IMAGE).stdout)[0]
        if image.get("Os") != "linux" or image.get("Config", {}).get("User") != "10001:10001":
            raise RuntimeError("published Linux image with expected UID required")
        self.image_id = image["Id"]
        self.record("image_verified", image_id=self.image_id, repo_digests=image.get("RepoDigests"))
        for suffix in ("source", "a", "b"):
            self.create_volume(suffix)
        self.create_app("source")
        if not self.start("source") or not self.ready("source"):
            raise RuntimeError("source failed to become ready")
        source = self.connection("source")
        self.write("source", source)
        self.stop_source()
        self.archive()
        for suffix in ("a", "b"):
            self.extract(suffix)
            self.create_app(suffix)
        # Start both before awaiting either readiness; each has its own volume and port.
        started = {suffix: self.start(suffix) for suffix in ("a", "b")}
        ready = {suffix: self.ready(suffix) if started[suffix] else False for suffix in ("a", "b")}
        configs = {}
        identities = {}
        for suffix in ("a", "b"):
            if ready[suffix]:
                configs[suffix] = self.connection(suffix)
                identities[suffix] = {
                    "database_equal_source": configs[suffix]["database"] == source["database"],
                    "project_key_equal_source": configs[suffix]["project_api_key"] == source["project_api_key"],
                    "source_ack_read": self.query(configs[suffix], self.tables["source"]) == [[self.values["source"]]],
                }
                self.record("clone_identity_and_baseline", suffix=suffix, **identities[suffix])
        if ready["a"] and ready["b"]:
            overlap_start = time.monotonic_ns()
            self.record("overlap_observed", states={s: self.state(s) for s in ("a", "b")})
            if not all(identities[s][key] for s in ("a", "b") for key in identities[s]):
                raise RuntimeError("clone identity or baseline read mismatch")
            for suffix in ("a", "b"):
                if not all(self.state(s)["running"] for s in ("a", "b")):
                    raise RuntimeError("overlap ended before both clone writes")
                self.write(suffix, configs[suffix])
            # Re-read both independent writes while both service processes remain live.
            for suffix in ("a", "b"):
                if self.query(configs[suffix], self.tables[suffix]) != [[self.values[suffix]]]:
                    raise RuntimeError("clone write not retained during overlap")
            cross_tables_absent = (not self.table_exists(configs["a"], self.tables["b"])
                                   and not self.table_exists(configs["b"], self.tables["a"]))
            final_states = {s: self.state(s) for s in ("a", "b")}
            overlap_end = time.monotonic_ns()
            both_running = all(v["running"] for v in final_states.values())
            self.record("overlap_complete", start_monotonic_ns=overlap_start,
                        end_monotonic_ns=overlap_end, duration_seconds=(overlap_end-overlap_start)/1e9,
                        both_running=both_running, states=final_states,
                        both_independent_sql_acks=True, cross_tables_absent=cross_tables_absent)
            if not both_running or not cross_tables_absent:
                raise RuntimeError("independent clone overlap not proven")
            return "BOTH_CLONES_ACK_WITH_SAME_IDENTITY"
        self.record("clone_start_outcome", started=started, ready=ready,
                    states={s: self.state(s) for s in ("a", "b")})
        return "CONCURRENT_CLONES_NOT_BOTH_READY"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--private-parent", default=tempfile.gettempdir())
    parser.add_argument("--ready-timeout", type=float, default=30)
    parser.add_argument("--stop-timeout", type=float, default=30)
    args = parser.parse_args()
    if not 1 <= args.ready_timeout <= 120 or not 1 <= args.stop_timeout <= 120:
        parser.error("timeouts outside permitted bounds")
    diagnostic = Diagnostic(args)
    try:
        status = diagnostic.run()
        diagnostic.record("diagnostic_complete", status=status)
        print(json.dumps({"status": status, "owner": diagnostic.owner,
                          "private_evidence": str(diagnostic.directory), "resources_retained": True}))
    except Exception as error:
        diagnostic.record("diagnostic_uncertain", error_type=type(error).__name__)
        print(json.dumps({"status": "UNCERTAIN", "error_type": type(error).__name__,
                          "owner": diagnostic.owner, "private_evidence": str(diagnostic.directory),
                          "resources_retained": True}))
        raise SystemExit(2)


if __name__ == "__main__":
    main()
