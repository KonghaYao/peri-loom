#!/usr/bin/env python3
"""Isolated v1.0.1 local-backend HA failure injection. No automatic cleanup.

The observation phase takes no recovery action. --standby-diagnostic starts a
second container only after that phase, and only if the primary remains dead.
"""

import argparse
import datetime as dt
import hashlib
import json
import os
import pathlib
import secrets
import socket
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


IMAGE = "ghcr.io/konghayao/peri-loom@sha256:901f37f2cceb62bf265072e5b0159fa86974af3440a79383ba31e351c0895333"
LABEL = "peri.ha-experiment.owner"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, destination):
        return None


def stamp():
    return {"utc": dt.datetime.now(dt.timezone.utc).isoformat(), "monotonic_ns": time.monotonic_ns()}


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Driver:
    def __init__(self, args):
        self.args = args
        self.owner = secrets.token_hex(16)
        self.name = "peri-ha-" + self.owner
        self.volume = self.name + "-data"
        self.standby = self.name + "-standby"
        self.directory = pathlib.Path(tempfile.mkdtemp(prefix="peri-ha-evidence-")).resolve()
        self.directory.chmod(0o700)
        self.events = self.directory / "evidence.jsonl"
        self.ids = {}
        self.config = None
        self.port = free_port()
        self.endpoint = f"https://127.0.0.1:{self.port}"
        self.table = "ha_probe_" + secrets.token_hex(8)
        self.value = secrets.token_hex(16)
        self.image_id = None
        self.phase = "setup"
        probe_path = self.directory / "probe.json"
        descriptor = os.open(probe_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            json.dump({"table": self.table, "value": self.value}, output)
            output.flush()
            os.fsync(output.fileno())
        self.record("run_created", owner=self.owner, image=IMAGE, endpoint=self.endpoint,
                    primary=self.name, standby=self.standby, volume=self.volume)

    def record(self, event, **fields):
        row = {**stamp(), "phase": self.phase, "event": event, **fields}
        with self.events.open("a") as output:
            output.write(json.dumps(row, sort_keys=True) + "\n")
            output.flush()
            os.fsync(output.fileno())

    def docker(self, *args, timeout=30, check=True):
        # Never emit raw Docker output: logs and connection files can contain secrets.
        result = subprocess.run(["docker", *args], text=True, capture_output=True, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError("Docker command failed: " + args[0])
        return result

    def inspect(self, name):
        result = self.docker("container", "inspect", name)
        item = json.loads(result.stdout)[0]
        if item["Name"] != "/" + name or item["Config"].get("Labels", {}).get(LABEL) != self.owner:
            raise RuntimeError("container ownership mismatch")
        if name in self.ids and self.ids[name] != item["Id"]:
            raise RuntimeError("container identity changed")
        self.ids[name] = item["Id"]
        return item

    def state(self, name):
        item = self.inspect(name)
        state = item["State"]
        return {"id": item["Id"], "running": state["Running"],
                "exit_code": state["ExitCode"], "oom_killed": state["OOMKilled"],
                "pid": state["Pid"], "started_at": state["StartedAt"],
                "finished_at": state["FinishedAt"]}

    def create(self, name):
        # Docker create names are random; an existing collision must fail closed.
        self.record("create_attempt", name=name)
        self.docker("create", "--name", name, "--label", LABEL + "=" + self.owner,
                    "--pull", "never", "--restart", "no", "--memory", "2g", "--cpus", "2",
                    "--pids-limit", "128", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                    "--publish", f"127.0.0.1:{self.port}:9443",
                    "--mount", f"type=volume,source={self.volume},target=/data/peri",
                    self.image_id, "--run-dir", "/data/peri", "--backend", "local",
                    "--listen", "0.0.0.0:9443", "--tls", "--no-build")
        item = self.inspect(name)
        if item["Image"] != self.image_id or item["Config"].get("User") != "10001:10001":
            raise RuntimeError("created container image or UID mismatch")
        bindings = item["HostConfig"]["PortBindings"]["9443/tcp"]
        if bindings != [{"HostIp": "127.0.0.1", "HostPort": str(self.port)}]:
            raise RuntimeError("fixed loopback binding mismatch")
        self.record("container_created", name=name, id=item["Id"])

    def copy_private(self, name, source, target):
        path = self.directory / target
        self.docker("cp", self.ids[name] + ":" + source, str(path))
        path.chmod(0o600)
        return path

    def ready(self, name, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = self.state(name)
            if not state["running"]:
                raise RuntimeError("container exited before readiness")
            logs = self.docker("logs", self.ids[name]).stdout
            for line in logs.splitlines():
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if isinstance(event, dict) and event.get("service") == "local-project" and event.get("ready") is True:
                    self.record("ready_event_seen", name=name, state=state)
                    return
            time.sleep(0.5)
        raise RuntimeError("readiness timeout")

    def setup_connection(self):
        raw = self.copy_private(self.name, "/data/peri/connection.json", "connection.json")
        config = json.loads(raw.read_text())
        if config["ca_file"] != "/data/peri/keys/dev-ca.crt":
            raise RuntimeError("unexpected CA path")
        ca = self.copy_private(self.name, config["ca_file"], "ca.crt")
        leaf = self.copy_private(self.name, "/data/peri/keys/gateway.crt", "gateway.crt")
        result = subprocess.run(["openssl", "verify", "-CAfile", str(ca), "-purpose", "sslserver",
                                 "-verify_ip", "127.0.0.1", str(leaf)], capture_output=True, text=True)
        if result.returncode or ca.read_bytes() == leaf.read_bytes():
            raise RuntimeError("TLS chain or loopback SAN verification failed")
        self.config = config
        self.record("tls_verified", ca_sha256=hashlib.sha256(ca.read_bytes()).hexdigest(),
                    leaf_sha256=hashlib.sha256(leaf.read_bytes()).hexdigest())

    def request(self, path, body, timeout=2):
        request_id = secrets.token_hex(16)
        payload = {**body, "request_id": request_id}
        request = urllib.request.Request(self.endpoint + path, json.dumps(payload).encode(),
                                         {"Authorization": "Bearer " + self.config["project_api_key"],
                                          "Content-Type": "application/json", "X-Request-ID": request_id},
                                         method="POST")
        context = ssl.create_default_context(cafile=str(self.directory / "ca.crt"))
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect(),
                                            urllib.request.HTTPSHandler(context=context))
        with opener.open(request, timeout=timeout) as response:
            reply = json.loads(response.read(1024 * 1024 + 1))
        if (not isinstance(reply, dict) or reply.get("version") != 1
                or reply.get("request_id") != request_id or reply.get("ok") is not True
                or reply.get("error") is not None):
            raise RuntimeError("SQL response rejected or identity mismatch")
        return reply.get("result")

    def select_probe(self):
        session = self.request("/v1/sql/open", {"database": self.config["database"]})["session"]
        try:
            result = self.request("/v1/sql/query", {"session": session,
                                                     "sql": f"SELECT value FROM {self.table}"})
        finally:
            try:
                self.request("/v1/sql/close", {"session": session})
            except Exception:
                pass
        return result == [[self.value]]

    def write_probe(self):
        session = self.request("/v1/sql/open", {"database": self.config["database"]})["session"]
        self.record("write_dispatch", table=self.table)
        try:
            response = self.request("/v1/sql/execute", {"session": session,
                "sql": f"CREATE TABLE {self.table}(value TEXT NOT NULL);INSERT INTO {self.table} VALUES('{self.value}');"})
        finally:
            try:
                self.request("/v1/sql/close", {"session": session})
            except Exception:
                pass
        if response != {"executed": True}:
            raise RuntimeError("SQL write acknowledgement missing")
        if not self.select_probe():
            raise RuntimeError("acknowledged SQL write not readable before failure")
        self.record("write_acknowledged_and_read", value_sha256=hashlib.sha256(self.value.encode()).hexdigest())

    def run(self):
        image = json.loads(self.docker("image", "inspect", IMAGE).stdout)[0]
        if image.get("Os") != "linux" or image["Config"].get("User") != "10001:10001":
            raise RuntimeError("released Linux image and expected UID required")
        self.image_id = image["Id"]
        self.record("image_inspected", image_id=self.image_id, repo_digests=image.get("RepoDigests"))
        self.record("volume_create_attempt", volume=self.volume)
        self.docker("volume", "create", "--label", LABEL + "=" + self.owner, self.volume)
        volume = json.loads(self.docker("volume", "inspect", self.volume).stdout)[0]
        if volume["Name"] != self.volume or volume.get("Labels", {}).get(LABEL) != self.owner:
            raise RuntimeError("volume ownership mismatch")
        self.record("volume_created", volume=self.volume)
        self.create(self.name)
        self.docker("start", self.ids[self.name])
        self.record("primary_started", state=self.state(self.name))
        self.ready(self.name, self.args.ready_timeout)
        self.setup_connection()
        self.write_probe()
        self.phase = "automatic_observation"
        self.record("kill_dispatch", state=self.state(self.name))
        killed = self.docker("kill", "--signal", "KILL", self.ids[self.name], check=False)
        self.record("kill_result", returncode=killed.returncode, state=self.state(self.name))
        if killed.returncode:
            raise RuntimeError("KILL was not accepted")
        deadline = time.monotonic() + self.args.observe_seconds
        recovered = False
        attempts = 0
        while True:
            attempts += 1
            state = self.state(self.name)
            try:
                readable = self.select_probe()
            except Exception:
                readable = False
            if readable:
                recovered = True
                self.record("endpoint_recovered", attempt=attempts, state=state, acknowledged_write_read=True)
                break
            self.record("endpoint_unavailable", attempt=attempts, state=state)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(self.args.poll_seconds, remaining))
        self.record("automatic_observation_complete", recovered=recovered, attempts=attempts,
                    final_state=self.state(self.name))
        if self.args.standby_diagnostic and not recovered:
            self.phase = "manual_standby_diagnostic"
            primary = self.state(self.name)
            if primary["running"]:
                self.record("standby_skipped", reason="primary_still_running")
                return recovered
            self.create(self.standby)
            start = self.docker("start", self.ids[self.standby], check=False)
            self.record("standby_start_command", returncode=start.returncode,
                        state=self.state(self.standby))
            if start.returncode == 0:
                try:
                    self.ready(self.standby, self.args.ready_timeout)
                    readable = self.select_probe()
                    self.record("manual_standby_result", acknowledged_write_read=readable,
                                state=self.state(self.standby))
                except Exception as error:
                    self.record("manual_standby_result", status="unavailable",
                                error_type=type(error).__name__, state=self.state(self.standby))
        return recovered


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observe-seconds", type=float, default=60)
    parser.add_argument("--poll-seconds", type=float, default=1)
    parser.add_argument("--ready-timeout", type=float, default=30)
    parser.add_argument("--standby-diagnostic", action="store_true")
    args = parser.parse_args()
    if not 5 <= args.observe_seconds <= 600 or not 0.2 <= args.poll_seconds <= 10 or not 1 <= args.ready_timeout <= 120:
        parser.error("timeouts outside permitted bounds")
    driver = Driver(args)
    try:
        recovered = driver.run()
        print(json.dumps({"status": "automatic_recovery_observed" if recovered else "automatic_recovery_not_observed",
                          "evidence": str(driver.events), "owner": driver.owner,
                          "resources_retained": True}))
    except Exception as error:
        driver.record("harness_error", error_type=type(error).__name__)
        print(json.dumps({"status": "uncertain", "error_type": type(error).__name__,
                          "evidence": str(driver.events), "owner": driver.owner,
                          "resources_retained": True}))
        raise SystemExit(2)


if __name__ == "__main__":
    main()
