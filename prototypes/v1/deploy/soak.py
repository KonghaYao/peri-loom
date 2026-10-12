import argparse
import hashlib
import hmac
import importlib.util
import json
import math
import os
import pathlib
import random
import secrets
import signal
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
import urllib.parse
import xml.etree.ElementTree as xml
from datetime import datetime, timezone
from decimal import Decimal


ROOT = pathlib.Path(__file__).resolve().parents[3]
specification = importlib.util.spec_from_file_location("image_verification", pathlib.Path(__file__).with_name("verify-image.py"))
image = importlib.util.module_from_spec(specification)
specification.loader.exec_module(image)
specification = importlib.util.spec_from_file_location("project_protocol", ROOT / "prototypes/v1/platform/dev.py")
protocol = importlib.util.module_from_spec(specification)
specification.loader.exec_module(protocol)

PROBE = """import json,pathlib
topology=json.loads(pathlib.Path('/data/peri/topology.json').read_text())
identities=dict(topology['processes'],launcher={'pid':topology['launcher_pid']})
result={}
for name,identity in identities.items():
    directory=pathlib.Path('/proc')/str(identity['pid'])
    status={line.split(':',1)[0]:line.split(':',1)[1].strip() for line in (directory/'status').read_text().splitlines() if ':' in line}
    uids=[int(value) for value in status['Uid'].split()]
    if uids!=[10001,10001,10001,10001]:
        raise RuntimeError('unexpected process owner')
    result[name]={'pid':identity['pid'],'start_ticks':int((directory/'stat').read_text().rsplit(')',1)[1].split()[19]),'uids':uids,'rss_bytes':int(status['VmRSS'].split()[0])*1024,'anon_bytes':int(status['RssAnon'].split()[0])*1024,'threads':int(status['Threads']),'fds':len(list((directory/'fd').iterdir()))}
storage={}
for name in ['backend','cache-a','cache-b','authority','platform','audit']:
    storage[name]=sum(path.stat().st_size for path in (pathlib.Path('/data/peri')/name).rglob('*') if path.is_file())
print(json.dumps({'processes':result,'storage_file_bytes':storage}))
"""

LIMITS = {
    "maximum_rss_bytes": 1024 * 1024 * 1024, "maximum_fds": 512, "maximum_threads": 128,
    "maximum_growth_rss_bytes": 64 * 1024 * 1024, "maximum_growth_fds": 12, "maximum_growth_threads": 8,
    "maximum_tail_rss_bytes_per_second": 16 * 1024, "maximum_sample_gap_seconds": 30,
    "maximum_local_file_bytes": 256 * 1024 * 1024, "maximum_backend_object_bytes": 128 * 1024 * 1024,
}


def append_json(stream, value):
    stream.write(json.dumps(value, separators=(",", ":")) + "\n")
    stream.flush()


def private_stream(path):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    return os.fdopen(descriptor, "w")


def resource_evidence(observed):
    if not isinstance(observed, dict):
        observed = {}
    fields = ("pid", "start_ticks", "rss_bytes", "anon_bytes", "threads", "fds")
    processes = {}
    for name in ("authority", "worker-a", "worker-b", "platform", "launcher"):
        collection = observed.get("processes", {})
        process = collection.get(name, {}) if isinstance(collection, dict) else {}
        if not isinstance(process, dict):
            process = {}
        processes[name] = {field: process.get(field) if type(process.get(field)) is int else None for field in fields}
        uids = process.get("uids")
        processes[name]["uids"] = uids if isinstance(uids, list) and len(uids) == 4 and all(type(value) is int for value in uids) else None
    storage = observed.get("storage_file_bytes", {})
    if not isinstance(storage, dict):
        storage = {}
    return {"processes": processes, "storage_file_bytes": {name: storage.get(name) if type(storage.get(name)) is int else None for name in ("backend", "cache-a", "cache-b", "authority", "platform", "audit")}}


def metrics_evidence(metrics):
    def collection(value):
        return value if isinstance(value, dict) else {}

    def counters(value, fields):
        value = collection(value)
        return {field: value.get(field) if type(value.get(field)) is int else None for field in fields}
    metrics = collection(metrics)
    native = collection(metrics.get("native"))
    authority = collection(native.get("authority"))
    workers = collection(native.get("workers"))
    failures = metrics.get("errors_total")
    error_total = sum(failures.values()) if isinstance(failures, dict) and all(type(value) is int and value >= 0 for value in failures.values()) else None
    return {**counters(metrics, ("active_permits", "unknown_held_permits")),
            "errors_total": {"total": error_total},
            "projects": {str(index): counters(project, ("active", "sessions", "unknown_held", "control_active", "owner_active", "classifying")) for index, project in enumerate(collection(metrics.get("projects")).values())},
            "native": {"authority": {name: counters(authority.get(name), ("total", "errors")) for name in ("get", "put", "checkpoint", "wal_durability")},
                       "workers": {name: {**counters(workers.get(name), ("sql_inflight", "sessions")), "resources": counters(collection(workers.get(name)).get("resources"), ("active_sessions", "loading_databases")), "retirement": counters(collection(workers.get(name)).get("retirement"), ("quarantined_databases", "retiring_databases", "pending_statements", "pending_io"))} for name in ("worker-a", "worker-b")}}}


def check_quiescent(metrics):
    failures = metrics.get("errors_total")
    authority = metrics.get("native", {}).get("authority")
    if not isinstance(failures, dict) or any(type(value) is not int or value != 0 for value in failures.values()):
        raise RuntimeError("gateway errors or unavailable error counters")
    if not isinstance(authority, dict) or "status" in authority or any(not isinstance(authority.get(name), dict) or type(authority[name].get("total")) is not int or authority[name]["total"] < 0 or type(authority[name].get("errors")) is not int or authority[name]["errors"] != 0 for name in ("get", "put", "checkpoint", "wal_durability")):
        raise RuntimeError("authority observations unavailable or failed")
    if any(type(metrics.get(name)) is not int or metrics[name] != 0 for name in ("active_permits", "unknown_held_permits")):
        raise RuntimeError("gateway permits did not return to quiescence")
    projects = metrics.get("projects")
    if not isinstance(projects, dict) or not projects:
        raise RuntimeError("project metrics missing")
    for project in projects.values():
        if any(type(project.get(name)) is not int or project[name] != 0 for name in ("active", "sessions", "unknown_held", "control_active", "owner_active", "classifying")):
            raise RuntimeError("project admission retained resources")
    workers = metrics.get("native", {}).get("workers", {})
    if set(workers) != {"worker-a", "worker-b"}:
        raise RuntimeError("worker observations missing")
    for worker in workers.values():
        if any(type(worker.get(name)) is not int or worker[name] != 0 for name in ("sql_inflight", "sessions")):
            raise RuntimeError("worker SQL resources did not drain")
        resources = worker.get("resources", {})
        if any(type(resources.get(name)) is not int or resources[name] != 0 for name in ("active_sessions", "loading_databases")):
            raise RuntimeError("native runtime did not reach quiescence")
        retirement = worker.get("retirement")
        if not isinstance(retirement, dict):
            raise RuntimeError("native retirement observations unavailable")
        required = ("quarantined_databases", "retiring_databases", "pending_statements", "pending_io")
        if any(type(retirement.get(name)) is not int or retirement[name] != 0 for name in required):
            raise RuntimeError("native retirement remained pending or unavailable")


def evaluate(samples, duration, limits, qualifying, trends=True):
    if len(samples) < 2:
        raise RuntimeError("insufficient resource observations")
    if qualifying and (duration < 3600 or len(samples) < 300):
        raise RuntimeError("long soak duration or sampling incomplete")
    identities = {name: (value["pid"], value["start_ticks"]) for name, value in samples[0]["processes"].items()}
    previous = None
    for sample in samples:
        stamp = sample["elapsed_seconds"]
        start = sample.get("observation_started_elapsed_seconds")
        if type(stamp) not in (int, float) or type(start) not in (int, float) or not math.isfinite(stamp) or not math.isfinite(start) or not 0 <= start <= stamp:
            raise RuntimeError("invalid observation time schema")
        if previous is not None and stamp <= previous:
            raise RuntimeError("observation time did not advance")
        if stamp - start > limits["maximum_sample_gap_seconds"] or (previous is not None and stamp - previous > limits["maximum_sample_gap_seconds"]):
            raise RuntimeError("resource observation gap exceeded")
        previous = stamp
        actual = {name: (value["pid"], value["start_ticks"]) for name, value in sample["processes"].items()}
        if any(type(pid) is not int or pid <= 0 or type(start_ticks) is not int or start_ticks <= 0 for pid, start_ticks in actual.values()):
            raise RuntimeError("process identity unavailable")
        if actual != identities or set(actual) != {"authority", "worker-a", "worker-b", "platform", "launcher"}:
            raise RuntimeError("process identity changed")
        if any(process.get("uids") != [10001, 10001, 10001, 10001] for process in sample["processes"].values()):
            raise RuntimeError("process non-root identities not proven")
        for field in ("rss_bytes", "threads", "fds"):
            if any(type(process.get(field)) is not int or process[field] < 0 for process in sample["processes"].values()):
                raise RuntimeError("process resource observations unavailable")
            if sum(process[field] for process in sample["processes"].values()) > limits["maximum_" + field]:
                raise RuntimeError("process resource limit exceeded")
        check_quiescent(sample["metrics"])
        if (not isinstance(sample["storage_file_bytes"], dict)
                or set(sample["storage_file_bytes"]) != {"backend", "cache-a", "cache-b", "authority", "platform", "audit"}
                or any(type(value) is not int or value < 0 for value in sample["storage_file_bytes"].values())
                or type(sample["backend_object_bytes"]) is not int or sample["backend_object_bytes"] < 0):
            raise RuntimeError("storage observations unavailable")
        if sum(sample["storage_file_bytes"].values()) > limits["maximum_local_file_bytes"] or sample["backend_object_bytes"] > limits["maximum_backend_object_bytes"]:
            raise RuntimeError("storage growth exceeded frozen capacity")
    if not trends:
        return {"samples": len(samples)}
    if samples[0]["elapsed_seconds"] > limits["maximum_sample_gap_seconds"] or samples[-1]["elapsed_seconds"] != duration:
        raise RuntimeError("observation endpoint coverage incomplete")
    window = 300 if qualifying else max(1, duration / 3)
    first = [sample for sample in samples if sample["elapsed_seconds"] <= window]
    last = [sample for sample in samples if sample["elapsed_seconds"] >= duration - window]
    if not first or not last:
        raise RuntimeError("trend endpoint window missing")
    means = {}
    for field in ("rss_bytes", "fds", "threads"):
        start = sum(sum(process[field] for process in sample["processes"].values()) for sample in first) / len(first)
        end = sum(sum(process[field] for process in sample["processes"].values()) for sample in last) / len(last)
        means[field] = {"first_window_mean": start, "last_window_mean": end, "growth": end - start}
        if end - start > limits["maximum_growth_" + field]:
            raise RuntimeError("resource growth exceeded frozen threshold")
    tail = [sample for sample in samples if sample["elapsed_seconds"] >= max(0, duration - 900)]
    timestamps = [sample["elapsed_seconds"] for sample in tail]
    memory = [sum(process["rss_bytes"] for process in sample["processes"].values()) for sample in tail]
    center = sum(timestamps) / len(timestamps)
    average = sum(memory) / len(memory)
    denominator = sum((stamp - center) ** 2 for stamp in timestamps)
    slope = sum((stamp - center) * (usage - average) for stamp, usage in zip(timestamps, memory)) / denominator if denominator else 0
    if slope > limits["maximum_tail_rss_bytes_per_second"]:
        raise RuntimeError("tail memory trend exceeded frozen threshold")
    return {"windows": means, "tail_rss_bytes_per_second": slope, "samples": len(samples)}


def verify_capacity_raw(result_path, approval_path, **options):
    directory = str(ROOT / "prototypes/v1/stress")
    sys.path.insert(0, directory)
    try:
        capacity = importlib.import_module("capacity_envelope")
        return capacity.validate_capacity_capsule(result_path, approval_path, **options)
    finally:
        sys.path.remove(directory)


def approve_capacity(arguments, source_sha256):
    if not 3600 <= arguments.duration <= 7200 or arguments.warmup != 60 or arguments.seed != 20261008 or arguments.writer_admission is not True or not 0.01 <= arguments.rate <= 1:
        raise RuntimeError("qualifying soak requires frozen duration, warmup, seed and writer admission")
    try:
        result_path = pathlib.Path(arguments.capacity_evidence).resolve(strict=True)
        approval_path = pathlib.Path(arguments.capacity_approval).resolve(strict=True)
        raw = json.loads(result_path.read_text(), object_pairs_hook=protocol.unique_object)
        approval = json.loads(approval_path.read_text(), object_pairs_hook=protocol.unique_object)
        source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        envelope = raw["safe_envelope"]
        safe = approval["rpc_rate_safe"]
        measured = envelope["safe_rpc_per_second"]
        required = float((Decimal(str(arguments.rate)) * 8 + Decimal("0.3")) * Decimal("1.2"))
        valid = (result_path != approval_path and raw["task"] == "D05/Q02"
                 and raw["status"] == "READY FOR INDEPENDENT ACCEPTANCE"
                 and envelope["status"] == "CANDIDATE FOR MAIN APPROVAL"
                 and envelope["d05_full_matrix_status"] == raw["status"]
                 and envelope["performance_pass"] is False and envelope["approval_required"] is True
                 and approval["status"] == "PASS" and approval["verified_by"] == "Main"
                 and approval["independent_review"] is True
                 and approval["result_sha256"] == image.digest(result_path)
                 and approval["image_id"] == envelope["image_id"] == arguments.image
                 and approval["writer_admission"] is True and envelope["writer_admission"] is True
                 and approval["source"] == envelope["source"] == envelope["source_hash"] == source
                 and approval["source_sha256"] == source_sha256
                 and approval["rpc_definition"] == envelope["rpc_definition"]
                 and type(safe) in (int, float) and type(measured) in (int, float)
                 and math.isfinite(safe) and math.isfinite(measured) and required <= safe <= measured)
        if not valid:
            raise RuntimeError("capacity independent approval binding or RPC margin invalid")
        evidence = raw["evidence_manifest"]
        if not isinstance(evidence, dict) or not evidence:
            raise RuntimeError("capacity raw evidence manifest missing")
        for name, entry in evidence.items():
            path = pathlib.Path(name)
            if not path.is_absolute():
                path = result_path.parent / path
            if path.stat().st_size != entry["bytes"] or image.digest(path) != entry["sha256"]:
                raise RuntimeError("capacity raw evidence changed")
        verify_capacity_raw(result_path, approval_path, image_id=arguments.image, source=source, writer_admission=True, tx_per_second=arguments.rate, source_sha256=source_sha256)
        return dict(approval, approval_sha256=image.digest(approval_path), required_rpc_with_margin=required)
    except (OSError, ValueError, KeyError, TypeError, AttributeError, subprocess.SubprocessError) as failure:
        raise RuntimeError("capacity approval unavailable or malformed") from failure


class Client:
    def __init__(self, config, journal):
        self.config = config
        self.journal = journal
        self.sequence = 0
        self.last_reply_id = None
        self.phase = "setup"
        self.transport = urllib.request.build_opener(urllib.request.ProxyHandler({}), protocol.NoRedirect(), urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=config["ca_file"])))

    def call(self, path, body, administrator=False):
        self.sequence += 1
        identifier = "soak_" + secrets.token_hex(12)
        credential = self.config["global_api_key"] if administrator else self.config["project_api_key"]
        payload = dict(body, request_id=identifier)
        action = str(body.get("sql", "")).split(" ", 1)[0].upper().rstrip(";")
        if action not in ("SELECT", "BEGIN", "COMMIT", "ROLLBACK", "INSERT", "UPDATE", "CREATE", "PRAGMA"):
            action = None
        request = urllib.request.Request(self.config["endpoint"] + path, data=json.dumps(payload).encode(), headers={"Authorization": "Bearer " + credential, "Content-Type": "application/json", "X-Request-ID": identifier}, method="POST")
        append_json(self.journal, {"event": "dispatch", "phase": self.phase, "sequence": self.sequence, "request_id": identifier, "path": path, "sql_action": action})
        started = time.monotonic()
        with self.transport.open(request, timeout=30) as response:
            encoded = response.read(8 * 1024 * 1024 + 1)
            if response.status != 200 or len(encoded) > 8 * 1024 * 1024:
                raise RuntimeError("unknown HTTP result")
            result = json.loads(encoded, object_pairs_hook=protocol.unique_object)
        if type(result.get("version")) is not int or result["version"] != 1 or result.get("request_id") != identifier or result.get("ok") is not True or result.get("error") is not None:
            raise RuntimeError("uncorrelated or incomplete acknowledgement")
        flags = {name: value for name, value in result["result"].items() if name in ("executed", "closed", "readonly", "checkpointed") and type(value) is bool} if isinstance(result["result"], dict) else {}
        append_json(self.journal, {"event": "rpc_success_reply", "phase": self.phase, "sequence": self.sequence, "request_id": identifier, "path": path, "sql_action": action, "result_flags": flags, "elapsed_seconds": time.monotonic() - started})
        self.last_reply_id = identifier
        return result["result"]

    def metrics(self):
        request = urllib.request.Request(self.config["endpoint"] + "/v1/metrics", headers={"Authorization": "Bearer " + self.config["global_api_key"]})
        with self.transport.open(request, timeout=15) as response:
            encoded = response.read(4 * 1024 * 1024 + 1)
            if response.status != 200 or len(encoded) > 4 * 1024 * 1024:
                raise RuntimeError("metrics unavailable")
            envelope = json.loads(encoded, object_pairs_hook=protocol.unique_object)
        if envelope.get("version") != 1 or envelope.get("ok") is not True or envelope.get("error") is not None:
            raise RuntimeError("invalid metrics reply")
        return envelope["result"]

    def opened(self):
        result = self.call("/v1/sql/open", {"database": self.config["database"]})
        if not isinstance(result, dict) or not isinstance(result.get("session"), str) or not result["session"] or result.get("readonly") is not False:
            raise RuntimeError("writer Open acknowledgement missing")
        return result["session"]

    def execute(self, session, sql):
        result = self.call("/v1/sql/execute", {"session": session, "sql": sql})
        if result != {"executed": True} or result["executed"] is not True:
            raise RuntimeError("Execute acknowledgement missing")

    def query(self, session, sql):
        result = self.call("/v1/sql/query", {"session": session, "sql": sql})
        if not isinstance(result, list):
            raise RuntimeError("Query rows missing")
        return result

    def closed(self, session):
        result = self.call("/v1/sql/close", {"session": session})
        if result != {"closed": True} or result["closed"] is not True:
            raise RuntimeError("Close acknowledgement missing")


class Soak:
    def __init__(self, arguments):
        self.arguments = arguments
        self.fixture = image.Verification(arguments)
        self.samples = []
        self.commits = []
        self.rollbacks = 0
        self.dropped = 0
        self.random = random.Random(arguments.seed)
        self.sources = [pathlib.Path(__file__), pathlib.Path(__file__).with_name("verify-image.py"), ROOT / "prototypes/v1/platform/dev.py", ROOT / "prototypes/v1/stress/capacity_envelope.py"]
        self.source_start = {str(path.relative_to(ROOT)): image.digest(path) for path in self.sources}
        self.result = {"pass": False, "qualifies_long_soak": False, "mode": arguments.mode, "duration_target_seconds": arguments.duration, "warmup_seconds": arguments.warmup, "offered_transactions_per_second": arguments.rate, "seed": arguments.seed, "limits": dict(LIMITS)}

    def backend_bytes(self, local_bytes):
        if self.arguments.backend == "local":
            return local_bytes
        endpoint = urllib.parse.urlsplit(self.arguments.s3_observe_endpoint)
        if endpoint.scheme not in ("http", "https") or endpoint.hostname != "127.0.0.1" or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment or endpoint.path not in ("", "/"):
            raise RuntimeError("S3 observations require explicit loopback endpoint")
        bucket = self.fixture.environment["S3_BUCKET"]
        if not bucket.startswith("peri-test-soak-"):
            raise RuntimeError("only an explicit separate soak bucket may be observed")
        observed = datetime.now(timezone.utc)
        stamp, day = observed.strftime("%Y%m%dT%H%M%SZ"), observed.strftime("%Y%m%d")
        region = self.fixture.environment.get("S3_REGION", "us-east-1")
        path, query = "/" + bucket, "list-type=2&max-keys=1000"
        payload = hashlib.sha256(b"").hexdigest()
        headers = {"host": endpoint.netloc, "x-amz-content-sha256": payload, "x-amz-date": stamp}
        token = self.fixture.environment.get("S3_SESSION_TOKEN")
        if token:
            headers["x-amz-security-token"] = token
        names = sorted(headers)
        signed = ";".join(names)
        canonical = "\n".join(("GET", path, query, "".join(name + ":" + headers[name] + "\n" for name in names), signed, payload))
        scope = "/".join((day, region, "s3", "aws4_request"))
        signing = "\n".join(("AWS4-HMAC-SHA256", stamp, scope, hashlib.sha256(canonical.encode()).hexdigest()))
        secret = ("AWS4" + self.fixture.environment["S3_SECRET_ACCESS_KEY"]).encode()
        for part in (day, region, "s3", "aws4_request"):
            secret = hmac.new(secret, part.encode(), hashlib.sha256).digest()
        signature = hmac.new(secret, signing.encode(), hashlib.sha256).hexdigest()
        headers["Authorization"] = "AWS4-HMAC-SHA256 Credential=" + self.fixture.environment["S3_ACCESS_KEY_ID"] + "/" + scope + ", SignedHeaders=" + signed + ", Signature=" + signature
        transport = urllib.request.build_opener(urllib.request.ProxyHandler({}), protocol.NoRedirect())
        request = urllib.request.Request(urllib.parse.urlunsplit((endpoint.scheme, endpoint.netloc, path, query, "")), headers=headers)
        with transport.open(request, timeout=10) as response:
            data = response.read(1024 * 1024 + 1)
            if len(data) > 1024 * 1024 or response.status != 200:
                raise RuntimeError("backend object observations unavailable")
        result = xml.fromstring(data)
        if result.findtext("{*}IsTruncated") != "false":
            raise RuntimeError("backend object listing truncated")
        return sum(int(item.findtext("{*}Size")) for item in result.findall("{*}Contents"))

    def sample(self, client, origin, observations):
        began = time.monotonic() - origin
        observed = {}
        try:
            observed = json.loads(self.fixture.docker("exec", self.fixture.container, "python3", "-c", PROBE).stdout)
            metrics = client.metrics()
            observed["metrics"] = metrics_evidence(metrics)
            check_quiescent(metrics)
            sample = dict(resource_evidence(observed), **{"observation_started_elapsed_seconds": began, "committed_transactions": len(self.commits), "rollback_transactions": self.rollbacks, "backend_object_bytes": self.backend_bytes(observed["storage_file_bytes"]["backend"]), "metrics": metrics_evidence(metrics)})
            sample["elapsed_seconds"] = time.monotonic() - origin
            append_json(observations, sample)
            self.samples.append(sample)
            check_quiescent(sample["metrics"])
            if len(self.samples) > 1:
                evaluate(self.samples, sample["elapsed_seconds"], self.result["limits"], False, trends=False)
        except BaseException as failure:
            partial = resource_evidence(observed)
            if isinstance(observed, dict) and "metrics" in observed:
                partial["metrics"] = observed["metrics"]
            append_json(observations, {"event": "observation_failure", "observation_started_elapsed_seconds": began, "elapsed_seconds": time.monotonic() - origin, "failure_type": type(failure).__name__, "partial": partial})
            os.fsync(observations.fileno())
            raise

    def setup(self, client):
        session = client.opened()
        client.execute(session, "CREATE TABLE soak_accounts(id INTEGER PRIMARY KEY,balance INTEGER NOT NULL);INSERT INTO soak_accounts VALUES(1,50000),(2,50000);CREATE TABLE soak_ledger(seq INTEGER PRIMARY KEY,delta INTEGER NOT NULL);CREATE TABLE soak_probe(seq INTEGER PRIMARY KEY);CREATE TABLE soak_seed(id INTEGER PRIMARY KEY,value INTEGER NOT NULL);")
        for start in range(1, 1001, 100):
            values = ",".join("(" + str(identifier) + "," + str(identifier * 3) + ")" for identifier in range(start, start + 100))
            client.execute(session, "INSERT INTO soak_seed VALUES" + values)
        if client.query(session, "SELECT COUNT(*),SUM(id) FROM soak_seed") != [[1000, 500500]]:
            raise RuntimeError("seed oracle failed")
        client.closed(session)

    def transaction(self, client, sequence, committed):
        session = client.opened()
        identifier = self.random.randint(1, 1000)
        if client.query(session, "SELECT value FROM soak_seed WHERE id=" + str(identifier)) != [[identifier * 3]]:
            raise RuntimeError("point read oracle failed")
        client.execute(session, "BEGIN")
        if sequence % 5 == 0:
            client.execute(session, "INSERT INTO soak_probe VALUES(" + str(sequence) + ")")
            client.execute(session, "ROLLBACK")
            if client.query(session, "SELECT COUNT(*) FROM soak_probe") != [[0]]:
                raise RuntimeError("rollback oracle failed")
            self.rollbacks += 1
        else:
            amount = self.random.randint(1, 9)
            client.execute(session, "UPDATE soak_accounts SET balance=balance-" + str(amount) + " WHERE id=1;UPDATE soak_accounts SET balance=balance+" + str(amount) + " WHERE id=2;INSERT INTO soak_ledger VALUES(" + str(sequence) + "," + str(amount) + ")")
            client.execute(session, "COMMIT")
            self.commits.append([sequence, amount])
            append_json(committed, {"sequence": sequence, "amount": amount, "phase": client.phase, "request_id": client.last_reply_id, "proof": "separate_COMMIT_ACK"})
            os.fsync(committed.fileno())
        client.closed(session)

    def oracle(self, client):
        session = client.opened()
        ledger = client.query(session, "SELECT seq,delta FROM soak_ledger ORDER BY seq")
        if ledger != self.commits:
            raise RuntimeError("committed prefix missing or duplicated")
        total = sum(amount for sequence, amount in self.commits)
        accounts = client.query(session, "SELECT id,balance FROM soak_accounts ORDER BY id")
        if accounts != [[1, 50000 - total], [2, 50000 + total]]:
            raise RuntimeError("account conservation failed")
        integrity = client.query(session, "PRAGMA integrity_check")
        rollback = client.query(session, "SELECT COUNT(*) FROM soak_probe")
        if integrity != [["ok"]] or rollback != [[0]]:
            raise RuntimeError("durable integrity or rollback failed")
        client.closed(session)
        if client.phase == "final_oracle":
            image.private_json(self.fixture.directory / "oracle.json", {"ledger_rows": ledger, "account_rows": accounts, "integrity_rows": integrity, "rollback_rows": rollback, "proof": "fresh_session_after_acknowledged_Close", "final_close_request_id": client.last_reply_id})

    def workload(self):
        self.fixture.smoke_dispatched = True
        with private_stream(self.fixture.directory / "requests.jsonl") as journal, private_stream(self.fixture.directory / "observations.jsonl") as observations, private_stream(self.fixture.directory / "commit-acks.jsonl") as committed:
            client = Client(json.loads((self.fixture.fixture / "connection.json").read_text()), journal)
            self.setup(client)
            client.phase = "warmup"
            warmup_end = time.monotonic() + self.arguments.warmup
            sequence = 0
            while time.monotonic() < warmup_end:
                sequence += 1
                self.transaction(client, sequence, committed)
                time.sleep(min(1 / self.arguments.rate, max(0, warmup_end - time.monotonic())))
            warmup_sequence = sequence
            self.result["warmup_transactions"] = sequence
            client.phase = "measure"
            started = time.monotonic()
            self.sample(client, started, observations)
            deadline = started + self.arguments.duration
            offer = started
            next_sample = started + 10
            next_checkpoint = started + 60
            while time.monotonic() < deadline:
                time.sleep(max(0, min(offer, next_sample, next_checkpoint, deadline) - time.monotonic()))
                if time.monotonic() >= deadline:
                    break
                current = time.monotonic()
                if current >= next_checkpoint:
                    result = client.call("/v1/databases/checkpoint", {"database": client.config["database"]})
                    if not isinstance(result, dict) or result.get("checkpointed") is not True:
                        raise RuntimeError("checkpoint acknowledgement missing")
                    self.oracle(client)
                    while next_checkpoint <= time.monotonic():
                        next_checkpoint += 60
                if time.monotonic() >= next_sample:
                    self.sample(client, started, observations)
                    while next_sample <= time.monotonic():
                        next_sample += 10
                if time.monotonic() >= offer:
                    sequence += 1
                    self.transaction(client, sequence, committed)
                    offer += 1 / self.arguments.rate
                    while offer < time.monotonic():
                        self.dropped += 1
                        offer += 1 / self.arguments.rate
            elapsed = time.monotonic() - started
            self.result["measurement_elapsed_seconds"] = elapsed
            self.sample(client, started, observations)
            if self.samples[-1]["elapsed_seconds"] < self.arguments.duration:
                raise RuntimeError("measurement endpoint not reached")
            client.phase = "final_oracle"
            self.oracle(client)
            self.sample(client, started, observations)
            elapsed = self.samples[-1]["elapsed_seconds"]
            self.result["trend"] = evaluate(self.samples, elapsed, self.result["limits"], self.arguments.mode == "soak")
            completed = sequence - warmup_sequence
            offered = completed + self.dropped
            self.result.update({"elapsed_seconds": elapsed, "offered_transactions": offered, "completed_transactions": completed, "committed_transactions_including_warmup": len(self.commits), "rollback_transactions_including_warmup": self.rollbacks, "generator_dropped": self.dropped, "durable_prefix_verified": True, "unknown": 0})
            if completed < math.floor(self.arguments.duration * self.arguments.rate * 0.8) or self.dropped > offered * 0.2:
                raise RuntimeError("frozen workload progress minimum not met")
            self.fixture.smoke_proven = True

    def run(self):
        stage = "approval"
        try:
            if self.arguments.mode == "soak":
                self.result["capacity_approval"] = approve_capacity(self.arguments, self.source_start)
            stage = "prepare"
            self.fixture.prepare()
            stage = "ready"
            self.fixture.ready()
            if self.arguments.mode == "soak" and self.result["capacity_approval"]["image_id"] != self.fixture.report["image"]["Id"]:
                raise RuntimeError("capacity envelope belongs to a different image")
            stage = "workload"
            self.workload()
        except BaseException as failure:
            self.result["failure_type"] = type(failure).__name__
            self.result["failure_stage"] = stage
            if type(failure) is RuntimeError and str(failure) == "image launcher differs from current full launcher":
                self.result["failure_code"] = "image_launcher_source_mismatch"
            if type(failure) is RuntimeError and str(failure) == "frozen workload progress minimum not met":
                self.result["failure_code"] = "workload_progress_below_frozen_minimum"
        finally:
            try:
                self.fixture.cleanup()
            except BaseException as failure:
                self.result["failure_type"] = type(failure).__name__
                self.result["failure_stage"] = "cleanup"
                self.fixture.smoke_proven = False
            self.result["runtime"] = self.fixture.report
            eligible = "failure_type" not in self.result and self.fixture.smoke_proven and self.fixture.report["cleanup"] == "removed_owned_container"
            self.result["pass"] = False
            destination = self.fixture.directory / "soak-result.json"
            try:
                self.publish(destination, eligible)
            except BaseException as failure:
                self.result.update({"pass": False, "qualifies_long_soak": False, "publication_failure_type": type(failure).__name__})
                for name in (".soak-result.pending.json", "soak-manifest.json", "soak-result.json"):
                    try:
                        (self.fixture.directory / name).unlink(missing_ok=True)
                    except OSError:
                        pass
                try:
                    image.private_json(destination, self.result)
                except OSError:
                    pass
        return self.result["pass"]

    def publish(self, destination, eligible):
        image.private_json(destination, self.result)
        files = [self.fixture.directory / name for name in ("requests.jsonl", "observations.jsonl", "commit-acks.jsonl", "oracle.json", "authority.lock")]
        if eligible and all(path.is_file() for path in files):
            if (self.fixture.directory / "authority.lock").read_bytes() != b"clean\n":
                raise RuntimeError("authority clean marker missing")
            if self.arguments.mode == "soak" and approve_capacity(self.arguments, self.source_start) != self.result["capacity_approval"]:
                raise RuntimeError("capacity approval changed during execution")
            candidate = dict(self.result, **{"pass": True, "qualifies_long_soak": self.arguments.mode == "soak"})
            pending = self.fixture.directory / ".soak-result.pending.json"
            image.private_json(pending, candidate)
            if any(image.digest(path) != self.source_start[str(path.relative_to(ROOT))] for path in self.sources):
                raise RuntimeError("soak source changed during execution")
            manifest = {"artifact_sha256": {path.name: image.digest(path) for path in files}, "source_sha256": self.source_start, "result_sha256": image.digest(pending)}
            manifest_path = self.fixture.directory / "soak-manifest.json"
            image.private_json(manifest_path, manifest)
            if json.loads(manifest_path.read_text()) != manifest or image.digest(pending) != manifest["result_sha256"] or any(image.digest(path) != manifest["artifact_sha256"][path.name] for path in files) or any(image.digest(path) != manifest["source_sha256"][str(path.relative_to(ROOT))] for path in self.sources):
                raise RuntimeError("soak evidence changed before publication")
            os.replace(pending, destination)
            self.result = candidate


def main():
    parser = argparse.ArgumentParser(description="Fresh image bounded soak; no retries or shared fixture use")
    parser.add_argument("--image", required=True)
    parser.add_argument("--backend", choices=("local", "s3"), default="local")
    parser.add_argument("--writer-admission", action="store_true")
    parser.add_argument("--mode", choices=("smoke", "soak"), default="soak")
    parser.add_argument("--duration", type=int, default=3600)
    parser.add_argument("--warmup", type=int, default=60)
    parser.add_argument("--s3-observe-endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--capacity-evidence")
    parser.add_argument("--capacity-approval")
    parser.add_argument("--rate", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=20261008)
    parser.add_argument("--ready-timeout", type=int, default=120)
    parser.add_argument("--smoke-timeout", type=int, default=300)
    parser.add_argument("--stop-timeout", type=int, default=90)
    arguments = parser.parse_args()
    if not 0.01 <= arguments.rate <= 1 or not 20 <= arguments.duration <= 7200 or not 0 <= arguments.warmup <= 120 or arguments.mode == "soak" and (arguments.duration < 3600 or arguments.warmup != 60 or arguments.seed != 20261008 or not arguments.writer_admission or not arguments.capacity_evidence or not arguments.capacity_approval) or min(arguments.ready_timeout, arguments.smoke_timeout, arguments.stop_timeout) <= 0:
        parser.error("invalid frozen resource/duration/rate bounds")
    signal.signal(signal.SIGTERM, image.interrupted)
    runner = Soak(arguments)
    passed = runner.run()
    print(json.dumps({"pass": passed, "qualifies_long_soak": runner.result["qualifies_long_soak"], "results": str(runner.fixture.directory), "cleanup": runner.fixture.report["cleanup"]}))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
