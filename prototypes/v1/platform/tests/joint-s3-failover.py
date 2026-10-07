import argparse
import ctypes
import io
import json
import os
import pathlib
import secrets
import shutil
import signal
import ssl
import stat
import struct
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from unittest.mock import Mock, patch


FIXTURE = pathlib.Path("/private/tmp/peri-stress-joint-xs2gO5jO")
MAX_BYTES = 1024 * 1024
PLAN = ["private_fixture_and_exact_process_preflight", "create_new_isolated_database_once", "commit_two_acknowledged_transactions", "acknowledge_uncommitted_insert_not_durable", "match_live_authority_and_placement", "kill_only_verified_owner", "wait_original_pid_gone", "delete_only_verified_owner_cache", "poll_readonly_state_twenty_seconds", "fresh_session_recover_and_commit", "old_session_denied_without_close_or_retry"]


class Failure(RuntimeError):
    pass


class PendingIdentity:
    def __init__(self, metadata):
        self.metadata = metadata


def require(condition, code):
    if not condition:
        raise Failure(code)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate_json")
        result[key] = value
    return result


def owned_path(path, directory=False, private=False):
    path = pathlib.Path(path)
    require(path.is_absolute() and path.resolve() == path, "noncanonical_path")
    for ancestor in (path, *path.parents):
        require(not ancestor.is_symlink(), "symlink_path")
    info = path.lstat()
    require(info.st_uid == os.getuid(), "foreign_path_owner")
    require(stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode), "wrong_path_type")
    if private:
        require(stat.S_IMODE(info.st_mode) == (0o700 if directory else 0o600), "nonprivate_path")
    return info


def private_json(path):
    before = owned_path(path, private=True)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        require(os.path.samestat(before, os.fstat(stream.fileno())), "file_changed")
        encoded = stream.read(MAX_BYTES + 1)
    require(len(encoded) <= MAX_BYTES, "oversized_private_file")
    return json.loads(encoded, object_pairs_hook=unique_object)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, destination):
        return None


class API:
    def __init__(self, endpoint, credential, ca_file=None):
        parsed = urllib.parse.urlsplit(endpoint)
        require(parsed.scheme == ("https" if ca_file else "http") and parsed.hostname == "127.0.0.1" and parsed.port and not parsed.username and not parsed.password and not parsed.query and not parsed.fragment and parsed.path == "", "invalid_endpoint")
        handlers = [urllib.request.ProxyHandler({}), NoRedirect()]
        if ca_file:
            owned_path(pathlib.Path(ca_file))
            handlers.append(urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=ca_file)))
        self.client = urllib.request.build_opener(*handlers)
        self.endpoint, self.credential = endpoint, credential

    def call(self, path, body, timeout=10, denial=False, authority=False):
        identifier = "joint_" + secrets.token_hex(12)
        payload = dict(body, request_id=identifier, **({"version": 1} if authority else {}))
        request = urllib.request.Request(self.endpoint + path, data=json.dumps(payload).encode(), headers={"Authorization": "Bearer " + self.credential, "Content-Type": "application/json"}, method="POST")
        try:
            response = self.client.open(request, timeout=timeout)
        except urllib.error.HTTPError as response_error:
            response = response_error
        except (urllib.error.URLError, OSError):
            raise Failure("dispatched_outcome_unknown_no_retry") from None
        with response:
            encoded = response.read(MAX_BYTES + 1)
            status_code = response.code
        require(len(encoded) <= MAX_BYTES, "oversized_reply_unknown")
        result = json.loads(encoded, object_pairs_hook=unique_object)
        fields = {"version", "request_id", "ok", "result", "error"} | ({"precondition_failed"} if authority else set())
        require(isinstance(result, dict) and set(result) == fields and type(result["version"]) is int and result["version"] == 1 and result["request_id"] == identifier, "reply_identity_unknown")
        if authority:
            require(result["precondition_failed"] is False, "authority_rejected")
        if denial:
            require(status_code == 409 and result["ok"] is False and result["result"] is None and isinstance(result["error"], dict) and result["error"].get("code") == "session_moved", "stale_session_not_fenced")
        else:
            require(status_code == 200 and result["ok"] is True and result["error"] is None, "rpc_rejected_or_unknown_no_retry")
        return result["result"]


def process_argv(pid):
    if sys.platform == "darwin":
        library = ctypes.CDLL(None, use_errno=True)
        mib = (ctypes.c_int * 3)(1, 49, pid)
        size = ctypes.c_size_t()
        if library.sysctl(mib, 3, None, ctypes.byref(size), None, 0) != 0:
            require(ctypes.get_errno() == 3, "process_argv_unavailable")
            return None
        require(4 < size.value <= 8 * MAX_BYTES, "process_argv_size")
        buffer = ctypes.create_string_buffer(size.value)
        require(library.sysctl(mib, 3, buffer, ctypes.byref(size), None, 0) == 0, "process_changed_during_argv")
        encoded = buffer.raw[:size.value]
        argc = struct.unpack_from("i", encoded)[0]
        require(0 < argc <= 32, "process_argc")
        offset = encoded.index(b"\0", 4) + 1
        while offset < len(encoded) and encoded[offset] == 0:
            offset += 1
        return [argument.decode() for argument in encoded[offset:].split(b"\0", argc)[:argc]]
    require(sys.platform.startswith("linux"), "unsupported_process_inspection")
    path = pathlib.Path("/proc") / str(pid) / "cmdline"
    try:
        return path.read_bytes().rstrip(b"\0").decode().split("\0")
    except FileNotFoundError:
        return None


def process_metadata(pid):
    require(type(pid) is int and pid > 1, "invalid_pid")
    result = subprocess.run(["ps", "-p", str(pid), "-o", "uid=,ppid=,lstart="], capture_output=True, text=True, timeout=2)
    if result.returncode == 1 and not result.stdout.strip():
        return None
    require(result.returncode == 0, "process_metadata_unavailable")
    fields = result.stdout.strip().split(maxsplit=2)
    require(len(fields) == 3, "invalid_process_metadata")
    return (int(fields[0]), int(fields[1]), fields[2])


def process_identity(pid):
    before = process_metadata(pid)
    if before is None:
        return None
    try:
        argv = process_argv(pid)
    except Failure as error:
        if str(error) not in ("process_argv_unavailable", "process_changed_during_argv"):
            raise
        argv = None
    after = process_metadata(pid)
    if after is None:
        return None
    require(after == before, "process_identity_changed")
    if not argv:
        return PendingIdentity(before)
    try:
        session = os.getsid(pid)
    except ProcessLookupError:
        after = process_metadata(pid)
        if after is None:
            return None
        require(after == before, "process_identity_changed")
        return PendingIdentity(before)
    return (before, tuple(argv), session)


def worker_proof(root, topology, launch, binary, worker, inspector=process_identity):
    require(worker in ("worker-a", "worker-b"), "unknown_worker")
    cache = root / ("cache-a" if worker == "worker-a" else "cache-b")
    info = owned_path(cache, directory=True, private=True)
    pid = topology["processes"][worker]["pid"]
    identity = inspector(pid)
    expected = (binary, "owned-worker", topology["authority"], str(cache), worker, launch["workers"][worker]["address"])
    require(identity and identity[0][0] == os.getuid() and identity[0][1] == topology["launcher_pid"] and identity[1] == expected and identity[2] == pid, "worker_identity_mismatch")
    require(launch["workers"][worker]["cache"] == str(cache) and topology["workers"][worker] == "http://" + expected[-1], "worker_cache_or_address_mismatch")
    return pid, identity, cache, (info.st_dev, info.st_ino)


def kill_and_wait(proof, inspector=process_identity, killer=os.kill, evidence=None):
    pid, identity, cache, cache_identity = proof
    require(inspector(pid) == identity, "worker_changed_before_signal")
    if evidence is not None:
        evidence.update(stage="signal_dispatch", signal_sent=False, waiting_uncertain=True)
    killer(pid, signal.SIGKILL)
    if evidence is not None:
        evidence.update(stage="signal_sent_waiting_pid_gone", signal_sent=True)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        observed = inspector(pid)
        if observed is None:
            if evidence is not None:
                evidence.update(stage="original_pid_gone", original_pid_gone=True, waiting_uncertain=False)
            return
        require(observed.metadata == identity[0] if isinstance(observed, PendingIdentity) else observed == identity, "pid_reused_no_cache_delete")
        time.sleep(0.1)
    raise Failure("worker_not_reaped_no_cache_delete")


def delete_cache(root, proof):
    pid, identity, cache, expected = proof
    require(process_identity(pid) is None, "pid_present_no_cache_delete")
    root_info = owned_path(root, directory=True, private=True)
    info = owned_path(cache, directory=True, private=True)
    require(cache.parent == root and cache.name in ("cache-a", "cache-b") and (info.st_dev, info.st_ino) == expected, "cache_identity_changed")
    for directory, folders, files in os.walk(cache, followlinks=False):
        for name in folders + files:
            entry = pathlib.Path(directory) / name
            owned_path(entry, directory=entry.is_dir())
    require(shutil.rmtree.avoids_symlink_attacks, "descriptor_safe_delete_unavailable")
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        require(os.path.samestat(root_info, os.fstat(descriptor)), "root_changed")
        shutil.rmtree(cache.name, dir_fd=descriptor)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def binding_from_status(status, database):
    placement = status["placement"]
    binding = (placement.get("grant") or {}).get("binding")
    if placement["phase"] != "active" or not binding:
        return None
    require(placement["database"] == database and binding["database"] == database and placement["epoch"] == binding["epoch"] and placement["desired_worker"] == binding["worker_id"] and binding["role"] == "writer", "inconsistent_placement")
    return binding


def same_owner(binding, state):
    current = state.get("binding")
    fields = ("database", "root_incarnation", "branch_incarnation", "authority_term", "worker_id", "epoch", "lease_id", "grant_id", "role")
    return binding and current and state["mode"] == "writer" and state["remaining_ttl_ms"] > 0 and all(binding[name] == current[name] for name in fields)


def launcher_directory(argv):
    values = []
    for position, argument in enumerate(argv):
        if argument == "--run-dir":
            require(position + 1 < len(argv), "launcher_run_directory_missing")
            values.append(argv[position + 1])
        elif argument.startswith("--run-dir="):
            values.append(argument.split("=", 1)[1])
    allowed = {str(FIXTURE), "/tmp/" + FIXTURE.name}
    require(len(values) == 1 and values[0] in allowed and pathlib.Path(values[0]).resolve() == FIXTURE, "launcher_run_directory_mismatch")
    return FIXTURE


def fixture(root, binary):
    require(root == FIXTURE, "fixture_not_authorized")
    owned_path(root, directory=True, private=True)
    require(pathlib.Path(binary).is_absolute(), "absolute_binary_required")
    owned_path(pathlib.Path(binary).resolve())
    connection, topology, launch, config = [private_json(root / name) for name in ("connection.json", "topology.json", "launch.json", "platform.json")]
    require(launch["backend"] == "s3" and launch["tls"] is True and launch["storage_identity"]["backend"] == "s3", "not_encrypted_s3_tls_fixture")
    require(topology["gateway"] == connection["endpoint"] == "https://" + launch["listen"] and topology["authority"] == config["controller"]["authority_url"] == "http://" + launch["authority"], "fixture_endpoint_mismatch")
    launcher = process_identity(topology["launcher_pid"])
    require(launcher and launcher[0][0] == os.getuid() and launcher_directory(launcher[1]) == root, "launcher_identity_mismatch")
    authority = process_identity(topology["processes"]["authority"]["pid"])
    require(authority and authority[0][0] == os.getuid() and authority[0][1] == topology["launcher_pid"] and authority[1] == (binary, "authority", "s3", launch["authority"], str(root / "authority")), "authority_identity_mismatch")
    proofs = {worker: worker_proof(root, topology, launch, binary, worker) for worker in ("worker-a", "worker-b")}
    api = API(connection["endpoint"], connection["project_api_key"], connection["ca_file"])
    control = API(topology["authority"], config["controller"]["authority_token"])
    return api, control, proofs


def run_joint(root, binary, evidence):
    api, control, proofs = fixture(root, binary)
    database = {"namespace": {"tenant": "local_default", "database": "joint_crash_" + secrets.token_hex(6)}, "branch": "main"}
    evidence.update(database=database, old_session_closed=False, uncertain_operations_retried=False, remaining_toctou=True)
    evidence["stage"] = "create_isolated_database"
    api.call("/v1/databases/create", {"database": database, "project": "default", "role": "writer"})
    opened = api.call("/v1/sql/open", {"database": database})
    require(opened["readonly"] is False, "unexpected_readonly_session")
    old_session = opened["session"]

    def execute(session, sql):
        result = api.call("/v1/sql/execute", {"session": session, "sql": sql})
        require(result == {"executed": True}, "execute_ack_missing")

    execute(old_session, "CREATE TABLE joint_ack_ledger(id INTEGER PRIMARY KEY)")
    for number in (1, 2):
        evidence["stage"] = "commit_ack_" + str(number)
        execute(old_session, "BEGIN")
        execute(old_session, "INSERT INTO joint_ack_ledger VALUES(" + str(number) + ")")
        execute(old_session, "COMMIT")
        evidence["durable_commit_ack_count"] = number
    evidence["stage"] = "uncommitted_statement_ack"
    execute(old_session, "BEGIN")
    execute(old_session, "INSERT INTO joint_ack_ledger VALUES(999)")
    evidence["uncommitted_statement_ack_not_durable"] = True
    evidence["stage"] = "confirm_owner_and_kill"
    status = api.call("/v1/databases/status", {"database": database})
    before = binding_from_status(status, database)
    state = control.call("/", {"command": {"command": "get_database_state", "database": database}}, authority=True)
    require(same_owner(before, state), "owner_not_confirmed_before_signal")
    proof = proofs[before["worker_id"]]
    evidence.update(killed_worker=before["worker_id"], killed_pid=proof[0], verified_uid=proof[1][0][0], epoch_before=before["epoch"])
    kill_and_wait(proof, evidence=evidence)
    evidence["original_pid_gone"] = True
    evidence["stage"] = "delete_only_dead_owner_cache"
    delete_cache(root, proof)
    evidence["owner_cache_deleted"] = True
    evidence["stage"] = "readonly_failover_poll"
    deadline = time.monotonic() + 20
    after = None
    while time.monotonic() < deadline:
        status = api.call("/v1/databases/status", {"database": database}, timeout=min(3, deadline - time.monotonic()))
        candidate = binding_from_status(status, database)
        if candidate and candidate["worker_id"] != before["worker_id"] and candidate["epoch"] > before["epoch"]:
            require(time.monotonic() < deadline, "failover_deadline")
            state = control.call("/", {"command": {"command": "get_database_state", "database": database}}, timeout=min(3, deadline - time.monotonic()), authority=True)
            if same_owner(candidate, state):
                after = candidate
                break
        time.sleep(0.2)
    require(after is not None, "failover_deadline")
    require(after["root_incarnation"] == before["root_incarnation"] and after["branch_incarnation"] == before["branch_incarnation"], "unexpected_incarnation_change")
    evidence.update(epoch_after=after["epoch"], recovered_worker=after["worker_id"], authoritative_epoch_barrier=True)
    evidence["stage"] = "fresh_session_recovery"
    fresh = api.call("/v1/sql/open", {"database": database})
    require(fresh["session"] != old_session and fresh["readonly"] is False, "fresh_session_required")
    session = fresh["session"]
    require(api.call("/v1/sql/query", {"session": session, "sql": "SELECT id FROM joint_ack_ledger ORDER BY id"}) == [[1], [2]], "durable_ledger_mismatch")
    require(api.call("/v1/sql/query", {"session": session, "sql": "PRAGMA integrity_check"}) == [["ok"]], "native_integrity_failure")
    execute(session, "BEGIN")
    execute(session, "INSERT INTO joint_ack_ledger VALUES(3)")
    execute(session, "COMMIT")
    require(api.call("/v1/sql/query", {"session": session, "sql": "SELECT id FROM joint_ack_ledger ORDER BY id"}) == [[1], [2], [3]], "post_failover_commit_mismatch")
    require(api.call("/v1/sql/close", {"session": session}) == {"closed": True}, "fresh_close_ack_missing")
    evidence["stage"] = "old_session_fence"
    api.call("/v1/sql/query", {"session": old_session, "sql": "SELECT id FROM joint_ack_ledger ORDER BY id"}, denial=True)
    evidence.update(pass_result=True, durable_commit_ack_count=3, integrity_verified=True, stale_session_status=409, stale_session_code="session_moved", stage="complete")


def save_evidence(root, evidence):
    owned_path(root, directory=True, private=True)
    encoded = json.dumps(evidence, sort_keys=True).encode()
    require(len(encoded) <= MAX_BYTES, "evidence_too_large")
    path = root / ("joint-s3-failover-" + secrets.token_hex(8) + ".json")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    return path


class SafetyTests(unittest.TestCase):
    def test_only_exact_system_run_directory_aliases(self):
        for directory in (str(FIXTURE), "/tmp/" + FIXTURE.name):
            for argv in (("python", "dev.py", "--run-dir", directory), ("python", "dev.py", "--run-dir=" + directory)):
                with self.subTest(argv=argv):
                    self.assertEqual(launcher_directory(argv), FIXTURE)
        for directory in ("/tmp/../tmp/" + FIXTURE.name, "/tmp//" + FIXTURE.name, str(FIXTURE) + "/.", str(FIXTURE) + "/", "/private/var/tmp/" + FIXTURE.name, "../" + FIXTURE.name):
            with self.subTest(directory=directory), self.assertRaises(Failure):
                launcher_directory(("python", "dev.py", "--run-dir=" + directory))
        with self.assertRaises(Failure):
            launcher_directory(("python", "dev.py", "--run-dir", str(FIXTURE), "--run-dir=" + str(FIXTURE)))

    def test_private_files_and_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory).resolve()
            path = root / "private.json"
            path.write_text('{"safe":true}')
            path.chmod(0o600)
            self.assertEqual(private_json(path), {"safe": True})
            alias = root / "alias.json"
            alias.symlink_to(path)
            with self.assertRaises(Failure):
                private_json(alias)
            path.chmod(0o644)
            with self.assertRaises(Failure):
                private_json(path)

    def test_exact_signal_identity_and_reuse_refusal(self):
        identity = ((os.getuid(), 12, "start"), ("binary", "owned-worker"), 123)
        proof = (123, identity, pathlib.Path("/not-used"), (1, 1))
        killer = Mock()
        kill_and_wait(proof, inspector=Mock(side_effect=[identity, None]), killer=killer)
        killer.assert_called_once_with(123, signal.SIGKILL)
        killer.reset_mock()
        with self.assertRaises(Failure):
            kill_and_wait(proof, inspector=Mock(return_value=("reused",)), killer=killer)
        killer.assert_not_called()
        with self.assertRaises(Failure):
            kill_and_wait(proof, inspector=Mock(side_effect=[identity, ("reused",)]), killer=killer)
        self.assertEqual(killer.call_count, 1)

    def test_dying_argv_requires_metadata_gone_or_pending(self):
        metadata = (os.getuid(), 12, "start")
        for after in (None, metadata, (os.getuid(), 12, "new-start")):
            with self.subTest(after=after), patch.dict(process_identity.__globals__, {"process_metadata": Mock(side_effect=[metadata, after]), "process_argv": Mock(side_effect=Failure("process_argv_unavailable"))}):
                if after not in (None, metadata):
                    with self.assertRaisesRegex(Failure, "identity_changed"):
                        process_identity(123)
                else:
                    result = process_identity(123)
                    self.assertIsNone(result) if after is None else self.assertEqual(result.metadata, metadata)

    def test_dying_pending_wait_gone_reuse_and_timeout(self):
        metadata = (os.getuid(), 12, "start")
        identity = (metadata, ("binary", "owned-worker"), 123)
        proof = (123, identity, pathlib.Path("/not-used"), (1, 1))
        for observed in (None, PendingIdentity((os.getuid(), 12, "reused")), "timeout"):
            evidence, killer = {}, Mock()
            inspector = Mock(side_effect=[identity, PendingIdentity(metadata), observed])
            with self.subTest(observed=type(observed).__name__), patch.object(time, "sleep"), patch.object(time, "monotonic", side_effect=[0, 0, 11] if observed == "timeout" else [0, 0, 1]):
                if observed is None:
                    kill_and_wait(proof, inspector=inspector, killer=killer, evidence=evidence)
                    self.assertTrue(evidence["original_pid_gone"])
                    self.assertFalse(evidence["waiting_uncertain"])
                else:
                    with self.assertRaisesRegex(Failure, "not_reaped|pid_reused"):
                        kill_and_wait(proof, inspector=inspector, killer=killer, evidence=evidence)
                    self.assertTrue(evidence["waiting_uncertain"])
                    self.assertNotIn("original_pid_gone", evidence)
                self.assertTrue(evidence["signal_sent"])
                killer.assert_called_once_with(123, signal.SIGKILL)

    def test_symlink_cache_never_deleted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory).resolve()
            cache = root / "cache-a"
            cache.mkdir(mode=0o700)
            external = root / "not-cache"
            external.write_text("keep")
            (cache / "escape").symlink_to(external)
            info = cache.stat()
            with patch.dict(delete_cache.__globals__, {"process_identity": lambda pid: None}), self.assertRaises(Failure):
                delete_cache(root, (123, (), cache, (info.st_dev, info.st_ino)))
            self.assertEqual(external.read_text(), "keep")
            self.assertTrue(cache.exists())

    def test_unknown_rpc_not_retried_and_redirect_disabled(self):
        response = io.BytesIO(b'{"version":1,"request_id":"wrong","ok":true,"result":null,"error":null}')
        response.code = 200
        opener = Mock()
        opener.open.return_value = response
        with patch.object(urllib.request, "build_opener", return_value=opener) as builder:
            api = API("http://127.0.0.1:9001", "synthetic-only")
            with self.assertRaises(Failure):
                api.call("/", {"command": {"command": "get_database_state"}}, authority=True)
            opener.open.assert_called_once()
            self.assertTrue(any(isinstance(handler, NoRedirect) for handler in builder.call_args.args))


def main():
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--self-test", action="store_true")
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--run-dir", default=str(FIXTURE))
    parser.add_argument("--worker-binary")
    arguments = parser.parse_args()
    if arguments.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SafetyTests)
        return 0 if unittest.TextTestRunner().run(suite).wasSuccessful() else 1
    if arguments.plan:
        print(json.dumps({"plan": PLAN, "execution_requires_explicit_authorization": True, "no_unknown_retry": True, "cache_delete_remaining_toctou": True, "old_session_cleanup_not_attempted": True}))
        return 0
    root = pathlib.Path(arguments.run_dir)
    evidence = {"pass_result": False, "stage": "private_preflight", "scope": "single_owned_fixture_fault_not_stress_not_HA"}
    try:
        require(arguments.worker_binary is not None, "worker_binary_required")
        run_joint(root, arguments.worker_binary, evidence)
    except Exception as error:
        evidence["failure"] = str(error) if isinstance(error, Failure) else type(error).__name__
    try:
        path = save_evidence(root, evidence)
        print(json.dumps({"pass": evidence["pass_result"], "stage": evidence["stage"], "evidence_file": str(path), "credentials_logged": False}))
    except Exception:
        print(json.dumps({"pass": False, "stage": "evidence_not_persisted", "credentials_logged": False}))
        return 1
    return 0 if evidence["pass_result"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
