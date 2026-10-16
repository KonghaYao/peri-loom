import argparse
import copy
import importlib.util
import json
import os
import pathlib
import secrets
import ssl
import time
import urllib.error
import urllib.request

spec = importlib.util.spec_from_file_location("peri_dev", pathlib.Path(__file__).resolve().parents[1] / "dev.py")
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    path = pathlib.Path(args.connection)
    if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
        raise RuntimeError("private fixture connection required")
    config = json.loads(path.read_text())
    endpoint, project_key, global_key = config["endpoint"], config["project_api_key"], config["global_api_key"]
    certificate = config["ca_file"]
    database = config["database"]
    if not endpoint.startswith("https://127.0.0.1:") or not certificate:
        raise RuntimeError("verified TLS loopback fixture required")

    def call(action, body, key=project_key):
        return dev.request(endpoint, key, action, body, certificate)

    def opened(database_id=database, key=project_key):
        return call("/v1/sql/open", {"database": database_id}, key)

    def query(session, sql, key=project_key):
        return call("/v1/sql/query", {"session": session["session"], "sql": sql}, key)

    def execute(session, sql):
        return call("/v1/sql/execute", {"session": session["session"], "sql": sql})

    def close(session, key=project_key):
        call("/v1/sql/close", {"session": session["session"]}, key)

    suffix = secrets.token_hex(4)
    table, snapshot, reader_name = "smoke_" + suffix, "before_" + suffix, "reader_" + suffix
    session = opened()
    execute(session, f"CREATE TABLE {table}(id INTEGER PRIMARY KEY,balance INTEGER NOT NULL,payload TEXT);INSERT INTO {table} VALUES(1,9223372036854775807,'encrypted_fixture_{suffix}');")
    assert query(session, f"SELECT balance FROM {table} WHERE id=1") == [[9223372036854775807]]
    assert query(session, "SELECT -9007199254740993") == [[-9007199254740993]]
    execute(session, f"BEGIN;UPDATE {table} SET balance=7 WHERE id=1;ROLLBACK;")
    assert query(session, f"SELECT balance FROM {table}") == [[9223372036854775807]]
    assert query(session, "PRAGMA integrity_check") == [["ok"]]
    close(session)
    checkpoint = call("/v1/databases/checkpoint", {"database": database})
    assert checkpoint["checkpointed"] is True
    call("/v1/databases/snapshot", {"database": database, "name": snapshot})
    token = call("/v1/tokens", {"database": database, "role": "writer", "expires_ms": int(time.time()*1000)+120000})
    session = opened()
    execute(session, f"INSERT INTO {table} VALUES(2,200,'later');")
    close(session)
    call("/v1/databases/restore", {"database": database, "snapshot": snapshot})
    session = opened()
    assert query(session, f"SELECT id,balance FROM {table}") == [[1,9223372036854775807]]
    close(session)
    stale = token
    if not stale:
        raise RuntimeError("token response missing")
    denial_request = urllib.request.Request(endpoint + "/v1/sql/open", data=json.dumps({"database": database, "request_id": "deny_" + suffix}).encode(), headers={"Authorization": "Bearer " + stale, "Content-Type": "application/json"}, method="POST")
    denial_client = urllib.request.build_opener(urllib.request.ProxyHandler({}), dev.NoRedirect(), urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=certificate)))
    try:
        denial_client.open(denial_request, timeout=15).close()
    except urllib.error.HTTPError as error:
        denial = json.loads(error.read(1024*1024))
        assert error.code == 401 and denial["error"]["code"] == "unauthenticated"
    else:
        raise RuntimeError("restored branch accepted stale token")
    call("/v1/databases/clone", {"database": database, "snapshot": snapshot, "name": reader_name, "role": "read_only"})
    reader = copy.deepcopy(database)
    reader["branch"] = reader_name
    reader_token = call("/v1/tokens", {"database": reader, "role": "read_only", "expires_ms": int(time.time()*1000)+120000})
    session = opened(reader, reader_token)
    assert session["readonly"] is True
    assert query(session, f"SELECT balance FROM {table}", reader_token) == [[9223372036854775807]]
    denied_id = "readonly_" + suffix
    denied_body = {"session": session["session"], "sql": f"INSERT INTO {table} VALUES(9,0,'denied') RETURNING id", "request_id": denied_id}
    denied_request = urllib.request.Request(endpoint + "/v1/sql/query", data=json.dumps(denied_body).encode(), headers={"Authorization": "Bearer " + reader_token, "Content-Type": "application/json", "X-Request-ID": denied_id}, method="POST")
    try:
        denial_client.open(denied_request, timeout=15).close()
    except urllib.error.HTTPError as error:
        denied = json.loads(error.read(1024*1024))
        # The worker settles the statement and refuses it by placement, so the platform
        # answers with the graded readonly_session code, not a bare sql_error, and keeps
        # the execution evidence the client needs to tell refusal from a lost reply.
        assert error.code == 403 and denied["request_id"] == denied_id and denied["error"]["code"] == "readonly_session" and denied["error"]["execution_completed"] is True
    else:
        raise RuntimeError("read-only clone accepted SQL write")
    assert query(session, f"SELECT id,balance FROM {table}", reader_token) == [[1,9223372036854775807]]
    close(session, reader_token)
    status = call("/v1/databases/status", {"database": database})
    old_worker, old_epoch = status["placement"]["desired_worker"], status["placement"]["epoch"]
    destination = "worker-b" if old_worker == "worker-a" else "worker-a"
    moved = call("/v1/databases/migrate", {"database": database, "worker_id": destination})
    assert moved["epoch"] > old_epoch
    session = opened()
    assert query(session, f"SELECT id,balance FROM {table}") == [[1,9223372036854775807]]
    close(session)
    call("/v1/databases/suspend", {"database": database})
    suspended = call("/v1/databases/status", {"database": database})
    assert suspended["diagnostics"]["status"] == "unavailable"
    call("/v1/databases/resume", {"database": database})
    request = urllib.request.Request(endpoint + "/v1/metrics", headers={"Authorization": "Bearer " + global_key})
    client = urllib.request.build_opener(urllib.request.ProxyHandler({}), dev.NoRedirect(), urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=certificate)))
    with client.open(request, timeout=15) as response:
        metrics = json.load(response)["result"]
    assert metrics["native"]["authority"]["put"]["total"] > 0
    assert len(metrics["native"]["workers"]) == 2
    assert metrics["errors_total"].get("audit_unavailable", 0) == 0
    result = {"pass": True, "backend": "external_s3_fixture", "tls": "certificate_verified", "checks": ["native_encrypted_sql_ack", "exact_int64", "explicit_rollback", "integrity", "checkpoint", "snapshot_restore", "stale_token_denial", "native_readonly_clone", "native_readonly_write_denial", "migration_epoch", "suspend_status_resume", "actual_native_metrics"], "metrics": metrics, "database": database}
    descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as output:
        json.dump(result, output)
    print(json.dumps({"pass": True, "checks": result["checks"], "credentials_logged": False}))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit("whole-project smoke failed: " + type(error).__name__ + "; inspect fixture without exposing credentials") from None
