import argparse
import json
import os
import pathlib
import secrets
import ssl
import stat
import time
import urllib.error
import urllib.request


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate response field")
        result[key] = value
    return result


def private_connection(path):
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o077 or metadata.st_uid != os.getuid() or metadata.st_nlink != 1:
        raise ValueError("private regular connection file required")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino) or opened.st_size > 65536:
            raise ValueError("connection file changed or oversized")
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            return json.loads(source.read(65537), object_pairs_hook=unique_object)
    finally:
        os.close(descriptor)


def catalog_matches(result, project, database):
    expected = "/".join((database["namespace"]["tenant"], database["namespace"]["database"], database["branch"]))
    return (isinstance(result, dict) and isinstance(result.get("projects"), dict)
            and isinstance(result.get("databases"), dict)
            and set(result["projects"]) == {project}
            and set(result["databases"]) == {expected}
            and result["databases"][expected].get("id") == database
            and result["databases"][expected].get("project") == project)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    arguments = parser.parse_args()
    connection = private_connection(arguments.connection)
    if not connection["endpoint"].startswith("https://127.0.0.1:"):
        raise ValueError("verified loopback HTTPS required")
    context = ssl.create_default_context(cafile=connection["ca_file"])
    records = []
    suffix = secrets.token_hex(4)
    organization = "ua" + suffix
    project_a, project_b = "a" + suffix, "b" + suffix
    database_a = {"namespace": {"tenant": organization + "_" + project_a, "database": "ledger"}, "branch": "main"}
    database_b = {"namespace": {"tenant": organization + "_" + project_b, "database": "ledger"}, "branch": "main"}
    counter = 0
    current = "preflight"

    def request(label, path, key, body=None, status=200, code=None, verify=lambda result: True, mismatch_header=False):
        nonlocal counter, current
        current = label
        counter += 1
        identifier = "userlive" + suffix + str(counter)
        header_identifier = identifier + "wrong" if mismatch_header else identifier
        headers = {"Authorization": "Bearer " + key, "X-Request-ID": header_identifier}
        if body is not None:
            headers["Content-Type"] = "application/json"
        outgoing = urllib.request.Request(
            connection["endpoint"] + path,
            data=json.dumps({**body, "request_id": identifier}).encode() if body is not None else None,
            headers=headers,
            method="POST" if body is not None else "GET",
        )
        client = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=context))
        try:
            incoming = client.open(outgoing, timeout=20)
        except urllib.error.HTTPError as failure:
            incoming = failure
        with incoming:
            actual_status = incoming.status
            payload = incoming.read(1024 * 1024 + 1)
        if len(payload) > 1024 * 1024:
            raise ValueError("oversized response")
        response = json.loads(payload, object_pairs_hook=unique_object)
        if (response.get("version") != 1 or response.get("request_id") != (header_identifier if body is not None else "")
                or response.get("ok") != (actual_status == 200) or "result" not in response or "error" not in response):
            raise ValueError("response envelope mismatch")
        actual_code = response["error"].get("code") if isinstance(response["error"], dict) else None
        if actual_status != status or actual_code != code or not verify(response["error"] if code is not None else response["result"]):
            raise ValueError("response did not match expected status, error or result")
        records.append({"case": label, "path": path, "request_id": identifier if body is not None else "", "status": actual_status, "code": actual_code, "result": "PASS"})
        return response["result"] if code is None else response["error"]

    try:
        global_key = connection["global_api_key"]
        request("organization", "/v1/organizations/create", global_key, {"id": organization, "name": "User acceptance"})
        quotas = {"max_databases": 1, "max_sessions": 2, "max_concurrent": 4, "requests_per_second": 100, "max_sql_bytes": 256, "max_result_bytes": 4096}
        request("project_a", "/v1/projects/create", global_key, {"id": project_a, "organization": organization, "region": "local", "quotas": quotas})
        request("project_b", "/v1/projects/create", global_key, {"id": project_b, "organization": organization, "region": "local"})

        def new_key(project, role, subject):
            return request("key_" + subject, "/v1/keys/create", global_key, {"project": project, "subject": subject, "role": role})

        admin = new_key(project_a, "admin", "admin_a")
        sql = new_key(project_a, "sql", "sql_a")
        viewer = new_key(project_a, "viewer", "viewer_a")
        other = new_key(project_b, "admin", "admin_b")
        request("database_a", "/v1/databases/create", admin["credential"], {"project": project_a, "database": database_a, "role": "writer"})
        request("database_b", "/v1/databases/create", other["credential"], {"project": project_b, "database": database_b, "role": "writer"})
        extra_database = {"namespace": {"tenant": organization + "_" + project_a, "database": "extra"}, "branch": "main"}
        request("database_quota_denied", "/v1/databases/create", admin["credential"], {"project": project_a, "database": extra_database, "role": "writer"}, 429, "quota_exceeded")
        request("catalog_a", "/v1/catalog", admin["credential"], verify=lambda value: catalog_matches(value, project_a, database_a))
        request("catalog_b", "/v1/catalog", other["credential"], verify=lambda value: catalog_matches(value, project_b, database_b))
        request("viewer_metrics_denied", "/v1/metrics", viewer["credential"], status=403, code="forbidden")
        request("sql_key_creation_denied", "/v1/keys/create", sql["credential"], {"project": project_a, "subject": "escalate", "role": "admin"}, 403, "forbidden")
        request("viewer_key_creation_denied", "/v1/keys/create", viewer["credential"], {"project": project_a, "subject": "escalate", "role": "admin"}, 403, "forbidden")
        request("cross_project_key_creation_denied", "/v1/keys/create", admin["credential"], {"project": project_b, "subject": "escalate", "role": "admin"}, 403, "forbidden")
        request("global_sql_denied", "/v1/sql/open", global_key, {"database": database_a}, 403, "forbidden")
        request("viewer_status", "/v1/databases/status", viewer["credential"], {"database": database_a})
        request("viewer_writer_token_denied", "/v1/tokens", viewer["credential"], {"database": database_a, "role": "writer", "expires_ms": int(time.time() * 1000) + 120000}, 403, "forbidden")
        request("viewer_sql_denied", "/v1/sql/open", viewer["credential"], {"database": database_a}, 403, "forbidden")
        request("other_project_open_denied", "/v1/sql/open", other["credential"], {"database": database_a}, 403, "forbidden")
        session = request("sql_open", "/v1/sql/open", sql["credential"], {"database": database_a})["session"]
        another_subject = new_key(project_a, "sql", "sql_second_subject")
        request("same_project_other_subject_denied", "/v1/sql/query", another_subject["credential"], {"session": session, "sql": "SELECT 1"}, 403, "invalid_session")
        request("other_project_session_denied", "/v1/sql/query", other["credential"], {"session": session, "sql": "SELECT 1"}, 403, "invalid_session")
        request("sql_size_denied", "/v1/sql/execute", sql["credential"], {"session": session, "sql": "SELECT '" + "a" * 280 + "'"}, 413, "sql_limit")
        request("header_body_mismatch", "/v1/sql/query", sql["credential"], {"session": session, "sql": "SELECT 1"}, 400, "invalid_request", mismatch_header=True)
        request("create_table", "/v1/sql/execute", sql["credential"], {"session": session, "sql": "CREATE TABLE accepted(id INTEGER PRIMARY KEY)"})
        request("insert_row", "/v1/sql/execute", sql["credential"], {"session": session, "sql": "INSERT INTO accepted VALUES(1)"})
        request("begin", "/v1/sql/execute", sql["credential"], {"session": session, "sql": "BEGIN"})
        request("duplicate_terminal_error", "/v1/sql/execute", sql["credential"], {"session": session, "sql": "INSERT INTO accepted VALUES(1)"}, 400, "sql_error", verify=lambda value: value.get("execution_completed") is True and value.get("may_have_committed") is True)
        request("explicit_rollback", "/v1/sql/execute", sql["credential"], {"session": session, "sql": "ROLLBACK"})
        request("confirmed_prefix", "/v1/sql/query", sql["credential"], {"session": session, "sql": "SELECT id FROM accepted"}, verify=lambda value: value == [[1]])
        request("terminal_result_limit", "/v1/sql/query", sql["credential"], {"session": session, "sql": "SELECT hex(zeroblob(4096))"}, 413, "result_limit", verify=lambda value: value.get("execution_completed") is True)
        second = request("second_session", "/v1/sql/open", sql["credential"], {"database": database_a})["session"]
        request("session_quota_denied", "/v1/sql/open", sql["credential"], {"database": database_a}, 429, "quota_exceeded")
        other_session = request("other_project_independent", "/v1/sql/open", other["credential"], {"database": database_b})["session"]
        request("other_project_close", "/v1/sql/close", other["credential"], {"session": other_session})
        request("first_close", "/v1/sql/close", sql["credential"], {"session": session})
        request("second_close", "/v1/sql/close", sql["credential"], {"session": second})
        token = request("token_issue", "/v1/tokens", admin["credential"], {"database": database_a, "role": "writer", "expires_ms": int(time.time() * 1000) + 120000})
        token_session = request("token_open", "/v1/sql/open", token, {"database": database_a})["session"]
        request("token_close", "/v1/sql/close", token, {"session": token_session})
        request("revoke_issuer", "/v1/keys/revoke", global_key, {"id": admin["key"]}, verify=lambda value: value["revoked"] is True)
        request("revoked_key_denied", "/v1/sql/open", admin["credential"], {"database": database_a}, 401, "unauthenticated")
        request("issued_token_denied", "/v1/sql/open", token, {"database": database_a}, 401, "unauthenticated")
    except Exception as error:
        records.append({"case": current, "result": "FAIL", "error_type": type(error).__name__})
        raise
    finally:
        descriptor = os.open(arguments.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            json.dump(records, output, indent=2)
            output.write("\n")


if __name__ == "__main__":
    main()
