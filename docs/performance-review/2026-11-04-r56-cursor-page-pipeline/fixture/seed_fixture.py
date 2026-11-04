#!/usr/bin/env python3
"""Recreate the Issue 28 dataset shape in a running peri-loom database.

The original `/tmp/threads-subset.db` snapshot is no longer available. This
script rebuilds a dataset that satisfies the harness checks exactly:

  threads=201, messages=8625, sum(length(content))=20177069,
  focus thread holds all 8625 messages.

Content is synthetic but deterministic. Absolute numbers produced against this
dataset are only comparable inside the same measurement window; they must not
be compared to historical Issue 28 numbers measured on the original snapshot.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
import urllib.error
import urllib.request

TOTAL_THREADS = 201
TOTAL_MESSAGES = 8625
TOTAL_CHARS = 20177069
FOCUS_THREAD = "issue28-focus"
LIST_CWD = "/workspace/issue28"
OTHER_CWD = "/workspace/issue28-other"
THREAD_CHUNK = 100
MESSAGE_CHUNK = 20


def call(endpoint: str, path: str, body: dict, key: str) -> object:
    request = urllib.request.Request(
        endpoint + path,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            document = json.load(response)
    except urllib.error.HTTPError as error:
        text = error.read(2048).decode("utf-8", "replace")
        raise SystemExit("SQL HTTP %d: %s" % (error.code, text)) from None
    if not document.get("ok"):
        raise SystemExit("SQL error: %r" % (document.get("error"),))
    return document["result"]


def lengths() -> list[int]:
    short, long = TOTAL_MESSAGES, 0
    # 3194 rows of 2340 chars + 5431 rows of 2339 chars = 20,177,069.
    long_count = TOTAL_CHARS - TOTAL_MESSAGES * 2339
    return [2340] * long_count + [2339] * (TOTAL_MESSAGES - long_count)


def content(index: int, size: int) -> str:
    pattern = "issue28/%05d/" % index
    repeats = size // len(pattern) + 1
    return (pattern * repeats)[:size]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("connection_file")
    arguments = parser.parse_args()

    document = json.loads(pathlib.Path(arguments.connection_file).read_text())
    endpoint = document["endpoint"].rstrip("/")
    key = document["project_api_key"]

    session = call(endpoint, "/v1/sql/open", {"database": document["database"]}, key)["session"]
    try:
        call(endpoint, "/v1/sql/query",
             {"session": session, "sql": "DROP TABLE IF EXISTS messages"}, key)
        call(endpoint, "/v1/sql/query",
             {"session": session, "sql": "DROP TABLE IF EXISTS threads"}, key)
        call(endpoint, "/v1/sql/query", {"session": session, "sql": """
            CREATE TABLE threads (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                cwd TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                message_count INTEGER NOT NULL,
                parent_thread_id TEXT,
                snapshot_at_message_id TEXT,
                hidden INTEGER NOT NULL,
                agent_status TEXT,
                workspace_id TEXT,
                archived INTEGER NOT NULL
            )"""}, key)
        call(endpoint, "/v1/sql/query", {"session": session, "sql": """
            CREATE TABLE messages (
                thread_id TEXT NOT NULL,
                content TEXT,
                projection BLOB
            )"""}, key)

        rows = []
        rows.append((FOCUS_THREAD, "issue28 focus", LIST_CWD, 1, TOTAL_MESSAGES + 1,
                     TOTAL_MESSAGES, 0, "idle", "ws-1", 0))
        for index in range(TOTAL_THREADS - 1):
            thread_id = "issue28-%03d" % index
            # The harness L1 scenario runs `... LIMIT 40` and expects
            # fixture.list_threads rows, so the list cwd must hold at most 40
            # threads (39 + the focus thread) for the row-count check to pass.
            cwd = LIST_CWD if index < 39 else OTHER_CWD
            rows.append((thread_id, "thread %03d" % index, cwd, 1, index + 1, 0,
                         0, "idle", "ws-1", 0))
        for start in range(0, len(rows), THREAD_CHUNK):
            chunk = rows[start:start + THREAD_CHUNK]
            values = ", ".join(
                "('%s','%s','%s',%d,%d,%d,NULL,NULL,%d,'%s','%s',%d)" % row
                for row in chunk)
            call(endpoint, "/v1/sql/query", {"session": session, "sql":
                 "INSERT INTO threads (id,title,cwd,created_at,updated_at,message_count,"
                 "parent_thread_id,snapshot_at_message_id,hidden,agent_status,workspace_id,"
                 "archived) VALUES " + values}, key)

        sizes = lengths()
        inserted = 0
        for start in range(0, len(sizes), MESSAGE_CHUNK):
            chunk = sizes[start:start + MESSAGE_CHUNK]
            values = ", ".join(
                "('%s','%s')" % (FOCUS_THREAD, content(start + offset, size).replace("'", "''"))
                for offset, size in enumerate(chunk))
            call(endpoint, "/v1/sql/query", {"session": session, "sql":
                 "INSERT INTO messages (thread_id,content) VALUES " + values}, key)
            inserted += len(chunk)
            print("inserted %d/%d messages" % (inserted, len(sizes)), file=sys.stderr)

        check = call(endpoint, "/v1/sql/query", {"session": session, "sql":
            "SELECT (SELECT count(*) FROM threads), (SELECT count(*) FROM messages), "
            "(SELECT sum(length(content)) FROM messages), "
            "(SELECT count(*) FROM messages WHERE thread_id = '%s')" % FOCUS_THREAD}, key)
        print("verify threads=%s messages=%s chars=%s focus=%s" % tuple(check[0]))
        expected = (TOTAL_THREADS, TOTAL_MESSAGES, TOTAL_CHARS, TOTAL_MESSAGES)
        actual = tuple(check[0])
        return 0 if actual == expected else 1
    finally:
        call(endpoint, "/v1/sql/close", {"session": session}, key)


if __name__ == "__main__":
    raise SystemExit(main())
