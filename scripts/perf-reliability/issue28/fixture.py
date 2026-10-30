"""Dataset discovery and verification for the Issue 28 performance plan."""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any

from .hrana import FatalTransportError, HranaError, Target


class FixtureError(RuntimeError):
    pass


@dataclass
class DatasetCheck:
    name: str
    expected: Any
    actual: Any
    ok: bool


@dataclass
class Fixture:
    threads: int
    messages: int
    content_chars: int
    content_bytes: int
    focus_thread: str
    focus_messages: int
    focus_content_bytes: int
    list_cwd: str
    list_threads: int
    empty_cwd: str
    projection_non_null: int
    projection_bytes: int
    quotas: dict = field(default_factory=dict)
    checks: list[DatasetCheck] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        return all(check.ok for check in self.checks)

    def as_record(self) -> dict:
        return {
            "threads": self.threads,
            "messages": self.messages,
            "content_chars": self.content_chars,
            "content_bytes": self.content_bytes,
            "focus_thread": self.focus_thread,
            "focus_messages": self.focus_messages,
            "focus_content_bytes": self.focus_content_bytes,
            "list_cwd": self.list_cwd,
            "list_threads": self.list_threads,
            "empty_cwd": self.empty_cwd,
            "projection_non_null": self.projection_non_null,
            "projection_bytes": self.projection_bytes,
            "quotas": self.quotas,
            "complete": self.complete,
            "checks": [check.__dict__ for check in self.checks],
        }


class SqlApi:
    def __init__(self, target: Target, timeout: float = 120.0):
        self.target = target
        self.timeout = timeout

    def call(self, path: str, body: dict) -> dict:
        request = urllib.request.Request(
            self.target.endpoint + path,
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers={
                "Authorization": "Bearer " + self.target.project_api_key,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                document = json.load(response)
        except urllib.error.HTTPError as error:
            text = error.read(1024).decode("utf-8", "replace")
            raise FixtureError("SQL HTTP %d: %s" % (error.code, _redact(text, self.target))) from None
        except (OSError, urllib.error.URLError) as error:
            raise FatalTransportError("fixture SQL transport failed: %s" % type(error).__name__) from None
        if not document.get("ok"):
            raise FixtureError("SQL error: %r" % (document.get("error"),))
        return document["result"]

    def open(self) -> str:
        return self.call("/v1/sql/open", {"database": self.target.database})["session"]

    def query(self, session: str, sql: str) -> list:
        return self.call("/v1/sql/query", {"session": session, "sql": sql})

    def close(self, session: str) -> None:
        self.call("/v1/sql/close", {"session": session})

    def capabilities(self) -> dict:
        request = urllib.request.Request(self.target.endpoint + "/v1/platform/capabilities")
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                return json.load(response)
        except (OSError, urllib.error.URLError) as error:
            raise FatalTransportError("capabilities request failed: %s" % type(error).__name__) from None

    def quotas(self) -> dict:
        request = urllib.request.Request(
            self.target.endpoint + "/v1/catalog",
            headers={"Authorization": "Bearer " + self.target.global_api_key},
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                document = json.load(response)
        except (OSError, urllib.error.URLError) as error:
            raise FatalTransportError("catalog request failed: %s" % type(error).__name__) from None
        projects = (document.get("result") or {}).get("projects") or {}
        for project in projects.values():
            quotas = project.get("quotas")
            if isinstance(quotas, dict):
                return quotas
        return {}


def discover(target: Target) -> Fixture:
    api = SqlApi(target)
    session = api.open()
    try:
        counts = api.query(session, """
            SELECT (SELECT count(*) FROM threads),
                   (SELECT count(*) FROM messages),
                   (SELECT sum(length(content)) FROM messages),
                   (SELECT sum(length(cast(content as blob))) FROM messages)
        """)[0]
        focus = api.query(session, """
            SELECT thread_id, count(*), sum(length(cast(content as blob)))
            FROM messages GROUP BY thread_id ORDER BY 3 DESC LIMIT 1
        """)[0]
        cwd_rows = api.query(session, """
            SELECT cwd, count(*) FROM threads
            WHERE hidden = 0 GROUP BY cwd HAVING count(*) >= 20
            ORDER BY count(*) ASC, cwd ASC LIMIT 1
        """)
        if not cwd_rows:
            cwd_rows = api.query(session, """
                SELECT cwd, count(*) FROM threads
                WHERE hidden = 0 GROUP BY cwd ORDER BY count(*) DESC, cwd ASC LIMIT 1
            """)
        list_cwd, list_threads = cwd_rows[0][0], int(cwd_rows[0][1])
        projection = api.query(session, """
            SELECT count(projection), coalesce(sum(length(cast(projection as blob))), 0)
            FROM messages
        """)[0]
    finally:
        api.close(session)

    try:
        quotas = api.quotas()
    except FatalTransportError:
        quotas = {}

    fixture = Fixture(
        threads=int(counts[0]),
        messages=int(counts[1]),
        content_chars=int(counts[2] or 0),
        content_bytes=int(counts[3] or 0),
        focus_thread=str(focus[0]),
        focus_messages=int(focus[1]),
        focus_content_bytes=int(focus[2] or 0),
        list_cwd=str(list_cwd),
        list_threads=list_threads,
        empty_cwd=str(list_cwd) + "/__issue28_missing__",
        projection_non_null=int(projection[0]),
        projection_bytes=int(projection[1]),
        quotas=quotas,
    )
    fixture.checks = [
        DatasetCheck("threads", 201, fixture.threads, fixture.threads == 201),
        DatasetCheck("messages", 8625, fixture.messages, fixture.messages == 8625),
        DatasetCheck("content_chars", 20177069, fixture.content_chars,
                     fixture.content_chars == 20177069),
        DatasetCheck("focus_messages", 8625, fixture.focus_messages,
                     fixture.focus_messages == 8625),
    ]
    return fixture


def _redact(text: str, target: Target) -> str:
    for secret in (target.project_api_key, target.global_api_key):
        if secret:
            text = text.replace(secret, "<REDACTED>")
    return text


class MetricsApi:
    def __init__(self, target: Target, timeout: float = 15.0):
        self.target = target
        self.timeout = timeout

    def snapshot(self) -> dict:
        request = urllib.request.Request(
            self.target.endpoint + "/v1/metrics",
            headers={"Authorization": "Bearer " + self.target.global_api_key},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                document = json.load(response)
        except urllib.error.HTTPError as error:
            raise FixtureError("metrics HTTP %d" % error.code) from None
        except (OSError, urllib.error.URLError) as error:
            raise FatalTransportError("metrics transport failed: %s" % type(error).__name__) from None
        result = document.get("result")
        if not isinstance(result, dict):
            raise FixtureError("metrics response has no result")
        return {
            "active_permits": int(result.get("active_permits") or 0),
            "unknown_held_permits": int(result.get("unknown_held_permits") or 0),
            "session_seats": int(result.get("session_seats") or 0),
            "rejected_requests_total": int(result.get("rejected_requests_total") or 0),
            "rejection_reasons_total": result.get("rejection_reasons_total") or {},
        }
