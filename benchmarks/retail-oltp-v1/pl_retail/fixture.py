"""Fixture lifecycle through the platform management API.

The benchmark needs a fresh complete database per repetition.  Creation and
deletion go through the documented management endpoints; a failed deletion is
reported and never silently ignored, because the next round would otherwise
inherit an unknown fixture.
"""
from __future__ import annotations

import json
import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from .business import Session
from .client import HranaClient, Target
from .errors import PrerequisiteError

MAX_RESPONSE_BYTES = 4 * 1024 * 1024


@dataclass
class FixtureConfig:
    endpoint: str
    tenant: str
    project: str
    global_api_key: str
    project_api_key: str
    ca_file: str | None = None
    database_prefix: str = "retail"

    @classmethod
    def from_file(cls, path: str) -> "FixtureConfig":
        with open(path, encoding="utf-8") as handle:
            document = json.load(handle)
        missing = [
            key
            for key in ("endpoint", "tenant", "project", "global_api_key", "project_api_key")
            if not document.get(key)
        ]
        if missing:
            raise PrerequisiteError("fixture file is missing %s" % ", ".join(missing))
        endpoint = str(document["endpoint"]).rstrip("/")
        if not endpoint.startswith(("https://", "http://")):
            raise PrerequisiteError("fixture endpoint must be http(s)")
        return cls(
            endpoint=endpoint,
            tenant=str(document["tenant"]),
            project=str(document["project"]),
            global_api_key=str(document["global_api_key"]),
            project_api_key=str(document["project_api_key"]),
            ca_file=document.get("ca_file"),
            database_prefix=str(document.get("database_prefix") or "retail"),
        )


class ManagementClient:
    def __init__(self, config: FixtureConfig, timeout: float = 60.0):
        self.config = config
        self.timeout = timeout
        self._context = (
            ssl.create_default_context(cafile=config.ca_file) if config.ca_file else None
        )

    def request(self, path: str, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        return self._request("POST", path, payload)

    def get(self, path: str) -> tuple[int, dict[str, Any]]:
        return self._request("GET", path, None)

    def _request(
        self, method: str, path: str, payload: dict[str, Any] | None
    ) -> tuple[int, dict[str, Any]]:
        body = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            self.config.endpoint + path, data=body, method=method
        )
        if body is not None:
            request.add_header("Content-Type", "application/json")
        request.add_header("Authorization", "Bearer %s" % self.config.global_api_key)
        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout, context=self._context
            ) as response:
                raw = response.read(MAX_RESPONSE_BYTES)
                status = response.status
        except urllib.error.HTTPError as error:
            raw = error.read(MAX_RESPONSE_BYTES)
            status = error.code
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            raise PrerequisiteError(
                "management %s failed: %s: %s" % (path, type(error).__name__, error)
            ) from error
        try:
            document = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            document = {"raw": raw.decode("utf-8", "replace")[:500]}
        if status >= 500:
            raise PrerequisiteError(
                "management %s returned %d: %s" % (path, status, document)
            )
        return status, document


def database_id(tenant: str, name: str) -> dict[str, Any]:
    return {"namespace": {"tenant": tenant, "database": name}, "branch": "main"}


class Fixture:
    """One database identity plus the client target that talks to it."""

    def __init__(self, config: FixtureConfig, name: str, round_id: str):
        self.config = config
        self.name = name
        self.round_id = round_id
        self.database = database_id(config.tenant, name)
        self.management = ManagementClient(config)
        self._created = False

    def create(self) -> Target:
        status, document = self.management.request(
            "/v1/databases/create",
            {
                "project": self.config.project,
                "database": self.database,
                "role": "writer",
                "request_id": "create-%s" % self.round_id,
            },
        )
        if status != 200:
            raise PrerequisiteError(
                "database create returned %d: %s" % (status, document)
            )
        self._created = True
        return self.target()

    def target(self) -> Target:
        return Target(
            endpoint=self.config.endpoint,
            project_api_key=self.config.project_api_key,
            global_api_key=self.config.global_api_key,
            database=self.database,
            ca_file=self.config.ca_file,
        )

    def delete(self) -> dict[str, Any]:
        if not self._created:
            return {"status": "not_created"}
        started = time.monotonic_ns()
        try:
            status, document = self.management.request(
                "/v1/databases/delete",
                {
                    "project": self.config.project,
                    "database": self.database,
                    "request_id": "delete-%s" % self.round_id,
                },
            )
        except PrerequisiteError as error:
            return {
                "status": "failed",
                "reason": str(error),
                "duration_ms": round((time.monotonic_ns() - started) / 1_000_000, 3),
            }
        return {
            "status": "ok" if status == 200 else "failed",
            "http_status": status,
            "body": document,
            "duration_ms": round((time.monotonic_ns() - started) / 1_000_000, 3),
        }

    def wait_ready(self, timeout_seconds: float = 30.0) -> dict[str, Any]:
        target = self.target()
        deadline = time.monotonic() + timeout_seconds
        attempts = 0
        last = ""
        while time.monotonic() < deadline:
            attempts += 1
            client = HranaClient(target, timeout=5.0)
            session = Session(client)
            try:
                session.open()
                return {"status": "ready", "attempts": attempts}
            except Exception as error:  # noqa: BLE001 - readiness probe records the reason
                last = "%s: %s" % (type(error).__name__, error)
            finally:
                try:
                    session.close()
                except Exception:
                    pass
                client.close_quietly()
            time.sleep(0.25)
        raise PrerequisiteError(
            "database %s did not accept work within %.1fs (%s)"
            % (self.name, timeout_seconds, last)
        )
