"""peri-loom 公开网关黑盒客户端：只用公开 HTTP 协议，不导入平台内部模块。

协议依据（只读证据）：prototypes/v1/platform/sdk/go/{client,session,types}.go、
cmd/peri/main.go、platform/README.md 与 sdk/README.md。
凭据只从环境变量或私有文件读取，绝不进入 argv、日志或报告；SQL 只派发一次，
传输失败/信封异常一律记为 outcome_unknown，不重试、不重开会话。
"""

import http.client
import ipaddress
import json
import os
import re
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

REQUEST_LIMIT = 1 << 20
RESPONSE_LIMIT = 8 << 20
CREDENTIAL_MIN = 32
CREDENTIAL_MAX = 4096
LABEL = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
SESSION_ID = re.compile(r"^[0-9a-f]{32}$")


class PlatformFailure(Exception):
    """公开协议失败；code 沿用 SDK 公布词汇，message 有界且不含凭据。

    timings 只在确实观测到时给出：派发前的拒绝没有时间戳，传输失败有
    completed 观测时刻但没有平台回执；flags 保留平台公布的终态标志。
    """

    def __init__(self, code, message="", timings=None, timed_out=False, flags=None):
        super().__init__(code)
        self.code = code
        self.message = "".join(character for character in message if character.isprintable())[:200]
        self.timings = timings
        self.timed_out = timed_out
        self.flags = flags or {}


def load_credential(environment=None):
    environment = os.environ if environment is None else environment
    credential = environment.get("PERI_API_KEY")
    if credential is None:
        path = environment.get("PERI_CREDENTIAL_FILE")
        if not path:
            raise PlatformFailure("missing_credential")
        try:
            if not os.path.isfile(path):
                raise OSError("not a regular file")
            with open(path, "rb") as stream:
                payload = stream.read(CREDENTIAL_MAX + 1)
        except OSError:
            raise PlatformFailure("credential_file_unreadable")
        if len(payload) > CREDENTIAL_MAX:
            raise PlatformFailure("invalid_credential")
        try:
            credential = payload.decode("utf-8").strip()
        except UnicodeDecodeError:
            raise PlatformFailure("invalid_credential")
    if not CREDENTIAL_MIN <= len(credential) <= CREDENTIAL_MAX:
        raise PlatformFailure("invalid_credential")
    if any(not 33 <= ord(character) <= 126 for character in credential):
        raise PlatformFailure("invalid_credential")
    return credential


def validate_endpoint(endpoint):
    parsed = urllib.parse.urlsplit(endpoint)
    if (parsed.scheme not in ("http", "https") or not parsed.hostname
            or parsed.username or parsed.password or parsed.query or parsed.fragment
            or parsed.path not in ("", "/")):
        raise PlatformFailure("invalid_endpoint")
    if parsed.scheme != "https":
        # 与 SDK 一致：明文只允许字面 loopback IP，localhost 也算外部主机。
        try:
            address = ipaddress.ip_address(parsed.hostname)
        except ValueError:
            raise PlatformFailure("https_required_outside_loopback")
        if not address.is_loopback:
            raise PlatformFailure("https_required_outside_loopback")
    return endpoint.rstrip("/")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *arguments, **keywords):
        return None


class PlatformClient:
    """connection_mode 控制传输层连接生命周期：

    - per-request（默认）：每个请求新建连接，保持既有语义；
    - reuse：每个 worker 线程（= 每个会话）独占一条持久连接，必须读完整响应后才复用；
      连接失活/半响应只影响"尚未派发的下一次操作"——它会新建连接，而已派发的请求
      一律以 outcome_unknown 结束，绝不自动重发。
    """

    def __init__(self, endpoint, credential, ca_file=None, timeout=30.0, connection_mode="per-request"):
        self.endpoint = validate_endpoint(endpoint)
        if not CREDENTIAL_MIN <= len(credential) <= CREDENTIAL_MAX:
            raise PlatformFailure("invalid_credential")
        if connection_mode not in ("per-request", "reuse"):
            raise PlatformFailure("invalid_connection_mode")
        self._credential = credential
        self._timeout = timeout
        self._mode = connection_mode
        self._local = threading.local()
        self._lock = threading.Lock()
        self._connections_created = 0
        self._requests_sent = 0
        self._parsed = urllib.parse.urlsplit(self.endpoint)
        context = ssl.create_default_context()
        if ca_file:
            context.load_verify_locations(cafile=ca_file)
        handlers = [_NoRedirect(), urllib.request.ProxyHandler({}),
                    urllib.request.HTTPSHandler(context=context)]
        self._opener = urllib.request.build_opener(*handlers)
        self._context = context

    def __repr__(self):
        return f"<PlatformClient {self._parsed.netloc} mode={self._mode}>"

    def connection_stats(self):
        """实际计数（不是按配置推断）：新建连接数与已派发请求数。"""
        with self._lock:
            return {"connection_mode": self._mode,
                    "connections_created": self._connections_created,
                    "requests_sent": self._requests_sent}

    def _connection(self):
        connection = getattr(self._local, "connection", None)
        if connection is None:
            host = self._parsed.hostname
            port = self._parsed.port or (443 if self._parsed.scheme == "https" else 80)
            if self._parsed.scheme == "https":
                connection = http.client.HTTPSConnection(host, port, context=self._context,
                                                         timeout=self._timeout)
            else:
                connection = http.client.HTTPConnection(host, port, timeout=self._timeout)
            self._local.connection = connection
            with self._lock:
                self._connections_created += 1
        return connection

    def close_connections(self):
        connection = getattr(self._local, "connection", None)
        if connection is not None:
            try:
                connection.close()
            finally:
                self._local.connection = None

    def _decode(self, raw, request_id, timings):
        if len(raw) > RESPONSE_LIMIT:
            raise PlatformFailure("outcome_unknown", timings=timings)
        try:
            reply = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise PlatformFailure("outcome_unknown", timings=timings)
        if not isinstance(reply, dict) or reply.get("version") != 1 \
                or reply.get("request_id") != request_id or "ok" not in reply:
            raise PlatformFailure("outcome_unknown", timings=timings)
        ok = reply.get("ok")
        if not isinstance(ok, bool):
            raise PlatformFailure("outcome_unknown", timings=timings)
        if not ok:
            failure = reply.get("error")
            if "result" not in reply or reply["result"] is not None or not isinstance(failure, dict):
                raise PlatformFailure("outcome_unknown", timings=timings)
            flags = {name: failure.get(name) for name in ("execution_completed", "may_have_committed")
                     if isinstance(failure.get(name), bool)}
            raise PlatformFailure(str(failure.get("code", "remote_error"))[:64],
                                   str(failure.get("message", "")), timings=timings, flags=flags)
        if "result" not in reply:
            raise PlatformFailure("outcome_unknown", timings=timings)
        return reply["result"]

    def call(self, path, body):
        """返回 (result, timings)；失败抛 PlatformFailure，绝不重试。"""
        payload = json.dumps(body).encode()
        if len(payload) > REQUEST_LIMIT:
            raise PlatformFailure("request_limit")
        planned = time.monotonic_ns()
        request = urllib.request.Request(self.endpoint + path, data=payload, method="POST")
        request.add_header("Content-Type", "application/json")
        request.add_header("Accept", "application/json")
        request.add_header("Authorization", "Bearer " + self._credential)
        request.add_header("X-Request-ID", body["request_id"])
        dispatched = time.monotonic_ns()
        with self._lock:
            self._requests_sent += 1
        if self._mode == "reuse":
            headers = {"Content-Type": "application/json", "Accept": "application/json",
                       "Authorization": "Bearer " + self._credential,
                       "X-Request-ID": body["request_id"]}
            try:
                connection = self._connection()
                connection.request("POST", path, body=payload, headers=headers)
                response = connection.getresponse()
                raw = response.read(RESPONSE_LIMIT + 1)
            except (TimeoutError, http.client.HTTPException, OSError, ValueError) as error:
                # 连接可能已失活：丢弃它，但绝不重发这次请求；下一次未派发的操作会新建连接。
                self.close_connections()
                raise PlatformFailure("outcome_unknown", timings={
                    "planned_ns": planned, "dispatched_ns": dispatched,
                    "completed_ns": time.monotonic_ns()},
                    timed_out=isinstance(error, (TimeoutError, socket.timeout)))
            completed = time.monotonic_ns()
            return self._decode(raw, body["request_id"],
                                {"planned_ns": planned, "dispatched_ns": dispatched,
                                 "completed_ns": completed}), \
                   {"planned_ns": planned, "dispatched_ns": dispatched, "completed_ns": completed}
        try:
            with self._opener.open(request, timeout=self._timeout) as response:
                raw = response.read(RESPONSE_LIMIT + 1)
        except urllib.error.HTTPError as error:
            # 网关用 4xx/5xx 携带同一个 v1 信封；状态码本身不改变失败语义。
            try:
                raw = error.read(RESPONSE_LIMIT + 1)
            except (TimeoutError, OSError, ValueError):
                raise PlatformFailure("outcome_unknown", timings={
                    "planned_ns": planned, "dispatched_ns": dispatched,
                    "completed_ns": time.monotonic_ns()})
        except (TimeoutError, urllib.error.URLError, OSError, ValueError) as error:
            # 传输层失败、超时、重定向或超限：结果未知，不得重放。
            raise PlatformFailure("outcome_unknown", timings={
                "planned_ns": planned, "dispatched_ns": dispatched,
                "completed_ns": time.monotonic_ns()},
                timed_out=isinstance(error, (TimeoutError, socket.timeout)))
        completed = time.monotonic_ns()
        timings = {"planned_ns": planned, "dispatched_ns": dispatched, "completed_ns": completed}
        return self._decode(raw, body["request_id"], timings), timings


def database_identity(database):
    parts = database.split("/")
    if len(parts) != 3 or not all(re.fullmatch(r"[A-Za-z0-9_-]{1,128}", part) for part in parts):
        raise PlatformFailure("invalid_database")
    tenant, name, branch = parts
    return {"namespace": {"tenant": tenant, "database": name}, "branch": branch}


class PlatformSession:
    """会话策略固定：每次运行一个会话，先 open 后 close，不确定即停用。"""

    def __init__(self, client, identifier, readonly):
        self._client = client
        self.identifier = identifier
        self.readonly = readonly
        self.unusable = False
        self.closed = False

    def _request(self, path, request_id, body):
        if self.unusable:
            raise PlatformFailure("session_unusable")
        if not LABEL.match(request_id):
            raise PlatformFailure("invalid_request_id")
        body = {**body, "session": self.identifier, "request_id": request_id}
        try:
            return self._client.call(path, body)
        except PlatformFailure as failure:
            if failure.code in ("outcome_unknown", "session_moved"):
                self.unusable = True
            raise

    def query(self, sql, request_id):
        if not sql:
            raise PlatformFailure("invalid_sql")
        result, timings = self._request("/v1/sql/query", request_id, {"sql": sql})
        if not isinstance(result, list):
            self.unusable = True
            raise PlatformFailure("outcome_unknown")
        return result, timings

    def execute(self, sql, request_id):
        if not sql:
            raise PlatformFailure("invalid_sql")
        result, timings = self._request("/v1/sql/execute", request_id, {"sql": sql})
        if not isinstance(result, dict) or result.get("executed") is not True:
            self.unusable = True
            raise PlatformFailure("outcome_unknown")
        return timings

    def close(self, request_id):
        if self.closed:
            return None
        result, timings = self._request("/v1/sql/close", request_id, {})
        if not isinstance(result, dict) or result.get("closed") is not True:
            self.unusable = True
            raise PlatformFailure("outcome_unknown")
        self.closed = True
        return timings


def open_session(client, database, request_id):
    result, timings = client.call("/v1/sql/open", {"database": database_identity(database),
                                                   "request_id": request_id})
    if not isinstance(result, dict) or not SESSION_ID.match(str(result.get("session", ""))):
        raise PlatformFailure("outcome_unknown")
    return PlatformSession(client, result["session"], bool(result.get("readonly"))), timings
