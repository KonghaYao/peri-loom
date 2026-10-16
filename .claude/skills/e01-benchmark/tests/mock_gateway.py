"""本地模拟网关：只验证适配器机制，**不是** peri-loom 行为验证。

应答形状取自公开证据（platform/sdk/go/client.go、session.go 的 v1 信封），
但模拟服务不代表真实网关语义；真实结论必须来自用户提供的隔离端点。
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class MockGateway:
    """本地模拟网关：**不是** peri-loom 行为验证。

    session=None 时每次 open 返回唯一 32 位小写十六进制会话 ID，用于验证
    真实多会话（会话归属与清理）；显式设置 self.session 时固定返回该值（测试用）。
    """

    def __init__(self, credential, session=None, max_open_sessions=None):
        self.credential = credential
        self.session = session
        self.max_open_sessions = max_open_sessions
        self.issued_sessions = {}
        self.session_usage = {}
        self.closed_sessions = []
        self.rejected_opens = 0
        self._counter = 0
        self.accepted_connections = 0
        self.requests = []
        self.behaviours = {}
        self.rows_for_sql = {}
        self.listener = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.thread = threading.Thread(target=self.listener.serve_forever, daemon=True)
        self.thread.start()

    @property
    def endpoint(self):
        host, port = self.listener.server_address[:2]
        return f"http://{host}:{port}"

    def close(self):
        self.listener.shutdown()
        self.listener.server_close()
        self.thread.join(timeout=5)

    def override(self, path, behaviour):
        self.behaviours[path] = behaviour

    def count(self, path):
        return sum(1 for request in self.requests if request["path"] == path)

    def envelope(self, request_id, result=None, ok=True, error=None):
        return {"version": 1, "request_id": request_id, "ok": ok, "result": result, "error": error}

    def default(self, path, body):
        identifier = body.get("request_id")
        if path == "/v1/sql/open":
            if self.max_open_sessions is not None and len(self.issued_sessions) >= self.max_open_sessions:
                self.rejected_opens += 1
                return 200, self.envelope(identifier, None, ok=False,
                                          error={"code": "sql_unavailable",
                                                 "message": "session admission limit"})
            self._counter += 1
            session = self.session or f"{self._counter:032x}"
            self.issued_sessions[session] = self.issued_sessions.get(session, 0) + 1
            self.session_usage.setdefault(session, 0)
            return 200, self.envelope(identifier, {"session": session, "readonly": False})
        if path == "/v1/sql/query":
            session = body.get("session")
            self.session_usage[session] = self.session_usage.get(session, 0) + 1
            if session not in self.issued_sessions:
                return 200, self.envelope(identifier, None, ok=False,
                                          error={"code": "forbidden", "message": "unknown session"})
            return 200, self.envelope(identifier, self.rows_for_sql.get(body.get("sql", ""), []))
        if path == "/v1/sql/execute":
            self.session_usage[body.get("session")] = self.session_usage.get(body.get("session"), 0) + 1
            return 200, self.envelope(identifier, {"executed": True})
        if path == "/v1/sql/close":
            if body.get("session") in self.issued_sessions:
                self.closed_sessions.append(body["session"])
            return 200, self.envelope(identifier, {"closed": True})
        return 404, self.envelope(identifier, ok=False, error={"code": "invalid_request",
                                                              "message": "unknown path"})

    def _handler(self):
        gateway = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            gateway = self

            def log_message(self, *arguments):
                pass

            def setup(self):
                Handler.gateway.accepted_connections += 1
                super().setup()

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length)
                try:
                    body = json.loads(raw)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    body = {}
                if body.get("sql") == "HALF":
                    self.send_response(200)
                    self.send_header("Content-Length", "200")
                    self.end_headers()
                    self.wfile.write(b"{\"version\":1")
                    self.close_connection = True
                    return
                gateway.requests.append({
                    "path": self.path, "body": body,
                    "authorization": self.headers.get("Authorization"),
                    "request_id": self.headers.get("X-Request-ID")})
                behaviour = gateway.behaviours.get(self.path)
                try:
                    if behaviour is None:
                        status, payload = gateway.default(self.path, body)
                    else:
                        status, payload = behaviour(body)
                except Exception:
                    # 模拟连接被直接切断：客户端必须把它当作结果未知。
                    self.close_connection = True
                    return
                encoded = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

        return Handler
