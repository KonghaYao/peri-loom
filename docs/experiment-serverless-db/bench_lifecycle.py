#!/usr/bin/env python3
"""Measure Simple startup and first query after an explicit stop."""
import asyncio
import json
import pathlib
import socket
import subprocess
import tempfile
import time

import aiohttp
import psutil

from bench_simple import get_json, post_json


async def main():
    out = pathlib.Path("docs/experiment-serverless-db/raw/release/lifecycle.json")
    binary = pathlib.Path("target/release/peri-loom").resolve()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    with tempfile.TemporaryDirectory(prefix="peri-lifecycle-") as tmp:
        data = pathlib.Path(tmp) / "data"
        with (out.parent / "lifecycle-server.log").open("w") as log:
            start = time.perf_counter()
            child = subprocess.Popen([str(binary), "serve", "--mode", "simple", "--data-dir", str(data), "--listen", f"127.0.0.1:{port}", "--log-level", "error"], stdout=log, stderr=log)
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                    for _ in range(300):
                        try:
                            async with session.get(base + "/readyz") as response:
                                if response.status == 200:
                                    break
                        except (aiohttp.ClientError, asyncio.TimeoutError):
                            pass
                        await asyncio.sleep(.1)
                    else:
                        raise RuntimeError("not ready")
                    ready_ms = (time.perf_counter() - start) * 1000
                    initial = json.loads((data / "secrets/initial-admin.json").read_text())
                    status, login = await post_json(session, base + "/api/v1/auth/login", initial)
                    assert status == 200
                    token = login["access_token"]
                    async def wait_operation(op_id):
                        for _ in range(300):
                            status, op = await get_json(session, base + "/api/v1/operations/" + op_id, token)
                            assert status == 200
                            if op["state"] == "SUCCEEDED":
                                return
                            if op["state"] in ("FAILED", "CANCELLED"):
                                raise RuntimeError(str(op))
                            await asyncio.sleep(.1)
                        raise RuntimeError("operation timeout")
                    samples = []
                    for i in range(5):
                        status, created = await post_json(session, base + "/api/v1/databases", {"name": f"lifecycle-{i}"}, token)
                        assert status == 202, (status, created)
                        db_id = created["database_id"]
                        await wait_operation(created["operation_id"])
                        url = base + f"/data/v1/databases/{db_id}/query"
                        status, body = await post_json(session, url, {"sql": "SELECT 1"}, token)
                        assert status == 200 and body.get("rows") == [[1]], (status, body)
                        status, stopped = await post_json(session, base + f"/api/v1/databases/{db_id}/stop", {}, token)
                        assert status == 202, (status, stopped)
                        await wait_operation(stopped["operation_id"])
                        t0 = time.perf_counter()
                        status, body = await post_json(session, url, {"sql": "SELECT 1"}, token)
                        elapsed_ms = (time.perf_counter() - t0) * 1000
                        samples.append({"sample": i + 1, "elapsed_ms": elapsed_ms, "http_status": status, "rows": body.get("rows"), "error": body.get("error")})
                    output = {"startup_ready_ms": ready_ms, "rss_after_lifecycle_mib": psutil.Process(child.pid).memory_info().rss / 1024 / 1024, "samples": samples, "note": "5 distinct databases, each SELECT 1 after explicit stop operation completes; localhost HTTP end-to-end latency"}
                    out.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n")
                    print(json.dumps(output, ensure_ascii=False))
            finally:
                child.terminate()
                try:
                    child.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()


if __name__ == "__main__":
    asyncio.run(main())
