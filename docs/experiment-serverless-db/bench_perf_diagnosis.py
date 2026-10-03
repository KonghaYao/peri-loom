#!/usr/bin/env python3
"""Sequential Simple-mode latency decomposition on an isolated local instance."""
import asyncio
import argparse
import csv
import hashlib
import json
import pathlib
import socket
import subprocess
import tempfile
import time

import aiohttp


async def main(args):
    output = pathlib.Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    binary = pathlib.Path(args.binary).resolve(strict=True)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    rows = []
    with tempfile.TemporaryDirectory(prefix="peri-perf-diag-") as tmp:
        data_dir = pathlib.Path(tmp) / "data"
        with (output / "server.log").open("w") as log:
            child = subprocess.Popen([str(binary), "serve", "--mode", "simple", "--data-dir", str(data_dir), "--listen", f"127.0.0.1:{port}", "--log-level", "error"], stdout=log, stderr=log)
            try:
                timeout = aiohttp.ClientTimeout(total=10)
                async with aiohttp.ClientSession(timeout=timeout, connector=aiohttp.TCPConnector(limit=1)) as session:
                    async def call(method, path, payload=None, token=None):
                        headers = {"Authorization": f"Bearer {token}"} if token else {}
                        start = time.perf_counter_ns()
                        async with session.request(method, base + path, json=payload, headers=headers) as response:
                            body = await response.json(content_type=None)
                            status = response.status
                        return status, body, (time.perf_counter_ns() - start) / 1000

                    for _ in range(300):
                        if child.poll() is not None:
                            raise RuntimeError(f"server exited {child.returncode}")
                        try:
                            if (await call("GET", "/readyz"))[0] == 200:
                                break
                        except (aiohttp.ClientError, asyncio.TimeoutError):
                            pass
                        await asyncio.sleep(.1)
                    else:
                        raise RuntimeError("readiness timeout")
                    initial = json.loads((data_dir / "secrets/initial-admin.json").read_text())
                    status, login, _ = await call("POST", "/api/v1/auth/login", initial)
                    assert status == 200
                    token = login["access_token"]
                    status, created, _ = await call("POST", "/api/v1/databases", {"name": "perf-diagnosis"}, token)
                    assert status == 202
                    for _ in range(300):
                        status, operation, _ = await call("GET", "/api/v1/operations/" + created["operation_id"], token=token)
                        assert status == 200
                        if operation["state"] == "SUCCEEDED":
                            break
                        assert operation["state"] not in ("FAILED", "CANCELLED")
                        await asyncio.sleep(.1)
                    else:
                        raise RuntimeError("create timeout")
                    query_path = "/data/v1/databases/" + created["database_id"] + "/query"
                    for sql in ("CREATE TABLE bench (id INTEGER PRIMARY KEY, v INTEGER)", "INSERT INTO bench VALUES (1, 42)"):
                        status, body, _ = await call("POST", query_path, {"sql": sql}, token)
                        assert status == 200 and "elapsed_micros" in body
                    # All routes share one persistent aiohttp connection; all operations are sequential.
                    for _ in range(100):
                        status, body, _ = await call("POST", query_path, {"sql": "SELECT v FROM bench WHERE id=1"}, token)
                        assert status == 200 and body["rows"] == [[42]]
                    for round_no in (1, 2):
                        for kind in ("readyz", "deployment", "read", "write"):
                            for index in range(120):
                                if kind == "readyz":
                                    status, body, client_us = await call("GET", "/readyz")
                                    assert status == 200
                                elif kind == "deployment":
                                    status, body, client_us = await call("GET", "/api/v1/deployment")
                                    assert status == 200 and body["mode"] == "simple"
                                else:
                                    sql = "SELECT v FROM bench WHERE id=1" if kind == "read" else "INSERT INTO bench(v) VALUES (42)"
                                    status, body, client_us = await call("POST", query_path, {"sql": sql}, token)
                                    assert status == 200
                                    if kind == "read":
                                        assert body["rows"] == [[42]]
                                    else:
                                        assert body["affected_rows"] == 1
                                server_us = body.get("elapsed_micros") if isinstance(body, dict) else None
                                rows.append({"round": round_no, "kind": kind, "index": index, "client_us": round(client_us, 3), "server_us": server_us, "outside_server_us": round(client_us-server_us, 3) if server_us is not None else None})
                    status, count, _ = await call("POST", query_path, {"sql": "SELECT count(*) FROM bench"}, token)
                    assert status == 200 and count["rows"] == [[241]]
            finally:
                child.terminate()
                try:
                    child.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    child.kill(); child.wait()
    with (output / "samples.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys(), lineterminator="\n")
        writer.writeheader(); writer.writerows(rows)
    metadata = {"binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "requests": len(rows), "rounds": 2, "samples_per_kind_round": 120, "mode": "simple", "client": "aiohttp persistent single connection, sequential requests", "data_dir": "temporary, deleted after test", "server_log": "server.log", "point_read": "SELECT v FROM bench WHERE id=1, one prepopulated row", "write": "INSERT INTO bench(v) VALUES (42), count verified 241"}
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(json.dumps(metadata))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", default="target/release/peri-loom")
    parser.add_argument("--output", default="docs/experiment-serverless-db/raw/perf_diagnosis")
    asyncio.run(main(parser.parse_args()))
