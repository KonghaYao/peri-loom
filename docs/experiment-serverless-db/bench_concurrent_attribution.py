#!/usr/bin/env python3
"""Isolated Simple concurrent point-read client/server timing comparison."""
import argparse
import asyncio
import csv
import gzip
import hashlib
import json
import pathlib
import socket
import subprocess
import tempfile
import time

import aiohttp


def percentile(values, fraction):
    values = sorted(values)
    return values[min(len(values) - 1, int((len(values) - 1) * fraction + .999999))]


async def main(args):
    output = pathlib.Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    binary = pathlib.Path(args.binary).resolve(strict=True)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    summaries = []
    with tempfile.TemporaryDirectory(prefix="peri-concurrent-attribution-") as tmp:
        data_dir = pathlib.Path(tmp) / "data"
        with (output / "server.log").open("w") as log:
            child = subprocess.Popen([str(binary), "serve", "--mode", "simple", "--data-dir", str(data_dir), "--listen", f"127.0.0.1:{port}", "--log-level", "error"], stdout=log, stderr=log)
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10), connector=aiohttp.TCPConnector(limit=0)) as session:
                    async def request(method, path, payload=None, token=None):
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
                            if (await request("GET", "/readyz"))[0] == 200:
                                break
                        except (aiohttp.ClientError, asyncio.TimeoutError):
                            pass
                        await asyncio.sleep(.1)
                    else:
                        raise RuntimeError("readiness timeout")
                    initial = json.loads((data_dir / "secrets/initial-admin.json").read_text())
                    status, login, _ = await request("POST", "/api/v1/auth/login", initial)
                    assert status == 200
                    token = login["access_token"]
                    status, created, _ = await request("POST", "/api/v1/databases", {"name": "attribution"}, token)
                    assert status == 202
                    for _ in range(300):
                        status, operation, _ = await request("GET", "/api/v1/operations/" + created["operation_id"], token=token)
                        assert status == 200
                        if operation["state"] == "SUCCEEDED":
                            break
                        assert operation["state"] not in ("FAILED", "CANCELLED")
                        await asyncio.sleep(.1)
                    else:
                        raise RuntimeError("create timeout")
                    query_path = "/data/v1/databases/" + created["database_id"] + "/query"
                    for sql in ("CREATE TABLE bench (id INTEGER PRIMARY KEY, v INTEGER)", "INSERT INTO bench VALUES (1, 42)"):
                        status, body, _ = await request("POST", query_path, {"sql": sql}, token)
                        assert status == 200 and "elapsed_micros" in body
                    query = {"sql": "SELECT v FROM bench WHERE id = 1"}
                    for _ in range(100):
                        status, body, _ = await request("POST", query_path, query, token)
                        assert status == 200 and body["rows"] == [[42]]

                    for round_no, levels in ((1, (32, 64)), (2, (64, 32))):
                        for concurrency in levels:
                            await asyncio.sleep(args.rest)
                            rows = []
                            deadline = time.perf_counter() + args.duration
                            phase_start = time.perf_counter()
                            async def worker(worker_id):
                                index = 0
                                while time.perf_counter() < deadline:
                                    index += 1
                                    started = time.perf_counter_ns()
                                    try:
                                        status, body, client_us = await request("POST", query_path, query, token)
                                        server_us = body.get("elapsed_micros") if isinstance(body, dict) else None
                                        ok = status == 200 and body.get("rows") == [[42]] and isinstance(server_us, int) and server_us >= 0
                                        error = "" if ok else "bad_response"
                                    except Exception as exc:
                                        client_us = (time.perf_counter_ns() - started) / 1000
                                        server_us, status, ok, error = None, 0, False, type(exc).__name__
                                    rows.append({"worker": worker_id, "index": index, "client_us": round(client_us, 3), "server_us": server_us if ok else "", "outside_us": round(client_us - server_us, 3) if ok else "", "status": status, "ok": int(ok), "error": error})
                            await asyncio.gather(*(worker(i) for i in range(concurrency)))
                            wall = time.perf_counter() - phase_start
                            name = f"round{round_no}_c{concurrency}.csv.gz"
                            with gzip.open(output / name, "wt", newline="") as f:
                                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                                writer.writeheader(); writer.writerows(rows)
                            valid = [row for row in rows if row["ok"]]
                            summary = {"round": round_no, "concurrency": concurrency, "duration_s": wall, "requests": len(rows), "success": len(valid), "errors": len(rows)-len(valid), "success_rps": len(valid)/wall, "sample_file": name}
                            for key in ("client_us", "server_us", "outside_us"):
                                values = [float(row[key]) / 1000 for row in valid]
                                for label, fraction in (("p50", .5), ("p95", .95), ("p99", .99)):
                                    summary[f"{key}_{label}_ms"] = percentile(values, fraction) if values else None
                            summaries.append(summary)
                            print(json.dumps(summary), flush=True)
            finally:
                child.terminate()
                try:
                    child.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    child.kill(); child.wait()
    metadata = {"binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "mode": "simple", "workload": "SELECT v FROM bench WHERE id = 1, validate rows [[42]]", "client": "aiohttp persistent connections, fixed concurrency, zero think time", "duration_per_phase_s": args.duration, "rest_s": args.rest, "warmup_requests": 100, "summary": summaries}
    (output / "results.json").write_text(json.dumps(metadata, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", default="target/release/peri-loom")
    parser.add_argument("--output", default="docs/experiment-serverless-db/raw/perf_concurrent_attribution")
    parser.add_argument("--duration", type=float, default=8)
    parser.add_argument("--rest", type=float, default=2)
    asyncio.run(main(parser.parse_args()))
