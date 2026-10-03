#!/usr/bin/env python3
"""Reproducible localhost Simple SQL load test; writes no credentials to results."""
import argparse
import asyncio
import gzip
import json
import pathlib
import platform
import socket
import statistics
import subprocess
import tempfile
import time
from datetime import datetime, timezone

import aiohttp
import psutil


async def post_json(session, url, data, token=None):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    async with session.post(url, json=data, headers=headers) as response:
        body = await response.json(content_type=None)
        return response.status, body


async def get_json(session, url, token):
    async with session.get(url, headers={"Authorization": f"Bearer {token}"}) as response:
        return response.status, await response.json(content_type=None)


async def main(args):
    out = pathlib.Path(args.output).resolve()
    out.mkdir(parents=True, exist_ok=True)
    binary = pathlib.Path(args.binary).resolve(strict=True)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    results = []
    with tempfile.TemporaryDirectory(prefix="peri-loom-load-") as tmp:
        data_dir = pathlib.Path(tmp) / "data"
        with (out / "server.log").open("w") as log:
            child = subprocess.Popen([str(binary), "serve", "--mode", "simple", "--data-dir", str(data_dir), "--listen", f"127.0.0.1:{port}", "--log-level", "error"], stdout=log, stderr=log)
            try:
                timeout = aiohttp.ClientTimeout(total=10)
                connector = aiohttp.TCPConnector(limit=0)
                async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                    for _ in range(300):
                        if child.poll() is not None:
                            raise RuntimeError(f"server exited {child.returncode}")
                        try:
                            async with session.get(base + "/readyz") as response:
                                if response.status == 200:
                                    break
                        except (aiohttp.ClientError, asyncio.TimeoutError):
                            pass
                        await asyncio.sleep(0.1)
                    else:
                        raise RuntimeError("readiness timeout")
                    status, deployment = await get_json(session, base + "/api/v1/deployment", "unused")
                    assert status == 200 and deployment["mode"] == "simple"
                    initial = json.loads((data_dir / "secrets/initial-admin.json").read_text())
                    status, login = await post_json(session, base + "/api/v1/auth/login", initial)
                    assert status == 200, (status, login)
                    token = login["access_token"]
                    status, created = await post_json(session, base + "/api/v1/databases", {"name": "load-test"}, token)
                    assert status == 202, (status, created)
                    op_id = created["operation_id"]
                    db_id = created["database_id"]
                    for _ in range(300):
                        status, operation = await get_json(session, base + "/api/v1/operations/" + op_id, token)
                        assert status == 200, (status, operation)
                        if operation["state"] == "SUCCEEDED":
                            break
                        if operation["state"] in ("FAILED", "CANCELLED"):
                            raise RuntimeError(f"create failed: {operation}")
                        await asyncio.sleep(0.1)
                    else:
                        raise RuntimeError("create database timeout")
                    query_url = base + f"/data/v1/databases/{db_id}/query"
                    async def query(sql):
                        return await post_json(session, query_url, {"sql": sql}, token)
                    for sql in ("CREATE TABLE bench (id INTEGER PRIMARY KEY, v INTEGER)", "INSERT INTO bench (id,v) VALUES (1,42)"):
                        status, body = await query(sql)
                        assert status == 200 and "error" not in body, (status, body)
                    read_sql = "SELECT v FROM bench WHERE id = 1"
                    status, body = await query(read_sql)
                    assert status == 200 and body.get("rows") == [[42]], (status, body)
                    for _ in range(100):
                        status, body = await query(read_sql)
                        assert status == 200 and body.get("rows") == [[42]], (status, body)
                    process = psutil.Process(child.pid)
                    phases = [("read", args.levels, args.duration)]
                    if args.write_levels:
                        phases.append(("write", args.write_levels, args.write_duration))
                    for workload, phase_levels, phase_duration in phases:
                      for round_no, levels in ((1, phase_levels), (2, list(reversed(phase_levels)))):
                        for concurrency in levels:
                            await asyncio.sleep(args.rest)
                            samples = []
                            failures = []
                            cpu0 = process.cpu_times()
                            rss_peak = process.memory_info().rss
                            wall0 = time.perf_counter()
                            deadline = wall0 + phase_duration
                            async def worker():
                                nonlocal rss_peak
                                while time.perf_counter() < deadline:
                                    start = time.perf_counter()
                                    try:
                                        sql = read_sql if workload == "read" else "INSERT INTO bench (v) VALUES (42)"
                                        status, body = await query(sql)
                                        elapsed = (time.perf_counter() - start) * 1000
                                        ok = status == 200 and (body.get("rows") == [[42]] if workload == "read" else "error" not in body)
                                        samples.append(elapsed)
                                        if not ok:
                                            failures.append({"status": status, "body": str(body)[:200]})
                                    except Exception as exc:
                                        samples.append((time.perf_counter() - start) * 1000)
                                        failures.append({"exception": type(exc).__name__, "message": str(exc)[:200]})
                            await asyncio.gather(*(worker() for _ in range(concurrency)))
                            wall = time.perf_counter() - wall0
                            cpu1 = process.cpu_times()
                            rss_peak = max(rss_peak, process.memory_info().rss)
                            ordered = sorted(samples)
                            def percentile(p):
                                if not ordered:
                                    return None
                                return ordered[min(len(ordered)-1, max(0, int((len(ordered)-1)*p + 0.999999)))]
                            row = {"workload": workload, "round": round_no, "concurrency": concurrency, "duration_s": wall, "requests": len(samples), "success": len(samples)-len(failures), "errors": len(failures), "rps": len(samples)/wall, "success_rps": (len(samples)-len(failures))/wall, "p50_ms": percentile(.5), "p95_ms": percentile(.95), "p99_ms": percentile(.99), "max_ms": ordered[-1] if ordered else None, "mean_ms": statistics.mean(samples) if samples else None, "server_cpu_cores": ((cpu1.user+cpu1.system)-(cpu0.user+cpu0.system))/wall, "server_rss_two_point_max_mib": rss_peak/1024/1024, "error_examples": failures[:5]}
                            results.append(row)
                            with gzip.open(out / f"latency_{workload}_round{round_no}_c{concurrency}.csv.gz", "wt") as f:
                                f.write("latency_ms\n")
                                for latency in samples:
                                    f.write(f"{latency:.6f}\n")
                            print(json.dumps(row, ensure_ascii=False), flush=True)
                    if args.write_levels:
                        status, count = await query("SELECT count(*) FROM bench")
                        expected = 1 + sum(row["success"] for row in results if row["workload"] == "write")
                        assert status == 200 and count.get("rows") == [[expected]], (expected, count)
                    metadata = {"timestamp_utc": datetime.now(timezone.utc).isoformat(), "platform": platform.platform(), "machine": platform.machine(), "logical_cpus": psutil.cpu_count(), "physical_cpus": psutil.cpu_count(logical=False), "memory_bytes": psutil.virtual_memory().total, "binary": str(binary), "binary_sha256": __import__("hashlib").sha256(binary.read_bytes()).hexdigest(), "deployment": deployment, "workload": {"read_sql": read_sql, "write_sql": "INSERT INTO bench (v) VALUES (42)" if args.write_levels else None, "expected_rows": [[42]], "protocol": "HTTP POST /data/v1/databases/{db_id}/query", "client": "aiohttp 3.13.5, persistent connections, fixed concurrency, zero think time", "warmup_requests": 100, "read_duration_s": args.duration, "write_duration_s": args.write_duration if args.write_levels else None, "rest_s": args.rest}, "rows": results}
                    (out / "results.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n")
            finally:
                child.terminate()
                try:
                    child.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", default="target/debug/peri-loom")
    parser.add_argument("--output", default="docs/experiment-serverless-db/raw")
    parser.add_argument("--levels", nargs="+", type=int, default=[1, 8, 32, 64])
    parser.add_argument("--duration", type=float, default=8.0)
    parser.add_argument("--write-levels", nargs="*", type=int, default=[])
    parser.add_argument("--write-duration", type=float, default=5.0)
    parser.add_argument("--rest", type=float, default=2.0)
    asyncio.run(main(parser.parse_args()))
