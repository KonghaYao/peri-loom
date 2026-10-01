#!/usr/bin/env python3
"""在临时目录验证独立二进制，无需源码前端目录或外部服务。"""
import argparse
import json
import pathlib
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=pathlib.Path)
    binary = parser.parse_args().binary.resolve(strict=True)
    subprocess.run([str(binary), "--version"], check=True, timeout=10)
    with tempfile.TemporaryDirectory(prefix="peri-simple-smoke-") as temp:
        root = pathlib.Path(temp)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        base = f"http://127.0.0.1:{port}"
        with (root / "server.log").open("w+") as log:
            process = subprocess.Popen(
                [str(binary), "serve", "--mode", "simple", "--data-dir",
                 str(root / "data"), "--listen", f"127.0.0.1:{port}"],
                cwd=root, stdout=log, stderr=log,
            )
            try:
                for _ in range(100):
                    if process.poll() is not None:
                        raise RuntimeError("Simple 提前退出")
                    try:
                        with urllib.request.urlopen(base + "/readyz", timeout=2) as response:
                            assert json.load(response)["ready"]
                        break
                    except (OSError, AssertionError):
                        time.sleep(0.1)
                else:
                    raise RuntimeError("Simple readiness 超时")
                with urllib.request.urlopen(base + "/api/v1/deployment", timeout=5) as response:
                    info = json.load(response)
                    assert info["mode"] == "simple"
                    assert info["durability"] == "local_fsync"
                with urllib.request.urlopen(base + "/", timeout=5) as response:
                    assert "text/html" in response.headers["Content-Type"]
                    assert b"<html" in response.read().lower()
                try:
                    urllib.request.urlopen(base + "/api/v1/unknown", timeout=5)
                except urllib.error.HTTPError as error:
                    assert error.code == 404
                    assert "application/json" in error.headers["Content-Type"]
                else:
                    raise AssertionError("未知 API 没有返回 404")
            except Exception:
                log.seek(0)
                print(log.read())
                raise
            finally:
                process.terminate()
                try:
                    process.wait(timeout=40)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise RuntimeError("Simple 正常停止超时")
            assert process.returncode == 0, "Simple 停止失败"
    print("Simple 原生二进制：就绪、能力、内嵌 Web、API 404、正常停止均通过")


if __name__ == "__main__":
    main()
