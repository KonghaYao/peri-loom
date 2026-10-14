#!/usr/bin/env python3
"""Inject existing RustFS credentials privately; create only a new isolated bucket."""
import argparse
import datetime
import hashlib
import hmac
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import urllib.request
import uuid


def create_bucket(environment):
    bucket = environment["S3_BUCKET"]
    now = datetime.datetime.now(datetime.timezone.utc)
    timestamp, date = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    empty = hashlib.sha256(b"").hexdigest()
    headers = {"host": "127.0.0.1:9000", "x-amz-content-sha256": empty, "x-amz-date": timestamp}
    signed = ";".join(sorted(headers))
    canonical = "PUT\n/" + bucket + "\n\n" + "".join(name + ":" + headers[name] + "\n" for name in sorted(headers)) + "\n" + signed + "\n" + empty
    scope = date + "/us-east-1/s3/aws4_request"
    to_sign = "AWS4-HMAC-SHA256\n" + timestamp + "\n" + scope + "\n" + hashlib.sha256(canonical.encode()).hexdigest()
    key = ("AWS4" + environment["S3_SECRET_ACCESS_KEY"]).encode()
    for part in [date, "us-east-1", "s3", "aws4_request"]:
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    headers["Authorization"] = "AWS4-HMAC-SHA256 Credential=" + environment["S3_ACCESS_KEY_ID"] + "/" + scope + ", SignedHeaders=" + signed + ", Signature=" + signature
    request = urllib.request.Request("http://127.0.0.1:9000/" + bucket, data=b"", headers=headers, method="PUT")
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=15) as response:
        if response.status not in (200, 204):
            raise RuntimeError("isolated_bucket_create_failed")


def main():
    signal.signal(signal.SIGTERM, lambda signum, frame: (_ for _ in ()).throw(KeyboardInterrupt()))
    parser = argparse.ArgumentParser()
    parser.add_argument("--driver", choices=["native", "http", "dirty"], required=True)
    parser.add_argument("--round", choices=["baseline", "repeat"], required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--build-provenance", type=Path)
    parser.add_argument("--operations", type=int, default=100)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output exists")
    os.umask(0o077)
    directory = Path(tempfile.mkdtemp(prefix="peri-perf-s3-"))
    try:
        result = subprocess.run(["docker", "inspect", "1Panel-rustfs-TcP9"], capture_output=True, text=True, timeout=10, check=True)
        service = json.loads(result.stdout)[0]
        private = dict(item.split("=", 1) for item in service["Config"]["Env"] if "=" in item)
        environment = {name: os.environ[name] for name in ["PATH", "HOME", "TMPDIR", "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG"] if name in os.environ}
        environment.update(S3_ENDPOINT="http://127.0.0.1:9000", S3_REGION="us-east-1",
                           S3_BUCKET="peri-perf-" + uuid.uuid4().hex,
                           S3_ACCESS_KEY_ID=private["RUSTFS_ACCESS_KEY"], S3_SECRET_ACCESS_KEY=private["RUSTFS_SECRET_KEY"])
        create_bucket(environment)
        if args.driver in {"http", "dirty"}:
            environment["S3_ENDPOINT"] = "http://host.docker.internal:9000"
        credentials = directory / "privateenv.json"
        with credentials.open("x") as output:
            json.dump({key: value for key, value in environment.items() if key.startswith("S3_")}, output)
        credentials.chmod(0o600)
        fixture = {"bucket": environment["S3_BUCKET"], "endpoint": environment["S3_ENDPOINT"],
                   "rustfs_container_id": service["Id"], "rustfs_image_id": service["Image"],
                   "private_directory": str(directory), "bucket_created": True, "retained": True}
        (directory / "fixture.json").write_text(json.dumps(fixture, indent=2))
        script = Path(__file__).with_name({"native": "runner.py", "http": "whole_stack.py", "dirty": "dirty_trial.py"}[args.driver])
        command = [sys.executable, str(script), "--backend", "s3", "--round", args.round, "--output", str(args.output)]
        if args.driver == "native":
            command.extend(["--operations", str(args.operations)])
            if args.binary:
                command.extend(["--binary", str(args.binary)])
            if args.build_provenance:
                command.extend(["--build-provenance", str(args.build_provenance)])
        child = subprocess.Popen(command, env=environment, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            returncode = child.wait(timeout=420)
        finally:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=120)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
        fixture["driver_returncode"] = returncode
        after = json.loads(subprocess.check_output(["docker", "inspect", "1Panel-rustfs-TcP9"], text=True, timeout=10))[0]
        fixture["rustfs_container_id_after"] = after["Id"]
        fixture["rustfs_image_id_after"] = after["Image"]
        fixture["rustfs_identity_unchanged"] = service["Id"] == after["Id"] and service["Image"] == after["Image"]
        if args.output.exists():
            report = json.loads(args.output.read_text())
            report["s3_fixture"] = fixture
            if not fixture["rustfs_identity_unchanged"]:
                report["pass"] = False
                report["status"] = "FAIL_OR_NOTRUN"
                report.setdefault("reasons", []).append("rustfs_identity_changed")
                returncode = 1
            args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({"output": str(args.output), "private_directory": str(directory), "returncode": returncode}))
    except Exception as failure:
        print(json.dumps({"status": "blocked", "failure_type": type(failure).__name__, "private_directory": str(directory)}))
        return 1
    return returncode


if __name__ == "__main__":
    sys.exit(main())
