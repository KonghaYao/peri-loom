#!/usr/bin/env python3
"""Separate fresh-fixture dirty restart experiment; does not replace clean NOTRUN."""
import argparse
import os
from pathlib import Path
import signal
import sys
import time
import uuid

from whole_stack import Reliability, verifier


class DirtyTrial(Reliability):
    expected_check_count = 2

    def lifecycle(self):
        self.report["trial"] = "independent_fresh_dirty_restart"
        database = self.configuration["database"]
        table = "dirty_" + uuid.uuid4().hex[:12]
        session = self.call("dirty_initial_open", "/v1/sql/open", {"database": database})
        self.call("dirty_write_ACK", "/v1/sql/execute", {"session": session["session"],
                  "sql": f"CREATE TABLE {table}(id INTEGER PRIMARY KEY,payload TEXT);INSERT INTO {table} VALUES(1,'dirty_ACK')"})
        actual = self.call("dirty_before_kill_read", "/v1/sql/query", {"session": session["session"],
                           "sql": f"SELECT id,payload FROM {table} ORDER BY id"})
        self.report["checks"].append({"stage": "dirty_before_kill_read", "actual": actual, "pass": actual == [[1, "dirty_ACK"]]})
        self.report["stage"] = "independent_dirty_restart"
        self.inspect()
        self.docker("kill", "--signal", "KILL", self.container)
        self.docker("wait", self.container, timeout=10)
        started = time.monotonic()
        self.docker("start", self.container)
        self.ready()
        self.report["dirty_restart_to_ready_ms"] = (time.monotonic() - started) * 1000
        session = self.call("dirty_recovery_open", "/v1/sql/open", {"database": database})
        actual = self.call("dirty_recovery_read", "/v1/sql/query", {"session": session["session"],
                           "sql": f"SELECT id,payload FROM {table} ORDER BY id"})
        self.report["checks"].append({"stage": "dirty_recovery_read", "actual": actual, "pass": actual == [[1, "dirty_ACK"]]})
        self.call("dirty_final_close", "/v1/sql/close", {"session": session["session"]})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", default="peri-loom:goal90-r26-20261009")
    parser.add_argument("--backend", choices=["local", "s3"], default="local")
    parser.add_argument("--round", choices=["baseline", "repeat"], required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output exists")
    args.ready_timeout, args.smoke_timeout, args.stop_timeout = 45, 120, 30
    args.writer_admission, args.capacity_trace = False, False
    os.umask(0o077)
    signal.signal(signal.SIGTERM, lambda signum, frame: (_ for _ in ()).throw(KeyboardInterrupt()))
    experiment = DirtyTrial(args)
    result = experiment.experiment()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    verifier.private_json(args.output, result)
    print(str(args.output) + " status=" + result["status"])
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
