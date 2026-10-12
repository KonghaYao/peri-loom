"""Explicit read-only capacity sidecar; never starts load, Docker or SQL sessions."""

import argparse
import json
import os
from pathlib import Path
import signal
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "stress"))
import run_guarded as guarded
from capacity_plan import read_plan, write_private
from capacity_collect import capture, normalize, select_job


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--capture", action="store_true")
    action.add_argument("--finalize", action="store_true")
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--job", required=True)
    parser.add_argument("--stop-file", type=Path)
    arguments = parser.parse_args()
    os.umask(0o077)
    try:
        plan = read_plan(arguments.plan)
        job = select_job(plan, arguments.job)
        if arguments.capture:
            guarded.require(arguments.stop_file is not None and arguments.stop_file.is_absolute()
                            and arguments.stop_file.parent == Path(job["output"]) and not arguments.stop_file.exists(), "collector_fresh_owned_stop_file_required")
            interrupted = [False]
            def stop(signum, frame):
                interrupted[0] = True
            signal.signal(signal.SIGTERM, stop)
            signal.signal(signal.SIGINT, stop)
            result = capture(plan, job, arguments.stop_file, lambda: interrupted[0])
        else:
            result = normalize(plan, job)
            write_private(Path(job["output"]) / "capacity-observations.json", result)
        print(json.dumps({"status": result.get("status", "COLLECTED NOT ACCEPTED"), "job": job["id"], "performance_pass": False}))
        return 0 if result.get("status", "COMPLETE") == "COMPLETE" else 1
    except (ValueError, OSError, KeyError, TypeError) as error:
        print(json.dumps({"status": "NOT MEASURED", "reason": str(error) if isinstance(error, ValueError) else "collector_private_error_details_withheld", "performance_pass": False}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
