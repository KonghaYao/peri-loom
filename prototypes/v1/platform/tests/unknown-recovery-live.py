import argparse
import json
import os
import pathlib
import re
import subprocess
import sys


CASES = ("open", "management-open", "checkpoint", "close", "gateway-close", "commit")
MARKER = re.compile(r"TRUE_FAULT case=([a-z-]+) native_ack=1 dropped_response=1 worker=([A-Za-z0-9_-]+) native_request_id=([A-Za-z0-9_-]+) durable_reload=isolated safe_reads=no_native_open decision=new_database_required historical_cause=not_attributed owned_worker_stop=proven local_resources_retired=1 namespace_isolated=1")
COMMIT_WITNESS = "COMMIT_WITNESS native_ack=1 ledger_rows=1 retained_session_read=1 commit_replays=0 blind_close=0 new_sessions=0"


def command(native_binary, case, race=False):
    binary = pathlib.Path(native_binary).resolve(strict=True)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError("explicit built native executable required")
    selected = CASES if case == "all" else (case,)
    if any(item not in CASES for item in selected):
        raise ValueError("unsupported fresh fault case")
    selector = "^TestUnknownRecoveryNativeResponseLoss$/^(" + "|".join(re.escape(item) for item in selected) + ")$"
    arguments = ["go", "test", ".", "-count=1", "-timeout=8m", "-v", "-run", selector]
    if race:
        arguments.append("-race")
    environment = {name: os.environ[name] for name in ("PATH", "HOME", "TMPDIR", "LANG", "TZ") if name in os.environ}
    environment["PERI_RUST_BINARY"] = str(binary)
    if race:
        environment["PERI_P6_VERIFY_RACE_CLI"] = "1"
    return arguments, environment, selected


def report(stdout, selected, returncode):
    matches = MARKER.findall(stdout)
    observed = [item[0] for item in matches]
    passed = returncode == 0 and sorted(observed) == sorted(selected) and len(observed) == len(set(observed)) and "--- SKIP:" not in stdout
    if "commit" in selected:
        passed = passed and stdout.count(COMMIT_WITNESS) == 1
    result = {
        "version": 1,
        "passed": passed,
        "fresh_fixtures_only": True,
        "historical_cause": "not_attributed",
        "recovery": "retain_isolation_new_database_or_fixture_required",
        "acceptance": {
            "native_ack_consumed_before_response_loss": passed,
            "correlated_outcome_unknown": passed,
            "business_ack_separate_from_cleanup_unknown": passed,
            "durable_reload_isolated": passed,
            "record_reads_do_not_open_native_sessions": passed,
            "retry_clear_and_reopen_denied": passed,
            "credentials_and_sessions_not_public": passed,
            "owned_worker_stop_retires_only_local_resources": passed,
        },
        "cases": [{"case": case, "worker": worker, "native_request_id": request_id} for case, worker, request_id in matches],
    }
    if "commit" in selected:
        result["acceptance"]["commit_ack_loss_with_independent_retained_session_ledger_read"] = passed
    return result


def main():
    parser = argparse.ArgumentParser(description="Fresh real native RPC response-loss integration; no existing private config, Docker, S3, benchmark, or historical attribution.")
    parser.add_argument("--native-binary", required=True)
    parser.add_argument("--case", choices=("all",) + CASES, default="all")
    parser.add_argument("--race", action="store_true")
    parser.add_argument("--output", required=True)
    arguments = parser.parse_args()
    output = pathlib.Path(arguments.output).absolute()
    if output.exists() or output.is_symlink():
        parser.error("new output path required; refusing overwrite")
    invocation, environment, selected = command(arguments.native_binary, arguments.case, arguments.race)
    platform = pathlib.Path(__file__).resolve().parents[1]
    completed = subprocess.run(invocation, cwd=platform, env=environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=510, check=False)
    result = report(completed.stdout, selected, completed.returncode)
    descriptor = os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    log = pathlib.Path(str(output) + ".log")
    descriptor = os.open(log, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(completed.stdout)
    print(json.dumps(result))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
