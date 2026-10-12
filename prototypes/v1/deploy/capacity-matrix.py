"""Freeze/execute/analyze the D05/Q02 functional matrix. Template/freeze/analyze never send load.

Python >=3.12, macOS guard. See stress/CAPACITY.md for Main commands and acceptance.
Only explicit --run invokes the existing guarded peri-stress CLI, serially, without retries.
"""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "stress"))
import run_guarded as guarded
from capacity_evidence import analyze, collect_job
from capacity_metrics import knee
from capacity_plan import digest, expected_phases, freeze, minimum_sessions, read_plan, template, write_private


def new_verification(arguments, verification_class=None):
    selected = argparse.Namespace(**{**vars(arguments), "writer_admission": True, "capacity_trace": True})
    if verification_class is None:
        path = Path(__file__).with_name("verify-image.py")
        spec = importlib.util.spec_from_file_location("capacity_owned_verification", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        verification_class = module.Verification
    return verification_class(selected)


def run(plan, selected=None, invoke=None):
    root = Path(plan["config"]["output"])
    guarded.private(root, 0o700, True)
    guarded.require(plan["config"]["external_busy"] is False, "external_busy_no_attributable_capacity_or_fairness_run")
    guarded.require(not (root / "matrix-stop.json").exists(), "first_matrix_stop_retained_no_automatic_continue")
    jobs = [job for job in plan["jobs"] if selected is None or job["id"] == selected]
    guarded.require(bool(jobs), "unknown_job")
    lock_path = root / ".matrix.lock"
    lock = os.open(lock_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        for job in jobs:
            output = Path(job["output"])
            guarded.require(set(path.name for path in output.iterdir()) == {"guard-spec.json"}, "job_has_original_evidence_no_rerun")
            if job["structural_unmeasured"]:
                continue
            if job.get("extension_round") is not None:
                prior = [previous for previous in plan["jobs"] if previous["purpose"] == "hot_knee"
                         and previous["workload"] == job["workload"] and previous["concurrency"] == job["concurrency"]
                         and previous["rate"] < job["rate"]]
                points = []
                found = False
                for previous in prior:
                    if points and knee(points, plan["config"]["acceptance"])["status"] == "OBSERVED FUNCTIONAL KNEE":
                        found = True
                        break
                    measured, _, error = collect_job(previous, plan)
                    cell = measured.get(("isolated_primary_measure", job["concurrency"]), {})
                    if error or cell.get("status") != "MEASURED":
                        raise ValueError("extension_requires_all_prior_original_points_MEASURED_no_FAIL_Unknown_retry")
                    points.append(cell)
                if found or knee(points, plan["config"]["acceptance"])["status"] != "NOT FOUND":
                    continue
            if invoke is None:
                previous = sys.argv
                try:
                    sys.argv = [str(Path(guarded.__file__)), "--spec", job["spec"]]
                    code = guarded.main()
                finally:
                    sys.argv = previous
            else:
                code = invoke(job)
            reason = None
            if code != 0:
                reason = "guard/CLI nonzero; preserve Unknown/session state, no Close/replay/probe"
            else:
                try:
                    cells, _, error = collect_job(job, plan)
                    required = [(phase, job["concurrency"]) for phase in expected_phases(job["topology"])
                                if job["concurrency"] >= minimum_sessions(job["topology"], phase)]
                    if error or not required or any(cells.get(key, {}).get("status") != "MEASURED" for key in required):
                        reason = "FAIL/NOT MEASURED required phase; frozen fairness/IO/resource/ledger evidence incomplete"
                except (ValueError, OSError, KeyError, TypeError):
                    reason = "NOT MEASURED malformed/missing original evidence"
            if reason:
                write_private(root / "matrix-stop.json", {"job": job["id"], "status": "HOLD", "reason": reason, "original_exit": code})
                return 1
        return 0
    finally:
        if lock_path.exists() and guarded.metadata(lock_path)[:2] == [os.fstat(lock).st_dev, os.fstat(lock).st_ino]:
            lock_path.unlink()
        os.close(lock)


def verify_job(plan, identifier):
    jobs = [job for job in plan["jobs"] if job["id"] == identifier]
    guarded.require(len(jobs) == 1, "unknown_job")
    job = jobs[0]
    root = Path(plan["config"]["output"])
    guarded.require(not (root / "matrix-stop.json").exists(), "first_matrix_stop_retained_no_automatic_continue")
    try:
        cells, evidence, error = collect_job(job, plan)
    except (ValueError, OSError, KeyError, TypeError):
        cells, evidence, error = {}, {}, "NOT MEASURED malformed/missing original collector or run evidence"
    required = [(phase, job["concurrency"]) for phase in expected_phases(job["topology"])]
    if error or any(cells.get(key, {}).get("status") != "MEASURED" for key in required):
        write_private(root / "matrix-stop.json", {"job": identifier, "status": "HOLD", "reason": "original collector/guard/phase oracle FAIL or NOT MEASURED", "original_exit": None})
        return {"status": "NOT MEASURED", "job": identifier, "performance_pass": False}
    result = {"status": "MEASURED", "job": identifier, "spec_sha256": job["spec_sha256"], "evidence_manifest": evidence, "performance_pass": False}
    write_private(Path(job["output"]) / "capacity-verification.json", result)
    return result


def next_job(plan):
    guarded.require(not (Path(plan["config"]["output"]) / "matrix-stop.json").exists(), "first_matrix_stop_retained_no_automatic_continue")
    for job in plan["jobs"]:
        root = Path(job["output"])
        verified = root / "capacity-verification.json"
        if verified.exists():
            marker = guarded.read_spec(verified)
            guarded.require(marker.get("status") == "MEASURED" and marker.get("spec_sha256") == job["spec_sha256"], "original_job_verification_invalid")
            continue
        if job.get("extension_round"):
            points = []
            for prior in plan["jobs"]:
                if prior["purpose"] != "hot_knee" or prior["workload"] != job["workload"] or prior["concurrency"] != job["concurrency"] or prior["rate"] >= job["rate"]:
                    continue
                if points and knee(points, plan["config"]["acceptance"])["status"] == "OBSERVED FUNCTIONAL KNEE":
                    break
                cells, _, error = collect_job(prior, plan)
                point = cells.get(("isolated_primary_measure", job["concurrency"]), {})
                guarded.require(not error and point.get("status") == "MEASURED", "extension_prior_not_MEASURED")
                points.append(point)
            if knee(points, plan["config"]["acceptance"])["status"] == "OBSERVED FUNCTIONAL KNEE":
                continue
        guarded.require(set(path.name for path in root.iterdir()) == {"guard-spec.json"}, "original_job_pending_verification_no_rerun")
        return {"status": "FROZEN NOT MEASURED", "job": job["id"], "spec": job["spec"], "output": job["output"], "performance_pass": False}
    return {"status": "NO MORE SELECTED JOBS; ANALYZE REQUIRED", "performance_pass": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--template", action="store_true")
    action.add_argument("--schema", action="store_true")
    action.add_argument("--freeze", action="store_true")
    action.add_argument("--run", action="store_true")
    action.add_argument("--analyze", action="store_true")
    action.add_argument("--verify-job", action="store_true")
    action.add_argument("--next-job", action="store_true")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--job", help="one frozen job ID; still serial and never retries")
    parser.add_argument("--report", type=Path, help="new private JSON evidence report; never overwrite original")
    args = parser.parse_args()
    os.umask(0o077)
    try:
        if args.template:
            print(json.dumps(template(), indent=2))
            return 0
        if args.schema:
            print((Path(__file__).resolve().parent.parent / "stress" / "capacity-result.schema.json").read_text())
            return 0
        guarded.require(args.plan is not None, "plan_required")
        if args.freeze:
            guarded.require(args.config is not None, "config_required")
            plan = freeze(args.config, args.plan)
            print(json.dumps({"status": "FROZEN NOT MEASURED", "jobs": len(plan["jobs"]), "coverage": plan["coverage"], "plan": str(args.plan), "performance_pass": False}))
            return 0
        plan = read_plan(args.plan)
        if args.next_job:
            print(json.dumps(next_job(plan)))
            return 0
        if args.verify_job:
            guarded.require(args.job is not None, "one_original_job_required")
            result = verify_job(plan, args.job)
            print(json.dumps(result))
            return 0 if result["status"] == "MEASURED" else 1
        if args.run:
            return run(plan, args.job)
        guarded.require(args.report is not None, "new_report_path_required")
        guarded.private(args.report.parent, 0o700, True)
        result = analyze(plan)
        write_private(args.report, result)
        result_sha256 = digest(args.report)
        write_private(str(args.report) + ".sha256.json", {"sha256": result_sha256})
        print(json.dumps({"status": result["status"], "counts": result["counts"], "report": str(args.report), "result_sha256": result_sha256, "performance_pass": False}))
        return 0 if result["status"] == "READY FOR INDEPENDENT ACCEPTANCE" else 1
    except (ValueError, OSError, KeyError, TypeError) as error:
        print(json.dumps({"status": "HOLD", "error_type": type(error).__name__, "reason": str(error) if isinstance(error, ValueError) else "private_error_details_withheld", "performance_pass": False}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
