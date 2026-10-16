"""E01 peri-loom 平台运行器：纯黑盒公开 URL + 凭据，不导入平台内部模块。

建表、导入、查询与 oracle 核验全部经公开 `/v1/sql/*` 接口完成。凭据只从
`PERI_API_KEY` 或 `PERI_CREDENTIAL_FILE` 读取，不进入 argv、日志或报告。
未提供 endpoint/凭据时直接拒绝，不搜索仓库内私有连接文件。
"""

import argparse
import os
import sys
import urllib.parse
from pathlib import Path

import jsonio
import platform_client as platform
import verify
from contract import VERSION, require, validate
from model import generate
from sql import APPROVED_DDL, import_statements, schema_sha256

REJECTED_CODES = {"sql_conflict", "sql_unavailable", "sql_limit", "result_limit", "request_limit",
                  "upstream_unavailable", "upstream_rejected", "invalid_request", "invalid_database",
                  "invalid_request_id", "invalid_sql", "missing_credential", "invalid_credential",
                  "session_unusable", "readonly_session"}


def failure_event(operation, failure):
    """按公开语义分类：先看是否真的派发，再看平台证明与错误码。"""
    if failure.timings is None:
        outcome, planned, dispatched, completed = "not_dispatched", None, None, None
    else:
        planned = failure.timings["planned_ns"]
        dispatched = failure.timings["dispatched_ns"]
        completed = failure.timings["completed_ns"]
        if failure.code == "outcome_unknown":
            outcome = "timeout" if failure.timed_out else "unknown"
        elif failure.code in REJECTED_CODES:
            outcome = "rejected"
        else:
            outcome = "known_failure"
    message = failure.message
    if failure.flags:
        message = (message + " flags=" + ",".join(f"{name}={str(value).lower()}"
                                                  for name, value in sorted(failure.flags.items()))).strip()
    return {"kind": "op", "op_id": operation["op_id"], "outcome": outcome,
            "planned_ns": planned, "dispatched_ns": dispatched, "completed_ns": completed,
            "rows": None, "error_class": failure.code, "error_message": message[:200] or None}


def collect_events(session, operations, labels):
    events, poisoned = [], False
    for operation in operations:
        if poisoned:
            break
        label = labels[operation["op_id"]]
        try:
            rows, timings = session.query(operation["sql"], label)
        except platform.PlatformFailure as failure:
            events.append(failure_event(operation, failure))
            poisoned = session.unusable
            continue
        events.append({"kind": "op", "op_id": operation["op_id"], "outcome": "success",
                       "planned_ns": timings["planned_ns"], "dispatched_ns": timings["dispatched_ns"],
                       "completed_ns": timings["completed_ns"], "rows": rows,
                       "error_class": None, "error_message": None})
    return events, poisoned


def import_fixture(session, rows, batch_size):
    """建表并导入；任何失败都回滚并中止，不留下半成品数据集。"""
    import_events = {"schema_statements": len(APPROVED_DDL), "statements": 0}
    for index, statement in enumerate(APPROVED_DDL):
        session.execute(statement, f"e01-ddl-{index}")
    import_events["statements"] += len(APPROVED_DDL)
    statements = list(import_statements(rows, batch_size))
    session.execute("BEGIN", "e01-begin")
    try:
        for index, statement in enumerate(statements):
            session.execute(statement, f"e01-import-{index}")
        session.execute("COMMIT", "e01-commit")
    except platform.PlatformFailure:
        try:
            session.execute("ROLLBACK", "e01-rollback")
            import_events["rollback"] = "attempted"
        except platform.PlatformFailure:
            import_events["rollback"] = "failed_or_unknown"
        raise
    import_events["rows"] = len(rows)
    import_events["batches"] = len(statements)
    counted = session.query("SELECT count(*) FROM e01_records", "e01-count")[0]
    require(counted == [[len(rows)]], "platform_row_count_mismatch")
    return import_events


def run(args):
    config = validate(jsonio.load(args.config))
    require(config["mode"] == "smoke", "platform_runner_smoke_only")
    out_dir = Path(args.out_dir)
    require(not out_dir.exists(), "output_directory_already_exists")
    endpoint = args.endpoint or os.environ.get("PERI_ENDPOINT")
    if not endpoint:
        raise platform.PlatformFailure("missing_endpoint")
    client = platform.PlatformClient(endpoint, platform.load_credential(),
                                     ca_file=os.environ.get("PERI_CA_FILE") or None,
                                     timeout=args.timeout_seconds)
    bundle = generate(config)
    rows = bundle["rows"]
    operations = verify.build_operations(bundle, args.binding)
    labels = verify.labels(operations)
    out_dir.mkdir(mode=0o700)
    session, open_timings = platform.open_session(client, args.database, "e01-open")
    harness = {"target": "platform", "adapter": "public_http_v1", "binding": args.binding,
               "endpoint_host": urllib.parse.urlsplit(endpoint).netloc,
               "database": args.database, "readonly_session": session.readonly,
               "open_timings": open_timings, "schema_sha256": schema_sha256(),
               "durability": "platform_deployment_defined_not_settable_via_public_api"}
    events, poisoned = [], False
    close_state = None
    try:
        harness["import"] = import_fixture(session, rows, args.batch_size)
        events, poisoned = collect_events(session, operations, labels)
    finally:
        try:
            session.close("e01-close")
            close_state = "closed"
        except platform.PlatformFailure as failure:
            close_state = failure.code
            poisoned = True
        harness["close"] = close_state
        harness["session_poisoned"] = poisoned
    records, pages = verify.verify_operations(bundle, operations, events)
    raw, report = verify.assemble(config, bundle, records, harness, target="platform")
    report["traversal"] = verify.traversal_verdicts(rows, pages)
    jsonio.write_new(out_dir / "scenario.json", {
        "version": VERSION, "config_sha256": bundle["config_sha256"],
        "dataset_sha256": bundle["dataset_sha256"], "queries_sha256": bundle["queries_sha256"],
        "schema_sha256": schema_sha256(), "operations": operations,
        "labels": labels, "endpoint_host": harness["endpoint_host"], "database": args.database})
    jsonio.write_new(out_dir / "evidence.json", raw)
    jsonio.write_new(out_dir / "report.json", report)
    print(f"E01 platform smoke: correctness={report['correctness']} "
          f"verified={report['oracle_verified_ops']}/{report['ops_total']} records={len(records)}")
    print("仅 oracle 正确性证据；无性能、规模或容量结论。平台行为以真实网关为准。")
    failures = [item for item in report["traversal"] if item["verdict"] != "complete"]
    if report["correctness"] != "ORACLE_VERIFIED" or failures:
        print(f"FAIL: correctness={report['correctness']} traversal_failures={len(failures)}",
              file=sys.stderr)
        return 2
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--database", required=True,
                        help="隔离 E01 fixture 的 tenant/database/branch；非机密")
    parser.add_argument("--endpoint", default=None, help="默认读取 PERI_ENDPOINT")
    parser.add_argument("--binding", default="literal", choices=("literal", "parameter"))
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    args = parser.parse_args()
    try:
        return run(args)
    except platform.PlatformFailure as failure:
        print(f"ERROR: E01 platform 运行中止：{failure.code}"
              + (f" ({failure.message})" if failure.message else ""), file=sys.stderr)
        return 2
    except (ValueError, TypeError, KeyError, OSError) as error:
        print(f"ERROR: E01 platform 运行中止：{type(error).__name__}: {str(error)[:200]}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
