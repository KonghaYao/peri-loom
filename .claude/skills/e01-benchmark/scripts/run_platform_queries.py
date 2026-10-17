"""E01 查询测量：清单驱动、只读、有界，只走公开 URL + 凭据。

并发语义：`--concurrency N`（默认 100，来自 PostgreSQL 典型 max_connections=100 的连接
规模参考，**不等价于 active queries，也不是 pgbench 默认值**）表示打开 N 个**真实独立会话**，
每个会话由唯一 worker 线程串行独占并在结束后显式关闭；不使用多线程共享单会话。
请求数少于并发时只开与请求数相同的会话。会话未全部准入时如实报告请求/打开/峰值在途，
不把它当成"已测 100"。

期望值来自离线 oracle manifest；不做建表、导入、删除或清理；失败与 Unknown 立即停止派发，
不重试、不重放。SLO 与测量分离：未获批 SLO 时只报探索测量。
"""

import argparse
import hashlib
import json
import os
import queue
import sys
import threading
import time
import urllib.parse
from pathlib import Path

import jsonio
import platform_client as platform
import stats
import timing
import verify
from contract import FAMILIES, VERSION, require, validate
from oracle_manifest import verify_manifest
from sql import render

MAX_CONCURRENCY = 256
DEFAULT_CONCURRENCY = 100
MAX_SECONDS = 600.0
DEFAULT_REQUEST_BUDGET = 4096
# 大计划清单是技能自产工件，允许更大的读取上限（仍拒绝超限输入）。
MANIFEST_INPUT_LIMIT = 512 * 1024 * 1024
COUNT_SQL = "SELECT count(*) FROM e01_records"


def as_values(rows):
    return [[row["id"], row["group_id"], row["created_at"], row["payload"]] for row in rows]


def build_operations(manifest, binding):
    operations = []
    for entry in manifest["operations"]:
        sql, params = render(entry, binding)
        operations.append({**entry, "sql": sql, "params": params})
    return operations


def open_sessions(client, database, count, timer, report):
    sessions, failures = [], []
    with timer.phase("open"):
        for index in range(count):
            try:
                session, _ = platform.open_session(client, database, f"e01-q-open-{index:03d}")
                sessions.append(session)
            except platform.PlatformFailure as failure:
                failures.append({"index": index, "code": failure.code})
    report["sessions_requested"] = count
    report["sessions_opened"] = len(sessions)
    report["open_failures"] = failures
    report["partial_admission"] = len(sessions) != count
    return sessions


def run_workers(args, sessions, operations, labels, report):
    work = queue.Queue()
    for index, operation in enumerate(operations):
        work.put((index, operation))
    events, stop, lock = [], threading.Event(), threading.Lock()
    in_flight, peak = 0, 0
    deadline = time.monotonic() + args.max_seconds
    # The rate is an aggregate arrival rate across sessions, so every operation's slot is
    # measured from one fixed epoch. Measuring each delay from the current instant would make
    # the k-th operation wait k/rate *again* after the previous one finished, so the effective
    # rate would decay with the backlog and a "rate 20" arm would dispatch about 0.8 ops/s.
    rate_epoch_ns = time.monotonic_ns()

    def worker(session):
        nonlocal in_flight, peak
        while not stop.is_set():
            try:
                index, operation = work.get_nowait()
            except queue.Empty:
                return
            planned = rate_epoch_ns + (
                0 if args.rate == 0 else int(index / args.rate * 1e9)
            )
            delay = (planned - time.monotonic_ns()) / 1e9
            if delay > 0:
                time.sleep(delay)
            if time.monotonic() > deadline:
                with lock:
                    events.append({"op_id": operation["op_id"], "outcome": "not_dispatched",
                                   "planned_ns": planned, "dispatched_ns": None, "completed_ns": None,
                                   "error_class": "time_budget_reached", "error_message": None})
                    stop.set()
                return
            with lock:
                in_flight += 1
                peak = max(peak, in_flight)
            try:
                rows, timings = session.query(operation["sql"], labels[operation["op_id"]])
            except platform.PlatformFailure as failure:
                with lock:
                    events.append({"op_id": operation["op_id"],
                                   "outcome": "timeout" if failure.timed_out else
                                              ("unknown" if failure.code == "outcome_unknown"
                                               else "known_failure"),
                                   "planned_ns": planned,
                                   "dispatched_ns": (failure.timings or {}).get("dispatched_ns"),
                                   "completed_ns": (failure.timings or {}).get("completed_ns"),
                                   "error_class": failure.code,
                                   "error_message": failure.message or None})
                    # Unknown/超时按契约停止全部派发：会话已不可信，不重试、不重放。
                    if session.unusable or failure.timed_out:
                        stop.set()
                continue
            finally:
                with lock:
                    in_flight -= 1
            with lock:
                events.append({"op_id": operation["op_id"], "outcome": "success",
                               "planned_ns": planned, "dispatched_ns": timings["dispatched_ns"],
                               "completed_ns": timings["completed_ns"], "rows": rows,
                               "error_class": None, "error_message": None})

    threads = [threading.Thread(target=worker, args=(session,)) for session in sessions]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    report["peak_in_flight"] = peak
    return events


def close_sessions(sessions, timer, report, client=None):
    closed, failures = 0, []
    with timer.phase("close"):
        for index, session in enumerate(sessions):
            try:
                session.close(f"e01-q-close-{index:03d}")
                closed += 1
            except platform.PlatformFailure as failure:
                failures.append({"index": index, "code": failure.code})
        if client is not None:
            client.close_connections()
    report["sessions_closed"] = closed
    report["close_failures"] = failures


def execute(args):
    timer = timing.PhaseTimer()
    with timer.phase("prepare"):
        config = validate(jsonio.load(args.config))
        manifest = jsonio.load(args.manifest, limit=MANIFEST_INPUT_LIMIT)
        verify_manifest(config, manifest)
        require(args.concurrency <= MAX_CONCURRENCY, "concurrency_budget_exceeded")
        require(0 < args.max_seconds <= MAX_SECONDS, "time_budget_exceeded")
        require(args.rate >= 0, "invalid_rate")
        require(args.request_budget >= 1, "invalid_request_budget")
        out_dir = Path(args.out_dir)
        require(not out_dir.exists(), "output_directory_already_exists")
        endpoint = args.endpoint or os.environ.get("PERI_ENDPOINT")
        if not endpoint:
            raise platform.PlatformFailure("missing_endpoint")
        client = platform.PlatformClient(endpoint, platform.load_credential(),
                                         ca_file=os.environ.get("PERI_CA_FILE") or None,
                                         timeout=args.timeout_seconds,
                                         connection_mode=args.connection_mode)
        operations = build_operations(manifest, args.binding)
        require(len(operations) <= args.request_budget, "request_budget_exceeded")
        labels = verify.labels(operations)
        out_dir.mkdir(mode=0o700)
    report = {
        "version": VERSION, "target": "platform", "mode": "measurement",
        "adapter": "public_http_v1", "endpoint_host": urllib.parse.urlsplit(endpoint).netloc,
        "database": args.database,
        "config_sha256": hashlib.sha256(Path(args.config).read_bytes()).hexdigest(),
        "manifest_sha256": manifest["manifest_sha256"], "manifest_scan": manifest["scan"],
        "ops": len(operations), "request_budget": args.request_budget,
        "concurrency": args.concurrency, "concurrency_source": "postgresql_typical_max_connections_reference",
        "connection_mode": args.connection_mode,
        "rate_ops_per_second": args.rate, "max_seconds": args.max_seconds, "binding": args.binding,
        "timing_boundary": {"per_operation": "planned_ns->completed_ns",
                            "service": "dispatched_ns->completed_ns",
                            "throughput_denominator": "query phase wall (ms)"},
        "slo": stats.slo_state(args.slo_approved),
        "notes": ["只读查询测量；判定只依据离线清单",
                  "并发是安全上限；速率为尽力整形，非高精度开环，派发滞后逐条可核对",
                  "不使用多线程共享单会话；每个会话由唯一 worker 串行独占并显式关闭"]}
    sessions = open_sessions(client, args.database, min(args.concurrency, len(operations)),
                             timer, report)
    if not sessions:
        report["timing"] = timer.report()
        report["correctness"] = "NOT MEASURED"
        jsonio.write_new(out_dir / "queries.json", report)
        print("E01 查询测量：未打开任何会话（配置/准入失败），NOT MEASURED。", file=sys.stderr)
        return 2
    counted = None
    with timer.phase("count_readback"):
        try:
            # 独立探针会话：池内会话始终由唯一 worker 独占。
            probe, _ = platform.open_session(client, args.database, "e01-q-probe")
            try:
                counted, _ = probe.query(COUNT_SQL, "e01-q-count")
            finally:
                probe.close("e01-q-probe-close")
        except platform.PlatformFailure as failure:
            report["count_error"] = failure.code
    report["dataset_rows_reported"] = counted[0][0] if counted and counted[0] else None
    report["dataset_rows_expected"] = config["distribution"]["rows"]
    report["scale_coverage"] = ("VERIFIED_BY_COUNT"
                                if counted == [[config["distribution"]["rows"]]] else "NOT_VERIFIED")
    with timer.phase("query"):
        events = run_workers(args, sessions, operations, labels, report)
    query_ms = timer.phases["query"]
    by_id = {operation["op_id"]: operation for operation in operations}
    checked = mismatched = 0
    with timer.phase("oracle_compare"):
        for event in events:
            if event["outcome"] != "success":
                continue
            checked += 1
            expected = by_id[event["op_id"]]["expected"]
            if event["rows"] != as_values(expected):
                mismatched += 1
    close_sessions(sessions, timer, report, client)
    counters = {}
    for event in events:
        counters[event["outcome"]] = counters.get(event["outcome"], 0) + 1
    success = counters.get("success", 0)
    missing = len(operations) - len(events)
    report["outcomes"] = counters
    report["oracle"] = {"checked": checked, "mismatched": mismatched, "missing_ops": missing,
                        "source": "offline_manifest"}
    report["families"] = stats.summarize(events, operations, FAMILIES)
    for family, entry in report["families"].items():
        entry["success_ops_per_second"] = timing.rate(entry["outcomes"].get("success", 0), query_ms)
    overall = timing.rate(success, query_ms)
    report["throughput"] = {
        "unit": "successful_operations_per_second",
        "denominator_ms": round(query_ms, 3),
        "denominator_note": "查询阶段墙钟（互斥阶段计时，与延迟统计同一边界）",
        "overall": overall,
        "per_session_efficiency": (round(overall / report["sessions_opened"], 3)
                                   if overall and report["sessions_opened"] else None),
        "operation_counts": {"success": success, "attempted": len(events),
                             "planned": len(operations)}}
    observed = client.connection_stats()
    # per-request 模式走 urllib，无法可靠计数连接：如实报 null 而不是用请求数推断。
    observed_connections = (observed["connections_created"]
                            if observed["connection_mode"] == "reuse" else None)
    observed_requests = observed["requests_sent"]
    report["resources"] = (json.loads(Path(args.resource_json).read_text())
                           if args.resource_json else
                           {"status": "NOT MEASURED",
                            "reason": "本工具不采集主机资源；未提供 --resource-json"})
    report["cpu_seconds_per_operation"] = (
        round(report["resources"]["cpu_seconds"] / len(events), 6)
        if isinstance(report["resources"], dict) and report["resources"].get("cpu_seconds")
        and events else None)
    failed_outcomes = sum(value for key, value in counters.items() if key != "success")
    report["correctness"] = ("NOT MEASURED" if not events else
                             "FAIL_STOP" if mismatched else
                             "INCOMPLETE" if (missing or checked == 0 or failed_outcomes) else
                             "ORACLE_VERIFIED")
    report["performance"] = "NOT MEASURED"
    report["capacity"] = "NOT MEASURED"
    report["transport"] = {"declared_mode": args.connection_mode,
                           "observed": {"connections_created": observed_connections,
                                        "requests_sent": observed_requests,
                                        "note": "per-request 模式不计数连接（urllib 路径），reuse 模式为真实计数"}}
    report["timing"] = timer.report()
    jsonio.write_lines_new(out_dir / "query-events.ndjson",
                           (json.dumps(redact(event), sort_keys=True) for event in events))
    jsonio.write_new(out_dir / "queries.json", report)
    print(f"E01 查询测量：correctness={report['correctness']} ops={len(operations)} "
          f"sessions={report['sessions_opened']}/{report['sessions_requested']} "
          f"peak_in_flight={report['peak_in_flight']} success={success} mismatched={mismatched} "
          f"missing={missing} scale={report['scale_coverage']}")
    print("探索测量；未获批 SLO，不构成性能通过。")
    return 0 if report["correctness"] == "ORACLE_VERIFIED" else 2


def redact(event):
    result = {key: value for key, value in event.items() if key != "rows"}
    if event.get("rows") is not None:
        result["row_count"] = len(event["rows"])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--endpoint", default=None)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                        help="真实独立会话数（默认 100，PG 典型连接规模参考）")
    parser.add_argument("--rate", type=float, default=0.0, help="每秒派发操作数；0 表示不整形")
    parser.add_argument("--max-seconds", type=float, default=120.0)
    parser.add_argument("--request-budget", type=int, default=DEFAULT_REQUEST_BUDGET)
    parser.add_argument("--resource-json", default=None, help="操作者采集的主机资源样本（可选）")
    parser.add_argument("--binding", default="literal", choices=("literal", "parameter"))
    parser.add_argument("--connection-mode", default="per-request", choices=("per-request", "reuse"))
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    parser.add_argument("--slo-approved", action="store_true")
    args = parser.parse_args()
    try:
        return execute(args)
    except platform.PlatformFailure as failure:
        print(f"ERROR: 查询测量中止：{failure.code}"
              + (f" ({failure.message})" if failure.message else ""), file=sys.stderr)
        return 2
    except (ValueError, TypeError, KeyError, OSError) as error:
        print(f"ERROR: 查询测量中止：{type(error).__name__}: {str(error)[:200]}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
