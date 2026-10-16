"""E01 正式导入：流式分批写入 8M/16M/32M 配置，只走公开 URL + 凭据。

与 ≤1M 标定的区别：本入口按冻结配置的 `distribution.rows` 全量导入，不做行数截断；
安全靠显式预算（时间、磁盘余量、单语句字节、批量大小）与计数分列：
`planned_rows` / `attempted_rows` / `insert_ack_rows` / `commit_ack_rows` / `readback_rows`。

不会删除或重置任何既有对象：`CREATE TABLE`/`CREATE INDEX` 不加 `IF NOT EXISTS`，
目标已有表或索引则直接失败退出（fail closed），不掩盖污染。
中断、失败或 Unknown 一律停止，不自动恢复、不重放、不回滚猜测；证据单独落盘。
"""

import argparse
import hashlib
import json
import os
import shutil
import sys
import time
import urllib.parse
from pathlib import Path

import jsonio
import platform_client as platform
from contract import VERSION, require, validate
from model import batched, iter_rows, mean_row_bytes
from sql import APPROVED_DDL, values_clause

# 实测上界：61,257 B 的语句被接受，71,417 B 被平台以 sql_limit 拒绝。
MAX_STATEMENT_BYTES = 61_257
DEFAULT_BATCH_SIZE = 250
DEFAULT_COMMIT_ROWS = 10_000
MAX_SECONDS_CEILING = 86_400.0


def directory_bytes(path):
    total = 0
    for root, _, names in os.walk(path):
        for name in names:
            try:
                total += os.stat(os.path.join(root, name)).st_size
            except OSError:
                continue
    return total


def run(args):
    config = validate(jsonio.load(args.config))
    planned_rows = config["distribution"]["rows"]
    require(1 <= args.batch_size <= 1000, "invalid_batch_size")
    require(args.commit_rows >= args.batch_size, "commit_rows_below_batch")
    require(0 < args.max_seconds <= MAX_SECONDS_CEILING, "time_budget_exceeded")
    require(args.min_free_disk_bytes >= 0, "invalid_disk_budget")
    out_dir = Path(args.out_dir)
    require(not out_dir.exists(), "output_directory_already_exists")
    endpoint = args.endpoint or os.environ.get("PERI_ENDPOINT")
    if not endpoint:
        raise platform.PlatformFailure("missing_endpoint")
    free_before = shutil.disk_usage(Path(args.storage_dir or out_dir.parent)).free
    require(free_before >= args.min_free_disk_bytes, "disk_headroom_below_budget")
    client = platform.PlatformClient(endpoint, platform.load_credential(),
                                     ca_file=os.environ.get("PERI_CA_FILE") or None,
                                     timeout=args.timeout_seconds)
    out_dir.mkdir(mode=0o700)
    storage_before = directory_bytes(args.storage_dir) if args.storage_dir else None
    report = {"version": VERSION, "target": "platform", "mode": "formal_import",
              "adapter": "public_http_v1", "endpoint_host": urllib.parse.urlsplit(endpoint).netloc,
              "database": args.database,
              "config_sha256": hashlib.sha256(Path(args.config).read_bytes()).hexdigest(),
              "planned_rows": planned_rows, "attempted_rows": 0, "insert_ack_rows": 0,
              "commit_ack_rows": 0, "readback_rows": None,
              "batch_size": args.batch_size, "commit_rows": args.commit_rows,
              "max_seconds": args.max_seconds, "min_free_disk_bytes": args.min_free_disk_bytes,
              "bytes_per_row_logical": round(mean_row_bytes(config), 3),
              "notes": ["正式导入入口；不做删除或重置，已有对象即失败退出",
                        "中断/失败/Unknown 均停止且不自动恢复或重放",
                        "planned 与 attempted 分列；insert ACK 不等于 durable commit"],
              "performance": "NOT MEASURED", "capacity": "NOT MEASURED"}
    session, report["open_timings"] = platform.open_session(client, args.database, "e01-imp-open")
    failures, commit_points, chain = [], [], hashlib.sha256()
    stop_reason, final_state = "planned_rows_reached", "not_started"
    ddl_seconds = import_seconds = commit_seconds = 0.0
    deadline = time.monotonic() + args.max_seconds
    next_commit = args.commit_rows
    stage = "ddl"
    try:
        try:
            started = time.monotonic()
            for index, statement in enumerate(APPROVED_DDL):
                stage = f"ddl-{index}"
                session.execute(statement, f"e01-imp-ddl-{index}")
            ddl_seconds = time.monotonic() - started
            stage = "begin"
            session.execute("BEGIN", "e01-imp-begin")
            iterator, commit_base = iter_rows(config), 0
            for batch in batched(iterator, args.batch_size):
                stage = f"batch-{report['attempted_rows'] // args.batch_size}"
                if time.monotonic() >= deadline:
                    stop_reason = "time_budget_reached"
                    break
                if report["insert_ack_rows"] >= planned_rows:
                    break
                statement = values_clause(batch)
                size = len(statement.encode())
                if size > MAX_STATEMENT_BYTES:
                    failures.append({"stage": stage, "code": "statement_byte_guard",
                                     "message": f"{size}>{MAX_STATEMENT_BYTES}"})
                    stop_reason = "failure:statement_byte_guard"
                    break
                free_now = shutil.disk_usage(Path(args.storage_dir or out_dir.parent)).free
                if free_now < args.min_free_disk_bytes:
                    failures.append({"stage": stage, "code": "disk_headroom_below_budget",
                                     "message": str(free_now)})
                    stop_reason = "failure:disk_headroom_below_budget"
                    break
                report["attempted_rows"] += len(batch)
                for row in batch:
                    chain.update(f"{row['id']}:{row['group_id']}:{row['created_at']}:{row['payload']}\n".encode())
                started = time.monotonic()
                try:
                    session.execute(statement, f"e01-imp-{report['attempted_rows']:09d}")
                except platform.PlatformFailure as failure:
                    import_seconds += time.monotonic() - started
                    failures.append({"stage": stage, "code": failure.code,
                                     "message": failure.message, "flags": failure.flags,
                                     "attempted_rows": report["attempted_rows"]})
                    stop_reason = f"failure:{failure.code}"
                    break
                import_seconds += time.monotonic() - started
                report["insert_ack_rows"] += len(batch)
                if (report["insert_ack_rows"] - commit_base) >= args.commit_rows:
                    stage = f"commit-{len(commit_points)}"
                    started = time.monotonic()
                    try:
                        session.execute("COMMIT", f"e01-imp-commit-{len(commit_points)}")
                        session.execute("BEGIN", f"e01-imp-resume-{len(commit_points)}")
                    except platform.PlatformFailure as failure:
                        failures.append({"stage": stage, "code": failure.code,
                                         "message": failure.message, "flags": failure.flags,
                                         "insert_ack_rows": report["insert_ack_rows"]})
                        stop_reason = f"failure:{failure.code}"
                        break
                    elapsed = time.monotonic() - started
                    commit_seconds += elapsed
                    commit_points.append({"rows": report["insert_ack_rows"], "seconds": round(elapsed, 4)})
                    commit_base = report["insert_ack_rows"]
            if stop_reason == "planned_rows_reached" and report["insert_ack_rows"] >= planned_rows:
                stage = "final-commit"
                started = time.monotonic()
                session.execute("COMMIT", "e01-imp-final-commit")
                elapsed = time.monotonic() - started
                commit_seconds += elapsed
                commit_points.append({"rows": report["insert_ack_rows"], "seconds": round(elapsed, 4)})
                final_state = "committed"
            elif failures:
                final_state = "stopped_without_rollback_or_resume"
            else:
                stage = "final-commit"
                started = time.monotonic()
                session.execute("COMMIT", "e01-imp-partial-commit")
                elapsed = time.monotonic() - started
                commit_seconds += elapsed
                final_state = "committed_partial"
                commit_points.append({"rows": report["insert_ack_rows"], "seconds": round(elapsed, 4)})
        except platform.PlatformFailure as failure:
            failures.append({"stage": stage, "code": failure.code, "message": failure.message,
                             "flags": failure.flags, "insert_ack_rows": report["insert_ack_rows"]})
            stop_reason = f"failure:{failure.code}"
            final_state = "stopped_without_rollback_or_resume"
    finally:
        try:
            session.close("e01-imp-close")
            report["session_close"] = "closed"
        except platform.PlatformFailure as failure:
            report["session_close"] = failure.code
        report["session_poisoned"] = session.unusable
    report.update({
        "commit_ack_rows": max([point["rows"] for point in commit_points], default=0),
        "commit_points": commit_points, "stop_reason": stop_reason,
        "transaction_final_state": final_state, "failures": failures,
        "ddl_seconds": round(ddl_seconds, 4), "import_seconds": round(import_seconds, 4),
        "commit_seconds": round(commit_seconds, 4),
        "stream_row_chain_sha256": chain.hexdigest(),
        "disk_free_before": free_before, "disk_free_after": shutil.disk_usage(
            Path(args.storage_dir or out_dir.parent)).free,
        "storage_bytes_before": storage_before,
        "storage_bytes_after": directory_bytes(args.storage_dir) if args.storage_dir else None})
    if failures:
        jsonio.write_lines_new(out_dir / "batches.ndjson",
                               (json.dumps(point, sort_keys=True) for point in commit_points))
        jsonio.write_new(out_dir / "import.json", report)
        print(f"导入中止：{failures[-1]['stage']} -> {failures[-1]['code']}；未重试、未自动恢复。",
              file=sys.stderr)
        return 2
    try:
        probe, _ = platform.open_session(client, args.database, "e01-imp-probe")
        try:
            counted, _ = probe.query("SELECT count(*) FROM e01_records", "e01-imp-count")
            report["readback_rows"] = counted[0][0] if counted and counted[0] else None
        finally:
            probe.close("e01-imp-probe-close")
    except platform.PlatformFailure as failure:
        report["readback_error"] = failure.code
    report["timing"] = {"unit": "ms", "phases": {"ddl": round(ddl_seconds * 1000, 3),
                                                 "import_inserts": round(import_seconds * 1000, 3),
                                                 "commit": round(commit_seconds * 1000, 3)},
                        "containment": "三段互斥、分别计量；未计会话 open/close、生成与校验"}
    durable = (report["readback_rows"] == planned_rows and report["commit_ack_rows"] == planned_rows
               and report["insert_ack_rows"] == planned_rows)
    report["durable_rows_per_second"] = (
        round(report["readback_rows"] / ((report["import_seconds"] + report["commit_seconds"]) or 1), 3)
        if durable else None)
    report["durable_throughput_basis"] = ("readback==planned 且 commit_ack==planned 且 insert_ack==planned；"
                                          "分母为 INSERT+COMMIT 阶段秒" if durable
                                          else "证据不足（缺少匹配的回读/提交/ACK 计数），不给持久吞吐")
    jsonio.write_new(out_dir / "import.json", report)
    print(f"正式导入结束：planned={planned_rows} attempted={report['attempted_rows']} "
          f"insert_ack={report['insert_ack_rows']} commit_ack={report['commit_ack_rows']} "
          f"readback={report['readback_rows']} stop={stop_reason}")
    print("导入计数证据；性能与容量未测量。")
    return 0 if report["insert_ack_rows"] == planned_rows else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--endpoint", default=None)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--commit-rows", type=int, default=DEFAULT_COMMIT_ROWS)
    parser.add_argument("--max-seconds", type=float, default=3600.0)
    parser.add_argument("--min-free-disk-bytes", type=int, default=1 << 30)
    parser.add_argument("--storage-dir", default=None, help="自有运行目录，用于磁盘增长计量")
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    args = parser.parse_args()
    try:
        return run(args)
    except platform.PlatformFailure as failure:
        print(f"ERROR: 导入中止：{failure.code}"
              + (f" ({failure.message})" if failure.message else ""), file=sys.stderr)
        return 2
    except (ValueError, TypeError, KeyError, OSError) as error:
        print(f"ERROR: 导入中止：{type(error).__name__}: {str(error)[:200]}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
