"""E01 平台导入标定：流式生成 + 分批导入，只走公开 URL + 凭据。

用途：为正式阶梯确定导入速率、每行字节成本与磁盘增长，不产生性能或容量结论。
有界：行数上限 100 万、时间上限 600 秒（默认 480），触限即停止并保留证据。
SQL 只派发一次，不重试；失败或 Unknown 立即停止，尝试显式回滚并如实记录。
"""

import argparse
import hashlib
import itertools
import json
import os
import re
import shutil
import sys
import time
import urllib.parse
from pathlib import Path

import jsonio
import platform_client as platform
from contract import VERSION, require, validate
from model import iter_rows, mean_row_bytes, row_at
from sql import APPROVED_DDL, values_clause

MAX_ROWS = 1_000_000
# 实测上界：61,257 B 的语句被接受，71,417 B 被平台以 sql_limit 拒绝。
MAX_STATEMENT_BYTES = 61_257
MAX_SECONDS = 600
SAMPLE_SQL = "SELECT id, group_id, created_at, payload FROM e01_records WHERE id = "
COUNT_SQL = "SELECT count(*) FROM e01_records"


def directory_bytes(path):
    """主机级磁盘计量（操作者自有运行目录），不是平台内部遥测。"""
    total = 0
    for root, _, names in os.walk(path):
        for name in names:
            try:
                total += os.stat(os.path.join(root, name)).st_size
            except OSError:
                continue
    return total


def sample_identities(config, count):
    """确定性抽样点查 id；用于在不驻留全量数据的情况下核验真实写入。"""
    total = config["distribution"]["rows"]
    step = max(1, total // max(1, count))
    identities = []
    for index in range(count):
        identity = 1 + (index * step + int(hashlib.sha256(
            f"{VERSION}:{config['seed']}:sample:{index}".encode()).hexdigest()[:8], 16)) % total
        identities.append(identity)
    return sorted(set(identities))


def run(args):
    config = validate(jsonio.load(args.config))
    rows_target = config["distribution"]["rows"]
    require(rows_target <= MAX_ROWS, "calibration_row_budget_exceeded")
    require(0 < args.max_seconds <= MAX_SECONDS, "calibration_time_budget_exceeded")
    require(1 <= args.batch_size <= 1000, "invalid_batch_size")
    require(args.commit_rows >= args.batch_size, "commit_rows_below_batch")
    out_dir = Path(args.out_dir)
    require(not out_dir.exists(), "output_directory_already_exists")
    endpoint = args.endpoint or os.environ.get("PERI_ENDPOINT")
    if not endpoint:
        raise platform.PlatformFailure("missing_endpoint")
    client = platform.PlatformClient(endpoint, platform.load_credential(),
                                     ca_file=os.environ.get("PERI_CA_FILE") or None,
                                     timeout=args.timeout_seconds)
    out_dir.mkdir(mode=0o700)
    free_before = shutil.disk_usage(out_dir).free
    storage_before = directory_bytes(args.storage_dir) if args.storage_dir else None
    report = {"version": VERSION, "target": "platform", "mode": "calibration",
              "adapter": "public_http_v1", "endpoint_host": urllib.parse.urlsplit(endpoint).netloc,
              "database": args.database,
              "config_sha256": hashlib.sha256(Path(args.config).read_bytes()).hexdigest(),
              "rows_requested": rows_target, "batch_size": args.batch_size,
              "commit_rows": args.commit_rows, "max_seconds": args.max_seconds,
              "bytes_per_row_logical": round(mean_row_bytes(config), 3),
              "disk_free_before": free_before, "storage_dir": args.storage_dir,
              "storage_bytes_before": storage_before, "notes": [
                  "仅导入标定：不产生性能、容量或足压结论",
                  "工作集/扫描量公开不可测，未测项保持 NOT MEASURED"],
              "performance": "NOT MEASURED", "capacity": "NOT MEASURED",
              "scale_coverage": "CALIBRATION_ONLY"}
    session, open_timings = platform.open_session(client, args.database, "e01-cal-open")
    report["open_timings"] = open_timings
    batches, commit_points, failures = [], [], []
    rows_imported, attempted_rows, batches_sent = 0, 0, 0
    generation_seconds = import_seconds = commit_seconds = 0.0
    chain = hashlib.sha256()
    stop_reason = "row_target_reached"
    final_state = "not_started"
    stage = "fixture"
    deadline = time.monotonic() + args.max_seconds
    next_commit = args.commit_rows
    try:
      try:
        for index, statement in enumerate(APPROVED_DDL):
            stage = f"ddl-{index}"
            session.execute(statement, f"e01-cal-ddl-{index}")
        stage = "begin"
        session.execute("BEGIN", "e01-cal-begin")
        iterator = iter_rows(config)
        while rows_imported < rows_target:
            if time.monotonic() >= deadline:
                stop_reason = "time_budget_reached"
                break
            started = time.monotonic()
            batch = list(itertools.islice(iterator, args.batch_size))
            generation_seconds += time.monotonic() - started
            if not batch:
                break
            for row in batch:
                chain.update(f"{row['id']}:{row['group_id']}:{row['created_at']}:{row['payload']}\n".encode())
            stage = f"batch-{batches_sent}"
            statement = values_clause(batch)
            if len(statement.encode()) > MAX_STATEMENT_BYTES:
                # 安全守卫：不得送出超过实测上界的语句（不截断、不缩小后假称原规模）。
                failures.append({"stage": stage, "code": "statement_byte_guard",
                                 "message": f"{len(statement.encode())}>{MAX_STATEMENT_BYTES}",
                                 "flags": {}, "rows_before": rows_imported})
                stop_reason = "failure:statement_byte_guard"
                break
            attempted_rows += len(batch)
            try:
                session.execute(statement, f"e01-cal-{batches_sent:06d}")
            except platform.PlatformFailure as failure:
                import_seconds += time.monotonic() - started
                failures.append({"stage": stage, "batch": batches_sent, "code": failure.code,
                                 "message": failure.message, "flags": failure.flags,
                                 "rows_before": rows_imported})
                stop_reason = f"failure:{failure.code}"
                break
            import_seconds += time.monotonic() - started
            rows_imported += len(batch)
            batches.append({"batch": batches_sent, "rows": len(batch),
                            "rows_total": rows_imported,
                            "request_ms": round(import_seconds * 1000, 3)})
            batches_sent += 1
            if rows_imported >= next_commit and rows_imported < rows_target:
                stage = f"commit-{len(commit_points)}"
                started = time.monotonic()
                try:
                    session.execute("COMMIT", f"e01-cal-commit-{len(commit_points)}")
                    session.execute("BEGIN", f"e01-cal-resume-{len(commit_points)}")
                except platform.PlatformFailure as failure:
                    failures.append({"stage": stage, "commit": len(commit_points),
                                     "code": failure.code, "message": failure.message,
                                     "flags": failure.flags, "rows_before": rows_imported})
                    stop_reason = f"failure:{failure.code}"
                    break
                elapsed = time.monotonic() - started
                commit_seconds += elapsed
                commit_points.append({"rows": rows_imported, "seconds": round(elapsed, 4)})
                next_commit = rows_imported + args.commit_rows
        final_state = "committed"
        if stop_reason.startswith("failure:"):
            try:
                session.execute("ROLLBACK", "e01-cal-rollback")
                final_state = "rolled_back"
            except platform.PlatformFailure as failure:
                final_state = f"rollback_failed:{failure.code}"
        else:
            stage = "final-commit"
            session.execute("COMMIT", "e01-cal-final-commit")
      except platform.PlatformFailure as failure:
        # 失败与 Unknown 都必须落盘证据；不重试、不覆盖既有目录。
        failures.append({"stage": stage, "code": failure.code, "message": failure.message,
                         "flags": failure.flags, "rows_before": rows_imported})
        stop_reason = f"failure:{failure.code}"
    finally:
        try:
            session.close("e01-cal-close")
            report["session_close"] = "closed"
        except platform.PlatformFailure as failure:
            report["session_close"] = failure.code
        report["session_poisoned"] = session.unusable
    report.update({
        "planned_rows": rows_target, "attempted_rows": attempted_rows,
        "insert_ack_rows": rows_imported,
        "commit_ack_rows": sum(point["rows"] for point in commit_points),
        "batches_sent": batches_sent,
        "stop_reason": stop_reason, "transaction_final_state": final_state,
        "generation_seconds": round(generation_seconds, 4),
        "import_seconds": round(import_seconds, 4), "commit_seconds": round(commit_seconds, 4),
        "rows_per_second_import": round(rows_imported / import_seconds, 1) if import_seconds else None,
        "rows_per_second_generation": round(rows_imported / generation_seconds, 1) if generation_seconds else None,
        "bytes_logical_imported": round(rows_imported * mean_row_bytes(config), 0),
        "stream_row_chain_sha256": chain.hexdigest(),
        "commit_points": commit_points, "failures": failures,
        "disk_free_after": shutil.disk_usage(out_dir).free})
    report["disk_free_delta_bytes"] = report["disk_free_before"] - report["disk_free_after"]
    if storage_before is not None:
        report["storage_bytes_after"] = directory_bytes(args.storage_dir)
        report["storage_growth_bytes"] = report["storage_bytes_after"] - storage_before
    if failures:
        jsonio.write_lines_new(out_dir / "batches.ndjson",
                               (json.dumps(entry, sort_keys=True) for entry in batches))
        jsonio.write_new(out_dir / "calibration.json", report)
        print(f"标定中止：{failures[-1].get('stage', 'unknown')} -> {failures[-1]['code']}；已保留证据，未重试。",
              file=sys.stderr)
        return 2
    try:
        probe, _ = platform.open_session(client, args.database, "e01-cal-probe")
    except platform.PlatformFailure as failure:
        report["probe_open"] = failure.code
        jsonio.write_new(out_dir / "calibration.json", report)
        return 2
    try:
        counted = probe.query(COUNT_SQL, "e01-cal-count")[0]
        checked = 0
        mismatched = []
        for identity in sample_identities(config, args.samples):
            expected = row_at(config, identity)
            rows = probe.query(SAMPLE_SQL + str(identity), f"e01-cal-check-{identity}")[0]
            actual = rows[0] if rows else None
            checked += 1
            if actual != [expected["id"], expected["group_id"], expected["created_at"], expected["payload"]]:
                mismatched.append(identity)
        report["sample_checks"] = {"checked": checked, "mismatched": mismatched}
        report["readback_rows"] = counted[0][0] if counted and counted[0] else None
        report["count_query"] = {"reported": counted, "expected_insert_ack": rows_imported,
                                 "matched": counted == [[rows_imported]],
                                 "note": "回读核对的是当时可见行数；不等于提交证明"}
    finally:
        try:
            probe.close("e01-cal-probe-close")
        except platform.PlatformFailure as failure:
            report["probe_close"] = failure.code
    jsonio.write_lines_new(out_dir / "batches.ndjson",
                           (json.dumps(entry, sort_keys=True) for entry in batches))
    jsonio.write_new(out_dir / "calibration.json", report)
    print(f"标定完成：rows={rows_imported} 批={batches_sent} 导入={report['import_seconds']}s "
          f"({report['rows_per_second_import']}/s) 生成={report['generation_seconds']}s "
          f"停止={stop_reason} 磁盘增长={report.get('storage_growth_bytes')}B")
    print("仅导入标定证据；性能/容量/工作集均未测量。")
    if report["failures"] or report.get("sample_checks", {}).get("mismatched"):
        print("FAIL: 标定存在失败或抽样不一致", file=sys.stderr)
        return 2
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--endpoint", default=None)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--commit-rows", type=int, default=50000)
    parser.add_argument("--max-seconds", type=float, default=480.0)
    parser.add_argument("--samples", type=int, default=8)
    parser.add_argument("--storage-dir", default=None,
                        help="自有运行目录，用于主机级磁盘增长计量（可选）")
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    args = parser.parse_args()
    try:
        return run(args)
    except platform.PlatformFailure as failure:
        print(f"ERROR: 标定中止：{failure.code}"
              + (f" ({failure.message})" if failure.message else ""), file=sys.stderr)
        return 2
    except (ValueError, TypeError, KeyError, OSError) as error:
        print(f"ERROR: 标定中止：{type(error).__name__}: {str(error)[:200]}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
