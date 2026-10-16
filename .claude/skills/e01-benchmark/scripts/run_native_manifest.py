"""原生 TursoDB 对照运行：消费与平台相同的 config 与 oracle manifest。

同一 seed/分布/SQL 文本/查询序列（manifest 由两者共用）；真实本地持久库，
不使用内存库、不关闭同步。只做本技能 fixture，不触碰业务数据。
"""

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import jsonio
import stats
import timing
import verify
from contract import FAMILIES, VERSION, require, validate
from model import batched, iter_rows
from oracle_manifest import verify_manifest
from sql import APPROVED_DDL, render, values_clause

BINARY = Path(__file__).resolve().parents[1] / "native-harness/target/debug/e01-native-harness"


def events(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def execute(args):
    timer = timing.PhaseTimer()
    with timer.phase("prepare"):
        config = validate(jsonio.load(args.config))
        manifest = jsonio.load(args.manifest, limit=512 * 1024 * 1024)
        verify_manifest(config, manifest)
        require(BINARY.is_file(), "native_harness_binary_missing")
        out_dir = Path(args.out_dir)
        require(not out_dir.exists(), "output_directory_already_exists")
        out_dir.mkdir(mode=0o700)
        database = Path(args.existing_db) if args.existing_db else out_dir / "fixture.db"
        operations = []
        for entry in manifest["operations"]:
            sql, params = render(entry, args.binding)
            operations.append({**entry, "sql": sql, "params": params})
        plan = {"version": VERSION, "database_path": str(database),
                "import_path": str(out_dir / "import.jsonl"), "import_transaction": "single",
                "sync_mode": config["sync_mode"], "schema": list(APPROVED_DDL),
                "dataset_rows": config["distribution"]["rows"],
                "ops": [{"op_id": op["op_id"], "sql": op["sql"], "params": op["params"]}
                        for op in operations]}
        jsonio.write_new(out_dir / "plan.json", plan)
    with timer.phase("generate_and_write"):
        if args.existing_db:
            pass
        else:
        # harness 契约：每行是一个 JSON 字符串（SQL 文本）。
            jsonio.write_lines_new(out_dir / "import.jsonl",
                                   (json.dumps(values_clause(batch)) for batch in
                                    batched(iter_rows(config), args.batch_size)))
    with timer.phase("import"):
        if args.existing_db:
            import_events = []
        else:
            subprocess.run([str(BINARY), "import", "--plan", str(out_dir / "plan.json"),
                            "--out", str(out_dir / "import.ndjson")], check=True)
            import_events = events(out_dir / "import.ndjson")
    with timer.phase("query"):
        subprocess.run([str(BINARY), "query", "--plan", str(out_dir / "plan.json"),
                        "--out", str(out_dir / "query.ndjson"),
                        "--connections", str(args.connections)], check=True)
    query_events = events(out_dir / "query.ndjson")
    started = min(event["planned_ns"] for event in query_events)
    finished = max(event["completed_ns"] for event in query_events)
    query_ms = (finished - started) / 1e6
    by_id = {op["op_id"]: op for op in operations}
    mismatched = checked = 0
    with timer.phase("oracle_compare"):
        for event in query_events:
            if event["outcome"] != "success":
                continue
            checked += 1
            expected = [[row["id"], row["group_id"], row["created_at"], row["payload"]]
                        for row in by_id[event["op_id"]]["expected"]]
            if event["rows"] != expected:
                mismatched += 1
    counters = {}
    for event in query_events:
        counters[event["outcome"]] = counters.get(event["outcome"], 0) + 1
    families = stats.summarize(query_events, operations, FAMILIES)
    success = counters.get("success", 0)
    overall = timing.rate(success, query_ms)
    for family, entry in families.items():
        entry["success_ops_per_second"] = timing.rate(entry["outcomes"].get("success", 0), query_ms)
    schema_event = next((e for e in import_events if e["kind"] == "schema"), {})
    import_event = next((e for e in import_events if e["kind"] == "import"), {})
    report = {"version": VERSION, "model_version": VERSION, "target": "native",
              "adapter": "turso_core-embedded", "binding": args.binding,
              "config_sha256": hashlib.sha256(Path(args.config).read_bytes()).hexdigest(),
              "manifest_sha256": manifest["manifest_sha256"],
              "timing_boundary": {"per_operation": "planned_ns->completed_ns",
                                  "service": "dispatched_ns->completed_ns",
                                  "throughput_denominator": "query phase wall (ms)"},
              "concurrency": args.connections, "sessions_requested": args.connections,
              "sessions_opened": args.connections, "peak_in_flight": args.connections,
              "correctness": ("NOT MEASURED" if not query_events else
                              "FAIL_STOP" if mismatched else
                              "INCOMPLETE" if (len(query_events) != len(operations) or checked == 0
                                               or success != len(operations)) else "ORACLE_VERIFIED"),
              "outcomes": counters,
              "oracle": {"checked": checked, "mismatched": mismatched, "source": "offline_manifest"},
              "families": families,
              "throughput": {"unit": "successful_operations_per_second",
                             "denominator_ms": round(query_ms, 3),
                             "denominator_note": "查询阶段：首条 planned 到末条 completed（原生在进程内）",
                             "overall": overall,
                             "per_session_efficiency": round(overall / args.connections, 3) if overall else None,
                             "operation_counts": {"success": success, "planned": len(operations)}},
              "dataset": {"rows_planned": config["distribution"]["rows"],
                          "rows_readback": import_event.get("rows"),
                          "sync_mode": config["sync_mode"],
                          "import_transaction": "single",
                          "ddl_statements": schema_event.get("statements")},
              "import_timing_ms": {"schema": round((schema_event.get("completed_ns", 0) - schema_event.get("started_ns", 0)) / 1e6, 3),
                                   "insert": round((import_event.get("completed_ns", 0) - import_event.get("started_ns", 0)) / 1e6, 3)},
              "resources": {"status": "NOT MEASURED", "reason": "本工具不采集主机资源"},
              "performance": "NOT MEASURED", "capacity": "NOT MEASURED",
              "slo": stats.slo_state(False)}
    report["timing"] = timer.report()
    jsonio.write_new(out_dir / "native-report.json", report)
    print(f"native: correctness={report['correctness']} rows={report['dataset']['rows_readback']} "
          f"connections={args.connections} ops={len(operations)} success={success} "
          f"query_ms={report['throughput']['denominator_ms']} ops/s={overall}")
    return 0 if report["correctness"] == "ORACLE_VERIFIED" else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--connections", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--binding", default="literal", choices=("literal", "parameter"))
    parser.add_argument("--existing-db", default=None, help="复用已导入的库，跳过导入与写入")
    args = parser.parse_args()
    try:
        return execute(args)
    except (ValueError, TypeError, KeyError, OSError, subprocess.SubprocessError) as error:
        print(f"ERROR: 原生运行中止：{type(error).__name__}: {str(error)[:200]}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
