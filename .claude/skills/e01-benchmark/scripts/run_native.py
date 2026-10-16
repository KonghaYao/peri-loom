"""E01 native 目标运行器：生成 → 导入 → 查询 → oracle 核验 → 证据。

只编排技能自带的本地 TursoDB harness（原生基线，用于跨边界对照，
不是平台验证）。不连接 Peri 平台、不读取凭据、不重试 SQL。
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

import jsonio
import verify
from contract import VERSION, require, validate
from model import generate
from sql import APPROVED_DDL, import_statements, schema_sha256

DEFAULT_BINARY = Path(__file__).resolve().parents[1] / "native-harness/target/debug/e01-native-harness"
KINDS = ("schema", "import", "op", "explain")


def read_events(path):
    events = []
    with Path(path).open("r", encoding="utf-8") as stream:
        for line in stream:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            require(type(event) is dict and event.get("kind") in KINDS, "invalid_harness_event")
            if event["kind"] in ("op", "explain"):
                require(type(event.get("op_id")) is str, "harness_event_without_op_id")
            events.append(event)
    return events


def invoke(binary, mode, plan, output, timeout_seconds, required=True):
    completed = subprocess.run(
        [str(binary), mode, "--plan", str(plan), "--out", str(output)],
        capture_output=True, text=True, timeout=timeout_seconds)
    if completed.returncode != 0:
        if required:
            raise ValueError(f"native_harness_{mode}_failed_{completed.returncode}")
        return None
    return read_events(output)


def run(args):
    config = validate(jsonio.load(args.config))
    require(config["mode"] == "smoke", "native_runner_smoke_only")
    binary = Path(args.binary)
    require(binary.is_file(), "native_harness_binary_missing")
    out_dir = Path(args.out_dir)
    require(not out_dir.exists(), "output_directory_already_exists")
    out_dir.mkdir(mode=0o700)
    bundle = generate(config)
    rows = bundle["rows"]
    operations = verify.build_operations(bundle, args.binding)
    database = out_dir / "fixture.db"
    require(not database.exists(), "fixture_database_already_exists")
    import_path = out_dir / "import.jsonl"
    jsonio.write_lines_new(import_path, (json.dumps(statement)
                                         for statement in import_statements(rows, args.batch_size)))
    jsonio.write_new(out_dir / "plan.json", {
        "version": VERSION, "database_path": str(database), "import_path": str(import_path),
        "import_transaction": "single", "sync_mode": config["sync_mode"],
        "schema": list(APPROVED_DDL), "dataset_rows": len(rows),
        "ops": [{"op_id": item["op_id"], "sql": item["sql"], "params": item["params"]}
                for item in operations]})
    jsonio.write_new(out_dir / "scenario.json", {
        "version": VERSION, "config_sha256": bundle["config_sha256"],
        "dataset_sha256": bundle["dataset_sha256"], "queries_sha256": bundle["queries_sha256"],
        "schema_sha256": schema_sha256(), "sync_mode": config["sync_mode"],
        "logical_rows": bundle["logical_rows"], "logical_bytes": bundle["logical_bytes"],
        "returned_working_set_bytes": bundle["returned_working_set_bytes"], "operations": operations})
    import_events = invoke(binary, "import", out_dir / "plan.json", out_dir / "import.ndjson",
                           args.timeout_seconds)
    query_events = invoke(binary, "query", out_dir / "plan.json", out_dir / "query.ndjson",
                          args.timeout_seconds)
    explain_events = invoke(binary, "explain", out_dir / "plan.json", out_dir / "explain.ndjson",
                            args.timeout_seconds, required=False)
    import_summary = next((event for event in import_events if event["kind"] == "import"), None)
    require(import_summary is not None, "harness_reported_no_import")
    records, pages = verify.verify_operations(bundle, operations, query_events)
    raw, report = verify.assemble(config, bundle, records, {
        "target": "native", "adapter": "turso_core-embedded", "binding": args.binding,
        "import": import_summary, "sync_mode": config["sync_mode"], "schema_sha256": schema_sha256(),
        "explains": len(explain_events) if explain_events is not None else None}, target="native")
    report["traversal"] = verify.traversal_verdicts(rows, pages)
    jsonio.write_new(out_dir / "evidence.json", raw)
    jsonio.write_new(out_dir / "report.json", report)
    print(f"E01 native smoke: correctness={report['correctness']} "
          f"verified={report['oracle_verified_ops']}/{report['ops_total']} "
          f"records={len(records)} rows={import_summary['rows']}")
    print("仅工具与 oracle 正确性证据；无性能、规模或容量结论。")
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
    parser.add_argument("--binary", default=str(DEFAULT_BINARY))
    parser.add_argument("--binding", default="literal", choices=("literal", "parameter"))
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    args = parser.parse_args()
    try:
        return run(args)
    except (ValueError, TypeError, KeyError, OSError, subprocess.SubprocessError) as error:
        print(f"ERROR: E01 native 运行中止：{type(error).__name__}: {str(error)[:200]}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
