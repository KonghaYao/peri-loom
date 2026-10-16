"""目标无关的核验：oracle 比较、完整翻页判定与证据组装。

native 与 platform 适配器只负责把返回行交给这里；判定逻辑只有一份。
"""

from contract import digest, require
from evidence import summarize, template as raw_template
from model import check_complete_pages, oracle, row_view
from sql import render


def labels(operations):
    """请求 ID：公开协议要求 ^[A-Za-z0-9_-]{1,64}$，且必须唯一。"""
    import hashlib
    import re

    result = {}
    for operation in operations:
        sanitized = re.sub(r"[^A-Za-z0-9_-]", "-", operation["op_id"])
        if len(sanitized) > 64:
            sanitized = sanitized[:48] + "-" + hashlib.sha256(operation["op_id"].encode()).hexdigest()[:15]
        require(sanitized not in result, "duplicate_request_label")
        result[sanitized] = operation["op_id"]
    return {value: key for key, value in result.items()}


def build_operations(bundle, binding):
    operations = []
    for query in bundle["queries"]:
        sql, params = render(query, binding)
        operations.append({"op_id": query["op_id"], "family": query["family"],
                           "traversal": query["traversal"], "sql": sql, "params": params})
    return operations


def verify_operations(bundle, operations, events, kind="op"):
    """按 oracle 比较每个操作的实际返回；形状错误记为 mismatch，不静默跳过。"""
    by_id = {}
    for event in events:
        if event["kind"] != kind:
            continue
        require(event["op_id"] not in by_id, "duplicate_target_operation")
        by_id[event["op_id"]] = event
    queries = {query["op_id"]: query for query in bundle["queries"]}
    records, pages = [], {}
    for operation in operations:
        event = by_id.get(operation["op_id"])
        if event is None:
            # 缺失操作交给摘要统计；这里不伪造终态。
            continue
        outcome = event["outcome"]
        record = {"op_id": operation["op_id"], "outcome": outcome,
                  "planned_ns": event["planned_ns"], "dispatched_ns": event["dispatched_ns"],
                  "completed_ns": event["completed_ns"], "result_sha256": None,
                  "oracle": "not_checked", "error_class": event.get("error_class"),
                  "error_message": event.get("error_message")}
        if outcome == "success":
            require(record["error_class"] is None and record["error_message"] is None,
                    "success_with_error_detail")
            expected = oracle(bundle["rows"], queries[operation["op_id"]])
            try:
                actual = [row_view(values) for values in event["rows"]]
            except (ValueError, TypeError) as error:
                actual, record["error_message"] = [], str(error)[:200]
            record["oracle"] = "match" if actual == expected else "mismatch"
            record["result_sha256"] = digest(actual)
            if operation["traversal"]:
                pages.setdefault(operation["traversal"], []).append(actual)
        records.append(record)
    return records, pages


def traversal_verdicts(rows, pages):
    verdicts = []
    for key, actual_pages in pages.items():
        family, group = key.split(":")
        entry = {"traversal": key, "family": family, "group": int(group), "pages": len(actual_pages)}
        try:
            check_complete_pages(rows, int(group), actual_pages)
            entry["verdict"] = "complete"
        except ValueError as error:
            entry["verdict"] = str(error)
        verdicts.append(entry)
    return verdicts


def assemble(config, bundle, records, harness, target):
    raw = raw_template(config)
    raw["target"] = target
    raw["records"] = records
    report = summarize(config, raw)
    report["harness"] = harness
    return raw, report
