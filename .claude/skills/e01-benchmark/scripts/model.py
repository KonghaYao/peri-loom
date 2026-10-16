"""不绑定数据库结构的候选逻辑模型；仅限低成本工具 smoke。"""

import hashlib
import heapq

from contract import FAMILIES, VERSION, digest, require, validate


def number(seed, domain, ordinal):
    raw = f"{VERSION}:{seed}:{domain}:{ordinal}".encode()
    return int.from_bytes(hashlib.sha256(raw).digest()[:8], "big")


def logical_bytes(row):
    # 三个有符号 64 位逻辑整数与 UTF-8 payload；不冒充物理页/索引字节。
    return 24 + len(row["payload"].encode("utf-8"))


def row_at(config, identity):
    """O(1) 单行生成；iter_rows/make_rows/抽样 oracle 共用同一确定性函数。"""
    data, seed = config["distribution"], config["seed"]
    weights = data["group_weights"]
    slot, group = number(seed, "group", identity) % sum(weights), 0
    while slot >= weights[group]:
        slot -= weights[group]
        group += 1
    length = data["payload_bytes"][number(seed, "length", identity) % len(data["payload_bytes"])]
    payload = hashlib.shake_256(f"{VERSION}:{seed}:payload:{identity}".encode()).hexdigest((length + 1) // 2)[:length]
    return {"id": identity, "group_id": group,
            "created_at": number(seed, "time", identity) % data["time_span"], "payload": payload}


def iter_rows(config):
    """流式生成；正式规模不驻留内存。"""
    validate(config)
    for identity in range(1, config["distribution"]["rows"] + 1):
        yield row_at(config, identity)


def mean_row_bytes(config):
    sizes = config["distribution"]["payload_bytes"]
    return 24 + sum(sizes) / len(sizes)


def make_rows(config):
    validate(config)
    data = config["distribution"]
    require(config["mode"] == "smoke", "formal_generation_not_implemented")
    require(data["rows"] <= 2048 and config["queries"]["per_family"] <= 256
            and config["queries"]["page_size"] <= 64, "offline_smoke_limit")
    require(data["rows"] * (24 + max(data["payload_bytes"])) <= 4 * 1024 * 1024, "offline_byte_limit")
    return list(iter_rows(config))


def batched(iterable, size):
    require(type(size) is int and size >= 1, "invalid_batch_size")
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def stream_group_window(config, group, depth, limit, scan_limit, depth_limit):
    """流式取某个组按 (created_at,id) 排序的第 [depth, depth+limit) 行。

    用大小为 depth+limit 的有界堆选前 K 名：内存上界即 depth_limit+limit 行。
    超过 depth_limit 或扫描上界时返回 complete=False，调用方必须把 oracle 判为
    not_checked，不得猜测结果；整库扫描完毕则结果完整（即使该组行数少于 depth）。
    """
    require(limit >= 1 and depth >= 0 and scan_limit >= 1, "invalid_oracle_budget")
    budget = depth + limit
    if budget > depth_limit:
        return [], 0, False
    window, scanned = [], 0
    for row in iter_rows(config):
        scanned += 1
        if scanned > scan_limit:
            return [], scanned, False
        if row["group_id"] != group:
            continue
        key = (-row["created_at"], -row["id"])
        if len(window) < budget:
            heapq.heappush(window, (key, row))
        elif key > window[0][0]:
            heapq.heapreplace(window, (key, row))
    ordered = [row for _, row in sorted(window, key=lambda item: item[0], reverse=True)]
    return ordered[depth:depth + limit], scanned, True


def order(row):
    return row["created_at"], row["id"]


def oracle(rows, operation):
    family, args = operation["family"], operation["args"]
    if family == "point":
        return [r for r in rows if r["id"] == args["id"]]
    selected = sorted((r for r in rows if r["group_id"] == args["group_id"]), key=order)
    if family == "index_list":
        selected = [r for r in selected if r["created_at"] >= args["since"]]
    elif family == "offset":
        selected = selected[args["offset"]:]
    elif family == "keyset":
        selected = [r for r in selected if order(r) > tuple(args["after"])]
    else:
        raise ValueError("unknown_query_family")
    return selected[:args["limit"]]


def operations(config, rows):
    seed, queries, data = config["seed"], config["queries"], config["distribution"]
    for family in FAMILIES:
        for index in range(queries["per_family"]):
            value = number(seed, f"query:{family}", index)
            hot = number(seed, f"hot:{family}", index) % 1000000 < data["hot_access_ppm"]
            keyspace = max(1, len(rows) * data["hot_fraction_ppm"] // 1000000) if hot else len(rows)
            anchor = rows[value % keyspace]
            args = {"id": anchor["id"]}
            if family != "point":
                candidates = sorted((r for r in rows if r["group_id"] == anchor["group_id"]), key=order)
                args = {"group_id": anchor["group_id"], "limit": queries["page_size"]}
                depth = len(candidates) * queries["deep_ppm"] // 1000000
                if family == "index_list":
                    args["since"] = anchor["created_at"]
                elif family == "offset":
                    args["offset"] = depth
                else:
                    args["after"] = list(order(candidates[depth - 1])) if depth else [-1, -1]
            yield {"op_id": f"{family}:{index}", "family": family, "args": args}


def traversal_operations(rows, page_size, group_limit=2):
    """首页到空尾页的连续翻页操作；OFFSET 与 keyset 各自独立成序列。"""
    groups = sorted({row["group_id"] for row in rows})[:group_limit]
    plans = []
    for group in groups:
        ordered = sorted((row for row in rows if row["group_id"] == group), key=order)
        for index in range(0, len(ordered) + page_size, page_size):
            plans.append({"op_id": f"offset:traverse:{group}:{index}", "family": "offset",
                          "args": {"group_id": group, "limit": page_size, "offset": index},
                          "traversal": f"offset:{group}"})
        cursor, page = [-1, -1], 0
        while True:
            operation = {"op_id": f"keyset:traverse:{group}:{page}", "family": "keyset",
                         "args": {"group_id": group, "limit": page_size, "after": cursor},
                         "traversal": f"keyset:{group}"}
            plans.append(operation)
            expected = oracle(rows, operation)
            if not expected:
                break
            cursor = list(order(expected[-1]))
            page += 1
    return plans


def build_queries(config, rows):
    queries = []
    for operation in operations(config, rows):
        queries.append({**operation, "traversal": None, "expected_ids": _expected_ids(rows, operation)})
    for operation in traversal_operations(rows, config["queries"]["page_size"]):
        queries.append({**operation, "expected_ids": _expected_ids(rows, operation)})
    for query in queries:
        query["expected_sha256"] = digest(oracle(rows, query))
    return queries


def _expected_ids(rows, operation):
    return [row["id"] for row in oracle(rows, operation)]


def row_view(values):
    require(type(values) is list and len(values) == 4, "invalid_result_row")
    identity, group, created, payload = values
    require(type(identity) is int and type(group) is int and type(created) is int, "invalid_result_integer")
    require(type(payload) is str, "invalid_result_text")
    return {"id": identity, "group_id": group, "created_at": created, "payload": payload}


def generate(config):
    rows = make_rows(config)
    queries = build_queries(config, rows)
    workset = {family: set() for family in FAMILIES}
    for query in queries:
        workset[query["family"]].update(query["expected_ids"])
    return {"version": VERSION, "mode": "smoke", "schema_status": "APPROVED_ISOLATED_FIXTURE",
            "config_sha256": digest(config), "dataset_sha256": digest(rows),
            "queries_sha256": digest(queries), "rows": rows, "queries": queries,
            "logical_rows": len(rows), "logical_bytes": sum(map(logical_bytes, rows)),
            "returned_working_set_bytes": {family: sum(logical_bytes(rows[identity - 1]) for identity in ids)
                                           for family, ids in workset.items()},
            "coverage": "SMOKE_ONLY", "performance": "NOT MEASURED"}


def query_plan(config, per_family, page_size, deep_ppm):
    """确定性生成四族查询参数；不驻留数据集。"""
    queries, rows_total = [], config["distribution"]["rows"]
    groups = len(config["distribution"]["group_weights"])
    for family in FAMILIES:
        for index in range(per_family):
            seed = number(config["seed"], f"plan:{family}", index)
            group = seed % groups
            depth = rows_total * deep_ppm // 1000000
            args = {"id": 1 + seed % rows_total}
            if family != "point":
                args = {"group_id": group, "limit": page_size}
                if family == "index_list":
                    args["since"] = number(config["seed"], f"plan:since:{index}", 0) % config["distribution"]["time_span"]
                elif family == "offset":
                    args["offset"] = depth
                else:
                    args["after"] = None
            queries.append({"op_id": f"{family}:formal:{index}", "family": family,
                            "args": args, "oracle_depth": depth})
    return queries


def check_result(rows, operation, actual):
    require(actual == oracle(rows, operation), "oracle_mismatch_stop_batch")


def check_complete_pages(rows, group, pages):
    expected = sorted((r for r in rows if r["group_id"] == group), key=order)
    require([r for page in pages for r in page] == expected, "pagination_gap_duplicate_or_order_stop_batch")
