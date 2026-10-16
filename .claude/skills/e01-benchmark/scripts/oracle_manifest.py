"""可复核的期望清单：一次流式扫描算出全部计划操作的期望结果。

不使用“每请求扫描全量 + not_checked”作为验收方式：深度预算不足时在**构建阶段**
直接拒绝（manifest_depth_budget_exceeded），不产出半成品清单。
内存上界为 各操作 (depth+limit+1) 行之和；不驻留数据集。
清单带来源（模型版本、config 摘要、计划、扫描量、算法）与内容摘要，可独立重算比对。
"""

import hashlib
import heapq

from contract import VERSION, digest, require, validate
from model import iter_rows, query_plan, row_at


def _ordered(heap):
    return [row for _, row in sorted(heap, key=lambda item: item[0], reverse=True)]


def build_manifest(config, per_family, page_size, deep_ppm, depth_limit):
    validate(config)
    plan = query_plan(config, per_family, page_size, deep_ppm)
    state, by_group = {}, {}
    for index, operation in enumerate(plan):
        if operation["family"] == "point":
            continue
        group = operation["args"]["group_id"]
        # index_list 的谓词是 created_at >= since，只需保留最小的 limit 行，与深度无关。
        since = operation["args"].get("since")
        budget = (operation["args"]["limit"] if since is not None else
                  operation["args"].get("offset", 0) + operation["args"]["limit"] + 1)
        require(budget <= depth_limit, "manifest_depth_budget_exceeded")
        state[index] = {"heap": [], "budget": budget, "group": group, "since": since}
        by_group.setdefault(group, []).append(index)
    scanned = 0
    for row in iter_rows(config):
        scanned += 1
        for index in by_group.get(row["group_id"], ()):
            entry = state[index]
            if entry["since"] is not None and row["created_at"] < entry["since"]:
                continue
            key = (-row["created_at"], -row["id"])
            heap = entry["heap"]
            if len(heap) < entry["budget"]:
                heapq.heappush(heap, (key, row))
            elif key > heap[0][0]:
                heapq.heapreplace(heap, (key, row))
    operations = []
    for index, operation in enumerate(plan):
        family, args = operation["family"], operation["args"]
        if family == "point":
            expected = [row_at(config, args["id"])]
        else:
            ordered = _ordered(state[index]["heap"])
            depth = args.get("offset", 0)
            if family == "keyset":
                cursor = ordered[depth - 1] if depth > 0 else {"created_at": -1, "id": -1}
                args = {**args, "after": [cursor["created_at"], cursor["id"]]}
            if family == "index_list":
                expected = ordered[:args["limit"]]
            elif family == "offset":
                expected = ordered[depth:depth + args["limit"]]
            else:
                expected = [row for row in ordered[depth:] if (row["created_at"], row["id"])
                            > (cursor["created_at"], cursor["id"])][:args["limit"]]
        operations.append({"op_id": operation["op_id"], "family": family, "args": args,
                           "expected": expected, "expected_sha256": digest(expected)})
    body = {"version": VERSION, "model_version": VERSION,
            "config_sha256": digest(config), "operations": operations,
            "scan": {"rows_scanned": scanned, "depth_limit": depth_limit,
                     "algorithm": "bounded-heap-single-pass-v1", "per_family": per_family,
                     "page_size": page_size, "deep_ppm": deep_ppm}}
    return {**body, "manifest_sha256": digest(body)}


def manifest_body(manifest):
    return {key: manifest[key] for key in ("version", "model_version", "config_sha256",
                                           "operations", "scan")}


def verify_manifest(config, manifest):
    """先校验清单自身摘要（防篡改），再独立重算并与存储内容比对。"""
    require(manifest.get("version") == VERSION, "manifest_version_mismatch")
    require(manifest.get("config_sha256") == digest(config), "manifest_config_mismatch")
    require(digest(manifest_body(manifest)) == manifest.get("manifest_sha256"),
            "manifest_digest_mismatch")
    scan = manifest.get("scan") or {}
    rebuilt = build_manifest(config, scan.get("per_family"), scan.get("page_size"),
                             scan.get("deep_ppm"), scan.get("depth_limit"))
    require(rebuilt["manifest_sha256"] == manifest.get("manifest_sha256"), "manifest_mismatch")
    return True
