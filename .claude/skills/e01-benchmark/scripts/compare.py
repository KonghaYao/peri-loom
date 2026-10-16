"""运行间对比：先验证可比性，再给倍数；不可比时不产出正式倍数。

复用条件（全部满足才算 COMPARABLE）：模型版本、config 摘要、清单摘要、绑定方式、
目标适配器、时钟与计时边界、并发配置。跨目标（原生嵌入式 vs 平台 E2E）永远标记
cross_boundary：差值包含协议、网络、会话、审计与部署边界，不是纯引擎收益。
缺目标或缺字段 → NOT_MEASURED；不匹配或含 Unknown → 仅探索且不可比。
"""

import json
from pathlib import Path

REQUIRED_FOR_COMPARABLE = ("version", "model_version", "config_sha256", "manifest_sha256",
                           "binding", "adapter", "target", "timing_boundary")


def normalize(report):
    """把平台 queries.json 或原生 report.json 归一成同一组可对比字段。"""
    if report is None:
        return None
    harness = report.get("harness") or {}
    families = report.get("families") or {}
    success = sum(entry.get("outcomes", {}).get("success", 0) for entry in families.values())
    return {
        "version": report.get("version"),
        "model_version": report.get("model_version", report.get("version")),
        "config_sha256": report.get("config_sha256"),
        "manifest_sha256": report.get("manifest_sha256"),
        "binding": report.get("binding") or harness.get("binding"),
        "adapter": report.get("adapter") or harness.get("adapter"),
        "target": report.get("target") or harness.get("target"),
        "timing_boundary": report.get("timing_boundary",
                                      "service+dispatch+planned" if report.get("clock") else
                                      ("response_to_full_body" if report.get("target") == "platform"
                                       else None)),
        "concurrency": report.get("concurrency", 1),
        "sessions_requested": report.get("sessions_requested"),
        "sessions_opened": report.get("sessions_opened"),
        "correctness": report.get("correctness"),
        "outcomes": report.get("outcomes", {}),
        "families": families,
        "success_ops": success,
        "wall_ms": (report.get("timing") or {}).get("total_wall_ms"),
        "query_phase_ms": (report.get("timing") or {}).get("phases", {}).get("query"),
        "engine": harness.get("engine") or report.get("engine"),
    }


def _ratio(numerator, denominator):
    if numerator is None or denominator in (None, 0):
        return None
    return round(numerator / denominator, 4)


def _family_throughput(entry, query_phase_ms):
    success = entry.get("outcomes", {}).get("success")
    if not success or not query_phase_ms:
        return None
    return round(success / (query_phase_ms / 1000.0), 3)


def compare(base_path, candidate_path):
    paths = {"base": base_path, "candidate": candidate_path}
    loaded = {}
    for name, path in paths.items():
        if not path or not Path(path).is_file():
            return {"comparability": "NOT_MEASURED", "targets": paths,
                    "reasons": [f"{name}_report_missing"], "scaling": None,
                    "cross_boundary": False}
        loaded[name] = normalize(json.loads(Path(path).read_text()))
    base, candidate = loaded["base"], loaded["candidate"]
    reasons = [f"missing_{field}" for field in REQUIRED_FOR_COMPARABLE
               if base.get(field) is None or candidate.get(field) is None]
    for field in ("model_version", "config_sha256", "manifest_sha256", "binding", "timing_boundary"):
        if base.get(field) is not None and candidate.get(field) is not None \
                and base[field] != candidate[field]:
            reasons.append(f"{field}_mismatch")
    cross = base["target"] != candidate["target"]
    if cross:
        reasons.append("cross_target_boundary")
    if base.get("sessions_requested") not in (None, base.get("sessions_opened")):
        reasons.append("base_sessions_partially_admitted")
    if candidate.get("sessions_requested") not in (None, candidate.get("sessions_opened")):
        reasons.append("candidate_sessions_partially_admitted")
    for name, entry in (("base", base), ("candidate", candidate)):
        non_success = {key: value for key, value in entry["outcomes"].items() if key != "success"}
        if non_success:
            reasons.append(f"{name}_non_success_outcomes_present")
    blocking = [reason for reason in reasons if reason.endswith("mismatch") or reason == "cross_target_boundary"]
    comparability = ("NOT_COMPARABLE" if blocking else
                     "EXPLORATION_ONLY" if reasons else "COMPARABLE")
    scaling = None
    if comparability == "COMPARABLE":
        base_phase, candidate_phase = base["query_phase_ms"], candidate["query_phase_ms"]
        scaling = {
            "wall_ratio": _ratio(candidate["wall_ms"], base["wall_ms"]),
            "query_phase_ratio": _ratio(candidate_phase, base_phase),
            "success_ops_per_second_ratio": _ratio(
                _family_throughput({"outcomes": {"success": candidate["success_ops"]}}, candidate_phase),
                _family_throughput({"outcomes": {"success": base["success_ops"]}}, base_phase)),
            "per_session_efficiency_ratio": _ratio(
                _ratio(candidate["success_ops"], candidate["concurrency"]),
                _ratio(base["success_ops"], base["concurrency"])),
            "throughput_per_family": {
                family: {"base": _family_throughput(base["families"][family], base_phase),
                         "candidate": _family_throughput(candidate["families"][family], candidate_phase),
                         "ratio": _ratio(_family_throughput(candidate["families"][family], candidate_phase),
                                         _family_throughput(base["families"][family], base_phase))}
                for family in sorted(set(base["families"]) & set(candidate["families"]))},
        }
    return {"comparability": comparability, "targets": paths, "reasons": sorted(set(reasons)),
            "cross_boundary": cross, "scaling": scaling,
            "denominators": {"throughput": "成功操作数 / 查询阶段墙钟秒（同一计时边界）",
                             "wall": "整轮墙钟（含准备与清理，跨目标时边界不同）"},
            "note": ("跨边界比较：平台 E2E 与原生嵌入式在协议/会话/网络/审计/部署上不同，"
                     "差值不是纯引擎收益" if cross else "同目标同边界对比")}
