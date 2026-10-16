"""每族时序与结果分类统计。测量是否存在与 SLO 是否获批彼此独立。

只有样本足够才输出对应分位，并显式标注 sufficient；不足时给 null 而不是编造。
所有时间来自被测量目标自身的原始事件（planned/dispatched/completed 单调时钟纳秒）。
"""

import math

QUANTILE_MIN_SAMPLES = {"p50": 1, "p95": 100, "p99": 1000}


def quantile(values, fraction):
    if not values:
        return None
    return sorted(values)[math.ceil(len(values) * fraction) - 1]


def quantiles(values):
    samples = len(values)
    return {
        "samples": samples,
        "min": min(values) if values else None,
        "p50": quantile(values, 0.50),
        "p95": quantile(values, 0.95) if samples >= QUANTILE_MIN_SAMPLES["p95"] else None,
        "p99": quantile(values, 0.99) if samples >= QUANTILE_MIN_SAMPLES["p99"] else None,
        "max": max(values) if values else None,
        "sufficient": {name: samples >= minimum for name, minimum in QUANTILE_MIN_SAMPLES.items()},
    }


def summarize(events, operations, families):
    """按族汇总：计划量、上报量、分类计数、服务时长、端到端时长、派发滞后。"""
    by_id = {operation["op_id"]: operation for operation in operations}
    summary = {}
    for family in families:
        planned = [operation for operation in operations if operation["family"] == family]
        rows = [event for event in events if by_id.get(event["op_id"], {}).get("family") == family]
        outcomes = {}
        for event in rows:
            outcomes[event["outcome"]] = outcomes.get(event["outcome"], 0) + 1
        service, end_to_end, lag = [], [], []
        success_service, success_end_to_end = [], []
        for event in rows:
            planned_ns = event.get("planned_ns")
            dispatched_ns = event.get("dispatched_ns")
            completed_ns = event.get("completed_ns")
            if None in (planned_ns, dispatched_ns, completed_ns):
                continue
            service.append((completed_ns - dispatched_ns) / 1e6)
            end_to_end.append((completed_ns - planned_ns) / 1e6)
            lag.append((dispatched_ns - planned_ns) / 1e6)
            if event["outcome"] == "success":
                success_service.append((completed_ns - dispatched_ns) / 1e6)
                success_end_to_end.append((completed_ns - planned_ns) / 1e6)
        summary[family] = {
            "planned_ops": len(planned),
            "reported_ops": len(rows),
            "missing_ops": len(planned) - len(rows),
            "outcomes": outcomes,
            "service_ms": quantiles(service),
            "end_to_end_ms": quantiles(end_to_end),
            "dispatch_lag_ms": quantiles(lag),
            "success_service_ms": quantiles(success_service),
            "success_end_to_end_ms": quantiles(success_end_to_end),
        }
    return summary


def slo_state(approved, comparison=None):
    """SLO 未获批时只报探索测量，不给 PASS；获批时才允许带入比较结果。"""
    return {"approved": bool(approved), "comparison": comparison if approved else None,
            "note": "未获批 SLO：仅探索测量，不构成性能通过" if not approved else "已获批 SLO 口径"}
