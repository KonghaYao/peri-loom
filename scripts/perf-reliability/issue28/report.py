"""Summaries and Markdown tables for the Issue 28 performance plan."""
from __future__ import annotations

import math
from dataclasses import dataclass, field

from .contract import ScenarioSpec
from .fixture import Fixture
from .runner import ConcurrencyMetrics, Measurement, RunResult


@dataclass
class SummaryRow:
    scenario: str
    title: str
    protocol: str
    kind: str
    samples: int
    median_ms: float
    p95_ms: float
    min_ms: float
    max_ms: float
    round_medians_ms: list[float]
    rows: int
    response_bytes: int
    query_ms: float | None
    status_counts: dict
    error_codes: dict
    outcomes: dict
    verdict: str
    verdict_detail: str = ""

    def as_record(self) -> dict:
        return self.__dict__.copy()


def summarize(result: RunResult, scenarios: list[ScenarioSpec],
              fixture: Fixture) -> list[SummaryRow]:
    specs = {scenario.key: scenario for scenario in scenarios}
    grouped: dict[tuple[str, str], list[Measurement]] = {}
    for measurement in result.timed:
        if measurement.concurrency:
            continue
        grouped.setdefault((measurement.scenario, measurement.protocol), []).append(measurement)
    rows: list[SummaryRow] = []
    for scenario in scenarios:
        if scenario.kind == "concurrency":
            continue
        for protocol in scenario.protocols:
            items = grouped.get((scenario.key, protocol.name), [])
            if not items:
                continue
            durations = [item.duration_ms for item in items]
            round_medians = []
            for round_index in sorted({item.round_index for item in items}):
                subset = [item.duration_ms for item in items if item.round_index == round_index]
                round_medians.append(_median(subset))
            query_values = [item.query_ms for item in items if item.query_ms is not None]
            row = SummaryRow(
                scenario=scenario.key,
                title=scenario.title,
                protocol=protocol.name,
                kind=scenario.kind,
                samples=len(items),
                median_ms=_median(durations),
                p95_ms=_percentile(durations, 95),
                min_ms=min(durations),
                max_ms=max(durations),
                round_medians_ms=round_medians,
                rows=int(_median([float(item.rows) for item in items])),
                response_bytes=int(_median([float(item.response_bytes) for item in items])),
                query_ms=_median(query_values) if query_values else None,
                status_counts=_counts(str(item.status) for item in items),
                error_codes=_counts(item.error_code for item in items if item.error_code),
                outcomes=_counts(item.outcome for item in items),
                verdict="—",
                verdict_detail="",
            )
            row.verdict, row.verdict_detail = _verdict(scenario, row, fixture)
            rows.append(row)
    _apply_protocol_comparison(rows)
    return rows


def _verdict(scenario: ScenarioSpec, row: SummaryRow, fixture: Fixture) -> tuple[str, str]:
    if not fixture.complete:
        return "N/A", "fixture counts differ from the fixed snapshot"
    bad_outcomes = {key: value for key, value in row.outcomes.items()
                    if key not in ("ok", "expected_limit")}
    if bad_outcomes:
        return "FAIL", "outcomes: " + ", ".join("%s=%d" % item for item in bad_outcomes.items())
    if scenario.kind == "limit" and scenario.expect_result_limit:
        if not any("result_limit" in code for code in row.error_codes):
            return "FAIL", "expected result_limit, got %r" % (row.error_codes,)
        if scenario.target_ms is not None and row.median_ms > scenario.target_ms:
            return "FAIL", "median %.2fms > %.2fms" % (row.median_ms, scenario.target_ms)
        return "PASS", ""
    if scenario.target_ms is not None:
        if row.median_ms > scenario.target_ms:
            return "FAIL", "median %.2fms > %.2fms" % (row.median_ms, scenario.target_ms)
        if (scenario.target_sql_ms is not None and row.query_ms is not None
                and row.query_ms > scenario.target_sql_ms):
            return "FAIL", "SQL-only %.3fms > %.3fms" % (row.query_ms, scenario.target_sql_ms)
        return "PASS", ""
    return "—", ""


def _apply_protocol_comparison(rows: list[SummaryRow]) -> None:
    for scenario in ("H1.content-20mb", "H3.content-projection"):
        json_row = next((row for row in rows
                         if row.scenario == scenario and row.protocol == "json-cursor"), None)
        pb_row = next((row for row in rows
                       if row.scenario == scenario and row.protocol == "protobuf-cursor"), None)
        if json_row is None or pb_row is None or json_row.median_ms <= 0:
            continue
        delta = (pb_row.median_ms - json_row.median_ms) / json_row.median_ms * 100.0
        pb_row.verdict_detail = "protobuf vs JSON %+.1f%%" % delta
        if delta > 5.0 and pb_row.verdict == "PASS":
            pb_row.verdict = "FAIL"
            pb_row.verdict_detail += " (more than 5% slower)"
        elif pb_row.verdict == "PASS":
            pb_row.verdict_detail += " (within noise)"


def concurrency_summary(result: RunResult) -> list[dict]:
    output = []
    for level in sorted({item.concurrency for item in result.timed if item.concurrency}):
        items = [item for item in result.timed if item.concurrency == level]
        durations = [item.duration_ms for item in items]
        metrics = next((item for item in result.concurrency if item.level == level), None)
        output.append({
            "level": level,
            "requests": len(items),
            "median_ms": _median(durations),
            "p95_ms": _percentile(durations, 95),
            "p99_ms": _percentile(durations, 99),
            "min_ms": min(durations),
            "max_ms": max(durations),
            "status_429": sum(1 for item in items if item.status == 429),
            "errors": sum(1 for item in items if item.outcome not in ("ok", "expected_limit")),
            "active_permits": metrics.drained.get("active_permits") if metrics else None,
            "unknown_held_permits": metrics.drained.get("unknown_held_permits") if metrics else None,
            "session_seats": metrics.drained.get("session_seats") if metrics else None,
            "rejected_delta": (
                metrics.after.get("rejected_requests_total", 0)
                - metrics.before.get("rejected_requests_total", 0)
                if metrics else None),
            "rejection_reasons": metrics.drained.get("rejection_reasons_total") if metrics else None,
            "drain_ms": metrics.drain_ms if metrics else None,
            "verdict": _concurrency_verdict(items, metrics),
        })
    return output


def _concurrency_verdict(items: list[Measurement], metrics: ConcurrencyMetrics | None) -> str:
    if any(item.status == 429 for item in items):
        return "FAIL"
    if metrics is not None:
        if metrics.drained.get("active_permits") not in (0, None):
            return "FAIL"
        if metrics.drained.get("unknown_held_permits") not in (0, None):
            return "FAIL"
        if metrics.drained.get("session_seats") not in (0, None):
            return "FAIL"
        if (metrics.after.get("rejected_requests_total", 0)
                > metrics.before.get("rejected_requests_total", 0)):
            return "FAIL"
    return "PASS"


def render_markdown(rows: list[SummaryRow], concurrency: list[dict], fixture: Fixture,
                    metadata: dict, warnings: list[str], fatal: str) -> str:
    lines = []
    lines.append("# Issue 28 纯 peri-loom 性能测试结果")
    lines.append("")
    lines.append("## 环境与数据")
    lines.append("")
    lines.append("| 项 | 值 |")
    lines.append("|---|---|")
    for key, value in metadata.items():
        lines.append("| %s | %s |" % (_escape(key), _escape(str(value))))
    lines.append("| threads | %s |" % _int(fixture.threads))
    lines.append("| messages | %s |" % _int(fixture.messages))
    lines.append("| content chars | %s |" % _int(fixture.content_chars))
    lines.append("| content bytes | %s |" % _int(fixture.content_bytes))
    lines.append("| focus thread messages | %s |" % _int(fixture.focus_messages))
    lines.append("| projection non-null | %s |" % _int(fixture.projection_non_null))
    lines.append("| projection bytes | %s |" % _int(fixture.projection_bytes))
    lines.append("| fixture | %s |" % ("PASS" if fixture.complete else "MISMATCH"))
    for key in ("max_sessions", "max_concurrent", "requests_per_second", "max_result_bytes"):
        if key in fixture.quotas:
            lines.append("| quota %s | %s |" % (key, _int(int(fixture.quotas[key]))))
    lines.append("")
    lines.append("## 结果")
    lines.append("")
    lines.append("| 版本 | 场景 | 协议 | 样本 | 中位数 | p95 | 最小 | 最大 | 行数 | 响应字节 | SQL-only | 结果 |")
    lines.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|")
    for row in rows:
        lines.append("| %s | %s | %s | %d | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            _escape(metadata.get("platform_label", "—")),
            _escape(row.scenario),
            _escape(row.protocol),
            row.samples,
            _ms(row.median_ms),
            _ms(row.p95_ms),
            _ms(row.min_ms),
            _ms(row.max_ms),
            _int(row.rows),
            _int(row.response_bytes),
            _ms(row.query_ms) if row.query_ms is not None else "—",
            _escape(_verdict_text(row)),
        ))
    lines.append("")
    lines.append("## 协议轮次中位数")
    lines.append("")
    lines.append("| 场景 | 协议 | 各轮中位数 |")
    lines.append("|---|---|---|")
    for row in rows:
        lines.append("| %s | %s | %s |" % (
            _escape(row.scenario), _escape(row.protocol),
            ", ".join(_ms(value) for value in row.round_medians_ms)))
    if concurrency:
        lines.append("")
        lines.append("## 并发")
        lines.append("")
        lines.append("| 并发 | 请求 | 中位数 | p95 | p99 | 429 | 错误 | active_permits | unknown_held_permits | session_seats | 排空 | 结果 |")
        lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
        for row in concurrency:
            lines.append("| %d | %d | %s | %s | %s | %d | %d | %s | %s | %s | %s | %s |" % (
                row["level"], row["requests"], _ms(row["median_ms"]),
                _ms(row["p95_ms"]), _ms(row["p99_ms"]), row["status_429"],
                row["errors"], _optional_int(row["active_permits"]),
                _optional_int(row["unknown_held_permits"]),
                _optional_int(row["session_seats"]),
                _ms(row["drain_ms"]) if row["drain_ms"] is not None else "—",
                row["verdict"]))
    lines.append("")
    lines.append("## 备注")
    lines.append("")
    lines.append("- 单流场景：%s 轮 × 每轮 %s 次 warmup + %s 次计时；表中统计各场景实际计时样本数。" % (
        metadata.get("rounds", "—"), metadata.get("warmup", "—"),
        metadata.get("samples", "—")))
    lines.append("- 并发场景每个并发级别 100 次，一次运行；warmup 不计入。")
    lines.append("- 所有请求带唯一 `X-Request-ID`；SQL 请求不自动重试。")
    lines.append("- `SQL-only` 来自响应中的 `query_duration_ms`；cursor 无该指标，显示 `—`。")
    lines.append("- R1 的 `result_limit` 是结果预算拒绝，属于预期行为，不计为错误。")
    if int(fixture.quotas.get("max_concurrent") or 0) < 16:
        lines.append("- X1 要求 `max_concurrent >= 16`；当前配额更低，8/16 并发的 429 不能用于验收。")
    else:
        lines.append("- X1 运行配额：`max_concurrent=%s`。" % fixture.quotas.get("max_concurrent"))
    if not fixture.complete:
        lines.append("- **fixture 与固定快照不一致，本表仅作探索性证据，不参与版本回归判定。**")
    if fixture.projection_non_null <= 1:
        lines.append("- 当前快照的 `projection` 基本为 NULL；H2/H3 不能代表真实投影载荷。")
    if warnings:
        lines.append("- warnings: `%s`" % _escape("; ".join(warnings[:8])))
    if fatal:
        lines.append("- **fatal: `%s`**" % _escape(fatal))
    lines.append("")
    return "\n".join(lines)


def _verdict_text(row: SummaryRow) -> str:
    text = row.verdict
    if row.verdict_detail:
        text += " (" + row.verdict_detail + ")"
    return text


def _counts(values) -> dict:
    output: dict[str, int] = {}
    for value in values:
        output[value] = output.get(value, 0) + 1
    return output


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _percentile(values: list[float], percent: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(1, math.ceil(percent / 100.0 * len(ordered)))
    return ordered[rank - 1]


def _ms(value: float | None) -> str:
    if value is None:
        return "—"
    return "%.2fms" % value


def _int(value: int) -> str:
    return "{:,}".format(value)


def _optional_int(value: int | None) -> str:
    return "—" if value is None else str(value)


def _escape(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
