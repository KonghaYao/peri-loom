"""Scenario contract for the Issue 28 pure peri-loom performance plan."""
from __future__ import annotations

from dataclasses import dataclass, field

from .fixture import Fixture

THREAD_COLUMNS = (
    "id", "title", "cwd", "created_at", "updated_at", "message_count",
    "parent_thread_id", "snapshot_at_message_id", "hidden", "agent_status",
    "workspace_id", "archived",
)
THREAD_SELECT = ", ".join(THREAD_COLUMNS)


@dataclass(frozen=True)
class ProtocolSpec:
    name: str
    mode: str
    codec: str


JSON_PIPELINE = ProtocolSpec("json-pipeline", "pipeline", "json")
PB_PIPELINE = ProtocolSpec("protobuf-pipeline", "pipeline", "protobuf")
JSON_CURSOR = ProtocolSpec("json-cursor", "cursor", "json")
PB_CURSOR = ProtocolSpec("protobuf-cursor", "cursor", "protobuf")

SMALL_READ_PROTOCOLS = (JSON_PIPELINE, PB_PIPELINE, JSON_CURSOR, PB_CURSOR)
LARGE_READ_PROTOCOLS = (JSON_CURSOR, PB_CURSOR)
WRITE_PROTOCOLS = (JSON_PIPELINE, PB_PIPELINE)


@dataclass(frozen=True)
class ScenarioSpec:
    key: str
    title: str
    kind: str
    protocols: tuple[ProtocolSpec, ...]
    sql: str = ""
    args: tuple = ()
    want_rows: bool = True
    target_ms: float | None = None
    target_sql_ms: float | None = None
    expected_rows: int | None = None
    expect_result_limit: bool = False
    batch_size: int = 0
    baton_executes: int = 0
    write_operation: str = ""
    concurrency_levels: tuple[int, ...] = ()
    notes: str = ""


def build_scenarios(fixture: Fixture, include_optional: bool = True) -> list[ScenarioSpec]:
    focus = fixture.focus_thread
    scenarios = [
        ScenarioSpec(
            key="F1.select-1",
            title="固定开销 select 1（返回 1 行）",
            kind="read",
            protocols=SMALL_READ_PROTOCOLS,
            sql="SELECT 1",
            want_rows=True,
            target_ms=13.0,
            target_sql_ms=0.5,
            expected_rows=1,
        ),
        ScenarioSpec(
            key="F2.select-1-no-rows",
            title="固定开销 select 1（want_rows=false）",
            kind="read",
            protocols=(JSON_PIPELINE, PB_PIPELINE),
            sql="SELECT 1",
            want_rows=False,
            target_sql_ms=0.5,
        ),
        ScenarioSpec(
            key="L1.list-20",
            title="列表 20 行（cwd 命中）",
            kind="read",
            protocols=SMALL_READ_PROTOCOLS,
            sql=("SELECT %s FROM threads WHERE hidden = 0 AND cwd = ? "
                 "ORDER BY updated_at DESC, id DESC LIMIT 40" % THREAD_SELECT),
            args=(fixture.list_cwd,),
            want_rows=True,
            target_ms=15.0,
            expected_rows=fixture.list_threads,
        ),
        ScenarioSpec(
            key="L2.list-empty",
            title="列表 20 行（空 cwd）",
            kind="read",
            protocols=SMALL_READ_PROTOCOLS,
            sql=("SELECT %s FROM threads WHERE hidden = 0 AND cwd = ? "
                 "ORDER BY updated_at DESC, id DESC LIMIT 40" % THREAD_SELECT),
            args=(fixture.empty_cwd,),
            want_rows=True,
            expected_rows=0,
        ),
        ScenarioSpec(
            key="P1.thread-by-id",
            title="按 id 投影单线程",
            kind="read",
            protocols=SMALL_READ_PROTOCOLS,
            sql="SELECT %s FROM threads WHERE id = ?" % THREAD_SELECT,
            args=(focus,),
            want_rows=True,
            expected_rows=1,
        ),
        ScenarioSpec(
            key="H1.content-20mb",
            title="20.2MB 历史内容（8,625 行）",
            kind="read",
            protocols=LARGE_READ_PROTOCOLS,
            sql="SELECT content FROM messages WHERE thread_id = ? ORDER BY rowid",
            args=(focus,),
            want_rows=True,
            target_ms=1000.0,
            expected_rows=fixture.focus_messages,
            notes="projection 为空时 H2 只测 NULL 物化，不能代表真实投影载荷",
        ),
        ScenarioSpec(
            key="H2.projection",
            title="投影历史（8,625 行，当前快照多为 NULL）",
            kind="read",
            protocols=LARGE_READ_PROTOCOLS,
            sql="SELECT projection FROM messages WHERE thread_id = ? ORDER BY rowid",
            args=(focus,),
            want_rows=True,
            expected_rows=fixture.focus_messages,
        ),
        ScenarioSpec(
            key="H3.content-projection",
            title="内容 + 投影（8,625 行）",
            kind="read",
            protocols=LARGE_READ_PROTOCOLS,
            sql=("SELECT content, projection FROM messages WHERE thread_id = ? "
                 "ORDER BY rowid"),
            args=(focus,),
            want_rows=True,
            target_ms=1000.0,
            expected_rows=fixture.focus_messages,
        ),
        ScenarioSpec(
            key="B1.batch-20",
            title="同 pipeline 20 个 select 1",
            kind="batch",
            protocols=(JSON_PIPELINE, PB_PIPELINE) if include_optional else (JSON_PIPELINE,),
            sql="SELECT 1",
            want_rows=True,
            target_ms=20.0,
            expected_rows=20,
            batch_size=20,
        ),
        ScenarioSpec(
            key="C1.baton-10",
            title="同 baton 连续 10 个 select 1",
            kind="baton",
            protocols=(JSON_PIPELINE,),
            sql="SELECT 1",
            want_rows=True,
            expected_rows=1,
            baton_executes=10,
        ),
        ScenarioSpec(
            key="X1.concurrency",
            title="select 1 并发 1/4/8/16，各 100 次",
            kind="concurrency",
            protocols=(JSON_PIPELINE,),
            sql="SELECT 1",
            want_rows=True,
            expected_rows=1,
            concurrency_levels=(1, 4, 8, 16),
        ),
    ]
    for limit in (100, 500, 1000, 2000, 5000, 8625):
        scenarios.append(ScenarioSpec(
            key="R1.limit-%d" % limit,
            title="大结果 LIMIT %d" % limit,
            kind="limit",
            protocols=(JSON_PIPELINE, PB_PIPELINE),
            sql=("SELECT content FROM messages WHERE thread_id = ? "
                 "ORDER BY rowid LIMIT ?"),
            args=(focus, limit),
            want_rows=True,
            target_ms=120.0 if limit == 5000 else None,
            expect_result_limit=limit >= 5000,
            notes="超过 4MiB 默认结果预算时应返回 result_limit",
        ))
    for operation in ("create", "insert", "delete", "drop"):
        scenarios.append(ScenarioSpec(
            key="W1.%s" % operation,
            title="写入 %s table" % operation,
            kind="write",
            protocols=WRITE_PROTOCOLS,
            write_operation=operation,
        ))
    return scenarios
