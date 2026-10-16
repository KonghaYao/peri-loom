"""冻结的 E01 SQL 渲染器；只产生文本，不连接数据库、不执行语句。

DDL 与用户 2026-10-16 批准的结构一致，仅用于新建隔离 E01 fixture：
e01_records(id INTEGER PRIMARY KEY, group_id INTEGER NOT NULL,
            created_at INTEGER NOT NULL, payload TEXT NOT NULL)
索引 e01_records_group_time_id(group_id, created_at, id)
"""

import hashlib

from contract import FAMILIES, require
from model import batched

APPROVED_DDL = (
    "CREATE TABLE e01_records (id INTEGER PRIMARY KEY, group_id INTEGER NOT NULL, "
    "created_at INTEGER NOT NULL, payload TEXT NOT NULL)",
    "CREATE INDEX e01_records_group_time_id ON e01_records(group_id, created_at, id)",
)
SELECT_COLUMNS = "SELECT id, group_id, created_at, payload FROM e01_records WHERE "
ORDER_BY = " ORDER BY created_at, id LIMIT "
INSERT_COLUMNS = "INSERT INTO e01_records (id, group_id, created_at, payload) VALUES "


def schema_sha256():
    return hashlib.sha256("\n".join(APPROVED_DDL).encode()).hexdigest()


def literal(text):
    require(type(text) is str and not any(ord(character) < 32 for character in text),
            "payload_must_be_printable_text")
    return "'" + text.replace("'", "''") + "'"


def render(operation, binding="literal"):
    """返回 (sql, params)；四个族各自独立。

    binding="literal" 时查询参数内联为整数字面量，两个目标消费同一 SQL 文本；
    binding="parameter" 时使用 ? 占位符，仅用于已核验支持绑定的适配器。
    查询参数只允许整数，正文不进入任何谓词。
    """
    family, args = operation["family"], operation["args"]
    require(binding in ("literal", "parameter"), "invalid_binding")
    require(family in FAMILIES, "unknown_query_family")
    params = []

    def placeholder(value):
        require(type(value) is int, "invalid_query_argument")
        if binding == "parameter":
            params.append(value)
            return "?"
        return str(value)

    if family == "point":
        # 主键唯一，点查不带排序或 LIMIT，保持与已批准 SQL 一致。
        return SELECT_COLUMNS + "id = " + placeholder(args["id"]), params
    if family == "index_list":
        predicate = ("group_id = " + placeholder(args["group_id"])
                     + " AND created_at >= " + placeholder(args["since"]))
    elif family == "offset":
        predicate = "group_id = " + placeholder(args["group_id"])
    else:
        after = args["after"]
        require(type(after) is list and len(after) == 2 and all(type(part) is int for part in after),
                "invalid_keyset_cursor")
        # 占位符按 SQL 文本顺序生成；同一时间参数出现两次，参数模式必须绑定两次。
        predicate = ("group_id = " + placeholder(args["group_id"])
                     + " AND (created_at > " + placeholder(after[0])
                     + " OR (created_at = " + placeholder(after[0])
                     + " AND id > " + placeholder(after[1]) + "))")
    sql = SELECT_COLUMNS + predicate + ORDER_BY + placeholder(args["limit"])
    if family == "offset":
        sql += " OFFSET " + placeholder(args["offset"])
    return sql, params


def values_clause(rows):
    values = ",".join(
        f"({row['id']},{row['group_id']},{row['created_at']},{literal(row['payload'])})" for row in rows)
    return INSERT_COLUMNS + values


def import_statements(rows, batch_size):
    """把逻辑行渲染成冻结的多行 VALUES 语句；statement 边界进入证据摘要。"""
    require(type(batch_size) is int and 1 <= batch_size <= 1000, "invalid_batch_size")
    return (values_clause(batch) for batch in batched(list(rows), batch_size))
