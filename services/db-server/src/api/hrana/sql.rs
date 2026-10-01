//! SQL 文本扫描：命名参数改写与多语句切分。
//!
//! Hrana 的命名参数（`:name` / `@name` / `$name`）到达服务端时 SQL 里仍然带着
//! `:name` 字面量 —— `@libsql/client` 在 HTTP 上**不做**客户端插值。平台的绑定通道
//! 只有位置参数，所以这里把命名占位符改写成 `?` 并给出对应的取值顺序。
//!
//! 改写而不是把值拼成 SQL 字面量，是刻意的选择：拼字符串要自己处理引号、二进制、
//! 以及"这个字符到底在不在字符串里"的问题，任何一处疏漏都是注入。改写后由引擎绑定，
//! 转义责任留在引擎侧。
//!
//! 扫描器只做一件事：把 SQL 按「代码区 / 非代码区」切开。字符串字面量、`"…"` 与
//! `` `…` `` 引号标识符、`[…]`、`--` 行注释、`/* */` 块注释里的字符**都不是代码**，
//! 里面的 `:name` / `;` 一律不参与识别。

use domain::value::SqlValue;

/// 命名参数绑定结果。
#[derive(Debug, Clone, PartialEq)]
pub struct NamedBind {
    /// 改写后的 SQL（命名占位符已替换成 `?`）。
    pub sql: String,
    /// 按占位符出现顺序排列的取值。
    pub args: Vec<SqlValue>,
}

/// 逐字节标记「该字节是否处于代码区」。
///
/// 只对 ASCII 标点做判断，而 UTF-8 续字节恒 ≥ 0x80，因此按字节扫描不会切坏多字节字符。
fn code_mask(sql: &str) -> Vec<bool> {
    let bytes = sql.as_bytes();
    let mut mask = vec![true; bytes.len()];
    let mut i = 0usize;
    while i < bytes.len() {
        let start = i;
        match bytes[i] {
            quote @ (b'\'' | b'"' | b'`') => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == quote {
                        // SQL 里两个连续引号表示一个字面引号，不结束字面量。
                        if bytes.get(i + 1) == Some(&quote) {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            b'[' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b']' {
                    i += 1;
                }
                i = (i + 1).min(bytes.len());
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
            }
            _ => {
                i += 1;
                continue;
            }
        }
        // 上面每个分支结束时 i 都指向"非代码区"的右边界，统一反填。
        for slot in mask.iter_mut().take(i.min(bytes.len())).skip(start) {
            *slot = false;
        }
    }
    mask
}

/// 是否是 SQLite 参数名的合法字符。
fn is_param_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

/// 把命名占位符改写成位置占位符 `?`。
///
/// 同一名字出现多次时，每个位置各取一份值（`select :a, :a` -> `select ?, ?` 且两个
/// 位置同值）——位置绑定没有"共享槽位"的概念，必须逐槽补齐。
///
/// # Errors
/// - SQL 里出现了没给值的命名参数；
/// - 给了值但 SQL 里没有对应占位符（多余参数）；
/// - 与位置占位符 `?` 混用（谁的顺序在前无法判定，宁可报错也不猜）。
pub fn bind_named_args(sql: &str, named: &[(String, SqlValue)]) -> Result<NamedBind, String> {
    if named.is_empty() {
        return Ok(NamedBind {
            sql: sql.to_string(),
            args: Vec::new(),
        });
    }

    let mask = code_mask(sql);
    let bytes = sql.as_bytes();

    let mut out = String::with_capacity(sql.len());
    let mut args: Vec<SqlValue> = Vec::new();
    let mut used = vec![false; named.len()];
    let mut positionals = 0usize;
    // 已经复制进 `out` 的字节数。
    let mut copied = 0usize;
    let mut cursor = 0usize;

    while cursor < bytes.len() {
        let byte = bytes[cursor];
        if !mask[cursor] {
            cursor += 1;
            continue;
        }
        if byte == b'?' {
            positionals += 1;
            cursor += 1;
            continue;
        }
        if byte != b':' && byte != b'@' && byte != b'$' {
            cursor += 1;
            continue;
        }
        // 前缀字符 + 至少一个名字字符才算占位符（裸 `:` / `::` 只是普通字符）。
        let name_start = cursor + 1;
        let mut name_end = name_start;
        while name_end < bytes.len() && is_param_char(bytes[name_end]) {
            name_end += 1;
        }
        if name_end == name_start {
            cursor += 1;
            continue;
        }
        let name = &sql[name_start..name_end];
        let index = named
            .iter()
            .position(|(candidate, _)| candidate == name)
            .ok_or_else(|| format!("命名参数 {name} 没有被赋值"))?;
        used[index] = true;

        out.push_str(&sql[copied..cursor]);
        out.push('?');
        args.push(named[index].1.clone());
        copied = name_end;
        cursor = name_end;
    }

    if positionals > 0 {
        return Err(format!(
            "SQL 同时使用了 {positionals} 个位置占位符 `?` 与命名参数，取值顺序无法判定"
        ));
    }
    if let Some(index) = used.iter().position(|used| !*used) {
        return Err(format!(
            "命名参数 {} 在 SQL 中没有对应的占位符",
            named[index].0
        ));
    }

    out.push_str(&sql[copied..]);
    Ok(NamedBind { sql: out, args })
}

/// SQL 里是否含位置占位符 `?`（只在代码区里找）。
#[must_use]
pub fn has_positional_placeholder(sql: &str) -> bool {
    let mask = code_mask(sql);
    sql.as_bytes()
        .iter()
        .enumerate()
        .any(|(index, byte)| *byte == b'?' && mask[index])
}

/// 按代码区里的 `;` 切分多语句 SQL，返回非空的语句文本。
///
/// `CREATE TRIGGER ... BEGIN ... END` 的语句体内含 `;`，会被误切；调用方应先用
/// [`is_trigger_definition`] 排除这类语句。
#[must_use]
pub fn split_statements(sql: &str) -> Vec<String> {
    let mask = code_mask(sql);
    let bytes = sql.as_bytes();
    let mut statements = Vec::new();
    let mut start = 0usize;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b';' && mask[index] {
            push_statement(&mut statements, &sql[start..index]);
            start = index + 1;
        }
    }
    push_statement(&mut statements, &sql[start..]);
    statements
}

fn push_statement(statements: &mut Vec<String>, raw: &str) {
    let trimmed = raw.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }
}

/// 是否是触发器定义（语句体内含 `;`，不能按 `;` 切分）。
#[must_use]
pub fn is_trigger_definition(sql: &str) -> bool {
    let lowered = strip_leading_trivia(sql).to_ascii_lowercase();
    [
        "create trigger",
        "create temp trigger",
        "create temporary trigger",
    ]
    .iter()
    .any(|prefix| lowered.starts_with(prefix))
}

/// 去掉开头的空白与注释。
fn strip_leading_trivia(sql: &str) -> &str {
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    loop {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if bytes.get(i) == Some(&b'-') && bytes.get(i + 1) == Some(&b'-') {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if bytes.get(i) == Some(&b'/') && bytes.get(i + 1) == Some(&b'*') {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            continue;
        }
        break;
    }
    &sql[i.min(sql.len())..]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(pairs: &[(&str, SqlValue)]) -> Vec<(String, SqlValue)> {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_string(), value.clone()))
            .collect()
    }

    #[test]
    fn named_placeholders_are_rewritten_in_order() {
        let bind = bind_named_args(
            "select :b, :a",
            &named(&[
                ("a", SqlValue::Integer(1)),
                ("b", SqlValue::Text("x".to_string())),
            ]),
        )
        .expect("改写成功");
        assert_eq!(bind.sql, "select ?, ?");
        // 顺序按占位符出现顺序（:b 在前），不是按参数列表顺序。
        assert_eq!(
            bind.args,
            vec![
                SqlValue::Text("x".to_string()),
                SqlValue::Integer(1)
            ]
        );
    }

    #[test]
    fn every_prefix_flavour_is_recognised() {
        for sql in ["select :a", "select @a", "select $a"] {
            let bind = bind_named_args(sql, &named(&[("a", SqlValue::Integer(7))]))
                .unwrap_or_else(|err| panic!("{sql} 改写失败: {err}"));
            assert_eq!(bind.sql, "select ?", "{sql}");
            assert_eq!(bind.args, vec![SqlValue::Integer(7)]);
        }
    }

    #[test]
    fn repeated_placeholder_yields_one_value_per_slot() {
        let bind = bind_named_args("select :a + :a", &named(&[("a", SqlValue::Integer(3))]))
            .expect("改写成功");
        assert_eq!(bind.sql, "select ? + ?");
        assert_eq!(bind.args, vec![SqlValue::Integer(3), SqlValue::Integer(3)]);
    }

    #[test]
    fn placeholders_inside_literals_and_comments_are_left_alone() {
        let sql = "select ':a', \":b\", `:c`, [':d'] -- :e\n, :f /* :g */";
        let bind = bind_named_args(sql, &named(&[("f", SqlValue::Integer(1))]))
            .expect("只剩 :f 需要改写");
        assert_eq!(bind.sql, "select ':a', \":b\", `:c`, [':d'] -- :e\n, ? /* :g */");
        assert_eq!(bind.args, vec![SqlValue::Integer(1)]);
    }

    #[test]
    fn escaped_quote_does_not_end_the_literal() {
        // 'it''s :a' 是一个整体字面量，里面的 :a 不是占位符。
        let bind = bind_named_args("select 'it''s :a' , :a", &named(&[("a", SqlValue::Integer(2))]))
            .expect("改写成功");
        assert_eq!(bind.sql, "select 'it''s :a' , ?");
        assert_eq!(bind.args, vec![SqlValue::Integer(2)]);
    }

    #[test]
    fn empty_named_table_passes_sql_through_untouched() {
        // 改写的入口条件就是"客户端给了命名参数"；没给时原样下发，
        // 缺绑定由引擎按"参数数量不匹配"报错（不会静默变 NULL）。
        let bind = bind_named_args("select :a", &[]).expect("空参数表不参与改写");
        assert_eq!(bind.sql, "select :a");
        assert!(bind.args.is_empty());
    }

    #[test]
    fn unbound_and_unused_named_args_are_both_errors() {
        let missing = bind_named_args(
            "select :a, :b",
            &named(&[("a", SqlValue::Integer(1))]),
        )
        .expect_err("SQL 里出现但没给值的参数应当失败");
        assert!(missing.contains("b 没有被赋值"), "{missing}");

        let unused = bind_named_args("select 1", &named(&[("a", SqlValue::Integer(1))]))
            .expect_err("多余参数应当失败");
        assert!(unused.contains("没有对应的占位符"), "{unused}");
    }

    #[test]
    fn mixing_named_and_positional_is_rejected() {
        let err = bind_named_args(
            "select ?, :a",
            &named(&[("a", SqlValue::Integer(1))]),
        )
        .expect_err("混用必须报错");
        assert!(err.contains("位置占位符"), "{err}");
    }

    #[test]
    fn bare_colon_is_not_a_placeholder() {
        let bind = bind_named_args("select 'a' || ':' || :a", &named(&[("a", SqlValue::Null)]))
            .expect("改写成功");
        assert_eq!(bind.sql, "select 'a' || ':' || ?");
    }

    #[test]
    fn statements_are_split_on_code_semicolons_only() {
        let parts = split_statements("create table a(x); select ';' ; -- ;\ncreate table b(y)");
        // 第三段带上了它的行注释：注释里的 `;` 不是分隔符，注释本身属于后一条语句。
        assert_eq!(
            parts,
            vec![
                "create table a(x)".to_string(),
                "select ';'".to_string(),
                "-- ;\ncreate table b(y)".to_string(),
            ]
        );
        assert!(split_statements("   ").is_empty());
        assert_eq!(split_statements("select 1;"), vec!["select 1".to_string()]);
    }

    #[test]
    fn trigger_definitions_are_detected_even_after_comments() {
        assert!(is_trigger_definition("CREATE TRIGGER t AFTER INSERT ON x BEGIN a; b; END"));
        assert!(is_trigger_definition("-- c\n/* c2 */ create temp trigger t BEGIN a; END"));
        assert!(!is_trigger_definition("select 1"));
        assert!(!is_trigger_definition("create table triggers(x)"));
    }

    #[test]
    fn positional_placeholder_detection_ignores_literals() {
        assert!(has_positional_placeholder("select ? from t"));
        assert!(!has_positional_placeholder("select '?' , 1 -- ?"));
    }
}
