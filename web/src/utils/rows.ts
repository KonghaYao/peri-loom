/**
 * 结果集归一化：把后端返回的行（列顺序数组）转换为表格可直接使用的数据。
 * 列描述来自 ColumnView（对象），这里只取 name。
 */
import { columnNames, type ColumnView, type RowData } from '../api/types';

export interface NormalizedResult {
  columns: string[];
  /** 每行统一为「按 columns 顺序的单元格数组」 */
  rows: unknown[][];
  /** 对象行保留原始映射，便于详情查看 */
  objectRows: Array<Record<string, unknown>>;
}

/** 单元格转字符串（用于过滤 / 展示兜底） */
export function cellToString(value: unknown): string {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') return String(value);
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

/** null 用弱化样式展示 */
export function isNullCell(value: unknown): boolean {
  return value === null || value === undefined;
}

export function normalizeRows(
  columns: Array<ColumnView | string>,
  rows: RowData[] | Array<Record<string, unknown>>,
): NormalizedResult {
  const names = columnNames(columns);
  const objectRows: Array<Record<string, unknown>> = [];
  if (rows.length === 0) return { columns: names, rows: [], objectRows };

  const isObjectRow = !Array.isArray(rows[0]);
  if (isObjectRow) {
    const cols = names.length
      ? names
      : Array.from(
          (rows as Array<Record<string, unknown>>).reduce<Set<string>>((acc, row) => {
            Object.keys(row).forEach((k) => acc.add(k));
            return acc;
          }, new Set<string>()),
        );
    const mapped = (rows as Array<Record<string, unknown>>).map((row) => cols.map((c) => row[c]));
    return { columns: cols, rows: mapped, objectRows: rows as Array<Record<string, unknown>> };
  }

  const mapped = rows as unknown[][];
  const cols = names.length
    ? names
    : Array.from({ length: Math.max(...mapped.map((r) => r.length), 0) }, (_, i) => `col_${i + 1}`);
  return {
    columns: cols,
    rows: mapped,
    objectRows: mapped.map((row) => Object.fromEntries(cols.map((c, i) => [c, row[i]]))),
  };
}
