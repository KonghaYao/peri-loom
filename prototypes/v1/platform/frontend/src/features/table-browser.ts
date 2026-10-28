export type SchemaTable = { name: string; sql: string | null };

export function isIsolatedSchema(result: unknown): boolean {
  if (result === null || typeof result !== 'object' || Array.isArray(result)) return false;
  const response = result as Record<string, unknown>;
  return response.status === 'isolated' && response.operation !== null && typeof response.operation === 'object' && !Array.isArray(response.operation);
}

export function isPendingSchema(result: unknown): boolean {
  return isIsolatedSchema(result) && (result as { operation: { state?: unknown } }).operation.state === 'in_flight';
}

export const previewLimit = 25;

const schemaRetrySchedule = [400, 800, 1600, 3200, 5000, 5000];

export function schemaRetryBackoff(attempt: number): number | null {
  return schemaRetrySchedule[attempt] ?? null;
}

export function schemaRetryDelay(failure: unknown, attempt: number, databaseState?: string): number | null {
  if (databaseState === 'suspended') return null;
  // Pending isolation responses are definitive "still running" signals and share the same bounded backoff.
  if (isPendingSchema(failure)) return schemaRetryBackoff(attempt);
  const response = failure as { code?: unknown; uncertain?: unknown } | null;
  return failure instanceof Error && response?.code === 'operation_incomplete' && response.uncertain === false
    ? schemaRetryBackoff(attempt)
    : null;
}

export function schemaFailureMessage(failure: unknown, databaseState?: string): string {
  const response = failure as { code?: unknown; uncertain?: unknown; requestID?: unknown; message?: unknown } | null;
  if (response?.uncertain === true) return '结果待确认，无法自动恢复。请勿重试；请核对服务端状态。';
  if (databaseState === 'suspended') return '该数据库已暂停，请恢复后重试。';
  const support = typeof response?.code === 'string'
    ? `（${response.code}${typeof response.requestID === 'string' && response.requestID ? ` · request ${response.requestID}` : ''}）`
    : '';
  if (response?.code === 'operation_incomplete') {
    return `数据库暂时不可读，自动重试未成功。请在 SQL 页执行任意查询唤醒数据库，或查看分支状态${support}。`;
  }
  if (typeof response?.message === 'string' && response.message.startsWith('Schema response')) {
    return '网关返回的表结构不是有效行列表。请检查当前网关与数据库状态。';
  }
  return failure instanceof Error ? failure.message : String(failure);
}

export function quoteSQLiteIdentifier(name: string): string {
  if (!name || name.includes('\0')) throw new Error('Invalid SQLite table name');
  return `"${name.replaceAll('"', '""')}"`;
}

export function tablePreviewSQL(name: string): string {
  return `SELECT * FROM ${quoteSQLiteIdentifier(name)} LIMIT ${previewLimit}`;
}

export function tableColumnsSQL(name: string): string {
  return `PRAGMA table_info(${quoteSQLiteIdentifier(name)})`;
}

export function parseSchemaTables(result: unknown): SchemaTable[] {
  if (!Array.isArray(result)) throw new Error('Schema response is not a row array');
  const tables = new Map<string, SchemaTable>();
  for (const row of result) {
    if (!Array.isArray(row) || row.length < 4 || typeof row[0] !== 'string' || typeof row[1] !== 'string') {
      throw new Error('Schema response contains an invalid row');
    }
    if (row[0] === 'table' && !row[1].toLowerCase().startsWith('sqlite_')) {
      quoteSQLiteIdentifier(row[1]);
      if (row[3] !== null && typeof row[3] !== 'string') throw new Error('Schema response contains invalid SQL');
      tables.set(row[1], { name: row[1], sql: row[3] });
    }
  }
  return [...tables.values()].sort((left, right) => left.name.localeCompare(right.name));
}

export function parseTableColumns(result: unknown): string[] {
  if (!Array.isArray(result)) throw new Error('Table column response is not a row array');
  return result.map(row => {
    if (!Array.isArray(row) || typeof row[1] !== 'string') throw new Error('Table column response contains an invalid row');
    return row[1];
  });
}

export function formatPreviewCell(value: unknown): string {
  if (value === null) return 'NULL';
  if (typeof value === 'string') return value;
  if (typeof value === 'object') return JSON.stringify(value, (_, item: unknown) => typeof item === 'bigint' ? item.toString() : item);
  return String(value);
}
