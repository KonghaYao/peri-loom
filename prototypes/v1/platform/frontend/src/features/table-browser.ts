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

export function schemaRetryDelay(failure: unknown, attempt: number): number | null {
  const response = failure as { code?: unknown; uncertain?: unknown } | null;
  return failure instanceof Error && response?.code === 'operation_incomplete' && response.uncertain === false
    ? [400, 800, 1600][attempt] ?? null
    : null;
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
