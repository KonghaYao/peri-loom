import test from 'node:test';
import assert from 'node:assert/strict';
import { formatPreviewCell, isIsolatedSchema, isPendingSchema, parseSchemaTables, parseTableColumns, previewLimit, quoteSQLiteIdentifier, schemaRetryDelay, tableColumnsSQL, tablePreviewSQL } from '../frontend/src/features/table-browser.ts';

test('schema isolation is a valid operation response, not a malformed table catalog', () => {
  assert.equal(isIsolatedSchema({ status: 'isolated', operation: { action: 'schema', business_outcome: 'not_proven', cleanup_outcome: 'pending' } }), true);
  assert.equal(isIsolatedSchema([]), false);
  assert.equal(isIsolatedSchema({ status: 'isolated', operation: null }), false);
  assert.equal(isIsolatedSchema({ rows: [] }), false);
  assert.equal(isPendingSchema({ status: 'isolated', operation: { state: 'in_flight', action: 'schema' } }), true);
  assert.equal(isPendingSchema({ status: 'isolated', operation: { state: 'quarantined' } }), false);
  assert.throws(() => parseSchemaTables({ rows: [] }), /row array/);
});

test('schema retries only definitive incomplete reads with a finite backoff', () => {
  const incomplete = Object.assign(new Error('incomplete'), { code: 'operation_incomplete', uncertain: false });
  assert.deepEqual([0, 1, 2, 3].map(attempt => schemaRetryDelay(incomplete, attempt)), [400, 800, 1600, null]);
  assert.equal(schemaRetryDelay(Object.assign(new Error('unknown'), { code: 'outcome_unknown', uncertain: true }), 0), null);
  assert.equal(schemaRetryDelay(Object.assign(new Error('isolated'), { code: 'database_isolated', uncertain: false }), 0), null);
  assert.equal(schemaRetryDelay({ code: 'operation_incomplete', uncertain: false }, 0), null);
  assert.equal(schemaRetryDelay(new Error('Schema response is not a row array'), 0), null);
});

test('table catalog includes user tables but excludes SQLite internal tables and views', () => {
  const tables = parseSchemaTables([
    ['table', 'Zoo', 'Zoo', 'CREATE TABLE Zoo (id INTEGER)'],
    ['table', 'sqlite_sequence', 'sqlite_sequence', 'CREATE TABLE sqlite_sequence'],
    ['view', 'VisibleView', 'VisibleView', 'CREATE VIEW VisibleView AS SELECT 1'],
    ['index', 'idx', 'Zoo', 'CREATE INDEX idx ON Zoo (id)'],
    ['table', 'A table', 'A table', 'CREATE TABLE "A table" (id INTEGER)'],
  ]);
  assert.deepEqual(tables.map(table => table.name), ['A table', 'Zoo']);
});

test('table names are quoted as identifiers and cannot inject statements', () => {
  const malicious = 'customers"; DROP TABLE customers; --';
  assert.equal(quoteSQLiteIdentifier(malicious), '"customers""; DROP TABLE customers; --"');
  assert.equal(tablePreviewSQL(malicious), `SELECT * FROM "customers""; DROP TABLE customers; --" LIMIT ${previewLimit}`);
  assert.equal(tableColumnsSQL(malicious), 'PRAGMA table_info("customers""; DROP TABLE customers; --")');
  assert.throws(() => quoteSQLiteIdentifier('foo\0bar'), /Invalid SQLite/);
  assert.throws(() => quoteSQLiteIdentifier(''), /Invalid SQLite/);
});

test('schema and columns reject malformed responses instead of displaying partial data', () => {
  assert.throws(() => parseSchemaTables({ rows: [] }), /row array/);
  assert.throws(() => parseSchemaTables([['table', 123, 'other', null]]), /invalid row/);
  assert.throws(() => parseSchemaTables([['table', 'users', 'users', 5]]), /invalid SQL/);
  assert.deepEqual(parseTableColumns([[0, 'id', 'INTEGER'], [1, 'name', 'TEXT']]), ['id', 'name']);
  assert.throws(() => parseTableColumns([[0, null]]), /invalid row/);
});

test('preview cells preserve exact integers and distinguish null from empty string', () => {
  assert.equal(formatPreviewCell(9223372036854775807n), '9223372036854775807');
  assert.equal(formatPreviewCell(null), 'NULL');
  assert.equal(formatPreviewCell(''), '');
  assert.equal(formatPreviewCell({ id: 9223372036854775807n }), '{"id":"9223372036854775807"}');
});
