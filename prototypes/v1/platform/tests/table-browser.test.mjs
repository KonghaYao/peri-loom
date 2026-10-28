import test from 'node:test';
import assert from 'node:assert/strict';
import { formatPreviewCell, isIsolatedSchema, isPendingSchema, parseSchemaTables, parseTableColumns, previewLimit, quoteSQLiteIdentifier, schemaFailureMessage, schemaRetryBackoff, schemaRetryDelay, tableColumnsSQL, tablePreviewSQL } from '../frontend/src/features/table-browser.ts';

test('schema isolation is a valid operation response, not a malformed table catalog', () => {
  assert.equal(isIsolatedSchema({ status: 'isolated', operation: { action: 'schema', business_outcome: 'not_proven', cleanup_outcome: 'pending' } }), true);
  assert.equal(isIsolatedSchema([]), false);
  assert.equal(isIsolatedSchema({ status: 'isolated', operation: null }), false);
  assert.equal(isIsolatedSchema({ rows: [] }), false);
  assert.equal(isPendingSchema({ status: 'isolated', operation: { state: 'in_flight', action: 'schema' } }), true);
  assert.equal(isPendingSchema({ status: 'isolated', operation: { state: 'quarantined' } }), false);
  assert.throws(() => parseSchemaTables({ rows: [] }), /row array/);
});

test('schema retries only definitive incomplete reads with a bounded backoff', () => {
  const incomplete = Object.assign(new Error('incomplete'), { code: 'operation_incomplete', uncertain: false });
  assert.deepEqual([0, 1, 2, 3, 4, 5, 6, 7].map(attempt => schemaRetryDelay(incomplete, attempt)), [400, 800, 1600, 3200, 5000, 5000, null, null]);
  assert.deepEqual([0, 5, 6].map(schemaRetryBackoff), [400, 5000, null]);
  for (const code of ['outcome_unknown', 'session_moved', 'database_isolated', 'audit_unavailable']) {
    assert.equal(schemaRetryDelay(Object.assign(new Error(code), { code, uncertain: code === 'outcome_unknown' }), 0), null, code);
  }
  assert.equal(schemaRetryDelay(Object.assign(new Error('unknown'), { code: 'outcome_unknown', uncertain: true }), 0), null);
  assert.equal(schemaRetryDelay(Object.assign(new Error('incomplete'), { code: 'operation_incomplete', uncertain: true }), 0), null);
  assert.equal(schemaRetryDelay({ code: 'operation_incomplete', uncertain: false }, 0), null);
  assert.equal(schemaRetryDelay(new Error('Schema response is not a row array'), 0), null);
});

test('schema failure text separates suspension, exhausted retries, and uncertain outcomes', () => {
  const incomplete = Object.assign(new Error('incomplete'), { code: 'operation_incomplete', uncertain: false, requestID: 'req-9' });
  assert.match(schemaFailureMessage(incomplete, 'suspended'), /该数据库已暂停，请恢复后重试/);
  assert.match(schemaFailureMessage(incomplete, 'active'), /在 SQL 页执行任意查询唤醒数据库/);
  assert.match(schemaFailureMessage(incomplete, 'active'), /operation_incomplete/);
  const uncertain = Object.assign(new Error('unknown'), { code: 'outcome_unknown', uncertain: true });
  assert.match(schemaFailureMessage(uncertain, 'active'), /结果待确认，无法自动恢复/);
  assert.match(schemaFailureMessage(new Error('Schema response is not a row array'), 'active'), /不是有效行列表/);
  assert.equal(schemaFailureMessage(new Error('boom'), 'active'), 'boom');
});

test('uncertain schema outcomes take precedence over suspension', () => {
  const uncertain = Object.assign(new Error('unknown'), { code: 'outcome_unknown', uncertain: true });
  assert.match(schemaFailureMessage(uncertain, 'suspended'), /结果待确认.*请勿重试/);
});

test('suspended databases never schedule schema retries', () => {
  const incomplete = Object.assign(new Error('incomplete'), { code: 'operation_incomplete', uncertain: false });
  const pending = { status: 'isolated', operation: { state: 'in_flight', action: 'schema' } };
  for (const attempt of [0, 42]) {
    assert.equal(schemaRetryDelay(incomplete, attempt, 'suspended'), null);
    assert.equal(schemaRetryDelay(pending, attempt, 'suspended'), null);
  }
  assert.equal(schemaRetryDelay(incomplete, 0, 'active'), 400);
  assert.equal(schemaRetryDelay(pending, 0, 'active'), 400);
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
