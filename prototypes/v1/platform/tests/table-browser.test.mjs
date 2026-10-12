import test from 'node:test';
import assert from 'node:assert/strict';
import { formatPreviewCell, parseSchemaTables, parseTableColumns, previewLimit, quoteSQLiteIdentifier, tableColumnsSQL, tablePreviewSQL } from '../frontend/src/features/table-browser.ts';

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
