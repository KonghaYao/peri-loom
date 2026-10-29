import assert from 'node:assert/strict';
import { mkdirSync, writeFileSync } from 'node:fs';
import { parseArgs } from 'node:util';
import { resultHash, hash } from './evidence.mjs';

const { values } = parseArgs({ options: { out: { type: 'string' }, rows: { type: 'string', default: '8625' },
  'content-bytes': { type: 'string', default: '2048' } } });
assert.ok(values.out, '--out is required');
const count = Number(values.rows), width = Number(values['content-bytes']);
assert.ok(Number.isSafeInteger(count) && count >= 20 && count <= 100000);
assert.ok(Number.isSafeInteger(width) && width >= 32 && width <= 16384);
mkdirSync(values.out, { recursive: false, mode: 0o700 });
const rows = Array.from({ length: count }, (_, index) => {
  const id = index + 1, prefix = `row:${id}:`;
  return { id, value: `value-${String(id).padStart(8, '0')}`, content: prefix + 'x'.repeat(width - prefix.length) };
});
const commands = [{ sql: 'CREATE TABLE diag_read_r7 (id INTEGER PRIMARY KEY, value TEXT NOT NULL, content TEXT NOT NULL)', args: [] }];
for (let start = 0; start < rows.length; start += 100) {
  const batch = rows.slice(start, start + 100);
  commands.push({ sql: 'INSERT INTO diag_read_r7 (id,value,content) VALUES ' + batch.map(() => '(?,?,?)').join(','),
    args: batch.flatMap(row => [row.id, row.value, row.content]) });
}
const narrow = rows.map(({ id, value }) => ({ id, value }));
const definitions = [
  ['proxy_empty', 'SELECT id,value FROM diag_read_r7 WHERE id = 0', [], []],
  ['proxy_one', 'SELECT id,value FROM diag_read_r7 WHERE id = ?', [1], narrow.slice(0, 1)],
  ['proxy_twenty', 'SELECT id,value FROM diag_read_r7 WHERE id <= 20 ORDER BY id', [], narrow.slice(0, 20)],
  ['proxy_narrow', 'SELECT id,value FROM diag_read_r7 ORDER BY id', [], narrow],
  ['proxy_text', 'SELECT id,content FROM diag_read_r7 ORDER BY id', [], rows.map(({ id, content }) => ({ id, content }))],
];
const setup = commands.map(command => JSON.stringify(command)).join('\n') + '\n';
const manifest = { schema: 'sqld-read-diagnostic/1', dataset_origin: 'synthetic-proxy',
  dataset_ref: `diag_read_r7-${count}x${width}-ascii`, dataset_sha256: resultHash(rows),
  description: 'Deterministic synthetic fixture. Not the issue15 threads database or its six real SQL queries.',
  rows: count, content_bytes_per_row: width, setup_sha256: hash(setup),
  scenarios: definitions.map(([name, sql, args, result]) => ({ name, sql, args,
    expected_rows: result.length, expected_result_sha256: resultHash(result) })) };
writeFileSync(values.out + '/setup.jsonl', setup, { mode: 0o600, flag: 'wx' });
writeFileSync(values.out + '/manifest.json', JSON.stringify(manifest, null, 2) + '\n', { mode: 0o600, flag: 'wx' });
console.log(JSON.stringify({ dataset_ref: manifest.dataset_ref, rows: count, setup_commands: commands.length,
  dataset_sha256: manifest.dataset_sha256 }));
