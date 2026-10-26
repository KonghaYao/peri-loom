import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';
import { parseArgs } from 'node:util';
import { fileURLToPath } from 'node:url';
import { hash } from './observe.mjs';

export const root = fileURLToPath(new URL('../', import.meta.url));
export function configuration(argv = process.argv.slice(2)) {
  const { values } = parseArgs({ args: argv, options: {
    connection: { type: 'string' }, url: { type: 'string' }, 'token-env': { type: 'string', default: 'PERI_SQL_TOKEN' },
    scenario: { type: 'string', default: 'H4' }, samples: { type: 'string', default: '2000' },
    warmup: { type: 'string', default: '200' }, 'fixture-rows': { type: 'string', default: '1000' },
    out: { type: 'string' }, arm: { type: 'string', default: 'A' }, 'source-sha': { type: 'string' },
    setup: { type: 'boolean', default: false }, 'diagnostic-plaintext': { type: 'boolean', default: false },
    help: { type: 'boolean', default: false },
  }});
  if (values.help) return { help: true };
  const connection = values.connection ? JSON.parse(readFileSync(values.connection, 'utf8')) : null;
  let url = values.url;
  if (!url && connection) {
    const { tenant, database } = connection.database.namespace;
    url = `${connection.endpoint}/db/${tenant}/${database}/${connection.database.branch}`;
  }
  assert.ok(url && values.out, '--out and --connection or --url are required');
  url = url.replace(/\/$/, '');
  const parsed = new URL(url);
  assert.ok(!parsed.username && !parsed.password && !parsed.search && !parsed.hash, 'unsafe URL');
  assert.ok(/^\/db\/[^/]+\/[^/]+\/[^/]+$/.test(parsed.pathname), 'expected /db/tenant/database/branch URL');
  assert.ok(parsed.protocol === 'https:' || (values['diagnostic-plaintext'] && parsed.protocol === 'http:'
    && ['localhost', '127.0.0.1', '[::1]'].includes(parsed.hostname)), 'verified HTTPS required');
  assert.notEqual(process.env.NODE_TLS_REJECT_UNAUTHORIZED, '0', 'TLS verification cannot be disabled');
  if (connection?.ca_file && parsed.protocol === 'https:') {
    assert.equal(process.env.NODE_EXTRA_CA_CERTS, connection.ca_file,
      'set NODE_EXTRA_CA_CERTS to fixture ca_file before starting Node');
  }
  const authToken = connection?.project_api_key || process.env[values['token-env']];
  assert.ok(authToken && !/[\r\n]/.test(authToken), 'credential missing/invalid');
  assert.ok(['H1', 'H3', 'H4', 'H6'].includes(values.scenario), 'unsupported scenario');
  assert.ok(['A', 'B'].includes(values.arm), 'arm must be A or B');
  for (const key of ['samples', 'warmup', 'fixture-rows']) {
    values[key] = Number(values[key]);
    assert.ok(Number.isSafeInteger(values[key]) && values[key] >= (key === 'warmup' ? 0 : 1), 'invalid ' + key);
  }
  assert.ok([1000, 100000].includes(values['fixture-rows']), 'fixture rows must be 1000 or 100000');
  assert.ok(!existsSync(values.out), 'evidence output already exists; never overwrite a run');
  return { ...values, url, authToken, protocol: 'v3/cursor + v3/pipeline close',
    sdk: '@tursodatabase/serverless@1.4.1',
    lock_sha256: hash(readFileSync(new URL('./package-lock.json', import.meta.url))) };
}

export function sql(name) { return readFileSync(root + 'sql/' + name, 'utf8').trim().replace(/;$/, ''); }
export function payload(id, insert = false) {
  const prefix = (insert ? 'i' : 'r') + String(id).padStart(8, '0') + ':';
  return prefix + '0'.repeat(insert ? 256 - prefix.length : 246);
}
export function statistics(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const percentile = p => sorted.length ? sorted[Math.max(0, Math.ceil(sorted.length * p) - 1)] : null;
  return { count: sorted.length, p50_ms: percentile(.5), p95_ms: percentile(.95),
    p99_ms: percentile(.99), max_ms: sorted.at(-1) ?? null,
    over_100ms: sorted.filter(value => value > 100).length };
}
