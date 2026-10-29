import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { openHttp } from '@libsql/hrana-client';

const [connectionFile] = process.argv.slice(2);
if (!connectionFile) throw new Error('private TLS fixture connection.json required');
const connection = JSON.parse(await readFile(connectionFile, 'utf8'));
if (!connection.endpoint.startsWith('https://127.0.0.1:') || process.env.NODE_EXTRA_CA_CERTS !== connection.ca_file || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('verified fixture TLS required');

const { tenant, database } = connection.database.namespace;
const base = `${connection.endpoint}/db/${tenant}/${database}/${connection.database.branch}`;
const authorization = `Bearer ${connection.project_api_key}`;
const table = `issue21_query_duration_${randomBytes(4).toString('hex')}`;
const typeTable = `${table}_types`;
const select = `SELECT id, payload FROM ${table} ORDER BY id`;
const boundedSelect = `SELECT id, payload FROM ${table} WHERE id <= 1000 ORDER BY id`;
const typeSelect = `SELECT id, i, r, t, b, n FROM ${typeTable}`;
const TYPE_COLUMNS = ['id', 'i', 'r', 't', 'b', 'n'];
const ROWS = 8625;
const BOUNDED_ROWS = 1000;
const PAYLOAD_BYTES = 2342;
const BATCH_ROWS = 20;
const METRICS_WARMUP = 3;
const METRICS_SAMPLES = 10;
const CURSOR_WARMUP = 1;
const CURSOR_SAMPLES = 5;
const redact = (text) => {
  let output = String(text).split(connection.project_api_key).join('<REDACTED>');
  if (connection.endpoint) output = output.split(connection.endpoint).join('<REDACTED>');
  if (connection.ca_file) output = output.split(connection.ca_file).join('<REDACTED>');
  return output;
};
const round = (value) => Math.round(value * 1000) / 1000;

const evidence = {
  synthetic: true,
  formal_pass: false,
  rows: ROWS,
  payload_bytes: ROWS * PAYLOAD_BYTES,
  table: {
    columns: ['id', 'payload'],
    covers: ['integer', 'text'],
    note: 'the main row set covers integer and text only; mixed-type coverage is in types',
  },
  protocol: { requested: 3, negotiated: null, fetch_paths: [] },
  metrics_bounded: {
    path: '/v2/pipeline',
    sql_kind: 'bounded',
    rows: BOUNDED_ROWS,
    want_rows: true,
    note: 'bounded statement within the per-statement result budget; wall_ms - query_duration_ms approximates transfer plus client overhead and does not describe the 20.2 MB scan',
    warmup: METRICS_WARMUP,
    samples: [],
  },
  metrics_full_scan: {
    path: '/v2/pipeline',
    sql_kind: 'full_scan',
    rows: ROWS,
    want_rows: false,
    note: 'probe only: the Worker result budget charges every produced row even when rows are not collected, so an over-budget scan is answered by the read-only cursor fallback and its synthesized StmtResult has no metrics',
  },
  large_result: {
    rows: ROWS,
    protobuf: {
      transport: 'v3-protobuf',
      warmup: CURSOR_WARMUP,
      samples: [],
      note: 'decode_ms is SDK decode completion; the SDK decodes while streaming, so the number includes download. Response.clone() byte counting adds overhead.',
    },
    json: {
      transport: 'v3 JSON',
      warmup: CURSOR_WARMUP,
      samples: [],
      note: 'download_ms stops when the raw body is complete; parse_ms covers local JSON parsing only. Not directly comparable with protobuf decode_ms.',
    },
  },
  pipeline_rejection: {},
  expected_sha256: '',
  hashes: { expected: '', protobuf: '', json: '' },
  hashes_equal: false,
  types: {},
  close_errors: [],
};

function payload(id) {
  const seed = `issue21-${String(id).padStart(5, '0')}-`;
  return seed.repeat(Math.ceil(PAYLOAD_BYTES / seed.length)).slice(0, PAYLOAD_BYTES);
}

function insertStatement(first, last) {
  const rows = [];
  for (let id = first; id <= last; id += 1) rows.push(`(${id},'${payload(id)}')`);
  return `INSERT INTO ${table}(id, payload) VALUES ${rows.join(',')}`;
}

function summarize(values) {
  assert.ok(values.length > 0, 'cannot summarize an empty sample');
  const sorted = [...values].sort((left, right) => left - right);
  const at = (fraction) => sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(fraction * sorted.length) - 1))];
  const middle = Math.floor(sorted.length / 2);
  return {
    count: sorted.length,
    median: round(sorted.length % 2 === 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle]),
    max: round(sorted.at(-1)),
    min: round(sorted[0]),
    ...(sorted.length >= 20 ? { p95: round(at(0.95)) } : {}),
  };
}

function hashCell(hash, value) {
  if (value === null) {
    hash.update('n;');
    return;
  }
  if (typeof value === 'bigint') {
    hash.update('i');
    hash.update(String(value));
    hash.update(';');
    return;
  }
  if (typeof value === 'number') {
    hash.update(Number.isInteger(value) ? 'i' : 'f');
    hash.update(String(value));
    hash.update(';');
    return;
  }
  if (typeof value === 'string') {
    hash.update('t');
    hash.update(String(Buffer.byteLength(value)));
    hash.update(':');
    hash.update(value);
    hash.update(';');
    return;
  }
  if (value instanceof ArrayBuffer) {
    hash.update('b');
    hash.update(String(value.byteLength));
    hash.update(':');
    hash.update(Buffer.from(value));
    hash.update(';');
    return;
  }
  if (ArrayBuffer.isView(value)) {
    const bytes = Buffer.from(value.buffer, value.byteOffset, value.byteLength);
    hash.update('b');
    hash.update(String(bytes.length));
    hash.update(':');
    hash.update(bytes);
    hash.update(';');
    return;
  }
  throw new Error(`unsupported value type ${typeof value}`);
}

function normalizeJSONValue(value) {
  if (value === null || typeof value !== 'object') return value;
  switch (value.type) {
    case 'null':
      return null;
    case 'integer':
      return BigInt(value.value);
    case 'float':
      return Number(value.value);
    case 'text':
      return value.value;
    case 'blob':
      return Buffer.from(value.base64, 'base64');
    default:
      throw new Error(`unsupported JSON value type ${value.type}`);
  }
}

function hashValues(label, values) {
  const hash = createHash('sha256');
  for (const value of values) hashCell(hash, value);
  hash.update('\n');
  return hash.digest('hex');
}

function hashRows(label, rows, cellsAt) {
  const hash = createHash('sha256');
  rows.forEach((row, index) => {
    const [idValue, payloadValue] = cellsAt(row);
    const id = Number(idValue);
    assert.equal(id, index + 1, `${label} id at index ${index}`);
    assert.equal(payloadValue, payload(id), `${label} payload at id ${id}`);
    hashCell(hash, idValue);
    hashCell(hash, payloadValue);
    hash.update('\n');
  });
  return hash.digest('hex');
}

function assertHash(label, actual, expected) {
  assert.equal(actual, expected, `${label} sha256 does not match the written payloads`);
}

function uvarint(value) {
  let rest = BigInt(value);
  const bytes = [];
  do {
    let byte = Number(rest & 0x7fn);
    rest >>= 7n;
    if (rest > 0n) byte |= 0x80;
    bytes.push(byte);
  } while (rest > 0n);
  return Buffer.from(bytes);
}

function protoBytesField(number, value) {
  const bytes = Buffer.from(value);
  return Buffer.concat([uvarint((BigInt(number) << 3n) | 2n), uvarint(bytes.length), bytes]);
}

function protoVarintField(number, value) {
  return Buffer.concat([uvarint((BigInt(number) << 3n) | 0n), uvarint(value)]);
}

function readVarint(buffer, offset) {
  let value = 0n;
  let shift = 0n;
  for (let index = offset; index < buffer.length; index += 1) {
    const byte = buffer[index];
    value |= BigInt(byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) return { value, offset: index + 1 };
    shift += 7n;
    if (shift > 63n) throw new Error('invalid protobuf varint');
  }
  throw new Error('truncated protobuf varint');
}

function readProtoFields(buffer) {
  const fields = [];
  let offset = 0;
  while (offset < buffer.length) {
    const header = readVarint(buffer, offset);
    offset = header.offset;
    const number = Number(header.value >> 3n);
    const wire = Number(header.value & 7n);
    if (wire === 0) {
      const integer = readVarint(buffer, offset);
      offset = integer.offset;
      fields.push({ number, wire, bytes: null, integer: integer.value });
      continue;
    }
    if (wire === 1) {
      if (offset + 8 > buffer.length) throw new Error('short protobuf fixed64');
      fields.push({ number, wire, bytes: buffer.subarray(offset, offset + 8), integer: null });
      offset += 8;
      continue;
    }
    if (wire === 2) {
      const length = readVarint(buffer, offset);
      const end = length.offset + Number(length.value);
      if (end > buffer.length) throw new Error('short protobuf field');
      fields.push({ number, wire, bytes: buffer.subarray(length.offset, end), integer: null });
      offset = end;
      continue;
    }
    throw new Error(`unsupported protobuf wire type ${wire}`);
  }
  return fields;
}

function protobufPipelineResult(buffer) {
  for (const result of readProtoFields(buffer)) {
    if (result.number !== 3 || result.wire !== 2) continue;
    let type = null;
    let code = null;
    for (const streamResult of readProtoFields(result.bytes)) {
      if (streamResult.number === 1 && streamResult.wire === 2) type = 'ok';
      if (streamResult.number === 2 && streamResult.wire === 2) {
        type = 'error';
        for (const error of readProtoFields(streamResult.bytes)) {
          if (error.number === 2 && error.wire === 2) code = error.bytes.toString('utf8');
        }
      }
    }
    return { type, code };
  }
  return { type: null, code: null };
}

function decodeFailureBody(body) {
  const text = body.toString('utf8');
  if (text.trim() !== '') {
    try {
      const parsed = JSON.parse(text);
      const first = Array.isArray(parsed?.results) ? parsed.results[0] : undefined;
      const type = first?.type ?? (parsed?.error ? 'error' : null);
      const code = parsed?.error?.code ?? first?.error?.code ?? null;
      if (type !== null || code !== null) return { encoding: 'json', type, code };
    } catch {
      // The 200 path is protobuf; JSON parsing is only the first attempt.
    }
  }
  if (body.length > 0) {
    try {
      return { encoding: 'protobuf', ...protobufPipelineResult(body) };
    } catch {
      // Plain-text non-200 bodies are expected to fail both decoders.
    }
  }
  return { encoding: 'unknown', type: null, code: null };
}

function singleResponsePipeline() {
  const statement = Buffer.concat([
    protoBytesField(1, Buffer.from(select, 'utf8')),
    protoVarintField(5, 1),
  ]);
  const execute = protoBytesField(2, protoBytesField(1, statement));
  const close = protoBytesField(1, Buffer.alloc(0));
  return Buffer.concat([protoBytesField(2, execute), protoBytesField(2, close)]);
}

let countProtobuf = false;
let protobufBytes = 0;
let cloneErrors = 0;
let pendingBytes = [];

const trackedFetch = async (input, init) => {
  const request = input instanceof Request ? input : new Request(input, init);
  const path = new URL(request.url).pathname;
  evidence.protocol.fetch_paths.push(path);
  const counting = countProtobuf;
  const response = await fetch(request);
  if (counting && path.endsWith('/v3-protobuf/cursor')) {
    // Byte counting clones the response; that extra read is measurement overhead.
    const clone = response.clone();
    pendingBytes.push(clone.arrayBuffer().then((buffer) => {
      protobufBytes += buffer.byteLength;
    }, () => {
      cloneErrors += 1;
    }));
  }
  return response;
};

async function postJSON(path, body) {
  const response = await trackedFetch(`${base}${path}`, {
    method: 'POST',
    headers: { Authorization: authorization, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  if (response.status !== 200) throw new Error(redact(`${path} returned ${response.status}: ${text.slice(0, 200)}`));
  return JSON.parse(text);
}

async function closeBaton(baton) {
  if (typeof baton !== 'string' || baton.length === 0) return;
  try {
    await postJSON('/v2/pipeline', { baton, requests: [{ type: 'close' }] });
  } catch (error) {
    evidence.close_errors.push(redact(error?.message ?? error));
  }
}

let stream;
let hrana;

async function readProtobufCursor(sql, expectedRows, expectedColumns) {
  assert.ok(stream, 'SDK stream is not open');
  countProtobuf = true;
  protobufBytes = 0;
  pendingBytes = [];
  const started = performance.now();
  try {
    const batch = stream.batch(true);
    const selected = batch.step().query(sql);
    // If execute fails before delivering a result, this promise may never settle;
    // the handler keeps a later rejection from becoming an unhandled rejection.
    selected.catch(() => {});
    await batch.execute();
    const result = await selected;
    const decodeMs = performance.now() - started;
    assert.deepEqual(result.columnNames, expectedColumns, `protobuf cursor columns ${JSON.stringify(result.columnNames)}`);
    assert.equal(result.rows.length, expectedRows, `protobuf cursor returned ${result.rows.length} rows`);
    await Promise.all(pendingBytes);
    return { rows: result.rows, decode_ms: decodeMs, bytes: protobufBytes };
  } finally {
    countProtobuf = false;
    await Promise.all(pendingBytes).catch(() => {});
  }
}

async function readJSONCursor(sql, expectedRows, expectedColumns) {
  let baton = null;
  try {
    const started = performance.now();
    const response = await trackedFetch(`${base}/v3/cursor`, {
      method: 'POST',
      headers: { Authorization: authorization, 'Content-Type': 'application/json' },
      body: JSON.stringify({ baton: null, batch: { steps: [{ stmt: { sql, want_rows: true } }] } }),
    });
    const body = Buffer.from(await response.arrayBuffer());
    const downloadMs = performance.now() - started;
    if (response.status !== 200) throw new Error(redact(`/v3/cursor returned ${response.status}: ${body.toString('utf8').slice(0, 200)}`));
    const parseStarted = performance.now();
    const lines = body.toString('utf8').split('\n').filter((line) => line.length > 0);
    assert.ok(lines.length > 1, 'JSON cursor response has no entries');
    const header = JSON.parse(lines[0]);
    baton = header.baton;
    assert.equal(typeof baton, 'string', 'JSON cursor response has no baton');
    assert.match(response.headers.get('content-type') ?? '', /^application\/x-ndjson/, 'JSON cursor Content-Type');
    let columns = null;
    const rows = [];
    for (const line of lines.slice(1)) {
      const entry = JSON.parse(line);
      if (entry.type === 'step_begin') {
        columns = entry.cols.map((col) => col.name);
        assert.deepEqual(columns, expectedColumns, `JSON cursor columns ${JSON.stringify(columns)}`);
        continue;
      }
      if (entry.type === 'step_error' || entry.type === 'error') {
        throw new Error(`JSON cursor error: ${entry.error?.code ?? 'unknown'}`);
      }
      if (entry.type !== 'row') continue;
      assert.equal(entry.row.length, expectedColumns.length, 'JSON cursor row shape changed');
      rows.push(entry.row.map(normalizeJSONValue));
    }
    const parseMs = performance.now() - parseStarted;
    assert.ok(columns, 'JSON cursor response has no step_begin');
    assert.equal(rows.length, expectedRows, `JSON cursor returned ${rows.length} rows`);
    return { rows, download_ms: downloadMs, parse_ms: parseMs, bytes: body.length, baton };
  } finally {
    await closeBaton(baton);
  }
}

let stage = 'fixture validation';
assert.equal(payload(1).length, PAYLOAD_BYTES);
assert.equal(payload(ROWS).length, PAYLOAD_BYTES);
const expectedSha256 = (() => {
  const hash = createHash('sha256');
  for (let id = 1; id <= ROWS; id += 1) {
    hashCell(hash, BigInt(id));
    hashCell(hash, payload(id));
    hash.update('\n');
  }
  return hash.digest('hex');
})();
evidence.expected_sha256 = expectedSha256;
evidence.hashes.expected = expectedSha256;
let metricsBaton = null;

try {
  stage = 'negotiate v3';
  hrana = openHttp(`${base}/`, connection.project_api_key, trackedFetch, 3);
  hrana.intMode = 'bigint';
  const version = await hrana.getVersion();
  evidence.protocol.negotiated = version;
  assert.equal(version, 3, `Hrana negotiated version ${version}, expected 3`);

  stage = 'create and populate';
  stream = hrana.openStream();
  await stream.run(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY, payload TEXT)`);
  for (let first = 1; first <= ROWS; first += BATCH_ROWS) {
    const statement = insertStatement(first, Math.min(first + BATCH_ROWS - 1, ROWS));
    assert.ok(Buffer.byteLength(statement, 'utf8') < 65536, `INSERT statement is ${Buffer.byteLength(statement, 'utf8')} bytes`);
    await stream.run(statement);
  }
  await stream.run(`CREATE TABLE ${typeTable}(id INTEGER PRIMARY KEY, i INTEGER, r REAL, t TEXT, b BLOB, n TEXT)`);
  await stream.run(`INSERT INTO ${typeTable}(id, i, r, t, b, n) VALUES (1, 42, 1.5, 'hello', X'0001FF', NULL)`);

  stage = 'bounded statement metrics (want_rows:true)';
  const sampleBoundedMetrics = async (label) => {
    const started = performance.now();
    const response = await postJSON('/v2/pipeline', {
      baton: metricsBaton,
      requests: [{ type: 'execute', stmt: { sql: boundedSelect, want_rows: true } }],
    });
    const wallMs = performance.now() - started;
    if (typeof response.baton === 'string') metricsBaton = response.baton;
    const result = response.results?.[0]?.response?.result;
    assert.ok(result && typeof result === 'object', `${label} JSON pipeline must return an execute result`);
    for (const key of ['rows_read', 'rows_written', 'query_duration_ms']) assert.ok(key in result, `${label} execute result missing ${key}`);
    const rowsRead = Number(result.rows_read);
    const rowsWritten = Number(result.rows_written);
    const durationMs = Number(result.query_duration_ms);
    assert.ok(Number.isFinite(rowsRead) && rowsRead > 0, `${label} rows_read ${result.rows_read} must be finite and > 0`);
    assert.equal(rowsWritten, 0, `${label} SELECT must report no written rows`);
    assert.ok(Number.isFinite(durationMs) && durationMs > 0, `${label} query_duration_ms ${result.query_duration_ms} must be finite and > 0`);
    assert.equal(result.rows?.length, BOUNDED_ROWS, `${label} materialized ${result.rows?.length} rows, expected ${BOUNDED_ROWS}`);
    return {
      rows_read: rowsRead,
      rows_written: rowsWritten,
      query_duration_ms: durationMs,
      wall_ms: round(wallMs),
      transfer_ms: round(Math.max(0, wallMs - durationMs)),
    };
  };

  try {
    for (let index = 0; index < METRICS_WARMUP; index += 1) await sampleBoundedMetrics('bounded metrics');
    for (let index = 0; index < METRICS_SAMPLES; index += 1) evidence.metrics_bounded.samples.push(await sampleBoundedMetrics('bounded metrics'));
    for (const field of ['rows_read', 'rows_written', 'query_duration_ms', 'wall_ms', 'transfer_ms']) {
      evidence.metrics_bounded[field] = summarize(evidence.metrics_bounded.samples.map((sample) => sample[field]));
    }

    stage = 'full-scan metrics probe (want_rows:false)';
    const probeStarted = performance.now();
    const probeResponse = await postJSON('/v2/pipeline', {
      baton: metricsBaton,
      requests: [{ type: 'execute', stmt: { sql: select, want_rows: false } }],
    });
    const probeWallMs = performance.now() - probeStarted;
    if (typeof probeResponse.baton === 'string') metricsBaton = probeResponse.baton;
    const probeResult = probeResponse.results?.[0]?.response?.result;
    assert.ok(probeResult && typeof probeResult === 'object', 'full-scan probe must return an execute result');
    evidence.metrics_full_scan.result_keys = Object.keys(probeResult).sort();
    evidence.metrics_full_scan.wall_ms = round(probeWallMs);
    const probeMetrics = ['rows_read', 'rows_written', 'query_duration_ms'].every((key) => key in probeResult);
    evidence.metrics_full_scan.metrics_available = probeMetrics;
    if (probeMetrics) {
      evidence.metrics_full_scan.mode = 'statement';
      evidence.metrics_full_scan.rows_read = Number(probeResult.rows_read);
      evidence.metrics_full_scan.rows_written = Number(probeResult.rows_written);
      evidence.metrics_full_scan.query_duration_ms = Number(probeResult.query_duration_ms);
    } else {
      evidence.metrics_full_scan.mode = 'cursor_fallback';
      evidence.metrics_full_scan.reason = 'worker result budget charged produced rows';
    }
  } finally {
    await closeBaton(metricsBaton);
    metricsBaton = null;
  }

  stage = 'large-result protobuf cursor (v3-protobuf)';
  for (let index = 0; index < CURSOR_WARMUP; index += 1) await readProtobufCursor(select, ROWS, ['id', 'payload']);
  for (let index = 0; index < CURSOR_SAMPLES; index += 1) {
    const sample = await readProtobufCursor(select, ROWS, ['id', 'payload']);
    const sampleHash = hashRows('protobuf cursor', sample.rows, (row) => [row.id, row.payload]);
    assertHash('protobuf cursor', sampleHash, expectedSha256);
    evidence.hashes.protobuf = sampleHash;
    evidence.large_result.protobuf.samples.push(round(sample.decode_ms));
    evidence.large_result.protobuf.bytes = sample.bytes;
    evidence.large_result.protobuf.rows = sample.rows.length;
    evidence.large_result.protobuf.hash = sampleHash;
  }
  assert.ok(
    evidence.protocol.fetch_paths.some((path) => path.endsWith('/v3-protobuf/cursor')),
    `protobuf cursor did not use /v3-protobuf/cursor; observed paths ${JSON.stringify(evidence.protocol.fetch_paths)}`,
  );
  evidence.large_result.protobuf.decode_ms = summarize(evidence.large_result.protobuf.samples);

  stage = 'large-result JSON cursor (/v3/cursor)';
  for (let index = 0; index < CURSOR_WARMUP; index += 1) await readJSONCursor(select, ROWS, ['id', 'payload']);
  for (let index = 0; index < CURSOR_SAMPLES; index += 1) {
    const sample = await readJSONCursor(select, ROWS, ['id', 'payload']);
    const sampleHash = hashRows('JSON cursor', sample.rows, (row) => row);
    assertHash('JSON cursor', sampleHash, expectedSha256);
    evidence.hashes.json = sampleHash;
    evidence.large_result.json.samples.push(round(sample.download_ms));
    evidence.large_result.json.parse_ms_samples = evidence.large_result.json.parse_ms_samples ?? [];
    evidence.large_result.json.parse_ms_samples.push(round(sample.parse_ms));
    evidence.large_result.json.bytes = sample.bytes;
    evidence.large_result.json.rows = sample.rows.length;
    evidence.large_result.json.hash = sampleHash;
  }
  evidence.large_result.json.download_ms = summarize(evidence.large_result.json.samples);
  evidence.large_result.json.parse_ms = summarize(evidence.large_result.json.parse_ms_samples);

  assert.equal(evidence.hashes.protobuf, expectedSha256, 'protobuf cursor hash differs from the written payloads');
  assert.equal(evidence.hashes.json, expectedSha256, 'JSON cursor hash differs from the written payloads');
  evidence.hashes_equal = evidence.hashes.protobuf === evidence.hashes.json && evidence.hashes.json === expectedSha256;
  assert.ok(evidence.hashes_equal, 'protobuf, JSON and written-content hashes differ');

  stage = 'mixed-type normalization';
  const protobufTypes = await readProtobufCursor(typeSelect, 1, TYPE_COLUMNS);
  const jsonTypes = await readJSONCursor(typeSelect, 1, TYPE_COLUMNS);
  const expectedTypeHash = hashValues('expected', [1n, 42n, 1.5, 'hello', Buffer.from([0, 1, 255]), null]);
  const protobufTypeRow = protobufTypes.rows[0];
  const protobufTypeHash = hashValues('protobuf', [protobufTypeRow.id, protobufTypeRow.i, protobufTypeRow.r, protobufTypeRow.t, protobufTypeRow.b, protobufTypeRow.n]);
  const jsonTypeHash = hashValues('JSON', jsonTypes.rows[0]);
  assert.equal(protobufTypeHash, expectedTypeHash, 'protobuf mixed-type row normalization differs');
  assert.equal(jsonTypeHash, expectedTypeHash, 'JSON mixed-type row normalization differs');
  evidence.types = {
    columns: TYPE_COLUMNS,
    expected_sha256: expectedTypeHash,
    protobuf_sha256: protobufTypeHash,
    json_sha256: jsonTypeHash,
    equal: true,
  };
  evidence.large_result.protobuf.clone_errors = cloneErrors;

  stage = 'pipeline result limit rejection';
  const limitResponse = await trackedFetch(`${base}/v3-protobuf/pipeline`, {
    method: 'POST',
    headers: { Authorization: authorization, 'Content-Type': 'application/x-protobuf' },
    body: singleResponsePipeline(),
  });
  const limitBody = Buffer.from(await limitResponse.arrayBuffer());
  const limit = decodeFailureBody(limitBody);
  evidence.pipeline_rejection = {
    path: '/v3-protobuf/pipeline',
    status: limitResponse.status,
    encoding: limit.encoding,
    result_type: limit.type,
    code: limit.code,
  };
  if (limitResponse.status === 200) {
    assert.equal(limit.type, 'error', `single response status 200 but results[0].type=${limit.type}`);
    assert.equal(limit.code, 'result_limit', `single response status 200 but code=${limit.code}`);
  } else if (limitResponse.status === 413) {
    assert.ok(limit.code === null || limit.code === 'pipeline_result_limit' || limit.code === 'result_limit', `single response status 413 but code=${limit.code}`);
  } else {
    assert.fail(`single response returned ${limitResponse.status}, expected 200 result_limit or 413 pipeline_result_limit`);
  }
  evidence.pipeline_rejection.accepted = true;

  console.log(JSON.stringify(evidence));
} catch (error) {
  console.error(`query-duration-live FAIL stage=${stage}: ${redact(error?.stack ?? error)}`);
  console.error(`query-duration-live evidence: ${redact(JSON.stringify(evidence))}`);
  process.exitCode = 1;
} finally {
  await closeBaton(metricsBaton);
  try {
    await stream?.run(`DROP TABLE IF EXISTS ${typeTable}`);
  } catch {
    // Cleanup is best effort and must not hide the stage result.
  }
  try {
    await stream?.run(`DROP TABLE IF EXISTS ${table}`);
  } catch {
    // Cleanup is best effort and must not hide the stage result.
  }
  try {
    stream?.closeGracefully();
  } catch {
    // The SDK stream may already be closed after a failed stage.
  }
  try {
    hrana?.close();
  } catch {
    // Closing an already-closed client is not evidence of failure.
  }
}
