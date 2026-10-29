import assert from 'node:assert/strict';
import { randomUUID, createHash } from 'node:crypto';
import { openSync, closeSync, writeFileSync, fsyncSync, renameSync, readFileSync, existsSync, rmSync } from 'node:fs';

export const storagePolicy = 'append only independent .partial.jsonl diagnostics; final filenames created once from original in-memory observations via temporary file + fsync + atomic rename + directory fsync; refuse replacement; verify bytes; SHA manifest last';

// Called outside the business clock. Failure is fatal: never publish a success
// manifest after an incomplete durable write, and never reconstruct observations.
export function publishFile(directory, name, data, { replace = false } = {}) {
  assert.ok(name && !name.includes('/') && !['.', '..'].includes(name), 'invalid evidence filename');
  const target = `${directory}/${name}`;
  assert.ok(replace || !existsSync(target), 'refusing to overwrite published evidence');
  const temporary = `${target}.tmp-${randomUUID()}`;
  const bytes = Buffer.isBuffer(data) ? data : Buffer.from(data);
  let fd;
  try {
    fd = openSync(temporary, 'wx', 0o600);
    writeFileSync(fd, bytes);
    fsyncSync(fd);
    closeSync(fd);
    fd = undefined;
    renameSync(temporary, target);
    const parent = openSync(directory, 'r');
    try { fsyncSync(parent); } finally { closeSync(parent); }
    assert.ok(readFileSync(target).equals(bytes), 'published evidence differs from original observations');
  } finally {
    if (fd !== undefined) closeSync(fd);
    if (existsSync(temporary)) rmSync(temporary);
  }
  return { bytes: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex') };
}

export function publishJSONL(directory, name, records) {
  const bytes = records.map(record => JSON.stringify(record) + '\n').join('');
  return { records: records.length, ...publishFile(directory, name, bytes) };
}
