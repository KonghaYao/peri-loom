import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, readdir } from 'node:fs/promises';

const root = new URL('../frontend/src/', import.meta.url);

test('console controls use Kumo components rather than custom native controls', async () => {
  const pending = [root];
  while (pending.length) {
    const directory = pending.pop();
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const location = new URL(entry.name + (entry.isDirectory() ? '/' : ''), directory);
      if (entry.isDirectory()) pending.push(location);
      else if (entry.name.endsWith('.tsx')) {
        const source = await readFile(location, 'utf8');
        assert.ok(!/<(?:button|input|textarea|select|label|table|dialog|details|summary)\b/.test(source), `${location.pathname} contains a native control`);
      }
    }
  }
});
