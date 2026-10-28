import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const app = new URL('../frontend/src/App.tsx', import.meta.url);

test('the connect screen shows the API Key label and its permission description', async () => {
  const source = await readFile(app, 'utf8');
  assert.match(source, /label="API Key"/);
  assert.match(source, /全局管理员 Key 可管理全部项目，项目 Key 仅访问所属项目/);
  assert.doesNotMatch(source, /管理凭证/);
  assert.doesNotMatch(source, /master-key/);
  assert.match(source, /features\/api-key/);
});

test('remembering is a visible checkbox rather than a collapsed disclosure', async () => {
  const source = await readFile(app, 'utf8');
  assert.match(source, /在此浏览器记住 API Key/);
  assert.match(source, /data-testid="remember-api-key"/);
  assert.doesNotMatch(source, /Collapsible\.DefaultTrigger/);
});

test('preferences can forget the key without disconnecting', async () => {
  const source = await readFile(app, 'utf8');
  assert.match(source, /清除已保存的 API Key/);
  assert.match(source, /记住 API Key/);
  assert.match(source, /断开连接/);
  assert.match(source, /但不会断开当前连接/);
});
