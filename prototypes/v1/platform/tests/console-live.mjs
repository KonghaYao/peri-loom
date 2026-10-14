import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs';

const connection = JSON.parse(fs.readFileSync(process.env.PERI_UI_CONNECTION_FILE, 'utf8'));
const port = Number(process.env.PERI_UI_CDP_PORT);
const httpsCheck = process.env.PERI_UI_TEST_HTTPS === '1';
const twoDatabasesCheck = process.env.PERI_UI_TEST_TWO_DATABASES === '1';
const writePermissionsCheck = process.env.PERI_UI_TEST_WRITE_PERMISSIONS === '1';
const databaseKey = database => `${database.namespace.tenant}/${database.namespace.database}/${database.branch}`;
const dualFixture = twoDatabasesCheck && process.env.PERI_UI_DUAL_FIXTURE_FILE ? JSON.parse(fs.readFileSync(process.env.PERI_UI_DUAL_FIXTURE_FILE, 'utf8')) : null;
const secondDatabase = dualFixture?.database_b ?? connection.second_database;
assert(Number.isInteger(port) && port > 0 && port < 65536);
assert(new RegExp(`^${httpsCheck ? 'https' : 'http'}://127\\.0\\.0\\.1:\\d+/?$`).test(connection.endpoint));
if (twoDatabasesCheck) {
  if (dualFixture) assert.equal(databaseKey(dualFixture.database_a), databaseKey(connection.database), 'Fixture database_a must match the connection database');
  assert(secondDatabase?.namespace?.tenant && secondDatabase?.namespace?.database && secondDatabase?.branch && databaseKey(secondDatabase) !== databaseKey(connection.database), 'Provide a distinct second_database in the connection file or PERI_UI_DUAL_FIXTURE_FILE');
}
if (writePermissionsCheck) {
  assert(httpsCheck && twoDatabasesCheck, 'Write permissions require HTTPS and the two-database fixture');
  assert.equal(process.env.PERI_UI_DISPOSABLE_WRITE_FIXTURE, '1', 'Writing requires an explicitly disposable fixture');
  assert(/^peri_ui_[A-Za-z0-9_]+$/.test(process.env.PERI_UI_WRITE_PROBE_TABLE ?? ''), 'Provide a disposable peri_ui_ fixture table (probe_value TEXT) in writer and read-only databases');
  assert(typeof connection.readonly_project_api_key === 'string' && connection.readonly_project_api_key !== connection.project_api_key, 'Provide a distinct readonly_project_api_key');
  assert(connection.readonly_database?.namespace?.tenant && connection.readonly_database?.namespace?.database && connection.readonly_database?.branch && databaseKey(connection.readonly_database) !== databaseKey(connection.database), 'Provide a distinct pre-provisioned read-only database with the probe table');
  assert(connection.database && databaseKey(connection.database));
  assert(process.env.PERI_UI_ISSUE_PROJECT_KEY !== '1' && process.env.PERI_UI_TEST_MASTER !== '1', 'Write permissions check requires direct project-key login');
}
if (process.env.PERI_UI_ISSUE_PROJECT_KEY === '1') {
  assert.equal(process.env.PERI_UI_ALLOW_KEY_ISSUE_CONFIRM, '1', 'Key creation requires explicit confirm opt-in');
}

let target;
let socket;
let stage = 'browser startup';
let sequence = 0;
let dialogFailure;
const requests = new Map();

async function command(method, params = {}) {
  const id = ++sequence;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { requests.delete(id); reject(new Error('CDP timeout')); }, 15000);
    requests.set(id, { resolve, reject, timer });
    socket.send(JSON.stringify({ id, method, params }));
  });
}

async function evaluate(expression) {
  const reply = await command('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true });
  if (reply.exceptionDetails) throw new Error('browser evaluation failed');
  return reply.result.value;
}

async function until(expression) {
  const deadline = Date.now() + 20000;
  while (Date.now() < deadline) {
    if (await evaluate(expression)) return;
    await new Promise(resolve => setTimeout(resolve, 75));
  }
  throw new Error('browser condition timed out');
}

async function click(text) {
  assert.equal(await evaluate(`(() => { const button = [...document.querySelectorAll('button')].find(item => item.textContent.trim() === ${JSON.stringify(text)}); if (!button || button.disabled) return false; button.click(); return true; })()`), true);
}

async function fill(selector, value) {
  assert.equal(await evaluate(`(() => { const input = document.querySelector(${JSON.stringify(selector)}); if (!input) return false; const setter = Object.getOwnPropertyDescriptor(input.tagName === 'TEXTAREA' ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype, 'value').set; setter.call(input, ${JSON.stringify(value)}); input.dispatchEvent(new Event('input', { bubbles: true })); return true; })()`), true);
}

async function selectDatabase(key) {
  assert.equal(await evaluate(`(() => { const select = document.querySelector('#database-select'); if (!select || select.disabled || ![...select.options].some(option => option.value === ${JSON.stringify(key)})) return false; select.value = ${JSON.stringify(key)}; select.dispatchEvent(new Event('change', { bubbles: true })); return true; })()`), true);
  await until(`document.querySelector('#database-select')?.value === ${JSON.stringify(key)} && document.querySelector('.page-subtitle')?.textContent?.includes(${JSON.stringify(key.split('/')[1])})`);
}

async function queryFor(value, sql) {
  await fill('#sql-input', sql);
  await click('Run query');
  await until(`[...document.querySelectorAll('.panel td')].some(cell => cell.textContent === ${JSON.stringify(value)})`);
  assert.equal(await evaluate("document.querySelector('.toast-error') === null"), true);
  assert.equal(await evaluate(`[...document.querySelectorAll('.panel td')].some(cell => cell.textContent === ${JSON.stringify(value)})`), true);
}

try {
  const opened = await fetch(`http://127.0.0.1:${port}/json/new?${encodeURIComponent(connection.endpoint)}`, { method: 'PUT' }).then(reply => reply.json());
  target = opened.id;
  socket = new WebSocket(opened.webSocketDebuggerUrl);
  await new Promise((resolve, reject) => { socket.addEventListener('open', resolve, { once: true }); socket.addEventListener('error', reject, { once: true }); });
  socket.addEventListener('message', event => {
    const message = JSON.parse(event.data);
    if (message.method === 'Page.javascriptDialogOpening') {
      const allowed = (stage === 'writer permission probe' && writePermissionsCheck && process.env.PERI_UI_DISPOSABLE_WRITE_FIXTURE === '1' && message.params.message === `Execute SQL against ${connection.database.namespace.database} / ${connection.database.branch}? Changes may be committed.`) || (stage === 'issue project SQL API key' && process.env.PERI_UI_ALLOW_KEY_ISSUE_CONFIRM === '1' && /^Create a new SQL API key for project .+\? This key can execute SQL \(including writes\), is shown once, and must be revoked when no longer needed\.$/.test(message.params.message));
      if (!allowed) dialogFailure = new Error('Unexpected browser confirmation dismissed');
      void command('Page.handleJavaScriptDialog', { accept: allowed }).catch(error => { dialogFailure = error; });
      return;
    }
    const pending = requests.get(message.id);
    if (!pending) return;
    requests.delete(message.id);
    clearTimeout(pending.timer);
    if (message.error) pending.reject(new Error('CDP command failed'));
    else pending.resolve(message.result);
  });
  await command('Runtime.enable');
  await command('Page.enable');
  await command('Emulation.setDeviceMetricsOverride', { width: Number(process.env.PERI_UI_WIDTH ?? 1440), height: Number(process.env.PERI_UI_HEIGHT ?? 900), deviceScaleFactor: 1, mobile: false });
  stage = 'navigation';
  await until("document.querySelectorAll('nav[aria-label=\"Main navigation\"] button').length === 8");
  if (httpsCheck) {
    stage = 'HTTPS transport';
    assert.equal(await evaluate("location.protocol === 'https:' && location.hostname === '127.0.0.1' && window.isSecureContext"), true);
    assert.equal(await evaluate(`location.origin === ${JSON.stringify(new URL(connection.endpoint).origin)}`), true);
  }
  stage = 'authenticate';
  if (process.env.PERI_UI_ISSUE_PROJECT_KEY === '1') {
    await fill('input[type=password]', connection.global_api_key);
    await click('Connect');
    await until("document.querySelector('#database-select option:not([value=\"\"])') !== null");
    await click('Databases');
    await until("[...document.querySelectorAll('button')].some(button => button.textContent === 'Create project SQL API key')");
    assert.equal(await evaluate("document.querySelector('[data-testid=native-api-url]')?.value === location.origin"), true);
    assert.equal(await evaluate("document.body.textContent.includes('libSQL-compatible endpoint: not available in V1.')"), true);
    assert.equal(await evaluate('localStorage.length'), 0);
    stage = 'issue project SQL API key';
    await click('Create project SQL API key');
    await until("document.querySelector('[data-testid=issued-project-key]')?.value?.startsWith('plk1_') || document.querySelector('.project-key-request [role=alert]') !== null");
    assert.equal(dialogFailure, undefined);
    assert.equal(await evaluate("document.querySelector('.project-key-request [role=alert]') === null"), true);
    if (process.env.PERI_UI_KEY_SCREENSHOT) {
      const image = await command('Page.captureScreenshot', { format: 'png' });
      fs.writeFileSync(process.env.PERI_UI_KEY_SCREENSHOT, Buffer.from(image.data, 'base64'), { mode: 0o600 });
    }
    await click('Use key for table preview');
    await until("document.querySelector('#database-select option:not([value=\"\"])') !== null && document.querySelector('[data-testid=project-sql-key]') === null");
    assert.equal(await evaluate('localStorage.length'), 0);
  } else if (process.env.PERI_UI_TEST_MASTER === '1') {
    await fill('input[type=password]', connection.global_api_key);
    assert.equal(await evaluate("(() => { const option = document.querySelector('[data-testid=remember-master-key]'); option.click(); return option.checked; })()"), true);
    await click('Connect');
    await until("localStorage.length === 1 && document.querySelector('[data-testid=clear-master-key]') === null");
    assert.equal(await evaluate(`localStorage.getItem('peri-loom:v1:console:master-key') === ${JSON.stringify(connection.global_api_key)}`), true);
    await command('Page.reload');
    await until(`document.querySelector('input[type=password]')?.value === ${JSON.stringify(connection.global_api_key)}`);
    await click('Connect');
    await until("document.querySelector('#database-select option:not([value=\"\"])') !== null");
    if (process.env.PERI_UI_PREVIEW_TABLE) {
      await click('Databases');
      await until("[...document.querySelectorAll('button')].some(button => button.textContent === 'Create project SQL API key')");
      assert.equal(await evaluate("document.querySelector('.table-browser-list') === null"), true);
    }
    await click('Settings');
    await click('Clear saved master key');
    await until('localStorage.length === 0');
    await click('Disconnect');
    await fill('input[type=password]', connection.project_api_key);
    assert.equal(await evaluate("(() => { const option = document.querySelector('[data-testid=remember-master-key]'); option.click(); return option.checked; })()"), true);
    await click('Connect');
    await until("document.querySelector('#database-select option:not([value=\"\"])') !== null");
    await until("document.querySelector('[role=alert]')?.textContent?.includes('project keys are not saved')");
    assert.equal(await evaluate('localStorage.length'), 0);
  } else {
    await fill('input[type=password]', connection.project_api_key);
    await click('Connect');
    await until("document.querySelector('#database-select option:not([value=\"\"])') !== null");
  }
  await until("document.querySelector('#database-select option:not([value=\"\"])') !== null");
  if (httpsCheck) {
    stage = 'HTTPS native API origin';
    await click('Databases');
    await until("document.querySelector('[data-testid=native-api-url]')?.value");
    assert.equal(await evaluate("document.querySelector('[data-testid=native-api-url]').value === location.origin"), true);
  }
  if (process.env.PERI_UI_PREVIEW_TABLE) {
    stage = 'database table and data preview';
    await click('Databases');
    await until(`[...document.querySelectorAll('.table-browser-list button')].some(button => button.textContent === ${JSON.stringify(process.env.PERI_UI_PREVIEW_TABLE)})`);
    await click(process.env.PERI_UI_PREVIEW_TABLE);
    await until("[...document.querySelectorAll('.table-browser-table td')].some(cell => cell.textContent === '9223372036854775807') || document.querySelector('.table-browser-error') !== null");
    assert.equal(await evaluate("document.querySelector('.table-browser-error') === null"), true);
    assert.equal(await evaluate("[...document.querySelectorAll('.table-browser-table th')].some(cell => cell.textContent === 'id')"), true);
    if (process.env.PERI_UI_TABLE_SCREENSHOT) {
      const image = await command('Page.captureScreenshot', { format: 'png' });
      fs.writeFileSync(process.env.PERI_UI_TABLE_SCREENSHOT, Buffer.from(image.data, 'base64'), { mode: 0o600 });
    }
  }
  stage = 'SQL session';
  await click('SQL Editor');
  await click('Open session');
  await until("document.querySelector('.panel .badge')?.textContent?.includes('Writer') || document.querySelector('.panel .badge')?.textContent?.includes('Read-only')");
  stage = 'lossless streaming query';
  await fill('#sql-input', 'SELECT 9223372036854775807');
  await click('Run query');
  await until("[...document.querySelectorAll('td')].some(cell => cell.textContent === '9223372036854775807')");
  stage = 'close acknowledgement';
  await click('Close session');
  await until("document.querySelector('.panel .badge')?.textContent === 'Closed'");
  if (twoDatabasesCheck) {
    stage = 'second database selection';
    const secondKey = databaseKey(secondDatabase);
    assert.equal(await evaluate(`document.querySelector('#database-select')?.value === ${JSON.stringify(databaseKey(connection.database))}`), true);
    await selectDatabase(secondKey);
    stage = 'second database SQL session';
    await click('Open session');
    await until("document.querySelector('.panel .badge')?.textContent === 'Writer' || document.querySelector('.panel .badge')?.textContent === 'Read-only'");
    await queryFor('702271', 'SELECT 702271');
    await click('Close session');
    await until("document.querySelector('.panel .badge')?.textContent === 'Closed'");
    await selectDatabase(databaseKey(connection.database));
  }
  if (writePermissionsCheck) {
    stage = 'writer permission probe';
    const marker = `peri_ui_${randomUUID().replaceAll('-', '')}`;
    const table = process.env.PERI_UI_WRITE_PROBE_TABLE;
    await click('Open session');
    await until("document.querySelector('.panel .badge')?.textContent === 'Writer' || document.querySelector('.toast-error') !== null");
    assert.equal(await evaluate("document.querySelector('.panel .badge')?.textContent"), 'Writer');
    await fill('#sql-input', `INSERT INTO "${table}" (probe_value) VALUES ('${marker}')`);
    await click('Execute statement');
    await until("document.querySelector('[role=status] .toast')?.textContent?.includes('Execute completed') || document.querySelector('.toast-error') !== null");
    assert.equal(dialogFailure, undefined);
    assert.equal(await evaluate("document.querySelector('.toast-error') === null"), true);
    await queryFor(marker, `SELECT probe_value FROM "${table}" WHERE probe_value = '${marker}'`);
    await click('Close session');
    await until("document.querySelector('.panel .badge')?.textContent === 'Closed'");
    stage = 'read-only permission probe';
    await click('Settings');
    await click('Disconnect');
    await fill('input[type=password]', connection.readonly_project_api_key);
    await click('Connect');
    await until("document.querySelector('#database-select option:not([value=\"\"])') !== null");
    await selectDatabase(databaseKey(connection.readonly_database));
    await click('SQL Editor');
    await click('Open session');
    await until("document.querySelector('.panel .badge')?.textContent === 'Read-only' || document.querySelector('.toast-error') !== null");
    assert.equal(await evaluate("document.querySelector('.panel .badge')?.textContent"), 'Read-only');
    assert.equal(await evaluate("[...document.querySelectorAll('button')].find(button => button.textContent.trim() === 'Execute statement')?.disabled"), true);
    await queryFor('0', `SELECT COUNT(*) FROM "${table}" WHERE probe_value = '${marker}_denied'`);
    const deniedMarker = `${marker}_denied`;
    await fill('#sql-input', `INSERT INTO "${table}" (probe_value) VALUES ('${deniedMarker}') RETURNING probe_value`);
    await click('Run query');
    await until("document.querySelector('.toast-error') !== null");
    assert.match(await evaluate("document.querySelector('.toast-error').textContent"), /sql_error|read.only|readonly/i);
    assert.equal(await evaluate(`[...document.querySelectorAll('.panel td')].some(cell => cell.textContent === ${JSON.stringify(deniedMarker)})`), false);
    await queryFor('0', `SELECT COUNT(*) FROM "${table}" WHERE probe_value = '${deniedMarker}'`);
    await click('Close session');
    await until("document.querySelector('.panel .badge')?.textContent === 'Closed'");
  }
  stage = 'reference inspection';
  await click('Branches & Snapshots');
  await click('Load refs');
  await until("document.querySelector('.tabs') !== null || document.querySelector('[role=alert]') !== null");
  assert.equal(await evaluate("document.querySelector('[role=alert]') === null"), true);
  stage = 'SaaS menu coverage';
  for (const page of ['Overview', 'Databases', 'Operations', 'Access', 'Observability', 'Settings']) {
    await click(page);
    await until(`document.querySelector('h1')?.textContent === ${JSON.stringify(page)}`);
  }
  if (process.env.PERI_UI_ISSUE_PROJECT_KEY !== '1') {
    stage = 'database inspection';
    await click('Observability');
    await click('Load status & schema');
    await until("[...document.querySelectorAll('h3')].some(heading => heading.textContent === 'Schema') || document.querySelector('[role=alert]') !== null");
    assert.equal(await evaluate("document.querySelector('[role=alert]') === null"), true);
  }
  stage = 'browser storage';
  assert.equal(await evaluate('localStorage.length'), 0);
  assert.equal(await evaluate('sessionStorage.length'), 0);
  if (process.env.PERI_UI_SCREENSHOT) {
    const image = await command('Page.captureScreenshot', { format: 'png' });
    fs.writeFileSync(process.env.PERI_UI_SCREENSHOT, Buffer.from(image.data, 'base64'), { mode: 0o600 });
  }
  console.log(JSON.stringify({ event: 'console_live_complete', pages: 8, menus: 8, key_issued_and_activated: process.env.PERI_UI_ISSUE_PROJECT_KEY === '1', master_saved_cleared: process.env.PERI_UI_TEST_MASTER === '1', project_key_not_saved: process.env.PERI_UI_TEST_MASTER === '1' || process.env.PERI_UI_ISSUE_PROJECT_KEY === '1', table_preview: !!process.env.PERI_UI_PREVIEW_TABLE, sql_bigint: true, sql_stream: true, close_ack: true, refs: true, status_schema: process.env.PERI_UI_ISSUE_PROJECT_KEY !== '1', storage_empty: true, ...(httpsCheck && { https: true }), ...(twoDatabasesCheck && { two_databases: true }), ...(writePermissionsCheck && { write_permissions: true }) }));
} catch {
  if (socket && process.env.PERI_UI_FAILURE_SCREENSHOT) {
    try {
      const image = await command('Page.captureScreenshot', { format: 'png' });
      fs.writeFileSync(process.env.PERI_UI_FAILURE_SCREENSHOT, Buffer.from(image.data, 'base64'), { mode: 0o600 });
    } catch {}
  }
  console.error(JSON.stringify({ event: 'console_live_failed', stage }));
  process.exitCode = 1;
} finally {
  for (const pending of requests.values()) { clearTimeout(pending.timer); pending.reject(new Error('socket closed')); }
  socket?.close();
  if (target) await fetch(`http://127.0.0.1:${port}/json/close/${target}`).catch(() => {});
}
