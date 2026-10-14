import test from 'node:test';
import assert from 'node:assert/strict';
import { parseConnectionCapabilities, hranaDatabaseURI, hranaWebSocketURI } from '../frontend/src/features/database-connection.ts';

const database = { namespace: { tenant: 'demo_team', database: 'orders' }, branch: 'main' };

test('only the verified live gateway advertises a usable Hrana HTTP URI', () => {
  const enabled = parseConnectionCapabilities({ version: 1, hrana_http: true, public_origin: 'https://127.0.0.1:9443' });
  assert.equal(hranaDatabaseURI(enabled, database), 'https://127.0.0.1:9443/db/demo_team/orders/main/');
  const disabled = parseConnectionCapabilities({ version: 1, hrana_http: false, public_origin: 'https://127.0.0.1:9443' });
  assert.throws(() => hranaDatabaseURI(disabled, database), /not enabled/);
});

test('only a live WebSocket capability exposes a credential-free Hrana WS URI', () => {
  const enabled = parseConnectionCapabilities({ version: 1, hrana_http: true, hrana_ws: true, public_origin: 'https://127.0.0.1:9443' });
  assert.equal(hranaWebSocketURI(enabled, database), 'wss://127.0.0.1:9443/db/demo_team/orders/main/');
  const legacy = parseConnectionCapabilities({ version: 1, hrana_http: true, public_origin: 'https://127.0.0.1:9443' });
  assert.throws(() => hranaWebSocketURI(legacy, database), /not enabled/);
  const disabled = parseConnectionCapabilities({ version: 1, hrana_http: false, hrana_ws: true, public_origin: 'https://127.0.0.1:9443' });
  assert.throws(() => hranaWebSocketURI(disabled, database), /not enabled/);
  assert.throws(() => parseConnectionCapabilities({ version: 1, hrana_http: true, hrana_ws: 'true', public_origin: 'https://127.0.0.1:9443' }));
});

test('connection URI never accepts injected credentials, paths, unsafe remote HTTP or database segments', () => {
  for (const origin of ['https://user:token@example.test', 'https://example.test/path', 'http://example.test', 'javascript:alert(1)', 'https://example.test/#fragment']) {
    assert.throws(() => parseConnectionCapabilities({ version: 1, hrana_http: true, public_origin: origin }));
  }
  assert.throws(() => hranaDatabaseURI({ enabled: true, origin: 'https://example.test' }, { ...database, branch: '../other' }), /Invalid database/);
  assert.throws(() => parseConnectionCapabilities({ version: 1, hrana_http: 'true', public_origin: 'https://example.test' }), /Invalid connection/);
});
