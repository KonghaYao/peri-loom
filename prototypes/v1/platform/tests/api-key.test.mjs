import test from 'node:test';
import assert from 'node:assert/strict';
import {
  clearRememberedAPIKey,
  loadRememberPreference,
  loadRememberedAPIKey,
  saveRememberPreference,
  saveRememberedAPIKey,
  syncRememberedAPIKey,
} from '../frontend/src/features/api-key.ts';

function storage() {
  const entries = new Map();
  return {
    entries,
    getItem: key => entries.get(key) ?? null,
    setItem: (key, value) => entries.set(key, value),
    removeItem: key => entries.delete(key),
  };
}

const API_KEY = 'peri-loom:v1:console:master-key';
const PREFERENCE = 'peri-loom:v1:console:remember-api-key';

test('remembering is the default when this browser has no preference', () => {
  const local = storage();
  assert.equal(loadRememberPreference(local), true);
  saveRememberPreference(local, false);
  assert.equal(loadRememberPreference(local), false);
  saveRememberPreference(local, true);
  assert.equal(loadRememberPreference(local), true);
  local.setItem(PREFERENCE, 'unexpected');
  assert.equal(loadRememberPreference(local), true);
});

test('the remembered API key keeps its historical storage key', () => {
  const local = storage();
  assert.equal(loadRememberedAPIKey(local), '');
  saveRememberedAPIKey(local, 'global-secret');
  assert.equal(loadRememberedAPIKey(local), 'global-secret');
  assert.deepEqual([...local.entries.keys()], [API_KEY]);
});

test('preference and key are independent entries', () => {
  const local = storage();
  saveRememberedAPIKey(local, 'global-secret');
  saveRememberPreference(local, false);
  assert.equal(loadRememberedAPIKey(local), 'global-secret');
  assert.equal(loadRememberPreference(local), false);
  assert.deepEqual([...local.entries.keys()].sort(), [API_KEY, PREFERENCE]);
});

test('sync saves while remembering and deletes the key when unchecked', () => {
  const local = storage();
  assert.equal(syncRememberedAPIKey(local, true, 'global-secret'), true);
  assert.equal(loadRememberedAPIKey(local), 'global-secret');
  assert.equal(syncRememberedAPIKey(local, false, 'ignored-secret'), false);
  assert.equal(loadRememberedAPIKey(local), '');
  assert.equal(local.entries.has(API_KEY), false);
});

test('clearing the saved key removes only its entry', () => {
  const local = storage();
  local.setItem('other-app:setting', 'keep');
  saveRememberedAPIKey(local, 'global-secret');
  clearRememberedAPIKey(local);
  assert.equal(loadRememberedAPIKey(local), '');
  assert.equal(local.getItem('other-app:setting'), 'keep');
});

test('empty values are rejected without changing the remembered key', () => {
  const local = storage();
  saveRememberedAPIKey(local, 'global-secret');
  assert.throws(() => saveRememberedAPIKey(local, '  '), /must not be empty/);
  assert.equal(loadRememberedAPIKey(local), 'global-secret');
});

test('storage failures propagate instead of reporting a successful save or clear', () => {
  const blocked = {
    getItem: () => { throw new Error('blocked read'); },
    setItem: () => { throw new Error('blocked write'); },
    removeItem: () => { throw new Error('blocked removal'); },
  };
  assert.throws(() => loadRememberedAPIKey(blocked), /blocked read/);
  assert.throws(() => loadRememberPreference(blocked), /blocked read/);
  assert.throws(() => saveRememberedAPIKey(blocked, 'global-secret'), /blocked write/);
  assert.throws(() => saveRememberPreference(blocked, true), /blocked write/);
  assert.throws(() => clearRememberedAPIKey(blocked), /blocked removal/);
  assert.throws(() => syncRememberedAPIKey(blocked, false, 'ignored-secret'), /blocked removal/);
});

test('only the explicit API key argument is stored', () => {
  const local = storage();
  local.setItem('other-app:project', 'project-secret');
  saveRememberedAPIKey(local, 'global-secret');
  assert.equal(loadRememberedAPIKey(local), 'global-secret');
  assert.deepEqual([...local.entries.values()], ['project-secret', 'global-secret']);
  assert.equal(local.entries.size, 2);
});
