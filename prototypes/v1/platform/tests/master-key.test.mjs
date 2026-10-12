import test from 'node:test';
import assert from 'node:assert/strict';
import {
  loadRememberedMasterKey,
  saveRememberedMasterKey,
  clearRememberedMasterKey,
} from '../frontend/src/features/master-key.ts';

function storage() {
  const entries = new Map();
  return {
    entries,
    getItem: key => entries.get(key) ?? null,
    setItem: (key, value) => entries.set(key, value),
    removeItem: key => entries.delete(key),
  };
}

test('remembered master API key survives reload and is namespaced', () => {
  const local = storage();
  assert.equal(loadRememberedMasterKey(local), '');
  saveRememberedMasterKey(local, 'global-secret');
  assert.equal(loadRememberedMasterKey(local), 'global-secret');
  assert.equal(local.entries.size, 1);
  assert.deepEqual([...local.entries.keys()], ['peri-loom:v1:console:master-key']);
});

test('clearing remembered master key removes only its entry', () => {
  const local = storage();
  local.setItem('other-app:setting', 'keep');
  saveRememberedMasterKey(local, 'global-secret');
  clearRememberedMasterKey(local);
  assert.equal(loadRememberedMasterKey(local), '');
  assert.equal(local.getItem('other-app:setting'), 'keep');
});

test('empty values are rejected without changing remembered key', () => {
  const local = storage();
  saveRememberedMasterKey(local, 'global-secret');
  assert.throws(() => saveRememberedMasterKey(local, '  '), /must not be empty/);
  assert.equal(loadRememberedMasterKey(local), 'global-secret');
});

test('storage failures propagate instead of reporting a successful save or clear', () => {
  const blocked = {
    getItem: () => { throw new Error('blocked read'); },
    setItem: () => { throw new Error('blocked write'); },
    removeItem: () => { throw new Error('blocked removal'); },
  };
  assert.throws(() => loadRememberedMasterKey(blocked), /blocked read/);
  assert.throws(() => saveRememberedMasterKey(blocked, 'global-secret'), /blocked write/);
  assert.throws(() => clearRememberedMasterKey(blocked), /blocked removal/);
});

test('only the explicit master API key argument is stored, not project credentials', () => {
  const local = storage();
  local.setItem('other-app:project', 'project-secret');
  saveRememberedMasterKey(local, 'global-secret');
  assert.equal(loadRememberedMasterKey(local), 'global-secret');
  assert.deepEqual([...local.entries.values()], ['project-secret', 'global-secret']);
  assert.equal(local.entries.size, 2);
});
