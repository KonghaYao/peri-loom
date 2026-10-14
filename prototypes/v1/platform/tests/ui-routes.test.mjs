import test from 'node:test';
import assert from 'node:assert/strict';
import { branchesPath, databasePath, navigate, parseRoute, projectPath, setNavigationBlocker } from '../frontend/src/app/routes.ts';

test('database workspace routes preserve project, database and branch identities', () => {
  const route = parseRoute(`#${databasePath('team-a', 'orders', 'main', 'sql')}`);
  assert.deepEqual(route, { kind: 'sql', project: 'team-a', database: 'orders', branch: 'main' });
  assert.deepEqual(parseRoute(`#${branchesPath('team-a', 'orders')}`), { kind: 'branches', project: 'team-a', database: 'orders' });
});

test('project routes and table query parameters remain independently addressable', () => {
  assert.deepEqual(parseRoute(`#${projectPath('team-a', 'access')}`), { kind: 'access', project: 'team-a' });
  assert.deepEqual(parseRoute(`#${databasePath('team-a', 'orders', 'reader', 'data')}?table=events`), { kind: 'data', project: 'team-a', database: 'orders', branch: 'reader' });
  assert.equal(parseRoute('#/projects/%GG/databases').kind, 'connect');
});

test('an active SQL session prevents programmatic navigation', () => {
  const previous = globalThis.window;
  globalThis.window = { location: { hash: '#/projects/team-a/databases' } };
  try {
    setNavigationBlocker(() => false);
    navigate('/preferences');
    assert.equal(window.location.hash, '#/projects/team-a/databases');
    setNavigationBlocker(() => true);
    navigate('/preferences');
    assert.equal(window.location.hash, '/preferences');
  } finally { setNavigationBlocker(null); globalThis.window = previous; }
});
