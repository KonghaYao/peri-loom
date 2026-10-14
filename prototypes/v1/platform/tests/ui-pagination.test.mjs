import test from 'node:test';
import assert from 'node:assert/strict';
import { pageSlice } from '../frontend/src/features/catalog/pagination.ts';

test('one thousand branches render in bounded pages without losing the last entry', () => {
  const branches = Array.from({ length: 1000 }, (_, index) => `branch-${index}`);
  assert.deepEqual(pageSlice(branches, 0).items, branches.slice(0, 100));
  assert.deepEqual(pageSlice(branches, 9).items, branches.slice(900));
  assert.deepEqual(pageSlice(branches, 99).items, branches.slice(900));
  assert.equal(pageSlice(branches, 99).pages, 10);
});
