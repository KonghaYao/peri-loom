import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import {
  creationFailure,
  defaultRegion,
  organizationIDs,
  projectCreateBody,
  projectTenant,
  validateProjectDraft,
} from '../frontend/src/features/platform/project-create.ts';

const project = (id, organization, region) => ({ id, organization, tenant: `${organization}_${id}`, region });
const catalog = {
  revision: 1,
  projects: {
    alpha: project('alpha', 'org-a', 'local'),
    beta: project('beta', 'org-a', 'local'),
    gamma: project('gamma', 'org-b', 'eu'),
  },
  databases: {},
};

test('organizations are derived from visible projects and deduplicated', () => {
  assert.deepEqual(organizationIDs(catalog), ['org-a', 'org-b']);
  assert.deepEqual(organizationIDs(catalog, ['org-c', 'org-a']), ['org-a', 'org-b', 'org-c']);
  assert.deepEqual(organizationIDs({ projects: {} }), []);
});

test('the default region reuses an existing project region and falls back to local', () => {
  assert.equal(defaultRegion(catalog), 'local');
  assert.equal(defaultRegion(catalog, 'eu'), 'eu');
  assert.equal(defaultRegion({ projects: {} }), 'local');
});

test('the create body carries only the fields the server accepts', () => {
  const body = projectCreateBody({ id: ' new ', organization: ' org-a ', region: ' local ', manual: false });
  assert.deepEqual(body, { id: 'new', organization: 'org-a', region: 'local' });
  assert.deepEqual(Object.keys(body).sort(), ['id', 'organization', 'region']);
  assert.equal(projectTenant({ id: 'new', organization: 'org-a' }), 'org-a_new');
});

test('project validation rejects malformed, duplicate, and colliding drafts', () => {
  assert.equal(validateProjectDraft({ id: 'fresh', organization: 'org-a', region: 'local' }, catalog, ['org-a']), '');
  assert.match(validateProjectDraft({ id: '', organization: 'org-a', region: 'local' }, catalog, ['org-a']), /请输入项目 ID/);
  assert.match(validateProjectDraft({ id: 'bad id', organization: 'org-a', region: 'local' }, catalog, ['org-a']), /项目 ID 仅允许/);
  assert.match(validateProjectDraft({ id: 'fresh', organization: 'org-a', region: 'bad region' }, catalog, ['org-a']), /部署区域仅允许/);
  assert.match(validateProjectDraft({ id: 'fresh', organization: 'org-x', region: 'local' }, catalog, ['org-a']), /组织不存在/);
  assert.equal(validateProjectDraft({ id: 'fresh', organization: 'org-x', region: 'local', manual: true }, catalog, ['org-a']), '');
  assert.match(validateProjectDraft({ id: 'alpha', organization: 'org-a', region: 'local' }, catalog, ['org-a']), /已被占用/);
  const collision = { revision: 1, projects: { ...catalog.projects, other: { ...project('other', 'org-a_fresh', 'local'), tenant: 'org-a_fresh' } }, databases: {} };
  assert.match(validateProjectDraft({ id: 'fresh', organization: 'org-a', region: 'local' }, collision, ['org-a']), /重复的租户命名空间/);
  assert.equal(validateProjectDraft({ id: 'x'.repeat(65), organization: 'org-a', region: 'local' }, catalog, ['org-a']), '项目 ID 仅允许 1–64 位字母、数字、下划线或连字符。');
});

test('creation failures are classified instead of retried blindly', () => {
  const forbidden = creationFailure(Object.assign(new Error('denied'), { code: 'forbidden' }), 'project');
  assert.match(forbidden.text, /权限不足/);
  assert.equal(forbidden.uncertain, false);
  assert.match(creationFailure(Object.assign(new Error('denied'), { code: 'unauthorized' })).text, /全局管理员 API Key/);
  assert.match(creationFailure(Object.assign(new Error('incomplete'), { code: 'operation_incomplete' })).text, /项目 ID/);
  const unknown = creationFailure(Object.assign(new Error('unknown'), { code: 'outcome_unknown', uncertain: true, requestID: 'req-1' }), 'project');
  assert.equal(unknown.uncertain, true);
  assert.match(unknown.text, /不要直接重复提交/);
  assert.match(unknown.text, /req-1/);
  assert.equal(creationFailure(Object.assign(new Error('audit'), { code: 'audit_unavailable' })).uncertain, true);
  assert.equal(creationFailure(new Error('boom')).text, 'boom');
});

test('both entries share the create-project dialog and the non-admin hint is visible', async () => {
  const dialog = await readFile(new URL('../frontend/src/features/platform/CreateProject.tsx', import.meta.url), 'utf8');
  assert.match(dialog, /新建项目/);
  assert.match(dialog, /项目 ID/);
  assert.match(dialog, /部署区域/);
  assert.match(dialog, /projectCreateBody/);
  assert.match(dialog, /<Modal/);
  const platform = await readFile(new URL('../frontend/src/features/platform/pages.tsx', import.meta.url), 'utf8');
  assert.match(platform, /新建项目/);
  assert.match(platform, /新建组织/);
  assert.match(platform, /<CreateProject/);
  const app = await readFile(new URL('../frontend/src/App.tsx', import.meta.url), 'utf8');
  assert.match(app, /新建项目/);
  assert.match(app, /新建项目需要全局管理员 API Key/);
  assert.match(app, /<CreateProject/);
});
