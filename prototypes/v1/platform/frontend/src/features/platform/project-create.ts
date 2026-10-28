import type { Catalog } from '../../api';

export const PROJECT_LABEL_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;

export type ProjectDraft = { id: string; organization: string; region: string; manual?: boolean };
export type ProjectCreateBody = { organization: string; id: string; region: string };
export type CreationFailure = { text: string; uncertain: boolean };

export function normalizeLabel(value: string): string {
  return value.trim();
}

export function projectTenant(draft: Pick<ProjectDraft, 'id' | 'organization'>): string {
  return `${normalizeLabel(draft.organization)}_${normalizeLabel(draft.id)}`;
}

export function organizationIDs(catalog: Pick<Catalog, 'projects'>, created: Iterable<string> = []): string[] {
  const ids = new Set<string>();
  for (const project of Object.values(catalog.projects)) {
    if (project.organization) ids.add(project.organization);
  }
  for (const id of created) {
    if (id) ids.add(id);
  }
  return [...ids].sort((left, right) => left.localeCompare(right));
}

// The API has no organization or region registry. Reuse the region the visible
// projects already use; `local` is the single-node default when none exist.
export function defaultRegion(catalog: Pick<Catalog, 'projects'>, preferred = ''): string {
  const counts = new Map<string, number>();
  for (const project of Object.values(catalog.projects)) {
    if (!project.region || !PROJECT_LABEL_PATTERN.test(project.region)) continue;
    counts.set(project.region, (counts.get(project.region) ?? 0) + 1);
  }
  if (preferred && counts.has(preferred)) return preferred;
  const ranked = [...counts.entries()].sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]));
  return ranked[0]?.[0] ?? 'local';
}

export function validateProjectDraft(draft: ProjectDraft, catalog: Pick<Catalog, 'projects'>, organizations: string[]): string {
  const id = normalizeLabel(draft.id);
  const organization = normalizeLabel(draft.organization);
  const region = normalizeLabel(draft.region);
  if (!id) return '请输入项目 ID。';
  if (!PROJECT_LABEL_PATTERN.test(id)) return '项目 ID 仅允许 1–64 位字母、数字、下划线或连字符。';
  if (!organization) return '请输入组织 ID。';
  if (!PROJECT_LABEL_PATTERN.test(organization)) return '组织 ID 仅允许 1–64 位字母、数字、下划线或连字符。';
  if (!region) return '请输入部署区域。';
  if (!PROJECT_LABEL_PATTERN.test(region)) return '部署区域仅允许 1–64 位字母、数字、下划线或连字符。';
  if (!draft.manual && !organizations.includes(organization)) return '组织不存在，请先新建组织或手动输入组织 ID。';
  if (Object.prototype.hasOwnProperty.call(catalog.projects, id)) return `项目 ID“${id}”已被占用，请更换。`;
  const tenant = projectTenant({ id, organization });
  if (Object.values(catalog.projects).some(project => project.tenant === tenant)) {
    return `组织“${organization}”与项目 ID“${id}”会生成重复的租户命名空间，请更换项目 ID。`;
  }
  return '';
}

// `request_id` is added by the API wrapper; sending `tenant` or `quotas` here
// would let the console disagree with the server's own derivation.
export function projectCreateBody(draft: ProjectDraft): ProjectCreateBody {
  return { organization: normalizeLabel(draft.organization), id: normalizeLabel(draft.id), region: normalizeLabel(draft.region) };
}

type FailureShape = { code?: unknown; uncertain?: unknown; requestID?: unknown; message?: unknown };

export function creationFailure(failure: unknown, kind: 'project' | 'organization' = 'project'): CreationFailure {
  const record = failure !== null && typeof failure === 'object' ? failure as FailureShape : {};
  const code = typeof record.code === 'string' ? record.code : '';
  const request = typeof record.requestID === 'string' && record.requestID ? `（request ${record.requestID}）` : '';
  const subject = kind === 'project' ? '项目' : '组织';
  if (record.uncertain === true || code === 'outcome_unknown' || code === 'audit_unavailable') {
    return { text: `结果未确认：${subject}创建请求可能已提交。请先刷新列表核对${request}，不要直接重复提交。`, uncertain: true };
  }
  if (code === 'forbidden' || code === 'unauthorized') {
    return { text: '权限不足：新建项目需要全局管理员 API Key。', uncertain: false };
  }
  if (code === 'operation_incomplete') {
    return { text: '创建未完成：请检查项目 ID、组织 ID 与租户命名空间是否冲突；组织必须已存在。', uncertain: false };
  }
  if (code === 'credential_required') {
    return { text: '需要先连接 API Key。', uncertain: false };
  }
  if (code === 'invalid_request') {
    return { text: '请求参数无效，请检查项目 ID、组织与区域。', uncertain: false };
  }
  return { text: failure instanceof Error ? failure.message : String(failure), uncertain: false };
}
