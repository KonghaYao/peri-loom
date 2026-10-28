import { useEffect, useState, type FormEvent } from 'react';
import { Button } from '@cloudflare/kumo/components/button';
import { Input } from '@cloudflare/kumo/components/input';
import { Select } from '@cloudflare/kumo/components/select';
import { Collapsible } from '@cloudflare/kumo/components/collapsible';
import { Banner } from '@cloudflare/kumo/components/banner';
import { Empty } from '@cloudflare/kumo/components/empty';
import { SensitiveInput } from '@cloudflare/kumo/components/sensitive-input';
import type { PageProps } from '../../App';
import { navigate, projectPath } from '../../app/routes';
import { CopyButton, ErrorNotice, Grid, Modal } from '../../app/ui';
import { CreateProject } from './CreateProject';
import { creationFailure } from './project-create';

export function AccessPage({ call, project }: PageProps) {
  const [open, setOpen] = useState(false);
  const [revoke, setRevoke] = useState(false);
  const [subject, setSubject] = useState('');
  const [role, setRole] = useState('sql');
  const [keyID, setKeyID] = useState('');
  const [secret, setSecret] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const create = async (event: FormEvent) => { event.preventDefault(); setBusy(true); setError(''); try { const reply = await call<{ credential: string }>('/v1/keys/create', { subject, project, role, service_account: true }); setSecret(reply.credential); setOpen(false); } catch (failure) { setError(String(failure)); } finally { setBusy(false); } };
  return <><div className="toolbar"><p className="muted">项目：{project}</p><Button variant="primary" onClick={() => setOpen(true)}>创建凭证</Button></div><section className="section"><h2>项目凭证</h2><p className="muted">当前接口不提供完整凭证列表。新建凭证仅显示一次。</p><Button variant="secondary" onClick={() => setRevoke(true)}>按 ID 撤销</Button></section><ErrorNotice error={error} />
    <Modal open={open} onClose={() => { setOpen(false); setError(''); }} title="创建项目凭证" footer={<Button form="create-key-form" variant="primary" disabled={!subject.trim() || busy} type="submit">创建</Button>}><form id="create-key-form" onSubmit={create} className="stack"><Input label="名称" value={subject} onChange={event => setSubject(event.target.value)} required /><Select label="权限" value={role} items={[{ value: 'sql', label: 'SQL 读写' }, { value: 'viewer', label: '查看元数据（非 SQL 只读）' }, { value: 'developer', label: '开发者' }, { value: 'admin', label: '项目管理员' }]} onValueChange={value => setRole(value ?? 'sql')}><Select.Option value="sql">SQL 读写</Select.Option><Select.Option value="viewer">查看元数据（非 SQL 只读）</Select.Option><Select.Option value="developer">开发者</Select.Option><Select.Option value="admin">项目管理员</Select.Option></Select><Banner variant="alert" description="凭证创建后仅展示一次。SQL 权限允许写入。" /><ErrorNotice error={error} /></form></Modal>
    <Modal open={!!secret} onClose={() => setSecret('')} title="凭证已创建" footer={<CopyButton value={secret} label="复制凭证" onError={setError} />}><Banner variant="alert" description="请立即复制；关闭后无法再次查看。" /><SensitiveInput aria-label="新建凭证" readOnly value={secret} /><ErrorNotice error={error} /></Modal>
    <Modal open={revoke} onClose={() => setRevoke(false)} title="撤销凭证" danger footer={<Button variant="destructive" disabled={!keyID || busy} onClick={() => { setBusy(true); void call('/v1/keys/revoke', { id: keyID }).then(() => { setRevoke(false); setKeyID(''); }).catch(failure => setError(String(failure))).finally(() => setBusy(false)); }}>撤销</Button>}><div className="stack"><Input label="凭证 ID" value={keyID} onChange={event => setKeyID(event.target.value)} /><p>撤销 {keyID} 后无法继续使用。</p><ErrorNotice error={error} /></div></Modal></>;
}

export function ProjectSettings({ catalog, project }: PageProps) {
  const item = catalog.projects[project];
  const count = Object.values(catalog.databases).filter(database => database.project === project && database.state !== 'deleted').length;
  return <section className="section"><h2>项目信息</h2><dl className="facts"><dt>ID</dt><dd>{item?.id}</dd><dt>组织</dt><dd>{item?.organization}</dd><dt>租户</dt><dd>{item?.tenant}</dd><dt>区域</dt><dd>{item?.region}</dd><dt>分支资源</dt><dd>{count} / {item?.quotas?.max_databases ?? '—'}</dd></dl><Collapsible.Root><Collapsible.Trigger>技术详情</Collapsible.Trigger><Collapsible.Panel><pre className="code">{JSON.stringify(item, null, 2)}</pre></Collapsible.Panel></Collapsible.Root></section>;
}

export function PlatformPages({ call, catalog, refresh, page }: PageProps & { page: string }) {
  const [metrics, setMetrics] = useState<Record<string, unknown> | null>(null);
  const [error, setError] = useState('');
  const [open, setOpen] = useState<'' | 'organization' | 'project'>('');
  const [organizationID, setOrganizationID] = useState('');
  const [organizationName, setOrganizationName] = useState('');
  const [createdOrganizations, setCreatedOrganizations] = useState<string[]>([]);
  const [createdOrganization, setCreatedOrganization] = useState('');
  const [busy, setBusy] = useState(false);
  useEffect(() => { if (page !== 'platform/status') return; let active = true; call<Record<string, unknown>>('/v1/metrics').then(reply => { if (active) setMetrics(reply); }).catch(failure => { if (active) setError(String(failure)); }); return () => { active = false; }; }, [call, page]);
  const createOrganization = async (event: FormEvent) => {
    event.preventDefault();
    if (busy) return;
    setBusy(true); setError('');
    try {
      const id = organizationID.trim();
      await call('/v1/organizations/create', { id, name: organizationName.trim() });
      setCreatedOrganizations(previous => [...new Set([...previous, id])]);
      setCreatedOrganization(id);
      setOrganizationID(''); setOrganizationName('');
      setOpen('project');
    } catch (failure) {
      setError(creationFailure(failure, 'organization').text);
    } finally {
      setBusy(false);
    }
  };
  if (page === 'platform/status') return <section className="section"><div className="section-heading"><h2>网关进程指标</h2><Button variant="secondary" onClick={() => void call<Record<string, unknown>>('/v1/metrics').then(setMetrics).catch(failure => setError(String(failure)))}>刷新</Button></div><ErrorNotice error={error} />{metrics ? <><Grid columns={['指标', '当前值']} rows={[
    ['活动许可', metrics.active_permits ?? '—'], ['待确认占用', metrics.unknown_held_permits ?? '—'],
    ['已观测 Worker', metrics.observed_workers && typeof metrics.observed_workers === 'object' ? Object.keys(metrics.observed_workers).length : '—'],
    ['累计动作', metrics.actions_total && typeof metrics.actions_total === 'object' ? Object.values(metrics.actions_total).reduce<number>((sum, value) => sum + Number(value), 0) : '—'],
    ['累计错误', metrics.errors_total && typeof metrics.errors_total === 'object' ? Object.values(metrics.errors_total).reduce<number>((sum, value) => sum + Number(value), 0) : '—'],
  ]} /><Banner variant="alert" description="仅本次网关进程口径；已观测不代表所有 Worker 健康。" /><Collapsible.Root><Collapsible.Trigger>技术详情</Collapsible.Trigger><Collapsible.Panel><pre className="code">{JSON.stringify(metrics, (_, value) => typeof value === 'bigint' ? value.toString() : value, 2)}</pre></Collapsible.Panel></Collapsible.Root></> : <p>加载中…</p>}</section>;
  if (page === 'platform/operations') return <section className="section"><h2>操作记录</h2><p>当前没有跨数据库聚合查询接口。请在数据库的“活动”中查看该库保留的操作记录。</p><p className="muted">保留记录不是完整审计日志；结果待确认时不得重放业务请求。</p></section>;
  return <><div className="toolbar"><p className="muted">项目与组织的管理入口</p><Button variant="secondary" onClick={() => { setError(''); setOpen('organization'); }}>新建组织</Button><Button variant="primary" onClick={() => { setError(''); setOpen('project'); }}>新建项目</Button></div><section className="section"><h2>项目</h2>{Object.keys(catalog.projects).length ? <Grid columns={['项目', '组织', '区域']} rows={Object.values(catalog.projects).map(item => [item.id, item.organization, item.region])} /> : <Empty title="还没有项目" description="选择一个组织，然后创建项目。" />}<p className="muted">组织列表由可见项目推导；没有项目的组织不会出现在列表中。可手动输入组织 ID。</p></section>
    <CreateProject open={open === 'project'} call={call} catalog={catalog} refresh={refresh} organizations={createdOrganizations} initialOrganization={createdOrganization} onClose={() => setOpen('')} onCreated={id => { setOpen(''); navigate(projectPath(id)); }} />
    <Modal open={open === 'organization'} onClose={() => { if (busy) return; setOpen(''); setError(''); }} title="新建组织" footer={<Button form="platform-org-form" type="submit" variant="primary" disabled={busy || !organizationID.trim() || !organizationName.trim()}>{busy ? '创建中…' : '新建组织'}</Button>}><form id="platform-org-form" onSubmit={createOrganization} className="stack"><Input label="组织 ID" value={organizationID} onChange={event => setOrganizationID(event.target.value)} required description="1–64 位字母、数字、下划线或连字符；创建后作为命名空间前缀。" /><Input label="组织名称" value={organizationName} onChange={event => setOrganizationName(event.target.value)} required /><ErrorNotice error={error} /></form></Modal></>;
}
