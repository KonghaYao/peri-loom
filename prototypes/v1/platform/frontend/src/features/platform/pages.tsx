import { useEffect, useState, type FormEvent } from 'react';
import { Button } from '@cloudflare/kumo/components/button';
import { Input } from '@cloudflare/kumo/components/input';
import type { PageProps } from '../../App';
import { ErrorNotice, Grid, Modal } from '../../app/ui';

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
    <Modal open={open} onClose={() => { setOpen(false); setError(''); }} title="创建项目凭证"><form onSubmit={create} className="stack"><label>名称<Input value={subject} onChange={event => setSubject(event.target.value)} required /></label><label>权限<select value={role} onChange={event => setRole(event.target.value)}><option value="sql">SQL 读写</option><option value="viewer">查看元数据（非 SQL 只读）</option><option value="developer">开发者</option><option value="admin">项目管理员</option></select></label><p className="muted">凭证创建后仅展示一次。SQL 权限允许写入。</p><ErrorNotice error={error} /><Button variant="primary" disabled={!subject.trim() || busy} type="submit">创建</Button></form></Modal>
    <Modal open={!!secret} onClose={() => setSecret('')} title="凭证已创建"><p>请立即复制；关闭后无法再次查看。</p><Input type="password" readOnly value={secret} /><Button variant="secondary" onClick={() => void navigator.clipboard.writeText(secret).catch(() => setError('复制失败，请手动选择。'))}>复制凭证</Button><ErrorNotice error={error} /></Modal>
    <Modal open={revoke} onClose={() => setRevoke(false)} title="撤销凭证" danger><div className="stack"><label>凭证 ID<Input value={keyID} onChange={event => setKeyID(event.target.value)} /></label><p>撤销 {keyID} 后无法继续使用。</p><Button variant="destructive" disabled={!keyID || busy} onClick={() => { setBusy(true); void call('/v1/keys/revoke', { id: keyID }).then(() => { setRevoke(false); setKeyID(''); }).catch(failure => setError(String(failure))).finally(() => setBusy(false)); }}>撤销</Button><ErrorNotice error={error} /></div></Modal></>;
}

export function ProjectSettings({ catalog, project }: PageProps) {
  const item = catalog.projects[project];
  const count = Object.values(catalog.databases).filter(database => database.project === project && database.state !== 'deleted').length;
  return <section className="section"><h2>项目信息</h2><dl className="facts"><dt>ID</dt><dd>{item?.id}</dd><dt>组织</dt><dd>{item?.organization}</dd><dt>租户</dt><dd>{item?.tenant}</dd><dt>区域</dt><dd>{item?.region}</dd><dt>分支资源</dt><dd>{count} / {item?.quotas?.max_databases ?? '—'}</dd></dl><details className="disclosure"><summary>技术详情</summary><pre className="code">{JSON.stringify(item, null, 2)}</pre></details></section>;
}

export function PlatformPages({ call, catalog, refresh, page }: PageProps & { page: string }) {
  const [metrics, setMetrics] = useState<Record<string, unknown> | null>(null);
  const [error, setError] = useState('');
  const [open, setOpen] = useState('');
  const [organization, setOrganization] = useState('');
  const [name, setName] = useState('');
  const [region, setRegion] = useState('');
  const [busy, setBusy] = useState(false);
  useEffect(() => { if (page !== 'platform/status') return; let active = true; call<Record<string, unknown>>('/v1/metrics').then(reply => { if (active) setMetrics(reply); }).catch(failure => { if (active) setError(String(failure)); }); return () => { active = false; }; }, [call, page]);
  const create = async (event: FormEvent) => { event.preventDefault(); setBusy(true); setError(''); try { if (open === 'organization') await call('/v1/organizations/create', { id: organization, name }); else await call('/v1/projects/create', { organization, id: name, region }); await refresh(); setOpen(''); setName(''); } catch (failure) { setError(String(failure)); } finally { setBusy(false); } };
  if (page === 'platform/status') return <section className="section"><div className="section-heading"><h2>网关进程指标</h2><Button variant="secondary" onClick={() => void call<Record<string, unknown>>('/v1/metrics').then(setMetrics).catch(failure => setError(String(failure)))}>刷新</Button></div><ErrorNotice error={error} />{metrics ? <><Grid columns={['指标', '当前值']} rows={[
    ['活动许可', metrics.active_permits ?? '—'], ['待确认占用', metrics.unknown_held_permits ?? '—'],
    ['已观测 Worker', metrics.observed_workers && typeof metrics.observed_workers === 'object' ? Object.keys(metrics.observed_workers).length : '—'],
    ['累计动作', metrics.actions_total && typeof metrics.actions_total === 'object' ? Object.values(metrics.actions_total).reduce<number>((sum, value) => sum + Number(value), 0) : '—'],
    ['累计错误', metrics.errors_total && typeof metrics.errors_total === 'object' ? Object.values(metrics.errors_total).reduce<number>((sum, value) => sum + Number(value), 0) : '—'],
  ]} /><p className="muted">仅本次网关进程口径；已观测不代表所有 Worker 健康。</p><details className="disclosure"><summary>技术详情</summary><pre className="code">{JSON.stringify(metrics, (_, value) => typeof value === 'bigint' ? value.toString() : value, 2)}</pre></details></> : <p>加载中…</p>}</section>;
  if (page === 'platform/operations') return <section className="section"><h2>操作记录</h2><p>当前没有跨数据库聚合查询接口。请在数据库的“活动”中查看该库保留的操作记录。</p><p className="muted">保留记录不是完整审计日志；结果待确认时不得重放业务请求。</p></section>;
  return <><div className="toolbar"><Button variant="secondary" onClick={() => setOpen('organization')}>创建组织</Button><Button variant="primary" onClick={() => setOpen('project')}>创建项目</Button></div><section className="section"><h2>项目</h2><Grid columns={['项目', '组织', '区域']} rows={Object.values(catalog.projects).map(item => [item.id, item.organization, item.region])} /><p className="muted">组织仅从可见项目推导，不代表完整组织列表。</p></section><Modal open={!!open} onClose={() => setOpen('')} title={open === 'organization' ? '创建组织' : '创建项目'}><form onSubmit={create} className="stack"><label>组织 ID<Input value={organization} onChange={event => setOrganization(event.target.value)} required /></label><label>{open === 'organization' ? '组织名称' : '项目 ID'}<Input value={name} onChange={event => setName(event.target.value)} required /></label>{open === 'project' && <label>部署区域<Input value={region} onChange={event => setRegion(event.target.value)} required /></label>}<ErrorNotice error={error} /><Button type="submit" variant="primary" disabled={busy || !name || !organization}>创建</Button></form></Modal></>;
}
