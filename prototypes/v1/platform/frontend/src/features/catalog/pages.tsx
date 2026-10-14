import { useState, type FormEvent } from 'react';
import { Button } from '@cloudflare/kumo/components/button';
import { Input } from '@cloudflare/kumo/components/input';
import { Badge } from '@cloudflare/kumo/components/badge';
import { Table } from '@cloudflare/kumo/components/table';
import type { PageProps } from '../../App';
import { databasePath, navigate } from '../../app/routes';
import { ErrorNotice, Modal } from '../../app/ui';
import { pageSlice } from './pagination';

const valid = (value: string) => /^[A-Za-z0-9_-]{1,64}$/.test(value);
export function CatalogPage({ call, catalog, project, refresh }: PageProps) {
  const [search, setSearch] = useState('');
  const [filter, setFilter] = useState('all');
  const [open, setOpen] = useState(false);
  const [name, setName] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const branches = Object.values(catalog.databases).filter(item => item.project === project && item.state !== 'deleted');
  const groups = Object.values(branches.reduce<Record<string, { name: string; count: number; state: string; branch: string }>>((all, item) => {
    const name = item.id.namespace.database;
    if (!all[name]) all[name] = { name, count: 0, state: item.state, branch: item.id.branch };
    all[name].count++;
    if (item.state !== 'active') all[name].state = item.state;
    if (item.id.branch === 'main') all[name].branch = 'main';
    return all;
  }, {}));
  const visible = groups.filter(item => item.name.toLowerCase().includes(search.toLowerCase()) && (filter === 'all' || item.state === filter));
  const create = async (event: FormEvent) => {
    event.preventDefault(); if (!valid(name) || busy) return;
    setBusy(true); setError('');
    try { await call('/v1/databases/create', { project, database: { namespace: { tenant: catalog.projects[project].tenant, database: name }, branch: 'main' }, role: 'writer' }); await refresh(); setOpen(false); navigate(databasePath(project, name, 'main')); }
    catch (failure) { setError(String(failure)); } finally { setBusy(false); }
  };
  return <><div className="toolbar"><Input aria-label="搜索数据库" placeholder="搜索名称" value={search} onChange={event => setSearch(event.target.value)} /><select aria-label="筛选状态" value={filter} onChange={event => setFilter(event.target.value)}><option value="all">全部状态</option>{[...new Set(groups.map(item => item.state))].map(state => <option key={state}>{state}</option>)}</select><Button variant="secondary" onClick={() => void refresh().catch(failure => setError(String(failure)))}>刷新</Button><Button variant="primary" onClick={() => setOpen(true)}>创建数据库</Button></div><ErrorNotice error={error} />
    {visible.length ? <div className="table-wrap"><Table><Table.Header><Table.Row><Table.Head>名称</Table.Head><Table.Head>状态</Table.Head><Table.Head>分支</Table.Head><Table.Head>区域</Table.Head><Table.Head /></Table.Row></Table.Header><Table.Body>{visible.map(item => <Table.Row key={item.name}><Table.Cell><button className="text-link strong" onClick={() => navigate(databasePath(project, item.name, item.branch))}>{item.name}</button></Table.Cell><Table.Cell><Badge>{item.state}</Badge></Table.Cell><Table.Cell>{item.count}</Table.Cell><Table.Cell>{catalog.projects[project]?.region ?? '—'}</Table.Cell><Table.Cell><Button variant="secondary" onClick={() => navigate(databasePath(project, item.name, item.branch))}>打开</Button></Table.Cell></Table.Row>)}</Table.Body></Table></div> : <div className="empty">{groups.length ? '没有匹配结果' : '还没有数据库'}{groups.length ? <Button variant="secondary" onClick={() => { setSearch(''); setFilter('all'); }}>清除筛选</Button> : <Button variant="primary" onClick={() => setOpen(true)}>创建数据库</Button>}</div>}
    <Modal open={open} onClose={() => { setOpen(false); setError(''); }} title="创建数据库"><form onSubmit={create} className="stack"><label>名称<Input autoFocus value={name} onChange={event => setName(event.target.value)} required /></label><span className="muted">将在 {project} 创建 main 分支。已删除的名称可能仍被占用。</span><ErrorNotice error={error} /><Button type="submit" variant="primary" disabled={!valid(name) || busy}>{busy ? '创建中…' : '创建'}</Button></form></Modal></>;
}

export function BranchPage({ call, catalog, project, refresh, databaseName, currentBranch }: PageProps & { databaseName: string; currentBranch: string }) {
  const [page, setPage] = useState(0);
  const [open, setOpen] = useState(false);
  const [name, setName] = useState('');
  const [source, setSource] = useState(currentBranch || 'main');
  const [role, setRole] = useState('writer');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const branches = Object.values(catalog.databases).filter(item => item.project === project && item.id.namespace.database === databaseName && item.state !== 'deleted').sort((first, second) => first.id.branch.localeCompare(second.id.branch));
  const visibleBranches = pageSlice(branches, page);
  const base = branches.find(item => item.id.branch === source) ?? branches[0];
  const quota = catalog.projects[project]?.quotas?.max_databases;
  const count = Object.values(catalog.databases).filter(item => item.project === project && item.state !== 'deleted').length;
  const create = async (event: FormEvent) => { event.preventDefault(); if (!base || !valid(name) || busy) return; setBusy(true); setError(''); try { await call('/v1/databases/branch', { database: base.id, name, role }); await refresh(); setOpen(false); setName(''); } catch (failure) { setError(String(failure)); } finally { setBusy(false); } };
  return <><div className="toolbar"><span className="muted">{databaseName} · {branches.length} 个分支{quota !== undefined && ` · ${count} / ${quota} 分支资源`}</span><Button variant="primary" onClick={() => setOpen(true)}>创建分支</Button></div><ErrorNotice error={error} /><div className="table-wrap"><Table><Table.Header><Table.Row><Table.Head>分支</Table.Head><Table.Head>状态</Table.Head><Table.Head>模式</Table.Head><Table.Head /></Table.Row></Table.Header><Table.Body>{visibleBranches.items.map(item => <Table.Row key={item.id.branch}><Table.Cell>{item.id.branch} {item.id.branch === currentBranch && <Badge>当前</Badge>}</Table.Cell><Table.Cell><Badge>{item.state}</Badge></Table.Cell><Table.Cell>{item.role ?? '—'}</Table.Cell><Table.Cell><Button variant="secondary" onClick={() => navigate(databasePath(project, databaseName, item.id.branch))}>打开</Button></Table.Cell></Table.Row>)}</Table.Body></Table></div>{visibleBranches.pages > 1 && <div className="pagination"><Button variant="secondary" disabled={visibleBranches.current === 0} onClick={() => setPage(visibleBranches.current - 1)}>上一页</Button><span>{visibleBranches.current + 1} / {visibleBranches.pages}</span><Button variant="secondary" disabled={visibleBranches.current + 1 >= visibleBranches.pages} onClick={() => setPage(visibleBranches.current + 1)}>下一页</Button></div>}
    <Modal open={open} onClose={() => setOpen(false)} title="创建分支"><form onSubmit={create} className="stack"><label>源分支<select value={source} onChange={event => setSource(event.target.value)}>{branches.map(item => <option key={item.id.branch}>{item.id.branch}</option>)}</select></label><label>新分支名称<Input value={name} onChange={event => setName(event.target.value)} required /></label><details><summary>高级选项</summary><label>权限模式<select value={role} onChange={event => setRole(event.target.value)}><option value="writer">可写</option><option value="read_only">只读</option></select></label></details><ErrorNotice error={error} /><Button type="submit" variant="primary" disabled={!valid(name) || busy || quota !== undefined && count >= quota}>创建分支</Button></form></Modal></>;
}
