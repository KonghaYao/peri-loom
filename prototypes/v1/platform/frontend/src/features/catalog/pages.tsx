import { useState, type FormEvent } from 'react';
import { Button } from '@cloudflare/kumo/components/button';
import { Input } from '@cloudflare/kumo/components/input';
import { Select } from '@cloudflare/kumo/components/select';
import { Collapsible } from '@cloudflare/kumo/components/collapsible';
import { Badge } from '@cloudflare/kumo/components/badge';
import { Table } from '@cloudflare/kumo/components/table';
import { Empty } from '@cloudflare/kumo/components/empty';
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
  return <><div className="toolbar"><Input aria-label="搜索数据库" placeholder="搜索名称" value={search} onChange={event => setSearch(event.target.value)} /><Select aria-label="筛选状态" value={filter} onValueChange={value => setFilter(value ?? 'all')}><Select.Option value="all">全部状态</Select.Option>{[...new Set(groups.map(item => item.state))].map(state => <Select.Option key={state} value={state}>{state}</Select.Option>)}</Select><Button variant="secondary" onClick={() => void refresh().catch(failure => setError(String(failure)))}>刷新</Button><Button variant="primary" onClick={() => setOpen(true)}>创建数据库</Button></div><ErrorNotice error={error} />
    {visible.length ? <div className="table-wrap"><Table><Table.Header><Table.Row><Table.Head>名称</Table.Head><Table.Head>状态</Table.Head><Table.Head>分支</Table.Head><Table.Head>区域</Table.Head><Table.Head /></Table.Row></Table.Header><Table.Body>{visible.map(item => <Table.Row key={item.name}><Table.Cell><Button variant="ghost" onClick={() => navigate(databasePath(project, item.name, item.branch))}>{item.name}</Button></Table.Cell><Table.Cell><Badge>{item.state}</Badge></Table.Cell><Table.Cell>{item.count}</Table.Cell><Table.Cell>{catalog.projects[project]?.region ?? '—'}</Table.Cell><Table.Cell><Button variant="secondary" onClick={() => navigate(databasePath(project, item.name, item.branch))}>打开</Button></Table.Cell></Table.Row>)}</Table.Body></Table></div> : <Empty title={groups.length ? '没有匹配结果' : '还没有数据库'} contents={groups.length ? <Button variant="secondary" onClick={() => { setSearch(''); setFilter('all'); }}>清除筛选</Button> : <Button variant="primary" onClick={() => setOpen(true)}>创建数据库</Button>} />}
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
    <Modal open={open} onClose={() => setOpen(false)} title="创建分支"><form onSubmit={create} className="stack"><Select label="源分支" value={source} onValueChange={value => { if (value !== null) setSource(value); }}>{branches.map(item => <Select.Option key={item.id.branch} value={item.id.branch}>{item.id.branch}</Select.Option>)}</Select><label>新分支名称<Input value={name} onChange={event => setName(event.target.value)} required /></label><Collapsible.Root><Collapsible.DefaultTrigger>高级选项</Collapsible.DefaultTrigger><Collapsible.DefaultPanel><Select label="权限模式" value={role} onValueChange={value => { if (value !== null) setRole(value); }}><Select.Option value="writer">可写</Select.Option><Select.Option value="read_only">只读</Select.Option></Select></Collapsible.DefaultPanel></Collapsible.Root><ErrorNotice error={error} /><Button type="submit" variant="primary" disabled={!valid(name) || busy || quota !== undefined && count >= quota}>创建分支</Button></form></Modal></>;
}
