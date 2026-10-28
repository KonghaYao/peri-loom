import { useRef, useState, type FormEvent } from 'react';
import { Button } from '@cloudflare/kumo/components/button';
import { Input } from '@cloudflare/kumo/components/input';
import { Select } from '@cloudflare/kumo/components/select';
import { Collapsible } from '@cloudflare/kumo/components/collapsible';
import { Table } from '@cloudflare/kumo/components/table';
import { Empty } from '@cloudflare/kumo/components/empty';
import { Pagination } from '@cloudflare/kumo/components/pagination';
import { Plus, RefreshCw, Search, X } from 'lucide-react';
import type { PageProps } from '../../App';
import { databasePath, navigate } from '../../app/routes';
import { ErrorNotice, Modal, StateBadge, useSearchShortcut } from '../../app/ui';
import { pageSlice } from './pagination';

const valid = (value: string) => /^[A-Za-z0-9_-]{1,64}$/.test(value);
const stateLabel = (state: string) => ({ active: '运行中', suspended: '已暂停', deleting: '删除中', deleted: '已删除' } as Record<string, string>)[state] ?? state;

export function CatalogPage({ call, catalog, project, refresh }: PageProps) {
  const [search, setSearch] = useState('');
  const [filter, setFilter] = useState('all');
  const [open, setOpen] = useState(false);
  const [name, setName] = useState('');
  const [busy, setBusy] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState('');
  const searchRef = useRef<HTMLInputElement>(null);
  useSearchShortcut(searchRef);
  const branches = Object.values(catalog.databases).filter(item => item.project === project && item.state !== 'deleted');
  const groups = Object.values(branches.reduce<Record<string, { name: string; count: number; state: string; branch: string }>>((all, item) => {
    const name = item.id.namespace.database;
    if (!all[name]) all[name] = { name, count: 0, state: item.state, branch: item.id.branch };
    all[name].count++;
    if (item.state !== 'active') all[name].state = item.state;
    if (item.id.branch === 'main') all[name].branch = 'main';
    return all;
  }, {})).sort((left, right) => left.name.localeCompare(right.name));
  const visible = groups.filter(item => item.name.toLowerCase().includes(search.toLowerCase()) && (filter === 'all' || item.state === filter));
  const filterOptions = [{ value: 'all', label: '全部状态' }, ...[...new Set(groups.map(item => item.state))].sort().map(state => ({ value: state, label: stateLabel(state) }))];
  const openDatabase = (item: { name: string; branch: string }) => navigate(databasePath(project, item.name, item.branch));
  const reload = async () => {
    if (refreshing) return;
    setRefreshing(true); setError('');
    try { await refresh(); } catch (failure) { setError(String(failure)); } finally { setRefreshing(false); }
  };
  const create = async (event: FormEvent) => {
    event.preventDefault(); if (!valid(name) || busy) return;
    setBusy(true); setError('');
    try { await call('/v1/databases/create', { project, database: { namespace: { tenant: catalog.projects[project].tenant, database: name }, branch: 'main' }, role: 'writer' }); await refresh(); setOpen(false); navigate(databasePath(project, name, 'main')); }
    catch (failure) { setError(String(failure)); } finally { setBusy(false); }
  };
  return <>
    <div className="toolbar">
      <div className="toolbar-lead">
        <div className="search-box">
          <Search size={15} className="search-icon" aria-hidden="true" />
          <Input ref={searchRef} aria-label="搜索数据库" placeholder="搜索数据库（按 /）" value={search} onChange={event => setSearch(event.target.value)} onKeyDown={event => { if (event.key === 'Escape') setSearch(''); }} />
          {search && <Button variant="ghost" className="search-clear" aria-label="清除搜索" icon={<X size={14} />} onClick={() => setSearch('')} />}
        </div>
        <span className="toolbar-meta">{visible.length} / {groups.length} 个数据库</span>
      </div>
      <div className="toolbar-actions">
        <Select aria-label="筛选状态" size="sm" value={filter} items={filterOptions} onValueChange={value => setFilter(value ?? 'all')}>
          {filterOptions.map(option => <Select.Option key={option.value} value={option.value}>{option.label}</Select.Option>)}
        </Select>
        <Button variant="secondary" icon={<RefreshCw size={15} />} disabled={refreshing} onClick={() => void reload()}>{refreshing ? '刷新中…' : '刷新'}</Button>
        <Button variant="primary" icon={<Plus size={16} />} onClick={() => setOpen(true)}>创建数据库</Button>
      </div>
    </div>
    <ErrorNotice error={error} />
    {visible.length ? <div className="table-wrap">
      <Table>
        <Table.Header><Table.Row><Table.Head>数据库</Table.Head><Table.Head>状态</Table.Head><Table.Head className="numeric">分支数</Table.Head><Table.Head>区域</Table.Head><Table.Head aria-label="操作" /></Table.Row></Table.Header>
        <Table.Body>{visible.map(item => <Table.Row key={item.name} className="row-clickable" onClick={() => openDatabase(item)}>
          <Table.Cell><span className="row-title mono">{item.name}</span></Table.Cell>
          <Table.Cell><StateBadge state={item.state} /></Table.Cell>
          <Table.Cell className="numeric">{item.count}</Table.Cell>
          <Table.Cell>{catalog.projects[project]?.region ?? '—'}</Table.Cell>
          <Table.Cell className="row-action"><Button variant="ghost" onClick={event => { event.stopPropagation(); openDatabase(item); }}>打开 →</Button></Table.Cell>
        </Table.Row>)}</Table.Body>
      </Table>
    </div> : <Empty title={groups.length ? '没有匹配的数据库' : '还没有数据库'} description={groups.length ? '清除筛选或换一个关键词。' : '创建一个数据库后即可开始使用。'} contents={groups.length ? <Button variant="secondary" onClick={() => { setSearch(''); setFilter('all'); }}>清除筛选</Button> : <Button variant="primary" icon={<Plus size={16} />} onClick={() => setOpen(true)}>创建数据库</Button>} />}
    <Modal open={open} onClose={() => { setOpen(false); setError(''); }} title="创建数据库" footer={<Button form="create-database-form" type="submit" variant="primary" disabled={!valid(name) || busy}>{busy ? '创建中…' : '创建'}</Button>}><form id="create-database-form" onSubmit={create} className="stack"><Input label="名称" autoFocus value={name} onChange={event => setName(event.target.value)} required /><span className="muted small">名称会作为该数据库在项目内的唯一标识；将在 {project} 创建 main 分支。已删除的名称可能仍被占用。</span><ErrorNotice error={error} /></form></Modal>
  </>;
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
  const openBranch = (branch: string) => navigate(databasePath(project, databaseName, branch));
  const create = async (event: FormEvent) => { event.preventDefault(); if (!base || !valid(name) || busy) return; setBusy(true); setError(''); try { await call('/v1/databases/branch', { database: base.id, name, role }); await refresh(); setOpen(false); setName(''); } catch (failure) { setError(String(failure)); } finally { setBusy(false); } };
  return <>
    <div className="toolbar"><div className="toolbar-lead"><span className="toolbar-meta">{branches.length} 个分支{quota !== undefined && ` · 已用 ${count} / ${quota} 份分支资源`}</span></div><div className="toolbar-actions"><Button variant="primary" icon={<Plus size={16} />} onClick={() => setOpen(true)}>创建分支</Button></div></div>
    <ErrorNotice error={error} />
    {visibleBranches.items.length ? <div className="table-wrap"><Table>
      <Table.Header><Table.Row><Table.Head>分支</Table.Head><Table.Head>状态</Table.Head><Table.Head>模式</Table.Head><Table.Head aria-label="操作" /></Table.Row></Table.Header>
      <Table.Body>{visibleBranches.items.map(item => <Table.Row key={item.id.branch} className="row-clickable" onClick={() => openBranch(item.id.branch)}>
        <Table.Cell><span className="row-title mono">{item.id.branch}</span>{item.id.branch === currentBranch && <span className="row-note">当前</span>}</Table.Cell>
        <Table.Cell><StateBadge state={item.state} /></Table.Cell>
        <Table.Cell>{item.role === 'read_only' ? '只读' : item.role === 'writer' ? '可写' : item.role ?? '—'}</Table.Cell>
        <Table.Cell className="row-action"><Button variant="ghost" onClick={event => { event.stopPropagation(); openBranch(item.id.branch); }}>打开 →</Button></Table.Cell>
      </Table.Row>)}</Table.Body>
    </Table></div> : <Empty title="还没有分支" description="从任一分支创建新的写入或只读副本。" contents={<Button variant="primary" icon={<Plus size={16} />} onClick={() => setOpen(true)}>创建分支</Button>} />}
    {visibleBranches.pages > 1 && <Pagination page={visibleBranches.current + 1} setPage={next => setPage(next - 1)} perPage={100} totalCount={branches.length} labels={{ firstPage: '首页', previousPage: '上一页', nextPage: '下一页', lastPage: '末页', pageNumber: '页码' }}><Pagination.Controls /></Pagination>}
    <Modal open={open} onClose={() => { setOpen(false); setError(''); }} title="创建分支" footer={<Button form="create-branch-form" type="submit" variant="primary" disabled={!valid(name) || busy || quota !== undefined && count >= quota}>{busy ? '创建中…' : '创建分支'}</Button>}><form id="create-branch-form" onSubmit={create} className="stack"><Select label="源分支" value={source} onValueChange={value => { if (value !== null) setSource(value); }}>{branches.map(item => <Select.Option key={item.id.branch} value={item.id.branch}>{item.id.branch}</Select.Option>)}</Select><Input label="新分支名称" value={name} onChange={event => setName(event.target.value)} required /><Collapsible.Root><Collapsible.DefaultTrigger>高级选项</Collapsible.DefaultTrigger><Collapsible.DefaultPanel><Select label="权限模式" value={role} items={[{ value: 'writer', label: '可写' }, { value: 'read_only', label: '只读' }]} onValueChange={value => { if (value !== null) setRole(value); }}><Select.Option value="writer">可写</Select.Option><Select.Option value="read_only">只读</Select.Option></Select></Collapsible.DefaultPanel></Collapsible.Root><ErrorNotice error={error} /></form></Modal>
  </>;
}
