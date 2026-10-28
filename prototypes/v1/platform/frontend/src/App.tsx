import { useEffect, useMemo, useRef, useState, type FormEvent } from 'react';
import { Button } from '@cloudflare/kumo/components/button';
import { Input } from '@cloudflare/kumo/components/input';
import { Badge } from '@cloudflare/kumo/components/badge';
import { Select } from '@cloudflare/kumo/components/select';
import { Tabs } from '@cloudflare/kumo/components/tabs';
import { Checkbox } from '@cloudflare/kumo/components/checkbox';
import { Banner } from '@cloudflare/kumo/components/banner';
import { Sidebar, useSidebar } from '@cloudflare/kumo/components/sidebar';
import { Activity, Building2 as Buildings, ChevronRight, Database as DatabaseIcon, KeyRound, Plug, Server, Settings } from 'lucide-react';
import { createAPI, type Catalog, type Database } from './api';
import { clearRememberedAPIKey, loadRememberPreference, loadRememberedAPIKey, saveRememberPreference, saveRememberedAPIKey, syncRememberedAPIKey } from './features/api-key';
import { databasePath, navigate, parseRoute, projectPath, branchesPath } from './app/routes';
import { ErrorNotice, StateBadge } from './app/ui';
import { CatalogPage, BranchPage } from './features/catalog/pages';
import { DatabasePages, ConnectionDialog } from './features/database/pages';
import { AccessPage, ProjectSettings, PlatformPages } from './features/platform/pages';
import { CreateProject } from './features/platform/CreateProject';

export type Call = ReturnType<typeof createAPI>;
export type PageProps = { call: Call; catalog: Catalog; refresh: () => Promise<void>; project: string; database?: Database; scopeKey: string; sqlCall: Call | null; setSQLCredential: (value: string) => void };

const pageTitles: Record<string, string> = {
  databases: '数据库', access: '访问凭证', settings: '项目设置', data: '数据', sql: 'SQL', branches: '分支', recovery: '恢复',
  activity: '活动', 'platform/status': '运行状态', 'platform/operations': '操作记录', 'platform/projects': '组织与项目',
  preferences: '连接与偏好', connect: '连接控制台',
};
const pageLabels = ['数据', 'SQL', '分支', '恢复', '活动', '设置'] as const;
const pageValues = ['data', 'sql', 'branches', 'recovery', 'activity', 'settings'] as const;
const storageFailureText = (failure: unknown) => `浏览器存储不可用，本次不会记住 API Key：${String(failure)}`;

function CloseMobileNavigation({ path }: { path: string }) {
  const { setOpenMobile } = useSidebar();
  useEffect(() => { setOpenMobile(false); }, [path, setOpenMobile]);
  return null;
}

export default function App() {
  const [route, setRoute] = useState(() => parseRoute(location.hash));
  const [credential, setCredential] = useState('');
  const [draft, setDraft] = useState('');
  const [sqlCredential, setSQLCredentialState] = useState<{ project: string; value: string } | null>(null);
  const [remember, setRemember] = useState(() => { try { return loadRememberPreference(localStorage); } catch { return true; } });
  const [saved, setSaved] = useState(false);
  const [admin, setAdmin] = useState<boolean | null>(null);
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [error, setError] = useState('');
  const [storageError, setStorageError] = useState('');
  const [loading, setLoading] = useState(false);
  const [identity, setIdentity] = useState(0);
  const [connectionOpen, setConnectionOpen] = useState(false);
  const [createProjectOpen, setCreateProjectOpen] = useState(false);
  const mainRef = useRef<HTMLElement>(null);
  const keyInputRef = useRef<HTMLInputElement>(null);
  const savedKeyRef = useRef('');
  const rememberRef = useRef(remember);
  const autoConnectRef = useRef(false);
  const call = useMemo(() => createAPI(() => credential), [credential]);
  const project = route.project && catalog?.projects[route.project] ? route.project : Object.keys(catalog?.projects ?? {})[0] ?? '';
  const sqlCall = useMemo(() => sqlCredential?.project === project ? createAPI(() => sqlCredential.value) : admin ? call : null, [sqlCredential, project, admin, call]);
  const selected = route.kind;
  const title = pageTitles[selected] ?? '数据库';
  const branches = Object.values(catalog?.databases ?? {}).filter(item => item.project === project && item.state !== 'deleted');
  const database = branches.find(item => item.id.namespace.database === route.database && item.id.branch === route.branch);
  const base = database?.id;
  const databaseBranches = base ? branches.filter(item => item.id.namespace.database === base.namespace.database) : [];
  const dbCount = new Set(branches.map(item => item.id.namespace.database)).size;
  const subtitle = base
    ? selected === 'branches' ? '分支列表' : selected === 'settings' ? `数据库设置 · 分支 ${base.branch}` : `${title} · 分支 ${base.branch}`
    : ({
      databases: `${dbCount} 个数据库 · ${branches.length} 个分支`,
      access: `项目 ${project} 的 SQL 与应用凭证`,
      settings: `项目 ${project} 的配额与技术信息`,
      preferences: '管理此浏览器中的 API Key 与连接',
      'platform/status': '当前网关进程的实时指标',
      'platform/operations': '跨数据库操作记录的边界说明',
      'platform/projects': '组织与项目的管理入口',
    } as Record<string, string>)[selected] ?? '';

  useEffect(() => { setSQLCredentialState(current => current?.project === project ? current : null); }, [project]);
  useEffect(() => { const update = () => setRoute(parseRoute(location.hash)); window.addEventListener('hashchange', update); return () => window.removeEventListener('hashchange', update); }, []);
  useEffect(() => {
    try {
      const preference = loadRememberPreference(localStorage);
      setRemember(preference); rememberRef.current = preference;
      const key = loadRememberedAPIKey(localStorage);
      if (!key) return;
      savedKeyRef.current = key; setSaved(true);
      if (!preference) {
        clearRememberedAPIKey(localStorage);
        savedKeyRef.current = ''; setSaved(false);
        return;
      }
      if (autoConnectRef.current) return;
      autoConnectRef.current = true;
      setDraft(key);
      setCredential(key);
    } catch (failure) {
      setStorageError(storageFailureText(failure));
    }
  }, []);
  useEffect(() => { setConnectionOpen(false); }, [route.kind, route.database, route.branch]);
  useEffect(() => { document.title = `${title} · ${project || 'Peri Loom'}`; }, [title, project]);
  useEffect(() => { mainRef.current?.scrollTo({ top: 0 }); }, [route.kind, route.project, route.database, route.branch]);

  const refresh = async () => { const next = await call<Catalog>('/v1/catalog'); if (!next?.projects || !next?.databases) throw new Error('目录响应无效'); setCatalog(next); };

  useEffect(() => {
    if (!credential) return;
    let active = true;
    setLoading(true);
    call<Catalog>('/v1/catalog').then(async next => {
      if (!next?.projects || !next?.databases) throw new Error('目录响应无效');
      if (!active) return;
      setCatalog(next); setError('');
      if (parseRoute(location.hash).kind === 'connect') navigate(projectPath(Object.keys(next.projects)[0] ?? 'default'));
      try {
        const remembered = syncRememberedAPIKey(localStorage, rememberRef.current, credential);
        savedKeyRef.current = remembered ? credential : '';
        setSaved(remembered); setStorageError('');
      } catch (failure) {
        if (active) setStorageError(storageFailureText(failure));
      }
      try { await call('/v1/metrics'); if (active) setAdmin(true); }
      catch { if (active) setAdmin(false); }
    }).catch(failure => { if (active) { setCatalog(null); setError(String(failure)); } }).finally(() => { if (active) setLoading(false); });
    return () => { active = false; };
  }, [call, credential, identity]);

  const connect = (event: FormEvent) => { event.preventDefault(); setCatalog(null); setAdmin(null); setSQLCredentialState(null); setIdentity(value => value + 1); setError(''); setStorageError(''); setCredential(draft.trim()); };
  const disconnect = () => { setCredential(''); setDraft(''); setSQLCredentialState(null); setCatalog(null); setAdmin(null); setIdentity(value => value + 1); setStorageError(''); navigate('/connect'); };
  const rememberKey = (value: boolean) => {
    setRemember(value); rememberRef.current = value;
    try {
      saveRememberPreference(localStorage, value);
      if (!value) {
        const previous = savedKeyRef.current;
        clearRememberedAPIKey(localStorage);
        savedKeyRef.current = ''; setSaved(false);
        if (draft && draft === previous) setDraft('');
      } else if (credential && catalog) {
        saveRememberedAPIKey(localStorage, credential);
        savedKeyRef.current = credential; setSaved(true);
      }
      setStorageError('');
    } catch (failure) {
      setStorageError(storageFailureText(failure));
    }
  };
  const clearSavedKey = () => {
    try {
      const previous = savedKeyRef.current;
      clearRememberedAPIKey(localStorage);
      savedKeyRef.current = ''; setSaved(false);
      if (draft && draft === previous) setDraft('');
      setError(''); setStorageError('');
      keyInputRef.current?.focus();
    } catch (failure) {
      setStorageError(storageFailureText(failure));
    }
  };
  const setSQLCredential = (value: string) => setSQLCredentialState({ project, value });
  const scopeKey = `${identity}:${project}:${database?.id.namespace.tenant ?? ''}:${route.database ?? ''}:${route.branch ?? ''}:${database?.root_incarnation ?? ''}:${String(database?.branch_incarnation ?? '')}`;
  const go = (path: string) => navigate(path);
  useEffect(() => { if (catalog && selected === 'connect') navigate(projectPath(project)); }, [catalog, selected, project]);

  if (!catalog) return <div className="connect-shell"><div className="connect-panel">
    <div className="brand connect-brand"><span className="brand-symbol">◈</span><span className="brand-label">Peri Loom</span></div>
    <h1>连接控制台</h1>
    <p className="connect-endpoint mono">{location.origin}</p>
    <form onSubmit={connect} className="stack">
      <Input ref={keyInputRef} label="API Key" description="使用平台 API Key 连接；全局管理员 Key 可管理全部项目，项目 Key 仅访问所属项目。" type="password" autoComplete="off" autoFocus value={draft} onChange={event => setDraft(event.target.value)} required placeholder="输入 API Key" />
      <div className="connect-remember" data-testid="remember-api-key">
        <Checkbox checked={remember} onCheckedChange={value => rememberKey(value === true)} label="在此浏览器记住 API Key" />
        <p className="muted small">保存在此站点的 localStorage；同一浏览器的脚本和使用者可读取。取消勾选会立即清除已保存的 API Key。</p>
      </div>
      {saved && <div className="connect-actions"><Button type="button" variant="secondary" onClick={clearSavedKey}>清除已保存的 API Key</Button></div>}
      {error && saved && <p className="muted small">已保存的 API Key 可能已失效；修改后重试，或清除后重新输入。</p>}
      {storageError && <ErrorNotice error={storageError} />}
      <ErrorNotice error={error} />
      <Button type="submit" variant="primary" className="connect-submit" disabled={loading || !draft.trim()}>{loading ? '连接中…' : '连接'}</Button>
    </form>
    <p className="connect-foot muted small">SQL 请求不会自动重试；结果待确认时请先核对服务端状态。</p>
  </div></div>;

  const props: PageProps = { call, catalog, refresh, project, database, scopeKey, sqlCall, setSQLCredential };
  const platformView = selected.startsWith('platform/');
  const workbench = (selected === 'data' || selected === 'sql') && !!base;

  return <Sidebar.Provider className="shell" collapsible="icon" mobileBreakpoint={900}><CloseMobileNavigation path={route.kind + location.hash} /><Sidebar>
    <Sidebar.Header><div className="brand sidebar-brand"><span className="brand-symbol">◈</span><span className="brand-label">Peri Loom</span></div></Sidebar.Header>
    <Sidebar.Content>
      <Sidebar.Group><Sidebar.GroupLabel>项目</Sidebar.GroupLabel><div className="project-picker"><Select aria-label="切换项目" value={project} onValueChange={value => { if (value) go(projectPath(value)); }}>{Object.keys(catalog.projects).sort((left, right) => left.localeCompare(right)).map(candidate => <Select.Option key={candidate} value={candidate}>{candidate}</Select.Option>)}</Select><span className="project-org">{catalog.projects[project]?.organization}</span>{admin === true && <Button variant="secondary" onClick={() => setCreateProjectOpen(true)}>新建项目</Button>}{admin === false && <p className="muted small">新建项目需要全局管理员 API Key</p>}</div></Sidebar.Group>
      <Sidebar.Group><Sidebar.GroupLabel>工作区</Sidebar.GroupLabel><Sidebar.Menu>
        <Sidebar.MenuButton icon={DatabaseIcon} active={['databases', 'data', 'sql', 'branches', 'recovery', 'activity'].includes(selected) || !!base} onClick={() => go(projectPath(project))}>数据库</Sidebar.MenuButton>
        <Sidebar.MenuButton icon={KeyRound} active={selected === 'access'} onClick={() => go(projectPath(project, 'access'))}>访问凭证</Sidebar.MenuButton>
        <Sidebar.MenuButton icon={Settings} active={selected === 'settings' && !base} onClick={() => go(projectPath(project, 'settings'))}>项目设置</Sidebar.MenuButton>
      </Sidebar.Menu></Sidebar.Group>
      {admin === true && <Sidebar.Group><Sidebar.GroupLabel>平台</Sidebar.GroupLabel><Sidebar.Menu>
        <Sidebar.MenuButton icon={Server} active={selected === 'platform/status'} onClick={() => go('/platform/status')}>运行状态</Sidebar.MenuButton>
        <Sidebar.MenuButton icon={Activity} active={selected === 'platform/operations'} onClick={() => go('/platform/operations')}>操作记录</Sidebar.MenuButton>
        <Sidebar.MenuButton icon={Buildings} active={selected === 'platform/projects'} onClick={() => go('/platform/projects')}>组织与项目</Sidebar.MenuButton>
      </Sidebar.Menu></Sidebar.Group>}
    </Sidebar.Content>
    <Sidebar.Footer><Sidebar.Menu><Sidebar.MenuButton icon={Plug} active={selected === 'preferences'} onClick={() => go('/preferences')}>连接与偏好</Sidebar.MenuButton></Sidebar.Menu></Sidebar.Footer>
  </Sidebar>
  <div className="main-shell">
    <header className="topbar">
      <Sidebar.Trigger className="sidebar-toggle" aria-label="切换导航" />
      <nav className="crumbs" aria-label="当前位置">
        {platformView ? <>
          <span className="crumb-muted">平台</span><ChevronRight className="crumb-sep" size={14} aria-hidden="true" /><span className="crumb-current">{title}</span>
        </> : selected === 'preferences' ? <>
          <span className="crumb-muted">{catalog.projects[project]?.organization ?? '工作区'}</span><ChevronRight className="crumb-sep" size={14} aria-hidden="true" /><span className="crumb-current">连接与偏好</span>
        </> : <>
          <span className="crumb-muted">{catalog.projects[project]?.organization ?? '工作区'}</span>
          <ChevronRight className="crumb-sep" size={14} aria-hidden="true" />
          <Button variant="ghost" className="crumb" onClick={() => go(projectPath(project))}>{project}</Button>
          {base ? <>
            <ChevronRight className="crumb-sep" size={14} aria-hidden="true" />
            <Button variant="ghost" className="crumb" onClick={() => go(databasePath(project, base.namespace.database, base.branch))}>{base.namespace.database}</Button>
            <ChevronRight className="crumb-sep" size={14} aria-hidden="true" />
            <span className="crumb-current mono">{base.branch}</span>
          </> : <>
            <ChevronRight className="crumb-sep" size={14} aria-hidden="true" />
            <span className="crumb-current">{title}</span>
          </>}
        </>}
      </nav>
      <div className="topbar-right"><Badge variant={admin === true ? 'success' : 'neutral'} appearance="dot">{admin === true ? '平台管理员' : admin === false ? '项目连接' : '权限确认中'}</Badge></div>
    </header>
    <main ref={mainRef} className={workbench ? 'content content-workbench' : 'content'}>
      {storageError && <Banner className="storage-notice" variant="alert" role="alert" description={storageError} action={<Button variant="ghost" onClick={() => setStorageError('')} aria-label="关闭存储异常提示">关闭</Button>} />}
      <div className="page-chrome">
        <div className="page-heading">
          <div className="page-heading-text">
            <div className="page-title-row">
              <h1>{base ? base.namespace.database : title}</h1>
              {base && <StateBadge state={database?.state} />}
            </div>
            {subtitle && <p className="page-subtitle">{subtitle}</p>}
          </div>
          {base && <div className="page-actions">
            <Select aria-label="切换分支" size="sm" value={base.branch} onValueChange={branch => { if (branch) go(databasePath(project, base.namespace.database, branch)); }}>{databaseBranches.map(item => <Select.Option key={item.id.branch} value={item.id.branch}>{item.id.branch}</Select.Option>)}</Select>
            <Button variant="secondary" icon={<Plug size={16} />} onClick={() => setConnectionOpen(true)}>连接</Button>
          </div>}
        </div>
        {base && <div className="workspace-nav" role="navigation" aria-label="数据库导航"><Tabs variant="underline" value={selected} onValueChange={page => go(page === 'branches' ? `${branchesPath(project, base.namespace.database)}?source=${encodeURIComponent(base.branch)}` : databasePath(project, base.namespace.database, base.branch, page))} tabs={pageValues.map((value, index) => ({ value, label: pageLabels[index] }))} /></div>}
      </div>
      <div className="page-body">
        {selected === 'databases' && <CatalogPage {...props} />}
        {selected === 'branches' && <BranchPage {...props} databaseName={route.database ?? ''} currentBranch={route.branch ?? ''} />}
        {(['data', 'sql', 'recovery', 'activity'].includes(selected) || selected === 'settings' && !!base) && <DatabasePages key={`${scopeKey}:${selected}`} {...props} page={selected} />}
        {selected === 'access' && <AccessPage {...props} />}
        {selected === 'settings' && !base && <ProjectSettings {...props} />}
        {selected.startsWith('platform/') && (admin === true ? <PlatformPages {...props} page={selected} /> : <section className="section"><h2>需要平台管理员权限</h2><p className="muted">当前凭证只能访问项目范围；请使用平台管理员 API Key 重新连接。</p></section>)}
        {selected === 'preferences' && <>
          <section className="section preferences"><h2>当前连接</h2><dl className="facts"><dt>网关</dt><dd className="mono">{location.origin}</dd><dt>当前项目</dt><dd className="mono">{project}</dd><dt>权限</dt><dd>{admin === true ? '平台管理员 API Key' : admin === false ? '项目 API Key' : '权限确认中'}</dd></dl><div className="actions"><Button variant="secondary" onClick={disconnect}>断开连接</Button></div><p className="muted small">断开后返回连接页，可更换 API Key。</p></section>
          <section className="section preferences"><h2>此浏览器</h2><div className="stack" data-testid="remember-api-key-preferences"><Checkbox checked={remember} onCheckedChange={value => rememberKey(value === true)} label="记住 API Key" /><p className="muted small">保存在此站点的 localStorage。取消勾选会立即清除已保存的 API Key，但不会断开当前连接。</p><div className="actions"><Button variant="secondary" disabled={!saved} onClick={clearSavedKey}>清除已保存的 API Key</Button></div><p className="muted small">清除后若仍勾选“记住 API Key”，下次连接成功会再次保存。</p><ErrorNotice error={storageError} /></div></section>
        </>}
      </div>
    </main>
  </div>
  {connectionOpen && base && <ConnectionDialog database={base} close={() => setConnectionOpen(false)} />}
  <CreateProject open={createProjectOpen} call={call} catalog={catalog} refresh={refresh} initialOrganization={catalog.projects[project]?.organization ?? ''} onClose={() => setCreateProjectOpen(false)} onCreated={id => { setCreateProjectOpen(false); go(projectPath(id)); }} />
  </Sidebar.Provider>;
}
