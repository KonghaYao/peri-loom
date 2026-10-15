import { useEffect, useMemo, useState, type FormEvent } from 'react';
import { Button } from '@cloudflare/kumo/components/button';
import { Input } from '@cloudflare/kumo/components/input';
import { Badge } from '@cloudflare/kumo/components/badge';
import { Select } from '@cloudflare/kumo/components/select';
import { Tabs } from '@cloudflare/kumo/components/tabs';
import { Checkbox } from '@cloudflare/kumo/components/checkbox';
import { Collapsible } from '@cloudflare/kumo/components/collapsible';
import { Sidebar, useSidebar } from '@cloudflare/kumo/components/sidebar';
import { Database as DatabaseIcon, KeyRound, Settings, Activity, Server, Building2 as Buildings, Plug as Plugs } from 'lucide-react';
import { createAPI, type Catalog, type Database } from './api';
import { clearRememberedMasterKey, loadRememberedMasterKey, saveRememberedMasterKey } from './features/master-key';
import { databasePath, navigate, parseRoute, projectPath, branchesPath } from './app/routes';
import { ErrorNotice } from './app/ui';
import { CatalogPage, BranchPage } from './features/catalog/pages';
import { DatabasePages } from './features/database/pages';
import { AccessPage, ProjectSettings, PlatformPages } from './features/platform/pages';

export type Call = ReturnType<typeof createAPI>;
export type PageProps = { call: Call; catalog: Catalog; refresh: () => Promise<void>; project: string; database?: Database; scopeKey: string; sqlCall: Call | null; setSQLCredential: (value: string) => void };

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
  const [remember, setRemember] = useState(false);
  const [saved, setSaved] = useState(false);
  const [admin, setAdmin] = useState(false);
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [identity, setIdentity] = useState(0);
  const call = useMemo(() => createAPI(() => credential), [credential]);
  const project = route.project && catalog?.projects[route.project] ? route.project : Object.keys(catalog?.projects ?? {})[0] ?? '';
  const sqlCall = useMemo(() => sqlCredential?.project === project ? createAPI(() => sqlCredential.value) : admin ? call : null, [sqlCredential, project, admin, call]);
  useEffect(() => { setSQLCredentialState(current => current?.project === project ? current : null); }, [project]);
  useEffect(() => { const update = () => setRoute(parseRoute(location.hash)); window.addEventListener('hashchange', update); return () => window.removeEventListener('hashchange', update); }, []);
  useEffect(() => { try { const key = loadRememberedMasterKey(localStorage); if (key) { setDraft(key); setSaved(true); } } catch { /* Browser storage is optional. */ } }, []);
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
      try { await call('/v1/metrics'); if (active) { setAdmin(true); if (remember) { saveRememberedMasterKey(localStorage, credential); setSaved(true); } } }
      catch { if (active) setAdmin(false); }
    }).catch(failure => { if (active) { setCatalog(null); setError(String(failure)); } }).finally(() => { if (active) setLoading(false); });
    return () => { active = false; };
  }, [call, credential]);

  const connect = (event: FormEvent) => { event.preventDefault(); setCatalog(null); setAdmin(false); setSQLCredentialState(null); setIdentity(value => value + 1); setError(''); setCredential(draft.trim()); };
  const disconnect = () => { setCredential(''); setDraft(''); setSQLCredentialState(null); setCatalog(null); setAdmin(false); setIdentity(value => value + 1); navigate('/connect'); };
  const setSQLCredential = (value: string) => setSQLCredentialState({ project, value });
  const branches = Object.values(catalog?.databases ?? {}).filter(item => item.project === project && item.state !== 'deleted');
  const database = branches.find(item => item.id.namespace.database === route.database && item.id.branch === route.branch);
  const scopeKey = `${identity}:${project}:${database?.id.namespace.tenant ?? ''}:${route.database ?? ''}:${route.branch ?? ''}:${database?.root_incarnation ?? ''}:${String(database?.branch_incarnation ?? '')}`;
  const props: PageProps = { call, catalog: catalog!, refresh, project, database, scopeKey, sqlCall, setSQLCredential };
  const go = (path: string) => navigate(path);
  const selected = route.kind;

  if (!catalog) return <div className="connect-shell"><div className="connect-panel"><div className="brand"><span className="brand-symbol">◈</span> Peri Loom</div><h1>连接控制台</h1><p className="muted">{location.origin}</p><form onSubmit={connect} className="stack"><Input label="管理凭证" type="password" autoComplete="off" value={draft} onChange={event => setDraft(event.target.value)} required placeholder="输入 API key" /><Button type="submit" variant="primary" disabled={loading || !draft.trim()}>{loading ? '连接中…' : '连接'}</Button></form><Collapsible.Root className="disclosure"><Collapsible.DefaultTrigger>记住此设备</Collapsible.DefaultTrigger><Collapsible.DefaultPanel><Checkbox checked={remember} onCheckedChange={value => setRemember(value === true)} label="仅在确认是平台管理员后保存 master key" /><p className="muted">此浏览器中的脚本和使用者可能读取该 key。</p></Collapsible.DefaultPanel></Collapsible.Root>{saved && <Button variant="secondary" onClick={() => { clearRememberedMasterKey(localStorage); setSaved(false); }}>清除此设备凭证</Button>}<ErrorNotice error={error} /></div></div>;

  const base = database?.id;
  const title = ({ databases: '数据库', access: '访问凭证', settings: base ? '数据库设置' : '项目设置', data: '数据', sql: 'SQL', branches: '分支', recovery: '恢复', activity: '活动', 'platform/status': '运行状态', 'platform/operations': '操作记录', 'platform/projects': '组织与项目', preferences: '连接与偏好' } as Record<string, string>)[selected] ?? '数据库';
  const links = [
    { label: '数据库', path: projectPath(project), icon: DatabaseIcon, active: ['databases', 'data', 'sql', 'branches', 'recovery', 'activity'].includes(selected) || !!base },
    { label: '访问凭证', path: projectPath(project, 'access'), icon: KeyRound, active: selected === 'access' },
    { label: '项目设置', path: projectPath(project, 'settings'), icon: Settings, active: selected === 'settings' && !base },
  ];
  const platformLinks = [{ label: '运行状态', path: '/platform/status', icon: Server }, { label: '操作记录', path: '/platform/operations', icon: Activity }, { label: '组织与项目', path: '/platform/projects', icon: Buildings }];
  return <Sidebar.Provider className="shell" collapsible="offcanvas" mobileBreakpoint={800}><CloseMobileNavigation path={route.kind + location.hash} /><Sidebar>
    <Sidebar.Header><div className="brand sidebar-brand"><span className="brand-symbol">◈</span> Peri Loom</div></Sidebar.Header>
    <Sidebar.Content><Sidebar.Group><Sidebar.GroupLabel>项目</Sidebar.GroupLabel><div className="project-picker"><Select aria-label="切换项目" value={project} onValueChange={value => { if (value) go(projectPath(value)); }} items={Object.values(catalog.projects).map(item => ({ value: item.id, label: item.id }))} /><span className="muted small">{catalog.projects[project]?.organization}</span></div></Sidebar.Group>
      <Sidebar.Group><Sidebar.GroupLabel>工作区</Sidebar.GroupLabel><Sidebar.Menu>{links.map(link => <Sidebar.MenuButton key={link.path} icon={link.icon} active={link.active} onClick={() => go(link.path)}>{link.label}</Sidebar.MenuButton>)}</Sidebar.Menu></Sidebar.Group>
      {admin && <Sidebar.Group><Sidebar.GroupLabel>平台</Sidebar.GroupLabel><Sidebar.Menu>{platformLinks.map(link => <Sidebar.MenuButton key={link.path} icon={link.icon} active={selected === link.path.slice(1)} onClick={() => go(link.path)}>{link.label}</Sidebar.MenuButton>)}</Sidebar.Menu></Sidebar.Group>}
    </Sidebar.Content><Sidebar.Footer><Sidebar.Menu><Sidebar.MenuButton icon={Plugs} active={selected === 'preferences'} onClick={() => go('/preferences')}>连接与偏好</Sidebar.MenuButton></Sidebar.Menu></Sidebar.Footer>
  </Sidebar>
    <div className="main-shell"><header className="topbar"><Sidebar.Trigger className="mobile-toggle" aria-label="打开导航" /><span>{catalog.projects[project]?.organization ?? '工作区'} <span className="muted">/</span> {project}{base && <> <span className="muted">/</span> {base.namespace.database}</>}</span><span className="topbar-right"><Badge>{admin ? '平台管理' : '项目连接'}</Badge></span></header><main className="content"><div className="page-heading"><div>{base && <Button variant="ghost" className="text-link" onClick={() => go(projectPath(project))}>← 数据库</Button>}<h1>{base ? base.namespace.database : title}</h1>{base && <span className="muted">{title} · {base.branch}</span>}</div>{base && <Badge>{database?.state}</Badge>}</div>
      {base && <div className="workspace-nav" role="navigation" aria-label="数据库导航"><Tabs variant="underline" value={selected} onValueChange={page => go(page === 'branches' ? `${branchesPath(project, base.namespace.database)}?source=${encodeURIComponent(base.branch)}` : databasePath(project, base.namespace.database, base.branch, page))} tabs={(['data', 'sql', 'branches', 'recovery', 'activity', 'settings'] as const).map((page, index) => ({ value: page, label: ['数据', 'SQL', '分支', '恢复', '活动', '设置'][index] }))} /></div>}
      {selected === 'databases' && <CatalogPage {...props} />}
      {selected === 'branches' && <BranchPage {...props} databaseName={route.database ?? ''} currentBranch={route.branch ?? ''} />}
      {(['data', 'sql', 'recovery', 'activity'].includes(selected) || selected === 'settings' && !!base) && <DatabasePages key={`${scopeKey}:${selected}`} {...props} page={selected} />}
      {selected === 'access' && <AccessPage {...props} />}
      {selected === 'settings' && !base && <ProjectSettings {...props} />}
      {selected.startsWith('platform/') && admin && <PlatformPages {...props} page={selected} />}
      {selected === 'preferences' && <section className="section"><h2>当前连接</h2><p>网关：{location.origin}</p><p>项目：{project}</p><div className="actions"><Button variant="secondary" onClick={disconnect}>断开连接</Button><Button variant="secondary" disabled={!saved} onClick={() => { clearRememberedMasterKey(localStorage); setSaved(false); }}>清除此设备凭证</Button></div><p className="muted">清除本机凭证不会断开当前连接。</p></section>}
    </main></div></Sidebar.Provider>;
}
