import { useCallback, useEffect, useMemo, useRef, useState, type FormEvent, type ReactNode } from 'react';
import { Activity, ArrowRight, BookOpen, Boxes, ChevronDown, CircleAlert, Clock3, Code2, Database as DatabaseIcon, GitBranch, KeyRound, LayoutDashboard, LockKeyhole, Pause, Play, Plus, RefreshCw, Settings2, Shield, Terminal, Trash2 } from 'lucide-react';
import { APIError, createAPI, type Catalog, type DatabaseID } from './api';
import { displayJSON } from './features/lossless';
import { clearRememberedMasterKey, loadRememberedMasterKey, saveRememberedMasterKey } from './features/master-key';
import TableBrowser from './features/TableBrowser';
import ProjectSQLKey from './features/ProjectSQLKey';
import DatabaseConnection from './features/DatabaseConnection';

type Page = 'Overview' | 'Databases' | 'SQL Editor' | 'Branches & Snapshots' | 'Operations' | 'Access' | 'Observability' | 'Settings';
type Session = { session: string; readonly: boolean; databaseKey: string; unusable?: boolean };
type RecoveryPoint = { databaseKey: string; sequence: bigint; result: unknown };
const links = [
  { title: 'Overview', icon: LayoutDashboard }, { title: 'Databases', icon: DatabaseIcon }, { title: 'SQL Editor', icon: Code2 },
  { title: 'Branches & Snapshots', icon: GitBranch }, { title: 'Operations', icon: Boxes }, { title: 'Access', icon: KeyRound },
  { title: 'Observability', icon: Activity }, { title: 'Settings', icon: Settings2 },
] as const;
const label = (id: DatabaseID) => `${id.namespace.database} / ${id.branch}`;
const valueText = (value: unknown) => value === null ? 'NULL' : typeof value === 'object' ? displayJSON(value) : String(value);
const raw = (value: unknown) => <pre className="response-output">{displayJSON(value)}</pre>;
const validName = (name: string) => /^[A-Za-z0-9_-]{1,64}$/.test(name);

function Panel({ title, children, action }: { title: string; children: ReactNode; action?: ReactNode }) {
  return <section className="panel"><div className="panel-heading"><h2>{title}</h2>{action}</div><div className="panel-body">{children}</div></section>;
}

function Rows({ rows }: { rows: unknown[][] }) {
  if (!rows.length) return <p className="empty-state">Query completed · 0 rows returned.</p>;
  return <div className="table-scroll"><table className="data-table"><caption>Query results · {rows.length} rows</caption><thead><tr>{rows[0].map((_, index) => <th key={index} scope="col">Column {index + 1}</th>)}</tr></thead><tbody>{rows.map((row, index) => <tr key={index}>{row.map((cell, column) => <td key={column}>{valueText(cell)}</td>)}</tr>)}</tbody></table></div>;
}

export default function App() {
  const [credential, setCredential] = useState('');
  const [draftCredential, setDraftCredential] = useState('');
  const [rememberMaster, setRememberMaster] = useState(false);
  const [rememberedMaster, setRememberedMaster] = useState(false);
  const [globalAdmin, setGlobalAdmin] = useState<boolean | null>(null);
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [loadingCatalog, setLoadingCatalog] = useState(false);
  const [databaseKey, setDatabaseKey] = useState('');
  const [projectID, setProjectID] = useState('');
  const [page, setPage] = useState<Page>('Overview');
  const [busy, setBusy] = useState('');
  const [notice, setNotice] = useState('');
  const [error, setError] = useState('');
  const [result, setResult] = useState<unknown>(null);
  const [detail, setDetail] = useState<Record<string, unknown>>({});
  const [detailBusy, setDetailBusy] = useState(false);
  const [session, setSession] = useState<Session | null>(null);
  const [sql, setSQL] = useState('SELECT 1;');
  const [stream, setStream] = useState(true);
  const [rows, setRows] = useState<unknown[][] | null>(null);
  const [recovery, setRecovery] = useState<RecoveryPoint | null>(null);
  const [name, setName] = useState('');
  const [snapshot, setSnapshot] = useState('');
  const [worker, setWorker] = useState('');
  const [sequence, setSequence] = useState('');
  const [timestamp, setTimestamp] = useState('');
  const [organization, setOrganization] = useState('');
  const [organizationName, setOrganizationName] = useState('');
  const [newProject, setNewProject] = useState('');
  const [region, setRegion] = useState('');
  const [newDatabase, setNewDatabase] = useState('');
  const [subject, setSubject] = useState('');
  const [role, setRole] = useState('admin');
  const [keyID, setKeyID] = useState('');
  const [secret, setSecret] = useState<unknown>(null);
  const [metrics, setMetrics] = useState<unknown>(null);
  const busyRef = useRef(false);
  const contextRef = useRef('');
  const rememberRef = useRef(false);
  const credentialRef = useRef('');
  credentialRef.current = credential;
  rememberRef.current = rememberMaster;
  contextRef.current = databaseKey;
  const call = useMemo(() => createAPI(() => credentialRef.current), [credential]);
  const projects = Object.values(catalog?.projects ?? {});
  const databases = Object.entries(catalog?.databases ?? {}).filter(([, entry]) => entry.state !== 'deleted' && (!projectID || entry.project === projectID));
  const database = catalog?.databases[databaseKey];
  const project = catalog?.projects[projectID];

  const report = (failure: unknown) => {
    const message = failure instanceof Error ? failure.message : String(failure);
    setError(message);
    setNotice('');
  };
  const run = async (title: string, action: () => Promise<unknown>, after?: (reply: unknown) => void) => {
    if (busyRef.current) return;
    busyRef.current = true;
    setSecret(null);
    setBusy(title); setError(''); setNotice('');
    try {
      const reply = await action();
      setResult(title === 'Create API key' || title === 'Issue token' ? null : reply);
      setNotice(`${title} completed · server response received`);
      after?.(reply);
    } catch (failure) {
      report(failure);
      if (failure instanceof APIError && failure.uncertain && session) setSession(previous => previous ? { ...previous, unusable: true } : null);
    } finally { busyRef.current = false; setBusy(''); }
  };
  const refresh = useCallback(async () => {
    const next = await call<Catalog>('/v1/catalog');
    if (!next || !next.projects || !next.databases) throw new Error('Invalid catalog response');
    const selectedProject = next.projects[projectID] ? projectID : Object.keys(next.projects)[0] ?? '';
    setCatalog(next);
    setProjectID(selectedProject);
    setDatabaseKey(previous => next.databases[previous]?.project === selectedProject && next.databases[previous].state !== 'deleted' ? previous : Object.entries(next.databases).find(([, entry]) => entry.project === selectedProject && entry.state !== 'deleted')?.[0] ?? '');
    return next;
  }, [call, projectID]);
  const connect = (event: FormEvent) => {
    event.preventDefault();
    if (session) { setError('Close or explicitly forget the current SQL session before changing credentials.'); return; }
    setCatalog(null); setDetail({}); setMetrics(null); setResult(null); setSecret(null); setRecovery(null);
    setGlobalAdmin(null);
    setCredential(draftCredential.trim());
    setError(''); setNotice('');
  };
  const clearMaster = () => {
    try {
      rememberRef.current = false;
      clearRememberedMasterKey(window.localStorage);
      setRememberedMaster(false);
      setRememberMaster(false);
      setNotice('Saved master API key removed from this browser. Current connection remains active until disconnected.');
      setError('');
    } catch {
      setError('Could not clear browser storage; the master API key may still be saved.');
    }
  };
  const useSavedMaster = () => {
    try {
      const saved = loadRememberedMasterKey(window.localStorage);
      if (!saved) { setRememberedMaster(false); setError('No saved master API key remains in this browser.'); return; }
      setDraftCredential(saved);
      setNotice('Saved master API key loaded into the connection field; reconnect to use it.');
      setError('');
    } catch { setError('Could not read the saved master API key from browser storage.'); }
  };
  useEffect(() => {
    try {
      const saved = loadRememberedMasterKey(window.localStorage);
      if (saved) { setRememberedMaster(true); setDraftCredential(saved); }
    } catch { setError('Browser storage is unavailable; saved master API key could not be read.'); }
  }, []);
  useEffect(() => {
    if (!credential) return;
    let live = true;
    setLoadingCatalog(true);
    call<Catalog>('/v1/catalog').then(next => {
      if (!live) return;
      if (!next || !next.projects || !next.databases) throw new Error('Invalid catalog response');
      setCatalog(next);
      const selectedProject = Object.keys(next.projects)[0] ?? '';
      setProjectID(selectedProject);
      setDatabaseKey(Object.entries(next.databases).find(([, entry]) => entry.project === selectedProject && entry.state !== 'deleted')?.[0] ?? '');
      void call('/v1/metrics').then(() => {
          if (!live || credentialRef.current !== credential) return;
          setGlobalAdmin(true);
          if (!rememberRef.current) return;
          try {
            saveRememberedMasterKey(window.localStorage, credential);
            setRememberedMaster(true);
            setNotice('Global management API key saved in this browser.');
          } catch { setError('Connected, but browser storage refused the master API key.'); }
        }).catch(failure => { if (live) {
          if (failure instanceof APIError && failure.code === 'forbidden') {
            setGlobalAdmin(false);
            if (rememberRef.current) { setRememberMaster(false); setError('Connected, but this is not a global management API key; project keys are not saved.'); }
          } else report(failure);
        } });
    }).catch(failure => { if (live) report(failure); }).finally(() => { if (live) setLoadingCatalog(false); });
    return () => { live = false; };
  }, [call, credential]);
  const activateProjectKey = (projectCredential: string) => {
    if (session || busyRef.current) { setError('Close the active SQL session before switching API keys.'); return; }
    setGlobalAdmin(null);
    setRememberMaster(false);
    setCatalog(null); setDetail({}); setMetrics(null); setResult(null); setSecret(null); setRecovery(null);
    setDraftCredential(projectCredential);
    setCredential(projectCredential);
    setError(''); setNotice('Switched to a project SQL key. It remains in this page memory only.');
  };
  useEffect(() => {
    setDetail({}); setRows(null); setResult(null); setRecovery(null);
  }, [databaseKey]);
  useEffect(() => { if (page !== 'Access') setSecret(null); }, [page]);
  const inspect = async (parts: string[]) => {
    if (!database || busyRef.current) return;
    const identity = databaseKey;
    setDetailBusy(true); setError('');
    try {
      const replies = await Promise.all(parts.map(part => call(`/v1/databases/${part}`, { database: database.id })));
      if (contextRef.current === identity) setDetail(previous => ({ ...previous, ...Object.fromEntries(parts.map((part, index) => [part, replies[index]])) }));
    } catch (failure) { if (contextRef.current === identity) report(failure); }
    finally { setDetailBusy(false); }
  };
  const act = (title: string, endpoint: string, fields: Record<string, unknown> = {}, confirmation?: string, updateCatalog = false) => {
    if (!database) return;
    if (confirmation && !window.confirm(`${confirmation}\n\n${label(database.id)} (${databaseKey})`)) return;
    const identity = databaseKey;
    void run(title, () => call(`/v1/databases/${endpoint}`, { database: database.id, ...fields }), () => {
      if (contextRef.current !== identity) return;
      setRecovery(null);
      setDetail({});
      if (updateCatalog || endpoint === 'suspend') void refresh().catch(report);
    });
  };
  const positive = (input: string): bigint | null => /^\d+$/.test(input) && BigInt(input) <= 18446744073709551615n ? BigInt(input) : null;
  const submitSequence = (input: string, action: (parsed: bigint) => void) => {
    const parsed = positive(input);
    if (parsed === null) { setError('Enter a non-negative uint64 integer.'); return; }
    action(parsed);
  };
  const operationButton = (title: string, click: () => void, disabled = false, danger = false) => <button type="button" className={`button ${danger ? 'button-danger' : ''}`} disabled={!!busy || disabled} onClick={click}>{title}</button>;
  const emptyDatabase = <div className="empty-state"><DatabaseIcon aria-hidden="true" /><p>No database selected. Select or create one to use this workspace.</p><button className="button" onClick={() => setPage('Databases')}>View databases</button></div>;
  const storageControls = <>
    <div className="credential-warning"><LockKeyhole size={16} aria-hidden="true" /><p>Optional: remember only the global management API key (master key) in this browser’s localStorage. Scripts and extensions on this origin, or anyone using this browser profile, can read it. Never use this on a shared device. Project keys and KMS keys are never saved here.</p></div>
    <div className="credential-actions"><label><input data-testid="remember-master-key" type="checkbox" checked={rememberMaster} onChange={event => setRememberMaster(event.target.checked)} /> Remember master key after verified login</label>{rememberedMaster && <button className="button" type="button" onClick={useSavedMaster}>Use saved master key</button>}<button data-testid="clear-master-key" className="button" type="button" disabled={!rememberedMaster} onClick={clearMaster}>Clear saved master key</button>{rememberedMaster && <span className="badge">Saved on this device</span>}</div>
  </>;

  return <div className="shell">
    <aside className="sidebar" aria-label="Workspace navigation">
      <div className="brand"><span className="brand-mark" aria-hidden="true">◈</span><span>Peri Loom <small>Console</small></span></div>
      <div className="sidebar-context"><span className="eyebrow">WORKSPACE</span><strong>{project?.organization ?? 'Your team'}</strong><span>{project?.id ?? 'Select a project'}</span></div>
      <nav aria-label="Main navigation">{links.map(({ title, icon: Icon }) => <button key={title} type="button" className={`sidebar-link ${page === title ? 'active' : ''}`} aria-current={page === title ? 'page' : undefined} onClick={() => setPage(title)}><Icon size={17} aria-hidden="true" /><span>{title}</span></button>)}</nav>
      <div className="sidebar-footer"><Shield size={16} aria-hidden="true" /> Direct API · no silent retries</div>
    </aside>
    <div className="workspace">
      <header className="topbar"><div className="breadcrumbs"><span>{project?.organization ?? 'Workspace'}</span><span aria-hidden="true">/</span><strong>{project?.id ?? 'Projects'}</strong><span aria-hidden="true">/</span><span>{page}</span></div><div className="topbar-actions"><label className="sr-only" htmlFor="project-select">Project</label><select id="project-select" className="input" value={projectID} disabled={!catalog || !!session || !!busy} onChange={event => { setProjectID(event.target.value); const first = Object.entries(catalog?.databases ?? {}).find(([, item]) => item.project === event.target.value && item.state !== 'deleted'); setDatabaseKey(first?.[0] ?? ''); }}>{!projects.length && <option value="">No projects</option>}{projects.map(item => <option value={item.id} key={item.id}>{item.id}</option>)}</select><label className="sr-only" htmlFor="database-select">Database</label><select id="database-select" className="input" value={databaseKey} disabled={!catalog || !!session || !!busy} onChange={event => setDatabaseKey(event.target.value)}><option value="">Select database</option>{databases.map(([key, item]) => <option key={key} value={key}>{label(item.id)}</option>)}</select><button type="button" className="button" disabled={!credential || !!busy} onClick={() => void run('Catalog refresh', refresh)} aria-label="Refresh catalog"><RefreshCw size={16} aria-hidden="true" /></button></div></header>
      <main className="page"><div className="page-header"><div><p className="eyebrow">{project?.region ? `${project.region} / ` : ''}PLATFORM</p><h1 className="page-title">{page}</h1><p className="page-subtitle">{page === 'Overview' ? 'Your live project, from catalog to database state.' : database ? label(database.id) : 'Connect and select a database to get started.'}</p></div><div className="header-actions">{database && <span className="badge">{database.state}</span>}{page !== 'SQL Editor' && database && <button className="button button-primary" onClick={() => setPage('SQL Editor')}><Terminal size={16} aria-hidden="true" /> SQL Editor <ArrowRight size={15} aria-hidden="true" /></button>}</div></div>
        <div role="status" aria-live="polite">{busy && <div className="toast">Working · {busy}. Do not repeat this request.</div>}{notice && <div className="toast">{notice}</div>}</div>
        {error && <div className="toast toast-error" role="alert"><CircleAlert size={17} aria-hidden="true" /> {error} · No automatic retry.</div>}
        {!catalog && <Panel title="Connect to platform"><form onSubmit={connect} className="form-row"><label className="field">Management API credential<input className="input" data-testid="management-credential" type="password" autoComplete="off" value={draftCredential} onChange={event => setDraftCredential(event.target.value)} placeholder="Paste your API key" required /></label><button className="button button-primary" disabled={loadingCatalog} type="submit">Connect</button></form>{loadingCatalog && <p role="status">Connecting · loading live catalog…</p>}{storageControls}</Panel>}
        {catalog && <>
          {page === 'Databases' && database && <Panel title="Connect to this database"><DatabaseConnection key={databaseKey} database={database.id} globalAdmin={globalAdmin} onGetKey={() => document.querySelector('[data-testid=project-sql-key]')?.scrollIntoView({ behavior: 'smooth' })} /></Panel>}
          {page === 'Databases' && database && globalAdmin === true && <Panel title={`Tables & table data · ${label(database.id)}`}><ProjectSQLKey key={`${projectID}:${databaseKey}`} project={database.project} database={database.id} call={call} canActivate={!session && !busy} onActivate={activateProjectKey} /></Panel>}
          {page === 'Databases' && database && globalAdmin === false && <Panel title={`Tables & table data · ${label(database.id)}`}><TableBrowser key={databaseKey} database={database.id} databaseKey={databaseKey} call={call} /></Panel>}
          {page === 'Databases' && database && globalAdmin === null && <Panel title={`Tables & table data · ${label(database.id)}`}><p role="status">Checking project SQL access before loading tables…</p></Panel>}
          {page === 'Settings' && <Panel title="Master key in this browser">{storageControls}</Panel>}
          {page === 'Overview' && <><div className="stat-grid"><div className="stat"><span className="eyebrow">PROJECTS</span><strong>{projects.length}</strong><span>Accessible projects</span></div><div className="stat"><span className="eyebrow">DATABASES</span><strong>{databases.length}</strong><span>Non-deleted in this project</span></div><div className="stat"><span className="eyebrow">CATALOG REVISION</span><strong>{String(catalog.revision)}</strong><span>From management API</span></div></div><Panel title="Current database" action={database && operationButton('Load status', () => void inspect(['status']))}>{database ? <><p><strong>{label(database.id)}</strong> <span className="badge">{database.state}</span></p><p>Tenant {database.id.namespace.tenant} · Project {database.project}</p>{detail.status !== undefined && raw(detail.status)}</> : emptyDatabase}</Panel><Panel title="Quick access"><div className="action-row"><button className="button" onClick={() => setPage('Databases')}><DatabaseIcon size={16} /> Databases</button><button className="button" onClick={() => setPage('Branches & Snapshots')}><GitBranch size={16} /> Refs & snapshots</button><button className="button" onClick={() => setPage('Observability')}><Activity size={16} /> Observability</button></div></Panel></>}
          {page === 'Databases' && <><Panel title="Project databases" action={<button className="button" disabled={!!busy} onClick={() => void run('Catalog refresh', refresh)}><RefreshCw size={15} /> Refresh</button>}>{databases.length ? <div className="table-scroll"><table className="data-table"><thead><tr><th scope="col">Database</th><th scope="col">Branch</th><th scope="col">State</th><th scope="col">Project</th><th scope="col">Action</th></tr></thead><tbody>{databases.map(([key, item]) => <tr key={key}><td>{item.id.namespace.database}</td><td>{item.id.branch}</td><td><span className="badge">{item.state}</span></td><td>{item.project}</td><td><button className="button" disabled={!!session || !!busy} onClick={() => setDatabaseKey(key)}>Select</button></td></tr>)}</tbody></table></div> : <p className="empty-state">No active databases in this project.</p>}</Panel><Panel title="Create database"><div className="form-row"><label className="field">Database name<input className="input" value={newDatabase} onChange={event => setNewDatabase(event.target.value)} placeholder="production" /></label><button className="button button-primary" disabled={!project || !validName(newDatabase) || !!busy} onClick={() => void run('Database creation', () => call('/v1/databases/create', { project: projectID, database: { namespace: { tenant: project!.tenant, database: newDatabase }, branch: 'main' }, role: 'writer' }), () => { void refresh().catch(report); })}><Plus size={16} /> Create database</button></div><p>Creates a writer database in the selected project.</p></Panel>{database && <Panel title={`Manage ${label(database.id)}`}><div className="action-row">{operationButton('Status', () => void inspect(['status']))}{database.state === 'active' && operationButton('Suspend', () => act('Suspend', 'suspend', {}, `Suspend this database?`))}{database.state === 'suspended' && operationButton('Resume', () => act('Resume', 'resume', {}, `Resume this database?`, true))}{operationButton('Delete database', () => act('Delete database', 'delete', {}, 'Permanently delete this database? This cannot be undone.', true), false, true)}</div><div className="form-row"><label className="field">Destination worker ID<input className="input" value={worker} onChange={event => setWorker(event.target.value)} placeholder="Worker ID" /></label>{operationButton('Migrate', () => act('Migrate', 'migrate', { worker_id: worker }, 'Migrate this database to the named worker?'), !validName(worker))}</div>{detail.status !== undefined && raw(detail.status)}</Panel>}</>}
          {page === 'SQL Editor' && (database ? <><Panel title="Session" action={<span className="badge">{session ? session.unusable ? 'Unusable · verify server state' : session.readonly ? 'Read-only' : 'Writer' : 'Closed'}</span>}><p>A session is bound to {label(database.id)}. Close it explicitly before switching databases.</p><div className="action-row">{operationButton('Open session', () => { const identity = databaseKey; void run('Open SQL session', () => call('/v1/sql/open', { database: database.id }), reply => { const opened = reply as { session: string; readonly: boolean }; setSession({ ...opened, databaseKey: identity }); }); }, !!session)}{operationButton('Close session', () => void run('Close SQL session', () => call('/v1/sql/close', { session: session!.session }), () => setSession(null)), !session || !!session.unusable)}{session?.unusable && operationButton('Forget unusable session', () => { if (window.confirm('Forget local session handle? This does NOT close or resolve the server session. Verify server state first.')) setSession(null); })}</div></Panel><Panel title="SQL editor"><label className="field" htmlFor="sql-input">Statement</label><textarea id="sql-input" className="textarea" rows={8} spellCheck={false} value={sql} onChange={event => setSQL(event.target.value)} /><label className="field"><input type="checkbox" checked={stream} onChange={event => setStream(event.target.checked)} /> Stream query (NDJSON)</label><div className="action-row">{operationButton('Run query', () => { setRows(null); void run('Query', () => call<unknown[][]>('/v1/sql/query', { session: session!.session, sql }, stream), reply => setRows(reply as unknown[][])); }, !session || session.unusable || !sql.trim())}{operationButton('Execute statement', () => { setRows(null); if (!window.confirm(`Execute SQL against ${label(database.id)}? Changes may be committed.`)) return; void run('Execute', () => call('/v1/sql/execute', { session: session!.session, sql })); }, !session || session.readonly || session.unusable || !sql.trim())}</div></Panel><Panel title="Results">{rows ? <Rows rows={rows} /> : result !== null ? raw(result) : <p className="empty-state">Run a query to see actual returned rows.</p>}</Panel></> : emptyDatabase)}
          {page === 'Branches & Snapshots' && (database ? <><Panel title="Reference state" action={operationButton('Load refs', () => void inspect(['refs']))}>{detail.refs === undefined ? <p className="empty-state">Load refs to inspect server-side branches and snapshots.</p> : <>{(() => { const refs = detail.refs as Record<string, unknown>; return <><div className="tabs"><span className="tab">Branches · {Object.keys(refs.branches as object ?? {}).length}</span><span className="tab">Snapshots · {Object.keys(refs.snapshots as object ?? {}).length}</span></div>{['branches', 'snapshots'].map(kind => <div key={kind}><h3>{kind}</h3>{Object.entries(refs[kind] as Record<string, unknown> ?? {}).length ? <div className="table-scroll"><table className="data-table"><thead><tr><th scope="col">Name</th><th scope="col">Reference (server)</th></tr></thead><tbody>{Object.entries(refs[kind] as Record<string, unknown>).map(([key, entry]) => <tr key={key}><td>{key}</td><td>{raw(entry)}</td></tr>)}</tbody></table></div> : <p className="empty-state">No {kind} returned.</p>}</div>)}</>; })()}</>}</Panel><Panel title="Create reference"><div className="form-row"><label className="field">New name<input className="input" value={name} onChange={event => setName(event.target.value)} placeholder="release_01" /></label>{operationButton('Create snapshot', () => act('Snapshot', 'snapshot', { name }), !validName(name))}{operationButton('Create branch', () => act('Branch', 'branch', { name }, undefined, true), !validName(name))}</div><div className="form-row"><label className="field">Existing snapshot<input className="input" value={snapshot} onChange={event => setSnapshot(event.target.value)} placeholder="Snapshot name" /></label>{operationButton('Clone snapshot to new name', () => act('Clone', 'clone', { snapshot, name }, undefined, true), !validName(snapshot) || !validName(name))}{operationButton('Restore snapshot', () => act('Restore snapshot', 'restore', { snapshot }, 'Restore this snapshot into the current database? This changes live data.', true), !validName(snapshot), true)}{operationButton('Drop snapshot', () => act('Drop snapshot', 'drop-snapshot', { name: snapshot }, 'Delete this snapshot reference?', false), !validName(snapshot), true)}</div></Panel><Panel title="Point-in-time recovery"><p>Locate a recovery point for this database. Switching databases invalidates the selected point.</p><div className="form-row"><label className="field">Sequence (uint64)<input className="input" inputMode="numeric" value={sequence} onChange={event => { setSequence(event.target.value); setRecovery(null); }} placeholder="Exact sequence" /></label><label className="field">Or timestamp (milliseconds)<input className="input" inputMode="numeric" value={timestamp} onChange={event => { setTimestamp(event.target.value); setRecovery(null); }} placeholder="Unix milliseconds" /></label>{operationButton('Find recovery point', () => submitSequence(timestamp || sequence, parsed => { const identity = databaseKey; void run('PITR lookup', () => call('/v1/databases/pitr', { database: database.id, ...(timestamp ? { timestamp_ms: parsed } : { sequence: parsed }) }), reply => { if (contextRef.current !== identity) return; const point = reply as Record<string, unknown>; const resolved = point?.sequence; if (typeof resolved === 'bigint' || typeof resolved === 'number' && Number.isSafeInteger(resolved) || typeof resolved === 'string' && /^\d+$/.test(resolved)) setRecovery({ databaseKey: identity, sequence: BigInt(resolved), result: reply }); else setError('Lookup returned no valid sequence; restore disabled.'); }); }), !timestamp && !sequence)}</div>{recovery?.databaseKey === databaseKey && <><p>Resolved sequence <strong>{recovery.sequence.toString()}</strong></p>{raw(recovery.result)}{operationButton('Restore this recovery point', () => { if (recovery.databaseKey !== databaseKey || !window.confirm(`Restore sequence ${recovery.sequence} into ${label(database.id)} (${databaseKey})? This changes live data.`)) return; act('Restore point', 'restore-point', { sequence: recovery.sequence }, undefined, true); }, false, true)}</>}</Panel></> : emptyDatabase)}
          {page === 'Operations' && (database ? <><Panel title="Operation status" action={operationButton('Load operations', () => void inspect(['operations']))}>{detail.operations !== undefined ? raw(detail.operations) : <p className="empty-state">Load operations to inspect actual server records and isolation state.</p>}</Panel><Panel title="Maintenance"><div className="action-row">{operationButton('Status', () => void inspect(['status']))}{operationButton('GC inspect', () => void inspect(['gc-inspect']))}{operationButton('Schema', () => void inspect(['schema']))}{operationButton('Diagnostics', () => void inspect(['diagnostics']))}</div><p>Maintenance changes are explicit. No automatic retry or cleanup.</p><div className="action-row">{operationButton('Checkpoint', () => act('Checkpoint', 'checkpoint', {}, 'Run a checkpoint?'))}{operationButton('Compact', () => act('Compact', 'compact', {}, 'Compact database storage?'))}{operationButton('Collect garbage', () => act('Garbage collection', 'gc', { safety_window_ms: 1000 }, 'Collect garbage with a 1000ms safety window?'))}</div><div className="form-row"><label className="field">Retain after sequence<input className="input" value={sequence} onChange={event => setSequence(event.target.value)} inputMode="numeric" /></label>{operationButton('Advance retention', () => submitSequence(sequence, parsed => act('Retain after', 'retain-after', { sequence: parsed }, `Advance retention after ${parsed}?`)), !sequence)}</div>{Object.entries(detail).filter(([key]) => key !== 'operations').map(([key, entry]) => <div key={key}><h3>{key}</h3>{raw(entry)}</div>)}</Panel></> : emptyDatabase)}
          {page === 'Access' && <><Panel title="Create API key"><div className="form-row"><label className="field">Subject<input className="input" value={subject} onChange={event => setSubject(event.target.value)} placeholder="service-name" /></label><label className="field">Role<select className="input" value={role} onChange={event => setRole(event.target.value)}><option value="admin">Admin</option><option value="developer">Developer</option><option value="viewer">Viewer</option></select></label><button className="button button-primary" disabled={!project || !subject.trim() || !!busy} onClick={() => void run('Create API key', () => call('/v1/keys/create', { subject, project: projectID, role, service_account: true }), reply => setSecret(reply))}><Plus size={16} /> Create key</button></div><p>New credentials appear once, only in this page’s memory. Do not share screenshots.</p></Panel><Panel title="Revoke API key"><div className="form-row"><label className="field">Key ID<input className="input" value={keyID} onChange={event => setKeyID(event.target.value)} /></label>{operationButton('Revoke key', () => { if (window.confirm(`Revoke key ${keyID}? This cannot be undone.`)) void run('Revoke key', () => call('/v1/keys/revoke', { id: keyID })); }, !validName(keyID), true)}</div></Panel><Panel title="Database token">{database ? <div className="action-row"><button className="button" disabled={!!busy} onClick={() => void run('Issue token', () => call('/v1/tokens', { database: database.id, role: 'writer', expires_ms: Date.now() + 120000 }), reply => setSecret(reply))}><LockKeyhole size={16} /> Issue 2-minute writer token</button></div> : emptyDatabase}</Panel>{secret !== null && <Panel title="One-time credential"><div role="status">Copy now. This value is not stored on disk and disappears when you leave or reconnect.</div>{raw(secret)}<button className="button" onClick={() => setSecret(null)}>Dismiss</button></Panel>}</>}
          {page === 'Observability' && <><Panel title="Platform metrics" action={operationButton('Load metrics', () => void run('Metrics', () => call('/v1/metrics'), setMetrics))}>{metrics !== null ? raw(metrics) : <p className="empty-state">Metrics require a platform administrator credential. No estimates are shown.</p>}</Panel>{database && <Panel title="Database inspection" action={operationButton('Load status & schema', () => void inspect(['status', 'schema']))}>{detailBusy && <p role="status">Loading inspection…</p>}{detail.status !== undefined && <><h3>Status</h3>{raw(detail.status)}</>}{detail.schema !== undefined && <><h3>Schema</h3>{raw(detail.schema)}</>}{detail.status === undefined && detail.schema === undefined && <p className="empty-state">Load live status and schema for {label(database.id)}.</p>}</Panel>}</>}
          {page === 'Settings' && <><Panel title="Project configuration">{project ? raw(project) : <p className="empty-state">Select a project.</p>}</Panel><Panel title="Create organization"><div className="form-row"><label className="field">Organization ID<input className="input" value={organization} onChange={event => setOrganization(event.target.value)} /></label><label className="field">Display name<input className="input" value={organizationName} onChange={event => setOrganizationName(event.target.value)} /></label>{operationButton('Create organization', () => void run('Create organization', () => call('/v1/organizations/create', { id: organization, name: organizationName })), !validName(organization) || !organizationName)}</div></Panel><Panel title="Create project"><div className="form-row"><label className="field">Organization ID<input className="input" value={organization} onChange={event => setOrganization(event.target.value)} /></label><label className="field">Project ID<input className="input" value={newProject} onChange={event => setNewProject(event.target.value)} /></label><label className="field">Region<input className="input" value={region} onChange={event => setRegion(event.target.value)} /></label>{operationButton('Create project', () => void run('Create project', () => call('/v1/projects/create', { organization, id: newProject, region }), () => { void refresh().catch(report); }), !validName(organization) || !validName(newProject) || !validName(region))}</div></Panel><Panel title="Connection"><form onSubmit={connect} className="form-row"><label className="field">Replace in-memory credential<input className="input" type="password" autoComplete="off" value={draftCredential} onChange={event => setDraftCredential(event.target.value)} /></label><button className="button" disabled={!!session || !!busy} type="submit">Reconnect</button></form><button className="button button-danger" disabled={!!session} onClick={() => { setCredential(''); setDraftCredential(''); setCatalog(null); setSecret(null); setRecovery(null); }}>Disconnect</button></Panel></>}
          {result !== null && page !== 'SQL Editor' && <Panel title="Latest API response">{raw(result)}</Panel>}
        </>}
        <footer className="page-footer"><BookOpen size={15} aria-hidden="true" /> Server-backed operations only · <Clock3 size={15} aria-hidden="true" /> No automatic retries</footer>
      </main>
    </div>
  </div>;
}
