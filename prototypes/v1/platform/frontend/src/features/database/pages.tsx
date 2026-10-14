import { useEffect, useRef, useState, type FormEvent } from 'react';
import { Button } from '@cloudflare/kumo/components/button';
import { Input } from '@cloudflare/kumo/components/input';
import { Badge } from '@cloudflare/kumo/components/badge';
import { Table } from '@cloudflare/kumo/components/table';
import { Tabs } from '@cloudflare/kumo/components/tabs';
import type { PageProps } from '../../App';
import { type DatabaseID, APIError } from '../../api';
import { ErrorNotice, Grid, Modal } from '../../app/ui';
import { databasePath, navigate, setNavigationBlocker } from '../../app/routes';
import { parseSchemaTables, tableColumnsSQL, tablePreviewSQL, parseTableColumns } from '../table-browser';
import { parseConnectionCapabilities, hranaDatabaseURI, hranaWebSocketURI, type ConnectionCapabilities } from '../database-connection';

type Statement = { cols: { name: string; decltype: string | null }[]; rows: unknown[][]; affected_row_count: number | bigint; last_insert_rowid: number | bigint | null; autocommit: boolean };
const failureText = (failure: unknown) => failure instanceof APIError && failure.uncertain ? `结果待确认 · ${failure.message}。不要重试。` : String(failure);

function SQLAccess({ setSQLCredential }: Pick<PageProps, 'setSQLCredential'>) {
  const [key, setKey] = useState('');
  return <section className="section"><h2>需要项目 SQL 凭证</h2><p className="muted">管理凭证不会用于执行 SQL。请添加当前项目授权的 SQL key。</p><form className="actions" onSubmit={event => { event.preventDefault(); setSQLCredential(key.trim()); setKey(''); }}><Input aria-label="项目 SQL 凭证" type="password" autoComplete="off" value={key} onChange={event => setKey(event.target.value)} /><Button variant="primary" type="submit" disabled={!key.trim()}>添加凭证</Button></form></section>;
}

function Connection({ database, close }: { database: DatabaseID; close: () => void }) {
  const [capabilities, setCapabilities] = useState<ConnectionCapabilities | null>(null);
  const [error, setError] = useState('');
  useEffect(() => { const controller = new AbortController(); fetch('/v1/platform/capabilities', { signal: controller.signal, cache: 'no-store' }).then(response => response.json()).then(data => { if (!controller.signal.aborted) setCapabilities(parseConnectionCapabilities(data)); }).catch(() => { if (!controller.signal.aborted) setError('无法验证接入能力，不提供 libSQL 地址。'); }); return () => controller.abort(); }, []);
  const copy = async (text: string) => { try { await navigator.clipboard.writeText(text); } catch { setError('复制失败，请手动选择地址。'); } };
  const uri = capabilities?.enabled ? hranaDatabaseURI(capabilities, database) : '';
  return <Modal open onClose={close} title="连接数据库"><p>{database.namespace.database} / {database.branch}</p><ErrorNotice error={error} /><div className="stack"><label>原生 API 地址<Input readOnly value={capabilities?.origin ?? location.origin} /></label><Button variant="secondary" onClick={() => void copy(capabilities?.origin ?? location.origin)}>复制 API 地址</Button>{uri ? <><Badge>实验性</Badge><p className="muted">libSQL 兼容性尚未完整验证。</p><label>libSQL 地址<Input readOnly value={uri} /></label><Button variant="secondary" onClick={() => void copy(uri)}>复制 libSQL 地址</Button>{capabilities?.wsEnabled && <label>WebSocket 地址<Input readOnly value={hranaWebSocketURI(capabilities, database)} /></label>}<pre className="code">{`const db = createClient({ url: ${JSON.stringify(uri)}, authToken: process.env.PROJECT_SQL_API_KEY });`}</pre></> : <p className="muted">此网关未启用 libSQL 接入。</p>}</div></Modal>;
}

function DataPage({ database, call, sqlCall, setSQLCredential }: PageProps) {
  const [tables, setTables] = useState<string[]>([]);
  const [table, setTable] = useState('');
  const [search, setSearch] = useState('');
  const [view, setView] = useState('rows');
  const [columns, setColumns] = useState<string[]>([]);
  const [schemaRows, setSchemaRows] = useState<unknown[][]>([]);
  const [rows, setRows] = useState<unknown[][]>([]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const loadingRef = useRef(false);
  const [blocked, setBlocked] = useState(false);
  const restoredTable = useRef('');
  useEffect(() => { if (!database) return; let active = true; call<unknown[][]>('/v1/databases/schema', { database: database.id }).then(result => { if (active) setTables(parseSchemaTables(result).map(item => item.name)); }).catch(failure => { if (active) setError(failureText(failure)); }); return () => { active = false; }; }, [call, database]);
  const preview = async (selected: string) => {
    if (!database || !sqlCall || blocked || loadingRef.current) return;
    loadingRef.current = true;
    setTable(selected); setLoading(true); setError(''); setRows([]); setColumns([]);
    const params = new URLSearchParams(location.hash.split('?')[1] ?? '');
    const selectedView = params.get('table') === selected ? params.get('view') ?? view : view;
    params.set('table', selected); params.set('view', selectedView);
    history.replaceState(null, '', `${location.pathname}${location.search}${location.hash.split('?')[0]}?${params}`);
    let session = '';
    let uncertain = false;
    try {
      session = (await sqlCall<{ session: string }>('/v1/sql/open', { database: database.id })).session;
      const schema = await sqlCall<unknown[][]>('/v1/sql/query', { session, sql: tableColumnsSQL(selected) });
      const cols = parseTableColumns(schema);
      const result = await sqlCall<unknown[][]>('/v1/sql/query', { session, sql: tablePreviewSQL(selected) });
      setColumns(cols); setRows(result); setSchemaRows(schema);
    } catch (failure) { setError(failureText(failure)); if (failure instanceof APIError && failure.uncertain) { uncertain = true; setBlocked(true); } }
    finally { if (session && !uncertain) try { await sqlCall('/v1/sql/close', { session }); } catch { setError('数据已读取，会话关闭待确认；请检查服务端会话。'); setBlocked(true); } loadingRef.current = false; setLoading(false); }
  };
  useEffect(() => {
    const params = new URLSearchParams(location.hash.split('?')[1] ?? '');
    const selected = params.get('table') ?? '';
    if (!selected || !tables.includes(selected) || !sqlCall || restoredTable.current === selected) return;
    restoredTable.current = selected;
    setView(params.get('view') === 'schema' ? 'schema' : 'rows');
    void preview(selected);
  }, [tables, sqlCall]);
  return <div className="data-layout">
    <aside className="table-tree"><Input aria-label="搜索表" placeholder="搜索表" value={search} onChange={event => setSearch(event.target.value)} />{tables.filter(item => item.toLowerCase().includes(search.toLowerCase())).map(item => <button key={item} disabled={loading || blocked} className={item === table ? 'selected' : ''} onClick={() => void preview(item)}>{item}</button>)}{!tables.length && <p className="muted">暂无表或正在加载</p>}</aside>
    <section className="data-panel"><div className="section-heading"><h2>{table || '选择表'}</h2>{table && <Button variant="secondary" onClick={() => navigate(databasePath(database!.project, database!.id.namespace.database, database!.id.branch, 'sql'))}>在 SQL 中打开</Button>}</div>
      {table && <Tabs tabs={[{ value: 'rows', label: '数据' }, { value: 'schema', label: '结构' }]} value={view} onValueChange={setView} />}
      <ErrorNotice error={error} />{!sqlCall ? <SQLAccess setSQLCredential={setSQLCredential} /> : loading ? <p role="status">加载中…</p> : table ? view === 'rows' ? <><Grid columns={columns} rows={rows} /><p className="muted">{rows.length} 行 · 最多 25 行 · 未排序</p></> : <Grid columns={['字段', '类型', '可空', '默认值', '主键']} rows={schemaRows.map(row => [row[1], row[2], row[3] === 1 ? '否' : '是', row[4] ?? '—', row[5] ? '是' : '否'])} /> : <p className="empty">选择表查看数据和结构</p>}
    </section>
  </div>;
}

function SQLPage({ database, sqlCall, setSQLCredential, isolated }: PageProps & { isolated: boolean }) {
  const [sql, setSQL] = useState('SELECT 1 AS result;');
  const [result, setResult] = useState<Statement | null>(null);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [blocked, setBlocked] = useState(false);
  const [confirm, setConfirm] = useState(false);
  const [approvedSQL, setApprovedSQL] = useState('');
  const [elapsed, setElapsed] = useState(0);
  const [session, setSession] = useState('');
  const [transaction, setTransaction] = useState(false);
  const sessionRef = useRef('');
  const busyRef = useRef(false);
  const blockedRef = useRef(false);
  const transactionRef = useRef(false);
  const execute = async () => {
    if (!database || !sqlCall || busyRef.current || blocked || isolated || !sql.trim()) return;
    busyRef.current = true; setBusy(true); setError(''); setConfirm(false);
    let handle = sessionRef.current;
    try {
      if (!handle) { handle = (await sqlCall<{ session: string }>('/v1/sql/open', { database: database.id })).session; sessionRef.current = handle; setSession(handle); }
      const start = performance.now();
      const reply = await sqlCall<Statement>('/v1/sql/statement', { session: handle, sql: confirm ? approvedSQL : sql, args: [], named_args: [] });
      if (!Array.isArray(reply.cols) || !Array.isArray(reply.rows) || typeof reply.autocommit !== 'boolean') throw new Error('语句结果格式无效');
      setElapsed(Math.round(performance.now() - start)); setResult(reply);
      transactionRef.current = !reply.autocommit;
      setTransaction(!reply.autocommit);
      if (!reply.autocommit) setError('事务未结束；离开前请明确提交或回滚。');
    } catch (failure) { setError(failureText(failure)); if (failure instanceof APIError && failure.uncertain) { blockedRef.current = true; setBlocked(true); } }
    finally { setBusy(false); busyRef.current = false; }
  };
  const run = async () => {
    if (!sqlCall || !database || busyRef.current || blocked || isolated) return;
    try {
      if (!sessionRef.current) { const opened = await sqlCall<{ session: string }>('/v1/sql/open', { database: database.id }); sessionRef.current = opened.session; setSession(opened.session); }
      const metadata = await sqlCall<{ is_readonly: boolean }>('/v1/sql/describe', { session: sessionRef.current, sql });
      if (typeof metadata.is_readonly !== 'boolean') throw new Error('服务端未明确语句权限');
      if (metadata.is_readonly) await execute(); else { setApprovedSQL(sql); setConfirm(true); }
    } catch (failure) { setError(`无法确认语句是否写入：${failureText(failure)}`); if (failure instanceof APIError && failure.uncertain) { blockedRef.current = true; setBlocked(true); } }
  };
  const close = async () => {
    if (!sessionRef.current || !sqlCall || busyRef.current || blockedRef.current || transactionRef.current) return;
    const handle = sessionRef.current;
    busyRef.current = true; setBusy(true);
    try { await sqlCall('/v1/sql/close', { session: handle }); sessionRef.current = ''; setSession(''); setError(''); }
    catch (failure) { blockedRef.current = true; setBlocked(true); setError(`会话关闭待确认：${failureText(failure)}`); }
    finally { busyRef.current = false; setBusy(false); }
  };
  useEffect(() => {
    const originalHash = location.hash;
    const guard = () => {
      if (!sessionRef.current && !busyRef.current && !blockedRef.current) return true;
      setError(transactionRef.current ? '请先明确运行 COMMIT 或 ROLLBACK，再结束会话。' : '请先结束会话；结果未知时需运维确认。');
      return false;
    };
    const onHashChange = () => {
      if (location.hash !== originalHash && !guard()) {
        history.replaceState(null, '', `${location.pathname}${location.search}${originalHash}`);
        window.dispatchEvent(new Event('hashchange'));
      }
    };
    setNavigationBlocker(guard);
    window.addEventListener('hashchange', onHashChange);
    return () => { setNavigationBlocker(null); window.removeEventListener('hashchange', onHashChange); };
  }, []);
  if (!sqlCall) return <SQLAccess setSQLCredential={setSQLCredential} />;
  return <>
    <section className="section"><div className="section-heading"><h2>编辑器</h2><span className="muted">{database?.id.namespace.database} / {database?.id.branch}</span></div>
      <textarea className="sql-editor" spellCheck={false} value={sql} onChange={event => setSQL(event.target.value)} onKeyDown={event => { if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) { event.preventDefault(); void run(); } }} />
      <div className="actions"><Button variant="primary" disabled={busy || blocked || isolated} onClick={() => void run()}>{busy ? '运行中…' : '运行 SQL'}</Button><Button variant="secondary" disabled={!session || busy || blocked || transaction} onClick={() => void close()}>结束会话</Button><span className="muted">⌘ / Ctrl + Enter</span></div>
      <ErrorNotice error={error} />{transaction && <Badge>事务进行中 · 请运行 COMMIT 或 ROLLBACK</Badge>}{blocked && <Badge>结果待确认 · 不可重试</Badge>}
      <details className="disclosure"><summary>会话状态</summary><p>{session ? '已打开' : '未打开'} {blocked && '· 待确认'}</p></details>
    </section>
    {result && <section className="section"><div className="section-heading"><h2>结果</h2><span className="muted">{result.rows.length} 行 · 影响 {String(result.affected_row_count)} 行 · 往返耗时 {elapsed} ms</span></div><Grid columns={result.cols.map((column, index) => column.name || `列 ${index + 1}`)} rows={result.rows.slice(0, 200)} />{result.rows.length > 200 && <p className="muted">仅显示前 200 行。请缩小查询范围。</p>}</section>}
    <Modal open={confirm} onClose={() => setConfirm(false)} title="确认写入"><p>将在 {database?.id.namespace.database} / {database?.id.branch} 写入数据。</p><Button variant="primary" onClick={() => void execute()}>确认运行</Button></Modal>
  </>;
}

function RecoveryPage({ database, call, isolated }: PageProps & { isolated: boolean }) {
  const [tab, setTab] = useState('snapshots');
  const [snapshots, setSnapshots] = useState<string[]>([]);
  const [name, setName] = useState('');
  const [target, setTarget] = useState('');
  const [confirm, setConfirm] = useState('');
  const [time, setTime] = useState('');
  const [point, setPoint] = useState<bigint | null>(null);
  const lookupGeneration = useRef(0);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const id = database!.id;
  const load = async () => { try { const refs = await call<{ snapshots?: Record<string, unknown> }>('/v1/databases/refs', { database: id }); setSnapshots(Object.keys(refs.snapshots ?? {})); } catch (failure) { setError(failureText(failure)); } };
  useEffect(() => { void load(); }, [call, database]);
  const action = async (path: string, body: Record<string, unknown>, after?: () => void) => { if (isolated) { setError('数据库已隔离，禁止继续更改。'); return; } setBusy(true); setError(''); try { await call(`/v1/databases/${path}`, { database: id, ...body }); after?.(); await load(); } catch (failure) { setError(failureText(failure)); } finally { setBusy(false); } };
  return <>
    <Tabs tabs={[{ value: 'snapshots', label: '快照' }, { value: 'point', label: '时间点恢复' }]} value={tab} onValueChange={setTab} />
    <ErrorNotice error={error} />
    {tab === 'snapshots' ? <section className="section"><div className="section-heading"><h2>快照</h2><Button variant="primary" disabled={isolated} onClick={() => setConfirm('create')}>创建快照</Button></div>
      <div className="table-wrap"><Table><Table.Header><Table.Row><Table.Head>名称</Table.Head><Table.Head>操作</Table.Head></Table.Row></Table.Header><Table.Body>{snapshots.map(snapshot => <Table.Row key={snapshot}><Table.Cell>{snapshot}</Table.Cell><Table.Cell><div className="actions"><Button variant="secondary" disabled={isolated} onClick={() => { setTarget(snapshot); setConfirm('clone'); }}>创建分支</Button><Button variant="secondary" disabled={isolated} onClick={() => { setTarget(snapshot); setConfirm('restore'); }}>恢复当前分支</Button><Button variant="destructive" disabled={isolated} onClick={() => { setTarget(snapshot); setConfirm('drop'); }}>删除快照</Button></div></Table.Cell></Table.Row>)}</Table.Body></Table></div>{!snapshots.length && <div className="empty">暂无快照</div>}</section>
      : <section className="section"><h2>查找恢复点</h2><label>请求时间 · 本地时区<Input type="datetime-local" value={time} onChange={event => { lookupGeneration.current++; setTime(event.target.value); setPoint(null); }} /></label><div className="actions"><Button variant="secondary" disabled={!time || busy} onClick={() => { const timestamp = new Date(time).getTime(); if (!Number.isSafeInteger(timestamp)) { setError('时间无效'); return; } const generation = ++lookupGeneration.current; void call<{ sequence?: bigint | number }>('/v1/databases/pitr', { database: id, timestamp_ms: timestamp }).then(reply => { if (generation !== lookupGeneration.current) return; if (reply.sequence !== undefined) setPoint(BigInt(reply.sequence)); else setError('未找到恢复点'); }).catch(failure => { if (generation === lookupGeneration.current) setError(failureText(failure)); }); }}>查找恢复点</Button>{point !== null && <>实际恢复点：sequence {point.toString()} <Button variant="destructive" disabled={isolated} onClick={() => setConfirm('point')}>恢复到当前分支</Button></>}</div></section>}
    <Modal open={!!confirm} onClose={() => { setConfirm(''); setName(''); }} title={{ create: '创建快照', clone: '从快照创建分支', drop: '删除快照', restore: '恢复快照', point: '时间点恢复' }[confirm] ?? '恢复'} danger={['drop', 'restore', 'point'].includes(confirm)}>
      {confirm === 'create' || confirm === 'clone' ? <form onSubmit={event => { event.preventDefault(); void action(confirm === 'create' ? 'snapshot' : 'clone', confirm === 'create' ? { name } : { snapshot: target, name, role: 'writer' }, () => { setConfirm(''); setName(''); }); }} className="stack"><p>{confirm === 'clone' && `源快照：${target}`}</p><label>{confirm === 'create' ? '快照名称' : '新分支名称'}<Input value={name} onChange={event => setName(event.target.value)} required /></label><Button variant="primary" disabled={busy || !name.trim()} type="submit">创建</Button></form>
        : <div className="stack"><p>{confirm === 'drop' ? `将永久删除快照 ${target}。` : `将覆盖 ${id.namespace.database} / ${id.branch} 的当前数据。此操作不可撤销。`}</p><p>{confirm === 'point' ? `恢复点 sequence ${point?.toString()}` : `快照 ${target}`}</p><label>输入 {confirm === 'drop' ? target : `${id.namespace.database}/${id.branch}`} 以确认<Input value={name} onChange={event => setName(event.target.value)} /></label><Button variant="destructive" disabled={busy || name !== (confirm === 'drop' ? target : `${id.namespace.database}/${id.branch}`)} onClick={() => void action(confirm === 'point' ? 'restore-point' : confirm === 'drop' ? 'drop-snapshot' : 'restore', confirm === 'point' ? { sequence: point } : confirm === 'drop' ? { name: target } : { snapshot: target }, () => { setConfirm(''); setName(''); setPoint(null); })}>确认操作</Button></div>}
    </Modal>
  </>;
}

function OperationRecords({ data }: { data: unknown }) {
  const records = (data as { operations?: { operation_id: string; database?: DatabaseID; action: string; state: string; business_outcome: string; cleanup_outcome: string }[] })?.operations ?? [];
  return <>{records.length ? <Grid columns={['对象', '动作', '状态', '业务结果', '清理结果']} rows={records.map(item => [item.database ? `${item.database.namespace.database}/${item.database.branch}` : '—', item.action, item.state === 'quarantined' ? '已隔离' : item.state, item.business_outcome === 'unknown' ? '结果待确认' : item.business_outcome, item.cleanup_outcome])} /> : <p className="empty">没有保留的操作记录。未找到记录不证明请求未执行。</p>}<details className="disclosure"><summary>技术详情</summary><pre className="code">{JSON.stringify(data, (_, value) => typeof value === 'bigint' ? value.toString() : value, 2)}</pre></details></>;
}

function DetailsPage({ database, call, page, refresh, isolated }: PageProps & { page: string; isolated: boolean }) {
  const [data, setData] = useState<unknown>(null);
  const [error, setError] = useState('');
  const [confirm, setConfirm] = useState('');
  const [typed, setTyped] = useState('');
  const [busy, setBusy] = useState(false);
  const id = database!.id;
  const endpoint = page === 'activity' ? 'operations' : 'status';
  const load = async () => { try { setData(await call(`/v1/databases/${endpoint}`, { database: id })); setError(''); } catch (failure) { setError(failureText(failure)); } };
  useEffect(() => { void load(); }, [call, database, page]);
  const act = async () => { if (isolated) { setError('数据库已隔离，禁止继续更改。'); return; } setBusy(true); setError(''); try { await call(`/v1/databases/${confirm}`, { database: id }); setConfirm(''); setTyped(''); await refresh(); await load(); } catch (failure) { setError(failureText(failure)); } finally { setBusy(false); } };
  return <>
    <section className="section"><div className="section-heading"><h2>{page === 'activity' ? '保留的操作记录' : '分支状态'}</h2><Button variant="secondary" onClick={() => void load()}>刷新</Button></div><ErrorNotice error={error} />
      {data ? page === 'activity' ? <OperationRecords data={data} /> : <details className="disclosure"><summary>技术详情</summary><pre className="code">{JSON.stringify(data, (_, value) => typeof value === 'bigint' ? value.toString() : value, 2)}</pre></details> : <p className="muted">加载中…</p>}
    </section>
    {page === 'settings' && <section className="section"><h2>分支管理</h2><p>{id.namespace.database} / {id.branch} · {database?.state}</p><div className="actions"><Button variant="secondary" onClick={() => setConfirm(database?.state === 'suspended' ? 'resume' : 'suspend')}>{database?.state === 'suspended' ? '恢复运行' : '暂停分支'}</Button></div>
      <details className="disclosure"><summary>高级维护</summary><div className="actions">{['checkpoint', 'compact', 'rewrap'].map(item => <Button key={item} variant="secondary" onClick={() => setConfirm(item)}>{item}</Button>)}</div></details>
      <div className="danger-zone"><h2>危险操作</h2><Button variant="destructive" onClick={() => setConfirm('delete')}>删除分支</Button><p className="muted">仅删除此分支，不删除整个数据库；名称可能仍被占用。</p></div></section>}
    <Modal open={!!confirm} onClose={() => { setConfirm(''); setTyped(''); }} title={confirm === 'delete' ? '删除分支' : `确认 ${confirm}`} danger={confirm === 'delete'}><div className="stack"><p>目标：{id.namespace.database} / {id.branch}{confirm === 'delete' && '。删除不能撤销。'}</p><label>输入 {id.namespace.database}/{id.branch} 以确认<Input value={typed} onChange={event => setTyped(event.target.value)} /></label><Button variant={confirm === 'delete' ? 'destructive' : 'primary'} disabled={busy || typed !== `${id.namespace.database}/${id.branch}`} onClick={() => void act()}>确认操作</Button></div></Modal></>;
}

export function DatabasePages(props: PageProps & { page: string }) {
  const [connection, setConnection] = useState(false);
  const [isolated, setIsolated] = useState(false);
  useEffect(() => {
    if (!props.database) return;
    let active = true;
    props.call<{ operations?: { state?: string; local_resources_retired?: boolean }[] }>('/v1/databases/operations', { database: props.database.id })
      .then(reply => { if (active) setIsolated(!!reply.operations?.some(record => record.state === 'quarantined' || record.local_resources_retired)); })
      .catch(() => { /* Permission to inspect operations is independent of SQL. */ });
    return () => { active = false; };
  }, [props.scopeKey, props.call]);
  if (!props.database) return <div className="empty">找不到此分支；请返回数据库列表。</div>;
  return <>{isolated && <div className="alert" role="alert">此数据库命名空间已隔离，可能影响所有分支；请查看活动中的处理记录，不要重试写入。</div>}<div className="database-toolbar"><label>分支 <select value={props.database.id.branch} onChange={event => navigate(databasePath(props.project, props.database!.id.namespace.database, event.target.value, props.page))}>{Object.values(props.catalog.databases).filter(item => item.project === props.project && item.id.namespace.database === props.database?.id.namespace.database && item.state !== 'deleted').map(item => <option key={item.id.branch}>{item.id.branch}</option>)}</select></label><Button variant="secondary" onClick={() => setConnection(true)}>连接</Button></div>{props.page === 'data' && <DataPage {...props} />}{props.page === 'sql' && <SQLPage {...props} isolated={isolated} />}{props.page === 'recovery' && <RecoveryPage {...props} isolated={isolated} />}{['activity', 'settings'].includes(props.page) && <DetailsPage {...props} isolated={isolated} />}{connection && <Connection database={props.database.id} close={() => setConnection(false)} />}</>;
}
