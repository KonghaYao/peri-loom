import { useEffect, useRef, useState, type FormEvent } from 'react';
import { Button } from '@cloudflare/kumo/components/button';
import { Input, InputArea } from '@cloudflare/kumo/components/input';
import { Badge } from '@cloudflare/kumo/components/badge';
import { Table } from '@cloudflare/kumo/components/table';
import { Tabs } from '@cloudflare/kumo/components/tabs';
import { Select } from '@cloudflare/kumo/components/select';
import { Collapsible } from '@cloudflare/kumo/components/collapsible';
import { DropdownMenu } from '@cloudflare/kumo/components/dropdown';
import { Banner } from '@cloudflare/kumo/components/banner';
import { Empty } from '@cloudflare/kumo/components/empty';
import type { PageProps } from '../../App';
import { type DatabaseID, APIError } from '../../api';
import { ErrorNotice, Grid, Modal } from '../../app/ui';
import { databasePath, navigate, setNavigationBlocker } from '../../app/routes';
import { isIsolatedSchema, isPendingSchema, parseSchemaTables, schemaRetryDelay, tableColumnsSQL, tablePreviewSQL, parseTableColumns } from '../table-browser';
import { parseConnectionCapabilities, hranaDatabaseURI, hranaWebSocketURI, type ConnectionCapabilities } from '../database-connection';

type Statement = { cols: { name: string; decltype: string | null }[]; rows: unknown[][]; affected_row_count: number | bigint; last_insert_rowid: number | bigint | null; autocommit: boolean };
const failureText = (failure: unknown) => failure instanceof APIError && failure.uncertain ? `结果待确认 · ${failure.message}。不要重试。` : String(failure);

function SQLAccess({ setSQLCredential }: Pick<PageProps, 'setSQLCredential'>) {
  const [key, setKey] = useState('');
  return <section className="section"><h2>查看表数据或运行 SQL</h2><p className="muted">请输入当前项目的 SQL 凭证；全局管理员可直接使用管理凭证执行 SQL。</p><form className="actions" onSubmit={event => { event.preventDefault(); setSQLCredential(key.trim()); setKey(''); }}><Input aria-label="项目 SQL 凭证" type="password" autoComplete="off" value={key} onChange={event => setKey(event.target.value)} /><Button variant="primary" type="submit" disabled={!key.trim()}>授权 SQL 操作</Button></form></section>;
}

function Connection({ database, close }: { database: DatabaseID; close: () => void }) {
  const [capabilities, setCapabilities] = useState<ConnectionCapabilities | null>(null);
  const [error, setError] = useState('');
  useEffect(() => { const controller = new AbortController(); fetch('/v1/platform/capabilities', { signal: controller.signal, cache: 'no-store' }).then(response => response.json()).then(data => { if (!controller.signal.aborted) setCapabilities(parseConnectionCapabilities(data)); }).catch(() => { if (!controller.signal.aborted) setError('无法验证接入能力，不提供 libSQL 地址。'); }); return () => controller.abort(); }, []);
  const copy = async (text: string) => { try { await navigator.clipboard.writeText(text); } catch { setError('复制失败，请手动选择地址。'); } };
  const uri = capabilities?.enabled ? hranaDatabaseURI(capabilities, database) : '';
  return <Modal open onClose={close} title="连接数据库"><p>{database.namespace.database} / {database.branch}</p><ErrorNotice error={error} /><div className="stack"><Input label="原生 API 地址" readOnly value={capabilities?.origin ?? location.origin} /><Button variant="secondary" onClick={() => void copy(capabilities?.origin ?? location.origin)}>复制 API 地址</Button>{uri ? <><Badge>实验性</Badge><p className="muted">libSQL 兼容性尚未完整验证。</p><Input label="libSQL 地址" readOnly value={uri} /><Button variant="secondary" onClick={() => void copy(uri)}>复制 libSQL 地址</Button>{capabilities?.wsEnabled && <Input label="WebSocket 地址" readOnly value={hranaWebSocketURI(capabilities, database)} />}<pre className="code">{`const db = createClient({ url: ${JSON.stringify(uri)}, authToken: process.env.PROJECT_SQL_API_KEY });`}</pre></> : <p className="muted">此网关未启用 libSQL 接入。</p>}</div></Modal>;
}

function DataPage({ database, call, sqlCall, setSQLCredential, onIsolation }: PageProps & { onIsolation: (isolated: boolean) => void }) {
  const [tables, setTables] = useState<string[]>([]);
  const [table, setTable] = useState('');
  const [search, setSearch] = useState('');
  const [view, setView] = useState('rows');
  const [columns, setColumns] = useState<string[]>([]);
  const [schemaRows, setSchemaRows] = useState<unknown[][]>([]);
  const [rows, setRows] = useState<unknown[][]>([]);
  const [error, setError] = useState('');
  const [schemaError, setSchemaError] = useState('');
  const [schemaRetryable, setSchemaRetryable] = useState(true);
  const [schemaIsolated, setSchemaIsolated] = useState(false);
  const [schemaPending, setSchemaPending] = useState(false);
  const [loading, setLoading] = useState(false);
  const [schemaLoading, setSchemaLoading] = useState(true);
  const [schemaReload, setSchemaReload] = useState(0);
  const loadingRef = useRef(false);
  const schemaRequest = useRef<{ call: PageProps['call']; key: string; promise: Promise<unknown> } | null>(null);
  const [blocked, setBlocked] = useState(false);
  const restoredTable = useRef('');
  useEffect(() => {
    if (!database) return;
    let active = true;
    let retryTimer: ReturnType<typeof setTimeout> | undefined;
    setSchemaLoading(true);
    setSchemaError('');
    setSchemaPending(false);
    const load = async (attempt: number) => {
      try {
        const key = JSON.stringify(database.id);
        if (!schemaRequest.current || schemaRequest.current.call !== call || schemaRequest.current.key !== key) {
          const promise = call<unknown>('/v1/databases/schema', { database: database.id });
          const request = { call, key, promise };
          schemaRequest.current = request;
          void promise.then(() => { if (schemaRequest.current === request) schemaRequest.current = null; }, () => { if (schemaRequest.current === request) schemaRequest.current = null; });
        }
        const result = await schemaRequest.current.promise;
        if (!active) return;
        if (isIsolatedSchema(result)) {
          if (isPendingSchema(result)) {
            setSchemaPending(true);
            const delay = [400, 800, 1600][attempt];
            if (delay !== undefined) {
              retryTimer = setTimeout(() => { retryTimer = undefined; void load(attempt + 1); }, delay);
            } else {
              setSchemaError('另一项操作仍在进行，暂时无法读取表结构。请稍后刷新表列表。');
              setSchemaRetryable(true);
            }
            return;
          }
          setSchemaPending(false);
          setSchemaIsolated(true); onIsolation(true);
          setTables([]); setTable(''); setRows([]); setColumns([]); setSchemaRows([]);
          setSchemaError(''); setSchemaRetryable(false);
          return;
        }
        const names = parseSchemaTables(result).map(item => item.name);
        if (active) { setSchemaPending(false); setSchemaIsolated(false); onIsolation(false); setTables(names); setSchemaError(''); setSchemaRetryable(true); }
      } catch (failure) {
        if (!active) return;
        if (failure instanceof APIError && failure.code === 'database_isolated') {
          setSchemaIsolated(true); onIsolation(true);
          setTables([]); setTable(''); setRows([]); setColumns([]); setSchemaRows([]);
          setSchemaError(''); setSchemaRetryable(false);
          return;
        }
        const delay = schemaRetryDelay(failure, attempt);
        if (delay !== null) {
          retryTimer = setTimeout(() => { retryTimer = undefined; void load(attempt + 1); }, delay);
          return;
        }
        setSchemaRetryable(!(failure instanceof APIError && failure.uncertain));
        setSchemaError(failure instanceof APIError && failure.code === 'operation_incomplete'
          ? '数据库正在切换状态，暂时无法读取表。请稍后刷新表列表。'
          : failure instanceof Error && failure.message.startsWith('Schema response')
              ? '网关返回的表结构不是有效行列表。请检查当前网关与数据库状态。'
              : failureText(failure));
      } finally {
        if (active && retryTimer === undefined) setSchemaLoading(false);
      }
    };
    void load(0);
    return () => { active = false; if (retryTimer !== undefined) clearTimeout(retryTimer); };
  }, [call, database, schemaReload, onIsolation]);
  const preview = async (selected: string) => {
    if (!database || !sqlCall || blocked || schemaIsolated || loadingRef.current) return;
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
  const selectTable = (selected: string) => {
    if (schemaIsolated) return;
    if (sqlCall) { void preview(selected); return; }
    setTable(selected);
    const params = new URLSearchParams(location.hash.split('?')[1] ?? '');
    params.set('table', selected);
    params.set('view', view);
    history.replaceState(null, '', `${location.pathname}${location.search}${location.hash.split('?')[0]}?${params}`);
  };
  useEffect(() => {
    const params = new URLSearchParams(location.hash.split('?')[1] ?? '');
    const selected = params.get('table') ?? '';
    if (schemaIsolated || !selected || !tables.includes(selected) || !sqlCall || restoredTable.current === selected) return;
    restoredTable.current = selected;
    setView(params.get('view') === 'schema' ? 'schema' : 'rows');
    void preview(selected);
  }, [tables, sqlCall, schemaIsolated]);
  return <div className="data-layout">
    <aside className="table-tree"><Input aria-label="搜索表" placeholder="搜索表" value={search} onChange={event => setSearch(event.target.value)} />{tables.filter(item => item.toLowerCase().includes(search.toLowerCase())).map(item => <Button key={item} variant={item === table ? 'secondary' : 'ghost'} disabled={loading || blocked || schemaIsolated} aria-current={item === table ? 'true' : undefined} onClick={() => selectTable(item)}>{item}</Button>)}{!tables.length && <p className="muted">{schemaLoading ? schemaPending ? '操作进行中，等待表结构…' : '正在加载表…' : schemaIsolated ? '数据库已隔离' : schemaError ? '加载表失败' : '暂无表'}</p>}{schemaError && schemaRetryable && !schemaLoading && <Button variant="secondary" onClick={() => setSchemaReload(value => value + 1)}>刷新表列表</Button>}</aside>
    <section className="data-panel"><div className="section-heading"><h2>{table || '选择表'}</h2>{table && <Button variant="secondary" onClick={() => navigate(databasePath(database!.project, database!.id.namespace.database, database!.id.branch, 'sql'))}>在 SQL 中打开</Button>}</div>
      {table && <Tabs tabs={[{ value: 'rows', label: '数据' }, { value: 'schema', label: '结构' }]} value={view} onValueChange={setView} />}
      <ErrorNotice error={schemaError} /><ErrorNotice error={error} />{schemaIsolated ? <><Empty title="数据库已隔离" description="表结构读取被暂停，操作结果待确认；请勿重试写入。" /><Button variant="secondary" onClick={() => navigate(databasePath(database!.project, database!.id.namespace.database, database!.id.branch, 'activity'))}>查看操作记录</Button></> : !sqlCall ? table ? <SQLAccess setSQLCredential={setSQLCredential} /> : <Empty title="选择表查看数据和结构" /> : loading ? <p role="status">加载中…</p> : table ? view === 'rows' ? <><Grid columns={columns} rows={rows} /><p className="muted">{rows.length} 行 · 最多 25 行 · 未排序</p></> : <Grid columns={['字段', '类型', '可空', '默认值', '主键']} rows={schemaRows.map(row => [row[1], row[2], row[3] === 1 ? '否' : '是', row[4] ?? '—', row[5] ? '是' : '否'])} /> : <Empty title="选择表查看数据和结构" />}
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
      <InputArea aria-label="SQL 编辑器" className="sql-editor" spellCheck={false} value={sql} onChange={event => setSQL(event.target.value)} onKeyDown={event => { if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) { event.preventDefault(); void run(); } }} />
      <div className="actions"><Button variant="primary" disabled={busy || blocked || isolated} onClick={() => void run()}>{busy ? '运行中…' : '运行 SQL'}</Button><Button variant="secondary" disabled={!session || busy || blocked || transaction} onClick={() => void close()}>结束会话</Button><span className="muted">⌘ / Ctrl + Enter</span></div>
      <ErrorNotice error={error} />{transaction && <Badge>事务进行中 · 请运行 COMMIT 或 ROLLBACK</Badge>}{blocked && <Badge>结果待确认 · 不可重试</Badge>}
      <Collapsible.Root className="disclosure"><Collapsible.DefaultTrigger>会话状态</Collapsible.DefaultTrigger><Collapsible.DefaultPanel><p>{session ? '已打开' : '未打开'} {blocked && '· 待确认'}</p></Collapsible.DefaultPanel></Collapsible.Root>
    </section>
    {result && <section className="section"><div className="section-heading"><h2>结果</h2><span className="muted">{result.rows.length} 行 · 影响 {String(result.affected_row_count)} 行 · 往返耗时 {elapsed} ms</span></div><Grid columns={result.cols.map((column, index) => column.name || `列 ${index + 1}`)} rows={result.rows.slice(0, 200)} />{result.rows.length > 200 && <p className="muted">仅显示前 200 行。请缩小查询范围。</p>}</section>}
    <Modal open={confirm} onClose={() => setConfirm(false)} title="确认写入" footer={<Button variant="primary" onClick={() => void execute()}>确认运行</Button>}><p>将在 {database?.id.namespace.database} / {database?.id.branch} 写入数据。</p></Modal>
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
      {snapshots.length ? <div className="table-wrap"><Table><Table.Header><Table.Row><Table.Head>名称</Table.Head><Table.Head>操作</Table.Head></Table.Row></Table.Header><Table.Body>{snapshots.map(snapshot => <Table.Row key={snapshot}><Table.Cell>{snapshot}</Table.Cell><Table.Cell><DropdownMenu><DropdownMenu.Trigger render={<Button variant="secondary" disabled={isolated}>操作</Button>} /><DropdownMenu.Content><DropdownMenu.Item onClick={() => { setTarget(snapshot); setConfirm('clone'); }}>创建分支</DropdownMenu.Item><DropdownMenu.Item onClick={() => { setTarget(snapshot); setConfirm('restore'); }}>恢复当前分支</DropdownMenu.Item><DropdownMenu.Item variant="danger" onClick={() => { setTarget(snapshot); setConfirm('drop'); }}>删除快照</DropdownMenu.Item></DropdownMenu.Content></DropdownMenu></Table.Cell></Table.Row>)}</Table.Body></Table></div> : <Empty title="暂无快照" />}</section>
      : <section className="section"><h2>查找恢复点</h2><Input label="请求时间 · 本地时区" type="datetime-local" value={time} onChange={event => { lookupGeneration.current++; setTime(event.target.value); setPoint(null); }} /><div className="actions"><Button variant="secondary" disabled={!time || busy} onClick={() => { const timestamp = new Date(time).getTime(); if (!Number.isSafeInteger(timestamp)) { setError('时间无效'); return; } const generation = ++lookupGeneration.current; void call<{ sequence?: bigint | number }>('/v1/databases/pitr', { database: id, timestamp_ms: timestamp }).then(reply => { if (generation !== lookupGeneration.current) return; if (reply.sequence !== undefined) setPoint(BigInt(reply.sequence)); else setError('未找到恢复点'); }).catch(failure => { if (generation === lookupGeneration.current) setError(failureText(failure)); }); }}>查找恢复点</Button>{point !== null && <>实际恢复点：sequence {point.toString()} <Button variant="destructive" disabled={isolated} onClick={() => setConfirm('point')}>恢复到当前分支</Button></>}</div></section>}
    <Modal open={!!confirm} onClose={() => { setConfirm(''); setName(''); }} title={{ create: '创建快照', clone: '从快照创建分支', drop: '删除快照', restore: '恢复快照', point: '时间点恢复' }[confirm] ?? '恢复'} danger={['drop', 'restore', 'point'].includes(confirm)} footer={confirm === 'create' || confirm === 'clone'
      ? <Button form="recovery-create-form" variant="primary" disabled={busy || !name.trim()} type="submit">创建</Button>
      : <Button variant="destructive" disabled={busy || name !== (confirm === 'drop' ? target : `${id.namespace.database}/${id.branch}`)} onClick={() => void action(confirm === 'point' ? 'restore-point' : confirm === 'drop' ? 'drop-snapshot' : 'restore', confirm === 'point' ? { sequence: point } : confirm === 'drop' ? { name: target } : { snapshot: target }, () => { setConfirm(''); setName(''); setPoint(null); })}>确认操作</Button>}>
      {confirm === 'create' || confirm === 'clone' ? <form id="recovery-create-form" onSubmit={event => { event.preventDefault(); void action(confirm === 'create' ? 'snapshot' : 'clone', confirm === 'create' ? { name } : { snapshot: target, name, role: 'writer' }, () => { setConfirm(''); setName(''); }); }} className="stack"><p>{confirm === 'clone' && `源快照：${target}`}</p><Input label={confirm === 'create' ? '快照名称' : '新分支名称'} value={name} onChange={event => setName(event.target.value)} required /></form>
        : <div className="stack"><p>{confirm === 'drop' ? `将永久删除快照 ${target}。` : `将覆盖 ${id.namespace.database} / ${id.branch} 的当前数据。此操作不可撤销。`}</p><p>{confirm === 'point' ? `恢复点 sequence ${point?.toString()}` : `快照 ${target}`}</p><Input label={`输入 ${confirm === 'drop' ? target : `${id.namespace.database}/${id.branch}`} 以确认`} value={name} onChange={event => setName(event.target.value)} /></div>}
    </Modal>
  </>;
}

function OperationRecords({ data }: { data: unknown }) {
  const records = (data as { operations?: { operation_id: string; database?: DatabaseID; action: string; state: string; business_outcome: string; cleanup_outcome: string }[] })?.operations ?? [];
  return <>{records.length ? <Grid columns={['对象', '动作', '状态', '业务结果', '清理结果']} rows={records.map(item => [item.database ? `${item.database.namespace.database}/${item.database.branch}` : '—', item.action, item.state === 'quarantined' ? '已隔离' : item.state, item.business_outcome === 'unknown' ? '结果待确认' : item.business_outcome, item.cleanup_outcome])} /> : <Empty title="没有保留的操作记录" description="未找到记录不证明请求未执行。" />}<Collapsible.Root className="disclosure"><Collapsible.DefaultTrigger>技术详情</Collapsible.DefaultTrigger><Collapsible.DefaultPanel><pre className="code">{JSON.stringify(data, (_, value) => typeof value === 'bigint' ? value.toString() : value, 2)}</pre></Collapsible.DefaultPanel></Collapsible.Root></>;
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
      {data ? page === 'activity' ? <OperationRecords data={data} /> : <Collapsible.Root className="disclosure"><Collapsible.DefaultTrigger>技术详情</Collapsible.DefaultTrigger><Collapsible.DefaultPanel><pre className="code">{JSON.stringify(data, (_, value) => typeof value === 'bigint' ? value.toString() : value, 2)}</pre></Collapsible.DefaultPanel></Collapsible.Root> : <p className="muted">加载中…</p>}
    </section>
    {page === 'settings' && <section className="section"><h2>分支管理</h2><p>{id.namespace.database} / {id.branch} · {database?.state}</p><div className="actions"><Button variant="secondary" onClick={() => setConfirm(database?.state === 'suspended' ? 'resume' : 'suspend')}>{database?.state === 'suspended' ? '恢复运行' : '暂停分支'}</Button></div>
      <Collapsible.Root className="disclosure"><Collapsible.DefaultTrigger>高级维护</Collapsible.DefaultTrigger><Collapsible.DefaultPanel><div className="actions">{['checkpoint', 'compact', 'rewrap'].map(item => <Button key={item} variant="secondary" onClick={() => setConfirm(item)}>{item}</Button>)}</div></Collapsible.DefaultPanel></Collapsible.Root>
      <div className="danger-zone"><h2>危险操作</h2><Button variant="destructive" onClick={() => setConfirm('delete')}>删除分支</Button><p className="muted">仅删除此分支，不删除整个数据库；名称可能仍被占用。</p></div></section>}
    <Modal open={!!confirm} onClose={() => { setConfirm(''); setTyped(''); }} title={confirm === 'delete' ? '删除分支' : `确认 ${confirm}`} danger={confirm === 'delete'} footer={<Button variant={confirm === 'delete' ? 'destructive' : 'primary'} disabled={busy || typed !== `${id.namespace.database}/${id.branch}`} onClick={() => void act()}>确认操作</Button>}><div className="stack"><p>目标：{id.namespace.database} / {id.branch}{confirm === 'delete' && '。删除不能撤销。'}</p><Input label={`输入 ${id.namespace.database}/${id.branch} 以确认`} value={typed} onChange={event => setTyped(event.target.value)} /></div></Modal></>;
}

export function DatabasePages(props: PageProps & { page: string }) {
  const [connection, setConnection] = useState(false);
  const [isolated, setIsolated] = useState(false);
  const [schemaIsolated, setSchemaIsolated] = useState(false);
  useEffect(() => { setSchemaIsolated(false); }, [props.scopeKey]);
  useEffect(() => {
    if (!props.database) return;
    let active = true;
    props.call<{ operations?: { state?: string; local_resources_retired?: boolean }[] }>('/v1/databases/operations', { database: props.database.id })
      .then(reply => { if (active) setIsolated(!!reply.operations?.some(record => record.state === 'quarantined' || record.local_resources_retired)); })
      .catch(() => { /* Permission to inspect operations is independent of SQL. */ });
    return () => { active = false; };
  }, [props.scopeKey, props.call]);
  if (!props.database) return <Empty title="找不到此分支" description="请返回数据库列表。" />;
  return <>{(isolated || schemaIsolated) && <Banner variant="error" role="alert" description="此数据库命名空间已隔离，可能影响所有分支；请查看活动中的处理记录，不要重试写入。" />}<div className="database-toolbar"><Select label="分支" value={props.database.id.branch} onValueChange={branch => { if (branch) navigate(databasePath(props.project, props.database!.id.namespace.database, branch, props.page)); }}>{Object.values(props.catalog.databases).filter(item => item.project === props.project && item.id.namespace.database === props.database?.id.namespace.database && item.state !== 'deleted').map(item => <Select.Option key={item.id.branch} value={item.id.branch}>{item.id.branch}</Select.Option>)}</Select><Button variant="secondary" onClick={() => setConnection(true)}>连接</Button></div>{props.page === 'data' && <DataPage {...props} onIsolation={setSchemaIsolated} />}{props.page === 'sql' && <SQLPage {...props} isolated={isolated || schemaIsolated} />}{props.page === 'recovery' && <RecoveryPage {...props} isolated={isolated || schemaIsolated} />}{['activity', 'settings'].includes(props.page) && <DetailsPage {...props} isolated={isolated || schemaIsolated} />}{connection && <Connection database={props.database.id} close={() => setConnection(false)} />}</>;
}
