import { useEffect, useRef, useState } from 'react';
import { Button } from '@cloudflare/kumo/components/button';
import { Table } from '@cloudflare/kumo/components/table';
import { Banner } from '@cloudflare/kumo/components/banner';
import { Badge } from '@cloudflare/kumo/components/badge';
import { APIError, type DatabaseID, createAPI } from '../api';
import { formatPreviewCell, parseSchemaTables, parseTableColumns, previewLimit, tableColumnsSQL, tablePreviewSQL, type SchemaTable } from './table-browser';

type Props = { database: DatabaseID; databaseKey: string; call: ReturnType<typeof createAPI> };
type Scoped<Value> = { scope: number; value: Value };
type Preview = { table: string; columns: string[]; rows: unknown[][] };

export default function TableBrowser({ database, databaseKey, call }: Props) {
  const scopeRef = useRef({ databaseKey, call, generation: 0 });
  if (scopeRef.current.databaseKey !== databaseKey || scopeRef.current.call !== call) {
    scopeRef.current = { databaseKey, call, generation: scopeRef.current.generation + 1 };
  }
  const scope = scopeRef.current.generation;
  const requestRef = useRef(0);
  const [tables, setTables] = useState<Scoped<SchemaTable[]> | null>(null);
  const [selected, setSelected] = useState<Scoped<string> | null>(null);
  const [preview, setPreview] = useState<Scoped<Preview> | null>(null);
  const [loading, setLoading] = useState<Scoped<string> | null>(null);
  const [error, setError] = useState<Scoped<string> | null>(null);
  const [closeWarning, setCloseWarning] = useState<Scoped<string> | null>(null);
  const [previewBusy, setPreviewBusy] = useState(false);
  const [reload, setReload] = useState(0);
  const busyRef = useRef(false);

  useEffect(() => {
    let active = true;
    const current = scope;
    setTables(null);
    setSelected(null);
    setPreview(null);
    setLoading({ scope: current, value: 'Loading tables…' });
    setError(null);
    void call('/v1/databases/schema', { database }).then(result => {
      if (active && scopeRef.current.generation === current) setTables({ scope: current, value: parseSchemaTables(result) });
    }).catch(failure => {
      if (active && scopeRef.current.generation === current) setError({ scope: current, value: `Cannot load table schema: ${String(failure)}. Check database access and verify server state before retrying unknown outcomes.` });
    }).finally(() => {
      if (active && scopeRef.current.generation === current) setLoading(null);
    });
    return () => { active = false; requestRef.current++; };
  }, [databaseKey, call, reload]);

  const loadPreview = async (table: string) => {
    if (busyRef.current) return;
    const current = scopeRef.current.generation;
    const request = ++requestRef.current;
    busyRef.current = true;
    setPreviewBusy(true);
    setSelected({ scope: current, value: table });
    setPreview(null);
    setError(null);
    setCloseWarning(null);
    setLoading({ scope: current, value: `Loading ${table}…` });
    let session: string | undefined;
    let sessionUnusable = false;
    let nextPreview: Preview | undefined;
    let failureMessage = '';
    let closureMessage = '';
    try {
      const opened = await call<{ session: string; readonly: boolean }>('/v1/sql/open', { database });
      session = opened.session;
      const columnsResult = await call<unknown[][]>('/v1/sql/query', { session, sql: tableColumnsSQL(table) });
      const columns = parseTableColumns(columnsResult);
      const rows = await call<unknown[][]>('/v1/sql/query', { session, sql: tablePreviewSQL(table) });
      if (rows.length > previewLimit || rows.some(row => !Array.isArray(row) || row.length !== columns.length)) throw new Error('Invalid table preview response');
      nextPreview = { table, columns, rows };
    } catch (failure) {
      failureMessage = failure instanceof APIError && failure.code === 'forbidden'
        ? 'SQL access denied. Check the credential permissions and database scope.'
        : String(failure);
      sessionUnusable = failure instanceof Error && 'uncertain' in failure && failure.uncertain === true;
    } finally {
      if (session && !sessionUnusable) {
        try {
          const closed = await call<{ closed: boolean }>('/v1/sql/close', { session });
          if (closed.closed !== true) throw new Error('Close acknowledgement missing');
        } catch (failure) {
          closureMessage = `SQL session closure is not confirmed: ${String(failure)}. Verify server session state before further actions.`;
        }
      } else if (sessionUnusable) {
        closureMessage = 'SQL session outcome is unknown; it cannot be safely closed automatically. Verify server session state before further actions.';
      }
      if (scopeRef.current.generation === current && requestRef.current === request) {
        if (failureMessage) setError({ scope: current, value: `Table preview failed: ${failureMessage}. Do not retry unknown outcomes without checking server state.` });
        if (closureMessage) setCloseWarning({ scope: current, value: closureMessage });
        if (nextPreview) setPreview({ scope: current, value: nextPreview });
        setLoading(null);
      }
      busyRef.current = false;
      setPreviewBusy(false);
    }
  };

  const visibleTables = tables?.scope === scope ? tables.value : null;
  const visibleSelected = selected?.scope === scope ? selected.value : '';
  const visiblePreview = preview?.scope === scope ? preview.value : null;
  const visibleLoading = loading?.scope === scope ? loading.value : '';
  const visibleError = error?.scope === scope ? error.value : '';
  const visibleWarning = closeWarning?.scope === scope ? closeWarning.value : '';

  return <section className="table-browser" aria-label="Database tables">
    <div className="table-browser-list" aria-label="User tables">
      <h2>Tables</h2>
      <Button type="button" variant="secondary" disabled={previewBusy || !!visibleLoading} onClick={() => setReload(previous => previous + 1)}>Refresh tables</Button>
      {visibleTables?.map(table => <Button type="button" variant={visibleSelected === table.name ? 'secondary' : 'ghost'} key={table.name} disabled={previewBusy} aria-pressed={visibleSelected === table.name} aria-current={visibleSelected === table.name ? 'true' : undefined} onClick={() => void loadPreview(table.name)}>{table.name}</Button>)}
      {visibleTables?.length === 0 && <p className="table-browser-empty">No user tables found in this database.</p>}
      {visibleTables === null && !visibleLoading && !visibleError && <p className="table-browser-empty">Loading table catalog…</p>}
    </div>
    <div className="table-browser-preview">
      <div className="table-browser-toolbar"><strong>{visiblePreview?.table ?? (visibleSelected || 'Data preview')}</strong><Badge variant="neutral">Read-only · up to {previewLimit} rows · no guaranteed order</Badge></div>
      {visibleLoading && <p role="status">{visibleLoading}</p>}
      {visibleError && <Banner variant="error" role="alert" description={visibleError} />}
      {visibleWarning && <Banner variant="alert" role="alert" description={visibleWarning} />}
      {visiblePreview ? <><p>{visiblePreview.rows.length} rows shown · limit {previewLimit} · unordered · read-only query</p><div className="table-browser-table"><Table><caption>Preview of {visiblePreview.table}</caption><Table.Header><Table.Row>{visiblePreview.columns.map((column, index) => <Table.Head key={index}>{column}</Table.Head>)}</Table.Row></Table.Header><Table.Body>{visiblePreview.rows.map((row, rowIndex) => <Table.Row key={rowIndex}>{visiblePreview.columns.map((_, index) => <Table.Cell key={index}>{formatPreviewCell(row[index] ?? null)}</Table.Cell>)}</Table.Row>)}</Table.Body></Table></div>{visiblePreview.rows.length === 0 && <p className="table-browser-empty">This table has no preview rows.</p>}</> : <p className="table-browser-empty">Select a table to preview its columns and first {previewLimit} rows.</p>}
    </div>
  </section>;
}
