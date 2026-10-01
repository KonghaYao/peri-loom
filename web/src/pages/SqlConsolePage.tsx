/**
 * SQL 控制台。
 *
 * 设计要点：
 *   1) 大结果集走 NDJSON 流式：Accept: application/x-ndjson，用 fetch + ReadableStream 逐行解析，
 *      边收边渲染（分块渲染，默认每次 200 行），避免一次性把完整结果集读进内存/塞进 DOM；
 *   2) 结果行数超过 MAX_BUFFER_ROWS 时主动中断流并提示加 LIMIT —— 服务端不无上限缓存，
 *      客户端同样不无上限接收；
 *   3) 显式会话模式：先 POST /sessions 拿到 session_id，之后的语句走 /sessions/{id}/query，
 *      支持 BEGIN / COMMIT / ROLLBACK（会话内事务）。
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert,
  App as AntdApp,
  Button,
  Card,
  Col,
  Descriptions,
  Form,
  Input,
  Modal,
  Row,
  Select,
  Space,
  Statistic,
  Switch,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import {
  CloseCircleOutlined,
  DatabaseOutlined,
  SaveOutlined,
  StopOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { useSearchParams } from 'react-router-dom';
import {
  api,
  executeQuery,
  isUnimplementedError,
  sessions,
  streamQuery,
  toItems,
} from '../api/client';
import {
  columnNames,
  isHeaderChunk,
  isRowChunk,
  isTrailerChunk,
  type QueryResult,
  type RowData,
  type SessionOpened,
} from '../api/types';
import { ErrorAlert } from '../components/ErrorAlert';
import { ResultTable } from '../components/ResultTable';
import { SqlEditor } from '../components/SqlEditor';
import { PREF_KEYS, usePreferences } from '../hooks/usePreferences';
import { dbName } from '../utils/database';
import { formatMicros, formatMillis } from '../utils/format';

/** 分块渲染步长：每次「继续渲染」增加的行数 */
const RENDER_STEP = 200;
/** 客户端接收上限：超过则中断流，提示使用 LIMIT */
const MAX_BUFFER_ROWS = 50_000;
/** 流式渲染刷新节流（毫秒） */
const FLUSH_INTERVAL_MS = 120;

interface DisplayResult {
  columns: string[];
  rows: unknown[][];
  /** 已接收行数（可能大于已渲染行数） */
  received: number;
  affectedRows?: number;
  walLsn?: string | number;
  elapsedMicros?: number;
  /** 流式 chunk 计数，用于展示接收进度 */
  chunks: number;
}

function fromJsonResult(res: QueryResult): DisplayResult {
  const columns = columnNames(res.columns);
  const rows = (res.rows ?? []) as RowData[];
  const normalized: unknown[][] = rows.map((row) => (Array.isArray(row) ? row : Object.values(row)));
  return {
    columns,
    rows: normalized,
    received: normalized.length,
    affectedRows: res.affected_rows,
    walLsn: res.wal_lsn,
    elapsedMicros: res.elapsed_micros,
    chunks: 0,
  };
}

export function SqlConsolePage(): JSX.Element {
  const [searchParams] = useSearchParams();
  const { message } = AntdApp.useApp();
  const { get, set: setPref } = usePreferences();

  const [dbId, setDbId] = useState<string>(() => searchParams.get('db') ?? get<string>(PREF_KEYS.defaultDatabaseId, ''));
  const [sql, setSql] = useState('select 1 as ok;');
  const [streamMode, setStreamMode] = useState<boolean>(() => get<boolean>(PREF_KEYS.sqlStream, true));
  const [sessionMode, setSessionMode] = useState(false);
  const [session, setSession] = useState<SessionOpened | null>(null);
  const [running, setRunning] = useState(false);
  const [elapsedMs, setElapsedMs] = useState<number | null>(null);
  const [result, setResult] = useState<DisplayResult | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [warning, setWarning] = useState<string | null>(null);
  const [renderLimit, setRenderLimit] = useState(RENDER_STEP);
  const [saveOpen, setSaveOpen] = useState(false);
  const [saveForm] = Form.useForm<{ name: string; description?: string }>();

  const abortRef = useRef<AbortController | null>(null);
  const sessionRef = useRef<SessionOpened | null>(null);
  sessionRef.current = session;

  // ---------------------------------------------------------------- 基础数据

  const databasesQuery = useQuery({
    queryKey: ['databases', 'console-options'],
    queryFn: () => api.databases.list({ limit: 200 }),
    staleTime: 30_000,
  });
  const databases = toItems(databasesQuery.data);

  const savedQueriesQuery = useQuery({
    queryKey: ['saved-queries', dbId],
    queryFn: () => api.savedQueries.list(),
    staleTime: 15_000,
  });
  const savedQueries = toItems(savedQueriesQuery.data);

  // 默认选中：优先用偏好里的默认数据库，其次取列表第一个
  useEffect(() => {
    if (dbId || databases.length === 0) return;
    const preferred = get<string>(PREF_KEYS.defaultDatabaseId, '');
    const matched = preferred && databases.some((db) => db.id === preferred);
    setDbId(matched ? preferred : databases[0].id);
  }, [dbId, databases, get]);

  // 切换数据库时关闭已有会话
  useEffect(() => {
    const current = sessionRef.current;
    if (current) {
      void sessions.close(current.session_id).catch(() => undefined);
      setSession(null);
    }
  }, [dbId]);

  // 卸载时清理会话
  useEffect(
    () => () => {
      const current = sessionRef.current;
      if (current) void sessions.close(current.session_id).catch(() => undefined);
    },
    [],
  );

  // ---------------------------------------------------------------- 会话管理

  const openSession = useCallback(async () => {
    setError(null);
    try {
      const opened = await sessions.open(dbId);
      setSession(opened);
      message.success(`会话已打开：${opened.session_id}`);
    } catch (err) {
      setError(err);
      setSessionMode(false);
    }
  }, [dbId, message]);

  const closeSession = useCallback(async () => {
    const current = sessionRef.current;
    setSession(null);
    setSessionMode(false);
    if (!current) return;
    try {
      await sessions.close(current.session_id);
      message.info('会话已关闭');
    } catch (err) {
      setError(err);
    }
  }, [message]);

  const toggleSessionMode = async (checked: boolean) => {
    if (checked) {
      setSessionMode(true);
      await openSession();
    } else {
      await closeSession();
    }
  };

  // ---------------------------------------------------------------- 执行

  /** 统一收口执行结果 */
  const applyResult = useCallback((next: DisplayResult) => {
    setResult(next);
    setRenderLimit(RENDER_STEP);
  }, []);

  const execute = useCallback(
    async (statement: string) => {
      const text = statement.trim();
      if (!text) return;
      if (!dbId) {
        setError(new Error('请先选择数据库'));
        return;
      }

      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      setRunning(true);
      setError(null);
      setWarning(null);
      setResult(null);
      setElapsedMs(null);

      const startedAt = performance.now();
      try {
        // 会话模式：语句走 /sessions/{id}/query（支持 BEGIN/COMMIT/ROLLBACK）
        if (sessionMode) {
          const current = sessionRef.current;
          if (!current) throw new Error('会话尚未打开');
          const res = await sessions.query(current.session_id, { sql: text }, controller.signal);
          applyResult(fromJsonResult(res));
          return;
        }

        // 流式模式：NDJSON 逐行解析 + 节流渲染
        if (streamMode) {
          let columns: string[] = [];
          let rows: unknown[][] = [];
          let affectedRows: number | undefined;
          let walLsn: string | number | undefined;
          let elapsedMicros: number | undefined;
          let chunks = 0;
          let lastFlush = 0;
          let truncated = false;

          const flush = (force: boolean) => {
            const now = performance.now();
            if (!force && now - lastFlush < FLUSH_INTERVAL_MS) return;
            lastFlush = now;
            setResult({
              columns,
              rows: rows.slice(),
              received: rows.length,
              affectedRows,
              walLsn,
              elapsedMicros,
              chunks,
            });
          };

          try {
            for await (const chunk of streamQuery(dbId, { sql: text }, controller.signal)) {
              chunks += 1;
              if (isHeaderChunk(chunk)) {
                columns = columnNames(chunk.columns);
              } else if (isRowChunk(chunk)) {
                rows.push(chunk.values);
                if (rows.length > MAX_BUFFER_ROWS) {
                  truncated = true;
                  controller.abort();
                  break;
                }
              } else if (isTrailerChunk(chunk)) {
                affectedRows = chunk.affected_rows;
                walLsn = chunk.wal_lsn;
                elapsedMicros = chunk.elapsed_micros;
              }
              flush(false);
            }
          } finally {
            rows = rows.slice(0, MAX_BUFFER_ROWS); // abort 后可能略超上限
            flush(true);
          }

          if (truncated) {
            setWarning(
              `结果超过 ${MAX_BUFFER_ROWS} 行，已停止接收（服务端不缓存完整结果集，客户端同样不无上限接收）。请为查询加上 LIMIT 后重试。`,
            );
          }
          return;
        }

        // 普通 JSON 模式：适合小结果集 / DDL
        const res = await executeQuery(dbId, { sql: text }, controller.signal);
        applyResult(fromJsonResult(res));
      } catch (err) {
        // 流式不被支持时（404/501）自动回退到 JSON 模式
        if (streamMode && isUnimplementedError(err)) {
          setWarning('服务端未启用 NDJSON 流式响应，已回退为普通 JSON 模式。');
          setStreamMode(false);
        } else {
          setError(err);
        }
      } finally {
        setElapsedMs(performance.now() - startedAt);
        setRunning(false);
        abortRef.current = null;
      }
    },
    [applyResult, dbId, sessionMode, streamMode],
  );

  const handleRun = () => void execute(sql);

  const handleCancel = () => {
    abortRef.current?.abort();
    message.info('已请求中断（前端停止接收；服务端策略取决于实现）');
  };

  // ---------------------------------------------------------------- Saved SQL

  const handleSave = async () => {
    const values = await saveForm.validateFields();
    try {
      await api.savedQueries.create({
        name: values.name.trim(),
        sql,
        ...(dbId ? { database_id: dbId } : {}),
        ...(values.description?.trim() ? { description: values.description.trim() } : {}),
      });
      message.success('已保存');
      setSaveOpen(false);
      saveForm.resetFields();
      void savedQueriesQuery.refetch();
    } catch (err) {
      setError(err);
      setSaveOpen(false);
    }
  };

  const dbOptions = useMemo(
    () =>
      databases.map((db) => ({
        value: db.id,
        label: `${dbName(db)}（${db.state}）`,
      })),
    [databases],
  );

  const renderedRows = result ? result.rows.slice(0, renderLimit) : [];

  return (
    <div>
      <Card
        title={
          <Space>
            <DatabaseOutlined />
            <span>SQL 控制台</span>
          </Space>
        }
        extra={
          <Space wrap>
            <Select
              showSearch
              style={{ width: 260 }}
              placeholder="选择数据库"
              optionFilterProp="label"
              value={dbId || undefined}
              onChange={(value) => {
                setDbId(value);
                void setPref(PREF_KEYS.defaultDatabaseId, value).catch(() => undefined);
              }}
              loading={databasesQuery.isLoading}
              options={dbOptions}
            />
            <Select
              allowClear
              style={{ width: 220 }}
              placeholder="载入 Saved SQL"
              value={undefined}
              onSelect={(id) => {
                const item = savedQueries.find((q) => q.id === id);
                if (item) {
                  setSql(item.sql);
                  message.success(`已载入：${item.name}`);
                }
              }}
              options={savedQueries.map((q) => ({ value: q.id, label: q.name }))}
            />
          </Space>
        }
      >
        <ErrorAlert error={databasesQuery.error} onRetry={() => databasesQuery.refetch()} />

        <SqlEditor
          value={sql}
          onChange={setSql}
          onRun={handleRun}
          onClear={() => setSql('')}
          running={running}
          height={220}
          extra={
            <Space wrap>
              <Tooltip title="流式（NDJSON）：大结果集逐行接收、分块渲染">
                <Space size={6}>
                  <Switch
                    size="small"
                    checked={streamMode && !sessionMode}
                    disabled={sessionMode}
                    onChange={(checked) => {
                      setStreamMode(checked);
                      void setPref(PREF_KEYS.sqlStream, checked).catch(() => undefined);
                    }}
                  />
                  <Typography.Text type="secondary">流式</Typography.Text>
                </Space>
              </Tooltip>
              <Tooltip title="显式会话：在同一个 session 内执行多条语句，支持 BEGIN/COMMIT/ROLLBACK">
                <Space size={6}>
                  <Switch size="small" checked={sessionMode} onChange={(checked) => void toggleSessionMode(checked)} />
                  <Typography.Text type="secondary">会话</Typography.Text>
                </Space>
              </Tooltip>
              <Button icon={<SaveOutlined />} onClick={() => setSaveOpen(true)} disabled={!sql.trim()}>
                保存
              </Button>
              {running ? (
                <Button danger icon={<StopOutlined />} onClick={handleCancel}>
                  中断
                </Button>
              ) : null}
            </Space>
          }
        />

        {/* 会话模式工具条 */}
        {sessionMode ? (
          <Card size="small" style={{ marginTop: 12 }}>
            <Space wrap size={12}>
              <Tag color="blue">session_id: {session?.session_id ?? '打开中…'}</Tag>
              <Typography.Text type="secondary">
                {session?.expires_at_unix_ms
                  ? `过期时间：${new Date(session.expires_at_unix_ms).toLocaleString()}`
                  : '会话内语句共享事务上下文'}
              </Typography.Text>
              <Button size="small" onClick={() => void execute('BEGIN')} disabled={running}>
                BEGIN
              </Button>
              <Button size="small" onClick={() => void execute('COMMIT')} disabled={running}>
                COMMIT
              </Button>
              <Button size="small" onClick={() => void execute('ROLLBACK')} disabled={running}>
                ROLLBACK
              </Button>
              <Button size="small" icon={<CloseCircleOutlined />} onClick={() => void closeSession()}>
                关闭会话
              </Button>
            </Space>
          </Card>
        ) : null}

        {warning ? (
          <Alert type="warning" showIcon style={{ marginTop: 12 }} message={warning} closable onClose={() => setWarning(null)} />
        ) : null}
        <div style={{ marginTop: 12 }}>
          <ErrorAlert error={error} onRetry={() => void execute(sql)} alwaysRetry />
        </div>

        {/* 执行统计 */}
        {result || elapsedMs !== null ? (
          <Row gutter={[16, 8]} style={{ marginTop: 8 }}>
            <Col xs={12} md={6}>
              <Statistic title="返回行数" value={result?.received ?? 0} />
            </Col>
            <Col xs={12} md={6}>
              <Statistic title="影响行数" value={result?.affectedRows ?? 0} />
            </Col>
            <Col xs={12} md={6}>
              <Statistic
                title="服务端耗时"
                value={result?.elapsedMicros !== undefined ? formatMicros(result.elapsedMicros) : '-'}
              />
            </Col>
            <Col xs={12} md={6}>
              <Statistic title="端到端耗时" value={elapsedMs === null ? '-' : formatMillis(elapsedMs)} />
              {result?.chunks ? (
                <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                  流式分片：{result.chunks}
                </Typography.Text>
              ) : null}
            </Col>
          </Row>
        ) : null}

        {result?.walLsn !== undefined ? (
          <Typography.Text type="secondary" style={{ display: 'block', marginTop: 4 }}>
            WAL LSN：<Typography.Text code>{String(result.walLsn)}</Typography.Text>
          </Typography.Text>
        ) : null}
      </Card>

      {result ? (
        <Card title="执行结果" style={{ marginTop: 16 }}>
          {result.rows.length > 0 ? (
            <ResultTable
              tableKey="console"
              columns={result.columns}
              rows={renderedRows}
              receivedRows={result.received}
              renderStep={RENDER_STEP}
              onRenderMore={
                renderedRows.length < result.rows.length
                  ? () => setRenderLimit((v) => v + RENDER_STEP)
                  : undefined
              }
            />
          ) : (
            <Space direction="vertical" size={8}>
              <Typography.Text>
                语句执行完成，没有返回结果集
                {result.affectedRows !== undefined ? `（影响 ${result.affectedRows} 行）` : ''}。
              </Typography.Text>
              <Descriptions size="small" column={1}>
                {result.affectedRows !== undefined ? (
                  <Descriptions.Item label="affected_rows">{result.affectedRows}</Descriptions.Item>
                ) : null}
                {result.walLsn !== undefined ? (
                  <Descriptions.Item label="wal_lsn">{String(result.walLsn)}</Descriptions.Item>
                ) : null}
                {result.elapsedMicros !== undefined ? (
                  <Descriptions.Item label="elapsed">{formatMicros(result.elapsedMicros)}</Descriptions.Item>
                ) : null}
              </Descriptions>
            </Space>
          )}
        </Card>
      ) : null}

      <Modal
        open={saveOpen}
        title="保存为 Saved SQL"
        onCancel={() => setSaveOpen(false)}
        onOk={() => void handleSave()}
        okText="保存"
        destroyOnClose
      >
        <Form form={saveForm} layout="vertical" preserve={false}>
          <Form.Item name="name" label="名称" rules={[{ required: true, message: '请输入名称' }]}>
            <Input placeholder="例如：今日订单统计" />
          </Form.Item>
          <Form.Item name="description" label="描述（可选）">
            <Input.TextArea rows={2} placeholder="用途说明" />
          </Form.Item>
          <Typography.Text type="secondary">
            关联数据库：{dbId || '未选择'}（可在「设置 → Saved SQL」中管理）
          </Typography.Text>
        </Form>
      </Modal>
    </div>
  );
}
