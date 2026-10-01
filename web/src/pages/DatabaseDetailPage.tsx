/**
 * 数据库详情：基本信息、生命周期状态、路由信息（worker / epoch）、快照列表、慢查询。
 */
import { useState } from 'react';
import {
  App as AntdApp,
  Button,
  Card,
  Col,
  Descriptions,
  Empty,
  Row,
  Space,
  Table,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import {
  CameraOutlined,
  CloudUploadOutlined,
  ReloadOutlined,
  SwapOutlined,
  ThunderboltOutlined,
  UndoOutlined,
} from '@ant-design/icons';
import { Link, useParams } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { api, toItems } from '../api/client';
import type { OperationAccepted, SlowQuery, Snapshot } from '../api/types';
import { ErrorAlert } from '../components/ErrorAlert';
import { JsonBlock } from '../components/JsonBlock';
import { OperationModal } from '../components/OperationModal';
import { DatabaseStateTag } from '../components/StatusTag';
import { MoveDatabaseModal, RestoreDatabaseModal } from '../components/database/DatabaseModals';
import { useSubmitOperation } from '../hooks/useSubmitOperation';
import { dbName, dbWorkerId, slowQueryMillis, slowQuerySql } from '../utils/database';
import { formatBytes, formatMillis, formatTime, shortId } from '../utils/format';

interface ActionDef {
  key: string;
  label: string;
  icon: JSX.Element;
  confirm: string;
  danger?: boolean;
  run: (dbId: string) => Promise<OperationAccepted>;
}

const ACTIONS: ActionDef[] = [
  { key: 'start', label: '启动', icon: <ThunderboltOutlined />, confirm: '启动该数据库？', run: (id) => api.databases.start(id) },
  { key: 'stop', label: '停止', icon: <ThunderboltOutlined />, confirm: '停止后连接会中断，确认停止？', run: (id) => api.databases.stop(id) },
  { key: 'restart', label: '重启', icon: <UndoOutlined />, confirm: '重启期间短暂不可用，确认重启？', run: (id) => api.databases.restart(id) },
  { key: 'snapshot', label: '快照', icon: <CameraOutlined />, confirm: '立即创建快照？', run: (id) => api.databases.snapshot(id) },
  { key: 'backup', label: '备份', icon: <CloudUploadOutlined />, confirm: '将数据备份到对象存储？', run: (id) => api.databases.backup(id) },
];

export function DatabaseDetailPage(): JSX.Element {
  const { dbId = '' } = useParams<{ dbId: string }>();
  const { modal, message } = AntdApp.useApp();
  const { pending, operationId, error, run, track, clearOperation, clearError } = useSubmitOperation();
  const [restoreOpen, setRestoreOpen] = useState(false);
  const [moveOpen, setMoveOpen] = useState(false);

  const dbQuery = useQuery({
    queryKey: ['database', dbId],
    queryFn: () => api.databases.get(dbId),
    enabled: Boolean(dbId),
  });

  const snapshotsQuery = useQuery({
    queryKey: ['snapshots', dbId],
    queryFn: () => api.snapshots.list(dbId),
    enabled: Boolean(dbId),
  });

  const slowQueriesQuery = useQuery({
    queryKey: ['slow-queries', dbId],
    queryFn: () => api.slowQueries.list(dbId, 20),
    enabled: Boolean(dbId),
  });

  const db = dbQuery.data;
  const snapshots = toItems(snapshotsQuery.data);
  const slowQueries = toItems(slowQueriesQuery.data);

  const actionByKey = (key: string): ActionDef => {
    const found = ACTIONS.find((a) => a.key === key);
    if (!found) throw new Error(`未知动作：${key}`);
    return found;
  };

  const confirmAndRun = (action: ActionDef) => {
    modal.confirm({
      title: `确认${action.label}`,
      content: action.confirm,
      okText: `确认${action.label}`,
      okButtonProps: action.danger ? { danger: true } : undefined,
      cancelText: '取消',
      onOk: async () => {
        const accepted = await run(() => action.run(dbId));
        if (accepted) message.info(`已提交，操作 ID：${shortId(accepted.operation_id ?? '幂等无操作')}`);
      },
    });
  };

  const refreshAll = () => {
    void dbQuery.refetch();
    void snapshotsQuery.refetch();
    void slowQueriesQuery.refetch();
  };

  const snapshotColumns: ColumnsType<Snapshot> = [
    {
      title: '快照 ID',
      key: 'id',
      render: (_: unknown, s) => (
        <Typography.Text copyable={{ text: s.id }}>{s.id}</Typography.Text>
      ),
    },
    { title: '状态', dataIndex: 'state', key: 'state', width: 110, render: (v?: string) => v ?? '-' },
    { title: '压缩', dataIndex: 'compression', key: 'compression', width: 110, render: (v?: string) => v ?? '-' },
    {
      title: '大小',
      key: 'size_bytes',
      width: 110,
      render: (_: unknown, s) => formatBytes(s.size_bytes),
    },
    {
      title: 'Base LSN',
      key: 'base_lsn',
      width: 130,
      render: (_: unknown, s) => (s.base_lsn === undefined || s.base_lsn === null ? '-' : String(s.base_lsn)),
    },
    { title: '创建时间', key: 'created_at', width: 175, render: (_: unknown, s) => formatTime(s.created_at) },
  ];

  const slowQueryColumns: ColumnsType<SlowQuery> = [
    {
      title: 'SQL',
      key: 'sql',
      render: (_: unknown, q) => (
        <Typography.Text code style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
          {slowQuerySql(q).slice(0, 300)}
          {slowQuerySql(q).length > 300 ? ' …' : ''}
        </Typography.Text>
      ),
    },
    {
      title: '耗时',
      key: 'duration',
      width: 120,
      sorter: (a, b) => (slowQueryMillis(a) ?? 0) - (slowQueryMillis(b) ?? 0),
      render: (_: unknown, q) => {
        const ms = slowQueryMillis(q);
        return ms === null ? '-' : formatMillis(ms);
      },
    },
    {
      title: '返回行数',
      key: 'rows_returned',
      width: 100,
      render: (_: unknown, q) => q.rows_returned ?? '-',
    },
    { title: '时间', key: 'created_at', width: 175, render: (_: unknown, q) => formatTime(q.created_at) },
  ];

  return (
    <div>
      <Space style={{ marginBottom: 12 }} wrap>
        <Link to="/databases">← 返回数据库列表</Link>
        <Button size="small" icon={<ReloadOutlined />} onClick={refreshAll} loading={dbQuery.isFetching}>
          刷新
        </Button>
      </Space>

      <ErrorAlert error={dbQuery.error} onRetry={() => dbQuery.refetch()} />
      <ErrorAlert error={error} onRetry={clearError} closable onClose={clearError} />

      <Card
        title={
          <Space>
            <span>{db ? dbName(db) : dbId}</span>
            {db ? <DatabaseStateTag state={db.state} /> : null}
          </Space>
        }
        loading={dbQuery.isLoading}
        extra={
          <Space wrap>
            {ACTIONS.map((action) => (
              <Button
                key={action.key}
                size="small"
                icon={action.icon}
                disabled={pending}
                onClick={() => confirmAndRun(action)}
              >
                {action.label}
              </Button>
            ))}
            <Button size="small" danger icon={<UndoOutlined />} disabled={pending} onClick={() => setRestoreOpen(true)}>
              恢复
            </Button>
            <Button size="small" icon={<SwapOutlined />} disabled={pending} onClick={() => setMoveOpen(true)}>
              迁移
            </Button>
          </Space>
        }
      >
        <Row gutter={[16, 16]}>
          <Col xs={24} lg={12}>
            <Descriptions title="基本信息" size="small" column={1} bordered>
              <Descriptions.Item label="db_id">
                <Typography.Text copyable={{ text: dbId }} code>
                  {dbId}
                </Typography.Text>
              </Descriptions.Item>
              <Descriptions.Item label="名称">{db ? dbName(db) : '-'}</Descriptions.Item>
              <Descriptions.Item label="租户">{db?.tenant_id ?? '-'}</Descriptions.Item>
              <Descriptions.Item label="磁盘配额">{typeof db?.budget?.disk_mib === 'number' ? `${db.budget.disk_mib} MiB` : '-'}</Descriptions.Item>
              <Descriptions.Item label="创建时间">{formatTime(db?.created_at)}</Descriptions.Item>
              <Descriptions.Item label="更新时间">{formatTime(db?.updated_at)}</Descriptions.Item>
            </Descriptions>
          </Col>

          <Col xs={24} lg={12}>
            <Descriptions title="生命周期与路由" size="small" column={1} bordered>
              <Descriptions.Item label="状态">
                <DatabaseStateTag state={db?.state} />
              </Descriptions.Item>
              <Descriptions.Item label="Owner Worker">
                {db && dbWorkerId(db) !== '-' ? (
                  <Link to={`/workers/${dbWorkerId(db)}`}>{dbWorkerId(db)}</Link>
                ) : (
                  '-'
                )}
              </Descriptions.Item>
              <Descriptions.Item label="Epoch">
                <Tooltip title="迁移 / 故障切换后递增，用于识别陈旧路由">
                  <Tag>{db?.owner_epoch ?? '-'}</Tag>
                </Tooltip>
              </Descriptions.Item>
              <Descriptions.Item label="引擎版本">{db?.engine_version ?? '-'}</Descriptions.Item>
              <Descriptions.Item label="存储区域">{db?.storage_region ?? '-'}</Descriptions.Item>
              <Descriptions.Item label="存储前缀">
                <Typography.Text code>{db?.storage_prefix || '-'}</Typography.Text>
              </Descriptions.Item>
            </Descriptions>
          </Col>
        </Row>
      </Card>

      <Card
        title={<Space>快照<Link to={`/sql?db=${encodeURIComponent(dbId)}`}>去 SQL 控制台</Link></Space>}
        style={{ marginTop: 16 }}
        loading={snapshotsQuery.isLoading}
        extra={
          <Button size="small" icon={<CameraOutlined />} disabled={pending} onClick={() => confirmAndRun(actionByKey('snapshot'))}>
            立即快照
          </Button>
        }
      >
        <ErrorAlert error={snapshotsQuery.error} onRetry={() => snapshotsQuery.refetch()} />
        <Table<Snapshot>
          size="small"
          rowKey={(s) => s.id}
          columns={snapshotColumns}
          dataSource={snapshots}
          pagination={{ pageSize: 10, hideOnSinglePage: true }}
          locale={{ emptyText: '暂无快照' }}
          scroll={{ x: 'max-content' }}
        />
        <Typography.Text type="secondary">共 {snapshots.length} 个快照</Typography.Text>
      </Card>

      <Card
        title="慢查询"
        style={{ marginTop: 16 }}
        loading={slowQueriesQuery.isLoading}
        extra={<Typography.Text type="secondary">最近 20 条</Typography.Text>}
      >
        <ErrorAlert error={slowQueriesQuery.error} onRetry={() => slowQueriesQuery.refetch()} />
        {slowQueries.length > 0 ? (
          <Table<SlowQuery>
            size="small"
            rowKey={(q, index) => q.id ?? `${index}`}
            columns={slowQueryColumns}
            dataSource={slowQueries}
            pagination={{ pageSize: 10, hideOnSinglePage: true }}
            scroll={{ x: 'max-content' }}
            expandable={{
              expandedRowRender: (q) => <JsonBlock value={slowQuerySql(q)} />,
              rowExpandable: (q) => slowQuerySql(q).length > 100,
            }}
          />
        ) : (
          <Empty description="暂无慢查询记录" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        )}
        {slowQueries.length > 0 ? (
          <Typography.Text type="secondary">
            最长耗时：{formatMillis(Math.max(...slowQueries.map((q) => slowQueryMillis(q) ?? 0), 0))}
          </Typography.Text>
        ) : null}
      </Card>

      <MoveDatabaseModal
        open={moveOpen}
        dbId={dbId}
        onClose={() => setMoveOpen(false)}
        onSubmitted={(opId) => {
          track(opId);
          refreshAll();
        }}
      />

      <RestoreDatabaseModal
        open={restoreOpen}
        dbId={dbId}
        onClose={() => setRestoreOpen(false)}
        onSubmitted={(opId) => {
          track(opId);
          refreshAll();
        }}
      />

      <OperationModal
        open={Boolean(operationId)}
        operationId={operationId}
        title="数据库操作进度"
        onClose={() => {
          clearOperation();
          refreshAll();
        }}
        onSettled={refreshAll}
      />
    </div>
  );
}
