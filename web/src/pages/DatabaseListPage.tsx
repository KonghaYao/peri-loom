/**
 * 数据库列表：分页 / 按状态过滤 / 创建 / 生命周期操作（启动·停止·重启·迁移·快照·备份·恢复·删除）。
 * 所有长操作提交后（202）都会打开进度弹窗，轮询 /operations/{id} 实时展示进度。
 */
import { useMemo, useState } from 'react';
import {
  App as AntdApp,
  Button,
  Card,
  Dropdown,
  Input,
  Space,
  Select,
  Table,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import {
  DeleteOutlined,
  EllipsisOutlined,
  PlusOutlined,
  ReloadOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { api, hasMorePage, toItems } from '../api/client';
import { DATABASE_STATE_META, type Database, type OperationAccepted } from '../api/types';
import { CreateDatabaseModal, MoveDatabaseModal, RestoreDatabaseModal } from '../components/database/DatabaseModals';
import { ErrorAlert } from '../components/ErrorAlert';
import { OperationModal } from '../components/OperationModal';
import { DatabaseStateTag } from '../components/StatusTag';
import { PREF_KEYS, usePreferences } from '../hooks/usePreferences';
import { useSubmitOperation } from '../hooks/useSubmitOperation';
import { dbEpoch, dbName, dbWorkerId } from '../utils/database';
import { formatTime, shortId } from '../utils/format';

/** 二次确认型动作定义 */
interface LifecycleAction {
  key: string;
  label: string;
  danger?: boolean;
  /** 二次确认文案 */
  confirm: string;
  run: (dbId: string) => Promise<OperationAccepted>;
}

const LIFECYCLE_ACTIONS: LifecycleAction[] = [
  {
    key: 'restart',
    label: '重启',
    confirm: '重启期间数据库会短暂不可用，确认重启？',
    run: (dbId) => api.databases.restart(dbId),
  },
  {
    key: 'snapshot',
    label: '快照',
    confirm: '立即为该数据库创建快照？',
    run: (dbId) => api.databases.snapshot(dbId),
  },
  {
    key: 'backup',
    label: '备份',
    confirm: '将当前数据备份到对象存储？',
    run: (dbId) => api.databases.backup(dbId),
  },
  {
    key: 'delete',
    label: '删除',
    danger: true,
    confirm: '删除后不可恢复（除非另有备份），确认删除该数据库？',
    run: (dbId) => api.databases.remove(dbId),
  },
];

export function DatabaseListPage(): JSX.Element {
  const { modal, message } = AntdApp.useApp();
  const { get } = usePreferences();
  const defaultPageSize = get<number>(PREF_KEYS.pageSize, 20);
  const { pending, operationId, error, run, track, clearOperation, clearError } = useSubmitOperation();

  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(defaultPageSize);
  const [stateFilter, setStateFilter] = useState<string | undefined>(undefined);
  const [keyword, setKeyword] = useState('');
  const [createOpen, setCreateOpen] = useState(false);
  const [moveTarget, setMoveTarget] = useState<string | null>(null);
  const [restoreTarget, setRestoreTarget] = useState<string | null>(null);

  const query = useQuery({
    queryKey: ['databases', { page, pageSize, stateFilter }],
    queryFn: () =>
      api.databases.list({
        limit: pageSize,
        offset: (page - 1) * pageSize,
        ...(stateFilter ? { state: stateFilter } : {}),
      }),
  });

  const databases = toItems(query.data);
  const loaded = databases.length;
  /** 后端分页只回 items/limit/offset（没有 total）：本页取满 limit 就认为还有下一页 */
  const hasMore = hasMorePage(query.data);

  /** 名称 / ID 关键字过滤（当前页） */
  const rows = useMemo(() => {
    const kw = keyword.trim().toLowerCase();
    if (!kw) return databases;
    return databases.filter(
      (db) =>
        dbName(db).toLowerCase().includes(kw) ||
        db.id.toLowerCase().includes(kw) ||
        (db.tenant_id ?? '').toLowerCase().includes(kw),
    );
  }, [databases, keyword]);

  /** 带二次确认地提交长操作 */
  const confirmAndRun = (db: Database, action: LifecycleAction) => {
    modal.confirm({
      title: `确认${action.label}`,
      content: (
        <Space direction="vertical" size={4}>
          <span>
            数据库：<Typography.Text strong>{dbName(db)}</Typography.Text>
          </span>
          <Typography.Text type="secondary">{action.confirm}</Typography.Text>
        </Space>
      ),
      okText: `确认${action.label}`,
      okButtonProps: action.danger ? { danger: true } : undefined,
      cancelText: '取消',
      onOk: async () => {
        const accepted = await run(() => action.run(db.id));
        if (accepted) message.info(`已提交，操作 ID：${shortId(accepted.operation_id ?? '幂等无操作')}`);
      },
    });
  };

  /** 启动 / 停止（按当前状态给出对应动作）：WARM/HOT 视为已启动 */
  const quickToggle = (db: Database) => {
    const isRunning = db.state === 'WARM' || db.state === 'HOT';
    const label = isRunning ? '停止' : '启动';
    modal.confirm({
      title: `确认${label}`,
      content: `数据库 ${dbName(db)} 当前状态：${db.state}`,
      okText: `确认${label}`,
      cancelText: '取消',
      onOk: async () => {
        const accepted = await run(() => (isRunning ? api.databases.stop(db.id) : api.databases.start(db.id)));
        if (accepted) message.info(`已提交，操作 ID：${shortId(accepted.operation_id ?? '幂等无操作')}`);
      },
    });
  };

  const columns: ColumnsType<Database> = [
    {
      title: '名称',
      key: 'name',
      fixed: 'left',
      render: (_: unknown, db) => (
        <Space direction="vertical" size={0}>
          <Link to={`/databases/${db.id}`}>{dbName(db)}</Link>
          <Typography.Text type="secondary" copyable={{ text: db.id }} style={{ fontSize: 12 }}>
            {shortId(db.id, 12, 6)}
          </Typography.Text>
        </Space>
      ),
    },
    {
      title: '状态',
      dataIndex: 'state',
      key: 'state',
      width: 110,
      render: (state: string) => <DatabaseStateTag state={state} />,
    },
    {
      title: 'Owner Worker',
      key: 'owner_worker_id',
      width: 180,
      render: (_: unknown, db) => {
        const workerId = dbWorkerId(db);
        return workerId === '-' ? '-' : <Link to={`/workers/${workerId}`}>{shortId(workerId, 14, 6)}</Link>;
      },
    },
    {
      title: 'Epoch',
      key: 'epoch',
      width: 90,
      render: (_: unknown, db) => <Tag>{dbEpoch(db)}</Tag>,
    },
    {
      title: '租户',
      dataIndex: 'tenant_id',
      key: 'tenant_id',
      width: 140,
      render: (v?: string) => v ?? '-',
    },
    {
      title: '创建时间',
      key: 'created_at',
      width: 175,
      render: (_: unknown, db) => formatTime(db.created_at),
    },
    {
      title: '操作',
      key: 'actions',
      fixed: 'right',
      width: 240,
      render: (_: unknown, db) => (
        <Space size={4} wrap>
          <Button
            size="small"
            type="link"
            icon={<ThunderboltOutlined />}
            disabled={pending}
            onClick={() => quickToggle(db)}
          >
            {db.state === 'WARM' || db.state === 'HOT' ? '停止' : '启动'}
          </Button>
          <Dropdown
            menu={{
              items: [
                { key: 'restart', label: '重启' },
                { key: 'move', label: '迁移' },
                { key: 'snapshot', label: '快照' },
                { key: 'backup', label: '备份' },
                { key: 'restore', label: '恢复' },
                { type: 'divider' },
                { key: 'delete', label: '删除', danger: true, icon: <DeleteOutlined /> },
              ],
              onClick: ({ key }) => {
                if (key === 'move') return setMoveTarget(db.id);
                if (key === 'restore') return setRestoreTarget(db.id);
                const action = LIFECYCLE_ACTIONS.find((a) => a.key === key);
                if (action) confirmAndRun(db, action);
              },
            }}
          >
            <Button size="small" icon={<EllipsisOutlined />} disabled={pending}>
              更多
            </Button>
          </Dropdown>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <Card
        title={
          <Space>
            <span>数据库</span>
            <Tag>{loaded} 个（本页）</Tag>
          </Space>
        }
        extra={
          <Space wrap>
            <Input.Search
              allowClear
              placeholder="搜索名称 / ID / 租户（当前页）"
              style={{ width: 240 }}
              onSearch={setKeyword}
              onChange={(e) => {
                if (!e.target.value) setKeyword('');
              }}
            />
            <Select
              allowClear
              placeholder="按状态过滤"
              style={{ width: 150 }}
              value={stateFilter}
              onChange={(v) => {
                setStateFilter(v);
                setPage(1);
              }}
              options={Object.entries(DATABASE_STATE_META).map(([value, meta]) => ({
                value,
                label: meta.label,
              }))}
            />
            <Tooltip title="刷新">
              <Button icon={<ReloadOutlined />} onClick={() => query.refetch()} loading={query.isFetching} />
            </Tooltip>
            <Button type="primary" icon={<PlusOutlined />} onClick={() => setCreateOpen(true)}>
              创建数据库
            </Button>
          </Space>
        }
      >
        <ErrorAlert error={query.error} onRetry={() => query.refetch()} />
        <ErrorAlert error={error} onRetry={clearError} closable onClose={clearError} />

        <Table<Database>
          size="small"
          rowKey={(db) => db.id}
          columns={columns}
          dataSource={rows}
          loading={query.isLoading}
          scroll={{ x: 'max-content' }}
          pagination={{
            current: page,
            pageSize,
            // 后端不返回总数：用「本页条数 + 是否还有下一页」凑出分页器需要的上界
            total: (page - 1) * pageSize + loaded + (hasMore ? 1 : 0),
            showSizeChanger: true,
            showTotal: (_t, range) => `第 ${range[0]}-${range[1]} 条（后端未返回总数）`,
            onChange: (nextPage, nextSize) => {
              setPage(nextPage);
              setPageSize(nextSize);
            },
          }}
          locale={{ emptyText: '没有匹配的数据库' }}
        />
      </Card>

      <CreateDatabaseModal
        open={createOpen}
        onClose={() => setCreateOpen(false)}
        onSubmitted={(opId) => {
          track(opId);
          query.refetch();
        }}
      />

      {moveTarget ? (
        <MoveDatabaseModal
          open
          dbId={moveTarget}
          onClose={() => setMoveTarget(null)}
          onSubmitted={(opId) => {
            track(opId);
            query.refetch();
          }}
        />
      ) : null}

      {restoreTarget ? (
        <RestoreDatabaseModal
          open
          dbId={restoreTarget}
          onClose={() => setRestoreTarget(null)}
          onSubmitted={(opId) => {
            track(opId);
            query.refetch();
          }}
        />
      ) : null}

      <OperationModal
        open={Boolean(operationId)}
        operationId={operationId}
        title="数据库操作进度"
        onClose={() => {
          clearOperation();
          query.refetch();
        }}
        onSettled={() => query.refetch()}
      />

    </div>
  );
}
