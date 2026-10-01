/**
 * 操作中心：所有长操作（创建/迁移/备份/恢复/排空…）的列表与详情。
 * 存在未完成操作时列表自动轮询；单条详情用 OperationModal 实时跟踪进度。
 */
import { useMemo, useState } from 'react';
import { Button, Card, Input, Progress, Select, Space, Table, Tooltip, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { ReloadOutlined, SearchOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import {
  OPERATION_STATE_META,
  isTerminalOperation,
  type Operation,
} from '../api/types';
import { hasMorePage } from '../api/client';
import { ErrorAlert } from '../components/ErrorAlert';
import { OperationModal } from '../components/OperationModal';
import { OperationStateTag } from '../components/StatusTag';
import { useOperationsListQuery } from '../hooks/useOperationQuery';
import { operationDbId, operationKind } from '../utils/database';
import { formatTime, shortId } from '../utils/format';

export function OperationsPage(): JSX.Element {
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [stateFilter, setStateFilter] = useState<string | undefined>(undefined);
  const [keyword, setKeyword] = useState('');
  const [detailId, setDetailId] = useState<string | null>(null);

  const query = useOperationsListQuery(pageSize, (page - 1) * pageSize);
  const operations = query.data?.items ?? [];
  /** 后端分页不返回总数：本页取满 limit 即认为还有下一页 */
  const hasMore = hasMorePage(query.data);

  /** 仅筛当前页；翻页由后端 limit/offset 完成 */
  const rows = useMemo(() => {
    const kw = keyword.trim().toLowerCase();
    return operations.filter((op) => {
      if (stateFilter && op.state !== stateFilter) return false;
      if (!kw) return true;
      return (
        op.id.toLowerCase().includes(kw) ||
        operationKind(op).toLowerCase().includes(kw) ||
        (operationDbId(op) ?? '').toLowerCase().includes(kw)
      );
    });
  }, [operations, stateFilter, keyword]);

  const runningCount = operations.filter((op) => !isTerminalOperation(op.state)).length;

  const columns: ColumnsType<Operation> = [
    {
      title: 'operation_id',
      key: 'operation_id',
      fixed: 'left',
      render: (_: unknown, op) => (
        <Space direction="vertical" size={0}>
          <Typography.Text copyable={{ text: op.id }}>{shortId(op.id, 16, 6)}</Typography.Text>
          <Button type="link" size="small" style={{ padding: 0 }} onClick={() => setDetailId(op.id)}>
            查看详情
          </Button>
        </Space>
      ),
    },
    {
      title: '类型',
      key: 'kind',
      width: 170,
      render: (_: unknown, op) => <Typography.Text>{operationKind(op)}</Typography.Text>,
    },
    {
      title: '数据库',
      key: 'db',
      width: 200,
      render: (_: unknown, op) => {
        const dbId = operationDbId(op);
        return dbId ? <Link to={`/databases/${dbId}`}>{shortId(dbId, 12, 6)}</Link> : '-';
      },
    },
    {
      title: '状态',
      dataIndex: 'state',
      key: 'state',
      width: 110,
      render: (state: string) => <OperationStateTag state={state} />,
    },
    {
      title: '进度',
      key: 'progress',
      width: 150,
      render: (_: unknown, op) => (
        <Progress
          percent={typeof op.progress === 'number' ? Math.min(100, Math.max(0, op.progress)) : 0}
          size="small"
          status={op.state === 'FAILED' ? 'exception' : op.state === 'SUCCEEDED' ? 'success' : 'active'}
        />
      ),
    },
    {
      title: '错误',
      key: 'error',
      width: 220,
      render: (_: unknown, op) =>
        op.error_code || op.error_message ? (
          <Tooltip title={op.error_message ?? undefined}>
            <Typography.Text type="danger" ellipsis style={{ maxWidth: 200 }}>
              {op.error_code ?? '错误'}
            </Typography.Text>
          </Tooltip>
        ) : (
          '-'
        ),
    },
    {
      title: '提交时间',
      key: 'created_at',
      width: 175,
      render: (_: unknown, op) => formatTime(op.created_at),
    },
    {
      title: '完成时间',
      key: 'finished_at',
      width: 175,
      render: (_: unknown, op) => formatTime(op.finished_at),
    },
  ];

  return (
    <div>
      <Card
        title={
          <Space>
            <span>操作中心</span>
            {runningCount > 0 ? <Typography.Text type="warning">进行中 {runningCount} 条（自动刷新）</Typography.Text> : null}
          </Space>
        }
        extra={
          <Space wrap>
            <Input
              allowClear
              prefix={<SearchOutlined />}
              placeholder="筛选操作 ID / 类型 / 数据库（当前页）"
              style={{ width: 300 }}
              value={keyword}
              onChange={(e) => setKeyword(e.target.value)}
            />
            <Select
              allowClear
              placeholder="按状态过滤"
              style={{ width: 140 }}
              value={stateFilter}
              onChange={setStateFilter}
              options={Object.entries(OPERATION_STATE_META).map(([value, meta]) => ({ value, label: meta.label }))}
            />
            <Button icon={<ReloadOutlined />} onClick={() => query.refetch()} loading={query.isFetching}>
              刷新
            </Button>
          </Space>
        }
      >
        <ErrorAlert error={query.error} onRetry={() => query.refetch()} />
        <Table<Operation>
          size="small"
          rowKey={(op) => op.id}
          columns={columns}
          dataSource={rows}
          loading={query.isLoading}
          scroll={{ x: 'max-content' }}
          pagination={{
            current: page,
            pageSize,
            total: (page - 1) * pageSize + operations.length + (hasMore ? 1 : 0),
            showSizeChanger: true,
            showTotal: (_t, range) => `第 ${range[0]}-${range[1]} 条（后端未返回总数）`,
            onChange: (p, size) => {
              setPage(p);
              setPageSize(size);
            },
          }}
          locale={{ emptyText: '暂无操作记录' }}
        />
      </Card>

      <OperationModal
        open={Boolean(detailId)}
        operationId={detailId}
        title="操作详情"
        onClose={() => {
          setDetailId(null);
          query.refetch();
        }}
        onSettled={() => query.refetch()}
      />
    </div>
  );
}
