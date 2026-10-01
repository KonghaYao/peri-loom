/**
 * 概览 Dashboard：DB 状态分布、Worker 饱和度（含 70%~75% 目标区间与 80%/90% 水位线）、最近操作。
 */
import { useQuery } from '@tanstack/react-query';
import { Card, Col, Empty, List, Progress, Row, Space, Statistic, Table, Tag, Tooltip, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { Link } from 'react-router-dom';
import { api, toItems } from '../api/client';
import { isTerminalOperation, type Operation, type Worker } from '../api/types';
import { ErrorAlert } from '../components/ErrorAlert';
import { SaturationBar } from '../components/SaturationBar';
import { DatabaseStateTag, OperationStateTag, WorkerStateTag } from '../components/StatusTag';
import { useOperationsListQuery } from '../hooks/useOperationQuery';
import { useDeployment } from '../hooks/useDeployment';
import { operationDbId, operationKind } from '../utils/database';
import { formatTime, shortId } from '../utils/format';
import { SAT_TARGET_MAX, SAT_TARGET_MIN, SAT_WARN, SAT_DANGER, saturationColor } from '../utils/saturation';
import { workerDbCount, workerSaturation, workerState } from '../utils/worker';

interface StateCount {
  state: string;
  count: number;
}

interface DbStats {
  total: number;
  byState: StateCount[];
  /** server：按状态过滤得到的精确计数；client：后端忽略 state 参数时按前 200 条聚合 */
  mode: 'server' | 'client';
  sampled: number;
}

/**
 * DB 状态分布：后端分页响应只有 items/limit/offset（没有 total），
 * 因此这里按单页样本（最多 200 条）本地聚合，并标注样本量。
 */
async function loadDbStats(): Promise<DbStats> {
  const sample = await api.databases.list({ limit: 200 });
  const items = toItems(sample);
  const map = new Map<string, number>();
  for (const db of items) {
    const key = db.state || 'UNKNOWN';
    map.set(key, (map.get(key) ?? 0) + 1);
  }
  return {
    total: items.length,
    byState: [...map.entries()].map(([state, count]) => ({ state, count })).sort((a, b) => b.count - a.count),
    mode: 'client',
    sampled: items.length,
  };
}

const RECENT_OPERATION_COLUMNS: ColumnsType<Operation> = [
  {
    title: '操作',
    dataIndex: 'id',
    key: 'kind',
    render: (_: unknown, op) => (
      <Space direction="vertical" size={0}>
        <Typography.Text>{operationKind(op)}</Typography.Text>
        <Typography.Text type="secondary" copyable={{ text: op.id }} style={{ fontSize: 12 }}>
          {shortId(op.id, 16, 6)}
        </Typography.Text>
      </Space>
    ),
  },
  {
    title: '数据库',
    key: 'db',
    render: (_: unknown, op) => {
      const dbId = operationDbId(op);
      return dbId ? <Link to={`/databases/${dbId}`}>{dbId}</Link> : '-';
    },
  },
  {
    title: '状态',
    dataIndex: 'state',
    key: 'state',
    width: 130,
    render: (state: string) => <OperationStateTag state={state} />,
  },
  {
    title: '进度',
    key: 'progress',
    width: 160,
    render: (_: unknown, op) => (
      <Progress
        percent={typeof op.progress === 'number' ? Math.min(100, Math.max(0, op.progress)) : 0}
        size="small"
        status={op.state === 'FAILED' ? 'exception' : op.state === 'SUCCEEDED' ? 'success' : 'active'}
      />
    ),
  },
  {
    title: '提交时间',
    key: 'created_at',
    width: 170,
    render: (_: unknown, op) => formatTime(op.created_at),
  },
];

export function DashboardPage(): JSX.Element {
  const deployment = useDeployment().data;
  const hasWorkers = deployment?.capabilities.workers ?? false;
  const dbStats = useQuery({ queryKey: ['dashboard', 'db-stats'], queryFn: loadDbStats, staleTime: 15_000 });
  const workersQuery = useQuery({
    queryKey: ['workers'],
    queryFn: () => api.workers.list(),
    staleTime: 10_000,
    enabled: hasWorkers,
  });
  const operationsQuery = useOperationsListQuery(10, 0);

  const workers: Worker[] = toItems(workersQuery.data);
  const operations: Operation[] = operationsQuery.data?.items ?? [];

  /** 饱和度超过 80% 预警线的 Worker */
  const hotWorkers = workers.filter((w) => (workerSaturation(w) ?? 0) >= SAT_WARN);
  const runningOps = operations.filter((op) => !isTerminalOperation(op.state)).length;

  const avgSaturation = (() => {
    const values = workers.map(workerSaturation).filter((v): v is number => v !== null);
    if (values.length === 0) return null;
    return values.reduce((a, b) => a + b, 0) / values.length;
  })();

  return (
    <div>
      <ErrorAlert error={dbStats.error} onRetry={() => dbStats.refetch()} />
      {hasWorkers && <ErrorAlert error={workersQuery.error} onRetry={() => workersQuery.refetch()} />}

      <Row gutter={[16, 16]}>
        <Col xs={24} sm={12} lg={6}>
          <Card loading={dbStats.isLoading}>
            <Statistic title="数据库总数" value={dbStats.data?.total ?? 0} />
          </Card>
        </Col>
        {hasWorkers && <Col xs={24} sm={12} lg={6}>
          <Card loading={workersQuery.isLoading}>
            <Statistic title="Worker 数量" value={workers.length} />
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              预警水位 ≥ {SAT_WARN}%：{hotWorkers.length} 个
            </Typography.Text>
          </Card>
        </Col>}
        {hasWorkers && <Col xs={24} sm={12} lg={6}>
          <Card loading={workersQuery.isLoading}>
            <div className="stat-card__value" style={{ color: avgSaturation === null ? undefined : saturationColor(avgSaturation) }}>
              {avgSaturation === null ? '-' : `${avgSaturation.toFixed(1)}%`}
            </div>
            <Typography.Text type="secondary">平均饱和度（目标 {SAT_TARGET_MIN}%~{SAT_TARGET_MAX}%）</Typography.Text>
          </Card>
        </Col>}
        {!hasWorkers && <Col xs={24} sm={12} lg={8}><Card title="本机实例"><Typography.Text>本地同步持久化；数据库共享主进程资源，无逐库硬隔离。</Typography.Text></Card></Col>}
        <Col xs={24} sm={12} lg={6}>
          <Card loading={operationsQuery.isLoading}>
            <Statistic title="进行中的操作" value={runningOps} />
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              最近 {operations.length} 条操作
            </Typography.Text>
          </Card>
        </Col>
      </Row>

      {/* DB 状态分布 */}
      <Card
        title="数据库状态分布"
        style={{ marginTop: 16 }}
        extra={
          dbStats.data?.mode === 'client' ? (
            <Tooltip title="后端分页不返回总数，这里按单页样本（最多 200 条）本地聚合">
              <Tag color="orange">本地聚合（样本 {dbStats.data.sampled} 条）</Tag>
            </Tooltip>
          ) : null
        }
        loading={dbStats.isLoading}
      >
        {dbStats.data && dbStats.data.byState.length > 0 ? (
          <List
            grid={{ gutter: 16, xs: 1, sm: 2, md: 3, lg: 4 }}
            dataSource={dbStats.data.byState}
            renderItem={(item) => {
              const total = dbStats.data?.total || 1;
              const ratio = Math.min(100, (item.count / total) * 100);
              return (
                <List.Item>
                  <Space direction="vertical" size={2} style={{ width: '100%' }}>
                    <Space>
                      <DatabaseStateTag state={item.state} />
                      <Typography.Text strong>{item.count}</Typography.Text>
                      <Typography.Text type="secondary">{ratio.toFixed(1)}%</Typography.Text>
                    </Space>
                    <Progress percent={Number(ratio.toFixed(1))} showInfo={false} size="small" />
                  </Space>
                </List.Item>
              );
            }}
          />
        ) : (
          <Empty description="暂无数据库" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        )}
      </Card>

      {/* Worker 饱和度 */}
      {hasWorkers && <Card
        title="Worker 饱和度"
        style={{ marginTop: 16 }}
        loading={workersQuery.isLoading}
        extra={
          <Space size={12} wrap>
            <span className="legend-inline">
              <span className="legend-swatch" style={{ background: 'rgba(82,196,26,0.35)', border: '1px dashed #52c41a' }} />
              目标区间 {SAT_TARGET_MIN}%~{SAT_TARGET_MAX}%
            </span>
            <span className="legend-inline">
              <span className="legend-swatch" style={{ background: 'rgba(250,173,20,0.95)' }} />
              预警 {SAT_WARN}%
            </span>
            <span className="legend-inline">
              <span className="legend-swatch" style={{ background: 'rgba(255,77,79,0.95)' }} />
              危险 {SAT_DANGER}%
            </span>
          </Space>
        }
      >
        {workers.length > 0 ? (
          <Table<Worker>
            size="small"
            rowKey={(w) => w.id}
            dataSource={workers}
            pagination={false}
            columns={[
              {
                title: 'Worker',
                key: 'id',
                render: (_: unknown, w) => <Link to={`/workers/${w.id}`}>{w.id}</Link>,
              },
              {
                title: '状态',
                key: 'state',
                width: 110,
                render: (_: unknown, w) => <WorkerStateTag state={workerState(w)} />,
              },
              {
                title: '饱和度（CPU / 内存 / 进程 取最高）',
                key: 'saturation',
                render: (_: unknown, w) => <SaturationBar value={workerSaturation(w)} />,
              },
              {
                title: 'DB 数',
                key: 'db_count',
                width: 90,
                render: (_: unknown, w) => workerDbCount(w),
              },
            ]}
          />
        ) : (
          <Empty description="暂无 Worker 数据" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        )}
      </Card>}

      {/* 最近操作 */}
      <Card
        title="最近操作"
        style={{ marginTop: 16 }}
        extra={<Link to="/operations">查看全部</Link>}
        loading={operationsQuery.isLoading}
      >
        <ErrorAlert error={operationsQuery.error} onRetry={() => operationsQuery.refetch()} />
        <Table<Operation>
          size="small"
          rowKey={(op) => op.id}
          columns={RECENT_OPERATION_COLUMNS}
          dataSource={operations}
          pagination={false}
          locale={{ emptyText: '暂无操作记录' }}
          scroll={{ x: 'max-content' }}
        />
      </Card>
    </div>
  );
}
