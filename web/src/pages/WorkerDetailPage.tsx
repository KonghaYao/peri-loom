/**
 * Worker 详情：容量 / 用量 / 状态、饱和度水位、运行中的数据库列表、Drain（二次确认 + 进度）。
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
  Statistic,
  Table,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { LogoutOutlined, ReloadOutlined } from '@ant-design/icons';
import { Link, useParams } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { api, toItems } from '../api/client';
import type { Database, Worker } from '../api/types';
import { ErrorAlert } from '../components/ErrorAlert';
import { OperationModal } from '../components/OperationModal';
import { SaturationBar } from '../components/SaturationBar';
import { DatabaseStateTag, WorkerStateTag } from '../components/StatusTag';
import { useSubmitOperation } from '../hooks/useSubmitOperation';
import { dbName, dbWorkerId } from '../utils/database';
import { formatTime, shortId } from '../utils/format';
import { SAT_DANGER, SAT_TARGET_MAX, SAT_TARGET_MIN, SAT_WARN } from '../utils/saturation';
import {
  workerAddress,
  workerCpuPercent,
  workerDbCount,
  workerMaxProcesses,
  workerMemoryPercent,
  workerProcessCount,
  workerProcessPercent,
  workerSaturation,
  workerState,
} from '../utils/worker';

/** 详情未加载完成时的占位对象，避免在渲染期到处判空 */
const EMPTY_WORKER: Worker = {
  id: '',
  endpoint: '-',
  state: 'UNKNOWN',
  region: '',
  zone: '',
  version: '',
  missed_heartbeats: 0,
  reserved_for_failover: false,
  capacity: {
    cpu_milli_total: 0,
    memory_mib_total: 0,
    fd_total: 0,
    disk_mib_total: 0,
    process_slots_total: 0,
    iops_total: 0,
  },
  usage: {
    cpu_milli_used: 0,
    memory_mib_used: 0,
    fd_used: 0,
    disk_mib_used: 0,
    process_slots_used: 0,
    iops_used: 0,
  },
};

/** 运行中的 DB：优先用 Worker 详情自带列表，缺失时回退到按 owner_worker_id 过滤的数据库列表 */
function useRunningDatabases(worker: Worker | undefined, needFallback: boolean) {
  return useQuery({
    queryKey: ['databases', 'by-worker', worker?.id],
    queryFn: () => api.databases.list({ limit: 200 }),
    enabled: needFallback && Boolean(worker?.id),
    staleTime: 15_000,
  });
}

export function WorkerDetailPage(): JSX.Element {
  const { workerId = '' } = useParams<{ workerId: string }>();
  const { modal, message } = AntdApp.useApp();
  const { pending, operationId, error, run, clearOperation, clearError } = useSubmitOperation();
  const [draining, setDraining] = useState(false);

  const workerQuery = useQuery({
    queryKey: ['worker', workerId],
    queryFn: () => api.workers.get(workerId),
    enabled: Boolean(workerId),
    refetchInterval: 15_000,
  });
  const worker = workerQuery.data;
  const w: Worker = worker ?? EMPTY_WORKER;

  const embeddedDbs = worker?.running_databases;
  const needFallback = !embeddedDbs || embeddedDbs.length === 0;
  const databasesQuery = useRunningDatabases(worker, needFallback);

  const runningDbs: Database[] =
    embeddedDbs && embeddedDbs.length > 0
      ? embeddedDbs
      : toItems(databasesQuery.data).filter((db) => dbWorkerId(db) === workerId);

  const confirmDrain = () => {
    if (!worker) return;
    modal.confirm({
      title: '排空 Worker',
      content: (
        <Space direction="vertical" size={4}>
          <span>
            Worker：<Typography.Text strong>{worker.id}</Typography.Text>
          </span>
          <Typography.Text type="secondary">
            排空后不再承载新数据库，其上 {workerDbCount(worker)} 个数据库通常会被迁移。确认继续？
          </Typography.Text>
        </Space>
      ),
      okText: '确认排空',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: async () => {
        setDraining(true);
        const accepted = await run(() => api.workers.drain(worker.id));
        if (accepted) message.info(`已提交，操作 ID：${shortId(accepted.operation_id ?? '幂等无操作')}`);
        else setDraining(false);
      },
    });
  };

  const dbColumns: ColumnsType<Database> = [
    {
      title: '数据库',
      key: 'name',
      render: (_: unknown, db) => <Link to={`/databases/${db.id}`}>{dbName(db)}</Link>,
    },
    {
      title: '状态',
      key: 'state',
      width: 110,
      render: (_: unknown, db) => <DatabaseStateTag state={db.state} />,
    },
    { title: 'Epoch', dataIndex: 'owner_epoch', key: 'owner_epoch', width: 90, render: (v?: number) => v ?? '-' },
    { title: '创建时间', key: 'created_at', width: 175, render: (_: unknown, db) => formatTime(db.created_at) },
  ];

  return (
    <div>
      <Space style={{ marginBottom: 12 }} wrap>
        <Link to="/workers">← 返回 Worker 列表</Link>
        <Button size="small" icon={<ReloadOutlined />} onClick={() => workerQuery.refetch()} loading={workerQuery.isFetching}>
          刷新
        </Button>
        <Button
          size="small"
          danger
          icon={<LogoutOutlined />}
          disabled={pending || draining || workerState(w) === 'DRAINING'}
          onClick={confirmDrain}
        >
          排空（Drain）
        </Button>
      </Space>

      <ErrorAlert error={workerQuery.error} onRetry={() => workerQuery.refetch()} />
      <ErrorAlert error={error} onRetry={clearError} closable onClose={clearError} />

      <Card
        title={
          <Space>
            <span>{workerId}</span>
            {worker ? <WorkerStateTag state={workerState(worker)} /> : null}
          </Space>
        }
        loading={workerQuery.isLoading}
      >
        <Row gutter={[16, 16]}>
          <Col xs={12} md={6}>
            <Statistic title="CPU 使用率" value={workerCpuPercent(w) ?? '-'} suffix="%" />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title="内存使用率" value={workerMemoryPercent(w) ?? '-'} suffix="%" />
          </Col>
          <Col xs={12} md={6}>
            <Statistic
              title="进程数"
              value={workerProcessCount(w) ?? '-'}
              suffix={workerMaxProcesses(w) ? `/ ${workerMaxProcesses(w)}` : ''}
            />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title="承载数据库" value={worker ? workerDbCount(worker) : '-'} />
          </Col>
        </Row>

        <div style={{ marginTop: 16, maxWidth: 520 }}>
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            饱和度（max CPU / 内存 / 进程数 取最高）｜目标区间 {SAT_TARGET_MIN}%~{SAT_TARGET_MAX}%｜
            {SAT_WARN}% 预警｜{SAT_DANGER}% 危险
          </Typography.Text>
          <div style={{ marginTop: 6 }}>
            <SaturationBar value={worker ? workerSaturation(worker) : null} height={14} />
          </div>
        </div>

        <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
          <Col xs={24} lg={12}>
            <Descriptions title="容量 / 用量" size="small" column={1} bordered>
              <Descriptions.Item label="地址">{worker ? workerAddress(worker) : '-'}</Descriptions.Item>
              <Descriptions.Item label="版本">{worker?.version ?? '-'}</Descriptions.Item>
              <Descriptions.Item label="CPU 容量">
                {typeof worker?.capacity?.cpu_milli_total === 'number'
                  ? `${worker.capacity.cpu_milli_total} m-core`
                  : '-'}
              </Descriptions.Item>
              <Descriptions.Item label="内存容量">
                {typeof worker?.capacity?.memory_mib_total === 'number'
                  ? `${worker.capacity.memory_mib_total} MiB`
                  : '-'}
              </Descriptions.Item>
              <Descriptions.Item label="最大进程数">
                {workerMaxProcesses(w) ?? '-'}
              </Descriptions.Item>
              <Descriptions.Item label="进程占用率">
                {workerProcessPercent(w) === null ? '-' : `${workerProcessPercent(w)!.toFixed(1)}%`}
              </Descriptions.Item>
            </Descriptions>
          </Col>
          <Col xs={24} lg={12}>
            <Descriptions title="生命周期" size="small" column={1} bordered>
              <Descriptions.Item label="worker_id">
                <Typography.Text copyable={{ text: workerId }} code>
                  {workerId}
                </Typography.Text>
              </Descriptions.Item>
              <Descriptions.Item label="状态">
                <WorkerStateTag state={worker ? workerState(worker) : undefined} />
              </Descriptions.Item>
              <Descriptions.Item label="排空状态">
                {worker?.state === 'DRAINING' ? 'DRAINING' : '正常'}
              </Descriptions.Item>
              <Descriptions.Item label="最近心跳">{formatTime(worker?.last_heartbeat_at)}</Descriptions.Item>
              <Descriptions.Item label="错失心跳">{worker?.missed_heartbeats ?? '-'}</Descriptions.Item>
              <Descriptions.Item label="DB 数量">
                <Tooltip title="来自 /workers/{id} 的 running_databases 字段">
                  <Tag>{worker ? workerDbCount(worker) : '-'}</Tag>
                </Tooltip>
              </Descriptions.Item>
            </Descriptions>
          </Col>
        </Row>
      </Card>

      <Card
        title="运行中的数据库"
        style={{ marginTop: 16 }}
        extra={
          needFallback ? (
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              由 /databases 列表按 Owner Worker 过滤
            </Typography.Text>
          ) : null
        }
        loading={workerQuery.isLoading || (needFallback && databasesQuery.isLoading)}
      >
        <ErrorAlert error={databasesQuery.error} onRetry={() => databasesQuery.refetch()} />
        {runningDbs.length > 0 ? (
          <Table<Database>
            size="small"
            rowKey={(db) => db.id}
            columns={dbColumns}
            dataSource={runningDbs}
            pagination={{ pageSize: 10, hideOnSinglePage: true, showTotal: (t) => `共 ${t} 个` }}
            scroll={{ x: 'max-content' }}
          />
        ) : (
          <Empty description="该 Worker 当前没有运行中的数据库" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        )}
      </Card>

      <OperationModal
        open={Boolean(operationId)}
        operationId={operationId}
        title="Worker 排空进度"
        onClose={() => {
          clearOperation();
          setDraining(false);
          workerQuery.refetch();
        }}
        onSettled={() => workerQuery.refetch()}
      />
    </div>
  );
}
