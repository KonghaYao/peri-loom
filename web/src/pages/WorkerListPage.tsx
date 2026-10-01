/**
 * Worker 管理：状态、CPU/内存/进程数进度条与饱和度着色（70%~75% 目标区间、80%/90% 水位线）、Drain 排空。
 */
import { App as AntdApp, Button, Card, Progress, Space, Table, Tag, Tooltip, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { LogoutOutlined, ReloadOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { api, toItems } from '../api/client';
import type { Worker } from '../api/types';
import { ErrorAlert } from '../components/ErrorAlert';
import { OperationModal } from '../components/OperationModal';
import { SaturationBar } from '../components/SaturationBar';
import { WorkerStateTag } from '../components/StatusTag';
import { useSubmitOperation } from '../hooks/useSubmitOperation';
import { formatTime, shortId } from '../utils/format';
import { SAT_DANGER, SAT_WARN, saturationColor } from '../utils/saturation';
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

/** 按饱和度着色的小进度条 */
function PercentBar({ value }: { value: number | null }): JSX.Element {
  if (value === null) return <Typography.Text type="secondary">无数据</Typography.Text>;
  return (
    <Tooltip title={`${value.toFixed(1)}%`}>
      <Progress
        percent={Number(value.toFixed(1))}
        size="small"
        showInfo={false}
        strokeColor={saturationColor(value)}
        style={{ minWidth: 90, marginBottom: 0 }}
      />
    </Tooltip>
  );
}

export function WorkerListPage(): JSX.Element {
  const { modal, message } = AntdApp.useApp();
  const { pending, operationId, error, run, clearOperation, clearError } = useSubmitOperation();
  const query = useQuery({
    queryKey: ['workers'],
    queryFn: () => api.workers.list(),
    refetchInterval: 15_000,
  });

  const workers = toItems(query.data);

  const confirmDrain = (worker: Worker) => {
    modal.confirm({
      title: '排空 Worker',
      content: (
        <Space direction="vertical" size={4}>
          <span>
            Worker：<Typography.Text strong>{worker.id}</Typography.Text>
          </span>
          <Typography.Text type="secondary">
            排空后该 Worker 不再承载新数据库，通常会触发其上数据库迁移到其它 Worker
            （当前承载 {workerDbCount(worker)} 个 DB）。确认继续？
          </Typography.Text>
        </Space>
      ),
      okText: '确认排空',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: async () => {
        const accepted = await run(() => api.workers.drain(worker.id));
        if (accepted) message.info(`已提交，操作 ID：${shortId(accepted.operation_id ?? '幂等无操作')}`);
      },
    });
  };

  const columns: ColumnsType<Worker> = [
    {
      title: 'Worker',
      key: 'worker_id',
      fixed: 'left',
      render: (_: unknown, w) => (
        <Space direction="vertical" size={0}>
          <Link to={`/workers/${w.id}`}>{w.id}</Link>
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            {workerAddress(w)}
            {w.version ? ` · ${w.version}` : ''}
          </Typography.Text>
        </Space>
      ),
    },
    {
      title: '状态',
      key: 'state',
      width: 110,
      render: (_: unknown, w) => <WorkerStateTag state={workerState(w)} />,
    },
    {
      title: 'CPU',
      key: 'cpu',
      width: 130,
      sorter: (a, b) => (workerCpuPercent(a) ?? 0) - (workerCpuPercent(b) ?? 0),
      render: (_: unknown, w) => <PercentBar value={workerCpuPercent(w)} />,
    },
    {
      title: '内存',
      key: 'memory',
      width: 130,
      sorter: (a, b) => (workerMemoryPercent(a) ?? 0) - (workerMemoryPercent(b) ?? 0),
      render: (_: unknown, w) => <PercentBar value={workerMemoryPercent(w)} />,
    },
    {
      title: '进程数',
      key: 'processes',
      width: 140,
      render: (_: unknown, w) => {
        const used = workerProcessCount(w);
        const max = workerMaxProcesses(w);
        return (
          <Space direction="vertical" size={0} style={{ width: '100%' }}>
            <Typography.Text style={{ fontSize: 12 }}>
              {used ?? '-'}
              {max ? ` / ${max}` : ''}
            </Typography.Text>
            <PercentBar value={workerProcessPercent(w)} />
          </Space>
        );
      },
    },
    {
      title: '饱和度',
      key: 'saturation',
      width: 210,
      sorter: (a, b) => (workerSaturation(a) ?? -1) - (workerSaturation(b) ?? -1),
      defaultSortOrder: 'descend',
      render: (_: unknown, w) => <SaturationBar value={workerSaturation(w)} />,
    },
    {
      title: 'DB 数',
      key: 'db_count',
      width: 90,
      sorter: (a, b) => workerDbCount(a) - workerDbCount(b),
      render: (_: unknown, w) => <Tag>{workerDbCount(w)}</Tag>,
    },
    {
      title: '心跳',
      key: 'last_heartbeat_at',
      width: 170,
      render: (_: unknown, w) => formatTime(w.last_heartbeat_at),
    },
    {
      title: '操作',
      key: 'actions',
      fixed: 'right',
      width: 170,
      render: (_: unknown, w) => (
        <Space size={4}>
          <Link to={`/workers/${w.id}`}>详情</Link>
          <Button
            size="small"
            danger
            type="link"
            icon={<LogoutOutlined />}
            disabled={pending || workerState(w) === 'DRAINING'}
            onClick={() => confirmDrain(w)}
          >
            Drain
          </Button>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <Card
        title={
          <Space>
            <span>Worker</span>
            <Tag>{workers.length} 个</Tag>
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              饱和度 = max(CPU, 内存, 进程数)；目标区间 70%~75%，{SAT_WARN}% 预警，{SAT_DANGER}% 危险
            </Typography.Text>
          </Space>
        }
        extra={
          <Button icon={<ReloadOutlined />} onClick={() => query.refetch()} loading={query.isFetching}>
            刷新
          </Button>
        }
      >
        <ErrorAlert error={query.error} onRetry={() => query.refetch()} />
        <ErrorAlert error={error} onRetry={clearError} closable onClose={clearError} />

        <Table<Worker>
          size="small"
          rowKey={(w) => w.id}
          columns={columns}
          dataSource={workers}
          loading={query.isLoading}
          scroll={{ x: 'max-content' }}
          pagination={{ pageSize: 20, showSizeChanger: true, showTotal: (t) => `共 ${t} 个 Worker` }}
          locale={{ emptyText: '暂无 Worker' }}
        />
      </Card>

      <OperationModal
        open={Boolean(operationId)}
        operationId={operationId}
        title="Worker 排空进度"
        onClose={() => {
          clearOperation();
          query.refetch();
        }}
        onSettled={() => query.refetch()}
      />
    </div>
  );
}
