/**
 * 长操作进度弹窗：提交返回 202 后轮询 /operations/{id}，实时显示进度、状态与错误。
 * 终态时回调 onSettled，便于调用方刷新列表。
 */
import { useEffect, useRef } from 'react';
import { Alert, Descriptions, Modal, Progress, Space, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { useOperationQuery } from '../hooks/useOperationQuery';
import { isTerminalOperation, type Operation } from '../api/types';
import { operationDbId, operationKind } from '../utils/database';
import { formatTime } from '../utils/format';
import { JsonBlock } from './JsonBlock';
import { OperationStateTag } from './StatusTag';
import { ErrorAlert } from './ErrorAlert';
import { DatabaseConnectionCard } from './database/DatabaseConnectionCard';

interface OperationModalProps {
  operationId: string | null;
  open: boolean;
  title?: string;
  onClose: () => void;
  /** 操作到达终态时回调（只触发一次） */
  onSettled?: (op: Operation) => void;
}

function progressStatus(op?: Operation): 'active' | 'success' | 'exception' | 'normal' {
  if (!op) return 'active';
  if (op.state === 'SUCCEEDED') return 'success';
  if (op.state === 'FAILED' || op.state === 'CANCELLED') return 'exception';
  return 'active';
}

export function OperationModal({
  operationId,
  open,
  title = '操作进度',
  onClose,
  onSettled,
}: OperationModalProps): JSX.Element {
  const query = useOperationQuery(operationId, open);
  const op = query.data;
  const settledRef = useRef<string | null>(null);

  useEffect(() => {
    if (!op || !operationId) return;
    if (!isTerminalOperation(op.state)) return;
    if (settledRef.current === operationId) return;
    settledRef.current = operationId;
    onSettled?.(op);
  }, [op, operationId, onSettled]);

  const progress = typeof op?.progress === 'number' ? Math.max(0, Math.min(100, op.progress)) : undefined;

  return (
    <Modal
      open={open}
      title={title}
      onCancel={onClose}
      onOk={onClose}
      okText="关闭"
      cancelButtonProps={{ style: { display: 'none' } }}
      width={640}
      destroyOnClose={false}
    >
      {operationId ? (
        <Space direction="vertical" size={12} style={{ width: '100%' }}>
          {op?.kind === 'CREATE_DB' && op.state === 'SUCCEEDED' && operationDbId(op) ? (
            <DatabaseConnectionCard databaseId={operationDbId(op)!} tokenAction="detail-link" />
          ) : null}
          <Descriptions size="small" column={1} bordered>
            <Descriptions.Item label="operation_id">
              <Typography.Text copyable={{ text: operationId }} code>
                {operationId}
              </Typography.Text>
            </Descriptions.Item>
            <Descriptions.Item label="类型">{operationKind(op)}</Descriptions.Item>
            <Descriptions.Item label="状态">
              <OperationStateTag state={op?.state} />
              {isTerminalOperation(op?.state) ? null : (
                <Typography.Text type="secondary" style={{ marginLeft: 8 }}>
                  每 1s 自动刷新
                </Typography.Text>
              )}
            </Descriptions.Item>
            {operationDbId(op) ? (
              <Descriptions.Item label="数据库">
                <Link to={`/databases/${operationDbId(op)}`}>{operationDbId(op)}</Link>
              </Descriptions.Item>
            ) : null}
            <Descriptions.Item label="提交时间">{formatTime(op?.created_at)}</Descriptions.Item>
            {op?.finished_at ? (
              <Descriptions.Item label="完成时间">{formatTime(op.finished_at)}</Descriptions.Item>
            ) : null}
          </Descriptions>

          <Progress percent={progress ?? (op?.state === 'SUCCEEDED' ? 100 : 0)} status={progressStatus(op)} />

          {query.error ? <ErrorAlert error={query.error} onRetry={() => query.refetch()} /> : null}

          {op?.error_code || op?.error_message ? (
            <Alert
              type="error"
              showIcon
              message={`错误码：${op.error_code ?? '未知'}`}
              description={op.error_message ?? undefined}
            />
          ) : null}

          {op?.result !== undefined && op?.result !== null ? (
            <div>
              <Typography.Text strong>结果</Typography.Text>
              <JsonBlock value={op.result} />
            </div>
          ) : null}
        </Space>
      ) : (
        <Typography.Text type="secondary">未提交任何操作</Typography.Text>
      )}
      {op?.updated_at ? (
        <Typography.Text type="secondary" style={{ display: 'block', marginTop: 8 }}>
          最后更新：{formatTime(op.updated_at)}
        </Typography.Text>
      ) : null}
    </Modal>
  );
}
