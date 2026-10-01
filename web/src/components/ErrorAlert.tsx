/**
 * 错误提示：按错误码展示中文提示，retryable=true 时提供「重试」按钮，
 * 并展示 request_id 便于对照服务端日志。
 */
import { Alert, Button, Space, Typography } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { ApiError, toApiError } from '../api/client';

interface ErrorAlertProps {
  error: unknown;
  /** 重试回调；传入后才会显示重试按钮（且仅当 retryable） */
  onRetry?: () => void;
  /** 忽略 retryable 强制显示重试按钮 */
  alwaysRetry?: boolean;
  closable?: boolean;
  onClose?: () => void;
}

export function ErrorAlert({ error, onRetry, alwaysRetry, closable, onClose }: ErrorAlertProps): JSX.Element | null {
  if (!error) return null;
  const e: ApiError = toApiError(error);

  const description = (
    <Space direction="vertical" size={2} style={{ width: '100%' }}>
      <Typography.Text type="secondary">
        错误码：<Typography.Text code>{e.code}</Typography.Text>
        {e.status ? ` ｜ HTTP ${e.status}` : ''}
        {e.requestId ? (
          <>
            {' ｜ request_id：'}
            <Typography.Text copyable={{ text: e.requestId }} code>
              {e.requestId}
            </Typography.Text>
          </>
        ) : null}
      </Typography.Text>
      {/* 服务端原始 message 与中文提示不同时补充展示 */}
      {e.message && e.message !== e.friendlyMessage ? (
        <Typography.Text type="secondary" style={{ wordBreak: 'break-all' }}>
          服务端消息：{e.message}
        </Typography.Text>
      ) : null}
    </Space>
  );

  const action =
    onRetry && (alwaysRetry || e.retryable) ? (
      <Button size="small" icon={<ReloadOutlined />} onClick={onRetry}>
        重试
      </Button>
    ) : undefined;

  return (
    <Alert
      type="error"
      showIcon
      style={{ marginBottom: 12 }}
      message={e.friendlyMessage}
      description={description}
      action={action}
      closable={closable}
      onClose={onClose}
    />
  );
}
