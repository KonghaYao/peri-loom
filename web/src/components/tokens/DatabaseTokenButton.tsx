import { useRef, useState } from 'react';
import { Alert, App, Button, Modal, Space, Typography } from 'antd';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { api } from '../../api/client';
import type { TokenCreated, TokenInfo } from '../../api/types';
import { ErrorAlert } from '../ErrorAlert';

interface Props {
  databaseId: string;
  defaultName?: string;
}

function isUnrevoked(token: TokenInfo): boolean {
  return !token.revoked_at;
}

export function DatabaseTokenButton({ databaseId, defaultName }: Props): JSX.Element {
  const { modal } = App.useApp();
  const queryClient = useQueryClient();
  const issuingRef = useRef(false);
  const [requesting, setRequesting] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [issued, setIssued] = useState<TokenCreated | null>(null);
  const viewer = useQuery({ queryKey: ['viewer'], queryFn: api.me });
  const canManage = viewer.data?.permissions.some((permission) => permission === '*' || permission === 'token:admin') ?? false;
  const tokens = useQuery({ queryKey: ['tokens'], queryFn: api.tokens.list, enabled: canManage });
  const existing = (tokens.data ?? []).find((token) => token.database_id === databaseId && isUnrevoked(token));

  const issue = async (rotate: boolean) => {
    if (issuingRef.current) return;
    issuingRef.current = true;
    setRequesting(true);
    setError(null);
    try {
      const token = await api.tokens.create({
        name: defaultName || `db-${databaseId}`,
        database_id: databaseId,
        rotate,
        permissions: ['db:read', 'db:write'],
        expires_at: null,
      });
      setIssued(token);
      void queryClient.invalidateQueries({ queryKey: ['tokens'] });
    } catch (err) {
      setError(err);
    } finally {
      issuingRef.current = false;
      setRequesting(false);
    }
  };

  const requestToken = () => {
    if (!existing) {
      void issue(false);
      return;
    }
    modal.confirm({
      title: '轮换本库 Token？',
      content: (
        <Typography.Paragraph>
          轮换会立即吊销本库现有的未吊销 Token，并使正在使用它的客户端失去访问权限。请确认你能及时更新客户端的 <Typography.Text code>DB_TOKEN</Typography.Text>。
        </Typography.Paragraph>
      ),
      okText: '确认轮换',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: () => issue(true),
    });
  };

  const closeIssued = () => setIssued(null);

  return (
    <>
      <Space wrap>
        <Button
          type="primary"
          danger={Boolean(existing)}
          loading={requesting}
          disabled={viewer.isPending || tokens.isPending || tokens.isFetching || tokens.isError || !canManage}
          onClick={requestToken}
        >
          {existing ? '轮换本库 Token' : '申请本库 Token'}
        </Button>
        <Typography.Text type="secondary">
          仅授权当前数据库的 db:read、db:write 权限；Token 不过期，明文只展示一次。
        </Typography.Text>
      </Space>
      <ErrorAlert error={viewer.error || tokens.error || error} onRetry={() => {
        setError(null);
        if (viewer.error) void viewer.refetch();
        if (tokens.error) void tokens.refetch();
      }} />
      {!viewer.isPending && !viewer.isError && !canManage ? (
        <Alert
          type="info"
          showIcon
          style={{ marginTop: 8 }}
          message="需要 token:admin 权限"
          description="当前账号没有本库 Token 申请权限，请联系管理员。服务端会再次校验权限。"
        />
      ) : null}
      <Modal
        open={Boolean(issued)}
        title="本库 Token 已申请"
        onCancel={closeIssued}
        onOk={closeIssued}
        okText="我已保存 Token"
        cancelButtonProps={{ style: { display: 'none' } }}
        closable={false}
        maskClosable={false}
        keyboard={false}
        destroyOnClose
        width={640}
      >
        {issued ? (
          <Space direction="vertical" size={12} style={{ width: '100%' }}>
            <Alert
              type="success"
              showIcon
              message="仅用于此数据库的 SDK authToken"
              description={`数据库：${issued.database_id}`}
            />
            <Typography.Paragraph code copyable={{ text: issued.token }} style={{ wordBreak: 'break-all' }}>
              {issued.token}
            </Typography.Paragraph>
            <Typography.Text>权限：{issued.permissions.join(', ')}</Typography.Text>
            <Typography.Text>有效期：不过期</Typography.Text>
            <Alert
              type="warning"
              showIcon
              message="明文仅展示这一次，请现在复制保存"
              description="关闭后无法再次查看。请存入服务端 DB_TOKEN 环境变量，不要放进浏览器代码。"
            />
          </Space>
        ) : null}
      </Modal>
    </>
  );
}
