import { useState } from 'react';
import { Alert, App, Button, Card, Space, Table, Tag, Typography } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import type { ColumnsType } from 'antd/es/table';
import { Link, useSearchParams } from 'react-router-dom';
import type { TokenInfo } from '../../api/types';
import { api } from '../../api/client';
import { ErrorAlert } from '../ErrorAlert';
import { formatTime } from '../../utils/format';

export function TokenManager(): JSX.Element {
  const [searchParams] = useSearchParams();
  const databaseId = searchParams.get('db') || undefined;
  const [error, setError] = useState<unknown>(null);
  const { modal, message } = App.useApp();
  const queryClient = useQueryClient();
  const viewer = useQuery({ queryKey: ['viewer'], queryFn: api.me });
  const canManage = viewer.data?.permissions.some((permission) => permission === '*' || permission === 'token:admin') ?? false;
  const query = useQuery({ queryKey: ['tokens'], queryFn: api.tokens.list, enabled: canManage });
  const allTokens = query.data ?? [];
  const tokens = databaseId ? allTokens.filter((token) => token.database_id === databaseId) : allTokens;

  const revoke = (token: TokenInfo) => modal.confirm({
    title: '吊销数据库 Token',
    content: `吊销后，使用「${token.name}」的客户端将立即失去访问权限。`,
    okText: '确认吊销', okButtonProps: { danger: true }, cancelText: '取消',
    onOk: async () => {
      try {
        await api.tokens.revoke(token.id);
        void queryClient.invalidateQueries({ queryKey: ['tokens'] });
        message.success('Token 已吊销');
      } catch (err) { setError(err); }
    },
  });

  const columns: ColumnsType<TokenInfo> = [
    { title: '名称', dataIndex: 'name' },
    {
      title: '绑定数据库',
      render: (_, token) => token.database_id
        ? <Link to={`/databases/${encodeURIComponent(token.database_id)}`}>{token.database_id}</Link>
        : <Tag color="error">未绑定（不可用于 SDK）</Tag>,
    },
    { title: '权限', render: (_, token) => token.permissions.length
      ? token.permissions.map((permission) => <Tag key={permission}>{permission}</Tag>)
      : <Tag>无权限</Tag> },
    { title: '创建时间', render: (_, token) => formatTime(token.created_at) },
    { title: '过期时间', render: (_, token) => token.expires_at ? formatTime(token.expires_at) : '不过期' },
    { title: '最近使用', render: (_, token) => formatTime(token.last_used_at) },
    {
      title: '状态',
      render: (_, token) => !token.database_id ? <Tag>不可使用</Tag>
        : token.revoked_at ? <Tag color="error">已吊销</Tag>
          : token.expires_at && Date.parse(token.expires_at) <= Date.now() ? <Tag color="default">已过期</Tag>
            : <Tag color="success">有效</Tag>,
    },
    {
      title: '操作',
      render: (_, token) => (
        <Space>
          {token.database_id ? (
            <Link to={`/databases/${encodeURIComponent(token.database_id)}`}>前往数据库申请/轮换</Link>
          ) : null}
          <Button type="link" danger disabled={Boolean(token.revoked_at)} onClick={() => revoke(token)}>吊销</Button>
        </Space>
      ),
    },
  ];

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <Typography.Paragraph>
        每个数据库的 Token 只能在该数据库详情页申请或轮换；轮换会立即使旧 Token 失效。此页仅用于查看状态和吊销 Token。
      </Typography.Paragraph>
      <ErrorAlert error={viewer.error} onRetry={() => viewer.refetch()} />
      {!viewer.isPending && !viewer.isError && !canManage ? (
        <Alert type="info" showIcon message="当前账号没有 Token 管理权限，请联系实例管理员。" />
      ) : null}
      <Card
        title={databaseId ? `数据库 API Token · ${databaseId}` : '数据库 API Token'}
        extra={(
          <Button icon={<ReloadOutlined />} disabled={!canManage} loading={query.isFetching} onClick={() => query.refetch()}>
            刷新
          </Button>
        )}
      >
        <ErrorAlert error={error || query.error} onRetry={() => {
          setError(null);
          void query.refetch();
        }} />
        <Table<TokenInfo>
          rowKey="id"
          columns={columns}
          dataSource={tokens}
          loading={viewer.isPending || query.isFetching}
          pagination={{ pageSize: 10 }}
          scroll={{ x: 'max-content' }}
          locale={{ emptyText: databaseId ? '该数据库暂无 Token' : '暂无数据库 Token' }}
        />
      </Card>
    </Space>
  );
}
