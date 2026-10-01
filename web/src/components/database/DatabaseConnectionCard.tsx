import { Card, Descriptions, Space, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { databaseConnections } from '../../utils/connections';
import { DatabaseTokenButton } from '../tokens/DatabaseTokenButton';

interface Props {
  databaseId: string;
  tokenAction?: 'request' | 'detail-link';
}

export function DatabaseConnectionCard({ databaseId, tokenAction = 'request' }: Props): JSX.Element {
  const { libsqlUrl, tursoUrl, port, dataApiBase, queryEndpoint } = databaseConnections(databaseId);

  return (
    <>
      <Card title="数据库连接" style={{ marginBottom: 16 }}>
        <Typography.Paragraph type="secondary">
          两个 SDK 共用当前 Panel 的主机和端口（{port}），通过数据库路径定位本库；平台不会为每个数据库单独监听端口。本卡创建的数据库 API Token 只供此库的 SDK 使用，不能登录管理台；管理台使用账号密码或管理员 JWT。
        </Typography.Paragraph>
        <Descriptions size="small" column={1} bordered>
          <Descriptions.Item label="libSQL 地址 · @libsql/client">
            <Typography.Text copyable={{ text: libsqlUrl }} code>{libsqlUrl}</Typography.Text>
          </Descriptions.Item>
          <Descriptions.Item label="TursoDB 地址 · @tursodatabase/serverless">
            <Typography.Text copyable={{ text: tursoUrl }} code>{tursoUrl}</Typography.Text>
          </Descriptions.Item>
          <Descriptions.Item label="数据库标识">
            <Typography.Text copyable={{ text: databaseId }} code>{databaseId}</Typography.Text>
          </Descriptions.Item>
          <Descriptions.Item label="凭据参数">
            <Typography.Text code>authToken: process.env.DB_TOKEN!</Typography.Text>
          </Descriptions.Item>
          <Descriptions.Item label="libSQL SDK 示例">
            <Typography.Text copyable={{ text: `createClient({ url: '${libsqlUrl}', authToken: process.env.DB_TOKEN! })` }} code>
              {`createClient({ url: '${libsqlUrl}', authToken: process.env.DB_TOKEN! })`}
            </Typography.Text>
          </Descriptions.Item>
          <Descriptions.Item label="TursoDB SDK 示例">
            <Typography.Text copyable={{ text: `connect({ url: '${tursoUrl}', authToken: process.env.DB_TOKEN! })` }} code>
              {`connect({ url: '${tursoUrl}', authToken: process.env.DB_TOKEN! })`}
            </Typography.Text>
          </Descriptions.Item>
          <Descriptions.Item label="Data API 基址">
            <Typography.Text copyable={{ text: dataApiBase }} code>{dataApiBase}</Typography.Text>
          </Descriptions.Item>
          <Descriptions.Item label="查询端点">
            <Typography.Text copyable={{ text: `POST ${queryEndpoint}` }} code>{`POST ${queryEndpoint}`}</Typography.Text>
          </Descriptions.Item>
        </Descriptions>
        <Space wrap style={{ marginTop: 12 }}>
          {tokenAction === 'detail-link' ? (
            <Link to={`/databases/${encodeURIComponent(databaseId)}`}>前往详情申请/轮换 Token</Link>
          ) : (
            <DatabaseTokenButton key={databaseId} databaseId={databaseId} defaultName={`db-${databaseId}`} />
          )}
          <Link to={`/tokens?db=${encodeURIComponent(databaseId)}`}>管理本库 Token</Link>
        </Space>
      </Card>
    </>
  );
}
