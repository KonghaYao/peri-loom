/**
 * 登录页：
 *   1) 账号密码换取管理员 JWT；
 *   2) 登录接口不可用时粘贴管理员 JWT。
 * dbp_ API Token 只供数据库 SDK 使用，不能作为管理台登录凭据。
 */
import { useState } from 'react';
import {
  Alert,
  App as AntdApp,
  Button,
  Card,
  Divider,
  Form,
  Input,
  Space,
  Tabs,
  Typography,
} from 'antd';
import { KeyOutlined, LockOutlined, UserOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { api, isUnimplementedError } from '../api/client';
import { useAuth } from '../hooks/useAuth';
import { ErrorAlert } from '../components/ErrorAlert';

interface AccountForm {
  username: string;
  password: string;
}

interface TokenForm {
  token: string;
  name?: string;
}

export function LoginPage(): JSX.Element {
  const navigate = useNavigate();
  const { signInWithToken } = useAuth();
  const { message } = AntdApp.useApp();

  const [tab, setTab] = useState<'account' | 'token'>('account');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [notice, setNotice] = useState<string | null>(null);

  /** 账号密码登录：接口未实现时自动切换到管理员 JWT 输入 */
  const handleAccountLogin = async (values: AccountForm) => {
    setSubmitting(true);
    setError(null);
    setNotice(null);
    try {
      const token = await api.login({ username: values.username, password: values.password });
      const warning = await signInWithToken(token, values.username);
      if (warning) message.warning(warning);
      navigate('/dashboard', { replace: true });
    } catch (err) {
      if (isUnimplementedError(err)) {
        setTab('token');
        setNotice('后端未提供账号密码登录接口，已切换到「粘贴管理员 JWT」模式。dbp_ API Token 仅供数据库 SDK 使用，不能登录管理台。');
      } else {
        setError(err);
      }
    } finally {
      setSubmitting(false);
    }
  };

  /** 直接使用已有 Token */
  const handleTokenLogin = async (values: TokenForm) => {
    setSubmitting(true);
    setError(null);
    setNotice(null);
    try {
      const warning = await signInWithToken(values.token, values.name?.trim() || undefined);
      if (warning) message.warning(warning);
      navigate('/dashboard', { replace: true });
    } catch (err) {
      setError(err);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="login-shell">
      <Card className="login-card">
        <Space direction="vertical" size={4} style={{ width: '100%' }}>
          <Typography.Title level={4} style={{ marginBottom: 0 }}>
            Peri Loom · DBA Panel
          </Typography.Title>
          <Typography.Text type="secondary">
            基于自建 TursoDB Engine 的 DB Platform 控制台
          </Typography.Text>
        </Space>

        <Divider style={{ margin: '16px 0' }} />

        <ErrorAlert error={error} />
        {notice ? (
          <Alert type="warning" showIcon message={notice} style={{ marginBottom: 12 }} />
        ) : null}

        <Tabs
          activeKey={tab}
          onChange={(key) => {
            setTab(key as 'account' | 'token');
            setError(null);
            setNotice(null);
          }}
          items={[
            {
              key: 'account',
              label: (
                <span>
                  <LockOutlined /> 账号密码
                </span>
              ),
              children: (
                <Form<AccountForm> layout="vertical" onFinish={handleAccountLogin} disabled={submitting}>
                  <Form.Item
                    name="username"
                    label="用户名"
                    rules={[{ required: true, message: '请输入用户名' }]}
                  >
                    <Input prefix={<UserOutlined />} placeholder="username" autoComplete="username" />
                  </Form.Item>
                  <Form.Item
                    name="password"
                    label="密码"
                    rules={[{ required: true, message: '请输入密码' }]}
                  >
                    <Input.Password
                      prefix={<LockOutlined />}
                      placeholder="password"
                      autoComplete="current-password"
                    />
                  </Form.Item>
                  <Button type="primary" htmlType="submit" block loading={submitting}>
                    登录
                  </Button>
                  <Typography.Text type="secondary" style={{ display: 'block', marginTop: 8 }}>
                    若后端未实现该接口，可切换为粘贴管理员 JWT；dbp_ API Token 不能登录。
                  </Typography.Text>
                </Form>
              ),
            },
            {
              key: 'token',
              label: (
                <span>
                  <KeyOutlined /> 粘贴管理员 JWT
                </span>
              ),
              children: (
                <Form<TokenForm> layout="vertical" onFinish={handleTokenLogin} disabled={submitting}>
                  <Form.Item
                    name="token"
                    label="管理员 JWT"
                    rules={[{ required: true, message: '请粘贴管理员 JWT' }]}
                  >
                    <Input.TextArea
                      rows={3}
                      placeholder="eyJhbGciOi...（dbp_ API Token 不可用于登录）"
                      autoComplete="off"
                      spellCheck={false}
                    />
                  </Form.Item>
                  <Form.Item name="name" label="标识（可选，仅用于界面显示）">
                    <Input placeholder="例如：dba-alice" autoComplete="off" />
                  </Form.Item>
                  <Button type="primary" htmlType="submit" block loading={submitting}>
                    使用 JWT 登录
                  </Button>
                  <Typography.Text type="secondary" style={{ display: 'block', marginTop: 8 }}>
                    管理员 JWT 保存在浏览器 localStorage，并在每次请求以
                    <Typography.Text code>Authorization: Bearer</Typography.Text> 发送。
                    <Typography.Text type="danger"> dbp_ 开头的数据库 API Token 不能登录管理台，只能配置在数据库 SDK 中。</Typography.Text>
                  </Typography.Text>
                </Form>
              ),
            },
          ]}
        />
      </Card>
    </div>
  );
}
