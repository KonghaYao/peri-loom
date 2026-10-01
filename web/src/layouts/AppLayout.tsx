/**
 * Panel 主框架：左侧导航 + 顶部工具条 + 内容区。
 * 导航按 DBA 工作流分组：概览 / 数据库 / SQL / Worker / 操作 / 审计 / 设置。
 */
import { useMemo, useState } from 'react';
import { App as AntdApp, Layout, Menu, Space, Tag, Tooltip, Typography, Button } from 'antd';
import {
  ApiOutlined,
  AuditOutlined,
  ClusterOutlined,
  ConsoleSqlOutlined,
  DashboardOutlined,
  DatabaseOutlined,
  LogoutOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  SettingOutlined,
  SyncOutlined,
} from '@ant-design/icons';
import { Link, Outlet, useLocation, useNavigate } from 'react-router-dom';
import { useAuth } from '../hooks/useAuth';
import { useDeployment } from '../hooks/useDeployment';
import { PREF_KEYS, usePreferences, type ThemeMode } from '../hooks/usePreferences';

const { Header, Sider, Content } = Layout;

interface NavItem {
  key: string;
  label: string;
  icon: JSX.Element;
}

const NAV_ITEMS: NavItem[] = [
  { key: '/dashboard', label: '概览', icon: <DashboardOutlined /> },
  { key: '/databases', label: '数据库', icon: <DatabaseOutlined /> },
  { key: '/sql', label: 'SQL 控制台', icon: <ConsoleSqlOutlined /> },
  { key: '/workers', label: 'Worker', icon: <ClusterOutlined /> },
  { key: '/operations', label: '操作中心', icon: <SyncOutlined /> },
  { key: '/audit', label: '审计日志', icon: <AuditOutlined /> },
  { key: '/settings', label: '设置', icon: <SettingOutlined /> },
];

/** 依据当前路径高亮菜单（详情页归属其列表项） */
function selectedKey(pathname: string): string {
  const match = NAV_ITEMS.find((item) => pathname === item.key || pathname.startsWith(`${item.key}/`));
  return match?.key ?? '/dashboard';
}

export function AppLayout(): JSX.Element {
  const location = useLocation();
  const navigate = useNavigate();
  const { user, signOut } = useAuth();
  const deployment = useDeployment().data;
  const { get, set } = usePreferences();
  const { message } = AntdApp.useApp();
  const [collapsed, setCollapsed] = useState(false);
  const themeMode = get<ThemeMode>(PREF_KEYS.theme, 'light');

  const active = useMemo(() => selectedKey(location.pathname), [location.pathname]);

  const toggleTheme = async () => {
    const next: ThemeMode = themeMode === 'dark' ? 'light' : 'dark';
    try {
      await set(PREF_KEYS.theme, next);
    } catch {
      message.warning('主题已切换，但偏好保存到 /panel/preferences 失败');
    }
  };

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider collapsible collapsed={collapsed} trigger={null} theme="dark" width={208}>
        <div className="brand">
          <ApiOutlined style={{ fontSize: 20 }} />
          {!collapsed && (
            <div className="brand__text">
              <div className="brand__title">Peri Loom</div>
              <div className="brand__sub">{deployment?.mode === 'simple' ? '本机实例' : 'DBA Panel'}</div>
            </div>
          )}
        </div>
        <Menu
          theme="dark"
          mode="inline"
          selectedKeys={[active]}
          items={NAV_ITEMS.filter((item) => item.key !== '/workers' || deployment?.capabilities.workers).map((item) => ({
            key: item.key,
            icon: item.icon,
            label: <Link to={item.key}>{item.label}</Link>,
          }))}
        />
      </Sider>

      <Layout>
        <Header className="app-header">
          <Space size={12}>
            <Button
              type="text"
              icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
              onClick={() => setCollapsed((v) => !v)}
            />
            <Typography.Text strong>{NAV_ITEMS.find((i) => i.key === active)?.label}</Typography.Text>
          </Space>
          <Space size={12}>
            <Tooltip title="切换明暗主题（偏好写入 /panel/preferences）">
              <Button size="small" onClick={() => void toggleTheme()}>
                {themeMode === 'dark' ? '深色' : '浅色'}
              </Button>
            </Tooltip>
            {user ? <Tag color="blue">{user}</Tag> : null}
            <Button
              size="small"
              icon={<LogoutOutlined />}
              onClick={() => {
                signOut();
                navigate('/login', { replace: true });
              }}
            >
              退出
            </Button>
          </Space>
        </Header>

        <Content className="app-content">
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  );
}
