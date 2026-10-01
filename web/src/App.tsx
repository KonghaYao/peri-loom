/**
 * 应用外壳：未登录只暴露登录页；登录后按 Panel 偏好注入主题，并挂载完整路由。
 */
import { useEffect } from 'react';
import { Alert, Spin } from 'antd';
import { App as AntdApp, ConfigProvider, theme as antdTheme } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import { Navigate, Route, Routes } from 'react-router-dom';
import { useAuth } from './hooks/useAuth';
import { useDeployment } from './hooks/useDeployment';
import { PREF_KEYS, PreferencesProvider, usePreferences, type ThemeMode } from './hooks/usePreferences';
import { AppLayout } from './layouts/AppLayout';
import { LoginPage } from './pages/LoginPage';
import { DashboardPage } from './pages/DashboardPage';
import { DatabaseListPage } from './pages/DatabaseListPage';
import { DatabaseDetailPage } from './pages/DatabaseDetailPage';
import { SqlConsolePage } from './pages/SqlConsolePage';
import { WorkerListPage } from './pages/WorkerListPage';
import { WorkerDetailPage } from './pages/WorkerDetailPage';
import { OperationsPage } from './pages/OperationsPage';
import { AuditPage } from './pages/AuditPage';
import { SettingsPage } from './pages/SettingsPage';
import { NotFoundPage } from './pages/NotFoundPage';

/** 解析实际生效的明暗主题（system 跟随操作系统） */
export function useResolvedTheme(mode: ThemeMode): 'light' | 'dark' {
  if (mode === 'system') {
    const prefersDark =
      typeof window !== 'undefined' && typeof window.matchMedia === 'function'
        ? window.matchMedia('(prefers-color-scheme: dark)').matches
        : false;
    return prefersDark ? 'dark' : 'light';
  }
  return mode;
}

/** 已认证路由树 */
function AuthenticatedRoutes(): JSX.Element {
  const deployment = useDeployment();
  if (deployment.isPending) return <Spin fullscreen tip="读取部署能力…" />;
  if (deployment.isError) return <Alert type="error" showIcon message="无法读取部署能力" description="请检查服务端连接后刷新页面。" />;
  const workers = deployment.data.capabilities.workers;
  return (
    <Routes>
      <Route path="/login" element={<Navigate to="/" replace />} />
      <Route element={<AppLayout />}>
        <Route index element={<Navigate to="/dashboard" replace />} />
        <Route path="/dashboard" element={<DashboardPage />} />
        <Route path="/databases" element={<DatabaseListPage />} />
        <Route path="/databases/:dbId" element={<DatabaseDetailPage />} />
        <Route path="/sql" element={<SqlConsolePage />} />
        <Route path="/workers" element={workers ? <WorkerListPage /> : <Navigate to="/dashboard" replace />} />
        <Route path="/workers/:workerId" element={workers ? <WorkerDetailPage /> : <Navigate to="/dashboard" replace />} />
        <Route path="/operations" element={<OperationsPage />} />
        <Route path="/audit" element={<AuditPage />} />
        <Route path="/settings" element={<SettingsPage />} />
        <Route path="*" element={<NotFoundPage />} />
      </Route>
    </Routes>
  );
}

/** 登录前的极简路由：任何路径都回落到登录页 */
function AnonymousRoutes(): JSX.Element {
  return (
    <Routes>
      <Route path="*" element={<LoginPage />} />
    </Routes>
  );
}

/** 认证后按偏好设置主题 */
function ThemedShell(): JSX.Element {
  const { get } = usePreferences();
  const themeMode = get<ThemeMode>(PREF_KEYS.theme, 'light');
  const resolved = useResolvedTheme(themeMode);

  useEffect(() => {
    document.documentElement.dataset.theme = resolved;
    document.body.style.background = resolved === 'dark' ? '#141414' : '#f5f5f5';
  }, [resolved]);

  return (
    <ConfigProvider
      locale={zhCN}
      theme={{
        algorithm: resolved === 'dark' ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        token: { borderRadius: 6 },
      }}
    >
      <AntdApp>
        <AuthenticatedRoutes />
      </AntdApp>
    </ConfigProvider>
  );
}

export default function App(): JSX.Element {
  const { token } = useAuth();

  if (!token) {
    return (
      <ConfigProvider locale={zhCN}>
        <AntdApp>
          <AnonymousRoutes />
        </AntdApp>
      </ConfigProvider>
    );
  }

  return (
    <PreferencesProvider>
      <ThemedShell />
    </PreferencesProvider>
  );
}
