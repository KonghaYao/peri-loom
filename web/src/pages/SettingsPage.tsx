/**
 * 设置：API Token 管理（创建后明文只显示一次）、Panel 偏好（存 /panel/preferences/{key}）、Saved SQL 管理。
 */
import { useState } from 'react';
import {
  Alert,
  App as AntdApp,
  Button,
  Card,
  Form,
  Input,
  Modal,
  Select,
  Space,
  Switch,
  Table,
  Tabs,
  Tag,
  Typography,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { DeleteOutlined, PlusOutlined, ReloadOutlined } from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { api, toItems } from '../api/client';
import type { SavedQuery } from '../api/types';
import { TokenManager } from '../components/tokens/TokenManager';
import { ErrorAlert } from '../components/ErrorAlert';
import { PREF_KEYS, usePreferences, type ThemeMode } from '../hooks/usePreferences';
import { dbName } from '../utils/database';
import { formatTime } from '../utils/format';

// ---------------------------------------------------------------- Panel 偏好

interface PrefForm {
  theme: ThemeMode;
  pageSize: number;
  sqlStream: boolean;
  defaultDatabaseId?: string;
  tableDensity: 'default' | 'middle' | 'small';
}

function PreferencePanel(): JSX.Element {
  const { message } = AntdApp.useApp();
  const { get, set, loading, error, reload } = usePreferences();
  const [saving, setSaving] = useState(false);
  const [form] = Form.useForm<PrefForm>();

  const databasesQuery = useQuery({
    queryKey: ['databases', 'pref-options'],
    queryFn: () => api.databases.list({ limit: 200 }),
    staleTime: 60_000,
  });

  const initialValues: PrefForm = {
    theme: get<ThemeMode>(PREF_KEYS.theme, 'light'),
    pageSize: get<number>(PREF_KEYS.pageSize, 20),
    sqlStream: get<boolean>(PREF_KEYS.sqlStream, true),
    defaultDatabaseId: get<string>(PREF_KEYS.defaultDatabaseId, '') || undefined,
    tableDensity: get<PrefForm['tableDensity']>(PREF_KEYS.tableDensity, 'small'),
  };

  const handleSave = async () => {
    const values = await form.validateFields();
    setSaving(true);
    try {
      await Promise.all([
        set(PREF_KEYS.theme, values.theme),
        set(PREF_KEYS.pageSize, values.pageSize),
        set(PREF_KEYS.sqlStream, values.sqlStream),
        set(PREF_KEYS.defaultDatabaseId, values.defaultDatabaseId ?? ''),
        set(PREF_KEYS.tableDensity, values.tableDensity),
      ]);
      message.success('偏好已保存到 /panel/preferences');
    } catch (err) {
      message.error('保存失败，请检查后端 /panel/preferences 接口');
      console.error(err);
    } finally {
      setSaving(false);
    }
  };

  return (
    <Card
      size="small"
      title="Panel 偏好"
      extra={
        <Button size="small" icon={<ReloadOutlined />} onClick={reload} loading={loading}>
          重新读取
        </Button>
      }
    >
      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 12 }}
        message="偏好按 key 存储：GET/PUT /api/v1/panel/preferences/{key}（404 表示未设置）"
        description={error ? `部分偏好读取失败：${error}` : undefined}
      />
      <Form<PrefForm> form={form} layout="vertical" initialValues={initialValues} style={{ maxWidth: 460 }}>
        <Form.Item name="theme" label="主题">
          <Select
            options={[
              { value: 'light', label: '浅色' },
              { value: 'dark', label: '深色' },
              { value: 'system', label: '跟随系统' },
            ]}
          />
        </Form.Item>
        <Form.Item name="pageSize" label="默认每页条数">
          <Select
            options={[10, 20, 50, 100].map((v) => ({ value: v, label: `${v} 条` }))}
          />
        </Form.Item>
        <Form.Item name="tableDensity" label="表格密度">
          <Select
            options={[
              { value: 'small', label: '紧凑' },
              { value: 'middle', label: '中等' },
              { value: 'default', label: '宽松' },
            ]}
          />
        </Form.Item>
        <Form.Item name="sqlStream" label="SQL 控制台默认使用 NDJSON 流式" valuePropName="checked">
          <Switch />
        </Form.Item>
        <Form.Item name="defaultDatabaseId" label="SQL 控制台默认数据库">
          <Select
            allowClear
            showSearch
            optionFilterProp="label"
            placeholder="不设置"
            loading={databasesQuery.isLoading}
            options={toItems(databasesQuery.data).map((db) => ({
              value: db.id,
              label: `${dbName(db)}（${db.state}）`,
            }))}
          />
        </Form.Item>
        <Button type="primary" onClick={() => void handleSave()} loading={saving}>
          保存偏好
        </Button>
      </Form>
    </Card>
  );
}

// ---------------------------------------------------------------- Saved SQL

function SavedQueryPanel(): JSX.Element {
  const { message, modal } = AntdApp.useApp();
  const [dbFilter, setDbFilter] = useState<string | undefined>(undefined);
  const [createOpen, setCreateOpen] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [form] = Form.useForm<{ name: string; sql: string; description?: string; database_id?: string }>();

  const databasesQuery = useQuery({
    queryKey: ['databases', 'saved-query-options'],
    queryFn: () => api.databases.list({ limit: 200 }),
    staleTime: 60_000,
  });

  const query = useQuery({
    queryKey: ['saved-queries'],
    // 后端不支持按 database_id 过滤，只能取回后在前端筛
    queryFn: () => api.savedQueries.list(),
  });
  const allItems = toItems(query.data);
  const items = dbFilter ? allItems.filter((q) => q.database_id === dbFilter) : allItems;

  const handleCreate = async () => {
    const values = await form.validateFields();
    try {
      await api.savedQueries.create({
        name: values.name.trim(),
        sql: values.sql,
        ...(values.database_id ? { database_id: values.database_id } : {}),
        ...(values.description?.trim() ? { description: values.description.trim() } : {}),
      });
      message.success('已保存');
      setCreateOpen(false);
      form.resetFields();
      void query.refetch();
    } catch (err) {
      setError(err);
      setCreateOpen(false);
    }
  };

  const handleDelete = (item: SavedQuery) => {
    modal.confirm({
      title: '删除 Saved SQL',
      content: `确认删除「${item.name}」？`,
      okText: '删除',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: async () => {
        try {
          await api.savedQueries.remove(item.id);
          message.success('已删除');
          void query.refetch();
        } catch (err) {
          setError(err);
        }
      },
    });
  };

  const columns: ColumnsType<SavedQuery> = [
    { title: '名称', key: 'name', render: (_: unknown, q) => q.name },
    {
      title: 'SQL',
      key: 'sql',
      render: (_: unknown, q) => (
        <Typography.Text code style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
          {q.sql.slice(0, 120)}
          {q.sql.length > 120 ? ' …' : ''}
        </Typography.Text>
      ),
    },
    { title: '数据库', key: 'database_id', width: 180, render: (_: unknown, q) => q.database_id ?? '（通用）' },
    { title: '描述', key: 'description', width: 180, render: (_: unknown, q) => q.description ?? '-' },
    { title: '创建时间', key: 'created_at', width: 175, render: (_: unknown, q) => formatTime(q.created_at) },
    {
      title: '操作',
      key: 'actions',
      width: 100,
      render: (_: unknown, q) => (
        <Button size="small" danger type="link" icon={<DeleteOutlined />} onClick={() => handleDelete(q)}>
          删除
        </Button>
      ),
    },
  ];

  return (
    <Card
      size="small"
      title={<Space>Saved SQL<Tag>{items.length}</Tag></Space>}
      extra={
        <Space>
          <Select
            allowClear
            placeholder="按数据库过滤"
            style={{ width: 220 }}
            value={dbFilter}
            onChange={setDbFilter}
            loading={databasesQuery.isLoading}
            options={toItems(databasesQuery.data).map((db) => ({
              value: db.id,
              label: dbName(db),
            }))}
          />
          <Button icon={<ReloadOutlined />} onClick={() => query.refetch()} loading={query.isFetching}>
            刷新
          </Button>
          <Button type="primary" icon={<PlusOutlined />} onClick={() => setCreateOpen(true)}>
            新建
          </Button>
        </Space>
      }
    >
      <ErrorAlert error={query.error} onRetry={() => query.refetch()} />
      <ErrorAlert error={error} onRetry={() => setError(null)} alwaysRetry closable onClose={() => setError(null)} />
      <Table<SavedQuery>
        size="small"
        rowKey={(q) => q.id}
        columns={columns}
        dataSource={items}
        loading={query.isLoading}
        pagination={{ pageSize: 10, hideOnSinglePage: true }}
        scroll={{ x: 'max-content' }}
        locale={{ emptyText: '暂无 Saved SQL' }}
      />

      <Modal
        open={createOpen}
        title="新建 Saved SQL"
        onCancel={() => setCreateOpen(false)}
        onOk={() => void handleCreate()}
        okText="保存"
        width={640}
        destroyOnClose
      >
        <Form form={form} layout="vertical" preserve={false}>
          <Form.Item name="name" label="名称" rules={[{ required: true, message: '请输入名称' }]}>
            <Input placeholder="例如：慢查询 Top20" />
          </Form.Item>
          <Form.Item name="sql" label="SQL" rules={[{ required: true, message: '请输入 SQL' }]}>
            <Input.TextArea rows={6} placeholder="select * from ..." style={{ fontFamily: 'monospace' }} />
          </Form.Item>
          <Form.Item name="database_id" label="关联数据库（可选）">
            <Select
              allowClear
              showSearch
              optionFilterProp="label"
              placeholder="不关联"
              options={toItems(databasesQuery.data).map((db) => ({ value: db.id, label: dbName(db) }))}
            />
          </Form.Item>
          <Form.Item name="description" label="描述（可选）">
            <Input placeholder="用途说明" />
          </Form.Item>
        </Form>
      </Modal>
    </Card>
  );
}

// ---------------------------------------------------------------- 页面

export function SettingsPage(): JSX.Element {
  return (
    <Card title="设置">
      <Tabs
        items={[
          { key: 'tokens', label: 'API Token', children: <TokenManager /> },
          { key: 'preferences', label: 'Panel 偏好', children: <PreferencePanel /> },
          { key: 'saved-queries', label: 'Saved SQL', children: <SavedQueryPanel /> },
        ]}
      />
    </Card>
  );
}
