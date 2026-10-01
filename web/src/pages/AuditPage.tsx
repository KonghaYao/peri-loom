/**
 * 审计日志：分页表格 + 当前页关键字筛选。
 * 说明：契约只冻结了 limit/offset，其它过滤条件在前端对「当前页」生效（见 REPORT 精简项）。
 */
import { useMemo, useState } from 'react';
import { Button, Card, Input, Space, Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { ReloadOutlined, SearchOutlined } from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { api, hasMorePage } from '../api/client';
import type { AuditLog } from '../api/types';
import { ErrorAlert } from '../components/ErrorAlert';
import { JsonBlock } from '../components/JsonBlock';
import { auditActor, auditId } from '../utils/database';
import { formatTime, shortId } from '../utils/format';

function resultColor(result?: string): string {
  if (!result) return 'default';
  const v = result.toUpperCase();
  if (v === 'SUCCESS' || v === 'OK' || v === 'ALLOW') return 'success';
  if (v === 'FAILURE' || v === 'FAILED' || v === 'ERROR' || v === 'DENY') return 'error';
  return 'default';
}

export function AuditPage(): JSX.Element {
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [keyword, setKeyword] = useState('');

  const query = useQuery({
    queryKey: ['audit', page, pageSize],
    queryFn: () => api.audit.list({ limit: pageSize, offset: (page - 1) * pageSize }),
  });

  const logs = query.data?.items ?? [];
  /** 后端分页不返回总数：本页取满 limit 即认为还有下一页 */
  const hasMore = hasMorePage(query.data);

  const rows = useMemo(() => {
    const kw = keyword.trim().toLowerCase();
    if (!kw) return logs;
    return logs.filter((log) => {
      const haystack = [
        auditId(log),
        auditActor(log),
        log.action,
        log.target_id,
        log.target_type,
        log.database_id,
        log.request_id,
        log.source_ip,
      ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase();
      return haystack.includes(kw);
    });
  }, [logs, keyword]);

  const columns: ColumnsType<AuditLog> = [
    {
      title: '时间',
      key: 'created_at',
      width: 175,
      render: (_: unknown, log) => formatTime(log.created_at),
    },
    {
      title: '操作者',
      key: 'actor',
      width: 160,
      render: (_: unknown, log) => auditActor(log),
    },
    {
      title: '动作',
      key: 'action',
      width: 200,
      render: (_: unknown, log) => <Tag color="blue">{log.action}</Tag>,
    },
    {
      title: '对象',
      key: 'target',
      width: 220,
      render: (_: unknown, log) => {
        const target = log.target_id || log.database_id;
        if (!target) return '-';
        return (
          <Typography.Text copyable={{ text: target }} style={{ fontSize: 12 }}>
            {shortId(target, 18, 6)}
          </Typography.Text>
        );
      },
    },
    {
      title: '结果',
      key: 'result',
      width: 100,
      render: (_: unknown, log) => <Tag color={resultColor(log.result)}>{log.result || '-'}</Tag>,
    },
    {
      title: 'request_id',
      key: 'request_id',
      width: 200,
      render: (_: unknown, log) =>
        log.request_id ? (
          <Typography.Text copyable={{ text: log.request_id }} style={{ fontSize: 12 }}>
            {shortId(log.request_id, 14, 6)}
          </Typography.Text>
        ) : (
          '-'
        ),
    },
    { title: '来源 IP', dataIndex: 'source_ip', key: 'source_ip', width: 140, render: (v?: string) => v ?? '-' },
  ];

  return (
    <div>
      <Card
        title={
          <Space>
            <span>审计日志</span>
            <Tag>{logs.length} 条（本页）</Tag>
          </Space>
        }
        extra={
          <Space wrap>
            <Input
              allowClear
              prefix={<SearchOutlined />}
              placeholder="筛选操作者 / 动作 / 对象 / request_id（当前页）"
              style={{ width: 340 }}
              value={keyword}
              onChange={(e) => setKeyword(e.target.value)}
            />
            <Button icon={<ReloadOutlined />} onClick={() => query.refetch()} loading={query.isFetching}>
              刷新
            </Button>
          </Space>
        }
      >
        <ErrorAlert error={query.error} onRetry={() => query.refetch()} />
        <Table<AuditLog>
          size="small"
          rowKey={(log, index) => auditId(log) !== '-' ? auditId(log) : `${index}`}
          columns={columns}
          dataSource={rows}
          loading={query.isLoading}
          scroll={{ x: 'max-content' }}
          expandable={{
            expandedRowRender: (log) => <JsonBlock value={log.detail ?? log} />,
            rowExpandable: () => true,
          }}
          pagination={{
            current: page,
            pageSize,
            total: (page - 1) * pageSize + logs.length + (hasMore ? 1 : 0),
            showSizeChanger: true,
            pageSizeOptions: ['20', '50', '100', '200'],
            showTotal: (_t, range) => `第 ${range[0]}-${range[1]} 条（后端未返回总数）`,
            onChange: (p, size) => {
              setPage(p);
              setPageSize(size);
            },
          }}
          locale={{ emptyText: '暂无审计记录' }}
        />
      </Card>
    </div>
  );
}
