/**
 * 结果表格：只渲染已确认的行数（分块渲染），避免大结果集一次性铺满 DOM。
 * 列名来自 NDJSON 的 {columns:[...]} chunk 或普通 JSON 响应的 columns 字段。
 */
import { useMemo } from 'react';
import { Button, Space, Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { cellToString, isNullCell } from '../utils/rows';

interface ResultTableProps {
  columns: string[];
  /** 已渲染的行（调用方负责按渲染上限切片） */
  rows: unknown[][];
  /** 已接收的行总数（可能大于已渲染行数） */
  receivedRows: number;
  /** 每块渲染的行数，用于「继续渲染」按钮文案 */
  renderStep?: number;
  onRenderMore?: () => void;
  /** 列主键前缀，避免同页多结果表 key 冲突 */
  tableKey?: string;
}

export function ResultTable({
  columns,
  rows,
  receivedRows,
  renderStep = 200,
  onRenderMore,
  tableKey = 'result',
}: ResultTableProps): JSX.Element {
  const tableColumns = useMemo<ColumnsType<Record<string, unknown>>>(() => {
    const cols: ColumnsType<Record<string, unknown>> = [
      {
        title: '#',
        dataIndex: '__row_index__',
        key: '__row_index__',
        width: 64,
        fixed: 'left',
        render: (v: unknown) => <Typography.Text type="secondary">{String(v)}</Typography.Text>,
      },
    ];
    columns.forEach((name, index) => {
      cols.push({
        title: name,
        dataIndex: `c${index}`,
        key: `c${index}`,
        ellipsis: true,
        render: (value: unknown) =>
          isNullCell(value) ? (
            <Typography.Text type="secondary" italic>
              NULL
            </Typography.Text>
          ) : (
            <span className="cell-value">{cellToString(value)}</span>
          ),
      });
    });
    return cols;
  }, [columns]);

  const dataSource = useMemo(
    () =>
      rows.map((row, rowIndex) => {
        const record: Record<string, unknown> = { __row_index__: rowIndex + 1, __key: `${tableKey}-${rowIndex}` };
        row.forEach((cell, i) => {
          record[`c${i}`] = cell;
        });
        return record;
      }),
    [rows, tableKey],
  );

  const hidden = Math.max(0, receivedRows - rows.length);

  return (
    <div>
      <Table<Record<string, unknown>>
        size="small"
        bordered
        rowKey="__key"
        columns={tableColumns}
        dataSource={dataSource}
        pagination={false}
        scroll={{ x: 'max-content', y: 420 }}
        locale={{ emptyText: '没有返回行（可能是写入 / DDL 语句）' }}
      />
      <Space style={{ marginTop: 8 }} size={8} wrap>
        <Tag color="blue">已接收 {receivedRows} 行</Tag>
        <Tag>已渲染 {rows.length} 行</Tag>
        {hidden > 0 && onRenderMore ? (
          <Button size="small" onClick={onRenderMore}>
            继续渲染 {Math.min(renderStep, hidden)} 行
          </Button>
        ) : null}
        {hidden > 0 && !onRenderMore ? (
          <Typography.Text type="secondary">其余 {hidden} 行未渲染</Typography.Text>
        ) : null}
      </Space>
    </div>
  );
}
