/** JSON 结果展示（长文本可展开，超长内容折叠） */
import { Typography } from 'antd';

interface JsonBlockProps {
  value: unknown;
  /** 折叠阈值（字符数） */
  collapseAt?: number;
  maxHeight?: number;
}

export function JsonBlock({ value, collapseAt = 400, maxHeight = 260 }: JsonBlockProps): JSX.Element {
  if (value === undefined || value === null || value === '') {
    return <Typography.Text type="secondary">无</Typography.Text>;
  }
  let text: string;
  if (typeof value === 'string') {
    text = value;
  } else {
    try {
      text = JSON.stringify(value, null, 2);
    } catch {
      text = String(value);
    }
  }
  const isLong = text.length > collapseAt;

  return (
    <Typography.Paragraph
      style={{ marginBottom: 0 }}
      ellipsis={isLong ? { rows: 8, expandable: true, symbol: '展开' } : false}
    >
      <pre className="json-block" style={{ maxHeight }}>
        {text}
      </pre>
    </Typography.Paragraph>
  );
}
