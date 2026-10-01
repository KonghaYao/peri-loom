/**
 * SQL 编辑器：textarea + 行号槽（滚动同步），Ctrl/Cmd + Enter 执行。
 * 刻意不引入 Monaco/CodeMirror：Panel 只需要「够用的编辑体验」，保持包体精简。
 */
import { useMemo, useRef, type KeyboardEvent, type ReactNode } from 'react';
import { Button, Space } from 'antd';
import { CaretRightOutlined, ClearOutlined } from '@ant-design/icons';

interface SqlEditorProps {
  value: string;
  onChange: (value: string) => void;
  onRun?: () => void;
  onClear?: () => void;
  height?: number;
  readOnly?: boolean;
  running?: boolean;
  placeholder?: string;
  /** 右下角附加操作区（如「保存为 Saved SQL」） */
  extra?: ReactNode;
}

export function SqlEditor({
  value,
  onChange,
  onRun,
  onClear,
  height = 220,
  readOnly = false,
  running = false,
  placeholder = '-- 在此输入 SQL，Ctrl/Cmd + Enter 执行',
  extra,
}: SqlEditorProps): JSX.Element {
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const gutterRef = useRef<HTMLDivElement>(null);

  const lineCount = useMemo(() => value.split('\n').length, [value]);
  const lineNumbers = useMemo(() => Array.from({ length: lineCount }, (_, i) => i + 1), [lineCount]);

  const syncScroll = () => {
    if (gutterRef.current && textareaRef.current) {
      gutterRef.current.scrollTop = textareaRef.current.scrollTop;
    }
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      onRun?.();
      return;
    }
    // Tab 缩进两个空格，避免焦点跳出编辑器
    if (e.key === 'Tab' && !readOnly) {
      e.preventDefault();
      const el = e.currentTarget;
      const start = el.selectionStart;
      const end = el.selectionEnd;
      const next = `${value.slice(0, start)}  ${value.slice(end)}`;
      onChange(next);
      requestAnimationFrame(() => {
        el.selectionStart = start + 2;
        el.selectionEnd = start + 2;
      });
    }
  };

  return (
    <div className="sql-editor">
      <div className="sql-editor__body" style={{ height }}>
        <div className="sql-editor__gutter" ref={gutterRef} aria-hidden>
          {lineNumbers.map((n) => (
            <div key={n} className="sql-editor__line-no">
              {n}
            </div>
          ))}
        </div>
        <textarea
          ref={textareaRef}
          className="sql-editor__textarea"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          onScroll={syncScroll}
          onKeyDown={handleKeyDown}
          spellCheck={false}
          wrap="off"
          readOnly={readOnly}
          placeholder={placeholder}
        />
      </div>
      <div className="sql-editor__toolbar">
        <Space>
          <Button
            type="primary"
            icon={<CaretRightOutlined />}
            onClick={onRun}
            loading={running}
            disabled={readOnly || !value.trim()}
          >
            执行
          </Button>
          <Button icon={<ClearOutlined />} onClick={onClear} disabled={!value}>
            清空
          </Button>
          <span className="sql-editor__hint">Ctrl/Cmd + Enter 执行</span>
        </Space>
        {extra}
      </div>
    </div>
  );
}
