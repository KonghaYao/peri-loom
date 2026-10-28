import { useEffect, useRef, useState, type ReactNode, type RefObject } from 'react';
import { Dialog } from '@cloudflare/kumo/components/dialog';
import { Table } from '@cloudflare/kumo/components/table';
import { Button } from '@cloudflare/kumo/components/button';
import { Banner } from '@cloudflare/kumo/components/banner';
import { Empty } from '@cloudflare/kumo/components/empty';
import { Badge } from '@cloudflare/kumo/components/badge';
import { Check, Copy } from 'lucide-react';
import { formatPreviewCell } from '../features/table-browser';

export function Modal({ open, onClose, title, children, footer, danger = false }: { open: boolean; onClose: () => void; title: string; children: ReactNode; footer?: ReactNode; danger?: boolean }) {
  return <Dialog.Root open={open} onOpenChange={value => { if (!value) onClose(); }} role={danger ? 'alertdialog' : 'dialog'}><Dialog className="app-modal">
    <Dialog.Title>{title}</Dialog.Title><div className="modal-body">{children}</div>
    <div className="modal-footer"><Button variant="secondary" onClick={onClose}>取消</Button>{footer}</div>
  </Dialog></Dialog.Root>;
}

export function Grid({ columns, rows }: { columns: string[]; rows: unknown[][] }) {
  return <div className="table-wrap"><Table><Table.Header><Table.Row>{columns.map((column, index) => <Table.Head key={index}>{column}</Table.Head>)}</Table.Row></Table.Header>
    <Table.Body>{rows.map((row, index) => <Table.Row key={index}>{columns.map((_, cell) => <Table.Cell key={cell}><span className={row[cell] === null ? 'cell cell-null' : 'cell'} title={formatPreviewCell(row[cell])}>{formatPreviewCell(row[cell])}</span></Table.Cell>)}</Table.Row>)}</Table.Body></Table>
    {!rows.length && <Empty title="没有数据" />}</div>;
}

export function ErrorNotice({ error }: { error: string }) { return error ? <Banner variant="error" role="alert" description={error} /> : null; }

const stateMeta: Record<string, { label: string; variant: 'success' | 'warning' | 'error' | 'neutral' }> = {
  active: { label: '运行中', variant: 'success' },
  suspended: { label: '已暂停', variant: 'warning' },
  deleting: { label: '删除中', variant: 'warning' },
  deleted: { label: '已删除', variant: 'neutral' },
  quarantined: { label: '已隔离', variant: 'error' },
};

export function StateBadge({ state, label }: { state?: string; label?: string }) {
  const meta = state ? stateMeta[state] : undefined;
  return <Badge variant={meta?.variant ?? 'neutral'} appearance="dot">{label ?? meta?.label ?? state ?? '未知'}</Badge>;
}

export function CopyButton({ value, label = '复制', copiedLabel = '已复制', onError, disabled = false }: { value: string; label?: string; copiedLabel?: string; onError?: (message: string) => void; disabled?: boolean }) {
  const [copied, setCopied] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => { if (timer.current !== null) clearTimeout(timer.current); }, []);
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(value);
      setCopied(true);
      if (timer.current !== null) clearTimeout(timer.current);
      timer.current = setTimeout(() => setCopied(false), 1600);
    } catch {
      onError?.('复制失败，请手动选择内容。');
    }
  };
  return <Button type="button" variant="secondary" disabled={disabled} icon={copied ? <Check size={15} /> : <Copy size={15} />} onClick={() => void copy()}>{copied ? copiedLabel : label}</Button>;
}

export function useSearchShortcut(ref: RefObject<HTMLInputElement | null>) {
  useEffect(() => {
    const focus = (event: KeyboardEvent) => {
      if (event.key !== '/' || event.metaKey || event.ctrlKey || event.altKey) return;
      const target = event.target as HTMLElement | null;
      if (target && (/^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName) || target.isContentEditable)) return;
      event.preventDefault();
      ref.current?.focus();
      ref.current?.select();
    };
    window.addEventListener('keydown', focus);
    return () => window.removeEventListener('keydown', focus);
  }, [ref]);
}
