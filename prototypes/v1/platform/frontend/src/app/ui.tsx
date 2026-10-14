import type { ReactNode } from 'react';
import { Dialog } from '@cloudflare/kumo/components/dialog';
import { Table } from '@cloudflare/kumo/components/table';
import { Button } from '@cloudflare/kumo/components/button';
import { formatPreviewCell } from '../features/table-browser';

export function Modal({ open, onClose, title, children, danger = false }: { open: boolean; onClose: () => void; title: string; children: ReactNode; danger?: boolean }) {
  return <Dialog.Root open={open} onOpenChange={value => { if (!value) onClose(); }} role={danger ? 'alertdialog' : 'dialog'}><Dialog>
    <Dialog.Title>{title}</Dialog.Title><div className="modal-body">{children}</div>
    <div className="modal-footer"><Button variant="secondary" onClick={onClose}>取消</Button></div>
  </Dialog></Dialog.Root>;
}

export function Grid({ columns, rows }: { columns: string[]; rows: unknown[][] }) {
  return <div className="table-wrap"><Table><Table.Header><Table.Row>{columns.map((column, index) => <Table.Head key={index}>{column}</Table.Head>)}</Table.Row></Table.Header>
    <Table.Body>{rows.map((row, index) => <Table.Row key={index}>{columns.map((_, cell) => <Table.Cell key={cell}><span className="cell" title={formatPreviewCell(row[cell])}>{formatPreviewCell(row[cell])}</span></Table.Cell>)}</Table.Row>)}</Table.Body></Table>
    {!rows.length && <div className="empty">没有数据</div>}</div>;
}

export function ErrorNotice({ error }: { error: string }) { return error ? <div className="alert" role="alert">{error}</div> : null; }
