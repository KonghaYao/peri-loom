import type { ReactNode } from 'react';
import { Dialog } from '@cloudflare/kumo/components/dialog';
import { Table } from '@cloudflare/kumo/components/table';
import { Button } from '@cloudflare/kumo/components/button';
import { Banner } from '@cloudflare/kumo/components/banner';
import { Empty } from '@cloudflare/kumo/components/empty';
import { formatPreviewCell } from '../features/table-browser';

export function Modal({ open, onClose, title, children, footer, danger = false }: { open: boolean; onClose: () => void; title: string; children: ReactNode; footer?: ReactNode; danger?: boolean }) {
  return <Dialog.Root open={open} onOpenChange={value => { if (!value) onClose(); }} role={danger ? 'alertdialog' : 'dialog'}><Dialog className="app-modal">
    <Dialog.Title>{title}</Dialog.Title><div className="modal-body">{children}</div>
    <div className="modal-footer"><Button variant="secondary" onClick={onClose}>取消</Button>{footer}</div>
  </Dialog></Dialog.Root>;
}

export function Grid({ columns, rows }: { columns: string[]; rows: unknown[][] }) {
  return <div className="table-wrap"><Table><Table.Header><Table.Row>{columns.map((column, index) => <Table.Head key={index}>{column}</Table.Head>)}</Table.Row></Table.Header>
    <Table.Body>{rows.map((row, index) => <Table.Row key={index}>{columns.map((_, cell) => <Table.Cell key={cell}><span className="cell" title={formatPreviewCell(row[cell])}>{formatPreviewCell(row[cell])}</span></Table.Cell>)}</Table.Row>)}</Table.Body></Table>
    {!rows.length && <Empty title="没有数据" />}</div>;
}

export function ErrorNotice({ error }: { error: string }) { return error ? <Banner variant="error" role="alert" description={error} /> : null; }
