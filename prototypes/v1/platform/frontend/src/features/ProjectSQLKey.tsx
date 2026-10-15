import { useState } from 'react';
import { Button } from '@cloudflare/kumo/components/button';
import { Input } from '@cloudflare/kumo/components/input';
import { SensitiveInput } from '@cloudflare/kumo/components/sensitive-input';
import { Badge } from '@cloudflare/kumo/components/badge';
import { Banner } from '@cloudflare/kumo/components/banner';
import { type DatabaseID, createAPI } from '../api';
import { Modal } from '../app/ui';

type IssuedKey = { key: string; credential: string; project: string; role: string; shown_once: boolean };
type Props = {
  project: string;
  database: DatabaseID;
  call: ReturnType<typeof createAPI>;
  canActivate: boolean;
  onActivate: (credential: string) => void;
};

export function isIssuedProjectSQLKey(value: unknown, project: string): value is IssuedKey {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const reply = value as Record<string, unknown>;
  return typeof reply.key === 'string' && /^[a-f0-9]{32}$/.test(reply.key)
    && typeof reply.credential === 'string' && /^plk1_[a-f0-9]{32}_[A-Za-z0-9_-]{43}$/.test(reply.credential)
    && reply.project === project && reply.role === 'sql' && reply.shown_once === true;
}

export default function ProjectSQLKey({ project, database, call, canActivate, onActivate }: Props) {
  const [subject, setSubject] = useState('console_user');
  const [busy, setBusy] = useState(false);
  const [issued, setIssued] = useState<IssuedKey | null>(null);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [confirmOpen, setConfirmOpen] = useState(false);

  const issue = async () => {
    if (busy || issued || !/^[A-Za-z0-9_.@:-]{1,128}$/.test(subject)) return;
    setConfirmOpen(false);
    setBusy(true);
    setError('');
    setNotice('');
    try {
      const reply = await call('/v1/keys/create', { project, subject, role: 'sql', service_account: false });
      if (!isIssuedProjectSQLKey(reply, project)) throw new Error('无法确认凭证创建结果，请先检查项目凭证，不要重复创建。');
      setIssued(reply);
      setNotice('项目 SQL 凭证已创建。仅显示这一次，请保存后启用。');
    } catch (failure) {
      setError(`${failure instanceof Error ? failure.message : '凭证创建结果未知'}。请勿自动重试创建。`);
    } finally {
      setBusy(false);
    }
  };

  const copy = async () => {
    if (!issued) return;
    try {
      await navigator.clipboard.writeText(issued.credential);
      setNotice('已复制。请安全保存项目凭证，剪贴板内容可能被其他应用读取。');
    } catch {
      setError('无法使用剪贴板，请手动复制凭证。');
    }
  };

  return <div className="stack" data-testid="project-sql-key">
    <p className="muted">平台管理员可为项目 {project} 创建 SQL 凭证，查看 {database.namespace.database} / {database.branch} 的数据。</p>
    {!issued ? <form className="actions" onSubmit={event => { event.preventDefault(); setConfirmOpen(true); }}>
      <Input label="凭证名称" value={subject} onChange={event => setSubject(event.target.value)} required maxLength={128} pattern="[A-Za-z0-9_.@:-]+" />
      <Button type="submit" variant="secondary" disabled={busy || !project || !/^[A-Za-z0-9_.@:-]{1,128}$/.test(subject)}>{busy ? '创建中…' : '创建项目 SQL 凭证'}</Button>
    </form> : <div className="stack">
      <p><Badge variant="orange">仅显示一次 · SQL 读写权限</Badge> · 项目 {issued.project}</p>
      <SensitiveInput label="新项目 SQL 凭证" data-testid="issued-project-key" readOnly value={issued.credential} />
      <div className="actions">
        <Button type="button" variant="secondary" onClick={() => void copy()}>复制凭证</Button>
        <Button type="button" variant="primary" disabled={!canActivate} onClick={() => { onActivate(issued.credential); setIssued(null); }}>使用凭证查看数据</Button>
      </div>
      {!canActivate && <p>请先结束当前 SQL 会话再切换凭证。</p>}
      <p className="muted">凭证仅保留在当前页面内存中；请妥善保存，不再需要时可在“访问凭证”中撤销。</p>
    </div>}
    {notice && <p role="status">{notice}</p>}
    {error && <Banner variant="error" role="alert" description={error} />}
    <Modal open={confirmOpen} onClose={() => setConfirmOpen(false)} title="创建项目 SQL 凭证" danger footer={<Button type="button" variant="primary" disabled={busy} onClick={() => void issue()}>确认创建</Button>}>
      <Banner variant="alert" description={`确认为项目 ${project} 创建新凭证？它拥有 SQL 读写权限，仅显示一次；不用时请撤销。`} />
    </Modal>
  </div>;
}
