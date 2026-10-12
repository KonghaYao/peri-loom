import { useState, type FormEvent } from 'react';
import { type DatabaseID, createAPI } from '../api';

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
  const [revealed, setRevealed] = useState(false);

  const issue = async (event: FormEvent) => {
    event.preventDefault();
    if (busy || issued || !/^[A-Za-z0-9_.@:-]{1,128}$/.test(subject)) return;
    if (!window.confirm(`Create a new SQL API key for project ${project}? This key can execute SQL (including writes), is shown once, and must be revoked when no longer needed.`)) return;
    setBusy(true);
    setError('');
    setNotice('');
    try {
      const reply = await call('/v1/keys/create', { project, subject, role: 'sql', service_account: false });
      if (!isIssuedProjectSQLKey(reply, project)) throw new Error('Key issuance reply could not be verified; inspect the project before trying again.');
      setIssued(reply);
      setNotice('Project SQL key issued. Copy it or activate it now; it will not be shown again.');
    } catch (failure) {
      setError(`${failure instanceof Error ? failure.message : 'Key issuance outcome unknown'}. Do not automatically retry key creation.`);
    } finally {
      setBusy(false);
    }
  };

  const copy = async () => {
    if (!issued) return;
    try {
      await navigator.clipboard.writeText(issued.credential);
      setNotice('Copied. Store this project key securely; clipboard contents may be visible to other applications.');
    } catch {
      setError('Clipboard unavailable. Reveal the key and copy it manually.');
    }
  };

  return <div className="project-key-request" data-testid="project-sql-key">
    <p>Table names and rows require a project SQL key in this view. Your global management master key can create one for <strong>{project}</strong> to preview <strong>{database.namespace.database} / {database.branch}</strong>.</p>
    {!issued ? <form className="form-row" onSubmit={event => void issue(event)}>
      <label className="field">Key subject<input className="input" value={subject} onChange={event => setSubject(event.target.value)} required maxLength={128} pattern="[A-Za-z0-9_.@:-]+" /></label>
      <button type="submit" className="button button-primary" disabled={busy || !project || !/^[A-Za-z0-9_.@:-]{1,128}$/.test(subject)}>{busy ? 'Creating key…' : 'Create project SQL API key'}</button>
    </form> : <div className="project-key-issued">
      <p><strong>Shown once · SQL role · project {issued.project}</strong></p>
      <label className="field">New project API key<input data-testid="issued-project-key" className="input" readOnly type={revealed ? 'text' : 'password'} value={issued.credential} /></label>
      <div className="action-row">
        <button type="button" className="button" onClick={() => setRevealed(value => !value)}>{revealed ? 'Hide key' : 'Reveal key'}</button>
        <button type="button" className="button" onClick={() => void copy()}>Copy key</button>
        <button type="button" className="button button-primary" disabled={!canActivate} onClick={() => { onActivate(issued.credential); setIssued(null); setRevealed(false); }}>Use key for table preview</button>
      </div>
      {!canActivate && <p>Close the current SQL session before switching credentials.</p>}
      <p>This project key remains in page memory only. Save it in a secure credential manager if needed later; revoke unused keys in Access.</p>
    </div>}
    {notice && <p role="status">{notice}</p>}
    {error && <p className="table-browser-error" role="alert">{error}</p>}
  </div>;
}
