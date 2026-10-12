import { useEffect, useState } from 'react';
import type { DatabaseID } from '../api';
import { hranaDatabaseURI, hranaWebSocketURI, parseConnectionCapabilities, type ConnectionCapabilities } from './database-connection';

type Props = { database: DatabaseID; globalAdmin: boolean | null; onGetKey: () => void };

export default function DatabaseConnection({ database, globalAdmin, onGetKey }: Props) {
  const [capabilities, setCapabilities] = useState<ConnectionCapabilities | null>(null);
  const [status, setStatus] = useState('Checking gateway connection options…');
  const [notice, setNotice] = useState('');

  useEffect(() => {
    const controller = new AbortController();
    void fetch('/v1/platform/capabilities', { headers: { Accept: 'application/json' }, cache: 'no-store', credentials: 'omit', redirect: 'error', signal: controller.signal })
      .then(async response => {
        if (!response.ok || response.headers.get('Content-Type')?.split(';')[0].trim().toLowerCase() !== 'application/json') throw new Error('Gateway connection options unavailable');
        return parseConnectionCapabilities(await response.json());
      })
      .then(reply => { if (!controller.signal.aborted) { setCapabilities(reply); setStatus(''); } })
      .catch(() => { if (!controller.signal.aborted) setStatus('Could not verify gateway connection options. No libSQL URI is provided.'); });
    return () => controller.abort();
  }, []);

  const uri = capabilities?.enabled ? hranaDatabaseURI(capabilities, database) : '';
  const wsUri = capabilities?.wsEnabled ? hranaWebSocketURI(capabilities, database) : '';
  const copy = async (address: string, label: string) => {
    if (!address) return;
    try { await navigator.clipboard.writeText(address); setNotice(`${label} copied. The project API key is separate and is not included.`); }
    catch { setNotice('Clipboard unavailable; select and copy the address from the field.'); }
  };

  return <div className="database-connection">
    {status && <p role="status">{status}</p>}
    {capabilities?.enabled === false && <div className="credential-warning"><p><strong>Hrana HTTP and WebSocket are disabled on this gateway.</strong> Ask the operator to start the gateway with the experimental adapter enabled; issuing a project SQL key does not enable it. The native API URL is not a libSQL endpoint.</p></div>}
    {uri && <>
      <div className="credential-warning"><p><strong>Experimental Hrana endpoint.</strong> The official JS client connects over HTTP v2; HTTP v3 JSON pipeline/cursor and low-level v3 protobuf are also available. Rust SDK v3 reads and a disposable write batch have passed on loopback; production writes and full libSQL compatibility are not yet verified. Clients must trust this gateway’s TLS certificate.</p></div>
      <div className="form-row"><label className="field">Database URI for @libsql/client<input data-testid="libsql-database-uri" className="input" readOnly value={uri} /></label><button type="button" className="button" onClick={() => void copy(uri, 'Database URI')}>Copy URI</button></div>
      {wsUri && <div className="form-row"><label className="field">Experimental Hrana WebSocket URI<input data-testid="hrana-ws-uri" className="input" readOnly value={wsUri} /></label><button type="button" className="button" onClick={() => void copy(wsUri, 'WebSocket URI')}>Copy WS URI</button></div>}
      <p>Use a project SQL API key as <code>authToken</code>. A global administrator can create one below; it is shown once and permits writes. Never put the key in the URI or localStorage; the global master key cannot run SQL.</p>
      <p>Node.js example (set <code>PROJECT_SQL_API_KEY</code> in your environment):</p>
      <pre className="connection-example"><code>{`import { createClient } from '@libsql/client';\nconst db = createClient({ url: ${JSON.stringify(uri)}, authToken: process.env.PROJECT_SQL_API_KEY });\nconst result = await db.execute('SELECT 1 AS value');`}</code></pre>
    </>}
    <div className="form-row"><label className="field">V1 native API base URL<input className="input" data-testid="native-api-url" readOnly value={capabilities?.origin ?? ''} placeholder="Gateway origin unavailable" /></label><button type="button" className="button" disabled={!capabilities?.origin} onClick={() => void copy(capabilities?.origin ?? '', 'Native API base URL')}>Copy API URL</button></div>
    <p>Selected database: <code>{`${database.namespace.tenant}/${database.namespace.database}/${database.branch}`}</code></p>
    <p>Native V1 SDK: <code>/v1/sql/open</code>, <code>/v1/sql/query</code>, <code>/v1/sql/execute</code>, <code>/v1/sql/close</code>. Do not pass the native base URL to a libSQL SDK.</p>
    {globalAdmin === true && <button type="button" className="button" onClick={onGetKey}>Get a project SQL API key ↓</button>}
    {notice && <p role="status">{notice}</p>}
  </div>;
}
