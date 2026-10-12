import type { DatabaseID } from '../api';

export type ConnectionCapabilities = { enabled: boolean; wsEnabled: boolean; origin: string };

export function parseConnectionCapabilities(value: unknown): ConnectionCapabilities {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Invalid connection capabilities');
  const reply = value as Record<string, unknown>;
  if (reply.version !== 1 || typeof reply.hrana_http !== 'boolean' || typeof reply.public_origin !== 'string') throw new Error('Invalid connection capabilities');
  if (reply.hrana_ws !== undefined && typeof reply.hrana_ws !== 'boolean') throw new Error('Invalid WebSocket capability');
  const url = new URL(reply.public_origin);
  if (url.origin !== reply.public_origin || url.username || url.password || url.pathname !== '/' || url.search || url.hash || !['http:', 'https:'].includes(url.protocol)) throw new Error('Invalid public gateway origin');
  if (url.protocol === 'http:' && !['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)) throw new Error('Remote gateway requires HTTPS');
  return { enabled: reply.hrana_http, wsEnabled: reply.hrana_http && reply.hrana_ws === true, origin: reply.public_origin };
}

export function hranaDatabaseURI(capabilities: ConnectionCapabilities, database: DatabaseID): string {
  if (!capabilities.enabled) throw new Error('Hrana HTTP is not enabled');
  const parts = [database.namespace.tenant, database.namespace.database, database.branch];
  if (parts.some(part => !/^[A-Za-z0-9_-]{1,64}$/.test(part))) throw new Error('Invalid database identity');
  return `${capabilities.origin}/db/${parts.map(encodeURIComponent).join('/')}/`;
}

export function hranaWebSocketURI(capabilities: ConnectionCapabilities, database: DatabaseID): string {
  if (!capabilities.wsEnabled) throw new Error('Hrana WebSocket is not enabled');
  return hranaDatabaseURI(capabilities, database).replace(/^https?:/, capabilities.origin.startsWith('https:') ? 'wss:' : 'ws:');
}
