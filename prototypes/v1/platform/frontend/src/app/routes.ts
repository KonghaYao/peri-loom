export type Route = { kind: string; project?: string; database?: string; branch?: string };
const segment = (value: string) => encodeURIComponent(value);

export const projectPath = (project: string, page = 'databases') => `/projects/${segment(project)}/${page}`;
export const databasePath = (project: string, database: string, branch: string, page = 'data') =>
  `${projectPath(project)}/${segment(database)}/branches/${segment(branch)}/${page}`;
export const branchesPath = (project: string, database: string) => `${projectPath(project)}/${segment(database)}/branches`;

export function parseRoute(hash: string): Route {
  const path = hash.replace(/^#/, '').split('?')[0];
  if (path === '/connect' || !path) return { kind: 'connect' };
  if (path === '/preferences') return { kind: 'preferences' };
  if (path.startsWith('/platform/')) return { kind: `platform/${path.slice(10)}` };
  const parts = path.split('/').filter(Boolean);
  if (parts[0] !== 'projects' || !parts[1]) return { kind: 'connect' };
  try {
    const project = decodeURIComponent(parts[1]);
    if (parts[2] === 'access' || parts[2] === 'settings') return { kind: parts[2], project };
    if (parts[2] !== 'databases') return { kind: 'databases', project };
    if (!parts[3]) return { kind: 'databases', project };
    const database = decodeURIComponent(parts[3]);
    if (parts[4] !== 'branches') return { kind: 'databases', project };
    if (!parts[5]) return { kind: 'branches', project, database };
    return { kind: ['data', 'sql', 'recovery', 'activity', 'settings'].includes(parts[6]) ? parts[6] : 'data', project, database, branch: decodeURIComponent(parts[5]) };
  } catch { return { kind: 'connect' }; }
}

let navigationBlocker: (() => boolean) | null = null;
export function setNavigationBlocker(blocker: (() => boolean) | null) { navigationBlocker = blocker; }
export function navigate(path: string) { if (!navigationBlocker || navigationBlocker()) window.location.hash = path; }
