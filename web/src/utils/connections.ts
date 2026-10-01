export interface DatabaseConnections {
  libsqlUrl: string;
  tursoUrl: string;
  port: string;
  dataApiBase: string;
  queryEndpoint: string;
}

/** Derive public endpoints from the Panel origin so nginx and Vite proxy paths stay reachable. */
export function databaseConnections(databaseId: string, origin = window.location.origin): DatabaseConnections {
  const base = new URL(origin);
  const port = base.port || (base.protocol === 'https:' ? '443' : '80');
  const hostPort = `${base.hostname}:${port}`;
  const databasePath = `/db/${encodeURIComponent(databaseId)}/`;
  const libsqlTls = base.protocol === 'https:' ? '' : '?tls=0';
  const dataApiBase = `${base.origin}/data/v1`;

  return {
    libsqlUrl: `libsql://${hostPort}${databasePath}${libsqlTls}`,
    tursoUrl: `${base.protocol}//${hostPort}${databasePath}`,
    port,
    dataApiBase,
    queryEndpoint: `${dataApiBase}/databases/${encodeURIComponent(databaseId)}/query`,
  };
}
