import assert from "node:assert/strict";
import { constants } from "node:fs";
import { open, realpath } from "node:fs/promises";
import { Client, PeriError } from "../sdk/typescript/dist/client.js";

let stage = "private connection";

async function smoke() {
  if (process.argv.length !== 3 || typeof process.getuid !== "function" || !Number.isInteger(constants.O_NOFOLLOW)) throw new Error("connection path required");
  const file = await open(process.argv[2], constants.O_RDONLY | constants.O_NOFOLLOW);
  let connection;
  try {
    const metadata = await file.stat();
    if (!metadata.isFile() || (metadata.mode & 0o777) !== 0o600 || metadata.uid !== process.getuid() || metadata.size > 65536) throw new Error("private connection required");
    const bytes = Buffer.alloc(65537);
    let offset = 0;
    while (offset < bytes.length) {
      const { bytesRead } = await file.read(bytes, offset, bytes.length - offset, null);
      if (!bytesRead) break;
      offset += bytesRead;
    }
    if (offset > 65536) throw new Error("connection limit");
    connection = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, offset)));
  } finally {
    await file.close();
  }
  stage = "verified TLS configuration";
  if (typeof connection.endpoint !== "string" || !connection.endpoint.startsWith("https://") || !process.env.NODE_EXTRA_CA_CERTS || process.env.NODE_TLS_REJECT_UNAUTHORIZED === "0") throw new Error("verified TLS required");
  if (await realpath(process.env.NODE_EXTRA_CA_CERTS) !== await realpath(connection.ca_file)) throw new Error("CA mismatch");
  const client = new Client(connection.endpoint, connection.project_api_key, { timeoutMS: 3000 });
  stage = "readonly management status";
  const status = await client.management("status", { database: connection.database, request_id: "ts-live-readonly-status" });
  if (typeof status !== "object" || status === null || Array.isArray(status)) throw new Error("status shape");
  stage = "session open";
  const session = await client.open(connection.database);
  try {
    stage = "SELECT exact integers";
    const rows = await session.query("SELECT 9223372036854775807 AS max_i64, -9007199254740993 AS negative_i64", "ts-live-exact-integers");
    assert.deepEqual(rows, [[9223372036854775807n, -9007199254740993n]]);
  } finally {
    stage = "explicit session close";
    await session.close();
  }
  console.log("TypeScript SDK live verified-TLS SELECT exact-bigint/status/explicit-close PASS");
}

smoke().catch((failure) => {
  const code = failure instanceof PeriError ? failure.code : "helper_precondition";
  console.error(`TypeScript SDK live FAIL: ${stage} code=${code}`);
  process.exitCode = 1;
});
