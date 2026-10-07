import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import test from "node:test";

const source = readFileSync(new URL("./sdk-live.mjs", import.meta.url), "utf8");
const sdkImport = 'import { Client, PeriError } from "../sdk/typescript/dist/client.js";';
assert.equal(source.split(sdkImport).length, 2);

function run(mode) {
  const directory = mkdtempSync(join(tmpdir(), "peri-sdk-live-test-"));
  try {
    const ca = join(directory, "fake-ca.pem");
    const connection = join(directory, "connection.json");
    writeFileSync(ca, "fake certificate unused by fake SDK", { mode: 0o600 });
    writeFileSync(connection, JSON.stringify({ endpoint: "https://127.0.0.1:1", project_api_key: "fake-key-never-logged", database: {}, ca_file: ca }), { mode: 0o600 });
    const fake = `
      const mode = ${JSON.stringify(mode)};
      export class PeriError extends Error { constructor(code) { super("fake_sdk_failure"); this.code=code; } }
      export class Client {
        async management() { console.log("FAKE_CALL management"); return {}; }
        async open() {
          console.log("FAKE_CALL open");
          return {
            async query() {
              console.log("FAKE_CALL query");
              if (mode === "unknown") throw new PeriError("outcome_unknown");
              if (mode === "disconnect") throw new Error("fake socket disconnected");
              if (mode === "bad_oracle") return [[0n,0n]];
              return [[9223372036854775807n,-9007199254740993n]];
            },
            async close() {
              console.log("FAKE_CALL close");
              if (mode === "close_failure") throw new PeriError("outcome_unknown");
            }
          };
        }
      }
    `;
    const fakeURL = `data:text/javascript;base64,${Buffer.from(fake).toString("base64")}`;
    const actual = source.replace(sdkImport, `import { Client, PeriError } from ${JSON.stringify(fakeURL)};`);
    const helperURL = `data:text/javascript;base64,${Buffer.from(actual).toString("base64")}`;
    const entry = `process.argv=[process.execPath,"actual-helper",${JSON.stringify(connection)}];process.env.NODE_EXTRA_CA_CERTS=${JSON.stringify(ca)};await import(${JSON.stringify(helperURL)});`;
    const result = spawnSync(process.execPath, ["--input-type=module", "--eval", entry], { cwd: directory, env: { PATH: process.env.PATH }, encoding: "utf8", timeout: 5000, maxBuffer: 65536 });
    assert.equal(result.error, undefined);
    assert.equal(result.signal, null);
    assert.equal(`${result.stdout}${result.stderr}`.includes("fake-key-never-logged"), false);
    return result;
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

for (const mode of ["unknown", "disconnect", "bad_oracle"]) {
  test(`actual helper ${mode} stops without Close or PASS`, () => {
    const result = run(mode);
    assert.equal(result.status, 1);
    assert.equal(result.stdout.match(/FAKE_CALL query/g)?.length, 1);
    assert.equal(result.stdout.includes("FAKE_CALL close"), false);
    assert.equal(result.stdout.includes(" PASS"), false);
    assert.match(result.stderr, /FAIL: SELECT exact integers/);
    if (mode === "unknown") assert.match(result.stderr, /code=outcome_unknown/);
  });
}

test("actual helper valid query and oracle acknowledge one Close", () => {
  const result = run("success");
  assert.equal(result.status, 0);
  assert.equal(result.stdout.match(/FAKE_CALL query/g)?.length, 1);
  assert.equal(result.stdout.match(/FAKE_CALL close/g)?.length, 1);
  assert.match(result.stdout, /explicit-close PASS/);
});

test("actual helper Close failure retains code and cannot PASS", () => {
  const result = run("close_failure");
  assert.equal(result.status, 1);
  assert.equal(result.stdout.match(/FAKE_CALL close/g)?.length, 1);
  assert.equal(result.stdout.includes(" PASS"), false);
  assert.match(result.stderr, /FAIL: explicit session close code=outcome_unknown/);
});
