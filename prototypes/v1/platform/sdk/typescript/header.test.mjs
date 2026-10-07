import test from "node:test";
import assert from "node:assert/strict";
import {Client, PeriError} from "./dist/client.js";

const credential = "synthetic-api-credential__________________";
const database = {namespace: {tenant: "tenant", database: "database"}, branch: "main"};
const sessionID = "0123456789abcdef0123456789abcdef";
const originalFetch = globalThis.fetch;
test.afterEach(() => { globalThis.fetch = originalFetch; });

for (const action of ["open", "execute", "query"]) {
  for (const status of [401, 429]) {
    for (const mismatched of [false, true]) {
      test(`${action} ${status} correlated rejection mismatch=${mismatched} never replays`, async () => {
        let calls = 0;
        const code = status === 401 ? "unauthenticated" : "quota_exceeded";
        globalThis.fetch = async (url, options) => {
          calls++;
          const body = JSON.parse(new TextDecoder().decode(options.body));
          const headerID = new Headers(options.headers).get("X-Request-ID");
          assert.equal(headerID, body.request_id);
          assert.match(headerID, /^[A-Za-z0-9_-]{1,64}$/);
          if (action !== "open" && url.endsWith("/open")) {
            return new Response(JSON.stringify({version: 1, request_id: headerID, ok: true,
              result: {session: sessionID, readonly: false}, error: null}));
          }
          return new Response(JSON.stringify({version: 1, request_id: mismatched ? "wrong-response-id" : headerID,
            ok: false, result: null, error: {code}}), {status});
        };
        const client = new Client("http://127.0.0.1:8080", credential);
        const session = action === "open" ? null : await client.open(database);
        await assert.rejects(action === "open" ? client.open(database) : session[action]("COMMIT", "early-reject"),
          (error) => error instanceof PeriError && error.code === (mismatched ? "outcome_unknown" : code)
            && error.outcomeUnknown === mismatched && !error.executionCompleted && !error.mayHaveCommitted
            && (action === "open" || error.requestID === "early-reject"));
        if (mismatched && session) {
          await assert.rejects(session.query("INSERT INTO items VALUES(1)", "no-replay"), (error) => error.code === "session_unusable");
        }
        assert.equal(calls, action === "open" ? 1 : 2);
      });
    }
  }
}
