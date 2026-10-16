import test from "node:test";
import assert from "node:assert/strict";
import {contract, reply, sessionID, transport} from "./ui-rpc-support.mjs";

// The browser clients decide through web/sqlcontract.js. This is the consumer by
// failure class matrix for them: every published code crossed with every evidence
// shape the gateway can produce and every status the platform answers with. The
// console's rpc client must reach exactly the answer the shared module reaches,
// so a code added to the platform cannot arrive as a definite failure and a
// failure the platform proved cannot arrive as an unknown outcome.
const STATUSES = [400, 403, 409, 413, 429, 500, 502, 503, 504];
const SHAPES = [
  ["no-evidence", undefined, undefined],
  ["committed-only", undefined, true],
  ["settled-committing", true, true],
  ["settled-non-committing", true, false],
  ["rejected", false, false],
  ["contradictory", false, true],
];

test("the browser RPC client agrees with the shared failure contract", async () => {
  let covered = 0;
  for (const code of Object.keys(contract.CODES)) {
    for (const status of STATUSES) {
      for (const [shape, completed, committed] of SHAPES) {
        const failure = {code};
        if (completed !== undefined) failure.execution_completed = completed;
        if (committed !== undefined) failure.may_have_committed = committed;
        const rpc = transport((_path, body) => reply(body.request_id, null, failure, status));
        const reference = contract.unknown(failure, status);
        await assert.rejects(rpc.call("/v1/sql/query", {session: sessionID, sql: "SELECT 1"}), (error) => {
          const name = `${code}/${shape}@${status}`;
          assert.equal(error.code, reference ? "outcome_unknown" : code, `${name}: code`);
          assert.equal(error.outcomeUnknown, reference, `${name}: unknown outcome must be marked`);
          if (!reference) {
            assert.equal(error.executionCompleted, contract.executed(failure), `${name}: executed`);
            assert.equal(error.mayHaveCommitted, contract.commitRisk(failure), `${name}: commit risk`);
          }
          return true;
        }, `${code}/${shape}@${status}`);
        covered++;
      }
    }
  }
  assert.ok(covered > 0, "the contract published no code");
});

test("the shared failure contract keeps proof apart from the transport status", () => {
  const settled = {code: "sql_timeout", execution_completed: true, may_have_committed: false};
  assert.equal(contract.unknown(settled, 504), false);
  assert.equal(contract.executed(settled), true);
  assert.equal(contract.commitRisk(settled), false);
  assert.equal(contract.unknown({code: "sql_timeout"}, 504), true);
  assert.equal(contract.unknown({code: "sql_timeout"}, 400), true);
  assert.equal(contract.unknown({code: "session_moved"}, 409), false);
  assert.equal(contract.unknown({code: "outcome_unknown", execution_completed: true, may_have_committed: true}, 400), true);
  assert.equal(contract.commitRisk({code: "outcome_unknown", possible_commit: true}), true);
});
