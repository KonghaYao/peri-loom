import {test} from "node:test";
import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
const source = await readFile(new URL("../web/refs.js", import.meta.url), "utf8");
const {referenceGraph} = await import(`data:text/javascript;base64,${Buffer.from(source).toString("base64")}`);

test("native shared objects and distinct timelines remain explicit", () => {
  const object = {blob:"segment/shared-head",hash:"a".repeat(64)};
  const graph = referenceGraph({branches:{main:{timeline:1,sequence:0,current_sequence:9223372036854775807n,head:object,wal:null},clone:{timeline:2,sequence:3,head:object,wal:null}},snapshots:{before:{timeline:1,sequence:3,head:object,wal:null}}});
  assert.equal(graph.nodes.length,4);
  assert.equal(graph.edges.length,3);
  assert.equal(graph.edges.every(edge => edge.to === object.blob),true);
  assert.match(graph.nodes[0].detail,/9223372036854775807/);
  assert.equal(graph.nodes.filter(node=>node.side === "object").length,1);
  assert.equal(graph.edges.some(edge=>edge.from === "branches:main" && edge.to === "branches:clone"),false);
});

test("empty native refs do not fabricate objects", () => {
  assert.deepEqual(referenceGraph({branches:{},snapshots:{}}),{nodes:[],edges:[]});
});
