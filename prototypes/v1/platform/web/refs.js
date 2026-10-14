export function referenceGraph(refs) {
  const nodes = [], edges = [], objects = new Map();
  for (const kind of ["branches", "snapshots"]) {
    for (const [name, reference] of Object.entries(refs[kind] || {})) {
      if (nodes.length >= 200) throw new Error("引用过多，请通过 refs API 分页检查原始响应");
      const identity = `${kind}:${name}`;
      nodes.push({id: identity, label: `${kind === "branches" ? "branch" : "snapshot"} ${name}`, detail: `timeline ${reference.timeline} · sequence ${reference.current_sequence ?? reference.sequence}`, side: "reference"});
      for (const field of ["head", "wal"]) {
        const entry = reference[field];
        if (!entry) continue;
        if (!objects.has(entry.blob)) {
          const object = {id: entry.blob, label: `${field} ${entry.hash.slice(0, 12)}`, detail: entry.blob, side: "object"};
          objects.set(entry.blob, object);
        }
        edges.push({from: identity, to: entry.blob, kind: field});
      }
    }
  }
  nodes.push(...objects.values());
  return {nodes, edges};
}

export function renderReferenceGraph(container, refs) {
  const graph = referenceGraph(refs);
  const namespace = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(namespace, "svg");
  const references = graph.nodes.filter(node => node.side === "reference");
  const objects = graph.nodes.filter(node => node.side === "object");
  const positions = new Map();
  for (const [column, nodes] of [[0, references], [1, objects]]) {
    nodes.forEach((node, index) => positions.set(node.id, {horizontal: 20 + column * 340, vertical: 25 + index * 68}));
  }
  svg.setAttribute("viewBox", `0 0 680 ${Math.max(100, Math.max(references.length, objects.length) * 68 + 25)}`);
  svg.setAttribute("role", "img");
  svg.setAttribute("aria-label", "真实 branch/snapshot 到不可变 head/WAL 对象的引用关系；不是推测的分支血缘");
  for (const edge of graph.edges) {
    const from = positions.get(edge.from), to = positions.get(edge.to);
    const line = document.createElementNS(namespace, "line");
    for (const [name, value] of Object.entries({x1: from.horizontal + 300, y1: from.vertical + 15, x2: to.horizontal, y2: to.vertical + 15, stroke: edge.kind === "head" ? "#89c9ff" : "#a4dfa3"})) line.setAttribute(name, String(value));
    svg.appendChild(line);
  }
  for (const node of graph.nodes) {
    const position = positions.get(node.id);
    const group = document.createElementNS(namespace, "g");
    const title = document.createElementNS(namespace, "title");
    title.textContent = node.detail;
    group.appendChild(title);
    for (const [offset, text] of [[0, node.label], [19, node.side === "reference" ? node.detail : "immutable object"]]) {
      const label = document.createElementNS(namespace, "text");
      label.setAttribute("x", String(position.horizontal)); label.setAttribute("y", String(position.vertical + offset));
      label.setAttribute("fill", "#dce8f7"); label.setAttribute("font-size", offset ? "11" : "13");
      label.textContent = text;
      group.appendChild(label);
    }
    svg.appendChild(group);
  }
  container.replaceChildren(svg);
}
