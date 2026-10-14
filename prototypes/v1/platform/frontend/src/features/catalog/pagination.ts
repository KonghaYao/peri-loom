export function pageSlice<Value>(items: Value[], page: number, pageSize = 100) {
  const pages = Math.max(1, Math.ceil(items.length / pageSize));
  const current = Math.max(0, Math.min(page, pages - 1));
  return { items: items.slice(current * pageSize, (current + 1) * pageSize), current, pages };
}
