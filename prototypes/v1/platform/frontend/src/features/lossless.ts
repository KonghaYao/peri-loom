export function parseLosslessJSON(text: string): unknown {
  let offset = 0;
  const whitespace = () => { while (/[\t\n\r ]/.test(text[offset] ?? '')) offset++; };
  const string = (): string => {
    const start = offset++;
    while (offset < text.length) {
      const character = text[offset++];
      if (character === '\\') offset++;
      else if (character === '"') return JSON.parse(text.slice(start, offset)) as string;
    }
    throw new Error('Invalid JSON string');
  };
  const value = (depth: number): unknown => {
    if (depth > 64) throw new Error('JSON nesting limit');
    whitespace();
    const character = text[offset];
    if (character === '"') return string();
    if (character === '{' || character === '[') {
      offset++;
      const closing = character === '{' ? '}' : ']';
      const container: Record<string, unknown> | unknown[] = character === '{' ? {} : [];
      const keys = new Set<string>();
      whitespace();
      if (text[offset] === closing) { offset++; return container; }
      while (offset < text.length) {
        let key: string | undefined;
        if (character === '{') {
          whitespace();
          if (text[offset] !== '"') throw new Error('Invalid JSON key');
          key = string();
          if (keys.has(key)) throw new Error('Duplicate JSON key');
          keys.add(key);
          whitespace();
          if (text[offset++] !== ':') throw new Error('Invalid JSON separator');
        }
        const parsed = value(depth + 1);
        if (key !== undefined) Object.defineProperty(container, key, { value: parsed, enumerable: true, writable: true, configurable: true });
        else (container as unknown[]).push(parsed);
        whitespace();
        const ending = text[offset++];
        if (ending === closing) return container;
        if (ending !== ',') throw new Error('Invalid JSON container');
      }
      throw new Error('Incomplete JSON container');
    }
    const primitive = /^(?:null|true|false|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/.exec(text.slice(offset));
    if (!primitive) throw new Error('Invalid JSON value');
    const token = primitive[0];
    offset += token.length;
    if (token === 'null') return null;
    if (token === 'true' || token === 'false') return token === 'true';
    if (/^-?\d+$/.test(token)) {
      const integer = BigInt(token);
      if (integer > BigInt(Number.MAX_SAFE_INTEGER) || integer < -BigInt(Number.MAX_SAFE_INTEGER)) return integer;
    }
    const number = Number(token);
    if (!Number.isFinite(number)) throw new Error('Nonfinite JSON number');
    return number;
  };
  const parsed = value(0);
  whitespace();
  if (offset !== text.length) throw new Error('Invalid JSON suffix');
  return parsed;
}

export function encodeLosslessJSON(value: unknown): string {
  const seen = new Set<object>();
  const encode = (item: unknown, depth: number): string => {
    if (depth > 64) throw new Error('JSON nesting limit');
    if (typeof item === 'bigint') return item.toString();
    if (item === null || typeof item === 'string' || typeof item === 'boolean') return JSON.stringify(item);
    if (typeof item === 'number' && Number.isFinite(item)) return JSON.stringify(item);
    if (typeof item !== 'object' || seen.has(item)) throw new Error('Invalid JSON value');
    seen.add(item);
    const result = Array.isArray(item)
      ? `[${item.map(child => encode(child, depth + 1)).join(',')}]`
      : `{${Object.entries(item).map(([key, child]) => `${JSON.stringify(key)}:${encode(child, depth + 1)}`).join(',')}}`;
    seen.delete(item);
    return result;
  };
  return encode(value, 0);
}

export const displayJSON = (value: unknown) => JSON.stringify(value, (_, item: unknown) => typeof item === 'bigint' ? item.toString() : item, 2);
