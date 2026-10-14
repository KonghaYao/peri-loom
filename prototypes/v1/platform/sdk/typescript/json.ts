export type JSONValue = null | boolean | number | bigint | string | JSONValue[] | { [key: string]: JSONValue };

export function parseLosslessJSON(text: string): JSONValue {
  let offset = 0;
  const whitespace = () => { while (offset < text.length && /[ \t\r\n]/.test(text[offset])) offset++; };
  const string = (): string => {
    const start = offset++;
    while (offset < text.length) {
      const character = text[offset++];
      if (character === "\\") offset++;
      else if (character === '"') return JSON.parse(text.slice(start, offset)) as string;
    }
    throw new Error("invalid JSON string");
  };
  const value = (depth: number): JSONValue => {
    if (depth > 64) throw new Error("JSON nesting limit");
    whitespace();
    const character = text[offset];
    if (character === '"') return string();
    if (character === "{" || character === "[") {
      offset++;
      const closing = character === "{" ? "}" : "]";
      const keys = new Set<string>();
      const container: JSONValue[] | { [key: string]: JSONValue } = character === "{" ? {} : [];
      whitespace();
      if (text[offset] === closing) { offset++; return container; }
      while (offset < text.length) {
        let key: string | undefined;
        if (character === "{") {
          whitespace();
          if (text[offset] !== '"') throw new Error("invalid JSON key");
          key = string();
          if (keys.has(key)) throw new Error("duplicate JSON key");
          keys.add(key);
          whitespace();
          if (text[offset++] !== ":") throw new Error("invalid JSON separator");
        }
        const parsed = value(depth + 1);
        if (key !== undefined) Object.defineProperty(container, key, { value: parsed, enumerable: true, writable: true, configurable: true });
        else (container as JSONValue[]).push(parsed);
        whitespace();
        const ending = text[offset++];
        if (ending === closing) return container;
        if (ending !== ",") throw new Error("invalid JSON container");
      }
      throw new Error("incomplete JSON container");
    }
    const primitive = /^(?:null|true|false|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/.exec(text.slice(offset));
    if (!primitive) throw new Error("invalid JSON value");
    const token = primitive[0];
    offset += token.length;
    if (token === "null") return null;
    if (token === "true" || token === "false") return token === "true";
    if (/^-?\d+$/.test(token)) {
      const integer = BigInt(token);
      const safe = BigInt(Number.MAX_SAFE_INTEGER);
      if (integer > safe || integer < -safe) return integer;
    }
    const number = Number(token);
    if (!Number.isFinite(number)) throw new Error("nonfinite JSON number");
    return number;
  };
  const parsed = value(0);
  whitespace();
  if (offset !== text.length) throw new Error("invalid JSON suffix");
  return parsed;
}
