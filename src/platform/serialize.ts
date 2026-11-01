// A JSON form of what the engine stores, for a platform that has no structured clone on disk: `bigint`, `Uint8Array`, `undefined` members and the
// numbers JSON has no text for survive the round trip, and nothing that a server chose can be mistaken for a tag.
//
// The values are tagged as objects with one member: `{"$big": "123"}`, `{"$u8": "<base64>"}`, `{"$num": "NaN"}`. An object of the engine's data
// that has a member named like a tag is written inside `{"$obj": {...}}`, which the reader unwraps without reading its members as tags again, so a
// map that a server filled (`Reject.details` has arbitrary keys) can name any key it likes. protobuf-es messages are plain objects (`$typeName`
// is an ordinary member), so they need no wrapper.

const TAGS = ["$big", "$u8", "$num", "$obj"] as const;

/** Sets an own member, a member named `__proto__` included: a plain assignment to it would set the prototype of the object instead (a map that a server filled can name it). */
function own(o: Record<string, unknown>, k: string, v: unknown): void {
  Object.defineProperty(o, k, { value: v, enumerable: true, writable: true, configurable: true });
}

function hasTagKey(o: object): boolean {
  for (const k of TAGS) if (Object.prototype.hasOwnProperty.call(o, k)) return true;
  return false;
}

function toBase64(b: Uint8Array): string {
  return Buffer.from(b.buffer, b.byteOffset, b.byteLength).toString("base64");
}

/** The JSON-ready form of a value (`JSON.stringify` of it is the text). */
export function pack(v: unknown): unknown {
  switch (typeof v) {
    case "bigint":
      return { $big: v.toString() };
    case "number":
      if (Number.isFinite(v) && !Object.is(v, -0)) return v;
      return { $num: Object.is(v, -0) ? "-0" : String(v) };
    case "undefined":
      return undefined;
    case "object": {
      if (v === null) return null;
      if (v instanceof Uint8Array) return { $u8: toBase64(v) };
      if (Array.isArray(v)) return v.map((x) => pack(x) ?? null);
      const out: Record<string, unknown> = {};
      for (const [k, x] of Object.entries(v)) {
        const p = pack(x);
        if (p !== undefined) own(out, k, p);
      }
      return hasTagKey(v) ? { $obj: out } : out;
    }
    default:
      return v;
  }
}

/** The value of a packed form. */
export function unpack(v: unknown): unknown {
  if (Array.isArray(v)) return v.map(unpack);
  if (typeof v !== "object" || v === null) return v;
  const o = v as Record<string, unknown>;
  const keys = Object.keys(o);
  if (keys.length === 1) {
    const k = keys[0] as string;
    const x = o[k];
    if (k === "$big" && typeof x === "string") return BigInt(x);
    if (k === "$u8" && typeof x === "string") return new Uint8Array(Buffer.from(x, "base64"));
    if (k === "$num" && typeof x === "string") return x === "-0" ? -0 : Number(x);
    if (k === "$obj" && typeof x === "object" && x !== null && !Array.isArray(x)) {
      const inner: Record<string, unknown> = {};
      for (const [ik, iv] of Object.entries(x)) own(inner, ik, unpack(iv));
      return inner;
    }
  }
  const out: Record<string, unknown> = {};
  for (const [k, x] of Object.entries(o)) own(out, k, unpack(x));
  return out;
}

export function stringify(v: unknown): string {
  return JSON.stringify(pack(v));
}

export function parse(text: string): unknown {
  return unpack(JSON.parse(text));
}
