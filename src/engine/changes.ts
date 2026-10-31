// Small builders for the changes of an action (`ChangeInput`).

import { ChangeKind } from "../gen/abpc/v2/types_pb.js";
import type { ChangeInput } from "./types.js";

/** The bytes of a JSON value as `json/v1` text. */
export function jsonBytes(v: unknown): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(v));
}

/** `SET` of an `lww` key to a JSON value. */
export function set(key: string, value: unknown, encoding = "json/v1"): ChangeInput {
  return { key, kind: ChangeKind.SET, value: encoding === "json/v1" ? jsonBytes(value) : (value as Uint8Array), encoding };
}

/** `SET` of an explicit null (05 [STATE-14]). */
export function setNull(key: string): ChangeInput {
  return { key, kind: ChangeKind.SET, isNull: true, encoding: "json/v1" };
}

export function del(key: string): ChangeInput {
  return { key, kind: ChangeKind.DELETE };
}

/** `INCR` of a counter. */
export function incr(key: string, delta: bigint | number): ChangeInput {
  return { key, kind: ChangeKind.INCR, delta: BigInt(delta) };
}

/** `CAS` of a `cas` key; `expected` omitted: the key's version in the confirmed state (14 [CLI-17]). */
export function cas(key: string, value: unknown, expected?: ChangeInput["expected"]): ChangeInput {
  return { key, kind: ChangeKind.CAS, value: jsonBytes(value), encoding: "json/v1", expected: expected ?? {} };
}

/** `CRDT_UPDATE` of a CRDT key with a Yjs update. */
export function crdtUpdate(key: string, update: Uint8Array): ChangeInput {
  return { key, kind: ChangeKind.CRDT_UPDATE, update };
}
