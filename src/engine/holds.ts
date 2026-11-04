// The holds of 14 Table 14.1: every `HOLD` code and every drop or close reason and client condition that holds. A hold keeps the
// action, stops sending in its scope (the lane, the keyspace, every keyspace of the app, or the action alone), is shown in the
// sync status and in a `held` event, and is bounded (14 [CLI-45]).

import type { Reject } from "../gen/abpc/v2/types_pb.js";
import type { LaneName } from "./manifests.js";
import type { HoldInfo } from "./types.js";

/** The rows of Table 14.1. */
export type HoldKind =
  | "UNKNOWN_OP_TYPE"
  | "CLIENT_FORKED"
  | "CLIENT_UNKNOWN"
  | "KEYSPACE_NOT_FOUND"
  | "UPGRADE_REQUIRED"
  | "PROTOCOL_ERROR"
  | "CLIENT_TOO_OLD"
  | "UNSUPPORTED_FEATURE"
  | "HISTORY_DIVERGED"
  | "APP_SUSPENDED"
  | "SESSION_CAP"
  | "AUTHORIZER_UNAVAILABLE"
  | "UNKNOWN_HOLD"
  /** The row of Table 14.1 for a `Mutate` call that fails as a whole with a status and no ABPC detail (14 [CLI-50], A.7); its code is `PROTOCOL_ERROR` (A.11). */
  | "MUTATE_FAILURE"
  | "UNKNOWN_DROP_REASON"
  | "CONFLICT_WAIT"
  /**
   * The row of Table 14.1 for `BLOB_MISSING` for a name in an action's `blob_refs`, when the client holds the blob's bytes in no stash (09 [READ-94], 14 [CLI-44]): the action alone is held, its
   * resends are made with the hold backoff, and the app is told that the bytes are missing, so that it can stage them again (`blobs.ts`).
   */
  | "BLOB_MISSING"
  | "EPOCH_RESOLUTION"
  | "NO_TOKEN"
  | "SUBJECT_REVOKED";

export type HoldScope = "lane" | "keyspace" | "app" | "action";

export interface Hold {
  key: string;
  kind: HoldKind;
  scope: HoldScope;
  keyspaceId?: string;
  lane?: LaneName;
  mutationId?: string;
  /** The code or reason that the app is told (A.11). */
  code: string;
  /** The server time at which it started, ms. */
  startedMs: number;
  /** A bound shorter than the default one, in server time (14 [CLI-45]): the wait of 07 [MUT-101], for `KEYSPACE_NOT_FOUND`. */
  boundMs?: number;
  /** The number of probes made, the exponent of the hold backoff. */
  attempts: number;
  /** The action that a probe resends: the lane's head, or the held action itself (`BLOB_MISSING`). */
  probeId?: string;
  /** The reject that started the hold, when one did: an action that the hold ends before it was ever sent is dead-lettered with it (14 [CLI-45]). */
  reject?: Reject;
  /** The time of the next probe has come: the head action may be resent, the keyspace re-added, the call repeated. */
  probeDue?: boolean;
  /** `BLOB_MISSING`: the blobs of the action whose bytes the engine holds in no stash. */
  blobIds?: string[];
}

export function holdKey(kind: HoldKind, keyspaceId?: string, lane?: LaneName, mutationId?: string): string {
  return `${kind}|${keyspaceId ?? ""}|${lane ?? ""}|${mutationId ?? ""}`;
}

export class HoldRegistry {
  private readonly m = new Map<string, Hold>();

  get(key: string): Hold | undefined {
    return this.m.get(key);
  }

  has(kind: HoldKind, keyspaceId?: string, lane?: LaneName, mutationId?: string): boolean {
    return this.m.has(holdKey(kind, keyspaceId, lane, mutationId));
  }

  set(h: Hold): void {
    this.m.set(h.key, h);
  }

  delete(key: string): Hold | undefined {
    const h = this.m.get(key);
    this.m.delete(key);
    return h;
  }

  all(): Hold[] {
    return [...this.m.values()];
  }

  clear(): void {
    this.m.clear();
  }

  /** The holds that stop an action: every app hold, the keyspace's, its lane's, and its own. */
  blocking(e: { keyspaceId: string; lane: LaneName; mutationId: string }): Hold[] {
    const out: Hold[] = [];
    for (const h of this.m.values()) {
      switch (h.scope) {
        case "app":
          out.push(h);
          break;
        case "keyspace":
          if (h.keyspaceId === e.keyspaceId) out.push(h);
          break;
        case "lane":
          if (h.keyspaceId === e.keyspaceId && h.lane === e.lane) out.push(h);
          break;
        case "action":
          if (h.mutationId === e.mutationId) out.push(h);
          break;
      }
    }
    return out;
  }

  info(): HoldInfo[] {
    return this.all().map((h) => {
      const i: HoldInfo = { scope: h.scope, code: h.code };
      if (h.keyspaceId !== undefined) i.keyspaceId = h.keyspaceId;
      if (h.lane !== undefined) i.lane = h.lane;
      if (h.mutationId !== undefined) i.mutationId = h.mutationId;
      if (h.boundMs !== undefined) i.bound = h.boundMs;
      if (h.blobIds !== undefined) i.blobIds = [...h.blobIds];
      return i;
    });
  }
}
