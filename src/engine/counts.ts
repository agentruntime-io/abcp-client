// The counts of a received message, judged on its octets before one of its elements is decoded (16 [SEC-1], [SEC-3], 02 [ARC-58], [ARC-59]).
//
// A message of 16 MiB can hold eight million empty elements of a repeated field (`1a 00` repeated), and the decoder turns each of them into an object of
// its own: 2.2 GB of heap and nine seconds, from one `LoadStateResponse` (audit V19, MUST-1). The server's transport counts the entries of the
// repeated fields that a limit bounds on the octets of a request before it decodes one (`server/internal/transport/counts.go`); this is the same walk for
// what a client receives. It reads tags and lengths only, allocates nothing for an element, and stops at the element that passes a bound. A message that
// does not parse is the decoder's to refuse: the walk stops there and says nothing.
//
// Two bounds apply to every message:
//  - the count of each repeated field (a map included) of one message, where the text gives one (`BOUNDS` below, each with its rule), or, for the few
//    fields whose count the text leaves to the 16 MiB bound of the frame, the count that the octets can hold of an element at its least well-formed
//    size (`perOctets`);
//  - the **heap budget** of the message: every element that the decoder would allocate costs `weight` slots of 8 octets (`weightOf`), and the sum may not pass four
//    slots (32 octets of heap) for each octet of the message. A message of 16 MiB costs at most some 512 MiB of heap, where the decoder alone costs 270 times the
//    octets (an empty element of a list is two octets and an object of some 400); the least valid element of each type that a client decodes fits its own octets
//    (`test/engine-counts.test.ts` checks the ones that matter), and a message under 2,048 elements is never judged by it.
// Unknown fields are kept by the decoder, an object for each, so they are counted too: 64 for a message, 65,536 for the whole.
//
// The counts that depend on the request (the page of a `LoadState`, the results of a `Mutate`) are given by the caller (`DecodeLimits`).

import { ScalarType, type DescField, type DescMessage } from "@bufbuild/protobuf";
import { ProtocolViolation } from "./errors.js";

const WT_VARINT = 0;
const WT_FIXED64 = 1;
const WT_LEN = 2;
const WT_FIXED32 = 5;

/** The protocol maxima of 04 Table 4.10a that the bounds below are made of. */
export const MAX_KEYSPACES_PER_SESSION = 10_000;
export const MAX_INTEREST_PREFIXES = 1_000;
export const MAX_MUTATIONS_PER_REQUEST = 256;
export const MAX_CHANGES_PER_ACTION = 2_000;
/** 09 [READ-10]: the default `page_size`, and the most that a server takes (a larger one counts as this). */
export const DEFAULT_PAGE_SIZE = 1_000;
export const MAX_PAGE_SIZE = 10_000;
/** types.proto (`SuccessorRule`), 12 [ADM-45]: a keyspace's successor map holds at most 1,000 rules. */
export const MAX_SUCCESSORS = 1_000;
/** 09 [READ-100]: a page carries at most 1,000 redactions. */
export const MAX_REDACTIONS_PER_PAGE = 1_000;
/** 07 [MUT-7]: one `LaneWatermark` for each (keyspace, lane); the lanes are SEQUENCED and GATED. */
export const LANES = 2;
/** 10 [AUTH-13], [AUTH-26]: an actor chain holds at most 16 subjects. */
export const MAX_ACTOR_CHAIN = 16;
/** 04 Table 4.10a: the protocol maximum of `max_presence_state_bytes` (so that a `PresenceFrame` stays within 16 MiB, 04 [MAN-75]) and of `presence_min_publish_interval_ms`. */
export const MAX_PRESENCE_STATE_BYTES = 64 << 10;
export const MAX_PRESENCE_MIN_INTERVAL_MS = 5_000;
/** 09 §9.9: a record's `blob_refs` hold at most 1,000 entries. */
export const MAX_BLOB_REFS = 1_000;
/** 06 [LOG-8], [LOG-18]: a record holds the changes of one action, at most `max_changes_per_action` (2,000), and the intent or decision row that goes with them. */
export const MAX_CHANGES_PER_RECORD = MAX_CHANGES_PER_ACTION + 2;
/** A list of feature names (added by this client, as the server's `MaxFeatureNames` for what it receives): a peer names the few features that it knows. */
export const MAX_FEATURE_NAMES = 256;
/**
 * 09 [READ-86]: `CreateUploadResponse.upload_headers` are "the exact headers that the PUT carries besides Content-Length": the headers that a store's signature binds
 * (a checksum, a length, a content type, an encryption mode), a handful. The text gives no count; a client bounds the list, since the answer is the server's and
 * what it holds is untrusted input (16 [SEC-1]). SPEC-QUESTION: no bound in the text (see `engine/blobs.ts`).
 */
export const MAX_UPLOAD_HEADERS = 32;
/** 08 [GATE-39]: a gate's `details` hold at most 32 entries; a server's own reject holds a few more (`moved_to`, `effect_key`, ...). */
export const MAX_REJECT_DETAILS = 64;
/** The unknown fields that the decoder keeps: for one message, and for the whole of a decoded message. */
export const MAX_UNKNOWN_PER_MESSAGE = 64;
export const MAX_UNKNOWN_TOTAL = 65_536;
/** The heap budget: slots (8 octets of heap each) for each octet of the message, and a floor for the small messages. */
export const SLOTS_PER_OCTET = 4;
export const BUDGET_FLOOR_SLOTS = 65_536;
/** A bound of `perOctets` never refuses fewer elements than this: a small message is never judged by it. */
export const PER_OCTETS_FLOOR = 64;

/** A bound of a repeated field: a count, or the count that the octets of the message hold at `perOctets` octets for the least element. */
export type Bound = number | { perOctets: number };

/**
 * The bound of each repeated or map field that a client decodes, by `<message>.<field>`. Every field of the protos that a response, a frame or an error
 * detail holds is here: `test/engine-counts.test.ts` walks the descriptors and fails for one that is not.
 */
export const BOUNDS: Readonly<Record<string, Bound>> = {
  // -- a Mutate answer: the request's own counts apply (`DecodeLimits`), these are the protocol maxima --
  "abcp.v2.Appended.overwrote_newer": MAX_CHANGES_PER_ACTION, // 05 [STATE-62]: indexes of the action's changes
  "abcp.v2.MutateResponse.results": MAX_MUTATIONS_PER_REQUEST, // 07 [MUT-4]
  "abcp.v2.MutateResponse.watermarks": MAX_MUTATIONS_PER_REQUEST, // 07 [MUT-7]: a pair for each lane that the request touched
  "abcp.v2.GetMutationResultsResponse.results": 100, // 07 [MUT-107]
  // -- state --
  "abcp.v2.LoadStateResponse.entries": MAX_PAGE_SIZE, // 09 [READ-10] (the request's `page_size` applies)
  "abcp.v2.LoadStateResponse.successors": MAX_SUCCESSORS,
  "abcp.v2.LoadStateResponse.lane_watermarks": MAX_KEYSPACES_PER_SESSION * LANES,
  "abcp.v2.LoadStateResponse.redactions": MAX_REDACTIONS_PER_PAGE,
  "abcp.v2.UpdateSubscriptionResponse.rejected": 2 * MAX_KEYSPACES_PER_SESSION, // 09 [READ-62]: bounded by the request's `add` and `set_interest`
  "abcp.v2.RefreshSessionResponse.dropped_keyspace_ids": MAX_KEYSPACES_PER_SESSION,
  // -- the stream --
  "abcp.v2.RecordsFrame.records": { perOctets: 32 }, // 09 [READ-101]: only the 16 MiB bound; a record holds its 32-octet chain hash (06 [LOG-71])
  "abcp.v2.HeartbeatFrame.cursors": MAX_KEYSPACES_PER_SESSION, // 09 [READ-54]: one for each followed keyspace
  "abcp.v2.HeartbeatFrame.lane_watermarks": MAX_KEYSPACES_PER_SESSION * LANES,
  "abcp.v2.ControlFrame.KeyspaceDropped.successors": MAX_SUCCESSORS,
  "abcp.v2.PresenceFrame.entries": { perOctets: 16 }, // 09 [READ-101], [READ-79]: members are capped by the app's `max_presence_members_per_topic`, whose maximum is the u32
  "abcp.v2.PresenceFrame.left_client_ids": { perOctets: 38 }, // a client_id is a UUID, 36 characters (03 [ID-35])
  // -- the blob calls (09 §9.9): the one field of an answer that holds many; the calls of 09 §9.8 answer with a timestamp or nothing --
  "abcp.v2.CreateUploadResponse.upload_headers": MAX_UPLOAD_HEADERS,
  // -- what a message holds --
  "abcp.v2.KeyspaceCursor.interest_prefixes": MAX_INTEREST_PREFIXES, // 04 Table 4.10a: the frame of a `KeyspaceAdded`
  "abcp.v2.Principal.actor_chain": MAX_ACTOR_CHAIN,
  "abcp.v2.ServerInfo.features": MAX_FEATURE_NAMES,
  "abcp.v2.Reject.successors": MAX_SUCCESSORS,
  "abcp.v2.Reject.details": MAX_REJECT_DETAILS,
  "abcp.v2.Record.required_features": MAX_FEATURE_NAMES,
  "abcp.v2.Record.changes": MAX_CHANGES_PER_RECORD,
  "abcp.v2.Record.overwrote_newer": MAX_CHANGES_PER_RECORD,
  "abcp.v2.Record.previous": MAX_CHANGES_PER_RECORD, // 06 [LOG-21]: aligned with `changes`
  "abcp.v2.Record.blob_refs": MAX_BLOB_REFS,
};

/** The counts that the call fixes: by `<message>.<field>`, each at most the bound of the table. */
export type DecodeLimits = Readonly<Record<string, number>>;

/** The count of the entries that a `LoadState` of this `page_size` may return (09 [READ-10]). */
export function pageBound(pageSize: number): number {
  if (pageSize === 0) return DEFAULT_PAGE_SIZE;
  return Math.min(pageSize, MAX_PAGE_SIZE);
}

interface Plan {
  field: DescField;
  /** What the walk does with an occurrence of the field. */
  kind: "message" | "list-message" | "map" | "list-scalar" | "list-packable" | "scalar";
  /** The wire type of an element that is not packed. */
  wire: number;
  /** For a packed field: the size of an element in octets (0: a varint). */
  fixed: number;
  nested: DescMessage | undefined;
  /** The slots that an element costs. */
  weight: number;
  key: string;
  bound: Bound | undefined;
}

const plans = new WeakMap<DescMessage, Map<number, Plan>>();
const weights = new WeakMap<DescMessage, number>();

function wireOf(t: ScalarType): number {
  switch (t) {
    case ScalarType.STRING:
    case ScalarType.BYTES:
      return WT_LEN;
    case ScalarType.DOUBLE:
    case ScalarType.FIXED64:
    case ScalarType.SFIXED64:
      return WT_FIXED64;
    case ScalarType.FLOAT:
    case ScalarType.FIXED32:
    case ScalarType.SFIXED32:
      return WT_FIXED32;
    default:
      return WT_VARINT;
  }
}

/**
 * The slots (8 octets of heap each) that an element of a message type costs once decoded: what `create` fills, as measured on protobuf-es objects (a key change of
 * nine fields is some 400 octets, most of them the two empty byte arrays): 6 for the object, 1 for a string, a number, a boolean, an enum or a message
 * reference, 2 for a 64-bit integer (a bigint), 16 for bytes, 4 for a list and 8 for a map.
 */
function weightOf(m: DescMessage): number {
  let w = weights.get(m);
  if (w === undefined) {
    w = 6;
    for (const f of m.fields) {
      switch (f.fieldKind) {
        case "list":
          w += 4;
          break;
        case "map":
          w += 8;
          break;
        case "scalar":
          w += f.scalar === ScalarType.BYTES ? 16 : f.scalar === ScalarType.INT64 || f.scalar === ScalarType.UINT64 || f.scalar === ScalarType.SINT64 || f.scalar === ScalarType.FIXED64 || f.scalar === ScalarType.SFIXED64 ? 2 : 1;
          break;
        default:
          w += 1;
      }
    }
    weights.set(m, w);
  }
  return w;
}

function planOf(m: DescMessage): Map<number, Plan> {
  let p = plans.get(m);
  if (p !== undefined) return p;
  p = new Map();
  plans.set(m, p);
  for (const f of m.fields) {
    const key = `${m.typeName}.${f.name}`;
    const bound = BOUNDS[key];
    let plan: Plan;
    switch (f.fieldKind) {
      case "message":
        plan = { field: f, kind: "message", wire: WT_LEN, fixed: 0, nested: f.message, weight: 0, key, bound };
        break;
      case "list":
        if (f.listKind === "message") plan = { field: f, kind: "list-message", wire: WT_LEN, fixed: 0, nested: f.message, weight: weightOf(f.message), key, bound };
        else if (f.listKind === "scalar" && (f.scalar === ScalarType.STRING || f.scalar === ScalarType.BYTES)) plan = { field: f, kind: "list-scalar", wire: WT_LEN, fixed: 0, nested: undefined, weight: 4, key, bound };
        else {
          const w = f.listKind === "scalar" ? wireOf(f.scalar) : WT_VARINT;
          plan = { field: f, kind: "list-packable", wire: w, fixed: w === WT_FIXED64 ? 8 : w === WT_FIXED32 ? 4 : 0, nested: undefined, weight: 1, key, bound };
        }
        break;
      case "map":
        plan = { field: f, kind: "map", wire: WT_LEN, fixed: 0, nested: f.mapKind === "message" ? f.message : undefined, weight: 8, key, bound };
        break;
      default:
        plan = { field: f, kind: "scalar", wire: f.fieldKind === "enum" ? WT_VARINT : wireOf(f.scalar), fixed: 0, nested: undefined, weight: 0, key, bound };
    }
    p.set(f.number, plan);
  }
  return p;
}

/** The counts of the fields of one message, as the decoder sees it: the occurrences of a singular message field are one message (they merge). */
class Scope {
  counts = new Map<number, number>();
  kids = new Map<number, Scope>();
  unknown = 0;
}

class Refusal extends Error {}

/** The walk over the octets of one message. */
class Walk {
  slots = 0;
  unknownTotal = 0;
  readonly budget: number;

  constructor(
    private readonly b: Uint8Array,
    private readonly limits: DecodeLimits,
  ) {
    this.budget = Math.max(BUDGET_FLOOR_SLOTS, b.length * SLOTS_PER_OCTET);
  }

  /** Reads a varint at p (not past end); returns the value and the position after it, or undefined when it is not well formed (the decoder refuses it). */
  private varint(p: number, end: number): { v: number; next: number } | undefined {
    let v = 0;
    let mul = 1;
    for (let i = 0; i < 10; i++) {
      if (p + i >= end) return undefined;
      const x = this.b[p + i] as number;
      v += (x & 0x7f) * mul;
      mul *= 128;
      if (x < 0x80) return { v, next: p + i + 1 };
    }
    return undefined;
  }

  private spend(slots: number, what: string): void {
    this.slots += slots;
    if (this.slots > this.budget) throw new Refusal(`${what}: the elements of the message would cost more than ${SLOTS_PER_OCTET} slots of heap for each of its ${this.b.length} octets`);
  }

  private count(sc: Scope, plan: Plan, n: number): void {
    const c = (sc.counts.get(plan.field.number) ?? 0) + n;
    sc.counts.set(plan.field.number, c);
    const own = this.limits[plan.key];
    let max = plan.bound === undefined ? MAX_FEATURE_NAMES : typeof plan.bound === "number" ? plan.bound : Math.max(PER_OCTETS_FLOOR, Math.floor(this.b.length / plan.bound.perOctets));
    if (own !== undefined && own < max) max = own;
    if (c > max) throw new Refusal(`${plan.key} holds more than ${max} entries`);
  }

  walk(m: DescMessage, start: number, end: number, depth: number, sc: Scope): void {
    if (depth > 100) return; // the decoder refuses a message nested deeper than MAX_DEPTH
    const plan = planOf(m);
    let p = start;
    while (p < end) {
      const t = this.varint(p, end);
      if (t === undefined) return;
      p = t.next;
      const no = Math.floor(t.v / 8);
      const wt = t.v % 8;
      if (no === 0) return;
      // the value: where it starts and ends, and its length for a length-delimited one
      let vs = p;
      let ve: number;
      switch (wt) {
        case WT_VARINT: {
          const v = this.varint(p, end);
          if (v === undefined) return;
          ve = v.next;
          break;
        }
        case WT_FIXED64:
          ve = p + 8;
          break;
        case WT_FIXED32:
          ve = p + 4;
          break;
        case WT_LEN: {
          const l = this.varint(p, end);
          if (l === undefined || l.v > end - l.next) return;
          vs = l.next;
          ve = l.next + l.v;
          break;
        }
        default:
          return; // a group or a wire type that does not exist: the decoder refuses it
      }
      if (ve > end) return;
      const f = plan.get(no);
      if (f === undefined || (f.kind !== "list-packable" && wt !== f.wire) || (f.kind === "list-packable" && wt !== f.wire && wt !== WT_LEN)) {
        // an unknown field, or a known one with the wire type that its kind does not have: kept as an unknown field, an object for each
        if (++sc.unknown > MAX_UNKNOWN_PER_MESSAGE) throw new Refusal(`${m.typeName} holds more than ${MAX_UNKNOWN_PER_MESSAGE} unknown fields`);
        if (++this.unknownTotal > MAX_UNKNOWN_TOTAL) throw new Refusal(`the message holds more than ${MAX_UNKNOWN_TOTAL} unknown fields`);
        this.spend(12, m.typeName);
        p = ve;
        continue;
      }
      switch (f.kind) {
        case "message": {
          const kid = sc.kids.get(no) ?? new Scope();
          sc.kids.set(no, kid);
          if (f.nested !== undefined) this.walk(f.nested, vs, ve, depth + 1, kid);
          break;
        }
        case "list-message": {
          this.count(sc, f, 1);
          this.spend(f.weight, f.key);
          if (f.nested !== undefined) this.walk(f.nested, vs, ve, depth + 1, new Scope());
          break;
        }
        case "map": {
          this.count(sc, f, 1);
          this.spend(f.weight, f.key);
          if (f.nested !== undefined) this.walkEntry(f.nested, vs, ve, depth + 1);
          break;
        }
        case "list-scalar":
          this.count(sc, f, 1);
          this.spend(f.weight, f.key);
          break;
        case "list-packable": {
          let n: number;
          if (wt === WT_LEN) {
            // packed: the elements of the payload
            if (f.fixed > 0) n = Math.floor((ve - vs) / f.fixed);
            else {
              n = 0;
              for (let i = vs; i < ve; i++) if ((this.b[i] as number) < 0x80) n++;
            }
          } else n = 1;
          this.count(sc, f, n);
          this.spend(n * f.weight, f.key);
          break;
        }
        default:
          break;
      }
      p = ve;
    }
  }

  /** A map entry is a message of its own (key 1, value 2): a value that is a message holds counts of its own. */
  private walkEntry(value: DescMessage, start: number, end: number, depth: number): void {
    let p = start;
    while (p < end) {
      const t = this.varint(p, end);
      if (t === undefined) return;
      p = t.next;
      const no = Math.floor(t.v / 8);
      const wt = t.v % 8;
      if (wt !== WT_LEN) return;
      const l = this.varint(p, end);
      if (l === undefined || l.v > end - l.next) return;
      if (no === 2) this.walk(value, l.next, l.next + l.v, depth, new Scope());
      p = l.next + l.v;
    }
  }
}

/**
 * Checks the counts of the message of type `schema` that the octets `b` encode, before any of it is decoded. Throws a `ProtocolViolation` for the first bound
 * that they pass (16 [SEC-3]); a message that does not parse is left to the decoder.
 */
export function checkCounts(schema: DescMessage, b: Uint8Array, limits: DecodeLimits = {}): void {
  try {
    new Walk(b, limits).walk(schema, 0, b.length, 0, new Scope());
  } catch (e) {
    if (e instanceof Refusal) throw new ProtocolViolation(schema.name, `${e.message} (16 [SEC-1], 09 [READ-10])`);
    throw e;
  }
}
