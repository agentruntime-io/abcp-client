// The `Manifests` of the engine over `GetManifest` (04 §4.7): the version of the manifest that a record, a load or an action names is fetched
// once, read from its canonical json/v1 form (04 [MAN-4]) into what the client uses of it, and kept by version (a published version never
// changes, 04 §4.7).
//
// What the client reads of a manifest (not a validator: the server judged it at publish, 04 §4.11):
//  - for the fold (`fold.Rules`): for each key template (`keys`) its merge rule, whether it is an entity marker, and its parent, which is the
//    longest proper prefix of the template that is an entity template (04 [MAN-20]);
//  - for the engine: for each op type its dispatch, `optimistic`, `barrier`, `offline_mode`, whether it is a spend op type (`escrow`), the
//    declared templates that its `writes` select (03 [ID-21]); the `crdt_roots` of a CRDT template; the range of a counter; the owning key of a
//    fenced key (`fenced_by`, 04 [MAN-24]).
//  - the op types that no manifest declares: `abpc.Override` (03 [ID-51]) and the client-emittable op types of the modules that the
//    coordination section enables (11 Table 11.1), all GATED but the first.
//
// What it checks of the answer, because a client does not trust a document it cannot verify: the encoding is json/v1, the version is the one
// asked for, the document reads as a manifest document (04 [MAN-3]), and its SHA-256 is the `sha256` that the answer carries.

import { create } from "@bufbuild/protobuf";
import { GetManifestRequestSchema, GetManifestResponseSchema, type GetManifestResponse } from "../gen/abpc/v2/sync_pb.js";
import type { EngineManifest, Manifests, OpTypeInfo } from "../engine/manifests.js";
import { ProtocolViolation, RpcError } from "../engine/errors.js";
import type { Transport } from "../engine/transport.js";
import { decodeMessage, encodeMessage } from "../engine/wire.js";
import { sha256 } from "../rules/crypto.js";
import type { Roots, RootType } from "../rules/crdt/index.js";
import { expandKeyTemplate, matchKeyTemplate, parseKeyTemplate, type KeyTemplate, type Rule, type Template } from "../rules/fold/index.js";
import { JsonV1Error, getMember, intRange, parseDocument, schemaFromValue, type JsonValue, type Range } from "../rules/jsonv1/index.js";

const ROOT_TYPES: ReadonlySet<string> = new Set(["text", "map", "array", "xml_fragment"]);

/** The op types of the modules that a client emits (11 Table 11.1), by the module of the coordination section that enables them. */
const MODULE_OP_TYPES: Readonly<Record<string, readonly string[]>> = {
  lease: ["abpc.lease.v1.Acquire", "abpc.lease.v1.Renew", "abpc.lease.v1.Release"],
  saga: ["abpc.saga.v1.Start", "abpc.saga.v1.Abandon", "abpc.saga.v1.ResolveMove"],
  escrow: ["abpc.escrow.v1.Grant", "abpc.escrow.v1.Return"],
};

interface Compiled {
  tpl: KeyTemplate;
  template: Template;
  roots: Roots | undefined;
  range: Range | undefined;
  fencedBy: KeyTemplate | undefined;
}

/** One version of a manifest, as the client reads it. */
export class ParsedManifest implements EngineManifest {
  private readonly bySegments = new Map<number, Compiled[]>();
  private readonly ops = new Map<string, OpTypeInfo>();

  constructor(
    readonly version: number,
    /** The manifest's SHA-256 (04 [MAN-4]). */
    readonly hash: Uint8Array,
    readonly app: string,
    readonly requiredFeatures: readonly string[],
    doc: JsonValue,
  ) {
    const keys = getMember(doc, "keys");
    const texts = keys instanceof Map ? [...keys.keys()] : [];
    const parsed = new Map<string, KeyTemplate>();
    for (const t of texts) {
      const p = parseKeyTemplate(t);
      if (p !== undefined) parsed.set(t, p);
    }
    const entities = new Set<string>();
    if (keys instanceof Map) for (const [t, a] of keys) if (getMember(a, "entity") === true) entities.add(t);
    for (const [text, tpl] of parsed) {
      const attrs = (keys as Map<string, JsonValue>).get(text) as JsonValue;
      const merge = getMember(attrs, "merge");
      const rule: Rule = merge === "crdt:yjs/v1" ? "crdt" : merge === "cas" ? "cas" : merge === "counter" ? "counter" : "lww";
      const entity = getMember(attrs, "entity") === true;
      const fb = getMember(attrs, "fenced_by");
      const c: Compiled = {
        tpl,
        template: { rule, entity, parent: parentOf(tpl, parsed, entities) },
        roots: rootsOf(getMember(attrs, "crdt_roots")),
        range: rule === "counter" ? counterRange(getMember(attrs, "value_schema")) : undefined,
        fencedBy: typeof fb === "string" ? parseKeyTemplate(fb) : undefined,
      };
      const bucket = this.bySegments.get(tpl.segments.length) ?? [];
      bucket.push(c);
      this.bySegments.set(tpl.segments.length, bucket);
    }
    const ops = getMember(doc, "op_types");
    if (ops instanceof Map) for (const [name, o] of ops) this.ops.set(name, opInfo(name, o, parsed));
    // 03 [ID-51]: `abpc.Override` exists in every manifest version, SEQUENCED
    this.ops.set("abpc.Override", { name: "abpc.Override", lane: "SEQUENCED", optimistic: true, barrier: false, offlineMode: "queue_recheck", spend: false, logIntent: false });
    const coordination = getMember(doc, "coordination");
    if (coordination instanceof Map) {
      for (const [module, names] of Object.entries(MODULE_OP_TYPES)) {
        if (!coordination.has(module)) continue;
        for (const name of names) this.ops.set(name, { name, lane: "GATED", optimistic: true, barrier: false, offlineMode: "queue_recheck", spend: false, logIntent: true });
      }
    }
  }

  private find(key: string): { c: Compiled; bindings: Map<string, string> } | undefined {
    const segments = key.split("/").length;
    for (const c of this.bySegments.get(segments) ?? []) {
      const bindings = matchKeyTemplate(c.tpl, key);
      if (bindings !== undefined) return { c, bindings };
    }
    return undefined;
  }

  template(_version: number, key: string): Template | undefined {
    return this.find(key)?.c.template;
  }

  opType(name: string): OpTypeInfo | undefined {
    return this.ops.get(name);
  }

  templatePattern(key: string): string | undefined {
    return this.find(key)?.c.tpl.text;
  }

  crdtRoots(key: string): Roots | undefined {
    return this.find(key)?.c.roots;
  }

  counterRange(key: string): Range | undefined {
    return this.find(key)?.c.range;
  }

  fencedBy(key: string): string | undefined {
    const f = this.find(key);
    if (f?.c.fencedBy === undefined) return undefined;
    return expandKeyTemplate(f.c.fencedBy, f.bindings);
  }
}

/** The shape of a template segment: a variable, whatever its name, or a literal (03 [ID-21]). */
function sameShape(a: KeyTemplate, b: KeyTemplate, n: number): boolean {
  for (let i = 0; i < n; i++) {
    const x = a.segments[i] as KeyTemplate["segments"][number];
    const y = b.segments[i] as KeyTemplate["segments"][number];
    if ("variable" in x !== "variable" in y) return false;
    if ("literal" in x && "literal" in y && x.literal !== y.literal) return false;
  }
  return true;
}

/** 04 [MAN-20]: the longest proper prefix of the template, by segments, that is an entity template; its text, or "". */
function parentOf(tpl: KeyTemplate, all: ReadonlyMap<string, KeyTemplate>, entities: ReadonlySet<string>): string {
  for (let n = tpl.segments.length - 1; n >= 1; n--) {
    for (const e of entities) {
      const p = all.get(e);
      if (p !== undefined && p.segments.length === n && sameShape(p, tpl, n)) return e;
    }
  }
  return "";
}

function rootsOf(v: JsonValue | undefined): Roots | undefined {
  if (!(v instanceof Map)) return undefined;
  const out = new Map<string, RootType>();
  for (const [name, t] of v) if (typeof t === "string" && ROOT_TYPES.has(t)) out.set(name, t as RootType);
  return out;
}

function counterRange(schema: JsonValue | undefined): Range {
  if (schema === undefined) return intRange(undefined);
  try {
    return intRange(schemaFromValue(schema, "counter"));
  } catch (e) {
    if (e instanceof JsonV1Error) return intRange(undefined);
    throw e;
  }
}

/** The declared templates that the patterns of `writes` select (03 [ID-21]): `T`, `T/*` (one more segment) and `T/**` (one or more). */
function selected(patterns: JsonValue | undefined, all: ReadonlyMap<string, KeyTemplate>): string[] {
  const out: string[] = [];
  if (!Array.isArray(patterns)) return out;
  for (const p of patterns) {
    if (typeof p !== "string") continue;
    const form = p.endsWith("/**") ? "many" : p.endsWith("/*") ? "one" : "plain";
    const base = parseKeyTemplate(form === "many" ? p.slice(0, -3) : form === "one" ? p.slice(0, -2) : p);
    if (base === undefined) continue;
    const n = base.segments.length;
    for (const [text, t] of all) {
      const len = t.segments.length;
      if (form === "plain" ? len !== n : form === "one" ? len !== n + 1 : len <= n) continue;
      if (sameShape(base, t, n) && !out.includes(text)) out.push(text);
    }
  }
  return out;
}

function opInfo(name: string, o: JsonValue, all: ReadonlyMap<string, KeyTemplate>): OpTypeInfo {
  const flag = (k: string, d: boolean): boolean => {
    const v = getMember(o, k);
    return typeof v === "boolean" ? v : d;
  };
  const gated = getMember(o, "dispatch") === "GATED";
  const escrow = getMember(o, "escrow");
  return {
    name,
    lane: gated ? "GATED" : "SEQUENCED",
    optimistic: flag("optimistic", true),
    barrier: flag("barrier", false),
    offlineMode: getMember(o, "offline_mode") === "fail_fast" ? "fail_fast" : "queue_recheck",
    spend: typeof escrow === "string" && escrow !== "",
    writes: selected(getMember(o, "writes"), all),
    logIntent: gated ? flag("log_intent", true) : false,
  };
}

const bytesEqual = (a: Uint8Array, b: Uint8Array): boolean => a.length === b.length && a.every((x, i) => x === b[i]);

/**
 * Reads the answer of `GetManifest` into a manifest. Throws a `ProtocolViolation` (`GetManifestResponse`) for an answer that is not a json/v1
 * manifest document of the version asked for, or whose SHA-256 is not the one it carries. `version` 0 (the active version) accepts any.
 */
export function readManifest(resp: GetManifestResponse, version: number, appId?: string): ParsedManifest {
  const bad = (detail: string): ProtocolViolation => new ProtocolViolation("GetManifestResponse", detail);
  if (resp.encoding !== "json/v1") throw bad(`the encoding is "${resp.encoding}", not json/v1 (04 [MAN-4])`);
  if (version !== 0 && resp.version !== version) throw bad(`the answer is for version ${resp.version}, which was not asked for (asked for ${version})`);
  if (resp.version === 0) throw bad("the answer names no version");
  if (!bytesEqual(sha256(resp.manifest), resp.sha256)) throw bad("the sha256 of the answer is not the SHA-256 of its manifest (04 [MAN-4])");
  let doc: JsonValue;
  try {
    doc = parseDocument(resp.manifest);
  } catch (e) {
    if (e instanceof JsonV1Error) throw bad(`the manifest is not a json/v1 document: ${e.message}`);
    throw e;
  }
  const app = getMember(doc, "app");
  if (typeof app !== "string") throw bad("the manifest names no app");
  if (appId !== undefined && app !== appId) throw bad(`the manifest is the app "${app}"'s, not "${appId}"'s`);
  const rf = getMember(doc, "required_features");
  return new ParsedManifest(resp.version, resp.sha256, app, Array.isArray(rf) ? rf.filter((x): x is string => typeof x === "string") : [], doc);
}

/** A place to keep the manifests that were fetched, so that an engine that starts without a network can restore its replicas (14 [CLI-2]). */
export interface ManifestCache {
  /** The answer of `GetManifest` for the version, as it was encoded by `put`, or undefined. */
  get(version: number): Promise<Uint8Array | undefined>;
  put(version: number, answer: Uint8Array): Promise<void>;
}

export interface ConnectManifestsOptions {
  /** The transport whose `getManifest` is called. */
  transport: Pick<Transport, "getManifest">;
  /** The app's id: a manifest of another app is refused. */
  appId?: string;
  /** Kept manifests; consulted before the network, and filled from it. A read that fails or a bad entry is ignored: the network answers. */
  cache?: ManifestCache;
}

/** The manifests of an app, by version, over `GetManifest`. */
export class ConnectManifests implements Manifests {
  private readonly loaded = new Map<number, ParsedManifest>();
  private readonly pending = new Map<number, Promise<boolean>>();
  /** How many times each version was fetched from the server, for a test. */
  readonly fetches: number[] = [];

  constructor(private readonly o: ConnectManifestsOptions) {}

  get(version: number): EngineManifest | undefined {
    return this.loaded.get(version);
  }

  template(version: number, key: string): Template | undefined {
    return this.loaded.get(version)?.template(version, key);
  }

  /** The manifest of the version, with the app's required features and its hash, for the app (not the engine). */
  parsed(version: number): ParsedManifest | undefined {
    return this.loaded.get(version);
  }

  ensure(version: number): Promise<boolean> {
    if (this.loaded.has(version)) return Promise.resolve(true);
    return this.load(version, false);
  }

  reload(version: number): Promise<boolean> {
    return this.load(version, true);
  }

  private load(version: number, force: boolean): Promise<boolean> {
    if (version === 0) return Promise.resolve(false); // a version of a record or a load is never 0; the active version has a number of its own
    const have = this.pending.get(version);
    if (have !== undefined) return have;
    const p = this.fetch(version, force).finally(() => this.pending.delete(version));
    this.pending.set(version, p);
    return p;
  }

  private async fetch(version: number, force: boolean): Promise<boolean> {
    if (!force && this.o.cache !== undefined) {
      try {
        const kept = await this.o.cache.get(version);
        if (kept !== undefined) {
          this.loaded.set(version, readManifest(decodeMessage<GetManifestResponse>(GetManifestResponseSchema, kept), version, this.o.appId));
          return true;
        }
      } catch {
        // a cache that fails, or holds a bad entry, is no cache: the server answers
      }
    }
    let resp: GetManifestResponse;
    try {
      this.fetches.push(version);
      resp = await this.o.transport.getManifest(create(GetManifestRequestSchema, { version }));
    } catch (e) {
      if (e instanceof RpcError && e.status === "NotFound") return false; // the server holds no such version (04 §4.7)
      throw e;
    }
    this.loaded.set(version, readManifest(resp, version, this.o.appId));
    if (this.o.cache !== undefined) await this.o.cache.put(version, encodeMessage(GetManifestResponseSchema, resp)).catch(() => undefined);
    return true;
  }
}

/** A `ManifestCache` in memory. */
export class MemoryManifestCache implements ManifestCache {
  private readonly rows = new Map<number, Uint8Array>();
  get(version: number): Promise<Uint8Array | undefined> {
    return Promise.resolve(this.rows.get(version));
  }
  put(version: number, answer: Uint8Array): Promise<void> {
    this.rows.set(version, answer);
    return Promise.resolve();
  }
}
