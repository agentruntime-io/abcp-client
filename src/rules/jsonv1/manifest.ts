// The canonical form and hash of a manifest document of encoding json/v1 (04 [MAN-4]).

import { sha256 } from "../crypto.js";
import { canonical } from "./canonical.js";
import { JsonV1Error, definitionError } from "./errors.js";
import { parseDocument } from "./parse.js";
import { type JsonValue, MAX_CANONICAL_DOCUMENT_BYTES } from "./value.js";

/**
 * Returns the canonical form of a manifest document of encoding json/v1 (04 [MAN-4]): the parsed document with each number
 * as the binary64 value that it denotes (05 [STATE-7]), serialized by RFC 8785 (JCS). Its octets are the manifest's
 * canonical form, and their SHA-256 is its hash (`manifestHash`). The document is read as `parseDocument` says (04 [MAN-3]),
 * so unlike a json/v1 value it may use any whitespace, member order and number text. The canonical form is at most 1 MiB
 * and the document at most 4 MiB. Throws a JsonV1Error with code INVALID_ENVELOPE (04 [MAN-77]).
 */
export function canonicalDocument(doc: Uint8Array): Uint8Array {
  return canonicalManifest(parseDocument(doc));
}

/**
 * The canonical form of a manifest given as a parsed value, which is how a yaml/v1 document, once converted to JSON, gets
 * its canonical form (04 [MAN-3], [MAN-4]). It refuses a canonical form over 1 MiB.
 */
export function canonicalManifest(v: JsonValue): Uint8Array {
  let out: Uint8Array;
  try {
    out = canonical(v);
  } catch (e) {
    throw definitionError("MAN-4", "", `the document has no canonical form: ${e instanceof Error ? e.message : String(e)}`);
  }
  if (out.length > MAX_CANONICAL_DOCUMENT_BYTES) {
    throw new JsonV1Error(
      "INVALID_ENVELOPE",
      "json-limits",
      "MAN-4",
      `the canonical form is ${out.length} octets, over the limit of ${MAX_CANONICAL_DOCUMENT_BYTES}`,
    );
  }
  return out;
}

/** The manifest hash of a document of encoding json/v1: the SHA-256 of its canonical form (04 [MAN-4]), which a MANIFEST_ACTIVATED record carries (06 §6.7). */
export function manifestHash(doc: Uint8Array): Uint8Array {
  return sha256(canonicalDocument(doc));
}
