// The canonical protobuf encoding of 07 [MUT-45] and the hashes built on it, and the refusals of 07 [MUT-45] and 02 [ARC-61]
// that are made on received bytes.

export { CanonError, MAX_DEPTH, type CanonKind } from "./errors.js";
export { encode, type EncodeView } from "./encode.js";
export { checkEncoding, checkShortest, checkTimestamps, checkUtf8, checkUtf8Request, checkWireTypes, timestampInRange } from "./check.js";
export { decode, unmarshal, unmarshalJson } from "./decode.js";
export {
  SALT_SIZE,
  changeValueDigest,
  contentDigest,
  contentHash,
  contentHashInput,
  entryValueDigest,
  hashRecord,
  hashRecordFromDigests,
  overwroteNewerBytes,
  recordHash,
  requestDigest,
  valueDigests,
  type RecordHashes,
} from "./hash.js";
