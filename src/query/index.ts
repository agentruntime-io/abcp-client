// The local query layer over the client's replica (C4): queries over one keyspace's predicted or confirmed state, by key, prefix, template and entity, with
// filters on decoded json/v1 values, ordering and paging; live queries with incremental updates; keys outside the interest reported as not loaded.

export { Queries, createQueries, type QueryEngine } from "./layer.js";
export { QueryError, type QueryErrorCode } from "./errors.js";
export { compileFilter } from "./filter.js";
export { parsePath, getAt } from "./json.js";
export type {
  BytesRowValue,
  CounterRowValue,
  Coverage,
  CrdtRowValue,
  EntityRef,
  Filter,
  JsonPath,
  JsonRowValue,
  KeyRead,
  KeyspaceState,
  KeyspaceStatus,
  LiveCause,
  LiveListener,
  LiveQuery,
  LiveStats,
  LiveUpdate,
  MovedRowValue,
  NullRowValue,
  Query,
  QueryJson,
  QueryMode,
  QueryResult,
  RedactedRowValue,
  Row,
  RowChange,
  RowValue,
  Select,
  UndecodableRowValue,
} from "./types.js";
