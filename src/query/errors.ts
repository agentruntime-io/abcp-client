// What a query layer refuses: a query that is not one (a prefix that is no key path, a template that is no template, a filter with an
// operator that does not exist). A keyspace that is not loaded, or was purged, is not an error: it is the state of a result (types.ts).

export type QueryErrorCode =
  | "INVALID_QUERY"
  | "INVALID_KEY"
  | "INVALID_PREFIX"
  | "INVALID_TEMPLATE"
  | "INVALID_FILTER"
  | "INVALID_PATH"
  | "INVALID_LIMIT"
  | "NOT_ENTITY"
  | "CLOSED";

export class QueryError extends Error {
  constructor(
    readonly code: QueryErrorCode,
    readonly detail: string,
  ) {
    super(`query refused (${code}): ${detail}`);
    this.name = "QueryError";
  }
}
