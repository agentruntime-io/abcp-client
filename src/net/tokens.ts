// The bearer token of a client and its in-band refresh (10 §10.7, 14 [CLI-68]).
//
// The engine asks for a new token before the current one expires (`expiresAt`), or when a call or the session is refused for the token
// (`refresh`), and then tells the session with `RefreshSession{session_id}`, whose `Authorization` header carries the new token (10 [AUTH-48]):
// the session is not reconnected. So the transport and the engine share one holder of the token, which this is: the transport reads
// `current()` for each call, the engine's `TokenProvider` side installs the token that the app gives it.
//
// The app gives the engine a new token through `source`: an OIDC refresh, a call to its own backend, whatever it has. At most one refresh is
// in flight at a time (10 [AUTH-48]); a call to `refresh` that arrives while one runs shares its answer.

import type { TokenProvider } from "../engine/types.js";

/** A token with the expiry that its holder knows of, in ms since the epoch. Without `expiresAt` the expiry is the token's own `exp` claim. */
export interface TokenGrant {
  token: string;
  expiresAt?: number;
}

/** What the app gives the engine when it asks for a token: a token (a string, or with its expiry), or nothing when it has none. */
export type TokenSource = () => Promise<string | TokenGrant | undefined> | string | TokenGrant | undefined;

export interface TokenStoreOptions {
  /** The token that the client starts with. */
  initial?: string | TokenGrant;
  /** Gets a new token; without it `refresh` resolves false. */
  source?: TokenSource;
}

/** Tokens are bounded by the server (10 [AUTH-10]); a claim reader reads no more than this. */
const MAX_TOKEN_BYTES = 16 * 1024;

/**
 * The `exp` claim of a JWT in ms since the epoch, or undefined when the token has none or is not a JWT. The signature is not checked:
 * the server is the judge of the token, and the client only needs to know when to ask for another.
 */
export function jwtExpiresAt(token: string): number | undefined {
  if (token.length > MAX_TOKEN_BYTES) return undefined;
  const parts = token.split(".");
  if (parts.length !== 3) return undefined;
  try {
    const b64 = (parts[1] as string).replace(/-/g, "+").replace(/_/g, "/");
    const json = new TextDecoder().decode(Uint8Array.from(atob(b64.padEnd(Math.ceil(b64.length / 4) * 4, "=")), (c) => c.charCodeAt(0)));
    const exp = (JSON.parse(json) as { exp?: unknown }).exp;
    return typeof exp === "number" && Number.isFinite(exp) ? Math.floor(exp * 1000) : undefined;
  } catch {
    return undefined;
  }
}

export class TokenStore implements TokenProvider {
  private token_: string | undefined;
  private expiresAt_: number | undefined;
  private inflight: Promise<boolean> | undefined;
  private readonly source: TokenSource | undefined;

  constructor(opts: TokenStoreOptions = {}) {
    this.source = opts.source;
    if (opts.initial !== undefined) this.install(opts.initial);
  }

  private install(g: string | TokenGrant): void {
    const grant = typeof g === "string" ? { token: g } : g;
    this.token_ = grant.token;
    this.expiresAt_ = grant.expiresAt ?? jwtExpiresAt(grant.token);
  }

  /** The token that the next call sends, or undefined when there is none. */
  get token(): string | undefined {
    return this.token_;
  }

  /** What the transport is given to read the token of each call: `new ConnectCaller({ token: store.reader })`. */
  readonly reader = (): string | undefined => this.token_;

  /** Installs a token that the app has now, such as one that its sign-in flow has just returned. */
  set(token: string | TokenGrant): void {
    this.install(token);
  }

  /** Forgets the token (a sign-out): the next call is sent with none. */
  clear(): void {
    this.token_ = undefined;
    this.expiresAt_ = undefined;
  }

  expiresAt(): number | undefined {
    return this.expiresAt_;
  }

  /**
   * Gets a new token from the app and installs it. Resolves false when the app has none to give or fails: the old token stays (the engine
   * holds `NO_TOKEN` and asks again, 14 [CLI-68]). A call that arrives while another is waiting for the app shares its answer.
   */
  refresh(): Promise<boolean> {
    if (this.source === undefined) return Promise.resolve(false);
    if (this.inflight !== undefined) return this.inflight;
    const run = async (): Promise<boolean> => {
      try {
        const g = await this.source?.();
        if (g === undefined || (typeof g === "string" ? g === "" : g.token === "")) return false;
        this.install(g);
        return true;
      } catch {
        return false;
      }
    };
    const p = run().finally(() => {
      this.inflight = undefined;
    });
    this.inflight = p;
    return p;
  }
}
