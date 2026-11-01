// C5: the token of a client and its in-band refresh (10 §10.7, 14 [CLI-68]): src/net/tokens.ts.

import { describe, expect, it } from "vitest";
import { jwtExpiresAt, TokenStore } from "../src/net/index.js";

const jwt = (claims: object): string => `${Buffer.from("{}").toString("base64url")}.${Buffer.from(JSON.stringify(claims)).toString("base64url")}.sig`;

describe("the expiry of a token", () => {
  it("is the `exp` claim in ms, and nothing when the token is not a JWT or has none", () => {
    expect(jwtExpiresAt(jwt({ exp: 1_800_000_000 }))).toBe(1_800_000_000_000);
    expect(jwtExpiresAt(jwt({ exp: 1_800_000_000.5 }))).toBe(1_800_000_000_500);
    expect(jwtExpiresAt(jwt({ sub: "x" }))).toBeUndefined();
    expect(jwtExpiresAt(jwt({ exp: "soon" }))).toBeUndefined();
    expect(jwtExpiresAt("opaque-token")).toBeUndefined();
    expect(jwtExpiresAt("a.b.c")).toBeUndefined();
    expect(jwtExpiresAt(`x.${"A".repeat(20_000)}.y`)).toBeUndefined();
  });
});

describe("the token store", () => {
  it("holds the token that the transport reads for each call and the expiry that the engine reads, from the token or from the app", () => {
    const store = new TokenStore({ initial: jwt({ exp: 1_800_000_000 }) });
    expect(store.reader()).toBe(store.token);
    expect(store.expiresAt()).toBe(1_800_000_000_000);
    store.set({ token: "opaque", expiresAt: 5 });
    expect(store.reader()).toBe("opaque");
    expect(store.expiresAt()).toBe(5);
    store.clear();
    expect(store.reader()).toBeUndefined();
    expect(store.expiresAt()).toBeUndefined();
  });

  it("refresh installs what the app gives and says so; a refusal, nothing and a failure leave the old token and say no", async () => {
    let next: string | { token: string; expiresAt?: number } | undefined | Error = jwt({ exp: 1_900_000_000 });
    const store = new TokenStore({
      initial: "old",
      source: () => {
        if (next instanceof Error) throw next;
        return next;
      },
    });
    expect(await store.refresh()).toBe(true);
    expect(store.expiresAt()).toBe(1_900_000_000_000);
    const fresh = store.token;
    for (const bad of [undefined, "", { token: "" }, new Error("the backend is down")] as const) {
      next = bad;
      expect(await store.refresh()).toBe(false);
      expect(store.token).toBe(fresh);
    }
    expect(await new TokenStore({ initial: "x" }).refresh()).toBe(false); // an app with no way to get another token
  });

  it("has one refresh in flight: a call that comes while one waits shares its answer (10 [AUTH-48])", async () => {
    let calls = 0;
    let release: (t: string) => void = () => undefined;
    const store = new TokenStore({
      source: () => {
        calls++;
        return new Promise<string>((r) => (release = r));
      },
    });
    const a = store.refresh();
    const b = store.refresh();
    release("shared");
    expect(await Promise.all([a, b])).toEqual([true, true]);
    expect(calls).toBe(1);
    expect(store.token).toBe("shared");
    const c = store.refresh(); // the next one is a new refresh
    release("second");
    await c;
    expect(calls).toBe(2);
  });
});
