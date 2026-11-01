// A test issuer for the Go server (10 §10.1): one HTTPS server on the loopback that serves, for each of its issuers, the OpenID Connect
// discovery document and the JWKS, and mints tokens with that issuer's key. The Go server fetches the keys over TLS (04 [MAN-85]), so the
// server's certificate is a self-signed one, built here (a minimal X.509 writer on node:crypto), that the server's configuration
// trusts (`fetch_ca_file`). This is the Node counterpart of `server/internal/auth/authtest`, which a test outside the Go module cannot import.

import * as crypto from "node:crypto";
import * as https from "node:https";
import type { AddressInfo } from "node:net";

// ---- a minimal DER writer ------------------------------------------------------------------------------------------

const der = (tag: number, ...parts: Uint8Array[]): Uint8Array => {
  const body = Buffer.concat(parts);
  let len: Buffer;
  if (body.length < 0x80) len = Buffer.from([body.length]);
  else if (body.length < 0x100) len = Buffer.from([0x81, body.length]);
  else len = Buffer.from([0x82, body.length >> 8, body.length & 0xff]);
  return Buffer.concat([Buffer.from([tag]), len, body]);
};
const seq = (...p: Uint8Array[]): Uint8Array => der(0x30, ...p);
const set = (...p: Uint8Array[]): Uint8Array => der(0x31, ...p);
const oid = (dotted: string): Uint8Array => {
  const n = dotted.split(".").map(Number);
  const out: number[] = [(n[0] as number) * 40 + (n[1] as number)];
  for (const v of n.slice(2)) {
    const bytes = [v & 0x7f];
    for (let x = v >> 7; x > 0; x >>= 7) bytes.unshift((x & 0x7f) | 0x80);
    out.push(...bytes);
  }
  return der(0x06, Buffer.from(out));
};
const int = (v: number): Uint8Array => der(0x02, Buffer.from(v < 0x80 ? [v] : [0, v]));
const utf8 = (s: string): Uint8Array => der(0x0c, Buffer.from(s, "utf8"));
const bool = (v: boolean): Uint8Array => der(0x01, Buffer.from([v ? 0xff : 0]));
const octets = (b: Uint8Array): Uint8Array => der(0x04, b);
const bits = (b: Uint8Array, unused = 0): Uint8Array => der(0x03, Buffer.from([unused]), b);
const utcTime = (d: Date): Uint8Array => {
  const p = (x: number): string => String(x).padStart(2, "0");
  return der(0x17, Buffer.from(`${p(d.getUTCFullYear() % 100)}${p(d.getUTCMonth() + 1)}${p(d.getUTCDate())}${p(d.getUTCHours())}${p(d.getUTCMinutes())}${p(d.getUTCSeconds())}Z`, "ascii"));
};
const extension = (id: string, critical: boolean, value: Uint8Array): Uint8Array => seq(oid(id), ...(critical ? [bool(true)] : []), octets(value));

/** A self-signed certificate of an ECDSA P-256 key for 127.0.0.1 and `localhost`, which can be a trust root (a CA) for itself. */
export function selfSignedCertificate(): { certPem: string; keyPem: string } {
  const { publicKey, privateKey } = crypto.generateKeyPairSync("ec", { namedCurve: "P-256" });
  const name = seq(set(seq(oid("2.5.4.3"), utf8("abpc-test-issuer"))));
  const sigAlg = seq(oid("1.2.840.10045.4.3.2")); // ecdsa-with-SHA256
  const now = Date.now();
  const tbs = seq(
    der(0xa0, int(2)), // v3
    int(1),
    sigAlg,
    name,
    seq(utcTime(new Date(now - 3600_000)), utcTime(new Date(now + 24 * 3600_000))),
    name,
    publicKey.export({ type: "spki", format: "der" }),
    der(
      0xa3,
      seq(
        extension("2.5.29.19", true, seq(bool(true))), // basicConstraints: CA
        extension("2.5.29.15", true, bits(Buffer.from([0x84]), 2)), // keyUsage: digitalSignature, keyCertSign
        extension("2.5.29.37", false, seq(oid("1.3.6.1.5.5.7.3.1"))), // extKeyUsage: serverAuth
        extension("2.5.29.17", false, seq(der(0x87, Buffer.from([127, 0, 0, 1])), der(0x82, Buffer.from("localhost", "ascii")))), // subjectAltName
      ),
    ),
  );
  const signature = crypto.sign("sha256", tbs, privateKey);
  const cert = seq(tbs, sigAlg, bits(signature));
  const pem = (label: string, body: Uint8Array): string => `-----BEGIN ${label}-----\n${(Buffer.from(body).toString("base64").match(/.{1,64}/g) ?? []).join("\n")}\n-----END ${label}-----\n`;
  return { certPem: pem("CERTIFICATE", cert), keyPem: privateKey.export({ type: "pkcs8", format: "pem" }) as string };
}

// ---- the issuers -------------------------------------------------------------------------------------------------

export interface Grant {
  ks: string;
  actions: string[];
}

export interface TokenOptions {
  sub: string;
  /** The app (the audience); default the issuer's app. */
  aud?: string;
  grants?: Grant[];
  deviceId?: string;
  /** The lifetime in seconds from now; default 600. */
  lifetimeS?: number;
  /** Extra or overriding claims. */
  claims?: Record<string, unknown>;
}

export class TestIssuer {
  readonly kid: string;
  private readonly privateKey: crypto.KeyObject;
  private readonly jwk: crypto.JsonWebKey;
  /** The number of times the Go server fetched the keys. */
  fetches = 0;

  constructor(
    /** The issuer URL: the server's origin and this issuer's path. */
    readonly url: string,
    readonly path: string,
    readonly app: string,
    kid: string,
  ) {
    this.kid = kid;
    const { publicKey, privateKey } = crypto.generateKeyPairSync("ec", { namedCurve: "P-256" });
    this.privateKey = privateKey;
    this.jwk = { ...publicKey.export({ format: "jwk" }), kid, use: "sig", alg: "ES256" };
  }

  jwks(): string {
    return JSON.stringify({ keys: [this.jwk] });
  }

  discovery(): string {
    return JSON.stringify({ issuer: this.url, jwks_uri: `${this.url}/jwks.json` });
  }

  /** A signed token of the issuer: the claims of 10 [AUTH-8], [AUTH-39]. */
  mint(o: TokenOptions): string {
    const now = Math.floor(Date.now() / 1000);
    const claims: Record<string, unknown> = {
      iss: this.url,
      aud: o.aud ?? this.app,
      sub: o.sub,
      iat: now,
      exp: now + (o.lifetimeS ?? 600),
      abpc: { grants: (o.grants ?? [{ ks: "acme/**", actions: ["read", "write:*", "keyspace:create"] }]).map((g) => ({ ks: g.ks, actions: g.actions })) },
      ...(o.deviceId !== undefined ? { device_id: o.deviceId } : {}),
      ...(o.claims ?? {}),
    };
    const b64 = (v: unknown): string => Buffer.from(typeof v === "string" ? v : JSON.stringify(v)).toString("base64url");
    const input = `${b64({ alg: "ES256", typ: "JWT", kid: this.kid })}.${b64(claims)}`;
    const sig = crypto.sign("sha256", Buffer.from(input), { key: this.privateKey, dsaEncoding: "ieee-p1363" });
    return `${input}.${sig.toString("base64url")}`;
  }
}

/** The HTTPS server of the user issuer (path `/user`) and the admin issuer (path `/admin`). */
export class IssuerServer {
  readonly user: TestIssuer;
  readonly admin: TestIssuer;
  readonly certPem: string;
  readonly origin: string;
  private readonly server: https.Server;

  private constructor(server: https.Server, certPem: string, origin: string, app: string) {
    this.server = server;
    this.certPem = certPem;
    this.origin = origin;
    this.user = new TestIssuer(`${origin}/user`, "/user", app, "user-k1");
    this.admin = new TestIssuer(`${origin}/admin`, "/admin", app, "admin-k1");
  }

  static async start(app: string): Promise<IssuerServer> {
    const { certPem, keyPem } = selfSignedCertificate();
    // the handler needs the issuers, which need the origin, which is known once the server listens
    let issuers: IssuerServer | undefined;
    const server = https.createServer({ key: keyPem, cert: certPem }, (req, res) => {
      const url = req.url ?? "";
      for (const is of [issuers?.user, issuers?.admin]) {
        if (is === undefined) continue;
        const respond = (body: string): void => {
          res.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
          res.end(body);
        };
        if (url === `${is.path}/.well-known/openid-configuration`) return respond(is.discovery());
        if (url === `${is.path}/jwks.json`) {
          is.fetches++;
          return respond(is.jwks());
        }
      }
      res.writeHead(404);
      res.end();
    });
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const origin = `https://127.0.0.1:${(server.address() as AddressInfo).port}`;
    issuers = new IssuerServer(server, certPem, origin, app);
    return issuers;
  }

  async close(): Promise<void> {
    this.server.closeAllConnections();
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}
