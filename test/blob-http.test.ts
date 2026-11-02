// C6: `fetchBlobHttp`, the engine's default HTTP for the signed URLs of the blob store (09 [READ-86], [READ-90]), against a server on the loopback: the PUT carries exactly the headers that it is given and
// the body's length; the GET reads at most the size that it is allowed; a redirect is an error (a signed URL is a bearer capability, so it is not followed to another host).

import * as http from "node:http";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { fetchBlobHttp } from "../src/engine/index.js";

interface Seen {
  method: string;
  url: string;
  headers: http.IncomingHttpHeaders;
  body: Buffer;
}

describe("09 [READ-86], [READ-90]: fetchBlobHttp", () => {
  let server: http.Server;
  let base: string;
  const seen: Seen[] = [];

  beforeAll(async () => {
    server = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (c: Buffer) => chunks.push(c));
      req.on("end", () => {
        seen.push({ method: req.method ?? "", url: req.url ?? "", headers: req.headers, body: Buffer.concat(chunks) });
        if (req.url === "/redirect") {
          res.writeHead(302, { location: "/elsewhere" });
          res.end();
        } else if (req.url === "/missing") {
          res.writeHead(404);
          res.end("no");
        } else if (req.url === "/refuse") {
          res.writeHead(403);
          res.end();
        } else if (req.method === "GET") {
          res.writeHead(200, { "content-type": "application/octet-stream", "content-disposition": "attachment" });
          res.end(Buffer.from("0123456789"));
        } else {
          res.writeHead(200);
          res.end();
        }
      });
    });
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
    base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  });
  afterAll(async () => {
    await new Promise<void>((r) => server.close(() => r()));
  });

  it("a PUT carries exactly the headers that it is given, and the length of its body", async () => {
    const h = fetchBlobHttp();
    const r = await h.put(`${base}/put`, { "x-abpc-checksum-sha256": "AbC+/=" }, new Uint8Array([1, 2, 3, 4, 5]));
    expect(r.status).toBe(200);
    const s = seen.at(-1) as Seen;
    expect(s.method).toBe("PUT");
    expect(s.headers["x-abpc-checksum-sha256"]).toBe("AbC+/="); // opaque: unchanged (03 [ID-27])
    expect(s.headers["content-length"]).toBe("5");
    expect(s.headers["cookie"]).toBeUndefined();
    expect([...s.body]).toEqual([1, 2, 3, 4, 5]);
    // an empty body
    expect((await h.put(`${base}/put`, {}, new Uint8Array(0))).status).toBe(200);
    expect(seen.at(-1)?.headers["content-length"]).toBe("0");
  });

  it("a status other than 2xx is returned, not thrown: the engine decides what a status means", async () => {
    const h = fetchBlobHttp();
    expect((await h.put(`${base}/refuse`, {}, new Uint8Array(1))).status).toBe(403);
    expect(await h.get(`${base}/missing`, { maxBytes: 100 })).toMatchObject({ status: 404 });
  });

  it("a GET reads the body up to the size that it is allowed; a longer body is refused as too large", async () => {
    const h = fetchBlobHttp();
    const ok = await h.get(`${base}/get`, { maxBytes: 10 });
    expect(ok.status).toBe(200);
    expect(Buffer.from(ok.bytes).toString()).toBe("0123456789");
    const over = await h.get(`${base}/get`, { maxBytes: 9 });
    expect(over).toMatchObject({ status: 200, tooLarge: true });
    expect(over.bytes.length).toBe(0);
  });

  it("a redirect is an error: the signed URL is not followed to another place", async () => {
    const h = fetchBlobHttp();
    await expect(h.get(`${base}/redirect`, { maxBytes: 10 })).rejects.toBeInstanceOf(Error);
    await expect(h.put(`${base}/redirect`, {}, new Uint8Array(1))).rejects.toBeInstanceOf(Error);
  });

  it("an abort ends the call", async () => {
    const h = fetchBlobHttp();
    const ac = new AbortController();
    ac.abort();
    await expect(h.get(`${base}/get`, { maxBytes: 10, signal: ac.signal })).rejects.toBeInstanceOf(Error);
  });
});
