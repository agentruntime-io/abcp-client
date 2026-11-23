// C6: `fetchBlobHttp`, the engine's default HTTP for the signed URLs of the blob store (09 [READ-86], [READ-90]), against a server on the loopback: the PUT carries exactly the headers that it is given and
// the body's length; the GET reads at most the size that it is allowed; a redirect is an error (a signed URL is a bearer capability, so it is not followed to another host).

import * as http from "node:http";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { DRAIN_BUDGET_BYTES, fetchBlobHttp } from "../src/engine/index.js";

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
    const r = await h.put(`${base}/put`, { "x-abcp-checksum-sha256": "AbC+/=" }, new Uint8Array([1, 2, 3, 4, 5]));
    expect(r.status).toBe(200);
    const s = seen.at(-1) as Seen;
    expect(s.method).toBe("PUT");
    expect(s.headers["x-abcp-checksum-sha256"]).toBe("AbC+/="); // opaque: unchanged (03 [ID-27])
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

// V21 MUST-2, 16 [SEC-1]: the URLs are the server's, and what is behind them is not trusted: an answer that the engine does not use (the body of a PUT's answer, of a GET that is not 200) is read
// for at most `DRAIN_BUDGET_BYTES` and then dropped, and a 200 GET is read for at most `maxBytes`, whatever the body is.
describe("16 [SEC-1]: fetchBlobHttp reads a bounded part of every body", () => {
  /** A `fetch` whose answer is a body of `total` octets, `chunk` octets at a time, that counts what the reader pulled; `length` is the `Content-Length` it declares, when it does. */
  const counting = (status: number, total: number, chunk: number, length?: number): { fetchFn: typeof fetch; pulled: () => number } => {
    let pulled = 0;
    const fetchFn = (async () => {
      const body = new ReadableStream<Uint8Array>({
        pull(c) {
          if (pulled >= total) {
            c.close();
            return;
          }
          pulled += chunk;
          c.enqueue(new Uint8Array(chunk));
        },
      });
      return new Response(body, { status, ...(length !== undefined ? { headers: { "content-length": String(length) } } : {}) });
    }) as unknown as typeof fetch;
    return { fetchFn, pulled: () => pulled };
  };
  const total = 256 << 20;

  it("a GET answered with a status other than 200 reads at most the drain budget of a 256 MiB body", async () => {
    const small = counting(404, total, 1024);
    expect(await fetchBlobHttp(small.fetchFn).get("https://x.test/a", { maxBytes: 10 })).toMatchObject({ status: 404 });
    expect(small.pulled()).toBeLessThanOrEqual(DRAIN_BUDGET_BYTES + 4 * 1024); // the budget, and the chunk or two that a stream pulls ahead
    const big = counting(500, total, 1 << 20);
    expect(await fetchBlobHttp(big.fetchFn).get("https://x.test/a", { maxBytes: 10 })).toMatchObject({ status: 500 });
    expect(big.pulled()).toBeLessThanOrEqual(2 << 20);
  });

  it("a PUT whose answer has a 256 MiB body reads at most the drain budget of it, whatever the status", async () => {
    for (const status of [200, 403, 500]) {
      const small = counting(status, total, 1024);
      expect((await fetchBlobHttp(small.fetchFn).put("https://x.test/a", {}, new Uint8Array(10))).status).toBe(status);
      expect(small.pulled()).toBeLessThanOrEqual(DRAIN_BUDGET_BYTES + 4 * 1024);
      const big = counting(status, total, 1 << 20);
      await fetchBlobHttp(big.fetchFn).put("https://x.test/a", {}, new Uint8Array(10));
      expect(big.pulled()).toBeLessThanOrEqual(2 << 20);
    }
  });

  it("a 200 GET that declares a Content-Length over maxBytes reads nothing; one that does not is cut at maxBytes", async () => {
    const declared = counting(200, total, 1 << 20, total);
    expect(await fetchBlobHttp(declared.fetchFn).get("https://x.test/a", { maxBytes: 10 })).toMatchObject({ status: 200, tooLarge: true });
    expect(declared.pulled()).toBeLessThanOrEqual(2 << 20);
    const hidden = counting(200, total, 1 << 20);
    expect(await fetchBlobHttp(hidden.fetchFn).get("https://x.test/a", { maxBytes: 10 })).toMatchObject({ status: 200, tooLarge: true });
    expect(hidden.pulled()).toBeLessThanOrEqual(2 << 20);
  });

  it("against a server on the loopback that streams 128 MiB: the client stops reading, and holds a small part of it", async () => {
    let written = 0;
    const chunk = Buffer.alloc(1 << 20, 0x61);
    const server = http.createServer((req, res) => {
      req.resume();
      res.writeHead(req.url === "/get200" ? 200 : 404, { "content-type": "application/octet-stream" });
      let n = 0;
      const pump = (): void => {
        while (n < 128 && !res.destroyed) {
          n++;
          written += chunk.length;
          if (!res.write(chunk)) {
            res.once("drain", pump);
            return;
          }
        }
        res.end();
      };
      pump();
    });
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
    const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    try {
      const h = fetchBlobHttp();
      const before = process.memoryUsage().arrayBuffers;
      for (const path of ["/get404", "/get200"]) {
        written = 0;
        const r = await h.get(`${url}${path}`, { maxBytes: 1 << 20 });
        expect(r.status).toBe(path === "/get200" ? 200 : 404);
        expect(r.bytes.length).toBe(0);
        await new Promise((res) => setTimeout(res, 100)); // the server notices the closed connection
        expect(written).toBeLessThan(48 << 20); // not the 128 MiB that it would have sent
      }
      written = 0;
      expect((await h.put(`${url}/put404`, {}, new Uint8Array(10))).status).toBe(404);
      await new Promise((res) => setTimeout(res, 100));
      expect(written).toBeLessThan(48 << 20);
      expect(process.memoryUsage().arrayBuffers - before).toBeLessThan(48 << 20);
    } finally {
      await new Promise<void>((r) => {
        server.close(() => r());
        server.closeAllConnections();
      });
    }
  }, 60_000);
});
