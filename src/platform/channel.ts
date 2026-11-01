// A `TabChannel` (14 [CLI-61]) over a `BroadcastChannel`: the standard API of browsers, which Node has as a global too (it reaches the other
// channels of the same name in the process, the worker threads included). A message is structured-cloned and arrives later than it was posted,
// never to the channel that posted it.

import type { TabChannel } from "../engine/tabs.js";

/** The part of `BroadcastChannel` that is used. */
export interface BroadcastChannelLike {
  postMessage(message: unknown): void;
  close(): void;
  onmessage: ((ev: { data: unknown }) => void) | null;
  /** Node's channels keep the process alive until they are closed; a host lets an idle one not. */
  unref?: () => unknown;
}

export type BroadcastChannelConstructor = new (name: string) => BroadcastChannelLike;

export class BroadcastTabChannel implements TabChannel {
  private readonly bc: BroadcastChannelLike;
  private readonly listeners = new Set<(m: unknown) => void>();
  private closed = false;

  constructor(ctor: BroadcastChannelConstructor, name: string, opts: { unref?: boolean } = {}) {
    this.bc = new ctor(name);
    if (opts.unref === true) this.bc.unref?.();
    this.bc.onmessage = (ev): void => {
      for (const l of [...this.listeners]) {
        try {
          l(ev.data);
        } catch {
          // a listener never stops the channel
        }
      }
    };
  }

  post(message: unknown): void {
    if (this.closed) return;
    try {
      this.bc.postMessage(message);
    } catch {
      // a message that cannot be cloned is not sent: the other tabs read the storage, which is the truth
    }
  }

  onMessage(fn: (message: unknown) => void): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.listeners.clear();
    this.bc.onmessage = null;
    this.bc.close();
  }
}
