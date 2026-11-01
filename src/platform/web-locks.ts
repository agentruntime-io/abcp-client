// The cross-tab lock of 14 [CLI-61] over Web Locks (`navigator.locks`), the standard API: one exclusive lock for each partition, granted to the waiting
// tabs in the order that they asked, and freed by the platform when its holder closes or crashes. A lock is held as long as the callback that was
// given to `request` has not settled, so `release` settles it.
//
// Where there is no `navigator.locks` (an insecure context, an old browser) the host says `reliable: false`, and each tab is an engine of its own
// (14 [CLI-61]); `tryAcquire` and `acquire` are not called then.

import type { CrossTabLocks, LockLease } from "../engine/tabs.js";

/** The part of `LockManager` that is used. */
export interface LockManagerLike {
  request(
    name: string,
    options: { mode?: "exclusive" | "shared"; ifAvailable?: boolean; steal?: boolean; signal?: AbortSignal },
    callback: (lock: object | null) => Promise<unknown> | unknown,
  ): Promise<unknown>;
}

export class WebLocks implements CrossTabLocks {
  readonly reliable: boolean;

  constructor(private readonly manager: LockManagerLike | undefined) {
    this.reliable = manager !== undefined;
  }

  tryAcquire(name: string): Promise<LockLease | undefined> {
    return this.take(name, { ifAvailable: true });
  }

  async acquire(name: string, signal?: AbortSignal): Promise<LockLease> {
    const l = await this.take(name, signal !== undefined ? { signal } : {});
    if (l === undefined) throw new Error("the lock request was not granted");
    return l;
  }

  private take(name: string, options: { ifAvailable?: boolean; signal?: AbortSignal }): Promise<LockLease | undefined> {
    const manager = this.manager;
    if (manager === undefined) return Promise.reject(new Error("this platform has no Web Locks"));
    return new Promise<LockLease | undefined>((resolve, reject) => {
      let lease: (LockLease & { held: boolean }) | undefined;
      manager
        .request(name, { mode: "exclusive", ...options }, (lock) => {
          if (lock === null) {
            resolve(undefined); // `ifAvailable`, and another holder has it
            return undefined;
          }
          // the lock is held until this promise settles: `release` settles it
          return new Promise<void>((settle) => {
            const l = {
              held: true,
              release: (): Promise<void> => {
                if (l.held) {
                  l.held = false;
                  settle();
                }
                return Promise.resolve();
              },
            };
            lease = l;
            resolve(l);
          });
        })
        .catch((e: unknown) => {
          // a request that is aborted before it is granted rejects (an `AbortError`); a lock that another context stole rejects after the grant
          if (lease !== undefined) lease.held = false;
          else reject(e);
        });
    });
  }
}
