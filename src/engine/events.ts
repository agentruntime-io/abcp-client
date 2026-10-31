// The engine's event stream (14 §14.5). Events reach the app's listeners synchronously, in the order that 14 [CLI-38] gives for
// one result; a listener that throws never stops the engine.

import type { EngineEvent, EngineEventListener } from "./types.js";

export class EventBus {
  private readonly listeners = new Set<EngineEventListener>();
  private n = 0;
  /** The events emitted so far, oldest first, for the test adapter of 17 [CONF-6]; bounded. */
  readonly history: EngineEvent[] = [];

  constructor(private readonly keep = 20_000) {}

  on(listener: EngineEventListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  emit(e: EngineEvent): void {
    const ev = { ...e, n: ++this.n } as EngineEvent;
    this.history.push(ev);
    if (this.history.length > this.keep) this.history.splice(0, this.history.length - this.keep);
    for (const l of [...this.listeners]) {
      try {
        l(ev);
      } catch {
        // an app's listener never stops the engine
      }
    }
  }
}
