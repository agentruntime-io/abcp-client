// Making a rename or a removal durable (a Node host): the directory that holds the name is flushed, since flushing a file does not flush the entry that names it. The outbox's storage
// (`file-storage.ts`) flushes its directory after the snapshot is renamed over the journal; the stash of the bytes that its actions name (`file-blob-stash.ts`) does the same (V23 SHOULD-4).

import { open } from "node:fs/promises";

/** Makes the rename or the removal in the directory durable where the platform can (a directory is not opened for reading on Windows): the file itself is on disk when this does nothing. */
export async function syncDirectory(dir: string): Promise<void> {
  try {
    const h = await open(dir, "r");
    try {
      await h.sync();
    } finally {
      await h.close();
    }
  } catch {
    // not possible here: the file itself is on disk
  }
}
