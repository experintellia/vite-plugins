// Shared scaffolding for the webxdc simulator suites. Both suites exercise the
// *real published artifact* (`src/webxdc.js`) by evaluating it as a classic
// script in a jsdom window with `fake-indexeddb` installed, driving it only
// through the public webxdc API. Re-evaluating the source simulates a page
// reload (a fresh closure over the same persisted database).

import { readFileSync } from "node:fs";
import path from "node:path";
import { IDBFactory } from "fake-indexeddb";

export const SRC = readFileSync(
  path.join(process.cwd(), "src", "webxdc.js"),
  "utf-8",
);

export const UPDATES_KEY = "__xdcUpdatesKey__";
export const EPHEMERAL_KEY = "__xdcEphemeralUpdateKey__";

// Indirect eval runs in global scope, so `window.webxdc = (() => { ... })()`
// assigns onto the jsdom window. Each call creates a brand-new closure (a
// simulated reload).
export function loadStub() {
  (0, eval)(SRC);
  return window.webxdc;
}

export function freshIndexedDB() {
  const factory = new IDBFactory();
  globalThis.indexedDB = factory;
  window.indexedDB = factory;
}

export const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

export async function waitFor(predicate) {
  for (let i = 0; i < 100 && !predicate(); i++) {
    await tick();
  }
}
