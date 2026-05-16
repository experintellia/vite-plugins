// Cross-window notification test. Runs in its own file so Vitest isolates it
// in a separate worker, where Node's process-wide global BroadcastChannel is
// intact (the storage-layer suite forces the fallback path for determinism).
//
// Two evaluations of the stub = two windows: each IIFE closure keeps its own
// BroadcastChannel + updateListener but shares the same fake-indexeddb. A
// message posted on one channel instance is delivered to the other (Node's
// BroadcastChannel does not echo to the sender), exactly like two browser tabs.

import { readFileSync } from "node:fs";
import path from "node:path";
import { IDBFactory } from "fake-indexeddb";
import { beforeEach, describe, expect, it } from "vitest";

const SRC = readFileSync(path.join(process.cwd(), "src", "webxdc.js"), "utf-8");
const hasBroadcastChannel = typeof globalThis.BroadcastChannel !== "undefined";

function loadStub() {
  (0, eval)(SRC);
  return window.webxdc;
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));
async function waitFor(predicate) {
  for (let i = 0; i < 100 && !predicate(); i++) {
    await tick();
  }
}

beforeEach(() => {
  window.localStorage.clear();
  const factory = new IDBFactory();
  globalThis.indexedDB = factory;
  window.indexedDB = factory;
});

describe.skipIf(!hasBroadcastChannel)(
  "cross-window notification via BroadcastChannel",
  () => {
    it("delivers a new update from one window to another window's listener", async () => {
      const w1 = loadStub(); // window 1
      const got1 = [];
      await w1.setUpdateListener((u) => got1.push(u), 0);

      const w2 = loadStub(); // window 2: separate closure + channel
      await w2.setUpdateListener(() => {}, 0);

      await w2.sendUpdate({ payload: "from-w2" });
      await waitFor(() => got1.length > 0);

      expect(got1.map((u) => u.payload)).toEqual(["from-w2"]);
      expect(got1[0].serial).toBe(1);
      expect(got1[0].max_serial).toBe(1);
    });

    it("is not delivered twice when the update is also in the shared store", async () => {
      const w1 = loadStub();
      const w2 = loadStub();

      // w2 sends before w1 attaches its listener: the record is already in the
      // shared IndexedDB, so w1 must replay it exactly once (not replay + a
      // duplicate broadcast).
      await w2.sendUpdate({ payload: "early" });
      await tick();

      const got1 = [];
      await w1.setUpdateListener((u) => got1.push(u), 0);

      await w2.sendUpdate({ payload: "late" });
      await waitFor(() => got1.length >= 2);
      await tick();

      expect(got1.map((u) => u.payload)).toEqual(["early", "late"]);
      expect(got1.map((u) => u.serial)).toEqual([1, 2]);
    });
  },
);
