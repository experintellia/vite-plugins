// Storage-layer tests: IndexedDB persistence, legacy migration idempotency
// (verified by re-evaluating the source to simulate a reload), and graceful
// degradation when IndexedDB is unavailable.

import { beforeEach, describe, expect, it } from "vitest";
import {
  EPHEMERAL_KEY,
  freshIndexedDB,
  loadStub,
  UPDATES_KEY,
} from "./helpers.js";

beforeEach(() => {
  window.localStorage.clear();
  freshIndexedDB();
  // Force the localStorage-event fallback signaling path (jsdom does not
  // dispatch cross-document storage events) so re-evaluated instances within a
  // test do not cross-talk through a process-wide BroadcastChannel.
  globalThis.BroadcastChannel = undefined;
  window.BroadcastChannel = undefined;
});

describe("IndexedDB persistence", () => {
  it("appends each update as its own record and reads them back in order", async () => {
    const w = loadStub();
    await w.sendUpdate({ payload: { a: 1 }, info: "i1" });
    await w.sendUpdate({ payload: { a: 2 }, info: "i2" });

    const got = [];
    await w.setUpdateListener((u) => got.push(u), 0);

    expect(got.map((u) => u.serial)).toEqual([1, 2]);
    expect(got.map((u) => u.payload.a)).toEqual([1, 2]);
    expect(got.every((u) => u.max_serial === 2)).toBe(true);
    // Nothing was written to the legacy localStorage key.
    expect(window.localStorage.getItem(UPDATES_KEY)).toBe(null);
  });

  it("setUpdateListener replays only updates with serial > arg", async () => {
    const w = loadStub();
    for (let i = 0; i < 3; i++) {
      await w.sendUpdate({ payload: i });
    }

    const got = [];
    await w.setUpdateListener((u) => got.push(u), 1);

    expect(got.map((u) => u.serial)).toEqual([2, 3]);
    expect(got.every((u) => u.max_serial === 3)).toBe(true);
  });

  it("does not throw on a history far larger than the localStorage quota", async () => {
    const w = loadStub();
    const big = "x".repeat(100 * 1024); // 100 KiB each
    // ~10 MiB total — would exceed the ~5 MiB localStorage string quota.
    for (let i = 0; i < 100; i++) {
      await w.sendUpdate({ payload: big });
    }

    let count = 0;
    await w.setUpdateListener(() => count++, 0);
    expect(count).toBe(100);
  });

  it("serializes concurrent sendUpdate calls without serial collisions", async () => {
    const w = loadStub();
    await Promise.all(
      Array.from({ length: 25 }, (_, i) => w.sendUpdate({ payload: i })),
    );

    const serials = [];
    await w.setUpdateListener((u) => serials.push(u.serial), 0);
    expect(serials).toEqual(Array.from({ length: 25 }, (_, i) => i + 1));
  });
});

describe("legacy localStorage migration", () => {
  it("imports legacy updates in serial order then removes the key", async () => {
    window.localStorage.setItem(
      UPDATES_KEY,
      JSON.stringify([
        { payload: "p1", info: "a", serial: 1, _sender: "x" },
        { payload: "p2", info: "b", serial: 2, _sender: "x" },
        { payload: "p3", info: "c", serial: 3, _sender: "x" },
      ]),
    );

    const w = loadStub();
    const got = [];
    await w.setUpdateListener((u) => got.push(u), 0);

    expect(got.map((u) => u.payload)).toEqual(["p1", "p2", "p3"]);
    expect(got.map((u) => u.serial)).toEqual([1, 2, 3]);
    expect(window.localStorage.getItem(UPDATES_KEY)).toBe(null);
  });

  it("is idempotent and a no-op when IndexedDB already has data", async () => {
    window.localStorage.setItem(
      UPDATES_KEY,
      JSON.stringify([{ payload: "p1", serial: 1, _sender: "x" }]),
    );

    let w = loadStub();
    await w.setUpdateListener(() => {}, 0);
    expect(window.localStorage.getItem(UPDATES_KEY)).toBe(null);

    // Simulate a reload where stale legacy data is present but IndexedDB is
    // already populated: the migration must NOT import it (no-op) and must not
    // duplicate existing records.
    window.localStorage.setItem(
      UPDATES_KEY,
      JSON.stringify([{ payload: "SHOULD_NOT_IMPORT", serial: 1 }]),
    );
    w = loadStub(); // same IDBFactory — DB persists across this "reload"

    const got = [];
    await w.setUpdateListener((u) => got.push(u), 0);

    expect(got.map((u) => u.payload)).toEqual(["p1"]);
    expect(got.length).toBe(1);
    // No-op left the (stale) legacy key untouched.
    expect(window.localStorage.getItem(UPDATES_KEY)).not.toBe(null);
  });

  it("clears a stale ephemeral signaling key on init", async () => {
    window.localStorage.setItem(
      EPHEMERAL_KEY,
      JSON.stringify(["peer", [1, 2, 3], Date.now()]),
    );

    const w = loadStub();
    await w.setUpdateListener(() => {}, 0);

    expect(window.localStorage.getItem(EPHEMERAL_KEY)).toBe(null);
  });

  it("ignores a corrupt legacy value and clears it", async () => {
    window.localStorage.setItem(UPDATES_KEY, "{not valid json");

    const w = loadStub();
    const got = [];
    await w.setUpdateListener((u) => got.push(u), 0);

    expect(got).toEqual([]);
    expect(window.localStorage.getItem(UPDATES_KEY)).toBe(null);
  });
});

describe("graceful degradation without IndexedDB", () => {
  it("falls back to localStorage instead of throwing", async () => {
    globalThis.indexedDB = undefined;
    window.indexedDB = undefined;

    const w = loadStub();
    await w.sendUpdate({ payload: "fb1" });
    await w.sendUpdate({ payload: "fb2" });

    const got = [];
    await w.setUpdateListener((u) => got.push(u), 0);

    expect(got.map((u) => u.payload)).toEqual(["fb1", "fb2"]);
    expect(got.map((u) => u.serial)).toEqual([1, 2]);
    const stored = JSON.parse(window.localStorage.getItem(UPDATES_KEY));
    expect(stored.length).toBe(2);
  });
});

// Regression: an earlier IndexedDB rework deferred the local echo to a later
// microtask (it awaited the IDB write / an awaited count() for the serial).
// data_dealer uses setUpdateListener as its sole setState site with no
// optimistic apply, so a deferred echo made an awaited action handler observe
// stale state — 6 e2e tests failed deterministically. The echo must be
// synchronous; persistence happens in the background.
describe("synchronous local echo", () => {
  it("invokes the listener on the same tick — before a pre-queued microtask and before the IDB write", async () => {
    const w = loadStub();
    const order = [];
    await w.setUpdateListener(() => order.push("echo"), 0);

    // Queued *before* sendUpdate: a synchronous echo runs before it; a
    // deferred (await-ed) echo would run after it.
    Promise.resolve().then(() => order.push("microtask"));

    let writeSettled = false;
    const write = w.sendUpdate({ payload: { v: 1 } }).then(() => {
      writeSettled = true;
    });
    order.push("after-call");

    // Echo already happened, synchronously, on this tick:
    expect(order).toEqual(["echo", "after-call"]);
    // ...and the background IndexedDB write has not resolved yet:
    expect(writeSettled).toBe(false);

    await write;
    expect(order).toEqual(["echo", "after-call", "microtask"]);
    expect(writeSettled).toBe(true);

    // Durability: awaiting the returned promise guarantees the write
    // committed, so a reload replays it.
    const w2 = loadStub();
    const got = [];
    await w2.setUpdateListener((u) => got.push(u), 0);
    expect(got.map((u) => u.serial)).toEqual([1]);
    expect(got.map((u) => u.payload)).toEqual([{ v: 1 }]);
  });

  it("an awaited sendUpdate then state read observes the update (the exact downstream failure)", async () => {
    const w = loadStub();
    const state = [];
    // Mirrors data_dealer: setUpdateListener is the only place state is set;
    // no optimistic local apply.
    await w.setUpdateListener((u) => state.push(u.payload), 0);

    await w.sendUpdate({ payload: "a" });
    expect(state).toEqual(["a"]);
    await w.sendUpdate({ payload: "b" });
    expect(state).toEqual(["a", "b"]);
  });

  it("delivers serial and max_serial synchronously in the echoed update", async () => {
    const w = loadStub();
    let last = null;
    await w.setUpdateListener((u) => {
      last = u;
    }, 0);

    w.sendUpdate({ payload: "x" });
    expect(last.serial).toBe(1);
    expect(last.max_serial).toBe(1);
    expect(last._sender).toBe(w.selfAddr);

    w.sendUpdate({ payload: "y" });
    expect(last.serial).toBe(2);
  });
});
