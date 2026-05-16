// This file originates from
// https://github.com/webxdc/vite-plugins/blob/main/src/webxdc.js
// It's a stub `webxdc.js` that adds a webxdc API stub for easy testing in
// browsers. In an actual webxdc environment (e.g. Delta Chat messenger) this
// file is not used and will automatically be replaced with a real one.
// See https://docs.webxdc.org/spec.html#webxdc-api
//
// The update log is kept in IndexedDB (one record per update, keyed by
// `serial`) so long dev sessions are not bounded by the ~5 MB localStorage
// quota. Cross-window sync uses a BroadcastChannel because IndexedDB has no
// cross-document change event, with a localStorage fallback when either is
// unavailable.

// @ts-check
/** @typedef {import('@webxdc/types/global')} */

/** @type {import('@webxdc/types').Webxdc<any>} */
window.webxdc = (() => {
  function h(tag, attributes, ...children) {
    const element = document.createElement(tag);
    if (attributes) {
      Object.entries(attributes).forEach((entry) => {
        element.setAttribute(entry[0], entry[1]);
      });
    }
    element.append(...children);
    return element;
  }

  let appIcon = undefined;
  async function getIcon() {
    if (appIcon) {
      return appIcon;
    }
    const img = new Image();
    try {
      img.src = "icon.png";
      await img.decode();
      appIcon = "icon.png";
    } catch (e) {
      img.src = "icon.jpg";
      try {
        await img.decode();
        appIcon = "icon.jpg";
      } catch (e) {}
    }
    return appIcon;
  }
  getIcon();

  // Legacy localStorage keys (migrated away from / cleaned up on init).
  const updatesKey = "__xdcUpdatesKey__";
  const ephemeralUpdateKey = "__xdcEphemeralUpdateKey__";

  // IndexedDB layout.
  const DB_NAME = "__xdcSimulatorDB__";
  const STORE_NAME = "updates";
  const DB_VERSION = 1;

  // Cross-window signaling.
  const CHANNEL_NAME = "__xdcSimulatorChannel__";
  // Used only when BroadcastChannel is unavailable: writing this key triggers
  // a cross-document `storage` event we use purely as a notification ping.
  const SIGNAL_KEY = "__xdcSimulatorSignal__";

  // Cross-window message types.
  const MSG_UPDATE = "update";
  const MSG_RESET = "reset";
  const MSG_EPHEMERAL = "ephemeral";

  /** @type {BroadcastChannel | null} */
  let channel = null;
  try {
    if (typeof BroadcastChannel !== "undefined") {
      channel = new BroadcastChannel(CHANNEL_NAME);
    }
  } catch (e) {
    channel = null;
  }

  /**
   * @param {{ type: string, [k: string]: any }} message
   */
  function postToPeers(message) {
    if (channel) {
      channel.postMessage(message);
      return;
    }
    try {
      window.localStorage.setItem(
        SIGNAL_KEY,
        // The nonce guarantees the value changes so the `storage` event fires.
        JSON.stringify(
          Object.assign({}, message, { _n: Date.now() + Math.random() }),
        ),
      );
    } catch (e) {}
  }

  /**
   * @typedef {import('@webxdc/types').RealtimeListener} RT
   * @type {RT}
   */
  class RealtimeListener {
    constructor() {
      /** @private */
      this.listener = null;
      /** @private */
      this.trashed = false;
    }

    is_trashed() {
      return this.trashed;
    }

    receive(data) {
      if (this.trashed) {
        throw new Error(
          "realtime listener is trashed and can no longer be used",
        );
      }
      if (this.listener) {
        this.listener(data);
      }
    }

    setListener(listener) {
      this.listener = listener;
    }

    send(data) {
      if (!(data instanceof Uint8Array)) {
        throw new Error("realtime listener data must be a Uint8Array");
      }
      // Ephemeral / realtime is transient and carries no persisted state.
      postToPeers({
        type: MSG_EPHEMERAL,
        sender: window.webxdc.selfAddr,
        data: Array.from(data),
      });
    }

    leave() {
      this.trashed = true;
    }
  }

  let updateListener = (_) => {};
  let listenerSet = false;
  // Highest serial already handed to the real listener; used to dedupe so a
  // cross-window broadcast is never delivered twice (once via replay from the
  // shared store, once via the BroadcastChannel notification).
  let lastDeliveredSerial = 0;
  // Peer updates that arrived before `setUpdateListener` finished its replay.
  /** @type {any[]} */
  let pendingPeerUpdates = [];
  /**
   * @type {RT | null}
   */
  let realtimeListener = null;

  // ---------------------------------------------------------------------------
  // Storage layer: IndexedDB (preferred) with a localStorage fallback.
  // ---------------------------------------------------------------------------

  /** @type {IDBDatabase | null} */
  let db = null;
  let useIdb = true;

  /** @returns {Promise<IDBDatabase>} */
  function openDatabase() {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open(DB_NAME, DB_VERSION);
      request.onupgradeneeded = () => {
        const database = request.result;
        if (!database.objectStoreNames.contains(STORE_NAME)) {
          database.createObjectStore(STORE_NAME, { keyPath: "serial" });
        }
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
      request.onblocked = () => reject(new Error("IndexedDB open blocked"));
    });
  }

  /** @returns {Promise<number>} */
  function idbCount() {
    return new Promise((resolve, reject) => {
      // @ts-ignore: db is non-null when useIdb is true
      const tx = db.transaction(STORE_NAME, "readonly");
      const req = tx.objectStore(STORE_NAME).count();
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  /** @returns {Promise<any[]>} */
  function idbGetAll() {
    return new Promise((resolve, reject) => {
      // @ts-ignore: db is non-null when useIdb is true
      const tx = db.transaction(STORE_NAME, "readonly");
      const req = tx.objectStore(STORE_NAME).getAll();
      // getAll() yields records in ascending key (serial) order.
      req.onsuccess = () => resolve(req.result || []);
      req.onerror = () => reject(req.error);
    });
  }

  /**
   * Assign a sequential serial and persist the record atomically. `count` and
   * `put` share one readwrite transaction; IndexedDB serializes overlapping
   * readwrite transactions (within and across windows), so serials cannot
   * collide under concurrent `sendUpdate` calls.
   *
   * @param {any} record
   * @returns {Promise<number>}
   */
  function idbAppend(record) {
    return new Promise((resolve, reject) => {
      // @ts-ignore: db is non-null when useIdb is true
      const tx = db.transaction(STORE_NAME, "readwrite");
      const store = tx.objectStore(STORE_NAME);
      const countReq = store.count();
      countReq.onsuccess = () => {
        record.serial = countReq.result + 1;
        store.put(record);
      };
      tx.oncomplete = () => resolve(record.serial);
      tx.onerror = () => reject(tx.error);
      tx.onabort = () => reject(tx.error);
    });
  }

  function deleteDatabase() {
    return new Promise((resolve) => {
      try {
        if (db) {
          db.close();
          db = null;
        }
      } catch (e) {}
      let settled = false;
      const done = () => {
        if (!settled) {
          settled = true;
          resolve(undefined);
        }
      };
      try {
        const request = indexedDB.deleteDatabase(DB_NAME);
        request.onsuccess = done;
        request.onerror = done;
        // If another window still holds a connection the delete is blocked;
        // those windows reload on the `reset` broadcast and release it. Don't
        // hang the Reset button regardless.
        request.onblocked = () => setTimeout(done, 500);
      } catch (e) {
        done();
      }
      setTimeout(done, 1500);
    });
  }

  // localStorage fallback (same observable behavior as the original stub,
  // including its quota limitation — used only when IndexedDB is unavailable).
  function lsGetAll() {
    try {
      const json = window.localStorage.getItem(updatesKey);
      const arr = json ? JSON.parse(json) : [];
      return Array.isArray(arr) ? arr : [];
    } catch (e) {
      return [];
    }
  }
  /** @param {any} record */
  function lsAppend(record) {
    const arr = lsGetAll();
    record.serial = arr.length + 1;
    arr.push(record);
    window.localStorage.setItem(updatesKey, JSON.stringify(arr));
    return record.serial;
  }

  /** @returns {Promise<any[]>} */
  function storeGetAll() {
    return useIdb ? idbGetAll() : Promise.resolve(lsGetAll());
  }
  /**
   * @param {any} record
   * @returns {Promise<number>}
   */
  function storeAppend(record) {
    return useIdb ? idbAppend(record) : Promise.resolve(lsAppend(record));
  }

  /**
   * One-time, idempotent, crash-safe migration of legacy localStorage data.
   * The localStorage key is removed only after the IndexedDB write transaction
   * has committed, so an interrupted migration safely re-runs.
   */
  async function migrateIfNeeded() {
    if (!useIdb) {
      return;
    }
    const count = await idbCount();
    if (count > 0) {
      // IndexedDB already has data: no-op (do not touch legacy localStorage).
      return;
    }
    const legacy = window.localStorage.getItem(updatesKey);
    if (!legacy) {
      // Nothing to migrate; still drop any stale ephemeral signaling key
      // (it carries no persisted state).
      window.localStorage.removeItem(ephemeralUpdateKey);
      return;
    }
    /** @type {any} */
    let parsed = null;
    try {
      parsed = JSON.parse(legacy);
    } catch (e) {
      parsed = null;
    }
    if (!Array.isArray(parsed) || parsed.length === 0) {
      window.localStorage.removeItem(updatesKey);
      window.localStorage.removeItem(ephemeralUpdateKey);
      return;
    }
    await new Promise((resolve, reject) => {
      // @ts-ignore: db is non-null when useIdb is true
      const tx = db.transaction(STORE_NAME, "readwrite");
      const store = tx.objectStore(STORE_NAME);
      parsed.forEach((update, index) => {
        if (update && typeof update.serial !== "number") {
          update.serial = index + 1;
        }
        store.put(update);
      });
      tx.oncomplete = () => resolve(undefined);
      tx.onerror = () => reject(tx.error);
      tx.onabort = () => reject(tx.error);
    });
    // Only now that the import transaction has committed:
    window.localStorage.removeItem(updatesKey);
    window.localStorage.removeItem(ephemeralUpdateKey);
  }

  // Resolves once the storage backend is ready (DB open + migration done).
  const ready = (async () => {
    if (typeof indexedDB === "undefined" || !indexedDB) {
      useIdb = false;
      console.log(
        "[Webxdc] WARNING: IndexedDB unavailable, falling back to localStorage.",
      );
      return;
    }
    try {
      db = await openDatabase();
      await migrateIfNeeded();
    } catch (e) {
      useIdb = false;
      db = null;
      console.log(
        "[Webxdc] WARNING: IndexedDB unavailable, falling back to localStorage: " +
          e,
      );
    }
  })();

  // Serializes persistence operations so serial assignment + append is atomic
  // within this window and the local listener sees updates in serial order.
  /** @type {Promise<any>} */
  let opQueue = Promise.resolve();
  /**
   * @template T
   * @param {() => Promise<T>} task
   * @returns {Promise<T>}
   */
  function enqueue(task) {
    const result = opQueue.then(task, task);
    opQueue = result.then(
      () => {},
      () => {},
    );
    return result;
  }

  /** @param {any} update */
  function deliverPeerUpdate(update) {
    if (!listenerSet) {
      // Buffer until setUpdateListener has replayed from the shared store.
      pendingPeerUpdates.push(update);
      return;
    }
    if (
      typeof update.serial === "number" &&
      update.serial <= lastDeliveredSerial
    ) {
      return;
    }
    if (typeof update.serial === "number") {
      lastDeliveredSerial = update.serial;
    }
    updateListener(update);
  }

  /** @param {{ type: string, [k: string]: any }} message */
  function handlePeerMessage(message) {
    if (!message) {
      return;
    }
    if (message.type === MSG_RESET) {
      window.location.reload();
      return;
    }
    if (message.type === MSG_UPDATE) {
      const update = message.update;
      console.log("[Webxdc] " + JSON.stringify(update));
      if (update.notify && update._sender !== window.webxdc.selfAddr) {
        if (update.notify[window.webxdc.selfAddr]) {
          sendNotification(update.notify[window.webxdc.selfAddr]);
        } else if (update.notify["*"]) {
          sendNotification(update.notify["*"]);
        }
      }
      deliverPeerUpdate(update);
      return;
    }
    if (message.type === MSG_EPHEMERAL) {
      // @ts-ignore: is_trashed() is private
      if (
        window.webxdc.selfAddr !== message.sender &&
        realtimeListener &&
        // @ts-ignore: is_trashed() is private
        !realtimeListener.is_trashed()
      ) {
        // @ts-ignore: receive() is private
        realtimeListener.receive(Uint8Array.from(message.data));
      }
      return;
    }
  }

  if (channel) {
    channel.onmessage = (event) => handlePeerMessage(event.data);
  }
  window.addEventListener("storage", (event) => {
    if (event.key == null) {
      // Another window cleared localStorage (e.g. legacy Reset).
      window.location.reload();
      return;
    }
    if (!channel && event.key === SIGNAL_KEY && event.newValue) {
      try {
        handlePeerMessage(JSON.parse(event.newValue));
      } catch (e) {}
    }
  });

  async function sendNotification(text) {
    console.log("[NOTIFICATION] " + text);

    const opts = { body: text, icon: await getIcon() };
    const title = "To: " + window.webxdc.selfName;
    if (Notification.permission === "granted") {
      new Notification(title, opts);
    } else {
      Notification.requestPermission((permission) => {
        if (Notification.permission === "granted") {
          new Notification(title, opts);
        }
      });
    }
  }

  function addXdcPeer() {
    const loc = window.location;
    // get next peer ID
    const params = new URLSearchParams(loc.hash.substr(1));
    const peerId = Number(params.get("next_peer")) || 1;

    // open a new window
    const peerName = "device" + peerId;
    const url =
      loc.protocol +
      "//" +
      loc.host +
      loc.pathname +
      "#name=" +
      peerName +
      "&addr=" +
      peerName +
      "@local.host";
    window.open(url);

    // update next peer ID
    params.set("next_peer", String(peerId + 1));
    window.location.hash = "#" + params.toString();
  }

  window.addEventListener("load", async () => {
    const styleControlPanel =
      "position: fixed; bottom:1em; left:1em; background-color: #000; opacity:0.8; padding:.5em; font-size:16px; font-family: sans-serif; color:#fff; z-index: 9999";
    const styleMenuLink =
      "color:#fff; text-decoration: none; vertical-align: middle";
    const styleAppIcon =
      "height: 1.5em; width: 1.5em; margin-right: 0.5em; border-radius:10%; vertical-align: middle";
    let title = document.getElementsByTagName("title")[0];
    if (typeof title == "undefined") {
      title = h("title");
      document.getElementsByTagName("head")[0].append(title);
    }
    title.innerText = window.webxdc.selfAddr;

    if (window.webxdc.selfName === "device0") {
      const addPeerBtn = h(
        "a",
        { href: "javascript:void(0);", style: styleMenuLink },
        "Add Peer",
      );
      addPeerBtn.onclick = () => addXdcPeer();
      const resetBtn = h(
        "a",
        { href: "javascript:void(0);", style: styleMenuLink },
        "Reset",
      );
      resetBtn.onclick = async () => {
        postToPeers({ type: MSG_RESET });
        try {
          await ready;
        } catch (e) {}
        await deleteDatabase();
        try {
          window.localStorage.clear();
        } catch (e) {}
        window.location.reload();
      };
      const controlPanel = h(
        "div",
        { style: styleControlPanel },
        h(
          "header",
          { style: "margin-bottom: 0.5em; font-size:12px;" },
          "webxdc dev tools",
        ),
        addPeerBtn,
        h("span", { style: styleMenuLink }, " | "),
        resetBtn,
      );

      const icon = await getIcon();
      if (icon) {
        controlPanel.insertBefore(
          h("img", { src: icon, style: styleAppIcon }),
          controlPanel.childNodes[1],
        );
        document.head.append(h("link", { rel: "icon", href: icon }));
      }

      document.getElementsByTagName("body")[0].append(controlPanel);
    }
  });

  const params = new URLSearchParams(window.location.hash.substr(1));
  return {
    sendUpdateInterval: 1000,
    sendUpdateMaxSize: 999999,
    selfAddr: params.get("addr") || "device0@local.host",
    selfName: params.get("name") || "device0",
    setUpdateListener: (cb, serial = 0) =>
      enqueue(async () => {
        await ready;
        const updates = await storeGetAll();
        const maxSerial = updates.length;
        updates.forEach((update) => {
          if (update.serial > serial) {
            update.max_serial = maxSerial;
            cb(update);
          }
        });
        updateListener = cb;
        listenerSet = true;
        lastDeliveredSerial = Math.max(serial, maxSerial);
        // Flush peer updates that arrived during the async replay above;
        // deliverPeerUpdate applies the same serial-based dedupe.
        const buffered = pendingPeerUpdates;
        pendingPeerUpdates = [];
        buffered.forEach(deliverPeerUpdate);
      }),
    joinRealtimeChannel: (cb) => {
      // @ts-ignore: is_trashed() is private
      if (realtimeListener && realtimeListener.is_trashed()) {
        return;
      }
      const rt = new RealtimeListener();
      // mimic connection establishment time
      setTimeout(() => (realtimeListener = rt), 500);
      return rt;
    },
    getAllUpdates: () => {
      console.log("[Webxdc] WARNING: getAllUpdates() is deprecated.");
      return Promise.resolve([]);
    },
    sendUpdate: (update) =>
      enqueue(async () => {
        await ready;
        /** @type {any} */
        const payload = {
          payload: update.payload,
          summary: update.summary,
          info: update.info,
          notify: update.notify,
          href: update.href,
          document: update.document,
        };
        /** @type {any} */
        const _update = Object.assign({}, payload, {
          _sender: window.webxdc.selfAddr,
        });
        const serial = await storeAppend(_update);
        console.log(
          `[Webxdc] ${JSON.stringify(Object.assign({}, payload, { serial }))}`,
        );
        _update.max_serial = serial;
        lastDeliveredSerial = Math.max(lastDeliveredSerial, serial);
        updateListener(_update);
        postToPeers({ type: MSG_UPDATE, update: _update });
      }),
    sendToChat: async (content) => {
      if (!content.file && !content.text) {
        alert("🚨 Error: either file or text need to be set. (or both)");
        return Promise.reject(
          "Error from sendToChat: either file or text need to be set",
        );
      }

      /** @type {(file: Blob) => Promise<string>} */
      const blob_to_base64 = (file) => {
        const data_start = ";base64,";
        return new Promise((resolve, reject) => {
          const reader = new FileReader();
          reader.readAsDataURL(file);
          reader.onload = () => {
            /** @type {string} */
            //@ts-ignore
            let data = reader.result;
            resolve(data.slice(data.indexOf(data_start) + data_start.length));
          };
          reader.onerror = () => reject(reader.error);
        });
      };

      let base64Content;
      if (content.file) {
        if (!content.file.name) {
          return Promise.reject("file name is missing");
        }
        if (
          Object.keys(content.file).filter((key) =>
            ["blob", "base64", "plainText"].includes(key),
          ).length > 1
        ) {
          return Promise.reject(
            "you can only set one of `blob`, `base64` or `plainText`, not multiple ones",
          );
        }

        // @ts-ignore - needed because typescript imagines that blob would not exist
        if (content.file.blob instanceof Blob) {
          // @ts-ignore - needed because typescript imagines that blob would not exist
          base64Content = await blob_to_base64(content.file.blob);
          // @ts-ignore - needed because typescript imagines that base64 would not exist
        } else if (typeof content.file.base64 === "string") {
          // @ts-ignore - needed because typescript imagines that base64 would not exist
          base64Content = content.file.base64;
          // @ts-ignore - needed because typescript imagines that plainText would not exist
        } else if (typeof content.file.plainText === "string") {
          base64Content = await blob_to_base64(
            // @ts-ignore - needed because typescript imagines that plainText would not exist
            new Blob([content.file.plainText]),
          );
        } else {
          return Promise.reject(
            "data is not set or wrong format, set one of `blob`, `base64` or `plainText`, see webxdc documentation for sendToChat",
          );
        }
      }
      const msg = `The app would now close and the user would select a chat to send this message:\nText: ${
        content.text ? `"${content.text}"` : "No Text"
      }\nFile: ${
        content.file
          ? `${content.file.name} - ${base64Content.length} bytes`
          : "No File"
      }`;
      if (content.file) {
        const confirmed = confirm(
          msg + "\n\nDownload the file in the browser instead?",
        );
        if (confirmed) {
          const dataURL =
            "data:application/octet-stream;base64," + base64Content;
          const element = h("a", {
            href: dataURL,
            download: content.file.name,
          });
          document.body.appendChild(element);
          element.click();
          document.body.removeChild(element);
        }
      } else {
        alert(msg);
      }
    },
    importFiles: (filters) => {
      const accept = [
        ...(filters.extensions || []),
        ...(filters.mimeTypes || []),
      ].join(",");
      const element = h("input", {
        type: "file",
        accept,
        multiple: filters.multiple || false,
      });
      const promise = new Promise((resolve, _reject) => {
        element.onchange = (_ev) => {
          console.log("element.files", element.files);
          const files = Array.from(element.files || []);
          document.body.removeChild(element);
          resolve(files);
        };
      });
      element.style.display = "none";
      document.body.appendChild(element);
      element.click();
      console.log(element);
      return promise;
    },
  };
})();
