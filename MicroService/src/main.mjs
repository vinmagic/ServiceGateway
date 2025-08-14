// src/main.mjs
// Auto-pump microservice (continuous pull after handler finishes)
// - onStream("<id>") => reads from "events_<id>"; "default" => "events"
// - Auto-starts and keeps pulling: after each wave drains, pulls the next batch
// - If a batch is EMPTY, schedules a single "idle nudge" and retries after idleNudgeMs
// - Bounded concurrency within a wave; offsets saved after each batch
// - Per-stream overrides: initialBackfillMs, resetOffset, mapDoc, idleNudgeMs
// - Graceful shutdown: drains handlers, persists offsets

import fs from "fs";
import path from "path";
import yaml from "js-yaml";
import pino from "pino";
import { MongoClient } from "mongodb";
import { v4 as uuidv4 } from "uuid";
import { ConnectStore } from "./db.mjs";
import got from "got";
import http from "http";
import https from "https";

/* -------------------- Config + logger -------------------- */
function loadConfig(cfgPath = "./config.yml") {
  const abs = path.resolve(process.cwd(), cfgPath);
  if (!fs.existsSync(abs)) throw new Error(`Missing config.yml at ${abs}`);
  return yaml.load(fs.readFileSync(abs, "utf8"));
}

function createLogger(appName, serviceName, containerName) {
  return pino({
    level: process.env.LOG_LEVEL || "info",
    base: { appName, serviceName, containerName },
    timestamp: () => `,"time":"${new Date().toISOString()}"`,
  });
}

/* -------------------- Helpers -------------------- */
function collFor(streamId, defaultColl) {
  return streamId === "default" ? defaultColl : `events_${streamId}`;
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/* -------------------- CreateService -------------------- */
export async function CreateService({
  appName = "microservice",
  serviceName = "stream_consumer",
  containerName = `container.${uuidv4()}`,
  configPath = "./config.yml",
} = {}) {
  const config = loadConfig(configPath);
  if (!config?.mongo?.uri) throw new Error("mongo.uri is required");

  const logger = createLogger(appName, serviceName, containerName);
  logger.info("service_ready");

  // DB connect
  const client = new MongoClient(config.mongo.uri, {
    maxPoolSize: Number(config.mongo?.maxPoolSize ?? 50),
    readPreference: "primary",
  });
  await client.connect();
  const db = client.db(config.mongo.db);

  // Tunables (global defaults)
  const timeField = config.mongo.timeSeries?.timeField || "receivedAt";
  const BATCH_LIMIT = Number(config.mongo.timeSeries?.batchLimit ?? 2000);
  const FIND_MAX_TIME_MS = Number(
    config.mongo.timeSeries?.findMaxTimeMs ?? 5000
  );
  const OFFSETS_COLL =
    config.mongo.timeSeries?.offsetsCollection || "_consumer_offsets";
  const MAX_BACKFILL_MS = 7 * 24 * 3600 * 1000;
  const DEFAULT_INITIAL_BACKFILL_MS = Math.min(
    Number(config.mongo.timeSeries?.initialBackfillMs ?? 10_000),
    MAX_BACKFILL_MS
  );
  const GUARD_LAG_MS = Number(config.mongo.timeSeries?.guardLagMs ?? 0);
  const defaultEventsCollection = config.mongo.eventsCollection || "events";
  const SHUTDOWN_TIMEOUT_MS = Number(
    config.runtime?.shutdownTimeoutMs ?? 30_000
  );

  // Indices (best-effort)
  try {
    await db.collection(OFFSETS_COLL).createIndex({ ts: 1 });
  } catch (e) {
    logger.warn({ err: e }, "offsets_index_create_warning");
  }
  async function ensureEventIndex(coll) {
    try {
      await db.collection(coll).createIndex({ [timeField]: 1, _id: 1 });
    } catch (e) {
      logger.warn({ err: e, coll }, "events_index_create_warning");
    }
  }

  // State
  const pollState = new Map(); // coll -> { since: Date, lastId: ObjectId|null }
  const processors = new Map(); // streamId -> { fn, concurrency, maxQueue, queue:Set<Promise>, mapDoc, pump, pumping, idleNudgeMs, idleTimer, batchSize }
  const perCollOverrides = new Map(); // coll -> { initialBackfillMs?: number, resetOffset?: boolean }

  async function loadOffset(coll, { initialBackfillMs, resetOffset } = {}) {
    if (!resetOffset) {
      const row = await db
        .collection(OFFSETS_COLL)
        .findOne({ _id: `ts_tail:${coll}` });
      if (row?.since) {
        return {
          since: new Date(row.since),
          lastId: row._lastId || null,
          used: "stored",
        };
      }
    }
    const ms = Math.min(
      Number(initialBackfillMs ?? DEFAULT_INITIAL_BACKFILL_MS),
      MAX_BACKFILL_MS
    );
    return {
      since: new Date(Date.now() - ms),
      lastId: null,
      used: "override_or_default",
    };
  }

  async function saveOffset(coll, since, lastId) {
    await db
      .collection(OFFSETS_COLL)
      .updateOne(
        { _id: `ts_tail:${coll}` },
        { $set: { since, _lastId: lastId, ts: new Date() } },
        { upsert: true }
      );
  }

  // Deliver a doc to a stream handler with backpressure + optional mapping
  async function routeDocTo(streamId, doc, srcTag) {
    const proc = processors.get(streamId) || processors.get("default");
    if (!proc) return;

    const { fn, concurrency, maxQueue, queue, mapDoc } = proc;

    // Backpressure
    if (queue.size >= maxQueue) {
      await Promise.race(queue);
    }

    const p = (async () => {
      try {
        const child = logger.child({ streamId, src: srcTag });
        const finalDoc = mapDoc ? await mapDoc(doc) : doc;

        // Handler context (no next/defer needed in auto-pump)
        await fn(finalDoc, { logger: child, streamId, config });
      } catch (err) {
        logger.error({ err, streamId }, "stream_handler_error");
      } finally {
        queue.delete(p);
      }
    })();

    queue.add(p);
    if (queue.size >= concurrency) {
      await Promise.race(queue); // bounded concurrency
    }
  }

  const badTimeWarned = new Set(); // coll

  // Fetch + dispatch one batch; wait for wave drain; return docs length
  // Fetch + dispatch one batch via a streaming cursor; wait for wave drain; return docs length
  async function fetchWave(streamId, batchSize) {
    const coll = collFor(streamId, defaultEventsCollection);
    const state = pollState.get(coll);
    if (!state) return 0;

    // Build monotonic query (timeField, then _id)
    const query = state.lastId
      ? {
          $or: [
            { [timeField]: { $gt: state.since } },
            { [timeField]: state.since, _id: { $gt: state.lastId } },
          ],
        }
      : { [timeField]: { $gt: state.since } };

    const limit = Math.max(1, Math.floor(batchSize ?? BATCH_LIMIT));

    // Stream the batch instead of materializing it
    const cursor = db
      .collection(coll)
      .find(query, { noCursorTimeout: false })
      .sort({ [timeField]: 1, _id: 1 })
      .limit(limit)
      .maxTimeMS(FIND_MAX_TIME_MS);

    let count = 0;
    const proc = processors.get(streamId);

    try {
      while (await cursor.hasNext()) {
        const d = await cursor.next();
        count += 1;

        const dt = d[timeField];
        if (dt instanceof Date) {
          // routeDocTo already enforces backpressure using proc.queue/concurrency
          await routeDocTo(streamId, d, "autopump");

          if (dt > state.since) {
            state.since = new Date(dt.getTime() - GUARD_LAG_MS);
          }
        } else if (!badTimeWarned.has(coll)) {
          logger.warn(
            { coll, badDocId: d?._id, timeField },
            "doc_missing_or_non_date_timefield"
          );
          badTimeWarned.add(coll);
        }

        state.lastId = d._id;

        // Trickle-save offsets to improve crash resilience on big waves
        if (count % 100 === 0) {
          await saveOffset(coll, state.since, state.lastId);
          // Yield occasionally to keep the event loop snappy
          await new Promise((r) => setImmediate(r));
        }
      }
    } catch (err) {
      logger.error({ err, streamId }, "fetch_wave_cursor_error");
      // Re-throw so the pump schedules a retry via its catch block
      throw err;
    } finally {
      try {
        await cursor.close();
      } catch {}
    }

    if (count > 0) {
      // Persist final offset for this wave
      await saveOffset(coll, state.since, state.lastId);

      // Wait for the current wave to drain (bounded by concurrency)
      if (proc?.queue?.size > 0) {
        await Promise.allSettled([...proc.queue]);
      }
    }

    return count;
  }
  /**
   * Register a stream handler in auto-pump mode.
   * It will keep pulling batches automatically as long as it finds docs;
   * on empty batch, it schedules a single idle retry after idleNudgeMs.
   *
   * Returns: { pause, resume, unsubscribe, setBatchSize }
   */
  function onStream(
    streamId,
    handler,
    {
      concurrency = 1,
      maxQueue = 2048,
      initialBackfillMs,
      resetOffset = false,
      mapDoc = null,
      idleNudgeMs = 1000, // retry delay when a batch is empty
      batchSize = BATCH_LIMIT,
    } = {}
  ) {
    const coll = collFor(streamId, defaultEventsCollection);

    if (initialBackfillMs != null || resetOffset) {
      perCollOverrides.set(coll, { initialBackfillMs, resetOffset });
    }

    // Processor
    const proc = {
      fn: handler,
      concurrency,
      maxQueue,
      queue: new Set(),
      mapDoc,
      pump: null, // function
      pumping: false, // true while the pump loop is active
      idleNudgeMs,
      idleTimer: null,
      batchSize,
      paused: false,
    };
    processors.set(streamId, proc);

    (async () => {
      const initial = await loadOffset(coll, perCollOverrides.get(coll) || {});
      pollState.set(coll, { since: initial.since, lastId: initial.lastId });
      await ensureEventIndex(coll);

      // The pump loop: fetch wave -> if >0, loop; if 0, schedule idle nudge
      proc.pump = async function pump() {
        if (proc.pumping || proc.paused) return;
        proc.pumping = true;
        try {
          while (!proc.paused) {
            // Cancel any pending idle nudge since we're actively pumping
            if (proc.idleTimer) {
              clearTimeout(proc.idleTimer);
              proc.idleTimer = null;
            }

            const pulled = await fetchWave(streamId, proc.batchSize);
            if (pulled === 0) {
              // Schedule a one-shot idle retry and stop pumping for now
              if (proc.idleNudgeMs > 0 && !proc.paused) {
                proc.idleTimer = setTimeout(() => {
                  proc.idleTimer = null;
                  // fire-and-forget: will restart pump if not paused
                  setImmediate(() => proc.pump());
                }, proc.idleNudgeMs);
                proc.idleTimer.unref?.();
              }
              break; // exit pump loop; will be revived by idle nudge
            }
            // else: pulled > 0 -> continue loop immediately
          }
        } catch (err) {
          logger.error({ err, streamId }, "pump_error");
          // Be resilient: schedule an idle nudge after error
          if (!proc.paused) {
            proc.idleTimer = setTimeout(() => {
              proc.idleTimer = null;
              setImmediate(() => proc.pump());
            }, Math.max(proc.idleNudgeMs, 1000));
            proc.idleTimer?.unref?.();
          }
        } finally {
          proc.pumping = false;
        }
      };

      // Auto-start: kick pump once
      setImmediate(() => proc.pump());

      logger.info(
        {
          coll,
          streamId,
          timeField,
          initialSinceIso: initial.since?.toISOString?.(),
          initialSource: initial.used,
          guardLagMs: GUARD_LAG_MS,
          batchLimit: BATCH_LIMIT,
          idleNudgeMs: proc.idleNudgeMs,
          batchSize: proc.batchSize,
        },
        "autopump_stream_ready"
      );
    })().catch((e) =>
      logger.error({ err: e, streamId }, "onStream_setup_failed")
    );

    // External control surface
    const external = {
      pause: () => {
        proc.paused = true;
        if (proc.idleTimer) {
          clearTimeout(proc.idleTimer);
          proc.idleTimer = null;
        }
      },
      resume: () => {
        if (!proc.paused) return;
        proc.paused = false;
        setImmediate(() => proc.pump());
      },
      setBatchSize: (n) => {
        if (Number.isFinite(n) && n > 0) proc.batchSize = Math.floor(n);
      },
      unsubscribe: async () => {
        external.pause();
        processors.delete(streamId);
        const st = pollState.get(coll);
        if (st) await saveOffset(coll, st.since, st.lastId);
        if (proc.queue.size > 0) await Promise.allSettled([...proc.queue]);
      },
    };

    return external;
  }

  async function shutdown() {
    const shutdownStart = Date.now();
    try {
      // Pause all, cancel idle nudges
      for (const proc of processors.values()) {
        proc.paused = true;
        if (proc.idleTimer) clearTimeout(proc.idleTimer);
      }

      // Drain all processors with optional timeout
      const waits = [];
      for (const { queue } of processors.values())
        waits.push(Promise.allSettled([...queue]));
      const all = Promise.allSettled(waits);

      if (SHUTDOWN_TIMEOUT_MS > 0) {
        await Promise.race([
          all,
          (async () => {
            await sleep(SHUTDOWN_TIMEOUT_MS);
            throw new Error("shutdown_timeout");
          })(),
        ]);
      } else {
        await all;
      }

      // Persist final offsets
      for (const [coll, st] of pollState.entries()) {
        if (st) await saveOffset(coll, st.since, st.lastId);
      }
      await client.close();
    } catch (e) {
      logger.error(
        { err: e, elapsedMs: Date.now() - shutdownStart },
        "service_shutdown_error"
      );
    } finally {
      logger.info(
        { elapsedMs: Date.now() - shutdownStart },
        "service_shutdown_done"
      );
    }
  }

  /* ---------- HTTP client registry (got) ---------- */
  const createGotInstance = (baseURL, cookieJar = null, headers = null) => {
    const httpAgent = new http.Agent({ keepAlive: true });
    const httpsAgent = new https.Agent({ keepAlive: true });

    const options = {
      prefixUrl: baseURL,
      agent: { http: httpAgent, https: httpsAgent },
    };
    if (cookieJar) options.cookieJar = cookieJar;
    if (headers) options.headers = headers;

    return got.extend(options);
  };

  const services = new Map();
  function getService(serviceUrl) {
    if (!services.has(serviceUrl)) {
      services.set(serviceUrl, createGotInstance(serviceUrl));
    }
    return services.get(serviceUrl);
  }

  /* ---------- DB store registry ---------- */
  const stores = new Map();
  function getStore(dbUrl) {
    if (!stores.has(dbUrl)) {
      stores.set(dbUrl, ConnectStore(dbUrl));
    }
    return stores.get(dbUrl);
  }

  process.on("SIGINT", () => shutdown().finally(() => process.exit(0)));
  process.on("SIGTERM", () => shutdown().finally(() => process.exit(0)));

  return {
    onStream,
    shutdown,
    logger,
    config,
    getStore,
    getService,
  };
}

/* -------------------- Example usage (commented) --------------------
// const service = await CreateService();
// const ctrl = service.onStream(
//   "cargogowhere",
//   async (doc, { logger }) => {
//     logger.info({ id: doc?._id }, "process_doc");
//     // ... your work ...
//     // Nothing else needed — it will auto-fetch next wave after this wave finishes.
//   },
//   {
//     initialBackfillMs: 24 * 60 * 60 * 1000, // 24h
//     resetOffset: true,
//     mapDoc: (doc) => ({ data: doc?.query?.data, receivedAt: doc?.receivedAt }),
//     idleNudgeMs: 1000,   // retry after 1s if a batch is empty
//     batchSize: 200,      // optional override (default=BATCH_LIMIT)
//   }
// );
// // You can temporarily pause/resume:
// // ctrl.pause(); ctrl.resume();
// // ctrl.setBatchSize(500);
// // await ctrl.unsubscribe();
*/
