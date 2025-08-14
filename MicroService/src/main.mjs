// src/main.mjs
// Poller-only microservice (auto events_<streamId>)
// - onStream("<id>") => polls collection "events_<id>"
//   onStream("default") => polls collection "events"
// - Per-collection offsets in _consumer_offsets (since + _lastId)
// - Per-stream bounded concurrency with backpressure
// - Guard lag for clock skew, exponential backoff on errors
// - Per-stream overrides: initialBackfillMs, resetOffset
// - Per-stream mapDoc: shape the doc before it reaches the handler (sync or async)
// - Graceful shutdown: drains handlers, persists offsets

import fs from "fs";
import path from "path";
import yaml from "js-yaml";
import pino from "pino";
import { MongoClient } from "mongodb";
import { v4 as uuidv4 } from "uuid";

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
  appName = "cube",
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
  const POLL_MS = Number(config.mongo.timeSeries?.pollMs ?? 1000);
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
  const MAX_LOOPS_PER_CYCLE = Number(
    config.mongo.timeSeries?.maxLoopsPerCycle ?? 10
  );
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
  const pollers = new Map(); // coll -> { running:boolean, delay:number }
  const pollState = new Map(); // coll -> { since: Date, lastId: ObjectId|null }
  // processors map stores per-stream handler + runtime settings
  // value shape: { fn, concurrency, maxQueue, queue:Set<Promise>, mapDoc?: (doc)=>any|Promise<any> }
  const processors = new Map();
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

  async function routeDocTo(streamId, doc, srcTag) {
    const proc = processors.get(streamId) || processors.get("default");
    if (!proc) {
      // No handler: skip processing safely
      logger.debug({ streamId }, "no_handler_for_stream");
      return;
    }
    const { fn, concurrency, maxQueue, queue, mapDoc } = proc;

    // Backpressure
    if (queue.size >= maxQueue) {
      logger.warn(
        { streamId, size: queue.size, maxQueue },
        "stream_queue_overflow"
      );
      await Promise.race(queue);
    }

    const p = (async () => {
      try {
        const child = logger.child({ streamId, src: srcTag });
        const finalDoc = mapDoc ? await mapDoc(doc) : doc;
        await fn(finalDoc, { logger: child, streamId, config });
      } catch (err) {
        logger.error({ err, streamId }, "stream_handler_error");
      } finally {
        queue.delete(p);
      }
    })();

    queue.add(p);
    if (queue.size >= concurrency) await Promise.race(queue);
  }

  const badTimeWarned = new Set(); // coll
  async function pollOnce(coll, streamId) {
    const state = pollState.get(coll);
    if (!state) return 0;

    // Safer tie-break query
    let query;
    if (state.lastId) {
      query = {
        $or: [
          { [timeField]: { $gt: state.since } },
          { [timeField]: state.since, _id: { $gt: state.lastId } },
        ],
      };
    } else {
      query = { [timeField]: { $gt: state.since } };
    }

    const docs = await db
      .collection(coll)
      .find(query)
      .sort({ [timeField]: 1, _id: 1 })
      .limit(BATCH_LIMIT)
      .maxTimeMS(FIND_MAX_TIME_MS)
      .toArray();

    if (!docs.length) return 0;

    for (const d of docs) {
      const dt = d[timeField];
      if (!(dt instanceof Date)) {
        if (!badTimeWarned.has(coll)) {
          logger.warn(
            { coll, badDocId: d?._id, timeField },
            "doc_missing_or_non_date_timefield"
          );
          badTimeWarned.add(coll);
        }
      } else {
        await routeDocTo(streamId, d, "poll");
        if (dt > state.since)
          state.since = new Date(dt.getTime() - GUARD_LAG_MS);
      }
      state.lastId = d._id;
    }

    await saveOffset(coll, state.since, state.lastId);
    return docs.length;
  }

  async function pollLoop(coll, streamId) {
    const ctl = pollers.get(coll);
    if (!ctl || ctl.running) return;
    ctl.running = true;

    try {
      let fetched = 0;
      let loops = 0;
      do {
        fetched = await pollOnce(coll, streamId);
        loops++;
        if (loops >= MAX_LOOPS_PER_CYCLE) {
          await sleep(50);
          loops = 0;
        }
        if (fetched >= Math.min(BATCH_LIMIT, 1000)) {
          await new Promise((r) => setImmediate(r));
        }
      } while (fetched > 0);

      ctl.delay = POLL_MS;
    } catch (err) {
      logger.error({ err, coll }, "poller_error");
      ctl.delay = Math.min((ctl.delay || POLL_MS) * 2, POLL_MS * 10);
    } finally {
      ctl.running = false;
      if (pollers.has(coll)) {
        setTimeout(() => {
          pollLoop(coll, streamId).catch(() => {});
        }, ctl.delay || POLL_MS).unref?.();
      }
    }
  }

  async function startPollerForStream(streamId) {
    const coll = collFor(streamId, defaultEventsCollection);
    if (pollers.has(coll)) return;

    // Resolve overrides (if any) for this collection
    const overrides = perCollOverrides.get(coll) || {};
    const initial = await loadOffset(coll, overrides);
    pollState.set(coll, { since: initial.since, lastId: initial.lastId });
    pollers.set(coll, { running: false, delay: POLL_MS });

    logger.info(
      {
        coll,
        streamId,
        timeField,
        pollMs: POLL_MS,
        initialSinceIso: initial.since?.toISOString?.(),
        initialSource: initial.used,
        overrideInitialBackfillMs: overrides.initialBackfillMs,
        resetOffset: !!overrides.resetOffset,
        guardLagMs: GUARD_LAG_MS,
        batchLimit: BATCH_LIMIT,
      },
      "poller_started"
    );

    await ensureEventIndex(coll);
    pollLoop(coll, streamId).catch(() => {});
  }

  /**
   * Register a stream handler and start polling its collection.
   *
   * @param {string} streamId - "default" => polls 'events', otherwise 'events_<streamId>'
   * @param {(doc: any, ctx: {logger:any, streamId:string, config:any}) => Promise<void>} handler
   * @param {object} [opts]
   * @param {number} [opts.concurrency=8]
   * @param {number} [opts.maxQueue=2048]
   * @param {number} [opts.initialBackfillMs] - Per-stream override for backfill window when no stored offset,
   *                                            or when resetOffset=true. Capped at 7 days.
   * @param {boolean} [opts.resetOffset=false] - If true, ignore any stored offset and start fresh using
   *                                             `initialBackfillMs` (or global default).
   * @param {(doc:any)=>any|Promise<any>} [opts.mapDoc] - Optional per-stream mapper: receives raw Mongo doc,
   *                                                      returns the payload delivered to the handler.
   */
  function onStream(
    streamId,
    handler,
    {
      concurrency = 8,
      maxQueue = 2048,
      initialBackfillMs,
      resetOffset = false,
      mapDoc = null, // NEW
    } = {}
  ) {
    const coll = collFor(streamId, defaultEventsCollection);

    // Store per-collection overrides BEFORE starting poller
    if (initialBackfillMs != null || resetOffset) {
      perCollOverrides.set(coll, {
        initialBackfillMs,
        resetOffset,
      });
    }

    // Register handler, then start poller
    processors.set(streamId, {
      fn: handler,
      concurrency,
      maxQueue,
      queue: new Set(),
      mapDoc, // store mapper
    });

    startPollerForStream(streamId).catch((e) =>
      logger.error({ err: e, streamId }, "start_poller_failed")
    );

    // Unsubscribe (drain any pending tasks for that stream)
    return () => {
      const proc = processors.get(streamId);
      processors.delete(streamId);
      return proc ? Promise.allSettled([...proc.queue]) : Promise.resolve();
    };
  }

  async function shutdown() {
    const shutdownStart = Date.now();
    try {
      // Stop future polls
      for (const coll of pollers.keys()) pollers.delete(coll);

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
      for (const coll of pollState.keys()) {
        const st = pollState.get(coll);
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

  process.on("SIGINT", () => shutdown().finally(() => process.exit(0)));
  process.on("SIGTERM", () => shutdown().finally(() => process.exit(0)));

  return { onStream, shutdown, logger, config };
}

/* -------------------- Example usage (commented) --------------------
// const service = await CreateService();
// service.onStream("payments", async (doc, { logger }) => {
//   // doc is already mapped by mapDoc below
//   logger.info({ preview: doc }, "payments_doc");
// }, {
//   initialBackfillMs: 3600000, // 1h
//   resetOffset: true,
//   mapDoc: (doc) => ({
//     data: doc?.body?.data,
//     receivedAt: doc?.receivedAt,
//   }),
// });
*/
