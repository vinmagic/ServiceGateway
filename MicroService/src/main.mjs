// src/main.mjs
// Poller-only microservice (auto events_<streamId>)
// - No streams.d; no regex matching
// - onStream("<id>") => polls collection "events_<id>"
//   onStream("default") => polls collection "events"
// - Per-collection offsets in _consumer_offsets (since + _id)
// - Per-stream bounded concurrency with backpressure
// - Guard lag for clock skew, exponential backoff on errors
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

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/* -------------------- CreateService -------------------- */
export async function CreateService({
  appName = "cube",
  serviceName = "stream_consumer",
  containerName = `container.${uuidv4()}`,
  configPath = "./config.yml",
} = {}) {
  const config = loadConfig(configPath);
  if (!config?.mongo?.uri) throw new Error("mongo.uri is required");
  if (!config?.mongo?.db) throw new Error("mongo.db is required");

  const logger = createLogger(appName, serviceName, containerName);
  logger.info("service_ready");

  // DB connect
  const client = new MongoClient(config.mongo.uri, {
    maxPoolSize: Number(config.mongo?.maxPoolSize ?? 50),
    readPreference: "primary",
  });
  await client.connect();
  const db = client.db(config.mongo.db);

  // Tunables
  const timeField = config.mongo.timeSeries?.timeField || "receivedAt";
  const POLL_MS = Number(config.mongo.timeSeries?.pollMs ?? 1000);
  const BATCH_LIMIT = Number(config.mongo.timeSeries?.batchLimit ?? 2000);
  const FIND_MAX_TIME_MS = Number(
    config.mongo.timeSeries?.findMaxTimeMs ?? 5000
  );
  const OFFSETS_COLL =
    config.mongo.timeSeries?.offsetsCollection || "_consumer_offsets";
  const MAX_BACKFILL_MS = 7 * 24 * 3600 * 1000;
  const INITIAL_BACKFILL_MS = Math.min(
    Number(config.mongo.timeSeries?.initialBackfillMs ?? 10_000),
    MAX_BACKFILL_MS
  );
  const GUARD_LAG_MS = Number(config.mongo.timeSeries?.guardLagMs ?? 0);
  const defaultEventsCollection = config.mongo.eventsCollection || "events";

  try {
    await db.collection(OFFSETS_COLL).createIndex({ ts: 1 });
  } catch (e) {
    logger.warn({ err: e }, "offsets_index_create_warning");
  }

  // State
  const pollers = new Map(); // coll -> { running:boolean, delay:number }
  const pollState = new Map(); // coll -> { since: Date, lastId: ObjectId|null }
  const processors = new Map(); // streamId -> { fn, concurrency, maxQueue, queue:Set<Promise> }

  async function loadOffset(coll) {
    const row = await db
      .collection(OFFSETS_COLL)
      .findOne({ _id: `ts_tail:${coll}` });
    if (row?.since)
      return { since: new Date(row.since), lastId: row._lastId || null };
    return { since: new Date(Date.now() - INITIAL_BACKFILL_MS), lastId: null };
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
      logger.debug({ streamId }, "no_handler_for_stream");
      return;
    }
    const { fn, concurrency, maxQueue, queue } = proc;

    // Basic backpressure
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
        await fn(doc, { logger: child, streamId, config });
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

    const query = state.lastId
      ? { [timeField]: { $gte: state.since }, _id: { $gt: state.lastId } }
      : { [timeField]: { $gt: state.since } };

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
      do {
        fetched = await pollOnce(coll, streamId);
        // Yield when catching up heavily
        if (fetched >= Math.min(BATCH_LIMIT, 1000))
          await new Promise((r) => setImmediate(r));
      } while (fetched > 0);
      ctl.delay = POLL_MS; // reset backoff after a clean pass
    } catch (err) {
      logger.error({ err, coll }, "poller_error");
      // exponential backoff up to 10x
      ctl.delay = Math.min((ctl.delay || POLL_MS) * 2, POLL_MS * 10);
    } finally {
      ctl.running = false;
      if (pollers.has(coll)) {
        setTimeout(
          () => pollLoop(coll, streamId).catch(() => {}),
          ctl.delay || POLL_MS
        ).unref?.();
      }
    }
  }

  async function startPollerForStream(streamId) {
    const coll = collFor(streamId, defaultEventsCollection);
    if (pollers.has(coll)) return;
    pollState.set(coll, await loadOffset(coll));
    pollers.set(coll, { running: false, delay: POLL_MS });
    logger.info(
      { coll, streamId, timeField, pollMs: POLL_MS },
      "poller_started"
    );
    pollLoop(coll, streamId).catch(() => {});
  }

  function onStream(
    streamId,
    handler,
    { concurrency = 8, maxQueue = 2048 } = {}
  ) {
    processors.set(streamId, {
      fn: handler,
      concurrency,
      maxQueue,
      queue: new Set(),
    });
    // Kick off the poller for this streamId’s collection
    startPollerForStream(streamId).catch((e) =>
      logger.error({ err: e, streamId }, "start_poller_failed")
    );
    // Convenience: return an unsubscribe
    return () => processors.delete(streamId);
  }

  async function shutdown() {
    try {
      // Stop future polls
      for (const coll of pollers.keys()) pollers.delete(coll);

      // Drain all processors
      const waits = [];
      for (const { queue } of processors.values())
        waits.push(Promise.allSettled([...queue]));
      await Promise.allSettled(waits);

      // Persist final offsets
      for (const coll of pollState.keys()) {
        const st = pollState.get(coll);
        if (st) await saveOffset(coll, st.since, st.lastId);
      }
      await client.close();
    } finally {
      logger.info("service_shutdown_done");
    }
  }

  process.on("SIGINT", () => shutdown().finally(() => process.exit(0)));
  process.on("SIGTERM", () => shutdown().finally(() => process.exit(0)));

  return { onStream, shutdown, logger, config };
}
