// createService.js (poller-only, improved)
// - streams.d hot-reload; "default" catch-all stream
// - Per-TS-collection poller with resume (since + _id) stored in _consumer_offsets
// - Routes docs to handlers based on method+path matching
// - No overlapping polls; drains until caught up; graceful shutdown

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

/* -------------------- streams.d loader & matcher -------------------- */
function loadYamlFile(fp) {
  try {
    return yaml.load(fs.readFileSync(fp, "utf8"));
  } catch {
    return null;
  }
}

function loadStreamsFromDir(dir) {
  const out = [];
  let entries = [];
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return out;
  }
  for (const ent of entries) {
    if (!ent.isFile()) continue;
    const lower = ent.name.toLowerCase();
    if (!lower.endsWith(".yml") && !lower.endsWith(".yaml")) continue;
    const doc = loadYamlFile(path.join(dir, ent.name));
    if (!doc) continue;
    if (Array.isArray(doc)) out.push(...doc);
    else if (typeof doc === "object") out.push(doc);
  }
  return out;
}

function compileStreams(rawList) {
  return rawList.map((s, i) => {
    const label = s?.id || `streams[${i}]`;
    if (!s?.match?.path) throw new Error(`${label}.match.path is required`);
    const flags =
      typeof s.match?.flags === "string" ? s.match.flags.replace(/g/g, "") : "";
    let _re;
    try {
      _re = new RegExp(s.match.path, flags);
    } catch (e) {
      throw new Error(`Invalid regex in ${label}.match.path: ${e?.message}`);
    }
    const _methods = s.match?.methods
      ? s.match.methods.map((m) => String(m).toUpperCase())
      : null;
    const collection = s.collection || null; // optional per-stream TS collection name
    return { ...s, _re, _methods, collection };
  });
}

function matchStream(list, method, pathStr) {
  const M = String(method || "").toUpperCase();
  for (const s of list) {
    if (s._methods && !s._methods.includes(M)) continue;
    s._re.lastIndex = 0;
    if (s._re.test(pathStr)) return s;
  }
  return null;
}

/* -------------------- helpers -------------------- */
function computeAllowedTsCollections(config, streamsCfg) {
  const names = new Set();
  const eventsCol = config.mongo.eventsCollection || "events";
  names.add(eventsCol);
  for (const s of streamsCfg) {
    if (
      s?.collection &&
      typeof s.collection === "string" &&
      s.collection.trim()
    ) {
      names.add(s.collection.trim());
    }
  }
  return names;
}

/* -------------------- CreateService (poller-only) -------------------- */
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

  // streams.d hot-reload
  const STREAMS_DIR =
    config.server?.streamsDir || path.resolve(process.cwd(), "streams.d");

  const buildStreamsCfg = () =>
    compileStreams([
      ...loadStreamsFromDir(STREAMS_DIR),
      ...(Array.isArray(config.streams) ? config.streams : []),
    ]);

  let streamsRef = { cfg: [] };
  let allowedTs = new Set();

  async function startPoller(coll) {
    if (pollers.has(coll)) return;
    // preload offset state
    pollState.set(coll, await loadOffset(coll));
    pollers.set(coll, { running: false });
    logger.warn({ coll, POLL_MS }, "ts_poller_started");
    pollLoop(coll).catch(() => {});
  }

  function stopPoller(coll) {
    if (!pollers.has(coll)) return;
    pollers.delete(coll);
    logger.info({ coll }, "ts_poller_stopped");
  }

  const reloadStreamsImpl = () => {
    let nextCfg;
    try {
      nextCfg = buildStreamsCfg();
    } catch (e) {
      logger.error({ err: e }, "streams_reload_failed_keep_previous");
      return;
    }
    streamsRef = { cfg: nextCfg };
    const newAllowed = computeAllowedTsCollections(config, streamsRef.cfg);

    // start/stop pollers based on allowed set diffs
    for (const c of newAllowed)
      if (!allowedTs.has(c)) startPoller(c).catch(() => {});
    for (const c of allowedTs) if (!newAllowed.has(c)) stopPoller(c);

    allowedTs = newAllowed;
    logger.info(
      { count: streamsRef.cfg.length, allowedTs: [...allowedTs] },
      "streams_reloaded"
    );
  };

  reloadStreamsImpl();

  let streamsReloadTimer = null;
  try {
    fs.watch(STREAMS_DIR, { persistent: true }, () => {
      if (streamsReloadTimer) clearTimeout(streamsReloadTimer);
      streamsReloadTimer = setTimeout(reloadStreamsImpl, 200);
      streamsReloadTimer.unref?.();
    });
  } catch {
    /* dir may not exist; that's fine */
  }

  // Mongo
  const client = new MongoClient(config.mongo.uri, {
    maxPoolSize: 50,
    readPreference: "primary",
  });
  await client.connect();
  const db = client.db(config.mongo.db);

  const timeField = config.mongo.timeSeries?.timeField || "receivedAt";
  const metaField = config.mongo.timeSeries?.metaField || "route";
  const POLL_MS = Number(config.mongo.timeSeries?.pollMs ?? 1000);
  const OFFSETS_COLL =
    config.mongo.timeSeries?.offsetsCollection || "_consumer_offsets";
  const MAX_BACKFILL_MS = 7 * 24 * 3600 * 1000; // clamp to 7 days
  const INITIAL_BACKFILL_MS = Math.min(
    Number(config.mongo.timeSeries?.initialBackfillMs ?? 10_000),
    MAX_BACKFILL_MS
  );

  // Ensure offsets index exists (unique on _id by default, but explicit is fine)
  try {
    await db.collection(OFFSETS_COLL).createIndex({ ts: 1 });
  } catch (e) {
    logger.warn({ err: e }, "offsets_index_create_warning");
  }

  // ---- Processors registry ----
  const processors = new Map(); // id -> { fn, concurrency }
  function onStream(id, handler, { concurrency = 8 } = {}) {
    processors.set(id, { fn: handler, concurrency });
    return () => processors.delete(id);
  }
  function getProcessor(streamId) {
    return processors.get(streamId) || processors.get("default") || null;
  }

  async function routeDoc(doc, srcTag) {
    if (!doc.route && metaField !== "route" && doc[metaField])
      doc.route = doc[metaField];

    const method = (
      doc?.route?.method ||
      doc?.route?.Method ||
      "GET"
    ).toUpperCase();
    const pathStr =
      doc?.route?.path || doc?.route?.url || doc?.route?.Path || "";

    const matched = matchStream(streamsRef.cfg, method, pathStr);
    const streamId = matched?.id || "default";
    const proc = getProcessor(streamId);

    if (proc) {
      const { fn } = proc;
      await fn(doc, {
        logger: logger.child({ streamId, src: srcTag }),
        streamRule: matched,
        config,
      });
    } else {
      logger.debug(
        { streamId, method, path: pathStr },
        "no_handler_for_stream"
      );
    }
  }

  /* -------------------- Poller core -------------------- */
  const pollers = new Map(); // coll -> { running:boolean }
  const pollState = new Map(); // coll -> { since: Date, lastId: ObjectId|null }

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

  // Return number of docs processed this iteration
  async function pollOnce(coll) {
    const state = pollState.get(coll);
    if (!state) return 0;
    const col = db.collection(coll);

    const query = state.lastId
      ? { [timeField]: { $gte: state.since }, _id: { $gt: state.lastId } }
      : { [timeField]: { $gt: state.since } };

    const docs = await col
      .find(query)
      .sort({ [timeField]: 1, _id: 1 })
      // .hint({ [timeField]: 1, _id: 1 }) // optional if you create this index
      .limit(2000)
      .toArray();

    if (!docs.length) return 0;

    for (const d of docs) {
      if (!d.route && metaField !== "route" && d[metaField])
        d.route = d[metaField];
      await routeDoc(d, "poll");
      if (d[timeField] > state.since) state.since = d[timeField];
      state.lastId = d._id;
    }
    await saveOffset(coll, state.since, state.lastId);
    return docs.length;
  }

  async function pollLoop(coll) {
    const state = pollState.get(coll);
    if (!state) return;
    const ctl = pollers.get(coll);
    if (!ctl || ctl.running) return;
    ctl.running = true;

    try {
      let fetched = 0;
      do {
        fetched = await pollOnce(coll);
      } while (fetched > 0);
    } catch (err) {
      logger.error({ err, coll }, "ts_poller_loop_error");
    } finally {
      ctl.running = false;
      if (pollers.has(coll)) {
        setTimeout(() => pollLoop(coll).catch(() => {}), POLL_MS).unref?.();
      }
    }
  }

  // start pollers for initial allowed set
  for (const coll of allowedTs) await startPoller(coll);

  async function shutdown() {
    try {
      // stop scheduling further loops
      for (const coll of pollers.keys()) {
        const st = pollState.get(coll);
        if (st) await saveOffset(coll, st.since, st.lastId);
      }
      await client.close();
    } finally {
      logger.info("service_shutdown_done");
    }
  }

  // Graceful shutdown hooks
  process.on("SIGINT", () => {
    shutdown().finally(() => process.exit(0));
  });
  process.on("SIGTERM", () => {
    shutdown().finally(() => process.exit(0));
  });

  return { onStream, shutdown, logger, config };
}
