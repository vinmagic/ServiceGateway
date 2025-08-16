// apigw/index.mjs
// Node 18+
// deps: fastify, mongodb, js-yaml, pino

import Fastify from "fastify";
import { MongoClient, GridFSBucket } from "mongodb";
import crypto from "crypto";
import fs from "fs";
import yaml from "js-yaml";
import { PassThrough, Writable, Readable } from "stream";
import pino from "pino";
import path from "path";

/* -------------------- Load config -------------------- */
const config = yaml.load(fs.readFileSync("./config.yml", "utf8"));
Object.freeze(config);

function assert(cond, msg) {
  if (!cond) throw new Error(`[config] ${msg}`);
}

assert(config?.server?.port, "server.port is required");
assert(config?.server?.maxBodyBytes, "server.maxBodyBytes is required");
assert(config?.mongo?.uri, "mongo.uri is required");
assert(config?.mongo?.eventsCollection, "mongo.eventsCollection is required");
assert(
  typeof config?.mongo?.retentionDays === "number",
  "mongo.retentionDays is required"
);

const DEBUG_RESPONSES = !!config.server?.debugResponses;
const DENY_BY_DEFAULT = !!config.server?.denyByDefault;
const SYNC_WAIT_MS = Number(config.server?.syncWaitMs ?? 4000);
const CORS_ALLOW_ORIGIN = config.server?.corsAllowOrigin ?? "*";

/* Collection names (with sensible defaults) */
const METRICS_COLLECTION = config.mongo.metricsCollection || "events_rt";
const LOGS_COLLECTION = config.mongo.logsCollection || "applogs";
const DENIED_COLLECTION = config.mongo.deniedCollection || "events_denied";
const GLOBAL_RESULTS_COLLECTION =
  config.mongo.resultsCollection || "events_results";
const RESULTS_TTL_DAYS = Number(config.mongo.resultsTtlDays ?? 1);

/* -------------------- Mongo bootstrap -------------------- */
const client = new MongoClient(config.mongo.uri, {
  maxPoolSize: 50,
  minPoolSize: 0,
  maxIdleTimeMS: 30000,
  serverSelectionTimeoutMS: 5000,
  heartbeatFrequencyMS: 10000,
});
await client.connect();

const db = client.db(config.mongo.db);
const gfs = new GridFSBucket(db, { bucketName: "bodies" });

/* -------------------- Helpers: ensure time-series collections -------------------- */
async function ensureTsCollection(
  name,
  { timeField, metaField, granularity, expireAfterSeconds }
) {
  const exists = await db.listCollections({ name }).hasNext();
  if (!exists) {
    await db.createCollection(name, {
      timeseries: { timeField, metaField, granularity },
      expireAfterSeconds,
    });
  } else {
    const [coll] = await db.listCollections({ name }).toArray();
    if (!coll?.options?.timeseries) {
      throw new Error(
        `Collection "${name}" exists but is NOT time-series. Drop/rename it or choose a new name.`
      );
    }
    const currentTTL = coll?.options?.expireAfterSeconds;
    if (
      typeof expireAfterSeconds === "number" &&
      currentTTL !== expireAfterSeconds
    ) {
      try {
        await db.command({ collMod: name, expireAfterSeconds });
      } catch (e) {
        console.warn("ttl_update_failed", { name, err: e?.message });
      }
    }
  }
  return db.collection(name);
}

const TIME_FIELD = config.mongo.timeSeries?.timeField || "receivedAt";

/* -------------------- Prepare TS collections -------------------- */
const eventsCol = await ensureTsCollection(config.mongo.eventsCollection, {
  timeField: TIME_FIELD,
  metaField: config.mongo.timeSeries?.metaField || "route",
  granularity: config.mongo.timeSeries?.granularity || "seconds",
  expireAfterSeconds: config.mongo.retentionDays * 86400,
});
await Promise.allSettled([
  eventsCol.createIndex({ "route.streamId": 1, "route.path": 1 }),
  eventsCol.createIndex({ eventId: 1 }), // TS: non-unique
  eventsCol.createIndex({ "route.streamId": 1, [TIME_FIELD]: -1 }),
  eventsCol.createIndex({ correlationId: 1, [TIME_FIELD]: -1 }),
]);

const metricsCol = await ensureTsCollection(METRICS_COLLECTION, {
  timeField: "at",
  metaField: "route",
  granularity: "seconds",
  expireAfterSeconds:
    (config.mongo.metricsRetentionDays ?? config.mongo.retentionDays) * 86400,
});
await Promise.allSettled([
  metricsCol.createIndex({ "route.streamId": 1, "route.path": 1, at: -1 }),
  metricsCol.createIndex({ eventId: 1 }),
]);

const logsCol = await ensureTsCollection(LOGS_COLLECTION, {
  timeField: "at",
  metaField: "meta",
  granularity: "seconds",
  expireAfterSeconds:
    (config.mongo.logsRetentionDays ?? config.mongo.retentionDays) * 86400,
});
await Promise.allSettled([
  logsCol.createIndex({ "meta.level": 1, "meta.app": 1, at: -1 }),
  logsCol.createIndex({ "meta.reqId": 1, at: -1 }),
]);

// Denied requests audit (time-series)
const deniedCol = await ensureTsCollection(DENIED_COLLECTION, {
  timeField: "at",
  metaField: "route",
  granularity: "seconds",
  expireAfterSeconds:
    (config.mongo.deniedRetentionDays ?? config.mongo.retentionDays) * 86400,
});
await Promise.allSettled([
  deniedCol.createIndex({ "route.host": 1, "route.path": 1, at: -1 }),
  deniedCol.createIndex({ code: 1, at: -1 }),
]);

/* -------------------- Results collections (non-TS) -------------------- */
const preparedResultCols = new Map(); // name -> collection
const knownResultCollectionNames = new Set(); // filled from streams + global fallback

async function ensureResultsCollection(name) {
  const exists = await db.listCollections({ name }).hasNext();
  if (!exists) await db.createCollection(name);
  const col = db.collection(name);
  await Promise.allSettled([
    col.createIndex({ correlationId: 1 }, { unique: true }),
    col.createIndex({ updatedAt: -1 }),
    col.createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }), // absolute TTL
  ]);
  return col;
}

async function getResultsCollectionFor(name) {
  const key = name || GLOBAL_RESULTS_COLLECTION;
  if (!preparedResultCols.has(key)) {
    preparedResultCols.set(key, await ensureResultsCollection(key));
  }
  return preparedResultCols.get(key);
}

/* -------------------- Log streams: Mongo writer + tee -------------------- */
function createMongoLogStream({
  collection,
  appName = "apigw",
  batchSize = 100,
  flushMs = 1000,
}) {
  let buf = [];
  let timer = null;

  const flush = async () => {
    if (!buf.length) return;
    const batch = buf;
    buf = [];
    try {
      await collection.insertMany(batch, { ordered: false });
    } catch {
      /* swallow log insert errors */
    }
  };

  const schedule = () => {
    if (timer) return;
    timer = setTimeout(() => {
      timer = null;
      void flush();
    }, flushMs);
    if (typeof timer.unref === "function") timer.unref();
  };

  const dest = new Writable({
    write(chunk, _enc, cb) {
      try {
        const j = JSON.parse(chunk.toString("utf8"));
        const doc = {
          at: j.time ? new Date(j.time) : new Date(),
          meta: {
            app: j.name || appName,
            level: j.level, // numeric pino level
            reqId: j.reqId || j.req?.id,
          },
          msg: j.msg,
          ctx: {
            pid: j.pid,
            hostname: j.hostname,
            route: j.req?.url
              ? { method: j.req?.method, url: j.req?.url }
              : undefined,
            statusCode: j.res?.statusCode,
          },
          raw: j, // keep original line
        };
        buf.push(doc);
        if (buf.length >= batchSize) {
          void flush().then(() => cb());
        } else {
          schedule();
          cb();
        }
      } catch {
        cb();
      }
    },
    final(cb) {
      void (async () => {
        await flush();
        cb();
      })();
    },
  });

  // expose a manual flush/end hook
  dest.flushNow = flush;
  return dest;
}

/** Tee stream writing to multiple destinations */
function createTeeStream(streams) {
  return new Writable({
    write(chunk, enc, cb) {
      let waiting = 0;
      let finished = false;

      const finishOnce = () => {
        if (!finished) {
          finished = true;
          cb();
        }
      };

      for (const s of streams) {
        try {
          const ok = s.write(chunk);
          if (!ok && typeof s.once === "function") {
            waiting++;
            s.once("drain", () => {
              waiting--;
              if (waiting === 0) finishOnce();
            });
          }
        } catch {
          // ignore
        }
      }

      if (waiting === 0) finishOnce();
    },

    final(cb) {
      let remaining = streams.length;
      if (remaining === 0) return cb();
      const done = () => {
        remaining--;
        if (remaining === 0) cb();
      };
      for (const s of streams) {
        try {
          if (typeof s.end === "function") s.end(done);
          else done();
        } catch {
          done();
        }
      }
    },
  });
}

const mongoLogStream = createMongoLogStream({
  collection: logsCol,
  appName: "apigw",
  batchSize: 200,
  flushMs: 1500,
});
const teeLogStream = createTeeStream([process.stdout, mongoLogStream]);

/* Build Pino logger that writes to our tee stream */
const logger = pino({ level: "info" }, teeLogStream);

/* -------------------- Fastify init -------------------- */
const app = Fastify({
  trustProxy: !!config.server.trustProxy,
  loggerInstance: logger, // use our Pino instance
});

// IMPORTANT: leave Fastify with no body parser so req.raw is an untouched stream
app.removeAllContentTypeParsers();

app.addContentTypeParser("*", (req, payload, done) => {
  done(null, payload);
});

/* -------------------- Streams config (hot-reload) -------------------- */
function loadYamlFile(fp) {
  try {
    const raw = fs.readFileSync(fp, "utf8");
    return yaml.load(raw);
  } catch (e) {
    console.warn("streams_yaml_load_failed", { file: fp, err: e?.message });
    return null;
  }
}

function loadStreamsFromDir(dir) {
  const out = [];
  if (!dir) return out;
  let entries = [];
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return out; // directory optional
  }
  for (const ent of entries) {
    if (!ent.isFile()) continue;
    const lower = ent.name.toLowerCase();
    if (!lower.endsWith(".yml") && !lower.endsWith(".yaml")) continue;
    const fp = path.join(dir, ent.name);
    const doc = loadYamlFile(fp);
    if (!doc) continue;
    if (Array.isArray(doc)) out.push(...doc);
    else if (doc && typeof doc === "object") out.push(doc);
  }
  return out;
}

/* --- robust host normalizer (HTTP/2, LB, IPv6) --- */
function normalizeHost(req) {
  const raw =
    req.headers[":authority"] ||
    (Array.isArray(req.headers["x-forwarded-host"])
      ? req.headers["x-forwarded-host"][0]
      : req.headers["x-forwarded-host"]) ||
    (Array.isArray(req.headers["host"])
      ? req.headers["host"][0]
      : req.headers["host"]) ||
    "";
  let h = raw.trim().toLowerCase();
  // Strip port (handles IPv6 "[::1]:3000")
  if (h.startsWith("[") && h.includes("]")) {
    const i = h.indexOf("]");
    h = h.slice(0, i + 1);
  } else if (h.includes(":")) {
    h = h.split(":")[0];
  }
  // strip trailing dot if any
  if (h.endsWith(".")) h = h.slice(0, -1);
  return h;
}

/* --- compileStreams supports match.host (regex, optional) --- */
function compileStreams(rawList) {
  return rawList.map((s, i) => {
    const label = s?.id || `streams[${i}]`;
    assert(s?.match?.path, `${label}.match.path is required`);
    let _rePath,
      _reHost = null;
    try {
      _rePath = new RegExp(s.match.path);
      if (s.match.host) _reHost = new RegExp(s.match.host);
    } catch (e) {
      throw new Error(`Invalid regex in ${label}: ${String(e?.message || e)}`);
    }
    return {
      ...s,
      _rePath,
      _reHost,
      _methods: s.match.methods
        ? s.match.methods.map((m) => String(m).toUpperCase())
        : null,
    };
  });
}

const streamsDir =
  config.server?.streamsDir || path.resolve(process.cwd(), "streams.d");

// Atomic reference to current compiled config + known result collections
let streamsRef = { cfg: [] };

function buildStreamsCfg() {
  const merged = [
    ...loadStreamsFromDir(streamsDir),
    ...(Array.isArray(config.streams) ? config.streams : []),
  ];
  const compiled = compileStreams(merged);

  // Rebuild known result collections set from all rules:
  knownResultCollectionNames.clear();
  // From request rules declaring resultCollection
  for (const s of compiled) {
    if (s?.resultCollection) {
      knownResultCollectionNames.add(s.resultCollection);
    }
  }
  // Also consider any rule whose 'collection' name looks like a results collection
  for (const s of compiled) {
    if (s?.collection && /^results[_-]/i.test(s.collection)) {
      knownResultCollectionNames.add(s.collection);
    }
  }
  // Always include global fallback
  knownResultCollectionNames.add(GLOBAL_RESULTS_COLLECTION);

  return compiled;
}

// initial load
streamsRef.cfg = buildStreamsCfg();

// debounced watcher
(function watchStreamsDir() {
  let timer = null;
  try {
    fs.watch(streamsDir, { persistent: true }, () => {
      if (timer) clearTimeout(timer);
      timer = setTimeout(async () => {
        try {
          const next = buildStreamsCfg();
          streamsRef = { cfg: next }; // atomic swap
          // pre-create result collections that are referenced
          for (const name of knownResultCollectionNames) {
            await getResultsCollectionFor(name).catch(() => {});
          }
          logger?.info?.({ count: next.length }, "streams_reloaded_from_dir");
        } catch (e) {
          logger?.warn?.({ err: e?.message }, "streams_reload_failed");
        }
      }, 150);
    });
  } catch (e) {
    logger?.warn?.(
      { dir: streamsDir, err: e?.message },
      "streams_watch_failed"
    );
  }
})();

// helper
function findStream(method, urlPath, host) {
  const M = String(method).toUpperCase();
  const list = streamsRef.cfg;
  return list.find((s) => {
    if (s._methods && !s._methods.includes(M)) return false;
    if (s._reHost && !s._reHost.test(host || "")) return false;
    return s._rePath.test(urlPath);
  });
}

const preparedCols = new Map();

async function getCollectionFor(name) {
  if (!config.mongo.usePerStreamCollections) return eventsCol;
  if (!preparedCols.has(name)) {
    const col = await ensureTsCollection(name, {
      timeField: TIME_FIELD,
      metaField: config.mongo.timeSeries?.metaField || "route",
      granularity: config.mongo.timeSeries?.granularity || "seconds",
      expireAfterSeconds: config.mongo.retentionDays * 86400,
    });
    await Promise.allSettled([
      col.createIndex({ "route.streamId": 1, "route.path": 1 }),
      col.createIndex({ eventId: 1 }), // TS: non-unique
      col.createIndex({ "route.streamId": 1, [TIME_FIELD]: -1 }),
      col.createIndex({ correlationId: 1, [TIME_FIELD]: -1 }),
    ]);
    preparedCols.set(name, col);
  }
  return preparedCols.get(name);
}

/* -------------------- Security (API key) -------------------- */
app.addHook("onRequest", async (req, reply) => {
  req._t0_req = process.hrtime.bigint();
  req.clientIp = req.ip;
  req.clientIpChain = req.headers["x-forwarded-for"];
  const apikeys = config.security?.apiKeys;
  if (apikeys?.enabled) {
    assert(apikeys.header, "security.apiKeys.header is required when enabled");
    assert(
      Array.isArray(apikeys.allow) && apikeys.allow.length,
      "security.apiKeys.allow[] is required when enabled"
    );
    const headerName = String(apikeys.header || "").toLowerCase();
    const rawVal = req.headers[headerName];
    const key = Array.isArray(rawVal) ? rawVal[0] : rawVal;
    if (!key || !apikeys.allow?.includes(key)) {
      setMetrics(req, { code: 401, outcomeError: "invalid_api_key" });
      reply.code(401).send({ error: "invalid_api_key" });
      return;
    }
  }
});

/* -------------------- Small helpers -------------------- */
const nsToMs = (ns) => (ns ? Number(ns) / 1e6 : 0);

const isJsonContentType = (ct) =>
  /^application\/(json|.*\+json)/i.test(ct || "");
const isTextLike = (ct) =>
  /^text\//i.test(ct || "") ||
  /^application\/(json|xml|x-www-form-urlencoded|javascript|.*\+json|.*\+xml)/i.test(
    ct || ""
  );

const INLINE_MAX = Math.min(
  config.server.inlineMaxBytes ?? 1 * 1024 * 1024,
  config.server.maxBodyBytes
);

function chooseEventId() {
  return (
    crypto.randomUUID?.() ||
    `${Date.now()}-${crypto.randomBytes(8).toString("hex")}`
  );
}

function ensureBodyStream(req) {
  if (req.body && typeof req.body.pipe === "function") return req.body;
  if (req.raw && typeof req.raw.pipe === "function") return req.raw;
  return Readable.from([]); // empty stream
}

async function readToBufferWithLimit(stream, limit) {
  let counted = 0;
  const hasher = crypto.createHash("sha256");
  const limiter = new PassThrough();
  const chunks = [];

  limiter.on("data", (chunk) => {
    counted += chunk.length;
    hasher.update(chunk);
    if (counted > limit) limiter.destroy(new Error("payload_too_large"));
  });

  await new Promise((resolve, reject) => {
    stream
      .on("error", reject)
      .pipe(limiter)
      .on("error", reject)
      .on("data", (c) => chunks.push(c))
      .on("end", resolve);
  });

  return {
    buf: Buffer.concat(chunks),
    size: counted,
    hash: `sha256:${hasher.digest("hex")}`,
  };
}

// Lazy GridFS: don't create a file until the first byte arrives
function streamToGridFS({ stream, filename, contentType, limit, metadata }) {
  return new Promise((resolve, reject) => {
    let counted = 0;
    let started = false;
    let finished = false;
    const hasher = crypto.createHash("sha256");
    const limiter = new PassThrough();
    const staging = new PassThrough();
    let up = null;

    const bail = (err) => {
      if (finished) return;
      finished = true;
      try {
        if (up) up.abort();
      } catch {}
      reject(err);
    };

    const openUpload = () => {
      if (up) return;
      up = gfs.openUploadStream(filename, { contentType, metadata });
      up.on("error", bail).on("finish", () => {
        if (finished) return;
        finished = true;
        resolve({ id: up.id, size: counted, hash: hasher.digest("hex") });
      });
      staging.pipe(up);
    };

    limiter.on("data", (chunk) => {
      if (!started) {
        started = true;
        openUpload();
      }
      counted += chunk.length;
      hasher.update(chunk);
      if (counted > limit) limiter.destroy(new Error("payload_too_large"));
      staging.write(chunk);
    });

    limiter.on("end", () => {
      if (!started) {
        finished = true;
        return resolve({ id: null, size: 0, hash: "" });
      }
      staging.end();
    });

    limiter.on("error", bail);
    stream.on("error", bail).pipe(limiter);
  });
}

/* -------------------- Metrics + redaction helpers -------------------- */
function setMetrics(req, overrides = {}) {
  const base = {
    route: {
      method: req.method,
      path: (req.raw.url || "").split("?")[0],
      streamId: "default",
    },
    readNs: 0n,
    storeNs: 0n,
    totalHandlerNs: req._t0_req ? process.hrtime.bigint() - req._t0_req : 0n,
    declaredLen: undefined,
    bodySize: 0,
    code: 0,
    outcomeError: null,
  };
  req._metrics = Object.assign(base, req._metrics || {}, overrides);
}

function redactHeaders(h) {
  if (!h) return h;
  const out = { ...h };
  const redactList = [
    "authorization",
    "proxy-authorization",
    String(config.security?.apiKeys?.header || "").toLowerCase(),
    "x-api-key",
    "x-auth-token",
  ].filter(Boolean);
  for (const k of redactList) {
    if (out[k]) out[k] = "[redacted]";
  }
  return out;
}

/* -------------------- Health + version endpoints (excluded from capture) -------------------- */
app.get("/__health", async () => ({ ok: true }));
app.get("/__version", async () => ({
  name: "apigw",
  uptimeSec: Math.round(process.uptime()),
  streams: streamsRef?.cfg?.length ?? 0,
}));

/* -------------------- Wait helper against a specific results collection -------------------- */
async function waitForResultIn(resultsCol, correlationId, timeoutMs) {
  const deadline = Date.now() + Math.max(0, timeoutMs ?? 0);

  // 1) Fast path: already written?
  const existing = await resultsCol.findOne({ correlationId });
  if (existing && (existing.status === "done" || existing.status === "error")) {
    return { ready: true, doc: existing };
  }

  // 2) Change stream (if available) + polling
  let cs = null;
  try {
    cs = resultsCol.watch(
      [
        {
          $match: {
            "fullDocument.correlationId": correlationId,
            operationType: { $in: ["insert", "update", "replace"] },
          },
        },
      ],
      { fullDocument: "updateLookup", maxAwaitTimeMS: 500 }
    );
  } catch {}

  try {
    while (Date.now() < deadline) {
      if (cs) {
        const change = await cs.tryNext();
        if (change && change.fullDocument) {
          const doc = change.fullDocument;
          if (doc.status === "done" || doc.status === "error") {
            try {
              await cs.close();
            } catch {}
            return { ready: true, doc };
          }
        }
      }

      const doc = await resultsCol.findOne({ correlationId });
      if (doc && (doc.status === "done" || doc.status === "error")) {
        if (cs) {
          try {
            await cs.close();
          } catch {}
        }
        return { ready: true, doc };
      }

      await new Promise((r) => setTimeout(r, 120));
    }
  } catch {}

  try {
    if (cs) await cs.close();
  } catch {}
  return { ready: false };
}

/* -------------------- Universal capture route -------------------- */
app.all("/*", async (req, reply) => {
  // CORS preflight handled here (avoid duplicate app.options route)
  if (req.method === "OPTIONS") {
    reply.header("access-control-allow-origin", CORS_ALLOW_ORIGIN);
    reply.header("access-control-allow-headers", "*");
    reply.header(
      "access-control-allow-methods",
      "GET,POST,PUT,PATCH,DELETE,HEAD,OPTIONS"
    );

    return reply.code(204).send();
  }

  // --- Result lookup inside universal handler ---
  // Runs only for GET .../<eventId> where eventId is UUID v1–5 or 32-hex
  if (req.method === "GET") {
    const urlPath = (req.raw.url || "").split("?")[0];
    const m = urlPath.match(
      /\/\/([0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|[0-9a-f]{32})$/i
    );
    if (m) {
      const correlationId = m[1];

      // Fallback: try to infer by matching the BASE path (strip the /:eventId)
      const basePath = urlPath.slice(
        0,
        urlPath.length - (correlationId.length + 1)
      );
      const host = normalizeHost(req);
      // find stream ignoring method (some rules are POST-only)
      const candidates = streamsRef.cfg.filter((s) => {
        if (s._reHost && !s._reHost.test(host || "")) return false;
        return s._rePath.test(basePath);
      });
      candidates.sort(
        (a, b) => Number(b.priority || 0) - Number(a.priority || 0)
      );
      if (!candidates.length) {
        return reply.code(404).send({
          error: "no_matching_stream_for_request",
          details: { path: basePath, host },
        });
      }

      const streamId = candidates[0].id || "default";

      // 2) Find that stream’s configured result collection
      const streamCfg = streamsRef.cfg.find(
        (s) => (s.id || "default") === streamId
      );
      const resultColName =
        streamCfg?.resultCollection || GLOBAL_RESULTS_COLLECTION;

      // 3) Query the right results collection
      knownResultCollectionNames.add(resultColName);
      const resultsCol = await getResultsCollectionFor(resultColName);

      // Prefer matching by correlationId
      const findQuery = { correlationId };

      const doc = await resultsCol.findOne(findQuery);

      if (!doc) {
        return reply.send({
          status: "processing",
          streamId,
          eventId: correlationId,
        });
      }
      if (doc.status === "done") {
        return reply.send({
          status: "done",
          streamId,
          eventId: correlationId,
          result: doc.result ?? null,
        });
      }
      if (doc.status === "error") {
        return reply.code(500).send({
          status: "error",
          streamId,
          eventId: correlationId,
          error: doc.error ?? "processing_failed",
        });
      }

      return reply.send({
        status: String(doc.status || "processing"),
        streamId,
        eventId: correlationId,
      });
    }
  }

  // Skip health without capturing
  if (
    (req.raw.url || "").startsWith("/__health") ||
    (req.raw.url || "").startsWith("/__version")
  ) {
    return reply.code(200).send({ ok: true });
  }

  const t0Handler = process.hrtime.bigint();

  const receivedAt = new Date();
  const urlPath = (req.raw.url || "").split("?")[0];
  const method = req.method;
  const host = normalizeHost(req);

  const matched = findStream(method, urlPath, host);

  // If deny-by-default is enabled, reject when no rule matches
  if (!matched && DENY_BY_DEFAULT) {
    const audit = {
      at: new Date(),
      route: { method, path: urlPath, host, streamId: "deny-default" },
      client: {
        ip: req.clientIp,
        ipChain: req.clientIpChain,
        userAgent: req.headers["user-agent"],
        transferEncoding: req.headers["transfer-encoding"],
      },
      headers: {
        "content-type": req.headers["content-type"],
        "content-length": req.headers["content-length"],
        "x-request-id": req.headers["x-request-id"],
      },
      code: 403,
      reason: "denied_by_default",
    };
    deniedCol
      .insertOne(audit)
      .catch((err) => req.log.warn({ err }, "deny_default_audit_failed"));
    setMetrics(req, {
      route: { method, path: urlPath, streamId: "deny-default" },
      code: 403,
      outcomeError: "denied_by_default",
    });
    return reply.code(403).send({ error: "forbidden" });
  }

  // If a matched rule says action: deny, audit + 403 (before reading body)
  if (matched && String(matched.action).toLowerCase() === "deny") {
    const audit = {
      at: new Date(),
      route: { method, path: urlPath, host, streamId: matched.id || "deny" },
      client: {
        ip: req.clientIp,
        ipChain: req.clientIpChain,
        userAgent: req.headers["user-agent"],
        transferEncoding: req.headers["transfer-encoding"],
      },
      headers: {
        "content-type": req.headers["content-type"],
        "content-length": req.headers["content-length"],
        "x-request-id": req.headers["x-request-id"],
      },
      code: 403,
      reason: "denied_by_stream_rule",
    };
    deniedCol
      .insertOne(audit)
      .catch((err) => req.log.warn({ err }, "deny_rule_audit_failed"));
    setMetrics(req, {
      route: { method, path: urlPath, streamId: matched.id || "deny" },
      code: 403,
      outcomeError: "denied_by_stream_rule",
    });
    return reply.code(403).send({ error: "forbidden" });
  }

  // If we have a match and the matched collection is a RESULT collection (Option A "treat as normal rule"),
  // we perform a results upsert instead of TS insert.
  if (matched && knownResultCollectionNames.has(matched.collection)) {
    try {
      const contentType = req.headers["content-type"] || "application/json";
      const bodyStream = ensureBodyStream(req);
      const { buf, size } = await readToBufferWithLimit(
        bodyStream,
        config.server.maxBodyBytes
      );
      if (size === 0) return reply.code(400).send({ error: "empty_body" });

      let payload = null;
      if (isJsonContentType(contentType)) {
        try {
          payload = JSON.parse(buf.toString("utf8"));
        } catch {
          return reply.code(400).send({ error: "invalid_json" });
        }
      } else if (isTextLike(contentType)) {
        payload = { text: buf.toString("utf8") };
      } else {
        // binary — store opaque bytes base64 if needed
        payload = { base64: buf.toString("base64") };
      }

      const correlationId =
        req.headers["x-correlation-id"] ||
        payload?.correlationId ||
        payload?.eventId;

      if (!correlationId) {
        return reply.code(400).send({ error: "missing_correlation_id" });
      }

      const status = payload?.status || "done";
      const result = Object.prototype.hasOwnProperty.call(payload, "result")
        ? payload.result
        : payload; // store full payload if no result field
      const error = payload?.error ?? null;

      const resultsCol = await getResultsCollectionFor(matched.collection);

      await resultsCol.updateOne(
        { correlationId },
        {
          $set: {
            correlationId,
            status,
            result,
            error,
            updatedAt: new Date(),
            expiresAt: new Date(Date.now() + RESULTS_TTL_DAYS * 86400_000),
            meta: {
              streamId: matched.id,
              host,
              path: urlPath,
              sourceIp: req.clientIp,
              ua: req.headers["user-agent"],
            },
          },
          $setOnInsert: { createdAt: new Date() },
        },
        { upsert: true }
      );

      return reply
        .header("X-Correlation-Id", correlationId)
        .header("X-Result-Collection", matched.collection)
        .code(200)
        .send({ ok: true, correlationId, status });
    } catch (e) {
      req.log.error(e, "result_ingest_failed");
      return reply.code(500).send({
        error: "result_ingest_failed",
        ...(DEBUG_RESPONSES ? { message: String(e?.message) } : {}),
      });
    }
  }

  // ----- Normal capture flow (TS insert + optional short wait) -----

  const streamId = matched?.id || "default";
  const targetCol = await getCollectionFor(
    matched?.collection || config.mongo.eventsCollection
  );

  const contentType = req.headers["content-type"] || "application/octet-stream";
  const declaredLen = Number(req.headers["content-length"] || 0);
  const transferEncoding = req.headers["transfer-encoding"];
  const isChunked = /chunked/i.test(transferEncoding || "");
  const limit = config.server.maxBodyBytes;
  const eventId = chooseEventId();
  const correlationId = eventId; // root correlation is request event id

  // Pre-check: if client declares a too-large body, reject early
  if (declaredLen && declaredLen > limit) {
    const nowNs = process.hrtime.bigint();
    req._metrics = {
      eventId,
      route: { method, path: urlPath, streamId },
      readNs: 0n,
      storeNs: 0n,
      totalHandlerNs: nowNs - t0Handler,
      declaredLen,
      bodySize: 0,
      code: 413,
      outcomeError: "payload_too_large",
    };
    reply
      .header("Connection", "close")
      .code(413)
      .send({ error: "payload_too_large" });
    return;
  }

  let bodyInfo = { type: "none", size: 0, data: null };
  let payloadHash = null;
  let gridfsId = null;

  // metrics scaffold
  req._metrics = {
    eventId,
    route: { method, path: urlPath, streamId },
    readNs: 0n,
    storeNs: 0n,
    totalHandlerNs: 0n,
    declaredLen: declaredLen || undefined,
    bodySize: 0,
    code: 202,
    outcomeError: null,
  };

  try {
    const t0Read = process.hrtime.bigint();

    const bodyStream = ensureBodyStream(req);

    if (req.method === "GET" || req.method === "HEAD") {
      bodyInfo = { type: "none", size: 0, data: null };
    } else if (isTextLike(contentType)) {
      if (!isChunked && declaredLen === 0) {
        bodyInfo = { type: "none", size: 0, data: null };
      } else if (declaredLen && declaredLen > INLINE_MAX) {
        const { id, size, hash } = await streamToGridFS({
          stream: bodyStream,
          filename: `${eventId}`,
          contentType,
          limit,
          metadata: {
            eventId,
            correlationId,
            streamId,
            path: urlPath,
            method,
            receivedAt,
          },
        });
        if (size === 0 || !id) {
          bodyInfo = { type: "none", size: 0, data: null };
        } else {
          bodyInfo = { type: "gridfs", size, data: id };
          payloadHash = `sha256:${hash}`;
          gridfsId = id;
        }
      } else {
        // Buffer text-like payloads up to limit (INLINE_MAX governs how big we keep inline)
        const { buf, size, hash } = await readToBufferWithLimit(
          bodyStream,
          limit
        );
        payloadHash = hash;

        if (size === 0) {
          bodyInfo = { type: "none", size: 0, data: null };
        } else if (size <= INLINE_MAX) {
          if (isJsonContentType(contentType)) {
            try {
              let parsed = JSON.parse(buf.toString("utf8"));
              bodyInfo = { type: "json", size, data: parsed };
            } catch {
              bodyInfo = { type: "text", size, data: buf.toString("utf8") };
            }
          } else {
            bodyInfo = { type: "text", size, data: buf.toString("utf8") };
          }
        } else {
          // Too large to inline; store to GridFS
          const {
            id,
            size: gsize,
            hash: ghash,
          } = await streamToGridFS({
            stream: Readable.from(buf),
            filename: `${eventId}`,
            contentType,
            limit,
            metadata: {
              eventId,
              correlationId,
              streamId,
              path: urlPath,
              method,
              receivedAt,
            },
          });
          if (gsize === 0 || !id) {
            bodyInfo = { type: "none", size: 0, data: null };
          } else {
            bodyInfo = { type: "gridfs", size: gsize, data: id };
            payloadHash = `sha256:${ghash}`;
            gridfsId = id;
          }
        }
      }
    } else {
      // Binary / non text-like
      if (!isChunked && declaredLen === 0) {
        bodyInfo = { type: "none", size: 0, data: null };
      } else {
        const { id, size, hash } = await streamToGridFS({
          stream: bodyStream,
          filename: `${eventId}`,
          contentType,
          limit,
          metadata: {
            eventId,
            correlationId,
            streamId,
            path: urlPath,
            method,
            receivedAt,
          },
        });
        if (size === 0 || !id) {
          bodyInfo = { type: "none", size: 0, data: null };
        } else {
          bodyInfo = { type: "gridfs", size, data: id };
          payloadHash = `sha256:${hash}`;
          gridfsId = id;
        }
      }
    }

    req._metrics.readNs = process.hrtime.bigint() - t0Read;

    const t0Store = process.hrtime.bigint();

    const doc = {
      eventId,
      correlationId, // <— root correlation
      receivedAt, // TS timeField (must be set at insert)
      route: { method, path: urlPath, streamId, host },
      source: {
        ip: req.clientIp,
        ipChain: req.clientIpChain,
        apiKey:
          req.headers[
            String(config.security?.apiKeys?.header || "").toLowerCase()
          ] || undefined,
        userAgent: req.headers["user-agent"],
        transferEncoding,
      },
      headers: redactHeaders(req.headers),
      query: req.query,
      body: bodyInfo,
      meta: {
        hash: payloadHash,
        contentType,
        contentLength: declaredLen || undefined,
        inline: bodyInfo.type !== "gridfs",
        v: 1,
      },
    };

    const targetCol = await getCollectionFor(
      matched?.collection || config.mongo.eventsCollection
    );

    await targetCol.insertOne(doc);

    req._metrics.storeNs = process.hrtime.bigint() - t0Store;
    req._metrics.bodySize = bodyInfo.size || 0;
    req._metrics.totalHandlerNs = process.hrtime.bigint() - t0Handler;

    const res = reply
      .header("X-Event-Id", eventId)
      .header("X-Correlation-Id", correlationId)
      .header("X-Stream-Id", streamId);

    if (bodyInfo.type === "gridfs" && gridfsId) {
      res
        .header("X-Body-Store", "gridfs")
        .header("X-Body-Id", String(gridfsId));
    }

    // Optional short wait per stream using that stream's resultCollection
    const shouldWait = !!matched?.waitForResult;
    const waitMs = Number.isFinite(matched?.waitMs)
      ? Number(matched.waitMs)
      : SYNC_WAIT_MS;

    if (shouldWait && waitMs > 0) {
      const resultColName =
        matched?.resultCollection || GLOBAL_RESULTS_COLLECTION;

      knownResultCollectionNames.add(resultColName);
      const resultsCol = await getResultsCollectionFor(resultColName);

      let fast = { ready: false };
      try {
        fast = await waitForResultIn(resultsCol, correlationId, waitMs);
      } catch {
        fast = { ready: false };
      }

      if (fast.ready && fast.doc?.status === "done") {
        return res.code(200).send({
          status: "done",
          eventId,
          streamId,
          result: fast.doc.result ?? null,
        });
      }
      if (fast.ready && fast.doc?.status === "error") {
        return res.code(500).send({
          status: "error",
          eventId,
          streamId,
          error: fast.doc.error ?? "processing_failed",
        });
      }
    }

    // async fallback
    res.code(202).send({
      status: "accepted",
      eventId,
      streamId,
      pollUrl: `/${eventId}`,
      resultCollection: matched?.resultCollection || GLOBAL_RESULTS_COLLECTION,
    });
  } catch (e) {
    if (e?.message === "payload_too_large") {
      req._metrics.code = 413;
      req._metrics.outcomeError = "payload_too_large";
      req._metrics.totalHandlerNs = process.hrtime.bigint() - t0Handler;
      reply
        .header("Connection", "close")
        .code(413)
        .send({ error: "payload_too_large" });
      return;
    }
    req._metrics.code = 500;
    req._metrics.outcomeError = e?.message || "capture_failed";
    req._metrics.totalHandlerNs = process.hrtime.bigint() - t0Handler;
    req.log.error(e, "capture_error");
    return reply.code(500).send({
      error: "capture_failed",
      ...(DEBUG_RESPONSES
        ? { message: String(e?.message), stack: e?.stack }
        : {}),
    });
  }
});

/* -------------------- Debug body fetch (auth enforced by onRequest) -------------------- */
app.get("/__body/:eventId", async (req, reply) => {
  const { eventId } = req.params;
  let ev = await eventsCol.findOne(
    { eventId },
    { projection: { body: 1, meta: 1 } }
  );
  if (!ev && config.mongo.usePerStreamCollections) {
    for (const [_name, col] of preparedCols.entries()) {
      ev = await col.findOne({ eventId }, { projection: { body: 1, meta: 1 } });
      if (ev) break;
    }
  }
  if (!ev) return reply.code(404).send({ error: "not_found" });
  const ct = ev.meta?.contentType || "application/octet-stream";
  if (ev.body?.type === "gridfs") {
    reply.header("content-type", ct);
    return gfs.openDownloadStream(ev.body.data).pipe(reply.raw);
  }
  reply.header("content-type", ct.includes("json") ? "application/json" : ct);
  if (ev.body?.type === "json") return reply.send(ev.body.data);
  return reply.send(String(ev.body?.data ?? ""));
});

/* -------------------- After-response: write metrics -------------------- */
app.addHook("onResponse", async (req, reply) => {
  try {
    const totalNs = req._t0_req ? process.hrtime.bigint() - req._t0_req : 0n;
    const m = req._metrics || {};
    const metricsDoc = {
      at: new Date(), // TS timeField
      route: m.route || {
        method: req.method,
        path: (req.raw.url || "").split("?")[0],
        streamId: "default",
      },
      eventId: m.eventId,
      durations: {
        readMs: nsToMs(m.readNs || 0n),
        storeMs: nsToMs(m.storeNs || 0n),
        totalHandlerMs: nsToMs(m.totalHandlerNs || 0n),
        responseTotalMs: nsToMs(totalNs),
      },
      sizes: {
        declaredLen: m.declaredLen,
        bodySize: m.bodySize,
      },
      outcome: {
        code: m.code || reply.statusCode,
        error: m.outcomeError || null,
      },
      client: {
        ip: req.clientIp,
        ipChain: req.clientIpChain,
        userAgent: req.headers["user-agent"],
        transferEncoding: req.headers["transfer-encoding"],
      },
    };

    // fire-and-forget
    metricsCol.insertOne(metricsDoc).catch((err) => {
      req.log.warn({ err }, "metrics_insert_failed");
    });
  } catch (err) {
    req.log.warn({ err }, "metrics_hook_failed");
  }
});

/* -------------------- Graceful shutdown -------------------- */
async function shutdown() {
  try {
    // ensure log buffer is flushed
    if (typeof mongoLogStream.flushNow === "function") {
      await mongoLogStream.flushNow();
    }
    // allow tee to end child streams
    try {
      teeLogStream.end();
    } catch {}
    await new Promise((r) => setTimeout(r, 100));
    await app.close();
  } catch {}
  try {
    await client.close();
  } catch {}
  process.exit(0);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
process.on("SIGHUP", () => {
  console.log(
    { streamsDir },
    "SIGHUP received — restart to reload stream configs"
  );
});

/* -------------------- Start server -------------------- */
await app.listen({ port: config.server.port, host: "0.0.0.0" });
app.log.info(
  `API GW listening on :${config.server.port} (TS-only; metrics=${METRICS_COLLECTION}; logs=${LOGS_COLLECTION}; denied=${DENIED_COLLECTION}; denyByDefault=${DENY_BY_DEFAULT})`
);
