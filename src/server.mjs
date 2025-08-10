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

/* -------------------- Load config -------------------- */
const config = yaml.load(fs.readFileSync("./config.yml", "utf8"));
Object.freeze(config);

function assert(cond, msg) {
  if (!cond) throw new Error(`[config] ${msg}`);
}

assert(config?.server?.port, "server.port is required");
assert(config?.server?.maxBodyBytes, "server.maxBodyBytes is required");
assert(config?.mongo?.uri, "mongo.uri is required");
assert(config?.mongo?.db, "mongo.db is required");
assert(config?.mongo?.eventsCollection, "mongo.eventsCollection is required");
assert(
  typeof config?.mongo?.retentionDays === "number",
  "mongo.retentionDays is required"
);

const DEBUG_RESPONSES = !!config.server?.debugResponses;

/* Collection names (with sensible defaults) */
const METRICS_COLLECTION = config.mongo.metricsCollection || "events_rt";
const LOGS_COLLECTION = config.mongo.logsCollection || "applogs";

/* -------------------- Mongo bootstrap -------------------- */
const client = new MongoClient(config.mongo.uri, { maxPoolSize: 50 });
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

/* -------------------- Prepare collections (TS only) -------------------- */
const eventsCol = await ensureTsCollection(config.mongo.eventsCollection, {
  timeField: config.mongo.timeSeries?.timeField || "receivedAt",
  metaField: config.mongo.timeSeries?.metaField || "route",
  granularity: config.mongo.timeSeries?.granularity || "seconds",
  expireAfterSeconds: config.mongo.retentionDays * 86400,
});
await Promise.allSettled([
  eventsCol.createIndex({ "route.streamId": 1, "route.path": 1 }),
  eventsCol.createIndex({ eventId: 1 }),
  eventsCol.createIndex({ "route.streamId": 1, receivedAt: -1 }),
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

/** Tee stream that writes to multiple destinations (stdout + mongo logger) with backpressure, single-callback safety. */
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
          // ignore write errors on individual destinations
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
  // Note: bodyLimit is irrelevant since we disable parsers below.
});

// IMPORTANT: leave Fastify with no body parser so req.raw is an untouched stream
app.removeAllContentTypeParsers();

/* -------------------- Streams config (precompiled) -------------------- */
const streamsCfgRaw = Array.isArray(config.streams) ? config.streams : [];
const streamsCfg = streamsCfgRaw.map((s, i) => {
  assert(s?.match?.path, `streams[${i}].match.path is required`);
  let _re;
  try {
    _re = new RegExp(s.match.path);
  } catch (e) {
    throw new Error(
      `Invalid regex in streams[${i}].match.path: ${String(e?.message || e)}`
    );
  }
  return {
    ...s,
    _re,
    _methods: s.match.methods
      ? s.match.methods.map((m) => String(m).toUpperCase())
      : null,
  };
});
const preparedCols = new Map();

async function getCollectionFor(name) {
  if (!config.mongo.usePerStreamCollections) return eventsCol;
  if (!preparedCols.has(name)) {
    const col = await ensureTsCollection(name, {
      timeField: config.mongo.timeSeries?.timeField || "receivedAt",
      metaField: config.mongo.timeSeries?.metaField || "route",
      granularity: config.mongo.timeSeries?.granularity || "seconds",
      expireAfterSeconds: config.mongo.retentionDays * 86400,
    });
    await Promise.allSettled([
      col.createIndex({ "route.streamId": 1, "route.path": 1 }),
      col.createIndex({ eventId: 1 }),
      col.createIndex({ "route.streamId": 1, receivedAt: -1 }),
    ]);
    preparedCols.set(name, col);
  }
  return preparedCols.get(name);
}

/* -------------------- Security (API key) -------------------- */
app.addHook("onRequest", async (req, reply) => {
  req._t0_req = process.hrtime.bigint(); // earliest stamp for total time
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
      req._metrics = { outcomeError: "invalid_api_key", code: 401 };
      reply.code(401).send({ error: "invalid_api_key" });
      return;
    }
  }
});

/* -------------------- Small helpers -------------------- */
const nsToMs = (ns) => (ns ? Number(ns) / 1e6 : 0);

function findStream(method, path) {
  const M = String(method).toUpperCase();
  return streamsCfg.find(
    (s) => (!s._methods || s._methods.includes(M)) && s._re.test(path)
  );
}

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

/* -------------------- Health endpoint (excluded from capture) -------------------- */
app.get("/__health", async () => ({ ok: true }));

/* -------------------- Universal capture route -------------------- */
app.all("/*", async (req, reply) => {
  // Skip health without capturing
  if ((req.raw.url || "").startsWith("/__health")) {
    return reply.code(200).send({ ok: true });
  }

  const t0Handler = process.hrtime.bigint();

  const receivedAt = new Date();
  const path = (req.raw.url || "").split("?")[0];
  const method = req.method;

  const matched = findStream(method, path);
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

  // Pre-check: if client declares a too-large body, reject early
  if (declaredLen && declaredLen > limit) {
    const nowNs = process.hrtime.bigint();
    req._metrics = {
      eventId,
      route: { method, path, streamId },
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
    route: { method, path, streamId },
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
          metadata: { eventId, streamId, path, method, receivedAt },
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
            metadata: { eventId, streamId, path, method, receivedAt },
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
          metadata: { eventId, streamId, path, method, receivedAt },
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
      receivedAt, // TS timeField (must be set at insert)
      route: { method, path, streamId },
      source: {
        ip: req.ip,
        apiKey:
          req.headers[
            String(config.security?.apiKeys?.header || "").toLowerCase()
          ] || undefined,
        userAgent: req.headers["user-agent"],
        transferEncoding,
      },
      headers: req.headers,
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

    await targetCol.insertOne(doc);

    req._metrics.storeNs = process.hrtime.bigint() - t0Store;
    req._metrics.bodySize = bodyInfo.size || 0;
    req._metrics.totalHandlerNs = process.hrtime.bigint() - t0Handler;

    const res = reply
      .header("X-Event-Id", eventId)
      .header("X-Stream-Id", streamId);

    if (bodyInfo.type === "gridfs" && gridfsId) {
      res
        .header("X-Body-Store", "gridfs")
        .header("X-Body-Id", String(gridfsId));
    }

    res.code(202).send({ status: "accepted", eventId, streamId });
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
  const ev = await eventsCol.findOne(
    { eventId },
    { projection: { body: 1, meta: 1 } }
  );
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
        ip: req.ip,
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

/* -------------------- Start server -------------------- */
await app.listen({ port: config.server.port, host: "0.0.0.0" });
app.log.info(
  `API GW listening on :${config.server.port} (TS-only; metrics=${METRICS_COLLECTION}; logs=${LOGS_COLLECTION})`
);
