// apigw/index.mjs
// Node 18+
// deps: fastify, mongodb, js-yaml

import Fastify from "fastify";
import { MongoClient, GridFSBucket } from "mongodb";
import crypto from "crypto";
import fs from "fs";
import yaml from "js-yaml";
import { PassThrough } from "stream";

/* -------------------- Load config -------------------- */
const config = yaml.load(fs.readFileSync("./config.yml", "utf8"));

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

/* Optionally allow a custom metrics collection name; default to 'events_rt' */
const METRICS_COLLECTION = config.mongo.metricsCollection || "events_rt";

/* -------------------- Fastify init -------------------- */
const app = Fastify({
  trustProxy: !!config.server.trustProxy,
  logger: true,
  bodyLimit: Math.max(config.server.maxBodyBytes || 1, 16 * 1024 * 1024),
});

/* -------------------- Mongo bootstrap -------------------- */
const client = new MongoClient(config.mongo.uri, { maxPoolSize: 50 });
await client.connect();

const db = client.db(config.mongo.db);
const gfs = new GridFSBucket(db, { bucketName: "bodies" });

/* -------------------- Streams config (precompiled) -------------------- */
const streams = (config.streams || []).map((s, i) => {
  assert(s?.match?.path, `streams[${i}].match.path is required`);
  return {
    ...s,
    _re: new RegExp(s.match.path),
    _methods: s.match.methods || null,
  };
});

/* -------------------- Collections (time-series only) -------------------- */
const preparedCols = new Map();

async function ensureTsCollection(
  name,
  { timeField, metaField, granularity, expireAfterSeconds }
) {
  const exists = await db.listCollections({ name }).hasNext();

  if (!exists) {
    await db.createCollection(name, {
      timeseries: {
        timeField,
        metaField,
        granularity,
      },
      expireAfterSeconds,
    });
    app.log.info({ name }, "created time-series collection");
  } else {
    // Ensure existing collection is time-series
    const info = await db.command({ listCollections: 1, filter: { name } });
    const coll = info.cursor.firstBatch?.[0];
    if (!coll?.options?.timeseries) {
      throw new Error(
        `Collection "${name}" exists but is not time-series. Drop/rename it or choose a new name.`
      );
    }
  }
  return db.collection(name);
}

const eventsCol = await ensureTsCollection(config.mongo.eventsCollection, {
  timeField: config.mongo.timeSeries?.timeField || "receivedAt",
  metaField: config.mongo.timeSeries?.metaField || "route",
  granularity: config.mongo.timeSeries?.granularity || "seconds",
  expireAfterSeconds: config.mongo.retentionDays * 86400,
});
await Promise.allSettled([
  eventsCol.createIndex({ "route.streamId": 1, "route.path": 1 }),
  eventsCol.createIndex({ eventId: 1 }),
]);

/* Metrics time-series: timeField 'at', metaField 'route' */
const metricsCol = await ensureTsCollection(METRICS_COLLECTION, {
  timeField: "at",
  metaField: "route",
  granularity: "seconds",
  expireAfterSeconds:
    (config.mongo.metricsRetentionDays || config.mongo.retentionDays) * 86400,
});
await Promise.allSettled([
  metricsCol.createIndex({ "route.streamId": 1, "route.path": 1 }),
  metricsCol.createIndex({ eventId: 1 }),
]);

async function getCollectionFor(streamId, name) {
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
    ]);
    preparedCols.set(name, col);
  }
  return preparedCols.get(name);
}

/* -------------------- Security (API key) -------------------- */
app.addHook("onRequest", async (req, reply) => {
  req._t0_req = process.hrtime.bigint(); // start clock as early as possible

  const apikeys = config.security?.apiKeys;
  if (apikeys?.enabled) {
    const headerName = String(apikeys.header || "").toLowerCase();
    const key = req.headers[headerName];
    if (!key || !apikeys.allow?.includes(key)) {
      reply.code(401).send({ error: "invalid_api_key" });
      // mark outcome for metrics
      req._metrics = { outcomeError: "invalid_api_key", code: 401 };
      return reply;
    }
  }
});

/* -------------------- Helpers -------------------- */
const nsToMs = (ns) => Number(ns) / 1e6;

function findStream(method, path) {
  return streams.find(
    (s) => (!s._methods || s._methods.includes(method)) && s._re.test(path)
  );
}

function isJsonContentType(ct) {
  return /^application\/(json|.*\+json)/i.test(ct || "");
}

function chooseEventId() {
  return (
    crypto.randomUUID?.() ||
    `${Date.now()}-${crypto.randomBytes(8).toString("hex")}`
  );
}

function streamToGridFS({ req, filename, contentType, limit }) {
  return new Promise((resolve, reject) => {
    let counted = 0;
    const hasher = crypto.createHash("sha256");

    const limiter = new PassThrough();
    limiter.on("data", (chunk) => {
      counted += chunk.length;
      hasher.update(chunk);
      if (counted > limit) {
        limiter.destroy(new Error("payload_too_large"));
      }
    });

    const up = gfs.openUploadStream(filename, { contentType });
    up.on("error", reject).on("finish", () => {
      const hash = hasher.digest("hex");
      resolve({ id: up.id, size: counted, hash });
    });

    req.raw.on("error", reject).pipe(limiter).pipe(up);
  });
}

/* -------------------- Universal capture route -------------------- */
app.all("/*", async (req, reply) => {
  const t0Handler = process.hrtime.bigint();

  const receivedAt = new Date();
  const path = req.raw.url.split("?")[0];
  const method = req.method;

  const matched = findStream(method, path);
  const streamId = matched?.id || "default";
  const targetCol = await getCollectionFor(
    streamId,
    matched?.collection || config.mongo.eventsCollection
  );

  const contentType = req.headers["content-type"] || "application/octet-stream";
  const declaredLen = Number(req.headers["content-length"] || 0);
  const limit = config.server.maxBodyBytes;
  const eventId = chooseEventId();

  let bodyInfo = { type: "none", size: 0, data: null };
  let payloadHash = null;

  // metrics scaffolding for this request
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

    if (req.method === "GET" || req.method === "HEAD" || declaredLen === 0) {
      bodyInfo = { type: "none", size: 0, data: null };
    } else if (declaredLen && declaredLen <= limit) {
      const chunks = [];
      for await (const chunk of req.raw) chunks.push(chunk);
      const buf = Buffer.concat(chunks);

      const hash = crypto.createHash("sha256").update(buf).digest("hex");
      payloadHash = `sha256:${hash}`;

      if (isJsonContentType(contentType)) {
        try {
          let parsed = JSON.parse(buf.toString("utf8"));
          // TODO: apply redactions from matched?.redact?.jsonPaths
          bodyInfo = { type: "json", size: buf.length, data: parsed };
        } catch {
          bodyInfo = {
            type: "text",
            size: buf.length,
            data: buf.toString("utf8"),
          };
        }
      } else {
        bodyInfo = {
          type: "text",
          size: buf.length,
          data: buf.toString("utf8"),
        };
      }
    } else {
      const { id, size, hash } = await streamToGridFS({
        req,
        filename: `${eventId}`,
        contentType,
        limit,
      });
      bodyInfo = { type: "gridfs", size, data: id };
      payloadHash = `sha256:${hash}`;
    }

    req._metrics.readNs = process.hrtime.bigint() - t0Read;

    const t0Store = process.hrtime.bigint();

    const doc = {
      eventId,
      receivedAt,
      route: { method, path, streamId },
      source: {
        ip: req.ip,
        apiKey:
          req.headers[
            String(config.security?.apiKeys?.header || "").toLowerCase()
          ] || undefined,
        userAgent: req.headers["user-agent"],
      },
      headers: req.headers,
      query: req.query,
      body: bodyInfo,
      meta: {
        hash: payloadHash,
        contentType,
        contentLength: declaredLen || undefined,
        v: 1,
      },
    };

    await targetCol.insertOne(doc);

    req._metrics.storeNs = process.hrtime.bigint() - t0Store;
    req._metrics.bodySize = bodyInfo.size || 0;

    req._metrics.totalHandlerNs = process.hrtime.bigint() - t0Handler;

    reply.code(202).send({ status: "accepted", eventId, streamId });
  } catch (e) {
    if (e.message === "payload_too_large") {
      req._metrics.code = 413;
      req._metrics.outcomeError = "payload_too_large";
      req._metrics.totalHandlerNs = process.hrtime.bigint() - t0Handler;
      return reply.code(413).send({ error: "payload_too_large" });
    }
    req._metrics.code = 500;
    req._metrics.outcomeError = e?.message || "capture_failed";
    req._metrics.totalHandlerNs = process.hrtime.bigint() - t0Handler;

    req.log.error(e, "capture_error");
    return reply.code(500).send({ error: "capture_failed" });
  }
});

/* -------------------- After-response metrics write -------------------- */
app.addHook("onResponse", async (req, reply) => {
  try {
    const totalNs = req._t0_req ? process.hrtime.bigint() - req._t0_req : 0n;

    const m = req._metrics || {};
    const metricsDoc = {
      at: new Date(), // time of response completion
      route: m.route || {
        method: req.method,
        path: req.raw.url?.split("?")[0] || "",
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
      // OPTIONAL: include a subset of request metadata (safe to keep small)
      client: {
        ip: req.ip,
        userAgent: req.headers["user-agent"],
      },
    };

    // Fire-and-forget insert so we don't slow down the socket close path
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
  `API GW listening on :${config.server.port} (time-series only; metrics in ${METRICS_COLLECTION})`
);
