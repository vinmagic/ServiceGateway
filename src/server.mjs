// apigw/index.mjs
// Node 18+
// deps: fastify, mongodb, js-yaml

import Fastify from "fastify";
import { MongoClient, GridFSBucket } from "mongodb";
import crypto from "crypto";
import fs from "fs";
import yaml from "js-yaml";
import { PassThrough } from "stream";

/* -------------------- Load & basic-validate config -------------------- */
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

/* -------------------- Fastify init -------------------- */
// Set a high bodyLimit; we implement our own limiter/streaming below.
const app = Fastify({
  trustProxy: !!config.server.trustProxy,
  logger: true,
  bodyLimit: Math.max(config.server.maxBodyBytes || 1, 16 * 1024 * 1024),
});

/* -------------------- Mongo bootstrap -------------------- */
const client = new MongoClient(config.mongo.uri, {
  // sensible pool defaults; override via URI if you prefer
  maxPoolSize: 50,
});
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

/* -------------------- Collections (time-series aware) -------------------- */
const preparedCols = new Map(); // name -> collection

async function ensureCollection(name) {
  const exists = await db.listCollections({ name }).hasNext();

  if (!exists) {
    if (config.mongo.useTimeSeries) {
      await db.createCollection(name, {
        timeseries: {
          timeField: config.mongo.timeSeries?.timeField || "receivedAt",
          metaField: config.mongo.timeSeries?.metaField || "route",
          granularity: config.mongo.timeSeries?.granularity || "seconds",
        },
        expireAfterSeconds: config.mongo.retentionDays * 86400,
      });
      app.log.info({ name }, "created time-series collection");
    } else {
      await db.createCollection(name);
      app.log.info({ name }, "created regular collection");
    }
  } else {
    // sanity: warn if mismatch with flag
    const info = await db.command({ listCollections: 1, filter: { name } });
    const coll = info.cursor.firstBatch?.[0];
    const isTS = !!coll?.options?.timeseries;
    if (isTS !== !!config.mongo.useTimeSeries) {
      app.log.warn(
        {
          name,
          isTimeSeries: isTS,
          configWantsTS: !!config.mongo.useTimeSeries,
        },
        "collection/storage type differs from config.mongo.useTimeSeries"
      );
    }
  }

  const col = db.collection(name);

  // Indexing strategy:
  if (config.mongo.useTimeSeries) {
    // In TS collections, index meta fields you filter on.
    await Promise.allSettled([
      col.createIndex({ "route.streamId": 1, "route.path": 1 }),
      col.createIndex({ eventId: 1 }),
    ]);
  } else {
    await Promise.allSettled([
      col.createIndex(
        { receivedAt: 1 },
        { expireAfterSeconds: config.mongo.retentionDays * 86400 }
      ),
      col.createIndex({ "route.streamId": 1, receivedAt: -1 }),
      col.createIndex({ eventId: 1 }),
    ]);
  }

  return col;
}

await ensureCollection(config.mongo.eventsCollection);
const baseCol = db.collection(config.mongo.eventsCollection);

async function getCollectionFor(streamId, name) {
  if (!config.mongo.usePerStreamCollections) return baseCol;
  if (!preparedCols.has(name)) {
    const col = await ensureCollection(name);
    preparedCols.set(name, col);
  }
  return preparedCols.get(name);
}

/* -------------------- Security (API key) -------------------- */
app.addHook("onRequest", async (req, reply) => {
  const apikeys = config.security?.apiKeys;
  if (apikeys?.enabled) {
    const headerName = String(apikeys.header || "").toLowerCase();
    const key = req.headers[headerName];
    if (!key || !apikeys.allow?.includes(key)) {
      reply.code(401).send({ error: "invalid_api_key" });
      return reply; // stop pipeline
    }
  }
});

/* -------------------- Helpers -------------------- */
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

/**
 * Stream req.raw to GridFS with:
 *  - byte counting + limit enforcement
 *  - sha256 hash computation
 * Returns { id, size, hash }
 */
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

  try {
    // No body for GET/HEAD or declared 0
    if (req.method === "GET" || req.method === "HEAD" || declaredLen === 0) {
      bodyInfo = { type: "none", size: 0, data: null };
    } else if (declaredLen && declaredLen <= limit) {
      // Small (by header): buffer fully
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
        // Store as text (you can switch to base64 if you expect binary)
        bodyInfo = {
          type: "text",
          size: buf.length,
          data: buf.toString("utf8"),
        };
      }
    } else {
      // Unknown or large: stream to GridFS with limit + hash
      const { id, size, hash } = await streamToGridFS({
        req,
        filename: `${eventId}`,
        contentType,
        limit,
      });
      bodyInfo = { type: "gridfs", size, data: id };
      payloadHash = `sha256:${hash}`;
    }
  } catch (e) {
    if (e.message === "payload_too_large") {
      return reply.code(413).send({ error: "payload_too_large" });
    }
    req.log.error(e, "capture_error");
    return reply.code(500).send({ error: "capture_failed" });
  }

  const doc = {
    eventId,
    receivedAt, // must be set at insert time for time-series
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
      hash: payloadHash, // sha256:... or null
      contentType,
      contentLength: declaredLen || undefined,
      v: 1,
    },
  };

  // Write-through (fast path)
  await targetCol.insertOne(doc);

  // Respond immediately (don’t proxy; this is a capture gateway)
  reply.code(202).send({ status: "accepted", eventId, streamId });
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
  `API GW listening on :${config.server.port} (ts=${!!config.mongo
    .useTimeSeries})`
);
