import Fastify from "fastify";
import path from "path";
import { ensureTsCollection } from "./mongo.mjs";
import { buildLogger } from "./logging.mjs";
import {
  loadStreamsFromDir,
  compileStreams,
  sortByPriority,
} from "./streams.mjs";
import { registerApiKeyHook } from "./security.mjs";
import { registerMetricsHook, setMetrics } from "./metrics.mjs";
import { registerHealthRoutes } from "./routes/health.mjs";
import { registerBodyRoute } from "./routes/body.mjs";
import { registerUniversalRoute } from "./routes/universal.mjs";
import { seedKnownResultNames, registerKnownResultName } from "./results.mjs";

import helmet from "@fastify/helmet";
import cors from "@fastify/cors";
import rateLimit from "@fastify/rate-limit";
import underPressure from "@fastify/under-pressure";

export async function buildApp({ config, db, gfs }) {
  // Collections
  const eventsCol = await ensureTsCollection(
    db,
    config.mongo.eventsCollection,
    {
      timeField: config.mongo.timeSeries.timeField,
      metaField: config.mongo.timeSeries.metaField,
      granularity: config.mongo.timeSeries.granularity,
      expireAfterSeconds: config.mongo.retentionDays * 86400,
    }
  );
  await Promise.allSettled([
    eventsCol.createIndex({ "route.streamId": 1, "route.path": 1 }),
    eventsCol.createIndex({ eventId: 1 }),
    eventsCol.createIndex({
      "route.streamId": 1,
      [config.mongo.timeSeries.timeField]: -1,
    }),
    eventsCol.createIndex({
      correlationId: 1,
      [config.mongo.timeSeries.timeField]: -1,
    }),
  ]);

  const metricsCol = await ensureTsCollection(
    db,
    config.mongo.metricsCollection,
    {
      timeField: "at",
      metaField: "route",
      granularity: "seconds",
      expireAfterSeconds:
        (config.mongo.metricsRetentionDays ?? config.mongo.retentionDays) *
        86400,
    }
  );
  await Promise.allSettled([
    metricsCol.createIndex({ "route.streamId": 1, "route.path": 1, at: -1 }),
    metricsCol.createIndex({ eventId: 1 }),
  ]);

  const logsCol = await ensureTsCollection(db, config.mongo.logsCollection, {
    timeField: "at",
    metaField: "meta",
    granularity: "seconds",
    expireAfterSeconds:
      (config.mongo.logsRetentionDays ?? config.mongo.retentionDays) * 86400,
  });
  const deniedCol = await ensureTsCollection(
    db,
    config.mongo.deniedCollection,
    {
      timeField: "at",
      metaField: "route",
      granularity: "seconds",
      expireAfterSeconds:
        (config.mongo.deniedRetentionDays ?? config.mongo.retentionDays) *
        86400,
    }
  );

  // Logger
  const { logger, teeStream, mongoLogStream } = buildLogger({ logsCol });

  // App
  const app = Fastify({
    trustProxy: !!config.server.trustProxy,
    loggerInstance: logger,
  });
  app.removeAllContentTypeParsers();
  app.addContentTypeParser("*", (req, payload, done) => done(null, payload));

  // Security headers (CSP off unless you explicitly need it)
  await app.register(helmet, {
    contentSecurityPolicy: false,
    crossOriginResourcePolicy: { policy: "cross-origin" },
  });

  // CORS (let plugin handle preflight; no custom OPTIONS route needed)
  await app.register(cors, {
    origin: config.server?.cors?.origins ?? "*", // string | regex | array | function | boolean
    methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
    allowedHeaders: [
      "content-type",
      "authorization",
      "x-api-key",
      "x-correlation-id",
      "x-request-id",
    ],
    maxAge: 600,
  });

  // Rate limit (respect proxies if set)
  await app.register(rateLimit, {
    max: config.server?.rateLimit?.max ?? 100,
    timeWindow: config.server?.rateLimit?.timeWindow ?? "1 minute",
    keyGenerator: (r) =>
      (Array.isArray(r.headers["x-api-key"])
        ? r.headers["x-api-key"][0]
        : r.headers["x-api-key"]) || r.ip,
    allowList: config.server?.rateLimit?.allowList ?? [], // e.g. internal IPs
    continueExceeding: false, // immediately 429
  });

  // Backpressure / health protection
  await app.register(underPressure, {
    maxEventLoopDelay: config.server?.pressure?.maxEventLoopDelay ?? 250, // ms
    maxHeapUsedBytes:
      config.server?.pressure?.maxHeapUsedBytes ?? 512 * 1024 * 1024,
    maxRssBytes: config.server?.pressure?.maxRssBytes, // optional
    healthCheck: async () => ({ ok: true }),
    healthCheckInterval: 5000, // <- add this (ms)
    exposeStatusRoute: false, // keep false since you already have /__health
  });

  registerApiKeyHook(app, config, setMetrics);
  registerMetricsHook(app, metricsCol);

  // Streams
  const streamsDir =
    config.server?.streamsDir || path.resolve(process.cwd(), "streams.d");
  let streamsRef = { cfg: [] };
  const rebuildStreams = () => {
    const raw = loadStreamsFromDir(streamsDir);
    const compiled = sortByPriority(compileStreams(raw));
    // known result collections
    const known = new Set([config.mongo.resultsCollection]);
    for (const s of compiled) {
      if (s?.resultCollection) known.add(s.resultCollection);
      if (s?.collection && /^results[_-]/i.test(s.collection))
        known.add(s.collection);
    }
    seedKnownResultNames([...known]);
    streamsRef = { cfg: compiled };
  };
  rebuildStreams();

  // Watch streams.d
  try {
    let timer = null;
    app.log.info({ streamsDir }, "watching_streams_dir");
    const watcher = (await import("fs")).watch(
      streamsDir,
      { persistent: true },
      () => {
        if (timer) clearTimeout(timer);
        timer = setTimeout(() => {
          try {
            rebuildStreams();
            app.log.info(
              { count: streamsRef.cfg.length },
              "streams_reloaded_from_dir"
            );
          } catch (e) {
            app.log.warn({ err: e?.message }, "streams_reload_failed");
          }
        }, 150);
      }
    );
    app.addHook("onClose", async () => {
      try {
        watcher.close();
      } catch {}
    });
  } catch (e) {
    app.log.warn({ err: e?.message, dir: streamsDir }, "streams_watch_failed");
  }

  // Per-stream TS cache
  const preparedCols = new Map();

  // Routes
  registerHealthRoutes(app, () => streamsRef?.cfg?.length ?? 0);
  registerBodyRoute(app, {
    eventsCol,
    preparedCols,
    gfs,
    timeField: config.mongo.timeSeries.timeField,
  });

  registerUniversalRoute(app, {
    config,
    db,
    gfs,
    eventsCol,
    metricsCol,
    preparedCols,
    streamsRef,
    timeField: config.mongo.timeSeries.timeField,
    globalResultsCollection: config.mongo.resultsCollection,
    setMetrics,
    deniedCol,
  });

  // Graceful shutdown (flush logs)
  app.addHook("onClose", async () => {
    try {
      if (typeof mongoLogStream.flushNow === "function")
        await mongoLogStream.flushNow();
    } catch {}
    try {
      teeStream.end();
    } catch {}
  });

  return app;
}
