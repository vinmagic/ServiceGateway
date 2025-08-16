import { ensureTsCollection } from "../mongo.mjs";
import { findStream, normalizeHost } from "../streams.mjs";
import {
  isKnownResultName,
  getResultsCollection,
  waitForResultIn,
  registerKnownResultName,
} from "../results.mjs";
import {
  isJsonContentType,
  isTextLike,
  chooseEventId,
  ensureBodyStream,
  readToBufferWithLimit,
  readCorrelationIdFromHeaders,
  redactHeaders,
} from "../utils.mjs";

export function registerUniversalRoute(app, ctx) {
  const {
    config,
    db,
    gfs,
    eventsCol,
    metricsCol,
    preparedCols,
    streamsRef,
    timeField,
    globalResultsCollection,
    setMetrics,
  } = ctx;

  // GET /:eventId -> poll results
  app.get("/:eventId", async (req, reply) => {
    const eventId = req.params.eventId;
    const host = normalizeHost(req);
    const urlPath = (req.raw.url || "").split("?")[0];

    // find candidate stream by base path (there is no base segment here, but we can pick best-priority rule by host)
    const candidates = streamsRef.cfg.filter(
      (s) => !s._reHost || s._reHost.test(host || "")
    );
    candidates.sort(
      (a, b) => Number(b.priority || 0) - Number(a.priority || 0)
    );
    if (!candidates.length)
      return reply.code(404).send({
        error: "no_matching_stream_for_request",
        details: { path: urlPath, host },
      });

    const streamCfg = candidates[0];
    const streamId = streamCfg.id || "default";
    const resultColName =
      streamCfg?.resultCollection || globalResultsCollection;
    registerKnownResultName(resultColName);
    const resultsCol = await getResultsCollection(
      db,
      globalResultsCollection,
      resultColName
    );

    const doc = await resultsCol.findOne({ correlationId: eventId });
    if (!doc) return reply.send({ status: "processing", streamId, eventId });
    if (doc.status === "done")
      return reply.send({
        status: "done",
        streamId,
        eventId,
        result: doc.result ?? null,
      });
    if (doc.status === "error")
      return reply.code(500).send({
        status: "error",
        streamId,
        eventId,
        error: doc.error ?? "processing_failed",
      });
    return reply.send({
      status: String(doc.status || "processing"),
      streamId,
      eventId,
    });
  });
  app.route({
    method: ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"],
    url: "/*",
    handler: async (req, reply) => {
      // skip internal endpoints
      const rawPath = req.raw.url || "";
      if (
        rawPath.startsWith("/__health") ||
        rawPath.startsWith("/__version") ||
        rawPath.startsWith("/__body/")
      ) {
        return reply.code(200).send({ ok: true });
      }

      const t0Handler = process.hrtime.bigint();
      const receivedAt = new Date();
      const urlPath = rawPath.split("?")[0];
      const method = req.method;
      const host = normalizeHost(req);
      const matched = findStream(streamsRef.cfg, method, urlPath, host);

      // deny-by-default
      if (!matched && config.server.denyByDefault) {
        ctx.deniedCol
          .insertOne({
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
          })
          .catch((err) => req.log.warn({ err }, "deny_default_audit_failed"));
        setMetrics(req, {
          route: { method, path: urlPath, streamId: "deny-default" },
          code: 403,
          outcomeError: "denied_by_default",
        });
        return reply.code(403).send({ error: "forbidden" });
      }

      // deny rule
      if (matched && String(matched.action).toLowerCase() === "deny") {
        ctx.deniedCol
          .insertOne({
            at: new Date(),
            route: {
              method,
              path: urlPath,
              host,
              streamId: matched.id || "deny",
            },
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
          })
          .catch((err) => req.log.warn({ err }, "deny_rule_audit_failed"));
        setMetrics(req, {
          route: { method, path: urlPath, streamId: matched.id || "deny" },
          code: 403,
          outcomeError: "denied_by_stream_rule",
        });
        return reply.code(403).send({ error: "forbidden" });
      }

      // results ingest path (when a rule's 'collection' is a results collection)
      if (matched && isKnownResultName(matched.collection)) {
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
            payload = { base64: buf.toString("base64") };
          }

          const headerCid = readCorrelationIdFromHeaders(req);
          const correlationId =
            headerCid || payload?.correlationId || payload?.eventId;
          if (!correlationId)
            return reply.code(400).send({ error: "missing_correlation_id" });

          const allowed = new Set(["processing", "done", "error"]);
          const status = allowed.has(
            String(payload?.status || "").toLowerCase()
          )
            ? String(payload.status).toLowerCase()
            : "done";

          const result = Object.prototype.hasOwnProperty.call(payload, "result")
            ? payload.result
            : payload;
          const error = payload?.error ?? null;
          const resultsCol = await getResultsCollection(
            db,
            globalResultsCollection,
            matched.collection
          );

          await resultsCol.updateOne(
            { correlationId },
            {
              $set: {
                correlationId,
                status,
                result,
                error,
                updatedAt: new Date(),
                expiresAt: new Date(
                  Date.now() + config.mongo.resultsTtlDays * 86400_000
                ),
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
            ...(config.server?.debugResponses
              ? { message: String(e?.message) }
              : {}),
          });
        }
      }

      // normal capture path
      const streamId = matched?.id || "default";
      const targetColName =
        matched?.collection || config.mongo.eventsCollection;
      const targetCol = preparedCols.has(targetColName)
        ? preparedCols.get(targetColName)
        : await ensureTsCollection(db, targetColName, {
            timeField,
            metaField: config.mongo.timeSeries.metaField,
            granularity: config.mongo.timeSeries.granularity,
            expireAfterSeconds: config.mongo.retentionDays * 86400,
          });

      if (!preparedCols.has(targetColName)) {
        await Promise.allSettled([
          targetCol.createIndex({ "route.streamId": 1, "route.path": 1 }),
          targetCol.createIndex({ eventId: 1 }),
          targetCol.createIndex({ "route.streamId": 1, [timeField]: -1 }),
          targetCol.createIndex({ correlationId: 1, [timeField]: -1 }),
        ]);
        preparedCols.set(targetColName, targetCol);
      }

      const contentType =
        req.headers["content-type"] || "application/octet-stream";
      const declaredLen = Number(req.headers["content-length"] || 0);
      const transferEncoding = req.headers["transfer-encoding"];
      const isChunked = /chunked/i.test(transferEncoding || "");
      const limit = config.server.maxBodyBytes;

      const eventId = chooseEventId();
      const headerCid = readCorrelationIdFromHeaders(req);
      const correlationId = headerCid || eventId;

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
        return reply
          .header("Connection", "close")
          .code(413)
          .send({ error: "payload_too_large" });
      }

      let bodyInfo = { type: "none", size: 0, data: null };
      let payloadHash = null;
      let gridfsId = null;

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
          } else {
            // small inline or GridFS
            const { buf, size, hash } = await readToBufferWithLimit(
              bodyStream,
              limit
            );
            payloadHash = hash;
            if (size === 0) {
              bodyInfo = { type: "none", size: 0, data: null };
            } else if (size <= config.server.inlineMaxBytes) {
              if (isJsonContentType(contentType)) {
                try {
                  bodyInfo = {
                    type: "json",
                    size,
                    data: JSON.parse(buf.toString("utf8")),
                  };
                } catch {
                  bodyInfo = { type: "text", size, data: buf.toString("utf8") };
                }
              } else {
                bodyInfo = { type: "text", size, data: buf.toString("utf8") };
              }
            } else {
              // store to GridFS
              const { id } = await new Promise((resolve, reject) => {
                const up = gfs.openUploadStream(`${eventId}`, {
                  contentType,
                  metadata: {
                    eventId,
                    correlationId,
                    streamId,
                    path: urlPath,
                    method,
                    receivedAt,
                  },
                });
                up.on("error", reject).on("finish", () =>
                  resolve({ id: up.id })
                );
                up.end(buf);
              });
              bodyInfo = { type: "gridfs", size, data: id };
              gridfsId = id;
            }
          }
        } else {
          // binary – direct to GridFS
          const { id } = await new Promise((resolve, reject) => {
            const up = gfs.openUploadStream(`${eventId}`, {
              contentType,
              metadata: {
                eventId,
                correlationId,
                streamId,
                path: urlPath,
                method,
                receivedAt,
              },
            });
            up.on("error", reject).on("finish", () => resolve({ id: up.id }));
            bodyStream.pipe(up);
          });
          bodyInfo = { type: "gridfs", size: declaredLen || 0, data: id };
          gridfsId = id;
        }

        req._metrics.readNs = process.hrtime.bigint() - t0Read;

        const t0Store = process.hrtime.bigint();
        const doc = {
          eventId,
          correlationId,
          [timeField]: receivedAt,
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
          headers: redactHeaders(req.headers, [
            String(config.security?.apiKeys?.header || "").toLowerCase(),
          ]),
          query: req.query,
          body: bodyInfo,
          meta: {
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
          .header("X-Correlation-Id", correlationId)
          .header("X-Stream-Id", streamId);
        if (bodyInfo.type === "gridfs" && gridfsId)
          res
            .header("X-Body-Store", "gridfs")
            .header("X-Body-Id", String(gridfsId));

        // optional short wait
        const shouldWait = !!matched?.waitForResult;
        const waitMs = Number.isFinite(matched?.waitMs)
          ? Number(matched.waitMs)
          : config.server.syncWaitMs;

        if (shouldWait && waitMs > 0) {
          const resultColName =
            matched?.resultCollection || globalResultsCollection;
          registerKnownResultName(resultColName);
          const resultsCol = await getResultsCollection(
            db,
            globalResultsCollection,
            resultColName
          );

          let fast = { ready: false };
          try {
            fast = await waitForResultIn(resultsCol, correlationId, waitMs);
          } catch {
            fast = { ready: false };
          }

          if (fast.ready && fast.doc?.status === "done")
            return res.code(200).send({
              status: "done",
              eventId,
              streamId,
              result: fast.doc.result ?? null,
            });
          if (fast.ready && fast.doc?.status === "error")
            return res.code(500).send({
              status: "error",
              eventId,
              streamId,
              error: fast.doc.error ?? "processing_failed",
            });
        }

        return res.code(202).send({
          status: "accepted",
          eventId,
          streamId,
          pollUrl: `/${eventId}`,
          resultCollection:
            matched?.resultCollection || globalResultsCollection,
        });
      } catch (e) {
        if (e?.message === "payload_too_large") {
          req._metrics.code = 413;
          req._metrics.outcomeError = "payload_too_large";
          req._metrics.totalHandlerNs = process.hrtime.bigint() - t0Handler;
          return reply
            .header("Connection", "close")
            .code(413)
            .send({ error: "payload_too_large" });
        }
        req._metrics.code = 500;
        req._metrics.outcomeError = e?.message || "capture_failed";
        req._metrics.totalHandlerNs = process.hrtime.bigint() - t0Handler;
        req.log.error(e, "capture_error");
        return reply.code(500).send({
          error: "capture_failed",
          ...(config.server?.debugResponses
            ? { message: String(e?.message), stack: e?.stack }
            : {}),
        });
      }
    },
  });
}
