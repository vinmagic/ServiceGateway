import fp from "fastify-plugin";
import crypto from "crypto";
import {
  ensureBodyStream,
  readToBufferWithLimit,
  isJsonContentType,
  isTextLike,
} from "./body.mjs";
import { findStream } from "../streams/manager.mjs";
import { db } from "../db/mongo.mjs";
import { waitForResultIn } from "./results.mjs";

export default fp(async (app, opts) => {
  const { config } = opts;
  const CORS_ALLOW_ORIGIN = config.server?.corsAllowOrigin ?? "*";

  app.all("/*", async (req, reply) => {
    // CORS preflight
    if (req.method === "OPTIONS") {
      reply.header("access-control-allow-origin", CORS_ALLOW_ORIGIN);
      reply.header("access-control-allow-headers", "*");
      reply.header(
        "access-control-allow-methods",
        "GET,POST,PUT,PATCH,DELETE,HEAD,OPTIONS"
      );
      return reply.code(204).send();
    }

    // Result lookup by trailing eventId (GET /.../:id)
    if (req.method === "GET") {
      const urlPath = (req.raw.url || "").split("?")[0];
      const m = urlPath.match(
        /\/([0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|[0-9a-f]{32})$/i
      );
      if (m) {
        const correlationId = m[1];
        const resultCol = config.mongo.resultsCollection || "events_results";
        const doc = await db.collection(resultCol).findOne({ correlationId });
        if (!doc)
          return reply.send({ status: "processing", eventId: correlationId });
        if (doc.status === "done")
          return reply.send({
            status: "done",
            eventId: correlationId,
            result: doc.result ?? null,
          });
        if (doc.status === "error")
          return reply
            .code(500)
            .send({
              status: "error",
              eventId: correlationId,
              error: doc.error ?? "processing_failed",
            });
        return reply.send({
          status: String(doc.status || "processing"),
          eventId: correlationId,
        });
      }
    }

    // Match
    const urlPath = (req.raw.url || "").split("?")[0];
    const matched = findStream(req.method, urlPath);
    if (!matched) {
      if (opts.config.server?.denyByDefault)
        return reply.code(403).send({ error: "forbidden" });
      return reply.code(404).send({ error: "no_matching_stream" });
    }

    // Read body
    const bodyStream = ensureBodyStream(req);
    const buf = await readToBufferWithLimit(
      bodyStream,
      config.server.maxBodyBytes
    );

    // Build body doc
    let bodyDoc = { type: "none", size: 0, data: null };
    const contentType =
      req.headers["content-type"] || "application/octet-stream";
    if (buf.length) {
      if (isJsonContentType(contentType)) {
        try {
          bodyDoc = {
            type: "json",
            size: buf.length,
            data: JSON.parse(buf.toString("utf8")),
          };
        } catch {
          bodyDoc = {
            type: "text",
            size: buf.length,
            data: buf.toString("utf8"),
          };
        }
      } else if (isTextLike(contentType)) {
        bodyDoc = {
          type: "text",
          size: buf.length,
          data: buf.toString("utf8"),
        };
      } else {
        bodyDoc = {
          type: "base64",
          size: buf.length,
          data: buf.toString("base64"),
        };
      }
    }

    const eventId = crypto.randomUUID();
    const doc = {
      eventId,
      correlationId: eventId,
      receivedAt: new Date(),
      route: {
        method: req.method,
        path: urlPath,
        streamId: matched.id || "default",
      },
      headers: req.headers,
      body: bodyDoc,
      meta: { contentType, inline: bodyDoc.type !== "base64" },
    };

    // Insert into target collection
    const targetCollection =
      matched.collection || config.mongo.eventsCollection;
    await db.collection(targetCollection).insertOne(doc);

    // Optional short wait for result
    if (matched.waitForResult) {
      const resultCol =
        matched.resultCollection ||
        config.mongo.resultsCollection ||
        "events_results";
      const r = await waitForResultIn(
        resultCol,
        eventId,
        matched.waitMs ?? 1500
      );
      if (r?.status === "done")
        return reply
          .code(200)
          .send({ status: "done", eventId, result: r.result ?? null });
      if (r?.status === "error")
        return reply
          .code(500)
          .send({
            status: "error",
            eventId,
            error: r.error ?? "processing_failed",
          });
      return reply
        .code(202)
        .send({ status: "accepted", eventId, pollUrl: `/${eventId}` });
    }

    return reply
      .code(202)
      .send({ status: "accepted", eventId, pollUrl: `/${eventId}` });
  });
});
