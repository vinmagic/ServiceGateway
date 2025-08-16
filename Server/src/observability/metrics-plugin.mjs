import fp from "fastify-plugin";
import { db } from "../db/mongo.mjs";

export default fp(async (app) => {
  app.addHook("onResponse", async (req, reply) => {
    try {
      const m = req._metrics || {};
      await db.collection("metrics").insertOne({
        at: new Date(),
        route: m.route || {
          method: req.method,
          path: (req.raw.url || "").split("?")[0],
          streamId: "default",
        },
        eventId: m.eventId,
        durations: {
          readMs: m.readMs ?? 0,
          storeMs: m.storeMs ?? 0,
          totalHandlerMs: m.totalHandlerMs ?? 0,
        },
        sizes: {
          declaredLen: m.declaredLen,
          bodySize: m.bodySize,
        },
        outcome: { code: reply.statusCode, error: m.outcomeError || null },
        client: { ip: req.ip, ua: req.headers["user-agent"] },
      });
    } catch (e) {
      req.log.warn({ err: e?.message }, "metrics_insert_failed");
    }
  });
});
