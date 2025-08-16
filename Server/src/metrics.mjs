const nsToMs = (ns) => (ns ? Number(BigInt.asUintN(64, ns)) / 1e6 : 0);

export function setMetrics(req, overrides = {}) {
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

export function registerMetricsHook(app, metricsCol) {
  app.addHook("onResponse", async (req, reply) => {
    try {
      const totalNs = req._t0_req ? process.hrtime.bigint() - req._t0_req : 0n;
      const m = req._metrics || {};
      const metricsDoc = {
        at: new Date(),
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
        sizes: { declaredLen: m.declaredLen, bodySize: m.bodySize },
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
      metricsCol
        .insertOne(metricsDoc)
        .catch((err) => req.log.warn({ err }, "metrics_insert_failed"));
    } catch (err) {
      req.log.warn({ err }, "metrics_hook_failed");
    }
  });
}
