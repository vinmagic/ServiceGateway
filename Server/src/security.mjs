export function registerApiKeyHook(app, config, setMetrics) {
  app.addHook("onRequest", async (req, reply) => {
    req._t0_req = process.hrtime.bigint();
    req.clientIp = req.ip;
    req.clientIpChain = req.headers["x-forwarded-for"];

    const apikeys = config.security?.apiKeys;
    if (!apikeys?.enabled) return;
    if (
      !apikeys.header ||
      !Array.isArray(apikeys.allow) ||
      !apikeys.allow.length
    ) {
      throw new Error("security.apiKeys misconfigured");
    }
    const headerName = String(apikeys.header || "").toLowerCase();
    const rawVal = req.headers[headerName];
    const key = Array.isArray(rawVal) ? rawVal[0] : rawVal;
    if (!key || !apikeys.allow?.includes(key)) {
      setMetrics(req, { code: 401, outcomeError: "invalid_api_key" });
      reply.code(401).send({ error: "invalid_api_key" });
      return;
    }
  });
}
