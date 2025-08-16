import fp from "fastify-plugin";

export default fp(async (app, opts) => {
  const apikeys = opts?.config?.security?.apiKeys;
  if (!apikeys?.enabled) return;

  app.addHook("onRequest", (req, reply, done) => {
    const headerName = String(apikeys.header || "").toLowerCase();
    const rawVal = req.headers[headerName];
    const key = Array.isArray(rawVal) ? rawVal[0] : rawVal;
    if (!key || !apikeys.allow?.includes(key)) {
      reply.code(401).send({ error: "invalid_api_key" });
      return;
    }
    done();
  });
});