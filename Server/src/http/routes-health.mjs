import fp from "fastify-plugin";

export default fp(async (app) => {
  app.get("/__health", async () => ({ ok: true }));
  app.get("/__version", async () => ({
    name: "apigw",
    uptimeSec: Math.round(process.uptime()),
    env: process.env.NODE_ENV || "development",
  }));
});