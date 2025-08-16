export function registerHealthRoutes(app, streamsCountProvider) {
  app.get("/__health", async () => ({ ok: true }));
  app.get("/__version", async () => ({
    name: "apigw",
    uptimeSec: Math.round(process.uptime()),
    streams: streamsCountProvider(),
  }));
}