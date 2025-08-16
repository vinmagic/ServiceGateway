import Fastify from "fastify";
import { logger } from "./observability/logger.mjs";
import metricsPlugin from "./observability/metrics-plugin.mjs";
import apikeyPlugin from "./security/apikey-plugin.mjs";
import healthRoutes from "./http/routes-health.mjs";
import universalRoutes from "./http/routes-universal.mjs";
import { initStreams } from "./streams/manager.mjs";
import { config } from "./config/config.mjs";

export async function buildServer() {
  const app = Fastify({
    trustProxy: !!config.server.trustProxy,
    loggerInstance: logger,
  });

  // Keep request body as raw stream
  app.removeAllContentTypeParsers();
  app.addContentTypeParser("*", (req, payload, done) => done(null, payload));

  // Streams
  const streamsRef = await initStreams(app.log, config.server.streamsDir);

  // Plugins & routes
  await app.register(apikeyPlugin, { config });
  await app.register(metricsPlugin, { config });
  await app.register(healthRoutes, { config, streamsRef });
  await app.register(universalRoutes, { config, streamsRef });

  return app;
}
