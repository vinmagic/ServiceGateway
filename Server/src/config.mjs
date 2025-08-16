import fs from "fs";
import yaml from "js-yaml";

function assert(cond, msg) {
  if (!cond) throw new Error(`[config] ${msg}`);
}

export function loadConfig(path = "./config.yml") {
  const cfg = yaml.load(fs.readFileSync(path, "utf8"));
  Object.freeze(cfg);

  assert(cfg?.server?.port, "server.port is required");
  assert(cfg?.server?.maxBodyBytes, "server.maxBodyBytes is required");
  assert(cfg?.mongo?.uri, "mongo.uri is required");
  assert(cfg?.mongo?.eventsCollection, "mongo.eventsCollection is required");
  assert(
    typeof cfg?.mongo?.retentionDays === "number",
    "mongo.retentionDays is required"
  );

  return {
    ...cfg,
    server: {
      ...cfg.server,
      syncWaitMs: Number(cfg.server?.syncWaitMs ?? 4000),
      corsAllowOrigin: cfg.server?.corsAllowOrigin ?? "*",
      inlineMaxBytes: Math.min(
        cfg.server?.inlineMaxBytes ?? 1 * 1024 * 1024,
        cfg.server?.maxBodyBytes
      ),
    },
    mongo: {
      ...cfg.mongo,
      metricsCollection: cfg.mongo.metricsCollection || "events_rt",
      logsCollection: cfg.mongo.logsCollection || "applogs",
      deniedCollection: cfg.mongo.deniedCollection || "events_denied",
      resultsCollection: cfg.mongo.resultsCollection || "events_results",
      resultsTtlDays: Number(cfg.mongo.resultsTtlDays ?? 1),
      timeSeries: {
        timeField: cfg.mongo?.timeSeries?.timeField || "receivedAt",
        metaField: cfg.mongo?.timeSeries?.metaField || "route",
        granularity: cfg.mongo?.timeSeries?.granularity || "seconds",
      },
    },
  };
}
