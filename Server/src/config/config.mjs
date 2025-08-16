import fs from "fs";
import yaml from "js-yaml";

export function assert(cond, msg) {
  if (!cond) throw new Error(`[config] ${msg}`);
}

export const config = (() => {
  const c = yaml.load(fs.readFileSync("./config.yml", "utf8"));
  Object.freeze(c);
  assert(c?.server?.port, "server.port is required");
  assert(c?.server?.maxBodyBytes, "server.maxBodyBytes is required");
  assert(c?.mongo?.uri, "mongo.uri is required");
  assert(c?.mongo?.eventsCollection, "mongo.eventsCollection is required");
  assert(typeof c?.mongo?.retentionDays === "number", "mongo.retentionDays is required");
  return c;
})();