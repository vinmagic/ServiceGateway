import { db } from "./mongo.mjs";
import { config } from "../config/config.mjs";

const TIME_FIELD = config.mongo.timeSeries?.timeField || "receivedAt";

export async function ensureTsCollection(
  name,
  {
    timeField = TIME_FIELD,
    metaField = "route",
    granularity = "seconds",
    expireAfterSeconds,
  } = {}
) {
  const exists = await db.listCollections({ name }).hasNext();
  if (!exists) {
    await db.createCollection(name, {
      timeseries: { timeField, metaField, granularity },
      expireAfterSeconds:
        expireAfterSeconds ?? config.mongo.retentionDays * 86400,
    });
  }
}

const preparedCols = new Map();
export async function getCollectionFor(name) {
  if (!preparedCols.has(name)) {
    await ensureTsCollection(name);
    preparedCols.set(name, db.collection(name));
  }
  return preparedCols.get(name);
}
