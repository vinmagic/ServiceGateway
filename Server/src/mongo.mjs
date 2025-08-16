import { MongoClient, GridFSBucket } from "mongodb";

export async function connectMongo(cfg) {
  const client = new MongoClient(cfg.mongo.uri, {
    maxPoolSize: 50,
    minPoolSize: 0,
    maxIdleTimeMS: 30000,
    serverSelectionTimeoutMS: 5000,
    heartbeatFrequencyMS: 10000,
  });
  await client.connect();
  const db = client.db(cfg.mongo.db);
  const gfs = new GridFSBucket(db, { bucketName: "bodies" });
  return { client, db, gfs };
}

export async function ensureTsCollection(
  db,
  name,
  { timeField, metaField, granularity, expireAfterSeconds }
) {
  const exists = await db.listCollections({ name }).hasNext();
  if (!exists) {
    await db.createCollection(name, {
      timeseries: { timeField, metaField, granularity },
      expireAfterSeconds,
    });
  } else {
    const [coll] = await db.listCollections({ name }).toArray();
    if (!coll?.options?.timeseries) {
      throw new Error(
        `Collection "${name}" exists but is NOT time-series. Drop/rename it or choose a new name.`
      );
    }
    const currentTTL = coll?.options?.expireAfterSeconds;
    if (
      typeof expireAfterSeconds === "number" &&
      currentTTL !== expireAfterSeconds
    ) {
      try {
        await db.command({ collMod: name, expireAfterSeconds });
      } catch (e) {
        console.warn("ttl_update_failed", { name, err: e?.message });
      }
    }
  }
  return db.collection(name);
}
