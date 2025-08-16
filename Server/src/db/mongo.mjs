import { MongoClient, GridFSBucket } from "mongodb";
import { config } from "../config/config.mjs";

export const client = new MongoClient(config.mongo.uri, {
  maxPoolSize: 50,
  minPoolSize: 0,
  maxIdleTimeMS: 30000,
  serverSelectionTimeoutMS: 5000,
  heartbeatFrequencyMS: 10000,
});
await client.connect();

export const db = client.db(config.mongo.db);
export const gfs = new GridFSBucket(db, { bucketName: "bodies" });