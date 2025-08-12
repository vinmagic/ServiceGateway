// ts_tail.mjs  — poll time-series reliably (no special privileges needed)
import { MongoClient } from "mongodb";

const uri = "";
// consumer.mjs

const dbName = "servicegateway";
const tsColl = "events";
const timeField = "receivedAt";
const OFFSETS = "_consumer_offsets";
const CONSUMER_ID = "ts_tail:events";
const POLL_MS = 1000;

const client = new MongoClient(uri, { readPreference: "primary" });
await client.connect();
const db = client.db(dbName);

/* ---- Time-series poller ---- */
const col = db.collection(tsColl);
const offCol = db.collection(OFFSETS);
const off = await offCol.findOne({ _id: CONSUMER_ID });
let since = off?.since ? new Date(off.since) : new Date(Date.now() - 10_000);
let lastId = off?._lastId ?? null;

async function saveOffset() {
  await offCol.updateOne(
    { _id: CONSUMER_ID },
    { $set: { since, _lastId: lastId } },
    { upsert: true }
  );
}

async function processMeasurements(docs) {
  for (const d of docs) {
    // TODO: send to your pipeline
    console.log("[MEAS]", d);
    if (d[timeField] > since) since = d[timeField];
    lastId = d._id;
  }
  await saveOffset();
}

async function tick() {
  const query = lastId
    ? { [timeField]: { $gte: since }, _id: { $gt: lastId } }
    : { [timeField]: { $gt: since } };

  const docs = await col
    .find(query)
    .sort({ [timeField]: 1, _id: 1 })
    .limit(2000)
    .toArray();

  if (docs.length) await processMeasurements(docs);
}

const timer = setInterval(
  () => tick().catch((e) => console.error("tick", e)),
  POLL_MS
);

/* ---- Graceful shutdown ---- */
const shutdown = async () => {
  clearInterval(timer);
  try {
    await tick();
  } catch {}
  await client.close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

console.log("Consumer running: CS (non-TS) + TS poller.");
