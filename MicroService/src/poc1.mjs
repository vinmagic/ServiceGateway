import { MongoClient, Timestamp } from "mongodb";

const uri = "";
// watch-proof.mjs  (Node 18+)
// ts_probe.mjs
const dbName = "servicegateway";
const tsName = "events"; // time-series view
const timeField = "receivedAt"; // from your TS config
const metaField = "route";

const client = new MongoClient(uri, { readPreference: "primary" });
await client.connect();
const db = client.db(dbName);

// start slightly in the past to avoid any race
const startAt = new Timestamp({ t: Math.floor(Date.now() / 1000) - 5, i: 1 });

// 1) Watch EVERYTHING at DB scope (no filters)
const cs = db.watch([], {
  showExpandedEvents: true,
  fullDocument: "updateLookup",
  startAtOperationTime: startAt,
});
(async () => {
  try {
    for await (const ev of cs) {
      console.log("[DB-CS]", ev.operationType, ev.ns, ev.fullDocument ?? {});
    }
  } catch (e) {
    console.error("[DB-CS error]", e);
  }
})();

// 2) Also watch with a loose filter for TS in case opType differs
const csTs = db.watch(
  [
    {
      $match: {
        "ns.db": dbName,
        "ns.coll": tsName,
        operationType: { $in: ["insert", "update", "replace"] },
      },
    },
  ],
  {
    showExpandedEvents: true,
    fullDocument: "updateLookup",
    startAtOperationTime: startAt,
  }
);
(async () => {
  try {
    for await (const ev of csTs) {
      console.log("[TS-CS]", ev.operationType, ev.ns, ev.fullDocument ?? {});
    }
  } catch (e) {
    console.error("[TS-CS error]", e);
  }
})();

// 3) Self-test inserts after watchers are live
setTimeout(async () => {
  try {
    await db.collection("debug").insertOne({ t: new Date(), x: "probe" });
    await db.collection(tsName).insertOne({
      [timeField]: new Date(),
      [metaField]: { path: "/probe", method: "POST" },
      value: Math.random(),
    });
    console.log("Inserted self-test docs.");
  } catch (e) {
    console.error("Self-test insert failed:", e);
  }
}, 1500);

console.log("Probing… leave running and watch the logs.");
