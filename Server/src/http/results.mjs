import { db } from "../db/mongo.mjs";

export async function waitForResultIn(collectionName, correlationId, timeoutMs) {
  const deadline = Date.now() + Math.max(0, timeoutMs ?? 0);
  while (Date.now() < deadline) {
    const doc = await db.collection(collectionName).findOne({ correlationId });
    if (doc && (doc.status === "done" || doc.status === "error")) return doc;
    await new Promise(r => setTimeout(r, 150));
  }
  return null;
}