const KNOWN_RESULTS = new Set();
const preparedResultCols = new Map();

export async function ensureResultsCollection(db, name) {
  const exists = await db.listCollections({ name }).hasNext();
  if (!exists) await db.createCollection(name);
  const col = db.collection(name);
  await Promise.allSettled([
    col.createIndex({ correlationId: 1 }, { unique: true }),
    col.createIndex({ updatedAt: -1 }),
    col.createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }),
  ]);
  return col;
}

export async function getResultsCollection(db, fallbackName, name) {
  const key = name || fallbackName;
  if (!preparedResultCols.has(key)) {
    preparedResultCols.set(key, await ensureResultsCollection(db, key));
  }
  return preparedResultCols.get(key);
}

export function registerKnownResultName(name) {
  KNOWN_RESULTS.add(name);
}
export function isKnownResultName(name) {
  return KNOWN_RESULTS.has(name);
}
export function seedKnownResultNames(names = []) {
  for (const n of names) KNOWN_RESULTS.add(n);
}

export async function waitForResultIn(col, correlationId, timeoutMs) {
  const deadline = Date.now() + Math.max(0, timeoutMs ?? 0);

  const existing = await col.findOne({ correlationId });
  if (existing && (existing.status === "done" || existing.status === "error"))
    return { ready: true, doc: existing };

  let cs = null;
  try {
    cs = col.watch(
      [
        {
          $match: {
            "fullDocument.correlationId": correlationId,
            operationType: { $in: ["insert", "update", "replace"] },
          },
        },
      ],
      { fullDocument: "updateLookup", maxAwaitTimeMS: 500 }
    );
  } catch {}

  try {
    while (Date.now() < deadline) {
      if (cs) {
        const change = await cs.tryNext();
        if (change?.fullDocument) {
          const doc = change.fullDocument;
          if (doc.status === "done" || doc.status === "error") {
            try {
              await cs.close();
            } catch {}
            return { ready: true, doc };
          }
        }
      }
      const doc = await col.findOne({ correlationId });
      if (doc && (doc.status === "done" || doc.status === "error")) {
        if (cs) {
          try {
            await cs.close();
          } catch {}
        }
        return { ready: true, doc };
      }
      await new Promise((r) => setTimeout(r, 120));
    }
  } catch {}

  try {
    if (cs) await cs.close();
  } catch {}
  return { ready: false };
}
