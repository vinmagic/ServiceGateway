// store.js
// Node 18+
// deps: mongodb

import { MongoClient, ObjectId, ReadPreference, Timestamp } from "mongodb";

const default_dbUrl = `mongodb://localhost:27017`;

// Persistent connection state for each dbUrl
const globalClients = new Map(); // key: dbUrl, value: { client, clientPromise }

let __signalsHooked = false;
function hookSignalsOnce(closeFn) {
  if (__signalsHooked) return;
  __signalsHooked = true;
  ["SIGINT", "SIGTERM"].forEach((signal) => {
    process.on(signal, async () => {
      try {
        await closeFn();
      } catch {}
      process.exit(0);
    });
  });
}

// prefer the constant if present; fall back to the string (works across driver majors)
const DEFAULT_READ_PREF =
  (ReadPreference && ReadPreference.primaryPreferred) || "primaryPreferred";

const ConnectStore = (
  dbUrl = default_dbUrl,
  maxPoolSize = 20,
  minPoolSize = 1,
  maxIdleTimeMS = 300000,
  waitQueueTimeoutMS = 10000 // finite default to surface backpressure
) => {
  if (!globalClients.has(dbUrl)) {
    globalClients.set(dbUrl, {
      client: null,
      clientPromise: null,
    });
  }

  const clientState = globalClients.get(dbUrl);

  async function getClient() {
    // If we have an in-flight connect, try to reuse it
    if (clientState.clientPromise) {
      try {
        const c = await clientState.clientPromise;
        await c.db("admin").command({ ping: 1 });
        clientState.client = c;
        return c;
      } catch {
        clientState.client = null;
        clientState.clientPromise = null;
      }
    }

    // If we have a connected client, verify it's alive
    if (clientState.client) {
      try {
        await clientState.client.db("admin").command({ ping: 1 });
        return clientState.client;
      } catch {
        try {
          await clientState.client.close();
        } catch {}
        clientState.client = null;
        clientState.clientPromise = null;
      }
    }

    // Establish a new connection
    const client = new MongoClient(dbUrl, {
      maxPoolSize,
      minPoolSize,
      maxIdleTimeMS,
      waitQueueTimeoutMS,
    });

    clientState.clientPromise = client
      .connect()
      .then(async (connectedClient) => {
        await connectedClient.db("admin").command({ ping: 1 });
        clientState.client = connectedClient;
        return connectedClient;
      })
      .catch((err) => {
        clientState.client = null;
        clientState.clientPromise = null;
        throw err;
      });

    return clientState.clientPromise;
  }

  async function closeClient() {
    if (clientState.client) {
      try {
        await clientState.client.close();
      } catch (error) {
        console.error("Error closing Mongo client:", error);
      } finally {
        clientState.client = null;
        clientState.clientPromise = null;
      }
    }
  }

  // Register OS signal handlers only once per process
  hookSignalsOnce(closeClient);

  const UseDatabase = (dbName, retryInterval = 500) => {
    const collectionInitState = {};

    async function cleanUpWatch(watch) {
      if (watch) {
        try {
          await watch.close();
        } catch {
          // ignore
        }
      }
    }

    const _connectToDB = async () => {
      try {
        const client = await getClient();
        return client.db(dbName);
      } catch (error) {
        console.error("Error connecting to MongoDB:", error.message);
        await closeClient();
        return null;
      }
    };

    return {
      WATCH_OPTIONS: {
        INSERT: "insert",
        UPDATE: "update",
        REPLACE: "replace",
        DELETE: "delete",
      },

      // Renamed to avoid confusion with the exported class
      toObjectId: (id) => (id ? new ObjectId(String(id)) : new ObjectId()),

      getCollection: async (collectionName) => {
        const db = await _connectToDB();
        if (!db) throw new Error(`Error use database ${dbName}`);
        return db.collection(collectionName);
      },

      checkCollectionExists: async (collectionName) => {
        const db = await _connectToDB();
        if (!db) throw new Error(`Error use database ${dbName}`);
        const cur = db.listCollections({ name: collectionName });
        return await cur.hasNext();
      },

      createCollection: async (collectionName, options) => {
        const db = await _connectToDB();
        if (!db) throw new Error(`Error use database ${dbName}`);
        await db.createCollection(collectionName, options);
      },

      createView: async (viewName, sourceCollection, pipeline) => {
        const db = await _connectToDB();
        if (!db) throw new Error(`Error use database ${dbName}`);

        const exists = await db.listCollections({ name: viewName }).hasNext();
        if (exists) {
          await db.collection(viewName).drop();
        }

        await db.createCollection(viewName, {
          viewOn: sourceCollection,
          pipeline,
        });
      },

      /**
       * Start a resilient change stream on a collection.
       *
       * @param {string} collectionName
       * @param {(change|null, infoMsg?: string) => Promise<void>|void} callback
       * @param {{
       *   lastToken?: any,
       *   nextTimestamp?: Timestamp, // MUST be mongodb Timestamp if provided
       *   operations?: Array<'insert'|'update'|'replace'|'delete'>,
       *   readPreference?: any,       // ReadPreference or string
       *   maxAwaitTimeMS?: number,
       *   batchSize?: number,
       *   createIfMissing?: boolean,
       *   retryBaseMs?: number,
       *   retryMaxMs?: number
       * }} options
       * @param {Array<object>} pipelines
       * @returns {{ stop: () => Promise<void>, getResumeToken: () => any }}
       */
      watch: async (
        collectionName,
        callback,
        options = {
          lastToken: null,
          nextTimestamp: null,
          operations: ["insert", "update", "replace", "delete"],
          readPreference: DEFAULT_READ_PREF,
          maxAwaitTimeMS: 500,
          batchSize: 100,
          createIfMissing: false,
          retryBaseMs: 500,
          retryMaxMs: 15000,
        },
        pipelines = []
      ) => {
        let stream = null;
        let stopped = false;
        let processingPromise = Promise.resolve();
        let currentResumeToken = options.lastToken || null;

        const backoff = (attempt) =>
          Math.min(
            options.retryMaxMs,
            options.retryBaseMs * Math.pow(2, attempt)
          );

        const isMongoTimestamp = (v) =>
          v && (v instanceof Timestamp || v._bsontype === "Timestamp");

        const buildPipeline = () => {
          const matchOps = {
            $match: { operationType: { $in: options.operations } },
          };
          // ensure $match is first for efficiency
          return [matchOps, ...pipelines];
        };

        const buildChangeOpts = () => {
          const changeOpts = {
            fullDocument: "updateLookup",
            maxAwaitTimeMS: options.maxAwaitTimeMS,
            batchSize: options.batchSize,
          };
          if (currentResumeToken) {
            changeOpts.resumeAfter = currentResumeToken;
          } else if (options.nextTimestamp) {
            if (!isMongoTimestamp(options.nextTimestamp)) {
              throw new Error("nextTimestamp must be a MongoDB Timestamp");
            }
            changeOpts.startAtOperationTime = options.nextTimestamp;
          }
          return changeOpts;
        };

        const getCollection = async (db) => {
          if (options.createIfMissing && !collectionInitState[collectionName]) {
            const exists = await db
              .listCollections({ name: collectionName })
              .hasNext();
            if (!exists) {
              try {
                await db.createCollection(collectionName);
              } catch {
                // ignore if concurrently created or not permitted
              }
            }
            collectionInitState[collectionName] = true;
          }
          return db.collection(collectionName, {
            readPreference: options.readPreference,
          });
        };

        const openStream = async () => {
          const db = await _connectToDB();
          if (!db) throw new Error(`Error using database ${dbName}`);

          const col = await getCollection(db);
          const pipeline = buildPipeline();
          const changeOpts = buildChangeOpts();

          const s = col.watch(pipeline, changeOpts);

          s.on("change", (change) => {
            processingPromise = processingPromise.then(async () => {
              try {
                await callback(change);
                if (change._id) currentResumeToken = change._id;
              } catch (err) {
                console.error(`Error processing change: ${err.message}`);
              }
            });
          });

          s.on("error", async (err) => {
            await callback(null, `Error watching store: ${err.message}`);
            await closeStream(s);
            if (!stopped) scheduleRetry();
          });

          s.on("end", async () => {
            await callback(null, "Stream ended.");
            await closeStream(s);
            if (!stopped) scheduleRetry();
          });

          return s;
        };

        const closeStream = async (s) => {
          try {
            await s?.close();
          } catch {
            // ignore
          }
          if (stream === s) stream = null;
        };

        let attempt = 0;
        const scheduleRetry = () => {
          const delay = backoff(attempt++);
          setTimeout(() => {
            if (stopped) return;
            tryStart();
          }, delay);
        };

        const tryStart = async () => {
          try {
            stream = await openStream();
            attempt = 0; // reset backoff on success
          } catch (e) {
            await callback(null, `Watch open failed: ${e.message}`);
            scheduleRetry();
          }
        };

        // kick off
        tryStart();

        return {
          stop: async () => {
            stopped = true;
            await closeStream(stream);
            await processingPromise; // drain in-flight handlers
          },
          getResumeToken: () => currentResumeToken,
        };
      },
    };
  };

  return {
    useDatabase: UseDatabase,
    type: "store",
    // expose for manual shutdown if needed
    _closeClient: closeClient,
    _getClient: getClient,
  };
};

export { ConnectStore, ObjectId };
