// demoStream.mjs
import { CreateService } from "../MicroService/src/main.mjs";

const { onStream, logger, config, getStore, getService,  } =
  await CreateService({
    appName: "test",
  });




const service = getService("http://localhost:3000");

// Will poll "events_payments"
onStream(
  "cargogowhere",
  async (doc, { logger }) => {
    console.log(`---->`, doc.receivedAt);
    await new Promise((resolve, reject) => {
      setTimeout(() => {
        resolve();
      }, 5000);
    });
    console.log(`---->done`);
  },
  {
    concurrency: 1, // one doc at a time => ordered logs
    batchSize: 50, // how many to pull per wave
    idleNudgeMs: 1000, // retry every 1s when empty
    //initialBackfillMs: 24 * 60 * 60 * 1000, // 1h
    //resetOffset: true,
    mapDoc: (doc) => ({
      data: doc?.route?.path,
      receivedAt: doc?.receivedAt,
    }),
    idleNudgeMs: 1000,
  }
);
