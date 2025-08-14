// demoStream.mjs
import { CreateService } from "../MicroService/src/main.mjs";

const { onStream, logger, config, getStore, getService } = await CreateService({
  appName: "test",
});

const service = getService("http://localhost:3000");


// Will poll "events_payments"
onStream(
  "payments",
  async (doc) => {
    console.log(`---->`, doc);
  },
  {
    initialBackfillMs: 24 * 60 * 60 * 1000, // 1h
    resetOffset: true,
    mapDoc: (doc) => ({
      data: doc?.body?.data,
      receivedAt: doc?.receivedAt,
    }),
  }
);
