// demoStream.mjs
import { CreateService } from "../MicroService/index.mjs";

const { onStream, logger, config, getStore, getService } = await CreateService({
  appName: "test",
});

const responseService = getService("http://localhost:8080");

// Will poll "events_payments"
onStream(
  "cargogowhere",
  async (doc, { logger }) => {
    await new Promise((resolve, reject) => {
      setTimeout(() => {
        resolve();
      }, 5000);
    });
    const result = await responseService.post(`cargogowhere_result/`, {
      headers: {
        ["x-api-key"]: `dev-key-123`,
        ["X-Correlation-Id"]: doc.correlationId,
      },
      json: {
        message: "all done",
      },
      responseType: "json",
    });
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
      correlationId: doc?.correlationId,
    }),
    idleNudgeMs: 1000,
  }
);
