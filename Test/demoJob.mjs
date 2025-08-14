// worker.mjs
import { CreateService } from "../MicroService/src/main.mjs";

const worker = await CreateService({
  appName: "cube",
  serviceName: "price_engine",
  containerName: process.env.HOSTNAME || "local",
  mongo: {
    uri: process.env.MONGO_URI,
    db: process.env.MONGO_DB,
    logsCollection: "applogs", // same as gateway default
    retentionDays: 7,
  },
});

const { logger, startJob } = worker;

const job = startJob(
  {
    /* state */
  },
  async (state, setInterval) => {
    // do work...
    logger.info({ n: 42 }, "processed_batch");
    setInterval(250);
  },
  (err, setInterval, { logger }) => {
    logger.error({ err }, "job_loop_error");
    setInterval(2000);
  },
  500
);

process.on("SIGINT", async () => {
  job.abort();
  await worker.shutdown();
  process.exit(0);
});
process.on("SIGTERM", async () => {
  job.abort();
  await worker.shutdown();
  process.exit(0);
});
