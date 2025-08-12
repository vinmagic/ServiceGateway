// demoStream.mjs
import { CreateService } from "../MicroService/src/main.mjs";

const svc = await CreateService({
  appName: "cube",
  serviceName: "stream_consumer",
  configPath: "./config.yml",
});

// catch-all during testing
const unDefault = svc.onStream("default", async (event, { logger }) => {
  logger.info(
    { eventId: event?.eventId, path: event?.route?.path },
    "default_stream_event"
  );
});

// ctrl+c clean shutdown if you want
process.on("SIGINT", async () => {
  unDefault();
  await svc.shutdown();
  process.exit(0);
});
