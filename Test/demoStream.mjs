// demoStream.mjs
import { CreateService } from "../MicroService/src/main.mjs";

const { onStream, logger } = await CreateService({
  configPath: "./config.yml",
});

// Will poll "events_payments"
onStream("payments", async (doc, { logger }) => {
  logger.info({ id: doc._id, amt: doc.amount }, "payment_processed");
});

// Will poll "events_orders"
onStream("orders", async (doc, { logger }) => {
  logger.info({ id: doc._id, orderId: doc.orderId }, "order_processed");
});

// Will poll "events"
onStream("default", async (doc, { logger }) => {
  logger.info({ path: doc?.route?.path }, "default_processed");
});
