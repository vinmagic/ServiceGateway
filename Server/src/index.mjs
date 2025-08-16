import { buildServer } from "./server.mjs";
import { client } from "./db/mongo.mjs";
import { config } from "./config/config.mjs";

const app = await buildServer();
await app.listen({ port: config.server.port, host: "0.0.0.0" });
app.log.info(`API GW listening on :${config.server.port}`);

async function shutdown() {
  try {
    await app.close();
  } catch {}
  try {
    await client.close();
  } catch {}
  process.exit(0);
}
for (const s of ["SIGINT", "SIGTERM"]) process.on(s, shutdown);
