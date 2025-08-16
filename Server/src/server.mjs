import { loadConfig } from "./config.mjs";
import { connectMongo, ensureTsCollection } from "./mongo.mjs";
import { buildApp } from "./app.mjs";
import { registerShutdown } from "./shutdown.mjs";

const config = loadConfig("./config.yml");
const { client, db, gfs } = await connectMongo(config);
const app = await buildApp({ config, db, gfs });

await app.listen({ port: config.server.port, host: "0.0.0.0" });
app.log.info(
  `API GW listening on :${config.server.port} (denyByDefault=${!!config.server
    .denyByDefault})`
);

registerShutdown(app, client);
