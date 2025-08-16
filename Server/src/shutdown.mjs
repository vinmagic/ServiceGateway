export function registerShutdown(app, client) {
  async function shutdown() {
    try {
      await app.close();
    } catch {}
    try {
      await client.close();
    } catch {}
    process.exit(0);
  }
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
  process.on("SIGHUP", () => {
    console.log("SIGHUP received — restart to reload stream configs");
  });
}
