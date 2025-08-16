export function registerBodyRoute(
  app,
  { eventsCol, preparedCols, gfs, timeField = "receivedAt" }
) {
  app.get("/__body/:eventId", async (req, reply) => {
    const { eventId } = req.params;
    let ev = await eventsCol.findOne(
      { eventId },
      { projection: { body: 1, meta: 1 } }
    );
    if (!ev && preparedCols) {
      for (const [_name, col] of preparedCols.entries()) {
        ev = await col.findOne(
          { eventId },
          { projection: { body: 1, meta: 1 } }
        );
        if (ev) break;
      }
    }
    if (!ev) return reply.code(404).send({ error: "not_found" });
    const ct = ev.meta?.contentType || "application/octet-stream";
    if (ev.body?.type === "gridfs") {
      reply.header("content-type", ct);
      return gfs.openDownloadStream(ev.body.data).pipe(reply.raw);
    }
    reply.header("content-type", ct.includes("json") ? "application/json" : ct);
    if (ev.body?.type === "json") return reply.send(ev.body.data);
    return reply.send(String(ev.body?.data ?? ""));
  });
}
