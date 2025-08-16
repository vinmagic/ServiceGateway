import pino from "pino";
import { Writable } from "stream";

function createMongoLogStream({
  collection,
  appName = "apigw",
  batchSize = 200,
  flushMs = 1500,
}) {
  let buf = [],
    timer = null;
  const flush = async () => {
    if (!buf.length) return;
    const b = buf;
    buf = [];
    try {
      await collection.insertMany(b, { ordered: false });
    } catch {}
  };
  const schedule = () => {
    if (timer) return;
    timer = setTimeout(() => {
      timer = null;
      void flush();
    }, flushMs);
    if (timer.unref) timer.unref();
  };
  const dest = new Writable({
    write(chunk, _enc, cb) {
      try {
        const j = JSON.parse(chunk.toString("utf8"));
        const doc = {
          at: j.time ? new Date(j.time) : new Date(),
          meta: {
            app: j.name || appName,
            level: j.level,
            reqId: j.reqId || j.req?.id,
          },
          msg: j.msg,
          ctx: {
            pid: j.pid,
            hostname: j.hostname,
            route: j.req?.url
              ? { method: j.req?.method, url: j.req?.url }
              : undefined,
            statusCode: j.res?.statusCode,
          },
          raw: j,
        };
        buf.push(doc);
        if (buf.length >= batchSize) {
          void flush().then(() => cb());
        } else {
          schedule();
          cb();
        }
      } catch {
        cb();
      }
    },
    final(cb) {
      void (async () => {
        await flush();
        cb();
      })();
    },
  });
  dest.flushNow = flush;
  return dest;
}

function tee(streams) {
  return new Writable({
    write(chunk, enc, cb) {
      let waiting = 0,
        finished = false;
      const finishOnce = () => {
        if (!finished) {
          finished = true;
          cb();
        }
      };
      for (const s of streams) {
        try {
          const ok = s.write(chunk);
          if (!ok && typeof s.once === "function") {
            waiting++;
            s.once("drain", () => {
              waiting--;
              if (waiting === 0) finishOnce();
            });
          }
        } catch {}
      }
      if (waiting === 0) finishOnce();
    },
    final(cb) {
      let remaining = streams.length;
      if (remaining === 0) return cb();
      const done = () => {
        remaining--;
        if (remaining === 0) cb();
      };
      for (const s of streams) {
        try {
          if (typeof s.end === "function") s.end(done);
          else done();
        } catch {
          done();
        }
      }
    },
  });
}

export function buildLogger({ logsCol }) {
  const mongoLogStream = createMongoLogStream({ collection: logsCol });
  const teeStream = tee([process.stdout, mongoLogStream]);
  const logger = pino({ level: "info" }, teeStream);
  return { logger, teeStream, mongoLogStream };
}
